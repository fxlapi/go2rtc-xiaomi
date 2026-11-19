package xiaomi

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/crypto"
	"github.com/rs/zerolog"
)

// tokenEncPrefix marks an encrypted Mi Home pass token stored in the config.
// The AES-256-GCM key lives in a separate hidden file next to the config
// (never inside the config itself), so a config file that is shared, backed
// up or pasted for support does not leak the account token. Plaintext tokens
// from older versions keep working and are re-encrypted on next login.
const tokenEncPrefix = "ENC:"

func tokenKeyPath() string {
	if app.ConfigPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(app.ConfigPath), ".xiaomi_key")
}

func loadTokenKey() ([]byte, error) {
	p := tokenKeyPath()
	if p == "" {
		return nil, errors.New("xiaomi: config path unknown, can't load token key")
	}
	if key, err := os.ReadFile(p); err == nil && len(key) == 32 {
		return key, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	if err := os.WriteFile(p, key, 0600); err != nil {
		return nil, err
	}
	return key, nil
}

func encryptToken(plain string) (string, error) {
	key, err := loadTokenKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plain), nil)
	return tokenEncPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

func decryptToken(stored string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, tokenEncPrefix))
	if err != nil {
		return "", err
	}
	key, err := loadTokenKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, data[:gcm.NonceSize()], data[gcm.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func Init() {
	var v struct {
		Cfg map[string]string `yaml:"xiaomi"`
	}
	app.LoadConfig(&v)

	// Decrypt tokens stored in encrypted form; plaintext tokens from older
	// versions keep working as-is.
	if v.Cfg != nil {
		for userID, stored := range v.Cfg {
			if !strings.HasPrefix(stored, tokenEncPrefix) {
				continue
			}
			plain, err := decryptToken(stored)
			if err != nil {
				log.Warn().Err(err).Msg("[xiaomi] failed to decrypt stored token")
				continue
			}
			v.Cfg[userID] = plain
		}
	}

	tokens = v.Cfg

	log = app.GetLogger("xiaomi")

	// Upgrade plaintext tokens to encrypted storage on startup so existing
	// users don't need to log in again.
	for userID, stored := range tokens {
		if stored == "" || strings.HasPrefix(stored, tokenEncPrefix) {
			continue
		}
		enc, err := encryptToken(stored)
		if err != nil {
			log.Warn().Err(err).Msg("[xiaomi] token encryption upgrade failed")
			continue
		}
		if err := app.PatchConfig([]string{"xiaomi", userID}, enc); err != nil {
			log.Warn().Err(err).Msg("[xiaomi] token encryption upgrade write failed")
			continue
		}
		log.Info().Msg("[xiaomi] stored token upgraded to encrypted form")
	}

	streams.HandleFunc("xiaomi", func(rawURL string) (core.Producer, error) {
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, err
		}

		if u.User != nil {
			rawURL, err = getCameraURL(u)
			if err != nil {
				return nil, err
			}
		}

		log.Debug().Msgf("xiaomi: dial %s", rawURL)

		return xiaomi.Dial(rawURL)
	})

	api.HandleFunc("api/xiaomi", apiXiaomi)
}

var log zerolog.Logger

var tokens map[string]string
var clouds map[string]*xiaomi.Cloud
var cloudsMu sync.Mutex

// missCred caches MISS authentication credentials so the second channel of a
// dual-channel camera can skip the cloud API call.
type missCred struct {
	clientPublic  string
	clientPrivate string
	devicePublic  string
	sign          string
	vendor        string
	uid           string // only for TUTK
}

var missCreds = map[string]missCred{}
var missCredsMu sync.Mutex

func getCloud(userID string) (*xiaomi.Cloud, error) {
	cloudsMu.Lock()
	defer cloudsMu.Unlock()

	if cloud := clouds[userID]; cloud != nil {
		return cloud, nil
	}

	cloud := xiaomi.NewCloud(AppXiaomiHome)
	if err := cloud.LoginWithToken(userID, tokens[userID]); err != nil {
		return nil, err
	}
	if clouds == nil {
		clouds = map[string]*xiaomi.Cloud{userID: cloud}
	} else {
		clouds[userID] = cloud
	}
	return cloud, nil
}

func cloudRequest(userID, region, apiURL, params string) ([]byte, error) {
	cloud, err := getCloud(userID)
	if err != nil {
		return nil, err
	}
	return cloud.Request(GetBaseURL(region), apiURL, params, nil)
}

func cloudUserRequest(user *url.Userinfo, apiURL, params string) ([]byte, error) {
	userID := user.Username()
	region, _ := user.Password()
	return cloudRequest(userID, region, apiURL, params)
}

func getCameraURL(url *url.URL) (string, error) {
	model := url.Query().Get("model")

	// It is not known which models need to be awakened.
	// Probably all the doorbells and all the battery cameras.
	if strings.Contains(model, ".cateye.") {
		_ = wakeUpCamera(url)
	}

	// The getMissURL request has a fallback to getP2PURL.
	// But for known models we can save one request to the cloud.
	if xiaomi.IsLegacy(model) {
		return getLegacyURL(url)
	}
	return getMissURL(url)
}

func getLegacyURL(url *url.URL) (string, error) {
	query := url.Query()

	clientPublic, clientPrivate, err := crypto.GenerateKey()
	if err != nil {
		return "", err
	}

	params := fmt.Sprintf(`{"did":"%s","toSignAppData":"%x"}`, query.Get("did"), clientPublic)

	userID := url.User.Username()
	region, _ := url.User.Password()
	res, err := cloudRequest(userID, region, "/device/devicepass", params)
	if err != nil {
		return "", err
	}

	var v struct {
		UID       string `json:"p2p_id"`
		Password  string `json:"password"`
		PublicKey string `json:"p2p_dev_public_key"`
		Sign      string `json:"signForAppData"`
	}
	if err = json.Unmarshal(res, &v); err != nil {
		return "", err
	}

	query.Set("uid", v.UID)

	if v.Sign != "" {
		query.Set("client_public", hex.EncodeToString(clientPublic))
		query.Set("client_private", hex.EncodeToString(clientPrivate))
		query.Set("device_public", v.PublicKey)
		query.Set("sign", v.Sign)
	} else {
		query.Set("password", v.Password)
	}

	url.RawQuery = query.Encode()
	return url.String(), nil
}

func getMissURL(url *url.URL) (string, error) {
	query := url.Query()
	region, _ := url.User.Password()

	// Check credential cache first. The second channel of a dual-channel
	// camera can reuse the same credentials without a cloud API call.
	cacheKey := url.User.Username() + ":" + region + ":" + query.Get("did")
	missCredsMu.Lock()
	if cred, ok := missCreds[cacheKey]; ok {
		missCredsMu.Unlock()
		query.Set("client_public", cred.clientPublic)
		query.Set("client_private", cred.clientPrivate)
		query.Set("device_public", cred.devicePublic)
		query.Set("sign", cred.sign)
		query.Set("vendor", cred.vendor)
		if cred.uid != "" {
			query.Set("uid", cred.uid)
		}
		url.RawQuery = query.Encode()
		return url.String(), nil
	}
	missCredsMu.Unlock()

	clientPublic, clientPrivate, err := crypto.GenerateKey()
	if err != nil {
		return "", err
	}

	params := fmt.Sprintf(
		`{"app_pubkey":"%x","did":"%s","support_vendors":"TUTK_CS2_MTP"}`,
		clientPublic, query.Get("did"),
	)

	res, err := cloudUserRequest(url.User, "/v2/device/miss_get_vendor", params)
	if err != nil {
		if strings.Contains(err.Error(), "no available vendor support") {
			return getLegacyURL(url)
		}
		return "", err
	}

	var v struct {
		Vendor struct {
			ID     byte `json:"vendor"`
			Params struct {
				UID string `json:"p2p_id"`
			} `json:"vendor_params"`
		} `json:"vendor"`
		PublicKey string `json:"public_key"`
		Sign      string `json:"sign"`
	}
	if err = json.Unmarshal(res, &v); err != nil {
		return "", err
	}

	cpub := hex.EncodeToString(clientPublic)
	cpriv := hex.EncodeToString(clientPrivate)
	vendor := getVendorName(v.Vendor.ID)

	query.Set("client_public", cpub)
	query.Set("client_private", cpriv)
	query.Set("device_public", v.PublicKey)
	query.Set("sign", v.Sign)
	query.Set("vendor", vendor)

	var uid string
	if v.Vendor.ID == 1 {
		uid = v.Vendor.Params.UID
		query.Set("uid", uid)
	}

	// Cache the credentials for dual-channel reuse.
	missCredsMu.Lock()
	missCreds[cacheKey] = missCred{
		clientPublic:  cpub,
		clientPrivate: cpriv,
		devicePublic:  v.PublicKey,
		sign:          v.Sign,
		vendor:        vendor,
		uid:           uid,
	}
	missCredsMu.Unlock()

	url.RawQuery = query.Encode()
	return url.String(), nil
}

func getVendorName(i byte) string {
	switch i {
	case 1:
		return "tutk"
	case 3:
		return "agora"
	case 4:
		return "cs2"
	case 6:
		return "mtp"
	}
	return fmt.Sprintf("%d", i)
}

func wakeUpCamera(url *url.URL) error {
	const params = `{"id":1,"method":"wakeup","params":{"video":"1"}}`
	did := url.Query().Get("did")
	_, err := cloudUserRequest(url.User, "/home/rpc/"+did, params)
	return err
}

func apiXiaomi(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		apiDeviceList(w, r)
	case "POST":
		apiAuth(w, r)
	}
}

func apiDeviceList(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	user := query.Get("id")
	if user == "" {
		cloudsMu.Lock()
		users := make([]string, 0, len(tokens))
		for s := range tokens {
			users = append(users, s)
		}
		cloudsMu.Unlock()

		api.ResponseJSON(w, users)
		return
	}

	err := func() error {
		region := query.Get("region")
		res, err := cloudRequest(user, region, "/v2/home/device_list_page", "{}")
		if err != nil {
			return err
		}
		var v struct {
			List []*Device `json:"list"`
		}

		log.Trace().Str("user", user).Msgf("[xiaomi] devices list: %s", res)

		if err = json.Unmarshal(res, &v); err != nil {
			return err
		}

		var items []*api.Source

		for _, device := range v.List {
			if !device.HasCamera() {
				continue
			}
			items = append(items, &api.Source{
				Name: device.Name,
				Info: fmt.Sprintf("ip: %s, mac: %s", device.IP, device.MAC),
				URL:  fmt.Sprintf("xiaomi://%s:%s@%s?did=%s&model=%s", user, region, device.IP, device.Did, device.Model),
			})
		}

		api.ResponseSources(w, items)
		return nil
	}()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type Device struct {
	Did   string `json:"did"`
	Name  string `json:"name"`
	Model string `json:"model"`
	MAC   string `json:"mac"`
	IP    string `json:"localip"`
}

func (d *Device) HasCamera() bool {
	return strings.Contains(d.Model, ".camera.") ||
		strings.Contains(d.Model, ".cateye.") ||
		strings.Contains(d.Model, ".feeder.")
}

var auth *xiaomi.Cloud

func apiAuth(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	username := r.Form.Get("username")
	password := r.Form.Get("password")
	captcha := r.Form.Get("captcha")
	verify := r.Form.Get("verify")
	qr := r.Form.Get("qr")

	var err error
	var qrPending bool
	var qrInfo *xiaomi.QRLogin

	switch {
	case qr == "start":
		auth = xiaomi.NewCloud(AppXiaomiHome)
		qrInfo, err = auth.QRStart()
	case qr == "poll":
		if auth == nil {
			http.Error(w, "qr login not started", http.StatusBadRequest)
			return
		}
		qrPending, err = auth.QRPoll()
	case username != "" || password != "":
		auth = xiaomi.NewCloud(AppXiaomiHome)
		err = auth.Login(username, password)
	case captcha != "":
		err = auth.LoginWithCaptcha(captcha)
	case verify != "":
		err = auth.LoginWithVerify(verify)
	default:
		http.Error(w, "wrong request", http.StatusBadRequest)
		return
	}

	if err != nil {
		var login *xiaomi.LoginError
		if errors.As(err, &login) {
			w.Header().Set("Content-Type", api.MimeJSON)
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(err)
			return
		}

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", api.MimeJSON)

	if qrInfo != nil {
		_ = json.NewEncoder(w).Encode(qrInfo)
		return
	}

	if qrPending {
		_ = json.NewEncoder(w).Encode(map[string]bool{"pending": true})
		return
	}

	// successful login (password flow or finished qr poll)
	userID, token := auth.UserToken()
	auth = nil

	cloudsMu.Lock()
	if tokens == nil {
		tokens = map[string]string{userID: token}
	} else {
		tokens[userID] = token
	}
	cloudsMu.Unlock()

	// Store the token encrypted; the plaintext never touches the config file.
	stored, err := encryptToken(token)
	if err != nil {
		log.Warn().Err(err).Msg("[xiaomi] token encryption failed, storing plaintext")
		stored = token
	}

	if err = app.PatchConfig([]string{"xiaomi", userID}, stored); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

const AppXiaomiHome = "xiaomiio"

func GetBaseURL(region string) string {
	switch region {
	case "de", "i2", "ru", "sg", "us":
		return "https://" + region + ".api.io.mi.com/app"
	}
	return "https://api.io.mi.com/app"
}
