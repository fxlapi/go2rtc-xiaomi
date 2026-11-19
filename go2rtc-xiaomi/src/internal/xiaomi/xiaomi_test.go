package xiaomi

import (
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/miss"
)

type fakeMux struct {
	closed atomic.Bool
	opens  atomic.Int32
}

func (m *fakeMux) Open(rawURL string) (core.Producer, error) {
	m.opens.Add(1)
	return &miss.Producer{}, nil
}

func (m *fakeMux) Closed() bool {
	return m.closed.Load()
}

func (m *fakeMux) Close() error {
	m.closed.Store(true)
	return nil
}

func stubDialHooks(
	t *testing.T,
	resolve func(*url.URL) (string, error),
	dial func(string) (core.Producer, error),
	openMux func(string) (missMux, error),
) {
	t.Helper()
	prevResolve, prevDial, prevOpenMux := resolveCameraURL, dialDirect, openMissMux
	resolveCameraURL, dialDirect, openMissMux = resolve, dial, openMux
	t.Cleanup(func() {
		resolveCameraURL, dialDirect, openMissMux = prevResolve, prevDial, prevOpenMux
	})
}

func cleanupCamera(t *testing.T, rawURL string) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	key := cameraKey(u)
	t.Cleanup(func() {
		camerasMu.Lock()
		delete(cameras, key)
		camerasMu.Unlock()
	})
}

func TestDialCameraSharesMuxAcrossDualChannels(t *testing.T) {
	ch0 := "xiaomi://user:cn@127.0.0.1?did=" + t.Name() + "&model=isa.camera.hlmax&channel=0"
	ch1 := "xiaomi://user:cn@127.0.0.1?did=" + t.Name() + "&model=isa.camera.hlmax&channel=2"
	cleanupCamera(t, ch0)

	var resolves, muxCreates atomic.Int32
	var mux *fakeMux

	stubDialHooks(t, func(u *url.URL) (string, error) {
		resolves.Add(1)
		q := u.Query()
		q.Set("vendor", "cs2")
		u.RawQuery = q.Encode()
		return u.String(), nil
	}, nil, func(string) (missMux, error) {
		muxCreates.Add(1)
		mux = &fakeMux{}
		return mux, nil
	})

	if _, err := dialCamera(ch0); err != nil {
		t.Fatal(err)
	}
	if _, err := dialCamera(ch1); err != nil {
		t.Fatal(err)
	}
	if resolves.Load() != 1 || muxCreates.Load() != 1 || mux.opens.Load() != 2 {
		t.Fatalf("resolves=%d muxCreates=%d opens=%d", resolves.Load(), muxCreates.Load(), mux.opens.Load())
	}

	// Simulate disconnect / reboot; next dial must fetch fresh cloud credentials.
	mux.closed.Store(true)
	if _, err := dialCamera(ch0); err != nil {
		t.Fatal(err)
	}
	if resolves.Load() != 2 || muxCreates.Load() != 2 {
		t.Fatalf("expected fresh resolve after mux close: resolves=%d muxCreates=%d", resolves.Load(), muxCreates.Load())
	}
}

func TestDialCameraConcurrentLensesShareSingleMux(t *testing.T) {
	base := "xiaomi://user:cn@127.0.0.1?did=" + t.Name() + "&model=isa.camera.hlmax"
	cleanupCamera(t, base)

	var resolves, muxCreates, active atomic.Int32
	stubDialHooks(t, func(u *url.URL) (string, error) {
		if active.Add(1) != 1 {
			t.Error("concurrent cloud resolve for same camera")
		}
		defer active.Add(-1)
		resolves.Add(1)
		q := u.Query()
		q.Set("vendor", "cs2")
		u.RawQuery = q.Encode()
		return u.String(), nil
	}, nil, func(string) (missMux, error) {
		muxCreates.Add(1)
		return &fakeMux{}, nil
	})

	const count = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		rawURL := fmt.Sprintf("%s&channel=%d", base, i%2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := dialCamera(rawURL)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if resolves.Load() != 1 || muxCreates.Load() != 1 {
		t.Fatalf("resolves=%d muxCreates=%d", resolves.Load(), muxCreates.Load())
	}
}

func TestDialCameraExplicitAndLegacyBypassCloudOrMux(t *testing.T) {
	explicit := "xiaomi://127.0.0.1?did=" + t.Name() + "&client_public=explicit&vendor=cs2"
	cleanupCamera(t, explicit)

	var muxCreates atomic.Int32
	stubDialHooks(t, func(*url.URL) (string, error) {
		t.Fatal("explicit credentials must not call cloud")
		return "", nil
	}, nil, func(string) (missMux, error) {
		muxCreates.Add(1)
		return &fakeMux{}, nil
	})
	if _, err := dialCamera(explicit); err != nil || muxCreates.Load() != 1 {
		t.Fatalf("explicit err=%v muxCreates=%d", err, muxCreates.Load())
	}

	legacyURL := "xiaomi://user:cn@127.0.0.1?did=legacy&model=chuangmi.camera.v2"
	var legacyDials atomic.Int32
	stubDialHooks(t, func(u *url.URL) (string, error) {
		return u.String(), nil
	}, func(string) (core.Producer, error) {
		legacyDials.Add(1)
		return &miss.Producer{}, nil
	}, func(string) (missMux, error) {
		t.Fatal("legacy model must not use shared mux")
		return nil, nil
	})
	if _, err := dialCamera(legacyURL); err != nil || legacyDials.Load() != 1 {
		t.Fatalf("legacy err=%v dials=%d", err, legacyDials.Load())
	}
}

func TestDialCameraDafangAndXiaofangUseIndependentClients(t *testing.T) {
	var dials atomic.Int32
	stubDialHooks(t, func(u *url.URL) (string, error) {
		return u.String(), nil
	}, func(string) (core.Producer, error) {
		dials.Add(1)
		return &miss.Producer{}, nil
	}, func(string) (missMux, error) {
		t.Fatal("Dafang/Xiaofang must not enter the shared camera mux")
		return nil, nil
	})
	for _, model := range []string{miss.ModelDafang, miss.ModelXiaofang} {
		for i := 0; i < 2; i++ {
			rawURL := "xiaomi://user:cn@127.0.0.1?model=" + model + "&vendor=cs2"
			if _, err := dialCamera(rawURL); err != nil {
				t.Fatal(err)
			}
		}
	}
	if dials.Load() != 4 {
		t.Fatalf("got %d direct dials, want 4", dials.Load())
	}
}
