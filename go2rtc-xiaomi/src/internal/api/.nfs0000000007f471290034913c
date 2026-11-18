package api

import (
	"net/http"
	"os"
)

// initPanel serves an optional custom web panel from a directory.
// The directory may be set with `api: panel_dir:`, otherwise well-known
// locations are probed. When found, files are served under /panel/.
// The embedded web UI at / stays untouched.
func initPanel(panelDir string) {
	if panelDir == "" {
		for _, dir := range []string{"/app/panel", "panel"} {
			if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
				panelDir = dir
				break
			}
		}
	}
	if panelDir == "" {
		return
	}
	fi, err := os.Stat(panelDir)
	if err != nil || !fi.IsDir() {
		return
	}

	log.Info().Str("dir", panelDir).Msg("[api] serve panel")

	fileServer := http.FileServer(http.Dir(panelDir))

	HandleFunc("panel", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, basePath+"/panel/", http.StatusFound)
	})
	HandleFunc("panel/", func(w http.ResponseWriter, r *http.Request) {
		// 面板更新后浏览器应及时拿到新版本，仅允许协商缓存
		w.Header().Set("Cache-Control", "no-cache")
		http.StripPrefix(basePath+"/panel", fileServer).ServeHTTP(w, r)
	})
}
