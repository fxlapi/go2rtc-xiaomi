package api

import (
	"net/http"

	"github.com/AlexxIT/go2rtc/www"
)

func initStatic(staticDir string) {
	var root http.FileSystem
	if staticDir != "" {
		log.Info().Str("dir", staticDir).Msg("[api] serve static")
		root = http.Dir(staticDir)
	} else {
		root = http.FS(www.Static)
	}

	base := len(basePath)
	fileServer := http.FileServer(root)

	// 官方原版前端入口：/classic -> /classic.html
	HandleFunc("classic", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, basePath+"/classic.html", http.StatusFound)
	})

	HandleFunc("", func(w http.ResponseWriter, r *http.Request) {
		if base > 0 {
			r.URL.Path = r.URL.Path[base:]
		}
		fileServer.ServeHTTP(w, r)
	})
}
