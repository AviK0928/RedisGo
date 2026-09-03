// Package web is the HTTP front door: the playground UI, a health check, and a
// stats endpoint. On Render this is the only thing listening, because a free
// web service exposes exactly one port.
package web

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"

	"github.com/AviK0928/RedisGo/internal/engine"
)

//go:embed static
var staticFiles embed.FS

// Handler wires up every route. The static files are compiled into the binary,
// so there is no separate frontend build or deploy.
func Handler(e *engine.Engine) http.Handler {
	static, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err) // only possible if the embed directive is wrong
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(static)))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(e.Stats()); err != nil {
			http.Error(w, "failed to encode stats", http.StatusInternalServerError)
		}
	})

	return mux
}
