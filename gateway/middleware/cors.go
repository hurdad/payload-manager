package middleware

import (
	"io/fs"
	"net/http"
	"strings"
)

func WithCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,X-Request-Id")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// SPAHandler serves a Single-Page Application from fsys, falling back to
// indexPath for any path that doesn't match an existing file.
func SPAHandler(fsys fs.FS, indexPath string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip leading slash for fs.FS path lookups.
		name := strings.TrimPrefix(r.URL.Path, "/")
		if name == "" {
			name = indexPath
		}

		// Fall back to index for unknown paths (SPA client-side routing).
		if _, err := fs.Stat(fsys, name); err != nil {
			name = indexPath
		}

		http.ServeFileFS(w, r, fsys, name)
	})
}
