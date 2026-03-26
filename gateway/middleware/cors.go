package middleware

import "net/http"

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

func SPAHandler(static http.FileSystem, indexPath string) http.Handler {
	files := http.FileServer(static)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "" {
			r.URL.Path = "/" + indexPath
		}
		_, err := static.Open(r.URL.Path)
		if err != nil {
			r.URL.Path = "/" + indexPath
		}
		files.ServeHTTP(w, r)
	})
}
