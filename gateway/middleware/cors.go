package middleware

import (
	"io/fs"
	"net/http"
	"strings"
)

// ParseOrigins turns a comma-separated -cors-origins value into the allow list
// WithCORS expects. Blank entries are dropped, so "" yields nil (CORS off).
func ParseOrigins(csv string) []string {
	var out []string
	for _, o := range strings.Split(csv, ",") {
		if o = strings.TrimSpace(o); o != "" {
			out = append(out, o)
		}
	}
	return out
}

// WithCORS answers cross-origin requests from the listed origins only. An empty
// list disables CORS entirely: no headers are added and preflights are not
// answered, which is the right default here because nothing in this repository
// needs it. The UI is compiled into the gateway binary and served from the same
// origin, and `npm run dev` reaches the API through vite's /v1 proxy, so both
// supported ways of running the UI are same-origin.
//
// This used to be an unconditional "Access-Control-Allow-Origin: *" alongside
// GET,POST,PUT,DELETE. With no authentication anywhere in the service, that let
// any page in an operator's browser enumerate and delete payloads on any
// payload-manager reachable from that browser — a much wider door than "someone
// on the network can reach the port". Pass -cors-origins (or CORS_ORIGINS) to
// name the origins that genuinely need it.
//
// The literal "*" is still accepted for the case where an operator really wants
// it, but it is then a deliberate, logged choice rather than the default.
func WithCORS(next http.Handler, allowedOrigins []string) http.Handler {
	if len(allowedOrigins) == 0 {
		return next
	}

	allowAny := false
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		if o == "*" {
			allowAny = true
		}
		allowed[o] = struct{}{}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			_, ok := allowed[origin]
			if ok || allowAny {
				// Echo the caller's origin rather than "*" even in the allowAny
				// case: "*" is incompatible with credentialed requests, and
				// echoing keeps the response usable if credentials are added.
				w.Header().Set("Access-Control-Allow-Origin", origin)
				// Caches must not serve one origin's response to another.
				w.Header().Add("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type,X-Request-Id")
				w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
			}
		}

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
