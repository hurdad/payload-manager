package middleware

import (
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"
)

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func WithUUIDPathRewrite(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		for i, part := range parts {
			if uuidRe.MatchString(part) {
				rawHex := strings.ReplaceAll(part, "-", "")
				buf, err := hex.DecodeString(rawHex)
				if err == nil && len(buf) == 16 {
					parts[i] = base64.RawURLEncoding.EncodeToString(buf)
				}
			}
		}
		r.URL.Path = strings.Join(parts, "/")
		next.ServeHTTP(w, r)
	})
}
