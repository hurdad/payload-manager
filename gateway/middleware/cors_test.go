package middleware

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// The gateway had no Go tests at all, and `go vet` in CI only ever ran over
// ./gen/... — the generated stubs — so nothing checked this file. CORS is the
// piece where a mistake is least visible locally and most consequential in
// deployment, since the service behind it has no authentication of any kind.

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})
}

func TestParseOrigins(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{",,", nil},
		{"https://a.example", []string{"https://a.example"}},
		{" https://a.example , https://b.example ", []string{"https://a.example", "https://b.example"}},
		{"*", []string{"*"}},
	}
	for _, c := range cases {
		if got := ParseOrigins(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("ParseOrigins(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestCORSDisabledAddsNoHeaders(t *testing.T) {
	// The default. Both supported ways of running the UI are same-origin, so a
	// deployment that configures nothing must not advertise cross-origin access.
	h := WithCORS(okHandler(), nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/payloads", nil)
	req.Header.Set("Origin", "https://evil.example")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty", got)
	}
	if rec.Code != http.StatusOK || rec.Body.String() != "body" {
		t.Errorf("request did not reach the handler: %d %q", rec.Code, rec.Body.String())
	}
}

func TestCORSAllowsOnlyListedOrigins(t *testing.T) {
	h := WithCORS(okHandler(), []string{"https://ui.example"})

	for _, tc := range []struct {
		origin string
		want   string
	}{
		{"https://ui.example", "https://ui.example"},
		{"https://evil.example", ""},
		// Prefix and suffix confusions are the classic allow-list bugs.
		{"https://ui.example.evil.com", ""},
		{"https://evil.com?https://ui.example", ""},
		{"http://ui.example", ""}, // scheme is part of an origin
	} {
		req := httptest.NewRequest(http.MethodGet, "/v1/payloads", nil)
		req.Header.Set("Origin", tc.origin)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != tc.want {
			t.Errorf("origin %q: allow-origin = %q, want %q", tc.origin, got, tc.want)
		}
	}
}

func TestCORSAllowedResponseVariesOnOrigin(t *testing.T) {
	// Without Vary, a shared cache can hand one origin's allowed response to
	// another origin.
	h := WithCORS(okHandler(), []string{"https://ui.example"})
	req := httptest.NewRequest(http.MethodGet, "/v1/payloads", nil)
	req.Header.Set("Origin", "https://ui.example")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want Origin", got)
	}
}

func TestCORSWildcardEchoesTheCallerOrigin(t *testing.T) {
	// "*" stays available for operators who ask for it, but the response echoes
	// the caller so it remains usable if credentials are ever added.
	h := WithCORS(okHandler(), []string{"*"})
	req := httptest.NewRequest(http.MethodGet, "/v1/payloads", nil)
	req.Header.Set("Origin", "https://anything.example")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://anything.example" {
		t.Errorf("allow-origin = %q, want the caller's origin", got)
	}
}

func TestCORSPreflightIsAnsweredWithoutReachingTheHandler(t *testing.T) {
	reached := false
	h := WithCORS(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }), []string{"https://ui.example"})

	req := httptest.NewRequest(http.MethodOptions, "/v1/payloads", nil)
	req.Header.Set("Origin", "https://ui.example")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("preflight status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if reached {
		t.Error("preflight reached the wrapped handler")
	}
}

func TestCORSRequestWithoutOriginIsUntouched(t *testing.T) {
	// Same-origin requests and non-browser clients send no Origin at all.
	h := WithCORS(okHandler(), []string{"https://ui.example"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/payloads", nil))

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("allow-origin = %q, want empty", got)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}
