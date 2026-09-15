package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// withPayloadDownload is the one handler that does not proxy: it re-enters the
// gateway's own mux with four synthesized sub-requests (snapshot, spill, lease,
// release). Those requests used to be built from scratch, carrying none of the
// caller's headers, which is invisible while the API is unauthenticated and
// breaks every download the moment it is not.
//
// These tests pin the propagation. The alternative fix — exempting
// internally-generated requests from auth — would turn this endpoint into a way
// to call any API method unauthenticated, since the client controls the URL that
// reaches it.

// recordingMux stands in for the gRPC-gateway mux, answering the four
// sub-requests plausibly while recording the headers each one arrived with.
type recordingMux struct {
	mu       sync.Mutex
	seen     map[string]http.Header
	tier     string
	diskPath string
	length   int
}

func (m *recordingMux) record(path string, h http.Header) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = map[string]http.Header{}
	}
	m.seen[path] = h.Clone()
}

func (m *recordingMux) paths() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.seen))
	for p := range m.seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func (m *recordingMux) saw(path string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.seen[path]
	return ok
}

func (m *recordingMux) authFor(path string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.seen[path].Get("Authorization")
}

func (m *recordingMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Method + " " + r.URL.Path
	m.record(key, r.Header)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.HasSuffix(r.URL.Path, "/snapshot"):
		_ = json.NewEncoder(w).Encode(map[string]any{
			"payloadDescriptor": map[string]any{"tier": m.tier},
		})
	case r.URL.Path == "/v1/payloads/spill":
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}})
	case strings.HasSuffix(r.URL.Path, "/lease"):
		_ = json.NewEncoder(w).Encode(map[string]any{
			"leaseId": map[string]any{"value": "lease-abc"},
			"payloadDescriptor": map[string]any{
				"disk": map[string]any{
					"path":        m.diskPath,
					"offsetBytes": "0",
					"lengthBytes": strconv.Itoa(m.length),
				},
			},
		})
	case strings.HasPrefix(r.URL.Path, "/v1/leases/"):
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// downloadOnce drives one full download through withPayloadDownload and returns
// the recorder plus the mux that observed the sub-requests.
func downloadOnce(t *testing.T, tier, authHeader string) (*httptest.ResponseRecorder, *recordingMux) {
	t.Helper()

	dir := t.TempDir()
	diskPath := filepath.Join(dir, "payload.bin")
	content := []byte("payload-bytes")
	if err := os.WriteFile(diskPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	m := &recordingMux{tier: tier, diskPath: diskPath, length: len(content)}
	h := withPayloadDownload(m, dir)

	req := httptest.NewRequest(http.MethodGet, "/v1/payloads/ABEiM0RVZneImaq7zN3u_w/download", nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
		req.Header.Set("X-Request-Id", "req-42")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, m
}

func TestDownloadPropagatesCredentialsToEverySubRequest(t *testing.T) {
	// TIER_RAM forces the spill branch, so all four sub-requests run.
	rec, m := downloadOnce(t, "TIER_RAM", "Bearer token-xyz")

	if rec.Code != http.StatusOK {
		t.Fatalf("download failed: %d %q", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "payload-bytes" {
		t.Errorf("body = %q", got)
	}

	want := []string{
		"DELETE /v1/leases/lease-abc",
		"GET /v1/payloads/ABEiM0RVZneImaq7zN3u_w/snapshot",
		"POST /v1/payloads/ABEiM0RVZneImaq7zN3u_w/lease",
		"POST /v1/payloads/spill",
	}
	got := m.paths()
	if len(got) != len(want) {
		t.Fatalf("sub-requests = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sub-requests = %v, want %v", got, want)
		}
	}

	// The assertion that matters: every one of them carried the credential.
	for _, p := range want {
		if auth := m.authFor(p); auth != "Bearer token-xyz" {
			t.Errorf("sub-request %q carried Authorization %q, want the caller's token", p, auth)
		}
	}
}

func TestDownloadPropagatesRequestID(t *testing.T) {
	_, m := downloadOnce(t, "TIER_RAM", "Bearer token-xyz")
	m.mu.Lock()
	defer m.mu.Unlock()
	for p, h := range m.seen {
		if got := h.Get("X-Request-Id"); got != "req-42" {
			t.Errorf("sub-request %q carried X-Request-Id %q, want req-42", p, got)
		}
	}
}

func TestDownloadWithoutCredentialsSendsNone(t *testing.T) {
	// No Authorization in, none forged on the way out — the handler must not
	// invent a credential of its own.
	_, m := downloadOnce(t, "TIER_DISK", "")
	m.mu.Lock()
	defer m.mu.Unlock()
	for p, h := range m.seen {
		if got := h.Get("Authorization"); got != "" {
			t.Errorf("sub-request %q carried Authorization %q, want none", p, got)
		}
	}
}

func TestDownloadSkipsSpillForDiskTier(t *testing.T) {
	// A payload already on disk needs no spill; this pins that the credential
	// plumbing did not change the tier logic.
	rec, m := downloadOnce(t, "TIER_DISK", "Bearer t")
	if rec.Code != http.StatusOK {
		t.Fatalf("download failed: %d %q", rec.Code, rec.Body.String())
	}
	for _, p := range m.paths() {
		if p == "POST /v1/payloads/spill" {
			t.Error("spilled a payload that was already on disk")
		}
	}
}

func TestNonDownloadPathsPassStraightThrough(t *testing.T) {
	m := &recordingMux{}
	h := withPayloadDownload(m, t.TempDir())

	// Wrong method on the download path, and an unrelated path.
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/v1/payloads/ABEiM0RVZneImaq7zN3u_w/download"},
		{http.MethodGet, "/v1/payloads"},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if !m.saw(tc.method + " " + tc.path) {
			t.Errorf("%s %s did not reach the wrapped handler", tc.method, tc.path)
		}
	}
}
