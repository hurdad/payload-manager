package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The UI and payloadctl both address payloads by canonical UUID, while the
// gRPC-gateway routes expect the 16 raw bytes as URL-safe base64. This rewrite
// is the only thing bridging the two, so a change to it breaks every payload
// route at once.

func capturePath(t *testing.T, in string) string {
	t.Helper()
	var seen string
	h := WithUUIDPathRewrite(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = r.URL.Path
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, in, nil))
	return seen
}

func TestUUIDSegmentIsRewrittenToBase64(t *testing.T) {
	// 00112233-4455-6677-8899-aabbccddeeff -> the same 16 bytes, base64url.
	got := capturePath(t, "/v1/payloads/00112233-4455-6677-8899-aabbccddeeff")
	want := "/v1/payloads/ABEiM0RVZneImaq7zN3u_w"
	if got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
}

func TestUUIDRewriteIsCaseInsensitive(t *testing.T) {
	lower := capturePath(t, "/v1/payloads/00112233-4455-6677-8899-aabbccddeeff")
	upper := capturePath(t, "/v1/payloads/00112233-4455-6677-8899-AABBCCDDEEFF")
	if lower != upper {
		t.Errorf("case changed the result: %q vs %q", lower, upper)
	}
}

func TestUUIDRewriteLeavesTrailingSegmentsAlone(t *testing.T) {
	got := capturePath(t, "/v1/payloads/00112233-4455-6677-8899-aabbccddeeff/download")
	want := "/v1/payloads/ABEiM0RVZneImaq7zN3u_w/download"
	if got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
}

func TestNonUUIDSegmentsAreUntouched(t *testing.T) {
	for _, path := range []string{
		"/v1/payloads",
		"/v1/streams/ns/name",
		"/v1/payloads/not-a-uuid",
		// One hex digit short: must not be rewritten into something shorter.
		"/v1/payloads/00112233-4455-6677-8899-aabbccddeef",
	} {
		if got := capturePath(t, path); got != path {
			t.Errorf("path %q was rewritten to %q", path, got)
		}
	}
}
