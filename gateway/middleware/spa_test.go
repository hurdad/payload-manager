package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
)

func testFS() fstest.MapFS {
	return fstest.MapFS{
		"index.html":    {Data: []byte("<!doctype html>index")},
		"assets/app.js": {Data: []byte("console.log(1)")},
	}
}

func TestSPAServesRealFiles(t *testing.T) {
	h := SPAHandler(testFS(), "index.html")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/assets/app.js", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != "console.log(1)" {
		t.Errorf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestSPAFallsBackToIndexForClientRoutes(t *testing.T) {
	h := SPAHandler(testFS(), "index.html")
	for _, path := range []string{"/", "/payloads", "/streams/deeply/nested"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK || rec.Body.String() != "<!doctype html>index" {
			t.Errorf("%s: got %d %q, want the index", path, rec.Code, rec.Body.String())
		}
	}
}

func TestSPADoesNotEscapeTheEmbeddedFS(t *testing.T) {
	// The handler itself does nothing to stop a traversal — its safety comes
	// entirely from the layers underneath, and they disagree about which
	// mechanism catches it. A name io/fs rejects falls through to the index;
	// one http.ServeFileFS rejects gets a 400. Both are fine. What is pinned
	// here is the invariant: nothing outside the embedded FS is ever served,
	// whichever layer does the rejecting.
	h := SPAHandler(testFS(), "index.html")
	for _, path := range []string{"/../etc/passwd", "/..%2fetc%2fpasswd", "//etc/passwd", "/assets/../../etc/passwd"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

		switch rec.Code {
		case http.StatusOK:
			if body := rec.Body.String(); body != "<!doctype html>index" {
				t.Errorf("%s: served %q, want the index", path, body)
			}
		case http.StatusBadRequest, http.StatusNotFound, http.StatusMovedPermanently:
			// Rejected before reaching the filesystem.
		default:
			t.Errorf("%s: unexpected status %d (%q)", path, rec.Code, rec.Body.String())
		}
	}
}
