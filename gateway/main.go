package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	adminv1 "github.com/payload-manager/payload-manager/gateway/gen/go/payload/manager/services/v1"
	"github.com/payload-manager/payload-manager/gateway/middleware"
)

//go:embed static
var staticFS embed.FS
var payloadDownloadPathRe = regexp.MustCompile(`^/v1/payloads/([^/]+)/download$`)

func main() {
	grpcAddr := flag.String("grpc-addr", envOr("GRPC_ADDR", "localhost:50051"), "gRPC server address")
	httpAddr := flag.String("http-addr", envOr("HTTP_ADDR", ":8080"), "HTTP listen address")
	diskRootPath := flag.String("disk-root", envOr("DISK_ROOT_PATH", "/var/lib/payload-manager/payloads"), "Payload disk storage root path")
	corsOrigins := flag.String("cors-origins", envOr("CORS_ORIGINS", ""), "Comma-separated origins allowed to make cross-origin requests; empty disables CORS, '*' allows any")
	healthcheck := flag.Bool("healthcheck", false, "Probe /healthz on -http-addr and exit; for container HEALTHCHECK")
	grpcCA := flag.String("grpc-ca", envOr("GRPC_CA", ""), "PEM CA bundle for the payload-manager connection; empty means plaintext")
	grpcServerName := flag.String("grpc-server-name", envOr("GRPC_SERVER_NAME", ""), "Name to verify against the payload-manager certificate, when -grpc-addr is not it")
	tlsCert := flag.String("tls-cert", envOr("TLS_CERT", ""), "PEM certificate to serve HTTPS with; empty serves plain HTTP")
	tlsKey := flag.String("tls-key", envOr("TLS_KEY", ""), "Private key for -tls-cert")
	flag.Parse()

	if (*tlsCert == "") != (*tlsKey == "") {
		log.Fatal("gateway: -tls-cert and -tls-key must be given together")
	}
	serveTLS := *tlsCert != ""

	// Container HEALTHCHECK entry point. The distroless runtime image has no
	// shell and no curl, so the binary probes itself.
	if *healthcheck {
		os.Exit(probeHealth(*httpAddr, serveTLS))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Without this, grpc-gateway's DefaultHeaderMatcher treats Authorization as
	// a permanent HTTP header and forwards it renamed to the metadata key
	// "grpcgateway-authorization". The server's AuthMetadataProcessor reads
	// "authorization", so every request arriving through the gateway would fail
	// authentication while direct gRPC clients succeeded — a difference with no
	// visible cause on either side.
	mux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(incomingHeaderMatcher))
	dialOpts, err := backendDialOptions(*grpcCA, *grpcServerName)
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}

	register := []func(context.Context, *runtime.ServeMux, string, []grpc.DialOption) error{
		adminv1.RegisterPayloadAdminServiceHandlerFromEndpoint,
		adminv1.RegisterPayloadCatalogServiceHandlerFromEndpoint,
		adminv1.RegisterPayloadDataServiceHandlerFromEndpoint,
		adminv1.RegisterPayloadStreamServiceHandlerFromEndpoint,
	}
	for _, fn := range register {
		if err := fn(ctx, mux, *grpcAddr, dialOpts); err != nil {
			log.Fatalf("failed to register gateway handler: %v", err)
		}
	}

	rootMux := http.NewServeMux()
	rootMux.Handle("/v1/", withPayloadDownload(mux, *diskRootPath))

	// Liveness only: it reports that the HTTP server is up, not that the gRPC
	// backend is reachable. Deliberate — a gateway that still serves the UI and
	// returns useful errors while payload-manager restarts should not itself be
	// restarted by the orchestrator.
	rootMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})

	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("failed to read embedded static files: %v", err)
	}
	spa := middleware.SPAHandler(staticSub, "index.html")
	rootMux.Handle("/", spa)

	origins := middleware.ParseOrigins(*corsOrigins)
	if len(origins) == 0 {
		log.Printf("CORS disabled (no -cors-origins); the embedded UI and the vite dev proxy are both same-origin and need none")
	} else {
		log.Printf("CORS enabled for %v", origins)
	}
	handler := middleware.WithUUIDPathRewrite(middleware.WithCORS(rootMux, origins))

	srv := &http.Server{Addr: *httpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	// Stop accepting, then let in-flight requests finish. Payload downloads
	// stream straight off disk and can run for minutes; ListenAndServe alone
	// cut every one of them the instant the container got SIGTERM.
	idle := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Printf("shutting down; waiting for in-flight requests")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
		close(idle)
	}()

	scheme := "http"
	if serveTLS {
		scheme = "https"
	}
	log.Printf("gateway listening on %s://%s -> %s", scheme, *httpAddr, *grpcAddr)

	listenErr := func() error {
		if serveTLS {
			return srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		}
		return srv.ListenAndServe()
	}()
	if listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
		log.Fatal(listenErr)
	}
	<-idle
}

// backendDialOptions builds the dial options for the payload-manager
// connection. No CA configured means plaintext, which is what every deployment
// predating TLS gets and why they keep working untouched.
//
// The gateway attaches no bearer token of its own. Tokens belong to the caller
// and are forwarded per request, so payload-manager stays the single place
// authorization is decided and the gateway never becomes a way to act with more
// authority than the browser behind it.
// incomingHeaderMatcher decides which HTTP headers reach the gRPC backend, and
// under what metadata key.
//
// Authorization is the reason this exists. grpc-gateway's DefaultHeaderMatcher
// treats it as a permanent HTTP header and forwards it *renamed* to
// "grpcgateway-authorization". The server's AuthMetadataProcessor reads
// "authorization", so with the default matcher every request arriving through
// the gateway fails authentication while direct gRPC clients succeed — with
// nothing on either side to suggest why.
func incomingHeaderMatcher(key string) (string, bool) {
	if strings.EqualFold(key, "authorization") {
		return "authorization", true
	}
	return runtime.DefaultHeaderMatcher(key)
}

func backendDialOptions(caFile, serverName string) ([]grpc.DialOption, error) {
	if caFile == "" {
		if serverName != "" {
			return nil, fmt.Errorf("-grpc-server-name was set without -grpc-ca; there is no certificate to verify")
		}
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}

	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read -grpc-ca %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		// AppendCertsFromPEM reports only a bool, so a DER file or a truncated
		// PEM would otherwise surface much later as a handshake failure.
		return nil, fmt.Errorf("-grpc-ca %q contains no PEM certificates", caFile)
	}

	return []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:    pool,
			ServerName: serverName, // empty => derived from the dial target
			MinVersion: tls.VersionTLS12,
		})),
	}, nil
}

// probeHealth GETs /healthz on addr and returns a process exit code. addr is
// the listen address, so a bare ":8080" has to be dialled on loopback.
func probeHealth(addr string, useTLS bool) int {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Printf("healthcheck: cannot parse -http-addr %q: %v", addr, err)
		return 1
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	// The probe talks to this same process over loopback, so certificate
	// verification would only be checking the certificate against itself — and
	// would fail for any cert that does not happen to name 127.0.0.1. What the
	// healthcheck establishes is that the server is answering, not who it is.
	client := &http.Client{Timeout: 3 * time.Second}
	scheme := "http://"
	if useTLS {
		scheme = "https://"
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // #nosec G402 -- loopback self-probe
	}
	resp, err := client.Get(scheme + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		log.Printf("healthcheck: %v", err)
		return 1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Printf("healthcheck: /healthz returned %s", resp.Status)
		return 1
	}
	return 0
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type acquireLeaseResponse struct {
	PayloadDescriptor struct {
		Disk *struct {
			Path        string `json:"path"`
			OffsetBytes string `json:"offsetBytes"`
			LengthBytes string `json:"lengthBytes"`
		} `json:"disk"`
	} `json:"payloadDescriptor"`
	LeaseID struct {
		Value string `json:"value"`
	} `json:"leaseId"`
}

func withPayloadDownload(next http.Handler, diskRoot string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		match := payloadDownloadPathRe.FindStringSubmatch(r.URL.EscapedPath())
		if len(match) != 2 || r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		// Decode the percent-encoded ID from the URL, then normalize to
		// URL-safe base64 (no padding) so internal gRPC-gateway routes work
		// even when the standard base64 ID contains '/' or '+'.
		rawID := match[1]
		if decoded, err := url.PathUnescape(rawID); err == nil {
			rawID = decoded
		}
		payloadID := toURLSafeBase64(rawID)

		// Every sub-request below runs as the original caller.
		c := callerFrom(r)

		// For RAM/GPU payloads: spill to disk first so they can be served.
		if snap, _, snapErr := resolveSnapshot(next, c, payloadID); snapErr == nil {
			t := snap.PayloadDescriptor.Tier
			if t != "" && t != "TIER_DISK" && t != "TIER_OBJECT" {
				spillCaller, spillCancel := c.withTimeout(5 * time.Minute)
				spillErr := spillPayload(next, spillCaller, payloadID)
				spillCancel()
				if spillErr != nil {
					http.Error(w, "failed to spill payload to disk: "+spillErr.Error(), http.StatusInternalServerError)
					return
				}
			}
		}

		lease, statusCode, err := acquireReadLease(next, c, payloadID)
		if err != nil {
			http.Error(w, err.Error(), statusCode)
			return
		}
		if lease.LeaseID.Value != "" {
			defer func() {
				// Detached: by the time this runs the response is written and a
				// client that disconnected mid-stream has already cancelled
				// r.Context(). Releasing on the caller's live context would skip
				// exactly the releases that matter, pinning the payload against
				// eviction until the lease TTL expires.
				if releaseErr := releaseReadLease(next, c.detached(), lease.LeaseID.Value); releaseErr != nil {
					log.Printf("release lease failed for payload %s: %v", payloadID, releaseErr)
				}
			}()
		}
		if lease.PayloadDescriptor.Disk == nil {
			http.Error(w, "download is only supported for payloads that have disk placement", http.StatusConflict)
			return
		}

		offset, err := decodeJSONUint64(lease.PayloadDescriptor.Disk.OffsetBytes)
		if err != nil {
			http.Error(w, "invalid disk offset in lease response", http.StatusInternalServerError)
			return
		}
		length, err := decodeJSONUint64(lease.PayloadDescriptor.Disk.LengthBytes)
		if err != nil {
			http.Error(w, "invalid disk length in lease response", http.StatusInternalServerError)
			return
		}
		filePath := lease.PayloadDescriptor.Disk.Path
		if !strings.HasPrefix(filePath, "/") {
			filePath = diskRoot + "/" + filePath
		}
		if err := streamPayloadBytes(w, payloadID, filePath, offset, length); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

// caller carries the originating request's identity and deadline into the
// sub-requests the download handler issues against the gateway's own mux.
//
// withPayloadDownload does not proxy — it re-enters `next` with synthesized
// requests. Those used to be built by bare httptest.NewRequest, copying no
// headers and (for the snapshot) not even the context. That was invisible while
// the API was unauthenticated; the moment a credential is required, all four
// sub-requests arrive without one and every download fails.
//
// The fix has to be propagation, never an exemption for internally-generated
// requests: the client controls the URL that reaches this handler, so a
// "skip auth for internal calls" rule would make the download endpoint a
// general-purpose way to call the API unauthenticated.
type caller struct {
	ctx    context.Context
	header http.Header
}

// Headers copied onto sub-requests. Deliberately a short allow list rather than
// the whole header set: copying Content-Length or Content-Type from a GET onto
// a synthesized POST body produces requests that describe themselves wrongly.
var forwardedSubRequestHeaders = []string{"Authorization", "X-Request-Id"}

func callerFrom(r *http.Request) caller {
	return caller{ctx: r.Context(), header: r.Header}
}

// withTimeout returns a caller whose context expires after d, and the cancel
// func the call site must defer.
func (c caller) withTimeout(d time.Duration) (caller, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.ctx, d)
	return caller{ctx: ctx, header: c.header}, cancel
}

// detached returns a caller that keeps the credentials but drops the deadline.
// Releasing a lease has to run even when the client has gone away, which is
// exactly when the original context is already cancelled.
func (c caller) detached() caller {
	return caller{ctx: context.WithoutCancel(c.ctx), header: c.header}
}

func (c caller) newRequest(method, path string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, path, body).WithContext(c.ctx)
	for _, h := range forwardedSubRequestHeaders {
		if v := c.header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	return req
}

type resolveSnapshotResponse struct {
	PayloadDescriptor struct {
		Tier string `json:"tier"`
	} `json:"payloadDescriptor"`
}

// toURLSafeBase64 converts standard base64 to URL-safe base64 (RFC 4648 §5), keeping
// padding. grpc-gateway decodes bytes path params with base64.URLEncoding which requires
// '-'/'_' alphabet and '=' padding. The '/' → '_' swap avoids splitting the URL path.
func toURLSafeBase64(s string) string {
	return strings.NewReplacer("+", "-", "/", "_").Replace(s)
}

func resolveSnapshot(next http.Handler, c caller, payloadID string) (*resolveSnapshotResponse, int, error) {
	rr := httptest.NewRecorder()
	req := c.newRequest(http.MethodGet, "/v1/payloads/"+payloadID+"/snapshot", nil)
	next.ServeHTTP(rr, req)
	if rr.Code >= 400 {
		return nil, rr.Code, parseGatewayError(rr.Body.Bytes())
	}
	var resp resolveSnapshotResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &resp, http.StatusOK, nil
}

func spillPayload(next http.Handler, c caller, payloadID string) error {
	reqBody := map[string]any{
		"ids":           []map[string]any{{"value": payloadID}},
		"policy":        "SPILL_POLICY_BLOCKING",
		"waitForLeases": false,
		"fsync":         true,
	}
	body, _ := json.Marshal(reqBody)
	rr := httptest.NewRecorder()
	req := c.newRequest(http.MethodPost, "/v1/payloads/spill", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	next.ServeHTTP(rr, req)
	if rr.Code >= 400 {
		return parseGatewayError(rr.Body.Bytes())
	}
	return nil
}

func acquireReadLease(next http.Handler, c caller, payloadID string) (*acquireLeaseResponse, int, error) {
	reqBody := map[string]any{
		"mode":               "LEASE_MODE_READ",
		"minTier":            "TIER_DISK",
		"promotionPolicy":    "PROMOTION_POLICY_BLOCKING",
		"minLeaseDurationMs": "30000",
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	rr := httptest.NewRecorder()
	req := c.newRequest(http.MethodPost, "/v1/payloads/"+payloadID+"/lease", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	next.ServeHTTP(rr, req)
	if rr.Code >= 400 {
		return nil, rr.Code, parseGatewayError(rr.Body.Bytes())
	}

	var resp acquireLeaseResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &resp, http.StatusOK, nil
}

func releaseReadLease(next http.Handler, c caller, leaseID string) error {
	rr := httptest.NewRecorder()
	req := c.newRequest(http.MethodDelete, "/v1/leases/"+leaseID, nil)
	next.ServeHTTP(rr, req)
	if rr.Code >= 400 {
		return parseGatewayError(rr.Body.Bytes())
	}
	return nil
}

func parseGatewayError(body []byte) error {
	if len(body) == 0 {
		return errors.New("gateway request failed")
	}
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Message != "" {
		return errors.New(payload.Message)
	}
	return fmt.Errorf("gateway request failed: %s", strings.TrimSpace(string(body)))
}

func decodeJSONUint64(raw string) (uint64, error) {
	n, err := strconv.ParseUint(raw, 10, 64)
	if err == nil {
		return n, nil
	}
	var quoted string
	if err := json.Unmarshal([]byte(raw), &quoted); err != nil {
		return 0, err
	}
	return strconv.ParseUint(quoted, 10, 64)
}

func streamPayloadBytes(w http.ResponseWriter, payloadID, path string, offset, length uint64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	filename := "payload-" + safePayloadIDForFilename(payloadID) + ".bin"
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	w.Header().Set("Content-Length", strconv.FormatUint(length, 10))
	_, err = io.CopyN(w, f, int64(length))
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func safePayloadIDForFilename(encodedID string) string {
	idBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(encodedID, "="))
	if err == nil && len(idBytes) == 16 {
		src := fmt.Sprintf("%x", idBytes)
		return src[0:8] + "-" + src[8:12] + "-" + src[12:16] + "-" + src[16:20] + "-" + src[20:32]
	}
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_':
			return r
		default:
			return '-'
		}
	}, encodedID)
	return strings.Trim(cleaned, "-")
}
