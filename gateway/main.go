package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
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
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := runtime.NewServeMux()
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

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
	rootMux.Handle("/v1/", withPayloadDownload(mux))

	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("failed to read embedded static files: %v", err)
	}
	spa := middleware.SPAHandler(http.FS(staticSub), "index.html")
	rootMux.Handle("/", spa)

	handler := middleware.WithUUIDPathRewrite(middleware.WithCORS(rootMux))

	srv := &http.Server{Addr: *httpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("gateway listening on %s -> %s", *httpAddr, *grpcAddr)
	log.Fatal(srv.ListenAndServe())
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

func withPayloadDownload(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		match := payloadDownloadPathRe.FindStringSubmatch(r.URL.EscapedPath())
		if len(match) != 2 || r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		payloadID := match[1]
		if decoded, err := url.PathUnescape(payloadID); err == nil {
			payloadID = decoded
		}

		lease, statusCode, err := acquireReadLease(next, payloadID)
		if err != nil {
			http.Error(w, err.Error(), statusCode)
			return
		}
		if lease.LeaseID.Value != "" {
			defer func() {
				if releaseErr := releaseReadLease(next, lease.LeaseID.Value); releaseErr != nil {
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
		if err := streamPayloadBytes(w, payloadID, lease.PayloadDescriptor.Disk.Path, offset, length); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func acquireReadLease(next http.Handler, payloadID string) (*acquireLeaseResponse, int, error) {
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
	req := httptest.NewRequest(http.MethodPost, "/v1/payloads/"+payloadID+"/lease", bytes.NewReader(body))
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

func releaseReadLease(next http.Handler, leaseID string) error {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/leases/"+leaseID, nil)
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
