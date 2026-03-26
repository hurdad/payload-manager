package main

import (
	"context"
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminv1 "github.com/payload-manager/payload-manager/gateway/gen/go/payload/manager/services/v1"
	"github.com/payload-manager/payload-manager/gateway/middleware"
)

//go:embed static
var staticFS embed.FS

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
	rootMux.Handle("/v1/", mux)

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
