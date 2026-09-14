.PHONY: generate build run

# Delegates to scripts/codegen.sh, which pins the Go toolchain from go.mod
# along with every plugin version. `buf generate` on its own uses whatever Go
# is installed, and that changes the generated output — see the script.
generate:
	../scripts/codegen.sh go

build:
	mkdir -p static
	cd ../ui && npm run build
	rm -rf static/*
	cp -R ../ui/dist/* static/
	mkdir -p bin
	go build -o bin/ ./...

run:
	go run . --grpc-addr localhost:50051 --http-addr :8080
