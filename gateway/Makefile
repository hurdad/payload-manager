.PHONY: generate build run

generate:
	buf generate

build:
	mkdir -p static
	cd ../ui && npm run build
	rm -rf static/*
	cp -R ../ui/dist/* static/
	mkdir -p bin
	go build -o bin/gateway ./...

run:
	go run . --grpc-addr localhost:50051 --http-addr :8080
