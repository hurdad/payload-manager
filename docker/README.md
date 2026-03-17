# Docker Assets

This directory contains all Docker build and Docker Compose assets for Payload Manager.

## Dockerfiles

- `Dockerfile` — default payload-manager image (no OpenTelemetry, no GPU).
- `Dockerfile.otel` — payload-manager image with OpenTelemetry support.
- `Dockerfile.cuda` — payload-manager image with GPU + OpenTelemetry support.
- `Dockerfile.payloadctl` — `payloadctl` CLI image.
- `Dockerfile.test` — integration test image used by Compose overlays.
- `Dockerfile.examples.cpp` — C++ examples image.
- `Dockerfile.examples.python` — Python examples image.
- `Dockerfile.examples.cuda` — CUDA C++ examples image.
- `Dockerfile.examples.python.cuda` — CUDA Python examples image.

Build examples from repository root:

```bash
docker build -f docker/Dockerfile -t payload-manager:latest .
docker build -f docker/Dockerfile.otel -t payload-manager:otel .
docker build -f docker/Dockerfile.cuda -t payload-manager:cuda .
docker build -f docker/Dockerfile.payloadctl -t payloadctl:latest .
```

## Compose files

Base stacks:

- `docker-compose.sqlite.yml`
- `docker-compose.postgres.yml`
- `docker-compose.otel.sqlite.yml`
- `docker-compose.otel.postgres.yml`
- `docker-compose.gpu.sqlite.yml`
- `docker-compose.gpu.postgres.yml`

Overlays:

- `docker-compose.observability.yml`
- `docker-compose.examples.yml`
- `docker-compose.examples.python.yml`
- `docker-compose.examples.cuda.yml`
- `docker-compose.examples.python.cuda.yml`
- `docker-compose.test.yml`
- `docker-compose.stress.yml`

Run examples from repository root:

```bash
docker compose -f docker/docker-compose.sqlite.yml up --build
docker compose -f docker/docker-compose.otel.sqlite.yml -f docker/docker-compose.observability.yml up --build
```

> Note: Compose files set `build.context: ..` and `dockerfile: docker/...` so they can be executed via `-f docker/<file>.yml` while still building from the repository root context.
