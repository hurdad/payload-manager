# Payload Manager

[![CI](https://github.com/hurdad/payload-manager/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/hurdad/payload-manager/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/hurdad/payload-manager/branch/main/graph/badge.svg)](https://codecov.io/gh/hurdad/payload-manager)

Payload Manager is a high-performance control plane for managing opaque binary payloads across multiple storage tiers (GPU, RAM, disk, object storage, or void) without routing payload bytes through the service itself.

The platform is designed around a strict control-plane/data-plane split:

- **Control plane:** metadata, placement, leases, lineage, lifecycle, and APIs.
- **Data plane:** direct producer/consumer access to memory regions, files, or object references.

## Why Payload Manager

Modern pipelines often spend more time moving bytes through orchestration services than doing useful work. Payload Manager avoids this by returning descriptors and leases so clients can access data directly from the selected tier.

## Core capabilities

- Tier-aware placement across GPU, RAM, disk, object storage, and void (discard-on-eviction).
- Lease-based read stability for payload access.
- Lifecycle orchestration (`allocate -> commit -> active -> expire/delete`).
- Metadata and lineage tracking.
- Multiple repository backends (memory, PostgreSQL).
- gRPC service interfaces for admin, data, catalog, and stream workflows.

## Architecture at a glance

```text
Browser / HTTP clients
   |
   v
gRPC-Gateway  (REST → gRPC, embedded Svelte UI, OpenAPI docs)
   |
   v
gRPC Servers (admin/data/catalog/stream)
   |
   v
Service Layer (lifecycle, placement, leasing, metadata, streams)
   |
   v
Repository API (transaction + persistence abstraction)
   |
   +--> Memory
   +--> PostgreSQL

Placement + Tiering + Spill
   |
   +--> GPU (CUDA IPC)
   +--> RAM (shared memory)
   +--> Disk
   +--> Object storage
   +--> Void (delete on eviction)
```

For detailed documentation, see:

- [Architecture Overview](docs/ARCHITECTURE.md)
- [Design Details](docs/DESIGN.md)
- [Testing Strategy](docs/TESTING_STRATEGY.md)
- [Metrics Reference](docs/METRICS.md)

## Build

```bash
mkdir -p build
cd build
cmake ..
cmake --build .
```

Optional CMake flags:

- `-DPAYLOAD_MANAGER_ENABLE_POSTGRES=ON`
- `-DPAYLOAD_MANAGER_ENABLE_OTEL=ON` (requires `opentelemetry-cpp-dev`)
- `-DPAYLOAD_MANAGER_ENABLE_ARROW_CUDA=ON` (enables GPU tier runtime support in the service via Arrow CUDA; requires `libarrow-cuda-dev`)

Client build switch:

- C++ CUDA-capable client build: `-DPAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON`

### Dependencies

Arrow and OpenTelemetry are taken from system packages; this repository has no
git submodules. On Ubuntu 26.04:

```bash
sudo apt install libarrow-dev            # Arrow 23.0.1 from universe
sudo apt install opentelemetry-cpp-dev   # only for -DPAYLOAD_MANAGER_ENABLE_OTEL=ON
```

Ubuntu universe carries no Arrow CUDA build, and releases before 26.04 carry no
usable Arrow or OpenTelemetry at all. For those — and for `libarrow-cuda-dev` —
use the [Apache Arrow apt repository](https://arrow.apache.org/install/), which
is what the container images do (pinned to 25.0.1):

```bash
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev libarrow-cuda-dev
```

To build with OpenTelemetry enabled:

```bash
cmake -S . -B build-otel -DPAYLOAD_MANAGER_ENABLE_OTEL=ON
cmake --build build-otel
```

## Run

```bash
./payload-manager --config config.yaml
```

## Docker

All Dockerfiles and Compose manifests now live under [`docker/`](docker/README.md).

### Dockerfiles

| Dockerfile | OTEL | GPU | Use |
|---|---|---|---|
| `docker/Dockerfile` | Off | Off | Lightweight production image |
| `docker/Dockerfile.otel` | On | Off | Production image with OpenTelemetry |
| `docker/Dockerfile.cuda` | On | On | GPU-capable image with OpenTelemetry |
| `docker/Dockerfile.gateway` | — | — | gRPC-Gateway + embedded Svelte UI |

```bash
# No-OTEL image (default)
docker build -f docker/Dockerfile -t payload-manager:latest .

# OTEL-enabled image
docker build -f docker/Dockerfile.otel -t payload-manager:otel .

# GPU + OTEL image
docker build -f docker/Dockerfile.cuda -t payload-manager:cuda .

# payloadctl CLI image
docker build -f docker/Dockerfile.payloadctl -t payloadctl:latest .
```

### Docker Compose

Full compose matrix — pick a feature combination:

| Compose file | Database | OTEL | GPU | Host port |
|---|---|---|---|---|
| `docker/docker-compose.postgres.yml` | Postgres | Off | Off | 50051 |
| `docker/docker-compose.otel.postgres.yml` | Postgres | On | Off | 50055 |
| `docker/docker-compose.gpu.postgres.yml` | Postgres | On | On | 50056 |
| `docker/docker-compose.gateway.yml` | Postgres | Off | Off | 8080 (HTTP) |

```bash
# Postgres, no OTEL
docker compose -f docker/docker-compose.postgres.yml up --build

# Postgres + OTEL (add observability stack)
docker compose -f docker/docker-compose.otel.postgres.yml -f docker/docker-compose.observability.yml up --build

# GPU + Postgres + OTEL (add observability stack)
docker compose -f docker/docker-compose.gpu.postgres.yml -f docker/docker-compose.observability.yml up --build
```

The `docker/docker-compose.observability.yml` overlay adds Grafana Alloy (OTLP receiver), Prometheus, Grafana (`:3000`), and Tempo. It should only be layered on OTEL-enabled compose files.

### payloadctl

`payloadctl` supports tiering advisory commands that are useful during placement tuning and spill control:

```bash
# Best-effort hint to stage a payload in a faster tier.
payloadctl <addr> prefetch <uuid> <tier=ram|disk|gpu>

# Best-effort advisory pin. duration_ms=0 means "stay pinned until explicit unpin".
payloadctl <addr> pin <uuid> [duration_ms]

# Removes an active pin (idempotent if the payload is already unpinned).
payloadctl <addr> unpin <uuid>
```

Behavior notes:

- `prefetch` is best-effort and idempotent; it does not guarantee immediate movement.
- `pin` blocks spill while active. Use a finite `duration_ms` for bounded pinning windows.
- `unpin` is safe to call repeatedly and is a no-op when no pin exists.

## Clients (CPU vs CUDA)

C++ client build modes:

```bash
# CPU-focused (default)
cmake -S . -B build
cmake --build build

# CUDA-capable build intent (requires Arrow CUDA artifacts)
cmake -S . -B build-cuda -DPAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON
cmake --build build-cuda
```

Python client install modes:

```bash
# CPU-safe base install
pip install ./client/python

# Explicit CUDA-capable install intent
pip install './client/python[cuda]'
```

Current GPU client runtime status:

- C++ client: GPU descriptor runtime handling is implemented when built with `-DPAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON` and Arrow CUDA libraries are available.
- Python client: GPU descriptor read/write runtime handling is implemented when installed with CUDA extras and Arrow CUDA dependencies are available.
- Result: Both C++ and Python clients can use GPU descriptors at runtime in CUDA-capable environments.

## Gateway and UI

The `gateway/` directory contains a Go binary that bridges REST/HTTP to the gRPC backend and serves an embedded Svelte web UI.

### Features

- REST API via [gRPC-Gateway](https://github.com/grpc-ecosystem/grpc-gateway) — all gRPC services exposed as JSON over HTTP.
- OpenAPI spec at `gateway/openapi/apidocs.swagger.json`.
- Embedded Svelte UI served at `/` — no separate web server needed.
- Payload download endpoint (`GET /v1/payloads/{id}/download`) — automatically spills RAM/GPU payloads to disk before streaming.

### Quick start (Docker)

```bash
# Start payload-manager + gateway (Postgres, no GPU)
docker compose -f docker/docker-compose.gateway.yml up --build
```

The UI is then available at `http://localhost:8080/`.

### UI overview

| Page | API coverage |
|------|-------------|
| Payloads | List, filter by tier, download, spill, promote, pin/unpin, prefetch, delete, view snapshot/lineage/metadata |
| Streams | Create/delete streams, read entries, append entries, manage consumer group offsets |
| Admin | Per-tier stats (GPU/RAM/Disk/Object) with totals |

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_ADDR` | `localhost:50051` | gRPC backend address |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `DISK_ROOT_PATH` | `/var/lib/payload-manager/payloads` | Disk storage root (must match payload-manager config) |

### Regenerate code

```bash
# Regenerate Go stubs + OpenAPI from proto definitions
make generate
```

Requires `buf`, `protoc-gen-go`, `protoc-gen-go-grpc`, `protoc-gen-grpc-gateway`, and `protoc-gen-openapiv2` on `PATH`.

## Repository layout

- `cmd/`: executable entrypoints (`payload-manager`, `payloadctl`).
- `internal/`: core runtime, services, storage tiers, DB adapters, lease/tiering/spill logic.
- `api/`: protobuf definitions and generated interface artifacts.
- `gateway/`: gRPC-Gateway binary and Svelte UI.
- `client/`: C++ and Python client surfaces.
- `tests/`: unit and integration coverage.
- `docs/`: architecture, design, and testing documentation.

## License

Apache-2.0
