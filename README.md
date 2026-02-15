# Payload Manager

Payload Manager is a high-performance control plane for managing opaque binary payloads across multiple storage tiers (GPU, RAM, disk, object storage) without routing payload bytes through the service itself.

The platform is designed around a strict control-plane/data-plane split:

- **Control plane:** metadata, placement, leases, lineage, lifecycle, and APIs.
- **Data plane:** direct producer/consumer access to memory regions, files, or object references.

## Why Payload Manager

Modern pipelines often spend more time moving bytes through orchestration services than doing useful work. Payload Manager avoids this by returning descriptors and leases so clients can access data directly from the selected tier.

## Core capabilities

- Tier-aware placement across GPU, RAM, disk, and object storage.
- Lease-based read stability for payload access.
- Lifecycle orchestration (`allocate -> commit -> active -> expire/delete`).
- Metadata and lineage tracking.
- Multiple repository backends (memory, SQLite, PostgreSQL).
- gRPC service interfaces for admin, data, catalog, and stream workflows.

## Architecture at a glance

```text
Clients (producers / workers)
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
   +--> SQLite
   +--> PostgreSQL

Placement + Tiering + Spill
   |
   +--> GPU (CUDA IPC)
   +--> RAM (shared memory)
   +--> Disk
   +--> Object storage
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

- `-DPAYLOAD_MANAGER_ENABLE_SQLITE=ON`
- `-DPAYLOAD_MANAGER_ENABLE_POSTGRES=ON`
- `-DPAYLOAD_MANAGER_ENABLE_OTEL=ON` (builds OpenTelemetry support from `third_party/opentelemetry-cpp`)

Client build switch:

- C++ CUDA-capable client build: `-DPAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON`

To build with OpenTelemetry enabled:

```bash
git submodule update --init --recursive
cmake -S . -B build-otel -DPAYLOAD_MANAGER_ENABLE_OTEL=ON
cmake --build build-otel
```

## Run

```bash
./payload-manager --config config.yaml
```

## Docker

Build matrix:

| Component | CPU-focused mode | CUDA-capable mode |
| --- | --- | --- |
| payload-manager image | `Dockerfile` | `Dockerfile.cuda` |
| C++ client | default build (`-DPAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=OFF`) | build with `-DPAYLOAD_MANAGER_CLIENT_ENABLE_CUDA=ON` |
| Python client | default install (`pip install ./client/python`) | explicit CUDA intent (`pip install './client/python[cuda]'`) |

Build the production image:

```bash
docker build -t payload-manager:latest .
```

Build the CUDA-capable production image:

```bash
docker build -f Dockerfile.cuda -t payload-manager:cuda .
```

Build the `payloadctl` image:

```bash
docker build -f Dockerfile.payloadctl -t payloadctl:latest .
```

Run the service with PostgreSQL using Docker Compose:

```bash
docker compose up --build
```

Compose uses `docker-compose.yml` and the Docker-ready config file `confng/runtime-docker-postgres.yaml`.

The compose stack also includes Grafana Alloy (`confng/alloy/config.alloy`) to receive OTLP metrics from `payload-manager` and forward them to your backend. Set `ALLOY_METRICS_BACKEND_OTLP_ENDPOINT` (and optionally `ALLOY_METRICS_BACKEND_AUTH_HEADER`) before starting compose.

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

- C++ client: CUDA-capable build option exists, but GPU buffer import/open at runtime is **not yet implemented**.
- Python client: GPU descriptor read/write path is **not yet implemented** (RAM/disk paths are implemented).
- Result: treat current client-side GPU handling as not yet supported at runtime, even when using CUDA-oriented build/install switches.

## Repository layout

- `cmd/`: executable entrypoints (`payload-manager`, `payloadctl`).
- `internal/`: core runtime, services, storage tiers, DB adapters, lease/tiering/spill logic.
- `api/`: protobuf definitions and generated interface artifacts.
- `client/`: C++ and Python client surfaces.
- `tests/`: unit and integration coverage.
- `docs/`: architecture, design, and testing documentation.

## License

Apache-2.0
