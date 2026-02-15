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

## Repository layout

- `cmd/`: executable entrypoints (`payload-manager`, `payloadctl`).
- `internal/`: core runtime, services, storage tiers, DB adapters, lease/tiering/spill logic.
- `api/`: protobuf definitions and generated interface artifacts.
- `client/`: C++ and Python client surfaces.
- `tests/`: unit and integration coverage.
- `docs/`: architecture, design, and testing documentation.

## License

Apache-2.0
