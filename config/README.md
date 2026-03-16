# Example runtime config variants

This folder contains example Payload Manager runtime configuration variants across
GPU usage, database backend, and deployment type.

## Local / bare-metal

- `runtime-with-gpu.yaml` - GPU-enabled + SQLite backend.
- `runtime-with-gpu-postgres.yaml` - GPU-enabled + PostgreSQL backend.
- `runtime-no-gpu.yaml` - no GPU tier + PostgreSQL backend.
- `runtime-no-gpu-sqlite.yaml` - no GPU tier + SQLite backend.

## Docker / container

- `runtime-docker-gpu-sqlite.yaml` - Docker, GPU-enabled + SQLite backend.
- `runtime-docker-gpu-postgres.yaml` - Docker, GPU-enabled + PostgreSQL backend.
- `runtime-docker-gpu-sqlite-stress.yaml` - Docker, GPU-enabled + SQLite backend (high-load / stress-test settings).
- `runtime-docker-sqlite.yaml` - Docker, no GPU + SQLite backend.
- `runtime-docker-postgres.yaml` - Docker, no GPU + PostgreSQL backend.
- `runtime-docker-no-otel-sqlite.yaml` - Docker, no GPU + SQLite backend, OpenTelemetry disabled.
- `runtime-docker-no-otel-postgres.yaml` - Docker, no GPU + PostgreSQL backend, OpenTelemetry disabled.

Use any variant as a starting point, then update paths, credentials, and
capacity limits for your deployment.
