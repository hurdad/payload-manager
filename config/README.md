# Example runtime config variants

This folder contains example Payload Manager runtime configuration variants across
GPU usage, database backend, and deployment type.

## Local / bare-metal

- `runtime-with-gpu.yaml` - GPU-enabled + SQLite backend.
- `runtime-with-gpu-postgres.yaml` - GPU-enabled + PostgreSQL backend.
- `runtime-no-gpu.yaml` - no GPU tier + PostgreSQL backend.
- `runtime-no-gpu-sqlite.yaml` - no GPU tier + SQLite backend.

## Docker / container

### Without OpenTelemetry

- `runtime-docker-sqlite.yaml` - no GPU + SQLite backend.
- `runtime-docker-postgres.yaml` - no GPU + PostgreSQL backend.
- `runtime-docker-sqlite-minio.yaml` - no GPU + SQLite backend + MinIO object store.
- `runtime-docker-gpu-postgres.yaml` - GPU-enabled + PostgreSQL backend.

### With OpenTelemetry

- `runtime-docker-otel-sqlite.yaml` - no GPU + SQLite backend.
- `runtime-docker-otel-postgres.yaml` - no GPU + PostgreSQL backend.
- `runtime-docker-gpu-otel-sqlite.yaml` - GPU-enabled + SQLite backend.
- `runtime-docker-gpu-otel-sqlite-minio.yaml` - GPU-enabled + SQLite backend + MinIO object store.
- `runtime-docker-gpu-otel-sqlite-stress.yaml` - GPU-enabled + SQLite backend, high-load / stress-test settings.

Use any variant as a starting point, then update paths, credentials, and
capacity limits for your deployment.
