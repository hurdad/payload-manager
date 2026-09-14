# Example runtime config variants

This folder contains example Payload Manager runtime configuration variants across
GPU usage and deployment type.

The persistent backend is PostgreSQL; the in-memory backend remains available for
tests and ephemeral runs. (SQLite was removed — see the repository history.)

## Local / bare-metal

- `runtime-with-gpu.yaml` - GPU-enabled.
- `runtime-with-gpu-postgres.yaml` - GPU-enabled, explicit PostgreSQL settings.
- `runtime-no-gpu.yaml` - no GPU tier.

## Docker / container

### Without OpenTelemetry

- `runtime-docker-postgres.yaml` - no GPU.
- `runtime-docker-postgres-minio.yaml` - no GPU + MinIO object store.
- `runtime-docker-gpu-postgres.yaml` - GPU-enabled.

### With OpenTelemetry

- `runtime-docker-otel-postgres.yaml` - no GPU.
- `runtime-docker-gpu-otel-postgres.yaml` - GPU-enabled.
- `runtime-docker-gpu-otel-postgres-minio.yaml` - GPU-enabled + MinIO object store.
- `runtime-docker-gpu-otel-postgres-stress.yaml` - GPU-enabled, high-load / stress-test settings.

Use any variant as a starting point, then update paths, credentials, and
capacity limits for your deployment.
