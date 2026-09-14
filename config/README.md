# Example runtime config variants

This folder contains example Payload Manager runtime configuration variants across
GPU usage and deployment type.

The persistent backend is PostgreSQL; the in-memory backend remains available for
tests and ephemeral runs. (SQLite was removed — see the repository history.)

OpenTelemetry is compiled into every service image, so the headings below are
about what each file *configures*, not which image it needs. A file with
`metrics_enabled` and `tracing_enabled` unset exports nothing and starts no
exporter; setting either without an `otlp_endpoint` assumes a collector on
localhost and logs a warning saying so.

## Local / bare-metal

- `runtime-with-gpu.yaml` - GPU-enabled, in-memory catalog, and the only file
  here that declares a ring. Runs from this file alone with nothing to install;
  payload references do not survive a restart. `examples/cpp/ring_example.cpp`
  targets its `example` ring.
- `runtime-with-gpu-postgres.yaml` - the same deployment backed by PostgreSQL.
- `runtime-no-gpu.yaml` - no GPU tier, PostgreSQL.

Every file here names its `database` backend explicitly. An absent `database`
block also selects the in-memory catalog, which behaves correctly right up until
the first restart, so it is worth saying which one you meant.

## Docker / container

### Observability not configured

- `runtime-docker-postgres.yaml` - no GPU.
- `runtime-docker-postgres-minio.yaml` - no GPU + MinIO object store.
- `runtime-docker-gpu-postgres.yaml` - GPU-enabled.

### Observability configured (OTLP to `alloy:4317`)

- `runtime-docker-otel-postgres.yaml` - no GPU.
- `runtime-docker-gpu-otel-postgres.yaml` - GPU-enabled.
- `runtime-docker-gpu-otel-postgres-minio.yaml` - GPU-enabled + MinIO object store.
- `runtime-docker-gpu-otel-postgres-stress.yaml` - GPU-enabled, high-load / stress-test settings.

Use any variant as a starting point, then update paths, credentials, and
capacity limits for your deployment.
