# Example runtime config variants

This folder contains example Payload Manager runtime configuration variants across
GPU usage and database backend.

- `runtime-with-gpu.yaml` - GPU-enabled + SQLite backend.
- `runtime-with-gpu-postgres.yaml` - GPU-enabled + PostgreSQL backend.
- `runtime-no-gpu.yaml` - no GPU tier + PostgreSQL backend.
- `runtime-no-gpu-sqlite.yaml` - no GPU tier + SQLite backend.

Use any variant as a starting point, then update paths, credentials, and
capacity limits for your deployment.
