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
- `runtime-disk-hot-cold.yaml` - two local durable levels: NVMe for warm
  payloads, HDD for cold bulk, demoting RAM -> disk -> cold disk -> object.
  The `disk_cold` block is what enables `TIER_DISK_COLD`; omit it and the chain
  reverts to RAM -> disk -> object. See [Cold Disk Tier](../docs/DISK_COLD_TIER.md).

Every file here names its `database` backend explicitly. An absent `database`
block also selects the in-memory catalog, which behaves correctly right up until
the first restart, so it is worth saying which one you meant.

## Docker / container

### Observability not configured

- `runtime-docker-memory.yaml` - in-memory catalog, no GPU. Used by
  `docker-compose.memory.yml`, which needs no database container.

- `runtime-docker-postgres.yaml` - no GPU.
- `runtime-docker-postgres-minio.yaml` - no GPU + MinIO object store.
- `runtime-docker-gpu-postgres.yaml` - GPU-enabled.

### Observability configured (OTLP to `alloy:4317`)

- `runtime-docker-otel-postgres.yaml` - no GPU.
- `runtime-docker-gpu-otel-postgres.yaml` - GPU-enabled.
- `runtime-docker-gpu-otel-postgres-minio.yaml` - GPU-enabled + MinIO object store.
- `runtime-docker-gpu-otel-postgres-stress.yaml` - GPU-enabled, high-load / stress-test settings.

- `runtime-docker-tls.yaml` - the PostgreSQL stack with `server.tls` and
  `server.auth` switched on, used by `docker-compose.tls.yml` and by the TLS
  integration job. It mounts the development credentials from `certs/`, so run
  `scripts/gen-dev-certs.sh` first.

Use any variant as a starting point, then update paths, credentials, and
capacity limits for your deployment.

## Transport security and authentication

Both live under `server:` and both are off when absent, so every config here
predating them works unchanged:

```yaml
server:
  bind_address: "0.0.0.0:50051"

  # Additional listeners, same syntax. Lets one process serve a Unix socket to
  # node-local clients and TCP to everything else.
  extra_bind_addresses:
    - "unix:///run/payload-manager/pm.sock"

  tls:
    enabled: true            # optional: absent block is off, present is on
    cert_file: /etc/payload-manager/tls/server.pem
    key_file:  /etc/payload-manager/tls/server-key.pem
    client_ca_file: ""       # set to also require and verify client certificates

  auth:
    enabled: true
    jwt_hs256_key_file: /etc/payload-manager/auth/hs256.key
    # jwt_public_key_file: ...   # instead, for tokens from an external issuer
    issuer: ""                   # empty means the claim is not checked
    audience: ""
    clock_skew_seconds: 60       # 0 selects 60
```

`enabled` exists so a templated config can render the block unconditionally and
flip one value rather than adding and removing the block. A present-but-disabled
block keeps its paths and warns at startup.

`auth` requires `tls`: gRPC validates tokens only on non-insecure credentials,
and a bearer token in cleartext is readable by anything on the path. Configuring
one without the other fails at startup rather than starting insecure.

Unix socket addresses use three slashes for an absolute path
(`unix:///run/payload-manager/pm.sock`) or none for a relative one
(`unix:relative.sock`). `unix://host/path` is an authority form the scheme
rejects — and the socket path itself must be under 108 characters, a kernel
limit on `sockaddr_un`.
