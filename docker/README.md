# Docker Assets

This directory contains all Docker build and Docker Compose assets for Payload Manager.

## Dockerfiles

- `Dockerfile` — payload-manager image. OpenTelemetry is compiled in; no GPU.
- `Dockerfile.cuda` — payload-manager image with GPU + OpenTelemetry support.
- `Dockerfile.payloadctl` — `payloadctl` CLI image.
- `Dockerfile.gateway` — multi-stage image: Node UI build → Go gateway build → distroless runtime. Embeds the compiled Svelte UI into the gateway binary. Runs as the distroless `nonroot` user; the healthcheck is the binary probing its own `/healthz`, because distroless carries neither a shell nor curl.

  Configuration is by environment variable (or the equivalent flag):

  | Variable | Flag | Default | Meaning |
  | --- | --- | --- | --- |
  | `GRPC_ADDR` | `-grpc-addr` | `localhost:50051` | payload-manager to proxy to |
  | `HTTP_ADDR` | `-http-addr` | `:8080` | HTTP listen address |
  | `DISK_ROOT_PATH` | `-disk-root` | `/var/lib/payload-manager/payloads` | Shared disk-tier root, for `GET /v1/payloads/{id}/download` |
  | `CORS_ORIGINS` | `-cors-origins` | *(empty — CORS off)* | Comma-separated origins allowed to make cross-origin requests |
  | `GRPC_CA` | `-grpc-ca` | *(empty — plaintext)* | PEM CA bundle for the payload-manager connection |
  | `GRPC_SERVER_NAME` | `-grpc-server-name` | *(from the dial target)* | Name to verify against payload-manager's certificate |
  | `TLS_CERT` / `TLS_KEY` | `-tls-cert` / `-tls-key` | *(empty — plain HTTP)* | Certificate and key to serve HTTPS with |

  Leave `CORS_ORIGINS` unset unless something genuinely needs it. The embedded UI
  is served from the gateway itself and `npm run dev` proxies `/v1` through vite,
  so both supported ways of running the UI are same-origin. The service behind
  a deployment without `server.auth` has no caller identity at all, so enabling
  CORS — `*` especially — lets any
  page in an operator's browser reach every payload-manager that browser can
  route to. See `SECURITY.md`.
- `Dockerfile.test` — integration test image (`payload_manager_integration_api`), used by Compose overlays.
- `Dockerfile.test.minio` — object-tier spill test image (`payload_manager_integration_object_spill`), used by `docker-compose.minio.test.yml`.
- `Dockerfile.e2e` — Playwright image for the UI end-to-end suite, used by `docker-compose.e2e.yml` and `docker-compose.e2e.cuda.yml`.
- `Dockerfile.examples.cpp` — C++ examples image.
- `Dockerfile.examples.python` — Python examples image.
- `Dockerfile.examples.cuda` — CUDA C++ examples image.
- `Dockerfile.examples.python.cuda` — CUDA Python examples image.

Build examples from repository root:

```bash
docker build -f docker/Dockerfile -t payload-manager:latest .
docker build -f docker/Dockerfile.cuda -t payload-manager:cuda .
docker build -f docker/Dockerfile.payloadctl -t payloadctl:latest .
```

## OpenTelemetry

The two integration test images build with OpenTelemetry on, matching the
service images. That is not cosmetic: `internal/runtime/server.cpp` installs
`OtelServerInterceptorFactory` whenever `ENABLE_OTEL` is defined, with no config
gate, so an OTEL-off build would put the integration suite against a gRPC server
whose request path has no interceptor in it — a shape that no longer ships
anywhere. `otel_interceptor.cpp` and most of `tracing.cpp`, `metrics.cpp` and
`logging.cpp` sit behind the same guard.

`Dockerfile.examples.*` build with it off, correctly: those are client programs
(`BUILD_SERVICE=OFF`, `BUILD_CLIENT=ON`) and never start a server.

There is no separate `-otel` image. OpenTelemetry is compiled into
`Dockerfile` and `Dockerfile.cuda` unconditionally, and is inert until
configured: `observability.metrics_enabled` and `observability.tracing_enabled`
are plain proto3 bools defaulting to false, and `InitializeMetrics` /
`InitializeTracing` return before constructing an exporter, so an unconfigured
deployment starts no threads and opens no connections.

Carrying it costs 553 KB of binary and 3.9 MB of image — 1.6%, because both
variants were already dominated by the shared Arrow, gRPC and protobuf runtime.
That did not justify a second image, tag, pair of build jobs and compose
variant to keep in step.

There is no `-otel` tag any more. `ghcr.io/hurdad/payload-manager` is the
service image, with or without observability configured.

When `metrics_enabled` or `tracing_enabled` is set with no `otlp_endpoint`, the
service assumes a collector on localhost and logs a warning saying so — without
it the export fails on a loop with nothing in the log tying it back to config.

## Published images and architectures

| Image | Architectures |
|---|---|
| `ghcr.io/hurdad/payload-manager` | `linux/amd64`, `linux/arm64` |
| `ghcr.io/hurdad/payload-manager-payloadctl` | `linux/amd64`, `linux/arm64` |
| `ghcr.io/hurdad/payload-manager-gateway` | `linux/amd64`, `linux/arm64` |
| `ghcr.io/hurdad/payload-manager-cuda` | `linux/amd64` only — see below |

One tag serves both architectures, so the same command works on an x86 server
and on a Jetson:

```bash
docker pull ghcr.io/hurdad/payload-manager
```

Every image is tagged `latest` (default branch only), the branch name, `sha-<short>`,
and the git tag on a `v*` release. To see what a tag actually contains:

```bash
docker buildx imagetools inspect ghcr.io/hurdad/payload-manager:latest
```

### How the tags are built

Each architecture is built **natively on its own runner** — amd64 on a hosted
runner, arm64 on the self-hosted Jetson — and pushed **by digest with no tag**.
A separate manifest job then joins the digests into the tag with
`docker buildx imagetools create`. That is 7 build jobs and 4 manifest jobs in
`.github/workflows/ci.yml`.

The obvious alternative, one job emitting `linux/amd64,linux/arm64` through
QEMU, emulates an entire C++ compile and takes hours rather than minutes.

Two consequences worth knowing:

- **The arm64 builds queue.** There is one self-hosted board, so its builds run
  one at a time rather than in parallel with each other.
- **Each build's digest must stay its own.** The digest each build hands to the
  manifest job is written to `${{ runner.temp }}/digests`, wiped first, and the
  step asserts the directory holds exactly one file. On a hosted runner — a
  fresh VM per job — a shared path would be harmless; on the long-lived board it
  is not, and a shared `/tmp/digests` once had the second image upload its own
  digest plus the first one's, which surfaced much later as a manifest job being
  told to join a digest from a different repository.

A local build produces only the host's architecture, which is usually what you
want:

```bash
docker build -f docker/Dockerfile -t payload-manager:latest .
```

`Dockerfile.cuda` is **amd64 only**. The only arm64 target is a Jetson, and the
GPU tier cannot work there: CUDA IPC is unsupported on Tegra, and it fails
one-sided. Measured on an Orin (sm_87, `integrated=1`, JetPack 6 / CUDA 12.2.12),
two processes in one container:

```
producer: cudaIpcGetMemHandle  -> cudaSuccess
consumer: cudaIpcOpenMemHandle -> cudaErrorInvalidValue ("invalid argument")
```

The export half succeeds, so `CudaArrowStore::ExportIPC` returns a handle and the
server looks healthy, while the consumer fails inside
`arrow::cuda::CudaIpcMemHandle::FromBuffer` with an error that points nowhere
near the cause.

On a Jetson the GPU is integrated and addressing is unified, so the equivalent of
a GPU handoff is the ring tier: a consumer maps the `TIER_RAM_RING` `/dev/shm`
segment and registers it with CUDA to get a device-visible pointer to the same
physical pages. No CUDA IPC is involved, and it is the same code path the x86
build uses. The C++ client implements this — `RingConsumer::Options::register_for_gpu`
gives you `Lease::dev_va` next to `Lease::host_va` — see
[GPU access on integrated-GPU hardware](../docs/ARCHITECTURE.md#gpu-access-on-integrated-gpu-hardware-jetson).

There is no Jetson-specific Dockerfile. There was one, building Abseil, re2,
Protobuf, gRPC, OpenTelemetry, the AWS SDK, Arrow and libpqxx from source against
an `l4t-cuda` base, because JetPack 6's Ubuntu 22.04 packages none of them at
usable versions. That existed only to get CUDA. Without CUDA there is no reason
to pin the image to JetPack's userspace, and Ubuntu 26.04 arm64 — which runs
fine on the L4T kernel — packages every dependency at the same version the x86
images already pin, `libarrow-dev` 25.0.1, `libpqxx-dev` 7.10.0 and
`opentelemetry-cpp-dev` 1.23.0 included. The Jetson now gets the same image as
everything else.

## TLS and authentication

Off by default. `docker-compose.tls.yml` swaps the service onto a config with
`server.tls` and `server.auth` enabled and mounts the development credentials;
each client needs its companion overlay, because compose requires every service
in a merged project to have an image or build context — naming a client the
chosen stack does not include fails the whole project.

| Overlay | Switches over |
| --- | --- |
| `docker-compose.tls.yml` | `payload-manager` (always needed) |
| `docker-compose.tls.test.yml` | `integration-test` |
| `docker-compose.tls.minio.yml` | `object-spill-test` |
| `docker-compose.tls.examples.yml` | `cpp-examples` |
| `docker-compose.tls.examples.python.yml` | `python-examples` |
| `docker-compose.tls.gateway.yml` | `payload-gateway` (also serves HTTPS) |

```bash
scripts/gen-dev-certs.sh       # credentials are gitignored, not in any image

docker compose -f docker/docker-compose.postgres.yml \
               -f docker/docker-compose.tls.yml \
               -f docker/docker-compose.test.yml \
               -f docker/docker-compose.tls.test.yml \
               up --build --exit-code-from integration-test
```

The base stacks' healthchecks are untouched: `bash -c '</dev/tcp/localhost/50051'`
is a connect-only probe and succeeds against a TLS listener because it never
sends a byte.

## Compose files

### Base stacks

Named for the catalog backend, which is the thing that actually differs between
them. Observability is not a stack: since OpenTelemetry was folded into the
single service image, the only thing separating a "with OTEL" deployment from a
plain one was the config it mounted, so `docker-compose.otel.yml` overrides that
and nothing else.

| Stack | Catalog | GPU | gRPC port |
|---|---|---|---|
| `docker-compose.memory.yml` | in-memory; no database container | — | 50052 |
| `docker-compose.postgres.yml` | PostgreSQL | — | 50051 |
| `docker-compose.gpu.postgres.yml` | PostgreSQL | yes | 50056 |

### Gateway stacks

Self-contained: `payload-manager` and `payload-gateway` together, sharing a data
volume so payload downloads work across every tier. UI on `http://localhost:8080/`.

| Stack | Catalog | GPU |
|---|---|---|
| `docker-compose.gateway.yml` | PostgreSQL | — |
| `docker-compose.gateway.gpu.yml` | PostgreSQL | yes |

### End-to-end stacks

Complete stacks that additionally run the Playwright UI suite (`Dockerfile.e2e`)
and exit with its result.

| Stack | GPU |
|---|---|
| `docker-compose.e2e.yml` | — |
| `docker-compose.e2e.cuda.yml` | yes |

### Overlays

Layered onto a base stack with repeated `-f`, left to right.

| Overlay | Effect |
|---|---|
| `docker-compose.otel.yml` | Swaps in a config with metrics and tracing enabled |
| `docker-compose.observability.yml` | Adds Alloy (OTLP on 4317), Prometheus, Grafana (`:3000`) and Tempo to receive it |
| `docker-compose.minio.yml` | Adds MinIO (S3-compatible, `:9000`) and points the object tier at it |
| `docker-compose.minio.init.yml` | One-shot container that pre-creates the `payloads` bucket; layered on `minio.yml` |
| `docker-compose.minio.gpu.yml` | Swaps `minio.yml`'s config for the combined GPU + OTEL + MinIO one |
| `docker-compose.test.yml` | Runs the integration suite against the stack and exits with its result |
| `docker-compose.stress.yml` | Runs the tier-spill stress example |
| `docker-compose.examples.yml` | Runs the C++ examples |
| `docker-compose.examples.cuda.yml` | Runs the CUDA C++ examples |
| `docker-compose.examples.python.yml` | Runs the Python examples |
| `docker-compose.examples.python.cuda.yml` | Runs the CUDA Python examples |

`minio.yml` relies on the service creating the bucket itself through
`allow_bucket_creation`, which is what the integration tests use. Add
`minio.init.yml` when you would rather the bucket existed first.

Run from the repository root:

```bash
# Simplest thing that runs: one container, no database
docker compose -f docker/docker-compose.memory.yml up --build

# PostgreSQL, observability not configured
docker compose -f docker/docker-compose.postgres.yml up --build

# gRPC-Gateway + UI
docker compose -f docker/docker-compose.gateway.yml up --build

# With observability, and something to receive it
docker compose -f docker/docker-compose.postgres.yml \
               -f docker/docker-compose.otel.yml \
               -f docker/docker-compose.observability.yml up --build

# Object tier on MinIO, bucket pre-created
docker compose -f docker/docker-compose.postgres.yml \
               -f docker/docker-compose.minio.yml \
               -f docker/docker-compose.minio.init.yml up --build
```

> Note: Compose files set `build.context: ..` and `dockerfile: docker/...` so they can be executed via `-f docker/<file>.yml` while still building from the repository root context.
