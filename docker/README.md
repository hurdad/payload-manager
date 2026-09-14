# Docker Assets

This directory contains all Docker build and Docker Compose assets for Payload Manager.

## Dockerfiles

- `Dockerfile` — payload-manager image. OpenTelemetry is compiled in; no GPU.
- `Dockerfile.cuda` — payload-manager image with GPU + OpenTelemetry support.
- `Dockerfile.payloadctl` — `payloadctl` CLI image.
- `Dockerfile.gateway` — multi-stage image: Node UI build → Go gateway build → distroless runtime. Embeds the compiled Svelte UI into the gateway binary.
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

## Compose files

Base stacks:

- `docker-compose.postgres.yml`
- `docker-compose.otel.postgres.yml`
- `docker-compose.gpu.postgres.yml`

Gateway stack (self-contained):

- `docker-compose.gateway.yml` — runs `payload-manager` (Postgres) + `payload-gateway` together. The gateway UI is available at `http://localhost:8080/`. Both containers share the same data volume so payload downloads work across all tiers.

Overlays:

- `docker-compose.observability.yml`
- `docker-compose.examples.yml`
- `docker-compose.examples.python.yml`
- `docker-compose.examples.cuda.yml`
- `docker-compose.examples.python.cuda.yml`
- `docker-compose.test.yml`
- `docker-compose.stress.yml`

Run examples from repository root:

```bash
# gRPC-Gateway + UI (Postgres)
docker compose -f docker/docker-compose.gateway.yml up --build

# Plain gRPC only (Postgres)
docker compose -f docker/docker-compose.postgres.yml up --build

# With observability
docker compose -f docker/docker-compose.otel.postgres.yml -f docker/docker-compose.observability.yml up --build
```

> Note: Compose files set `build.context: ..` and `dockerfile: docker/...` so they can be executed via `-f docker/<file>.yml` while still building from the repository root context.
