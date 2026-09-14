# Docker Assets

This directory contains all Docker build and Docker Compose assets for Payload Manager.

## Dockerfiles

- `Dockerfile` — default payload-manager image (no OpenTelemetry, no GPU).
- `Dockerfile.otel` — payload-manager image with OpenTelemetry support.
- `Dockerfile.cuda` — payload-manager image with GPU + OpenTelemetry support.
- `Dockerfile.payloadctl` — `payloadctl` CLI image.
- `Dockerfile.gateway` — multi-stage image: Node UI build → Go gateway build → distroless runtime. Embeds the compiled Svelte UI into the gateway binary.
- `Dockerfile.test` — integration test image used by Compose overlays.
- `Dockerfile.examples.cpp` — C++ examples image.
- `Dockerfile.examples.python` — Python examples image.
- `Dockerfile.examples.cuda` — CUDA C++ examples image.
- `Dockerfile.examples.python.cuda` — CUDA Python examples image.

Build examples from repository root:

```bash
docker build -f docker/Dockerfile -t payload-manager:latest .
docker build -f docker/Dockerfile.otel -t payload-manager:otel .
docker build -f docker/Dockerfile.cuda -t payload-manager:cuda .
docker build -f docker/Dockerfile.payloadctl -t payloadctl:latest .
```

## Architectures

`Dockerfile`, `Dockerfile.otel`, `Dockerfile.payloadctl` and `Dockerfile.gateway`
publish as multi-arch tags covering `linux/amd64` and `linux/arm64`, so a single
pull resolves itself:

```bash
docker pull ghcr.io/hurdad/payload-manager        # amd64 server or Jetson, same tag
```

CI builds each architecture natively — amd64 on a hosted runner, arm64 on a
self-hosted Jetson — pushes each by digest with no tag, and a manifest job joins
the digests into the tag. Emitting both platforms from one job through QEMU
would emulate an entire C++ build and take hours.

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
segment and calls `cudaHostRegister` on it to get a device-visible pointer. No
CUDA IPC is involved, and it is the same code path the x86 build uses.

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
