# Docker Assets

This directory contains all Docker build and Docker Compose assets for Payload Manager.

## Dockerfiles

- `Dockerfile` — default payload-manager image (no OpenTelemetry, no GPU).
- `Dockerfile.otel` — payload-manager image with OpenTelemetry support.
- `Dockerfile.cuda` — payload-manager image with GPU + OpenTelemetry support.
- `Dockerfile.jetson` — payload-manager image for NVIDIA Jetson (aarch64 / L4T / JetPack 6), GPU + OpenTelemetry. Builds no dependencies itself; see below.
- `Dockerfile.jetson-deps` — the dependency images `Dockerfile.jetson` builds on. Two targets, `dev` and `runtime`.
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

## Jetson

`Dockerfile.jetson` is the only image whose dependencies are not available as
distribution packages. Ubuntu 22.04 — what JetPack 6 ships — has no
opentelemetry-cpp, no libarrow with both S3 and CUDA, an re2 with no CMake
config, and a libpqxx four minor versions too old to compile the repository
layer. So Abseil, re2, Protobuf, gRPC, OpenTelemetry, the AWS SDK, Arrow and
libpqxx are built from source by `Dockerfile.jetson-deps`.

That takes hours on Jetson hardware, and none of it changes when this project's
sources do, so it is a separate image rather than a stage — two, in fact:

| Target | Published as | Contents |
|---|---|---|
| `dev` | `ghcr.io/hurdad/payload-manager-jetson-deps-dev` | Headers, static archives, CUDA toolchain |
| `runtime` | `ghcr.io/hurdad/payload-manager-jetson-deps-runtime` | Shared objects only, on the smaller CUDA base |

`Dockerfile.jetson` starts `FROM` both, so it rebuilds in minutes.
`.github/workflows/jetson-deps.yml` republishes the pair when
`Dockerfile.jetson-deps` changes, tagging by a hash of that file's contents —
an unchanged recipe resolves to a tag already in the registry and the workflow
skips the build. Old tags stay resolvable after a bump.

```bash
# Normal case: against the published dependency images.
docker build -f docker/Dockerfile.jetson -t payload-manager:jetson .

# Changing a dependency version: build the pair locally first.
docker build -f docker/Dockerfile.jetson-deps --target dev \
  -t pm-jetson-deps-dev:local .
docker build -f docker/Dockerfile.jetson-deps --target runtime \
  -t pm-jetson-deps-runtime:local .
docker build -f docker/Dockerfile.jetson \
  --build-arg DEPS_DEV_IMAGE=pm-jetson-deps-dev:local \
  --build-arg DEPS_RUNTIME_IMAGE=pm-jetson-deps-runtime:local \
  -t payload-manager:jetson .
```

On a board with 8 GB or less, pass `--build-arg BUILD_JOBS=2`. Arrow's heavier
translation units run past 2 GB each, and a Jetson has only zram to fall back on.

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
