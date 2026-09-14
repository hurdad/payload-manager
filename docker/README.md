# Docker Assets

This directory contains all Docker build and Docker Compose assets for Payload Manager.

## Dockerfiles

- `Dockerfile` — default payload-manager image (no OpenTelemetry, no GPU).
- `Dockerfile.otel` — payload-manager image with OpenTelemetry support.
- `Dockerfile.cuda` — payload-manager image with GPU + OpenTelemetry support.
- `Dockerfile.jetpack6` — payload-manager image for NVIDIA Jetson on JetPack 6 (Orin, aarch64, L4T R36), GPU + OpenTelemetry. Builds no dependencies itself; see below.
- `Dockerfile.jetpack6-deps` — the dependency images `Dockerfile.jetpack6` builds on. Two targets, `dev` and `runtime`.
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

## JetPack 6

These are named for the JetPack release, not for the board. That is the axis
everything depends on: JetPack 6 fixes L4T R36, Ubuntu 22.04 and CUDA 12.2
together, and the base image tags and every distro package name follow from that
trio. A second JetPack generation gets its own file pair rather than a flag —
see [Other JetPack releases](#other-jetpack-releases).

`Dockerfile.jetpack6` is the only image whose dependencies are not available as
distribution packages. Ubuntu 22.04 — what JetPack 6 ships — has no
opentelemetry-cpp, no libarrow with both S3 and CUDA, an re2 with no CMake
config, and a libpqxx four minor versions too old to compile the repository
layer. So Abseil, re2, Protobuf, gRPC, OpenTelemetry, the AWS SDK, Arrow and
libpqxx are built from source by `Dockerfile.jetpack6-deps`.

That takes hours on Jetson hardware, and none of it changes when this project's
sources do, so it is a separate image rather than a stage — two, in fact:

| Target | Published as | Contents |
|---|---|---|
| `dev` | `ghcr.io/hurdad/payload-manager-jetpack6-deps-dev` | Headers, static archives, CUDA toolchain |
| `runtime` | `ghcr.io/hurdad/payload-manager-jetpack6-deps-runtime` | Shared objects only, on the smaller CUDA base |

`Dockerfile.jetpack6` starts `FROM` both, so it rebuilds in minutes.
`.github/workflows/jetpack6-deps.yml` republishes the pair when
`Dockerfile.jetpack6-deps` changes, tagging by a hash of that file's contents —
an unchanged recipe resolves to a tag already in the registry and the workflow
skips the build. Old tags stay resolvable after a bump.

```bash
# Normal case: against the published dependency images.
docker build -f docker/Dockerfile.jetpack6 -t payload-manager:jetpack6 .

# Changing a dependency version: build the pair locally first.
docker build -f docker/Dockerfile.jetpack6-deps --target dev \
  -t pm-jetpack6-deps-dev:local .
docker build -f docker/Dockerfile.jetpack6-deps --target runtime \
  -t pm-jetpack6-deps-runtime:local .
docker build -f docker/Dockerfile.jetpack6 \
  --build-arg DEPS_DEV_IMAGE=pm-jetpack6-deps-dev:local \
  --build-arg DEPS_RUNTIME_IMAGE=pm-jetpack6-deps-runtime:local \
  -t payload-manager:jetpack6 .
```

`BUILD_JOBS` defaults to 6, which is every core on an Orin Nano or Orin NX.
Raising it past the core count only oversubscribes — no extra throughput, and
peak memory rises, which is the binding constraint on a board with 8 GB and only
zram behind it.

Arrow is capped separately by `ARROW_BUILD_JOBS`, default 4. Its compute kernels
are the outlier: `scalar_compare`, `scalar_arithmetic` and `vector_selection`
each peak near 2 GB in the compiler, where the other seven dependencies sit
nearer 0.5–1 GB. On a 4 GB board drop both (`--build-arg BUILD_JOBS=2
--build-arg ARROW_BUILD_JOBS=1`); on an AGX Orin with 32–64 GB, raise them to the
core count.

### Other JetPack releases

There is no JetPack 7 image yet, so **AGX Thor is not supported**. The blocker is
not the GPU: nothing in this project or in Arrow's CUDA module compiles device
code, so Blackwell's compute capability never comes into it. The blocker is
userspace.

| | JetPack 6 (Orin) | JetPack 7 (Thor) |
|---|---|---|
| L4T | R36 | R38 |
| Ubuntu | 22.04 jammy | 24.04 noble |
| CUDA | 12.2 | 13 |

The container runtime bind-mounts the *host's* driver libraries into the
container. Mounting R38's libraries, built against glibc 2.39, into a 22.04
container on glibc 2.35 fails on symbol versions — so the JetPack 6 image cannot
simply be run on a Thor board.

Adding `Dockerfile.jetpack7` and `Dockerfile.jetpack7-deps` is mostly mechanical.
The from-source half carries over unchanged; Abseil, re2, Protobuf, gRPC,
OpenTelemetry, the AWS SDK, Arrow and libpqxx all build the same on noble. What
changes is the base image tags and the distro package names, which are renamed
across the release: `libre2-9` → `libre2-11`, `libyaml-cpp0.7` → `libyaml-cpp0.8`,
`libssl3` → `libssl3t64`, `libcurl4` → `libcurl4t64`, `libutf8proc2` →
`libutf8proc3`, `libxml2` → `libxml2-16`. `docker/Dockerfile.cuda` already targets
a newer Ubuntu and is a useful reference for the renamed set.

It is deliberately not written in advance: an image nobody can run on real
hardware cannot be verified, and a broken published tag is worse than a missing
one.

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
