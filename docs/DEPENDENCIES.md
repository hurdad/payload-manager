# Dependency Reference

Every third-party version payload-manager builds against, and — more importantly
— where each one is pinned. The numbers below are a snapshot; the file paths are
the contract. When they disagree, the file wins.

Versions current as of 2026-09-15, on **Ubuntu 26.04 LTS (Resolute Raccoon)**,
which is the base for every image and every C++ CI job.

## 1. Where versions are pinned

There is no single manifest. Each ecosystem owns its own file, and a few numbers
are deliberately repeated in more than one place:

| Ecosystem | Source of truth |
|---|---|
| C++ system libraries | `docker/Dockerfile*`, `.github/actions/setup-cpp-deps/action.yml` |
| Arrow | `ARROW_VERSION` build arg, defaulted in each Dockerfile |
| Arrow minimum for CMake | `PAYLOAD_MANAGER_ARROW_MIN_VERSION` in `CMakeLists.txt` |
| Go | `gateway/go.mod` |
| Python client | `client/python/setup.py` |
| Python example images | `docker/Dockerfile.examples.python{,.cuda}` |
| UI | `ui/package.json` |
| Code generation | `scripts/codegen.sh` |
| CI actions | `.github/workflows/ci.yml` |

Dependabot watches four of these — `gateway/go.mod`, `ui/package.json`, the
`docker/` base images and the workflow's `uses:` refs (see
`.github/dependabot.yml`). It cannot see the rest. Section 9 lists the couplings
that therefore have to be maintained by hand.

## 2. Ubuntu 26.04 system packages

Installed by `docker/Dockerfile` and by `setup-cpp-deps` for the C++ CI jobs.
These are Ubuntu's own builds; they move with Ubuntu security updates, so treat
the versions as "what 26.04 shipped", not as pins.

| Package | Version | Used for |
|---|---|---|
| `build-essential` | 12.12ubuntu2.26.04.2 | gcc/g++ toolchain |
| `cmake` | 4.2.3-2ubuntu2 | build system (project requires ≥ 3.20) |
| `ninja-build` | 1.13.2-1 | build driver |
| `pkg-config` | 2.5.1-4 | dependency discovery |
| `git` | 1:2.53.0-1ubuntu1 | source control |
| `protobuf-compiler` / `libprotobuf-dev` | 3.21.12-15ubuntu1 | C++ protobuf runtime and `protoc` |
| `protobuf-compiler-grpc` / `libgrpc++-dev` / `libgrpc-dev` | 1.51.1-8ubuntu1 | C++ gRPC |
| `libyaml-cpp-dev` | 0.8.0+dfsg-9 | config parsing |
| `libpqxx-dev` | 7.10.0-2build1 | PostgreSQL metadata backend |
| `libpq-dev` | 18.6-0ubuntu0.26.04.1 | libpq, under libpqxx |
| `libspdlog-dev` | 1:1.15.3+ds-1build1 | logging |
| `libjemalloc-dev` | 5.3.0-4 | allocator (`-DPAYLOAD_MANAGER_ENABLE_JEMALLOC=ON`) |
| `opentelemetry-cpp-dev` | 1.23.0-3build1 | metrics and tracing |
| `libcurl4-openssl-dev` | 8.18.0-1ubuntu2.5 | OTLP/HTTP exporter |
| `libgtest-dev` | 1.17.0-1build1 | unit tests |
| `libssl-dev` | 3.5.5-1ubuntu3.5 | TLS |
| `libre2-dev` | 20250805-1build3 | Arrow dependency |
| `libutf8proc-dev` | 2.10.0-2 | Arrow dependency |
| `libxml2-dev` | 2.15.2+dfsg-0.1ubuntu0.1 | Arrow dependency |
| `zlib1g-dev` | 1:1.3.dfsg+really1.3.1-1ubuntu3.1 | compression |
| `liblz4-dev` | 1.10.0-8 | compression |
| `libzstd-dev` | 1.5.7+dfsg-3 | compression |
| `libsnappy-dev` | 1.2.2-2 | compression |
| `libbrotli-dev` | 1.2.0-3build1 | compression |
| `libbz2-dev` | 1.0.8-6ubuntu0.1 | compression |

`opentelemetry-cpp-dev` and the Arrow dependencies live in **universe**, which
the `ubuntu:26.04` base image does not enable; every build enables it by
rewriting `Components:` in `/etc/apt/sources.list.d/ubuntu.sources`.

## 3. Apache Arrow

**25.0.1-1**, pinned as `libarrow-dev=${ARROW_VERSION}-1`.

Not from Ubuntu. Universe carries 23.0.1-9, built with `ARROW_S3=OFF`, which
cannot link the object tier or the client, and has no CUDA build. Every image
and CI job installs the Apache Arrow apt repository instead:

```
https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-resolute.deb
```

`CMakeLists.txt` sets `PAYLOAD_MANAGER_ARROW_MIN_VERSION` to 23.0 and enforces
it after `find_package(Arrow REQUIRED)` rather than through it —
`ArrowConfigVersion.cmake` declares `SameMajorVersion`, so asking for 23.0
*rejects* 25.x instead of treating it as a floor.

The `resolute` in that URL is the Ubuntu 26.04 codename, so it changes with the
base image.

## 4. Go — the gRPC gateway

`gateway/go.mod`. Language version `go 1.25.0`, `toolchain go1.25.8`.

| Module | Version |
|---|---|
| `github.com/grpc-ecosystem/grpc-gateway/v2` | v2.30.0 |
| `google.golang.org/grpc` | v1.83.0 |
| `google.golang.org/protobuf` | v1.36.12 |
| `google.golang.org/genproto/googleapis/api` | v0.0.0-20260803160001-6ac0973c030d |
| `golang.org/x/net` (indirect) | v0.57.0 |
| `golang.org/x/sys` (indirect) | v0.47.0 |
| `golang.org/x/text` (indirect) | v0.40.0 |
| `google.golang.org/genproto/googleapis/rpc` (indirect) | v0.0.0-20260803160001-6ac0973c030d |

CI resolves the compiler through `setup-go`'s `go-version-file: gateway/go.mod`,
so it builds on the `toolchain` line — 1.25.8. The release image builds on
`golang:1.27-bookworm`. Both satisfy `go 1.25.0`; they are not the same compiler.

## 5. Python client

`client/python/setup.py` holds the real floors. They are dictated by the
committed stubs under `client/python/payload/`, not chosen: the `*_pb2` modules
call `ValidateProtobufRuntimeVersion` against their protoc gencode version, and
the `*_pb2_grpc` modules raise on a grpcio older than the `grpcio-tools` that
produced them.

| Package | Floor |
|---|---|
| `grpcio` | ≥ 1.75.1 |
| `protobuf` | ≥ 6.31.1 |
| `pyarrow` | ≥ 14 |
| `opentelemetry-api` (extra `otel`) | ≥ 1.20 |

The example images install the same floors plus a pinned pyarrow:

- `docker/Dockerfile.examples.python` — `python:3.14-slim`, `pyarrow==25.0.1`,
  `numpy>=1.25`, `googleapis-common-protos>=1.56`. pyarrow comes from the
  published wheel, so this image needs no compiler.
- `docker/Dockerfile.examples.python.cuda` — builds Arrow C++ and pyarrow from
  source with `PYARROW_WITH_CUDA=1`, because no published wheel or deb ships
  `pyarrow.cuda`.

CI's Python job runs on **3.14**, matching the example image's base.

## 6. UI

`ui/package.json`.

| Package | Version |
|---|---|
| `svelte` | ^5.57.0 |
| `vite` | ^8.3.0 |
| `@sveltejs/vite-plugin-svelte` | ^7.3.0 |
| `@playwright/test` | ^1.63.0 |
| `orval` | ^8.32.0 |
| `typescript` | ^7.0.2 |

There is no `tsconfig.json` and no typecheck script; TypeScript is a transitive
build-time dependency, and `.svelte`/`.ts` are transpiled by Vite without type
checking. `orval` regenerates a client into `src/lib/client/` on `npm run
generate`; that output is not committed and the app uses the hand-written
`src/lib/api.js`.

## 7. Container base images

Bumped by Dependabot's `docker` ecosystem.

| Image | Base | Used by |
|---|---|---|
| `Dockerfile` | `ubuntu:26.04` | server |
| `Dockerfile.payloadctl` | `ubuntu:26.04` | CLI |
| `Dockerfile.test`, `Dockerfile.test.minio` | `ubuntu:26.04` | test stacks |
| `Dockerfile.examples.cpp` | `ubuntu:26.04` | C++ examples |
| `Dockerfile.cuda`, `Dockerfile.examples.cuda`, `Dockerfile.examples.python.cuda` | `nvidia/cuda:13.3.1-{devel,runtime}-ubuntu26.04` | GPU builds |
| `Dockerfile.examples.python` | `python:3.14-slim` | Python examples |
| `Dockerfile.gateway` | `node:26-alpine` → `golang:1.27-bookworm` → `gcr.io/distroless/base-debian12:nonroot` | gateway + UI |
| `Dockerfile.e2e` | `mcr.microsoft.com/playwright:v1.63.0-noble` | Playwright |

`ARROW_VERSION` is an `ARG`, not a `FROM`, so Dependabot does not track it. Eight
Dockerfiles declare `ARG ARROW_VERSION=25.0.1`; `Dockerfile.examples.python`
spells the same number `PYARROW_VERSION`, since it installs the wheel rather
than building Arrow. All nine must be changed by hand, together.

## 8. GitHub Actions

| Action | Version |
|---|---|
| `actions/checkout` | v7 |
| `actions/setup-go` | v7 |
| `actions/setup-python` | v7 |
| `actions/upload-artifact` | v7 |
| `actions/download-artifact` | v8 |
| `docker/setup-buildx-action` | v3 |
| `docker/login-action` | v4 |
| `docker/metadata-action` | v6 |
| `docker/build-push-action` | v6 |
| `codecov/codecov-action` | v6 |
| `codecov/test-results-action` | v1 |

## 9. Code generation, and what must move together

`scripts/codegen.sh` regenerates the committed Go and Python stubs and pins
every input, because CI fails when regenerating produces a diff:

| Tool | Pin |
|---|---|
| `buf` | v1.73.0 |
| `protoc-gen-go` | v1.36.11 |
| `protoc-gen-go-grpc` | v1.6.1 |
| `protoc-gen-grpc-gateway` / `protoc-gen-openapiv2` | v2.28.0 |
| `grpcio-tools` | 1.75.1 |

The Go toolchain that *compiles* those plugins is read out of `gateway/go.mod`
rather than taken from the machine, because building the same plugin under a
different Go changes its output.

These couplings are not enforced by any tool. Each has already broken once:

- **`client/python/setup.py` floors ↔ the two Python example Dockerfiles.**
  Three copies of the same two numbers. The Dockerfiles drifted below the
  stubs' gencode guard and every example died on import.
- **`scripts/codegen.sh` pins ↔ `gateway/go.mod`.** Dependabot bumps the module
  but cannot touch the shell variables, so the committed stubs are generated by
  plugin versions that may lag the declared dependencies. Changing a pin means
  regenerating and committing the result in the same change.
- **`Dockerfile.e2e`'s Playwright tag ↔ `@playwright/test`.** The image bundles
  browsers matched to one Playwright version.
- **`ci.yml`'s `python-version` ↔ `Dockerfile.examples.python`'s base.** Testing
  the client on a different interpreter than the image ships is how a
  version-specific failure reaches a published image.
- **`ARROW_VERSION` across the eight Dockerfiles that declare it,
  `PYARROW_VERSION` in `Dockerfile.examples.python`, `setup-cpp-deps`'
  `arrow-version` default, and the codename in the Arrow apt URL.**
