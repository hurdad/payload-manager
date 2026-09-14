#!/usr/bin/env bash
#
# Regenerate the committed Go and Python stubs.
#
# Every input here is pinned, because generated output is committed and CI
# fails the build when what it regenerates differs from what is in the tree.
# Pinning the plugin versions alone is not enough: the Go toolchain that
# *compiles* protoc-gen-go-grpc changes its output too. Building the same
# v1.6.1 plugin under Go 1.27 instead of the 1.25 named in gateway/go.mod
# rewrote comment formatting across 154 lines of two service stubs, which
# reached CI as an unexplained drift failure.
#
# So the toolchain comes from gateway/go.mod rather than from whatever the
# machine happens to have, and grpcio-tools is pinned to the version whose
# output matches the committed Python stubs. Those numbers are also what
# .github/workflows/ci.yml installs; change them in both places together.
#
# Usage:
#   scripts/codegen.sh        # both
#   scripts/codegen.sh go     # Go stubs + OpenAPI only
#   scripts/codegen.sh python # Python stubs only

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Pinned tool versions. Keep in step with .github/workflows/ci.yml.
BUF_VERSION="v1.73.0"
PROTOC_GEN_GO="v1.36.11"
PROTOC_GEN_GO_GRPC="v1.6.1"
GRPC_GATEWAY="v2.28.0"
GRPCIO_TOOLS="1.75.1"

# Read the toolchain out of go.mod so it cannot drift from what CI resolves
# through setup-go's go-version-file.
GO_TOOLCHAIN="$(awk '/^toolchain /{print $2}' gateway/go.mod)"
if [ -z "$GO_TOOLCHAIN" ]; then
  echo "error: gateway/go.mod has no toolchain directive to pin against" >&2
  exit 1
fi

generate_go() {
  command -v go >/dev/null || { echo "error: go is not installed" >&2; exit 1; }

  export PATH="$(go env GOPATH)/bin:$PATH"

  # The pin belongs on the plugins, because the toolchain that compiles them
  # changes the code they emit: the same protoc-gen-go-grpc v1.6.1 built under
  # Go 1.27 instead of 1.25 rewrote comments across 154 lines. GOTOOLCHAIN
  # makes Go fetch exactly this release whatever is installed; forcing 'local'
  # is what produced that drift.
  echo "go toolchain : $GO_TOOLCHAIN (from gateway/go.mod) — for the plugins"
  GOTOOLCHAIN="$GO_TOOLCHAIN" go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO}"
  GOTOOLCHAIN="$GO_TOOLCHAIN" go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC}"
  GOTOOLCHAIN="$GO_TOOLCHAIN" go install "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@${GRPC_GATEWAY}"
  GOTOOLCHAIN="$GO_TOOLCHAIN" go install "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@${GRPC_GATEWAY}"

  # buf is the driver, not a generator — it hands a request to the plugins
  # above and writes what they return, so the Go that builds it does not reach
  # the output. Its own version is pinned, which is the part that matters. It
  # also requires a newer Go than the plugins do, so it gets GOTOOLCHAIN=auto:
  # pinning it to the plugin toolchain fails outright with
  # "buf@v1.73.0 requires go >= 1.26.7". CI sidesteps this by downloading a
  # prebuilt binary instead of building one.
  GOTOOLCHAIN=auto go install "github.com/bufbuild/buf/cmd/buf@${BUF_VERSION}"

  echo "buf          : $(buf --version)"
  buf generate

  # The per-service swagger files buf emits are not what anything serves; the
  # gateway's published API doc is the merge of them. It is committed, so
  # leaving the merge out of this script let it rot — apidocs.swagger.json
  # described 22 paths and knew nothing about the ring tier, months after the
  # ring service shipped, because no generation path regenerated it.
  python3 scripts/merge_swagger.py
  echo "regenerated gateway/gen and gateway/openapi"
}

generate_python() {
  command -v python3 >/dev/null || { echo "error: python3 is not installed" >&2; exit 1; }

  local have
  have="$(python3 -c "import importlib.metadata as m; print(m.version('grpcio-tools'))" 2>/dev/null || echo none)"
  if [ "$have" != "$GRPCIO_TOOLS" ]; then
    echo "error: grpcio-tools ${GRPCIO_TOOLS} is required to match the committed stubs (found: ${have})" >&2
    echo "       pip install 'grpcio-tools==${GRPCIO_TOOLS}'" >&2
    exit 1
  fi
  echo "grpcio-tools : $have"

  # sync_python_client lives in the CMake build, so it needs a configured tree.
  local build="${CODEGEN_BUILD_DIR:-build}"
  if [ ! -f "$build/CMakeCache.txt" ]; then
    echo "error: no configured build at '$build'. Configure one first, e.g." >&2
    echo "       cmake -S . -B $build -G Ninja -DPAYLOAD_MANAGER_ENABLE_PYTHON_PROTO=ON" >&2
    echo "       (or set CODEGEN_BUILD_DIR to an existing build)" >&2
    exit 1
  fi
  cmake --build "$build" --target sync_python_client
  echo "regenerated client/python/payload"
}

case "${1:-all}" in
  go)     generate_go ;;
  python) generate_python ;;
  all)    generate_go; generate_python ;;
  *)      echo "usage: $0 [go|python|all]" >&2; exit 2 ;;
esac
