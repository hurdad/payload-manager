ARG ARROW_VERSION=25.0.1

FROM ubuntu:26.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /workspace

# Apache Arrow apt repository — provides libarrow-dev / libarrow-cuda-dev at an
# exact pinned version.  Ubuntu universe only carries Arrow 23.0.1 and has no
# CUDA build, so the Apache repo is used for every image to keep one version.
ARG ARROW_VERSION
RUN sed -i 's/^Components: main$/Components: main universe/' /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && wget -q -O /tmp/arrow-apt-source.deb \
         https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-resolute.deb \
    && apt-get install -y -V /tmp/arrow-apt-source.deb \
    && rm -f /tmp/arrow-apt-source.deb \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        pkg-config \
        git \
        protobuf-compiler \
        libprotobuf-dev \
        libgrpc++-dev \
        libgrpc-dev \
        libyaml-cpp-dev \
        libpqxx-dev \
        libspdlog-dev \
        libjemalloc-dev \
        opentelemetry-cpp-dev \
        libcurl4-openssl-dev \
        protobuf-compiler-grpc \
        libssl-dev \
        zlib1g-dev \
        liblz4-dev \
        libzstd-dev \
        libsnappy-dev \
        libbrotli-dev \
        libbz2-dev \
        libre2-dev \
        libutf8proc-dev \
        libxml2-dev \
        libgtest-dev \
        libarrow-dev=${ARROW_VERSION}-1 \
    && rm -rf /var/lib/apt/lists/*

COPY . ./src

# OTEL is compiled in unconditionally; there is no separate -otel image any
# more. It is inert until configured: observability.metrics_enabled and
# .tracing_enabled are plain proto3 bools defaulting to false, and
# InitializeMetrics/InitializeTracing return before constructing an exporter,
# so an unconfigured deployment starts no threads and opens no connections.
# The cost of carrying it is 553 KB of binary and 3.9 MB of image — 1.6% —
# against a separate image, tag, build job and compose variant to keep in step.
RUN cmake -S ./src -B /workspace/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DPAYLOAD_MANAGER_ENABLE_JEMALLOC=ON \
    -DPAYLOAD_MANAGER_ENABLE_OTEL=ON \
    -DBUILD_TESTING=ON \
    -DPAYLOAD_MANAGER_BUILD_SERVICE=ON \
    -DPAYLOAD_MANAGER_BUILD_CLIENT=OFF \
    -DPAYLOAD_MANAGER_BUILD_EXAMPLES=OFF \
    -DPAYLOAD_MANAGER_BUILD_PAYLOADCTL=OFF \
    && cmake --build /workspace/build \
    && ldd /workspace/build/bin/payload-manager | grep -q libjemalloc \
    && echo "jemalloc: linked" \
    && ctest --test-dir /workspace/build --output-on-failure -E payload_manager_integration_api


FROM ubuntu:26.04 AS runtime

LABEL org.opencontainers.image.title="payload-manager" \
      org.opencontainers.image.description="Payload Manager gRPC service with OpenTelemetry"

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Apache Arrow apt repository — provides libarrow-dev / libarrow-cuda-dev at an
# exact pinned version.  Ubuntu universe only carries Arrow 23.0.1 and has no
# CUDA build, so the Apache repo is used for every image to keep one version.
ARG ARROW_VERSION
RUN sed -i 's/^Components: main$/Components: main universe/' /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && wget -q -O /tmp/arrow-apt-source.deb \
         https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-resolute.deb \
    && apt-get install -y -V /tmp/arrow-apt-source.deb \
    && rm -f /tmp/arrow-apt-source.deb \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libprotobuf32t64 \
        libgrpc++1.51t64 \
        libgrpc29t64 \
        libyaml-cpp0.8 \
        libpq5 \
        libspdlog1.15 \
        libjemalloc2 \
        opentelemetry-cpp \
        libcurl4t64 \
        libssl3t64 \
        zlib1g \
        liblz4-1 \
        libzstd1 \
        libsnappy1v5 \
        libbrotli1 \
        libbz2-1.0 \
        libre2-11 \
        libutf8proc3 \
        libxml2-16 \
        libarrow2500=${ARROW_VERSION}-1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /sbin/nologin payload

COPY --from=builder --chown=payload:payload /workspace/build/bin/payload-manager ./payload-manager

# Create the disk-tier root in the image, owned by the user the container runs
# as. Docker gives a named volume the ownership of the image directory it is
# mounted over, and creates that directory root-owned when the image has none
# — which left a non-root payload-manager unable to create its payloads/ tree
# and exiting 2 on every start against a fresh volume.
RUN mkdir -p /var/lib/payload-manager/payloads \
    && chown -R payload:payload /var/lib/payload-manager

EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD bash -c 'exec 3<>/dev/tcp/localhost/50051' || exit 1

USER payload
ENTRYPOINT ["./payload-manager"]
