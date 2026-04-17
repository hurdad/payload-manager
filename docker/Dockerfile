FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /workspace

RUN sed -i 's/^Components: main$/Components: main universe/' /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get update \
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
        libsqlite3-dev \
        libspdlog-dev \
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
    && rm -rf /var/lib/apt/lists/*

COPY . ./src

RUN cmake -S ./src -B /workspace/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DPAYLOAD_MANAGER_ENABLE_OTEL=OFF \
    -DBUILD_TESTING=ON \
    -DPAYLOAD_MANAGER_BUILD_SERVICE=ON \
    -DPAYLOAD_MANAGER_BUILD_CLIENT=OFF \
    -DPAYLOAD_MANAGER_BUILD_EXAMPLES=OFF \
    -DPAYLOAD_MANAGER_BUILD_PAYLOADCTL=OFF \
    && cmake --build /workspace/build \
    && ctest --test-dir /workspace/build --output-on-failure -E payload_manager_integration_api


FROM ubuntu:24.04 AS runtime

LABEL org.opencontainers.image.title="payload-manager" \
      org.opencontainers.image.description="Payload Manager gRPC service"

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN sed -i 's/^Components: main$/Components: main universe/' /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        sqlite3 \
        libprotobuf32t64 \
        libgrpc++1.51t64 \
        libgrpc29t64 \
        libyaml-cpp0.8 \
        libpqxx-7.8t64 \
        libsqlite3-0 \
        libspdlog1.12 \
        libcurl4t64 \
        libssl3 \
        zlib1g \
        liblz4-1 \
        libzstd1 \
        libsnappy1v5 \
        libbrotli1 \
        libbz2-1.0 \
        libre2-10 \
        libutf8proc3 \
        libxml2 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /sbin/nologin payload

COPY --from=builder --chown=payload:payload /workspace/build/bin/payload-manager ./payload-manager

EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD bash -c 'exec 3<>/dev/tcp/localhost/50051' || exit 1

USER payload
ENTRYPOINT ["./payload-manager"]
