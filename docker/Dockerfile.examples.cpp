FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /workspace

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
        libsqlite3-dev \
        libspdlog-dev \
        libcurl4-openssl-dev \
        protobuf-compiler-grpc \
        python3 \
        python3-pip \
        python3-grpcio \
        python3-grpc-tools \
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
    -DBUILD_TESTING=OFF \
    -DPAYLOAD_MANAGER_BUILD_SERVICE=OFF \
    -DPAYLOAD_MANAGER_BUILD_CLIENT=ON \
    -DPAYLOAD_MANAGER_BUILD_EXAMPLES=ON \
    -DPAYLOAD_MANAGER_BUILD_PAYLOADCTL=OFF \
    && cmake --build /workspace/build


FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libprotobuf32t64 \
        libgrpc++1.51t64 \
        libgrpc29t64 \
        libspdlog1.12 \
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
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/build/examples/cpp/payload_manager_example_* ./
COPY --from=builder /workspace/src/examples/run_examples.sh ./
RUN chmod +x ./run_examples.sh

HEALTHCHECK NONE

ENTRYPOINT ["./run_examples.sh"]
