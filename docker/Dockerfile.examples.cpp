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
        libarrow-dev=${ARROW_VERSION}-1 \
    && rm -rf /var/lib/apt/lists/*

COPY . ./src

RUN cmake -S ./src -B /workspace/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DPAYLOAD_MANAGER_ENABLE_OTEL=OFF \
    -DBUILD_TESTING=OFF \
    -DPAYLOAD_MANAGER_BUILD_SERVICE=OFF \
    -DPAYLOAD_MANAGER_BUILD_CLIENT=ON \
    -DPAYLOAD_MANAGER_BUILD_EXAMPLES=ON \
    -DPAYLOAD_MANAGER_BUILD_PAYLOADCTL=OFF \
    -DBUILD_SHARED_LIBS=ON \
    && cmake --build /workspace/build \
    && cmake --install /workspace/build --prefix /workspace/install


FROM ubuntu:26.04 AS runtime

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
        libprotobuf32t64 \
        libgrpc++1.51t64 \
        libgrpc29t64 \
        libspdlog1.15 \
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
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/install/lib/ /usr/local/lib/
COPY --from=builder /workspace/build/examples/cpp/payload_manager_example_* ./
COPY --from=builder /workspace/src/examples/run_examples.sh ./
RUN ldconfig && chmod +x ./run_examples.sh

HEALTHCHECK NONE

ENTRYPOINT ["./run_examples.sh"]
