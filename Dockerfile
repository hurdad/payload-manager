FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        lsb-release \
        wget \
    && wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get install -y --no-install-recommends \
    ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        libarrow-dev \
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
    && rm -rf /var/lib/apt/lists/*

COPY . ./src

RUN cmake -S ./src -B /workspace/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DPAYLOAD_MANAGER_ENABLE_OTEL=ON \
    -DBUILD_TESTING=ON \
    -DPAYLOAD_MANAGER_BUILD_SERVICE=ON \
    -DPAYLOAD_MANAGER_BUILD_CLIENT=OFF \
    -DPAYLOAD_MANAGER_BUILD_EXAMPLES=OFF \
    -DPAYLOAD_MANAGER_BUILD_PAYLOADCTL=OFF \
    && cmake --build /workspace/build \
    && ctest --test-dir /workspace/build --output-on-failure \
    && cmake --build /workspace/build --target payload-manager


FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        lsb-release \
        wget \
    && wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get install -y --no-install-recommends \
    ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libarrow2300 \
        libprotobuf32t64 \
        libgrpc++1.51t64 \
        libgrpc29t64 \
        libyaml-cpp0.8 \
        libpqxx-7.8t64 \
        libsqlite3-0 \
        libspdlog1.12 \
        libcurl4t64 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/build/bin/payload-manager ./payload-manager
