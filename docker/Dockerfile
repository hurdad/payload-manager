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
        libpq-dev \
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
        libarrow-dev=${ARROW_VERSION}-1 \
    && rm -rf /var/lib/apt/lists/*

# Ubuntu 26.04's libpqxx 7.10.0-2build1 double-frees in its library finalizer,
# so ANY binary linking it aborts at exit — reproduced with a two-line program.
# 24.04's 7.8 is clean, and so is the same upstream 7.10.0 built from source,
# so this is Ubuntu's build, not upstream. Nothing fixed exists in
# resolute-updates/-proposed/-security. Build it ourselves until one does.
# Placed above the source COPY so it stays cached across code changes.
ARG LIBPQXX_VERSION=7.10.0
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:/usr/local/lib/x86_64-linux-gnu/pkgconfig
RUN git clone --depth 1 --branch ${LIBPQXX_VERSION} \
        https://github.com/jtv/libpqxx.git /tmp/libpqxx \
    && cmake -S /tmp/libpqxx -B /tmp/libpqxx-build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=ON \
        -DSKIP_BUILD_TEST=ON \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
    && cmake --build /tmp/libpqxx-build \
    && cmake --install /tmp/libpqxx-build \
    && ldconfig \
    && rm -rf /tmp/libpqxx /tmp/libpqxx-build

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


FROM ubuntu:26.04 AS runtime

LABEL org.opencontainers.image.title="payload-manager" \
      org.opencontainers.image.description="Payload Manager gRPC service"

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

# libpqxx is built from source in the builder (see the note there), so the
# runtime takes it from there rather than from apt. Only the soname is
# needed — the unversioned libpqxx.so is a dev symlink, and COPY would
# dereference it into a second full copy of the library.
COPY --from=builder /usr/local/lib/libpqxx-*.so /usr/local/lib/
RUN ldconfig

COPY --from=builder --chown=payload:payload /workspace/build/bin/payload-manager ./payload-manager

EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD bash -c 'exec 3<>/dev/tcp/localhost/50051' || exit 1

USER payload
ENTRYPOINT ["./payload-manager"]
