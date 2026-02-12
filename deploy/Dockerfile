FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /workspace/payload-manager

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        libprotobuf-dev \
        libgrpc++-dev \
        libgrpc-dev \
        protobuf-compiler-grpc \
        python3 \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cmake -S . -B build -G Ninja \
    && cmake --build build --target payload_manager_service


FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libprotobuf32t64 \
        libgrpc++1.51t64 \
        libgrpc29t64 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/payload-manager/build/service/payload_manager_service ./payload_manager_service

EXPOSE 50051
CMD ["./payload_manager_service", "0.0.0.0:50051"]
