from setuptools import find_namespace_packages, setup


setup(
    name="payload-manager-client",
    version="0.1.0",
    description="Python gRPC + Arrow client for payload-manager",
    # payload_manager_client.py lives directly in client/python/
    py_modules=["payload_manager_client"],
    # Generated protobuf/gRPC stubs under client/python/payload/
    packages=find_namespace_packages(include=["payload.*"]),
    # Floors are dictated by the generated stubs, not chosen: the *_pb2_grpc
    # modules raise at import when grpcio is older than the grpcio-tools that
    # produced them, and the *_pb2 modules call
    # _runtime_version.ValidateProtobufRuntimeVersion against the protobuf
    # gencode version. Regenerating with an older toolchain lowers both; the
    # two numbers must move together with client/python/payload/.
    install_requires=[
        "grpcio>=1.75.1",
        "protobuf>=6.31.1",
        "pyarrow>=14",
    ],
    extras_require={
        "otel": [
            "opentelemetry-api>=1.20",
        ],
        # Explicit opt-in for CUDA-capable client environments. The base
        # install remains CPU-safe; this extra conveys install intent.
        "cuda": [
            "pyarrow>=14",
        ],
    },
)
