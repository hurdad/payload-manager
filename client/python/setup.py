from setuptools import find_namespace_packages, setup


setup(
    name="payload-manager-client",
    version="0.1.0",
    description="Python gRPC + Arrow client for payload-manager",
    # payload_manager_client.py lives directly in client/python/
    py_modules=["payload_manager_client"],
    # Generated protobuf/gRPC stubs under client/python/payload/
    packages=find_namespace_packages(include=["payload.*"]),
    install_requires=[
        "grpcio>=1.60",
        "protobuf>=4.25",
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
