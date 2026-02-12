from setuptools import setup


setup(
    name="payload-manager-client",
    version="0.1.0",
    description="Python gRPC + Arrow client for payload-manager",
    py_modules=["payload_client"],
    install_requires=[
        "grpcio>=1.60",
        "protobuf>=4.25",
        "pyarrow>=14",
    ],
)
