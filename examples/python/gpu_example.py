#!/usr/bin/env python3
"""Allocate a GPU-tier payload, write an incrementing pattern via Arrow CUDA, and verify.

This example mirrors the C++ gpu_example.  It requires ``pyarrow`` built with
CUDA support (``pyarrow.cuda``) and a CUDA-capable GPU.  When neither is
available the script prints a message and exits cleanly with code 0 so CI
pipelines that lack a GPU are not broken.

``pyarrow.cuda`` is provided by ``libarrow-cuda`` and the ``python3-pyarrow``
package from the Apache Arrow apt repository on Ubuntu.

Usage:
    gpu_example.py [endpoint]

Arguments:
    endpoint  gRPC target (default: localhost:50051)

Example:
    python gpu_example.py
    python gpu_example.py localhost:50051
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))

import uuid as _uuid_mod

import grpc
import pyarrow as pa

from payload_manager_client import PayloadClient
from payload.manager.core.v1 import types_pb2


# ---------------------------------------------------------------------------
# Optional pyarrow.cuda import – skip gracefully when CUDA is not available.
# ---------------------------------------------------------------------------
try:
    import pyarrow.cuda as pac  # type: ignore[import]
    _HAS_ARROW_CUDA = True
except (ImportError, AttributeError):
    _HAS_ARROW_CUDA = False


def _check_cuda_available() -> bool:
    """Return True if at least one CUDA device is accessible."""
    if not _HAS_ARROW_CUDA:
        return False
    try:
        ctx = pac.Context(0)
        return ctx.device_count > 0
    except Exception:
        return False


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"

    if not _HAS_ARROW_CUDA:
        print(
            "pyarrow.cuda is not available – skipping GPU example.\n"
            "Install libarrow-cuda and python3-pyarrow from the Apache Arrow apt repo."
        )
        return 0

    if not _check_cuda_available():
        print("No CUDA-capable GPU found – skipping GPU example.")
        return 0

    client = PayloadClient(grpc.insecure_channel(target))

    payload_size = 64

    # Allocate a GPU-tier payload.  The server returns a descriptor whose
    # ``gpu`` field carries an Arrow-serialized CUDA IPC handle.  The Python
    # client opens that handle via pyarrow.cuda and returns a CudaBuffer as
    # the ``buffer`` field.  ``mmap_obj`` is None for GPU-tier payloads.
    writable = client.AllocateWritableBuffer(payload_size, types_pb2.TIER_GPU)

    # ``writable.buffer`` is a pa.cuda.CudaBuffer pointing into device memory.
    # Build an incrementing host buffer and copy it to the device.
    host_src = pa.py_buffer(bytes(i & 0xFF for i in range(payload_size)))
    cuda_buf = writable.buffer  # pa.cuda.CudaBuffer (mutable, GPU device memory)
    cuda_buf.copy_from_host(host_src)

    payload_id = writable.descriptor.payload_id
    payload_uuid = _uuid_mod.UUID(bytes=bytes(payload_id.value))

    # Commit makes the payload visible to readers.
    client.CommitPayload(payload_id)

    # Acquire a read lease.  The client reopens the IPC handle and returns
    # another CudaBuffer.
    readable = client.AcquireReadableBuffer(payload_id)

    gpu_read = readable.buffer  # pa.cuda.CudaBuffer
    host_dst = gpu_read.copy_to_host().to_pybytes()  # GPU → CPU

    print(f"GPU payload UUID={payload_uuid}, size={len(host_dst)} bytes")

    # Verify the incrementing byte pattern.
    mismatches = 0
    for i, byte in enumerate(host_dst):
        expected = i & 0xFF
        if byte != expected:
            print(
                f"mismatch at byte {i}: expected {expected} got {byte}",
                file=sys.stderr,
            )
            mismatches += 1

    if mismatches == 0:
        print(f"verify: OK ({payload_size} bytes match incrementing sequence)")
    else:
        print(f"verify: FAIL ({mismatches} mismatches)")

    client.Release(readable.lease_id)
    return 0 if mismatches == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
