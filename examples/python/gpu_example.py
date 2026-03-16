#!/usr/bin/env python3
"""Allocate a GPU-tier payload, write an incrementing pattern via CUDA, and verify.

This example mirrors the C++ gpu_example.  It requires ``cupy`` to be installed
and a CUDA-capable GPU to be present.  When neither is available the script
prints a message and exits cleanly with code 0 so CI pipelines that lack a GPU
are not broken.

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

import grpc

from payload_manager_client import PayloadClient
from payload.manager.core.v1 import types_pb2


# ---------------------------------------------------------------------------
# Optional cupy import – skip gracefully when CUDA is not available.
# ---------------------------------------------------------------------------
try:
    import cupy as cp  # type: ignore[import]
    _HAS_CUPY = True
except ImportError:
    _HAS_CUPY = False


def _check_cuda_available() -> bool:
    """Return True if at least one CUDA device is accessible via cupy."""
    if not _HAS_CUPY:
        return False
    try:
        return cp.cuda.runtime.getDeviceCount() > 0
    except cp.cuda.runtime.CUDARuntimeError:
        return False


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"

    if not _HAS_CUPY:
        print(
            "cupy is not installed – skipping GPU example.\n"
            "Install it with: pip install cupy-cuda12x  (adjust for your CUDA version)"
        )
        return 0

    if not _check_cuda_available():
        print("No CUDA-capable GPU found – skipping GPU example.")
        return 0

    client = PayloadClient(grpc.insecure_channel(target))

    payload_size = 64

    # Allocate a GPU-tier payload.  The server returns a descriptor whose
    # ``gpu`` field carries a CUDA IPC handle.  The Python client opens that
    # handle via cupy and returns a cupy ndarray as the ``buffer`` field.
    # The ``mmap_obj`` field is None for GPU-tier payloads.
    writable = client.AllocateWritableBuffer(payload_size, types_pb2.TIER_GPU)

    # ``writable.buffer`` is a cupy.ndarray of uint8 pointing into device
    # memory opened from the IPC handle.  Write an incrementing pattern.
    host_src = cp.arange(payload_size, dtype=cp.uint8)
    gpu_buf = writable.buffer  # cupy.ndarray (mutable, GPU device memory)
    gpu_buf[:] = host_src

    payload_id = writable.descriptor.payload_id
    import uuid as _uuid_mod
    payload_uuid = _uuid_mod.UUID(bytes=bytes(payload_id.value))

    # Commit makes the payload visible to readers.
    client.CommitPayload(payload_id)

    # Acquire a read lease.  The client reopens the IPC handle for the
    # committed snapshot and returns another cupy.ndarray.
    readable = client.AcquireReadableBuffer(payload_id)

    gpu_read = readable.buffer  # cupy.ndarray (read via IPC handle)
    host_dst = cp.asnumpy(gpu_read)  # copy GPU → CPU for verification

    print(f"GPU payload UUID={payload_uuid}, size={len(host_dst)} bytes")

    # Verify the incrementing byte pattern.
    mismatches = 0
    for i, byte in enumerate(host_dst):
        expected = i & 0xFF
        if byte != expected:
            print(
                f"mismatch at byte {i}: expected {expected} got {int(byte)}",
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
