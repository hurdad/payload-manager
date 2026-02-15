#!/usr/bin/env python3

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))
import uuid

import grpc

from payload_manager_client import PayloadClient
from payload.manager.core.v1 import placement_pb2


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    client = PayloadClient(grpc.insecure_channel(target))

    payload_size = 64
    writable = client.AllocateWritableBuffer(payload_size, placement_pb2.TIER_RAM)
    writable.mmap_obj[:] = bytes((i & 0xFF) for i in range(payload_size))

    payload_uuid = uuid.UUID(bytes=bytes(writable.descriptor.id.value))
    client.CommitPayload(str(payload_uuid))

    readable = client.AcquireReadableBuffer(str(payload_uuid))
    print(f"Committed and acquired payload UUID={payload_uuid}, size={readable.buffer.size} bytes")

    preview_len = min(8, readable.buffer.size)
    preview = " ".join(str(x) for x in readable.buffer.to_pybytes()[:preview_len])
    print(f"First {preview_len} bytes: {preview}")

    client.Release(readable.lease_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
