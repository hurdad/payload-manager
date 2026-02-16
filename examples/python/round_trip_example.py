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
    # Allow overriding the gRPC endpoint from the command line so this script can
    # run against local, containerized, or remote Payload Manager deployments.
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    client = PayloadClient(grpc.insecure_channel(target))

    payload_size = 64
    # Allocate a writable RAM-tier buffer. The server returns a descriptor
    # (including UUID) plus an mmap-backed byte region we can fill directly.
    writable = client.AllocateWritableBuffer(payload_size, placement_pb2.TIER_RAM)
    writable.mmap_obj[:] = bytes((i & 0xFF) for i in range(payload_size))

    # Use the canonical PayloadID protobuf returned by AllocatePayload and
    # derive a printable UUID only for logging.
    payload_id = writable.descriptor.id
    payload_uuid = uuid.UUID(bytes=bytes(payload_id.value))
    # Commit makes the payload visible/immutable for readers.
    client.CommitPayload(payload_id)

    # AcquireReadableBuffer returns a lease-scoped view of committed data.
    readable = client.AcquireReadableBuffer(payload_id)
    print(f"Committed and acquired payload UUID={payload_uuid}, size={readable.buffer.size} bytes")

    preview_len = min(8, readable.buffer.size)
    preview = " ".join(str(x) for x in readable.buffer.to_pybytes()[:preview_len])
    print(f"First {preview_len} bytes: {preview}")

    # Always release leases once done to avoid pinning payload placement.
    client.Release(readable.lease_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
