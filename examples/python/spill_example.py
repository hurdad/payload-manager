#!/usr/bin/env python3
"""Spill a RAM-tier payload to disk, promote it back to RAM, then verify data.

This example mirrors the C++ spill_example.  It expects the target payload to
contain an incrementing byte sequence (as written by allocate_example or
round_trip_example) and will report any mismatches found after the spill /
promote round trip.

Usage:
    spill_example.py <uuid> [endpoint]

Arguments:
    uuid      UUID of the committed payload to spill (required)
    endpoint  gRPC target (default: localhost:50051)

Example:
    python spill_example.py 550e8400-e29b-41d4-a716-446655440000
    python spill_example.py 550e8400-e29b-41d4-a716-446655440000 localhost:50051
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))

import grpc

from payload_manager_client import PayloadClient, payload_id_from_uuid
from payload.manager.core.v1 import policy_pb2, types_pb2
from payload.manager.runtime.v1 import tiering_pb2


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: spill_example.py <uuid> [endpoint]", file=sys.stderr)
        return 1

    payload_uuid_str = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else "localhost:50051"

    client = PayloadClient(grpc.insecure_channel(target))

    try:
        payload_id = payload_id_from_uuid(payload_uuid_str)
    except ValueError as exc:
        print(f"Invalid UUID: {exc}", file=sys.stderr)
        return 1

    # Spill the payload from RAM to disk.  fsync=True ensures the data reaches
    # persistent storage before the call returns, matching the C++ example.
    spill_request = tiering_pb2.SpillRequest(fsync=True)
    spill_request.ids.add().CopyFrom(payload_id)
    spill_response = client.Spill(spill_request)

    if len(spill_response.results) < 1 or not spill_response.results[0].ok:
        msg = spill_response.results[0].error_message if spill_response.results else "no results"
        print(f"Spill rejected: {msg}", file=sys.stderr)
        return 1

    print(f"spill: OK (UUID={payload_uuid_str} is now on disk)")

    # Promote the payload back to RAM so we can acquire a readable buffer via
    # the standard mmap path.
    promote_request = tiering_pb2.PromoteRequest(
        target_tier=types_pb2.TIER_RAM,
        policy=policy_pb2.PROMOTION_POLICY_BEST_EFFORT,
    )
    promote_request.id.CopyFrom(payload_id)
    client.Promote(promote_request)
    print("promote: OK (back in RAM)")

    # Acquire a read lease and open the mmap-backed buffer.
    readable = client.AcquireReadableBuffer(payload_id)
    data = readable.buffer.to_pybytes()
    size = len(data)
    print(f"read: {size} bytes")

    # Verify the incrementing byte pattern that allocate_example writes.
    mismatches = 0
    for i, byte in enumerate(data):
        expected = i & 0xFF
        if byte != expected:
            print(
                f"mismatch at byte {i}: expected {expected} got {byte}",
                file=sys.stderr,
            )
            mismatches += 1

    if mismatches == 0:
        print(f"verify: OK ({size} bytes match incrementing sequence)")
    else:
        print(f"verify: FAIL ({mismatches} mismatches)")

    client.Release(readable.lease_id)
    return 0 if mismatches == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
