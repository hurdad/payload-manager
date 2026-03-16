#!/usr/bin/env python3
"""List all payloads held by the Payload Manager, with optional tier filtering.

Usage:
    list_example.py [endpoint] [tier]

Arguments:
    endpoint  gRPC target (default: localhost:50051)
    tier      One of: ram, disk, gpu, all (default: all)

Example:
    python list_example.py localhost:50051 ram
"""

import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))

import grpc

from payload_manager_client import PayloadClient
from payload.manager.core.v1 import types_pb2
from payload.manager.runtime.v1 import lifecycle_pb2


_TIER_NAMES = {
    types_pb2.TIER_GPU: "gpu",
    types_pb2.TIER_RAM: "ram",
    types_pb2.TIER_DISK: "disk",
}

_STATE_NAMES = {
    types_pb2.PAYLOAD_STATE_ALLOCATED: "allocated",
    types_pb2.PAYLOAD_STATE_ACTIVE: "active",
    types_pb2.PAYLOAD_STATE_SPILLING: "spilling",
    types_pb2.PAYLOAD_STATE_DURABLE: "durable",
    types_pb2.PAYLOAD_STATE_EVICTING: "evicting",
    types_pb2.PAYLOAD_STATE_DELETING: "deleting",
}


def _tier_name(tier: int) -> str:
    return _TIER_NAMES.get(tier, "?")


def _state_name(state: int) -> str:
    return _STATE_NAMES.get(state, "?")


def _uuid_hex(raw_bytes: bytes) -> str:
    """Convert 16-byte UUID to the canonical 8-4-4-4-12 hex string."""
    h = raw_bytes.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    tier_arg = sys.argv[2].lower() if len(sys.argv) > 2 else "all"

    tier_filter = 0  # TIER_UNSPECIFIED → return all tiers
    if tier_arg == "ram":
        tier_filter = types_pb2.TIER_RAM
    elif tier_arg == "disk":
        tier_filter = types_pb2.TIER_DISK
    elif tier_arg == "gpu":
        tier_filter = types_pb2.TIER_GPU
    elif tier_arg not in ("all", ""):
        print(f"Unknown tier filter '{tier_arg}'; expected ram, disk, gpu, or all", file=sys.stderr)
        return 1

    client = PayloadClient(grpc.insecure_channel(target))

    # ListAllPayloads is a convenience wrapper around ListPayloads that builds
    # the request automatically. Pass tier_filter=0 (TIER_UNSPECIFIED) to
    # retrieve payloads across all storage tiers.
    response = client.ListAllPayloads(tier_filter=tier_filter)

    now_ms = int(time.time() * 1000)

    # Print a table matching the C++ list_example output format.
    header = f"{'UUID':<38}{'TIER':<6}{'STATE':<12}{'SIZE':<14}{'AGE(s)':<10}{'TTL(s)':<9}{'LEASES':<8}"
    print(header)

    for p in response.payloads:
        uuid_str = _uuid_hex(bytes(p.id.value))

        if p.created_at_ms > 0:
            age_s = str((now_ms - p.created_at_ms) // 1000)
        else:
            age_s = "?"

        if p.expires_at_ms > 0:
            ttl_s = str((p.expires_at_ms - now_ms) // 1000)
        else:
            ttl_s = "-"

        row = (
            f"{uuid_str:<38}"
            f"{_tier_name(p.tier):<6}"
            f"{_state_name(p.state):<12}"
            f"{p.size_bytes:<14}"
            f"{age_s:<10}"
            f"{ttl_s:<9}"
            f"{p.active_leases:<8}"
        )
        print(row)

    print(f"{len(response.payloads)} payload(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
