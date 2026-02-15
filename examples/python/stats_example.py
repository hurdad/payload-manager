#!/usr/bin/env python3

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))

import grpc

from payload_manager_client import PayloadClient
from payload.manager.admin.v1 import stats_pb2


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    client = PayloadClient(grpc.insecure_channel(target))

    stats = client.Stats(stats_pb2.StatsRequest())
    print(f"Payload Manager stats for {target}")
    print(
        "payload counts: "
        f"gpu={stats.payloads_gpu}, ram={stats.payloads_ram}, disk={stats.payloads_disk}"
    )
    print(f"bytes: gpu={stats.bytes_gpu}, ram={stats.bytes_ram}, disk={stats.bytes_disk}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
