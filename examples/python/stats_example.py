#!/usr/bin/env python3

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))

from payload_manager_client import PayloadClient, make_channel
from payload.manager.admin.v1 import stats_pb2


def main() -> int:
    # Optional endpoint argument keeps this example flexible across local and
    # remote deployments.
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    client = PayloadClient(make_channel(target))

    # Stats is a lightweight admin call that summarizes payload counts and
    # allocated bytes per storage tier.
    stats = client.Stats(stats_pb2.StatsRequest())
    print(f"Payload Manager stats for {target}")
    print(
        "payload counts: "
        f"gpu={stats.payloads_gpu}, ram={stats.payloads_ram}, disk_hot={stats.payloads_disk_hot}"
    )
    print(f"bytes: gpu={stats.bytes_gpu}, ram={stats.bytes_ram}, disk_hot={stats.bytes_disk_hot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
