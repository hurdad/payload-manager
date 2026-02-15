#!/usr/bin/env python3

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))
import uuid

import grpc

from payload_manager_client import PayloadClient
from payload.manager.catalog.v1 import lineage_pb2
from payload.manager.core.v1 import placement_pb2, policy_pb2
from payload.manager.runtime.v1 import lifecycle_pb2, tiering_pb2


def main() -> int:
    # Endpoint can be overridden to point this walkthrough at any running
    # Payload Manager instance.
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    client = PayloadClient(grpc.insecure_channel(target))

    # Allocate a short-lived RAM payload (TTL 60s) so we can safely exercise
    # admin APIs without leaving long-term test artifacts.
    writable = client.AllocateWritableBuffer(16, placement_pb2.TIER_RAM, ttl_ms=60_000, persist=False)
    writable.mmap_obj[:] = bytes(range(1, 17))

    raw_uuid = bytes(writable.descriptor.id.value)
    payload_uuid = uuid.UUID(bytes=raw_uuid)
    client.CommitPayload(str(payload_uuid))
    client.Resolve(str(payload_uuid))

    # Promotion/spill requests demonstrate tiering controls. BEST_EFFORT means
    # the server may partially succeed depending on current constraints.
    promote_request = tiering_pb2.PromoteRequest(target_tier=placement_pb2.TIER_RAM, policy=policy_pb2.PROMOTION_POLICY_BEST_EFFORT)
    promote_request.id.value = raw_uuid
    client.Promote(promote_request)

    spill_request = tiering_pb2.SpillRequest(policy=tiering_pb2.SPILL_POLICY_BEST_EFFORT, wait_for_leases=True)
    spill_request.ids.add().value = raw_uuid
    client.Spill(spill_request)

    # Record a synthetic lineage edge to show how transformation provenance can
    # be attached to payload IDs.
    add_lineage_request = lineage_pb2.AddLineageRequest()
    add_lineage_request.child.value = raw_uuid
    edge = add_lineage_request.parents.add()
    edge.parent.value = raw_uuid
    edge.operation = "identity"
    edge.role = "demo"
    edge.parameters = "{}"
    client.AddLineage(add_lineage_request)

    get_lineage_request = lineage_pb2.GetLineageRequest(upstream=True, max_depth=1)
    get_lineage_request.id.value = raw_uuid
    lineage = client.GetLineage(get_lineage_request)

    # Force-delete keeps the example idempotent and avoids leaving stale
    # payloads behind after the demo completes.
    delete_request = lifecycle_pb2.DeleteRequest(force=True)
    delete_request.id.value = raw_uuid
    client.Delete(delete_request)

    print(f"Catalog/Admin API calls completed for payload {payload_uuid} (lineage edges returned={len(lineage.edges)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
