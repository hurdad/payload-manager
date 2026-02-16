#!/usr/bin/env python3

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))
import uuid

import grpc

from payload_manager_client import PayloadClient
from payload.manager.catalog.v1 import metadata_pb2
from payload.manager.core.v1 import placement_pb2


def main() -> int:
    # Accept an optional target for convenience when running against staged
    # environments.
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    client = PayloadClient(grpc.insecure_channel(target))

    # Allocate and seed a payload that we will later annotate via catalog
    # metadata APIs.
    writable = client.AllocateWritableBuffer(8, placement_pb2.TIER_RAM)
    writable.mmap_obj[0] = 42

    payload_id = writable.descriptor.id
    payload_uuid = uuid.UUID(bytes=bytes(payload_id.value))
    client.CommitPayload(payload_id)

    # UpdatePayloadMetadata writes the canonical metadata document for this
    # payload ID. REPLACE mode swaps the full document in one operation.
    update_request = metadata_pb2.UpdatePayloadMetadataRequest(
        mode=metadata_pb2.METADATA_UPDATE_MODE_REPLACE,
        actor="examples/python/metadata_example",
        reason="demonstrate metadata update flow",
    )
    update_request.id.CopyFrom(payload_id)
    update_request.metadata.id.CopyFrom(payload_id)
    update_request.metadata.schema = "example.payload.v1"
    update_request.metadata.data = '{"producer":"metadata_example","notes":"hello payload manager"}'
    client.UpdatePayloadMetadata(update_request)

    # AppendPayloadMetadataEvent stores an immutable event entry so consumers
    # can track metadata evolution over time.
    event_request = metadata_pb2.AppendPayloadMetadataEventRequest(
        source="examples/python/metadata_example",
        version="v1",
    )
    event_request.id.CopyFrom(payload_id)
    event_request.metadata.id.CopyFrom(payload_id)
    event_request.metadata.schema = "example.payload.v1"
    event_request.metadata.data = '{"event":"metadata_updated","component":"metadata_example"}'
    client.AppendPayloadMetadataEvent(event_request)

    print(f"Metadata updated for payload {payload_uuid}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
