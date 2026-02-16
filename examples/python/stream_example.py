#!/usr/bin/env python3

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "client/python"))
import uuid

import grpc

from payload_manager_client import PayloadClient
from payload.manager.core.v1 import placement_pb2
from payload.manager.runtime.v1 import stream_pb2


def make_stream_id() -> stream_pb2.StreamID:
    # Stream IDs are a stable tuple (namespace, name). Using explicit values
    # here makes reruns deterministic and easy to inspect server-side.
    stream = stream_pb2.StreamID()
    stream.name = "python-client-demo"
    stream.namespace = "examples"
    return stream


def main() -> int:
    # Endpoint override is useful when the examples are run from CI or from a
    # developer machine targeting a non-default port.
    target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
    client = PayloadClient(grpc.insecure_channel(target))

    # Prepare a tiny payload and commit it so stream entries can reference a
    # durable payload ID instead of embedding raw bytes.
    writable = client.AllocateWritableBuffer(8, placement_pb2.TIER_RAM)
    writable.mmap_obj[:] = bytes(range(10, 18))

    payload_id = writable.descriptor.id
    payload_uuid = uuid.UUID(bytes=bytes(payload_id.value))
    client.CommitPayload(payload_id)

    stream = make_stream_id()

    # Create the stream before appending entries; retention_max_entries sets a
    # cap for this demo so test streams do not grow unbounded.
    create_request = stream_pb2.CreateStreamRequest(retention_max_entries=1024)
    create_request.stream.CopyFrom(stream)
    client.CreateStream(create_request)

    # Append one entry referencing the committed payload. Tags provide light
    # metadata to aid filtering/debugging downstream.
    append_request = stream_pb2.AppendRequest()
    append_request.stream.CopyFrom(stream)
    item = append_request.items.add()
    item.payload_id.CopyFrom(payload_id)
    item.duration_ns = 1_000_000
    item.tags["source"] = "examples/python/stream_example"
    append_response = client.Append(append_request)

    # Read performs a bounded fetch by offset range.
    read_request = stream_pb2.ReadRequest(start_offset=append_response.first_offset, max_entries=10)
    read_request.stream.CopyFrom(stream)
    read_response = client.Read(read_request)

    # Subscribe demonstrates the streaming RPC variant. We read a single item
    # then cancel intentionally to keep the example finite.
    subscribe_request = stream_pb2.SubscribeRequest(offset=append_response.first_offset, max_inflight=1)
    subscribe_request.stream.CopyFrom(stream)
    subscription = client.Subscribe(subscribe_request)
    got_entry = False
    try:
        next(subscription)
        got_entry = True
    except grpc.RpcError as exc:
        if exc.code() != grpc.StatusCode.CANCELLED:
            raise
    except StopIteration:
        pass
    finally:
        subscription.cancel()

    # Consumer-group commits store progress checkpoints for replayable streams.
    commit_request = stream_pb2.CommitRequest(consumer_group="example-group", offset=append_response.last_offset)
    commit_request.stream.CopyFrom(stream)
    client.Commit(commit_request)

    # GetCommitted reads the offset checkpoint written above.
    committed_request = stream_pb2.GetCommittedRequest(consumer_group="example-group")
    committed_request.stream.CopyFrom(stream)
    committed_response = client.GetCommitted(committed_request)

    # GetRange demonstrates server-side range inspection for stream offsets.
    range_request = stream_pb2.GetRangeRequest(start_offset=append_response.first_offset, end_offset=append_response.last_offset)
    range_request.stream.CopyFrom(stream)
    range_response = client.GetRange(range_request)

    # Delete the demo stream so repeated runs are idempotent.
    delete_request = stream_pb2.DeleteStreamRequest()
    delete_request.stream.CopyFrom(stream)
    client.DeleteStream(delete_request)

    print(
        f"Stream API calls completed for stream {stream.namespace}/{stream.name}, "
        f"read entries={len(read_response.entries)}, range entries={len(range_response.entries)}, "
        f"subscribe_received={'yes' if got_entry else 'no'}, committed_offset={committed_response.offset}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
