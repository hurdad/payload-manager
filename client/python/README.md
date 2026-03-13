# Python client (`client/python`)

This folder contains the Python `PayloadClient` and generated protobuf/gRPC stubs.
The client mirrors the C++ API surface and provides convenience helpers for mapping
payload descriptors to Arrow buffers.

## Install

From the repository root:

```bash
pip install -e client/python
```

Optional extras:

- `pip install -e 'client/python[otel]'` to enable OpenTelemetry trace-context propagation.

## Basic usage

```python
import grpc
import pyarrow as pa

from payload_manager_client import PayloadClient

channel = grpc.insecure_channel("localhost:50051")
client = PayloadClient(channel)

# 1) Allocate writable buffer
writable = client.AllocateWritableBuffer(size_bytes=1024)
view = memoryview(writable.buffer)
view[:4] = b"PMGR"

# 2) Commit payload
client.CommitPayload(writable.descriptor.id)

# 3) Acquire readable buffer
readable = client.AcquireReadableBuffer(writable.descriptor.id)
print(bytes(memoryview(readable.buffer)[:4]))

# 4) Release lease
client.Release(readable.lease_id)

# 5) Close mappings when done
readable.mmap_obj.close()
writable.mmap_obj.close()
```

## Running examples

Examples are in `examples/python/`:

- `round_trip_example.py`
- `metadata_example.py`
- `catalog_admin_example.py`
- `stats_example.py`
- `stream_example.py`
