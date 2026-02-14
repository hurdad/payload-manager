# Payload Manager API

This directory contains the **public network contract** for the Payload Manager.

It defines:

* the storage lifecycle protocol
* placement and leasing guarantees
* metadata catalog interfaces
* administrative inspection APIs

This API is versioned and backward-compatibility matters.  
Anything outside `/api` is considered internal and may change freely.

---

## Design Goals

The API is designed as a **storage runtime protocol**, not a file service.

Key principles:

1. Payload bytes never travel over gRPC
2. Payloads are immutable after commit
3. Readers must acquire a lease before accessing data
4. Placement may change at any time without a lease
5. Metadata is opaque to the storage engine

---

## Conceptual Model

There are three distinct layers:

| Layer | Responsibility |
|------|------|
Data Plane | Safe reading of payloads |
Catalog Plane | Lifecycle & metadata |
Admin Plane | Node inspection |

---

## Services

### PayloadDataService
High-frequency runtime operations used by compute workers.

Safe reading workflow:

```
ResolveSnapshot → advisory only
AcquireReadLease → stable location
read bytes
ReleaseLease
```

This service must be extremely fast and does not perform persistence operations.

---

### PayloadCatalogService
Persistent state modifications.

Responsible for:

* allocating payload IDs
* committing completed writes
* promotion and spilling
* deletion
* lineage tracking
* metadata storage

This is the authoritative state controller.

---

### PayloadAdminService
Operational inspection only.

Provides instantaneous node state and capacity information.

This is not a monitoring system — metrics belong in OpenTelemetry.

---

## Core Guarantees

### Immutability
After `CommitPayload`, payload bytes never change.

Any transformation produces a new `PayloadID`.

---

### Snapshot vs Lease

```
ResolveSnapshot → location may immediately change
AcquireReadLease → location guaranteed stable
```

Never read from a snapshot descriptor.

---

### Lease Semantics

A read lease guarantees:

* placement stability
* immutable bytes
* minimum requested tier

After lease expiration the payload may move instantly.

---

### Blocking Operations

Operations using `BLOCKING` policies return only when:

* the requested durability/tier condition is satisfied, or
* the operation definitively fails

---

### Delete Behavior

`force = true` immediately invalidates all active leases.

Clients must treat previously acquired descriptors as unusable.

---

## File Layout

```
api/payload/manager/

core/v1/
  id.proto
  placement.proto
  policy.proto
  types.proto

runtime/v1/
  stream.proto
  lease.proto
  lifecycle.proto
  tiering.proto

catalog/v1/
  archive_metadata.proto
  catalog.proto
  lineage.proto
  metadata.proto

admin/v1/
  stats.proto

services/v1/
  payload_admin_service.proto
  payload_catalog_service.proto
  payload_data_service.proto
  payload_stream_service.proto
```

---

## C++ Usage

Link only schema (no networking):

```
target_link_libraries(my_tool PRIVATE payload_manager::proto)
```

Link full gRPC server/client:

```
target_link_libraries(my_server PRIVATE payload_manager::grpc)
```

---

## Versioning Policy

* `/v1` is backward compatible
* fields may be added but not removed
* semantic guarantees must not change
* internal config is not part of API

---

## What This API Is Not

This is not:

* a file transfer protocol
* an object storage API
* a message queue

It is a **runtime memory/storage coordination protocol** for high-performance data pipelines.

---

## Future Compatibility

The API is designed to support:

* multi-tier memory (GPU/RAM/Disk/Object)
* distributed execution engines
* external metadata indexers
* alternative transports (REST / IPC)

Without breaking clients.

---

End of document.
