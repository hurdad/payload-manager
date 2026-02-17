# Payload Manager Architecture Overview

## 1. System purpose

Payload Manager provides a control plane for binary payload lifecycle and placement while keeping payload bytes on their native storage path. It coordinates metadata, placement decisions, and access guarantees (leases) so workers can read/write directly from storage tiers.

## 2. Major architectural layers

### API and runtime boundary

- **Entrypoints:** `cmd/payload-manager`, `cmd/payloadctl`.
- **Runtime:** server bootstrap and process wiring in `internal/runtime`.
- **Transport:** gRPC services under `internal/grpc` implementing API contracts from `api/payload/manager/services/v1`.

### Service layer

Core domain services live in `internal/service`:

- `data_service`: payload lifecycle operations and descriptor/lease-facing flows.
- `catalog_service`: metadata/catalog retrieval plus tiering advisories (`Prefetch`, `Pin`, `Unpin`).
- `admin_service`: administrative and operational actions.
- `stream_service`: stream-oriented APIs and consumer offset behavior.

These services operate on domain abstractions and call into core managers and repository interfaces.

### Core orchestration layer

Key responsibilities live in `internal/core`, `internal/lease`, `internal/tiering`, `internal/spill`, and `internal/metadata`:

- Payload state transitions and commit semantics.
- Placement and re-placement decisions.
- Lease issuance, tracking, and release.
- Spill scheduling and worker execution.
- Metadata caching and lookup helpers.

### Persistence abstraction

`internal/db/api` defines backend-agnostic repository + transaction contracts.

Implementations:

- `internal/db/memory`: in-process testing backend.
- `internal/db/sqlite`: embedded edge deployment backend.
- `internal/db/postgres`: multi-node catalog backend.

Migrations are maintained separately for SQLite and PostgreSQL under `internal/db/migrations`.

### Storage tier abstraction

`internal/storage` provides tier implementations and factory wiring:

- RAM store (`internal/storage/ram`)
- GPU store (`internal/storage/gpu`)
- Disk store (`internal/storage/disk`)
- Object store (`internal/storage/object`)

A common interface allows placement/tiering logic to stay backend-agnostic.

## 3. Cross-cutting concerns

- **Configuration:** protobuf-backed config loading in `internal/config`.
- **Observability:** tracing and metrics in `internal/observability`.
- **Utility primitives:** time and UUID helpers in `internal/util`.
- **Lineage:** graph model and traversal in `internal/lineage`.

## 4. Control flow (high-level)

1. Client calls gRPC API.
2. gRPC server validates and maps request to service layer.
3. Service executes lifecycle/placement/lease logic via core modules.
4. Service persists and reads state via repository interface.
5. Service returns descriptor + lease metadata to caller.
6. Caller accesses payload bytes directly from resolved storage tier.

## 5. Non-goals (explicit)

- Payload Manager is not intended to proxy large payload byte streams through gRPC.
- Payload Manager is not intended to collapse all storage tiers into a single physical medium.
- Payload Manager is not intended to hide durability/latency tradeoffs; policy controls are expected.
