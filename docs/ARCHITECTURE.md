# Payload Manager Architecture Overview

## 1. System purpose

Payload Manager provides a control plane for binary payload lifecycle and placement while keeping payload bytes on their native storage path. It coordinates metadata, placement decisions, and access guarantees (leases) so workers can read/write directly from storage tiers.

## 2. Major architectural layers

### Gateway (REST + UI)

`gateway/` is a standalone Go binary that sits in front of the gRPC server:

- Translates HTTP/JSON requests to gRPC using [gRPC-Gateway v2](https://github.com/grpc-ecosystem/grpc-gateway).
- Serves a compiled Svelte single-page application embedded directly in the binary via `embed.FS`.
- Exposes a merged OpenAPI 2.0 spec at `gateway/openapi/apidocs.swagger.json`.
- Provides a `GET /v1/payloads/{id}/download` endpoint that spills RAM/GPU payloads to disk on demand and streams the file bytes back to the client.
- HTTP annotations (`google.api.http`) are defined inline in the proto files under `api/payload/manager/services/v1/`.
- Generated Go stubs live in `gateway/gen/go/`; regenerate with `make generate` (requires `buf` and local `protoc-gen-*` plugins).

The gateway is stateless and can be restarted independently of the payload-manager. It requires read-only access to the disk payload storage path (`DISK_ROOT_PATH`) to serve downloads.

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

Key responsibilities live in `internal/core`, `internal/lease`, `internal/tiering`, `internal/spill`, `internal/expiration`, and `internal/metadata`:

- Payload state transitions and commit semantics.
- Placement and re-placement decisions.
- Lease issuance, tracking, and release.
- Spill scheduling and worker execution.
- TTL-based expiration via background worker.
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

`TIER_VOID` is a sentinel value — not a storage backend. When a payload is spilled to void the tiering layer deletes it rather than moving bytes. See [Design Details](./DESIGN.md#tiervoid-discard-on-eviction).

## 3. Cross-cutting concerns

- **Configuration:** protobuf-backed config loading in `internal/config`.
- **Observability:** tracing and metrics in `internal/observability`.
- **Utility primitives:** time and UUID helpers in `internal/util`.
- **Lineage:** graph model and traversal in `internal/lineage`.

## 4. Control flow (high-level)

### gRPC path (native clients)

1. Client calls gRPC API.
2. gRPC server validates and maps request to service layer.
3. Service executes lifecycle/placement/lease logic via core modules.
4. Service persists and reads state via repository interface.
5. Service returns descriptor + lease metadata to caller.
6. Caller accesses payload bytes directly from resolved storage tier.

### HTTP/REST path (gateway)

1. Browser or HTTP client sends JSON request to the gateway.
2. Gateway translates to gRPC and forwards to the payload-manager.
3. Response is translated back to JSON and returned.
4. For `GET /v1/payloads/{id}/download`: gateway resolves the current tier via `ResolveSnapshot`; if the payload is in RAM or GPU it calls `Spill` (blocking) to move it to disk, then acquires a disk read lease and streams the file contents directly from the shared data volume.

## 5. Deployment security considerations

### Configuration file permissions

The runtime configuration file (YAML) contains sensitive values including database connection URIs and object storage credentials. Restrict access so only the service account running `payload-manager` can read it:

```sh
chmod 600 /etc/payload-manager/runtime.yaml
chown payload-manager:payload-manager /etc/payload-manager/runtime.yaml
```

### Secrets and credentials

- **Database passwords** are embedded in `database.postgres.connection_uri`. Prefer a secrets manager (e.g. Vault, AWS Secrets Manager) and inject the URI via the `PAYLOAD_MANAGER_CONFIG` environment variable or a runtime-mounted file rather than baking credentials into static config files.
- **Object storage credentials** (`filesystem_options.s3.*`) should use IAM instance roles or workload identity where available; avoid long-lived static keys in config files.
- **Secret rotation** requires a service restart to pick up a new connection URI unless an external secret manager with dynamic injection is used.

### Network exposure

The gRPC server (`server.bind_address`) defaults to `0.0.0.0:50051`. In production:

- Bind to a loopback or internal address when the service is only accessed within the same node or cluster.
- Place a TLS-terminating proxy (e.g. Envoy) in front of `payload-manager` for external-facing deployments; the server currently uses insecure credentials and relies on the surrounding infrastructure for transport security.

## 6. Non-goals (explicit)

- Payload Manager is not intended to proxy large payload byte streams through gRPC.
- Payload Manager is not intended to collapse all storage tiers into a single physical medium.
- Payload Manager is not intended to hide durability/latency tradeoffs; policy controls are expected.
