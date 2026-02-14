# Payload Manager Design Notes

This document captures practical design decisions and operating concepts that complement the architecture overview.

## 1. Core design principles

1. **Control-plane/data-plane separation:** avoid byte shuttling through orchestration.
2. **Stable read via leases:** clients can safely read while placement may otherwise evolve.
3. **Backend-agnostic persistence contracts:** same domain behavior across memory/SQLite/PostgreSQL.
4. **Tier-aware placement:** match payload access patterns and pressure signals to appropriate media.
5. **Composable services:** admin, catalog, data, and stream concerns remain separable but consistent.

## 2. Payload lifecycle model

Typical path:

- **Allocate**: reserve identity and intended placement.
- **Commit**: finalize payload availability metadata.
- **Active**: payload available for lease-based reads.
- **Expire/Delete**: retention and explicit cleanup remove availability.

Design intent:

- Keep transitions explicit and auditable.
- Reject invalid state transitions early.
- Preserve metadata consistency even when payload bytes live externally.

## 3. Descriptor and lease semantics

### Descriptor

A descriptor communicates where/how data can be accessed (tier + location reference). It may be transient if placement changes.

### Read lease

A lease provides a temporary stability guarantee:

- Payload location is held stable while lease is valid.
- Service can enforce relocation constraints during lease lifetime.
- Explicit lease release allows aggressive resource reclamation.

## 4. Placement, tiering, and spill behavior

### Placement goals

- Prefer low-latency tiers (GPU/RAM) for hot payloads.
- Respect capacity and pressure signals.
- Consider policy inputs for durability/performance requirements.

### Tiering and spill

When pressure rises:

- Lower-priority or colder payloads are candidates for spill.
- Spill scheduler and workers coordinate movement to lower-cost tiers.
- Metadata must remain authoritative during and after relocation.

## 5. Repository and transaction design

The repository API centralizes consistency boundaries:

- Services depend on repository contracts, not concrete DB engines.
- Transactions wrap multi-step state updates (payload, metadata, lineage, offsets).
- Backend parity tests ensure semantics match across implementations.

## 6. Metadata and lineage

- Metadata supports both direct lookup and historical context.
- Immutable metadata event patterns reduce ambiguity in audit trails.
- Lineage graph tracks derivation relationships for dependency-aware operations.

## 7. Streams and consumer offsets

Stream design supports ordered processing scenarios:

- Append-like stream entries with durable indexing.
- Per-consumer offset tracking for replay and progress.
- Isolation of stream concerns from primary payload lifecycle where possible.

## 8. Operational considerations

- **Observability-first:** metrics + traces should expose lease pressure, spill latency, tier occupancy, and backend health.
- **Migration safety:** schema versioning and migration ordering must remain deterministic.
- **Config clarity:** runtime behavior should be explicit through validated config.

## 9. Testing implications

Design choices imply testing needs:

- State machine tests for lifecycle transitions.
- Lease stability and expiry behavior tests.
- Backend contract parity tests.
- Integration tests for migration, gRPC status mapping, and stream ordering.

See [Testing Strategy](./TESTING_STRATEGY.md) for the full plan.
