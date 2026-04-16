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

The tier chain is ordered from fastest to most durable:

```
GPU → RAM → DISK → (OBJECT | VOID)
```

When pressure rises:

- Lower-priority or colder payloads are candidates for spill.
- Spill scheduler and workers coordinate movement to lower-cost tiers.
- Metadata must remain authoritative during and after relocation.

Pressure is monitored independently for GPU, RAM, and disk. Each tier has a configurable capacity limit; when occupancy exceeds the limit, the tiering manager picks a least-recently-used victim from that tier and enqueues a spill task.

### TIER_VOID: discard on eviction

`TIER_VOID` is the terminal tier for ephemeral payloads. When a payload spills to void it is deleted — no bytes are written anywhere.

Set `spill_target = TIER_VOID` in `EvictionPolicy` to mark a payload as ephemeral:

- If the payload is evicted from RAM under RAM pressure, it is deleted immediately rather than moved to disk.
- If the payload is evicted from disk under disk pressure, it is deleted rather than moved to object storage.

The void tier is not durable. Setting `require_durable = true` alongside `spill_target = TIER_VOID` is rejected at spill time.

For payloads without an explicit `spill_target`:
- RAM eviction target defaults to `TIER_DISK`.
- Disk eviction target defaults to `TIER_OBJECT`.

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
