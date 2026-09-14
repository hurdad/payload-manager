# Metrics Reference

This document describes metrics emitted by Payload Manager when OpenTelemetry (OTEL) support is enabled and runtime metrics are turned on.

## 1. Prerequisites

Metrics are emitted only when all of the following are true:

1. Payload Manager is built with OTEL support (`-DPAYLOAD_MANAGER_ENABLE_OTEL=ON`).
2. Runtime config sets `observability.metrics_enabled: true`.
3. An OTLP exporter endpoint is reachable.

When metrics are disabled at runtime, the metrics pipeline is shut down and no instruments are recorded.

## 2. Export pipeline behavior

### OTLP transport

`observability.transport` controls protocol selection:

- `OTLP_TRANSPORT_GRPC` (default): OTLP gRPC exporter.
- `OTLP_TRANSPORT_HTTP`: OTLP HTTP/protobuf exporter.

### Endpoint resolution order

If `observability.otlp_endpoint` is set, that value wins. Otherwise Payload Manager checks:

1. `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
2. `OTEL_EXPORTER_OTLP_ENDPOINT`

If none are provided, the runtime default is:

- HTTP/protobuf: `http://localhost:4318/v1/metrics`
- gRPC: `localhost:4317`

### Resource attributes

Metrics use OTEL resource attribute `service.name`, sourced from OTEL config service name.

## 3. Metrics catalog

### `payload.request.count`

- **Type:** Counter (`uint64`)
- **Unit:** `1`
- **Meaning:** Total number of service requests observed.
- **Attributes:**
  - Always: `success` (`true` / `false`)
  - Optional: `route` (enabled by `route_labels_enabled`)
- **Enable controls:**
  - `request_metrics_enabled`

### `payload.request.latency_ms`

- **Type:** Histogram (`double`)
- **Unit:** `ms`
- **Meaning:** End-to-end request latency in milliseconds.
- **Attributes:**
  - Optional: `route` (enabled by `route_labels_enabled`)
- **Enable controls:**
  - `request_metrics_enabled`
  - `request_latency_histograms_enabled`

### `payload.spill.duration_ms`

- **Type:** Histogram (`double`)
- **Unit:** `ms`
- **Meaning:** Spill operation duration.
- **Attributes:**
  - `op` (spill operation label)
- **Enable controls:**
  - `spill_metrics_enabled`

### `payload.tier.occupancy_bytes`

- **Type:** Observable Gauge (`int64`)
- **Unit:** `By`
- **Meaning:** Current tier occupancy in bytes, sampled during collection.
- **Attributes:**
  - Optional: `tier` (enabled by `tier_labels_enabled`)
- **Enable controls:**
  - `tier_occupancy_metrics_enabled`

### `payload.ring.slots_total`

- **Type:** Observable Gauge (`int64`)
- **Unit:** `1`
- **Meaning:** Slots configured for the ring. The denominator for everything below.
- **Attributes:**
  - Always: `ring_id`
- **Enable controls:**
  - `ring_metrics_enabled`

### `payload.ring.slots_available`

- **Type:** Observable Gauge (`int64`)
- **Unit:** `1`
- **Meaning:** Slots `AcquireRingSlot` could take right now — neither being written nor
  pinned by a read lease. **This is the ring's headroom.** At `0`, the next
  `AcquireRingSlot` fails with `RESOURCE_EXHAUSTED` and the producer drops that
  capture, so sustained `0` means data loss rather than back-pressure.
- **Attributes:**
  - Always: `ring_id`
- **Enable controls:**
  - `ring_metrics_enabled`

### `payload.ring.slots_writing`

- **Type:** Observable Gauge (`int64`)
- **Unit:** `1`
- **Meaning:** Slots acquired by a producer that has not yet committed. Briefly non-zero
  in normal operation; a slot that stays here is a producer that died mid-capture,
  and is reclaimed after `slot_write_timeout_ms`.
- **Attributes:**
  - Always: `ring_id`
- **Enable controls:**
  - `ring_metrics_enabled`

### `payload.ring.slots_leased`

- **Type:** Observable Gauge (`int64`)
- **Unit:** `1`
- **Meaning:** Slots pinned by at least one consumer read lease. Rising toward
  `slots_total` means consumers are falling behind the producer.
- **Attributes:**
  - Always: `ring_id`
- **Enable controls:**
  - `ring_metrics_enabled`

### `payload.ring.leases_active`

- **Type:** Observable Gauge (`int64`)
- **Unit:** `1`
- **Meaning:** Sum of per-slot lease refcounts. Exceeds `slots_leased` whenever several
  consumers read the same capture, so `leases_active / slots_leased` is the effective
  fan-out.
- **Attributes:**
  - Always: `ring_id`
- **Enable controls:**
  - `ring_metrics_enabled`

### `payload.ring.slots_reclaimed_total`

- **Type:** Observable Gauge (`int64`), monotonic for the life of the process
- **Unit:** `1`
- **Meaning:** Cumulative count of slots the ring has had to take back from a producer
  that never committed. **Any increase means a producer died mid-capture**, or that
  `slot_write_timeout_ms` is set below the producer's honest worst case — both worth
  an alert. Reported as a gauge because the ring owns the running count; difference it
  downstream for a rate.
- **Attributes:**
  - Always: `ring_id`
- **Enable controls:**
  - `ring_metrics_enabled`

## 4. Runtime configuration knobs

`observability.metrics` supports the following controls:

- **Instrument toggles**
  - `request_metrics_enabled`
  - `spill_metrics_enabled`
  - `tier_occupancy_metrics_enabled`
  - `ring_metrics_enabled`
- **Cardinality/cost controls**
  - `request_latency_histograms_enabled`
  - `route_labels_enabled`
  - `tier_labels_enabled`
- **Collection/export timing**
  - `min_collection_interval_ms`
  - `collection_interval_ms`
  - `export_timeout_ms`

Export interval behavior:

- Effective interval = `max(min_collection_interval_ms, collection_interval_ms)`
- If `collection_interval_ms` is unset (`0`), a default of `1000 ms` is used before applying the minimum bound.

## 5. Operational guidance

- Disable `route_labels_enabled` if route-level cardinality becomes expensive.
- Disable `request_latency_histograms_enabled` for very constrained environments.
- Use `tier_labels_enabled` only when per-tier occupancy breakdown is needed.
- Ring gauges carry `ring_id` unconditionally: the numbers are meaningless pooled
  across rings, and the label set is bounded by static config rather than by traffic
  (one series per ring per gauge), so there is no traffic-driven cardinality to cap.
- For a ring tier, alert on `payload.ring.slots_available == 0` sustained (captures are
  being dropped) and on any increase in `payload.ring.slots_reclaimed_total` (a producer
  died).
- Keep export intervals at or above 1s unless you have a specific low-latency observability requirement.

## 6. Notes and caveats

- Metrics code is compiled behind `ENABLE_OTEL` and is inactive in non-OTEL builds.
- `payload.tier.occupancy_bytes` is emitted through an observable gauge callback; downstream backends typically show point-in-time values based on collection cadence.
- Ring gauges are sampled on a 1 s timer by `RingMetricsPublisher` and read at collection
  time, so a value can be up to one sampling interval plus one collection interval stale.
  They are deliberately not pushed from the ring hot path, which runs at capture rate
  (up to 1 kHz per ring).
- The same numbers are available as an instantaneous, unsampled snapshot from the admin
  `Stats` RPC (`StatsResponse.rings`), which does not require OTEL.
