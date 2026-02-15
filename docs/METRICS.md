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

- **Type:** UpDownCounter (`int64`)
- **Unit:** `By`
- **Meaning:** Tier occupancy updates in bytes.
- **Attributes:**
  - Optional: `tier` (enabled by `tier_labels_enabled`)
- **Enable controls:**
  - `tier_occupancy_metrics_enabled`

## 4. Runtime configuration knobs

`observability.metrics` supports the following controls:

- **Instrument toggles**
  - `request_metrics_enabled`
  - `spill_metrics_enabled`
  - `tier_occupancy_metrics_enabled`
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
- Keep export intervals at or above 1s unless you have a specific low-latency observability requirement.

## 6. Notes and caveats

- Metrics code is compiled behind `ENABLE_OTEL` and is inactive in non-OTEL builds.
- `payload.tier.occupancy_bytes` is emitted through an UpDownCounter; downstream backends may display this as cumulative deltas depending on aggregation settings.
