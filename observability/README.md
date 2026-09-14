# Observability stack

Configuration for the local telemetry stack that `docker/docker-compose.observability.yml`
brings up. Nothing here is required to run payload-manager — it is the
collector, storage and dashboards you point it at when
`PAYLOAD_MANAGER_ENABLE_OTEL=ON`.

(This directory was called `grafana/` until it grew Alloy, Prometheus and Tempo
configs alongside the dashboards.)

## Layout

| Path | Component | What it does |
|---|---|---|
| `alloy/config.alloy` | Grafana Alloy | OTLP receiver. Takes traces, metrics and logs from payload-manager and fans them out to Prometheus and Tempo. |
| `prometheus/prometheus.yml` | Prometheus | Scrape config and retention for the metrics Alloy forwards. |
| `tempo/tempo.yaml` | Tempo | Trace storage and query. |
| `provisioning/datasources/` | Grafana | Wires Prometheus and Tempo in as datasources on first boot. |
| `provisioning/dashboards/` | Grafana | Tells Grafana to load dashboards from `dashboards/`. |
| `dashboards/payload-manager.json` | Grafana | The "Payload Manager" dashboard (20 panels). |

## Running it

The observability stack layers on top of whichever runtime compose file you are
already using:

```bash
docker compose \
  -f docker/docker-compose.postgres.yml -f docker/docker-compose.otel.yml \
  -f docker/docker-compose.observability.yml \
  up --build
```

| Service | URL | Notes |
|---|---|---|
| Grafana | http://localhost:3000 | Dashboard is provisioned automatically |
| Prometheus | http://localhost:9090 | |
| Tempo | http://localhost:3200 | HTTP query API |
| Alloy UI | http://localhost:12345 | Useful for confirming data is arriving |

payload-manager exports to Alloy on **4317** (OTLP gRPC); **4318** is OTLP HTTP,
used by the integration test to export a root span. Tempo's own OTLP port is
remapped to **4327** on the host so it does not collide with Alloy's 4317.

Point the service at the collector with `observability.otlp_endpoint` in your
runtime config — the `*-otel-*` variants under `config/` already do:

```yaml
observability:
  metrics_enabled: true
  tracing_enabled: true
  otlp_endpoint: "alloy:4317"
  transport: OTLP_TRANSPORT_GRPC
```

## Changing the dashboard

Grafana mounts `dashboards/` read-only, so edits made in the UI are not
persisted back. To keep a change: edit the panel in Grafana, use **Share →
Export → Save to file** (with "Export for sharing externally" off), and replace
`dashboards/payload-manager.json`.

The metrics the dashboard draws on are documented in
[`../docs/METRICS.md`](../docs/METRICS.md). If you add an instrument, add it
there as well as here — a panel with no documented metric behind it is hard to
interpret later.
