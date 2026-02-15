#include "internal/observability/spans.hpp"

#ifdef ENABLE_OTEL

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#if __has_include(<opentelemetry/sdk/metrics/periodic_exporting_metric_reader_factory.h>)
#define PAYLOAD_OTEL_METRIC_READER_FACTORY 1
#include <opentelemetry/sdk/metrics/periodic_exporting_metric_reader_factory.h>
#elif __has_include(<opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>)
#define PAYLOAD_OTEL_METRIC_READER_FACTORY 1
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#elif __has_include(<opentelemetry/sdk/metrics/periodic_exporting_metric_reader.h>)
#include <opentelemetry/sdk/metrics/periodic_exporting_metric_reader.h>
#else
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#endif
#include <opentelemetry/sdk/resource/resource.h>

#include "config/config.pb.h"

namespace payload::observability {
namespace otlp        = opentelemetry::exporter::otlp;
namespace metrics_api = opentelemetry::metrics;
namespace sdkmetrics  = opentelemetry::sdk::metrics;
namespace resource    = opentelemetry::sdk::resource;

namespace {
using AttributePair = std::pair<opentelemetry::nostd::string_view, opentelemetry::common::AttributeValue>;
std::shared_ptr<sdkmetrics::MeterProvider> g_provider;

struct MetricsOptions {
  bool request_metrics_enabled{true};
  bool spill_metrics_enabled{true};
  bool tier_occupancy_metrics_enabled{true};
  bool request_latency_histograms_enabled{true};
  bool route_labels_enabled{true};
  bool tier_labels_enabled{true};
};

MetricsOptions g_metrics_options;

std::string ResolveEndpoint(const OtlpConfig& config) {
  if (!config.endpoint.empty()) {
    return config.endpoint;
  }

  if (const char* endpoint = std::getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")) {
    return endpoint;
  }
  if (const char* endpoint = std::getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) {
    return endpoint;
  }

  return config.transport == OtlpTransport::kHttpProtobuf ? "http://localhost:4318/v1/metrics" : "localhost:4317";
}

resource::Resource BuildResource(const OtlpConfig& config) {
  resource::ResourceAttributes attrs = {{"service.name", config.service_name}};
  return resource::Resource::Create(attrs);
}

template <typename Provider>
void ConfigureResource(Provider& provider, const resource::Resource& res) {
  if constexpr (requires { provider.SetResource(res); }) {
    provider.SetResource(res);
  }
}

template <typename Provider>
void AddMetricReaderCompat(const std::shared_ptr<Provider>& provider, std::unique_ptr<sdkmetrics::MetricReader> reader) {
  if constexpr (requires { provider->AddMetricReader(std::move(reader)); }) {
    provider->AddMetricReader(std::move(reader));
  } else {
    provider->AddMetricReader(std::shared_ptr<sdkmetrics::MetricReader>(std::move(reader)));
  }
}

template <typename Instrument, typename Value, typename Attributes>
void AddWithAttributes(const opentelemetry::nostd::shared_ptr<Instrument>& instrument, Value value, Attributes&& attributes) {
  if constexpr (requires { instrument->Add(value, std::forward<Attributes>(attributes), opentelemetry::context::Context{}); }) {
    instrument->Add(value, std::forward<Attributes>(attributes), opentelemetry::context::Context{});
  } else {
    instrument->Add(value, std::forward<Attributes>(attributes));
  }
}

template <typename Instrument, typename Value, typename Attributes>
void RecordWithAttributes(const opentelemetry::nostd::shared_ptr<Instrument>& instrument, Value value, Attributes&& attributes) {
  if constexpr (requires { instrument->Record(value, std::forward<Attributes>(attributes), opentelemetry::context::Context{}); }) {
    instrument->Record(value, std::forward<Attributes>(attributes), opentelemetry::context::Context{});
  } else {
    instrument->Record(value, std::forward<Attributes>(attributes));
  }
}

} // namespace

struct Metrics::Impl {
  opentelemetry::nostd::shared_ptr<metrics_api::Meter> meter;

  opentelemetry::nostd::shared_ptr<metrics_api::Counter<std::uint64_t>> request_count;
  opentelemetry::nostd::shared_ptr<metrics_api::Histogram<double>>      request_latency_ms;
  opentelemetry::nostd::shared_ptr<metrics_api::Histogram<double>>      spill_duration_ms;
  opentelemetry::nostd::shared_ptr<metrics_api::ObservableInstrument>   tier_occupancy_gauge;

  std::mutex                                    tier_occupancy_mutex;
  std::unordered_map<std::string, std::int64_t> tier_occupancy_values;
};

bool InitializeMetrics(const OtlpConfig& config) {
  auto endpoint = ResolveEndpoint(config);

  std::unique_ptr<sdkmetrics::PushMetricExporter> exporter;
  if (config.transport == OtlpTransport::kHttpProtobuf) {
    otlp::OtlpHttpMetricExporterOptions options;
    options.url = endpoint;
    exporter    = otlp::OtlpHttpMetricExporterFactory::Create(options);
  } else {
    otlp::OtlpGrpcMetricExporterOptions options;
    options.endpoint            = endpoint;
    options.use_ssl_credentials = !config.insecure;
    exporter                    = otlp::OtlpGrpcMetricExporterFactory::Create(options);
  }

  sdkmetrics::PeriodicExportingMetricReaderOptions reader_options;
  reader_options.export_interval_millis = std::chrono::milliseconds(1000);
#ifdef PAYLOAD_OTEL_METRIC_READER_FACTORY
  auto reader = sdkmetrics::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), reader_options);
#else
  auto reader = std::make_unique<sdkmetrics::PeriodicExportingMetricReader>(std::move(exporter), reader_options);
#endif

  auto resource = BuildResource(config);
  g_provider    = std::make_shared<sdkmetrics::MeterProvider>(std::unique_ptr<sdkmetrics::ViewRegistry>(new sdkmetrics::ViewRegistry()), resource);
  ConfigureResource(*g_provider, resource);
  AddMetricReaderCompat(g_provider, std::move(reader));

  metrics_api::Provider::SetMeterProvider(opentelemetry::nostd::shared_ptr<metrics_api::MeterProvider>(g_provider));
  return true;
}

bool InitializeMetrics(const payload::runtime::config::RuntimeConfig& config) {
  const auto& observability = config.observability();
  if (!observability.metrics_enabled()) {
    ShutdownMetrics();
    return false;
  }

  OtlpConfig otlp_config;
  otlp_config.endpoint = observability.otlp_endpoint();
  otlp_config.transport =
      observability.transport() == payload::runtime::config::OTLP_TRANSPORT_HTTP ? OtlpTransport::kHttpProtobuf : OtlpTransport::kGrpc;

  auto endpoint = ResolveEndpoint(otlp_config);

  std::unique_ptr<sdkmetrics::PushMetricExporter> exporter;
  if (otlp_config.transport == OtlpTransport::kHttpProtobuf) {
    otlp::OtlpHttpMetricExporterOptions options;
    options.url = endpoint;
    exporter    = otlp::OtlpHttpMetricExporterFactory::Create(options);
  } else {
    otlp::OtlpGrpcMetricExporterOptions options;
    options.endpoint            = endpoint;
    options.use_ssl_credentials = !otlp_config.insecure;
    exporter                    = otlp::OtlpGrpcMetricExporterFactory::Create(options);
  }

  const auto&                                      metric_config = observability.metrics();
  sdkmetrics::PeriodicExportingMetricReaderOptions reader_options;
  const auto                                       min_interval_ms = metric_config.min_collection_interval_ms();
  const auto configured_interval_ms     = metric_config.collection_interval_ms() > 0 ? metric_config.collection_interval_ms() : 1000;
  reader_options.export_interval_millis = std::chrono::milliseconds(std::max(min_interval_ms, configured_interval_ms));
  if (metric_config.export_timeout_ms() > 0) {
    reader_options.export_timeout_millis = std::chrono::milliseconds(metric_config.export_timeout_ms());
  }

#ifdef PAYLOAD_OTEL_METRIC_READER_FACTORY
  auto reader = sdkmetrics::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), reader_options);
#else
  auto reader = std::make_unique<sdkmetrics::PeriodicExportingMetricReader>(std::move(exporter), reader_options);
#endif

  auto resource = BuildResource(otlp_config);
  g_provider    = std::make_shared<sdkmetrics::MeterProvider>(std::unique_ptr<sdkmetrics::ViewRegistry>(new sdkmetrics::ViewRegistry()), resource);
  ConfigureResource(*g_provider, resource);
  AddMetricReaderCompat(g_provider, std::move(reader));

  metrics_api::Provider::SetMeterProvider(opentelemetry::nostd::shared_ptr<metrics_api::MeterProvider>(g_provider));

  g_metrics_options.request_metrics_enabled            = metric_config.request_metrics_enabled();
  g_metrics_options.spill_metrics_enabled              = metric_config.spill_metrics_enabled();
  g_metrics_options.tier_occupancy_metrics_enabled     = metric_config.tier_occupancy_metrics_enabled();
  g_metrics_options.request_latency_histograms_enabled = metric_config.request_latency_histograms_enabled();
  g_metrics_options.route_labels_enabled               = metric_config.route_labels_enabled();
  g_metrics_options.tier_labels_enabled                = metric_config.tier_labels_enabled();

  return true;
}

void ShutdownMetrics() {
  if (g_provider) {
    g_provider->ForceFlush();
    g_provider->Shutdown();
  }
  g_provider.reset();
}

Metrics::Metrics() : impl_(std::make_unique<Impl>()) {
  auto provider = metrics_api::Provider::GetMeterProvider();
  impl_->meter  = provider->GetMeter("payload-manager", "0.1.0");

  impl_->request_count        = impl_->meter->CreateUInt64Counter("payload.request.count", "1", "Total number of service requests");
  impl_->request_latency_ms   = impl_->meter->CreateDoubleHistogram("payload.request.latency_ms", "ms", "End-to-end request latency in milliseconds");
  impl_->spill_duration_ms    = impl_->meter->CreateDoubleHistogram("payload.spill.duration_ms", "ms", "Spill operation duration in milliseconds");
  impl_->tier_occupancy_gauge = impl_->meter->CreateInt64ObservableGauge("payload.tier.occupancy_bytes", "Current tier occupancy in bytes", "By");
  impl_->tier_occupancy_gauge->AddCallback(
      [](metrics_api::ObserverResult result, void* state) {
        auto*                       impl = static_cast<Impl*>(state);
        std::lock_guard<std::mutex> lock(impl->tier_occupancy_mutex);
        auto int_result = opentelemetry::nostd::get<opentelemetry::nostd::shared_ptr<metrics_api::ObserverResultT<std::int64_t>>>(result);
        for (const auto& [tier, bytes] : impl->tier_occupancy_values) {
          if (g_metrics_options.tier_labels_enabled) {
            const std::initializer_list<AttributePair> attributes = {{"tier", tier}};
            int_result->Observe(bytes, attributes);
          } else {
            int_result->Observe(bytes);
          }
        }
      },
      impl_.get());
}

Metrics& Metrics::Instance() {
  static Metrics instance;
  return instance;
}

void Metrics::RecordRequest(std::string_view route, bool success) {
  if (!impl_ || !impl_->request_count || !g_metrics_options.request_metrics_enabled) {
    return;
  }

  if (g_metrics_options.route_labels_enabled) {
    const std::initializer_list<AttributePair> attributes = {{"route", std::string(route)}, {"success", success}};
    AddWithAttributes(impl_->request_count, static_cast<std::uint64_t>(1), attributes);
    return;
  }

  const std::initializer_list<AttributePair> attributes = {{"success", success}};
  AddWithAttributes(impl_->request_count, static_cast<std::uint64_t>(1), attributes);
}

void Metrics::ObserveRequestLatencyMs(std::string_view route, double latency_ms) {
  if (!impl_ || !impl_->request_latency_ms || !g_metrics_options.request_metrics_enabled || !g_metrics_options.request_latency_histograms_enabled) {
    return;
  }

  if (g_metrics_options.route_labels_enabled) {
    const std::initializer_list<AttributePair> attributes = {{"route", std::string(route)}};
    RecordWithAttributes(impl_->request_latency_ms, latency_ms, attributes);
    return;
  }

  RecordWithAttributes(impl_->request_latency_ms, latency_ms, std::initializer_list<AttributePair>{});
}

void Metrics::ObserveSpillDurationMs(std::string_view op, double duration_ms) {
  if (!impl_ || !impl_->spill_duration_ms || !g_metrics_options.spill_metrics_enabled) {
    return;
  }

  const std::initializer_list<AttributePair> attributes = {{"op", std::string(op)}};
  RecordWithAttributes(impl_->spill_duration_ms, duration_ms, attributes);
}

void Metrics::SetTierOccupancyBytes(std::string_view tier, std::uint64_t bytes) {
  if (!impl_ || !impl_->tier_occupancy_gauge || !g_metrics_options.tier_occupancy_metrics_enabled) {
    return;
  }

  std::lock_guard<std::mutex> lock(impl_->tier_occupancy_mutex);
  impl_->tier_occupancy_values[std::string(tier)] = static_cast<std::int64_t>(bytes);
}

} // namespace payload::observability

#endif
