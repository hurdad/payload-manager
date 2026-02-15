#include "internal/observability/spans.hpp"

#ifdef ENABLE_OTEL

#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/samplers/always_off_factory.h>
#include <opentelemetry/sdk/trace/samplers/always_on_factory.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/provider.h>

#include <chrono>
#include <cstdlib>
#include <utility>

#include "config/config.pb.h"

namespace payload::observability {
namespace otlp      = opentelemetry::exporter::otlp;
namespace trace_api = opentelemetry::trace;
namespace sdktrace  = opentelemetry::sdk::trace;
namespace resource  = opentelemetry::sdk::resource;

namespace {
std::shared_ptr<sdktrace::TracerProvider>           g_sdk_provider;
opentelemetry::nostd::shared_ptr<trace_api::Tracer> g_tracer;

std::string ResolveEndpoint(const OtlpConfig& config) {
  if (!config.endpoint.empty()) {
    return config.endpoint;
  }

  if (const char* endpoint = std::getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) {
    return endpoint;
  }
  if (const char* endpoint = std::getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) {
    return endpoint;
  }

  return config.transport == OtlpTransport::kHttpProtobuf ? "http://localhost:4318/v1/traces" : "localhost:4317";
}

resource::Resource BuildResource(const OtlpConfig& config) {
  resource::ResourceAttributes attrs = {{"service.name", config.service_name}};
  return resource::Resource::Create(attrs);
}

} // namespace

bool InitializeTracing(const OtlpConfig& config) {
  auto endpoint = ResolveEndpoint(config);

  std::unique_ptr<sdktrace::SpanExporter> exporter;
  if (config.transport == OtlpTransport::kHttpProtobuf) {
    otlp::OtlpHttpExporterOptions options;
    options.url = endpoint;
    exporter    = otlp::OtlpHttpExporterFactory::Create(options);
  } else {
    otlp::OtlpGrpcExporterOptions options;
    options.endpoint            = endpoint;
    options.use_ssl_credentials = !config.insecure;
    exporter                    = otlp::OtlpGrpcExporterFactory::Create(options);
  }

  auto span_processor = sdktrace::BatchSpanProcessorFactory::Create(std::move(exporter), sdktrace::BatchSpanProcessorOptions{});
  auto provider       = sdktrace::TracerProviderFactory::Create(std::move(span_processor), BuildResource(config));

  g_sdk_provider = std::shared_ptr<sdktrace::TracerProvider>(std::move(provider));
  trace_api::Provider::SetTracerProvider(opentelemetry::nostd::shared_ptr<trace_api::TracerProvider>(g_sdk_provider));
  g_tracer = g_sdk_provider->GetTracer("payload-manager", "0.1.0");
  return static_cast<bool>(g_tracer);
}

bool InitializeTracing(const payload::runtime::config::RuntimeConfig& config) {
  const auto& observability = config.observability();
  if (!observability.tracing_enabled()) {
    ShutdownTracing();
    return false;
  }

  OtlpConfig otlp_config;
  otlp_config.endpoint = observability.otlp_endpoint();
  otlp_config.transport =
      observability.transport() == payload::runtime::config::OTLP_TRANSPORT_HTTP ? OtlpTransport::kHttpProtobuf : OtlpTransport::kGrpc;

  auto endpoint = ResolveEndpoint(otlp_config);

  std::unique_ptr<sdktrace::SpanExporter> exporter;
  if (otlp_config.transport == OtlpTransport::kHttpProtobuf) {
    otlp::OtlpHttpExporterOptions options;
    options.url = endpoint;
    exporter    = otlp::OtlpHttpExporterFactory::Create(options);
  } else {
    otlp::OtlpGrpcExporterOptions options;
    options.endpoint            = endpoint;
    options.use_ssl_credentials = !otlp_config.insecure;
    exporter                    = otlp::OtlpGrpcExporterFactory::Create(options);
  }

  std::unique_ptr<sdktrace::SpanProcessor> span_processor;
  if (observability.tracing().processor() == payload::runtime::config::ObservabilityConfig_TracingConfig_TraceProcessorType_TRACE_PROCESSOR_SIMPLE) {
    span_processor = sdktrace::SimpleSpanProcessorFactory::Create(std::move(exporter));
  } else {
    sdktrace::BatchSpanProcessorOptions batch_options;
    if (observability.tracing().batch().max_queue_size() > 0) {
      batch_options.max_queue_size = observability.tracing().batch().max_queue_size();
    }
    if (observability.tracing().batch().max_export_batch_size() > 0) {
      batch_options.max_export_batch_size = observability.tracing().batch().max_export_batch_size();
    }
    if (observability.tracing().batch().schedule_delay_ms() > 0) {
      batch_options.schedule_delay_millis = std::chrono::milliseconds(observability.tracing().batch().schedule_delay_ms());
    }
    span_processor = sdktrace::BatchSpanProcessorFactory::Create(std::move(exporter), batch_options);
  }

  std::unique_ptr<sdktrace::Sampler> sampler;
  if (observability.tracing().trace_hint() == payload::runtime::config::ObservabilityConfig_TracingConfig_TraceHint_TRACE_HINT_ALWAYS) {
    sampler = sdktrace::AlwaysOnSamplerFactory::Create();
  } else if (observability.tracing().trace_hint() == payload::runtime::config::ObservabilityConfig_TracingConfig_TraceHint_TRACE_HINT_NEVER) {
    sampler = sdktrace::AlwaysOffSamplerFactory::Create();
  }

  std::unique_ptr<sdktrace::TracerProvider> provider;
  if (sampler) {
    provider = sdktrace::TracerProviderFactory::Create(std::move(span_processor), BuildResource(otlp_config), std::move(sampler));
  } else {
    provider = sdktrace::TracerProviderFactory::Create(std::move(span_processor), BuildResource(otlp_config));
  }

  g_sdk_provider = std::shared_ptr<sdktrace::TracerProvider>(std::move(provider));
  trace_api::Provider::SetTracerProvider(opentelemetry::nostd::shared_ptr<trace_api::TracerProvider>(g_sdk_provider));
  g_tracer = g_sdk_provider->GetTracer("payload-manager", "0.1.0");
  return static_cast<bool>(g_tracer);
}

void ShutdownTracing() {
  if (g_sdk_provider) {
    g_sdk_provider->ForceFlush();
    g_sdk_provider->Shutdown();
  }
  g_sdk_provider.reset();
  g_tracer = nullptr;
}

struct SpanScope::Impl {
  opentelemetry::nostd::shared_ptr<trace_api::Span> span;
  std::unique_ptr<trace_api::Scope>                 scope;
};

SpanScope::SpanScope(std::string_view name) : impl_(std::make_unique<Impl>()) {
  if (!g_tracer) {
    auto provider = trace_api::Provider::GetTracerProvider();
    if (provider) {
      g_tracer = provider->GetTracer("payload-manager", "0.1.0");
    }
  }

  if (!g_tracer) {
    return;
  }

  impl_->span  = g_tracer->StartSpan(std::string(name));
  impl_->scope = std::make_unique<trace_api::Scope>(g_tracer->WithActiveSpan(impl_->span));
}

SpanScope::~SpanScope() {
  if (impl_ && impl_->span) {
    impl_->span->End();
  }
}

SpanScope::SpanScope(SpanScope&&) noexcept            = default;
SpanScope& SpanScope::operator=(SpanScope&&) noexcept = default;

void SpanScope::SetAttribute(std::string_view key, std::string_view value) {
  if (impl_ && impl_->span) {
    impl_->span->SetAttribute(std::string(key), std::string(value));
  }
}

void SpanScope::SetAttribute(std::string_view key, std::int64_t value) {
  if (impl_ && impl_->span) {
    impl_->span->SetAttribute(std::string(key), value);
  }
}

void SpanScope::SetAttribute(std::string_view key, double value) {
  if (impl_ && impl_->span) {
    impl_->span->SetAttribute(std::string(key), value);
  }
}

void SpanScope::AddEvent(std::string_view name) {
  if (impl_ && impl_->span) {
    impl_->span->AddEvent(std::string(name));
  }
}

void SpanScope::RecordException(std::string_view description) {
  if (impl_ && impl_->span) {
    impl_->span->AddEvent("exception", {{"exception.message", std::string(description)}});
    impl_->span->SetStatus(trace_api::StatusCode::kError, std::string(description));
  }
}

} // namespace payload::observability

#endif
