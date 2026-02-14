#include "internal/observability/spans.hpp"

#ifdef ENABLE_OTEL

#include <cstdlib>
#include <utility>

#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/provider.h>

namespace payload::observability {
namespace otlp      = opentelemetry::exporter::otlp;
namespace trace_api = opentelemetry::trace;
namespace sdktrace  = opentelemetry::sdk::trace;
namespace resource  = opentelemetry::sdk::resource;

namespace {
std::shared_ptr<trace_api::Tracer> g_tracer;

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
    options.endpoint = endpoint;
    options.use_ssl_credentials = !config.insecure;
    exporter         = otlp::OtlpGrpcExporterFactory::Create(options);
  }

  auto span_processor = sdktrace::BatchSpanProcessorFactory::Create(std::move(exporter));
  auto provider       = sdktrace::TracerProviderFactory::Create(std::move(span_processor), BuildResource(config));

  trace_api::Provider::SetTracerProvider(provider);
  g_tracer = provider->GetTracer("payload-manager", "0.1.0");
  return static_cast<bool>(g_tracer);
}

void ShutdownTracing() {
  auto provider = trace_api::Provider::GetTracerProvider();
  if (provider) {
    auto sdk_provider = std::static_pointer_cast<sdktrace::TracerProvider>(provider);
    sdk_provider->ForceFlush();
    sdk_provider->Shutdown();
  }
  g_tracer.reset();
}

struct SpanScope::Impl {
  opentelemetry::nostd::shared_ptr<trace_api::Span> span;
  std::unique_ptr<trace_api::Scope> scope;
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

SpanScope::SpanScope(SpanScope&&) noexcept = default;
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
