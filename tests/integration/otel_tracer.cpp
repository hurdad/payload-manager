// OTel SDK implementation — ALL OTel SDK headers are confined here.
// No client / protobuf / gRPC service headers may be included in this TU.
#include "otel_tracer.hpp"

#ifdef ENABLE_OTEL

#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/scope.h>

#include <iomanip>
#include <memory>
#include <sstream>

namespace {

namespace otlp      = opentelemetry::exporter::otlp;
namespace sdktrace  = opentelemetry::sdk::trace;
namespace resource  = opentelemetry::sdk::resource;
namespace trace_api = opentelemetry::trace;
namespace prop      = opentelemetry::context::propagation;

std::shared_ptr<sdktrace::TracerProvider>           g_sdk_provider;
opentelemetry::nostd::shared_ptr<trace_api::Tracer> g_tracer;

// Active root span + scope (one at a time per test).
opentelemetry::nostd::shared_ptr<trace_api::Span> g_span;
std::unique_ptr<trace_api::Scope>                 g_scope;

template <size_t N>
std::string BytesToHex(opentelemetry::nostd::span<const uint8_t, N> bytes) {
  std::ostringstream ss;
  ss << std::hex << std::setfill('0');
  for (uint8_t b : bytes) ss << std::setw(2) << static_cast<int>(b);
  return ss.str();
}

} // namespace

void OtelInit(const std::string& grpc_endpoint, const std::string& service_name) {
  if (grpc_endpoint.empty()) return;

  otlp::OtlpGrpcExporterOptions opts;
  opts.endpoint            = grpc_endpoint;
  opts.use_ssl_credentials = false;
  auto exporter            = otlp::OtlpGrpcExporterFactory::Create(opts);

  // SimpleSpanProcessor exports each span synchronously on End() — ensures
  // all root spans are flushed without needing a batch timeout.
  auto processor = sdktrace::SimpleSpanProcessorFactory::Create(std::move(exporter));

  auto res = resource::Resource::Create({
      {"service.name", service_name},
      {"service.version", std::string("1.0.0")},
  });

  g_sdk_provider = std::shared_ptr<sdktrace::TracerProvider>(sdktrace::TracerProviderFactory::Create(std::move(processor), res));

  trace_api::Provider::SetTracerProvider(opentelemetry::nostd::shared_ptr<trace_api::TracerProvider>(g_sdk_provider));

  prop::GlobalTextMapPropagator::SetGlobalPropagator(
      opentelemetry::nostd::shared_ptr<prop::TextMapPropagator>(new opentelemetry::trace::propagation::HttpTraceContext()));

  g_tracer = g_sdk_provider->GetTracer(service_name, "1.0.0");
}

void OtelShutdown() {
  g_span   = nullptr;
  g_scope  = nullptr;
  g_tracer = nullptr;
  if (g_sdk_provider) {
    g_sdk_provider->ForceFlush();
    g_sdk_provider->Shutdown();
    g_sdk_provider.reset();
  }
}

OtelSpanContext OtelStartSpan(const std::string& name) {
  OtelSpanContext result;
  if (!g_tracer) return result;

  // End any previous span that wasn't explicitly ended.
  if (g_span) {
    g_scope.reset();
    g_span->End();
    g_span = nullptr;
  }

  g_span  = g_tracer->StartSpan(name);
  g_scope = std::make_unique<trace_api::Scope>(g_tracer->WithActiveSpan(g_span));

  auto ctx = g_span->GetContext();
  if (!ctx.IsValid()) return result;

  result.trace_id_hex = BytesToHex(ctx.trace_id().Id());
  result.span_id_hex  = BytesToHex(ctx.span_id().Id());
  result.valid        = true;
  return result;
}

void OtelEndSpan() {
  if (!g_span) return;
  g_scope.reset();
  g_span->End();
  g_span = nullptr;
}

#else // !ENABLE_OTEL — stub implementations when OTel is disabled

void OtelInit(const std::string&, const std::string&) {}
void OtelShutdown() {}
OtelSpanContext OtelStartSpan(const std::string&) { return {}; }
void OtelEndSpan() {}

#endif // ENABLE_OTEL
