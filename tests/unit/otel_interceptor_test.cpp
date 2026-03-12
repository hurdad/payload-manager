#include <cassert>
#include <iostream>
#include <map>
#include <string>
#include <string_view>

#ifdef ENABLE_OTEL

#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/span_context.h>

#include "internal/grpc/otel_interceptor.hpp"

namespace {

// A simple map-backed TextMapCarrier used to simulate both the server-side
// GrpcMetadataCarrier (read path) and the client-side GrpcClientMetadataCarrier
// (write path) without needing real gRPC plumbing.
class MapCarrier : public opentelemetry::context::propagation::TextMapCarrier {
 public:
  explicit MapCarrier(std::map<std::string, std::string>& headers) : headers_(headers) {
  }

  opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override {
    const auto it = headers_.find(std::string(key));
    if (it != headers_.end()) {
      return {it->second};
    }
    return {};
  }

  void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override {
    headers_[std::string(key)] = std::string(value);
  }

 private:
  std::map<std::string, std::string>& headers_;
};

void SetW3CPropagator() {
  opentelemetry::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
      opentelemetry::nostd::shared_ptr<opentelemetry::context::propagation::TextMapPropagator>(
          new opentelemetry::trace::propagation::HttpTraceContext()));
}

opentelemetry::nostd::shared_ptr<opentelemetry::context::propagation::TextMapPropagator> GetPropagator() {
  return opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
}

// Test: W3C traceparent is correctly extracted into the active context.
// This mirrors what OtelServerInterceptor does in POST_RECV_INITIAL_METADATA
// (carrier.Get reads from gRPC metadata; propagator.Extract parses the header).
void TestExtractsTraceparent() {
  SetW3CPropagator();

  const std::string                  trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
  const std::string                  span_id  = "00f067aa0ba902b7";
  const std::string                  tp       = "00-" + trace_id + "-" + span_id + "-01";
  std::map<std::string, std::string> headers  = {{"traceparent", tp}};
  MapCarrier                         carrier(headers);

  auto current_ctx1 = opentelemetry::context::RuntimeContext::GetCurrent();
  auto extracted    = GetPropagator()->Extract(carrier, current_ctx1);
  auto token        = opentelemetry::context::RuntimeContext::Attach(extracted);

  auto span     = opentelemetry::trace::GetSpan(opentelemetry::context::RuntimeContext::GetCurrent());
  auto span_ctx = span->GetContext();

  assert(span_ctx.IsValid());
  assert(span_ctx.IsSampled()); // flags = 01

  char tid[32] = {};
  span_ctx.trace_id().ToLowerBase16({tid, 32});
  assert(std::string_view(tid, 32) == trace_id);

  char sid[16] = {};
  span_ctx.span_id().ToLowerBase16({sid, 16});
  assert(std::string_view(sid, 16) == span_id);

  opentelemetry::context::RuntimeContext::Detach(*token);
}

// Test: After Detach, the span context is no longer active — mirroring what
// OtelServerInterceptor does in PRE_SEND_STATUS.
void TestContextIsRestoredAfterDetach() {
  SetW3CPropagator();

  // Before attach: no span should be active.
  auto before = opentelemetry::trace::GetSpan(opentelemetry::context::RuntimeContext::GetCurrent());
  assert(!before->GetContext().IsValid());

  std::map<std::string, std::string> headers = {{"traceparent", "00-aabbccddeeff00112233445566778899-0102030405060708-01"}};
  MapCarrier                         carrier(headers);

  auto current_ctx2 = opentelemetry::context::RuntimeContext::GetCurrent();
  auto extracted    = GetPropagator()->Extract(carrier, current_ctx2);
  auto token        = opentelemetry::context::RuntimeContext::Attach(extracted);

  assert(opentelemetry::trace::GetSpan(opentelemetry::context::RuntimeContext::GetCurrent())->GetContext().IsValid());

  opentelemetry::context::RuntimeContext::Detach(*token);

  // After detach the span context must be invalid again.
  auto after = opentelemetry::trace::GetSpan(opentelemetry::context::RuntimeContext::GetCurrent());
  assert(!after->GetContext().IsValid());
}

// Test: Without an active span, Inject does not emit a traceparent header.
// This verifies that the noop span's invalid SpanContext is not serialized.
void TestNoInjectionWithoutActiveSpan() {
  SetW3CPropagator();

  std::map<std::string, std::string> headers;
  MapCarrier                         carrier(headers);
  GetPropagator()->Inject(carrier, opentelemetry::context::RuntimeContext::GetCurrent());

  assert(headers.count("traceparent") == 0);
}

// Test: When a span context is active, Inject writes a traceparent header
// containing the correct trace-id.  This mirrors the client-side InjectTraceContext.
void TestInjectsTraceparentFromActiveContext() {
  SetW3CPropagator();

  // Seed an active context by extracting a known traceparent.
  const std::string                  seed_trace_id = "aabbccddeeff00112233445566778899";
  std::map<std::string, std::string> seed          = {{"traceparent", "00-" + seed_trace_id + "-0102030405060708-01"}};
  MapCarrier                         seed_carrier(seed);
  auto                               current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
  auto                               ctx         = GetPropagator()->Extract(seed_carrier, current_ctx);
  auto                               token       = opentelemetry::context::RuntimeContext::Attach(ctx);

  std::map<std::string, std::string> out;
  MapCarrier                         out_carrier(out);
  GetPropagator()->Inject(out_carrier, opentelemetry::context::RuntimeContext::GetCurrent());

  assert(out.count("traceparent") == 1);
  assert(out.at("traceparent").find(seed_trace_id) != std::string::npos);

  opentelemetry::context::RuntimeContext::Detach(*token);
}

// Test: Carrier Get returns the correct value for a known key and empty for
// a missing key, matching the contract the server-side GrpcMetadataCarrier exposes.
void TestCarrierGetBehavior() {
  std::map<std::string, std::string> headers = {{"traceparent", "some-value"}, {"tracestate", "vendor=info"}};
  MapCarrier                         carrier(headers);

  assert(carrier.Get("traceparent") == "some-value");
  assert(carrier.Get("tracestate") == "vendor=info");
  assert(carrier.Get("x-unknown-header") == "");
}

// Test: Carrier Set writes values that are subsequently readable via Get.
// This mirrors the client-side GrpcClientMetadataCarrier write path.
void TestCarrierSetBehavior() {
  std::map<std::string, std::string> headers;
  MapCarrier                         carrier(headers);

  carrier.Set("traceparent", "00-abc-def-01");
  carrier.Set("tracestate", "k=v");

  assert(carrier.Get("traceparent") == "00-abc-def-01");
  assert(carrier.Get("tracestate") == "k=v");
}

// Test: OtelServerInterceptorFactory creates a non-null interceptor.
void TestInterceptorFactoryCreatesNonNullInterceptor() {
  payload::grpc::OtelServerInterceptorFactory factory;
  // Passing nullptr is safe here; the interceptor only stores the pointer and
  // we never call Intercept(), so info_ is never dereferenced.
  auto* interceptor = factory.CreateServerInterceptor(nullptr);
  assert(interceptor != nullptr);
  delete interceptor;
}

// Test: NotSampled flag in traceparent is propagated correctly.
void TestNotSampledFlagIsPropagated() {
  SetW3CPropagator();

  std::map<std::string, std::string> headers = {{"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"}};
  MapCarrier                         carrier(headers);

  auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
  auto extracted   = GetPropagator()->Extract(carrier, current_ctx);
  auto token       = opentelemetry::context::RuntimeContext::Attach(extracted);

  auto span_ctx = opentelemetry::trace::GetSpan(opentelemetry::context::RuntimeContext::GetCurrent())->GetContext();
  assert(span_ctx.IsValid());
  assert(!span_ctx.IsSampled()); // flags = 00

  opentelemetry::context::RuntimeContext::Detach(*token);
}

} // namespace

#endif // ENABLE_OTEL

int main() {
#ifdef ENABLE_OTEL
  TestExtractsTraceparent();
  TestContextIsRestoredAfterDetach();
  TestNoInjectionWithoutActiveSpan();
  TestInjectsTraceparentFromActiveContext();
  TestCarrierGetBehavior();
  TestCarrierSetBehavior();
  TestInterceptorFactoryCreatesNonNullInterceptor();
  TestNotSampledFlagIsPropagated();
#endif

  std::cout << "payload_manager_unit_otel_interceptor: pass\n";
  return 0;
}
