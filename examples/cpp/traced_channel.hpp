#pragma once
// gRPC client-channel helpers for W3C traceparent propagation.
// Requires NO OTel SDK headers — only gRPC client headers.
// Include alongside otel_tracer.hpp in examples that want tracing.
//
// When ENABLE_OTEL is not defined, StartSpanAndMakeChannel returns a plain
// channel so the examples work without any OTel dependency.
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <memory>
#include <string>

#include "client/cpp/channel.h"
#include "otel_tracer.hpp"

#ifdef ENABLE_OTEL

#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/interceptor.h>

#include <vector>

// Build a W3C traceparent string from an OtelSpanContext.
inline std::string MakeTraceparent(const OtelSpanContext& ctx) {
  return "00-" + ctx.trace_id_hex + "-" + ctx.span_id_hex + "-01";
}

// Injects a fixed W3C traceparent into every outgoing RPC's initial metadata.
class TraceInjectInterceptor : public grpc::experimental::Interceptor {
 public:
  explicit TraceInjectInterceptor(std::string tp) : tp_(std::move(tp)) {
  }

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override {
    if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      methods->GetSendInitialMetadata()->emplace("traceparent", tp_);
    }
    methods->Proceed();
  }

 private:
  std::string tp_;
};

class TraceInjectFactory : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  explicit TraceInjectFactory(std::string tp) : tp_(std::move(tp)) {
  }

  grpc::experimental::Interceptor* CreateClientInterceptor(grpc::experimental::ClientRpcInfo*) override {
    return new TraceInjectInterceptor(tp_);
  }

 private:
  std::string tp_;
};

// Returns a gRPC channel that injects traceparent into every RPC.
//
// Credentials come from payload::client::ChannelOptions rather than being
// hardcoded insecure, so an example run against a TLS deployment works by
// setting PAYLOAD_MANAGER_TLS_CA in the environment, with no edit here. The
// interceptor plumbing is why this cannot simply call MakeChannel: the
// interceptor variant of the factory function takes its own credentials
// argument.
inline std::shared_ptr<grpc::Channel> MakeTracedChannel(const std::string& endpoint, const std::string& traceparent) {
  grpc::ChannelArguments                                                              args;
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> factories;
  factories.push_back(std::make_unique<TraceInjectFactory>(traceparent));
  return grpc::experimental::CreateCustomChannelWithInterceptors(endpoint, payload::client::ChannelCredentialsFromEnvironment(args), args,
                                                                 std::move(factories));
}

// Start a named root span and return a traced channel carrying its traceparent.
// Falls back to a plain channel if the span context is invalid (e.g. OtelInit
// was called with an empty endpoint).
inline std::shared_ptr<grpc::Channel> StartSpanAndMakeChannel(const std::string& endpoint, const std::string& span_name) {
  auto ctx = OtelStartSpan(span_name);
  if (!ctx.valid) {
    return payload::client::MakeChannel(endpoint);
  }
  return MakeTracedChannel(endpoint, MakeTraceparent(ctx));
}

#else // !ENABLE_OTEL

// No tracing: start a (no-op) span and return a plain channel.
inline std::shared_ptr<grpc::Channel> StartSpanAndMakeChannel(const std::string& endpoint, const std::string& /*span_name*/) {
  OtelStartSpan(/*no-op*/ "");
  return payload::client::MakeChannel(endpoint);
}

#endif // ENABLE_OTEL
