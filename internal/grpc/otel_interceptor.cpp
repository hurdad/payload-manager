#include "otel_interceptor.hpp"

#ifdef ENABLE_OTEL

#include <grpcpp/server_context.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/nostd/string_view.h>
#include <opentelemetry/nostd/unique_ptr.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>

namespace payload::grpc {

namespace {

class GrpcMetadataCarrier : public opentelemetry::context::propagation::TextMapCarrier {
 public:
  explicit GrpcMetadataCarrier(const ::grpc::ServerContextBase* ctx) : ctx_(ctx) {
  }

  opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override {
    const auto& metadata = ctx_->client_metadata();
    const auto  it       = metadata.find({key.data(), key.size()});
    if (it != metadata.end()) {
      return opentelemetry::nostd::string_view(it->second.data(), it->second.size());
    }
    return opentelemetry::nostd::string_view();
  }

  void Set(opentelemetry::nostd::string_view, opentelemetry::nostd::string_view) noexcept override {
  }

 private:
  const ::grpc::ServerContextBase* ctx_;
};

class OtelServerInterceptor : public ::grpc::experimental::Interceptor {
 public:
  explicit OtelServerInterceptor(::grpc::experimental::ServerRpcInfo* info) : info_(info) {
  }

  void Intercept(::grpc::experimental::InterceptorBatchMethods* methods) override {
    if (methods->QueryInterceptionHookPoint(::grpc::experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
      GrpcMetadataCarrier carrier(info_->server_context());
      auto                propagator  = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
      auto                current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
      auto                ctx         = propagator->Extract(carrier, current_ctx);
      token_                          = opentelemetry::context::RuntimeContext::Attach(ctx);
    }

    if (methods->QueryInterceptionHookPoint(::grpc::experimental::InterceptionHookPoints::PRE_SEND_STATUS)) {
      if (token_) {
        opentelemetry::context::RuntimeContext::Detach(*token_);
      }
    }

    methods->Proceed();
  }

 private:
  ::grpc::experimental::ServerRpcInfo*                            info_;
  opentelemetry::nostd::unique_ptr<opentelemetry::context::Token> token_;
};

} // namespace

::grpc::experimental::Interceptor* OtelServerInterceptorFactory::CreateServerInterceptor(::grpc::experimental::ServerRpcInfo* info) {
  return new OtelServerInterceptor(info);
}

} // namespace payload::grpc

#endif // ENABLE_OTEL
