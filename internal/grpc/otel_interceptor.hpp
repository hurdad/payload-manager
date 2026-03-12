#pragma once

#include <grpcpp/support/server_interceptor.h>

namespace payload::grpc {

#ifdef ENABLE_OTEL

class OtelServerInterceptorFactory : public ::grpc::experimental::ServerInterceptorFactoryInterface {
 public:
  ::grpc::experimental::Interceptor* CreateServerInterceptor(::grpc::experimental::ServerRpcInfo* info) override;
};

#endif // ENABLE_OTEL

} // namespace payload::grpc
