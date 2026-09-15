#include "server.hpp"

#include <utility>

#include "internal/grpc/otel_interceptor.hpp"

namespace payload::runtime {

Server::Server(std::vector<std::string> bind_addresses, std::shared_ptr<::grpc::ServerCredentials> credentials,
               std::vector<std::unique_ptr<::grpc::Service>> services)
    : bind_addresses_(std::move(bind_addresses)), credentials_(std::move(credentials)), services_(std::move(services)) {
}

Server::~Server() {
  Stop();
}

void Server::Start() {
  ::grpc::ServerBuilder builder;

  // One credential set across every address: a Unix socket and a TCP port on
  // the same process should not differ in what they demand of a caller.
  //
  // selected_port is deliberately not requested. It is 0 for a Unix socket, so
  // a caller checking it for success would reject every socket bind; the real
  // signal is BuildAndStart returning null, which is checked below.
  for (const auto& address : bind_addresses_) {
    builder.AddListeningPort(address, credentials_);
  }

  for (const auto& service : services_) {
    builder.RegisterService(service.get());
  }

#ifdef ENABLE_OTEL
  {
    std::vector<std::unique_ptr<::grpc::experimental::ServerInterceptorFactoryInterface>> interceptors;
    interceptors.push_back(std::make_unique<payload::grpc::OtelServerInterceptorFactory>());
    builder.experimental().SetInterceptorCreators(std::move(interceptors));
  }
#endif

  grpc_server_ = builder.BuildAndStart();
  if (!grpc_server_) {
    // BuildAndStart swallows the reason, so name what was attempted. The usual
    // causes are a port already in use, a socket path in a directory the
    // process cannot write, and a certificate that does not match its key.
    std::string attempted;
    for (const auto& address : bind_addresses_) {
      if (!attempted.empty()) attempted += ", ";
      attempted += address;
    }
    throw std::runtime_error("Failed to start gRPC server on: " + attempted);
  }
}

void Server::Wait() {
  if (grpc_server_) {
    grpc_server_->Wait();
  }
}

void Server::Stop() {
  if (grpc_server_) {
    grpc_server_->Shutdown();
    grpc_server_.reset();
  }
}

} // namespace payload::runtime
