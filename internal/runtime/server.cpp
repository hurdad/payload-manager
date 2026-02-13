#include "server.hpp"

#include <utility>

namespace payload::runtime {

Server::Server(std::string bind_address, std::vector<std::unique_ptr<grpc::Service>> services)
    : bind_address_(std::move(bind_address)), services_(std::move(services)) {}

Server::~Server() { Stop(); }

void Server::Start() {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(bind_address_, grpc::InsecureServerCredentials());
  for (const auto& service : services_) {
    builder.RegisterService(service.get());
  }

  grpc_server_ = builder.BuildAndStart();
  if (!grpc_server_) {
    throw std::runtime_error("Failed to start gRPC server");
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
