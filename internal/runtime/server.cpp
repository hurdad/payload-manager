#include "server.hpp"

#include <iostream>
#include <csignal>

// gRPC service implementations (transport adapters)
#include "internal/grpc/data_server.hpp"
#include "internal/grpc/catalog_server.hpp"
#include "internal/grpc/admin_server.hpp"

namespace payload::runtime {

Server::Server(std::string bind_address)
    : bind_address_(std::move(bind_address)) {}

Server::~Server() {
  Shutdown();
}

void Server::Start() {
  grpc::ServerBuilder builder;

  builder.AddListeningPort(bind_address_, grpc::InsecureServerCredentials());

  // Register gRPC services (thin adapters)
  static payload::grpc::DataServer data_service;
  static payload::grpc::CatalogServer catalog_service;
  static payload::grpc::AdminServer admin_service;

  builder.RegisterService(&data_service);
  builder.RegisterService(&catalog_service);
  builder.RegisterService(&admin_service);

  grpc_server_ = builder.BuildAndStart();

  if (!grpc_server_) {
    throw std::runtime_error("Failed to start gRPC server");
  }

  std::cout << "Payload Manager listening on " << bind_address_ << std::endl;
}

void Server::Wait() {
  if (grpc_server_)
    grpc_server_->Wait();
}

void Server::Shutdown() {
  if (grpc_server_) {
    grpc_server_->Shutdown();
    grpc_server_.reset();
  }
}

} // namespace payload::runtime
