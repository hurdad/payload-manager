#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/service_type.h>

#include <memory>
#include <string>
#include <vector>

namespace payload::runtime {

class Server {
 public:
  /// Listens on every address in `bind_addresses` with `credentials`.
  ///
  /// Neither is derived from config here — see internal/runtime/credentials.hpp
  /// for that — so a test can start a server without a RuntimeConfig, and so
  /// the "what do we listen with" decision has one home.
  Server(std::vector<std::string> bind_addresses, std::shared_ptr<grpc::ServerCredentials> credentials,
         std::vector<std::unique_ptr<grpc::Service>> services);
  ~Server();

  Server(const Server&)            = delete;
  Server& operator=(const Server&) = delete;

  void Start();
  void Wait();
  void Stop();

 private:
  std::vector<std::string>                    bind_addresses_;
  std::shared_ptr<grpc::ServerCredentials>    credentials_;
  std::vector<std::unique_ptr<grpc::Service>> services_;
  std::unique_ptr<grpc::Server>               grpc_server_;
};

} // namespace payload::runtime
