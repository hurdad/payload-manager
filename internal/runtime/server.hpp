#pragma once

#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/service_type.h>

namespace payload::runtime {

class Server {
public:
  Server(std::string bind_address, std::vector<std::unique_ptr<grpc::Service>> services);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  void Start();
  void Wait();
  void Stop();

private:
  std::string bind_address_;
  std::vector<std::unique_ptr<grpc::Service>> services_;
  std::unique_ptr<grpc::Server> grpc_server_;
};

} // namespace payload::runtime
