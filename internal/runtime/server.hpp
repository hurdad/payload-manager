#pragma once

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

namespace payload::runtime {

class Server {
public:
  explicit Server(std::string bind_address);
  ~Server();

  // Non-copyable
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  void Start();
  void Wait();
  void Shutdown();

private:
  std::string bind_address_;
  std::unique_ptr<grpc::Server> grpc_server_;
};

} // namespace payload::runtime
