#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "payload_manager_service.h"

int main(int argc, char** argv) {
  std::string server_address = "0.0.0.0:50051";
  if (argc > 1) {
    server_address = argv[1];
  }

  PayloadManagerServiceImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (!server) {
    std::cerr << "Failed to start payload manager service" << std::endl;
    return 1;
  }

  std::cout << "PayloadManager service listening on " << server_address << std::endl;
  server->Wait();
  return 0;
}
