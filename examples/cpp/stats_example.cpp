#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "client/cpp/payload_client.h"

int main(int argc, char** argv) {
  const std::string target = argc > 1 ? argv[1] : "localhost:50051";

  payload::manager::client::PayloadClient client(
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));

  payload::manager::v1::StatsRequest request;
  auto result = client.Stats(request);
  if (!result.ok()) {
    std::cerr << "Stats RPC failed: " << result.status().ToString() << '\n';
    return 1;
  }

  const auto& stats = result.ValueOrDie();
  std::cout << "Payload Manager stats for " << target << '\n';
  std::cout << "payload counts: gpu=" << stats.payloads_gpu() << ", ram=" << stats.payloads_ram()
            << ", disk=" << stats.payloads_disk() << '\n';
  std::cout << "bytes: gpu=" << stats.bytes_gpu() << ", ram=" << stats.bytes_ram()
            << ", disk=" << stats.bytes_disk() << '\n';

  return 0;
}
