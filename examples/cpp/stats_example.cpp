#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include "client/cpp/payload_manager_client.h"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"
#include "traced_channel.hpp"

int main(int argc, char** argv) {
  // argv[1]: server endpoint  (default localhost:50051)
  // argv[2]: OTLP gRPC endpoint (default localhost:4317, empty to disable)
  const std::string target  = argc > 1 ? argv[1] : "localhost:50051";
  const std::string otlp_ep = argc > 2 ? argv[2] : "localhost:4317";

  OtelInit(otlp_ep, "cpp-examples");
  auto                                    channel = StartSpanAndMakeChannel(target, "stats_example");
  payload::manager::client::PayloadClient client(channel);

  // Stats returns a tier-wise summary of payload counts and byte usage.
  payload::manager::v1::StatsRequest request;
  auto                               result = client.Stats(request);
  if (!result.ok()) {
    std::cerr << "Stats RPC failed: " << result.status().ToString() << '\n';
    return 1;
  }

  const auto& stats = result.ValueOrDie();
  std::cout << "Payload Manager stats for " << target << '\n';
  std::cout << "payload counts: gpu=" << stats.payloads_gpu() << ", ram=" << stats.payloads_ram() << ", disk=" << stats.payloads_disk() << '\n';
  std::cout << "bytes: gpu=" << stats.bytes_gpu() << ", ram=" << stats.bytes_ram() << ", disk=" << stats.bytes_disk() << '\n';

  OtelEndSpan();
  OtelShutdown();
  return 0;
}
