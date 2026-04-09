#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>

#include "client/cpp/client.h"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"
#include "traced_channel.hpp"

int main(int argc, char** argv) {
  // argv[1]: UUID of the payload to read (required)
  // argv[2]: server endpoint             (default localhost:50051)
  // argv[3]: OTLP gRPC endpoint          (default localhost:4317, empty to disable)
  if (argc < 2) {
    std::cerr << "usage: read_example <uuid> [endpoint] [otlp-endpoint]\n";
    return 1;
  }

  const std::string uuid    = argv[1];
  const std::string target  = argc > 2 ? argv[2] : "localhost:50051";
  const std::string otlp_ep = argc > 3 ? argv[3] : "localhost:4317";

  OtelInit(otlp_ep, "cpp-examples");
  auto                                    channel = StartSpanAndMakeChannel(target, "read_example");
  payload::manager::client::PayloadClient client(channel);

  auto payload_id_result = payload::manager::client::PayloadClient::PayloadIdFromUuid(uuid);
  if (!payload_id_result.ok()) {
    std::cerr << "Invalid UUID: " << payload_id_result.status().ToString() << '\n';
    OtelShutdown();
    return 1;
  }
  const auto payload_id = payload_id_result.ValueOrDie();

  auto readable = client.AcquireReadableBuffer(payload_id);
  if (!readable.ok()) {
    std::cerr << "AcquireReadableBuffer failed: " << readable.status().ToString() << '\n';
    OtelShutdown();
    return 1;
  }

  auto       readable_payload = readable.ValueOrDie();
  const auto size             = readable_payload.buffer->size();
  const auto data             = readable_payload.buffer->data();

  std::cout << "UUID=" << uuid << ", size=" << size << " bytes\n";

  // Verify the incrementing sequence written by allocate_example.
  int mismatches = 0;
  for (int64_t i = 0; i < size; ++i) {
    const uint8_t expected = static_cast<uint8_t>(i & 0xFFu);
    if (data[i] != expected) {
      std::cerr << "mismatch at byte " << i << ": expected " << static_cast<int>(expected) << " got " << static_cast<int>(data[i]) << '\n';
      ++mismatches;
    }
  }

  if (mismatches == 0) {
    std::cout << "verify: OK (" << size << " bytes match incrementing sequence)\n";
  } else {
    std::cout << "verify: FAIL (" << mismatches << " mismatches)\n";
  }

  auto release_status = client.Release(readable_payload.lease_id);
  if (!release_status.ok()) {
    std::cerr << "Release failed: " << release_status.ToString() << '\n';
    OtelShutdown();
    return 1;
  }

  OtelEndSpan();
  OtelShutdown();
  return mismatches == 0 ? 0 : 1;
}
