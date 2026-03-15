#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "client/cpp/payload_manager_client.h"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"
#include "traced_channel.hpp"

int main(int argc, char** argv) {
  // argv[1]: UUID of the payload to spill (required)
  // argv[2]: server endpoint              (default localhost:50051)
  // argv[3]: OTLP gRPC endpoint           (default localhost:4317, empty to disable)
  if (argc < 2) {
    std::cerr << "usage: spill_example <uuid> [endpoint] [otlp-endpoint]\n";
    return 1;
  }

  const std::string uuid    = argv[1];
  const std::string target  = argc > 2 ? argv[2] : "localhost:50051";
  const std::string otlp_ep = argc > 3 ? argv[3] : "localhost:4317";

  OtelInit(otlp_ep, "cpp-examples");
  auto                                    channel = StartSpanAndMakeChannel(target, "spill_example");
  payload::manager::client::PayloadClient client(channel);

  auto payload_id_result = payload::manager::client::PayloadClient::PayloadIdFromUuid(uuid);
  if (!payload_id_result.ok()) {
    std::cerr << "Invalid UUID: " << payload_id_result.status().ToString() << '\n';
    OtelShutdown();
    return 1;
  }
  const auto payload_id = payload_id_result.ValueOrDie();

  // Spill RAM → disk.
  payload::manager::v1::SpillRequest spill_req;
  *spill_req.add_ids() = payload_id;
  spill_req.set_fsync(true);
  auto spill_resp = client.Spill(spill_req);
  if (!spill_resp.ok()) {
    std::cerr << "Spill RPC failed: " << spill_resp.status().ToString() << '\n';
    OtelShutdown();
    return 1;
  }
  if (spill_resp->results_size() < 1 || !spill_resp->results(0).ok()) {
    std::cerr << "Spill rejected: " << spill_resp->results(0).error_message() << '\n';
    OtelShutdown();
    return 1;
  }
  std::cout << "spill: OK (UUID=" << uuid << " is now on disk)\n";

  // Promote back to RAM so we can acquire a readable buffer.
  payload::manager::v1::PromoteRequest prom_req;
  *prom_req.mutable_id() = payload_id;
  prom_req.set_target_tier(payload::manager::v1::TIER_RAM);
  auto prom_resp = client.Promote(prom_req);
  if (!prom_resp.ok()) {
    std::cerr << "Promote RPC failed: " << prom_resp.status().ToString() << '\n';
    OtelShutdown();
    return 1;
  }
  std::cout << "promote: OK (back in RAM)\n";

  // Read and verify the incrementing sequence written by allocate_example.
  auto readable = client.AcquireReadableBuffer(payload_id);
  if (!readable.ok()) {
    std::cerr << "AcquireReadableBuffer failed: " << readable.status().ToString() << '\n';
    OtelShutdown();
    return 1;
  }

  auto       readable_payload = readable.ValueOrDie();
  const auto size             = readable_payload.buffer->size();
  const auto data             = readable_payload.buffer->data();

  std::cout << "read: " << size << " bytes\n";

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
