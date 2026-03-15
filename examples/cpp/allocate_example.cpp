#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "client/cpp/payload_manager_client.h"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"
#include "traced_channel.hpp"

namespace {

std::string UuidToHex(const std::string& uuid_bytes) {
  std::ostringstream os;
  for (size_t i = 0; i < uuid_bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) os << '-';
    os << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(static_cast<unsigned char>(uuid_bytes[i]));
  }
  return os.str();
}

} // namespace

int main(int argc, char** argv) {
  // argv[1]: server endpoint    (default localhost:50051)
  // argv[2]: payload size bytes (default 64)
  // argv[3]: OTLP gRPC endpoint (default localhost:4317, empty to disable)
  const std::string target     = argc > 1 ? argv[1] : "localhost:50051";
  const uint64_t    size_bytes = argc > 2 ? static_cast<uint64_t>(std::stoull(argv[2])) : 100ULL * 1024 * 1024;
  const std::string otlp_ep    = argc > 3 ? argv[3] : "localhost:4317";

  OtelInit(otlp_ep, "cpp-examples");
  auto                                    channel = StartSpanAndMakeChannel(target, "allocate_example");
  payload::manager::client::PayloadClient client(channel);

  auto writable = client.AllocateWritableBuffer(size_bytes, payload::manager::v1::TIER_RAM);
  if (!writable.ok()) {
    std::cerr << "AllocateWritableBuffer failed: " << writable.status().ToString() << '\n';
    OtelShutdown();
    return 1;
  }

  auto writable_payload = writable.ValueOrDie();

  // Fill buffer with a simple incrementing pattern.
  for (uint64_t i = 0; i < size_bytes; ++i) {
    writable_payload.buffer->mutable_data()[i] = static_cast<uint8_t>(i & 0xFFu);
  }

  const auto& payload_id = writable_payload.descriptor.payload_id();
  auto        status     = client.CommitPayload(payload_id);
  if (!status.ok()) {
    std::cerr << "CommitPayload failed: " << status.ToString() << '\n';
    OtelShutdown();
    return 1;
  }

  std::cout << UuidToHex(payload_id.value()) << '\n';

  OtelEndSpan();
  OtelShutdown();
  return 0;
}
