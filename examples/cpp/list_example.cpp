#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>

#include "client/cpp/payload_manager_client.h"
#include "example_util.hpp"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"
#include "traced_channel.hpp"

namespace {

const char* TierName(payload::manager::v1::Tier t) {
  switch (t) {
    case payload::manager::v1::TIER_GPU:
      return "gpu";
    case payload::manager::v1::TIER_RAM:
      return "ram";
    case payload::manager::v1::TIER_DISK:
      return "disk";
    default:
      return "?";
  }
}

const char* StateName(payload::manager::v1::PayloadState s) {
  switch (s) {
    case payload::manager::v1::PAYLOAD_STATE_ALLOCATED:
      return "allocated";
    case payload::manager::v1::PAYLOAD_STATE_ACTIVE:
      return "active";
    case payload::manager::v1::PAYLOAD_STATE_SPILLING:
      return "spilling";
    case payload::manager::v1::PAYLOAD_STATE_DURABLE:
      return "durable";
    case payload::manager::v1::PAYLOAD_STATE_EVICTING:
      return "evicting";
    case payload::manager::v1::PAYLOAD_STATE_DELETING:
      return "deleting";
    default:
      return "?";
  }
}

} // namespace

int main(int argc, char** argv) {
  // argv[1]: server endpoint   (default localhost:50051)
  // argv[2]: tier filter       (ram|disk|gpu|all, default all)
  // argv[3]: OTLP gRPC endpoint (default localhost:4317, empty to disable)
  const std::string target   = argc > 1 ? argv[1] : "localhost:50051";
  const std::string tier_arg = argc > 2 ? argv[2] : "all";
  const std::string otlp_ep  = argc > 3 ? argv[3] : "localhost:4317";

  OtelInit(otlp_ep, "cpp-examples");
  auto                                    channel = StartSpanAndMakeChannel(target, "list_example");
  payload::manager::client::PayloadClient client(channel);

  payload::manager::v1::ListPayloadsRequest req;
  if (tier_arg == "ram")
    req.set_tier_filter(payload::manager::v1::TIER_RAM);
  else if (tier_arg == "disk")
    req.set_tier_filter(payload::manager::v1::TIER_DISK);
  else if (tier_arg == "gpu")
    req.set_tier_filter(payload::manager::v1::TIER_GPU);

  auto result = client.ListPayloads(req);
  if (!result.ok()) {
    std::cerr << "ListPayloads failed: " << result.status().ToString() << '\n';
    OtelShutdown();
    return 1;
  }

  const auto& resp = result.ValueOrDie();
  const auto  now_ms =
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());

  std::cout << std::left << std::setw(38) << "UUID" << std::setw(6) << "TIER" << std::setw(12) << "STATE" << std::setw(14) << "SIZE" << std::setw(10)
            << "AGE(s)" << std::setw(9) << "TTL(s)" << std::setw(8) << "LEASES" << "\n";

  for (const auto& p : resp.payloads()) {
    const std::string uuid = payload::examples::UuidToHex(p.id().value());

    const std::string age = p.created_at_ms() > 0 ? std::to_string((now_ms - p.created_at_ms()) / 1000) : "?";

    const std::string ttl = p.expires_at_ms() > 0 ? std::to_string(static_cast<int64_t>(p.expires_at_ms() - now_ms) / 1000) : "-";

    std::cout << std::left << std::setw(38) << uuid << std::setw(6) << TierName(p.tier()) << std::setw(12) << StateName(p.state()) << std::setw(14)
              << p.size_bytes() << std::setw(10) << age << std::setw(9) << ttl << std::setw(8) << p.active_leases() << "\n";
  }

  std::cout << resp.payloads_size() << " payload(s)\n";

  OtelEndSpan();
  OtelShutdown();
  return 0;
}
