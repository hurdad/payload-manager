#include <grpcpp/grpcpp.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>

#include "payload/manager/services/v1/payload_admin_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_catalog_service.grpc.pb.h"
#include "payload/manager/services/v1/payload_data_service.grpc.pb.h"
#include "payload/manager/v1.hpp"

using namespace payload::manager::v1;

static void Usage() {
  std::cout << "Usage:\n"
            << "  payloadctl <addr> allocate <size_bytes> [tier=ram|disk|gpu]\n"
            << "  payloadctl <addr> commit <uuid>\n"
            << "  payloadctl <addr> resolve <uuid>\n"
            << "  payloadctl <addr> lease <uuid>\n"
            << "  payloadctl <addr> release <lease_id>\n"
            << "  payloadctl <addr> delete <uuid>\n"
            << "  payloadctl <addr> promote <uuid> <tier=ram|disk|gpu>\n"
            << "  payloadctl <addr> spill <uuid>\n"
            << "  payloadctl <addr> stats\n";
}

static int HexNibble(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
  if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
  return -1;
}

static PayloadID MakeID(const std::string& s) {
  // Strip dashes and collect hex characters.
  std::string hex;
  hex.reserve(32);
  for (char c : s) {
    if (c == '-') continue;
    if (HexNibble(c) < 0) {
      std::cerr << "invalid uuid: non-hex character in '" << s << "'\n";
      std::exit(1);
    }
    hex.push_back(c);
  }
  if (hex.size() != 32) {
    std::cerr << "invalid uuid: expected 32 hex chars, got " << hex.size() << "\n";
    std::exit(1);
  }

  // Convert 32 hex chars to 16 raw bytes.
  std::string bytes(16, '\0');
  for (size_t i = 0; i < 16; ++i) {
    bytes[i] = static_cast<char>((HexNibble(hex[2 * i]) << 4) | HexNibble(hex[2 * i + 1]));
  }

  PayloadID id;
  id.set_value(bytes);
  return id;
}

static std::optional<Tier> ParseTier(const std::string& value) {
  if (value == "ram") {
    return TIER_RAM;
  }
  if (value == "disk") {
    return TIER_DISK;
  }
  if (value == "gpu") {
    return TIER_GPU;
  }
  return std::nullopt;
}

int main(int argc, char** argv) {
  if (argc < 3) {
    Usage();
    return 1;
  }

  std::string addr = argv[1];
  std::string cmd  = argv[2];

  auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());

  auto data_stub    = PayloadDataService::NewStub(channel);
  auto catalog_stub = PayloadCatalogService::NewStub(channel);
  auto admin_stub   = PayloadAdminService::NewStub(channel);

  grpc::ClientContext ctx;

  // ------------------------------------------------------------

  if (cmd == "allocate") {
    if (argc < 4) return 1;

    uint64_t size_bytes = std::stoull(argv[3]);

    Tier preferred_tier = TIER_RAM;
    if (argc >= 5) {
      auto parsed = ParseTier(argv[4]);
      if (!parsed.has_value()) {
        std::cerr << "unsupported tier: " << argv[4] << "\n";
        return 1;
      }
      preferred_tier = parsed.value();
    }

    AllocatePayloadRequest req;
    req.set_size_bytes(size_bytes);
    req.set_preferred_tier(preferred_tier);

    AllocatePayloadResponse resp;

    auto status = catalog_stub->AllocatePayload(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "tier=" << resp.payload_descriptor().tier() << "\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "commit") {
    if (argc < 4) return 1;

    CommitPayloadRequest req;
    *req.mutable_id() = MakeID(argv[3]);

    CommitPayloadResponse resp;

    auto status = catalog_stub->CommitPayload(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "committed\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "resolve") {
    if (argc < 4) return 1;

    ResolveSnapshotRequest req;
    *req.mutable_id() = MakeID(argv[3]);

    ResolveSnapshotResponse resp;

    auto status = data_stub->ResolveSnapshot(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "tier=" << resp.payload_descriptor().tier() << "\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "lease") {
    if (argc < 4) return 1;

    AcquireReadLeaseRequest req;
    *req.mutable_id() = MakeID(argv[3]);
    req.set_min_lease_duration_ms(5000);

    AcquireReadLeaseResponse resp;

    auto status = data_stub->AcquireReadLease(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "lease=" << resp.lease_id() << "\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "delete") {
    if (argc < 4) return 1;

    DeleteRequest req;
    *req.mutable_id() = MakeID(argv[3]);

    google::protobuf::Empty resp;

    auto status = catalog_stub->Delete(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "deleted\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "release") {
    if (argc < 4) return 1;

    ReleaseLeaseRequest req;
    req.set_lease_id(argv[3]);

    google::protobuf::Empty resp;

    auto status = data_stub->ReleaseLease(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "released\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "promote") {
    if (argc < 5) return 1;

    auto parsed = ParseTier(argv[4]);
    if (!parsed.has_value()) {
      std::cerr << "unsupported tier: " << argv[4] << "\n";
      return 1;
    }

    PromoteRequest req;
    *req.mutable_id() = MakeID(argv[3]);
    req.set_target_tier(parsed.value());
    req.set_policy(PROMOTION_POLICY_BEST_EFFORT);

    PromoteResponse resp;

    auto status = catalog_stub->Promote(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "tier=" << resp.payload_descriptor().tier() << "\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "spill") {
    if (argc < 4) return 1;

    SpillRequest req;
    *req.add_ids() = MakeID(argv[3]);
    req.set_policy(SPILL_POLICY_BEST_EFFORT);

    SpillResponse resp;

    auto status = catalog_stub->Spill(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "results=" << resp.results_size() << "\n";
    return 0;
  }

  // ------------------------------------------------------------

  if (cmd == "stats") {
    StatsRequest  req;
    StatsResponse resp;

    auto status = admin_stub->Stats(&ctx, req, &resp);

    if (!status.ok()) {
      std::cerr << status.error_message() << "\n";
      return 2;
    }

    std::cout << "ram=" << resp.payloads_ram() << "\n";
    std::cout << "disk=" << resp.payloads_disk() << "\n";
    std::cout << "gpu=" << resp.payloads_gpu() << "\n";
    return 0;
  }

  Usage();
  return 1;
}
