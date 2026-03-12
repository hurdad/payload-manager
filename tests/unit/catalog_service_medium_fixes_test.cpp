/*
  Unit tests for medium-priority fixes in CatalogService:

  - Allocate with TIER_UNSPECIFIED is rejected with InvalidState
  - Promote with TIER_UNSPECIFIED is rejected with InvalidState
  - Spill uses per-payload GetSpillTarget instead of hardcoded TIER_DISK
*/

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/storage/storage_backend.hpp"
#include "payload/manager/core/v1/policy.pb.h"
#include "payload/manager/v1.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::core::v1::EVICTION_PRIORITY_UNSPECIFIED;
using payload::manager::core::v1::EvictionPolicy;
using payload::manager::v1::AllocatePayloadRequest;
using payload::manager::v1::PayloadID;
using payload::manager::v1::PromoteRequest;
using payload::manager::v1::SpillRequest;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_OBJECT;
using payload::manager::v1::TIER_RAM;
using payload::manager::v1::TIER_UNSPECIFIED;

// ---------------------------------------------------------------------------
// Minimal in-process storage backend
// ---------------------------------------------------------------------------

class SimpleStorageBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleStorageBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const PayloadID& id, uint64_t size_bytes) override {
    auto result = arrow::AllocateBuffer(size_bytes);
    if (!result.ok()) throw std::runtime_error(result.status().ToString());
    std::shared_ptr<arrow::Buffer> buf(std::move(*result));
    if (size_bytes > 0) std::memset(buf->mutable_data(), 0, static_cast<size_t>(size_bytes));
    buffers_[id.value()] = buf;
    return buf;
  }

  std::shared_ptr<arrow::Buffer> Read(const PayloadID& id) override {
    return buffers_.at(id.value());
  }

  void Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& buf, bool) override {
    buffers_[id.value()] = buf;
  }

  void Remove(const PayloadID& id) override {
    buffers_.erase(id.value());
  }

  bool Has(const PayloadID& id) const {
    return buffers_.count(id.value()) > 0;
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
};

// ---------------------------------------------------------------------------
// Fixture with RAM + DISK backends
// ---------------------------------------------------------------------------

struct Fixture {
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<SimpleStorageBackend>                  ram       = std::make_shared<SimpleStorageBackend>(TIER_RAM);
  std::shared_ptr<SimpleStorageBackend>                  disk      = std::make_shared<SimpleStorageBackend>(TIER_DISK);
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap storage;
    storage[TIER_RAM]  = ram;
    storage[TIER_DISK] = disk;
    return std::make_shared<PayloadManager>(storage, lease_mgr, repo);
  }()};
  payload::service::ServiceContext ctx{[&] {
    payload::service::ServiceContext c;
    c.manager    = manager;
    c.repository = repo;
    return c;
  }()};
  payload::service::CatalogService service{ctx};
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Allocate with TIER_UNSPECIFIED must throw.
void TestAllocateUnspecifiedTierThrows() {
  Fixture f;

  AllocatePayloadRequest req;
  req.set_size_bytes(64);
  req.set_preferred_tier(TIER_UNSPECIFIED);

  bool threw = false;
  try {
    f.service.Allocate(req);
  } catch (const std::exception&) {
    threw = true;
  }
  assert(threw && "Allocate with TIER_UNSPECIFIED must throw");
}

// Allocate with TIER_RAM succeeds (regression — valid tier still works).
void TestAllocateValidTierSucceeds() {
  Fixture f;

  AllocatePayloadRequest req;
  req.set_size_bytes(64);
  req.set_preferred_tier(TIER_RAM);

  bool threw = false;
  try {
    f.service.Allocate(req);
  } catch (const std::exception&) {
    threw = true;
  }
  assert(!threw && "Allocate with TIER_RAM must not throw");
}

// Promote with TIER_UNSPECIFIED must throw.
void TestPromoteUnspecifiedTierThrows() {
  Fixture f;

  // Allocate + commit a payload first.
  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_RAM);
  const auto alloc_resp = f.service.Allocate(alloc_req);

  payload::manager::v1::CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = alloc_resp.payload_descriptor().payload_id();
  f.service.Commit(commit_req);

  PromoteRequest promote_req;
  *promote_req.mutable_id() = alloc_resp.payload_descriptor().payload_id();
  promote_req.set_target_tier(TIER_UNSPECIFIED);

  bool threw = false;
  try {
    f.service.Promote(promote_req);
  } catch (const std::exception&) {
    threw = true;
  }
  assert(threw && "Promote with TIER_UNSPECIFIED must throw");
}

// Spill: default spill target (TIER_DISK) is used when no eviction policy set.
void TestSpillDefaultTargetIsDisk() {
  Fixture f;

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_RAM);
  const auto alloc_resp = f.service.Allocate(alloc_req);

  payload::manager::v1::CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = alloc_resp.payload_descriptor().payload_id();
  f.service.Commit(commit_req);

  SpillRequest spill_req;
  *spill_req.add_ids() = alloc_resp.payload_descriptor().payload_id();
  spill_req.set_fsync(false);

  const auto resp = f.service.Spill(spill_req);
  assert(resp.results_size() == 1);
  assert(resp.results(0).ok() && "spill to TIER_DISK (default target) must succeed");
  assert(f.disk->Has(alloc_resp.payload_descriptor().payload_id()));
}

// Spill: payload with spill_target=TIER_OBJECT fails because no OBJECT backend
// is registered, confirming that GetSpillTarget (not hardcoded TIER_DISK) is used.
void TestSpillUsesPerPayloadTarget() {
  Fixture f;  // No OBJECT backend registered.

  EvictionPolicy policy;
  policy.set_spill_target(TIER_OBJECT);

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_RAM);
  *alloc_req.mutable_eviction_policy() = policy;
  const auto alloc_resp                = f.service.Allocate(alloc_req);

  payload::manager::v1::CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = alloc_resp.payload_descriptor().payload_id();
  f.service.Commit(commit_req);

  SpillRequest spill_req;
  *spill_req.add_ids() = alloc_resp.payload_descriptor().payload_id();
  spill_req.set_fsync(false);

  const auto resp = f.service.Spill(spill_req);
  assert(resp.results_size() == 1);
  // Must fail — no OBJECT storage backend — proving we used TIER_OBJECT, not TIER_DISK.
  assert(!resp.results(0).ok() && "spill must fail when target tier has no backend");
  assert(!resp.results(0).error_message().empty());
}

} // namespace

int main() {
  TestAllocateUnspecifiedTierThrows();
  TestAllocateValidTierSucceeds();
  TestPromoteUnspecifiedTierThrows();
  TestSpillDefaultTargetIsDisk();
  TestSpillUsesPerPayloadTarget();

  std::cout << "catalog_service_medium_fixes: pass\n";
  return 0;
}
