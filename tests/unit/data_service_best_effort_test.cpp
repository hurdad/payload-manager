/*
  Tests for the DataService::AcquireReadLease PROMOTION_POLICY_BEST_EFFORT
  fix with TIER_UNSPECIFIED.
*/

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/util/errors.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::manager::v1::AcquireReadLeaseRequest;
using payload::manager::v1::AllocatePayloadRequest;
using payload::manager::v1::LEASE_MODE_READ;
using payload::manager::v1::PROMOTION_POLICY_BEST_EFFORT;
using payload::manager::v1::PROMOTION_POLICY_BLOCKING;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_GPU;
using payload::manager::v1::TIER_RAM;
using payload::manager::v1::TIER_UNSPECIFIED;
using payload::service::DataService;
using payload::service::ServiceContext;

class SimpleBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size) override {
    auto r = arrow::AllocateBuffer(size);
    if (!r.ok()) throw std::runtime_error("alloc");
    std::shared_ptr<arrow::Buffer> buf(std::move(*r));
    bufs_[id.value()] = buf;
    return buf;
  }
  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override {
    return bufs_.at(id.value());
  }
  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& b, bool) override {
    bufs_[id.value()] = b;
  }
  void Remove(const payload::manager::v1::PayloadID& id) override {
    bufs_.erase(id.value());
  }
  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> bufs_;
};

struct Fixture {
  std::shared_ptr<payload::lease::LeaseManager>          lease_mgr = std::make_shared<payload::lease::LeaseManager>();
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<SimpleBackend>                         ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                         disk      = std::make_shared<SimpleBackend>(TIER_DISK);
  std::shared_ptr<payload::core::PayloadManager>         manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]  = ram;
    s[TIER_DISK] = disk;
    return std::make_shared<payload::core::PayloadManager>(s, lease_mgr, repo);
  }()};
  ServiceContext                                         ctx{[&] {
    ServiceContext c;
    c.manager   = manager;
    c.lease_mgr = lease_mgr;
    return c;
  }()};
  DataService                                            data{ctx};

  payload::manager::v1::PayloadID AllocateOnRam() {
    return manager->Commit(manager->Allocate(64, TIER_RAM).payload_id()).payload_id();
  }

  payload::manager::v1::PayloadID AllocateOnDisk() {
    auto id = manager->Commit(manager->Allocate(64, TIER_RAM).payload_id()).payload_id();
    manager->ExecuteSpill(id, TIER_DISK, /*fsync=*/false);
    return id;
  }
};

} // namespace

// ---------------------------------------------------------------------------
// Test: BEST_EFFORT + TIER_UNSPECIFIED must not throw.
// ---------------------------------------------------------------------------
TEST(DataServiceBestEffort, BestEffortWithUnspecifiedTierDoesNotThrow) {
  Fixture    f;
  const auto id = f.AllocateOnRam();

  AcquireReadLeaseRequest req;
  *req.mutable_id() = id;
  req.set_mode(LEASE_MODE_READ);
  req.set_min_tier(TIER_UNSPECIFIED);
  req.set_promotion_policy(PROMOTION_POLICY_BEST_EFFORT);

  // Must not throw.
  const auto resp = f.data.AcquireReadLease(req);
  EXPECT_FALSE(resp.lease_id().value().empty()) << "lease must be granted";
}

// ---------------------------------------------------------------------------
// Test: BEST_EFFORT + min_tier == current tier does not throw
// ---------------------------------------------------------------------------
TEST(DataServiceBestEffort, BestEffortWithSameTierDoesNotThrow) {
  Fixture    f;
  const auto id = f.AllocateOnRam();

  AcquireReadLeaseRequest req;
  *req.mutable_id() = id;
  req.set_mode(LEASE_MODE_READ);
  req.set_min_tier(TIER_RAM); // payload is already on RAM
  req.set_promotion_policy(PROMOTION_POLICY_BEST_EFFORT);

  const auto resp = f.data.AcquireReadLease(req);
  EXPECT_FALSE(resp.lease_id().value().empty()) << "lease must be granted when tier requirement is already met";
}

// ---------------------------------------------------------------------------
// Test: BEST_EFFORT + min_tier faster than current tier throws InvalidState.
// ---------------------------------------------------------------------------
TEST(DataServiceBestEffort, BestEffortThrowsWhenTierRequiresPromotion) {
  Fixture    f;
  const auto id = f.AllocateOnDisk();

  AcquireReadLeaseRequest req;
  *req.mutable_id() = id;
  req.set_mode(LEASE_MODE_READ);
  req.set_min_tier(TIER_RAM); // faster than DISK → promotion required
  req.set_promotion_policy(PROMOTION_POLICY_BEST_EFFORT);

  EXPECT_THROW(f.data.AcquireReadLease(req), std::exception) << "BEST_EFFORT must throw when it cannot satisfy min_tier without promotion";
}

// ---------------------------------------------------------------------------
// Test: BLOCKING policy promotes and grants the lease even when the payload
//       is on a slower tier than min_tier.
// ---------------------------------------------------------------------------
TEST(DataServiceBestEffort, BlockingPolicyPromotesAndGrantsLease) {
  Fixture    f;
  const auto id = f.AllocateOnDisk();

  AcquireReadLeaseRequest req;
  *req.mutable_id() = id;
  req.set_mode(LEASE_MODE_READ);
  req.set_min_tier(TIER_RAM);
  req.set_promotion_policy(PROMOTION_POLICY_BLOCKING);

  const auto resp = f.data.AcquireReadLease(req);
  EXPECT_FALSE(resp.lease_id().value().empty()) << "BLOCKING must promote and grant the lease";

  // After promotion the payload must be on RAM.
  const auto desc = f.manager->ResolveSnapshot(id);
  EXPECT_EQ(desc.tier(), TIER_RAM) << "payload must be on RAM after BLOCKING promotion";
}
