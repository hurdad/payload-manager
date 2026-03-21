/*
  Tests for TODO #13: Spill RPC policy and wait_for_leases.
*/

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/spill/spill_scheduler.hpp"
#include "internal/storage/storage_backend.hpp"
#include "payload/manager/core/v1/policy.pb.h"
#include "payload/manager/v1.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::core::v1::SPILL_POLICY_BEST_EFFORT;
using payload::manager::core::v1::SPILL_POLICY_BLOCKING;
using payload::manager::v1::AcquireReadLeaseRequest;
using payload::manager::v1::AllocatePayloadRequest;
using payload::manager::v1::CommitPayloadRequest;
using payload::manager::v1::LEASE_MODE_READ;
using payload::manager::v1::ReleaseLeaseRequest;
using payload::manager::v1::SpillRequest;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

class SimpleBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size) override {
    auto r = arrow::AllocateBuffer(size);
    if (!r.ok()) throw std::runtime_error("alloc");
    std::shared_ptr<arrow::Buffer> buf(std::move(*r));
    if (size > 0) std::memset(buf->mutable_data(), 0, size);
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
  bool Has(const payload::manager::v1::PayloadID& id) const {
    return bufs_.count(id.value()) > 0;
  }
  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> bufs_;
};

// Fixture wires LeaseManager + SpillScheduler into ServiceContext.
struct Fixture {
  // Use a very short default lease (50 ms) so wait_for_leases tests complete quickly.
  std::shared_ptr<LeaseManager> lease_mgr = std::make_shared<LeaseManager>(
      /*default_lease_ms=*/50,
      /*max_lease_ms=*/100);
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<SimpleBackend>                         ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                         disk      = std::make_shared<SimpleBackend>(TIER_DISK);
  std::shared_ptr<payload::spill::SpillScheduler>        scheduler = std::make_shared<payload::spill::SpillScheduler>();
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]  = ram;
    s[TIER_DISK] = disk;
    return std::make_shared<PayloadManager>(s, lease_mgr, repo);
  }()};
  payload::service::ServiceContext                       ctx{[&] {
    payload::service::ServiceContext c;
    c.manager         = manager;
    c.repository      = repo;
    c.lease_mgr       = lease_mgr;
    c.spill_scheduler = scheduler;
    return c;
  }()};
  payload::service::CatalogService                       catalog{ctx};
  payload::service::DataService                          data{ctx};

  // Helper: allocate + commit a payload on RAM and return its ID.
  payload::manager::v1::PayloadID AllocateCommit(uint64_t size = 64) {
    AllocatePayloadRequest alloc;
    alloc.set_size_bytes(size);
    alloc.set_preferred_tier(TIER_RAM);
    CommitPayloadRequest commit;
    *commit.mutable_id() = catalog.Allocate(alloc).payload_descriptor().payload_id();
    return catalog.Commit(commit).payload_descriptor().payload_id();
  }
};

} // namespace

// ---------------------------------------------------------------------------
// Test: SPILL_POLICY_BEST_EFFORT enqueues the task but does NOT move data.
// ---------------------------------------------------------------------------
TEST(CatalogServiceSpillPolicy, BestEffortEnqueuesWithoutMovingData) {
  Fixture f;

  const auto id = f.AllocateCommit();

  SpillRequest req;
  *req.add_ids() = id;
  req.set_policy(SPILL_POLICY_BEST_EFFORT);
  req.set_fsync(false);

  // No spill workers are running, so if the call enqueues (best-effort)
  // rather than executing inline, the disk backend stays empty.
  const auto resp = f.catalog.Spill(req);

  EXPECT_EQ(resp.results_size(), 1) << "response must contain one result";
  EXPECT_TRUE(resp.results(0).ok()) << "best-effort spill must report ok=true";
  EXPECT_FALSE(f.disk->Has(id)) << "best-effort spill must not move data inline";
  EXPECT_TRUE(f.ram->Has(id)) << "data must still be in RAM (not moved inline)";
}

// ---------------------------------------------------------------------------
// Test: SPILL_POLICY_BLOCKING (default) executes synchronously.
// ---------------------------------------------------------------------------
TEST(CatalogServiceSpillPolicy, BlockingSpillMovesDataInline) {
  Fixture f;

  const auto id = f.AllocateCommit();

  SpillRequest req;
  *req.add_ids() = id;
  req.set_policy(SPILL_POLICY_BLOCKING);
  req.set_fsync(false);

  const auto resp = f.catalog.Spill(req);

  EXPECT_EQ(resp.results_size(), 1);
  EXPECT_TRUE(resp.results(0).ok()) << "blocking spill must report ok=true";
  EXPECT_TRUE(f.disk->Has(id)) << "blocking spill must move data to disk inline";
  EXPECT_FALSE(f.ram->Has(id)) << "RAM must be freed after blocking spill";

  // Response must include the post-spill descriptor.
  EXPECT_TRUE(resp.results(0).has_payload_descriptor()) << "blocking result must include descriptor";
  EXPECT_EQ(resp.results(0).payload_descriptor().tier(), TIER_DISK);
}

// ---------------------------------------------------------------------------
// Test: Unspecified policy falls back to blocking.
// ---------------------------------------------------------------------------
TEST(CatalogServiceSpillPolicy, UnspecifiedPolicyFallsBackToBlocking) {
  Fixture f;

  const auto id = f.AllocateCommit();

  SpillRequest req;
  *req.add_ids() = id;
  // policy left as SPILL_POLICY_UNSPECIFIED (proto default)
  req.set_fsync(false);

  const auto resp = f.catalog.Spill(req);
  EXPECT_TRUE(resp.results(0).ok());
  EXPECT_TRUE(f.disk->Has(id)) << "unspecified policy must behave as blocking";
}

// ---------------------------------------------------------------------------
// Test: wait_for_leases=true waits until the active read lease expires and
//       then successfully spills.
// ---------------------------------------------------------------------------
TEST(CatalogServiceSpillPolicy, WaitForLeasesWaitsForLeaseExpiry) {
  Fixture f;

  const auto id = f.AllocateCommit();

  // Acquire a short-lived read lease.
  AcquireReadLeaseRequest lr;
  *lr.mutable_id() = id;
  lr.set_mode(LEASE_MODE_READ);
  lr.set_min_lease_duration_ms(0); // use default (50 ms)
  const auto lease_resp = f.data.AcquireReadLease(lr);
  EXPECT_FALSE(lease_resp.lease_id().value().empty());

  // A plain BLOCKING spill without wait_for_leases must fail (lease is active).
  {
    SpillRequest req;
    *req.add_ids() = id;
    req.set_policy(SPILL_POLICY_BLOCKING);
    req.set_fsync(false);
    req.set_wait_for_leases(false);
    const auto resp = f.catalog.Spill(req);
    EXPECT_FALSE(resp.results(0).ok()) << "spill without wait_for_leases must fail while lease is held";
    EXPECT_FALSE(f.disk->Has(id)) << "data must not move while lease is active";
  }

  // With wait_for_leases=true the call waits for the 50 ms lease to expire
  // and then proceeds with the spill.
  {
    SpillRequest req;
    *req.add_ids() = id;
    req.set_policy(SPILL_POLICY_BLOCKING);
    req.set_fsync(false);
    req.set_wait_for_leases(true);
    const auto resp = f.catalog.Spill(req);
    EXPECT_TRUE(resp.results(0).ok()) << "wait_for_leases spill must eventually succeed";
    EXPECT_TRUE(f.disk->Has(id)) << "data must be on disk after wait_for_leases spill";
  }
}

// ---------------------------------------------------------------------------
// Test: wait_for_leases=true with an explicitly released lease succeeds
//       without waiting for expiry.
// ---------------------------------------------------------------------------
TEST(CatalogServiceSpillPolicy, WaitForLeasesSucceedsImmediatelyAfterRelease) {
  Fixture f;

  const auto id = f.AllocateCommit();

  AcquireReadLeaseRequest lr;
  *lr.mutable_id() = id;
  lr.set_mode(LEASE_MODE_READ);
  lr.set_min_lease_duration_ms(0);
  const auto lease_resp = f.data.AcquireReadLease(lr);

  // Release immediately.
  ReleaseLeaseRequest rr;
  *rr.mutable_lease_id() = lease_resp.lease_id();
  f.data.ReleaseLease(rr);

  SpillRequest req;
  *req.add_ids() = id;
  req.set_policy(SPILL_POLICY_BLOCKING);
  req.set_fsync(false);
  req.set_wait_for_leases(true);

  const auto resp = f.catalog.Spill(req);
  EXPECT_TRUE(resp.results(0).ok()) << "wait_for_leases must succeed immediately when no leases are held";
  EXPECT_TRUE(f.disk->Has(id));
}
