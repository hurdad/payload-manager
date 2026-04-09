/*
  Tests for CatalogService::Spill wait_for_leases timeout.

  When wait_for_leases=true and active leases do not expire before the
  spill_wait_timeout_ms deadline, the Spill RPC must return a LeaseConflict
  error rather than blocking forever.

  Covered:
    - wait_for_leases=false with an active lease returns LeaseConflict immediately
      (existing behaviour, unchanged)
    - wait_for_leases=true with an active lease that never expires throws
      LeaseConflict once the timeout elapses (not INTERNAL / not silent)
    - wait_for_leases=true with a lease that expires before the timeout
      allows the spill to proceed successfully
*/

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/util/errors.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::manager::v1::AcquireReadLeaseRequest;
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

struct Fixture {
  std::shared_ptr<payload::lease::LeaseManager> lease_mgr = std::make_shared<payload::lease::LeaseManager>(
      /*default_ms=*/200, /*max_ms=*/500); // short lease for fast tests
  std::shared_ptr<payload::db::memory::MemoryRepository> repo = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<SimpleBackend>                         ram  = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                         disk = std::make_shared<SimpleBackend>(TIER_DISK);
  std::shared_ptr<payload::core::PayloadManager>         manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]  = ram;
    s[TIER_DISK] = disk;
    return std::make_shared<payload::core::PayloadManager>(s, lease_mgr, repo);
  }()};
  payload::service::ServiceContext                       ctx{[&] {
    payload::service::ServiceContext c;
    c.manager               = manager;
    c.repository            = repo;
    c.lease_mgr             = lease_mgr;
    c.spill_wait_timeout_ms = 150; // 150 ms timeout
    return c;
  }()};
  payload::service::CatalogService                       catalog{ctx};
  payload::service::DataService                          data{ctx};

  payload::manager::v1::PayloadID Allocate() {
    payload::manager::v1::AllocatePayloadRequest req;
    req.set_size_bytes(64);
    req.set_preferred_tier(TIER_RAM);
    const auto                                 resp = catalog.Allocate(req);
    payload::manager::v1::CommitPayloadRequest commit;
    *commit.mutable_id() = resp.payload_descriptor().payload_id();
    catalog.Commit(commit);
    return resp.payload_descriptor().payload_id();
  }

  payload::manager::v1::LeaseID AcquireLease(const payload::manager::v1::PayloadID& id, uint64_t duration_ms) {
    AcquireReadLeaseRequest req;
    *req.mutable_id() = id;
    req.set_min_lease_duration_ms(duration_ms);
    req.set_mode(LEASE_MODE_READ);
    return data.AcquireReadLease(req).lease_id();
  }

  void ReleaseLease(const payload::manager::v1::LeaseID& lease_id) {
    ReleaseLeaseRequest req;
    *req.mutable_lease_id() = lease_id;
    data.ReleaseLease(req);
  }
};

// ---------------------------------------------------------------------------
// wait_for_leases=false with active lease returns LeaseConflict immediately.
// ---------------------------------------------------------------------------
TEST(SpillWaitTimeout, WaitFalseWithActiveLeaseFails) {
  Fixture    f;
  const auto id = f.Allocate();
  f.AcquireLease(id, 30000);

  SpillRequest req;
  *req.add_ids() = id;
  req.set_wait_for_leases(false);

  const auto resp = f.catalog.Spill(req);
  ASSERT_EQ(resp.results_size(), 1);
  EXPECT_FALSE(resp.results(0).ok()) << "Spill must fail when lease is active and wait_for_leases=false";
}

// ---------------------------------------------------------------------------
// wait_for_leases=true with a long-lived lease exceeds timeout → LeaseConflict.
// ---------------------------------------------------------------------------
TEST(SpillWaitTimeout, WaitTrueTimesOutWhenLeaseDoesNotExpire) {
  Fixture    f;
  const auto id = f.Allocate();
  // Acquire a 30s lease — will not expire within the 150ms wait timeout.
  f.AcquireLease(id, 30000);

  SpillRequest req;
  *req.add_ids() = id;
  req.set_wait_for_leases(true);

  const auto t0   = std::chrono::steady_clock::now();
  const auto resp = f.catalog.Spill(req);
  const auto ms   = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

  ASSERT_EQ(resp.results_size(), 1);
  EXPECT_FALSE(resp.results(0).ok()) << "Spill must fail after timeout when lease never expires";
  // Must have waited at least close to the configured timeout.
  EXPECT_GE(ms, 100) << "Must have waited at least 100ms before timing out";
}

// ---------------------------------------------------------------------------
// wait_for_leases=true, lease expires before timeout → spill succeeds.
// ---------------------------------------------------------------------------
TEST(SpillWaitTimeout, WaitTrueSucceedsAfterLeaseExpires) {
  // Use a very short default lease (50ms) so it expires before the 500ms timeout.
  auto lease_mgr = std::make_shared<payload::lease::LeaseManager>(/*default_ms=*/50, /*max_ms=*/100);
  auto repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  auto ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  auto disk      = std::make_shared<SimpleBackend>(TIER_DISK);

  payload::storage::StorageFactory::TierMap s;
  s[TIER_RAM]  = ram;
  s[TIER_DISK] = disk;
  auto manager = std::make_shared<payload::core::PayloadManager>(s, lease_mgr, repo);

  payload::service::ServiceContext ctx;
  ctx.manager               = manager;
  ctx.repository            = repo;
  ctx.lease_mgr             = lease_mgr;
  ctx.spill_wait_timeout_ms = 500;

  payload::service::CatalogService catalog(ctx);
  payload::service::DataService    data(ctx);

  payload::manager::v1::AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_RAM);
  const auto                                 alloc_resp = catalog.Allocate(alloc_req);
  const auto                                 id         = alloc_resp.payload_descriptor().payload_id();
  payload::manager::v1::CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = id;
  catalog.Commit(commit_req);

  // Acquire a 50ms lease — will expire on its own.
  AcquireReadLeaseRequest lease_req;
  *lease_req.mutable_id() = id;
  lease_req.set_min_lease_duration_ms(50);
  lease_req.set_mode(LEASE_MODE_READ);
  data.AcquireReadLease(lease_req);

  SpillRequest req;
  *req.add_ids() = id;
  req.set_wait_for_leases(true);

  const auto resp = catalog.Spill(req);
  ASSERT_EQ(resp.results_size(), 1);
  EXPECT_TRUE(resp.results(0).ok()) << "Spill must succeed after lease expires within timeout";
  EXPECT_TRUE(disk->Has(id)) << "Data must be on disk after successful spill";
}

} // namespace
