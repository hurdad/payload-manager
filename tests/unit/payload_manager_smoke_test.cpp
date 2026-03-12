/*
  End-to-end smoke test: exercises the full payload lifecycle through every
  layer (PayloadManager → service → in-process storage) using the Memory
  repository and in-process storage backends.

  Covered path:
    Allocate → Commit → ResolveSnapshot → AcquireReadLease →
    ReleaseLease → Spill → Promote → Delete
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
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/storage/storage_backend.hpp"
#include "payload/manager/core/v1/policy.pb.h"
#include "payload/manager/v1.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::AcquireReadLeaseRequest;
using payload::manager::v1::AllocatePayloadRequest;
using payload::manager::v1::CommitPayloadRequest;
using payload::manager::v1::DeleteRequest;
using payload::manager::v1::LEASE_MODE_READ;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;
using payload::manager::v1::PAYLOAD_STATE_DELETED;
using payload::manager::v1::PayloadID;
using payload::manager::v1::PromoteRequest;
using payload::manager::v1::ReleaseLeaseRequest;
using payload::manager::v1::ResolveSnapshotRequest;
using payload::manager::v1::SpillRequest;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

// ---------------------------------------------------------------------------
// Minimal in-process storage backend
// ---------------------------------------------------------------------------

class MemStorageBackend final : public payload::storage::StorageBackend {
 public:
  explicit MemStorageBackend(payload::manager::v1::Tier tier) : tier_(tier) {}

  std::shared_ptr<arrow::Buffer> Allocate(const PayloadID& id, uint64_t size_bytes) override {
    auto result = arrow::AllocateBuffer(size_bytes);
    if (!result.ok()) throw std::runtime_error(result.status().ToString());
    std::shared_ptr<arrow::Buffer> buf(std::move(*result));
    if (size_bytes > 0) std::memset(buf->mutable_data(), 0xCC, static_cast<size_t>(size_bytes));
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
// Test fixture
// ---------------------------------------------------------------------------

struct Fixture {
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<MemStorageBackend>                     ram       = std::make_shared<MemStorageBackend>(TIER_RAM);
  std::shared_ptr<MemStorageBackend>                     disk      = std::make_shared<MemStorageBackend>(TIER_DISK);
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
  payload::service::CatalogService catalog{ctx};
  payload::service::DataService    data{ctx};
};

// ---------------------------------------------------------------------------
// Smoke test: full lifecycle — allocate → commit → resolve → lease → release
//             → spill → promote → delete
// ---------------------------------------------------------------------------

void TestFullLifecycle() {
  Fixture f;

  // 1. Allocate
  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(128);
  alloc_req.set_preferred_tier(TIER_RAM);
  const auto alloc_resp = f.catalog.Allocate(alloc_req);
  const auto payload_id = alloc_resp.payload_descriptor().payload_id();
  assert(!payload_id.value().empty() && "payload ID must be non-empty after allocate");
  assert(alloc_resp.payload_descriptor().tier() == TIER_RAM);

  // 2. Commit
  CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = payload_id;
  const auto commit_resp   = f.catalog.Commit(commit_req);
  assert(commit_resp.payload_descriptor().state() == PAYLOAD_STATE_ACTIVE);
  assert(commit_resp.payload_descriptor().version() > alloc_resp.payload_descriptor().version());

  // 3. ResolveSnapshot (advisory)
  ResolveSnapshotRequest resolve_req;
  *resolve_req.mutable_id()  = payload_id;
  const auto resolve_resp    = f.data.ResolveSnapshot(resolve_req);
  assert(resolve_resp.payload_descriptor().state() == PAYLOAD_STATE_ACTIVE);
  assert(resolve_resp.payload_descriptor().tier() == TIER_RAM);

  // 4. AcquireReadLease
  AcquireReadLeaseRequest lease_req;
  *lease_req.mutable_id() = payload_id;
  lease_req.set_min_lease_duration_ms(5000);
  lease_req.set_mode(LEASE_MODE_READ);
  const auto lease_resp = f.data.AcquireReadLease(lease_req);
  assert(!lease_resp.lease_id().value().empty() && "lease_id must be non-empty");
  assert(lease_resp.payload_descriptor().tier() == TIER_RAM);

  // 5. Spill must be blocked while lease is active
  {
    SpillRequest spill_req;
    *spill_req.add_ids() = payload_id;
    spill_req.set_fsync(false);
    const auto spill_resp = f.catalog.Spill(spill_req);
    assert(spill_resp.results_size() == 1);
    assert(!spill_resp.results(0).ok() && "spill must fail while a read lease is active");
    assert(f.ram->Has(payload_id) && "data must remain in RAM while lease is held");
  }

  // 6. ReleaseLease
  ReleaseLeaseRequest release_req;
  *release_req.mutable_lease_id() = lease_resp.lease_id();
  f.data.ReleaseLease(release_req);

  // 7. Spill to disk (lease released, should succeed now)
  {
    SpillRequest spill_req;
    *spill_req.add_ids() = payload_id;
    spill_req.set_fsync(false);
    const auto spill_resp = f.catalog.Spill(spill_req);
    assert(spill_resp.results_size() == 1);
    assert(spill_resp.results(0).ok() && "spill must succeed after lease release");
    assert(!f.ram->Has(payload_id) && "RAM must be freed after spill");
    assert(f.disk->Has(payload_id) && "data must be present on disk after spill");
  }

  // 8. Promote back to RAM
  PromoteRequest promote_req;
  *promote_req.mutable_id() = payload_id;
  promote_req.set_target_tier(TIER_RAM);
  const auto promote_resp = f.catalog.Promote(promote_req);
  assert(promote_resp.payload_descriptor().tier() == TIER_RAM);
  assert(f.ram->Has(payload_id) && "data must be in RAM after promote");
  assert(!f.disk->Has(payload_id) && "disk must be freed after promote");

  // 9. Delete
  DeleteRequest delete_req;
  *delete_req.mutable_id() = payload_id;
  delete_req.set_force(false);
  f.catalog.Delete(delete_req);

  // Verify deletion is complete
  bool not_found = false;
  try {
    ResolveSnapshotRequest check_req;
    *check_req.mutable_id() = payload_id;
    f.data.ResolveSnapshot(check_req);
  } catch (const std::runtime_error&) {
    not_found = true;
  }
  assert(not_found && "deleted payload must not be resolvable");
  assert(!f.ram->Has(payload_id) && "storage must be cleaned up after delete");
}

// ---------------------------------------------------------------------------
// Smoke test: force-delete while lease is held
// ---------------------------------------------------------------------------

void TestForceDeleteWithActiveLease() {
  Fixture f;

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_RAM);
  const auto alloc_resp = f.catalog.Allocate(alloc_req);
  const auto payload_id = alloc_resp.payload_descriptor().payload_id();

  CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = payload_id;
  f.catalog.Commit(commit_req);

  // Acquire a lease and then force-delete without releasing.
  AcquireReadLeaseRequest lease_req;
  *lease_req.mutable_id()          = payload_id;
  lease_req.set_min_lease_duration_ms(30000);
  const auto lease_resp = f.data.AcquireReadLease(lease_req);
  assert(!lease_resp.lease_id().value().empty());

  DeleteRequest delete_req;
  *delete_req.mutable_id() = payload_id;
  delete_req.set_force(true);

  bool threw = false;
  try {
    f.catalog.Delete(delete_req);
  } catch (...) {
    threw = true;
  }
  assert(!threw && "force delete must succeed even with an active lease");

  // Payload must be gone.
  bool not_found = false;
  try {
    ResolveSnapshotRequest check_req;
    *check_req.mutable_id() = payload_id;
    f.data.ResolveSnapshot(check_req);
  } catch (const std::runtime_error&) {
    not_found = true;
  }
  assert(not_found && "force-deleted payload must not be resolvable");
}

// ---------------------------------------------------------------------------
// Smoke test: non-force delete while lease is held must be rejected
// ---------------------------------------------------------------------------

void TestSoftDeleteWithActiveLeaseFails() {
  Fixture f;

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_RAM);
  const auto alloc_resp = f.catalog.Allocate(alloc_req);
  const auto payload_id = alloc_resp.payload_descriptor().payload_id();

  CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = payload_id;
  f.catalog.Commit(commit_req);

  AcquireReadLeaseRequest lease_req;
  *lease_req.mutable_id()          = payload_id;
  lease_req.set_min_lease_duration_ms(30000);
  f.data.AcquireReadLease(lease_req);

  DeleteRequest delete_req;
  *delete_req.mutable_id() = payload_id;
  delete_req.set_force(false);

  bool threw = false;
  try {
    f.catalog.Delete(delete_req);
  } catch (const std::exception&) {
    threw = true;
  }
  assert(threw && "soft delete must fail while an active lease is held");
}

// ---------------------------------------------------------------------------
// Smoke test: HydrateCaches rebuilds snapshot cache from repository
// ---------------------------------------------------------------------------

void TestHydrateCachesRebuildsState() {
  Fixture f;

  AllocatePayloadRequest alloc_req;
  alloc_req.set_size_bytes(64);
  alloc_req.set_preferred_tier(TIER_RAM);
  const auto alloc_resp = f.catalog.Allocate(alloc_req);
  const auto payload_id = alloc_resp.payload_descriptor().payload_id();

  CommitPayloadRequest commit_req;
  *commit_req.mutable_id() = payload_id;
  f.catalog.Commit(commit_req);

  const auto version_before = [&] {
    ResolveSnapshotRequest req;
    *req.mutable_id() = payload_id;
    return f.data.ResolveSnapshot(req).payload_descriptor().version();
  }();

  // HydrateCaches simulates a restart: it bumps all payload versions.
  f.manager->HydrateCaches();

  const auto version_after = [&] {
    ResolveSnapshotRequest req;
    *req.mutable_id() = payload_id;
    return f.data.ResolveSnapshot(req).payload_descriptor().version();
  }();

  assert(version_after > version_before && "HydrateCaches must bump version to invalidate pre-restart descriptors");
  assert(version_after - version_before == 1 && "version must be incremented by exactly 1");
}

} // namespace

int main() {
  TestFullLifecycle();
  TestForceDeleteWithActiveLease();
  TestSoftDeleteWithActiveLeaseFails();
  TestHydrateCachesRebuildsState();

  std::cout << "payload_manager_smoke_test: pass\n";
  return 0;
}
