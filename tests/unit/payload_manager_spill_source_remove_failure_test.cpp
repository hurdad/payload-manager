/*
  Tests for spill/promote source-tier Remove() failure resilience.

  After the fix, ExecuteSpill and PromoteUnlocked wrap the source-tier
  Remove() in try-catch, matching the behaviour already present in Delete().

  If Remove() throws after the DB commit (which is the authoritative tier
  change), the operation must still succeed from the caller's perspective:
    - No exception is propagated.
    - The payload's DB record reflects the new tier.
    - Subsequent ResolveSnapshot returns the new tier.
    - The destination tier has the data.

  The source tier may still contain orphaned bytes (a resource leak), but
  the payload state is logically consistent.
*/

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

// Normal backend for the destination tier.
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

// Backend whose Remove() always throws — simulates a storage failure.
class ThrowingRemoveBackend final : public payload::storage::StorageBackend {
 public:
  explicit ThrowingRemoveBackend(payload::manager::v1::Tier tier) : tier_(tier) {
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
  void Remove(const payload::manager::v1::PayloadID&) override {
    throw std::runtime_error("simulated Remove() failure");
  }
  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> bufs_;
};

// ---------------------------------------------------------------------------
// Test: ExecuteSpill succeeds even when source Remove() throws.
// ---------------------------------------------------------------------------
TEST(SpillSourceRemoveFailure, SpillDoesNotPropagateRemoveException) {
  auto lease_mgr    = std::make_shared<payload::lease::LeaseManager>();
  auto ram_backend  = std::make_shared<ThrowingRemoveBackend>(TIER_RAM);
  auto disk_backend = std::make_shared<SimpleBackend>(TIER_DISK);

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = ram_backend;
  storage[TIER_DISK] = disk_backend;

  payload::core::PayloadManager manager(std::move(storage), lease_mgr, std::make_shared<payload::db::memory::MemoryRepository>());

  auto desc = manager.Commit(manager.Allocate(128, TIER_RAM).payload_id());

  // Must not throw despite Remove() failing.
  EXPECT_NO_THROW(manager.ExecuteSpill(desc.payload_id(), TIER_DISK, false));

  // DB must reflect new tier.
  auto snap = manager.ResolveSnapshot(desc.payload_id());
  EXPECT_EQ(snap.tier(), TIER_DISK) << "DB tier must be updated even when source Remove() throws";

  // Destination must have the data.
  EXPECT_TRUE(disk_backend->Has(desc.payload_id())) << "Destination must have data after spill";
}

// ---------------------------------------------------------------------------
// Test: PromoteUnlocked (via Promote) succeeds even when source Remove() throws.
// ---------------------------------------------------------------------------
TEST(SpillSourceRemoveFailure, PromoteDoesNotPropagateRemoveException) {
  auto lease_mgr = std::make_shared<payload::lease::LeaseManager>();

  // Use a manager where DISK Remove throws and verify Promote(DISK → RAM) succeeds.
  auto ram2  = std::make_shared<SimpleBackend>(TIER_RAM);
  auto disk2 = std::make_shared<ThrowingRemoveBackend>(TIER_DISK);

  // Pre-seed the disk backend manually with a buffer.
  const auto                      promote_uuid = payload::util::GenerateUUID();
  payload::manager::v1::PayloadID id           = payload::util::ToProto(promote_uuid);

  auto buf_result = arrow::AllocateBuffer(64);
  ASSERT_TRUE(buf_result.ok());
  std::shared_ptr<arrow::Buffer> buf(std::move(*buf_result));
  disk2->Write(id, buf, false);

  // Insert record directly into the repository so the manager knows about it.
  auto repo2 = std::make_shared<payload::db::memory::MemoryRepository>();
  {
    payload::db::model::PayloadRecord r;
    r.id         = promote_uuid;
    r.tier       = TIER_DISK;
    r.state      = payload::manager::v1::PAYLOAD_STATE_ACTIVE;
    r.size_bytes = 64;
    r.version    = 1;
    auto tx      = repo2->Begin();
    repo2->InsertPayload(*tx, r);
    tx->Commit();
  }

  payload::storage::StorageFactory::TierMap storage3;
  storage3[TIER_RAM]  = ram2;
  storage3[TIER_DISK] = disk2;
  payload::core::PayloadManager manager3(std::move(storage3), lease_mgr, repo2);
  manager3.HydrateCaches();

  // Promote DISK → RAM: disk Remove() will throw.
  EXPECT_NO_THROW(manager3.Promote(id, TIER_RAM));

  auto snap = manager3.ResolveSnapshot(id);
  EXPECT_EQ(snap.tier(), TIER_RAM) << "Payload must be on RAM after promote even if disk Remove() throws";
}

} // namespace
