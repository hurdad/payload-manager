#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;
using payload::storage::StorageBackend;

class TrackingStorageBackend final : public StorageBackend {
 public:
  explicit TrackingStorageBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override {
    auto maybe_buffer = arrow::AllocateBuffer(size_bytes);
    if (!maybe_buffer.ok()) {
      throw std::runtime_error("tracking storage allocate failed: " + maybe_buffer.status().ToString());
    }

    std::shared_ptr<arrow::Buffer> buffer(std::move(*maybe_buffer));
    if (size_bytes > 0) {
      std::memset(buffer->mutable_data(), 0, static_cast<size_t>(size_bytes));
    }
    buffers_[id.value()] = buffer;
    return buffer;
  }

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override {
    return buffers_.at(id.value());
  }

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool) override {
    buffers_[id.value()] = buffer;
  }

  void Remove(const payload::manager::v1::PayloadID& id) override {
    removed_ids_.push_back(id.value());
    buffers_.erase(id.value());
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

  bool WasRemoved(const payload::manager::v1::PayloadID& id) const {
    return std::find(removed_ids_.begin(), removed_ids_.end(), id.value()) != removed_ids_.end();
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
  std::vector<std::string>                                        removed_ids_;
};

PayloadManager MakeManager(const std::shared_ptr<LeaseManager>& lease_mgr) {
  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = std::make_shared<TrackingStorageBackend>(TIER_RAM);
  storage[TIER_DISK] = std::make_shared<TrackingStorageBackend>(TIER_DISK);

  return PayloadManager(std::move(storage), lease_mgr, std::make_shared<payload::db::memory::MemoryRepository>());
}

} // namespace

TEST(PayloadManagerDelete, ForceDeleteRemovesPayloadAndLeases) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto descriptor = manager.Commit(manager.Allocate(1024, TIER_RAM).payload_id());
  EXPECT_EQ(descriptor.state(), PAYLOAD_STATE_ACTIVE);

  auto lease = manager.AcquireReadLease(descriptor.payload_id(), TIER_RAM, 60'000);
  EXPECT_FALSE(lease.lease_id().value().empty());
  EXPECT_TRUE(lease_mgr->HasActiveLeases(descriptor.payload_id()));

  manager.Delete(descriptor.payload_id(), /*force=*/true);

  EXPECT_FALSE(lease_mgr->HasActiveLeases(descriptor.payload_id()));
  EXPECT_THROW((void)manager.ResolveSnapshot(descriptor.payload_id()), std::runtime_error);
}

TEST(PayloadManagerDelete, NonForceDeleteRejectsWhenLeaseIsActive) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto descriptor = manager.Commit(manager.Allocate(1024, TIER_RAM).payload_id());
  auto       lease      = manager.AcquireReadLease(descriptor.payload_id(), TIER_RAM, 60'000);
  EXPECT_FALSE(lease.lease_id().value().empty());

  EXPECT_THROW(manager.Delete(descriptor.payload_id(), /*force=*/false), std::runtime_error);
  EXPECT_TRUE(lease_mgr->HasActiveLeases(descriptor.payload_id()));
  EXPECT_EQ(manager.ResolveSnapshot(descriptor.payload_id()).payload_id().value(), descriptor.payload_id().value());
}

TEST(PayloadManagerDelete, PromoteRejectsWhenLeaseIsActive) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto descriptor = manager.Commit(manager.Allocate(1024, TIER_RAM).payload_id());
  auto       lease      = manager.AcquireReadLease(descriptor.payload_id(), TIER_RAM, 60'000);
  EXPECT_FALSE(lease.lease_id().value().empty());

  EXPECT_THROW((void)manager.Promote(descriptor.payload_id(), TIER_DISK), std::runtime_error);
  EXPECT_EQ(manager.ResolveSnapshot(descriptor.payload_id()).tier(), TIER_RAM);
}

TEST(PayloadManagerDelete, CacheCoherenceAcrossCommitPromoteAndDelete) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto allocated = manager.Allocate(1024, TIER_RAM);
  const auto committed = manager.Commit(allocated.payload_id());
  EXPECT_EQ(committed.state(), PAYLOAD_STATE_ACTIVE);
  EXPECT_EQ(committed.version(), 2);

  const auto committed_snapshot = manager.ResolveSnapshot(allocated.payload_id());
  EXPECT_EQ(committed_snapshot.state(), PAYLOAD_STATE_ACTIVE);
  EXPECT_EQ(committed_snapshot.version(), 2);

  const auto promoted = manager.Promote(allocated.payload_id(), TIER_DISK);
  EXPECT_EQ(promoted.tier(), TIER_DISK);

  const auto promoted_snapshot = manager.ResolveSnapshot(allocated.payload_id());
  EXPECT_EQ(promoted_snapshot.tier(), TIER_DISK);
  EXPECT_EQ(promoted_snapshot.version(), promoted.version());

  manager.Delete(allocated.payload_id(), /*force=*/true);

  EXPECT_THROW((void)manager.ResolveSnapshot(allocated.payload_id()), std::runtime_error);
}

TEST(PayloadManagerDelete, DeleteRemovesStoragePayload) {
  auto lease_mgr   = std::make_shared<LeaseManager>();
  auto ram_backend = std::make_shared<TrackingStorageBackend>(TIER_RAM);

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = ram_backend;
  storage[TIER_DISK] = std::make_shared<TrackingStorageBackend>(TIER_DISK);

  PayloadManager manager(std::move(storage), lease_mgr, std::make_shared<payload::db::memory::MemoryRepository>());

  const auto descriptor = manager.Commit(manager.Allocate(128, TIER_RAM).payload_id());
  manager.Delete(descriptor.payload_id(), /*force=*/true);

  EXPECT_TRUE(ram_backend->WasRemoved(descriptor.payload_id()));
}

TEST(PayloadManagerDelete, PrefetchPromotesToRequestedTier) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto descriptor = manager.Commit(manager.Allocate(128, TIER_RAM).payload_id());
  manager.Prefetch(descriptor.payload_id(), TIER_DISK);

  const auto snapshot = manager.ResolveSnapshot(descriptor.payload_id());
  EXPECT_EQ(snapshot.tier(), TIER_DISK);
}

TEST(PayloadManagerDelete, PinnedPayloadBlocksSpillUntilUnpinned) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto descriptor = manager.Commit(manager.Allocate(128, TIER_RAM).payload_id());
  manager.Pin(descriptor.payload_id(), /*duration_ms=*/0);

  EXPECT_THROW(manager.ExecuteSpill(descriptor.payload_id(), TIER_DISK, /*fsync=*/false), std::runtime_error);
  EXPECT_EQ(manager.ResolveSnapshot(descriptor.payload_id()).tier(), TIER_RAM);

  manager.Unpin(descriptor.payload_id());
  manager.ExecuteSpill(descriptor.payload_id(), TIER_DISK, /*fsync=*/false);
  EXPECT_EQ(manager.ResolveSnapshot(descriptor.payload_id()).tier(), TIER_DISK);
}

TEST(PayloadManagerDelete, DeleteClearsPinState) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto descriptor = manager.Commit(manager.Allocate(128, TIER_RAM).payload_id());
  manager.Pin(descriptor.payload_id(), /*duration_ms=*/0);
  manager.Delete(descriptor.payload_id(), /*force=*/true);

  manager.Unpin(descriptor.payload_id());
}
