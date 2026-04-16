/*
  Unit tests for TIER_VOID: disk pressure eviction, GetDiskSpillTarget, and the
  ExecuteSpill void path that deletes payloads instead of moving them.
*/

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/tiering/pressure_state.hpp"
#include "internal/tiering/tiering_policy.hpp"
#include "payload/manager/core/v1/policy.pb.h"
#include "payload/manager/v1.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::core::v1::EvictionPolicy;
using payload::manager::v1::PAYLOAD_STATE_DELETED;
using payload::manager::v1::PayloadID;
using payload::manager::v1::PayloadMetadata;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_OBJECT;
using payload::manager::v1::TIER_RAM;
using payload::manager::v1::TIER_VOID;
using payload::metadata::MetadataCache;
using payload::tiering::PressureState;
using payload::tiering::TieringPolicy;

// ---------------------------------------------------------------------------
// Minimal in-process storage backend
// ---------------------------------------------------------------------------

class SimpleBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleBackend(payload::manager::v1::Tier tier) : tier_(tier) {}

  std::shared_ptr<arrow::Buffer> Allocate(const PayloadID& id, uint64_t size) override {
    auto r = arrow::AllocateBuffer(size);
    if (!r.ok()) throw std::runtime_error("alloc failed");
    std::shared_ptr<arrow::Buffer> buf(std::move(*r));
    if (size > 0) std::memset(buf->mutable_data(), 0, size);
    bufs_[id.value()] = buf;
    return buf;
  }
  std::shared_ptr<arrow::Buffer> Read(const PayloadID& id) override { return bufs_.at(id.value()); }
  void Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& b, bool) override { bufs_[id.value()] = b; }
  void Remove(const PayloadID& id) override { bufs_.erase(id.value()); }
  payload::manager::v1::Tier TierType() const override { return tier_; }
  bool                       Has(const PayloadID& id) const { return bufs_.count(id.value()) > 0; }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> bufs_;
};

// ---------------------------------------------------------------------------
// Test fixture: RAM + DISK backends, no object storage
// ---------------------------------------------------------------------------

struct Fixture {
  std::shared_ptr<LeaseManager>                           lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<payload::db::memory::MemoryRepository>  repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<SimpleBackend>                          ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                          disk      = std::make_shared<SimpleBackend>(TIER_DISK);
  std::shared_ptr<PayloadManager>                         manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]  = ram;
    s[TIER_DISK] = disk;
    return std::make_shared<PayloadManager>(s, lease_mgr, repo);
  }()};

  // Allocate and commit a payload at the given tier with an optional eviction policy.
  PayloadID AllocateAndCommit(payload::manager::v1::Tier tier, uint64_t size = 64,
                               const EvictionPolicy& policy = {}) {
    auto desc = manager->Allocate(size, tier, /*ttl_ms=*/0, /*no_evict=*/false, policy);
    manager->Commit(desc.payload_id());
    return desc.payload_id();
  }
};

// ---------------------------------------------------------------------------
// PressureState::DiskPressure
// ---------------------------------------------------------------------------

TEST(VoidTier, DiskPressureFiresWhenBytesExceedLimit) {
  PressureState state;
  state.disk_limit = 1024;
  state.disk_bytes.store(512);
  EXPECT_FALSE(state.DiskPressure());
  state.disk_bytes.store(1025);
  EXPECT_TRUE(state.DiskPressure());
}

TEST(VoidTier, DiskPressureNotFiredAtExactLimit) {
  PressureState state;
  state.disk_limit = 100;
  state.disk_bytes.store(100);
  EXPECT_FALSE(state.DiskPressure());
}

// ---------------------------------------------------------------------------
// TieringPolicy::ChooseDiskEviction
// ---------------------------------------------------------------------------

TEST(VoidTier, ChooseDiskEvictionNoPressureReturnsNull) {
  auto  cache  = std::make_shared<MetadataCache>();
  PayloadMetadata meta;
  meta.mutable_id()->set_value("p1");
  cache->Put(meta.id(), meta);

  auto policy = TieringPolicy(cache, {}, {}, [](const PayloadID&) { return true; });

  PressureState state;
  state.disk_limit = 1000;
  state.disk_bytes.store(500);
  EXPECT_FALSE(policy.ChooseDiskEviction(state).has_value());
}

TEST(VoidTier, ChooseDiskEvictionUnderPressureReturnsVictim) {
  auto cache = std::make_shared<MetadataCache>();
  PayloadMetadata meta;
  meta.mutable_id()->set_value("disk-payload");
  cache->Put(meta.id(), meta);

  auto policy = TieringPolicy(cache, {}, {}, [](const PayloadID& id) { return id.value() == "disk-payload"; });

  PressureState state;
  state.disk_limit = 0;
  state.disk_bytes.store(1);
  const auto victim = policy.ChooseDiskEviction(state);
  ASSERT_TRUE(victim.has_value());
  EXPECT_EQ(victim->value(), "disk-payload");
}

TEST(VoidTier, ChooseDiskEvictionPredicateRejectsAllYieldsNull) {
  auto cache = std::make_shared<MetadataCache>();
  PayloadMetadata meta;
  meta.mutable_id()->set_value("p1");
  cache->Put(meta.id(), meta);

  auto policy = TieringPolicy(cache, {}, {}, [](const PayloadID&) { return false; });

  PressureState state;
  state.disk_limit = 0;
  state.disk_bytes.store(1);
  EXPECT_FALSE(policy.ChooseDiskEviction(state).has_value());
}

// ---------------------------------------------------------------------------
// GetDiskSpillTarget
// ---------------------------------------------------------------------------

TEST(VoidTier, GetDiskSpillTargetDefaultsToObject) {
  Fixture f;
  auto    id = f.AllocateAndCommit(TIER_DISK);
  // No explicit spill_target set → should default to TIER_OBJECT.
  EXPECT_EQ(f.manager->GetDiskSpillTarget(id), TIER_OBJECT);
}

TEST(VoidTier, GetDiskSpillTargetHonorsVoidOverride) {
  Fixture f;
  EvictionPolicy policy;
  policy.set_spill_target(TIER_VOID);
  auto id = f.AllocateAndCommit(TIER_DISK, 64, policy);
  EXPECT_EQ(f.manager->GetDiskSpillTarget(id), TIER_VOID);
}

TEST(VoidTier, GetDiskSpillTargetNonVoidSpillTargetFallsBackToObject) {
  Fixture f;
  // spill_target=TIER_DISK means "stay on disk" for RAM eviction, but for disk
  // eviction we fall back to TIER_OBJECT since TIER_DISK is not a terminal tier.
  EvictionPolicy policy;
  policy.set_spill_target(TIER_DISK);
  auto id = f.AllocateAndCommit(TIER_DISK, 64, policy);
  EXPECT_EQ(f.manager->GetDiskSpillTarget(id), TIER_OBJECT);
}

// ---------------------------------------------------------------------------
// ExecuteSpill to TIER_VOID
// ---------------------------------------------------------------------------

TEST(VoidTier, SpillToVoidDeletesRamPayload) {
  Fixture f;
  auto    id = f.AllocateAndCommit(TIER_RAM);

  ASSERT_TRUE(f.ram->Has(id));
  EXPECT_EQ(f.manager->GetTierBytes().at(static_cast<int>(TIER_RAM)), 64u);

  f.manager->ExecuteSpill(id, TIER_VOID, /*fsync=*/false);

  // Storage bytes removed.
  EXPECT_FALSE(f.ram->Has(id));
  // Tier accounting zeroed.
  auto bytes = f.manager->GetTierBytes();
  uint64_t ram_bytes = bytes.count(static_cast<int>(TIER_RAM)) ? bytes.at(static_cast<int>(TIER_RAM)) : 0;
  EXPECT_EQ(ram_bytes, 0u);
  // ResolveSnapshot should throw now that the payload is deleted.
  EXPECT_THROW(f.manager->ResolveSnapshot(id), std::exception);
}

TEST(VoidTier, SpillToVoidDeletesDiskPayload) {
  Fixture f;
  auto    id = f.AllocateAndCommit(TIER_RAM);
  f.manager->ExecuteSpill(id, TIER_DISK, /*fsync=*/false);
  ASSERT_TRUE(f.disk->Has(id));

  f.manager->ExecuteSpill(id, TIER_VOID, /*fsync=*/false);

  EXPECT_FALSE(f.disk->Has(id));
  auto bytes = f.manager->GetTierBytes();
  uint64_t disk_bytes = bytes.count(static_cast<int>(TIER_DISK)) ? bytes.at(static_cast<int>(TIER_DISK)) : 0;
  EXPECT_EQ(disk_bytes, 0u);
  EXPECT_THROW(f.manager->ResolveSnapshot(id), std::exception);
}

TEST(VoidTier, SpillToVoidWithRequireDurableThrows) {
  Fixture f;
  EvictionPolicy policy;
  policy.set_require_durable(true);
  auto id = f.AllocateAndCommit(TIER_RAM, 64, policy);

  // TIER_VOID is not durable, so this must be rejected.
  EXPECT_THROW(f.manager->ExecuteSpill(id, TIER_VOID, /*fsync=*/false), std::exception);

  // Payload must still be present after the failed spill.
  EXPECT_NO_THROW(f.manager->ResolveSnapshot(id));
}

TEST(VoidTier, SpillToVoidDecrementsTierCount) {
  Fixture    f;
  auto       id    = f.AllocateAndCommit(TIER_RAM);
  const auto bytes_before = f.manager->GetTierBytes();
  ASSERT_GT(bytes_before.count(static_cast<int>(TIER_RAM)), 0u);

  f.manager->ExecuteSpill(id, TIER_VOID, /*fsync=*/false);

  auto       bytes_after  = f.manager->GetTierBytes();
  uint64_t   ram_after    = bytes_after.count(static_cast<int>(TIER_RAM)) ? bytes_after.at(static_cast<int>(TIER_RAM)) : 0;
  EXPECT_EQ(ram_after, 0u);
}

// A subsequent Allocate+Commit after a void spill reuses the tier accounting
// cleanly (no underflow from the prior deletion).
TEST(VoidTier, TierBytesCleanAfterVoidThenReallocate) {
  Fixture f;
  auto    id = f.AllocateAndCommit(TIER_RAM, 128);
  f.manager->ExecuteSpill(id, TIER_VOID, /*fsync=*/false);

  auto id2 = f.AllocateAndCommit(TIER_RAM, 64);
  auto bytes = f.manager->GetTierBytes();
  EXPECT_EQ(bytes.at(static_cast<int>(TIER_RAM)), 64u);
}

} // namespace
