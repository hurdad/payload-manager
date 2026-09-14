/*
  Tests for Critical Fix #1: TieringManager + PressureState wiring.
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
#include "payload/manager/v1.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

class SimpleBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size) override {
    auto r = arrow::AllocateBuffer(size);
    if (!r.ok()) throw std::runtime_error("alloc failed");
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
  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> bufs_;
};

struct Fixture {
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<SimpleBackend>                         ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                         disk      = std::make_shared<SimpleBackend>(TIER_DISK);
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]  = ram;
    s[TIER_DISK] = disk;
    return std::make_shared<PayloadManager>(s, lease_mgr, repo);
  }()};
};

} // namespace

// ---------------------------------------------------------------------------
// Test: GetTierBytes reflects the allocated size after Allocate+Commit
// ---------------------------------------------------------------------------
TEST(TieringPressure, GetTierBytesAfterAllocate) {
  Fixture f;

  auto desc = f.manager->Commit(f.manager->Allocate(256, TIER_RAM).payload_id());

  const auto bytes = f.manager->GetTierBytes();
  const auto it    = bytes.find(static_cast<int>(TIER_RAM));
  ASSERT_NE(it, bytes.end()) << "RAM tier must appear in GetTierBytes after allocate";
  EXPECT_EQ(it->second, 256u) << "GetTierBytes must report the allocated size";
}

// ---------------------------------------------------------------------------
// Test: GetTierBytes decrements after Delete
// ---------------------------------------------------------------------------
TEST(TieringPressure, GetTierBytesAfterDelete) {
  Fixture f;

  auto desc = f.manager->Commit(f.manager->Allocate(128, TIER_RAM).payload_id());
  f.manager->Delete(desc.payload_id(), /*force=*/true);

  const auto bytes     = f.manager->GetTierBytes();
  const auto it_ram    = bytes.find(static_cast<int>(TIER_RAM));
  uint64_t   ram_bytes = (it_ram != bytes.end()) ? it_ram->second : 0;
  EXPECT_EQ(ram_bytes, 0u) << "RAM bytes must be zero after Delete";
}

// ---------------------------------------------------------------------------
// Test: GetTierBytes transfers bytes from source tier to dest tier after Spill
// ---------------------------------------------------------------------------
TEST(TieringPressure, GetTierBytesAfterSpill) {
  Fixture f;

  auto desc = f.manager->Commit(f.manager->Allocate(64, TIER_RAM).payload_id());

  // Verify RAM has bytes before spill.
  {
    const auto bytes = f.manager->GetTierBytes();
    EXPECT_TRUE(bytes.count(static_cast<int>(TIER_RAM)) && bytes.at(static_cast<int>(TIER_RAM)) == 64);
    EXPECT_TRUE(!bytes.count(static_cast<int>(TIER_DISK)) || bytes.at(static_cast<int>(TIER_DISK)) == 0);
  }

  f.manager->ExecuteSpill(desc.payload_id(), TIER_DISK, /*fsync=*/false);

  // RAM bytes gone; disk bytes added.
  {
    const auto bytes    = f.manager->GetTierBytes();
    uint64_t   ram_val  = bytes.count(static_cast<int>(TIER_RAM)) ? bytes.at(static_cast<int>(TIER_RAM)) : 0;
    uint64_t   disk_val = bytes.count(static_cast<int>(TIER_DISK)) ? bytes.at(static_cast<int>(TIER_DISK)) : 0;
    EXPECT_EQ(ram_val, 0u) << "RAM bytes must be zero after spill";
    EXPECT_EQ(disk_val, 64u) << "disk bytes must equal original size after spill";
  }
}

// ---------------------------------------------------------------------------
// Test: PressureState.RamPressure() fires only when bytes exceed the limit,
//       and TieringPolicy.ChooseRamEviction returns a victim exactly then.
// ---------------------------------------------------------------------------
TEST(TieringPressure, PressureStateAndPolicyIntegration) {
  Fixture f;
  auto    cache = std::make_shared<payload::metadata::MetadataCache>();

  // Allocate a payload so it is tracked in GetTierBytes.
  auto desc = f.manager->Commit(f.manager->Allocate(200, TIER_RAM).payload_id());

  // Seed the metadata cache so TieringPolicy has a candidate victim.
  payload::manager::v1::PayloadMetadata meta;
  *meta.mutable_id() = desc.payload_id();
  cache->Put(desc.payload_id(), meta);

  auto policy = std::make_shared<payload::tiering::TieringPolicy>(
      cache, [&](const payload::manager::v1::PayloadID& id) { return !f.manager->IsEvictionExempt(id); });

  // ---- Case 1: limit NOT exceeded ----
  payload::tiering::PressureState state_ok;
  state_ok.ram_limit = 300; // 200 < 300 → no pressure

  // Sync bytes as TieringManager::Loop would.
  const auto tier_bytes = f.manager->GetTierBytes();
  state_ok.ram_bytes.store(tier_bytes.count(static_cast<int>(TIER_RAM)) ? tier_bytes.at(static_cast<int>(TIER_RAM)) : 0);

  EXPECT_FALSE(state_ok.RamPressure()) << "no pressure when bytes are below limit";
  EXPECT_FALSE(policy->ChooseRamEviction(state_ok).has_value()) << "policy must not emit victim when not under pressure";

  // ---- Case 2: limit exceeded ----
  payload::tiering::PressureState state_over;
  state_over.ram_limit = 100; // 200 > 100 → pressure
  state_over.ram_bytes.store(tier_bytes.count(static_cast<int>(TIER_RAM)) ? tier_bytes.at(static_cast<int>(TIER_RAM)) : 0);

  EXPECT_TRUE(state_over.RamPressure()) << "pressure must be detected when bytes exceed limit";
  const auto victim = policy->ChooseRamEviction(state_over);
  ASSERT_TRUE(victim.has_value()) << "policy must return a victim when under pressure";
  EXPECT_EQ(victim->value(), desc.payload_id().value()) << "victim must be the allocated payload";
}

// ---------------------------------------------------------------------------
// Test: PressureState GPU limit works analogously to RAM
// ---------------------------------------------------------------------------
TEST(TieringPressure, GpuPressureStateLimit) {
  payload::tiering::PressureState state;
  state.gpu_limit = 512;
  state.gpu_bytes.store(256);
  EXPECT_FALSE(state.GpuPressure()) << "no GPU pressure when bytes < limit";
  state.gpu_bytes.store(1024);
  EXPECT_TRUE(state.GpuPressure()) << "GPU pressure when bytes > limit";
}

// ---------------------------------------------------------------------------
// Eviction high-water marks.
//
// Before eviction_high_water_pct existed, a tier only came under pressure once
// it was already over its hard cap, leaving the 100 ms tiering loop no headroom
// — a bursty producer could exhaust TIER_RAM between two passes and SIGBUS on
// its next write. These pin the soft-cap behaviour and, importantly, that an
// unset percentage still behaves exactly as before.
// ---------------------------------------------------------------------------

TEST(TieringPressure, UnsetHighWaterKeepsEvictAtHardCap) {
  payload::tiering::PressureState state;
  state.ram_limit = 1000; // ram_evict_pct left at 0 (unset)

  EXPECT_EQ(state.RamEvictThreshold(), 1000u) << "unset pct must leave the threshold at the hard cap";

  state.ram_bytes.store(999);
  EXPECT_FALSE(state.RamPressure());
  state.ram_bytes.store(1000);
  EXPECT_FALSE(state.RamPressure()) << "at exactly the cap is not yet over it";
  state.ram_bytes.store(1001);
  EXPECT_TRUE(state.RamPressure());
}

TEST(TieringPressure, HighWaterTriggersEvictionBeforeHardCap) {
  payload::tiering::PressureState state;
  state.ram_limit     = 1000;
  state.ram_evict_pct = 80;

  EXPECT_EQ(state.RamEvictThreshold(), 800u);

  state.ram_bytes.store(799);
  EXPECT_FALSE(state.RamPressure()) << "below the high-water mark there is no pressure";
  state.ram_bytes.store(801);
  EXPECT_TRUE(state.RamPressure()) << "eviction must start well before the hard cap";
}

TEST(TieringPressure, HighWaterAppliesToEveryTier) {
  payload::tiering::PressureState state;
  state.ram_limit      = 1000;
  state.gpu_limit      = 2000;
  state.disk_limit     = 4000;
  state.ram_evict_pct  = 50;
  state.gpu_evict_pct  = 25;
  state.disk_evict_pct = 90;

  EXPECT_EQ(state.RamEvictThreshold(), 500u);
  EXPECT_EQ(state.GpuEvictThreshold(), 500u);
  EXPECT_EQ(state.DiskEvictThreshold(), 3600u);

  state.gpu_bytes.store(600);
  EXPECT_TRUE(state.GpuPressure());
  state.disk_bytes.store(3000);
  EXPECT_FALSE(state.DiskPressure());
}

TEST(TieringPressure, HighWaterOf100EvictsOnlyAtHardCap) {
  payload::tiering::PressureState state;
  state.ram_limit     = 1000;
  state.ram_evict_pct = 100;

  EXPECT_EQ(state.RamEvictThreshold(), 1000u);
  state.ram_bytes.store(1000);
  EXPECT_FALSE(state.RamPressure());
  state.ram_bytes.store(1001);
  EXPECT_TRUE(state.RamPressure());
}

TEST(TieringPressure, HighWaterLeavesUncappedTiersUncapped) {
  payload::tiering::PressureState state;
  // factory.cpp maps an unconfigured capacity to UINT64_MAX meaning "never
  // evict"; applying a percentage to that must not create a finite threshold.
  state.ram_limit     = UINT64_MAX;
  state.ram_evict_pct = 80;

  EXPECT_EQ(state.RamEvictThreshold(), UINT64_MAX);
  state.ram_bytes.store(UINT64_MAX - 1);
  EXPECT_FALSE(state.RamPressure()) << "an uncapped tier must never report pressure";
}

TEST(TieringPressure, HighWaterDoesNotOverflowOnHugeCaps) {
  // limit*pct/100 would overflow above ~184 PiB; the implementation divides
  // first. 2^60 bytes = 1 EiB.
  constexpr uint64_t kHuge = uint64_t{1} << 60;
  const auto         t     = payload::tiering::PressureState::EvictionThreshold(kHuge, 80);
  EXPECT_GT(t, kHuge / 2) << "threshold must not wrap around";
  EXPECT_LT(t, kHuge) << "threshold must still be below the hard cap";
}
