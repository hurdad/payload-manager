/*
  Unit tests for TIER_DISK_COLD: the optional second local durable level that
  sits between TIER_DISK_HOT and TIER_OBJECT.

  The cold tier is opt-in. The most important cases here are the negative ones:
  with no cold backend configured the demotion chain must stay exactly as it was
  before this tier existed (DISK → OBJECT), because every config file written so
  far relies on that.
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
using payload::manager::v1::PayloadID;
using payload::manager::v1::PayloadMetadata;
using payload::manager::v1::TIER_DISK_COLD;
using payload::manager::v1::TIER_DISK_HOT;
using payload::manager::v1::TIER_OBJECT;
using payload::manager::v1::TIER_RAM;
using payload::manager::v1::TIER_VOID;
using payload::metadata::MetadataCache;
using payload::tiering::PressureState;
using payload::tiering::TieringPolicy;

// ---------------------------------------------------------------------------
// Minimal in-process storage backend (mirrors void_tier_test.cpp)
// ---------------------------------------------------------------------------

class SimpleBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const PayloadID& id, uint64_t size) override {
    auto r = arrow::AllocateBuffer(size);
    if (!r.ok()) throw std::runtime_error("alloc failed");
    std::shared_ptr<arrow::Buffer> buf(std::move(*r));
    if (size > 0) std::memset(buf->mutable_data(), 0, size);
    bufs_[id.value()] = buf;
    return buf;
  }
  std::shared_ptr<arrow::Buffer> Read(const PayloadID& id) override {
    return bufs_.at(id.value());
  }
  void Write(const PayloadID& id, const std::shared_ptr<arrow::Buffer>& b, bool) override {
    bufs_[id.value()] = b;
  }
  void Remove(const PayloadID& id) override {
    bufs_.erase(id.value());
  }
  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }
  bool Has(const PayloadID& id) const {
    return bufs_.count(id.value()) > 0;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> bufs_;
};

// RAM + DISK + DISK_COLD: a runtime with the cold level configured.
struct ColdFixture {
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<SimpleBackend>                         ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                         disk      = std::make_shared<SimpleBackend>(TIER_DISK_HOT);
  std::shared_ptr<SimpleBackend>                         cold      = std::make_shared<SimpleBackend>(TIER_DISK_COLD);
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]       = ram;
    s[TIER_DISK_HOT]  = disk;
    s[TIER_DISK_COLD] = cold;
    return std::make_shared<PayloadManager>(s, lease_mgr, repo);
  }()};

  PayloadID AllocateAndCommit(payload::manager::v1::Tier tier, uint64_t size = 64, const EvictionPolicy& policy = {}) {
    auto desc = manager->Allocate(size, tier, /*ttl_ms=*/0, /*no_evict=*/false, policy);
    manager->Commit(desc.payload_id());
    return desc.payload_id();
  }
};

// RAM + DISK only: a runtime with no cold level, i.e. every config written
// before TIER_DISK_COLD existed.
struct NoColdFixture {
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<SimpleBackend>                         ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                         disk      = std::make_shared<SimpleBackend>(TIER_DISK_HOT);
  std::shared_ptr<PayloadManager>                        manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]      = ram;
    s[TIER_DISK_HOT] = disk;
    return std::make_shared<PayloadManager>(s, lease_mgr, repo);
  }()};

  PayloadID AllocateAndCommit(payload::manager::v1::Tier tier, uint64_t size = 64, const EvictionPolicy& policy = {}) {
    auto desc = manager->Allocate(size, tier, /*ttl_ms=*/0, /*no_evict=*/false, policy);
    manager->Commit(desc.payload_id());
    return desc.payload_id();
  }
};

// ---------------------------------------------------------------------------
// Back-compat: no cold tier configured
// ---------------------------------------------------------------------------

TEST(DiskColdTier, WithoutColdBackendDiskStillSpillsToObject) {
  NoColdFixture f;
  auto          id = f.AllocateAndCommit(TIER_DISK_HOT);
  EXPECT_EQ(f.manager->GetDiskHotSpillTarget(id), TIER_OBJECT) << "an unconfigured cold tier must leave the chain at DISK -> OBJECT";
}

TEST(DiskColdTier, WithoutColdBackendVoidOverrideStillHonored) {
  NoColdFixture  f;
  EvictionPolicy policy;
  policy.set_spill_target(TIER_VOID);
  auto id = f.AllocateAndCommit(TIER_DISK_HOT, 64, policy);
  EXPECT_EQ(f.manager->GetDiskHotSpillTarget(id), TIER_VOID);
}

TEST(DiskColdTier, UnconfiguredColdTierNeverReportsPressure) {
  // disk_cold_limit is left at its default, as factory.cpp does when no cold
  // tier is configured, and no bytes are ever accounted to it.
  PressureState state;
  EXPECT_FALSE(state.DiskColdPressure());
}

// ---------------------------------------------------------------------------
// Chain with the cold tier configured
// ---------------------------------------------------------------------------

TEST(DiskColdTier, DiskSpillsToColdWhenConfigured) {
  ColdFixture f;
  auto        id = f.AllocateAndCommit(TIER_DISK_HOT);
  EXPECT_EQ(f.manager->GetDiskHotSpillTarget(id), TIER_DISK_COLD);
}

TEST(DiskColdTier, VoidOverrideBeatsTheConfiguredColdTier) {
  ColdFixture    f;
  EvictionPolicy policy;
  policy.set_spill_target(TIER_VOID);
  auto id = f.AllocateAndCommit(TIER_DISK_HOT, 64, policy);
  EXPECT_EQ(f.manager->GetDiskHotSpillTarget(id), TIER_VOID) << "an explicit discard request outranks the configured chain";
}

TEST(DiskColdTier, ColdSpillsToObject) {
  ColdFixture f;
  auto        id = f.AllocateAndCommit(TIER_DISK_COLD);
  EXPECT_EQ(f.manager->GetDiskColdSpillTarget(id), TIER_OBJECT);
}

TEST(DiskColdTier, ColdSpillHonorsVoidOverride) {
  ColdFixture    f;
  EvictionPolicy policy;
  policy.set_spill_target(TIER_VOID);
  auto id = f.AllocateAndCommit(TIER_DISK_COLD, 64, policy);
  EXPECT_EQ(f.manager->GetDiskColdSpillTarget(id), TIER_VOID);
}

// ---------------------------------------------------------------------------
// Moving bytes through the chain
// ---------------------------------------------------------------------------

TEST(DiskColdTier, PayloadDemotesRamToDiskToColdToObject) {
  ColdFixture f;
  auto        id = f.AllocateAndCommit(TIER_RAM);

  f.manager->ExecuteSpill(id, TIER_DISK_HOT, /*fsync=*/false);
  ASSERT_TRUE(f.disk->Has(id));
  EXPECT_FALSE(f.cold->Has(id));

  f.manager->ExecuteSpill(id, TIER_DISK_COLD, /*fsync=*/false);
  EXPECT_FALSE(f.disk->Has(id)) << "the hot copy must be released once the payload is cold";
  ASSERT_TRUE(f.cold->Has(id));

  // Still resolvable and still reporting the cold tier.
  EXPECT_EQ(f.manager->ResolveSnapshot(id).tier(), TIER_DISK_COLD);
}

TEST(DiskColdTier, ColdTierBytesAreAccountedSeparatelyFromDisk) {
  ColdFixture f;
  auto        id = f.AllocateAndCommit(TIER_RAM);

  f.manager->ExecuteSpill(id, TIER_DISK_HOT, /*fsync=*/false);
  EXPECT_EQ(f.manager->GetTierBytes().at(static_cast<int>(TIER_DISK_HOT)), 64u);

  f.manager->ExecuteSpill(id, TIER_DISK_COLD, /*fsync=*/false);

  const auto bytes          = f.manager->GetTierBytes();
  uint64_t   disk_hot_bytes = bytes.count(static_cast<int>(TIER_DISK_HOT)) ? bytes.at(static_cast<int>(TIER_DISK_HOT)) : 0;
  uint64_t   cold_bytes     = bytes.count(static_cast<int>(TIER_DISK_COLD)) ? bytes.at(static_cast<int>(TIER_DISK_COLD)) : 0;
  EXPECT_EQ(disk_hot_bytes, 0u);
  EXPECT_EQ(cold_bytes, 64u);
}

TEST(DiskColdTier, ColdTierSatisfiesRequireDurable) {
  // TIER_DISK_COLD is durable local media, so a require_durable payload must be
  // allowed to demote onto it. Were IsDurableTier to omit the cold tier this
  // would throw.
  ColdFixture    f;
  EvictionPolicy policy;
  policy.set_require_durable(true);
  auto id = f.AllocateAndCommit(TIER_RAM, 64, policy);

  EXPECT_NO_THROW(f.manager->ExecuteSpill(id, TIER_DISK_COLD, /*fsync=*/false));
  EXPECT_TRUE(f.cold->Has(id));
}

TEST(DiskColdTier, MinResidencyDiskBlocksDemotionToCold) {
  // The cold level ranks below TIER_DISK_HOT, so a payload pinned to at least
  // TIER_DISK_HOT must not be pushed onto it. This is the case that an ordinal
  // tier compare would get wrong, since TIER_DISK_COLD has the highest enum
  // value of any tier.
  ColdFixture    f;
  EvictionPolicy policy;
  policy.set_min_residency_tier(TIER_DISK_HOT);
  auto id = f.AllocateAndCommit(TIER_RAM, 64, policy);

  f.manager->ExecuteSpill(id, TIER_DISK_HOT, /*fsync=*/false);
  EXPECT_THROW(f.manager->ExecuteSpill(id, TIER_DISK_COLD, /*fsync=*/false), std::exception);
  EXPECT_TRUE(f.disk->Has(id)) << "the payload must stay on the hot level after the rejected spill";
}

TEST(DiskColdTier, MinResidencyColdAllowsDemotionToColdButNotObject) {
  ColdFixture    f;
  EvictionPolicy policy;
  policy.set_min_residency_tier(TIER_DISK_COLD);
  auto id = f.AllocateAndCommit(TIER_RAM, 64, policy);

  EXPECT_NO_THROW(f.manager->ExecuteSpill(id, TIER_DISK_COLD, /*fsync=*/false));
  EXPECT_THROW(f.manager->ExecuteSpill(id, TIER_OBJECT, /*fsync=*/false), std::exception);
}

TEST(DiskColdTier, ColdPayloadIsDescribedAsDiskEvenWithoutAColdBackend) {
  // An operator who removes `disk_cold` from the config after payloads have
  // already aged onto it restarts with cold-tier records and no cold backend.
  // ToPayloadDescriptor builds the descriptor from the record alone, and
  // PopulateLocation returns early when the backend is absent — so if the
  // descriptor switch does not name TIER_DISK_COLD it falls through to the RAM
  // default and hands the client a shm name for a segment that never existed.
  auto repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  auto lease_mgr = std::make_shared<LeaseManager>();

  PayloadID id;
  {
    payload::storage::StorageFactory::TierMap with_cold;
    with_cold[TIER_RAM]       = std::make_shared<SimpleBackend>(TIER_RAM);
    with_cold[TIER_DISK_HOT]  = std::make_shared<SimpleBackend>(TIER_DISK_HOT);
    with_cold[TIER_DISK_COLD] = std::make_shared<SimpleBackend>(TIER_DISK_COLD);
    auto manager              = std::make_shared<PayloadManager>(with_cold, lease_mgr, repo);

    auto desc = manager->Allocate(64, TIER_DISK_COLD, /*ttl_ms=*/0, /*no_evict=*/false, EvictionPolicy{});
    manager->Commit(desc.payload_id());
    id = desc.payload_id();
  }

  // Same catalog, cold tier no longer configured.
  payload::storage::StorageFactory::TierMap without_cold;
  without_cold[TIER_RAM]      = std::make_shared<SimpleBackend>(TIER_RAM);
  without_cold[TIER_DISK_HOT] = std::make_shared<SimpleBackend>(TIER_DISK_HOT);
  auto manager                = std::make_shared<PayloadManager>(without_cold, lease_mgr, repo);

  const auto desc = manager->ResolveSnapshot(id);
  EXPECT_EQ(desc.tier(), TIER_DISK_COLD);
  EXPECT_TRUE(desc.has_disk()) << "a cold payload must be described by DiskLocation";
  EXPECT_FALSE(desc.has_ram()) << "a cold payload must never be described as shm-resident";
}

// ---------------------------------------------------------------------------
// Pressure and victim selection
// ---------------------------------------------------------------------------

TEST(DiskColdTier, ColdPressureFiresWhenBytesExceedLimit) {
  PressureState state;
  state.disk_cold_limit = 1024;
  state.disk_cold_bytes.store(512);
  EXPECT_FALSE(state.DiskColdPressure());
  state.disk_cold_bytes.store(1025);
  EXPECT_TRUE(state.DiskColdPressure());
}

TEST(DiskColdTier, ColdPressureRespectsHighWaterMark) {
  PressureState state;
  state.disk_cold_limit     = 1000;
  state.disk_cold_evict_pct = 80;
  EXPECT_EQ(state.DiskColdEvictThreshold(), 800u);
  state.disk_cold_bytes.store(801);
  EXPECT_TRUE(state.DiskColdPressure());
  state.disk_cold_bytes.store(799);
  EXPECT_FALSE(state.DiskColdPressure());
}

TEST(DiskColdTier, ColdPressureIsIndependentOfDiskPressure) {
  PressureState state;
  state.disk_hot_limit  = 1000;
  state.disk_cold_limit = 1000;
  state.disk_hot_bytes.store(1500);
  EXPECT_TRUE(state.DiskHotPressure());
  EXPECT_FALSE(state.DiskColdPressure()) << "hot-tier pressure must not evict from the cold tier";
}

TEST(DiskColdTier, ChooseColdEvictionUnderPressureReturnsVictim) {
  auto            cache = std::make_shared<MetadataCache>();
  PayloadMetadata meta;
  meta.mutable_id()->set_value("cold-payload");
  cache->Put(meta.id(), meta);

  auto policy = TieringPolicy(cache, {}, {}, {}, [](const PayloadID& id) { return id.value() == "cold-payload"; });

  PressureState state;
  state.disk_cold_limit = 0;
  state.disk_cold_bytes.store(1);
  const auto victim = policy.ChooseDiskColdEviction(state);
  ASSERT_TRUE(victim.has_value());
  EXPECT_EQ(victim->value(), "cold-payload");
}

TEST(DiskColdTier, ChooseColdEvictionNoPressureReturnsNull) {
  auto            cache = std::make_shared<MetadataCache>();
  PayloadMetadata meta;
  meta.mutable_id()->set_value("p1");
  cache->Put(meta.id(), meta);

  auto policy = TieringPolicy(cache, {}, {}, {}, [](const PayloadID&) { return true; });

  PressureState state;
  state.disk_cold_limit = 1000;
  state.disk_cold_bytes.store(500);
  EXPECT_FALSE(policy.ChooseDiskColdEviction(state).has_value());
}

TEST(DiskColdTier, PolicyWithoutColdPredicateStillConstructs) {
  // The cold predicate is the fifth, defaulted ctor argument; every existing
  // four-argument call site must keep compiling and behaving.
  auto            cache = std::make_shared<MetadataCache>();
  PayloadMetadata meta;
  meta.mutable_id()->set_value("p1");
  cache->Put(meta.id(), meta);

  auto policy = TieringPolicy(cache, {}, {}, [](const PayloadID&) { return true; });

  PressureState state;
  state.disk_hot_limit = 0;
  state.disk_hot_bytes.store(1);
  EXPECT_TRUE(policy.ChooseDiskHotEviction(state).has_value());
}

} // namespace
