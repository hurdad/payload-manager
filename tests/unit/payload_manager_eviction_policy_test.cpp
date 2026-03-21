/*
  Unit tests for persist and eviction_policy fields introduced in AllocatePayloadRequest.
*/

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/tiering/pressure_state.hpp"
#include "internal/tiering/tiering_policy.hpp"
#include "payload/manager/core/v1/policy.pb.h"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::core::v1::EVICTION_PRIORITY_HIGH;
using payload::manager::core::v1::EVICTION_PRIORITY_NEVER;
using payload::manager::core::v1::EVICTION_PRIORITY_UNSPECIFIED;
using payload::manager::core::v1::EvictionPolicy;
using payload::manager::v1::PayloadID;
using payload::manager::v1::PayloadMetadata;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_OBJECT;
using payload::manager::v1::TIER_RAM;
using payload::metadata::MetadataCache;
using payload::tiering::PressureState;
using payload::tiering::TieringPolicy;

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
// Common test fixture
// ---------------------------------------------------------------------------

struct Fixture {
  std::shared_ptr<LeaseManager>                          lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<SimpleStorageBackend>                  ram       = std::make_shared<SimpleStorageBackend>(TIER_RAM);
  std::shared_ptr<SimpleStorageBackend>                  disk      = std::make_shared<SimpleStorageBackend>(TIER_DISK);
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  PayloadManager                                         manager{[&] {
                           payload::storage::StorageFactory::TierMap storage;
                           storage[TIER_RAM]  = ram;
                           storage[TIER_DISK] = disk;
                           return storage;
                         }(),
                         lease_mgr, repo};
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

PayloadID MakeID(const std::string& v) {
  PayloadID id;
  id.set_value(v);
  return id;
}

void PutCacheEntry(MetadataCache& cache, const std::string& v) {
  auto            id = MakeID(v);
  PayloadMetadata m;
  *m.mutable_id() = id;
  cache.Put(id, m);
}

void SetPressure(PressureState& s) {
  s.ram_limit = 0;
  s.gpu_limit = 0;
  s.ram_bytes.store(1);
  s.gpu_bytes.store(1);
}

} // namespace

// persist=true — TTL is ignored; payload survives ExpireStale.
TEST(PayloadManagerEvictionPolicy, PersistSuppressesTTL) {
  Fixture f;

  const auto desc = f.manager.Commit(f.manager.Allocate(128, TIER_RAM, /*ttl_ms=*/1, /*persist=*/true).payload_id());
  EXPECT_TRUE(f.ram->Has(desc.payload_id()));

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  f.manager.ExpireStale();

  // Must still exist despite TTL.
  EXPECT_TRUE(f.ram->Has(desc.payload_id()));
  const auto snap = f.manager.ResolveSnapshot(desc.payload_id());
  EXPECT_EQ(snap.payload_id().value(), desc.payload_id().value());
}

// persist=true marks payload as eviction-exempt immediately after Allocate.
TEST(PayloadManagerEvictionPolicy, PersistMarksEvictionExempt) {
  Fixture f;

  const auto desc = f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/0, /*persist=*/true);
  EXPECT_TRUE(f.manager.IsEvictionExempt(desc.payload_id()));
}

// Non-persisted payload is not eviction-exempt.
TEST(PayloadManagerEvictionPolicy, NonPersistIsNotEvictionExempt) {
  Fixture f;

  const auto desc = f.manager.Allocate(64, TIER_RAM);
  EXPECT_FALSE(f.manager.IsEvictionExempt(desc.payload_id()));
}

// eviction_policy.priority = NEVER marks payload as eviction-exempt.
TEST(PayloadManagerEvictionPolicy, EvictionPriorityNeverMarksExempt) {
  Fixture f;

  EvictionPolicy policy;
  policy.set_priority(EVICTION_PRIORITY_NEVER);

  const auto desc = f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/0, /*persist=*/false, policy);
  EXPECT_TRUE(f.manager.IsEvictionExempt(desc.payload_id()));
}

// eviction_policy.priority != NEVER does not mark as exempt.
TEST(PayloadManagerEvictionPolicy, EvictionPriorityHighIsNotExempt) {
  Fixture f;

  EvictionPolicy policy;
  policy.set_priority(EVICTION_PRIORITY_HIGH);

  const auto desc = f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/0, /*persist=*/false, policy);
  EXPECT_FALSE(f.manager.IsEvictionExempt(desc.payload_id()));
}

// eviction_policy.spill_target is returned by GetSpillTarget.
TEST(PayloadManagerEvictionPolicy, SpillTargetIsRespected) {
  Fixture f;

  EvictionPolicy policy;
  policy.set_spill_target(TIER_OBJECT);

  const auto desc = f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/0, /*persist=*/false, policy);
  EXPECT_EQ(f.manager.GetSpillTarget(desc.payload_id()), TIER_OBJECT);
}

// Default spill target (no policy set) falls back to TIER_DISK.
TEST(PayloadManagerEvictionPolicy, DefaultSpillTargetIsDisk) {
  Fixture f;

  const auto desc = f.manager.Allocate(64, TIER_RAM);
  EXPECT_EQ(f.manager.GetSpillTarget(desc.payload_id()), TIER_DISK);
}

// Deleting a persisted payload clears its eviction-exempt status.
TEST(PayloadManagerEvictionPolicy, DeleteClearsEvictionExempt) {
  Fixture f;

  const auto desc = f.manager.Commit(f.manager.Allocate(64, TIER_RAM, 0, /*persist=*/true).payload_id());
  EXPECT_TRUE(f.manager.IsEvictionExempt(desc.payload_id()));

  f.manager.Delete(desc.payload_id(), /*force=*/true);
  EXPECT_FALSE(f.manager.IsEvictionExempt(desc.payload_id()));
}

// eviction_policy fields round-trip through the memory repository.
TEST(PayloadManagerEvictionPolicy, EvictionFieldsRoundTripThroughRepository) {
  auto repo = std::make_shared<payload::db::memory::MemoryRepository>();

  payload::db::model::PayloadRecord r;
  r.id                = "evict-roundtrip";
  r.tier              = TIER_RAM;
  r.state             = payload::manager::v1::PAYLOAD_STATE_ALLOCATED;
  r.size_bytes        = 64;
  r.version           = 1;
  r.persist           = true;
  r.eviction_priority = static_cast<int>(EVICTION_PRIORITY_NEVER);
  r.spill_target      = static_cast<int>(TIER_OBJECT);

  {
    auto tx = repo->Begin();
    repo->InsertPayload(*tx, r);
    tx->Commit();
  }

  auto tx     = repo->Begin();
  auto loaded = repo->GetPayload(*tx, "evict-roundtrip");
  tx->Commit();

  ASSERT_TRUE(loaded.has_value());
  EXPECT_EQ(loaded->persist, true);
  EXPECT_EQ(loaded->eviction_priority, static_cast<int>(EVICTION_PRIORITY_NEVER));
  EXPECT_EQ(loaded->spill_target, static_cast<int>(TIER_OBJECT));
}

// HydrateCaches restores eviction-exempt status from the repository.
TEST(PayloadManagerEvictionPolicy, HydrateCachesRestoresEvictionExempt) {
  Fixture f;

  // Allocate and persist — creates entry in repo + in-memory set.
  const auto desc = f.manager.Allocate(64, TIER_RAM, 0, /*persist=*/true);
  EXPECT_TRUE(f.manager.IsEvictionExempt(desc.payload_id()));

  // Hydrate from the repo — must remain exempt.
  f.manager.HydrateCaches();
  EXPECT_TRUE(f.manager.IsEvictionExempt(desc.payload_id()));
}

// TieringPolicy with a filter skips eviction-exempt payloads.
TEST(PayloadManagerEvictionPolicy, TieringPolicySkipsExemptPayloads) {
  auto cache = std::make_shared<MetadataCache>();

  PutCacheEntry(*cache, "exempt-a");
  PutCacheEntry(*cache, "exempt-b");

  // Both IDs are exempt; policy must return no victim.
  auto is_evictable = [](const PayloadID& id) -> bool { return id.value() != "exempt-a" && id.value() != "exempt-b"; };

  auto          policy = TieringPolicy(cache, is_evictable);
  PressureState state;
  SetPressure(state);
  const auto victim = policy.ChooseRamEviction(state);

  EXPECT_FALSE(victim.has_value());
}

// TieringPolicy selects the LRU non-exempt ID when exempt ones are present.
TEST(PayloadManagerEvictionPolicy, TieringPolicySelectsNonExemptLRUVictim) {
  auto cache = std::make_shared<MetadataCache>();

  PutCacheEntry(*cache, "oldest"); // LRU
  PutCacheEntry(*cache, "exempt"); // should be skipped
  PutCacheEntry(*cache, "newest"); // most recently used

  // Touch newest to move it to MRU position.
  (void)cache->Get(MakeID("newest"));

  // Mark "exempt" as not evictable.
  auto is_evictable = [](const PayloadID& id) -> bool { return id.value() != "exempt"; };

  auto          policy = TieringPolicy(cache, is_evictable);
  PressureState state;
  SetPressure(state);
  const auto victim = policy.ChooseRamEviction(state);

  ASSERT_TRUE(victim.has_value());
  EXPECT_EQ(victim->value(), "oldest");
}

// TieringPolicy with no filter still works (backward compatibility).
TEST(PayloadManagerEvictionPolicy, TieringPolicyNoFilterStillWorks) {
  auto cache = std::make_shared<MetadataCache>();
  PutCacheEntry(*cache, "only");

  auto          policy = TieringPolicy(cache);
  PressureState state;
  SetPressure(state);
  const auto victim = policy.ChooseRamEviction(state);

  ASSERT_TRUE(victim.has_value());
  EXPECT_EQ(victim->value(), "only");
}
