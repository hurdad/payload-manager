/*
  Unit tests for persist and eviction_policy fields introduced in AllocatePayloadRequest.

  Covered:
    - persist=true suppresses TTL expiry
    - persist=true marks payload as eviction-exempt
    - eviction_policy.priority=NEVER marks payload as eviction-exempt
    - eviction_policy.spill_target is returned by GetSpillTarget
    - delete removes eviction-exempt status
    - eviction policy fields round-trip through the memory repository
    - TieringPolicy skips eviction-exempt payloads when choosing victims
    - TieringPolicy still selects non-exempt LRU victim when exempt IDs exist
*/

#include <cassert>
#include <cstring>
#include <iostream>
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
using payload::manager::core::v1::EvictionPolicy;
using payload::manager::core::v1::EVICTION_PRIORITY_HIGH;
using payload::manager::core::v1::EVICTION_PRIORITY_NEVER;
using payload::manager::core::v1::EVICTION_PRIORITY_UNSPECIFIED;
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
  std::shared_ptr<LeaseManager>                           lease_mgr = std::make_shared<LeaseManager>();
  std::shared_ptr<SimpleStorageBackend>                   ram       = std::make_shared<SimpleStorageBackend>(TIER_RAM);
  std::shared_ptr<SimpleStorageBackend>                   disk      = std::make_shared<SimpleStorageBackend>(TIER_DISK);
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  PayloadManager                                          manager{[&] {
    payload::storage::StorageFactory::TierMap storage;
    storage[TIER_RAM]  = ram;
    storage[TIER_DISK] = disk;
    return storage;
  }(),
                                                                  lease_mgr, nullptr, nullptr, repo};
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// persist=true — TTL is ignored; payload survives ExpireStale.
void TestPersistSuppressesTTL() {
  Fixture f;

  const auto desc =
      f.manager.Commit(f.manager.Allocate(128, TIER_RAM, /*ttl_ms=*/1, /*persist=*/true).payload_id());
  assert(f.ram->Has(desc.payload_id()));

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  f.manager.ExpireStale();

  // Must still exist despite TTL.
  assert(f.ram->Has(desc.payload_id()));
  const auto snap = f.manager.ResolveSnapshot(desc.payload_id());
  assert(snap.payload_id().value() == desc.payload_id().value());
}

// persist=true marks payload as eviction-exempt immediately after Allocate.
void TestPersistMarksEvictionExempt() {
  Fixture f;

  const auto desc = f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/0, /*persist=*/true);
  assert(f.manager.IsEvictionExempt(desc.payload_id()));
}

// Non-persisted payload is not eviction-exempt.
void TestNonPersistIsNotEvictionExempt() {
  Fixture f;

  const auto desc = f.manager.Allocate(64, TIER_RAM);
  assert(!f.manager.IsEvictionExempt(desc.payload_id()));
}

// eviction_policy.priority = NEVER marks payload as eviction-exempt.
void TestEvictionPriorityNeverMarksExempt() {
  Fixture f;

  EvictionPolicy policy;
  policy.set_priority(EVICTION_PRIORITY_NEVER);

  const auto desc = f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/0, /*persist=*/false, policy);
  assert(f.manager.IsEvictionExempt(desc.payload_id()));
}

// eviction_policy.priority != NEVER does not mark as exempt.
void TestEvictionPriorityHighIsNotExempt() {
  Fixture f;

  EvictionPolicy policy;
  policy.set_priority(EVICTION_PRIORITY_HIGH);

  const auto desc = f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/0, /*persist=*/false, policy);
  assert(!f.manager.IsEvictionExempt(desc.payload_id()));
}

// eviction_policy.spill_target is returned by GetSpillTarget.
void TestSpillTargetIsRespected() {
  Fixture f;

  EvictionPolicy policy;
  policy.set_spill_target(TIER_OBJECT);

  const auto desc = f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/0, /*persist=*/false, policy);
  assert(f.manager.GetSpillTarget(desc.payload_id()) == TIER_OBJECT);
}

// Default spill target (no policy set) falls back to TIER_DISK.
void TestDefaultSpillTargetIsDisk() {
  Fixture f;

  const auto desc = f.manager.Allocate(64, TIER_RAM);
  assert(f.manager.GetSpillTarget(desc.payload_id()) == TIER_DISK);
}

// Deleting a persisted payload clears its eviction-exempt status.
void TestDeleteClearsEvictionExempt() {
  Fixture f;

  const auto desc = f.manager.Commit(f.manager.Allocate(64, TIER_RAM, 0, /*persist=*/true).payload_id());
  assert(f.manager.IsEvictionExempt(desc.payload_id()));

  f.manager.Delete(desc.payload_id(), /*force=*/true);
  assert(!f.manager.IsEvictionExempt(desc.payload_id()));
}

// eviction_policy fields round-trip through the memory repository.
void TestEvictionFieldsRoundTripThroughRepository() {
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

  assert(loaded.has_value());
  assert(loaded->persist == true);
  assert(loaded->eviction_priority == static_cast<int>(EVICTION_PRIORITY_NEVER));
  assert(loaded->spill_target == static_cast<int>(TIER_OBJECT));
}

// HydrateCaches restores eviction-exempt status from the repository.
void TestHydrateCachesRestoresEvictionExempt() {
  Fixture f;

  // Allocate and persist — creates entry in repo + in-memory set.
  const auto desc = f.manager.Allocate(64, TIER_RAM, 0, /*persist=*/true);
  assert(f.manager.IsEvictionExempt(desc.payload_id()));

  // Hydrate from the repo — must remain exempt.
  f.manager.HydrateCaches();
  assert(f.manager.IsEvictionExempt(desc.payload_id()));
}

// TieringPolicy with a filter skips eviction-exempt payloads.
void TestTieringPolicySkipsExemptPayloads() {
  auto cache = std::make_shared<MetadataCache>();

  PutCacheEntry(*cache, "exempt-a");
  PutCacheEntry(*cache, "exempt-b");

  // Both IDs are exempt; policy must return no victim.
  auto is_evictable = [](const PayloadID& id) -> bool {
    return id.value() != "exempt-a" && id.value() != "exempt-b";
  };

  auto    policy = TieringPolicy(cache, is_evictable);
  PressureState state; SetPressure(state);
  const auto victim = policy.ChooseRamEviction(state);

  assert(!victim.has_value());
}

// TieringPolicy selects the LRU non-exempt ID when exempt ones are present.
void TestTieringPolicySelectsNonExemptLRUVictim() {
  auto cache = std::make_shared<MetadataCache>();

  PutCacheEntry(*cache, "oldest");    // LRU
  PutCacheEntry(*cache, "exempt");    // should be skipped
  PutCacheEntry(*cache, "newest");    // most recently used

  // Touch newest to move it to MRU position.
  (void)cache->Get(MakeID("newest"));

  // Mark "exempt" as not evictable.
  auto is_evictable = [](const PayloadID& id) -> bool { return id.value() != "exempt"; };

  auto    policy = TieringPolicy(cache, is_evictable);
  PressureState state; SetPressure(state);
  const auto victim = policy.ChooseRamEviction(state);

  assert(victim.has_value());
  assert(victim->value() == "oldest");
}

// TieringPolicy with no filter still works (backward compatibility).
void TestTieringPolicyNoFilterStillWorks() {
  auto cache = std::make_shared<MetadataCache>();
  PutCacheEntry(*cache, "only");

  auto    policy = TieringPolicy(cache);
  PressureState state; SetPressure(state);
  const auto victim = policy.ChooseRamEviction(state);

  assert(victim.has_value());
  assert(victim->value() == "only");
}

} // namespace

int main() {
  TestPersistSuppressesTTL();
  TestPersistMarksEvictionExempt();
  TestNonPersistIsNotEvictionExempt();
  TestEvictionPriorityNeverMarksExempt();
  TestEvictionPriorityHighIsNotExempt();
  TestSpillTargetIsRespected();
  TestDefaultSpillTargetIsDisk();
  TestDeleteClearsEvictionExempt();
  TestEvictionFieldsRoundTripThroughRepository();
  TestHydrateCachesRestoresEvictionExempt();
  TestTieringPolicySkipsExemptPayloads();
  TestTieringPolicySelectsNonExemptLRUVictim();
  TestTieringPolicyNoFilterStillWorks();

  std::cout << "payload_manager_unit_eviction_policy: pass\n";
  return 0;
}
