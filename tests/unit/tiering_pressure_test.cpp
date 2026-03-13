/*
  Tests for Critical Fix #1: TieringManager + PressureState wiring.

  Covered:
    - PayloadManager::GetTierBytes() reflects allocate / spill / delete
    - PressureState::RamPressure() triggers correctly from synced byte counts
    - TieringPolicy::ChooseRamEviction returns a victim only when pressure is
      above the configured limit (the same check TieringManager::Loop runs)
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

// ---------------------------------------------------------------------------
// Test: GetTierBytes reflects the allocated size after Allocate+Commit
// ---------------------------------------------------------------------------
void TestGetTierBytesAfterAllocate() {
  Fixture f;

  auto desc = f.manager->Commit(f.manager->Allocate(256, TIER_RAM).payload_id());

  const auto bytes = f.manager->GetTierBytes();
  const auto it    = bytes.find(static_cast<int>(TIER_RAM));
  assert(it != bytes.end() && "RAM tier must appear in GetTierBytes after allocate");
  assert(it->second == 256 && "GetTierBytes must report the allocated size");
}

// ---------------------------------------------------------------------------
// Test: GetTierBytes decrements after Delete
// ---------------------------------------------------------------------------
void TestGetTierBytesAfterDelete() {
  Fixture f;

  auto desc = f.manager->Commit(f.manager->Allocate(128, TIER_RAM).payload_id());
  f.manager->Delete(desc.payload_id(), /*force=*/true);

  const auto bytes     = f.manager->GetTierBytes();
  const auto it_ram    = bytes.find(static_cast<int>(TIER_RAM));
  uint64_t   ram_bytes = (it_ram != bytes.end()) ? it_ram->second : 0;
  assert(ram_bytes == 0 && "RAM bytes must be zero after Delete");
}

// ---------------------------------------------------------------------------
// Test: GetTierBytes transfers bytes from source tier to dest tier after Spill
// ---------------------------------------------------------------------------
void TestGetTierBytesAfterSpill() {
  Fixture f;

  auto desc = f.manager->Commit(f.manager->Allocate(64, TIER_RAM).payload_id());

  // Verify RAM has bytes before spill.
  {
    const auto bytes = f.manager->GetTierBytes();
    assert(bytes.count(static_cast<int>(TIER_RAM)) && bytes.at(static_cast<int>(TIER_RAM)) == 64);
    assert(!bytes.count(static_cast<int>(TIER_DISK)) || bytes.at(static_cast<int>(TIER_DISK)) == 0);
  }

  f.manager->ExecuteSpill(desc.payload_id(), TIER_DISK, /*fsync=*/false);

  // RAM bytes gone; disk bytes added.
  {
    const auto bytes    = f.manager->GetTierBytes();
    uint64_t   ram_val  = bytes.count(static_cast<int>(TIER_RAM)) ? bytes.at(static_cast<int>(TIER_RAM)) : 0;
    uint64_t   disk_val = bytes.count(static_cast<int>(TIER_DISK)) ? bytes.at(static_cast<int>(TIER_DISK)) : 0;
    assert(ram_val == 0 && "RAM bytes must be zero after spill");
    assert(disk_val == 64 && "disk bytes must equal original size after spill");
  }
}

// ---------------------------------------------------------------------------
// Test: PressureState.RamPressure() fires only when bytes exceed the limit,
//       and TieringPolicy.ChooseRamEviction returns a victim exactly then.
//       This mirrors the sync TieringManager::Loop performs each iteration.
// ---------------------------------------------------------------------------
void TestPressureStateAndPolicyIntegration() {
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

  assert(!state_ok.RamPressure() && "no pressure when bytes are below limit");
  assert(!policy->ChooseRamEviction(state_ok).has_value() && "policy must not emit victim when not under pressure");

  // ---- Case 2: limit exceeded ----
  payload::tiering::PressureState state_over;
  state_over.ram_limit = 100; // 200 > 100 → pressure
  state_over.ram_bytes.store(tier_bytes.count(static_cast<int>(TIER_RAM)) ? tier_bytes.at(static_cast<int>(TIER_RAM)) : 0);

  assert(state_over.RamPressure() && "pressure must be detected when bytes exceed limit");
  const auto victim = policy->ChooseRamEviction(state_over);
  assert(victim.has_value() && "policy must return a victim when under pressure");
  assert(victim->value() == desc.payload_id().value() && "victim must be the allocated payload");
}

// ---------------------------------------------------------------------------
// Test: PressureState GPU limit works analogously to RAM
// ---------------------------------------------------------------------------
void TestGpuPressureStateLimit() {
  payload::tiering::PressureState state;
  state.gpu_limit = 512;
  state.gpu_bytes.store(256);
  assert(!state.GpuPressure() && "no GPU pressure when bytes < limit");
  state.gpu_bytes.store(1024);
  assert(state.GpuPressure() && "GPU pressure when bytes > limit");
}

} // namespace

int main() {
  TestGetTierBytesAfterAllocate();
  TestGetTierBytesAfterDelete();
  TestGetTierBytesAfterSpill();
  TestPressureStateAndPolicyIntegration();
  TestGpuPressureStateLimit();

  std::cout << "tiering_pressure_test: pass\n";
  return 0;
}
