#include <cassert>
#include <iostream>
#include <memory>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/db/model/payload_record.hpp"
#include "internal/lease/lease_manager.hpp"

namespace {

using payload::core::PayloadManager;
using payload::db::memory::MemoryRepository;
using payload::db::model::PayloadRecord;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

void TestResolveSnapshotUsesCachedDescriptorUntilRefresh() {
  auto repository = std::make_shared<MemoryRepository>();
  {
    auto tx = repository->Begin();

    PayloadRecord seed;
    seed.id         = "payload-preloaded";
    seed.tier       = TIER_RAM;
    seed.state      = PAYLOAD_STATE_ACTIVE;
    seed.size_bytes = 1024;
    seed.version    = 1;

    auto insert_result = repository->InsertPayload(*tx, seed);
    assert(insert_result);
    tx->Commit();
  }

  auto manager = PayloadManager(/*storage=*/{}, std::make_shared<payload::lease::LeaseManager>(), /*metadata=*/nullptr,
                                /*lineage=*/nullptr, repository);

  payload::manager::v1::PayloadID id;
  id.set_value("payload-preloaded");

  const auto first = manager.ResolveSnapshot(id);
  assert(first.tier() == TIER_RAM);
  assert(first.version() == 1);

  {
    auto tx      = repository->Begin();
    auto current = repository->GetPayload(*tx, "payload-preloaded");
    assert(current.has_value());
    current->tier    = TIER_DISK;
    current->version = 2;
    auto update_result = repository->UpdatePayload(*tx, *current);
    assert(update_result);
    tx->Commit();
  }

  const auto stale_from_cache = manager.ResolveSnapshot(id);
  assert(stale_from_cache.tier() == TIER_RAM);
  assert(stale_from_cache.version() == 1);

  manager.HydrateCaches();

  const auto refreshed = manager.ResolveSnapshot(id);
  assert(refreshed.tier() == TIER_DISK);
  assert(refreshed.version() == 2);
}

} // namespace

int main() {
  TestResolveSnapshotUsesCachedDescriptorUntilRefresh();

  std::cout << "payload_manager_unit_snapshot_reads: pass\n";
  return 0;
}
