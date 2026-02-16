#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

PayloadManager MakeManager(const std::shared_ptr<LeaseManager>& lease_mgr) {
  return PayloadManager(/*storage=*/{}, lease_mgr, /*metadata=*/nullptr, /*lineage=*/nullptr,
                        std::make_shared<payload::db::memory::MemoryRepository>());
}

void TestForceDeleteRemovesPayloadAndLeases() {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto descriptor = manager.Commit(manager.Allocate(1024, TIER_RAM).id());
  assert(descriptor.state() == PAYLOAD_STATE_ACTIVE);

  auto lease = manager.AcquireReadLease(descriptor.id(), TIER_RAM, 60'000);
  assert(!lease.lease_id().value().empty());
  assert(lease_mgr->HasActiveLeases(descriptor.id()));

  manager.Delete(descriptor.id(), /*force=*/true);

  assert(!lease_mgr->HasActiveLeases(descriptor.id()));
  bool threw = false;
  try {
    (void)manager.ResolveSnapshot(descriptor.id());
  } catch (const std::runtime_error&) {
    threw = true;
  }
  assert(threw);
}

void TestNonForceDeleteRejectsWhenLeaseIsActive() {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto descriptor = manager.Commit(manager.Allocate(1024, TIER_RAM).id());
  auto       lease      = manager.AcquireReadLease(descriptor.id(), TIER_RAM, 60'000);
  assert(!lease.lease_id().value().empty());

  bool threw = false;
  try {
    manager.Delete(descriptor.id(), /*force=*/false);
  } catch (const std::runtime_error& ex) {
    threw = std::string(ex.what()).find("active lease") != std::string::npos;
  }

  assert(threw);
  assert(lease_mgr->HasActiveLeases(descriptor.id()));
  assert(manager.ResolveSnapshot(descriptor.id()).id().value() == descriptor.id().value());
}

void TestCacheCoherenceAcrossCommitPromoteAndDelete() {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto manager   = MakeManager(lease_mgr);

  const auto allocated = manager.Allocate(1024, TIER_RAM);
  const auto committed = manager.Commit(allocated.id());
  assert(committed.state() == PAYLOAD_STATE_ACTIVE);
  assert(committed.version() == 2);

  const auto committed_snapshot = manager.ResolveSnapshot(allocated.id());
  assert(committed_snapshot.state() == PAYLOAD_STATE_ACTIVE);
  assert(committed_snapshot.version() == 2);

  const auto promoted = manager.Promote(allocated.id(), TIER_DISK);
  assert(promoted.tier() == TIER_DISK);

  const auto promoted_snapshot = manager.ResolveSnapshot(allocated.id());
  assert(promoted_snapshot.tier() == TIER_DISK);
  assert(promoted_snapshot.version() == promoted.version());

  manager.Delete(allocated.id(), /*force=*/true);

  bool threw = false;
  try {
    (void)manager.ResolveSnapshot(allocated.id());
  } catch (const std::runtime_error&) {
    threw = true;
  }
  assert(threw);
}

} // namespace

int main() {
  TestForceDeleteRemovesPayloadAndLeases();
  TestNonForceDeleteRejectsWhenLeaseIsActive();
  TestCacheCoherenceAcrossCommitPromoteAndDelete();

  std::cout << "payload_manager_unit_payload_delete: pass\n";
  return 0;
}
