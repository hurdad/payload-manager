#include <gtest/gtest.h>

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

} // namespace

TEST(PayloadManagerSnapshotRead, ResolveSnapshotUsesCachedDescriptorUntilRefresh) {
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
    ASSERT_TRUE(insert_result);
    tx->Commit();
  }

  auto manager = PayloadManager(/*storage=*/{}, std::make_shared<payload::lease::LeaseManager>(), repository);

  payload::manager::v1::PayloadID id;
  id.set_value("payload-preloaded");

  const auto first = manager.ResolveSnapshot(id);
  EXPECT_EQ(first.tier(), TIER_RAM);
  EXPECT_EQ(first.version(), 1);

  {
    auto tx      = repository->Begin();
    auto current = repository->GetPayload(*tx, "payload-preloaded");
    ASSERT_TRUE(current.has_value());
    current->tier      = TIER_DISK;
    current->version   = 2;
    auto update_result = repository->UpdatePayload(*tx, *current);
    ASSERT_TRUE(update_result);
    tx->Commit();
  }

  const auto stale_from_cache = manager.ResolveSnapshot(id);
  EXPECT_EQ(stale_from_cache.tier(), TIER_RAM);
  EXPECT_EQ(stale_from_cache.version(), 1);

  manager.HydrateCaches();

  const auto refreshed = manager.ResolveSnapshot(id);
  EXPECT_EQ(refreshed.tier(), TIER_DISK);
  EXPECT_GT(refreshed.version(), 1) << "HydrateCaches must serve a fresher version than the stale cached one";
}
