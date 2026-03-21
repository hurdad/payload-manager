#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

class SimpleStorageBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleStorageBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override {
    auto result = arrow::AllocateBuffer(size_bytes);
    if (!result.ok()) throw std::runtime_error(result.status().ToString());
    std::shared_ptr<arrow::Buffer> buf(std::move(*result));
    if (size_bytes > 0) std::memset(buf->mutable_data(), 0, static_cast<size_t>(size_bytes));
    buffers_[id.value()] = buf;
    return buf;
  }

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override {
    return buffers_.at(id.value());
  }

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buf, bool) override {
    buffers_[id.value()] = buf;
  }

  void Remove(const payload::manager::v1::PayloadID& id) override {
    buffers_.erase(id.value());
  }

  bool Has(const payload::manager::v1::PayloadID& id) const {
    return buffers_.count(id.value()) > 0;
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
};

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

} // namespace

// Allocate with TTL=1ms, sleep past expiry, call ExpireStale —
// payload must be gone from storage and unresolvable.
TEST(PayloadManagerTTL, ExpireStaleDeletesExpiredPayload) {
  Fixture f;

  const auto desc = f.manager.Commit(f.manager.Allocate(128, TIER_RAM, /*ttl_ms=*/1).payload_id());
  EXPECT_TRUE(f.ram->Has(desc.payload_id()));

  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  f.manager.ExpireStale();

  EXPECT_FALSE(f.ram->Has(desc.payload_id()));

  EXPECT_THROW((void)f.manager.ResolveSnapshot(desc.payload_id()), std::runtime_error);
}

// Allocate with TTL=10000ms (not yet expired), call ExpireStale —
// payload must still exist.
TEST(PayloadManagerTTL, ExpireStaleDoesNotDeleteLivingPayload) {
  Fixture f;

  const auto desc = f.manager.Commit(f.manager.Allocate(128, TIER_RAM, /*ttl_ms=*/10'000).payload_id());
  EXPECT_TRUE(f.ram->Has(desc.payload_id()));

  f.manager.ExpireStale();

  EXPECT_TRUE(f.ram->Has(desc.payload_id()));
  const auto snapshot = f.manager.ResolveSnapshot(desc.payload_id());
  EXPECT_EQ(snapshot.payload_id().value(), desc.payload_id().value());
}

// Allocate with no TTL (ttl_ms=0) — ExpireStale must never touch it.
TEST(PayloadManagerTTL, ExpireStaleIgnoresPayloadWithNoTtl) {
  Fixture f;

  const auto desc = f.manager.Commit(f.manager.Allocate(128, TIER_RAM).payload_id());

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  f.manager.ExpireStale();

  EXPECT_TRUE(f.ram->Has(desc.payload_id()));
}

// Expired payload that has been spilled to disk — ExpireStale removes from disk.
TEST(PayloadManagerTTL, ExpireStaleRemovesExpiredSpilledPayload) {
  Fixture f;

  const auto desc = f.manager.Commit(f.manager.Allocate(128, TIER_RAM, /*ttl_ms=*/1).payload_id());
  f.manager.ExecuteSpill(desc.payload_id(), TIER_DISK, /*fsync=*/false);
  EXPECT_TRUE(f.disk->Has(desc.payload_id()));
  EXPECT_FALSE(f.ram->Has(desc.payload_id()));

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  f.manager.ExpireStale();

  EXPECT_FALSE(f.disk->Has(desc.payload_id()));
}

// Two payloads: one expired, one not. Only the expired one is removed.
TEST(PayloadManagerTTL, ExpireStaleIsSelective) {
  Fixture f;

  const auto expired = f.manager.Commit(f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/1).payload_id());
  const auto alive   = f.manager.Commit(f.manager.Allocate(64, TIER_RAM, /*ttl_ms=*/10'000).payload_id());

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  f.manager.ExpireStale();

  EXPECT_FALSE(f.ram->Has(expired.payload_id()));
  EXPECT_TRUE(f.ram->Has(alive.payload_id()));

  EXPECT_THROW((void)f.manager.ResolveSnapshot(expired.payload_id()), std::runtime_error);

  const auto snapshot = f.manager.ResolveSnapshot(alive.payload_id());
  EXPECT_EQ(snapshot.payload_id().value(), alive.payload_id().value());
}

// ListExpiredPayloads via the memory repository directly.
TEST(PayloadManagerTTL, MemoryRepositoryListExpiredPayloads) {
  auto repo = std::make_shared<payload::db::memory::MemoryRepository>();

  payload::db::model::PayloadRecord r1;
  r1.id            = "aaaa";
  r1.tier          = TIER_RAM;
  r1.state         = payload::manager::v1::PAYLOAD_STATE_ACTIVE;
  r1.size_bytes    = 64;
  r1.version       = 1;
  r1.expires_at_ms = 1; // already expired

  payload::db::model::PayloadRecord r2;
  r2.id            = "bbbb";
  r2.tier          = TIER_RAM;
  r2.state         = payload::manager::v1::PAYLOAD_STATE_ACTIVE;
  r2.size_bytes    = 64;
  r2.version       = 1;
  r2.expires_at_ms = 0; // no TTL

  payload::db::model::PayloadRecord r3;
  r3.id            = "cccc";
  r3.tier          = TIER_RAM;
  r3.state         = payload::manager::v1::PAYLOAD_STATE_ACTIVE;
  r3.size_bytes    = 64;
  r3.version       = 1;
  r3.expires_at_ms = 9999999999999ULL; // far future

  {
    auto tx = repo->Begin();
    repo->InsertPayload(*tx, r1);
    repo->InsertPayload(*tx, r2);
    repo->InsertPayload(*tx, r3);
    tx->Commit();
  }

  auto       tx      = repo->Begin();
  const auto expired = repo->ListExpiredPayloads(*tx, /*now_ms=*/1000);
  tx->Commit();

  EXPECT_EQ(expired.size(), 1u);
  EXPECT_EQ(expired[0].id, "aaaa");
}

// TTL is stored and round-trips through the memory repository.
TEST(PayloadManagerTTL, TtlRoundTripsThroughRepository) {
  auto repo = std::make_shared<payload::db::memory::MemoryRepository>();

  payload::db::model::PayloadRecord r;
  r.id            = "test-ttl";
  r.tier          = TIER_RAM;
  r.state         = payload::manager::v1::PAYLOAD_STATE_ALLOCATED;
  r.size_bytes    = 128;
  r.version       = 1;
  r.expires_at_ms = 123456789ULL;

  {
    auto tx = repo->Begin();
    repo->InsertPayload(*tx, r);
    tx->Commit();
  }

  auto tx     = repo->Begin();
  auto loaded = repo->GetPayload(*tx, "test-ttl");
  tx->Commit();

  ASSERT_TRUE(loaded.has_value());
  EXPECT_EQ(loaded->expires_at_ms, 123456789ULL);
}
