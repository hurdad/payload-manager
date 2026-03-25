#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <stdexcept>
#include <thread>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;
using payload::manager::v1::TIER_RAM;
using payload::storage::StorageBackend;

class MinimalStorageBackend final : public StorageBackend {
 public:
  explicit MinimalStorageBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID&, uint64_t size_bytes) override {
    auto maybe = arrow::AllocateBuffer(size_bytes);
    if (!maybe.ok()) throw std::runtime_error("alloc failed");
    std::shared_ptr<arrow::Buffer> buf(std::move(*maybe));
    if (size_bytes > 0) std::memset(buf->mutable_data(), 0, static_cast<size_t>(size_bytes));
    return buf;
  }

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID&) override {
    return arrow::AllocateBuffer(0).ValueOrDie();
  }

  void Write(const payload::manager::v1::PayloadID&, const std::shared_ptr<arrow::Buffer>&, bool) override {
  }

  void Remove(const payload::manager::v1::PayloadID&) override {
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier tier_;
};

PayloadManager MakeManager(const std::shared_ptr<LeaseManager>& lease_mgr) {
  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM] = std::make_shared<MinimalStorageBackend>(TIER_RAM);
  return PayloadManager(std::move(storage), lease_mgr, std::make_shared<payload::db::memory::MemoryRepository>());
}

} // namespace

// Concurrently race AcquireReadLease and Delete(force=true) on the same
// payload. Invariant: if a lease is successfully acquired the payload must
// still be resolvable at that instant (the lease guard must hold).
TEST(DeleteLeaseRace, AcquireAndDeleteNeverProducesStaleDescriptor) {
  constexpr int kIterations = 300;

  for (int i = 0; i < kIterations; ++i) {
    auto lease_mgr = std::make_shared<LeaseManager>(20'000, 120'000);
    auto manager   = MakeManager(lease_mgr);

    const auto descriptor = manager.Commit(manager.Allocate(64, TIER_RAM).payload_id());
    ASSERT_EQ(descriptor.state(), PAYLOAD_STATE_ACTIVE);

    const auto& id = descriptor.payload_id();

    std::atomic<bool> lease_acquired{false};
    std::atomic<bool> payload_resolvable_after_lease{false};

    std::thread t_lease([&] {
      try {
        auto resp = manager.AcquireReadLease(id, TIER_RAM, 5'000);
        lease_acquired.store(true, std::memory_order_relaxed);
        // If the lease was granted, the payload must still be resolvable.
        try {
          (void)manager.ResolveSnapshot(id);
          payload_resolvable_after_lease.store(true, std::memory_order_relaxed);
        } catch (...) {
        }
        manager.ReleaseLease(resp.lease_id());
      } catch (...) {
        // NotFound or LeaseConflict: acceptable outcomes.
      }
    });

    std::thread t_delete([&] {
      try {
        manager.Delete(id, /*force=*/true);
      } catch (...) {
      }
    });

    t_lease.join();
    t_delete.join();

    if (lease_acquired.load(std::memory_order_relaxed)) {
      EXPECT_TRUE(payload_resolvable_after_lease.load(std::memory_order_relaxed))
          << "Lease was granted but payload was already deleted in iteration " << i;
    }
  }
}
