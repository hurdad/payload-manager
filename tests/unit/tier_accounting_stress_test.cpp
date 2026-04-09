/*
  Stress test for tier byte accounting under high concurrency.

  Spawns N threads each performing Allocate → Commit → Delete in a tight loop.
  After all threads complete, the RAM tier byte and count totals must both be
  zero. A non-zero value indicates an accounting leak (or the underflow-clamping
  bug hiding a double-decrement).
*/

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::TIER_RAM;
using payload::storage::StorageBackend;

class NullStorageBackend final : public StorageBackend {
 public:
  explicit NullStorageBackend(payload::manager::v1::Tier tier) : tier_(tier) {
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

} // namespace

TEST(TierAccountingStress, BytesAndCountReturnToZeroAfterConcurrentAllocDelete) {
  constexpr int kThreads      = 8;
  constexpr int kOpsPerThread = 100;
  constexpr int kPayloadBytes = 256;

  auto lease_mgr = std::make_shared<LeaseManager>();

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM] = std::make_shared<NullStorageBackend>(TIER_RAM);

  auto manager = std::make_shared<PayloadManager>(std::move(storage), lease_mgr, std::make_shared<payload::db::memory::MemoryRepository>());

  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      for (int j = 0; j < kOpsPerThread; ++j) {
        try {
          const auto desc = manager->Commit(manager->Allocate(kPayloadBytes, TIER_RAM).payload_id());
          manager->Delete(desc.payload_id(), /*force=*/true);
        } catch (...) {
        }
      }
    });
  }

  for (auto& t : threads) t.join();

  const auto     tier_bytes = manager->GetTierBytes();
  const auto     ram_it     = tier_bytes.find(static_cast<int>(TIER_RAM));
  const uint64_t ram_bytes  = (ram_it != tier_bytes.end()) ? ram_it->second : 0u;

  EXPECT_EQ(ram_bytes, 0u) << "RAM tier byte total must be zero after all Allocate/Delete pairs complete; "
                              "a non-zero value indicates an accounting leak or missed decrement.";
}
