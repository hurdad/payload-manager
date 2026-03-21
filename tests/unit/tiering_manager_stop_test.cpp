/*
  Tests for TieringManager lifecycle fixes.
*/

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/spill/spill_scheduler.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/tiering/pressure_state.hpp"
#include "internal/tiering/tiering_manager.hpp"
#include "internal/tiering/tiering_policy.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

class SimpleBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size) override {
    auto r = arrow::AllocateBuffer(size);
    if (!r.ok()) throw std::runtime_error("alloc");
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

struct Env {
  std::shared_ptr<payload::lease::LeaseManager>          lease_mgr = std::make_shared<payload::lease::LeaseManager>();
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<SimpleBackend>                         ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                         disk      = std::make_shared<SimpleBackend>(TIER_DISK);
  std::shared_ptr<payload::core::PayloadManager>         manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]  = ram;
    s[TIER_DISK] = disk;
    return std::make_shared<payload::core::PayloadManager>(s, lease_mgr, repo);
  }()};
  std::shared_ptr<payload::metadata::MetadataCache>      cache     = std::make_shared<payload::metadata::MetadataCache>();
  std::shared_ptr<payload::spill::SpillScheduler>        scheduler = std::make_shared<payload::spill::SpillScheduler>();
  std::shared_ptr<payload::tiering::PressureState>       pressure  = std::make_shared<payload::tiering::PressureState>();
  std::shared_ptr<payload::tiering::TieringPolicy>       policy    = std::make_shared<payload::tiering::TieringPolicy>(cache);
  std::shared_ptr<payload::tiering::TieringManager>      tiering =
      std::make_shared<payload::tiering::TieringManager>(policy, scheduler, manager, pressure);
};

} // namespace

// ---------------------------------------------------------------------------
// Test: Stop() wakes the sleeping loop and returns in well under 100ms.
//       We allow 30ms — generous enough to avoid flakiness on loaded CI
//       machines but far below the 100ms loop sleep interval.
// ---------------------------------------------------------------------------
TEST(TieringManagerStop, StopReturnsQuickly) {
  Env env;
  env.tiering->Start();

  const auto t0 = std::chrono::steady_clock::now();
  env.tiering->Stop();
  const auto elapsed_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();

  EXPECT_LT(elapsed_ms, 30.0) << "Stop() must return in under 30ms (loop sleep is 100ms; CV must wake it)";
}

// ---------------------------------------------------------------------------
// Test: Calling Start() twice does not spawn a second thread.
//       The second Start() is a no-op; Stop() joins cleanly exactly once.
// ---------------------------------------------------------------------------
TEST(TieringManagerStop, DoubleStartIsIdempotent) {
  Env env;
  env.tiering->Start();
  env.tiering->Start(); // must not replace thread_ or spawn a second thread

  // If this hangs, Start() incorrectly replaced the first thread and the join
  // is waiting for a thread that will never finish.
  env.tiering->Stop();
  // Reaching here means the join returned exactly once without deadlock.
}
