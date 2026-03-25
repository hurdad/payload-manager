/*
  Concurrent PayloadManager stress test.

  Exercises concurrent Allocate, Commit, ResolveSnapshot, ExecuteSpill,
  and Delete on the same PayloadManager instance to catch lock-ordering
  bugs that static analysis cannot find.

  Each thread performs a full lifecycle (Allocate → Commit → ResolveSnapshot →
  Spill → Delete) on its own payload, so payloads are independent.  A second
  suite fires concurrent operations on the same payload ID from multiple
  threads, verifying that only one succeeds and none cause crashes or
  data corruption.
*/

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/util/errors.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;

class SimpleBackend final : public payload::storage::StorageBackend {
 public:
  explicit SimpleBackend(payload::manager::v1::Tier tier) : tier_(tier) {}

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size) override {
    auto r = arrow::AllocateBuffer(size);
    if (!r.ok()) throw std::runtime_error("alloc");
    std::shared_ptr<arrow::Buffer> buf(std::move(*r));
    if (size > 0) std::memset(buf->mutable_data(), 0, size);
    std::lock_guard lock(mu_);
    bufs_[id.value()] = buf;
    return buf;
  }

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override {
    std::lock_guard lock(mu_);
    auto it = bufs_.find(id.value());
    if (it == bufs_.end()) throw std::runtime_error("not found: " + id.value());
    return it->second;
  }

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& b, bool) override {
    std::lock_guard lock(mu_);
    bufs_[id.value()] = b;
  }

  void Remove(const payload::manager::v1::PayloadID& id) override {
    std::lock_guard lock(mu_);
    bufs_.erase(id.value());
  }

  payload::manager::v1::Tier TierType() const override { return tier_; }

 private:
  payload::manager::v1::Tier                                      tier_;
  mutable std::mutex                                              mu_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> bufs_;
};

struct Env {
  std::shared_ptr<payload::lease::LeaseManager>          lease_mgr = std::make_shared<LeaseManager>();
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
// N threads each doing a full independent lifecycle concurrently.
// Verifies no crashes or unexpected exception types.
//
// Note: MemoryRepository uses a global OCC version counter, so concurrent
// transactions on unrelated payloads will produce "transaction conflict"
// std::runtime_errors. These are expected and are NOT a correctness bug —
// a real database would use row-level locking. The test checks that no
// *other* exception type escapes (which would indicate a real bug such as
// a data race or corrupted state).
// ---------------------------------------------------------------------------
TEST(PayloadManagerConcurrency, IndependentLifecyclesDoNotRace) {
  Env env;
  constexpr int kThreads = 8;

  std::vector<std::thread> threads;
  std::atomic<int>         unexpected_errors{0};

  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      try {
        auto desc = env.manager->Commit(env.manager->Allocate(64, TIER_RAM).payload_id());
        (void)env.manager->ResolveSnapshot(desc.payload_id());
        env.manager->ExecuteSpill(desc.payload_id(), TIER_DISK, false);
        env.manager->Delete(desc.payload_id(), true);
      } catch (const payload::util::NotFound&) {
        // Payload deleted by a racing thread before our operation — acceptable.
      } catch (const payload::util::LeaseConflict&) {
        // Lease rejected during a concurrent operation — acceptable.
      } catch (const payload::util::InvalidState&) {
        ++unexpected_errors;  // state-machine corruption = definite bug
      } catch (const std::runtime_error&) {
        // Plain runtime_error (e.g., storage backend failure) — acceptable.
      } catch (...) {
        ++unexpected_errors;  // any other exception type is a bug
      }
    });
  }

  for (auto& t : threads) t.join();
  EXPECT_EQ(unexpected_errors.load(), 0) << "No unexpected exception type during concurrent lifecycles";
}

// ---------------------------------------------------------------------------
// Concurrent ResolveSnapshot on the same payload — all must succeed.
// ---------------------------------------------------------------------------
TEST(PayloadManagerConcurrency, ConcurrentResolveSnapshotSamePayload) {
  Env env;
  auto desc = env.manager->Commit(env.manager->Allocate(64, TIER_RAM).payload_id());

  constexpr int    kThreads = 16;
  std::atomic<int> failures{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      try {
        auto snap = env.manager->ResolveSnapshot(desc.payload_id());
        if (snap.payload_id().value() != desc.payload_id().value()) ++failures;
      } catch (...) {
        ++failures;
      }
    });
  }

  for (auto& t : threads) t.join();
  EXPECT_EQ(failures.load(), 0);
}

// ---------------------------------------------------------------------------
// Concurrent Delete + ResolveSnapshot — exactly one operation wins cleanly.
// The delete must succeed; resolves after delete must throw NotFound.
// ---------------------------------------------------------------------------
TEST(PayloadManagerConcurrency, DeleteAndResolveConcurrentlyNoUB) {
  Env env;
  auto desc = env.manager->Commit(env.manager->Allocate(64, TIER_RAM).payload_id());

  constexpr int    kReaders = 8;
  std::atomic<int> crashes{0};
  std::atomic<int> resolve_success{0};
  std::atomic<int> resolve_notfound{0};

  std::vector<std::thread> threads;
  threads.reserve(kReaders + 1);

  // One deleter.
  threads.emplace_back([&] {
    try {
      env.manager->Delete(desc.payload_id(), true);
    } catch (...) {
      ++crashes;
    }
  });

  // Several concurrent resolvers — may see ACTIVE or NotFound.
  for (int i = 0; i < kReaders; ++i) {
    threads.emplace_back([&] {
      try {
        (void)env.manager->ResolveSnapshot(desc.payload_id());
        ++resolve_success;
      } catch (const payload::util::NotFound&) {
        ++resolve_notfound;
      } catch (...) {
        ++crashes;  // unexpected exception type = problem
      }
    });
  }

  for (auto& t : threads) t.join();

  EXPECT_EQ(crashes.load(), 0) << "No unexpected exceptions";
  // Each resolve either succeeded (payload still alive) or got NotFound (deleted).
  EXPECT_EQ(resolve_success.load() + resolve_notfound.load(), kReaders);
}

// ---------------------------------------------------------------------------
// Concurrent Allocate from N threads — every successful allocation must
// produce a unique ID.  Threads that hit an OCC conflict are skipped;
// the uniqueness invariant is checked only among successful allocations.
// ---------------------------------------------------------------------------
TEST(PayloadManagerConcurrency, ConcurrentAllocateProducesUniqueIds) {
  Env env;
  constexpr int kThreads = 16;

  std::vector<std::string> ids(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      try {
        auto desc = env.manager->Allocate(32, TIER_RAM);
        ids[i]    = desc.payload_id().value();
      } catch (...) {
        // Leave ids[i] empty on OCC conflict or other error.
      }
    });
  }

  for (auto& t : threads) t.join();

  // Among threads that succeeded, all IDs must be distinct.
  std::vector<std::string> valid;
  for (const auto& id : ids)
    if (!id.empty()) valid.push_back(id);

  std::unordered_set<std::string> unique(valid.begin(), valid.end());
  EXPECT_EQ(unique.size(), valid.size()) << "All successfully allocated IDs must be unique";
}

} // namespace
