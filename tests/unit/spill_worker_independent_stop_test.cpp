/*
  Tests for Critical Fix #2: SpillWorker::Stop must not shut down the shared
  SpillScheduler so that sibling workers continue to function.

  Before the fix, SpillWorker::Stop() called scheduler_->Shutdown(), which
  set shutdown_=true and woke all blocked workers.  Every sibling would then
  drain its Dequeue() and exit, even though they were still nominally running.

  After the fix each worker uses its own running_ flag.  Calling Stop() on one
  worker calls Wakeup() (notify_all without setting shutdown_) then joins the
  calling worker's thread.  Siblings re-check their own running_ flag, find it
  still true, and go back to sleep.

  Covered:
    - Worker stops cleanly when its queue is empty
    - Stopping worker A does not prevent worker B from processing subsequent tasks
*/

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/spill/spill_scheduler.hpp"
#include "internal/spill/spill_task.hpp"
#include "internal/spill/spill_worker.hpp"
#include "internal/storage/storage_backend.hpp"
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
  bool Has(const payload::manager::v1::PayloadID& id) const {
    return bufs_.count(id.value()) > 0;
  }
  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> bufs_;
};

struct Env {
  std::shared_ptr<SimpleBackend>                         ram       = std::make_shared<SimpleBackend>(TIER_RAM);
  std::shared_ptr<SimpleBackend>                         disk      = std::make_shared<SimpleBackend>(TIER_DISK);
  std::shared_ptr<payload::lease::LeaseManager>          lease_mgr = std::make_shared<payload::lease::LeaseManager>();
  std::shared_ptr<payload::db::memory::MemoryRepository> repo      = std::make_shared<payload::db::memory::MemoryRepository>();
  std::shared_ptr<payload::core::PayloadManager>         manager{[&] {
    payload::storage::StorageFactory::TierMap s;
    s[TIER_RAM]  = ram;
    s[TIER_DISK] = disk;
    return std::make_shared<payload::core::PayloadManager>(s, lease_mgr, repo);
  }()};
  std::shared_ptr<payload::spill::SpillScheduler>        scheduler = std::make_shared<payload::spill::SpillScheduler>();
};

// ---------------------------------------------------------------------------
// Test: A worker with nothing in the queue stops immediately without hanging.
// ---------------------------------------------------------------------------
void TestWorkerStopsCleanlyOnEmptyQueue() {
  Env env;

  auto worker = std::make_shared<payload::spill::SpillWorker>(env.scheduler, env.manager);
  worker->Start();

  // Stop should return promptly (worker wakes on Wakeup(), sees running_=false,
  // queue empty → returns nullopt from Dequeue → exits loop).
  bool        completed = false;
  std::thread stopper([&] {
    worker->Stop();
    completed = true;
  });

  stopper.join();
  assert(completed && "Stop() must return without hanging on an empty queue");
}

// ---------------------------------------------------------------------------
// Test: Stopping worker A does not kill worker B.
//       After A is stopped, B must still process a newly enqueued spill task.
// ---------------------------------------------------------------------------
void TestStoppingOneWorkerDoesNotKillSibling() {
  Env env;

  // Allocate and commit a payload to give worker B a real task to execute.
  auto desc = env.manager->Commit(env.manager->Allocate(64, TIER_RAM).payload_id());

  auto worker_a = std::make_shared<payload::spill::SpillWorker>(env.scheduler, env.manager);
  auto worker_b = std::make_shared<payload::spill::SpillWorker>(env.scheduler, env.manager);
  worker_a->Start();
  worker_b->Start();

  // Stop worker A. Under the old (broken) code this would call Shutdown() on
  // the shared scheduler, causing worker B to wake and exit.
  worker_a->Stop();

  // Now enqueue a spill task. Worker B must be alive and able to pick this up.
  payload::spill::SpillTask task;
  task.id          = desc.payload_id();
  task.target_tier = TIER_DISK;
  task.fsync       = false;
  env.scheduler->Enqueue(task);

  // Wait up to 500 ms for the task to be processed.
  constexpr auto kPollInterval = std::chrono::milliseconds(10);
  constexpr int  kMaxIter      = 50;
  bool           processed     = false;
  for (int i = 0; i < kMaxIter; ++i) {
    if (env.disk->Has(desc.payload_id())) {
      processed = true;
      break;
    }
    std::this_thread::sleep_for(kPollInterval);
  }

  worker_b->Stop();

  assert(processed && "worker B must process tasks after worker A has been stopped");
  assert(env.disk->Has(desc.payload_id()) && "payload must have moved to disk");
  assert(!env.ram->Has(desc.payload_id()) && "payload must no longer be in RAM after spill");
}

// ---------------------------------------------------------------------------
// Test: Wakeup() does not cause a running worker to exit.
// ---------------------------------------------------------------------------
void TestWakeupDoesNotExitRunningWorker() {
  Env env;

  auto worker = std::make_shared<payload::spill::SpillWorker>(env.scheduler, env.manager);
  worker->Start();

  // Calling Wakeup() should wake the blocked Dequeue, but since running_=true
  // and queue is empty, the worker goes back to sleep rather than exiting.
  env.scheduler->Wakeup();
  env.scheduler->Wakeup();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  // Enqueue a task to verify the worker is still alive and processing.
  auto desc = env.manager->Commit(env.manager->Allocate(32, TIER_RAM).payload_id());

  payload::spill::SpillTask task;
  task.id          = desc.payload_id();
  task.target_tier = TIER_DISK;
  task.fsync       = false;
  env.scheduler->Enqueue(task);

  constexpr auto kPollInterval = std::chrono::milliseconds(10);
  constexpr int  kMaxIter      = 50;
  bool           processed     = false;
  for (int i = 0; i < kMaxIter; ++i) {
    if (env.disk->Has(desc.payload_id())) {
      processed = true;
      break;
    }
    std::this_thread::sleep_for(kPollInterval);
  }

  worker->Stop();

  assert(processed && "worker must still process tasks after Wakeup() calls");
}

} // namespace

int main() {
  TestWorkerStopsCleanlyOnEmptyQueue();
  TestStoppingOneWorkerDoesNotKillSibling();
  TestWakeupDoesNotExitRunningWorker();

  std::cout << "spill_worker_independent_stop_test: pass\n";
  return 0;
}
