#include "spill_worker.hpp"
#include <iostream>

namespace payload::spill {

SpillWorker::SpillWorker(std::shared_ptr<SpillScheduler> scheduler,
                         std::shared_ptr<payload::core::PayloadManager> manager)
    : scheduler_(std::move(scheduler)),
      manager_(std::move(manager)) {}

void SpillWorker::Start() {
  running_ = true;
  thread_ = std::thread(&SpillWorker::Run, this);
}

void SpillWorker::Stop() {
  scheduler_->Shutdown();
  running_ = false;
  if (thread_.joinable())
    thread_.join();
}

void SpillWorker::Run() {
  while (running_) {

    auto task = scheduler_->Dequeue();
    if (!task)
      break;

    try {
      manager_->ExecuteSpill(task->id, task->target_tier, task->fsync);
    }
    catch (const std::exception& e) {
      std::cerr << "[spill-worker] spill failed: " << e.what() << "\n";
    }
  }
}

}
