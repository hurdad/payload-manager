#include "spill_worker.hpp"

#include <chrono>

#include "internal/core/payload_manager.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"

namespace payload::spill {

SpillWorker::SpillWorker(std::shared_ptr<SpillScheduler> scheduler, std::shared_ptr<payload::core::PayloadManager> manager)
    : scheduler_(std::move(scheduler)), manager_(std::move(manager)) {
}

SpillWorker::~SpillWorker() {
  try {
    Stop();
  } catch (...) {
  }
}

void SpillWorker::Start() {
  running_ = true;
  thread_  = std::thread(&SpillWorker::Run, this);
}

void SpillWorker::Stop() {
  running_ = false;
  scheduler_->Wakeup();
  if (thread_.joinable()) thread_.join();
}

void SpillWorker::Run() {
  while (running_) {
    auto task = scheduler_->Dequeue(running_);
    if (!task) break;

    try {
      const auto spill_start = std::chrono::steady_clock::now();
      manager_->ExecuteSpill(task->id, task->target_tier, task->fsync);
      const auto spill_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - spill_start).count();
      payload::observability::Metrics::Instance().ObserveSpillDurationMs("background", spill_ms);
    } catch (const std::exception& e) {
      PAYLOAD_LOG_ERROR("spill failed", {payload::observability::StringField("payload_id", task->id.value()),
                                         payload::observability::StringField("error", e.what())});
    }
  }
}

} // namespace payload::spill
