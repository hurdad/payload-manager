#include "spill_worker.hpp"

#include <chrono>
#include <string>
#include <string_view>

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
  } catch (const std::exception& e) {
    // Destructors must not throw, but swallowing silently hid shutdown faults.
    PAYLOAD_LOG_ERROR("spill worker failed to stop cleanly", {payload::observability::StringField("error", e.what())});
  } catch (...) {
    PAYLOAD_LOG_ERROR("spill worker failed to stop cleanly", {payload::observability::StringField("error", "unknown exception")});
  }
}

void SpillWorker::Start() {
  std::lock_guard lock(mu_);
  if (thread_.joinable()) return; // already running
  running_ = true;
  thread_  = std::thread(&SpillWorker::Run, this);
}

void SpillWorker::Stop() {
  {
    std::lock_guard lock(mu_);
    running_ = false;
  }
  scheduler_->Wakeup();
  if (thread_.joinable()) thread_.join();
}

void SpillWorker::Run() {
  while (running_) {
    auto task = scheduler_->Dequeue(running_);
    if (!task) break;

    payload::observability::Metrics::Instance().SetSpillQueueDepth(scheduler_->QueueDepth());

    try {
      const auto spill_start = std::chrono::steady_clock::now();
      manager_->ExecuteSpill(task->id, task->target_tier, task->fsync);
      const auto spill_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - spill_start).count();
      payload::observability::Metrics::Instance().ObserveSpillDurationMs("background", spill_ms);
    } catch (const std::exception& e) {
      // A failed spill used to be invisible: it was logged, but the duration
      // histogram is skipped on this path and no counter existed, so a disk
      // tier that had filled up produced no metric at all. The reason label is
      // deliberately bounded to keep cardinality low; the full text stays in
      // the log line.
      const std::string_view what{e.what()};
      std::string_view       reason = "other";
      if (what.find("storage tier is not available") != std::string_view::npos) {
        reason = "tier_unavailable";
      } else if (what.find("not found") != std::string_view::npos) {
        reason = "not_found";
      } else if (what.find("deleted") != std::string_view::npos || what.find("invalid") != std::string_view::npos) {
        reason = "invalid_state";
      }

      PAYLOAD_LOG_ERROR("spill failed",
                        {payload::observability::StringField("payload_id", task->id.value()),
                         payload::observability::StringField("reason", std::string(reason)), payload::observability::StringField("error", e.what())});
      payload::observability::Metrics::Instance().RecordSpillFailure(reason);
    }
  }
}

} // namespace payload::spill
