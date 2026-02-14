#include "spill_scheduler.hpp"

namespace payload::spill {

void SpillScheduler::Enqueue(const SpillTask& task) {
  {
    std::lock_guard lock(mutex_);
    queue_.push(task);
  }
  cv_.notify_one();
}

std::optional<SpillTask> SpillScheduler::Dequeue() {
  std::unique_lock lock(mutex_);

  cv_.wait(lock, [&] { return shutdown_ || !queue_.empty(); });

  if (shutdown_ && queue_.empty()) return std::nullopt;

  SpillTask task = queue_.front();
  queue_.pop();
  return task;
}

void SpillScheduler::Shutdown() {
  {
    std::lock_guard lock(mutex_);
    shutdown_ = true;
  }
  cv_.notify_all();
}

} // namespace payload::spill
