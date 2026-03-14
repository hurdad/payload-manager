#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

#include "spill_task.hpp"

namespace payload::spill {

/*
  Thread-safe blocking queue for spill workers.
*/
class SpillScheduler {
 public:
  void Enqueue(const SpillTask& task);

  // Blocks until a task is available, shutdown is requested, or the running
  // flag goes false.  Returns nullopt when the caller should stop.
  std::optional<SpillTask> Dequeue(const std::atomic<bool>& running);

  std::size_t QueueDepth() const;

  // Wake all blocked Dequeue callers without triggering shutdown.
  void Wakeup();

  void Shutdown();

 private:
  mutable std::mutex      mutex_;
  std::condition_variable cv_;
  std::queue<SpillTask>   queue_;
  bool                    shutdown_ = false;
};

} // namespace payload::spill
