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

  // blocking wait
  std::optional<SpillTask> Dequeue();

  void Shutdown();

 private:
  std::mutex              mutex_;
  std::condition_variable cv_;
  std::queue<SpillTask>   queue_;
  bool                    shutdown_ = false;
};

} // namespace payload::spill
