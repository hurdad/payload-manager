#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "spill_scheduler.hpp"

namespace payload::core {
class PayloadManager;
}

namespace payload::spill {

/*
  Background worker that performs durability operations.

  Executes:
      spill RAM/GPU → DISK/OBJECT
*/
class SpillWorker {
 public:
  SpillWorker(std::shared_ptr<SpillScheduler> scheduler, std::shared_ptr<payload::core::PayloadManager> manager);
  ~SpillWorker();

  void Start();
  void Stop();

 private:
  void Run();

  std::shared_ptr<SpillScheduler>                scheduler_;
  std::shared_ptr<payload::core::PayloadManager> manager_;

  std::thread       thread_;
  std::atomic<bool> running_{false};
};

} // namespace payload::spill
