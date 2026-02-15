#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "internal/spill/spill_scheduler.hpp"
#include "tiering_policy.hpp"

namespace payload::core {
class PayloadManager;
}

namespace payload::tiering {

/*
  Periodically checks pressure and schedules spills/promotions.
*/
class TieringManager {
 public:
  TieringManager(std::shared_ptr<TieringPolicy> policy, std::shared_ptr<spill::SpillScheduler> scheduler,
                 std::shared_ptr<payload::core::PayloadManager> manager, std::shared_ptr<PressureState> state);

  void Start();
  void Stop();

 private:
  void Loop();

  std::shared_ptr<TieringPolicy>                 policy_;
  std::shared_ptr<spill::SpillScheduler>         scheduler_;
  std::shared_ptr<payload::core::PayloadManager> manager_;
  std::shared_ptr<PressureState>                 state_;

  std::thread       thread_;
  std::atomic<bool> running_{false};
};

} // namespace payload::tiering
