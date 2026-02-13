#pragma once

#include <memory>
#include <thread>
#include <atomic>

#include "tiering_policy.hpp"
#include "internal/spill/spill_scheduler.hpp"

namespace payload::core { class PayloadManager; }

namespace payload::tiering {

/*
  Periodically checks pressure and schedules spills/promotions.
*/
class TieringManager {
public:
  TieringManager(std::shared_ptr<TieringPolicy> policy,
                 std::shared_ptr<spill::SpillScheduler> scheduler,
                 std::shared_ptr<payload::core::PayloadManager> manager,
                 PressureState state);

  void Start();
  void Stop();

private:
  void Loop();

  std::shared_ptr<TieringPolicy> policy_;
  std::shared_ptr<spill::SpillScheduler> scheduler_;
  std::shared_ptr<payload::core::PayloadManager> manager_;
  PressureState state_;

  std::thread thread_;
  std::atomic<bool> running_{false};
};

}
