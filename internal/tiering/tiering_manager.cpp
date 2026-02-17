#include "tiering_manager.hpp"

#include <chrono>

#include "payload/manager/v1.hpp"

namespace payload::tiering {

using namespace std::chrono_literals;

TieringManager::TieringManager(std::shared_ptr<TieringPolicy> policy, std::shared_ptr<spill::SpillScheduler> scheduler,
                               std::shared_ptr<payload::core::PayloadManager> manager, std::shared_ptr<PressureState> state)
    : policy_(std::move(policy)), scheduler_(std::move(scheduler)), manager_(std::move(manager)), state_(std::move(state)) {
}

TieringManager::~TieringManager() {
  try {
    Stop();
  } catch (...) {
  }
}

void TieringManager::Start() {
  running_ = true;
  thread_  = std::thread(&TieringManager::Loop, this);
}

void TieringManager::Stop() {
  running_ = false;
  if (thread_.joinable()) thread_.join();
}

void TieringManager::Loop() {
  while (running_) {
    if (auto victim = policy_->ChooseRamEviction(*state_)) {
      spill::SpillTask task;
      task.id          = *victim;
      task.target_tier = payload::manager::v1::TIER_DISK;
      scheduler_->Enqueue(task);
    }

    if (auto victim = policy_->ChooseGpuEviction(*state_)) {
      spill::SpillTask task;
      task.id          = *victim;
      task.target_tier = payload::manager::v1::TIER_RAM;
      scheduler_->Enqueue(task);
    }

    std::this_thread::sleep_for(100ms);
  }
}

} // namespace payload::tiering
