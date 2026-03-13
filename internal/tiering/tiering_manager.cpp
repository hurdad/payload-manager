#include "tiering_manager.hpp"

#include <chrono>

#include "internal/core/payload_manager.hpp"
#include "internal/observability/logging.hpp"
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
    // Sync live byte counts into the pressure state so eviction thresholds
    // are evaluated against current occupancy.
    {
      const auto tier_bytes = manager_->GetTierBytes();
      auto       it_ram     = tier_bytes.find(static_cast<int>(payload::manager::v1::TIER_RAM));
      auto       it_gpu     = tier_bytes.find(static_cast<int>(payload::manager::v1::TIER_GPU));
      state_->ram_bytes.store(it_ram != tier_bytes.end() ? it_ram->second : 0);
      state_->gpu_bytes.store(it_gpu != tier_bytes.end() ? it_gpu->second : 0);
    }

    if (auto victim = policy_->ChooseRamEviction(*state_)) {
      spill::SpillTask task;
      task.id          = *victim;
      task.target_tier = manager_->GetSpillTarget(*victim);
      scheduler_->Enqueue(task);
    }

    if (auto victim = policy_->ChooseGpuEviction(*state_)) {
      spill::SpillTask task;
      task.id          = *victim;
      task.target_tier = payload::manager::v1::TIER_RAM;
      scheduler_->Enqueue(task);
    }

    try {
      manager_->ExpireStale();
    } catch (const std::exception& e) {
      PAYLOAD_LOG_ERROR("ExpireStale failed", {payload::observability::StringField("error", e.what())});
    }

    std::this_thread::sleep_for(100ms);
  }
}

} // namespace payload::tiering
