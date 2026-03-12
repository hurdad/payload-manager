#include "expiration_worker.hpp"

#include "internal/core/payload_manager.hpp"
#include "internal/observability/logging.hpp"

namespace payload::expiration {

ExpirationWorker::ExpirationWorker(std::shared_ptr<payload::core::PayloadManager> manager, std::chrono::milliseconds interval)
    : manager_(std::move(manager)), interval_(interval) {
}

ExpirationWorker::~ExpirationWorker() {
  try {
    Stop();
  } catch (...) {
  }
}

void ExpirationWorker::Start() {
  running_ = true;
  thread_  = std::thread(&ExpirationWorker::Run, this);
}

void ExpirationWorker::Stop() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    running_ = false;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

void ExpirationWorker::Run() {
  while (true) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_for(lock, interval_, [&] { return !running_.load(); });
    if (!running_) break;
    lock.unlock();

    try {
      manager_->ExpireStale();
    } catch (const std::exception& e) {
      PAYLOAD_LOG_ERROR("expiration sweep failed", {payload::observability::StringField("error", e.what())});
    }
  }
}

} // namespace payload::expiration
