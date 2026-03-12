#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace payload::core {
class PayloadManager;
}

namespace payload::expiration {

/*
  Background worker that periodically calls PayloadManager::ExpireStale()
  to remove TTL-expired payloads from storage and the repository.

  Default interval: 30 seconds.
*/
class ExpirationWorker {
 public:
  explicit ExpirationWorker(std::shared_ptr<payload::core::PayloadManager> manager,
                            std::chrono::milliseconds                      interval = std::chrono::seconds{30});
  ~ExpirationWorker();

  void Start();
  void Stop();

 private:
  void Run();

  std::shared_ptr<payload::core::PayloadManager> manager_;
  std::chrono::milliseconds                      interval_;

  std::thread             thread_;
  std::atomic<bool>       running_{false};
  std::mutex              mu_;
  std::condition_variable cv_;
};

} // namespace payload::expiration
