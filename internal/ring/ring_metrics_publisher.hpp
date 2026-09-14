#pragma once

// Pushes per-ring slot accounting into the observability gauges on a
// timer.
//
// Rings are passive: nothing in the ring path calls into metrics, and
// adding a push to Acquire / Commit / Lease / Release would put a metrics
// call on a path that runs at capture rate (up to 1 kHz per ring) to
// report numbers that are levels rather than events. Sampling on a timer
// costs one mutex and an N-slot scan per ring per tick instead.
//
// This is deliberately not folded into TieringManager's loop, which is
// the other periodic sampler in the process: rings have nothing to do
// with tiering or eviction, and TieringManager would have to grow a
// dependency on the ring tier to carry it.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "internal/ring/ring_tier_manager.hpp"

namespace payload::ring {

class RingMetricsPublisher {
 public:
  // `rings` is not owned and must outlive this publisher — factory::Build
  // keeps it in Application, which owns this too. A null manager, or one
  // with no rings configured, makes every tick a no-op.
  explicit RingMetricsPublisher(RingTierManager* rings, std::chrono::milliseconds interval = std::chrono::seconds(1));
  ~RingMetricsPublisher();

  RingMetricsPublisher(const RingMetricsPublisher&)            = delete;
  RingMetricsPublisher& operator=(const RingMetricsPublisher&) = delete;

  void Start();
  // Idempotent, and called by the destructor.
  void Stop();

  // One sampling pass. Public so tests can drive it without a thread;
  // Start()'s loop is then just this on a timer.
  void PublishOnce();

 private:
  void Loop();

  RingTierManager*          rings_;
  std::chrono::milliseconds interval_;

  std::atomic<bool>       running_{false};
  std::mutex              mu_;
  std::condition_variable cv_;
  std::thread             thread_;
};

} // namespace payload::ring
