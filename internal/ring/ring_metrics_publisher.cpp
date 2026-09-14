#include "internal/ring/ring_metrics_publisher.hpp"

#include "internal/observability/spans.hpp"

namespace payload::ring {

RingMetricsPublisher::RingMetricsPublisher(RingTierManager* rings, std::chrono::milliseconds interval) : rings_(rings), interval_(interval) {
}

RingMetricsPublisher::~RingMetricsPublisher() {
  Stop();
}

void RingMetricsPublisher::Start() {
  if (!rings_ || rings_->Count() == 0) return; // nothing to sample
  if (running_.exchange(true)) return;         // already started
  thread_ = std::thread([this] { Loop(); });
}

void RingMetricsPublisher::Stop() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    running_ = false;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

void RingMetricsPublisher::PublishOnce() {
  if (!rings_) return;
  for (const auto& ring_id : rings_->RingIds()) {
    auto* ring = rings_->GetRing(ring_id);
    if (!ring) continue; // raced a teardown; nothing to report
    const auto stats = ring->GetStats();
    payload::observability::Metrics::Instance().SetRingStats(ring_id, payload::observability::RingMetrics{
                                                                          .slots_total     = stats.slots_total,
                                                                          .slots_available = stats.slots_available,
                                                                          .slots_writing   = stats.slots_writing,
                                                                          .slots_leased    = stats.slots_leased,
                                                                          .leases_active   = stats.leases_active,
                                                                          .slots_reclaimed = stats.slots_reclaimed,
                                                                      });
  }
}

void RingMetricsPublisher::Loop() {
  while (running_) {
    PublishOnce();
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait_for(lk, interval_, [&] { return !running_.load(); });
  }
}

} // namespace payload::ring
