#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace payload::runtime::config {
class RuntimeConfig;
}

namespace payload::observability {

enum class OtlpTransport {
  kGrpc,
  kHttpProtobuf,
};

struct OtlpConfig {
  std::string   service_name{"payload-manager"};
  std::string   endpoint{};
  OtlpTransport transport{OtlpTransport::kGrpc};
  bool          insecure{true};
};

bool InitializeTracing(const OtlpConfig& config = {});
bool InitializeMetrics(const OtlpConfig& config = {});
bool InitializeTracing(const payload::runtime::config::RuntimeConfig& config);
bool InitializeMetrics(const payload::runtime::config::RuntimeConfig& config);
void ShutdownTracing();
void ShutdownMetrics();

class SpanScope {
 public:
  explicit SpanScope(std::string_view name);
  ~SpanScope();

  SpanScope(const SpanScope&)            = delete;
  SpanScope& operator=(const SpanScope&) = delete;

  SpanScope(SpanScope&&) noexcept;
  SpanScope& operator=(SpanScope&&) noexcept;

  void SetAttribute(std::string_view key, std::string_view value);
  void SetAttribute(std::string_view key, std::int64_t value);
  void SetAttribute(std::string_view key, double value);
  void AddEvent(std::string_view name);
  void RecordException(std::string_view description);

 private:
#ifdef ENABLE_OTEL
  struct Impl;
  std::unique_ptr<Impl> impl_;
#endif
};

// Per-ring slot accounting, pushed into the ring gauges. Mirrors
// payload::ring::Ring::Stats, restated here so observability does not
// depend on internal/ring.
struct RingMetrics {
  std::uint64_t slots_total     = 0;
  std::uint64_t slots_available = 0;
  std::uint64_t slots_writing   = 0;
  std::uint64_t slots_leased    = 0;
  std::uint64_t leases_active   = 0;
  std::uint64_t slots_reclaimed = 0;
};

class Metrics {
 public:
  static Metrics& Instance();

  void RecordRequest(std::string_view route, bool success);
  void ObserveRequestLatencyMs(std::string_view route, double latency_ms);
  void ObserveSpillDurationMs(std::string_view op, double duration_ms);
  void RecordSpillBytes(std::string_view op, std::uint64_t bytes);
  // reason is a bounded label: tier_unavailable | not_found | invalid_state | other
  void RecordSpillFailure(std::string_view reason);
  void SetTierOccupancyBytes(std::string_view tier, std::uint64_t bytes);
  void SetTierPayloadCount(std::string_view tier, std::uint64_t count);
  // Live size and free space of the tmpfs backing /dev/shm. Distinct from tier
  // occupancy: the configured cap only covers what this process handed out,
  // while anything else on the same tmpfs consumes it invisibly.
  void SetShmBytes(std::uint64_t total, std::uint64_t free_bytes);
  void RecordAllocationFailure(std::string_view tier);
  void SetSpillQueueDepth(std::size_t depth);
  // Latest slot accounting for one ring, labelled by ring_id. Pushed on a
  // timer rather than on state change: ring state moves at capture rate
  // (up to 1 kHz), and every gauge here is a level, not an event.
  void SetRingStats(std::string_view ring_id, const RingMetrics& stats);

 private:
  Metrics();
#ifdef ENABLE_OTEL
  struct Impl;
  std::unique_ptr<Impl> impl_;
#endif
};

#ifndef ENABLE_OTEL
inline bool InitializeTracing(const OtlpConfig&) {
  return false;
}

inline bool InitializeMetrics(const OtlpConfig&) {
  return false;
}

inline bool InitializeTracing(const payload::runtime::config::RuntimeConfig&) {
  return false;
}

inline bool InitializeMetrics(const payload::runtime::config::RuntimeConfig&) {
  return false;
}

inline void ShutdownTracing() {
}

inline void ShutdownMetrics() {
}

inline SpanScope::SpanScope(std::string_view) {
}

inline SpanScope::~SpanScope() {
}

inline SpanScope::SpanScope(SpanScope&&) noexcept = default;

inline SpanScope& SpanScope::operator=(SpanScope&&) noexcept = default;

inline void SpanScope::SetAttribute(std::string_view, std::string_view) {
}

inline void SpanScope::SetAttribute(std::string_view, std::int64_t) {
}

inline void SpanScope::SetAttribute(std::string_view, double) {
}

inline void SpanScope::AddEvent(std::string_view) {
}

inline void SpanScope::RecordException(std::string_view) {
}

inline Metrics::Metrics() {
}

inline Metrics& Metrics::Instance() {
  static Metrics instance;
  return instance;
}

inline void Metrics::RecordRequest(std::string_view, bool) {
}

inline void Metrics::ObserveRequestLatencyMs(std::string_view, double) {
}

inline void Metrics::ObserveSpillDurationMs(std::string_view, double) {
}

inline void Metrics::RecordSpillBytes(std::string_view, std::uint64_t) {
}

inline void Metrics::SetTierOccupancyBytes(std::string_view, std::uint64_t) {
}

inline void Metrics::SetTierPayloadCount(std::string_view, std::uint64_t) {
}

inline void Metrics::SetShmBytes(std::uint64_t, std::uint64_t) {
}

inline void Metrics::RecordSpillFailure(std::string_view) {
}

inline void Metrics::RecordAllocationFailure(std::string_view) {
}

inline void Metrics::SetSpillQueueDepth(std::size_t) {
}

inline void Metrics::SetRingStats(std::string_view, const RingMetrics&) {
}
#endif

} // namespace payload::observability
