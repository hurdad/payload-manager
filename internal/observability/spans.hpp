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

class Metrics {
 public:
  static Metrics& Instance();

  void RecordRequest(std::string_view route, bool success);
  void ObserveRequestLatencyMs(std::string_view route, double latency_ms);
  void ObserveSpillDurationMs(std::string_view op, double duration_ms);
  void SetTierOccupancyBytes(std::string_view tier, std::uint64_t bytes);

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

inline void Metrics::SetTierOccupancyBytes(std::string_view, std::uint64_t) {
}
#endif

} // namespace payload::observability
