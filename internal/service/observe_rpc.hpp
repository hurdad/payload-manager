#pragma once

#include <chrono>
#include <string_view>
#include <type_traits>

#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

// Shared RPC observability wrapper used by all service implementations.
// Records a trace span, request latency, and success/failure metrics.

template <typename Fn>
auto ObserveRpc(std::string_view route, const payload::manager::v1::PayloadID* payload_id, Fn&& fn) {
  payload::observability::SpanScope span(route);
  if (payload_id) {
    span.SetAttribute("payload.id", payload::util::PayloadIdToHex(*payload_id));
  }

  const auto started_at = std::chrono::steady_clock::now();
  try {
    if constexpr (std::is_void_v<std::invoke_result_t<Fn>>) {
      fn();
      payload::observability::Metrics::Instance().RecordRequest(route, true);
      payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
          route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
      return;
    } else {
      auto result = fn();
      payload::observability::Metrics::Instance().RecordRequest(route, true);
      payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
          route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
      return result;
    }
  } catch (const std::exception& ex) {
    span.RecordException(ex.what());
    PAYLOAD_LOG_ERROR("RPC failed", {payload::observability::StringField("route", route), payload::observability::StringField("error", ex.what()),
                                     payload_id ? payload::observability::StringField("payload_id", payload::util::PayloadIdToHex(*payload_id))
                                                : payload::observability::StringField("payload_id", "")});
    payload::observability::Metrics::Instance().RecordRequest(route, false);
    payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
        route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
    throw;
  }
}

template <typename Fn>
auto ObserveRpc(std::string_view route, const payload::manager::v1::StreamID* stream_id, const payload::manager::v1::PayloadID* payload_id, Fn&& fn) {
  payload::observability::SpanScope span(route);
  if (stream_id) {
    span.SetAttribute("stream.namespace", stream_id->namespace_());
    span.SetAttribute("stream.name", stream_id->name());
  }
  if (payload_id) {
    span.SetAttribute("payload.id", payload::util::PayloadIdToHex(*payload_id));
  }

  const auto started_at = std::chrono::steady_clock::now();
  try {
    if constexpr (std::is_void_v<std::invoke_result_t<Fn>>) {
      fn();
      payload::observability::Metrics::Instance().RecordRequest(route, true);
      payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
          route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
      return;
    } else {
      auto result = fn();
      payload::observability::Metrics::Instance().RecordRequest(route, true);
      payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
          route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
      return result;
    }
  } catch (const std::exception& ex) {
    span.RecordException(ex.what());
    PAYLOAD_LOG_ERROR("RPC failed", {payload::observability::StringField("route", route), payload::observability::StringField("error", ex.what()),
                                     stream_id ? payload::observability::StringField("stream", stream_id->namespace_() + "/" + stream_id->name())
                                               : payload::observability::StringField("stream", ""),
                                     payload_id ? payload::observability::StringField("payload_id", payload::util::PayloadIdToHex(*payload_id))
                                                : payload::observability::StringField("payload_id", "")});
    payload::observability::Metrics::Instance().RecordRequest(route, false);
    payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
        route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
    throw;
  }
}

} // namespace payload::service
