#include "data_service.hpp"

#include <chrono>
#include <stdexcept>
#include <type_traits>

#include "internal/core/payload_manager.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/util/errors.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

namespace {

template <typename Fn>
auto ObserveRpc(std::string_view route, const PayloadID* payload_id, Fn&& fn) {
  payload::observability::SpanScope span(route);
  if (payload_id) {
    span.SetAttribute("payload.id", payload_id->value());
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
                                     payload_id ? payload::observability::StringField("payload_id", payload_id->value())
                                                : payload::observability::StringField("payload_id", "")});
    payload::observability::Metrics::Instance().RecordRequest(route, false);
    payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
        route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
    throw;
  }
}

} // namespace

DataService::DataService(ServiceContext ctx) : ctx_(std::move(ctx)) {
}

ResolveSnapshotResponse DataService::ResolveSnapshot(const ResolveSnapshotRequest& req) {
  return ObserveRpc("DataService.ResolveSnapshot", &req.id(), [&] {
    ResolveSnapshotResponse resp;
    *resp.mutable_payload_descriptor() = ctx_.manager->ResolveSnapshot(req.id());
    return resp;
  });
}

AcquireReadLeaseResponse DataService::AcquireReadLease(const AcquireReadLeaseRequest& req) {
  return ObserveRpc("DataService.AcquireReadLease", &req.id(), [&] {
    if (req.mode() != LEASE_MODE_UNSPECIFIED && req.mode() != LEASE_MODE_READ) {
      throw payload::util::InvalidState("acquire lease: unsupported lease mode; use LEASE_MODE_READ");
    }

    if (req.promotion_policy() == PROMOTION_POLICY_BEST_EFFORT) {
      const auto snapshot = ctx_.manager->ResolveSnapshot(req.id());
      if (snapshot.tier() < req.min_tier()) {
        throw payload::util::InvalidState("acquire lease: best-effort promotion cannot satisfy min_tier; lower min_tier or change promotion policy");
      }
    }

    return ctx_.manager->AcquireReadLease(req.id(), req.min_tier(), req.min_lease_duration_ms());
  });
}

void DataService::ReleaseLease(const ReleaseLeaseRequest& req) {
  ObserveRpc("DataService.ReleaseLease", nullptr, [&] { ctx_.manager->ReleaseLease(req.lease_id()); });
}

} // namespace payload::service
