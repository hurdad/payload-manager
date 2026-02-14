#include "data_service.hpp"

#include <stdexcept>

#include "internal/core/payload_manager.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

DataService::DataService(ServiceContext ctx) : ctx_(std::move(ctx)) {
}

ResolveSnapshotResponse DataService::ResolveSnapshot(const ResolveSnapshotRequest& req) {
  ResolveSnapshotResponse resp;
  *resp.mutable_payload_descriptor() = ctx_.manager->ResolveSnapshot(req.id());
  return resp;
}

AcquireReadLeaseResponse DataService::AcquireReadLease(const AcquireReadLeaseRequest& req) {
  if (req.mode() != LEASE_MODE_UNSPECIFIED && req.mode() != LEASE_MODE_READ) {
    throw std::runtime_error("acquire lease: unsupported lease mode");
  }

  if (req.promotion_policy() == PROMOTION_POLICY_BEST_EFFORT) {
    const auto snapshot = ctx_.manager->ResolveSnapshot(req.id());
    if (snapshot.tier() < req.min_tier()) {
      throw std::runtime_error("acquire lease: best-effort promotion cannot satisfy min_tier lease guarantee");
    }
  }

  return ctx_.manager->AcquireReadLease(req.id(), req.min_tier(), req.min_lease_duration_ms());
}

void DataService::ReleaseLease(const ReleaseLeaseRequest& req) {
  ctx_.manager->ReleaseLease(req.lease_id());
}

} // namespace payload::service
