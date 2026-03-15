#include "data_service.hpp"

#include <stdexcept>

#include "internal/core/payload_manager.hpp"
#include "internal/service/observe_rpc.hpp"
#include "internal/util/errors.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

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

    return ctx_.manager->AcquireReadLease(req.id(), req.min_tier(), req.min_lease_duration_ms(), req.promotion_policy());
  });
}

void DataService::ReleaseLease(const ReleaseLeaseRequest& req) {
  ObserveRpc("DataService.ReleaseLease", nullptr, [&] { ctx_.manager->ReleaseLease(req.lease_id()); });
}

} // namespace payload::service
