#include "data_service.hpp"
#include "internal/core/payload_manager.hpp"
#include "payload/manager/v1_compat.hpp"

namespace payload::service {

using namespace payload::manager::v1;

DataService::DataService(ServiceContext ctx)
    : ctx_(std::move(ctx)) {}

ResolveSnapshotResponse
DataService::ResolveSnapshot(const ResolveSnapshotRequest& req) {

  ResolveSnapshotResponse resp;
  *resp.mutable_payload_descriptor() = ctx_.manager->ResolveSnapshot(req.id());
  return resp;
}

AcquireReadLeaseResponse
DataService::AcquireReadLease(const AcquireReadLeaseRequest& req) {

  return ctx_.manager->AcquireReadLease(
      req.id(),
      req.min_tier(),
      req.min_lease_duration_ms());
}

void DataService::ReleaseLease(const ReleaseLeaseRequest& req) {
  ctx_.manager->ReleaseLease(req.lease_id());
}

}
