#pragma once

#include "payload/manager/services/v1/payload_data_service.pb.h"
#include "service_context.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

class DataService {
public:
  explicit DataService(ServiceContext ctx);

  payload::manager::v1::ResolveSnapshotResponse
  ResolveSnapshot(const payload::manager::v1::ResolveSnapshotRequest& req);

  payload::manager::v1::AcquireReadLeaseResponse
  AcquireReadLease(const payload::manager::v1::AcquireReadLeaseRequest& req);

  void ReleaseLease(const payload::manager::v1::ReleaseLeaseRequest& req);

private:
  ServiceContext ctx_;
};

}
