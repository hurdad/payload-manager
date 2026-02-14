#pragma once

#include <memory>
#include <grpcpp/grpcpp.h>

#include "payload/manager/services/v1/payload_data_service.grpc.pb.h"
#include "internal/service/data_service.hpp"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

class DataServer final : public payload::manager::v1::PayloadDataService::Service {
public:
  explicit DataServer(std::shared_ptr<payload::service::DataService> svc);

  ::grpc::Status ResolveSnapshot(::grpc::ServerContext* ctx,
                               const payload::manager::v1::ResolveSnapshotRequest* req,
                               payload::manager::v1::ResolveSnapshotResponse* resp) override;

  ::grpc::Status AcquireReadLease(::grpc::ServerContext* ctx,
                                const payload::manager::v1::AcquireReadLeaseRequest* req,
                                payload::manager::v1::AcquireReadLeaseResponse* resp) override;

  ::grpc::Status ReleaseLease(::grpc::ServerContext* ctx,
                            const payload::manager::v1::ReleaseLeaseRequest* req,
                            google::protobuf::Empty* resp) override;

private:
  std::shared_ptr<payload::service::DataService> service_;
};

}
