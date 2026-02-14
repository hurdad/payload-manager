#include "data_server.hpp"
#include "grpc_error.hpp"
#include "payload/manager/v1_compat.hpp"

namespace payload::grpc {

DataServer::DataServer(std::shared_ptr<payload::service::DataService> svc)
    : service_(std::move(svc)) {}

::grpc::Status DataServer::ResolveSnapshot(::grpc::ServerContext*,
                                         const payload::manager::v1::ResolveSnapshotRequest* req,
                                         payload::manager::v1::ResolveSnapshotResponse* resp) {
  try {
    *resp = service_->ResolveSnapshot(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status DataServer::AcquireReadLease(::grpc::ServerContext*,
                                          const payload::manager::v1::AcquireReadLeaseRequest* req,
                                          payload::manager::v1::AcquireReadLeaseResponse* resp) {
  try {
    *resp = service_->AcquireReadLease(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status DataServer::ReleaseLease(::grpc::ServerContext*,
                                      const payload::manager::v1::ReleaseLeaseRequest* req,
                                      google::protobuf::Empty*) {
  try {
    service_->ReleaseLease(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

}
