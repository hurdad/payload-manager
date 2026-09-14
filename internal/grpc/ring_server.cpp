#include "internal/grpc/ring_server.hpp"

#include "internal/grpc/grpc_error.hpp"

namespace payload::grpc {

namespace runtimev1 = payload::manager::runtime::v1;

RingServer::RingServer(std::shared_ptr<payload::service::RingService> svc) : service_(std::move(svc)) {
}

::grpc::Status RingServer::AcquireRingSlot(::grpc::ServerContext*, const runtimev1::AcquireRingSlotRequest* req,
                                           runtimev1::AcquireRingSlotResponse* resp) {
  try {
    *resp = service_->AcquireRingSlot(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status RingServer::CommitRingSlot(::grpc::ServerContext*, const runtimev1::CommitRingSlotRequest* req,
                                          runtimev1::CommitRingSlotResponse* resp) {
  try {
    *resp = service_->CommitRingSlot(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status RingServer::MapRing(::grpc::ServerContext*, const runtimev1::MapRingRequest* req, runtimev1::MapRingResponse* resp) {
  try {
    *resp = service_->MapRing(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status RingServer::LeaseRingSlot(::grpc::ServerContext*, const runtimev1::LeaseRingSlotRequest* req, runtimev1::LeaseRingSlotResponse* resp) {
  try {
    *resp = service_->LeaseRingSlot(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status RingServer::ReleaseRingSlot(::grpc::ServerContext*, const runtimev1::ReleaseRingSlotRequest* req, google::protobuf::Empty*) {
  try {
    service_->ReleaseRingSlot(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

} // namespace payload::grpc
