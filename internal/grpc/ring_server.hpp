#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>

#include "google/protobuf/empty.pb.h"
#include "internal/service/ring_service.hpp"
#include "payload/manager/services/v1/payload_ring_service.grpc.pb.h"

namespace payload::grpc {

class RingServer final : public payload::manager::services::v1::PayloadRingService::Service {
 public:
  explicit RingServer(std::shared_ptr<payload::service::RingService> svc);

  ::grpc::Status AcquireRingSlot(::grpc::ServerContext* ctx, const payload::manager::runtime::v1::AcquireRingSlotRequest* req,
                                 payload::manager::runtime::v1::AcquireRingSlotResponse* resp) override;

  ::grpc::Status CommitRingSlot(::grpc::ServerContext* ctx, const payload::manager::runtime::v1::CommitRingSlotRequest* req,
                                payload::manager::runtime::v1::CommitRingSlotResponse* resp) override;

  ::grpc::Status MapRing(::grpc::ServerContext* ctx, const payload::manager::runtime::v1::MapRingRequest* req,
                         payload::manager::runtime::v1::MapRingResponse* resp) override;

  ::grpc::Status LeaseRingSlot(::grpc::ServerContext* ctx, const payload::manager::runtime::v1::LeaseRingSlotRequest* req,
                               payload::manager::runtime::v1::LeaseRingSlotResponse* resp) override;

  ::grpc::Status ReleaseRingSlot(::grpc::ServerContext* ctx, const payload::manager::runtime::v1::ReleaseRingSlotRequest* req,
                                 google::protobuf::Empty* resp) override;

 private:
  std::shared_ptr<payload::service::RingService> service_;
};

} // namespace payload::grpc
