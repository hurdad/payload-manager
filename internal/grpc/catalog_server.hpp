#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>

#include "internal/service/catalog_service.hpp"
#include "payload/manager/services/v1/payload_catalog_service.grpc.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

class CatalogServer final : public payload::manager::v1::PayloadCatalogService::Service {
 public:
  explicit CatalogServer(std::shared_ptr<payload::service::CatalogService> svc);

  ::grpc::Status AllocatePayload(::grpc::ServerContext*, const payload::manager::v1::AllocatePayloadRequest*,
                                 payload::manager::v1::AllocatePayloadResponse*) override;

  ::grpc::Status CommitPayload(::grpc::ServerContext*, const payload::manager::v1::CommitPayloadRequest*,
                               payload::manager::v1::CommitPayloadResponse*) override;

  ::grpc::Status Delete(::grpc::ServerContext*, const payload::manager::v1::DeleteRequest*, google::protobuf::Empty*) override;

  ::grpc::Status Promote(::grpc::ServerContext*, const payload::manager::v1::PromoteRequest*, payload::manager::v1::PromoteResponse*) override;

  ::grpc::Status Spill(::grpc::ServerContext*, const payload::manager::v1::SpillRequest*, payload::manager::v1::SpillResponse*) override;

  ::grpc::Status AddLineage(::grpc::ServerContext*, const payload::manager::v1::AddLineageRequest*, google::protobuf::Empty*) override;

  ::grpc::Status GetLineage(::grpc::ServerContext*, const payload::manager::v1::GetLineageRequest*,
                            payload::manager::v1::GetLineageResponse*) override;

  ::grpc::Status UpdatePayloadMetadata(::grpc::ServerContext*, const payload::manager::v1::UpdatePayloadMetadataRequest*,
                                       payload::manager::v1::UpdatePayloadMetadataResponse*) override;

  ::grpc::Status AppendPayloadMetadataEvent(::grpc::ServerContext*, const payload::manager::v1::AppendPayloadMetadataEventRequest*,
                                            payload::manager::v1::AppendPayloadMetadataEventResponse*) override;

 private:
  std::shared_ptr<payload::service::CatalogService> service_;
};

} // namespace payload::grpc
