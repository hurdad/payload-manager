#pragma once

#include <memory>
#include <grpcpp/grpcpp.h>

#include "payload/manager/services/v1/payload_catalog_service.grpc.pb.h"
#include "internal/service/catalog_service.hpp"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

class CatalogServer final : public payload::manager::v1::PayloadCatalogService::Service {
public:
  explicit CatalogServer(std::shared_ptr<payload::service::CatalogService> svc);

  ::grpc::Status AllocatePayload(::grpc::ServerContext*,
                               const payload::manager::v1::AllocatePayloadRequest*,
                               payload::manager::v1::AllocatePayloadResponse*) override;

  ::grpc::Status CommitPayload(::grpc::ServerContext*,
                             const payload::manager::v1::CommitPayloadRequest*,
                             payload::manager::v1::CommitPayloadResponse*) override;

  ::grpc::Status Delete(::grpc::ServerContext*,
                      const payload::manager::v1::DeleteRequest*,
                      google::protobuf::Empty*) override;

private:
  std::shared_ptr<payload::service::CatalogService> service_;
};

}
