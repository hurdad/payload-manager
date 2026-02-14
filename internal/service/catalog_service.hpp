#pragma once

#include "payload/manager/services/v1/payload_catalog_service.pb.h"
#include "service_context.hpp"
#include "payload/manager/v1_compat.hpp"

namespace payload::service {

class CatalogService {
public:
  explicit CatalogService(ServiceContext ctx);

  payload::manager::v1::AllocatePayloadResponse
  Allocate(const payload::manager::v1::AllocatePayloadRequest& req);

  payload::manager::v1::CommitPayloadResponse
  Commit(const payload::manager::v1::CommitPayloadRequest& req);

  void Delete(const payload::manager::v1::DeleteRequest& req);

  payload::manager::v1::UpdatePayloadMetadataResponse
  UpdateMetadata(const payload::manager::v1::UpdatePayloadMetadataRequest& req);

  payload::manager::v1::GetLineageResponse
  GetLineage(const payload::manager::v1::GetLineageRequest& req);

private:
  ServiceContext ctx_;
};

}
