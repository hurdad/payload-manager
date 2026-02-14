#pragma once

#include "payload/manager/services/v1/payload_catalog_service.pb.h"
#include "service_context.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

class CatalogService {
public:
  explicit CatalogService(ServiceContext ctx);

  payload::manager::v1::AllocatePayloadResponse
  Allocate(const payload::manager::v1::AllocatePayloadRequest& req);

  payload::manager::v1::CommitPayloadResponse
  Commit(const payload::manager::v1::CommitPayloadRequest& req);

  void Delete(const payload::manager::v1::DeleteRequest& req);

  payload::manager::v1::PromoteResponse
  Promote(const payload::manager::v1::PromoteRequest& req);

  payload::manager::v1::SpillResponse
  Spill(const payload::manager::v1::SpillRequest& req);

  void AddLineage(const payload::manager::v1::AddLineageRequest& req);

  payload::manager::v1::GetLineageResponse
  GetLineage(const payload::manager::v1::GetLineageRequest& req);

  payload::manager::v1::UpdatePayloadMetadataResponse
  UpdateMetadata(const payload::manager::v1::UpdatePayloadMetadataRequest& req);

  payload::manager::v1::AppendPayloadMetadataEventResponse
  AppendMetadataEvent(const payload::manager::v1::AppendPayloadMetadataEventRequest& req);

private:
  ServiceContext ctx_;
};

}
