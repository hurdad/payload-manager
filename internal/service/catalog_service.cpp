#include "catalog_service.hpp"
#include "internal/core/payload_manager.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/lineage/lineage_graph.hpp"

namespace payload::service {

using namespace payload::manager::v1;

CatalogService::CatalogService(ServiceContext ctx)
    : ctx_(std::move(ctx)) {}

AllocatePayloadResponse
CatalogService::Allocate(const AllocatePayloadRequest& req) {

  AllocatePayloadResponse resp;
  *resp.mutable_payload_descriptor() =
      ctx_.manager->Allocate(req.size_bytes(), req.preferred_tier());

  return resp;
}

CommitPayloadResponse
CatalogService::Commit(const CommitPayloadRequest& req) {

  CommitPayloadResponse resp;
  *resp.mutable_payload_descriptor() = ctx_.manager->Commit(req.id());
  return resp;
}

void CatalogService::Delete(const DeleteRequest& req) {
  ctx_.manager->Delete(req.id(), req.force());
  ctx_.metadata->Remove(req.id());
}

UpdatePayloadMetadataResponse
CatalogService::UpdateMetadata(const UpdatePayloadMetadataRequest& req) {

  UpdatePayloadMetadataResponse resp;

  if (req.mode() == METADATA_UPDATE_MODE_REPLACE)
    ctx_.metadata->Put(req.id(), req.metadata());
  else
    ctx_.metadata->Merge(req.id(), req.metadata());

  *resp.mutable_id() = req.id();
  *resp.mutable_metadata() = req.metadata();
  return resp;
}

GetLineageResponse
CatalogService::GetLineage(const GetLineageRequest& req) {

  GetLineageResponse resp;

  auto edges = ctx_.lineage->Query(req);
  for (auto& e : edges)
    *resp.add_edges() = e;

  return resp;
}

}
