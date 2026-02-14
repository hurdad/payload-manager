#include "catalog_service.hpp"

#include <stdexcept>

#include "internal/core/payload_manager.hpp"
#include "internal/lineage/lineage_graph.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/util/time.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

CatalogService::CatalogService(ServiceContext ctx) : ctx_(std::move(ctx)) {
}

AllocatePayloadResponse CatalogService::Allocate(const AllocatePayloadRequest& req) {
  if (req.ttl_ms() > 0 || req.persist() || req.has_eviction_policy()) {
    throw std::runtime_error("allocate: ttl_ms, persist, and eviction_policy are not implemented");
  }

  AllocatePayloadResponse resp;
  *resp.mutable_payload_descriptor() = ctx_.manager->Allocate(req.size_bytes(), req.preferred_tier());

  return resp;
}

CommitPayloadResponse CatalogService::Commit(const CommitPayloadRequest& req) {
  CommitPayloadResponse resp;
  *resp.mutable_payload_descriptor() = ctx_.manager->Commit(req.id());
  return resp;
}

PromoteResponse CatalogService::Promote(const PromoteRequest& req) {
  PromoteResponse resp;
  *resp.mutable_payload_descriptor() = ctx_.manager->Promote(req.id(), req.target_tier());
  return resp;
}

SpillResponse CatalogService::Spill(const SpillRequest& req) {
  SpillResponse resp;

  const auto target_tier = TIER_DISK;
  for (const auto& id : req.ids()) {
    auto* result          = resp.add_results();
    *result->mutable_id() = id;

    try {
      ctx_.manager->ExecuteSpill(id, target_tier, req.fsync());
      result->set_ok(true);
      *result->mutable_payload_descriptor() = ctx_.manager->ResolveSnapshot(id);
    } catch (const std::exception& e) {
      result->set_ok(false);
      result->set_error_message(e.what());
    }
  }

  (void)req.policy();
  (void)req.wait_for_leases();

  return resp;
}

void CatalogService::AddLineage(const AddLineageRequest& req) {
  ctx_.lineage->Add(req);
}

GetLineageResponse CatalogService::GetLineage(const GetLineageRequest& req) {
  GetLineageResponse resp;

  auto edges = ctx_.lineage->Query(req);
  for (auto& e : edges) *resp.add_edges() = e;

  return resp;
}

void CatalogService::Delete(const DeleteRequest& req) {
  ctx_.manager->Delete(req.id(), req.force());
  ctx_.metadata->Remove(req.id());
}

UpdatePayloadMetadataResponse CatalogService::UpdateMetadata(const UpdatePayloadMetadataRequest& req) {
  UpdatePayloadMetadataResponse resp;

  if (req.mode() == METADATA_UPDATE_MODE_REPLACE)
    ctx_.metadata->Put(req.id(), req.metadata());
  else
    ctx_.metadata->Merge(req.id(), req.metadata());

  *resp.mutable_id()         = req.id();
  *resp.mutable_metadata()   = req.metadata();
  *resp.mutable_updated_at() = payload::util::ToProto(payload::util::Now());
  return resp;
}

AppendPayloadMetadataEventResponse CatalogService::AppendMetadataEvent(const AppendPayloadMetadataEventRequest& req) {
  AppendPayloadMetadataEventResponse resp;
  *resp.mutable_id()         = req.id();
  *resp.mutable_event_time() = payload::util::ToProto(payload::util::Now());
  return resp;
}

} // namespace payload::service
