#include "catalog_service.hpp"

#include <chrono>
#include <queue>
#include <stdexcept>
#include <type_traits>
#include <unordered_set>

#include "internal/core/payload_manager.hpp"
#include "internal/db/api/repository.hpp"
#include "internal/db/model/lineage_record.hpp"
#include "internal/db/model/metadata_record.hpp"
#include "internal/lineage/lineage_graph.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/util/errors.hpp"
#include "internal/util/time.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

namespace {

void ThrowIfDbError(const payload::db::Result& result, const std::string& prefix) {
  if (result) {
    return;
  }
  throw std::runtime_error(prefix + ": " + result.message);
}

LineageEdge ToLineageEdge(const payload::db::model::LineageRecord& record) {
  LineageEdge edge;
  edge.mutable_parent()->set_value(record.parent_id);
  edge.set_operation(record.operation);
  edge.set_role(record.role);
  edge.set_parameters(record.parameters);
  return edge;
}

template <typename Fn>
auto ObserveRpc(std::string_view route, const PayloadID* payload_id, Fn&& fn) {
  payload::observability::SpanScope span(route);
  if (payload_id) {
    span.SetAttribute("payload.id", payload_id->value());
  }

  const auto started_at = std::chrono::steady_clock::now();
  try {
    if constexpr (std::is_void_v<std::invoke_result_t<Fn>>) {
      fn();
      payload::observability::Metrics::Instance().RecordRequest(route, true);
      payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
          route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
      return;
    } else {
      auto result = fn();
      payload::observability::Metrics::Instance().RecordRequest(route, true);
      payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
          route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
      return result;
    }
  } catch (const std::exception& ex) {
    span.RecordException(ex.what());
    PAYLOAD_LOG_ERROR("RPC failed", {payload::observability::StringField("route", route), payload::observability::StringField("error", ex.what()),
                                     payload_id ? payload::observability::StringField("payload_id", payload_id->value())
                                                : payload::observability::StringField("payload_id", "")});
    payload::observability::Metrics::Instance().RecordRequest(route, false);
    payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
        route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
    throw;
  }
}

} // namespace

CatalogService::CatalogService(ServiceContext ctx) : ctx_(std::move(ctx)) {
}

AllocatePayloadResponse CatalogService::Allocate(const AllocatePayloadRequest& req) {
  return ObserveRpc("CatalogService.Allocate", nullptr, [&] {
    if (req.ttl_ms() > 0 || req.persist() || req.has_eviction_policy()) {
      throw payload::util::InvalidState("allocate payload: ttl_ms, persist, and eviction_policy are not implemented; omit these fields and retry");
    }

    AllocatePayloadResponse resp;
    *resp.mutable_payload_descriptor() = ctx_.manager->Allocate(req.size_bytes(), req.preferred_tier());
    return resp;
  });
}

CommitPayloadResponse CatalogService::Commit(const CommitPayloadRequest& req) {
  return ObserveRpc("CatalogService.Commit", &req.id(), [&] {
    CommitPayloadResponse resp;
    *resp.mutable_payload_descriptor() = ctx_.manager->Commit(req.id());
    return resp;
  });
}

PromoteResponse CatalogService::Promote(const PromoteRequest& req) {
  return ObserveRpc("CatalogService.Promote", &req.id(), [&] {
    PromoteResponse resp;
    *resp.mutable_payload_descriptor() = ctx_.manager->Promote(req.id(), req.target_tier());
    return resp;
  });
}

SpillResponse CatalogService::Spill(const SpillRequest& req) {
  return ObserveRpc("CatalogService.Spill", nullptr, [&] {
    SpillResponse resp;

    const auto target_tier = TIER_DISK;
    for (const auto& id : req.ids()) {
      auto* result          = resp.add_results();
      *result->mutable_id() = id;

      try {
        payload::observability::SpanScope spill_span("CatalogService.SpillItem");
        spill_span.SetAttribute("payload.id", id.value());
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
  });
}

void CatalogService::AddLineage(const AddLineageRequest& req) {
  ObserveRpc("CatalogService.AddLineage", &req.child(), [&] {
    auto tx = ctx_.repository->Begin();
    for (const auto& edge : req.parents()) {
      payload::db::model::LineageRecord record;
      record.parent_id     = edge.parent().value();
      record.child_id      = req.child().value();
      record.operation     = edge.operation();
      record.role          = edge.role();
      record.parameters    = edge.parameters();
      record.created_at_ms = payload::util::ToUnixMillis(payload::util::Now());
      ThrowIfDbError(ctx_.repository->InsertLineage(*tx, record), "insert lineage");
    }
    tx->Commit();

    if (ctx_.lineage) {
      ctx_.lineage->Add(req);
    }
  });
}

GetLineageResponse CatalogService::GetLineage(const GetLineageRequest& req) {
  return ObserveRpc("CatalogService.GetLineage", &req.id(), [&] {
    GetLineageResponse resp;

    auto tx = ctx_.repository->Begin();

    const auto                                   upstream  = req.upstream();
    const uint32_t                               max_depth = req.max_depth();
    std::queue<std::pair<std::string, uint32_t>> q;
    std::unordered_set<std::string>              visited;

    q.emplace(req.id().value(), 0);
    visited.insert(req.id().value());

    while (!q.empty()) {
      const auto [node, depth] = q.front();
      q.pop();

      if (max_depth && depth >= max_depth) {
        continue;
      }

      const auto records = upstream ? ctx_.repository->GetParents(*tx, node) : ctx_.repository->GetChildren(*tx, node);

      for (const auto& record : records) {
        *resp.add_edges() = ToLineageEdge(record);

        const auto& next_id = upstream ? record.parent_id : record.child_id;
        if (visited.insert(next_id).second) {
          q.emplace(next_id, depth + 1);
        }
      }
    }

    tx->Commit();

    return resp;
  });
}

void CatalogService::Delete(const DeleteRequest& req) {
  ObserveRpc("CatalogService.Delete", &req.id(), [&] {
    ctx_.manager->Delete(req.id(), req.force());
    if (ctx_.metadata) {
      ctx_.metadata->Remove(req.id());
    }
  });
}

UpdatePayloadMetadataResponse CatalogService::UpdateMetadata(const UpdatePayloadMetadataRequest& req) {
  return ObserveRpc("CatalogService.UpdateMetadata", &req.id(), [&] {
    UpdatePayloadMetadataResponse resp;

    auto tx             = ctx_.repository->Begin();
    auto current_record = ctx_.repository->GetMetadata(*tx, req.id().value());

    payload::db::model::MetadataRecord record;
    record.id = req.id().value();

    if (req.mode() == METADATA_UPDATE_MODE_REPLACE || !current_record.has_value()) {
      record.json   = req.metadata().data();
      record.schema = req.metadata().schema();
    } else {
      record.json   = req.metadata().data().empty() ? current_record->json : req.metadata().data();
      record.schema = req.metadata().schema().empty() ? current_record->schema : req.metadata().schema();
    }
    record.updated_at_ms = payload::util::ToUnixMillis(payload::util::Now());

    ThrowIfDbError(ctx_.repository->UpsertMetadata(*tx, record), "upsert metadata");
    tx->Commit();

    PayloadMetadata stored;
    *stored.mutable_id() = req.id();
    stored.set_data(record.json);
    stored.set_schema(record.schema);

    if (ctx_.metadata) {
      ctx_.metadata->Put(req.id(), stored);
    }

    *resp.mutable_id()         = req.id();
    *resp.mutable_metadata()   = stored;
    *resp.mutable_updated_at() = payload::util::ToProto(payload::util::Now());
    return resp;
  });
}

AppendPayloadMetadataEventResponse CatalogService::AppendMetadataEvent(const AppendPayloadMetadataEventRequest& req) {
  return ObserveRpc("CatalogService.AppendMetadataEvent", &req.id(), [&] {
    AppendPayloadMetadataEventResponse resp;
    *resp.mutable_id()         = req.id();
    *resp.mutable_event_time() = payload::util::ToProto(payload::util::Now());
    return resp;
  });
}

} // namespace payload::service
