#include "catalog_service.hpp"

#include <chrono>
#include <queue>
#include <stdexcept>
#include <unordered_set>

#include "internal/core/payload_manager.hpp"
#include "internal/db/api/repository.hpp"
#include "internal/db/model/lineage_record.hpp"
#include "internal/db/model/metadata_event_record.hpp"
#include "internal/db/model/metadata_record.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/lineage/lineage_graph.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/service/observe_rpc.hpp"
#include "internal/spill/spill_scheduler.hpp"
#include "internal/spill/spill_task.hpp"
#include "internal/util/errors.hpp"
#include "internal/util/time.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

namespace {

void ThrowIfDbError(const payload::db::Result& result, const std::string& context) {
  if (result) {
    return;
  }

  const auto message = result.message.empty() ? context : context + ": " + result.message;
  switch (result.code) {
    case payload::db::ErrorCode::AlreadyExists:
      throw payload::util::AlreadyExists(message);
    case payload::db::ErrorCode::NotFound:
      throw payload::util::NotFound(message);
    case payload::db::ErrorCode::Conflict:
      throw payload::util::InvalidState(message);
    default:
      throw std::runtime_error(message);
  }
}

LineageEdge ToLineageEdge(const payload::db::model::LineageRecord& record) {
  LineageEdge edge;
  edge.mutable_parent()->set_value(record.parent_id);
  edge.set_operation(record.operation);
  edge.set_role(record.role);
  edge.set_parameters(record.parameters);
  return edge;
}

} // namespace

CatalogService::CatalogService(ServiceContext ctx) : ctx_(std::move(ctx)) {
}

AllocatePayloadResponse CatalogService::Allocate(const AllocatePayloadRequest& req) {
  return ObserveRpc("CatalogService.Allocate", nullptr, [&] {
    if (req.preferred_tier() == TIER_UNSPECIFIED) {
      throw payload::util::InvalidState("allocate: preferred_tier must be specified");
    }
    AllocatePayloadResponse resp;
    *resp.mutable_payload_descriptor() =
        ctx_.manager->Allocate(req.size_bytes(), req.preferred_tier(), req.ttl_ms(), req.persist(), req.eviction_policy());
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
    if (req.target_tier() == TIER_UNSPECIFIED) {
      throw payload::util::InvalidState("promote: target_tier must be specified");
    }
    PromoteResponse resp;
    *resp.mutable_payload_descriptor() = ctx_.manager->Promote(req.id(), req.target_tier());
    return resp;
  });
}

SpillResponse CatalogService::Spill(const SpillRequest& req) {
  return ObserveRpc("CatalogService.Spill", nullptr, [&] {
    SpillResponse resp;

    const bool best_effort = (req.policy() == SPILL_POLICY_BEST_EFFORT) && ctx_.spill_scheduler;

    for (const auto& id : req.ids()) {
      auto* result          = resp.add_results();
      *result->mutable_id() = id;

      try {
        // If requested, wait for all active read leases to expire before spilling.
        // Leases will naturally expire; we wait up to max_lease_ms rather than
        // spinning indefinitely.
        if (req.wait_for_leases() && ctx_.lease_mgr) {
          const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ctx_.spill_wait_timeout_ms);
          if (!ctx_.lease_mgr->WaitUntilNoLeases(id, deadline)) {
            throw payload::util::LeaseConflict("spill: active leases remain after wait timeout; release leases or retry without wait_for_leases");
          }
        }

        if (best_effort) {
          // Fire-and-forget: enqueue to the background spill workers.
          spill::SpillTask task;
          task.id          = id;
          task.target_tier = ctx_.manager->GetSpillTarget(id);
          task.fsync       = req.fsync();
          ctx_.spill_scheduler->Enqueue(task);
          result->set_ok(true);
          // No descriptor: data movement has not completed yet.
        } else {
          // Blocking: execute synchronously and return the final descriptor.
          payload::observability::SpanScope spill_span("CatalogService.SpillItem");
          spill_span.SetAttribute("payload.id", payload::util::PayloadIdToHex(id));
          const auto target_tier = ctx_.manager->GetSpillTarget(id);
          const auto spill_start = std::chrono::steady_clock::now();
          ctx_.manager->ExecuteSpill(id, target_tier, req.fsync());
          const auto spill_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - spill_start).count();
          payload::observability::Metrics::Instance().ObserveSpillDurationMs("rpc", spill_ms);
          result->set_ok(true);
          *result->mutable_payload_descriptor() = ctx_.manager->ResolveSnapshot(id);
        }
      } catch (const std::exception& e) {
        result->set_ok(false);
        result->set_error_message(e.what());
      }
    }

    return resp;
  });
}

void CatalogService::Prefetch(const PrefetchRequest& req) {
  ObserveRpc("CatalogService.Prefetch", &req.id(), [&] { ctx_.manager->Prefetch(req.id(), req.target_tier()); });
}

void CatalogService::Pin(const PinRequest& req) {
  ObserveRpc("CatalogService.Pin", &req.id(), [&] { ctx_.manager->Pin(req.id(), req.duration_ms()); });
}

void CatalogService::Unpin(const UnpinRequest& req) {
  ObserveRpc("CatalogService.Unpin", &req.id(), [&] { ctx_.manager->Unpin(req.id()); });
}

void CatalogService::AddLineage(const AddLineageRequest& req) {
  ObserveRpc("CatalogService.AddLineage", &req.child(), [&] {
    auto tx = ctx_.repository->Begin();
    for (const auto& edge : req.parents()) {
      payload::db::model::LineageRecord record;
      record.parent_id     = payload::util::PayloadIdToHex(edge.parent());
      record.child_id      = payload::util::PayloadIdToHex(req.child());
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

    q.emplace(payload::util::PayloadIdToHex(req.id()), 0);
    visited.insert(payload::util::PayloadIdToHex(req.id()));

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
    // PayloadManager::Delete() handles metadata cache removal internally.
    ctx_.manager->Delete(req.id(), req.force());
  });
}

UpdatePayloadMetadataResponse CatalogService::UpdateMetadata(const UpdatePayloadMetadataRequest& req) {
  return ObserveRpc("CatalogService.UpdateMetadata", &req.id(), [&] {
    UpdatePayloadMetadataResponse resp;

    auto tx             = ctx_.repository->Begin();
    auto current_record = ctx_.repository->GetMetadata(*tx, payload::util::PayloadIdToHex(req.id()));

    payload::db::model::MetadataRecord record;
    record.id = payload::util::PayloadIdToHex(req.id());

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
    const auto now = payload::util::Now();

    payload::db::model::MetadataEventRecord record;
    record.id      = payload::util::PayloadIdToHex(req.id());
    record.data    = req.metadata().data();
    record.schema  = req.metadata().schema();
    record.source  = req.source();
    record.version = req.version();
    record.ts_ms   = payload::util::ToUnixMillis(now);

    auto tx = ctx_.repository->Begin();
    ThrowIfDbError(ctx_.repository->InsertMetadataEvent(*tx, record), "insert metadata event");
    tx->Commit();

    AppendPayloadMetadataEventResponse resp;
    *resp.mutable_id()         = req.id();
    *resp.mutable_event_time() = payload::util::ToProto(now);
    return resp;
  });
}

ListPayloadsResponse CatalogService::ListPayloads(const ListPayloadsRequest& req) {
  return ObserveRpc("CatalogService.ListPayloads", nullptr, [&] {
    ListPayloadsResponse resp;

    auto tx      = ctx_.repository->Begin();
    auto records = ctx_.repository->ListPayloads(*tx, req.tier_filter());
    tx->Commit();

    for (const auto& r : records) {
      // Skip records whose stored ID cannot be parsed — this protects against
      // database corruption introducing malformed IDs crashing the whole list.
      PayloadID proto_id;
      try {
        proto_id.set_value(payload::util::FromString(r.id).data(), 16);
      } catch (const std::exception&) {
        PAYLOAD_LOG_WARN("list payloads: skipping record with malformed id", {payload::observability::StringField("id", r.id)});
        continue;
      }

      auto* entry          = resp.add_payloads();
      *entry->mutable_id() = proto_id;

      entry->set_tier(r.tier);
      entry->set_state(r.state);
      entry->set_size_bytes(r.size_bytes);
      entry->set_created_at_ms(r.created_at_ms);
      entry->set_expires_at_ms(r.expires_at_ms);

      if (ctx_.lease_mgr) {
        entry->set_active_leases(ctx_.lease_mgr->CountActiveLeases(proto_id));
      }
    }

    return resp;
  });
}

} // namespace payload::service
