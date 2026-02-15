#include "internal/service/catalog_service.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <string>

#include "internal/db/memory/memory_repository.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/service/service_context.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::manager::v1::AddLineageRequest;
using payload::manager::v1::GetLineageRequest;
using payload::manager::v1::METADATA_UPDATE_MODE_MERGE;
using payload::manager::v1::METADATA_UPDATE_MODE_REPLACE;
using payload::manager::v1::UpdatePayloadMetadataRequest;

payload::service::ServiceContext BuildServiceContext(bool with_metadata_cache) {
  payload::service::ServiceContext ctx;
  ctx.repository = std::make_shared<payload::db::memory::MemoryRepository>();
  if (with_metadata_cache) {
    ctx.metadata = std::make_shared<payload::metadata::MetadataCache>();
  }
  return ctx;
}

AddLineageRequest MakeLineageRequest(const std::string& child, const std::string& parent, const std::string& op) {
  AddLineageRequest req;
  req.mutable_child()->set_value(child);
  auto* edge = req.add_parents();
  edge->mutable_parent()->set_value(parent);
  edge->set_operation(op);
  edge->set_role("test");
  return req;
}

void TestUpdateMetadataUpsertsIntoRepositoryWithoutCache() {
  auto                             ctx = BuildServiceContext(false);
  payload::service::CatalogService service(ctx);

  UpdatePayloadMetadataRequest req;
  req.mutable_id()->set_value("payload-1");
  req.set_mode(METADATA_UPDATE_MODE_REPLACE);
  req.mutable_metadata()->set_data("{\"state\":\"new\"}");
  req.mutable_metadata()->set_schema("schema.v1");

  const auto resp = service.UpdateMetadata(req);

  auto       record_tx = ctx.repository->Begin();
  const auto db_record = ctx.repository->GetMetadata(*record_tx, "payload-1");
  assert(db_record.has_value());
  assert(db_record->json == "{\"state\":\"new\"}");
  assert(db_record->schema == "schema.v1");

  assert(resp.metadata().data() == "{\"state\":\"new\"}");
  assert(resp.metadata().schema() == "schema.v1");
}

void TestUpdateMetadataMergeUsesRepositoryStateAndWritesThroughCache() {
  auto                             ctx = BuildServiceContext(true);
  payload::service::CatalogService service(ctx);

  UpdatePayloadMetadataRequest replace;
  replace.mutable_id()->set_value("payload-merge");
  replace.set_mode(METADATA_UPDATE_MODE_REPLACE);
  replace.mutable_metadata()->set_data("initial");
  replace.mutable_metadata()->set_schema("schema.v1");
  service.UpdateMetadata(replace);

  UpdatePayloadMetadataRequest merge;
  merge.mutable_id()->set_value("payload-merge");
  merge.set_mode(METADATA_UPDATE_MODE_MERGE);
  merge.mutable_metadata()->set_schema("schema.v2");

  const auto resp = service.UpdateMetadata(merge);

  auto       record_tx = ctx.repository->Begin();
  const auto db_record = ctx.repository->GetMetadata(*record_tx, "payload-merge");
  assert(db_record.has_value());
  assert(db_record->json == "initial");
  assert(db_record->schema == "schema.v2");

  const auto cached = ctx.metadata->Get(merge.id());
  assert(cached.has_value());
  assert(cached->data() == "initial");
  assert(cached->schema() == "schema.v2");

  assert(resp.metadata().data() == "initial");
  assert(resp.metadata().schema() == "schema.v2");
}

void TestAddLineageInsertsIntoRepositoryWhenGraphIsMissing() {
  auto                             ctx = BuildServiceContext(false);
  payload::service::CatalogService service(ctx);

  service.AddLineage(MakeLineageRequest("child", "parent-a", "op-a"));
  service.AddLineage(MakeLineageRequest("child", "parent-b", "op-b"));

  auto       tx      = ctx.repository->Begin();
  const auto parents = ctx.repository->GetParents(*tx, "child");
  assert(parents.size() == 2);

  const auto has_op_a = std::any_of(parents.begin(), parents.end(), [](const auto& edge) { return edge.operation == "op-a"; });
  const auto has_op_b = std::any_of(parents.begin(), parents.end(), [](const auto& edge) { return edge.operation == "op-b"; });
  assert(has_op_a);
  assert(has_op_b);
}

void TestGetLineageTraversesRepositoryGraph() {
  auto                             ctx = BuildServiceContext(false);
  payload::service::CatalogService service(ctx);

  service.AddLineage(MakeLineageRequest("B", "A", "a_to_b"));
  service.AddLineage(MakeLineageRequest("C", "B", "b_to_c"));
  service.AddLineage(MakeLineageRequest("D", "C", "c_to_d"));

  GetLineageRequest upstream_all;
  upstream_all.mutable_id()->set_value("D");
  upstream_all.set_upstream(true);
  upstream_all.set_max_depth(0);
  const auto all_edges = service.GetLineage(upstream_all);
  assert(all_edges.edges_size() == 3);

  GetLineageRequest upstream_depth_1;
  upstream_depth_1.mutable_id()->set_value("D");
  upstream_depth_1.set_upstream(true);
  upstream_depth_1.set_max_depth(1);
  const auto one_hop = service.GetLineage(upstream_depth_1);
  assert(one_hop.edges_size() == 1);
  assert(one_hop.edges(0).operation() == "c_to_d");

  GetLineageRequest downstream;
  downstream.mutable_id()->set_value("A");
  downstream.set_upstream(false);
  downstream.set_max_depth(0);
  const auto down_edges = service.GetLineage(downstream);
  assert(down_edges.edges_size() == 3);
}

} // namespace

int main() {
  TestUpdateMetadataUpsertsIntoRepositoryWithoutCache();
  TestUpdateMetadataMergeUsesRepositoryStateAndWritesThroughCache();
  TestAddLineageInsertsIntoRepositoryWhenGraphIsMissing();
  TestGetLineageTraversesRepositoryGraph();

  std::cout << "payload_manager_unit_catalog_service: pass\n";
  return 0;
}
