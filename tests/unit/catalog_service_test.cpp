#include "internal/service/catalog_service.hpp"

#include <gtest/gtest.h>

#include <algorithm>
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

} // namespace

TEST(CatalogService, UpdateMetadataUpsertsIntoRepositoryWithoutCache) {
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
  ASSERT_TRUE(db_record.has_value());
  EXPECT_EQ(db_record->json, "{\"state\":\"new\"}");
  EXPECT_EQ(db_record->schema, "schema.v1");

  EXPECT_EQ(resp.metadata().data(), "{\"state\":\"new\"}");
  EXPECT_EQ(resp.metadata().schema(), "schema.v1");
}

TEST(CatalogService, UpdateMetadataMergeUsesRepositoryStateAndWritesThroughCache) {
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
  ASSERT_TRUE(db_record.has_value());
  EXPECT_EQ(db_record->json, "initial");
  EXPECT_EQ(db_record->schema, "schema.v2");

  const auto cached = ctx.metadata->Get(merge.id());
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(cached->data(), "initial");
  EXPECT_EQ(cached->schema(), "schema.v2");

  EXPECT_EQ(resp.metadata().data(), "initial");
  EXPECT_EQ(resp.metadata().schema(), "schema.v2");
}

TEST(CatalogService, AddLineageInsertsIntoRepositoryWhenGraphIsMissing) {
  auto                             ctx = BuildServiceContext(false);
  payload::service::CatalogService service(ctx);

  service.AddLineage(MakeLineageRequest("child", "parent-a", "op-a"));
  service.AddLineage(MakeLineageRequest("child", "parent-b", "op-b"));

  auto       tx      = ctx.repository->Begin();
  const auto parents = ctx.repository->GetParents(*tx, "child");
  EXPECT_EQ(parents.size(), 2u);

  const auto has_op_a = std::any_of(parents.begin(), parents.end(), [](const auto& edge) { return edge.operation == "op-a"; });
  const auto has_op_b = std::any_of(parents.begin(), parents.end(), [](const auto& edge) { return edge.operation == "op-b"; });
  EXPECT_TRUE(has_op_a);
  EXPECT_TRUE(has_op_b);
}

TEST(CatalogService, GetLineageTraversesRepositoryGraph) {
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
  EXPECT_EQ(all_edges.edges_size(), 3);

  GetLineageRequest upstream_depth_1;
  upstream_depth_1.mutable_id()->set_value("D");
  upstream_depth_1.set_upstream(true);
  upstream_depth_1.set_max_depth(1);
  const auto one_hop = service.GetLineage(upstream_depth_1);
  EXPECT_EQ(one_hop.edges_size(), 1);
  EXPECT_EQ(one_hop.edges(0).operation(), "c_to_d");

  GetLineageRequest downstream;
  downstream.mutable_id()->set_value("A");
  downstream.set_upstream(false);
  downstream.set_max_depth(0);
  const auto down_edges = service.GetLineage(downstream);
  EXPECT_EQ(down_edges.edges_size(), 3);
}

// GetLineage on a payload with no recorded edges must return an empty response,
// not throw.
TEST(CatalogService, GetLineageOnUnknownPayloadReturnsEmpty) {
  auto                             ctx = BuildServiceContext(false);
  payload::service::CatalogService service(ctx);

  GetLineageRequest req;
  req.mutable_id()->set_value("nonexistent-payload");
  req.set_upstream(true);
  req.set_max_depth(0);

  // If GetLineage throws, the test fails with an unhandled exception — which is
  // the "must not throw" assertion.  The separate edges_size check confirms the
  // empty-response contract.
  const auto resp = service.GetLineage(req);
  EXPECT_EQ(resp.edges_size(), 0) << "no edges must be returned for a payload with no lineage";
}

// Diamond DAG: A→B, A→C, B→D, C→D.
// Traversing upstream from D must return each edge exactly once (4 edges total:
// D←B, D←C, B←A, C←A) — the shared ancestor A must not be double-counted.
TEST(CatalogService, GetLineageDiamondDagNoDuplicateEdges) {
  auto                             ctx = BuildServiceContext(false);
  payload::service::CatalogService service(ctx);

  service.AddLineage(MakeLineageRequest("B", "A", "a_to_b"));
  service.AddLineage(MakeLineageRequest("C", "A", "a_to_c"));
  service.AddLineage(MakeLineageRequest("D", "B", "b_to_d"));
  service.AddLineage(MakeLineageRequest("D", "C", "c_to_d"));

  GetLineageRequest req;
  req.mutable_id()->set_value("D");
  req.set_upstream(true);
  req.set_max_depth(0);

  const auto resp = service.GetLineage(req);
  // 4 distinct edges in the diamond: D←B, D←C, B←A, C←A.
  EXPECT_EQ(resp.edges_size(), 4) << "diamond DAG must return 4 distinct edges with no duplicates";
}
