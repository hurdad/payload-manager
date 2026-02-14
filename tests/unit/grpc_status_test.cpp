#include <cassert>
#include <iostream>
#include <memory>

#include <grpcpp/grpcpp.h>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/grpc/catalog_server.hpp"
#include "internal/grpc/data_server.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/lineage/lineage_graph.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::manager::v1::TIER_RAM;

payload::service::ServiceContext BuildServiceContext() {
  payload::service::ServiceContext ctx;
  auto repository = std::make_shared<payload::db::memory::MemoryRepository>();
  ctx.manager = std::make_shared<payload::core::PayloadManager>(payload::storage::StorageFactory::TierMap{},
                                                                std::make_shared<payload::lease::LeaseManager>(), nullptr, nullptr, repository);
  ctx.repository = repository;
  ctx.metadata = std::make_shared<payload::metadata::MetadataCache>();
  ctx.lineage  = std::make_shared<payload::lineage::LineageGraph>();
  return ctx;
}

payload::manager::v1::PayloadID AllocateAndCommit(payload::service::ServiceContext& ctx) {
  const auto desc = ctx.manager->Allocate(128, TIER_RAM);
  const auto committed = ctx.manager->Commit(desc.id());
  (void)committed;
  return desc.id();
}

void TestCommitMissingPayloadReturnsNotFound() {
  auto ctx             = BuildServiceContext();
  auto catalog_service = std::make_shared<payload::service::CatalogService>(ctx);
  payload::grpc::CatalogServer server(catalog_service);

  payload::manager::v1::CommitPayloadRequest req;
  req.mutable_id()->set_value("missing-payload");
  payload::manager::v1::CommitPayloadResponse resp;
  ::grpc::ServerContext grpc_ctx;

  const auto status = server.CommitPayload(&grpc_ctx, &req, &resp);
  assert(status.error_code() == ::grpc::StatusCode::NOT_FOUND);
}

void TestAcquireUnsupportedLeaseModeReturnsFailedPrecondition() {
  auto ctx          = BuildServiceContext();
  auto data_service = std::make_shared<payload::service::DataService>(ctx);
  payload::grpc::DataServer server(data_service);

  payload::manager::v1::AcquireReadLeaseRequest req;
  req.mutable_id()->set_value("payload-id");
  req.set_mode(static_cast<payload::manager::v1::LeaseMode>(999));

  payload::manager::v1::AcquireReadLeaseResponse resp;
  ::grpc::ServerContext grpc_ctx;

  const auto status = server.AcquireReadLease(&grpc_ctx, &req, &resp);
  assert(status.error_code() == ::grpc::StatusCode::FAILED_PRECONDITION);
}

void TestDeleteWithActiveLeaseReturnsAborted() {
  auto ctx             = BuildServiceContext();
  auto catalog_service = std::make_shared<payload::service::CatalogService>(ctx);
  payload::grpc::CatalogServer server(catalog_service);

  const auto id = AllocateAndCommit(ctx);
  ctx.manager->AcquireReadLease(id, TIER_RAM, 10'000);

  payload::manager::v1::DeleteRequest req;
  *req.mutable_id() = id;
  req.set_force(false);

  google::protobuf::Empty resp;
  ::grpc::ServerContext grpc_ctx;

  const auto status = server.Delete(&grpc_ctx, &req, &resp);
  assert(status.error_code() == ::grpc::StatusCode::ABORTED);
}

} // namespace

int main() {
  TestCommitMissingPayloadReturnsNotFound();
  TestAcquireUnsupportedLeaseModeReturnsFailedPrecondition();
  TestDeleteWithActiveLeaseReturnsAborted();

  std::cout << "payload_manager_unit_grpc_status: pass\n";
  return 0;
}
