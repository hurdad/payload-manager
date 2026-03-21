#include <grpcpp/grpcpp.h>

#include <gtest/gtest.h>

#include <memory>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/grpc/catalog_server.hpp"
#include "internal/grpc/data_server.hpp"
#include "internal/grpc/stream_server.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/lineage/lineage_graph.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/service/stream_service.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::manager::v1::TIER_RAM;

payload::service::ServiceContext BuildServiceContext() {
  payload::service::ServiceContext ctx;
  auto                             repository = std::make_shared<payload::db::memory::MemoryRepository>();
  ctx.manager                                 = std::make_shared<payload::core::PayloadManager>(payload::storage::StorageFactory::TierMap{},
                                                                                                std::make_shared<payload::lease::LeaseManager>(), repository);
  ctx.repository                              = repository;
  ctx.metadata                                = std::make_shared<payload::metadata::MetadataCache>();
  ctx.lineage                                 = std::make_shared<payload::lineage::LineageGraph>();
  return ctx;
}

payload::manager::v1::PayloadID AllocateAndCommit(payload::service::ServiceContext& ctx) {
  const auto desc      = ctx.manager->Allocate(128, TIER_RAM);
  const auto committed = ctx.manager->Commit(desc.payload_id());
  (void)committed;
  return desc.payload_id();
}

} // namespace

TEST(GrpcStatus, CommitMissingPayloadReturnsNotFound) {
  auto                         ctx             = BuildServiceContext();
  auto                         catalog_service = std::make_shared<payload::service::CatalogService>(ctx);
  payload::grpc::CatalogServer server(catalog_service);

  payload::manager::v1::CommitPayloadRequest req;
  req.mutable_id()->set_value("missing-payload");
  payload::manager::v1::CommitPayloadResponse resp;
  ::grpc::ServerContext                       grpc_ctx;

  const auto status = server.CommitPayload(&grpc_ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
}

TEST(GrpcStatus, AcquireUnsupportedLeaseModeReturnsFailedPrecondition) {
  auto                      ctx          = BuildServiceContext();
  auto                      data_service = std::make_shared<payload::service::DataService>(ctx);
  payload::grpc::DataServer server(data_service);

  payload::manager::v1::AcquireReadLeaseRequest req;
  req.mutable_id()->set_value("payload-id");
  req.set_mode(static_cast<payload::manager::v1::LeaseMode>(999));

  payload::manager::v1::AcquireReadLeaseResponse resp;
  ::grpc::ServerContext                          grpc_ctx;

  const auto status = server.AcquireReadLease(&grpc_ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), ::grpc::StatusCode::FAILED_PRECONDITION);
}

TEST(GrpcStatus, DeleteWithActiveLeaseReturnsAborted) {
  auto                         ctx             = BuildServiceContext();
  auto                         catalog_service = std::make_shared<payload::service::CatalogService>(ctx);
  payload::grpc::CatalogServer server(catalog_service);

  const auto id = AllocateAndCommit(ctx);
  ctx.manager->AcquireReadLease(id, TIER_RAM, 10'000);

  payload::manager::v1::DeleteRequest req;
  *req.mutable_id() = id;
  req.set_force(false);

  google::protobuf::Empty resp;
  ::grpc::ServerContext   grpc_ctx;

  const auto status = server.Delete(&grpc_ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), ::grpc::StatusCode::ABORTED);
}

TEST(GrpcStatus, PinMissingPayloadReturnsNotFound) {
  auto                         ctx             = BuildServiceContext();
  auto                         catalog_service = std::make_shared<payload::service::CatalogService>(ctx);
  payload::grpc::CatalogServer server(catalog_service);

  payload::manager::v1::PinRequest req;
  req.mutable_id()->set_value("missing-payload");
  req.set_duration_ms(1000);

  google::protobuf::Empty resp;
  ::grpc::ServerContext   grpc_ctx;

  const auto status = server.Pin(&grpc_ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
}

TEST(GrpcStatus, CreateStreamMissingNameReturnsFailedPrecondition) {
  auto                        ctx            = BuildServiceContext();
  auto                        stream_service = std::make_shared<payload::service::StreamService>(ctx);
  payload::grpc::StreamServer server(stream_service);

  payload::manager::v1::CreateStreamRequest req;
  req.mutable_stream()->set_namespace_("ns");

  google::protobuf::Empty resp;
  ::grpc::ServerContext   grpc_ctx;

  const auto status = server.CreateStream(&grpc_ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), ::grpc::StatusCode::FAILED_PRECONDITION);
}

TEST(GrpcStatus, DeleteStreamMissingNameReturnsFailedPrecondition) {
  auto                        ctx            = BuildServiceContext();
  auto                        stream_service = std::make_shared<payload::service::StreamService>(ctx);
  payload::grpc::StreamServer server(stream_service);

  payload::manager::v1::DeleteStreamRequest req;
  req.mutable_stream()->set_namespace_("ns");

  google::protobuf::Empty resp;
  ::grpc::ServerContext   grpc_ctx;

  const auto status = server.DeleteStream(&grpc_ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), ::grpc::StatusCode::FAILED_PRECONDITION);
}

// Fix 3: Allocate with zero bytes must return INVALID_ARGUMENT (not INTERNAL).
TEST(GrpcStatus, AllocateZeroBytesReturnsInvalidArgument) {
  auto                         ctx             = BuildServiceContext();
  auto                         catalog_service = std::make_shared<payload::service::CatalogService>(ctx);
  payload::grpc::CatalogServer server(catalog_service);

  payload::manager::v1::AllocatePayloadRequest req;
  req.set_size_bytes(0);
  req.set_preferred_tier(TIER_RAM);

  payload::manager::v1::AllocatePayloadResponse resp;
  ::grpc::ServerContext                         grpc_ctx;

  const auto status = server.AllocatePayload(&grpc_ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
}

// Fix 3: Allocate with oversized bytes (> 128 GiB) must return INVALID_ARGUMENT.
TEST(GrpcStatus, AllocateOversizedBytesReturnsInvalidArgument) {
  auto                         ctx             = BuildServiceContext();
  auto                         catalog_service = std::make_shared<payload::service::CatalogService>(ctx);
  payload::grpc::CatalogServer server(catalog_service);

  payload::manager::v1::AllocatePayloadRequest req;
  req.set_size_bytes(static_cast<uint64_t>(128) * 1024 * 1024 * 1024 + 1);
  req.set_preferred_tier(TIER_RAM);

  payload::manager::v1::AllocatePayloadResponse resp;
  ::grpc::ServerContext                         grpc_ctx;

  const auto status = server.AllocatePayload(&grpc_ctx, &req, &resp);
  EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
}
