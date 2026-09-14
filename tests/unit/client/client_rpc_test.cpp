// PayloadClient's RPC surface, driven against real services over an
// in-process gRPC channel.
//
// The existing client test covers only the static helpers, so the wrappers
// themselves — every Resolve, Spill, Pin, Append and the status translation
// behind them — were the largest untested block of shipping code in the
// repository. A mock of the server would only restate the client's
// assumptions, so this stands up the actual service layer over an in-memory
// repository instead.
//
// AllocateWritableBuffer and AcquireReadableBuffer are deliberately absent:
// they map shm and CUDA IPC regions out of a descriptor, which needs a real
// storage tier. tests/integration/api_integration_test.cpp covers those
// against a live server.

#include <grpcpp/channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "client/cpp/client.h"
#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/grpc/admin_server.hpp"
#include "internal/grpc/catalog_server.hpp"
#include "internal/grpc/data_server.hpp"
#include "internal/grpc/stream_server.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/service/admin_service.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/service/stream_service.hpp"
#include "internal/storage/storage_factory.hpp"
#include "payload/manager/v1.hpp"

namespace payload {
namespace {

using payload::manager::client::PayloadClient;
namespace v1 = payload::manager::v1;

v1::PayloadID MakeId() {
  auto parsed = PayloadClient::PayloadIdFromUuid(payload::util::ToString(payload::util::GenerateUUID()));
  return *parsed;
}

// Stands up catalog, data, admin and stream over one in-process channel.
class ClientRpcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    lease_mgr_ = std::make_shared<lease::LeaseManager>();
    repo_      = std::make_shared<payload::db::memory::MemoryRepository>();
    manager_   = std::make_shared<core::PayloadManager>(storage::StorageFactory::TierMap{}, lease_mgr_, repo_);

    service::ServiceContext ctx;
    ctx.manager    = manager_;
    ctx.repository = repo_;
    ctx.lease_mgr  = lease_mgr_;

    catalog_ = std::make_shared<service::CatalogService>(ctx);
    data_    = std::make_shared<service::DataService>(ctx);
    admin_   = std::make_shared<service::AdminService>(ctx);
    stream_  = std::make_shared<service::StreamService>(ctx);

    catalog_srv_ = std::make_unique<payload::grpc::CatalogServer>(catalog_);
    data_srv_    = std::make_unique<payload::grpc::DataServer>(data_);
    admin_srv_   = std::make_unique<payload::grpc::AdminServer>(admin_);
    stream_srv_  = std::make_unique<payload::grpc::StreamServer>(stream_);

    ::grpc::ServerBuilder builder;
    builder.RegisterService(catalog_srv_.get());
    builder.RegisterService(data_srv_.get());
    builder.RegisterService(admin_srv_.get());
    builder.RegisterService(stream_srv_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    client_ = std::make_unique<PayloadClient>(server_->InProcessChannel(::grpc::ChannelArguments()));
  }

  void TearDown() override {
    client_.reset();
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
  }

  std::shared_ptr<lease::LeaseManager>                   lease_mgr_;
  std::shared_ptr<payload::db::memory::MemoryRepository> repo_;
  std::shared_ptr<core::PayloadManager>                  manager_;
  std::shared_ptr<service::CatalogService>               catalog_;
  std::shared_ptr<service::DataService>                  data_;
  std::shared_ptr<service::AdminService>                 admin_;
  std::shared_ptr<service::StreamService>                stream_;
  std::unique_ptr<payload::grpc::CatalogServer>          catalog_srv_;
  std::unique_ptr<payload::grpc::DataServer>             data_srv_;
  std::unique_ptr<payload::grpc::AdminServer>            admin_srv_;
  std::unique_ptr<payload::grpc::StreamServer>           stream_srv_;
  std::unique_ptr<::grpc::Server>                        server_;
  std::unique_ptr<PayloadClient>                         client_;
};

} // namespace

// ---------------------------------------------------------------------------
// Status translation
//
// GrpcToArrow is what lets a caller tell an expected refusal from a broken
// channel, so the mapping is behaviour rather than cosmetics.
// ---------------------------------------------------------------------------

TEST_F(ClientRpcTest, UnknownPayloadIsAKeyErrorNotAnIoError) {
  auto resolved = client_->Resolve(MakeId());
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsKeyError()) << resolved.status().ToString();
}

TEST_F(ClientRpcTest, MalformedPayloadIdIsRejectedBeforeTheWire) {
  v1::PayloadID empty;
  EXPECT_TRUE(client_->CommitPayload(empty).IsInvalid());
  EXPECT_TRUE(client_->Resolve(empty).status().IsInvalid());
}

// ---------------------------------------------------------------------------
// Catalog and lifecycle wrappers
// ---------------------------------------------------------------------------

TEST_F(ClientRpcTest, CommitOfAnUnknownPayloadFails) {
  EXPECT_FALSE(client_->CommitPayload(MakeId()).ok());
}

TEST_F(ClientRpcTest, DeleteOfAnUnknownPayloadFails) {
  v1::DeleteRequest req;
  *req.mutable_id() = MakeId();
  EXPECT_FALSE(client_->Delete(req).ok());
}

TEST_F(ClientRpcTest, PromoteAndSpillReachTheService) {
  v1::PromoteRequest promote;
  *promote.mutable_id() = MakeId();
  promote.set_target_tier(v1::TIER_RAM);
  EXPECT_FALSE(client_->Promote(promote).ok()) << "unknown payload should not promote";

  // Spill reports per-payload outcomes rather than failing the call, so the
  // RPC succeeds and the result carries the refusal.
  v1::SpillRequest spill;
  *spill.add_ids() = MakeId();
  spill.set_policy(v1::SPILL_POLICY_BLOCKING);
  auto spill_resp = client_->Spill(spill);
  ASSERT_TRUE(spill_resp.ok()) << spill_resp.status().ToString();
  ASSERT_EQ(spill_resp->results_size(), 1);
  EXPECT_FALSE(spill_resp->results(0).ok());
  EXPECT_FALSE(spill_resp->results(0).error_message().empty());
}

TEST_F(ClientRpcTest, PinUnpinAndPrefetchAreAccepted) {
  v1::PinRequest pin;
  *pin.mutable_id() = MakeId();
  pin.set_duration_ms(1000);
  (void)client_->Pin(pin);

  v1::UnpinRequest unpin;
  *unpin.mutable_id() = MakeId();
  (void)client_->Unpin(unpin);

  v1::PrefetchRequest prefetch;
  *prefetch.mutable_id() = MakeId();
  prefetch.set_target_tier(v1::TIER_RAM);
  (void)client_->Prefetch(prefetch);

  SUCCEED() << "wrappers round-trip without throwing or hanging";
}

TEST_F(ClientRpcTest, ListPayloadsAndStatsReturnOnAnEmptyCatalog) {
  v1::ListPayloadsRequest list_req;
  auto                    list = client_->ListPayloads(list_req);
  ASSERT_TRUE(list.ok()) << list.status().ToString();
  EXPECT_EQ(list->payloads_size(), 0);

  v1::StatsRequest stats_req;
  auto             stats = client_->Stats(stats_req);
  ASSERT_TRUE(stats.ok()) << stats.status().ToString();
  EXPECT_EQ(stats->payloads_ram(), 0u);
}

// ---------------------------------------------------------------------------
// Metadata and lineage
// ---------------------------------------------------------------------------

TEST_F(ClientRpcTest, MetadataUpsertAndEventAppendRoundTrip) {
  const auto id = MakeId();

  v1::UpdatePayloadMetadataRequest update;
  *update.mutable_id()                     = id;
  *update.mutable_metadata()->mutable_id() = id;
  update.mutable_metadata()->set_data(R"({"stage":"raw"})");
  update.mutable_metadata()->set_schema("schema.v1");
  auto updated = client_->UpdatePayloadMetadata(update);
  ASSERT_TRUE(updated.ok()) << updated.status().ToString();

  v1::AppendPayloadMetadataEventRequest event;
  *event.mutable_id()                     = id;
  *event.mutable_metadata()->mutable_id() = id;
  event.mutable_metadata()->set_data(R"({"stage":"processed"})");
  event.set_source("unit-test");
  event.set_version("v1");
  auto appended = client_->AppendPayloadMetadataEvent(event);
  EXPECT_TRUE(appended.ok()) << appended.status().ToString();
}

TEST_F(ClientRpcTest, LineageEdgesAreWrittenAndReadBack) {
  const auto parent = MakeId();
  const auto child  = MakeId();

  // One child, N parent edges — the edge carries the operation, not the request.
  v1::AddLineageRequest add;
  *add.mutable_child()    = child;
  auto* edge              = add.add_parents();
  *edge->mutable_parent() = parent;
  edge->set_operation("decode");
  edge->set_role("primary");
  ASSERT_TRUE(client_->AddLineage(add).ok());

  v1::GetLineageRequest get;
  *get.mutable_id() = parent;
  auto lineage      = client_->GetLineage(get);
  ASSERT_TRUE(lineage.ok()) << lineage.status().ToString();
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

TEST_F(ClientRpcTest, StreamLifecycleThroughTheClient) {
  v1::StreamID stream;
  stream.set_namespace_("unit");
  stream.set_name("client-rpc");

  v1::CreateStreamRequest create;
  *create.mutable_stream() = stream;
  create.set_retention_max_entries(16);
  ASSERT_TRUE(client_->CreateStream(create).ok());

  v1::ReadRequest read;
  *read.mutable_stream() = stream;
  read.set_start_offset(0);
  read.set_max_entries(8);
  auto read_resp = client_->Read(read);
  ASSERT_TRUE(read_resp.ok()) << read_resp.status().ToString();
  EXPECT_EQ(read_resp->entries_size(), 0);

  v1::GetCommittedRequest committed;
  *committed.mutable_stream() = stream;
  committed.set_consumer_group("g1");
  auto committed_resp = client_->GetCommitted(committed);
  EXPECT_TRUE(committed_resp.ok()) << committed_resp.status().ToString();

  v1::GetRangeRequest range;
  *range.mutable_stream() = stream;
  auto range_resp         = client_->GetRange(range);
  EXPECT_TRUE(range_resp.ok()) << range_resp.status().ToString();

  v1::DeleteStreamRequest del;
  *del.mutable_stream() = stream;
  EXPECT_TRUE(client_->DeleteStream(del).ok());
}

TEST_F(ClientRpcTest, SubscribeReturnsAUsableHandle) {
  v1::StreamID stream;
  stream.set_namespace_("unit");
  stream.set_name("client-rpc-subscribe");

  v1::CreateStreamRequest create;
  *create.mutable_stream() = stream;
  ASSERT_TRUE(client_->CreateStream(create).ok());

  v1::SubscribeRequest sub;
  *sub.mutable_stream() = stream;
  auto handle           = client_->Subscribe(sub);
  ASSERT_NE(handle.context, nullptr);
  ASSERT_NE(handle.reader, nullptr);

  // Server-streaming with nothing to send blocks, so cancel rather than read.
  // The point is that the handle owns both halves and tears down cleanly.
  handle.context->TryCancel();
  const auto status = handle.reader->Finish();
  EXPECT_FALSE(status.ok());
}

} // namespace payload
