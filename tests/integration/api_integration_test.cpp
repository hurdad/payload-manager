/*
  API integration test: exercises the full payload-manager gRPC surface against
  a live server via PayloadClient.

  Set PAYLOAD_MANAGER_ENDPOINT to override the default "localhost:50051".

  Covered:
    1. RAM round-trip     – allocate, write via shm, commit, read via shm, verify bytes, delete
    2. Spill + promote    – RAM → disk → RAM, data survives both transitions
    3. Pin / unpin        – pinned payload resists spill; spill succeeds after unpin
    4. Metadata           – upsert, replace, append event
    5. Lineage            – add edges, traverse graph
    6. Stats              – RAM payload count tracks allocate/delete
    7. Stream             – create, append, read, subscribe, commit offset,
                           get committed, get range, cascade delete
*/

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "client/cpp/payload_manager_client.h"
#include "payload/manager/v1.hpp"

using namespace payload::manager::v1;
using payload::manager::client::PayloadClient;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

#define ASSERT_OK(expr)                                                                                 \
  do {                                                                                                  \
    auto _s = (expr);                                                                                   \
    if (!_s.ok()) {                                                                                     \
      std::cerr << "FAIL [" #expr "]: " << _s.ToString() << "\n  at " __FILE__ ":" << __LINE__ << '\n'; \
      std::exit(1);                                                                                     \
    }                                                                                                   \
  } while (0)

#define ASSERT_TRUE(cond)                                                               \
  do {                                                                                  \
    if (!(cond)) {                                                                      \
      std::cerr << "FAIL assertion: " #cond "\n  at " __FILE__ ":" << __LINE__ << '\n'; \
      std::exit(1);                                                                     \
    }                                                                                   \
  } while (0)

#define ASSERT_EQ(a, b)                                                                                            \
  do {                                                                                                             \
    auto _a = (a);                                                                                                 \
    auto _b = (b);                                                                                                 \
    if (_a != _b) {                                                                                                \
      std::cerr << "FAIL: " #a " (" << _a << ") != " #b " (" << _b << ")\n  at " __FILE__ ":" << __LINE__ << '\n'; \
      std::exit(1);                                                                                                \
    }                                                                                                              \
  } while (0)

void log(const char* test) {
  std::cout << "  [run] " << test << '\n';
}

// Allocate, write a fill pattern, commit; returns the descriptor.
PayloadDescriptor AllocateAndCommit(const PayloadClient& client, uint64_t size_bytes, uint8_t fill) {
  auto wr = client.AllocateWritableBuffer(size_bytes, TIER_RAM);
  ASSERT_OK(wr.status());
  auto wp = wr.ValueOrDie();
  ASSERT_TRUE(wp.buffer != nullptr);
  ASSERT_EQ(static_cast<uint64_t>(wp.buffer->size()), size_bytes);
  std::memset(wp.buffer->mutable_data(), fill, static_cast<size_t>(size_bytes));

  ASSERT_OK(client.CommitPayload(wp.descriptor.payload_id()));
  return wp.descriptor;
}

void DeletePayload(const PayloadClient& client, const PayloadID& id, bool force = false) {
  DeleteRequest req;
  *req.mutable_id() = id;
  req.set_force(force);
  ASSERT_OK(client.Delete(req));
}

StreamID MakeStreamId(const std::string& name) {
  StreamID s;
  s.set_namespace_("api-integration");
  s.set_name(name);
  return s;
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

void TestRamRoundTrip(const PayloadClient& client) {
  log("RAM round-trip: allocate → write → commit → read → verify → delete");

  constexpr uint64_t kSize = 256;
  constexpr uint8_t  kFill = 0xAB;

  auto desc = AllocateAndCommit(client, kSize, kFill);

  auto rd = client.AcquireReadableBuffer(desc.payload_id());
  ASSERT_OK(rd.status());
  auto rp = rd.ValueOrDie();
  ASSERT_EQ(static_cast<uint64_t>(rp.buffer->size()), kSize);
  for (uint64_t i = 0; i < kSize; ++i) {
    ASSERT_EQ(rp.buffer->data()[i], kFill);
  }
  ASSERT_OK(client.Release(rp.lease_id));

  DeletePayload(client, desc.payload_id());
}

void TestSpillAndPromote(const PayloadClient& client) {
  log("Spill + Promote: RAM → disk → RAM, data survives");

  constexpr uint64_t kSize = 128;
  constexpr uint8_t  kFill = 0x7E;

  auto        desc = AllocateAndCommit(client, kSize, kFill);
  const auto& id   = desc.payload_id();

  // Spill to disk
  SpillRequest spill_req;
  *spill_req.add_ids() = id;
  spill_req.set_fsync(false);
  auto spill_resp = client.Spill(spill_req);
  ASSERT_OK(spill_resp.status());
  ASSERT_EQ(spill_resp->results_size(), 1);
  if (!spill_resp->results(0).ok()) {
    std::cerr << "  spill error: " << spill_resp->results(0).error_message() << '\n';
  }
  ASSERT_TRUE(spill_resp->results(0).ok());

  // Promote back to RAM (explicit)
  PromoteRequest prom_req;
  *prom_req.mutable_id() = id;
  prom_req.set_target_tier(TIER_RAM);
  auto prom_resp = client.Promote(prom_req);
  ASSERT_OK(prom_resp.status());
  ASSERT_EQ(prom_resp->payload_descriptor().tier(), TIER_RAM);

  // Verify data survived the round-trip through disk
  auto rd = client.AcquireReadableBuffer(id);
  ASSERT_OK(rd.status());
  auto rp = rd.ValueOrDie();
  ASSERT_EQ(static_cast<uint64_t>(rp.buffer->size()), kSize);
  for (uint64_t i = 0; i < kSize; ++i) {
    ASSERT_EQ(rp.buffer->data()[i], kFill);
  }
  ASSERT_OK(client.Release(rp.lease_id));

  DeletePayload(client, id);
}

void TestPinBlocksSpill(const PayloadClient& client) {
  log("Pin/Unpin: pinned payload resists spill; spill succeeds after unpin");

  auto        desc = AllocateAndCommit(client, 64, 0x11);
  const auto& id   = desc.payload_id();

  // Pin
  PinRequest pin_req;
  *pin_req.mutable_id() = id;
  ASSERT_OK(client.Pin(pin_req));

  // Spill should fail or be skipped for a pinned payload
  SpillRequest spill_req;
  *spill_req.add_ids() = id;
  spill_req.set_fsync(false);
  auto spill_resp = client.Spill(spill_req);
  ASSERT_OK(spill_resp.status());
  ASSERT_EQ(spill_resp->results_size(), 1);
  ASSERT_TRUE(!spill_resp->results(0).ok()); // must be blocked

  // Unpin, then spill should succeed
  UnpinRequest unpin_req;
  *unpin_req.mutable_id() = id;
  ASSERT_OK(client.Unpin(unpin_req));

  auto spill_resp2 = client.Spill(spill_req);
  ASSERT_OK(spill_resp2.status());
  ASSERT_EQ(spill_resp2->results_size(), 1);
  if (!spill_resp2->results(0).ok()) {
    std::cerr << "  spill after unpin error: " << spill_resp2->results(0).error_message() << '\n';
  }
  ASSERT_TRUE(spill_resp2->results(0).ok());

  DeletePayload(client, id);
}

void TestMetadata(const PayloadClient& client) {
  log("Metadata: upsert, replace, append event");

  auto        desc = AllocateAndCommit(client, 32, 0x00);
  const auto& id   = desc.payload_id();

  // Initial upsert
  UpdatePayloadMetadataRequest upsert_req;
  *upsert_req.mutable_id() = id;
  upsert_req.mutable_metadata()->set_data(R"({"stage":"raw"})");
  upsert_req.mutable_metadata()->set_schema("v1");
  upsert_req.set_mode(METADATA_UPDATE_MODE_MERGE);
  auto upsert_resp = client.UpdatePayloadMetadata(upsert_req);
  ASSERT_OK(upsert_resp.status());
  ASSERT_EQ(upsert_resp->metadata().data(), R"({"stage":"raw"})");

  // Replace
  UpdatePayloadMetadataRequest replace_req;
  *replace_req.mutable_id() = id;
  replace_req.mutable_metadata()->set_data(R"({"stage":"processed","version":2})");
  replace_req.mutable_metadata()->set_schema("v2");
  replace_req.set_mode(METADATA_UPDATE_MODE_REPLACE);
  auto replace_resp = client.UpdatePayloadMetadata(replace_req);
  ASSERT_OK(replace_resp.status());
  ASSERT_EQ(replace_resp->metadata().schema(), "v2");

  // Append an immutable event
  AppendPayloadMetadataEventRequest event_req;
  *event_req.mutable_id() = id;
  event_req.mutable_metadata()->set_data(R"({"action":"validated"})");
  event_req.mutable_metadata()->set_schema("event.v1");
  event_req.set_source("api-integration-test");
  auto event_resp = client.AppendPayloadMetadataEvent(event_req);
  ASSERT_OK(event_resp.status());

  DeletePayload(client, id);
}

void TestLineage(const PayloadClient& client) {
  log("Lineage: two payloads → add edge → traverse graph");

  auto        parent_desc = AllocateAndCommit(client, 32, 0x01);
  auto        child_desc  = AllocateAndCommit(client, 32, 0x02);
  const auto& parent_id   = parent_desc.payload_id();
  const auto& child_id    = child_desc.payload_id();

  AddLineageRequest add_req;
  *add_req.mutable_child() = child_id;
  auto* edge               = add_req.add_parents();
  *edge->mutable_parent()  = parent_id;
  edge->set_operation("transform");
  edge->set_role("source");
  ASSERT_OK(client.AddLineage(add_req));

  // Query upstream lineage from the child — should return the parent edge
  GetLineageRequest get_req;
  *get_req.mutable_id() = child_id;
  get_req.set_upstream(true);
  get_req.set_max_depth(1);
  auto get_resp = client.GetLineage(get_req);
  ASSERT_OK(get_resp.status());
  ASSERT_TRUE(get_resp->edges_size() >= 1);

  bool found = false;
  for (const auto& e : get_resp->edges()) {
    if (e.operation() == "transform") {
      found = true;
    }
  }
  ASSERT_TRUE(found);

  DeletePayload(client, child_id);
  DeletePayload(client, parent_id);
}

void TestStats(const PayloadClient& client) {
  log("Stats: RAM payload count tracks allocate/delete");

  auto before = client.Stats(StatsRequest{});
  ASSERT_OK(before.status());
  uint64_t ram_before = before->payloads_ram();

  auto desc = AllocateAndCommit(client, 64, 0xFF);

  auto during = client.Stats(StatsRequest{});
  ASSERT_OK(during.status());
  uint64_t ram_during = during->payloads_ram();
  ASSERT_TRUE(ram_during > ram_before);

  DeletePayload(client, desc.payload_id());

  auto after = client.Stats(StatsRequest{});
  ASSERT_OK(after.status());
  uint64_t ram_after = after->payloads_ram();
  ASSERT_EQ(ram_after, ram_before);
}

void TestStream(const PayloadClient& client) {
  log("Stream: create → append → read → subscribe → commit → get committed → range → delete cascade");

  const auto stream = MakeStreamId("api-integration-stream");

  // Ensure clean start
  {
    DeleteStreamRequest pre_del;
    *pre_del.mutable_stream() = stream;
    client.DeleteStream(pre_del); // ignore error
  }

  // Create
  CreateStreamRequest create_req;
  *create_req.mutable_stream() = stream;
  create_req.set_retention_max_entries(1000);
  ASSERT_OK(client.CreateStream(create_req));

  // Allocate two payloads to reference from stream entries
  auto p0 = AllocateAndCommit(client, 8, 0xA0);
  auto p1 = AllocateAndCommit(client, 8, 0xB0);

  // Append two entries
  AppendRequest append_req;
  *append_req.mutable_stream() = stream;
  {
    auto* item                  = append_req.add_items();
    *item->mutable_payload_id() = p0.payload_id();
    item->set_duration_ns(1000);
    (*item->mutable_tags())["source"] = "api-integration";
  }
  {
    auto* item                  = append_req.add_items();
    *item->mutable_payload_id() = p1.payload_id();
    item->set_duration_ns(2000);
  }

  auto append_resp = client.Append(append_req);
  ASSERT_OK(append_resp.status());
  int64_t first_offset = static_cast<int64_t>(append_resp->first_offset());
  int64_t last_offset  = static_cast<int64_t>(append_resp->last_offset());
  ASSERT_EQ(last_offset - first_offset, 1);

  // Read all entries
  ReadRequest read_req;
  *read_req.mutable_stream() = stream;
  read_req.set_start_offset(first_offset);
  read_req.set_max_entries(10);
  auto read_resp = client.Read(read_req);
  ASSERT_OK(read_resp.status());
  ASSERT_EQ(read_resp->entries_size(), 2);
  ASSERT_EQ(read_resp->entries(0).offset(), first_offset);

  // Subscribe — read one entry then cancel
  grpc::ClientContext sub_ctx;
  SubscribeRequest    sub_req;
  *sub_req.mutable_stream() = stream;
  sub_req.set_offset(first_offset);
  sub_req.set_max_inflight(1);
  auto              reader = client.Subscribe(sub_req, &sub_ctx);
  SubscribeResponse sub_resp;
  bool              got = reader->Read(&sub_resp);
  ASSERT_TRUE(got);
  ASSERT_EQ(sub_resp.entry().offset(), first_offset);
  sub_ctx.TryCancel();
  auto sub_status = reader->Finish();
  ASSERT_TRUE(sub_status.ok() || sub_status.error_code() == grpc::StatusCode::CANCELLED);

  // Commit consumer offset
  CommitRequest commit_req;
  *commit_req.mutable_stream() = stream;
  commit_req.set_consumer_group("test-group");
  commit_req.set_offset(last_offset);
  ASSERT_OK(client.Commit(commit_req));

  // Get committed
  GetCommittedRequest gc_req;
  *gc_req.mutable_stream() = stream;
  gc_req.set_consumer_group("test-group");
  auto gc_resp = client.GetCommitted(gc_req);
  ASSERT_OK(gc_resp.status());
  ASSERT_EQ(gc_resp->offset(), last_offset);

  // GetRange
  GetRangeRequest range_req;
  *range_req.mutable_stream() = stream;
  range_req.set_start_offset(first_offset);
  range_req.set_end_offset(last_offset);
  auto range_resp = client.GetRange(range_req);
  ASSERT_OK(range_resp.status());
  ASSERT_EQ(range_resp->entries_size(), 2);

  // Delete stream — entries and consumer offsets must cascade
  DeleteStreamRequest del_req;
  *del_req.mutable_stream() = stream;
  ASSERT_OK(client.DeleteStream(del_req));

  // Payloads survive stream deletion
  DeletePayload(client, p0.payload_id());
  DeletePayload(client, p1.payload_id());
}

} // namespace

int main(int argc, char** argv) {
  const char*       env_endpoint = std::getenv("PAYLOAD_MANAGER_ENDPOINT");
  const std::string endpoint     = (argc > 1) ? argv[1] : (env_endpoint ? env_endpoint : "");
  if (endpoint.empty()) {
    std::cout << "api_integration_test: skip (set PAYLOAD_MANAGER_ENDPOINT to run)\n";
    return 0;
  }

  std::cout << "api_integration_test: connecting to " << endpoint << '\n';

  PayloadClient client(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));

  TestRamRoundTrip(client);
  TestSpillAndPromote(client);
  TestPinBlocksSpill(client);
  TestMetadata(client);
  TestLineage(client);
  TestStats(client);
  TestStream(client);

  std::cout << "api_integration_test: pass\n";
  return 0;
}
