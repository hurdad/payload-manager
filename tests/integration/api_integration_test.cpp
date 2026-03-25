/*
  API integration test: exercises the full payload-manager gRPC surface against
  a live server via PayloadClient.

  Set PAYLOAD_MANAGER_ENDPOINT to override the default "localhost:50051".
  Set OTLP_ENDPOINT to export test root spans (default: localhost:4317).
    Use empty string to disable span export entirely.

  Each test function:
    1. Starts a root OTel span (exported to Tempo via Alloy)
    2. Builds the W3C traceparent from the SDK-generated trace/span IDs
    3. Injects that traceparent into every gRPC call so server child spans
       appear under the same trace in Tempo

  Covered:
    1. RAM round-trip     – allocate, write via shm, commit, read via shm, verify bytes, delete
    2. Disk round-trip    – allocate, write via mmap file, commit, read back, verify bytes, delete
    3. GPU round-trip     – allocate, commit, verify descriptor, read lease, delete
                           (compiled in only when PAYLOAD_CLIENT_ARROW_CUDA=1)
    4. Spill + promote    – RAM → disk → RAM, data survives both transitions
    5. Pin / unpin        – pinned payload resists spill; spill succeeds after unpin
    6. Metadata           – upsert, replace, append event
    7. Lineage            – add edges, traverse graph
    8. Stats              – RAM payload count tracks allocate/delete
    9. ListPayloads       – unfiltered list grows on allocate; tier filter
                           returns only RAM entries; deleted payloads vanish
   10. Stream             – create, append, read, subscribe, commit offset,
                           get committed, get range, cascade delete
*/

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/interceptor.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "client/cpp/payload_manager_client.h"
#include "otel_tracer.hpp"
#include "payload/manager/v1.hpp"

using namespace payload::manager::v1;
using payload::manager::client::PayloadClient;

// ---------------------------------------------------------------------------
// W3C traceparent helpers + gRPC channel interceptor
// ---------------------------------------------------------------------------

namespace {

std::string MakeTraceparent(const OtelSpanContext& ctx) {
  return "00-" + ctx.trace_id_hex + "-" + ctx.span_id_hex + "-01";
}

// Injects a fixed W3C traceparent into the initial metadata of every RPC.
class TraceInjectInterceptor : public grpc::experimental::Interceptor {
 public:
  explicit TraceInjectInterceptor(std::string tp) : tp_(std::move(tp)) {
  }

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override {
    if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      methods->GetSendInitialMetadata()->emplace("traceparent", tp_);
    }
    methods->Proceed();
  }

 private:
  std::string tp_;
};

class TraceInjectFactory : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  explicit TraceInjectFactory(std::string tp) : tp_(std::move(tp)) {
  }

  grpc::experimental::Interceptor* CreateClientInterceptor(grpc::experimental::ClientRpcInfo*) override {
    return new TraceInjectInterceptor(tp_);
  }

 private:
  std::string tp_;
};

std::shared_ptr<grpc::Channel> MakeTracedChannel(const std::string& endpoint, const std::string& traceparent) {
  grpc::ChannelArguments                                                              args;
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> factories;
  factories.push_back(std::make_unique<TraceInjectFactory>(traceparent));
  return grpc::experimental::CreateCustomChannelWithInterceptors(endpoint, grpc::InsecureChannelCredentials(), args, std::move(factories));
}

} // namespace

// ---------------------------------------------------------------------------
// Test helpers
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

void log(const char* test, const std::string& traceparent) {
  std::cout << "  [run] " << test << "  trace=" << (traceparent.size() > 3 ? traceparent.substr(3, 32) : "(no-otel)") << '\n';
}

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

void TestDiskRoundTrip(const PayloadClient& client, const std::string& tp) {
  log("Disk round-trip: allocate → write via mmap → commit → read → verify → delete", tp);

  constexpr uint64_t kSize = 256;
  constexpr uint8_t  kFill = 0xCD;

  auto wr = client.AllocateWritableBuffer(kSize, TIER_DISK);
  ASSERT_OK(wr.status());
  auto wp = wr.ValueOrDie();
  ASSERT_TRUE(wp.buffer != nullptr);
  ASSERT_EQ(static_cast<uint64_t>(wp.buffer->size()), kSize);
  ASSERT_TRUE(wp.descriptor.has_disk());
  ASSERT_EQ(wp.descriptor.tier(), TIER_DISK);
  std::memset(wp.buffer->mutable_data(), kFill, static_cast<size_t>(kSize));

  ASSERT_OK(client.CommitPayload(wp.descriptor.payload_id()));

  auto rd = client.AcquireReadableBuffer(wp.descriptor.payload_id(), TIER_DISK);
  ASSERT_OK(rd.status());
  auto rp = rd.ValueOrDie();
  ASSERT_EQ(static_cast<uint64_t>(rp.buffer->size()), kSize);
  for (uint64_t i = 0; i < kSize; ++i) ASSERT_EQ(rp.buffer->data()[i], kFill);
  ASSERT_OK(client.Release(rp.lease_id));

  DeletePayload(client, wp.descriptor.payload_id());
}

#if PAYLOAD_CLIENT_ARROW_CUDA
void TestGpuRoundTrip(const PayloadClient& client, const std::string& tp) {
  log("GPU round-trip: allocate → commit → verify descriptor → read lease → delete", tp);

  constexpr uint64_t kSize = 256;

  auto wr = client.AllocateWritableBuffer(kSize, TIER_GPU);
  if (!wr.ok()) {
    // GPU tier not configured on this server; skip gracefully.
    std::cout << "  [skip] GPU not available: " << wr.status().ToString() << '\n';
    return;
  }
  auto wp = wr.ValueOrDie();
  ASSERT_TRUE(wp.buffer != nullptr);
  ASSERT_EQ(static_cast<uint64_t>(wp.buffer->size()), kSize);
  ASSERT_TRUE(wp.descriptor.has_gpu());
  ASSERT_EQ(wp.descriptor.tier(), TIER_GPU);
  // GPU buffer is device memory — host-side data write requires CUDA APIs.
  // Lifecycle correctness (alloc → commit → lease → release → delete) is
  // validated here; data-plane verification belongs in CUDA-specific tests.
  ASSERT_OK(client.CommitPayload(wp.descriptor.payload_id()));

  auto rd = client.AcquireReadableBuffer(wp.descriptor.payload_id());
  ASSERT_OK(rd.status());
  auto rp = rd.ValueOrDie();
  ASSERT_EQ(static_cast<uint64_t>(rp.buffer->size()), kSize);
  ASSERT_OK(client.Release(rp.lease_id));

  DeletePayload(client, wp.descriptor.payload_id());
}
#endif // PAYLOAD_CLIENT_ARROW_CUDA

void TestRamRoundTrip(const PayloadClient& client, const std::string& tp) {
  log("RAM round-trip: allocate → write → commit → read → verify → delete", tp);

  constexpr uint64_t kSize = 256;
  constexpr uint8_t  kFill = 0xAB;

  auto desc = AllocateAndCommit(client, kSize, kFill);

  auto rd = client.AcquireReadableBuffer(desc.payload_id());
  ASSERT_OK(rd.status());
  auto rp = rd.ValueOrDie();
  ASSERT_EQ(static_cast<uint64_t>(rp.buffer->size()), kSize);
  for (uint64_t i = 0; i < kSize; ++i) ASSERT_EQ(rp.buffer->data()[i], kFill);
  ASSERT_OK(client.Release(rp.lease_id));

  DeletePayload(client, desc.payload_id());
}

void TestSpillAndPromote(const PayloadClient& client, const std::string& tp) {
  log("Spill + Promote: RAM → disk → RAM, data survives", tp);

  constexpr uint64_t kSize = 128;
  constexpr uint8_t  kFill = 0x7E;

  auto        desc = AllocateAndCommit(client, kSize, kFill);
  const auto& id   = desc.payload_id();

  SpillRequest spill_req;
  *spill_req.add_ids() = id;
  spill_req.set_fsync(false);
  auto spill_resp = client.Spill(spill_req);
  ASSERT_OK(spill_resp.status());
  ASSERT_EQ(spill_resp->results_size(), 1);
  ASSERT_TRUE(spill_resp->results(0).ok());

  PromoteRequest prom_req;
  *prom_req.mutable_id() = id;
  prom_req.set_target_tier(TIER_RAM);
  auto prom_resp = client.Promote(prom_req);
  ASSERT_OK(prom_resp.status());
  ASSERT_EQ(prom_resp->payload_descriptor().tier(), TIER_RAM);

  auto rd = client.AcquireReadableBuffer(id);
  ASSERT_OK(rd.status());
  auto rp = rd.ValueOrDie();
  ASSERT_EQ(static_cast<uint64_t>(rp.buffer->size()), kSize);
  for (uint64_t i = 0; i < kSize; ++i) ASSERT_EQ(rp.buffer->data()[i], kFill);
  ASSERT_OK(client.Release(rp.lease_id));

  DeletePayload(client, id);
}

void TestPinBlocksSpill(const PayloadClient& client, const std::string& tp) {
  log("Pin/Unpin: pinned payload resists spill; spill succeeds after unpin", tp);

  auto        desc = AllocateAndCommit(client, 64, 0x11);
  const auto& id   = desc.payload_id();

  PinRequest pin_req;
  *pin_req.mutable_id() = id;
  ASSERT_OK(client.Pin(pin_req));

  SpillRequest spill_req;
  *spill_req.add_ids() = id;
  spill_req.set_fsync(false);
  auto spill_resp = client.Spill(spill_req);
  ASSERT_OK(spill_resp.status());
  ASSERT_EQ(spill_resp->results_size(), 1);
  ASSERT_TRUE(!spill_resp->results(0).ok()); // must be blocked

  UnpinRequest unpin_req;
  *unpin_req.mutable_id() = id;
  ASSERT_OK(client.Unpin(unpin_req));

  auto spill_resp2 = client.Spill(spill_req);
  ASSERT_OK(spill_resp2.status());
  ASSERT_EQ(spill_resp2->results_size(), 1);
  ASSERT_TRUE(spill_resp2->results(0).ok());

  DeletePayload(client, id);
}

void TestMetadata(const PayloadClient& client, const std::string& tp) {
  log("Metadata: upsert, replace, append event", tp);

  auto        desc = AllocateAndCommit(client, 32, 0x00);
  const auto& id   = desc.payload_id();

  UpdatePayloadMetadataRequest upsert_req;
  *upsert_req.mutable_id() = id;
  upsert_req.mutable_metadata()->set_data(R"({"stage":"raw"})");
  upsert_req.mutable_metadata()->set_schema("v1");
  upsert_req.set_mode(METADATA_UPDATE_MODE_MERGE);
  auto upsert_resp = client.UpdatePayloadMetadata(upsert_req);
  ASSERT_OK(upsert_resp.status());
  ASSERT_EQ(upsert_resp->metadata().data(), R"({"stage":"raw"})");

  UpdatePayloadMetadataRequest replace_req;
  *replace_req.mutable_id() = id;
  replace_req.mutable_metadata()->set_data(R"({"stage":"processed","version":2})");
  replace_req.mutable_metadata()->set_schema("v2");
  replace_req.set_mode(METADATA_UPDATE_MODE_REPLACE);
  auto replace_resp = client.UpdatePayloadMetadata(replace_req);
  ASSERT_OK(replace_resp.status());
  ASSERT_EQ(replace_resp->metadata().schema(), "v2");

  AppendPayloadMetadataEventRequest event_req;
  *event_req.mutable_id() = id;
  event_req.mutable_metadata()->set_data(R"({"action":"validated"})");
  event_req.mutable_metadata()->set_schema("event.v1");
  event_req.set_source("api-integration-test");
  ASSERT_OK(client.AppendPayloadMetadataEvent(event_req).status());

  DeletePayload(client, id);
}

void TestLineage(const PayloadClient& client, const std::string& tp) {
  log("Lineage: two payloads → add edge → traverse graph", tp);

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

  GetLineageRequest get_req;
  *get_req.mutable_id() = child_id;
  get_req.set_upstream(true);
  get_req.set_max_depth(1);
  auto get_resp = client.GetLineage(get_req);
  ASSERT_OK(get_resp.status());
  ASSERT_TRUE(get_resp->edges_size() >= 1);

  bool found = false;
  for (const auto& e : get_resp->edges()) {
    if (e.operation() == "transform") found = true;
  }
  ASSERT_TRUE(found);

  DeletePayload(client, child_id);
  DeletePayload(client, parent_id);
}

void TestStats(const PayloadClient& client, const std::string& tp) {
  log("Stats: RAM payload count tracks allocate/delete", tp);

  auto before = client.Stats(StatsRequest{});
  ASSERT_OK(before.status());
  uint64_t ram_before = before->payloads_ram();

  auto desc = AllocateAndCommit(client, 64, 0xFF);

  auto during = client.Stats(StatsRequest{});
  ASSERT_OK(during.status());
  ASSERT_TRUE(during->payloads_ram() > ram_before);

  DeletePayload(client, desc.payload_id());

  auto after = client.Stats(StatsRequest{});
  ASSERT_OK(after.status());
  ASSERT_EQ(after->payloads_ram(), ram_before);
}

void TestListPayloads(const PayloadClient& client, const std::string& tp) {
  log("ListPayloads: unfiltered list grows on allocate; tier filter returns only matching tier", tp);

  // Snapshot counts before we touch anything.
  ListPayloadsRequest all_req;
  auto                before = client.ListPayloads(all_req);
  ASSERT_OK(before.status());
  size_t count_before = static_cast<size_t>(before->payloads_size());

  // Allocate two RAM payloads.
  auto ram0 = AllocateAndCommit(client, 64, 0xAA);
  auto ram1 = AllocateAndCommit(client, 128, 0xBB);

  // Unfiltered list must now contain at least 2 more entries.
  auto after = client.ListPayloads(all_req);
  ASSERT_OK(after.status());
  ASSERT_TRUE(static_cast<size_t>(after->payloads_size()) >= count_before + 2);

  // Both IDs must appear.
  auto find_id = [&](const ListPayloadsResponse& resp, const PayloadID& id) {
    for (const auto& s : resp.payloads()) {
      if (s.id().value() == id.value()) return true;
    }
    return false;
  };
  ASSERT_TRUE(find_id(*after, ram0.payload_id()));
  ASSERT_TRUE(find_id(*after, ram1.payload_id()));

  // Summary fields must be plausible.
  for (const auto& s : after->payloads()) {
    if (s.id().value() != ram0.payload_id().value() && s.id().value() != ram1.payload_id().value()) continue;
    ASSERT_TRUE(s.size_bytes() > 0);
    // state must be active (committed payloads are PAYLOAD_STATE_ACTIVE)
    ASSERT_EQ(static_cast<int>(s.state()), static_cast<int>(PAYLOAD_STATE_ACTIVE));
  }

  // Tier-filtered list: TIER_RAM must include both payloads.
  ListPayloadsRequest ram_req;
  ram_req.set_tier_filter(TIER_RAM);
  auto ram_resp = client.ListPayloads(ram_req);
  ASSERT_OK(ram_resp.status());
  ASSERT_TRUE(find_id(*ram_resp, ram0.payload_id()));
  ASSERT_TRUE(find_id(*ram_resp, ram1.payload_id()));

  // Every entry in a tier-filtered response must match that tier.
  for (const auto& s : ram_resp->payloads()) {
    ASSERT_EQ(static_cast<int>(s.tier()), static_cast<int>(TIER_RAM));
  }

  // Delete and verify payloads are no longer listed.
  DeletePayload(client, ram0.payload_id());
  DeletePayload(client, ram1.payload_id());

  auto gone = client.ListPayloads(all_req);
  ASSERT_OK(gone.status());
  ASSERT_TRUE(!find_id(*gone, ram0.payload_id()));
  ASSERT_TRUE(!find_id(*gone, ram1.payload_id()));
}

void TestStream(const PayloadClient& client, const std::string& tp) {
  log("Stream: create → append → read → subscribe → commit → get committed → range → delete cascade", tp);

  const auto stream = MakeStreamId("api-integration-stream");

  {
    DeleteStreamRequest pre_del;
    *pre_del.mutable_stream() = stream;
    (void)client.DeleteStream(pre_del);
  }

  CreateStreamRequest create_req;
  *create_req.mutable_stream() = stream;
  create_req.set_retention_max_entries(1000);
  ASSERT_OK(client.CreateStream(create_req));

  auto p0 = AllocateAndCommit(client, 8, 0xA0);
  auto p1 = AllocateAndCommit(client, 8, 0xB0);

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

  ReadRequest read_req;
  *read_req.mutable_stream() = stream;
  read_req.set_start_offset(first_offset);
  read_req.set_max_entries(10);
  auto read_resp = client.Read(read_req);
  ASSERT_OK(read_resp.status());
  ASSERT_EQ(read_resp->entries_size(), 2);
  ASSERT_EQ(read_resp->entries(0).offset(), first_offset);

  grpc::ClientContext sub_ctx;
  SubscribeRequest    sub_req;
  *sub_req.mutable_stream() = stream;
  sub_req.set_offset(first_offset);
  sub_req.set_max_inflight(1);
  auto              reader = client.Subscribe(sub_req, &sub_ctx);
  SubscribeResponse sub_resp;
  ASSERT_TRUE(reader->Read(&sub_resp));
  ASSERT_EQ(sub_resp.entry().offset(), first_offset);
  sub_ctx.TryCancel();
  auto sub_status = reader->Finish();
  ASSERT_TRUE(sub_status.ok() || sub_status.error_code() == grpc::StatusCode::CANCELLED);

  CommitRequest commit_req;
  *commit_req.mutable_stream() = stream;
  commit_req.set_consumer_group("test-group");
  commit_req.set_offset(last_offset);
  ASSERT_OK(client.Commit(commit_req));

  GetCommittedRequest gc_req;
  *gc_req.mutable_stream() = stream;
  gc_req.set_consumer_group("test-group");
  auto gc_resp = client.GetCommitted(gc_req);
  ASSERT_OK(gc_resp.status());
  ASSERT_EQ(gc_resp->offset(), last_offset);

  GetRangeRequest range_req;
  *range_req.mutable_stream() = stream;
  range_req.set_start_offset(first_offset);
  range_req.set_end_offset(last_offset);
  auto range_resp = client.GetRange(range_req);
  ASSERT_OK(range_resp.status());
  ASSERT_EQ(range_resp->entries_size(), 2);

  DeleteStreamRequest del_req;
  *del_req.mutable_stream() = stream;
  ASSERT_OK(client.DeleteStream(del_req));

  DeletePayload(client, p0.payload_id());
  DeletePayload(client, p1.payload_id());
}

} // namespace

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
  const char*       env_ep   = std::getenv("PAYLOAD_MANAGER_ENDPOINT");
  const std::string endpoint = (argc > 1) ? argv[1] : (env_ep ? env_ep : "");
  if (endpoint.empty()) {
    std::cout << "api_integration_test: skip (set PAYLOAD_MANAGER_ENDPOINT to run)\n";
    return 0;
  }
  std::cout << "api_integration_test: connecting to " << endpoint << '\n';

  // Initialise OTel tracer — default to localhost:4317 (Alloy gRPC).
  std::string otlp_ep = "localhost:4317";
  if (const char* e = std::getenv("OTLP_ENDPOINT")) otlp_ep = e;
  OtelInit(otlp_ep, "api-integration-test");
  std::cout << "api_integration_test: exporting traces to " << otlp_ep << '\n';

  // Each test:
  //   1. Start a root span → get SDK-generated trace/span IDs
  //   2. Build W3C traceparent from those IDs
  //   3. Create a channel that injects the traceparent into every RPC
  //   4. Run test under that channel
  //   5. End root span (exported via OTel batch processor)
  auto run = [&](const char* name, auto fn) {
    auto          ctx = OtelStartSpan(name);
    std::string   tp  = ctx.valid ? MakeTraceparent(ctx) : "";
    PayloadClient client(tp.empty() ? grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()) : MakeTracedChannel(endpoint, tp));
    fn(client, tp);
    OtelEndSpan();
  };

  run("TestRamRoundTrip", TestRamRoundTrip);
  run("TestDiskRoundTrip", TestDiskRoundTrip);
#if PAYLOAD_CLIENT_ARROW_CUDA
  run("TestGpuRoundTrip", TestGpuRoundTrip);
#endif
  run("TestSpillAndPromote", TestSpillAndPromote);
  run("TestPinBlocksSpill", TestPinBlocksSpill);
  run("TestMetadata", TestMetadata);
  run("TestLineage", TestLineage);
  run("TestStats", TestStats);
  run("TestListPayloads", TestListPayloads);
  run("TestStream", TestStream);

  std::cout << "api_integration_test: pass\n";
  OtelShutdown();
  return 0;
}
