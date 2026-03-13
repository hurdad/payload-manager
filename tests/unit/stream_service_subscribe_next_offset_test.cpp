/*
  Tests for TODO #14: Subscribe now returns a SubscribeBatch that includes
  next_offset so the gRPC streaming handler can implement a live polling loop.

  Before the fix Subscribe returned a plain vector<SubscribeResponse> and the
  gRPC handler wrote the batch then immediately closed the stream.  There was
  no way for the handler to know which offset to poll from next, especially
  when the initial batch was empty (e.g. from_latest on an empty stream).

  Covered:
    - next_offset is 0 when Subscribe is called on an empty stream
    - next_offset advances to (last_entry.offset + 1) after reading N entries
    - Subscribing from a non-zero start_offset sets next_offset accordingly
    - from_latest on a non-empty stream sets next_offset to one past the tail
      and returns an empty response batch (nothing to read yet)
    - Subscribing from next_offset after an append picks up the new entry
      (simulates the polling loop used by StreamServer::Subscribe)
*/

#include <cassert>
#include <iostream>
#include <memory>
#include <string>

#include "internal/db/memory/memory_repository.hpp"
#include "internal/service/service_context.hpp"
#include "internal/service/stream_service.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::db::memory::MemoryRepository;
using payload::manager::v1::AppendItem;
using payload::manager::v1::AppendRequest;
using payload::manager::v1::CreateStreamRequest;
using payload::manager::v1::StreamID;
using payload::manager::v1::SubscribeRequest;
using payload::service::ServiceContext;
using payload::service::StreamService;

StreamService MakeService() {
  ServiceContext ctx;
  ctx.repository = std::make_shared<MemoryRepository>();
  return StreamService(std::move(ctx));
}

StreamService MakeService(std::shared_ptr<payload::db::Repository> repo) {
  ServiceContext ctx;
  ctx.repository = std::move(repo);
  return StreamService(std::move(ctx));
}

StreamID MakeStream(const std::string& name = "s") {
  StreamID id;
  id.set_namespace_("test");
  id.set_name(name);
  return id;
}

AppendItem MakeItem() {
  AppendItem item;
  *item.mutable_payload_id() = payload::util::ToProto(payload::util::GenerateUUID());
  return item;
}

// Create a stream and return the service used to create it.
void CreateStream(StreamService& svc, const StreamID& stream) {
  CreateStreamRequest req;
  *req.mutable_stream() = stream;
  svc.CreateStream(req);
}

void AppendN(StreamService& svc, const StreamID& stream, int n) {
  AppendRequest req;
  *req.mutable_stream() = stream;
  for (int i = 0; i < n; ++i) {
    *req.add_items() = MakeItem();
  }
  svc.Append(req);
}

// ---------------------------------------------------------------------------
// Test: next_offset is 0 when Subscribe is called on an empty stream.
// ---------------------------------------------------------------------------
void TestSubscribeEmptyStreamNextOffsetIsZero() {
  auto svc    = MakeService();
  auto stream = MakeStream("empty");
  CreateStream(svc, stream);

  SubscribeRequest req;
  *req.mutable_stream() = stream;
  req.set_offset(0);

  const auto batch = svc.Subscribe(req);

  assert(batch.responses.empty() && "no entries expected on empty stream");
  assert(batch.next_offset == 0 && "next_offset must be 0 on an empty stream");
}

// ---------------------------------------------------------------------------
// Test: next_offset is (last_offset + 1) = N after appending N entries and
//       reading all of them from offset 0.
// ---------------------------------------------------------------------------
void TestSubscribeNextOffsetAdvancesWithEntries() {
  auto svc    = MakeService();
  auto stream = MakeStream("full");
  CreateStream(svc, stream);
  AppendN(svc, stream, 5);

  SubscribeRequest req;
  *req.mutable_stream() = stream;
  req.set_offset(0);

  const auto batch = svc.Subscribe(req);

  assert(batch.responses.size() == 5 && "must return all 5 entries");
  assert(batch.next_offset == 5 && "next_offset must be 5 after reading offsets 0-4");
}

// ---------------------------------------------------------------------------
// Test: Subscribing from a mid-stream offset sets next_offset relative to the
//       last returned entry, not the beginning of the stream.
// ---------------------------------------------------------------------------
void TestSubscribeFromMidOffsetSetsNextOffsetCorrectly() {
  auto svc    = MakeService();
  auto stream = MakeStream("mid");
  CreateStream(svc, stream);
  AppendN(svc, stream, 10); // offsets 0..9

  SubscribeRequest req;
  *req.mutable_stream() = stream;
  req.set_offset(7); // read offsets 7, 8, 9

  const auto batch = svc.Subscribe(req);

  assert(batch.responses.size() == 3 && "must return entries at offsets 7, 8, 9");
  assert(batch.next_offset == 10 && "next_offset must be 10 (one past offset 9)");
}

// ---------------------------------------------------------------------------
// Test: from_latest on a non-empty stream returns an empty response batch and
//       sets next_offset to one past the tail so the polling loop starts at
//       the correct position for future entries.
// ---------------------------------------------------------------------------
void TestSubscribeFromLatestReturnsEmptyBatchWithCorrectNextOffset() {
  auto svc    = MakeService();
  auto stream = MakeStream("latest");
  CreateStream(svc, stream);
  AppendN(svc, stream, 3); // offsets 0, 1, 2

  SubscribeRequest req;
  *req.mutable_stream() = stream;
  req.set_from_latest(true);

  const auto batch = svc.Subscribe(req);

  assert(batch.responses.empty() && "from_latest must return no entries when already at tail");
  assert(batch.next_offset == 3 && "from_latest next_offset must be one past the current tail (offset 2)");
}

// ---------------------------------------------------------------------------
// Test: from_latest on an empty stream returns next_offset = 0.
// ---------------------------------------------------------------------------
void TestSubscribeFromLatestOnEmptyStreamNextOffsetIsZero() {
  auto svc    = MakeService();
  auto stream = MakeStream("latest-empty");
  CreateStream(svc, stream);

  SubscribeRequest req;
  *req.mutable_stream() = stream;
  req.set_from_latest(true);

  const auto batch = svc.Subscribe(req);

  assert(batch.responses.empty());
  assert(batch.next_offset == 0 && "from_latest on empty stream must start at offset 0");
}

// ---------------------------------------------------------------------------
// Test: Simulated polling loop — Subscribe, append new entry, Subscribe again
//       from next_offset, and verify the new entry is returned.
//       This exercises the pattern used by StreamServer::Subscribe.
// ---------------------------------------------------------------------------
void TestPollingLoopPicksUpNewEntry() {
  auto       repo   = std::make_shared<MemoryRepository>();
  auto       svc    = MakeService(repo);
  const auto stream = MakeStream("poll");
  CreateStream(svc, stream);

  AppendN(svc, stream, 2); // offsets 0, 1

  // First poll: read from 0, consume existing entries.
  SubscribeRequest first_req;
  *first_req.mutable_stream() = stream;
  first_req.set_offset(0);
  const auto first_batch = svc.Subscribe(first_req);
  assert(first_batch.responses.size() == 2);
  assert(first_batch.next_offset == 2);

  // A new entry arrives.
  AppendN(svc, stream, 1); // offset 2

  // Second poll: use next_offset from previous batch.
  SubscribeRequest second_req;
  *second_req.mutable_stream() = stream;
  second_req.set_offset(first_batch.next_offset); // 2
  const auto second_batch = svc.Subscribe(second_req);

  assert(second_batch.responses.size() == 1 && "polling loop must pick up the new entry");
  assert(second_batch.responses[0].entry().offset() == 2 && "new entry must be at offset 2");
  assert(second_batch.next_offset == 3 && "next_offset must advance past the new entry");
}

// ---------------------------------------------------------------------------
// Test: Subscribing past the last offset yields empty batch and next_offset
//       equals the requested start (no regression into negative).
// ---------------------------------------------------------------------------
void TestSubscribePastEndReturnsEmptyWithSameOffset() {
  auto svc    = MakeService();
  auto stream = MakeStream("past-end");
  CreateStream(svc, stream);
  AppendN(svc, stream, 2); // offsets 0, 1

  SubscribeRequest req;
  *req.mutable_stream() = stream;
  req.set_offset(5); // past end

  const auto batch = svc.Subscribe(req);

  assert(batch.responses.empty() && "no entries past the end of the stream");
  assert(batch.next_offset == 5 && "next_offset must equal start_offset when nothing is read");
}

} // namespace

int main() {
  TestSubscribeEmptyStreamNextOffsetIsZero();
  TestSubscribeNextOffsetAdvancesWithEntries();
  TestSubscribeFromMidOffsetSetsNextOffsetCorrectly();
  TestSubscribeFromLatestReturnsEmptyBatchWithCorrectNextOffset();
  TestSubscribeFromLatestOnEmptyStreamNextOffsetIsZero();
  TestPollingLoopPicksUpNewEntry();
  TestSubscribePastEndReturnsEmptyWithSameOffset();

  std::cout << "stream_service_subscribe_next_offset_test: pass\n";
  return 0;
}
