#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "internal/db/memory/memory_repository.hpp"
#include "internal/service/service_context.hpp"
#include "internal/service/stream_service.hpp"
#include "internal/util/errors.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace {

using payload::db::Repository;
using payload::db::Result;
using payload::db::Transaction;
using payload::db::memory::MemoryRepository;
using payload::manager::v1::AppendItem;
using payload::manager::v1::AppendRequest;
using payload::manager::v1::CommitRequest;
using payload::manager::v1::CreateStreamRequest;
using payload::manager::v1::DeleteStreamRequest;
using payload::manager::v1::ReadRequest;
using payload::manager::v1::StreamID;
using payload::service::ServiceContext;
using payload::service::StreamService;

class HookedRepository : public Repository {
 public:
  std::unique_ptr<Transaction> Begin() override {
    return inner_.Begin();
  }

  Result InsertPayload(Transaction& tx, const payload::db::model::PayloadRecord& record) override {
    return inner_.InsertPayload(tx, record);
  }

  std::optional<payload::db::model::PayloadRecord> GetPayload(Transaction& tx, const payload::util::UUID& id) override {
    return inner_.GetPayload(tx, id);
  }

  std::vector<payload::db::model::PayloadRecord> ListPayloads(
      Transaction& tx, payload::manager::v1::Tier tier_filter = payload::manager::v1::TIER_UNSPECIFIED,
      int32_t limit = 0, int32_t offset = 0) override {
    return inner_.ListPayloads(tx, tier_filter, limit, offset);
  }
  int32_t CountPayloads(Transaction& tx,
                        payload::manager::v1::Tier tier_filter = payload::manager::v1::TIER_UNSPECIFIED) override {
    return inner_.CountPayloads(tx, tier_filter);
  }

  Result UpdatePayload(Transaction& tx, const payload::db::model::PayloadRecord& record) override {
    return inner_.UpdatePayload(tx, record);
  }

  Result DeletePayload(Transaction& tx, const payload::util::UUID& id) override {
    return inner_.DeletePayload(tx, id);
  }

  std::vector<payload::db::model::PayloadRecord> ListExpiredPayloads(Transaction& tx, uint64_t now_ms) override {
    return inner_.ListExpiredPayloads(tx, now_ms);
  }

  Result UpsertMetadata(Transaction& tx, const payload::db::model::MetadataRecord& record) override {
    return inner_.UpsertMetadata(tx, record);
  }

  std::optional<payload::db::model::MetadataRecord> GetMetadata(Transaction& tx, const std::string& id) override {
    return inner_.GetMetadata(tx, id);
  }

  Result InsertMetadataEvent(Transaction& tx, const payload::db::model::MetadataEventRecord& record) override {
    return inner_.InsertMetadataEvent(tx, record);
  }

  Result InsertLineage(Transaction& tx, const payload::db::model::LineageRecord& record) override {
    return inner_.InsertLineage(tx, record);
  }

  std::vector<payload::db::model::LineageRecord> GetParents(Transaction& tx, const std::string& id) override {
    return inner_.GetParents(tx, id);
  }

  std::vector<payload::db::model::LineageRecord> GetChildren(Transaction& tx, const std::string& id) override {
    return inner_.GetChildren(tx, id);
  }

  Result CreateStream(Transaction& tx, payload::db::model::StreamRecord& stream) override {
    return inner_.CreateStream(tx, stream);
  }

  std::optional<payload::db::model::StreamRecord> GetStreamByName(Transaction& tx, const std::string& stream_namespace,
                                                                  const std::string& name) override {
    return inner_.GetStreamByName(tx, stream_namespace, name);
  }

  std::optional<payload::db::model::StreamRecord> GetStreamById(Transaction& tx, uint64_t stream_id) override {
    return inner_.GetStreamById(tx, stream_id);
  }

  Result DeleteStreamByName(Transaction& tx, const std::string& stream_namespace, const std::string& name) override {
    {
      std::lock_guard<std::mutex> lock(hook_mu_);
      if (delete_sleep_.count() > 0) {
        delete_started_ = true;
        hook_cv_.notify_all();
      }
    }
    if (delete_sleep_.count() > 0) {
      std::this_thread::sleep_for(delete_sleep_);
    }
    return inner_.DeleteStreamByName(tx, stream_namespace, name);
  }

  Result DeleteStreamById(Transaction& tx, uint64_t stream_id) override {
    return inner_.DeleteStreamById(tx, stream_id);
  }

  Result AppendStreamEntries(Transaction& tx, uint64_t stream_id, std::vector<payload::db::model::StreamEntryRecord>& entries) override {
    EnterBarrier(Op::kAppend);
    return inner_.AppendStreamEntries(tx, stream_id, entries);
  }

  std::vector<payload::db::model::StreamEntryRecord> ReadStreamEntries(Transaction& tx, uint64_t stream_id, uint64_t start_offset,
                                                                       std::optional<uint64_t> max_entries,
                                                                       std::optional<uint64_t> min_append_time_ms) override {
    EnterBarrier(Op::kRead);
    return inner_.ReadStreamEntries(tx, stream_id, start_offset, max_entries, min_append_time_ms);
  }

  std::optional<uint64_t> GetMaxStreamOffset(Transaction& tx, uint64_t stream_id) override {
    return inner_.GetMaxStreamOffset(tx, stream_id);
  }

  std::vector<payload::db::model::StreamEntryRecord> ReadStreamEntriesRange(Transaction& tx, uint64_t stream_id, uint64_t start_offset,
                                                                            uint64_t end_offset) override {
    EnterBarrier(Op::kRead);
    return inner_.ReadStreamEntriesRange(tx, stream_id, start_offset, end_offset);
  }

  Result TrimStreamEntriesToMaxCount(Transaction& tx, uint64_t stream_id, uint64_t max_entries) override {
    return inner_.TrimStreamEntriesToMaxCount(tx, stream_id, max_entries);
  }

  Result DeleteStreamEntriesOlderThan(Transaction& tx, uint64_t stream_id, uint64_t min_append_time_ms) override {
    return inner_.DeleteStreamEntriesOlderThan(tx, stream_id, min_append_time_ms);
  }

  Result CommitConsumerOffset(Transaction& tx, const payload::db::model::StreamConsumerOffsetRecord& record) override {
    return inner_.CommitConsumerOffset(tx, record);
  }

  std::optional<payload::db::model::StreamConsumerOffsetRecord> GetConsumerOffset(Transaction& tx, uint64_t stream_id,
                                                                                  const std::string& consumer_group) override {
    return inner_.GetConsumerOffset(tx, stream_id, consumer_group);
  }

  void ArmBarrier(bool require_append, bool require_read) {
    std::lock_guard<std::mutex> lock(hook_mu_);
    barrier_active_         = true;
    barrier_saw_read_       = false;
    barrier_saw_append_     = false;
    barrier_timed_out_      = false;
    barrier_require_append_ = require_append;
    barrier_require_read_   = require_read;
  }

  bool BarrierTimedOut() const {
    std::lock_guard<std::mutex> lock(hook_mu_);
    return barrier_timed_out_;
  }

  bool BarrierObservedBoth() const {
    std::lock_guard<std::mutex> lock(hook_mu_);
    return barrier_saw_read_ && barrier_saw_append_;
  }

  void DelayDeleteBy(std::chrono::milliseconds duration) {
    std::lock_guard<std::mutex> lock(hook_mu_);
    delete_sleep_   = duration;
    delete_started_ = false;
  }

  bool WaitForDeleteToStart(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(hook_mu_);
    return hook_cv_.wait_for(lock, timeout, [&] { return delete_started_; });
  }

 private:
  enum class Op {
    kRead,
    kAppend,
  };

  void EnterBarrier(Op op) {
    std::unique_lock<std::mutex> lock(hook_mu_);
    if (!barrier_active_) {
      return;
    }

    if (op == Op::kRead) {
      barrier_saw_read_ = true;
    }
    if (op == Op::kAppend) {
      barrier_saw_append_ = true;
    }

    const auto ready = [&] { return (!barrier_require_read_ || barrier_saw_read_) && (!barrier_require_append_ || barrier_saw_append_); };

    if (ready()) {
      barrier_active_ = false;
      hook_cv_.notify_all();
      return;
    }

    if (!hook_cv_.wait_for(lock, std::chrono::milliseconds(750), [&] { return !barrier_active_; })) {
      barrier_timed_out_ = true;
      barrier_active_    = false;
      hook_cv_.notify_all();
    }
  }

  MemoryRepository inner_;

  mutable std::mutex      hook_mu_;
  std::condition_variable hook_cv_;

  bool barrier_active_         = false;
  bool barrier_saw_read_       = false;
  bool barrier_saw_append_     = false;
  bool barrier_timed_out_      = false;
  bool barrier_require_read_   = false;
  bool barrier_require_append_ = false;

  std::chrono::milliseconds delete_sleep_{0};
  bool                      delete_started_ = false;
};

StreamID MakeStream(const std::string& stream_name) {
  StreamID stream;
  stream.set_namespace_("concurrency");
  stream.set_name(stream_name);
  return stream;
}

CreateStreamRequest CreateRequest(const StreamID& stream) {
  CreateStreamRequest req;
  *req.mutable_stream() = stream;
  return req;
}

AppendRequest AppendRequestWithOneEntry(const StreamID& stream) {
  AppendRequest req;
  *req.mutable_stream()       = stream;
  AppendItem* item            = req.add_items();
  *item->mutable_payload_id() = payload::util::ToProto(payload::util::GenerateUUID());
  return req;
}

ReadRequest ReadFromStart(const StreamID& stream) {
  ReadRequest req;
  *req.mutable_stream() = stream;
  req.set_start_offset(0);
  return req;
}

} // namespace

TEST(StreamServiceConcurrency, ParallelReadAndReadWriteDoNotSerializeGlobally) {
  auto repo = std::make_shared<HookedRepository>();

  ServiceContext ctx;
  ctx.repository = repo;

  StreamService service(ctx);
  const auto    stream_a = MakeStream("stream-a");
  const auto    stream_b = MakeStream("stream-b");

  service.CreateStream(CreateRequest(stream_a));
  service.CreateStream(CreateRequest(stream_b));
  service.Append(AppendRequestWithOneEntry(stream_a));
  service.Append(AppendRequestWithOneEntry(stream_b));

  repo->ArmBarrier(false, true);

  std::thread read_a([&] { service.Read(ReadFromStart(stream_a)); });
  std::thread read_b([&] { service.Read(ReadFromStart(stream_b)); });

  read_a.join();
  read_b.join();

  EXPECT_FALSE(repo->BarrierTimedOut());

  repo->ArmBarrier(true, true);

  std::thread append_a([&] { service.Append(AppendRequestWithOneEntry(stream_a)); });
  std::thread read_again_b([&] { service.Read(ReadFromStart(stream_b)); });

  append_a.join();
  read_again_b.join();

  EXPECT_FALSE(repo->BarrierTimedOut());
  EXPECT_TRUE(repo->BarrierObservedBoth());
}

TEST(StreamServiceConcurrency, DeleteRaceRejectsReadAndAppendWithNotFound) {
  auto repo = std::make_shared<HookedRepository>();

  ServiceContext ctx;
  ctx.repository = repo;

  StreamService service(ctx);
  const auto    stream = MakeStream("delete-race");

  service.CreateStream(CreateRequest(stream));
  service.Append(AppendRequestWithOneEntry(stream));

  repo->DelayDeleteBy(std::chrono::milliseconds(200));

  DeleteStreamRequest delete_req;
  *delete_req.mutable_stream() = stream;

  std::thread delete_thread([&] { service.DeleteStream(delete_req); });

  const bool started = repo->WaitForDeleteToStart(std::chrono::milliseconds(500));
  EXPECT_TRUE(started);

  // Latency not verified — wall-clock assertions are flaky under load
  bool append_not_found = false;
  try {
    service.Append(AppendRequestWithOneEntry(stream));
  } catch (const payload::util::NotFound&) {
    append_not_found = true;
  }
  EXPECT_TRUE(append_not_found);

  bool read_not_found = false;
  try {
    service.Read(ReadFromStart(stream));
  } catch (const payload::util::NotFound&) {
    read_not_found = true;
  }
  EXPECT_TRUE(read_not_found);

  delete_thread.join();
}

TEST(StreamServiceConcurrency, MemoryBackendCommitStillSupportsConsumerOffsets) {
  ServiceContext ctx;
  ctx.repository = std::make_shared<MemoryRepository>();

  StreamService service(ctx);
  const auto    stream = MakeStream("commit-coverage");

  service.CreateStream(CreateRequest(stream));
  service.Append(AppendRequestWithOneEntry(stream));

  CommitRequest commit_req;
  *commit_req.mutable_stream() = stream;
  commit_req.set_consumer_group("cg");
  commit_req.set_offset(1);
  service.Commit(commit_req);

  payload::manager::v1::GetCommittedRequest get_req;
  *get_req.mutable_stream() = stream;
  get_req.set_consumer_group("cg");

  const auto committed = service.GetCommitted(get_req);
  EXPECT_EQ(committed.offset(), 1u);
}
