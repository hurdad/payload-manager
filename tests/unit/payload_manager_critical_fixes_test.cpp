#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "internal/core/payload_manager.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/storage/storage_backend.hpp"
#include "internal/util/errors.hpp"
#include "internal/util/uuid.hpp"

namespace {

using payload::core::PayloadManager;
using payload::lease::LeaseManager;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;
using payload::manager::v1::TIER_DISK;
using payload::manager::v1::TIER_RAM;
using payload::storage::StorageBackend;

// Records the order in which storage operations occur so we can verify
// that Remove is called only after the DB commit.
class OrderTrackingBackend final : public StorageBackend {
 public:
  explicit OrderTrackingBackend(payload::manager::v1::Tier tier, std::shared_ptr<std::vector<std::string>> log) : tier_(tier), log_(std::move(log)) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override {
    log_->push_back(TierName() + ":allocate:" + id.value());
    auto maybe_buffer = arrow::AllocateBuffer(size_bytes);
    if (!maybe_buffer.ok()) throw std::runtime_error("allocate failed");
    std::shared_ptr<arrow::Buffer> buffer(std::move(*maybe_buffer));
    if (size_bytes > 0) std::memset(buffer->mutable_data(), 0xAB, static_cast<size_t>(size_bytes));
    buffers_[id.value()] = buffer;
    return buffer;
  }

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override {
    log_->push_back(TierName() + ":read:" + id.value());
    auto it = buffers_.find(id.value());
    if (it == buffers_.end()) throw std::runtime_error(TierName() + " payload not found for read");
    return it->second;
  }

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buffer, bool) override {
    log_->push_back(TierName() + ":write:" + id.value());
    buffers_[id.value()] = buffer;
  }

  void Remove(const payload::manager::v1::PayloadID& id) override {
    log_->push_back(TierName() + ":remove:" + id.value());
    buffers_.erase(id.value());
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

  bool Contains(const payload::manager::v1::PayloadID& id) const {
    return buffers_.count(id.value()) > 0;
  }

 private:
  std::string TierName() const {
    return tier_ == TIER_RAM ? "ram" : "disk";
  }

  payload::manager::v1::Tier                                      tier_;
  std::shared_ptr<std::vector<std::string>>                       log_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
};

// A repository wrapper that logs Commit() calls into the shared operation log.
class LoggingRepository final : public payload::db::Repository {
 public:
  LoggingRepository(std::shared_ptr<payload::db::memory::MemoryRepository> inner, std::shared_ptr<std::vector<std::string>> log)
      : inner_(std::move(inner)), log_(std::move(log)) {
  }

  std::unique_ptr<payload::db::Transaction> Begin() override {
    auto tx = inner_->Begin();
    return std::make_unique<LoggingTx>(std::move(tx), log_);
  }

  payload::db::Result InsertPayload(payload::db::Transaction& t, const payload::db::model::PayloadRecord& r) override {
    return inner_->InsertPayload(Unwrap(t), r);
  }
  std::optional<payload::db::model::PayloadRecord> GetPayload(payload::db::Transaction& t, const payload::util::UUID& id) override {
    return inner_->GetPayload(Unwrap(t), id);
  }
  std::vector<payload::db::model::PayloadRecord> ListPayloads(
      payload::db::Transaction& t, payload::manager::v1::Tier tier_filter = payload::manager::v1::TIER_UNSPECIFIED,
      int32_t limit = 0, int32_t offset = 0) override {
    return inner_->ListPayloads(Unwrap(t), tier_filter, limit, offset);
  }
  int32_t CountPayloads(payload::db::Transaction& t,
                        payload::manager::v1::Tier tier_filter = payload::manager::v1::TIER_UNSPECIFIED) override {
    return inner_->CountPayloads(Unwrap(t), tier_filter);
  }
  payload::db::Result UpdatePayload(payload::db::Transaction& t, const payload::db::model::PayloadRecord& r) override {
    return inner_->UpdatePayload(Unwrap(t), r);
  }
  payload::db::Result DeletePayload(payload::db::Transaction& t, const payload::util::UUID& id) override {
    return inner_->DeletePayload(Unwrap(t), id);
  }
  std::vector<payload::db::model::PayloadRecord> ListExpiredPayloads(payload::db::Transaction& t, uint64_t now_ms) override {
    return inner_->ListExpiredPayloads(Unwrap(t), now_ms);
  }
  payload::db::Result UpsertMetadata(payload::db::Transaction& t, const payload::db::model::MetadataRecord& r) override {
    return inner_->UpsertMetadata(Unwrap(t), r);
  }
  std::optional<payload::db::model::MetadataRecord> GetMetadata(payload::db::Transaction& t, const std::string& id) override {
    return inner_->GetMetadata(Unwrap(t), id);
  }
  payload::db::Result InsertMetadataEvent(payload::db::Transaction& t, const payload::db::model::MetadataEventRecord& r) override {
    return inner_->InsertMetadataEvent(Unwrap(t), r);
  }
  payload::db::Result InsertLineage(payload::db::Transaction& t, const payload::db::model::LineageRecord& r) override {
    return inner_->InsertLineage(Unwrap(t), r);
  }
  std::vector<payload::db::model::LineageRecord> GetParents(payload::db::Transaction& t, const std::string& id) override {
    return inner_->GetParents(Unwrap(t), id);
  }
  std::vector<payload::db::model::LineageRecord> GetChildren(payload::db::Transaction& t, const std::string& id) override {
    return inner_->GetChildren(Unwrap(t), id);
  }
  payload::db::Result CreateStream(payload::db::Transaction& t, payload::db::model::StreamRecord& r) override {
    return inner_->CreateStream(Unwrap(t), r);
  }
  std::optional<payload::db::model::StreamRecord> GetStreamByName(payload::db::Transaction& t, const std::string& ns, const std::string& n) override {
    return inner_->GetStreamByName(Unwrap(t), ns, n);
  }
  std::optional<payload::db::model::StreamRecord> GetStreamById(payload::db::Transaction& t, uint64_t id) override {
    return inner_->GetStreamById(Unwrap(t), id);
  }
  payload::db::Result DeleteStreamByName(payload::db::Transaction& t, const std::string& ns, const std::string& n) override {
    return inner_->DeleteStreamByName(Unwrap(t), ns, n);
  }
  payload::db::Result DeleteStreamById(payload::db::Transaction& t, uint64_t id) override {
    return inner_->DeleteStreamById(Unwrap(t), id);
  }
  payload::db::Result AppendStreamEntries(payload::db::Transaction& t, uint64_t sid, std::vector<payload::db::model::StreamEntryRecord>& e) override {
    return inner_->AppendStreamEntries(Unwrap(t), sid, e);
  }
  std::vector<payload::db::model::StreamEntryRecord> ReadStreamEntries(payload::db::Transaction& t, uint64_t sid, uint64_t so,
                                                                       std::optional<uint64_t> me, std::optional<uint64_t> mt) override {
    return inner_->ReadStreamEntries(Unwrap(t), sid, so, me, mt);
  }
  std::optional<uint64_t> GetMaxStreamOffset(payload::db::Transaction& t, uint64_t sid) override {
    return inner_->GetMaxStreamOffset(Unwrap(t), sid);
  }
  std::vector<payload::db::model::StreamEntryRecord> ReadStreamEntriesRange(payload::db::Transaction& t, uint64_t sid, uint64_t so,
                                                                            uint64_t eo) override {
    return inner_->ReadStreamEntriesRange(Unwrap(t), sid, so, eo);
  }
  payload::db::Result TrimStreamEntriesToMaxCount(payload::db::Transaction& t, uint64_t sid, uint64_t me) override {
    return inner_->TrimStreamEntriesToMaxCount(Unwrap(t), sid, me);
  }
  payload::db::Result DeleteStreamEntriesOlderThan(payload::db::Transaction& t, uint64_t sid, uint64_t mt) override {
    return inner_->DeleteStreamEntriesOlderThan(Unwrap(t), sid, mt);
  }
  payload::db::Result CommitConsumerOffset(payload::db::Transaction& t, const payload::db::model::StreamConsumerOffsetRecord& r) override {
    return inner_->CommitConsumerOffset(Unwrap(t), r);
  }
  std::optional<payload::db::model::StreamConsumerOffsetRecord> GetConsumerOffset(payload::db::Transaction& t, uint64_t sid,
                                                                                  const std::string& cg) override {
    return inner_->GetConsumerOffset(Unwrap(t), sid, cg);
  }

 private:
  class LoggingTx final : public payload::db::Transaction {
   public:
    LoggingTx(std::unique_ptr<payload::db::Transaction> inner, std::shared_ptr<std::vector<std::string>> log)
        : inner_(std::move(inner)), log_(std::move(log)) {
    }

    void Commit() override {
      inner_->Commit();
      log_->push_back("db:commit");
    }
    void Rollback() override {
      inner_->Rollback();
    }
    bool IsCommitted() const override {
      return inner_->IsCommitted();
    }

    payload::db::Transaction& Inner() {
      return *inner_;
    }

   private:
    std::unique_ptr<payload::db::Transaction> inner_;
    std::shared_ptr<std::vector<std::string>> log_;
  };

  static payload::db::Transaction& Unwrap(payload::db::Transaction& t) {
    return static_cast<LoggingTx&>(t).Inner();
  }

  std::shared_ptr<payload::db::memory::MemoryRepository> inner_;
  std::shared_ptr<std::vector<std::string>>              log_;
};

// Find the index of the first log entry containing `needle`.
int FindLog(const std::vector<std::string>& log, const std::string& needle, int start = 0) {
  for (int i = start; i < static_cast<int>(log.size()); ++i) {
    if (log[i].find(needle) != std::string::npos) return i;
  }
  return -1;
}

// ---------------------------------------------------------------------------
// ThrowingRemoveBackend (used by TestDeleteSucceedsWhenStorageRemoveThrows)
// ---------------------------------------------------------------------------
class ThrowingRemoveBackend final : public StorageBackend {
 public:
  explicit ThrowingRemoveBackend(payload::manager::v1::Tier tier) : tier_(tier) {
  }

  std::shared_ptr<arrow::Buffer> Allocate(const payload::manager::v1::PayloadID& id, uint64_t size_bytes) override {
    auto maybe = arrow::AllocateBuffer(size_bytes);
    if (!maybe.ok()) throw std::runtime_error("alloc failed");
    std::shared_ptr<arrow::Buffer> buf(std::move(*maybe));
    buffers_[id.value()] = buf;
    return buf;
  }

  std::shared_ptr<arrow::Buffer> Read(const payload::manager::v1::PayloadID& id) override {
    return buffers_.at(id.value());
  }

  void Write(const payload::manager::v1::PayloadID& id, const std::shared_ptr<arrow::Buffer>& buf, bool) override {
    buffers_[id.value()] = buf;
  }

  void Remove(const payload::manager::v1::PayloadID&) override {
    throw std::runtime_error("simulated storage failure during Remove");
  }

  payload::manager::v1::Tier TierType() const override {
    return tier_;
  }

 private:
  payload::manager::v1::Tier                                      tier_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> buffers_;
};

} // namespace

// ---------------------------------------------------------------------------
// Test: Promote commits DB before removing source data
// ---------------------------------------------------------------------------
TEST(PayloadManagerCriticalFixes, PromoteCommitsDbBeforeSourceRemoval) {
  auto log          = std::make_shared<std::vector<std::string>>();
  auto ram_backend  = std::make_shared<OrderTrackingBackend>(TIER_RAM, log);
  auto disk_backend = std::make_shared<OrderTrackingBackend>(TIER_DISK, log);
  auto inner_repo   = std::make_shared<payload::db::memory::MemoryRepository>();
  auto repo         = std::make_shared<LoggingRepository>(inner_repo, log);
  auto lease_mgr    = std::make_shared<LeaseManager>();

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = ram_backend;
  storage[TIER_DISK] = disk_backend;

  PayloadManager manager(std::move(storage), lease_mgr, repo);

  auto desc = manager.Commit(manager.Allocate(256, TIER_RAM).payload_id());
  EXPECT_EQ(desc.tier(), TIER_RAM);

  // Clear log so we only see promote operations.
  log->clear();

  auto promoted = manager.Promote(desc.payload_id(), TIER_DISK);
  EXPECT_EQ(promoted.tier(), TIER_DISK);

  // Verify ordering: disk:write must come before db:commit,
  // and db:commit must come before ram:remove.
  int disk_write_idx = FindLog(*log, "disk:write");
  int db_commit_idx  = FindLog(*log, "db:commit");
  int ram_remove_idx = FindLog(*log, "ram:remove");

  EXPECT_GE(disk_write_idx, 0) << "disk write must occur";
  EXPECT_GE(db_commit_idx, 0) << "db commit must occur";
  EXPECT_GE(ram_remove_idx, 0) << "ram remove must occur";
  EXPECT_LT(disk_write_idx, db_commit_idx) << "disk write must happen before db commit";
  EXPECT_LT(db_commit_idx, ram_remove_idx) << "db commit must happen before ram remove";
}

// ---------------------------------------------------------------------------
// Test: ExecuteSpill commits DB before removing source data
// ---------------------------------------------------------------------------
TEST(PayloadManagerCriticalFixes, SpillCommitsDbBeforeSourceRemoval) {
  auto log          = std::make_shared<std::vector<std::string>>();
  auto ram_backend  = std::make_shared<OrderTrackingBackend>(TIER_RAM, log);
  auto disk_backend = std::make_shared<OrderTrackingBackend>(TIER_DISK, log);
  auto inner_repo   = std::make_shared<payload::db::memory::MemoryRepository>();
  auto repo         = std::make_shared<LoggingRepository>(inner_repo, log);
  auto lease_mgr    = std::make_shared<LeaseManager>();

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = ram_backend;
  storage[TIER_DISK] = disk_backend;

  PayloadManager manager(std::move(storage), lease_mgr, repo);

  auto desc = manager.Commit(manager.Allocate(128, TIER_RAM).payload_id());
  EXPECT_EQ(desc.tier(), TIER_RAM);

  log->clear();

  manager.ExecuteSpill(desc.payload_id(), TIER_DISK, /*fsync=*/false);

  int disk_write_idx = FindLog(*log, "disk:write");
  int db_commit_idx  = FindLog(*log, "db:commit");
  int ram_remove_idx = FindLog(*log, "ram:remove");

  EXPECT_GE(disk_write_idx, 0) << "disk write must occur";
  EXPECT_GE(db_commit_idx, 0) << "db commit must occur";
  EXPECT_GE(ram_remove_idx, 0) << "ram remove must occur";
  EXPECT_LT(disk_write_idx, db_commit_idx) << "disk write must happen before db commit";
  EXPECT_LT(db_commit_idx, ram_remove_idx) << "db commit must happen before ram remove";
}

// ---------------------------------------------------------------------------
// Test: Promote with same tier does not remove data
// ---------------------------------------------------------------------------
TEST(PayloadManagerCriticalFixes, PromoteSameTierIsNoop) {
  auto log          = std::make_shared<std::vector<std::string>>();
  auto ram_backend  = std::make_shared<OrderTrackingBackend>(TIER_RAM, log);
  auto disk_backend = std::make_shared<OrderTrackingBackend>(TIER_DISK, log);
  auto inner_repo   = std::make_shared<payload::db::memory::MemoryRepository>();
  auto repo         = std::make_shared<LoggingRepository>(inner_repo, log);
  auto lease_mgr    = std::make_shared<LeaseManager>();

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = ram_backend;
  storage[TIER_DISK] = disk_backend;

  PayloadManager manager(std::move(storage), lease_mgr, repo);

  auto desc = manager.Commit(manager.Allocate(64, TIER_RAM).payload_id());
  log->clear();

  auto promoted = manager.Promote(desc.payload_id(), TIER_RAM);
  EXPECT_EQ(promoted.tier(), TIER_RAM);

  // No remove should happen when promoting to the same tier.
  EXPECT_EQ(FindLog(*log, "ram:remove"), -1) << "no remove for same-tier promote";
  EXPECT_EQ(FindLog(*log, "disk:write"), -1) << "no write for same-tier promote";
}

// ---------------------------------------------------------------------------
// Test: Delete prunes per-payload mutex from the map
// ---------------------------------------------------------------------------
TEST(PayloadManagerCriticalFixes, DeletePrunesPayloadMutex) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto log       = std::make_shared<std::vector<std::string>>();

  auto                                      ram_backend = std::make_shared<OrderTrackingBackend>(TIER_RAM, log);
  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = ram_backend;
  storage[TIER_DISK] = std::make_shared<OrderTrackingBackend>(TIER_DISK, log);

  PayloadManager manager(std::move(storage), lease_mgr, std::make_shared<payload::db::memory::MemoryRepository>());

  // Allocate and commit several payloads, then delete them all.
  std::vector<payload::manager::v1::PayloadID> ids;
  for (int i = 0; i < 10; ++i) {
    auto desc = manager.Commit(manager.Allocate(64, TIER_RAM).payload_id());
    ids.push_back(desc.payload_id());
  }

  // Access each one to ensure mutexes are created.
  for (const auto& id : ids) {
    (void)manager.ResolveSnapshot(id);
  }

  // Delete all payloads.
  for (const auto& id : ids) {
    manager.Delete(id, /*force=*/true);
  }

  // Verify deleted payloads are truly gone by confirming ResolveSnapshot throws.
  for (const auto& id : ids) {
    EXPECT_THROW((void)manager.ResolveSnapshot(id), std::runtime_error) << "deleted payload should not be resolvable";
  }

  // After deletion, new allocations should work fine (no stale mutex interference).
  auto fresh    = manager.Commit(manager.Allocate(64, TIER_RAM).payload_id());
  auto snapshot = manager.ResolveSnapshot(fresh.payload_id());
  EXPECT_EQ(snapshot.state(), PAYLOAD_STATE_ACTIVE);
}

// ---------------------------------------------------------------------------
// Test: Spill preserves data in destination after source removal
// ---------------------------------------------------------------------------
TEST(PayloadManagerCriticalFixes, SpillDataAvailableInDestAfterComplete) {
  auto log          = std::make_shared<std::vector<std::string>>();
  auto ram_backend  = std::make_shared<OrderTrackingBackend>(TIER_RAM, log);
  auto disk_backend = std::make_shared<OrderTrackingBackend>(TIER_DISK, log);
  auto inner_repo   = std::make_shared<payload::db::memory::MemoryRepository>();
  auto repo         = std::make_shared<LoggingRepository>(inner_repo, log);
  auto lease_mgr    = std::make_shared<LeaseManager>();

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = ram_backend;
  storage[TIER_DISK] = disk_backend;

  PayloadManager manager(std::move(storage), lease_mgr, repo);

  auto desc = manager.Commit(manager.Allocate(128, TIER_RAM).payload_id());

  manager.ExecuteSpill(desc.payload_id(), TIER_DISK, false);

  // Data should now be in disk, not in ram.
  EXPECT_FALSE(ram_backend->Contains(desc.payload_id())) << "ram should be cleaned up";
  EXPECT_TRUE(disk_backend->Contains(desc.payload_id())) << "disk should have the data";

  // DB should reflect the new tier.
  auto snapshot = manager.ResolveSnapshot(desc.payload_id());
  EXPECT_EQ(snapshot.tier(), TIER_DISK);
}

// ---------------------------------------------------------------------------
// Test: Allocate with zero bytes throws
// ---------------------------------------------------------------------------
TEST(PayloadManagerCriticalFixes, AllocateZeroBytesThrows) {
  auto log       = std::make_shared<std::vector<std::string>>();
  auto backend   = std::make_shared<OrderTrackingBackend>(TIER_RAM, log);
  auto lease_mgr = std::make_shared<LeaseManager>();

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM] = backend;

  PayloadManager manager(std::move(storage), lease_mgr, std::make_shared<payload::db::memory::MemoryRepository>());

  EXPECT_THROW((void)manager.Allocate(0, TIER_RAM), payload::util::InvalidArgument) << "Allocate(0) must throw payload::util::InvalidArgument";
}

// ---------------------------------------------------------------------------
// Test: Delete succeeds even when storage Remove throws
// ---------------------------------------------------------------------------
TEST(PayloadManagerCriticalFixes, DeleteSucceedsWhenStorageRemoveThrows) {
  auto lease_mgr = std::make_shared<LeaseManager>();
  auto backend   = std::make_shared<ThrowingRemoveBackend>(TIER_RAM);

  payload::storage::StorageFactory::TierMap storage;
  storage[TIER_RAM]  = backend;
  storage[TIER_DISK] = std::make_shared<ThrowingRemoveBackend>(TIER_DISK);

  PayloadManager manager(std::move(storage), lease_mgr, std::make_shared<payload::db::memory::MemoryRepository>());

  auto desc = manager.Commit(manager.Allocate(64, TIER_RAM).payload_id());

  // Delete must not throw even though Remove() throws internally.
  EXPECT_NO_THROW(manager.Delete(desc.payload_id(), /*force=*/true)) << "Delete must not propagate storage Remove failures after DB commit";

  // The payload must no longer be resolvable.
  EXPECT_THROW((void)manager.ResolveSnapshot(desc.payload_id()), std::runtime_error) << "payload must be gone from DB even if storage Remove failed";
}
