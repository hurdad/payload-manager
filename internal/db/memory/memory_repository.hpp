#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#include "internal/db/api/repository.hpp"

namespace payload::db::memory {

class MemoryTransaction;

class MemoryRepository final : public db::Repository {
public:
  MemoryRepository();

  std::unique_ptr<Transaction> Begin() override;

  Result InsertPayload(Transaction&, const model::PayloadRecord&) override;
  std::optional<model::PayloadRecord> GetPayload(Transaction&, const std::string&) override;
  Result UpdatePayload(Transaction&, const model::PayloadRecord&) override;
  Result DeletePayload(Transaction&, const std::string&) override;

  Result UpsertMetadata(Transaction&, const model::MetadataRecord&) override;
  std::optional<model::MetadataRecord> GetMetadata(Transaction&, const std::string&) override;

  Result InsertLineage(Transaction&, const model::LineageRecord&) override;
  std::vector<model::LineageRecord> GetParents(Transaction&, const std::string&) override;
  std::vector<model::LineageRecord> GetChildren(Transaction&, const std::string&) override;

  Result CreateStream(Transaction&, model::StreamRecord&) override;
  std::optional<model::StreamRecord> GetStreamByName(
      Transaction&, const std::string& stream_namespace, const std::string& name) override;
  std::optional<model::StreamRecord> GetStreamById(Transaction&, uint64_t stream_id) override;
  Result DeleteStreamByName(Transaction&, const std::string& stream_namespace,
                            const std::string& name) override;
  Result DeleteStreamById(Transaction&, uint64_t stream_id) override;
  Result AppendStreamEntries(Transaction&, uint64_t stream_id,
                             std::vector<model::StreamEntryRecord>& entries) override;
  std::vector<model::StreamEntryRecord> ReadStreamEntries(
      Transaction&, uint64_t stream_id, uint64_t start_offset,
      std::optional<uint64_t> max_entries,
      std::optional<uint64_t> min_append_time_ms) override;
  std::vector<model::StreamEntryRecord> ReadStreamEntriesRange(
      Transaction&, uint64_t stream_id, uint64_t start_offset, uint64_t end_offset) override;
  Result TrimStreamEntriesToMaxCount(Transaction&, uint64_t stream_id,
                                     uint64_t max_entries) override;
  Result DeleteStreamEntriesOlderThan(Transaction&, uint64_t stream_id,
                                      uint64_t min_append_time_ms) override;
  Result CommitConsumerOffset(Transaction&, const model::StreamConsumerOffsetRecord&) override;
  std::optional<model::StreamConsumerOffsetRecord> GetConsumerOffset(
      Transaction&, uint64_t stream_id, const std::string& consumer_group) override;

private:
  friend class MemoryTransaction;

  struct State {
    std::unordered_map<std::string, model::PayloadRecord> payloads;
    std::unordered_map<std::string, model::MetadataRecord> metadata;
    std::vector<model::LineageRecord> lineage;

    std::unordered_map<uint64_t, model::StreamRecord> streams;
    std::unordered_map<std::string, uint64_t> stream_name_to_id;
    std::unordered_map<uint64_t, std::vector<model::StreamEntryRecord>> stream_entries;
    std::unordered_map<std::string, model::StreamConsumerOffsetRecord> consumer_offsets;
    std::unordered_map<uint64_t, uint64_t> next_stream_offset;
    uint64_t next_stream_id = 1;
  };

  std::mutex mutex_;
  State committed_;
};

}
