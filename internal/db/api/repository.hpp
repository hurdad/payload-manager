#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "internal/db/api/result.hpp"
#include "internal/db/api/transaction.hpp"
#include "internal/db/model/lineage_record.hpp"
#include "internal/db/model/metadata_record.hpp"
#include "internal/db/model/payload_record.hpp"
#include "internal/db/model/stream_consumer_offset_record.hpp"
#include "internal/db/model/stream_entry_record.hpp"
#include "internal/db/model/stream_record.hpp"

namespace payload::db {

/*
  Repository abstraction.

  CRITICAL GUARANTEES:

  - All writes require a Transaction
  - Reads inside a transaction see its writes
  - Version increments are atomic
  - Lease/state correctness depends on this behavior

  The DB is the source of truth for:
    payload state
    metadata
    lineage
*/

class Repository {
 public:
  virtual ~Repository() = default;

  // ---------------------------------------------------------------------
  // Transactions
  // ---------------------------------------------------------------------

  virtual std::unique_ptr<Transaction> Begin() = 0;

  // ---------------------------------------------------------------------
  // Payload lifecycle
  // ---------------------------------------------------------------------

  virtual Result InsertPayload(Transaction&, const model::PayloadRecord&) = 0;

  virtual std::optional<model::PayloadRecord> GetPayload(Transaction&, const std::string& id) = 0;

  virtual std::vector<model::PayloadRecord> ListPayloads(Transaction&) = 0;

  virtual Result UpdatePayload(Transaction&, const model::PayloadRecord&) = 0;

  virtual Result DeletePayload(Transaction&, const std::string& id) = 0;

  // ---------------------------------------------------------------------
  // Metadata (current snapshot)
  // ---------------------------------------------------------------------

  virtual Result UpsertMetadata(Transaction&, const model::MetadataRecord&) = 0;

  virtual std::optional<model::MetadataRecord> GetMetadata(Transaction&, const std::string& id) = 0;

  // ---------------------------------------------------------------------
  // Lineage
  // ---------------------------------------------------------------------

  virtual Result InsertLineage(Transaction&, const model::LineageRecord&) = 0;

  virtual std::vector<model::LineageRecord> GetParents(Transaction&, const std::string& id) = 0;

  virtual std::vector<model::LineageRecord> GetChildren(Transaction&, const std::string& id) = 0;

  // ---------------------------------------------------------------------
  // Streams
  // ---------------------------------------------------------------------

  virtual Result CreateStream(Transaction&, model::StreamRecord&) = 0;

  virtual std::optional<model::StreamRecord> GetStreamByName(Transaction&, const std::string& stream_namespace, const std::string& name) = 0;

  virtual std::optional<model::StreamRecord> GetStreamById(Transaction&, uint64_t stream_id) = 0;

  virtual Result DeleteStreamByName(Transaction&, const std::string& stream_namespace, const std::string& name) = 0;

  virtual Result DeleteStreamById(Transaction&, uint64_t stream_id) = 0;

  // Appends entries while assigning contiguous offsets for the stream.
  virtual Result AppendStreamEntries(Transaction&, uint64_t stream_id, std::vector<model::StreamEntryRecord>& entries) = 0;

  virtual std::vector<model::StreamEntryRecord> ReadStreamEntries(Transaction&, uint64_t stream_id, uint64_t start_offset,
                                                                  std::optional<uint64_t> max_entries,
                                                                  std::optional<uint64_t> min_append_time_ms) = 0;

  virtual std::vector<model::StreamEntryRecord> ReadStreamEntriesRange(Transaction&, uint64_t stream_id, uint64_t start_offset,
                                                                       uint64_t end_offset) = 0;

  virtual Result TrimStreamEntriesToMaxCount(Transaction&, uint64_t stream_id, uint64_t max_entries) = 0;

  virtual Result DeleteStreamEntriesOlderThan(Transaction&, uint64_t stream_id, uint64_t min_append_time_ms) = 0;

  virtual Result CommitConsumerOffset(Transaction&, const model::StreamConsumerOffsetRecord& record) = 0;

  virtual std::optional<model::StreamConsumerOffsetRecord> GetConsumerOffset(Transaction&, uint64_t stream_id, const std::string& consumer_group) = 0;
};

} // namespace payload::db
