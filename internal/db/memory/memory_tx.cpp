#include "memory_tx.hpp"

#include <stdexcept>

namespace payload::db::memory {

MemoryTransaction::MemoryTransaction(MemoryRepository& repo) : repo_(repo) {
  std::scoped_lock lock(repo_.mutex_);
  working_ = repo_.committed_; // snapshot copy
}

MemoryTransaction::~MemoryTransaction() {
  if (!committed_ && !rolled_back_) Rollback();
}

void MemoryTransaction::Commit() {
  std::scoped_lock lock(repo_.mutex_);

  // Per-key merge for payloads: apply only the keys this transaction touched
  // so that concurrent transactions on different keys never conflict.
  // (Mirrors the row-level isolation of the real SQLite/Postgres back-ends.)
  for (const auto& id : modified_payload_ids_) {
    auto it = working_.payloads.find(id);
    if (it != working_.payloads.end()) {
      repo_.committed_.payloads[id] = it->second;
    }
  }
  for (const auto& id : deleted_payload_ids_) {
    repo_.committed_.payloads.erase(id);
  }

  // Per-key merge for metadata.
  for (const auto& id : modified_metadata_ids_) {
    auto it = working_.metadata.find(id);
    if (it != working_.metadata.end()) {
      repo_.committed_.metadata[id] = it->second;
    }
  }
  for (const auto& id : deleted_metadata_ids_) {
    repo_.committed_.metadata.erase(id);
  }

  // Append-only collections: merge new entries that are not already present.
  // metadata_events and lineage are append-only; stream state is rarely written
  // concurrently, so last-write-wins from working_ is acceptable.
  const auto old_event_count = repo_.committed_.metadata_events.size();
  for (auto i = old_event_count; i < working_.metadata_events.size(); ++i) {
    repo_.committed_.metadata_events.push_back(working_.metadata_events[i]);
  }
  const auto old_lineage_count = repo_.committed_.lineage.size();
  for (auto i = old_lineage_count; i < working_.lineage.size(); ++i) {
    repo_.committed_.lineage.push_back(working_.lineage[i]);
  }

  // Stream state: merge streams / entries that this transaction modified by
  // replacing only keys present in the working_ diff vs. the snapshot.
  // For simplicity, replace the entire stream state (streams are rarely
  // written concurrently in practice).
  repo_.committed_.streams            = working_.streams;
  repo_.committed_.stream_name_to_id  = working_.stream_name_to_id;
  repo_.committed_.stream_entries     = working_.stream_entries;
  repo_.committed_.consumer_offsets   = working_.consumer_offsets;
  repo_.committed_.next_stream_offset = working_.next_stream_offset;
  repo_.committed_.next_stream_id     = working_.next_stream_id;

  repo_.committed_version_++;
  committed_ = true;
}

void MemoryTransaction::Rollback() {
  rolled_back_ = true;
}

} // namespace payload::db::memory