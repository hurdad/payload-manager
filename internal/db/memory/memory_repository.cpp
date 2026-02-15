#include "memory_repository.hpp"

#include <algorithm>
#include <chrono>

#include "memory_tx.hpp"

namespace payload::db::memory {

namespace {

uint64_t NowMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

std::string StreamNameKey(const std::string& stream_namespace, const std::string& name) {
  return stream_namespace + "#" + name;
}

std::string OffsetKey(uint64_t stream_id, const std::string& consumer_group) {
  return std::to_string(stream_id) + "#" + consumer_group;
}

} // namespace

MemoryRepository::MemoryRepository() = default;

std::unique_ptr<db::Transaction> MemoryRepository::Begin() {
  return std::make_unique<MemoryTransaction>(*this);
}

static MemoryTransaction& TX(db::Transaction& tx) {
  return static_cast<MemoryTransaction&>(tx);
}

Result MemoryRepository::InsertPayload(Transaction& t, const model::PayloadRecord& r) {
  auto& s = TX(t).Mutable();
  if (s.payloads.contains(r.id)) return Result::Err(ErrorCode::AlreadyExists);
  s.payloads[r.id] = r;
  return Result::Ok();
}

std::optional<model::PayloadRecord> MemoryRepository::GetPayload(Transaction& t, const std::string& id) {
  auto& s  = TX(t).Mutable();
  auto  it = s.payloads.find(id);
  if (it == s.payloads.end()) return std::nullopt;
  return it->second;
}

std::vector<model::PayloadRecord> MemoryRepository::ListPayloads(Transaction& t) {
  const auto&                       s = TX(t).View();
  std::vector<model::PayloadRecord> records;
  records.reserve(s.payloads.size());
  for (const auto& [_, record] : s.payloads) {
    records.push_back(record);
  }
  return records;
}

Result MemoryRepository::UpdatePayload(Transaction& t, const model::PayloadRecord& r) {
  auto& s = TX(t).Mutable();
  if (!s.payloads.contains(r.id)) return Result::Err(ErrorCode::NotFound);
  s.payloads[r.id] = r;
  return Result::Ok();
}

Result MemoryRepository::DeletePayload(Transaction& t, const std::string& id) {
  auto& s = TX(t).Mutable();
  s.payloads.erase(id);
  s.metadata.erase(id);
  return Result::Ok();
}

Result MemoryRepository::UpsertMetadata(Transaction& t, const model::MetadataRecord& r) {
  TX(t).Mutable().metadata[r.id] = r;
  return Result::Ok();
}

std::optional<model::MetadataRecord> MemoryRepository::GetMetadata(Transaction& t, const std::string& id) {
  auto& s  = TX(t).Mutable();
  auto  it = s.metadata.find(id);
  if (it == s.metadata.end()) return std::nullopt;
  return it->second;
}

Result MemoryRepository::InsertLineage(Transaction& t, const model::LineageRecord& r) {
  TX(t).Mutable().lineage.push_back(r);
  return Result::Ok();
}

std::vector<model::LineageRecord> MemoryRepository::GetParents(Transaction& t, const std::string& id) {
  std::vector<model::LineageRecord> out;
  for (auto& e : TX(t).View().lineage)
    if (e.child_id == id) out.push_back(e);
  return out;
}

std::vector<model::LineageRecord> MemoryRepository::GetChildren(Transaction& t, const std::string& id) {
  std::vector<model::LineageRecord> out;
  for (auto& e : TX(t).View().lineage)
    if (e.parent_id == id) out.push_back(e);
  return out;
}

Result MemoryRepository::CreateStream(Transaction& t, model::StreamRecord& r) {
  auto&      s   = TX(t).Mutable();
  const auto key = StreamNameKey(r.stream_namespace, r.name);
  if (s.stream_name_to_id.contains(key)) {
    return Result::Err(ErrorCode::AlreadyExists);
  }

  if (r.stream_id == 0) {
    r.stream_id = s.next_stream_id++;
  } else {
    s.next_stream_id = std::max(s.next_stream_id, r.stream_id + 1);
  }
  if (r.created_at_ms == 0) {
    r.created_at_ms = NowMs();
  }

  s.streams[r.stream_id]   = r;
  s.stream_name_to_id[key] = r.stream_id;
  s.next_stream_offset.try_emplace(r.stream_id, 0);
  return Result::Ok();
}

std::optional<model::StreamRecord> MemoryRepository::GetStreamByName(Transaction& t, const std::string& stream_namespace, const std::string& name) {
  auto&      s   = TX(t).View();
  const auto key = StreamNameKey(stream_namespace, name);
  const auto it  = s.stream_name_to_id.find(key);
  if (it == s.stream_name_to_id.end()) {
    return std::nullopt;
  }
  return GetStreamById(t, it->second);
}

std::optional<model::StreamRecord> MemoryRepository::GetStreamById(Transaction& t, uint64_t stream_id) {
  auto&      s  = TX(t).View();
  const auto it = s.streams.find(stream_id);
  if (it == s.streams.end()) {
    return std::nullopt;
  }
  return it->second;
}

Result MemoryRepository::DeleteStreamByName(Transaction& t, const std::string& stream_namespace, const std::string& name) {
  auto&      s   = TX(t).Mutable();
  const auto key = StreamNameKey(stream_namespace, name);
  const auto it  = s.stream_name_to_id.find(key);
  if (it == s.stream_name_to_id.end()) {
    return Result::Ok();
  }
  return DeleteStreamById(t, it->second);
}

Result MemoryRepository::DeleteStreamById(Transaction& t, uint64_t stream_id) {
  auto&      s   = TX(t).Mutable();
  const auto sit = s.streams.find(stream_id);
  if (sit == s.streams.end()) {
    return Result::Ok();
  }

  s.stream_name_to_id.erase(StreamNameKey(sit->second.stream_namespace, sit->second.name));
  s.streams.erase(stream_id);
  s.stream_entries.erase(stream_id);
  s.next_stream_offset.erase(stream_id);

  for (auto it = s.consumer_offsets.begin(); it != s.consumer_offsets.end();) {
    if (it->second.stream_id == stream_id) {
      it = s.consumer_offsets.erase(it);
    } else {
      ++it;
    }
  }
  return Result::Ok();
}

Result MemoryRepository::AppendStreamEntries(Transaction& t, uint64_t stream_id, std::vector<model::StreamEntryRecord>& entries) {
  auto& s = TX(t).Mutable();
  if (!s.streams.contains(stream_id)) {
    return Result::Err(ErrorCode::NotFound);
  }

  uint64_t next_offset    = s.next_stream_offset[stream_id];
  auto&    stream_entries = s.stream_entries[stream_id];
  for (auto& entry : entries) {
    entry.stream_id = stream_id;
    entry.offset    = next_offset++;
    if (entry.append_time_ms == 0) {
      entry.append_time_ms = NowMs();
    }
    stream_entries.push_back(entry);
  }
  s.next_stream_offset[stream_id] = next_offset;
  return Result::Ok();
}

std::vector<model::StreamEntryRecord> MemoryRepository::ReadStreamEntries(Transaction& t, uint64_t stream_id, uint64_t start_offset,
                                                                          std::optional<uint64_t> max_entries,
                                                                          std::optional<uint64_t> min_append_time_ms) {
  std::vector<model::StreamEntryRecord> out;
  const auto&                           s  = TX(t).View();
  const auto                            it = s.stream_entries.find(stream_id);
  if (it == s.stream_entries.end()) {
    return out;
  }

  for (const auto& entry : it->second) {
    if (entry.offset < start_offset) {
      continue;
    }
    if (min_append_time_ms.has_value() && entry.append_time_ms < *min_append_time_ms) {
      continue;
    }
    out.push_back(entry);
    if (max_entries.has_value() && out.size() >= *max_entries) {
      break;
    }
  }
  return out;
}

std::optional<uint64_t> MemoryRepository::GetMaxStreamOffset(Transaction& t, uint64_t stream_id) {
  const auto& s  = TX(t).View();
  const auto  it = s.stream_entries.find(stream_id);
  if (it == s.stream_entries.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second.back().offset;
}

std::vector<model::StreamEntryRecord> MemoryRepository::ReadStreamEntriesRange(Transaction& t, uint64_t stream_id, uint64_t start_offset,
                                                                               uint64_t end_offset) {
  std::vector<model::StreamEntryRecord> out;
  const auto&                           s  = TX(t).View();
  const auto                            it = s.stream_entries.find(stream_id);
  if (it == s.stream_entries.end()) {
    return out;
  }

  for (const auto& entry : it->second) {
    if (entry.offset < start_offset || entry.offset > end_offset) {
      continue;
    }
    out.push_back(entry);
  }
  return out;
}

Result MemoryRepository::TrimStreamEntriesToMaxCount(Transaction& t, uint64_t stream_id, uint64_t max_entries) {
  auto& s  = TX(t).Mutable();
  auto  it = s.stream_entries.find(stream_id);
  if (it == s.stream_entries.end() || max_entries == 0) {
    return Result::Ok();
  }

  auto& entries = it->second;
  if (entries.size() <= max_entries) {
    return Result::Ok();
  }

  const auto remove_count = entries.size() - static_cast<size_t>(max_entries);
  entries.erase(entries.begin(), entries.begin() + static_cast<std::ptrdiff_t>(remove_count));
  return Result::Ok();
}

Result MemoryRepository::DeleteStreamEntriesOlderThan(Transaction& t, uint64_t stream_id, uint64_t min_append_time_ms) {
  auto& s  = TX(t).Mutable();
  auto  it = s.stream_entries.find(stream_id);
  if (it == s.stream_entries.end()) {
    return Result::Ok();
  }

  auto& entries = it->second;
  entries.erase(std::remove_if(entries.begin(), entries.end(),
                               [&](const model::StreamEntryRecord& entry) { return entry.append_time_ms < min_append_time_ms; }),
                entries.end());
  return Result::Ok();
}

Result MemoryRepository::CommitConsumerOffset(Transaction& t, const model::StreamConsumerOffsetRecord& record) {
  auto& s = TX(t).Mutable();
  if (!s.streams.contains(record.stream_id)) {
    return Result::Err(ErrorCode::NotFound);
  }

  auto updated = record;
  if (updated.updated_at_ms == 0) {
    updated.updated_at_ms = NowMs();
  }
  s.consumer_offsets[OffsetKey(updated.stream_id, updated.consumer_group)] = std::move(updated);
  return Result::Ok();
}

std::optional<model::StreamConsumerOffsetRecord> MemoryRepository::GetConsumerOffset(Transaction& t, uint64_t stream_id,
                                                                                     const std::string& consumer_group) {
  const auto& s  = TX(t).View();
  const auto  it = s.consumer_offsets.find(OffsetKey(stream_id, consumer_group));
  if (it == s.consumer_offsets.end()) {
    return std::nullopt;
  }
  return it->second;
}

} // namespace payload::db::memory
