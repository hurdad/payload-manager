#include "stream_service.hpp"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <chrono>
#include <limits>
#include <optional>
#include <stdexcept>

#include "internal/db/api/repository.hpp"
#include "internal/db/model/stream_consumer_offset_record.hpp"
#include "internal/db/model/stream_entry_record.hpp"
#include "internal/db/model/stream_record.hpp"
#include "internal/util/time.hpp"
#include "internal/util/uuid.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

namespace {

bool IsTimestampSet(const google::protobuf::Timestamp& ts) {
  return ts.seconds() != 0 || ts.nanos() != 0;
}

uint64_t ToMillis(const google::protobuf::Timestamp& ts) {
  return payload::util::ToUnixMillis(payload::util::FromProto(ts));
}

google::protobuf::Timestamp FromMillis(uint64_t ms) {
  return payload::util::ToProto(payload::util::TimePoint{std::chrono::milliseconds(ms)});
}

void ThrowIfError(const payload::db::Result& result, const std::string& prefix) {
  if (!result) {
    throw std::runtime_error(prefix + ": " + result.message);
  }
}

std::string SerializeTags(const google::protobuf::Map<std::string, std::string>& tags) {
  google::protobuf::Struct as_struct;
  for (const auto& [key, value] : tags) {
    (*as_struct.mutable_fields())[key].set_string_value(value);
  }

  std::string json;
  google::protobuf::util::MessageToJsonString(as_struct, &json);
  return json;
}

void DeserializeTags(const std::string& raw, google::protobuf::Map<std::string, std::string>* tags) {
  if (raw.empty()) {
    return;
  }

  google::protobuf::Struct as_struct;
  if (!google::protobuf::util::JsonStringToMessage(raw, &as_struct).ok()) {
    return;
  }

  for (const auto& [key, value] : as_struct.fields()) {
    if (value.kind_case() == google::protobuf::Value::kStringValue) {
      (*tags)[key] = value.string_value();
    }
  }
}

payload::db::model::StreamEntryRecord ToRecord(const AppendItem& item) {
  payload::db::model::StreamEntryRecord record;
  record.payload_uuid = payload::util::ToString(payload::util::FromProto(item.payload_id()));
  if (item.has_event_time()) {
    record.event_time_ms = ToMillis(item.event_time());
  }
  record.duration_ns = item.duration_ns();
  record.tags        = SerializeTags(item.tags());
  return record;
}

StreamEntry ToProtoEntry(const StreamID& stream, const payload::db::model::StreamEntryRecord& record) {
  StreamEntry entry;
  *entry.mutable_stream() = stream;
  entry.set_offset(record.offset);
  *entry.mutable_payload_id() = payload::util::ToProto(payload::util::FromString(record.payload_uuid));
  if (record.event_time_ms > 0) {
    *entry.mutable_event_time() = FromMillis(record.event_time_ms);
  }
  *entry.mutable_append_time() = FromMillis(record.append_time_ms);
  entry.set_duration_ns(record.duration_ns);
  DeserializeTags(record.tags, entry.mutable_tags());
  return entry;
}

payload::db::model::StreamRecord GetStreamOrThrow(payload::db::Repository& repo, payload::db::Transaction& tx, const StreamID& stream,
                                                  const std::string& op) {
  auto stream_record = repo.GetStreamByName(tx, stream.namespace_(), stream.name());
  if (!stream_record.has_value()) {
    throw std::runtime_error(op + ": stream not found");
  }
  return *stream_record;
}

} // namespace

StreamService::StreamService(ServiceContext ctx) : ctx_(std::move(ctx)) {
}

std::string StreamService::Key(const StreamID& stream) {
  return stream.namespace_() + "/" + stream.name();
}

void StreamService::CreateStream(const CreateStreamRequest& req) {
  if (!req.has_stream() || req.stream().name().empty()) {
    throw std::runtime_error("create stream: missing stream name");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  auto                        tx = ctx_.repository->Begin();

  if (ctx_.repository->GetStreamByName(*tx, req.stream().namespace_(), req.stream().name()).has_value()) {
    throw std::runtime_error("create stream: stream already exists");
  }

  payload::db::model::StreamRecord stream;
  stream.stream_namespace      = req.stream().namespace_();
  stream.name                  = req.stream().name();
  stream.retention_max_entries = req.retention_max_entries();
  stream.retention_max_age_sec = req.retention_max_age_sec();

  ThrowIfError(ctx_.repository->CreateStream(*tx, stream), "create stream");
  tx->Commit();
}

void StreamService::DeleteStream(const DeleteStreamRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto                        tx = ctx_.repository->Begin();

  ThrowIfError(ctx_.repository->DeleteStreamByName(*tx, req.stream().namespace_(), req.stream().name()), "delete stream");
  tx->Commit();
}

AppendResponse StreamService::Append(const AppendRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto                        tx = ctx_.repository->Begin();

  const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "append");

  AppendResponse resp;
  if (req.items().empty()) {
    return resp;
  }

  std::vector<payload::db::model::StreamEntryRecord> records;
  records.reserve(req.items().size());
  for (const auto& item : req.items()) {
    records.push_back(ToRecord(item));
  }

  ThrowIfError(ctx_.repository->AppendStreamEntries(*tx, stream.stream_id, records), "append");

  if (stream.retention_max_entries > 0) {
    ThrowIfError(ctx_.repository->TrimStreamEntriesToMaxCount(*tx, stream.stream_id, stream.retention_max_entries), "append retention max entries");
  }

  if (stream.retention_max_age_sec > 0) {
    const auto now_ms       = payload::util::ToUnixMillis(payload::util::Now());
    const auto retention_ms = stream.retention_max_age_sec * 1000;
    const auto cutoff_ms    = now_ms > retention_ms ? now_ms - retention_ms : 0;
    ThrowIfError(ctx_.repository->DeleteStreamEntriesOlderThan(*tx, stream.stream_id, cutoff_ms), "append retention max age");
  }

  tx->Commit();

  resp.set_first_offset(records.front().offset);
  resp.set_last_offset(records.back().offset);
  return resp;
}

ReadResponse StreamService::Read(const ReadRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto                        tx = ctx_.repository->Begin();

  const auto stream      = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "read");
  const auto max_entries = req.max_entries() == 0 ? std::optional<uint64_t>{} : std::make_optional<uint64_t>(req.max_entries());

  const auto min_append_time =
      IsTimestampSet(req.not_before()) ? std::make_optional<uint64_t>(ToMillis(req.not_before())) : std::optional<uint64_t>{};

  const auto entries = ctx_.repository->ReadStreamEntries(*tx, stream.stream_id, req.start_offset(), max_entries, min_append_time);

  ReadResponse resp;
  for (const auto& entry : entries) {
    *resp.add_entries() = ToProtoEntry(req.stream(), entry);
  }
  return resp;
}

std::vector<SubscribeResponse> StreamService::Subscribe(const SubscribeRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto                        tx = ctx_.repository->Begin();

  const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "subscribe");

  uint64_t start_offset = 0;
  if (req.has_offset()) {
    start_offset = req.offset();
  } else if (req.has_from_latest() && req.from_latest()) {
    const auto last = ctx_.repository->ReadStreamEntries(*tx, stream.stream_id, 0, std::optional<uint64_t>{}, std::nullopt);
    if (!last.empty()) {
      start_offset = last.back().offset + 1;
    }
  }

  const auto entries = ctx_.repository->ReadStreamEntries(*tx, stream.stream_id, start_offset, std::optional<uint64_t>{}, std::nullopt);

  std::vector<SubscribeResponse> responses;
  responses.reserve(entries.size());
  for (const auto& entry : entries) {
    SubscribeResponse response;
    *response.mutable_entry() = ToProtoEntry(req.stream(), entry);
    responses.push_back(std::move(response));
  }

  return responses;
}

void StreamService::Commit(const CommitRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto                        tx = ctx_.repository->Begin();

  const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "commit");

  payload::db::model::StreamConsumerOffsetRecord offset;
  offset.stream_id      = stream.stream_id;
  offset.consumer_group = req.consumer_group();
  offset.offset         = req.offset();
  ThrowIfError(ctx_.repository->CommitConsumerOffset(*tx, offset), "commit");
  tx->Commit();
}

GetCommittedResponse StreamService::GetCommitted(const GetCommittedRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto                        tx = ctx_.repository->Begin();

  const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "get committed");

  GetCommittedResponse resp;
  const auto           committed = ctx_.repository->GetConsumerOffset(*tx, stream.stream_id, req.consumer_group());
  if (committed.has_value()) {
    resp.set_offset(committed->offset);
  }
  return resp;
}

GetRangeResponse StreamService::GetRange(const GetRangeRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto                        tx = ctx_.repository->Begin();

  const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "get range");

  const auto entries = ctx_.repository->ReadStreamEntriesRange(*tx, stream.stream_id, req.start_offset(), req.end_offset());

  GetRangeResponse resp;
  for (const auto& entry : entries) {
    *resp.add_entries() = ToProtoEntry(req.stream(), entry);
  }
  return resp;
}

} // namespace payload::service
