#include "stream_service.hpp"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <chrono>
#include <limits>
#include <optional>
#include <stdexcept>
#include <type_traits>

#include "internal/db/api/repository.hpp"
#include "internal/db/model/stream_consumer_offset_record.hpp"
#include "internal/db/model/stream_entry_record.hpp"
#include "internal/db/model/stream_record.hpp"
#include "internal/observability/logging.hpp"
#include "internal/observability/spans.hpp"
#include "internal/util/errors.hpp"
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
    throw payload::util::NotFound(op + ": stream not found; create the stream before retrying");
  }
  return *stream_record;
}

template <typename Fn>
auto ObserveRpc(std::string_view route, const StreamID* stream_id, const PayloadID* payload_id, Fn&& fn) {
  payload::observability::SpanScope span(route);
  if (stream_id) {
    span.SetAttribute("stream.namespace", stream_id->namespace_());
    span.SetAttribute("stream.name", stream_id->name());
  }
  if (payload_id) {
    span.SetAttribute("payload.id", payload_id->value());
  }

  const auto started_at = std::chrono::steady_clock::now();
  try {
    if constexpr (std::is_void_v<std::invoke_result_t<Fn>>) {
      fn();
      payload::observability::Metrics::Instance().RecordRequest(route, true);
      payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
          route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
      return;
    } else {
      auto result = fn();
      payload::observability::Metrics::Instance().RecordRequest(route, true);
      payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
          route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
      return result;
    }
  } catch (const std::exception& ex) {
    span.RecordException(ex.what());
    PAYLOAD_LOG_ERROR("RPC failed", {payload::observability::StringField("route", route), payload::observability::StringField("error", ex.what()),
                                     stream_id ? payload::observability::StringField("stream", stream_id->namespace_() + "/" + stream_id->name())
                                               : payload::observability::StringField("stream", ""),
                                     payload_id ? payload::observability::StringField("payload_id", payload_id->value())
                                                : payload::observability::StringField("payload_id", "")});
    payload::observability::Metrics::Instance().RecordRequest(route, false);
    payload::observability::Metrics::Instance().ObserveRequestLatencyMs(
        route, std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_at).count());
    throw;
  }
}

} // namespace

StreamService::StreamService(ServiceContext ctx) : ctx_(std::move(ctx)) {
}

std::string StreamService::Key(const StreamID& stream) {
  return stream.namespace_() + "/" + stream.name();
}

void StreamService::CreateStream(const CreateStreamRequest& req) {
  ObserveRpc("StreamService.CreateStream", req.has_stream() ? &req.stream() : nullptr, nullptr, [&] {
    if (!req.has_stream() || req.stream().name().empty()) {
      throw payload::util::InvalidState("create stream: missing stream name; set stream.name and retry");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto                        tx = ctx_.repository->Begin();

    if (ctx_.repository->GetStreamByName(*tx, req.stream().namespace_(), req.stream().name()).has_value()) {
      throw payload::util::AlreadyExists("create stream: stream already exists; choose a different stream name or delete existing stream");
    }

    payload::db::model::StreamRecord stream;
    stream.stream_namespace      = req.stream().namespace_();
    stream.name                  = req.stream().name();
    stream.retention_max_entries = req.retention_max_entries();
    stream.retention_max_age_sec = req.retention_max_age_sec();

    ThrowIfError(ctx_.repository->CreateStream(*tx, stream), "create stream");
    tx->Commit();
  });
}

void StreamService::DeleteStream(const DeleteStreamRequest& req) {
  ObserveRpc("StreamService.DeleteStream", &req.stream(), nullptr, [&] {
    if (!req.has_stream() || req.stream().name().empty()) {
      throw payload::util::InvalidState("delete stream: missing stream name; set stream.name and retry");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto                        tx = ctx_.repository->Begin();

    ThrowIfError(ctx_.repository->DeleteStreamByName(*tx, req.stream().namespace_(), req.stream().name()), "delete stream");
    tx->Commit();
  });
}

AppendResponse StreamService::Append(const AppendRequest& req) {
  const PayloadID* payload_id = req.items().empty() ? nullptr : &req.items().begin()->payload_id();
  return ObserveRpc("StreamService.Append", &req.stream(), payload_id, [&] {
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
  });
}

ReadResponse StreamService::Read(const ReadRequest& req) {
  return ObserveRpc("StreamService.Read", &req.stream(), nullptr, [&] {
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
  });
}

std::vector<SubscribeResponse> StreamService::Subscribe(const SubscribeRequest& req) {
  return ObserveRpc("StreamService.Subscribe", &req.stream(), nullptr, [&] {
    std::lock_guard<std::mutex> lock(mutex_);
    auto                        tx = ctx_.repository->Begin();

    const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "subscribe");

    uint64_t start_offset = 0;
    if (req.has_offset()) {
      start_offset = req.offset();
    } else if (req.has_from_latest() && req.from_latest()) {
      const auto max_offset = ctx_.repository->GetMaxStreamOffset(*tx, stream.stream_id);
      if (max_offset.has_value()) {
        start_offset = *max_offset + 1;
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
  });
}

void StreamService::Commit(const CommitRequest& req) {
  ObserveRpc("StreamService.Commit", &req.stream(), nullptr, [&] {
    std::lock_guard<std::mutex> lock(mutex_);
    auto                        tx = ctx_.repository->Begin();

    const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "commit");

    payload::db::model::StreamConsumerOffsetRecord offset;
    offset.stream_id      = stream.stream_id;
    offset.consumer_group = req.consumer_group();
    offset.offset         = req.offset();
    ThrowIfError(ctx_.repository->CommitConsumerOffset(*tx, offset), "commit");
    tx->Commit();
  });
}

GetCommittedResponse StreamService::GetCommitted(const GetCommittedRequest& req) {
  return ObserveRpc("StreamService.GetCommitted", &req.stream(), nullptr, [&] {
    std::lock_guard<std::mutex> lock(mutex_);
    auto                        tx = ctx_.repository->Begin();

    const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "get committed");

    GetCommittedResponse resp;
    const auto           committed = ctx_.repository->GetConsumerOffset(*tx, stream.stream_id, req.consumer_group());
    if (committed.has_value()) {
      resp.set_offset(committed->offset);
    }
    return resp;
  });
}

GetRangeResponse StreamService::GetRange(const GetRangeRequest& req) {
  return ObserveRpc("StreamService.GetRange", &req.stream(), nullptr, [&] {
    std::lock_guard<std::mutex> lock(mutex_);
    auto                        tx = ctx_.repository->Begin();

    const auto stream = GetStreamOrThrow(*ctx_.repository, *tx, req.stream(), "get range");

    const auto entries = ctx_.repository->ReadStreamEntriesRange(*tx, stream.stream_id, req.start_offset(), req.end_offset());

    GetRangeResponse resp;
    for (const auto& entry : entries) {
      *resp.add_entries() = ToProtoEntry(req.stream(), entry);
    }
    return resp;
  });
}

} // namespace payload::service
