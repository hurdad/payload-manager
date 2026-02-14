#include "stream_service.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include "payload/manager/v1.hpp"

namespace payload::service {

using namespace payload::manager::v1;

namespace {

bool IsTimestampSet(const google::protobuf::Timestamp& ts) {
  return ts.seconds() != 0 || ts.nanos() != 0;
}

bool IsAtOrAfter(const google::protobuf::Timestamp& lhs,
                 const google::protobuf::Timestamp& rhs) {
  return lhs.seconds() > rhs.seconds() ||
         (lhs.seconds() == rhs.seconds() && lhs.nanos() >= rhs.nanos());
}

} // namespace

StreamService::StreamService(ServiceContext ctx)
    : ctx_(std::move(ctx)) {}

std::string StreamService::Key(const StreamID& stream) {
  return stream.namespace_() + "/" + stream.name();
}

void StreamService::CreateStream(const CreateStreamRequest& req) {
  if (!req.has_stream() || req.stream().name().empty()) {
    throw std::runtime_error("create stream: missing stream name");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  const auto key = Key(req.stream());
  if (streams_.contains(key)) {
    throw std::runtime_error("create stream: stream already exists");
  }

  StreamState state;
  state.retention_max_entries = req.retention_max_entries();
  state.retention_max_age_sec = req.retention_max_age_sec();
  streams_.emplace(key, std::move(state));
}

void StreamService::DeleteStream(const DeleteStreamRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto key = Key(req.stream());
  streams_.erase(key);

  const auto prefix = key + "#";
  for (auto it = committed_offsets_.begin(); it != committed_offsets_.end();) {
    if (it->first.rfind(prefix, 0) == 0) {
      it = committed_offsets_.erase(it);
    } else {
      ++it;
    }
  }
}

AppendResponse StreamService::Append(const AppendRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto key = Key(req.stream());
  auto it = streams_.find(key);
  if (it == streams_.end()) {
    throw std::runtime_error("append: stream not found");
  }

  AppendResponse resp;
  if (req.items().empty()) {
    return resp;
  }

  const auto first_offset = it->second.next_offset;
  for (const auto& item : req.items()) {
    StreamEntry entry;
    *entry.mutable_stream() = req.stream();
    entry.set_offset(it->second.next_offset++);
    *entry.mutable_payload_id() = item.payload_id();
    *entry.mutable_event_time() = item.event_time();
    entry.set_duration_ns(item.duration_ns());
    *entry.mutable_tags() = item.tags();
    it->second.entries.push_back(std::move(entry));
  }

  if (it->second.retention_max_entries > 0) {
    while (it->second.entries.size() > it->second.retention_max_entries) {
      it->second.entries.erase(it->second.entries.begin());
    }
  }

  resp.set_first_offset(first_offset);
  resp.set_last_offset(it->second.next_offset - 1);
  return resp;
}

ReadResponse StreamService::Read(const ReadRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto key = Key(req.stream());
  auto it = streams_.find(key);
  if (it == streams_.end()) {
    throw std::runtime_error("read: stream not found");
  }

  ReadResponse resp;
  const auto max_entries = req.max_entries() == 0
                               ? std::numeric_limits<size_t>::max()
                               : static_cast<size_t>(req.max_entries());

  const auto use_time_filter = IsTimestampSet(req.not_before());
  for (const auto& entry : it->second.entries) {
    if (entry.offset() < req.start_offset()) {
      continue;
    }
    if (use_time_filter &&
        (!entry.has_event_time() || !IsAtOrAfter(entry.event_time(), req.not_before()))) {
      continue;
    }
    *resp.add_entries() = entry;
    if (resp.entries_size() >= static_cast<int>(max_entries)) {
      break;
    }
  }

  return resp;
}

std::vector<SubscribeResponse>
StreamService::Subscribe(const SubscribeRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto key = Key(req.stream());
  auto it = streams_.find(key);
  if (it == streams_.end()) {
    throw std::runtime_error("subscribe: stream not found");
  }

  uint64_t start_offset = 0;
  if (req.has_offset()) {
    start_offset = req.offset();
  } else if (req.has_from_latest() && req.from_latest()) {
    start_offset = it->second.next_offset;
  }

  std::vector<SubscribeResponse> responses;
  responses.reserve(it->second.entries.size());
  for (const auto& entry : it->second.entries) {
    if (entry.offset() < start_offset) {
      continue;
    }
    SubscribeResponse response;
    *response.mutable_entry() = entry;
    responses.push_back(std::move(response));
  }

  return responses;
}

void StreamService::Commit(const CommitRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto key = Key(req.stream());
  if (!streams_.contains(key)) {
    throw std::runtime_error("commit: stream not found");
  }

  committed_offsets_[key + "#" + req.consumer_group()] = req.offset();
}

GetCommittedResponse StreamService::GetCommitted(const GetCommittedRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto key = Key(req.stream());
  if (!streams_.contains(key)) {
    throw std::runtime_error("get committed: stream not found");
  }

  GetCommittedResponse resp;
  const auto checkpoint_key = key + "#" + req.consumer_group();
  if (auto it = committed_offsets_.find(checkpoint_key); it != committed_offsets_.end()) {
    resp.set_offset(it->second);
  }
  return resp;
}

GetRangeResponse StreamService::GetRange(const GetRangeRequest& req) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto key = Key(req.stream());
  auto it = streams_.find(key);
  if (it == streams_.end()) {
    throw std::runtime_error("get range: stream not found");
  }

  GetRangeResponse resp;
  for (const auto& entry : it->second.entries) {
    if (entry.offset() < req.start_offset() || entry.offset() > req.end_offset()) {
      continue;
    }
    *resp.add_entries() = entry;
  }
  return resp;
}

} // namespace payload::service
