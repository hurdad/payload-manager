#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "payload/manager/services/v1/payload_stream_service.pb.h"
#include "service_context.hpp"
#include "payload/manager/v1.hpp"

namespace payload::service {

class StreamService {
public:
  explicit StreamService(ServiceContext ctx);

  void CreateStream(const payload::manager::v1::CreateStreamRequest& req);
  void DeleteStream(const payload::manager::v1::DeleteStreamRequest& req);

  payload::manager::v1::AppendResponse
  Append(const payload::manager::v1::AppendRequest& req);

  payload::manager::v1::ReadResponse
  Read(const payload::manager::v1::ReadRequest& req);

  std::vector<payload::manager::v1::SubscribeResponse>
  Subscribe(const payload::manager::v1::SubscribeRequest& req);

  void Commit(const payload::manager::v1::CommitRequest& req);

  payload::manager::v1::GetCommittedResponse
  GetCommitted(const payload::manager::v1::GetCommittedRequest& req);

  payload::manager::v1::GetRangeResponse
  GetRange(const payload::manager::v1::GetRangeRequest& req);

private:
  struct StreamState {
    uint64_t next_offset = 0;
    uint64_t retention_max_entries = 0;
    uint64_t retention_max_age_sec = 0;
    std::vector<payload::manager::v1::StreamEntry> entries;
  };

  static std::string Key(const payload::manager::v1::StreamID& stream);

  ServiceContext ctx_;
  std::mutex mutex_;
  std::unordered_map<std::string, StreamState> streams_;
  std::unordered_map<std::string, uint64_t> committed_offsets_;
};

} // namespace payload::service
