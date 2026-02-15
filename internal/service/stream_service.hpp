#pragma once

#include <array>
#include <shared_mutex>
#include <string>
#include <vector>

#include "payload/manager/services/v1/payload_stream_service.pb.h"
#include "payload/manager/v1.hpp"
#include "service_context.hpp"

namespace payload::service {

class StreamService {
 public:
  explicit StreamService(ServiceContext ctx);

  void CreateStream(const payload::manager::v1::CreateStreamRequest& req);
  void DeleteStream(const payload::manager::v1::DeleteStreamRequest& req);

  payload::manager::v1::AppendResponse Append(const payload::manager::v1::AppendRequest& req);

  payload::manager::v1::ReadResponse Read(const payload::manager::v1::ReadRequest& req);

  std::vector<payload::manager::v1::SubscribeResponse> Subscribe(const payload::manager::v1::SubscribeRequest& req);

  void Commit(const payload::manager::v1::CommitRequest& req);

  payload::manager::v1::GetCommittedResponse GetCommitted(const payload::manager::v1::GetCommittedRequest& req);

  payload::manager::v1::GetRangeResponse GetRange(const payload::manager::v1::GetRangeRequest& req);

 private:
  static constexpr std::size_t kStreamLockShardCount = 64;

  static std::string Key(const payload::manager::v1::StreamID& stream);
  std::size_t        StreamShardIndex(const payload::manager::v1::StreamID& stream) const;

  std::shared_mutex& StreamShard(const payload::manager::v1::StreamID& stream);

  ServiceContext ctx_;
  // Locking strategy:
  // - global_mu_ protects stream lifecycle transitions (create/delete) from racing
  //   with per-stream operations.
  // - stream_mu_ shards coordinate stream-scoped read/write operations so work on
  //   different streams can proceed concurrently.
  // We always lock global_mu_ first and then a shard lock to avoid deadlocks.
  // This is especially important for the memory backend where transactions commit
  // by swapping a snapshot in memory (see internal/db/memory/memory_tx.cpp).
  std::shared_mutex                                global_mu_;
  std::array<std::shared_mutex, kStreamLockShardCount> stream_mu_;
};

} // namespace payload::service
