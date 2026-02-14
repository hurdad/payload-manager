#pragma once

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

  void Commit(const payload::manager::v1::CommitRequest& req);

  payload::manager::v1::GetCommittedResponse
  GetCommitted(const payload::manager::v1::GetCommittedRequest& req);

  payload::manager::v1::GetRangeResponse
  GetRange(const payload::manager::v1::GetRangeRequest& req);

private:
  ServiceContext ctx_;
};

} // namespace payload::service
