#pragma once

#include <memory>

#include <grpcpp/grpcpp.h>

#include "internal/service/stream_service.hpp"
#include "payload/manager/services/v1/payload_stream_service.grpc.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

class StreamServer final : public payload::manager::v1::PayloadStreamService::Service {
public:
  explicit StreamServer(std::shared_ptr<payload::service::StreamService> svc);

  ::grpc::Status CreateStream(::grpc::ServerContext*,
                              const payload::manager::v1::CreateStreamRequest*,
                              google::protobuf::Empty*) override;

  ::grpc::Status DeleteStream(::grpc::ServerContext*,
                              const payload::manager::v1::DeleteStreamRequest*,
                              google::protobuf::Empty*) override;

  ::grpc::Status Append(::grpc::ServerContext*,
                        const payload::manager::v1::AppendRequest*,
                        payload::manager::v1::AppendResponse*) override;

  ::grpc::Status Read(::grpc::ServerContext*,
                      const payload::manager::v1::ReadRequest*,
                      payload::manager::v1::ReadResponse*) override;

  ::grpc::Status Subscribe(::grpc::ServerContext*,
                           const payload::manager::v1::SubscribeRequest*,
                           ::grpc::ServerWriter<payload::manager::v1::SubscribeResponse>*) override;

  ::grpc::Status Commit(::grpc::ServerContext*,
                        const payload::manager::v1::CommitRequest*,
                        google::protobuf::Empty*) override;

  ::grpc::Status GetCommitted(::grpc::ServerContext*,
                              const payload::manager::v1::GetCommittedRequest*,
                              payload::manager::v1::GetCommittedResponse*) override;

  ::grpc::Status GetRange(::grpc::ServerContext*,
                          const payload::manager::v1::GetRangeRequest*,
                          payload::manager::v1::GetRangeResponse*) override;

private:
  std::shared_ptr<payload::service::StreamService> service_;
};

} // namespace payload::grpc
