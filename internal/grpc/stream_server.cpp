#include "stream_server.hpp"

#include "grpc_error.hpp"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

StreamServer::StreamServer(std::shared_ptr<payload::service::StreamService> svc)
    : service_(std::move(svc)) {}

::grpc::Status StreamServer::CreateStream(::grpc::ServerContext*,
                                          const payload::manager::v1::CreateStreamRequest* req,
                                          google::protobuf::Empty*) {
  try {
    service_->CreateStream(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::DeleteStream(::grpc::ServerContext*,
                                          const payload::manager::v1::DeleteStreamRequest* req,
                                          google::protobuf::Empty*) {
  try {
    service_->DeleteStream(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::Append(::grpc::ServerContext*,
                                    const payload::manager::v1::AppendRequest* req,
                                    payload::manager::v1::AppendResponse* resp) {
  try {
    *resp = service_->Append(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::Read(::grpc::ServerContext*,
                                  const payload::manager::v1::ReadRequest* req,
                                  payload::manager::v1::ReadResponse* resp) {
  try {
    *resp = service_->Read(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::Subscribe(
    ::grpc::ServerContext*,
    const payload::manager::v1::SubscribeRequest*,
    ::grpc::ServerWriter<payload::manager::v1::SubscribeResponse>*) {
  return ::grpc::Status(
      ::grpc::StatusCode::UNIMPLEMENTED,
      "PayloadStreamService::Subscribe is not implemented yet");
}

::grpc::Status StreamServer::Commit(::grpc::ServerContext*,
                                    const payload::manager::v1::CommitRequest* req,
                                    google::protobuf::Empty*) {
  try {
    service_->Commit(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::GetCommitted(::grpc::ServerContext*,
                                          const payload::manager::v1::GetCommittedRequest* req,
                                          payload::manager::v1::GetCommittedResponse* resp) {
  try {
    *resp = service_->GetCommitted(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::GetRange(::grpc::ServerContext*,
                                      const payload::manager::v1::GetRangeRequest* req,
                                      payload::manager::v1::GetRangeResponse* resp) {
  try {
    *resp = service_->GetRange(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

} // namespace payload::grpc
