#include "stream_server.hpp"

#include <chrono>
#include <thread>

#include "grpc_error.hpp"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

StreamServer::StreamServer(std::shared_ptr<payload::service::StreamService> svc) : service_(std::move(svc)) {
}

::grpc::Status StreamServer::CreateStream(::grpc::ServerContext*, const payload::manager::v1::CreateStreamRequest* req, google::protobuf::Empty*) {
  try {
    service_->CreateStream(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::DeleteStream(::grpc::ServerContext*, const payload::manager::v1::DeleteStreamRequest* req, google::protobuf::Empty*) {
  try {
    service_->DeleteStream(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::Append(::grpc::ServerContext*, const payload::manager::v1::AppendRequest* req,
                                    payload::manager::v1::AppendResponse* resp) {
  try {
    *resp = service_->Append(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::Read(::grpc::ServerContext*, const payload::manager::v1::ReadRequest* req, payload::manager::v1::ReadResponse* resp) {
  try {
    *resp = service_->Read(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::Subscribe(::grpc::ServerContext* ctx, const payload::manager::v1::SubscribeRequest* req,
                                       ::grpc::ServerWriter<payload::manager::v1::SubscribeResponse>* writer) {
  try {
    // Resolve the initial start offset and drain any existing entries.
    auto initial = service_->Subscribe(*req);
    for (const auto& response : initial.responses) {
      if (!writer->Write(response)) {
        return ::grpc::Status::OK;
      }
    }

    // Long-poll loop: keep reading new entries until the client disconnects.
    // Poll at 50 ms when idle; advance immediately when entries arrive.
    constexpr auto kPollInterval = std::chrono::milliseconds(50);

    payload::manager::v1::SubscribeRequest poll_req;
    *poll_req.mutable_stream() = req->stream();
    uint64_t next_offset       = initial.next_offset;

    while (!ctx->IsCancelled()) {
      poll_req.set_offset(next_offset);
      auto batch = service_->Subscribe(poll_req);
      for (const auto& response : batch.responses) {
        if (!writer->Write(response)) {
          return ::grpc::Status::OK;
        }
      }
      next_offset = batch.next_offset;
      if (batch.responses.empty()) {
        std::this_thread::sleep_for(kPollInterval);
      }
    }

    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::Commit(::grpc::ServerContext*, const payload::manager::v1::CommitRequest* req, google::protobuf::Empty*) {
  try {
    service_->Commit(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::GetCommitted(::grpc::ServerContext*, const payload::manager::v1::GetCommittedRequest* req,
                                          payload::manager::v1::GetCommittedResponse* resp) {
  try {
    *resp = service_->GetCommitted(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status StreamServer::GetRange(::grpc::ServerContext*, const payload::manager::v1::GetRangeRequest* req,
                                      payload::manager::v1::GetRangeResponse* resp) {
  try {
    *resp = service_->GetRange(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

} // namespace payload::grpc
