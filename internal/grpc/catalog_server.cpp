#include "catalog_server.hpp"

#include "grpc_error.hpp"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

CatalogServer::CatalogServer(std::shared_ptr<payload::service::CatalogService> svc) : service_(std::move(svc)) {
}

::grpc::Status CatalogServer::AllocatePayload(::grpc::ServerContext*, const payload::manager::v1::AllocatePayloadRequest* req,
                                              payload::manager::v1::AllocatePayloadResponse* resp) {
  try {
    *resp = service_->Allocate(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::CommitPayload(::grpc::ServerContext*, const payload::manager::v1::CommitPayloadRequest* req,
                                            payload::manager::v1::CommitPayloadResponse* resp) {
  try {
    *resp = service_->Commit(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::Delete(::grpc::ServerContext*, const payload::manager::v1::DeleteRequest* req, google::protobuf::Empty*) {
  try {
    service_->Delete(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::Promote(::grpc::ServerContext*, const payload::manager::v1::PromoteRequest* req,
                                      payload::manager::v1::PromoteResponse* resp) {
  try {
    *resp = service_->Promote(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::Spill(::grpc::ServerContext*, const payload::manager::v1::SpillRequest* req,
                                    payload::manager::v1::SpillResponse* resp) {
  try {
    *resp = service_->Spill(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::AddLineage(::grpc::ServerContext*, const payload::manager::v1::AddLineageRequest* req, google::protobuf::Empty*) {
  try {
    service_->AddLineage(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::GetLineage(::grpc::ServerContext*, const payload::manager::v1::GetLineageRequest* req,
                                         payload::manager::v1::GetLineageResponse* resp) {
  try {
    *resp = service_->GetLineage(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::UpdatePayloadMetadata(::grpc::ServerContext*, const payload::manager::v1::UpdatePayloadMetadataRequest* req,
                                                    payload::manager::v1::UpdatePayloadMetadataResponse* resp) {
  try {
    *resp = service_->UpdateMetadata(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::AppendPayloadMetadataEvent(::grpc::ServerContext*, const payload::manager::v1::AppendPayloadMetadataEventRequest* req,
                                                         payload::manager::v1::AppendPayloadMetadataEventResponse* resp) {
  try {
    *resp = service_->AppendMetadataEvent(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

} // namespace payload::grpc
