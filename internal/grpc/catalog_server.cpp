#include "catalog_server.hpp"
#include "grpc_error.hpp"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

CatalogServer::CatalogServer(std::shared_ptr<payload::service::CatalogService> svc)
    : service_(std::move(svc)) {}

::grpc::Status CatalogServer::AllocatePayload(::grpc::ServerContext*,
                                            const payload::manager::v1::AllocatePayloadRequest* req,
                                            payload::manager::v1::AllocatePayloadResponse* resp) {
  try {
    *resp = service_->Allocate(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::CommitPayload(::grpc::ServerContext*,
                                          const payload::manager::v1::CommitPayloadRequest* req,
                                          payload::manager::v1::CommitPayloadResponse* resp) {
  try {
    *resp = service_->Commit(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

::grpc::Status CatalogServer::Delete(::grpc::ServerContext*,
                                   const payload::manager::v1::DeleteRequest* req,
                                   google::protobuf::Empty*) {
  try {
    service_->Delete(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

}
