#include "admin_server.hpp"

#include "grpc_error.hpp"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

AdminServer::AdminServer(std::shared_ptr<payload::service::AdminService> svc) : service_(std::move(svc)) {
}

::grpc::Status AdminServer::Stats(::grpc::ServerContext*, const payload::manager::v1::StatsRequest* req, payload::manager::v1::StatsResponse* resp) {
  try {
    *resp = service_->Stats(*req);
    return ::grpc::Status::OK;
  } catch (const std::exception& e) {
    return ToStatus(e);
  }
}

} // namespace payload::grpc
