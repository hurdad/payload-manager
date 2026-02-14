#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>

#include "internal/service/admin_service.hpp"
#include "payload/manager/services/v1/payload_admin_service.grpc.pb.h"
#include "payload/manager/v1.hpp"

namespace payload::grpc {

class AdminServer final : public payload::manager::v1::PayloadAdminService::Service {
 public:
  explicit AdminServer(std::shared_ptr<payload::service::AdminService> svc);

  ::grpc::Status Stats(::grpc::ServerContext*, const payload::manager::v1::StatsRequest*, payload::manager::v1::StatsResponse*) override;

 private:
  std::shared_ptr<payload::service::AdminService> service_;
};

} // namespace payload::grpc
