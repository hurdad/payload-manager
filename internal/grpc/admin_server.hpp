#pragma once

#include <memory>
#include <grpcpp/grpcpp.h>

#include "payload/manager/v1/payload_admin_service.grpc.pb.h"
#include "internal/service/admin_service.hpp"

namespace payload::grpc {

class AdminServer final : public payload::manager::v1::PayloadAdminService::Service {
public:
  explicit AdminServer(std::shared_ptr<payload::service::AdminService> svc);

  ::grpc::Status Stats(::grpc::ServerContext*,
                     const payload::manager::v1::StatsRequest*,
                     payload::manager::v1::StatsResponse*) override;

private:
  std::shared_ptr<payload::service::AdminService> service_;
};

}
