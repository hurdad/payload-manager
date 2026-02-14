#pragma once

#include "payload/manager/services/v1/payload_admin_service.pb.h"
#include "payload/manager/v1.hpp"
#include "service_context.hpp"

namespace payload::service {

class AdminService {
 public:
  explicit AdminService(ServiceContext ctx);

  payload::manager::v1::StatsResponse Stats(const payload::manager::v1::StatsRequest& req);

 private:
  ServiceContext ctx_;
};

} // namespace payload::service
