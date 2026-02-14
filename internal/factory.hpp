#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <vector>

#include "config/config.pb.h"

namespace payload::factory {

struct Application {
  std::vector<std::unique_ptr<grpc::Service>> grpc_services;
  std::vector<std::shared_ptr<void>>          background_workers;
};

Application Build(const payload::runtime::config::RuntimeConfig& config);

} // namespace payload::factory
