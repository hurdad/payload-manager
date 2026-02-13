#pragma once

#include <grpcpp/grpcpp.h>
#include "internal/util/errors.hpp"

namespace payload::grpc {

/*
  Converts internal exceptions into gRPC status codes.
*/

grpc::Status ToStatus(const std::exception& e);

} // namespace payload::grpc
