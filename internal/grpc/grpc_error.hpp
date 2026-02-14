#pragma once

#include <grpcpp/grpcpp.h>

#include <exception>

namespace payload::grpc {

::grpc::Status ToStatus(const std::exception& e);

} // namespace payload::grpc
