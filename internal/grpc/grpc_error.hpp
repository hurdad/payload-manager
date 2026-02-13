#pragma once

#include <exception>
#include <grpcpp/grpcpp.h>

namespace payload::grpc {

::grpc::Status ToStatus(const std::exception& e);

} // namespace payload::grpc
