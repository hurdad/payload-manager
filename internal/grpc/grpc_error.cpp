#include "grpc_error.hpp"

#include "internal/util/errors.hpp"

namespace payload::grpc {

::grpc::Status ToStatus(const std::exception& e) {
  using namespace payload::util;

  if (dynamic_cast<const NotFound*>(&e)) {
    return {::grpc::StatusCode::NOT_FOUND, e.what()};
  }
  if (dynamic_cast<const AlreadyExists*>(&e)) {
    return {::grpc::StatusCode::ALREADY_EXISTS, e.what()};
  }
  if (dynamic_cast<const InvalidState*>(&e)) {
    return {::grpc::StatusCode::FAILED_PRECONDITION, e.what()};
  }
  if (dynamic_cast<const LeaseConflict*>(&e)) {
    return {::grpc::StatusCode::ABORTED, e.what()};
  }
  if (dynamic_cast<const ResourceExhausted*>(&e)) {
    return {::grpc::StatusCode::RESOURCE_EXHAUSTED, e.what()};
  }

  return {::grpc::StatusCode::INTERNAL, e.what()};
}

} // namespace payload::grpc
