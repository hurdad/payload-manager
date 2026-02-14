#pragma once

#include <string>

namespace payload::db {

/*
  Portable DB result codes.

  The repository layer must translate backend errors into these.
  Upper layers should never depend on pqxx/sqlite error types.
*/

enum class ErrorCode {
  OK = 0,

  NotFound,
  AlreadyExists,
  Conflict,
  Busy,

  ConstraintViolation,
  SerializationFailure,

  IOError,
  Corruption,

  Unsupported,
  InternalError
};

struct Result {
  ErrorCode   code = ErrorCode::OK;
  std::string message;

  static Result Ok() {
    return {};
  }

  static Result Err(ErrorCode c, std::string msg = {}) {
    return {c, std::move(msg)};
  }

  explicit operator bool() const {
    return code == ErrorCode::OK;
  }
};

} // namespace payload::db
