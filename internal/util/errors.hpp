#pragma once

#include <stdexcept>
#include <string>

namespace payload::util {

/*
  Central error types.

  These get translated later to gRPC status codes.
*/

class NotFound : public std::runtime_error {
 public:
  explicit NotFound(const std::string& msg) : std::runtime_error(msg) {
  }
};

class AlreadyExists : public std::runtime_error {
 public:
  explicit AlreadyExists(const std::string& msg) : std::runtime_error(msg) {
  }
};

class InvalidState : public std::runtime_error {
 public:
  explicit InvalidState(const std::string& msg) : std::runtime_error(msg) {
  }
};

class LeaseConflict : public std::runtime_error {
 public:
  explicit LeaseConflict(const std::string& msg) : std::runtime_error(msg) {
  }
};

class ResourceExhausted : public std::runtime_error {
 public:
  explicit ResourceExhausted(const std::string& msg) : std::runtime_error(msg) {
  }
};

} // namespace payload::util
