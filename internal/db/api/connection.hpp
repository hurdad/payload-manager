#pragma once

namespace payload::manager::internal::db::api {

class Connection {
 public:
  virtual ~Connection() = default;

  virtual bool IsHealthy() const = 0;
};

}  // namespace payload::manager::internal::db::api
