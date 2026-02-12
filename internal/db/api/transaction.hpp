#pragma once

namespace payload::manager::internal::db::api {

class Transaction {
 public:
  virtual ~Transaction() = default;

  virtual void Commit() = 0;
  virtual void Rollback() noexcept = 0;
};

}  // namespace payload::manager::internal::db::api
