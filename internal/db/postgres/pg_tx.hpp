#pragma once

#include <memory>
#include <pqxx/pqxx>
#include "internal/db/api/transaction.hpp"
#include "pg_pool.hpp"

namespace payload::db::postgres {

class PgTransaction final : public db::Transaction {
public:
  explicit PgTransaction(std::shared_ptr<PgPool> pool);
  ~PgTransaction();

  pqxx::work& Work() { return *tx_; }

  void Commit() override;
  void Rollback() override;
  bool IsCommitted() const override { return committed_; }

private:
  std::shared_ptr<pqxx::connection> conn_;
  std::unique_ptr<pqxx::work> tx_;
  bool committed_ = false;
};

}
