#include "pg_tx.hpp"

namespace payload::db::postgres {

PgTransaction::PgTransaction(std::shared_ptr<PgPool> pool)
{
  conn_ = pool->Acquire();
  tx_ = std::make_unique<pqxx::work>(*conn_);
}

PgTransaction::~PgTransaction() {
  if (!committed_) {
    try { tx_->abort(); }
    catch (...) {}
  }
}

void PgTransaction::Commit() {
  tx_->commit();
  committed_ = true;
}

void PgTransaction::Rollback() {
  tx_->abort();
  committed_ = true;
}

}
