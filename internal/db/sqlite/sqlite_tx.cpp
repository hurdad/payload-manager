#include "sqlite_tx.hpp"

namespace payload::db::sqlite {

SqliteTransaction::SqliteTransaction(std::shared_ptr<SqliteDB> db) : db_(std::move(db)) {
  db_->Exec("BEGIN IMMEDIATE;");
}

SqliteTransaction::~SqliteTransaction() {
  if (!finalized_) {
    try {
      db_->Exec("ROLLBACK;");
    } catch (...) {
    }
  }
}

void SqliteTransaction::Commit() {
  db_->Exec("COMMIT;");
  committed_ = true;
  finalized_ = true;
}

void SqliteTransaction::Rollback() {
  db_->Exec("ROLLBACK;");
  finalized_ = true;
}

} // namespace payload::db::sqlite
