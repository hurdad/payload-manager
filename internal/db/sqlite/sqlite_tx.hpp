#pragma once

#include <memory>

#include "internal/db/api/transaction.hpp"
#include "sqlite_db.hpp"

namespace payload::db::sqlite {

/*
  SQLite transaction wrapper.

  Uses BEGIN IMMEDIATE:
    - grabs write lock early
    - avoids deadlock-y behavior later
*/
class SqliteTransaction final : public db::Transaction {
public:
  explicit SqliteTransaction(std::shared_ptr<SqliteDB> db);
  ~SqliteTransaction();

  sqlite3* Handle() const { return db_->Handle(); }

  void Commit() override;
  void Rollback() override;
  bool IsCommitted() const override { return committed_; }

private:
  std::shared_ptr<SqliteDB> db_;
  bool committed_ = false;
};

}
