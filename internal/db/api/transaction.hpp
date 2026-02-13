#pragma once

namespace payload::db {

/*
  Abstract transaction.

  Semantics guaranteed for ALL backends:

  - Changes are invisible until Commit()
  - After Commit() all reads see the change
  - Rollback() discards all writes
  - Destructor MUST rollback if not committed

  SQLite: BEGIN IMMEDIATE
  Postgres: pqxx::work
  Memory: snapshot copy-on-write
*/

class Transaction {
public:
  virtual ~Transaction() = default;

  // commit changes atomically
  virtual void Commit() = 0;

  // explicit rollback
  virtual void Rollback() = 0;

  // true if commit already performed
  virtual bool IsCommitted() const = 0;
};

}
