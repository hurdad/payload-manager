/*
  Tests for SqliteTransaction::Rollback correctness.

  Before the fix, Rollback() set committed_=true.  This caused two problems:
    1. IsCommitted() returned true after an explicit rollback, making it
       impossible for callers to distinguish committed from rolled-back
       transactions.
    2. If Rollback() threw an exception, the destructor would not attempt a
       compensating ROLLBACK because committed_ was already true (even though
       the database was still in an open transaction), leaving the connection
       in a bad state.

  After the fix a separate finalized_ flag is set by both Commit() and
  Rollback().  The destructor checks finalized_ (not committed_), so:
    - Commit()  → committed_=true, finalized_=true → destructor is a no-op
    - Rollback() → committed_=false, finalized_=true → destructor is a no-op
    - (exception during tx) → finalized_=false → destructor rolls back

  Covered:
    - IsCommitted() returns false after Rollback().
    - IsCommitted() returns true after Commit().
    - Destructor after Rollback() does not cause a double-ROLLBACK error;
      subsequent use of the connection succeeds.
    - Destructor after an exception-interrupted transaction rolls back and
      the connection is usable for a new transaction.
*/

#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "internal/db/sqlite/sqlite_db.hpp"
#include "internal/db/sqlite/sqlite_tx.hpp"

namespace {

using payload::db::sqlite::SqliteDB;
using payload::db::sqlite::SqliteTransaction;

// Use an in-memory SQLite database so each test starts fresh with no disk I/O.
std::shared_ptr<SqliteDB> MakeDB() {
  return std::make_shared<SqliteDB>(":memory:");
}

// ---------------------------------------------------------------------------
// Test: IsCommitted() must return false after an explicit Rollback().
// ---------------------------------------------------------------------------
void TestIsCommittedFalseAfterRollback() {
  auto db = MakeDB();
  db->Exec("CREATE TABLE t (x INTEGER);");

  SqliteTransaction tx(db);
  db->Exec("INSERT INTO t VALUES (1);");
  tx.Rollback();

  assert(!tx.IsCommitted() && "IsCommitted() must be false after Rollback()");
}

// ---------------------------------------------------------------------------
// Test: IsCommitted() must return true after Commit().
// ---------------------------------------------------------------------------
void TestIsCommittedTrueAfterCommit() {
  auto db = MakeDB();
  db->Exec("CREATE TABLE t2 (x INTEGER);");

  SqliteTransaction tx(db);
  db->Exec("INSERT INTO t2 VALUES (42);");
  tx.Commit();

  assert(tx.IsCommitted() && "IsCommitted() must be true after Commit()");
}

// ---------------------------------------------------------------------------
// Test: After an explicit Rollback(), the destructor must not attempt a
//       second ROLLBACK.  If it did, SQLite would return
//       "cannot rollback — no transaction is active" and the destructor's
//       silent catch would hide the problem.  We verify by checking that a
//       new transaction can be opened immediately after the first object is
//       destroyed, which would fail if the connection were in a corrupt state.
// ---------------------------------------------------------------------------
void TestDestructorAfterRollbackDoesNotDoubleRollback() {
  auto db = MakeDB();
  db->Exec("CREATE TABLE t3 (x INTEGER);");

  {
    SqliteTransaction tx(db);
    db->Exec("INSERT INTO t3 VALUES (99);");
    tx.Rollback();
    // tx's destructor is called here.  It must not issue another ROLLBACK.
  }

  // Open a fresh transaction on the same connection.
  // If the previous destructor issued a spurious ROLLBACK, SQLite would
  // have already closed any active transaction; the new BEGIN would fail if
  // the connection is in an error state.
  {
    SqliteTransaction tx2(db);
    db->Exec("INSERT INTO t3 VALUES (1);");
    tx2.Commit();
  }

  // Verify only the committed row exists (the rolled-back 99 must be absent).
  auto* stmt = db->Prepare("SELECT COUNT(*) FROM t3;");
  assert(sqlite3_step(stmt) == SQLITE_ROW);
  const int count = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);
  assert(count == 1 && "only the committed row must exist; rollback must have discarded the other");
}

// ---------------------------------------------------------------------------
// Test: When a transaction is abandoned (destructor called without explicit
//       Commit or Rollback), the destructor's automatic rollback must leave
//       the connection usable.
// ---------------------------------------------------------------------------
void TestDestructorRollsBackAbortedTransaction() {
  auto db = MakeDB();
  db->Exec("CREATE TABLE t4 (x INTEGER);");

  {
    SqliteTransaction tx(db);
    db->Exec("INSERT INTO t4 VALUES (7);");
    // tx is destroyed without Commit() or Rollback() — destructor rolls back.
  }

  // The row must not be present.
  auto* stmt = db->Prepare("SELECT COUNT(*) FROM t4;");
  assert(sqlite3_step(stmt) == SQLITE_ROW);
  const int count = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);
  assert(count == 0 && "abandoned transaction must be rolled back by the destructor");

  // Connection must still accept new transactions.
  {
    SqliteTransaction tx2(db);
    db->Exec("INSERT INTO t4 VALUES (8);");
    tx2.Commit();
  }
}

} // namespace

int main() {
  TestIsCommittedFalseAfterRollback();
  TestIsCommittedTrueAfterCommit();
  TestDestructorAfterRollbackDoesNotDoubleRollback();
  TestDestructorRollsBackAbortedTransaction();

  std::cout << "sqlite_transaction_test: pass\n";
  return 0;
}
