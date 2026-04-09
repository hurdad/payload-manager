/*
  Verifies SQLite transaction isolation under concurrent access.

  SQLite uses file-level locking. With a shared in-memory DB, concurrent
  writers will receive SQLITE_BUSY on conflicting BEGIN IMMEDIATE statements.
  These tests verify:
    1. Committed writes from one transaction are visible to the next transaction.
    2. Rolled-back writes are NOT visible to subsequent transactions.
    3. Concurrent transactions on the same DB serialize correctly; the final
       counter value equals committed increments (none are lost or duplicated).
*/

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "internal/db/sqlite/sqlite_db.hpp"
#include "internal/db/sqlite/sqlite_tx.hpp"

namespace {

using payload::db::sqlite::SqliteDB;
using payload::db::sqlite::SqliteTransaction;

std::shared_ptr<SqliteDB> MakeDB() {
  return std::make_shared<SqliteDB>(":memory:");
}

int ReadCounter(SqliteDB& db) {
  int           val = -1;
  sqlite3_stmt* st  = nullptr;
  sqlite3_prepare_v2(db.Handle(), "SELECT n FROM ctr;", -1, &st, nullptr);
  if (sqlite3_step(st) == SQLITE_ROW) {
    val = sqlite3_column_int(st, 0);
  }
  sqlite3_finalize(st);
  return val;
}

} // namespace

TEST(SqliteConcurrentIsolation, CommittedWriteVisibleToNextTransaction) {
  auto db = MakeDB();
  db->Exec("CREATE TABLE ctr (n INTEGER);");
  db->Exec("INSERT INTO ctr VALUES(0);");

  {
    SqliteTransaction tx(db);
    db->Exec("UPDATE ctr SET n = 42;");
    tx.Commit();
  }

  // A new transaction must see the previously committed value.
  // ReadCounter uses the same db connection; with BEGIN IMMEDIATE active on
  // that connection the SELECT executes within tx2's transaction scope.
  {
    SqliteTransaction tx2(db);
    EXPECT_FALSE(tx2.IsCommitted()) << "tx2 must be open before the read";
    EXPECT_EQ(ReadCounter(*db), 42) << "committed write must be visible inside a new transaction";
    tx2.Commit();
    EXPECT_TRUE(tx2.IsCommitted());
  }
}

TEST(SqliteConcurrentIsolation, RolledBackWriteNotVisible) {
  auto db = MakeDB();
  db->Exec("CREATE TABLE ctr (n INTEGER);");
  db->Exec("INSERT INTO ctr VALUES(0);");

  {
    SqliteTransaction tx(db);
    db->Exec("UPDATE ctr SET n = 99;");
    tx.Rollback();
    EXPECT_FALSE(tx.IsCommitted());
  }

  // Rolled-back write must not be visible.
  SqliteTransaction tx2(db);
  EXPECT_EQ(ReadCounter(*db), 0);
  tx2.Commit();
}

TEST(SqliteConcurrentIsolation, ConcurrentWritesSerialize) {
  // Each thread attempts to increment the counter in its own transaction.
  // Some may receive SQLITE_BUSY; count the successes and verify the final
  // counter equals the number of successfully committed increments.
  constexpr int kThreads = 8;

  auto db = MakeDB();
  db->Exec("CREATE TABLE ctr (n INTEGER);");
  db->Exec("INSERT INTO ctr VALUES(0);");

  std::atomic<int>         committed{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      try {
        SqliteTransaction tx(db);
        int               cur = ReadCounter(*db);
        db->Exec("UPDATE ctr SET n = " + std::to_string(cur + 1) + ";");
        tx.Commit();
        committed.fetch_add(1, std::memory_order_relaxed);
      } catch (...) {
        // SQLITE_BUSY or other errors: this increment did not commit.
      }
    });
  }

  for (auto& t : threads) t.join();

  // Final counter must match committed increments — no double-counts, no lost updates.
  SqliteTransaction read_tx(db);
  EXPECT_EQ(ReadCounter(*db), committed.load(std::memory_order_relaxed));
  read_tx.Commit();
}
