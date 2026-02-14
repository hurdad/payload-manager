#include "sqlite_db.hpp"

#include <stdexcept>

namespace payload::db::sqlite {

static void ThrowIf(int rc, sqlite3* db, const char* what) {
  if (rc != SQLITE_OK) {
    throw std::runtime_error(std::string(what) + ": " + sqlite3_errmsg(db));
  }
}

SqliteDB::SqliteDB(std::string path) : path_(std::move(path)) {
  int rc = sqlite3_open_v2(path_.c_str(), &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);

  if (rc != SQLITE_OK) {
    std::string msg = db_ ? sqlite3_errmsg(db_) : "sqlite open failed";
    if (db_) sqlite3_close(db_);
    db_ = nullptr;
    throw std::runtime_error(msg);
  }

  Configure();
}

SqliteDB::~SqliteDB() {
  if (db_) sqlite3_close(db_);
}

void SqliteDB::Exec(const std::string& sql) {
  char* err = nullptr;
  int   rc  = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err);
  if (rc != SQLITE_OK) {
    std::string msg = err ? err : "sqlite exec failed";
    sqlite3_free(err);
    throw std::runtime_error(msg);
  }
}

sqlite3_stmt* SqliteDB::Prepare(const std::string& sql) {
  sqlite3_stmt* stmt = nullptr;
  int           rc   = sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
  ThrowIf(rc, db_, "sqlite prepare");
  return stmt;
}

void SqliteDB::Configure() {
  // IMPORTANT: WAL enables concurrent readers while writer holds lock
  Exec("PRAGMA journal_mode=WAL;");

  // NORMAL is a good tradeoff; use FULL if you want stronger durability
  Exec("PRAGMA synchronous=NORMAL;");

  // foreign keys are OFF by default in sqlite
  Exec("PRAGMA foreign_keys=ON;");

  // wait for locks instead of failing immediately
  ThrowIf(sqlite3_busy_timeout(db_, 5000), db_, "busy_timeout");

  // optional but helpful
  Exec("PRAGMA temp_store=MEMORY;");
  Exec("PRAGMA cache_size=-20000;"); // ~20MB (negative means KB)
}

} // namespace payload::db::sqlite
