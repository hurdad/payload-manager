#pragma once

#include <sqlite3.h>

#include <memory>
#include <string>

namespace payload::db::sqlite {

/*
  Thin RAII wrapper around sqlite3* + prepared statement cache.
*/
class SqliteDB {
 public:
  explicit SqliteDB(std::string path);
  ~SqliteDB();

  SqliteDB(const SqliteDB&)            = delete;
  SqliteDB& operator=(const SqliteDB&) = delete;

  sqlite3* Handle() const {
    return db_;
  }

  // Execute a SQL string (used for pragmas/migrations)
  void Exec(const std::string& sql);

  // Prepare a statement (caller must sqlite3_finalize)
  sqlite3_stmt* Prepare(const std::string& sql);

  // Configure recommended PRAGMAs (WAL, foreign keys, etc.)
  void Configure();

 private:
  sqlite3*    db_ = nullptr;
  std::string path_;
};

} // namespace payload::db::sqlite
