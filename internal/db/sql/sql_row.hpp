#pragma once

#include <string>
#include <cstdint>
#include <optional>

namespace payload::db::sql {

/*
  Generic row reader.

  Backends wrap their result row:
    postgres -> pqxx::row
    sqlite   -> sqlite3_stmt

  Prevents driver types leaking into repository logic.
*/

class Row {
public:
  virtual ~Row() = default;

  virtual std::string GetText(int col) const = 0;
  virtual int GetInt(int col) const = 0;
  virtual int64_t GetInt64(int col) const = 0;
  virtual bool IsNull(int col) const = 0;

  uint64_t GetU64(int col) const {
    return static_cast<uint64_t>(GetInt64(col));
  }
};

}
