#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace payload::db::sql {

/*
  Parameter abstraction.

  Postgres: $1 $2 $3

  Ordered binding. The placeholder indirection is a holdover from when
  SQLite (? ? ?) was also supported; it is harmless but no longer load-bearing.
*/

using Param = std::variant<std::nullptr_t, int32_t, int64_t, uint64_t, std::string>;

using Params = std::vector<Param>;

} // namespace payload::db::sql
