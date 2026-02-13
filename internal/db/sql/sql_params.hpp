#pragma once

#include <string>
#include <variant>
#include <vector>
#include <cstdint>

namespace payload::db::sql {

/*
  Parameter abstraction.

  Postgres: $1 $2 $3
  SQLite:   ? ? ?

  Both use ordered binding → this works perfectly.
*/

using Param = std::variant<
    std::nullptr_t,
    int32_t,
    int64_t,
    uint64_t,
    std::string
>;

using Params = std::vector<Param>;

}
