#pragma once

#include <memory>
#include <string>
#include <pqxx/pqxx>

namespace payload::db::postgres {

/*
  PgPool

  Connection factory used by PgRepository.

  Design notes:
  -------------
  - Each transaction gets its own connection.
  - libpqxx connections are NOT thread-safe → do not share.
  - Prepared statements are installed per connection.
  - This behaves like a "logical pool" and can later be
    replaced with a real queue-based pool without changing
    repository code.

  Lifetime:
    Repository owns shared_ptr<PgPool>
    Transaction acquires shared_ptr<pqxx::connection>
*/

class PgPool {
public:
  explicit PgPool(std::string conninfo);

  // Acquire a new ready-to-use connection
  std::shared_ptr<pqxx::connection> Acquire();

private:
  std::string conninfo_;
};

}
