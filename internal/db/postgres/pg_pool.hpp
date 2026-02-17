#pragma once

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <pqxx/pqxx>
#include <string>
#include <vector>

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

class PgPool : public std::enable_shared_from_this<PgPool> {
 public:
  explicit PgPool(std::string conninfo, std::size_t max_connections = 16);

  // Acquire a new ready-to-use connection
  std::shared_ptr<pqxx::connection> Acquire();

 private:
  static void                       PrepareStatements(pqxx::connection& conn);
  std::shared_ptr<pqxx::connection> Wrap(pqxx::connection* conn);
  void                              Release(pqxx::connection* conn);

  std::string conninfo_;
  std::size_t max_connections_;

  std::mutex                                     mutex_;
  std::condition_variable                        cv_;
  std::vector<std::unique_ptr<pqxx::connection>> idle_;
  std::size_t                                    live_connections_ = 0;
};

} // namespace payload::db::postgres
