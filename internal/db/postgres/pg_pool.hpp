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

  Bounded connection pool used by PgRepository.

  Design notes:
  -------------
  - Each transaction gets its own connection; libpqxx connections are NOT
    thread-safe, so they are never shared while checked out.
  - Connections are reused. Acquire() takes one off the idle list, opens a new
    one only while under max_connections, and otherwise waits on a condition
    variable until another transaction returns one. Release() puts it back.
  - Prepared statements are installed once per connection, when it is opened,
    and live for as long as the connection does.

  Do not put a transaction-pooling proxy (PgBouncer and friends) in front of
  this. Two things here need a pinned session and would break quietly:

    - The prepared statements above are named and server-side. Under
      transaction pooling a given query can land on any backend, where the
      name does not exist.
    - factory::BuildRepository holds a session-level pg_try_advisory_lock for
      the life of the process as the single-instance guard. Session-level
      locks need the session to stay put; proxied, the guard stops guarding
      without saying so.

  There is also nothing to gain: one process holding at most max_connections
  long-lived connections is not the many-short-lived-clients problem those
  proxies solve, and the single-instance lock rules out running several
  instances against one database anyway.

  Lifetime:
    Repository owns shared_ptr<PgPool>
    Transaction acquires shared_ptr<pqxx::connection>
*/

class PgPool : public std::enable_shared_from_this<PgPool> {
 public:
  explicit PgPool(std::string conninfo, std::size_t max_connections = 16);

  // Take a ready-to-use connection from the pool, opening one if the pool is
  // below max_connections and blocking if it is not.
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
