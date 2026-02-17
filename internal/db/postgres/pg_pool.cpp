#include "pg_pool.hpp"

namespace payload::db::postgres {

PgPool::PgPool(std::string conninfo, std::size_t max_connections)
    : conninfo_(std::move(conninfo)),
      max_connections_(max_connections == 0 ? 1 : max_connections) {
}

std::shared_ptr<pqxx::connection> PgPool::Acquire() {
  for (;;) {
    {
      std::unique_lock lock(mutex_);

      if (!idle_.empty()) {
        auto conn = std::move(idle_.back());
        idle_.pop_back();
        return Wrap(conn.release());
      }

      if (live_connections_ < max_connections_) {
        ++live_connections_;
        lock.unlock();

        try {
          auto* conn = new pqxx::connection(conninfo_);
          PrepareStatements(*conn);
          return Wrap(conn);
        } catch (...) {
          std::lock_guard rollback_lock(mutex_);
          --live_connections_;
          cv_.notify_one();
          throw;
        }
      }

      cv_.wait(lock, [this] {
        return !idle_.empty() || live_connections_ < max_connections_;
      });
    }
  }
}

void PgPool::PrepareStatements(pqxx::connection& conn) {
  conn.prepare("get_payload",
               "SELECT id, tier, state, size_bytes, version "
               "FROM payload WHERE id=$1");

  conn.prepare("insert_payload",
               "INSERT INTO payload(id,tier,state,size_bytes,version) "
               "VALUES($1,$2,$3,$4,$5)");

  conn.prepare("update_payload", "UPDATE payload SET tier=$2,state=$3,size_bytes=$4,version=$5 WHERE id=$1");

  conn.prepare("delete_payload", "DELETE FROM payload WHERE id=$1");
}

std::shared_ptr<pqxx::connection> PgPool::Wrap(pqxx::connection* conn) {
  std::weak_ptr<PgPool> weak_self = shared_from_this();
  return std::shared_ptr<pqxx::connection>(conn, [weak_self](pqxx::connection* released_conn) {
    if (auto self = weak_self.lock()) {
      self->Release(released_conn);
      return;
    }
    delete released_conn;
  });
}

void PgPool::Release(pqxx::connection* conn) {
  {
    std::lock_guard lock(mutex_);
    idle_.emplace_back(conn);
  }
  cv_.notify_one();
}

} // namespace payload::db::postgres
