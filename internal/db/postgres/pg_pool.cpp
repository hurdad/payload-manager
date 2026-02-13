#include "pg_pool.hpp"

namespace payload::db::postgres {

PgPool::PgPool(std::string conninfo)
    : conninfo_(std::move(conninfo)) {}

std::shared_ptr<pqxx::connection> PgPool::Acquire() {
  auto conn = std::make_shared<pqxx::connection>(conninfo_);
  conn->prepare("get_payload",
      "SELECT id, tier, state, size_bytes, version "
      "FROM payload WHERE id=$1");

  conn->prepare("insert_payload",
      "INSERT INTO payload(id,tier,state,size_bytes,version) "
      "VALUES($1,$2,$3,$4,$5)");

  conn->prepare("update_payload",
      "UPDATE payload SET tier=$2,state=$3,size_bytes=$4,version=$5 WHERE id=$1");

  conn->prepare("delete_payload",
      "DELETE FROM payload WHERE id=$1");

  return conn;
}

}
