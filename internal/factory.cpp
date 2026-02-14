#include "factory.hpp"

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "internal/core/payload_manager.hpp"
#include "internal/db/api/repository.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/grpc/admin_server.hpp"
#include "internal/grpc/catalog_server.hpp"
#include "internal/grpc/data_server.hpp"
#include "internal/grpc/stream_server.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/lineage/lineage_graph.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/service/admin_service.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/service/stream_service.hpp"
#include "internal/spill/spill_scheduler.hpp"
#include "internal/spill/spill_worker.hpp"
#include "internal/storage/storage_factory.hpp"
#if PAYLOAD_DB_SQLITE
#include "internal/db/sqlite/sqlite_db.hpp"
#include "internal/db/sqlite/sqlite_repository.hpp"
#endif
#if PAYLOAD_DB_POSTGRES
#include "internal/db/postgres/pg_pool.hpp"
#include "internal/db/postgres/pg_repository.hpp"
#endif

namespace payload::factory {

using namespace payload;

namespace {

#if PAYLOAD_DB_SQLITE
void BootstrapSqliteSchema(const std::shared_ptr<db::sqlite::SqliteDB>& sqlite_db) {
  static const std::vector<std::string> kBootstrapSql = {
      "CREATE TABLE IF NOT EXISTS payload (id TEXT PRIMARY KEY, tier INTEGER NOT NULL, state INTEGER NOT NULL, size_bytes INTEGER NOT NULL, version INTEGER NOT NULL, expires_at_ms INTEGER);",
      "CREATE TABLE IF NOT EXISTS payload_metadata (id TEXT PRIMARY KEY, json TEXT NOT NULL, schema TEXT, updated_at_ms INTEGER NOT NULL, FOREIGN KEY(id) REFERENCES payload(id) ON DELETE CASCADE);",
      "CREATE TABLE IF NOT EXISTS payload_lineage (parent_id TEXT NOT NULL, child_id TEXT NOT NULL, operation TEXT, role TEXT, parameters TEXT, created_at_ms INTEGER NOT NULL, FOREIGN KEY(parent_id) REFERENCES payload(id) ON DELETE CASCADE, FOREIGN KEY(child_id) REFERENCES payload(id) ON DELETE CASCADE);",
      "CREATE TABLE IF NOT EXISTS payload_schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
      "CREATE TABLE IF NOT EXISTS streams (stream_id INTEGER PRIMARY KEY AUTOINCREMENT, namespace TEXT NOT NULL, name TEXT NOT NULL, created_at INTEGER NOT NULL DEFAULT (unixepoch() * 1000), retention_max_entries INTEGER, retention_max_age_sec INTEGER, UNIQUE(namespace, name));",
      "CREATE TABLE IF NOT EXISTS stream_entries (stream_id INTEGER NOT NULL REFERENCES streams(stream_id), offset INTEGER NOT NULL, payload_uuid TEXT NOT NULL, event_time INTEGER, append_time INTEGER NOT NULL DEFAULT (unixepoch() * 1000), duration_ns INTEGER, tags TEXT, PRIMARY KEY (stream_id, offset));",
      "CREATE TABLE IF NOT EXISTS stream_consumer_offsets (stream_id INTEGER NOT NULL REFERENCES streams(stream_id), consumer_group TEXT NOT NULL, offset INTEGER NOT NULL, updated_at INTEGER NOT NULL DEFAULT (unixepoch() * 1000), PRIMARY KEY (stream_id, consumer_group));"};

  for (const auto& sql : kBootstrapSql) {
    sqlite_db->Exec(sql);
  }

  sqlite_db->Exec("SELECT id,tier,state,size_bytes,version FROM payload LIMIT 1;");
  sqlite_db->Exec("SELECT id,json,schema,updated_at_ms FROM payload_metadata LIMIT 1;");
  sqlite_db->Exec("SELECT parent_id,child_id,operation,role,parameters,created_at_ms FROM payload_lineage LIMIT 1;");
  sqlite_db->Exec("SELECT version FROM payload_schema_migrations LIMIT 1;");
}
#endif

#if PAYLOAD_DB_POSTGRES
void BootstrapPostgresSchema(const std::shared_ptr<db::postgres::PgPool>& pool) {
  auto conn = pool->Acquire();
  pqxx::work tx(*conn);

  tx.exec("CREATE TABLE IF NOT EXISTS payload (id TEXT PRIMARY KEY, tier SMALLINT NOT NULL, state SMALLINT NOT NULL, size_bytes BIGINT NOT NULL, version BIGINT NOT NULL, expires_at_ms BIGINT);");
  tx.exec("CREATE TABLE IF NOT EXISTS payload_metadata (id TEXT PRIMARY KEY REFERENCES payload(id) ON DELETE CASCADE, json JSONB NOT NULL, schema TEXT, updated_at_ms BIGINT NOT NULL);");
  tx.exec("CREATE TABLE IF NOT EXISTS payload_lineage (parent_id TEXT NOT NULL REFERENCES payload(id) ON DELETE CASCADE, child_id TEXT NOT NULL REFERENCES payload(id) ON DELETE CASCADE, operation TEXT, role TEXT, parameters TEXT, created_at_ms BIGINT NOT NULL);");
  tx.exec("CREATE TABLE IF NOT EXISTS payload_schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT NOW());");
  tx.exec("CREATE TABLE IF NOT EXISTS streams (stream_id BIGSERIAL PRIMARY KEY, namespace TEXT NOT NULL, name TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), retention_max_entries BIGINT, retention_max_age_sec BIGINT, UNIQUE(namespace, name));");
  tx.exec("CREATE TABLE IF NOT EXISTS stream_entries (stream_id BIGINT NOT NULL REFERENCES streams(stream_id), offset BIGINT NOT NULL, payload_uuid UUID NOT NULL, event_time TIMESTAMPTZ, append_time TIMESTAMPTZ NOT NULL DEFAULT now(), duration_ns BIGINT, tags JSONB, PRIMARY KEY (stream_id, offset));");
  tx.exec("CREATE TABLE IF NOT EXISTS stream_consumer_offsets (stream_id BIGINT NOT NULL REFERENCES streams(stream_id), consumer_group TEXT NOT NULL, offset BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (stream_id, consumer_group));");

  tx.exec("SELECT id,tier,state,size_bytes,version FROM payload LIMIT 1;");
  tx.exec("SELECT id,json,schema,updated_at_ms FROM payload_metadata LIMIT 1;");
  tx.exec("SELECT parent_id,child_id,operation,role,parameters,created_at_ms FROM payload_lineage LIMIT 1;");
  tx.exec("SELECT version FROM payload_schema_migrations LIMIT 1;");
  tx.commit();
}
#endif

std::shared_ptr<db::Repository> BuildRepository(const payload::runtime::config::RuntimeConfig& config) {
  const auto& database = config.database();
  if (database.has_sqlite()) {
#if PAYLOAD_DB_SQLITE
    auto sqlite_db = std::make_shared<db::sqlite::SqliteDB>(database.sqlite().path());
    BootstrapSqliteSchema(sqlite_db);
    return std::make_shared<db::sqlite::SqliteRepository>(std::move(sqlite_db));
#else
    throw std::runtime_error("sqlite backend requested but not enabled at build time");
#endif
  }

  if (database.has_postgres()) {
#if PAYLOAD_DB_POSTGRES
    auto pool = std::make_shared<db::postgres::PgPool>(database.postgres().connection_uri());
    BootstrapPostgresSchema(pool);
    return std::make_shared<db::postgres::PgRepository>(std::move(pool));
#else
    throw std::runtime_error("postgres backend requested but not enabled at build time");
#endif
  }

  return std::make_shared<db::memory::MemoryRepository>();
}

} // namespace

/*
    Build full application dependency graph
*/
Application Build(const payload::runtime::config::RuntimeConfig& config) {
  Application app;

  // ------------------------------------------------------------------
  // Storage backends
  // ------------------------------------------------------------------
  auto storage_map = storage::StorageFactory::Build(config.storage());

  // ------------------------------------------------------------------
  // Core components
  // ------------------------------------------------------------------
  auto lease_mgr      = std::make_shared<lease::LeaseManager>();
  auto metadata_cache = std::make_shared<metadata::MetadataCache>();
  auto lineage_graph  = std::make_shared<lineage::LineageGraph>();
  auto repository     = BuildRepository(config);

  auto payload_manager = std::make_shared<core::PayloadManager>(storage_map, lease_mgr, metadata_cache, lineage_graph, repository);
  payload_manager->HydrateCaches();

  // ------------------------------------------------------------------
  // Spill system
  // ------------------------------------------------------------------
  auto spill_scheduler = std::make_shared<spill::SpillScheduler>();
  auto spill_worker    = std::make_shared<spill::SpillWorker>(spill_scheduler, payload_manager);

  spill_worker->Start();

  // ------------------------------------------------------------------
  // Services
  // ------------------------------------------------------------------
  service::ServiceContext ctx;
  ctx.manager    = payload_manager;
  ctx.metadata   = metadata_cache;
  ctx.lineage    = lineage_graph;
  ctx.repository = repository;

  auto data_service    = std::make_shared<service::DataService>(ctx);
  auto catalog_service = std::make_shared<service::CatalogService>(ctx);
  auto admin_service   = std::make_shared<service::AdminService>(ctx);
  auto stream_service  = std::make_shared<service::StreamService>(ctx);

  // ------------------------------------------------------------------
  // gRPC servers
  // ------------------------------------------------------------------
  app.grpc_services.push_back(std::make_unique<grpc::DataServer>(data_service));
  app.grpc_services.push_back(std::make_unique<grpc::CatalogServer>(catalog_service));
  app.grpc_services.push_back(std::make_unique<grpc::AdminServer>(admin_service));
  app.grpc_services.push_back(std::make_unique<grpc::StreamServer>(stream_service));

  // Keep ownership of workers so they live for process lifetime
  app.background_workers.push_back(spill_worker);

  return app;
}

} // namespace payload::factory
