#include "factory.hpp"

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "internal/core/payload_manager.hpp"
#include "internal/db/api/repository.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/expiration/expiration_worker.hpp"
#include "internal/grpc/admin_server.hpp"
#include "internal/grpc/catalog_server.hpp"
#include "internal/grpc/data_server.hpp"
#include "internal/grpc/stream_server.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/lineage/lineage_graph.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/observability/logging.hpp"
#include "internal/service/admin_service.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/service/stream_service.hpp"
#include "internal/spill/spill_scheduler.hpp"
#include "internal/spill/spill_worker.hpp"
#include "internal/storage/storage_factory.hpp"
#include "internal/tiering/pressure_state.hpp"
#include "internal/tiering/tiering_manager.hpp"
#include "internal/tiering/tiering_policy.hpp"
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
// Execute a migration SQL statement, ignoring "duplicate column" errors so that
// ALTER TABLE ADD COLUMN is idempotent on older SQLite builds that don't support
// IF NOT EXISTS. All other errors are re-thrown.
static void TryExecSqlite(const std::shared_ptr<db::sqlite::SqliteDB>& db, const std::string& sql) {
  try {
    db->Exec(sql);
  } catch (const std::runtime_error& e) {
    if (std::string(e.what()).find("duplicate column") == std::string::npos) {
      throw;
    }
  }
}

void BootstrapSqliteSchema(const std::shared_ptr<db::sqlite::SqliteDB>& sqlite_db) {
  static const std::vector<std::string> kBootstrapSql = {
      "CREATE TABLE IF NOT EXISTS payload (id BLOB PRIMARY KEY, tier INTEGER NOT NULL, state INTEGER NOT NULL, size_bytes INTEGER NOT NULL, version "
      "INTEGER NOT NULL, expires_at_ms INTEGER, persist INTEGER NOT NULL DEFAULT 0, eviction_priority INTEGER NOT NULL DEFAULT 0, spill_target "
      "INTEGER NOT NULL DEFAULT 0);",
      "CREATE TABLE IF NOT EXISTS payload_metadata (id BLOB PRIMARY KEY, json TEXT NOT NULL, schema TEXT, updated_at_ms INTEGER NOT NULL, FOREIGN "
      "KEY(id) REFERENCES payload(id) ON DELETE CASCADE);",
      "CREATE TABLE IF NOT EXISTS payload_lineage (parent_id BLOB NOT NULL, child_id BLOB NOT NULL, operation TEXT, role TEXT, parameters TEXT, "
      "created_at_ms INTEGER NOT NULL, FOREIGN KEY(parent_id) REFERENCES payload(id) ON DELETE CASCADE, FOREIGN KEY(child_id) REFERENCES payload(id) "
      "ON DELETE CASCADE);",
      "CREATE TABLE IF NOT EXISTS payload_metadata_events (rowid INTEGER PRIMARY KEY AUTOINCREMENT, id BLOB NOT NULL, data BLOB, schema TEXT, source "
      "TEXT, version TEXT, ts_ms INTEGER NOT NULL);",
      "CREATE TABLE IF NOT EXISTS payload_schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
      "CREATE TABLE IF NOT EXISTS streams (stream_id INTEGER PRIMARY KEY AUTOINCREMENT, namespace TEXT NOT NULL, name TEXT NOT NULL, created_at "
      "INTEGER NOT NULL DEFAULT (unixepoch() * 1000), retention_max_entries INTEGER, retention_max_age_sec INTEGER, UNIQUE(namespace, name));",
      "CREATE TABLE IF NOT EXISTS stream_entries (stream_id INTEGER NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE, offset INTEGER NOT "
      "NULL, payload_uuid "
      "BLOB NOT NULL, event_time INTEGER, append_time INTEGER NOT NULL DEFAULT (unixepoch() * 1000), duration_ns INTEGER, tags TEXT, PRIMARY KEY "
      "(stream_id, offset));",
      "CREATE TABLE IF NOT EXISTS stream_consumer_offsets (stream_id INTEGER NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE, "
      "consumer_group TEXT NOT NULL, "
      "offset INTEGER NOT NULL, updated_at INTEGER NOT NULL DEFAULT (unixepoch() * 1000), PRIMARY KEY (stream_id, consumer_group));"};

  for (const auto& sql : kBootstrapSql) {
    sqlite_db->Exec(sql);
  }

  // Migrate existing databases that predate the eviction policy columns.
  TryExecSqlite(sqlite_db, "ALTER TABLE payload ADD COLUMN persist INTEGER NOT NULL DEFAULT 0;");
  TryExecSqlite(sqlite_db, "ALTER TABLE payload ADD COLUMN eviction_priority INTEGER NOT NULL DEFAULT 0;");
  TryExecSqlite(sqlite_db, "ALTER TABLE payload ADD COLUMN spill_target INTEGER NOT NULL DEFAULT 0;");

  sqlite_db->Exec("SELECT id,tier,state,size_bytes,version FROM payload LIMIT 1;");
  sqlite_db->Exec("SELECT id,json,schema,updated_at_ms FROM payload_metadata LIMIT 1;");
  sqlite_db->Exec("SELECT id,data,schema,source,version,ts_ms FROM payload_metadata_events LIMIT 1;");
  sqlite_db->Exec("SELECT parent_id,child_id,operation,role,parameters,created_at_ms FROM payload_lineage LIMIT 1;");
  sqlite_db->Exec("SELECT version FROM payload_schema_migrations LIMIT 1;");
}
#endif

#if PAYLOAD_DB_POSTGRES
void BootstrapPostgresSchema(const std::string& conninfo) {
  pqxx::connection conn(conninfo);
  pqxx::work       tx(conn);

  tx.exec(
      "CREATE TABLE IF NOT EXISTS payload (id UUID PRIMARY KEY, tier SMALLINT NOT NULL, state SMALLINT NOT NULL, size_bytes BIGINT NOT NULL, version "
      "BIGINT NOT NULL, expires_at_ms BIGINT, persist SMALLINT NOT NULL DEFAULT 0, eviction_priority SMALLINT NOT NULL DEFAULT 0, spill_target "
      "SMALLINT NOT NULL DEFAULT 0);");
  // Migrate existing databases that predate the eviction policy columns.
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS persist SMALLINT NOT NULL DEFAULT 0;");
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS eviction_priority SMALLINT NOT NULL DEFAULT 0;");
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS spill_target SMALLINT NOT NULL DEFAULT 0;");
  // Migrate id columns from TEXT to UUID if needed (no-op when already UUID).
  tx.exec(
      "DO $$ BEGIN "
      "  IF (SELECT data_type FROM information_schema.columns WHERE table_name='payload' AND column_name='id') = 'text' THEN "
      "    ALTER TABLE payload ALTER COLUMN id TYPE UUID USING id::uuid; "
      "    ALTER TABLE payload_metadata ALTER COLUMN id TYPE UUID USING id::uuid; "
      "    ALTER TABLE payload_metadata_events ALTER COLUMN id TYPE UUID USING id::uuid; "
      "    ALTER TABLE payload_lineage ALTER COLUMN parent_id TYPE UUID USING parent_id::uuid; "
      "    ALTER TABLE payload_lineage ALTER COLUMN child_id  TYPE UUID USING child_id::uuid; "
      "  END IF; "
      "END $$;");
  tx.exec(
      "CREATE TABLE IF NOT EXISTS payload_metadata (id UUID PRIMARY KEY REFERENCES payload(id) ON DELETE CASCADE, json JSONB NOT NULL, schema TEXT, "
      "updated_at_ms BIGINT NOT NULL);");
  tx.exec(
      "CREATE TABLE IF NOT EXISTS payload_lineage (parent_id UUID NOT NULL REFERENCES payload(id) ON DELETE CASCADE, child_id UUID NOT NULL "
      "REFERENCES payload(id) ON DELETE CASCADE, operation TEXT, role TEXT, parameters TEXT, created_at_ms BIGINT NOT NULL);");
  tx.exec(
      "CREATE TABLE IF NOT EXISTS payload_metadata_events (id UUID NOT NULL, data BYTEA, schema TEXT, source TEXT, version TEXT, ts_ms BIGINT NOT "
      "NULL);");
  tx.exec("CREATE TABLE IF NOT EXISTS payload_schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT NOW());");
  tx.exec(
      "CREATE TABLE IF NOT EXISTS streams (stream_id BIGSERIAL PRIMARY KEY, namespace TEXT NOT NULL, name TEXT NOT NULL, created_at TIMESTAMPTZ NOT "
      "NULL DEFAULT now(), retention_max_entries BIGINT, retention_max_age_sec BIGINT, UNIQUE(namespace, name));");
  tx.exec(
      "CREATE TABLE IF NOT EXISTS stream_entries (stream_id BIGINT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE, \"offset\" BIGINT NOT "
      "NULL, payload_uuid "
      "UUID "
      "NOT NULL, event_time TIMESTAMPTZ, append_time TIMESTAMPTZ NOT NULL DEFAULT now(), duration_ns BIGINT, tags JSONB, PRIMARY KEY (stream_id, "
      "\"offset\"));");
  tx.exec(
      "CREATE TABLE IF NOT EXISTS stream_consumer_offsets (stream_id BIGINT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE, consumer_group "
      "TEXT NOT NULL, "
      "\"offset\" BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (stream_id, consumer_group));");

  // Migrate stream FK constraints to add ON DELETE CASCADE for existing databases.
  tx.exec(
      "DO $$ BEGIN "
      "  IF EXISTS (SELECT 1 FROM information_schema.table_constraints "
      "             WHERE constraint_name='stream_entries_stream_id_fkey' AND constraint_type='FOREIGN KEY') THEN "
      "    IF NOT EXISTS (SELECT 1 FROM information_schema.referential_constraints "
      "                   WHERE constraint_name='stream_entries_stream_id_fkey' AND delete_rule='CASCADE') THEN "
      "      ALTER TABLE stream_entries DROP CONSTRAINT stream_entries_stream_id_fkey; "
      "      ALTER TABLE stream_entries ADD CONSTRAINT stream_entries_stream_id_fkey "
      "        FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE; "
      "    END IF; "
      "  END IF; "
      "END $$;");
  tx.exec(
      "DO $$ BEGIN "
      "  IF EXISTS (SELECT 1 FROM information_schema.table_constraints "
      "             WHERE constraint_name='stream_consumer_offsets_stream_id_fkey' AND constraint_type='FOREIGN KEY') THEN "
      "    IF NOT EXISTS (SELECT 1 FROM information_schema.referential_constraints "
      "                   WHERE constraint_name='stream_consumer_offsets_stream_id_fkey' AND delete_rule='CASCADE') THEN "
      "      ALTER TABLE stream_consumer_offsets DROP CONSTRAINT stream_consumer_offsets_stream_id_fkey; "
      "      ALTER TABLE stream_consumer_offsets ADD CONSTRAINT stream_consumer_offsets_stream_id_fkey "
      "        FOREIGN KEY (stream_id) REFERENCES streams(stream_id) ON DELETE CASCADE; "
      "    END IF; "
      "  END IF; "
      "END $$;");

  tx.exec("SELECT id,tier,state,size_bytes,version FROM payload LIMIT 1;");
  tx.exec("SELECT id,json,schema,updated_at_ms FROM payload_metadata LIMIT 1;");
  tx.exec("SELECT id,data,schema,source,version,ts_ms FROM payload_metadata_events LIMIT 1;");
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
    // The snapshot cache uses single-instance semantics: mutations applied
    // directly to the PostgreSQL database by another process (or a future
    // second instance) are NOT reflected until HydrateCaches() is called.
    // Run only one payload-manager instance per database to avoid silent
    // cache divergence.
    PAYLOAD_LOG_WARN(
        "PostgreSQL backend: snapshot cache enforces single-instance semantics; direct DB mutations from other processes will not be visible until "
        "HydrateCaches() is called");
    BootstrapPostgresSchema(database.postgres().connection_uri());
    auto pool = std::make_shared<db::postgres::PgPool>(database.postgres().connection_uri(), database.postgres().max_connections());
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
  const auto&    lease_cfg = config.leases();
  const uint64_t default_lease_ms =
      lease_cfg.has_default_lease()
          ? static_cast<uint64_t>(lease_cfg.default_lease().seconds() * 1000 + lease_cfg.default_lease().nanos() / 1'000'000)
          : 20'000;
  const uint64_t max_lease_ms =
      lease_cfg.has_max_lease() ? static_cast<uint64_t>(lease_cfg.max_lease().seconds() * 1000 + lease_cfg.max_lease().nanos() / 1'000'000) : 120'000;
  auto lease_mgr      = std::make_shared<lease::LeaseManager>(default_lease_ms, max_lease_ms);
  auto metadata_cache = std::make_shared<metadata::MetadataCache>();
  auto lineage_graph  = std::make_shared<lineage::LineageGraph>();
  auto repository     = BuildRepository(config);

  auto payload_manager = std::make_shared<core::PayloadManager>(storage_map, lease_mgr, repository);
  payload_manager->HydrateCaches();

  // ------------------------------------------------------------------
  // Spill system
  // ------------------------------------------------------------------
  const uint32_t num_spill_threads = config.spill_workers().threads() > 0 ? config.spill_workers().threads() : 1;

  auto                                             spill_scheduler = std::make_shared<spill::SpillScheduler>();
  std::vector<std::shared_ptr<spill::SpillWorker>> spill_worker_pool;
  spill_worker_pool.reserve(num_spill_threads);
  for (uint32_t i = 0; i < num_spill_threads; ++i) {
    auto w = std::make_shared<spill::SpillWorker>(spill_scheduler, payload_manager);
    w->Start();
    spill_worker_pool.push_back(std::move(w));
  }

  // ------------------------------------------------------------------
  // Tiering manager (automatic pressure-driven eviction)
  // ------------------------------------------------------------------
  auto pressure_state       = std::make_shared<tiering::PressureState>();
  pressure_state->ram_limit = config.storage().ram().capacity_bytes();
  for (const auto& dev : config.storage().gpu().devices()) {
    pressure_state->gpu_limit += dev.capacity_bytes();
  }

  auto tiering_policy = std::make_shared<tiering::TieringPolicy>(
      metadata_cache, [pm = payload_manager.get()](const manager::v1::PayloadID& id) { return !pm->IsEvictionExempt(id); });

  auto tiering_manager = std::make_shared<tiering::TieringManager>(tiering_policy, spill_scheduler, payload_manager, pressure_state);
  tiering_manager->Start();

  // ------------------------------------------------------------------
  // Expiration worker
  // ------------------------------------------------------------------
  auto expiration_worker = std::make_shared<expiration::ExpirationWorker>(payload_manager);
  expiration_worker->Start();

  // ------------------------------------------------------------------
  // Services
  // ------------------------------------------------------------------
  service::ServiceContext ctx;
  ctx.manager               = payload_manager;
  ctx.metadata              = metadata_cache;
  ctx.lineage               = lineage_graph;
  ctx.repository            = repository;
  ctx.lease_mgr             = lease_mgr;
  ctx.spill_scheduler       = spill_scheduler;
  ctx.spill_wait_timeout_ms = max_lease_ms;

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

  // Keep ownership of workers so they live for process lifetime.
  // TieringManager is stopped first so it stops enqueuing new tasks before
  // the spill workers drain and exit.
  app.background_workers.push_back(tiering_manager);
  app.background_workers.push_back(expiration_worker);
  for (auto& w : spill_worker_pool) {
    app.background_workers.push_back(w);
  }

  return app;
}

} // namespace payload::factory
