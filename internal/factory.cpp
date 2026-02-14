#include "factory.hpp"

#include <memory>
#include <stdexcept>

#include "internal/core/payload_manager.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/lineage/lineage_graph.hpp"

#include "internal/spill/spill_scheduler.hpp"
#include "internal/spill/spill_worker.hpp"

#include "internal/storage/storage_factory.hpp"

#include "internal/service/service_context.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/admin_service.hpp"
#include "internal/service/stream_service.hpp"

#include "internal/grpc/data_server.hpp"
#include "internal/grpc/catalog_server.hpp"
#include "internal/grpc/admin_server.hpp"
#include "internal/grpc/stream_server.hpp"

#include "internal/db/api/repository.hpp"
#include "internal/db/memory/memory_repository.hpp"
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

std::shared_ptr<db::Repository>
BuildRepository(const payload::runtime::config::RuntimeConfig& config) {
  const auto& database = config.database();
  if (database.has_sqlite()) {
#if PAYLOAD_DB_SQLITE
    auto sqlite_db = std::make_shared<db::sqlite::SqliteDB>(database.sqlite().path());
    return std::make_shared<db::sqlite::SqliteRepository>(std::move(sqlite_db));
#else
    throw std::runtime_error("sqlite backend requested but not enabled at build time");
#endif
  }

  if (database.has_postgres()) {
#if PAYLOAD_DB_POSTGRES
    auto pool = std::make_shared<db::postgres::PgPool>(database.postgres().connection_uri());
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
    auto storage_map =
        storage::StorageFactory::Build(config.storage());

    // ------------------------------------------------------------------
    // Core components
    // ------------------------------------------------------------------
    auto lease_mgr = std::make_shared<lease::LeaseManager>();
    auto metadata_cache = std::make_shared<metadata::MetadataCache>();
    auto lineage_graph = std::make_shared<lineage::LineageGraph>();
    auto repository = BuildRepository(config);

    auto payload_manager = std::make_shared<core::PayloadManager>(
        storage_map,
        lease_mgr,
        metadata_cache,
        lineage_graph
    );

    // ------------------------------------------------------------------
    // Spill system
    // ------------------------------------------------------------------
    auto spill_scheduler = std::make_shared<spill::SpillScheduler>();
    auto spill_worker = std::make_shared<spill::SpillWorker>(
        spill_scheduler,
        payload_manager
    );

    spill_worker->Start();

    // ------------------------------------------------------------------
    // Services
    // ------------------------------------------------------------------
    service::ServiceContext ctx;
    ctx.manager = payload_manager;
    ctx.metadata = metadata_cache;
    ctx.lineage = lineage_graph;
    ctx.repository = repository;

    auto data_service = std::make_shared<service::DataService>(ctx);
    auto catalog_service = std::make_shared<service::CatalogService>(ctx);
    auto admin_service = std::make_shared<service::AdminService>(ctx);
    auto stream_service = std::make_shared<service::StreamService>(ctx);

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

}
