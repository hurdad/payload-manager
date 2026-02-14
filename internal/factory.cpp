#include "factory.hpp"

#include <memory>

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

namespace payload::factory {

using namespace payload;

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
