#include "factory.hpp"

#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "internal/core/payload_manager.hpp"
#include "internal/db/api/repository.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/grpc/admin_server.hpp"
#include "internal/grpc/catalog_server.hpp"
#include "internal/grpc/data_server.hpp"
#include "internal/grpc/ring_server.hpp"
#include "internal/grpc/stream_server.hpp"
#include "internal/lease/lease_manager.hpp"
#include "internal/lineage/lineage_graph.hpp"
#include "internal/metadata/metadata_cache.hpp"
#include "internal/observability/logging.hpp"
#include "internal/ring/ring_metrics_publisher.hpp"
#include "internal/service/admin_service.hpp"
#include "internal/service/catalog_service.hpp"
#include "internal/service/data_service.hpp"
#include "internal/service/ring_service.hpp"
#include "internal/service/service_context.hpp"
#include "internal/service/stream_service.hpp"
#include "internal/spill/spill_scheduler.hpp"
#include "internal/spill/spill_worker.hpp"
#include "internal/storage/ram/ram_arrow_store.hpp"
#include "internal/storage/storage_factory.hpp"
#include "internal/tiering/pressure_state.hpp"
#include "internal/tiering/tiering_manager.hpp"
#include "internal/tiering/tiering_policy.hpp"
#if PAYLOAD_DB_POSTGRES
#include "internal/db/postgres/pg_pool.hpp"
#include "internal/db/postgres/pg_repository.hpp"
#include "internal/db/postgres/pg_schema.hpp"
#endif

namespace payload::factory {

using namespace payload;

namespace {

#if PAYLOAD_DB_POSTGRES
/*
  Retry the first connection with bounded backoff.

  Postgres reports itself started before its listener accepts connections, so on
  a cold boot this process would get "connection refused" and exit 2. Compose
  restarts it, but the gRPC clients that already resolved the name cache the
  negative DNS result and loop on "Domain name not found" until they are
  restarted too — a stack-wide failure from a few seconds of startup skew.

  Retrying here fixes it regardless of whether the orchestrator has a
  healthcheck configured. The ceiling is deliberately finite: a genuinely
  misconfigured connection string should still fail the process rather than hang
  forever pretending to start.
*/
void ConnectWithBackoff(const std::function<void()>& attempt) {
  constexpr int  kMaxAttempts  = 10;
  constexpr auto kInitialDelay = std::chrono::milliseconds(250);
  constexpr auto kMaxDelay     = std::chrono::seconds(5);

  auto delay = kInitialDelay;
  for (int i = 1;; ++i) {
    try {
      attempt();
      if (i > 1) {
        PAYLOAD_LOG_INFO("connected to postgres", {payload::observability::IntField("attempts", i)});
      }
      return;
    } catch (const std::exception& e) {
      if (i >= kMaxAttempts) {
        throw std::runtime_error("could not reach postgres after " + std::to_string(kMaxAttempts) + " attempts: " + e.what());
      }
      PAYLOAD_LOG_WARN("postgres not reachable yet; retrying",
                       {payload::observability::IntField("attempt", i), payload::observability::IntField("of", kMaxAttempts),
                        payload::observability::IntField("retry_in_ms", static_cast<int64_t>(delay.count())),
                        payload::observability::StringField("error", e.what())});
      std::this_thread::sleep_for(delay);
      delay = std::min(std::chrono::duration_cast<std::chrono::milliseconds>(delay * 2),
                       std::chrono::duration_cast<std::chrono::milliseconds>(kMaxDelay));
    }
  }
}
#endif

std::shared_ptr<db::Repository> BuildRepository(const payload::runtime::config::RuntimeConfig& config) {
  const auto& database = config.database();
  if (database.has_postgres()) {
#if PAYLOAD_DB_POSTGRES
    ConnectWithBackoff([&] { payload::db::postgres::BootstrapSchema(database.postgres().connection_uri()); });

    // Single-instance guard: acquire a PostgreSQL session-level advisory lock.
    // pg_try_advisory_lock returns true only if this session obtained the lock.
    // The lock is held until the connection is closed (process exit), so a
    // second instance connecting to the same database will fail here rather
    // than silently diverging the snapshot cache.
    static constexpr int64_t                 kSingleInstanceLockId = 0x5041594C4F414400LL; // "PAYLOAD\0"
    static std::unique_ptr<pqxx::connection> s_pg_guard;                                   // held for process lifetime
    s_pg_guard = std::make_unique<pqxx::connection>(database.postgres().connection_uri());
    {
      pqxx::nontransaction ntx(*s_pg_guard);
      auto                 res = ntx.exec("SELECT pg_try_advisory_lock($1);", pqxx::params{kSingleInstanceLockId});
      if (res.empty() || !res[0][0].as<bool>()) {
        s_pg_guard.reset();
        throw std::runtime_error(
            "payload-manager: another instance is already connected to this PostgreSQL database. "
            "Only one payload-manager instance may run per database.");
      }
    }

    auto pool = std::make_shared<db::postgres::PgPool>(database.postgres().connection_uri(), database.postgres().max_connections());
    PAYLOAD_LOG_INFO("using the postgres catalog",
                     {payload::observability::IntField("max_connections", static_cast<int64_t>(database.postgres().max_connections()))});
    return std::make_shared<db::postgres::PgRepository>(std::move(pool));
#else
    throw std::runtime_error("postgres backend requested but not enabled at build time");
#endif
  }

  // Explicitly chosen, or fallen back to. Either way say which, at a level
  // that matches the consequence: an in-memory catalog loses every payload
  // reference on restart, and nothing else in the logs would tell you that is
  // the mode you are running in.
  if (database.has_memory()) {
    PAYLOAD_LOG_INFO("using the in-memory catalog; payload references do not survive restart", {});
  } else {
    PAYLOAD_LOG_WARN(
        "no database backend configured — falling back to the in-memory catalog; "
        "payload references do not survive restart. Set database.memory to silence this, "
        "or database.postgres to persist",
        {});
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

  auto payload_manager = std::make_shared<core::PayloadManager>(storage_map, lease_mgr, repository, metadata_cache);
  payload_manager->HydrateCaches();

  // ------------------------------------------------------------------
  // Reclaim shm segments left behind by a previous process.
  //
  // Runs once the repository is available, because a segment is only an orphan
  // if no payload record refers to it. The set covers every tier, not just
  // TIER_RAM: a payload part-way through a spill still owns its segment.
  //
  // With the in-memory repository the set is empty at startup, so every
  // surviving segment is an orphan by definition — which is correct, since
  // nothing could refer to them.
  // ------------------------------------------------------------------
  if (auto ram_it = storage_map.find(payload::manager::v1::TIER_RAM); ram_it != storage_map.end() && ram_it->second) {
    if (auto* ram_store = dynamic_cast<storage::RamArrowStore*>(ram_it->second.get())) {
      std::unordered_set<std::string> known;
      try {
        auto tx      = repository->Begin();
        auto records = repository->ListPayloads(*tx);
        known.reserve(records.size());
        for (const auto& r : records) {
          known.insert(payload::util::ToString(r.id));
        }
        tx->Commit();
      } catch (const std::exception& e) {
        // Without a reliable known-set a sweep would delete live data, so skip
        // it rather than guess.
        PAYLOAD_LOG_WARN("shm orphan sweep skipped; could not list payloads", {payload::observability::StringField("error", e.what())});
        known.clear();
        ram_store = nullptr;
      }

      if (ram_store != nullptr) {
        const auto removed = ram_store->PurgeOrphans(known);
        if (removed > 0) {
          PAYLOAD_LOG_INFO("reclaimed orphaned shm segments",
                           {payload::observability::IntField("removed", static_cast<int64_t>(removed)),
                            payload::observability::IntField("known_payloads", static_cast<int64_t>(known.size()))});
        }
      }
    }
  }

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
  // A limit of 0 means "unconfigured / no cap". Set to UINT64_MAX so
  // pressure checks never trigger automatic eviction for that tier.
  if (pressure_state->ram_limit == 0) {
    pressure_state->ram_limit = std::numeric_limits<uint64_t>::max();
  }
  for (const auto& dev : config.storage().gpu().devices()) {
    pressure_state->gpu_limit += dev.capacity_bytes();
  }
  if (pressure_state->gpu_limit == 0) {
    pressure_state->gpu_limit = std::numeric_limits<uint64_t>::max();
  }
  pressure_state->disk_hot_limit = config.storage().disk_hot().capacity_bytes();
  if (pressure_state->disk_hot_limit == 0) {
    pressure_state->disk_hot_limit = std::numeric_limits<uint64_t>::max();
  }
  pressure_state->disk_cold_limit = config.storage().disk_cold().capacity_bytes();
  if (pressure_state->disk_cold_limit == 0) {
    pressure_state->disk_cold_limit = std::numeric_limits<uint64_t>::max();
  }

  // Eviction high-water marks. proto3 cannot tell "unset" from "0", so an
  // unset field takes the documented default of 80%; 100 means evict only at
  // the hard cap. GPU devices are configured individually but share one tier
  // budget, so take the first device that states a preference.
  constexpr uint32_t kDefaultEvictionHighWaterPct = 80;
  const auto         resolve_pct                  = [](uint32_t configured) { return configured == 0 ? kDefaultEvictionHighWaterPct : configured; };

  pressure_state->ram_evict_pct       = resolve_pct(config.storage().ram().eviction_high_water_pct());
  pressure_state->disk_hot_evict_pct  = resolve_pct(config.storage().disk_hot().eviction_high_water_pct());
  pressure_state->disk_cold_evict_pct = resolve_pct(config.storage().disk_cold().eviction_high_water_pct());

  uint32_t gpu_pct = 0;
  for (const auto& dev : config.storage().gpu().devices()) {
    if (dev.eviction_high_water_pct() != 0) {
      gpu_pct = dev.eviction_high_water_pct();
      break;
    }
  }
  pressure_state->gpu_evict_pct = resolve_pct(gpu_pct);

  // Clamp the RAM capacity to the tmpfs that actually backs /dev/shm.
  //
  // A capacity larger than the medium is unserviceable: this service would
  // happily accept allocations up to the configured figure, and the kernel
  // would SIGBUS a producer partway through. The mismatch is easy to arrive at
  // — compose's shm_size defaults to 64m, and WSL2 sizes /dev/shm from host
  // memory — so clamp down and say so loudly rather than failing later and
  // somewhere else. The eviction threshold is derived from the limit, so it
  // follows automatically.
  //
  // Ring slots live on the same tmpfs and are allocated in full at startup, so
  // they must come out of the budget before the RAM tier claims any of it.
  // Computed from config rather than from the built manager because the rings
  // are constructed later — and because an unserviceable ring configuration
  // should be refused before anything is allocated.
  uint64_t ring_total_bytes = 0;
  for (const auto& ring : config.storage().ring().rings()) {
    ring_total_bytes += static_cast<uint64_t>(ring.n_slots()) * ring.slot_size_bytes();
  }

  if (const auto shm_total = storage::RamArrowStore::ShmTotalBytes(); shm_total.has_value()) {
    if (ring_total_bytes > *shm_total) {
      // Not clampable: the rings alone cannot fit, so every slot cannot be
      // created. Dying here beats SIGBUSing a producer on its first write.
      throw std::runtime_error("ring tier requires " + std::to_string(ring_total_bytes) + " bytes but the tmpfs backing /dev/shm is only " +
                               std::to_string(*shm_total) + " bytes; reduce n_slots or slot_size_bytes, or raise the container's shm_size");
    }

    const uint64_t ram_budget = *shm_total - ring_total_bytes;
    if (pressure_state->ram_limit != std::numeric_limits<uint64_t>::max() && pressure_state->ram_limit > ram_budget) {
      PAYLOAD_LOG_WARN("configured RAM capacity exceeds the tmpfs left after ring reservations; clamping",
                       {payload::observability::IntField("configured_bytes", static_cast<int64_t>(pressure_state->ram_limit)),
                        payload::observability::IntField("tmpfs_bytes", static_cast<int64_t>(*shm_total)),
                        payload::observability::IntField("ring_reserved_bytes", static_cast<int64_t>(ring_total_bytes)),
                        payload::observability::IntField("ram_budget_bytes", static_cast<int64_t>(ram_budget))});
      pressure_state->ram_limit = ram_budget;
    }
  }

  // Hand the resolved limits to the manager so Allocate can refuse requests a
  // tier cannot take, rather than letting the producer discover it as SIGBUS.
  payload_manager->SetPressureState(pressure_state);
  payload_manager->SetDefaultPayloadTtlMs(config.default_payload_ttl_ms());
  if (config.default_payload_ttl_ms() > 0) {
    PAYLOAD_LOG_INFO("applying a default TTL to payloads allocated without one",
                     {payload::observability::IntField("default_payload_ttl_ms", static_cast<int64_t>(config.default_payload_ttl_ms()))});
  }

  PAYLOAD_LOG_INFO("resolved tier limits",
                   {payload::observability::IntField("ram_capacity_bytes", static_cast<int64_t>(pressure_state->ram_limit)),
                    payload::observability::IntField("ram_evict_at_bytes", static_cast<int64_t>(pressure_state->RamEvictThreshold())),
                    payload::observability::IntField("disk_hot_capacity_bytes", static_cast<int64_t>(pressure_state->disk_hot_limit)),
                    payload::observability::IntField("disk_hot_evict_at_bytes", static_cast<int64_t>(pressure_state->DiskHotEvictThreshold())),
                    payload::observability::IntField("disk_cold_capacity_bytes", static_cast<int64_t>(pressure_state->disk_cold_limit)),
                    payload::observability::IntField("disk_cold_evict_at_bytes", static_cast<int64_t>(pressure_state->DiskColdEvictThreshold())),
                    payload::observability::IntField("gpu_capacity_bytes", static_cast<int64_t>(pressure_state->gpu_limit)),
                    payload::observability::IntField("gpu_evict_at_bytes", static_cast<int64_t>(pressure_state->GpuEvictThreshold())),
                    payload::observability::IntField("ring_reserved_bytes", static_cast<int64_t>(ring_total_bytes))});

  auto tiering_policy = std::make_shared<tiering::TieringPolicy>(
      metadata_cache,
      // RAM eviction: only consider payloads currently resident in RAM.
      [pm = payload_manager.get()](const manager::v1::PayloadID& id) {
        if (pm->IsEvictionExempt(id)) return false;
        try {
          return pm->ResolveSnapshot(id).tier() == manager::v1::TIER_RAM;
        } catch (...) {
          return false;
        }
      },
      // GPU eviction: only consider payloads currently resident on GPU.
      [pm = payload_manager.get()](const manager::v1::PayloadID& id) {
        if (pm->IsEvictionExempt(id)) return false;
        try {
          return pm->ResolveSnapshot(id).tier() == manager::v1::TIER_GPU;
        } catch (...) {
          return false;
        }
      },
      // Disk eviction: only consider payloads currently resident on disk.
      [pm = payload_manager.get()](const manager::v1::PayloadID& id) {
        if (pm->IsEvictionExempt(id)) return false;
        try {
          return pm->ResolveSnapshot(id).tier() == manager::v1::TIER_DISK_HOT;
        } catch (...) {
          return false;
        }
      },
      // Cold-disk eviction: only consider payloads currently on the cold level.
      [pm = payload_manager.get()](const manager::v1::PayloadID& id) {
        if (pm->IsEvictionExempt(id)) return false;
        try {
          return pm->ResolveSnapshot(id).tier() == manager::v1::TIER_DISK_COLD;
        } catch (...) {
          return false;
        }
      });

  auto tiering_manager = std::make_shared<tiering::TieringManager>(tiering_policy, spill_scheduler, payload_manager, pressure_state);
  tiering_manager->Start();

  // ------------------------------------------------------------------
  // Services
  // Note: expiration is handled by TieringManager::Loop (calls ExpireStale
  // every 100 ms), so a separate ExpirationWorker is not needed here.
  // ------------------------------------------------------------------

  // ------------------------------------------------------------------
  // Ring tier (TIER_RAM_RING)
  //
  // Slots share the tmpfs with the UUID-addressed RAM tier, and are named
  // /<prefix>-ring-<ring_id>-slot<i> so they cannot collide with /<prefix>-<uuid>.
  // RingTierManager::Build is a no-op when no rings are configured.
  //
  // Built before the ServiceContext because AdminService::Stats reports
  // per-ring slot accounting and needs the manager in ctx.
  // ------------------------------------------------------------------
  const std::string ring_default_prefix = !config.storage().ram().shm_prefix().empty() ? config.storage().ram().shm_prefix() : "pm";
  auto              ring_manager        = ring::RingTierManager::Build(config.storage().ring(), ring_default_prefix);
  auto              ring_lease_table    = std::make_shared<ring::RingLeaseTable>();
  auto              ring_service        = std::make_shared<service::RingService>(ring_manager.get(), ring_lease_table.get());

  service::ServiceContext ctx;
  ctx.ring_mgr              = ring_manager.get();
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
  app.grpc_services.push_back(std::make_unique<grpc::RingServer>(ring_service));
  app.grpc_services.push_back(std::make_unique<grpc::CatalogServer>(catalog_service));
  app.grpc_services.push_back(std::make_unique<grpc::AdminServer>(admin_service));
  const uint32_t poll_ms       = config.stream().subscribe_poll_interval_ms();
  const auto     poll_interval = poll_ms > 0 ? std::chrono::milliseconds(poll_ms) : grpc::StreamServer::kDefaultPollInterval;
  app.grpc_services.push_back(std::make_unique<grpc::StreamServer>(stream_service, poll_interval));

  // Sample ring slot accounting into the gauges. Started before the
  // manager moves into Application: it holds a raw pointer, and moving a
  // unique_ptr does not move what it points at.
  auto ring_metrics_publisher = std::make_shared<ring::RingMetricsPublisher>(ring_manager.get());
  ring_metrics_publisher->Start();

  // RingService and RingMetricsPublisher hold non-owning pointers into
  // these, so they must outlive both.
  app.ring_manager     = std::move(ring_manager);
  app.ring_lease_table = std::move(ring_lease_table);

  // Keep ownership of workers so they live for process lifetime.
  // TieringManager is stopped first so it stops enqueuing new tasks before
  // the spill workers drain and exit.
  app.background_workers.push_back(tiering_manager);
  app.background_workers.push_back(ring_metrics_publisher);
  for (auto& w : spill_worker_pool) {
    app.background_workers.push_back(w);
  }

  return app;
}

} // namespace payload::factory
