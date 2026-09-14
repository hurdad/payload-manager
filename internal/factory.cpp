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
#endif

namespace payload::factory {

using namespace payload;

namespace {

#if PAYLOAD_DB_POSTGRES
void BootstrapPostgresSchema(const std::string& conninfo) {
  pqxx::connection conn(conninfo);
  pqxx::work       tx(conn);

  tx.exec(
      "CREATE TABLE IF NOT EXISTS payload (id UUID PRIMARY KEY, tier SMALLINT NOT NULL, state SMALLINT NOT NULL, size_bytes BIGINT NOT NULL, version "
      "BIGINT NOT NULL, expires_at_ms BIGINT, no_evict SMALLINT NOT NULL DEFAULT 0, eviction_priority SMALLINT NOT NULL DEFAULT 0, spill_target "
      "SMALLINT NOT NULL DEFAULT 0, created_at_ms BIGINT NOT NULL DEFAULT 0, "
      "min_residency_tier SMALLINT NOT NULL DEFAULT 0, require_durable SMALLINT NOT NULL DEFAULT 0);");
  // Migrate existing databases that predate the eviction policy columns.
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS no_evict SMALLINT NOT NULL DEFAULT 0;");
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS eviction_priority SMALLINT NOT NULL DEFAULT 0;");
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS spill_target SMALLINT NOT NULL DEFAULT 0;");
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS created_at_ms BIGINT NOT NULL DEFAULT 0;");
  // Eviction policy extension: min residency tier and durability requirement.
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS min_residency_tier SMALLINT NOT NULL DEFAULT 0;");
  tx.exec("ALTER TABLE payload ADD COLUMN IF NOT EXISTS require_durable SMALLINT NOT NULL DEFAULT 0;");
  // Rename persist → no_evict for databases created before the field was renamed.
  tx.exec(
      "DO $$ BEGIN "
      "  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='payload' AND column_name='persist') THEN "
      "    ALTER TABLE payload RENAME COLUMN persist TO no_evict; "
      "  END IF; "
      "END $$;");
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
    ConnectWithBackoff([&] { BootstrapPostgresSchema(database.postgres().connection_uri()); });

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
  pressure_state->disk_limit = config.storage().disk().capacity_bytes();
  if (pressure_state->disk_limit == 0) {
    pressure_state->disk_limit = std::numeric_limits<uint64_t>::max();
  }

  // Eviction high-water marks. proto3 cannot tell "unset" from "0", so an
  // unset field takes the documented default of 80%; 100 means evict only at
  // the hard cap. GPU devices are configured individually but share one tier
  // budget, so take the first device that states a preference.
  constexpr uint32_t kDefaultEvictionHighWaterPct = 80;
  const auto         resolve_pct                  = [](uint32_t configured) { return configured == 0 ? kDefaultEvictionHighWaterPct : configured; };

  pressure_state->ram_evict_pct  = resolve_pct(config.storage().ram().eviction_high_water_pct());
  pressure_state->disk_evict_pct = resolve_pct(config.storage().disk().eviction_high_water_pct());

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
                    payload::observability::IntField("disk_capacity_bytes", static_cast<int64_t>(pressure_state->disk_limit)),
                    payload::observability::IntField("disk_evict_at_bytes", static_cast<int64_t>(pressure_state->DiskEvictThreshold())),
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
          return pm->ResolveSnapshot(id).tier() == manager::v1::TIER_DISK;
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
  // ------------------------------------------------------------------
  // Ring tier (TIER_RAM_RING)
  //
  // Slots share the tmpfs with the UUID-addressed RAM tier, and are named
  // /<prefix>-ring-<ring_id>-slot<i> so they cannot collide with /<prefix>-<uuid>.
  // RingTierManager::Build is a no-op when no rings are configured.
  // ------------------------------------------------------------------
  const std::string ring_default_prefix = !config.storage().ram().shm_prefix().empty() ? config.storage().ram().shm_prefix() : "pm";
  auto              ring_manager        = ring::RingTierManager::Build(config.storage().ring(), ring_default_prefix);
  auto              ring_lease_table    = std::make_shared<ring::RingLeaseTable>();
  auto              ring_service        = std::make_shared<service::RingService>(ring_manager.get(), ring_lease_table.get());

  app.grpc_services.push_back(std::make_unique<grpc::DataServer>(data_service));
  app.grpc_services.push_back(std::make_unique<grpc::RingServer>(ring_service));
  app.grpc_services.push_back(std::make_unique<grpc::CatalogServer>(catalog_service));
  app.grpc_services.push_back(std::make_unique<grpc::AdminServer>(admin_service));
  const uint32_t poll_ms       = config.stream().subscribe_poll_interval_ms();
  const auto     poll_interval = poll_ms > 0 ? std::chrono::milliseconds(poll_ms) : grpc::StreamServer::kDefaultPollInterval;
  app.grpc_services.push_back(std::make_unique<grpc::StreamServer>(stream_service, poll_interval));

  // RingService holds non-owning pointers into these, so they must outlive it.
  app.ring_manager     = std::move(ring_manager);
  app.ring_lease_table = std::move(ring_lease_table);

  // Keep ownership of workers so they live for process lifetime.
  // TieringManager is stopped first so it stops enqueuing new tasks before
  // the spill workers drain and exit.
  app.background_workers.push_back(tiering_manager);
  for (auto& w : spill_worker_pool) {
    app.background_workers.push_back(w);
  }

  return app;
}

} // namespace payload::factory
