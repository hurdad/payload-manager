#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "internal/db/api/repository.hpp"
#include "internal/db/memory/memory_repository.hpp"
#include "internal/db/model/lineage_record.hpp"
#include "internal/db/model/metadata_record.hpp"
#include "internal/db/model/payload_record.hpp"

#if PAYLOAD_DB_SQLITE
#include "internal/db/sqlite/sqlite_db.hpp"
#include "internal/db/sqlite/sqlite_repository.hpp"
#endif

#if PAYLOAD_DB_POSTGRES
#include "internal/db/postgres/pg_pool.hpp"
#include "internal/db/postgres/pg_repository.hpp"
#endif

namespace {

using payload::db::Repository;
using payload::db::memory::MemoryRepository;
using payload::db::model::LineageRecord;
using payload::db::model::MetadataRecord;
using payload::db::model::PayloadRecord;
using payload::manager::v1::PAYLOAD_STATE_ACTIVE;
using payload::manager::v1::PAYLOAD_STATE_ALLOCATED;
using payload::manager::v1::TIER_RAM;

uint64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

struct BackendFactory {
  std::string                                       name;
  std::function<std::shared_ptr<Repository>()>      make_repository;
  std::function<bool()>                             supports_restart;
  std::function<void(std::shared_ptr<Repository>&)> restart;
  std::function<void()>                             cleanup;
  bool                                              supports_parallel_transactions = true;
};

void VerifyAllocateCommitResolveDelete(Repository& repo, const std::string& id) {
  auto tx = repo.Begin();

  PayloadRecord payload;
  payload.id         = id;
  payload.tier       = TIER_RAM;
  payload.state      = PAYLOAD_STATE_ALLOCATED;
  payload.size_bytes = 2048;
  payload.version    = 1;

  auto insert = repo.InsertPayload(*tx, payload);
  assert(insert);

  auto resolved = repo.GetPayload(*tx, id);
  assert(resolved.has_value());
  assert(resolved->state == PAYLOAD_STATE_ALLOCATED);

  resolved->state   = PAYLOAD_STATE_ACTIVE;
  resolved->version = 2;
  auto update       = repo.UpdatePayload(*tx, *resolved);
  assert(update);

  auto committed = repo.GetPayload(*tx, id);
  assert(committed.has_value());
  assert(committed->state == PAYLOAD_STATE_ACTIVE);
  assert(committed->version == 2);

  auto del = repo.DeletePayload(*tx, id);
  assert(del);
  assert(!repo.GetPayload(*tx, id).has_value());

  tx->Commit();
}

void VerifyMetadataReadWrite(Repository& repo, const std::string& id) {
  auto tx = repo.Begin();

  PayloadRecord payload;
  payload.id         = id;
  payload.tier       = TIER_RAM;
  payload.state      = PAYLOAD_STATE_ACTIVE;
  payload.size_bytes = 128;
  payload.version    = 1;
  assert(repo.InsertPayload(*tx, payload));

  MetadataRecord metadata;
  metadata.id            = id;
  metadata.json          = R"({"stage":"raw"})";
  metadata.schema        = "schema.v1";
  metadata.updated_at_ms = NowMs();
  assert(repo.UpsertMetadata(*tx, metadata));

  auto read = repo.GetMetadata(*tx, id);
  assert(read.has_value());
  assert(read->json == metadata.json);
  assert(read->schema == metadata.schema);

  metadata.json          = R"({"stage":"processed"})";
  metadata.updated_at_ms = NowMs() + 1000;
  assert(repo.UpsertMetadata(*tx, metadata));

  auto updated = repo.GetMetadata(*tx, id);
  assert(updated.has_value());
  assert(updated->json == metadata.json);

  tx->Commit();
}

void VerifyLineageReadWrite(Repository& repo, const std::string& parent_id, const std::string& child_id) {
  auto tx = repo.Begin();

  PayloadRecord parent{.id = parent_id, .tier = TIER_RAM, .state = PAYLOAD_STATE_ACTIVE, .size_bytes = 1, .version = 1};
  PayloadRecord child{.id = child_id, .tier = TIER_RAM, .state = PAYLOAD_STATE_ACTIVE, .size_bytes = 1, .version = 1};

  assert(repo.InsertPayload(*tx, parent));
  assert(repo.InsertPayload(*tx, child));

  LineageRecord edge;
  edge.parent_id     = parent_id;
  edge.child_id      = child_id;
  edge.operation     = "fft";
  edge.role          = "input";
  edge.parameters    = "{}";
  edge.created_at_ms = NowMs();

  assert(repo.InsertLineage(*tx, edge));

  const auto parents = repo.GetParents(*tx, child_id);
  assert(parents.size() == 1);
  assert(parents[0].parent_id == parent_id);

  const auto children = repo.GetChildren(*tx, parent_id);
  assert(children.size() == 1);
  assert(children[0].child_id == child_id);

  tx->Commit();
}

void VerifyRollbackBehavior(Repository& repo, const std::string& id) {
  {
    auto          tx = repo.Begin();
    PayloadRecord payload{.id = id, .tier = TIER_RAM, .state = PAYLOAD_STATE_ALLOCATED, .size_bytes = 64, .version = 1};
    assert(repo.InsertPayload(*tx, payload));
    tx->Rollback();
  }

  auto check_tx = repo.Begin();
  assert(!repo.GetPayload(*check_tx, id).has_value());
  check_tx->Commit();
}

void VerifyConcurrentUpdates(Repository& repo, const std::string& id, bool supports_parallel_transactions) {
  {
    auto          tx = repo.Begin();
    PayloadRecord seed{.id = id, .tier = TIER_RAM, .state = PAYLOAD_STATE_ALLOCATED, .size_bytes = 64, .version = 1};
    assert(repo.InsertPayload(*tx, seed));
    tx->Commit();
  }

  auto tx1 = repo.Begin();
  if (!supports_parallel_transactions) {
    bool threw = false;
    try {
      auto tx2 = repo.Begin();
      (void)tx2;
    } catch (const std::exception&) {
      threw = true;
    }
    assert(threw);
    tx1->Rollback();
    return;
  }

  auto tx2 = repo.Begin();

  auto r1 = repo.GetPayload(*tx1, id);
  auto r2 = repo.GetPayload(*tx2, id);
  assert(r1.has_value() && r2.has_value());

  r1->version = 2;
  r2->version = 3;

  assert(repo.UpdatePayload(*tx1, *r1));
  tx1->Commit();

  assert(repo.UpdatePayload(*tx2, *r2));
  tx2->Commit();

  auto verify_tx = repo.Begin();
  auto final     = repo.GetPayload(*verify_tx, id);
  assert(final.has_value());
  assert(final->version == 3);
  verify_tx->Commit();
}

void VerifyRestartDurability(BackendFactory& backend, const std::string& id) {
  if (!backend.supports_restart()) {
    return;
  }

  auto repo = backend.make_repository();
  {
    auto tx = repo->Begin();

    PayloadRecord payload{.id = id, .tier = TIER_RAM, .state = PAYLOAD_STATE_ACTIVE, .size_bytes = 1024, .version = 11};
    assert(repo->InsertPayload(*tx, payload));

    MetadataRecord metadata{.id = id, .json = R"({"k":"v"})", .schema = "schema.v1", .updated_at_ms = NowMs()};
    assert(repo->UpsertMetadata(*tx, metadata));

    LineageRecord lineage{
        .parent_id = id, .child_id = id + "-child", .operation = "copy", .role = "parent", .parameters = "{}", .created_at_ms = NowMs()};

    PayloadRecord child{.id = id + "-child", .tier = TIER_RAM, .state = PAYLOAD_STATE_ACTIVE, .size_bytes = 512, .version = 1};
    assert(repo->InsertPayload(*tx, child));
    assert(repo->InsertLineage(*tx, lineage));

    tx->Commit();
  }

  backend.restart(repo);

  auto tx = repo->Begin();
  auto p  = repo->GetPayload(*tx, id);
  assert(p.has_value());
  assert(p->version == 11);

  auto m = repo->GetMetadata(*tx, id);
  assert(m.has_value());
  assert(m->json == R"({"k":"v"})");

  auto children = repo->GetChildren(*tx, id);
  assert(children.size() == 1);
  assert(children[0].child_id == id + "-child");
  tx->Commit();

  backend.cleanup();
}

BackendFactory MakeMemoryFactory() {
  return BackendFactory{
      .name                           = "memory",
      .make_repository                = []() { return std::make_shared<MemoryRepository>(); },
      .supports_restart               = []() { return false; },
      .restart                        = [](std::shared_ptr<Repository>&) {},
      .cleanup                        = []() {},
      .supports_parallel_transactions = true,
  };
}

#if PAYLOAD_DB_SQLITE
BackendFactory MakeSqliteFactory() {
  auto db_path = (std::filesystem::temp_directory_path() / ("payload_manager_integration_sqlite_" + std::to_string(NowMs()) + ".db")).string();

  auto make_repo = [db_path]() {
    auto db = std::make_shared<payload::db::sqlite::SqliteDB>(db_path);
    db->Exec(
        "CREATE TABLE IF NOT EXISTS payload (id TEXT PRIMARY KEY, tier INTEGER NOT NULL, state INTEGER NOT NULL, size_bytes INTEGER NOT NULL, "
        "version INTEGER NOT NULL, expires_at_ms INTEGER);");
    db->Exec(
        "CREATE TABLE IF NOT EXISTS payload_metadata (id TEXT PRIMARY KEY, json TEXT NOT NULL, schema TEXT, updated_at_ms INTEGER NOT NULL, FOREIGN "
        "KEY(id) REFERENCES payload(id) ON DELETE CASCADE);");
    db->Exec(
        "CREATE TABLE IF NOT EXISTS payload_lineage (parent_id TEXT NOT NULL, child_id TEXT NOT NULL, operation TEXT, role TEXT, parameters TEXT, "
        "created_at_ms INTEGER NOT NULL, FOREIGN KEY(parent_id) REFERENCES payload(id) ON DELETE CASCADE, FOREIGN KEY(child_id) REFERENCES "
        "payload(id) ON DELETE CASCADE);");
    return std::make_shared<payload::db::sqlite::SqliteRepository>(std::move(db));
  };

  return BackendFactory{
      .name                           = "sqlite",
      .make_repository                = make_repo,
      .supports_restart               = []() { return true; },
      .restart                        = [make_repo](std::shared_ptr<Repository>& repo) { repo = make_repo(); },
      .cleanup                        = [db_path]() { std::filesystem::remove(db_path); },
      .supports_parallel_transactions = false,
  };
}
#endif

#if PAYLOAD_DB_POSTGRES
BackendFactory MakePostgresFactory() {
  const char* uri = std::getenv("PAYLOAD_TEST_POSTGRES_URI");
  if (uri == nullptr || std::string(uri).empty()) {
    throw std::runtime_error("PAYLOAD_TEST_POSTGRES_URI is not set");
  }

  auto conninfo  = std::string(uri);
  auto make_repo = [conninfo]() {
    auto       pool = std::make_shared<payload::db::postgres::PgPool>(conninfo);
    auto       conn = pool->Acquire();
    pqxx::work tx(*conn);
    tx.exec(
        "CREATE TABLE IF NOT EXISTS payload (id TEXT PRIMARY KEY, tier SMALLINT NOT NULL, state SMALLINT NOT NULL, size_bytes BIGINT NOT NULL, "
        "version BIGINT NOT NULL, expires_at_ms BIGINT);");
    tx.exec(
        "CREATE TABLE IF NOT EXISTS payload_metadata (id TEXT PRIMARY KEY REFERENCES payload(id) ON DELETE CASCADE, json JSONB NOT NULL, schema "
        "TEXT, updated_at_ms BIGINT NOT NULL);");
    tx.exec(
        "CREATE TABLE IF NOT EXISTS payload_lineage (parent_id TEXT NOT NULL REFERENCES payload(id) ON DELETE CASCADE, child_id TEXT NOT NULL "
        "REFERENCES payload(id) ON DELETE CASCADE, operation TEXT, role TEXT, parameters TEXT, created_at_ms BIGINT NOT NULL);");
    tx.commit();
    return std::make_shared<payload::db::postgres::PgRepository>(std::move(pool));
  };

  return BackendFactory{
      .name                           = "postgres",
      .make_repository                = make_repo,
      .supports_restart               = []() { return true; },
      .restart                        = [make_repo](std::shared_ptr<Repository>& repo) { repo = make_repo(); },
      .cleanup                        = []() {},
      .supports_parallel_transactions = true,
  };
}
#endif

void RunBackendSuite(BackendFactory& backend) {
  std::cout << "running backend suite: " << backend.name << "\n";
  auto repo = backend.make_repository();

  VerifyAllocateCommitResolveDelete(*repo, backend.name + "-payload-life");
  VerifyMetadataReadWrite(*repo, backend.name + "-metadata");
  VerifyLineageReadWrite(*repo, backend.name + "-lineage-parent", backend.name + "-lineage-child");
  VerifyRollbackBehavior(*repo, backend.name + "-rollback");
  VerifyConcurrentUpdates(*repo, backend.name + "-concurrency", backend.supports_parallel_transactions);

  VerifyRestartDurability(backend, backend.name + "-durable");

  backend.cleanup();
}

} // namespace

int main() {
  std::vector<BackendFactory> backends;
  backends.push_back(MakeMemoryFactory());

#if PAYLOAD_DB_SQLITE
  backends.push_back(MakeSqliteFactory());
#endif

#if PAYLOAD_DB_POSTGRES
  try {
    backends.push_back(MakePostgresFactory());
  } catch (const std::exception& ex) {
    std::cout << "skipping postgres integration suite: " << ex.what() << "\n";
  }
#endif

  for (auto& backend : backends) {
    RunBackendSuite(backend);
  }

  std::cout << "payload_manager_integration_repository_parity: pass\n";
  return 0;
}
