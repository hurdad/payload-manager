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
using payload::db::model::StreamConsumerOffsetRecord;
using payload::db::model::StreamEntryRecord;
using payload::db::model::StreamRecord;
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

void VerifyStreamReadWrite(Repository& repo, const std::string& stream_namespace, const std::string& stream_name) {
  StreamRecord stream;
  stream.stream_namespace      = stream_namespace;
  stream.name                  = stream_name;
  stream.retention_max_entries = 100;
  stream.retention_max_age_sec = 3600;

  {
    auto tx = repo.Begin();
    assert(repo.CreateStream(*tx, stream));
    assert(stream.stream_id != 0);
    tx->Commit();
  }

  {
    auto tx      = repo.Begin();
    auto by_name = repo.GetStreamByName(*tx, stream_namespace, stream_name);
    assert(by_name.has_value());
    assert(by_name->stream_id == stream.stream_id);

    auto by_id = repo.GetStreamById(*tx, stream.stream_id);
    assert(by_id.has_value());
    assert(by_id->name == stream_name);
    tx->Commit();
  }

  std::vector<StreamEntryRecord> entries;
  entries.push_back(StreamEntryRecord{.payload_uuid = stream_name + "-entry-0", .event_time_ms = 1000, .append_time_ms = 2000, .duration_ns = 10,
                                      .tags = R"({"kind":"seed"})"});
  entries.push_back(StreamEntryRecord{.payload_uuid = stream_name + "-entry-1", .event_time_ms = 1500, .append_time_ms = 2500, .duration_ns = 12,
                                      .tags = R"({"kind":"seed"})"});
  entries.push_back(StreamEntryRecord{.payload_uuid = stream_name + "-entry-2", .event_time_ms = 2000, .append_time_ms = 3500, .duration_ns = 14,
                                      .tags = R"({"kind":"seed"})"});

  {
    auto tx = repo.Begin();
    assert(repo.AppendStreamEntries(*tx, stream.stream_id, entries));
    assert(entries[0].offset == 0);
    assert(entries[1].offset == 1);
    assert(entries[2].offset == 2);

    auto max_offset = repo.GetMaxStreamOffset(*tx, stream.stream_id);
    assert(max_offset.has_value());
    assert(*max_offset == 2);
    tx->Commit();
  }

  {
    auto tx = repo.Begin();

    auto full_read = repo.ReadStreamEntries(*tx, stream.stream_id, 0, std::nullopt, std::nullopt);
    assert(full_read.size() == 3);

    auto limited_read = repo.ReadStreamEntries(*tx, stream.stream_id, 1, 1, std::nullopt);
    assert(limited_read.size() == 1);
    assert(limited_read[0].offset == 1);

    auto time_filtered = repo.ReadStreamEntries(*tx, stream.stream_id, 0, std::nullopt, 2600);
    assert(time_filtered.size() == 1);
    assert(time_filtered[0].offset == 2);

    auto ranged = repo.ReadStreamEntriesRange(*tx, stream.stream_id, 1, 2);
    assert(ranged.size() == 2);
    assert(ranged[0].offset == 1);
    assert(ranged[1].offset == 2);

    tx->Commit();
  }

  {
    auto tx = repo.Begin();
    assert(repo.TrimStreamEntriesToMaxCount(*tx, stream.stream_id, 2));
    auto remaining = repo.ReadStreamEntries(*tx, stream.stream_id, 0, std::nullopt, std::nullopt);
    assert(remaining.size() == 2);
    assert(remaining[0].offset == 1);
    assert(remaining[1].offset == 2);
    tx->Commit();
  }

  {
    auto tx = repo.Begin();
    assert(repo.DeleteStreamEntriesOlderThan(*tx, stream.stream_id, 3000));
    auto remaining = repo.ReadStreamEntries(*tx, stream.stream_id, 0, std::nullopt, std::nullopt);
    assert(remaining.size() == 1);
    assert(remaining[0].offset == 2);
    tx->Commit();
  }

  {
    auto tx = repo.Begin();

    StreamConsumerOffsetRecord offset{.stream_id = stream.stream_id, .consumer_group = stream_name + "-cg", .offset = 2, .updated_at_ms = 4500};
    assert(repo.CommitConsumerOffset(*tx, offset));

    auto read_offset = repo.GetConsumerOffset(*tx, stream.stream_id, offset.consumer_group);
    assert(read_offset.has_value());
    assert(read_offset->offset == 2);

    offset.offset = 7;
    assert(repo.CommitConsumerOffset(*tx, offset));

    auto updated_offset = repo.GetConsumerOffset(*tx, stream.stream_id, offset.consumer_group);
    assert(updated_offset.has_value());
    assert(updated_offset->offset == 7);

    tx->Commit();
  }

  {
    auto tx = repo.Begin();
    assert(repo.DeleteStreamByName(*tx, stream_namespace, stream_name));
    assert(!repo.GetStreamById(*tx, stream.stream_id).has_value());
    assert(repo.ReadStreamEntries(*tx, stream.stream_id, 0, std::nullopt, std::nullopt).empty());
    assert(!repo.GetConsumerOffset(*tx, stream.stream_id, stream_name + "-cg").has_value());
    tx->Commit();
  }
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
    db->Exec(
        "CREATE TABLE IF NOT EXISTS streams (stream_id INTEGER PRIMARY KEY AUTOINCREMENT, namespace TEXT NOT NULL, name TEXT NOT NULL, created_at "
        "INTEGER NOT NULL DEFAULT (unixepoch() * 1000), retention_max_entries INTEGER, retention_max_age_sec INTEGER, UNIQUE(namespace, name));");
    db->Exec(
        "CREATE TABLE IF NOT EXISTS stream_entries (stream_id INTEGER NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE, offset INTEGER NOT "
        "NULL, payload_uuid TEXT NOT NULL, event_time INTEGER, append_time INTEGER NOT NULL DEFAULT (unixepoch() * 1000), duration_ns INTEGER, tags "
        "TEXT, PRIMARY KEY (stream_id, offset));");
    db->Exec(
        "CREATE TABLE IF NOT EXISTS stream_consumer_offsets (stream_id INTEGER NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE, "
        "consumer_group TEXT NOT NULL, offset INTEGER NOT NULL, updated_at INTEGER NOT NULL DEFAULT (unixepoch() * 1000), PRIMARY KEY (stream_id, "
        "consumer_group));");
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
    tx.exec(
        "CREATE TABLE IF NOT EXISTS streams (stream_id BIGSERIAL PRIMARY KEY, namespace TEXT NOT NULL, name TEXT NOT NULL, created_at TIMESTAMPTZ "
        "NOT NULL DEFAULT now(), retention_max_entries BIGINT, retention_max_age_sec BIGINT, UNIQUE(namespace, name));");
    tx.exec(
        "CREATE TABLE IF NOT EXISTS stream_entries (stream_id BIGINT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE, offset BIGINT NOT "
        "NULL, payload_uuid UUID NOT NULL, event_time TIMESTAMPTZ, append_time TIMESTAMPTZ NOT NULL DEFAULT now(), duration_ns BIGINT, tags JSONB, "
        "PRIMARY KEY (stream_id, offset));");
    tx.exec(
        "CREATE TABLE IF NOT EXISTS stream_consumer_offsets (stream_id BIGINT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE, "
        "consumer_group TEXT NOT NULL, offset BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (stream_id, "
        "consumer_group));");
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
  VerifyStreamReadWrite(*repo, "integration", backend.name + "-stream");

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
