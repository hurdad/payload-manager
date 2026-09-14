#include "internal/db/postgres/pg_schema.hpp"

#include <pqxx/pqxx>

namespace payload::db::postgres {

void BootstrapSchema(const std::string& conninfo) {
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

} // namespace payload::db::postgres
