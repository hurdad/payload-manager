-- ============================================================
-- Add eviction policy and creation timestamp columns to payload.
-- Safe to run on existing databases: ADD COLUMN IF NOT EXISTS is idempotent.
-- ============================================================

ALTER TABLE payload ADD COLUMN IF NOT EXISTS no_evict          SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE payload ADD COLUMN IF NOT EXISTS eviction_priority SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE payload ADD COLUMN IF NOT EXISTS spill_target      SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE payload ADD COLUMN IF NOT EXISTS created_at_ms     BIGINT   NOT NULL DEFAULT 0;
