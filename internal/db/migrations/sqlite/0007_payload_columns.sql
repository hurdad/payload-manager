-- ============================================================
-- Add eviction policy and creation timestamp columns to payload.
-- SQLite does not support IF NOT EXISTS on ALTER TABLE ADD COLUMN
-- (prior to 3.37.0), so callers must handle SQLITE_ERROR for
-- "duplicate column name" and treat it as a no-op.
-- ============================================================

ALTER TABLE payload ADD COLUMN persist           INTEGER NOT NULL DEFAULT 0;
ALTER TABLE payload ADD COLUMN eviction_priority INTEGER NOT NULL DEFAULT 0;
ALTER TABLE payload ADD COLUMN spill_target      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE payload ADD COLUMN created_at_ms     INTEGER NOT NULL DEFAULT 0;
