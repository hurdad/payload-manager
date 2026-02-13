-- ============================================================
-- Generic lineage relationships
-- ============================================================

CREATE TABLE IF NOT EXISTS payload_lineage (
                                               parent_id TEXT NOT NULL REFERENCES payload(id) ON DELETE CASCADE,
    child_id  TEXT NOT NULL REFERENCES payload(id) ON DELETE CASCADE,
    operation TEXT,
    role TEXT,
    parameters TEXT,
    created_at_ms BIGINT NOT NULL
    );

CREATE INDEX IF NOT EXISTS idx_lineage_parent ON payload_lineage(parent_id);
CREATE INDEX IF NOT EXISTS idx_lineage_child  ON payload_lineage(child_id);
