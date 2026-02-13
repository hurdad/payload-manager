CREATE TABLE IF NOT EXISTS payload_lineage (
                                               parent_id TEXT NOT NULL,
                                               child_id  TEXT NOT NULL,
                                               operation TEXT,
                                               role TEXT,
                                               parameters TEXT,
                                               created_at_ms INTEGER NOT NULL,
                                               FOREIGN KEY(parent_id) REFERENCES payload(id) ON DELETE CASCADE,
    FOREIGN KEY(child_id)  REFERENCES payload(id) ON DELETE CASCADE
    );

CREATE INDEX IF NOT EXISTS idx_lineage_parent ON payload_lineage(parent_id);
CREATE INDEX IF NOT EXISTS idx_lineage_child  ON payload_lineage(child_id);
