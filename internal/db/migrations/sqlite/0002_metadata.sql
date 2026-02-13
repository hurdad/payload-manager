CREATE TABLE IF NOT EXISTS payload_metadata (
                                                id TEXT PRIMARY KEY,
                                                json TEXT NOT NULL,
                                                schema TEXT,
                                                updated_at_ms INTEGER NOT NULL,
                                                FOREIGN KEY(id) REFERENCES payload(id) ON DELETE CASCADE
    );

CREATE INDEX IF NOT EXISTS idx_payload_metadata_updated
    ON payload_metadata(updated_at_ms);
