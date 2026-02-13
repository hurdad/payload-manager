-- ============================================================
-- Payload metadata (current snapshot)
-- ============================================================

CREATE TABLE IF NOT EXISTS payload_metadata (
                                                id TEXT PRIMARY KEY REFERENCES payload(id) ON DELETE CASCADE,
    json JSONB NOT NULL,
    schema TEXT,
    updated_at_ms BIGINT NOT NULL
    );

CREATE INDEX IF NOT EXISTS idx_payload_metadata_updated
    ON payload_metadata(updated_at_ms);
