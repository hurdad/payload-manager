-- ============================================================
-- Payload core table
-- ============================================================

CREATE TABLE IF NOT EXISTS payload (
                                       id TEXT PRIMARY KEY,
                                       tier SMALLINT NOT NULL,
                                       state SMALLINT NOT NULL,
                                       size_bytes BIGINT NOT NULL,
                                       version BIGINT NOT NULL,
                                       expires_at_ms BIGINT
);

CREATE INDEX IF NOT EXISTS idx_payload_state ON payload(state);
CREATE INDEX IF NOT EXISTS idx_payload_tier  ON payload(tier);

