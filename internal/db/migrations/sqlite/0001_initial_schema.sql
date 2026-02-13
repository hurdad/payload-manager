CREATE TABLE IF NOT EXISTS payload (
                                       id TEXT PRIMARY KEY,
                                       tier INTEGER NOT NULL,
                                       state INTEGER NOT NULL,
                                       size_bytes INTEGER NOT NULL,
                                       version INTEGER NOT NULL,
                                       expires_at_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_payload_state ON payload(state);
CREATE INDEX IF NOT EXISTS idx_payload_tier  ON payload(tier);
