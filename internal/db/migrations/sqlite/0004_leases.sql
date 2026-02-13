CREATE TABLE IF NOT EXISTS payload_lease (
                                             lease_id TEXT PRIMARY KEY,
                                             payload_id TEXT NOT NULL,
                                             expires_at_ms INTEGER NOT NULL,
                                             FOREIGN KEY(payload_id) REFERENCES payload(id) ON DELETE CASCADE
    );

CREATE INDEX IF NOT EXISTS idx_lease_payload ON payload_lease(payload_id);
CREATE INDEX IF NOT EXISTS idx_lease_expiry  ON payload_lease(expires_at_ms);
