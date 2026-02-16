-- ============================================================
-- Active read leases
-- ============================================================

CREATE TABLE IF NOT EXISTS payload_lease (
                                             lease_id BYTEA PRIMARY KEY,
                                             payload_id TEXT NOT NULL REFERENCES payload(id) ON DELETE CASCADE,
    expires_at_ms BIGINT NOT NULL
    );

CREATE INDEX IF NOT EXISTS idx_lease_payload ON payload_lease(payload_id);
CREATE INDEX IF NOT EXISTS idx_lease_expiry  ON payload_lease(expires_at_ms);
