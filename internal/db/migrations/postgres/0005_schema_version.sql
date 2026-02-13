-- ============================================================
-- Migration tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS payload_schema_migrations (
                                                         version INTEGER PRIMARY KEY,
                                                         applied_at TIMESTAMPTZ DEFAULT NOW()
    );
