BEGIN;

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS btree_gin;

CREATE TABLE payload_access (
                                ts          TIMESTAMPTZ NOT NULL,
                                uuid        UUID NOT NULL,
                                tier        storage_tier,
                                access_type SMALLINT,
                                client_id   TEXT,
                                latency_us  BIGINT
);

SELECT create_hypertable('payload_access', 'ts', if_not_exists => TRUE);

CREATE TABLE tier_usage (
                            ts          TIMESTAMPTZ NOT NULL,
                            node_id     TEXT,
                            tier        storage_tier,
                            used_bytes  BIGINT,
                            free_bytes  BIGINT
);

SELECT create_hypertable('tier_usage', 'ts', if_not_exists => TRUE);

INSERT INTO schema_version(version) VALUES (8);

COMMIT;
