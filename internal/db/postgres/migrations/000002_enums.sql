BEGIN;

CREATE TYPE payload_state AS ENUM (
    'allocating',
    'writing',
    'sealed',
    'available',
    'evicted',
    'deleted'
);

CREATE TYPE durability_level AS ENUM (
    'ephemeral',
    'disk',
    'remote'
);

CREATE TYPE storage_tier AS ENUM (
    'GPU',
    'RAM',
    'SHM',
    'DISK',
    'S3'
);

CREATE TYPE job_action AS ENUM (
    'spill',
    'promote',
    'replicate',
    'delete'
);

CREATE TYPE job_status AS ENUM (
    'pending',
    'running',
    'done',
    'failed'
);

INSERT INTO schema_version(version) VALUES (2);

COMMIT;
