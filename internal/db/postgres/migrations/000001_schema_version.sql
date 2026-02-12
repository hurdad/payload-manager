BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_version (
                                              version     INT PRIMARY KEY,
                                              applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );

INSERT INTO schema_version(version)
SELECT 1
    WHERE NOT EXISTS (SELECT 1 FROM schema_version);

COMMIT;
