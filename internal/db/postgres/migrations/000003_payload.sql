BEGIN;

CREATE TABLE payload (
                         uuid            UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                         created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                         committed_at    TIMESTAMPTZ,
                         deleted_at      TIMESTAMPTZ,

                         size_bytes      BIGINT NOT NULL CHECK (size_bytes >= 0),
                         checksum        BYTEA,

                         state           payload_state NOT NULL,
                         durability      durability_level NOT NULL DEFAULT 'ephemeral',

                         owner           TEXT,
                         labels          JSONB DEFAULT '{}'::jsonb,
                         metadata        JSONB NOT NULL,

                         version         INT NOT NULL DEFAULT 1
);

CREATE INDEX payload_created_idx ON payload (created_at DESC);
CREATE INDEX payload_not_deleted_idx ON payload (uuid) WHERE deleted_at IS NULL;

INSERT INTO schema_version(version) VALUES (3);

COMMIT;
