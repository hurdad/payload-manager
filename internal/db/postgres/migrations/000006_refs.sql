BEGIN;

CREATE TABLE payload_ref (
                             uuid        UUID REFERENCES payload(uuid) ON DELETE CASCADE,
                             holder      TEXT,
                             ref_count   INT NOT NULL CHECK (ref_count >= 0),
                             expires_at  TIMESTAMPTZ,

                             PRIMARY KEY (uuid, holder)
);

INSERT INTO schema_version(version) VALUES (6);

COMMIT;
