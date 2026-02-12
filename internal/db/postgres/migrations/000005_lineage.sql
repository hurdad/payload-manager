BEGIN;

CREATE TABLE payload_lineage (
                                 parent_uuid UUID REFERENCES payload(uuid) ON DELETE CASCADE,
                                 child_uuid  UUID REFERENCES payload(uuid) ON DELETE CASCADE,
                                 relation    TEXT,
                                 created_at  TIMESTAMPTZ DEFAULT now(),

                                 PRIMARY KEY(parent_uuid, child_uuid)
);

CREATE INDEX payload_lineage_child_idx ON payload_lineage(child_uuid);

INSERT INTO schema_version(version) VALUES (5);

COMMIT;
