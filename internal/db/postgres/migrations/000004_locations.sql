BEGIN;

CREATE TABLE payload_location (
                                  uuid            UUID REFERENCES payload(uuid) ON DELETE CASCADE,
                                  tier            storage_tier NOT NULL,

                                  node_id         TEXT,
                                  device_id       TEXT,

                                  path            TEXT,
                                  offset_bytes    BIGINT,
                                  length_bytes    BIGINT,

                                  status          SMALLINT NOT NULL,

                                  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                                  last_access     TIMESTAMPTZ,

                                  PRIMARY KEY (uuid, tier)
);

CREATE INDEX payload_location_tier_idx ON payload_location (tier);
CREATE INDEX payload_location_node_idx ON payload_location (node_id);
CREATE INDEX payload_location_access_idx ON payload_location (last_access DESC);

INSERT INTO schema_version(version) VALUES (4);

COMMIT;
