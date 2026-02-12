BEGIN;

CREATE INDEX payload_metadata_gin ON payload USING GIN (metadata jsonb_path_ops);
CREATE INDEX payload_labels_gin   ON payload USING GIN (labels);

INSERT INTO schema_version(version) VALUES (9);

COMMIT;
