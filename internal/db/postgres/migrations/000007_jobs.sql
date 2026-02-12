BEGIN;

CREATE TABLE payload_job (
                             id            BIGSERIAL PRIMARY KEY,
                             uuid          UUID REFERENCES payload(uuid) ON DELETE CASCADE,

                             action        job_action NOT NULL,
                             from_tier     storage_tier,
                             to_tier       storage_tier,

                             priority      INT DEFAULT 0,
                             status        job_status DEFAULT 'pending',

                             attempts      INT DEFAULT 0,

                             scheduled_at  TIMESTAMPTZ DEFAULT now(),
                             started_at    TIMESTAMPTZ,
                             finished_at   TIMESTAMPTZ
);

CREATE INDEX payload_job_pending_idx
    ON payload_job (priority DESC, scheduled_at)
    WHERE status = 'pending';

INSERT INTO schema_version(version) VALUES (7);

COMMIT;
