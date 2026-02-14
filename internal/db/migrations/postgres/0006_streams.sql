-- ============================================================
-- Streams
-- ============================================================

CREATE TABLE IF NOT EXISTS streams (
                                       stream_id BIGSERIAL PRIMARY KEY,
                                       namespace TEXT NOT NULL,
                                       name TEXT NOT NULL,
                                       created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                       retention_max_entries BIGINT,
                                       retention_max_age_sec BIGINT,
                                       UNIQUE(namespace, name)
);

CREATE TABLE IF NOT EXISTS stream_entries (
                                              stream_id BIGINT NOT NULL REFERENCES streams(stream_id),
                                              offset BIGINT NOT NULL,
                                              payload_uuid UUID NOT NULL,
                                              event_time TIMESTAMPTZ,
                                              append_time TIMESTAMPTZ NOT NULL DEFAULT now(),
                                              duration_ns BIGINT,
                                              tags JSONB,
                                              PRIMARY KEY (stream_id, offset)
);

CREATE INDEX IF NOT EXISTS stream_entries_stream_time
    ON stream_entries(stream_id, append_time DESC);

CREATE TABLE IF NOT EXISTS stream_consumer_offsets (
                                                       stream_id BIGINT NOT NULL REFERENCES streams(stream_id),
                                                       consumer_group TEXT NOT NULL,
                                                       offset BIGINT NOT NULL,
                                                       updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                                       PRIMARY KEY (stream_id, consumer_group)
);
