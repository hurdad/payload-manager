CREATE TABLE IF NOT EXISTS streams (
                                       stream_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                       namespace TEXT NOT NULL,
                                       name TEXT NOT NULL,
                                       created_at INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
                                       retention_max_entries INTEGER,
                                       retention_max_age_sec INTEGER,
                                       UNIQUE(namespace, name)
);

CREATE TABLE IF NOT EXISTS stream_entries (
                                              stream_id INTEGER NOT NULL REFERENCES streams(stream_id),
                                              offset INTEGER NOT NULL,
                                              payload_uuid TEXT NOT NULL,
                                              event_time INTEGER,
                                              append_time INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
                                              duration_ns INTEGER,
                                              tags TEXT,
                                              PRIMARY KEY (stream_id, offset)
);

CREATE INDEX IF NOT EXISTS stream_entries_stream_time
    ON stream_entries(stream_id, append_time DESC);

CREATE TABLE IF NOT EXISTS stream_consumer_offsets (
                                                       stream_id INTEGER NOT NULL REFERENCES streams(stream_id),
                                                       consumer_group TEXT NOT NULL,
                                                       offset INTEGER NOT NULL,
                                                       updated_at INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
                                                       PRIMARY KEY (stream_id, consumer_group)
);
