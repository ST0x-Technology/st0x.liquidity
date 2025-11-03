-- Events table: stores all domain events
CREATE TABLE IF NOT EXISTS events (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_version TEXT NOT NULL,
    payload JSON NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(aggregate_type);
CREATE INDEX IF NOT EXISTS idx_events_aggregate
    ON events(aggregate_id);

-- Snapshots table: aggregate state cache for performance
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence BIGINT NOT NULL,
    payload JSON NOT NULL,
    timestamp TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
