-- Latency/error samples for external dependency calls (RPC provider and
-- broker API), batched in by the telemetry writer task. Same conventions as
-- the monitor telemetry tables: outside the CQRS event store, pruned after
-- a retention window, '%Y-%m-%dT%H:%M:%fZ' UTC text timestamps.

CREATE TABLE dependency_call_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    dependency TEXT NOT NULL CHECK (dependency IN ('rpc', 'broker')),
    operation TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'error')),
    -- Populated only when outcome = 'error'.
    error TEXT CHECK ((outcome = 'error') = (error IS NOT NULL))
);

CREATE INDEX idx_dependency_call_samples_recorded_at
    ON dependency_call_samples (recorded_at, dependency, operation);
