-- Rebuildable read model for lifecycle failure events (money-at-risk signals).
--
-- Maintained by the LifecycleFailureProjection reactor from live OffchainOrder,
-- UsdcRebalance, EquityRedemption, and TokenizedEquityMint events, and
-- reconstructable from the event log: startup catch-up replays each stream past
-- a per-aggregate checkpoint, and `rebuild-view lifecycle-failure --all`
-- truncates and re-folds. The report read path (load_failure_events) queries
-- ONLY this table and never folds the events table.
--
-- `event_type` is the canonical DomainEvent type string (e.g.
-- "OffchainOrderEvent::Failed"). `occurred_at` is RFC3339 TEXT (chrono
-- `to_rfc3339`) so the report can filter by report range.
--
-- The dedup identity `(aggregate_type, aggregate_id, event_type, occurred_at)`
-- is NOT NULL and UNIQUE: both the live reactor and the replay path write
-- `ON CONFLICT DO NOTHING`, so re-folding an already-recorded failure is a no-op
-- rather than a duplicate. The columns are NOT NULL because SQLite treats each
-- NULL as distinct in a UNIQUE index, which would let a NULL-identity row bypass
-- the dedup.
CREATE TABLE lifecycle_failure_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);

-- The report read path filters by `occurred_at`.
CREATE INDEX idx_lifecycle_failure_event_occurred_at
    ON lifecycle_failure_event (occurred_at);

-- Dedup key for the rebuildable read model: one row per distinct failure event.
CREATE UNIQUE INDEX idx_lifecycle_failure_event_identity
    ON lifecycle_failure_event (aggregate_type, aggregate_id, event_type, occurred_at);

-- Per-(aggregate_type, aggregate_id) replay checkpoint: the highest CONTIGUOUS
-- event `sequence` folded for that stream. A poison (unparseable) row caps the
-- checkpoint below the gap, so the gap is re-scanned on every catch-up rather
-- than silently skipped past. Advanced ONLY on the replay/catch-up path; the
-- live reactor has no sequence and leaves it untouched, so a restart replays
-- only the events past it rather than the whole history.
CREATE TABLE lifecycle_failure_checkpoint (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    last_sequence INTEGER NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
