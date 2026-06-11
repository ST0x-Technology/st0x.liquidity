-- Forward-only read model for lifecycle failure events (money-at-risk signals).
--
-- This table is maintained exclusively by the LifecycleFailureProjection
-- reactor from live OffchainOrder, UsdcRebalance, EquityRedemption, and
-- TokenizedEquityMint events. The report read path (load_failure_events)
-- queries ONLY this table and never folds the events table. One append-only
-- row per failure event the reactor witnesses.
--
-- `event_type` is the canonical DomainEvent type string (e.g.
-- "OffchainOrderEvent::Failed"). `occurred_at` is stored as RFC3339 TEXT
-- (chrono `to_rfc3339`) so the report can filter by report range.
CREATE TABLE lifecycle_failure_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);

CREATE INDEX idx_lifecycle_failure_event_occurred_at ON lifecycle_failure_event (occurred_at);
