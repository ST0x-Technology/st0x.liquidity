-- Forward-only read model for the rebalance stage-timing report.
--
-- This table is maintained exclusively by the RebalanceTimingProjection
-- reactor from live UsdcRebalance events. The report read path
-- (load_rebalance_timings) queries ONLY this table and never folds the events
-- table. One row per UsdcRebalance operation (aggregate id).
--
-- `started_at` is stored as RFC3339 TEXT (chrono `to_rfc3339`) so the report
-- can filter and order operations. This SQL column holds the timestamp of the
-- FIRST event this reactor observed for the operation (`first_seen_at`) and is
-- used exclusively for range filtering and ordering -- NOT the genuine business
-- start. `timing` is the JSON-serialized accumulated operation timing
-- (direction, amount, status, the genuine `started_at`/`completed_at`, and the
-- ordered stage list with per-stage durations) that the read path converts into
-- the dashboard DTO. `total_ms` is derived at read time from the `started_at`
-- and `completed_at` fields embedded in the `timing` JSON blob (not from this
-- SQL `started_at` column), and is not stored separately.
CREATE TABLE rebalance_stage_timing (
    operation_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    timing TEXT NOT NULL
);

CREATE INDEX idx_rebalance_stage_timing_started_at ON rebalance_stage_timing (started_at);
