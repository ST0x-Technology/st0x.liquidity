-- Forward-only read model for the equity mint/redemption stage-timing report.
--
-- This table is maintained exclusively by the EquityTimingProjection reactor
-- from live TokenizedEquityMint and EquityRedemption events. The report read
-- path (load_equity_timings) queries ONLY this table and never folds the
-- events table. One row per mint or redemption operation (aggregate id),
-- mirroring rebalance_stage_timing's schema exactly (see
-- migrations/20260616221000_rebalance_timing_read_model.sql and
-- migrations/20260620003813_rebalance_timing_checkpoint.sql for the USDC
-- precedent this combines into one migration).
--
-- `started_at` is stored as RFC3339 TEXT (chrono `to_rfc3339`) so the report
-- can filter and order operations. This SQL column holds the timestamp of the
-- FIRST event this reactor observed for the operation (`first_seen_at`) and is
-- used exclusively for range filtering and ordering -- NOT the genuine
-- business start. `timing` is the JSON-serialized accumulated operation
-- timing (kind, symbol, quantity, status, the genuine
-- `started_at`/`completed_at`, and the ordered stage list with per-stage
-- durations) that the read path converts into the dashboard DTO. `total_ms`
-- is derived at read time from the `started_at`/`completed_at` fields
-- embedded in the `timing` JSON blob, not stored separately.
--
-- `last_sequence` is the highest event `sequence` (within its own aggregate
-- stream) folded into this row by a catch-up/rebuild pass. Startup catch-up
-- replays only events past it. NULL means "never checkpointed", which
-- catch-up treats as 0 and replays the full stream once before establishing
-- a checkpoint.
CREATE TABLE equity_stage_timing (
    operation_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    timing TEXT NOT NULL,
    last_sequence INTEGER
);

CREATE INDEX idx_equity_stage_timing_started_at ON equity_stage_timing (started_at);
