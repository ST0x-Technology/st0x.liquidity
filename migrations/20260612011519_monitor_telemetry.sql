-- Lightweight ingestion-health telemetry sampled by the order-fill monitor.
-- High-frequency operational samples, deliberately outside the CQRS event
-- store. Rows are pruned after a retention window; timestamps are stored as
-- '%Y-%m-%dT%H:%M:%fZ' UTC text so lexicographic comparison equals time
-- comparison. Every sample carries the orderbook it was taken against, so a
-- database reused across configs never mixes telemetry series.

CREATE TABLE block_lag_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at TEXT NOT NULL,
    orderbook TEXT NOT NULL,
    chain_tip INTEGER NOT NULL CHECK (chain_tip >= 0),
    confirmed_tip INTEGER NOT NULL CHECK (confirmed_tip >= 0),
    -- NULL before the first backfill checkpoint exists.
    last_processed_block INTEGER CHECK (last_processed_block >= 0),
    -- chain_tip - last_processed_block; NULL while there is no checkpoint.
    lag_blocks INTEGER CHECK (lag_blocks >= 0)
);

-- Read path filters by orderbook then ranges over sampled_at.
CREATE INDEX idx_block_lag_samples_orderbook_sampled_at
    ON block_lag_samples (orderbook, sampled_at);

-- Retention pruning deletes by sampled_at alone.
CREATE INDEX idx_block_lag_samples_sampled_at ON block_lag_samples (sampled_at);

CREATE TABLE poll_cycle_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at TEXT NOT NULL,
    monitor TEXT NOT NULL,
    orderbook TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    skipped_ticks INTEGER NOT NULL CHECK (skipped_ticks >= 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'error')),
    -- Populated only when outcome = 'error'.
    error TEXT CHECK ((outcome = 'error') = (error IS NOT NULL))
);

-- Read path filters by monitor and orderbook then ranges over sampled_at.
CREATE INDEX idx_poll_cycle_samples_monitor_orderbook_sampled_at
    ON poll_cycle_samples (monitor, orderbook, sampled_at);

-- Retention pruning deletes by sampled_at alone.
CREATE INDEX idx_poll_cycle_samples_sampled_at ON poll_cycle_samples (sampled_at);
