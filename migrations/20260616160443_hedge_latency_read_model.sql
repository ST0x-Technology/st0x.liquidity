-- Forward-only read model for the hedge-latency report.
--
-- These tables are maintained exclusively by the HedgeLatencyProjection
-- reactor from live Position and OffchainOrder events. The report read path
-- queries ONLY these tables and never folds the events table. All timestamps
-- are stored as RFC3339 TEXT (chrono `to_rfc3339`), which renders UTC values
-- with the literal `+00:00` suffix. The CHECK constraints below enforce that
-- suffix so a non-UTC value can never corrupt the lexicographic ORDER BY the
-- recompute and read paths rely on.

-- Append-only: one row per Position OnChainOrderFilled event. Source for
-- detection latency (seen_at - block_timestamp). The originating fill's
-- (tx_hash, log_index) is the event's durable identity: a UNIQUE constraint
-- plus ON CONFLICT DO NOTHING on the writer makes redelivery idempotent, so a
-- replayed or duplicated Position event cannot double-count a fill.
CREATE TABLE hedge_fill (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_timestamp TEXT NOT NULL CHECK (block_timestamp LIKE '%+00:00'),
    seen_at TEXT NOT NULL CHECK (seen_at LIKE '%+00:00'),
    UNIQUE (tx_hash, log_index)
);

CREATE INDEX idx_hedge_fill_symbol_seen_at ON hedge_fill (symbol, seen_at);

-- One row per hedge order, keyed by the offchain order id. Created when the
-- Position stream places a hedge, snapshotting the symbol's uncovered fill
-- pool as the covered batch. Broker-pipeline timestamps are filled in as the
-- order progresses.
--
-- The covered-batch CHECK rejects contradictory rows at the DB level: a batch
-- either covered no fills (count 0, both timestamps NULL) or covered at least
-- one fill (count > 0, both timestamps present). A half-populated batch can
-- never be persisted.
CREATE TABLE hedge_cycle (
    offchain_order_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    placed_at TEXT NOT NULL CHECK (placed_at LIKE '%+00:00'),
    covered_count INTEGER NOT NULL CHECK (covered_count >= 0),
    -- NULL when the placement covered no fills (no attribution).
    covered_earliest_block_timestamp TEXT
        CHECK (covered_earliest_block_timestamp IS NULL
               OR covered_earliest_block_timestamp LIKE '%+00:00'),
    covered_latest_seen_at TEXT
        CHECK (covered_latest_seen_at IS NULL
               OR covered_latest_seen_at LIKE '%+00:00'),
    filled_at TEXT CHECK (filled_at IS NULL OR filled_at LIKE '%+00:00'),
    failed_at TEXT CHECK (failed_at IS NULL OR failed_at LIKE '%+00:00'),
    CHECK (
        (covered_count = 0
         AND covered_earliest_block_timestamp IS NULL
         AND covered_latest_seen_at IS NULL)
        OR (covered_count > 0
            AND covered_earliest_block_timestamp IS NOT NULL
            AND covered_latest_seen_at IS NOT NULL)
    )
);

CREATE INDEX idx_hedge_cycle_symbol_placed_at ON hedge_cycle (symbol, placed_at);

-- Append-only: one row per OffchainOrder Submitted event, keyed by offchain
-- order id. Decoupled from hedge_cycle so the submission timestamp survives
-- regardless of arrival order: a Submitted event that lands before its
-- placement still records here, and the read path joins it back in. Keep-first
-- semantics via ON CONFLICT DO NOTHING.
CREATE TABLE hedge_submission (
    offchain_order_id TEXT PRIMARY KEY,
    submitted_at TEXT NOT NULL CHECK (submitted_at LIKE '%+00:00')
);

-- Append-only: one row per Position ManualPositionAdjusted event. A manual
-- adjustment means accumulated fills no longer drive hedging decisions, so the
-- read path drops every fill and cycle at or before `adjusted_at` when
-- recomputing the uncovered pool. There is no mutable attribution blob -- the
-- uncovered pool is a pure function of hedge_fill, hedge_cycle, and this table.
CREATE TABLE hedge_attribution_reset (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    adjusted_at TEXT NOT NULL CHECK (adjusted_at LIKE '%+00:00')
);

CREATE INDEX idx_hedge_attribution_reset_symbol_adjusted_at ON hedge_attribution_reset (symbol, adjusted_at);
