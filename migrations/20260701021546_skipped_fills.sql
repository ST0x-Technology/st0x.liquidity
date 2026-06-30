-- Durable operational record of on-chain fills the accountant intentionally
-- skipped instead of hedging: unpriceable fills (non-zero equity at a
-- non-positive USDC/share price) and non-hedgeable token pairs. The accountant
-- swallows these to keep one anomalous fill from tripping the conductor-wide
-- fail-stop, so without this table the only trace is an `error!` log line that
-- rotation could erase, leaving the fill silently unhedged. Operators query
-- this table to reconcile skipped fills by hand.
--
-- (tx_hash, log_index) is the fill identity and therefore the PRIMARY KEY: a
-- backfill re-scan re-processes the same fill, and the writer's
-- `ON CONFLICT DO NOTHING` keeps it a single row rather than duplicating it on
-- every pass. `reason` is a closed set the writer maps from the skip cause;
-- `detail` carries the human-readable specifics (the computed price, or the
-- input/output symbols) for reconciliation.
CREATE TABLE skipped_fills (
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    skipped_at TEXT NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
