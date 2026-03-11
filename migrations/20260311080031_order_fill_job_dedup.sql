-- Deduplication table for OrderFillJob: prevents the same onchain event
-- from being enqueued multiple times when the bot restarts and replays
-- historical events from the deployment block.
--
-- The PRIMARY KEY enforces exactly-once semantics: INSERT OR IGNORE returns
-- rows_affected=0 for duplicates, so the caller skips re-enqueueing.
CREATE TABLE order_fill_job_dedup (
    tx_hash   TEXT    NOT NULL,
    log_index INTEGER NOT NULL,
    PRIMARY KEY (tx_hash, log_index)
);
