CREATE TABLE block_lag_samples_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at TEXT NOT NULL,
    orderbook TEXT NOT NULL,
    chain_tip INTEGER NOT NULL CHECK (chain_tip >= 0),
    -- NULL when the configured ingestion cutoff tag is unavailable.
    cutoff_block INTEGER CHECK (cutoff_block >= 0),
    -- NULL before the first backfill checkpoint exists.
    last_processed_block INTEGER CHECK (last_processed_block >= 0),
    -- cutoff_block - last_processed_block; NULL when either input is unknown.
    lag_blocks INTEGER CHECK (lag_blocks >= 0)
);

INSERT INTO block_lag_samples_new (
    id,
    sampled_at,
    orderbook,
    chain_tip,
    cutoff_block,
    last_processed_block,
    lag_blocks
)
SELECT
    id,
    sampled_at,
    orderbook,
    chain_tip,
    cutoff_block,
    last_processed_block,
    lag_blocks
FROM block_lag_samples;

DROP TABLE block_lag_samples;
ALTER TABLE block_lag_samples_new RENAME TO block_lag_samples;

CREATE INDEX idx_block_lag_samples_orderbook_sampled_at
    ON block_lag_samples (orderbook, sampled_at);
CREATE INDEX idx_block_lag_samples_sampled_at ON block_lag_samples (sampled_at);
