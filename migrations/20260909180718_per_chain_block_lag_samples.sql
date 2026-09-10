-- Block-lag samples keyed by chain: one fill watcher runs per watched chain,
-- and each records its own lag series. Without a chain column a secondary
-- chain's samples were indistinguishable from the primary's whenever the
-- Raindex orderbook lands at the same deterministic address on both chains.
--
-- Every existing row was taken by the Base watcher, the only one that has
-- ever run, so the rebuild files them under 'base'. No DEFAULT on the new
-- column: a write that names no chain is refused rather than silently filed
-- under Base (a second chain is already admissible by this point, so no
-- pre-per-chain rollback window remains to shim).
CREATE TABLE block_lag_samples_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at TEXT NOT NULL,
    chain TEXT NOT NULL,
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
    chain,
    orderbook,
    chain_tip,
    cutoff_block,
    last_processed_block,
    lag_blocks
)
SELECT
    id,
    sampled_at,
    'base',
    orderbook,
    chain_tip,
    cutoff_block,
    last_processed_block,
    lag_blocks
FROM block_lag_samples;

DROP TABLE block_lag_samples;
ALTER TABLE block_lag_samples_new RENAME TO block_lag_samples;

-- Read path filters by (chain, orderbook) then ranges over sampled_at.
CREATE INDEX idx_block_lag_samples_chain_orderbook_sampled_at
    ON block_lag_samples (chain, orderbook, sampled_at);

-- Retention pruning deletes by sampled_at alone.
CREATE INDEX idx_block_lag_samples_sampled_at ON block_lag_samples (sampled_at);
