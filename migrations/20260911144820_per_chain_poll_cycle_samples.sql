-- Poll-cycle samples keyed by chain: one fill watcher runs per hedged chain,
-- and each polls on its own cadence. Keyed by orderbook alone, two chains
-- whose Raindex orderbook lands at the same deterministic address aggregated
-- into one poll report, so a secondary chain's poll health was never shown on
-- its own -- the same defect the per-chain block-lag rebuild fixed.
--
-- Every existing row was taken by the Base watcher, the only one that has
-- ever run, so the rebuild files them under 'base'. No DEFAULT on the new
-- column: a write that names no chain is refused rather than silently filed
-- under Base (a second chain is already admissible by this point, so no
-- pre-per-chain rollback window remains to shim).
CREATE TABLE poll_cycle_samples_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at TEXT NOT NULL,
    monitor TEXT NOT NULL,
    chain TEXT NOT NULL,
    orderbook TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    skipped_ticks INTEGER NOT NULL CHECK (skipped_ticks >= 0),
    outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'error')),
    -- Populated only when outcome = 'error'.
    error TEXT CHECK ((outcome = 'error') = (error IS NOT NULL))
);

INSERT INTO poll_cycle_samples_new (
    id,
    sampled_at,
    monitor,
    chain,
    orderbook,
    duration_ms,
    skipped_ticks,
    outcome,
    error
)
SELECT
    id,
    sampled_at,
    monitor,
    'base',
    orderbook,
    duration_ms,
    skipped_ticks,
    outcome,
    error
FROM poll_cycle_samples;

DROP TABLE poll_cycle_samples;
ALTER TABLE poll_cycle_samples_new RENAME TO poll_cycle_samples;

-- Read path filters by (monitor, chain, orderbook) then ranges over sampled_at.
CREATE INDEX idx_poll_cycle_samples_monitor_chain_orderbook_sampled_at
    ON poll_cycle_samples (monitor, chain, orderbook, sampled_at);

-- Retention pruning deletes by sampled_at alone.
CREATE INDEX idx_poll_cycle_samples_sampled_at ON poll_cycle_samples (sampled_at);
