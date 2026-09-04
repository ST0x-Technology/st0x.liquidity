-- Per-chain fill-watch checkpoints (RAI-2079): one watcher runs per watched
-- chain, so the backfill checkpoint is keyed (chain, orderbook) instead of
-- orderbook alone. The existing single row belongs to Base by definition
-- (the only chain ever watched) and is rewritten in the same transaction as
-- the table rebuild.
--
-- Why two migrations: this one keeps a rollback window to the pre-per-chain
-- binary so it can deploy on its own; the cleanup migration closes that
-- window in the same change that first makes a second chain configurable,
-- which is the first point the schema must allow one.
--
-- The UNIQUE(orderbook) shadow index preserves one release of rollback: a
-- rolled-back (pre-per-chain) binary upserts with ON CONFLICT(orderbook),
-- which needs a unique constraint on that column to resolve, and omits the
-- chain column, which needs the DEFAULT to pass NOT NULL before the conflict
-- clause runs. Both rollback affordances also forbid or fake a second chain
-- (the index rejects two chains sharing an orderbook address, which
-- deterministic deploys make real), so they MUST go before a second chain is
-- ever watched: the cleanup migration ships together with the capability
-- grant that first admits a non-Base watched chain, keeping rollback
-- protection exactly as long as the deployment is single-chain.
--
-- The DEFAULT is a rollback shim only, never a fallback for live code: every
-- write in this binary binds chain explicitly, since an unbound write would
-- silently file a fill's checkpoint under Base (the same trap a reviewer
-- caught on pnl_onchain_fill.chain). The cleanup migration drops the default
-- and a test pins that a chain-less write is then refused.
CREATE TABLE backfill_checkpoints_new (
    chain TEXT NOT NULL DEFAULT 'base',
    orderbook TEXT NOT NULL,
    last_processed_block INTEGER NOT NULL CHECK (last_processed_block >= 0),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (chain, orderbook)
);

INSERT INTO backfill_checkpoints_new (chain, orderbook, last_processed_block, updated_at)
SELECT 'base', orderbook, last_processed_block, updated_at
FROM backfill_checkpoints;

DROP TABLE backfill_checkpoints;

ALTER TABLE backfill_checkpoints_new RENAME TO backfill_checkpoints;

CREATE UNIQUE INDEX idx_backfill_checkpoints_orderbook_rollback
    ON backfill_checkpoints (orderbook);
