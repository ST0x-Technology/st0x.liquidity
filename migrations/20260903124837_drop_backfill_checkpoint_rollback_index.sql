-- Why a second migration: 20260903112053 kept a rollback window to the
-- pre-per-chain binary so it could deploy on its own; this one closes that
-- window in the same change that first makes a second chain configurable,
-- which is the first point the schema must allow one.
--
-- Ships together with the capability grant that first admits a non-Base
-- watched chain, retiring both rollback affordances of 20260903112053: the
-- UNIQUE(orderbook) shadow index (forbids two chains sharing an orderbook
-- address, which deterministic deployments make real) and the DEFAULT 'base'
-- on chain (would file a chain-less write under Base implicitly). SQLite
-- cannot drop a column default in place, so the table is rebuilt; dropping
-- the old table takes the index with it. This ends the pre-per-chain
-- binary's rollback window for the checkpoint table (restore from backup
-- applies from here on).
CREATE TABLE backfill_checkpoints_new (
    chain TEXT NOT NULL,
    orderbook TEXT NOT NULL,
    last_processed_block INTEGER NOT NULL CHECK (last_processed_block >= 0),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (chain, orderbook)
);

INSERT INTO backfill_checkpoints_new (chain, orderbook, last_processed_block, updated_at)
SELECT chain, orderbook, last_processed_block, updated_at
FROM backfill_checkpoints;

DROP TABLE backfill_checkpoints;

ALTER TABLE backfill_checkpoints_new RENAME TO backfill_checkpoints;
