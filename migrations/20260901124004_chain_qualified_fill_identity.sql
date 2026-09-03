-- Chain-qualified fill identity (RAI-2078): a transaction hash is unique only
-- within one chain, so the chain joins every fill identity. Legacy Base rows
-- are upgraded in place to the uniform chain-prefixed form; no bare form
-- survives, so readers need no legacy special case.

-- OnChainTrade aggregate ids: "0x<tx>:<log>" -> "base:0x<tx>:<log>". The
-- onchain_trade_view projection is NOT rewritten here: the OnChainTrade
-- SCHEMA_VERSION bump discards and rebuilds it from these migrated ids.
UPDATE events
   SET aggregate_id = 'base:' || aggregate_id
 WHERE aggregate_type = 'OnChainTrade'
   AND aggregate_id LIKE '0x%';

UPDATE snapshots
   SET aggregate_id = 'base:' || aggregate_id
 WHERE aggregate_type = 'OnChainTrade'
   AND aggregate_id LIKE '0x%';

-- Position fill-event payloads: trade_id gains an explicit chain, so the
-- durable double-hedge guard matches on plain equality with no COALESCE.
UPDATE events
   SET payload = json_set(payload, '$.OnChainOrderFilled.trade_id.chain', 'base')
 WHERE aggregate_type = 'Position'
   AND json_extract(payload, '$.OnChainOrderFilled.trade_id') IS NOT NULL
   AND json_extract(payload, '$.OnChainOrderFilled.trade_id.chain') IS NULL;

UPDATE events
   SET payload = json_set(payload, '$.OnChainFillApplied.trade_id.chain', 'base')
 WHERE aggregate_type = 'Position'
   AND json_extract(payload, '$.OnChainFillApplied.trade_id') IS NOT NULL
   AND json_extract(payload, '$.OnChainFillApplied.trade_id.chain') IS NULL;

UPDATE events
   SET payload = json_set(payload, '$.OnChainFillSettled.trade_id.chain', 'base')
 WHERE aggregate_type = 'Position'
   AND json_extract(payload, '$.OnChainFillSettled.trade_id') IS NOT NULL
   AND json_extract(payload, '$.OnChainFillSettled.trade_id.chain') IS NULL;

-- Dashboard delivery ledger keys on the dto trade id string, which now
-- renders chain-prefixed for onchain fills (offchain ids are order UUIDs and
-- carry no 0x prefix).
UPDATE dashboard_trade_delivery
   SET trade_id = 'base:' || trade_id
 WHERE trade_id LIKE '0x%';

-- hedge_fill: fill identity widens to (chain, tx_hash, log_index). Inline
-- UNIQUE cannot be altered, so rebuild the table.
CREATE TABLE hedge_fill_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain TEXT NOT NULL,
    symbol TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_timestamp TEXT NOT NULL CHECK (block_timestamp LIKE '%+00:00'),
    seen_at TEXT NOT NULL CHECK (seen_at LIKE '%+00:00'),
    UNIQUE (chain, tx_hash, log_index)
);

INSERT INTO hedge_fill_new (id, chain, symbol, tx_hash, log_index, block_timestamp, seen_at)
SELECT id, 'base', symbol, tx_hash, log_index, block_timestamp, seen_at FROM hedge_fill;

DROP TABLE hedge_fill;
ALTER TABLE hedge_fill_new RENAME TO hedge_fill;
CREATE INDEX idx_hedge_fill_symbol_seen_at ON hedge_fill (symbol, seen_at);

-- skipped_fills: same widening; inline PRIMARY KEY forces a rebuild.
CREATE TABLE skipped_fills_new (
    chain TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    skipped_at TEXT NOT NULL,
    PRIMARY KEY (chain, tx_hash, log_index)
);

INSERT INTO skipped_fills_new (chain, tx_hash, log_index, event_type, reason, detail, skipped_at)
SELECT 'base', tx_hash, log_index, event_type, reason, detail, skipped_at FROM skipped_fills;

DROP TABLE skipped_fills;
ALTER TABLE skipped_fills_new RENAME TO skipped_fills;

-- pnl_onchain_fill keys on event_rowid; the chain is an attribute.
ALTER TABLE pnl_onchain_fill ADD COLUMN chain TEXT NOT NULL DEFAULT 'base';

-- onchain_trade_view: view_id is now "chain:0x<64 hex>:<log_index>", so the
-- generated sort/filter keys re-derive from the three-part form (the tx hash
-- is always 66 chars, so offsets are fixed once the chain prefix is skipped).
-- Contents are not copied: the OnChainTrade SCHEMA_VERSION bump rebuilds the
-- projection from the migrated aggregate ids on first startup.
DROP TABLE IF EXISTS onchain_trade_view;
CREATE TABLE onchain_trade_view (
    view_id TEXT PRIMARY KEY,             -- "chain:0x<64 hex>:<log_index>"
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- Fixed-width nanosecond timestamp, so lexicographic order is
    -- chronological order (see 20260814163552 for the padding rationale).
    occurred_at TEXT GENERATED ALWAYS AS (
        substr(json_extract(payload, '$.Live.block_timestamp'), 1, 19) || '.' ||
        substr(
            replace(replace(
                substr(json_extract(payload, '$.Live.block_timestamp'), 20),
            '.', ''), 'Z', '') || '000000000',
        1, 9)
    ) STORED,

    symbol TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Live.symbol')) STORED,

    -- Mirrors OnChainTradeSource::trading_venue(). A new source variant must
    -- be added here too; the exhaustive match in
    -- `onchain_view_venue_matches_trading_venue_for_every_source` fails to
    -- compile until it is.
    venue TEXT GENERATED ALWAYS AS (
        CASE
            WHEN json_extract(payload, '$.Live.source') IN ('Legacy', 'Raindex')
                THEN 'raindex'
            WHEN json_extract(payload, '$.Live.source.Inventory.venue') = 'Bebop'
                THEN 'bebop'
            WHEN json_extract(payload, '$.Live.source.Inventory.venue') = 'UniswapV4'
                THEN 'uniswap_v4'
            WHEN json_extract(payload, '$.Live.source.UnrecognizedInventory') IS NOT NULL
                THEN 'unknown_onchain'
        END
    ) STORED,

    chain TEXT GENERATED ALWAYS AS
        (substr(view_id, 1, instr(view_id, ':') - 1)) STORED,
    tx_hash TEXT GENERATED ALWAYS AS
        (substr(view_id, instr(view_id, ':') + 1, 66)) STORED,
    log_index INTEGER GENERATED ALWAYS AS
        (CAST(substr(view_id, instr(view_id, ':') + 68) AS INTEGER)) STORED
);

-- Covers the trade-history sort. The middle expression is the id prefix
-- ("chain:0x<tx>"), the same bytes `sort_trades_newest_first` groups by, so
-- SQL ordering stays equivalent to the dto comparator by construction.
CREATE INDEX idx_onchain_trade_view_order
    ON onchain_trade_view (occurred_at DESC, (chain || ':' || tx_hash) ASC, log_index DESC)
    WHERE occurred_at IS NOT NULL;

CREATE INDEX idx_onchain_trade_view_symbol
    ON onchain_trade_view (symbol, occurred_at DESC);
