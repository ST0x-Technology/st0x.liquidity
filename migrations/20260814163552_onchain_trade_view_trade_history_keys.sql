-- Give onchain_trade_view the sort and filter keys /trades needs, and wire the
-- table to a real projection for the first time.
--
-- The table has existed since 20251103172115 (json paths repaired in
-- 20260212115445) but nothing ever wrote to it: OnChainTrade was
-- `PROJECTION = Nil`, so trade history replayed every onchain aggregate per
-- request instead (~10k `load_entity` calls, 6-7s). That is the whole of the
-- /trades latency problem. Setting `PROJECTION = Table("onchain_trade_view")`
-- makes the framework maintain it and backfill every pre-existing aggregate on
-- the first `StoreBuilder::build` after this migration.
--
-- The table is empty in every environment (nothing has ever populated it), and
-- views are rebuilt from events at startup, so DROP + CREATE is safe -- the
-- same reasoning the two earlier migrations for this table used. The old
-- payload-expression indexes go with it; they indexed fields
-- (`$.Live.tx_hash`, `$.Live.log_index`) that are not part of the serialized
-- aggregate at all -- those live in the aggregate id.
--
-- SQLite cannot ALTER TABLE ADD a STORED generated column, so the added keys
-- require the recreate regardless.

DROP TABLE IF EXISTS onchain_trade_view;
CREATE TABLE onchain_trade_view (
    view_id TEXT PRIMARY KEY,             -- "0x<64 hex>:<log_index>"
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- Fixed-width nanosecond timestamp, so lexicographic order is
    -- chronological order. chrono's serde emits SecondsFormat::AutoSi, which
    -- pads to 0, 3, 6 or 9 fractional digits and is NOT sortable as stored:
    -- '...20.5Z' sorts before '...20Z' ('.' < 'Z'), and '...20.500Z' sorts
    -- after '...20.500000Z' though they are the same instant. Normalizing to
    -- a constant 9 digits fixes both.
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

    -- Derived from view_id rather than re-formatted from the payload, so the
    -- sort group and the trade id's prefix are the same bytes by construction.
    -- That identity is what makes ordering by tx_hash equivalent to
    -- `sort_trades_newest_first`'s comparison of whole ids.
    tx_hash TEXT GENERATED ALWAYS AS
        (substr(view_id, 1, instr(view_id, ':') - 1)) STORED,
    log_index INTEGER GENERATED ALWAYS AS
        (CAST(substr(view_id, instr(view_id, ':') + 1) AS INTEGER)) STORED
);

-- Covers the trade-history sort in full, so the newest-first page is an
-- index range scan with early LIMIT termination and no temp b-tree. Partial on
-- the same predicate every trade-history query opens with, which is what lets
-- it serve the unbounded COUNT(*) as well as the page -- without that the
-- count degrades to a full scan of every row and its payload.
CREATE INDEX idx_onchain_trade_view_order
    ON onchain_trade_view (occurred_at DESC, tx_hash ASC, log_index DESC)
    WHERE occurred_at IS NOT NULL;

CREATE INDEX idx_onchain_trade_view_symbol
    ON onchain_trade_view (symbol, occurred_at DESC);
