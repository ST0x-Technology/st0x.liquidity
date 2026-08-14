-- Add trade-history sort and filter keys to offchain_order_view so /trades can
-- page terminal counter-trades in SQL instead of parsing every row per
-- request. SQLite cannot ALTER TABLE ADD a STORED generated column, so this is
-- DROP + CREATE -- safe here for the same reason the cancelling/cancelled
-- migration gave: views are rebuilt from events at startup.
--
-- `status` and its index are preserved verbatim; /api/pending_orders depends
-- on them.

DROP TABLE IF EXISTS offchain_order_view;
CREATE TABLE offchain_order_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    status TEXT GENERATED ALWAYS AS (
        CASE
            WHEN json_extract(payload, '$.Live.Pending') IS NOT NULL THEN 'Pending'
            WHEN json_extract(payload, '$.Live.Submitted') IS NOT NULL THEN 'Submitted'
            WHEN json_extract(payload, '$.Live.PartiallyFilled') IS NOT NULL THEN 'PartiallyFilled'
            WHEN json_extract(payload, '$.Live.Cancelling') IS NOT NULL THEN 'Cancelling'
            WHEN json_extract(payload, '$.Live.Filled') IS NOT NULL THEN 'Filled'
            WHEN json_extract(payload, '$.Live.Failed') IS NOT NULL THEN 'Failed'
            WHEN json_extract(payload, '$.Live.Cancelled') IS NOT NULL THEN 'Cancelled'
        END
    ) STORED,

    -- Terminal outcome timestamp, normalized to fixed-width nanoseconds so
    -- lexicographic order is chronological order. See the onchain_trade_view
    -- migration for why chrono's AutoSi form cannot be sorted as stored.
    -- NULL for non-terminal orders, which is exactly the
    -- `status IN ('Filled','Failed','Cancelled')` predicate expressed
    -- structurally -- trade history selects on `occurred_at IS NOT NULL`.
    occurred_at TEXT GENERATED ALWAYS AS (
        substr(coalesce(
            json_extract(payload, '$.Live.Filled.filled_at'),
            json_extract(payload, '$.Live.Failed.failed_at'),
            json_extract(payload, '$.Live.Cancelled.cancelled_at')
        ), 1, 19) || '.' ||
        substr(
            replace(replace(substr(coalesce(
                json_extract(payload, '$.Live.Filled.filled_at'),
                json_extract(payload, '$.Live.Failed.failed_at'),
                json_extract(payload, '$.Live.Cancelled.cancelled_at')
            ), 20), '.', ''), 'Z', '') || '000000000',
        1, 9)
    ) STORED,

    symbol TEXT GENERATED ALWAYS AS (
        coalesce(
            json_extract(payload, '$.Live.Filled.symbol'),
            json_extract(payload, '$.Live.Failed.symbol'),
            json_extract(payload, '$.Live.Cancelled.symbol')
        )
    ) STORED,

    -- Mirrors the SupportedExecutor -> TradingVenue match in
    -- OffchainOrder::try_into_trade.
    venue TEXT GENERATED ALWAYS AS (
        CASE coalesce(
            json_extract(payload, '$.Live.Filled.executor'),
            json_extract(payload, '$.Live.Failed.executor'),
            json_extract(payload, '$.Live.Cancelled.executor')
        )
            WHEN 'AlpacaBrokerApi' THEN 'alpaca'
            WHEN 'DryRun' THEN 'dry_run'
        END
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_status
    ON offchain_order_view(status)
    WHERE status IS NOT NULL;

-- Covers the trade-history sort for the offchain branch: view_id is the
-- comparator's fallback tie-break, and terminal rows are the only ones with a
-- non-null occurred_at.
CREATE INDEX idx_offchain_order_view_occurred_at
    ON offchain_order_view(occurred_at DESC, view_id)
    WHERE occurred_at IS NOT NULL;
