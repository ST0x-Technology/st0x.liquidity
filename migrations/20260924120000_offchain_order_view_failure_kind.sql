-- Add a `failure_kind` key to offchain_order_view so /trades can exclude
-- deferred placements without parsing every terminal order's payload.
--
-- Trade history counts and pages terminal orders on every request and every
-- delivery reconciliation pass. Filtering deferrals with
-- `json_extract(payload, ...)` parsed the payload of every terminal row, about
-- ten times the cost of the count before the filter existed. A STORED column
-- is computed once at write time; a VIRTUAL one would be recomputed from the
-- payload on every read and save nothing. SQLite cannot ALTER TABLE ADD a
-- STORED generated column, so this is DROP + CREATE, as in the earlier
-- recreates of this table: views are rebuilt from events at startup, because
-- projection catch_up replays every aggregate whose view row is missing.
--
-- Every other column and both indexes are carried over unchanged. As before,
-- a recreate cannot preserve rowid, so /api/pending_orders (which orders by
-- `rowid DESC`) shows pending orders predating this migration in arbitrary
-- order until new orders take increasing rowids.

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
    ) STORED,

    -- Why a failed order ended, read from its `kind` discriminator. NULL for
    -- non failures and for failures persisted before the discriminator
    -- existed, so trade history excludes deferrals with the NULL safe
    -- `failure_kind IS NOT 'Deferral'`.
    failure_kind TEXT GENERATED ALWAYS AS (
        json_extract(payload, '$.Live.Failed.kind')
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
