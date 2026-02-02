-- Fix offchain_order_view generated columns for Lifecycle<OffchainOrder, Never> wrapping.
-- The aggregate serializes as {"Live": {"Submitted": {...fields...}}}, so we use
-- CASE expressions to extract the variant name (status) and COALESCE for shared fields.
-- SQLite requires table recreation to modify generated columns.

CREATE TABLE offchain_order_view_new (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- STORED generated columns using Lifecycle serialization paths
    symbol TEXT GENERATED ALWAYS AS (
        COALESCE(
            json_extract(payload, '$.Live.Pending.symbol'),
            json_extract(payload, '$.Live.Submitted.symbol'),
            json_extract(payload, '$.Live.PartiallyFilled.symbol'),
            json_extract(payload, '$.Live.Filled.symbol'),
            json_extract(payload, '$.Live.Failed.symbol')
        )
    ) STORED,
    status TEXT GENERATED ALWAYS AS (
        CASE
            WHEN json_extract(payload, '$.Live.Pending') IS NOT NULL THEN 'Pending'
            WHEN json_extract(payload, '$.Live.Submitted') IS NOT NULL THEN 'Submitted'
            WHEN json_extract(payload, '$.Live.PartiallyFilled') IS NOT NULL THEN 'PartiallyFilled'
            WHEN json_extract(payload, '$.Live.Filled') IS NOT NULL THEN 'Filled'
            WHEN json_extract(payload, '$.Live.Failed') IS NOT NULL THEN 'Failed'
            ELSE NULL
        END
    ) STORED,
    executor TEXT GENERATED ALWAYS AS (
        COALESCE(
            json_extract(payload, '$.Live.Pending.executor'),
            json_extract(payload, '$.Live.Submitted.executor'),
            json_extract(payload, '$.Live.PartiallyFilled.executor'),
            json_extract(payload, '$.Live.Filled.executor'),
            json_extract(payload, '$.Live.Failed.executor')
        )
    ) STORED,
    executor_order_id TEXT GENERATED ALWAYS AS (
        COALESCE(
            json_extract(payload, '$.Live.Submitted.executor_order_id'),
            json_extract(payload, '$.Live.PartiallyFilled.executor_order_id'),
            json_extract(payload, '$.Live.Filled.executor_order_id')
        )
    ) STORED
);

INSERT INTO offchain_order_view_new (view_id, version, payload)
SELECT view_id, version, payload FROM offchain_order_view;

DROP TABLE offchain_order_view;

ALTER TABLE offchain_order_view_new RENAME TO offchain_order_view;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_symbol
    ON offchain_order_view(symbol) WHERE symbol IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_status
    ON offchain_order_view(status) WHERE status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_executor
    ON offchain_order_view(executor) WHERE executor IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_executor_order_id
    ON offchain_order_view(executor_order_id) WHERE executor_order_id IS NOT NULL;
