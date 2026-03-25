-- Fix JSON paths in offchain_order_view to match Lifecycle<T> serialization.
--
-- OffchainOrder is an enum: Lifecycle serializes as {"Live": {"Submitted": {...}}}.
-- The variant name under $.Live IS the status. We extract it via CASE WHEN
-- and filter in Rust for everything else.
--
-- Views are rebuilt from events at startup, so DROP + CREATE is safe.

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
            WHEN json_extract(payload, '$.Live.Filled') IS NOT NULL THEN 'Filled'
            WHEN json_extract(payload, '$.Live.Failed') IS NOT NULL THEN 'Failed'
        END
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_status
    ON offchain_order_view(status) WHERE status IS NOT NULL;
