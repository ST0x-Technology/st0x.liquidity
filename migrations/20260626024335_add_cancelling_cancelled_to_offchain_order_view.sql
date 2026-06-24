-- Add the Cancelling and Cancelled lifecycle states to the generated status
-- column. Without these arms, an order in either state gets a NULL status and
-- is invisible to status-filtered queries (the /api/pending_orders query is
-- extended to include 'Cancelling' later in the stack).
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
            WHEN json_extract(payload, '$.Live.Cancelling') IS NOT NULL THEN 'Cancelling'
            WHEN json_extract(payload, '$.Live.Filled') IS NOT NULL THEN 'Filled'
            WHEN json_extract(payload, '$.Live.Failed') IS NOT NULL THEN 'Failed'
            WHEN json_extract(payload, '$.Live.Cancelled') IS NOT NULL THEN 'Cancelled'
        END
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_status
    ON offchain_order_view(status)
    WHERE status IS NOT NULL;
