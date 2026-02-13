-- Fix usdc_rebalance_view to match Lifecycle<T> serialization format.
--
-- UsdcRebalance is an enum wrapped in Lifecycle, so the payload structure
-- is {"Live": {"Mint": {...}}} etc. No generated columns needed.
--
-- Views are rebuilt from events at startup, so DROP + CREATE is safe.

DROP TABLE IF EXISTS usdc_rebalance_view;
CREATE TABLE usdc_rebalance_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
