-- Materialized view for TrackedOrder aggregates.
-- Used by the Active Orders View projection to query orders
-- by lifecycle state.
CREATE TABLE IF NOT EXISTS tracked_order_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
