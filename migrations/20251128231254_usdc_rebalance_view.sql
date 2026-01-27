CREATE TABLE IF NOT EXISTS usdc_rebalance_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- STORED generated columns for efficient querying
    direction TEXT GENERATED ALWAYS AS (json_extract(payload, '$.direction')) STORED,
    state TEXT GENERATED ALWAYS AS (json_extract(payload, '$.state')) STORED,
    amount_usdc TEXT GENERATED ALWAYS AS (json_extract(payload, '$.amount')) STORED
);

CREATE INDEX IF NOT EXISTS idx_usdc_rebalance_view_direction
    ON usdc_rebalance_view(direction) WHERE direction IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_usdc_rebalance_view_state
    ON usdc_rebalance_view(state) WHERE state IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_usdc_rebalance_view_amount
    ON usdc_rebalance_view(amount_usdc) WHERE amount_usdc IS NOT NULL;
