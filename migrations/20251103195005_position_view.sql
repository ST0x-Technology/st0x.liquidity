CREATE TABLE IF NOT EXISTS position_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- STORED generated columns for efficient querying
    symbol TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Position.symbol')) STORED,
    net_position TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Position.net')) STORED,
    last_updated TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Position.last_updated')) STORED
);

CREATE INDEX IF NOT EXISTS idx_position_view_symbol
    ON position_view(symbol) WHERE symbol IS NOT NULL;

-- Index on net_position as TEXT preserves exact decimal representation
CREATE INDEX IF NOT EXISTS idx_position_view_net_position
    ON position_view(net_position) WHERE net_position IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_position_view_last_updated
    ON position_view(last_updated) WHERE last_updated IS NOT NULL;
