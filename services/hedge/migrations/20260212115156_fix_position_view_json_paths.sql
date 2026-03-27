-- Fix JSON paths in position_view to match Lifecycle<T> serialization format.
--
-- Lifecycle<T> serializes as {"Live": <data>} via serde's externally-tagged
-- enum format. The original migration used incorrect paths like
-- $.Position.symbol instead of $.Live.symbol.
--
-- Views are rebuilt from events at startup, so DROP + CREATE is safe.

DROP TABLE IF EXISTS position_view;
CREATE TABLE position_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    symbol TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Live.symbol')) STORED,
    net_position TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Live.net')) STORED,
    last_updated TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Live.last_updated')) STORED
);

CREATE INDEX IF NOT EXISTS idx_position_view_symbol
    ON position_view(symbol) WHERE symbol IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_position_view_net_position
    ON position_view(net_position) WHERE net_position IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_position_view_last_updated
    ON position_view(last_updated) WHERE last_updated IS NOT NULL;
