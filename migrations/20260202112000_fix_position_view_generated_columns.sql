-- The position aggregate is now wrapped in Lifecycle<Position, E>, which
-- serializes as {"Live": {...}} instead of {"Position": {...}}.
-- The `symbol` field was removed from Position state (the aggregate ID
-- is the symbol, so view_id already holds it).
-- SQLite doesn't allow ALTER on generated columns, so recreate the table.

-- Preserve existing data
CREATE TABLE position_view_backup AS
    SELECT view_id, version, payload FROM position_view;

DROP TABLE position_view;

CREATE TABLE position_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- view_id IS the symbol (Position::aggregate_id returns symbol.to_string())
    symbol TEXT GENERATED ALWAYS AS (view_id) STORED,
    net_position TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Live.net')) STORED,
    last_updated TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Live.last_updated')) STORED
);

INSERT INTO position_view (view_id, version, payload)
    SELECT view_id, version, payload FROM position_view_backup;

DROP TABLE position_view_backup;

CREATE INDEX IF NOT EXISTS idx_position_view_symbol
    ON position_view(symbol) WHERE symbol IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_position_view_net_position
    ON position_view(net_position) WHERE net_position IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_position_view_last_updated
    ON position_view(last_updated) WHERE last_updated IS NOT NULL;
