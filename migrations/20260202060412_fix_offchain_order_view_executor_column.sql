-- Fix offchain_order_view: the 'broker' generated column used wrong JSON path
-- '$.Execution.broker' (doesn't exist) instead of '$.Execution.executor'.
-- SQLite requires table recreation to modify generated columns.

CREATE TABLE offchain_order_view_new (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- STORED generated columns for efficient querying
    execution_id INTEGER GENERATED ALWAYS AS (json_extract(payload, '$.Execution.execution_id')) STORED,
    symbol TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.symbol')) STORED,
    status TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.status')) STORED,
    executor TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.executor')) STORED,
    broker_order_id TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.broker_order_id')) STORED
);

INSERT INTO offchain_order_view_new (view_id, version, payload)
SELECT view_id, version, payload FROM offchain_order_view;

DROP TABLE offchain_order_view;

ALTER TABLE offchain_order_view_new RENAME TO offchain_order_view;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_execution_id
    ON offchain_order_view(execution_id) WHERE execution_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_symbol
    ON offchain_order_view(symbol) WHERE symbol IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_status
    ON offchain_order_view(status) WHERE status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_executor
    ON offchain_order_view(executor) WHERE executor IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_broker_order_id
    ON offchain_order_view(broker_order_id) WHERE broker_order_id IS NOT NULL;
