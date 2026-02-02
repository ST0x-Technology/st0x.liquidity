-- Fix offchain_order_view generated columns:
-- 1. 'broker' -> 'executor' (wrong JSON path)
-- 2. 'execution_id' INTEGER -> 'offchain_order_id' TEXT (renamed field, now UUID)
-- SQLite requires table recreation to modify generated columns.

CREATE TABLE offchain_order_view_new (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- STORED generated columns for efficient querying
    offchain_order_id TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.offchain_order_id')) STORED,
    symbol TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.symbol')) STORED,
    status TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.status')) STORED,
    executor TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.executor')) STORED,
    executor_order_id TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.executor_order_id')) STORED
);

INSERT INTO offchain_order_view_new (view_id, version, payload)
SELECT view_id, version, payload FROM offchain_order_view;

DROP TABLE offchain_order_view;

ALTER TABLE offchain_order_view_new RENAME TO offchain_order_view;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_offchain_order_id
    ON offchain_order_view(offchain_order_id) WHERE offchain_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_symbol
    ON offchain_order_view(symbol) WHERE symbol IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_status
    ON offchain_order_view(status) WHERE status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_executor
    ON offchain_order_view(executor) WHERE executor IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_executor_order_id
    ON offchain_order_view(executor_order_id) WHERE executor_order_id IS NOT NULL;
