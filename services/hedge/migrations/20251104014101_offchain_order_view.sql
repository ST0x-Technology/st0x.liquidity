-- OffchainOrder view: broker order execution tracking
CREATE TABLE IF NOT EXISTS offchain_order_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,

    -- STORED generated columns for efficient querying
    execution_id INTEGER GENERATED ALWAYS AS (json_extract(payload, '$.Execution.execution_id')) STORED,
    symbol TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.symbol')) STORED,
    status TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.status')) STORED,
    broker TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.broker')) STORED,
    broker_order_id TEXT GENERATED ALWAYS AS (json_extract(payload, '$.Execution.broker_order_id')) STORED
);

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_execution_id
    ON offchain_order_view(execution_id) WHERE execution_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_symbol
    ON offchain_order_view(symbol) WHERE symbol IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_status
    ON offchain_order_view(status) WHERE status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_broker
    ON offchain_order_view(broker) WHERE broker IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_offchain_order_view_broker_order_id
    ON offchain_order_view(broker_order_id) WHERE broker_order_id IS NOT NULL;
