-- OnChain trade view: immutable record of all blockchain trades
CREATE TABLE IF NOT EXISTS onchain_trade_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_onchain_trade_view_tx_hash
    ON onchain_trade_view(json_extract(payload, '$.Trade.tx_hash'));
CREATE INDEX IF NOT EXISTS idx_onchain_trade_view_symbol
    ON onchain_trade_view(json_extract(payload, '$.Trade.symbol'));
CREATE INDEX IF NOT EXISTS idx_onchain_trade_view_block_number
    ON onchain_trade_view(json_extract(payload, '$.Trade.block_number'));
CREATE INDEX IF NOT EXISTS idx_onchain_trade_view_tx_hash_log_index
    ON onchain_trade_view(json_extract(payload, '$.Trade.tx_hash'), json_extract(payload, '$.Trade.log_index'));
