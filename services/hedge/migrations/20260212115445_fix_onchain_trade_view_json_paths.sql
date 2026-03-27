-- Fix JSON paths in onchain_trade_view to match Lifecycle<T> serialization.
--
-- Lifecycle<T> serializes as {"Live": <data>} via serde's externally-tagged
-- enum format. The original migration used $.Trade.* instead of $.Live.*.
--
-- Views are rebuilt from events at startup, so DROP + CREATE is safe.

DROP TABLE IF EXISTS onchain_trade_view;
CREATE TABLE onchain_trade_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_onchain_trade_view_tx_hash
    ON onchain_trade_view(json_extract(payload, '$.Live.tx_hash'));
CREATE INDEX IF NOT EXISTS idx_onchain_trade_view_symbol
    ON onchain_trade_view(json_extract(payload, '$.Live.symbol'));
CREATE INDEX IF NOT EXISTS idx_onchain_trade_view_block_number
    ON onchain_trade_view(json_extract(payload, '$.Live.block_number'));
CREATE INDEX IF NOT EXISTS idx_onchain_trade_view_tx_hash_log_index
    ON onchain_trade_view(json_extract(payload, '$.Live.tx_hash'), json_extract(payload, '$.Live.log_index'));
