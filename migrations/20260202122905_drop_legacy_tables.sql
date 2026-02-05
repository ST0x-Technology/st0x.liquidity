-- Drop dependent tables first (foreign key references)
DROP TABLE IF EXISTS trade_execution_links;
DROP TRIGGER IF EXISTS update_trade_accumulators_last_updated;
DROP TABLE IF EXISTS trade_accumulators;

-- Drop primary tables
DROP TABLE IF EXISTS onchain_trades;
DROP TABLE IF EXISTS offchain_trades;
DROP TABLE IF EXISTS symbol_locks;
