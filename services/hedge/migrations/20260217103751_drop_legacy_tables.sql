-- Drop legacy pre-CQRS tables that are no longer referenced in application code.
-- All state is now managed through event sourcing (events/snapshots tables)
-- and CQRS projections (*_view tables).

-- Drop tables with foreign key dependencies first
DROP TABLE IF EXISTS trade_execution_links;
DROP TABLE IF EXISTS trade_accumulators;

-- Drop remaining legacy tables
DROP TABLE IF EXISTS offchain_trades;
DROP TABLE IF EXISTS onchain_trades;
DROP TABLE IF EXISTS metrics_pnl;
DROP TABLE IF EXISTS symbol_locks;
