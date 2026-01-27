-- Enforce NOT NULL constraint on underlying_amount column.
-- SQLite requires a table rebuild to add NOT NULL to an existing column.
--
-- This migration runs after 20260126135250_add_underlying_amount_to_onchain_trades.sql
-- which already backfilled all existing rows with amount values.

-- Step 1: Create new table with NOT NULL constraint
CREATE TABLE onchain_trades_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  tx_hash TEXT NOT NULL CHECK (length(tx_hash) = 66 AND tx_hash LIKE '0x%'),
  log_index INTEGER NOT NULL CHECK (log_index >= 0),
  symbol TEXT NOT NULL CHECK (symbol != ''),
  amount REAL NOT NULL CHECK (amount > 0.0),
  direction TEXT CHECK (direction IN ('BUY', 'SELL')) NOT NULL,
  price_usdc REAL NOT NULL CHECK (price_usdc > 0.0),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  block_timestamp TIMESTAMP,
  gas_used INTEGER CHECK (gas_used IS NULL OR gas_used >= 0),
  effective_gas_price INTEGER CHECK (effective_gas_price IS NULL OR effective_gas_price >= 0),
  pyth_price REAL,
  pyth_confidence REAL CHECK (pyth_confidence IS NULL OR pyth_confidence >= 0),
  pyth_exponent INTEGER,
  pyth_publish_time TIMESTAMP,
  underlying_amount REAL NOT NULL CHECK (underlying_amount > 0.0),
  UNIQUE (tx_hash, log_index)
);

-- Step 2: Copy all data from old table
INSERT INTO onchain_trades_new (
  id, tx_hash, log_index, symbol, amount, direction, price_usdc, created_at,
  block_timestamp, gas_used, effective_gas_price,
  pyth_price, pyth_confidence, pyth_exponent, pyth_publish_time,
  underlying_amount
)
SELECT
  id, tx_hash, log_index, symbol, amount, direction, price_usdc, created_at,
  block_timestamp, gas_used, effective_gas_price,
  pyth_price, pyth_confidence, pyth_exponent, pyth_publish_time,
  underlying_amount
FROM onchain_trades;

-- Step 3: Drop old table
DROP TABLE onchain_trades;

-- Step 4: Rename new table
ALTER TABLE onchain_trades_new RENAME TO onchain_trades;

-- Step 5: Recreate index
CREATE INDEX idx_onchain_trades_symbol ON onchain_trades(symbol);
