-- Migration: Fractional Execution Shares
-- Change offchain_trades.shares from INTEGER to REAL for fractional share support.
-- Alpaca supports fractional shares; Schwab executor will reject fractional values.

-- Step 1: Drop indexes that reference offchain_trades
DROP INDEX IF EXISTS idx_offchain_trades_symbol;
DROP INDEX IF EXISTS idx_offchain_trades_status;
DROP INDEX IF EXISTS idx_offchain_trades_broker;
DROP INDEX IF EXISTS idx_unique_in_progress_execution_per_symbol;

-- Step 2: Create new offchain_trades table with REAL shares
CREATE TABLE offchain_trades_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL CHECK (symbol != ''),
  shares REAL NOT NULL CHECK (shares > 0),  -- Changed from INTEGER to REAL
  direction TEXT CHECK (direction IN ('BUY', 'SELL')) NOT NULL,
  broker TEXT NOT NULL DEFAULT 'schwab' CHECK (broker != ''),
  broker_order_id TEXT CHECK (broker_order_id IS NULL OR broker_order_id != ''),
  order_id TEXT CHECK (order_id IS NULL OR order_id != ''),
  price_cents INTEGER CHECK (price_cents IS NULL OR price_cents >= 0),
  status TEXT CHECK (status IN ('PENDING', 'SUBMITTED', 'FILLED', 'FAILED')) NOT NULL DEFAULT 'PENDING',
  executed_at TIMESTAMP,
  CHECK (
    (status = 'PENDING' AND executed_at IS NULL) OR
    (status = 'SUBMITTED' AND (broker_order_id IS NOT NULL OR order_id IS NOT NULL) AND executed_at IS NULL) OR
    (status = 'FILLED' AND (broker_order_id IS NOT NULL OR order_id IS NOT NULL) AND executed_at IS NOT NULL AND price_cents IS NOT NULL) OR
    (status = 'FAILED' AND executed_at IS NOT NULL)
  )
);

-- Step 3: Copy existing data (INTEGER values auto-convert to REAL)
INSERT INTO offchain_trades_new (
  id, symbol, shares, direction, broker, broker_order_id, order_id,
  price_cents, status, executed_at
)
SELECT
  id, symbol, shares, direction, broker, broker_order_id, order_id,
  price_cents, status, executed_at
FROM offchain_trades;

-- Step 4: Drop old table and rename new one
DROP TABLE offchain_trades;
ALTER TABLE offchain_trades_new RENAME TO offchain_trades;

-- Step 5: Recreate indexes
CREATE INDEX idx_offchain_trades_symbol ON offchain_trades(symbol);
CREATE INDEX idx_offchain_trades_status ON offchain_trades(status);
CREATE INDEX idx_offchain_trades_broker ON offchain_trades(broker);
CREATE UNIQUE INDEX idx_unique_in_progress_execution_per_symbol
ON offchain_trades(symbol)
WHERE status IN ('PENDING', 'SUBMITTED');
