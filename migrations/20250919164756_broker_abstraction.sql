-- Migration: Broker Abstraction
-- Rename schwab_executions to offchain_trades and add broker type support

-- Step 1: Create new offchain_trades table with broker_type column
CREATE TABLE offchain_trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL CHECK (symbol != ''),  -- Trading symbol 
  shares INTEGER NOT NULL CHECK (shares > 0),  -- Must execute positive whole shares
  direction TEXT CHECK (direction IN ('BUY', 'SELL')) NOT NULL,
  broker TEXT NOT NULL DEFAULT 'schwab' CHECK (broker != ''),  -- Broker identifier
  broker_order_id TEXT CHECK (broker_order_id IS NULL OR broker_order_id != ''),  -- Broker-specific order ID
  order_id TEXT CHECK (order_id IS NULL OR order_id != ''),  -- Legacy order ID for backward compatibility
  price_cents INTEGER CHECK (price_cents IS NULL OR price_cents >= 0),  -- Price must be non-negative or NULL
  status TEXT CHECK (status IN ('PENDING', 'SUBMITTED', 'FILLED', 'FAILED')) NOT NULL DEFAULT 'PENDING',
  executed_at TIMESTAMP,
  CHECK (
    (status = 'PENDING' AND executed_at IS NULL) OR
    (status = 'SUBMITTED' AND (broker_order_id IS NOT NULL OR order_id IS NOT NULL) AND executed_at IS NULL) OR
    (status = 'FILLED' AND (broker_order_id IS NOT NULL OR order_id IS NOT NULL) AND executed_at IS NOT NULL AND price_cents IS NOT NULL) OR
    (status = 'FAILED' AND executed_at IS NOT NULL)
  )
);

-- Step 2: Copy data from schwab_executions to offchain_trades
INSERT INTO offchain_trades (
  id, symbol, shares, direction, broker, broker_order_id, order_id, 
  price_cents, status, executed_at
)
SELECT 
  id, symbol, shares, direction, 'schwab', order_id, order_id,
  price_cents, status, executed_at
FROM schwab_executions;

-- Step 3: Update foreign key references

-- Update trade_accumulators table
-- First, drop the view that depends on trade_accumulators
DROP VIEW IF EXISTS trade_accumulators_with_net;

-- Create a new table with the updated foreign key
CREATE TABLE trade_accumulators_new (
  symbol TEXT PRIMARY KEY NOT NULL,
  net_position REAL NOT NULL DEFAULT 0.0,  -- Running position for threshold checking
  accumulated_long REAL NOT NULL DEFAULT 0.0 CHECK (accumulated_long >= 0.0),  -- Fractional shares accumulated for buying
  accumulated_short REAL NOT NULL DEFAULT 0.0 CHECK (accumulated_short >= 0.0),  -- Fractional shares accumulated for selling
  pending_execution_id INTEGER REFERENCES offchain_trades(id) ON DELETE SET NULL ON UPDATE CASCADE,  -- Current pending execution if any
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CHECK (symbol != '')  -- Ensure symbol is not empty
);

-- Copy data from old to new (explicitly specify columns since we're adding net_position back)
INSERT INTO trade_accumulators_new (symbol, net_position, accumulated_long, accumulated_short, pending_execution_id, last_updated)
SELECT symbol, (accumulated_long - accumulated_short), accumulated_long, accumulated_short, pending_execution_id, last_updated 
FROM trade_accumulators;

-- Drop old table and rename new one
DROP TABLE trade_accumulators;
ALTER TABLE trade_accumulators_new RENAME TO trade_accumulators;

-- Update trade_execution_links table
-- Create new table with updated foreign key
CREATE TABLE trade_execution_links_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id INTEGER NOT NULL REFERENCES onchain_trades(id) ON DELETE CASCADE ON UPDATE CASCADE,
  execution_id INTEGER NOT NULL REFERENCES offchain_trades(id) ON DELETE CASCADE ON UPDATE CASCADE,
  contributed_shares REAL NOT NULL CHECK (contributed_shares > 0.0),  -- Fractional shares from this trade that contributed to this execution
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  UNIQUE (trade_id, execution_id)  -- Prevent duplicate linkages between same trade/execution pair
);

-- Copy data from old to new
INSERT INTO trade_execution_links_new SELECT * FROM trade_execution_links;

-- Drop old table and rename new one
DROP TABLE trade_execution_links;
ALTER TABLE trade_execution_links_new RENAME TO trade_execution_links;

-- Step 4: Drop the old schwab_executions table
DROP TABLE schwab_executions;

-- Step 5: Create indexes on new table
CREATE INDEX idx_offchain_trades_symbol ON offchain_trades(symbol);
CREATE INDEX idx_offchain_trades_status ON offchain_trades(status);
CREATE INDEX idx_offchain_trades_broker ON offchain_trades(broker);

-- Recreate indexes for the updated tables
CREATE INDEX idx_trade_execution_links_trade_id ON trade_execution_links(trade_id);
CREATE INDEX idx_trade_execution_links_execution_id ON trade_execution_links(execution_id);
CREATE INDEX idx_trade_execution_links_trade_exec ON trade_execution_links(trade_id, execution_id);

-- Create trigger for last_updated timestamp on trade_accumulators
-- Only update timestamp if it wasn't explicitly changed (allows manual timestamp setting for tests)
CREATE TRIGGER update_trade_accumulators_last_updated
AFTER UPDATE ON trade_accumulators
FOR EACH ROW
WHEN OLD.last_updated = NEW.last_updated
BEGIN
  UPDATE trade_accumulators SET last_updated = CURRENT_TIMESTAMP WHERE rowid = NEW.rowid;
END;

-- Create partial index for pending executions
CREATE UNIQUE INDEX idx_trade_accumulators_pending_execution
ON trade_accumulators(pending_execution_id)
WHERE pending_execution_id IS NOT NULL;

-- Create unique constraint to prevent multiple pending executions per symbol
CREATE UNIQUE INDEX idx_unique_in_progress_execution_per_symbol
ON offchain_trades(symbol)
WHERE status IN ('PENDING', 'SUBMITTED');

-- Note: The trade_accumulators_with_net view is no longer needed since net_position 
-- is now a column in the table rather than a calculated field