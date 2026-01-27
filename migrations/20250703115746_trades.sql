-- Onchain trades are immutable blockchain facts
CREATE TABLE onchain_trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  tx_hash TEXT NOT NULL CHECK (length(tx_hash) = 66 AND tx_hash LIKE '0x%'),  -- Ensure valid transaction hash format
  log_index INTEGER NOT NULL CHECK (log_index >= 0),  -- Log index must be non-negative
  symbol TEXT NOT NULL CHECK (symbol != ''),  -- Valid symbol constraints
  amount REAL NOT NULL CHECK (amount > 0.0),  -- Trade amount must be positive (quantity only)
  direction TEXT CHECK (direction IN ('BUY', 'SELL')) NOT NULL,  -- Explicit trade direction for Schwab execution
  price_usdc REAL NOT NULL CHECK (price_usdc > 0.0),  -- Price must be positive
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (tx_hash, log_index)
);

CREATE TABLE schwab_executions (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  symbol TEXT NOT NULL CHECK (symbol != ''),  -- Valid symbol constraints
  shares INTEGER NOT NULL CHECK (shares > 0),  -- Must execute positive whole shares
  direction TEXT CHECK (direction IN ('BUY', 'SELL')) NOT NULL,
  order_id TEXT CHECK (order_id IS NULL OR order_id != ''),  -- Valid order ID or NULL
  price_cents INTEGER CHECK (price_cents IS NULL OR price_cents >= 0),  -- Price must be non-negative or NULL
  status TEXT CHECK (status IN ('PENDING', 'SUBMITTED', 'FILLED', 'FAILED')) NOT NULL DEFAULT 'PENDING',
  executed_at TIMESTAMP,
  CHECK (
    (status = 'PENDING' AND executed_at IS NULL) OR
    (status = 'SUBMITTED' AND order_id IS NOT NULL AND executed_at IS NULL) OR
    (status = 'FILLED' AND order_id IS NOT NULL AND executed_at IS NOT NULL AND price_cents IS NOT NULL) OR
    (status = 'FAILED' AND executed_at IS NOT NULL)
  )
);

-- Unified trade accumulator - ONE table that tracks everything
CREATE TABLE trade_accumulators (
  symbol TEXT PRIMARY KEY NOT NULL,
  net_position REAL NOT NULL DEFAULT 0.0,  -- Running position for threshold checking
  accumulated_long REAL NOT NULL DEFAULT 0.0 CHECK (accumulated_long >= 0.0),  -- Fractional shares accumulated for buying
  accumulated_short REAL NOT NULL DEFAULT 0.0 CHECK (accumulated_short >= 0.0),  -- Fractional shares accumulated for selling
  pending_execution_id INTEGER REFERENCES schwab_executions(id) ON DELETE SET NULL ON UPDATE CASCADE,  -- Current pending execution if any
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CHECK (symbol != '')  -- Ensure symbol is not empty
);

-- Trade-Execution linkage table for complete audit trail
-- Links individual onchain trades to their contributing Schwab executions
-- Supports many-to-many relationships as multiple trades can contribute to one execution
-- and a single large trade could theoretically span multiple executions
CREATE TABLE trade_execution_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  trade_id INTEGER NOT NULL REFERENCES onchain_trades(id) ON DELETE CASCADE ON UPDATE CASCADE,
  execution_id INTEGER NOT NULL REFERENCES schwab_executions(id) ON DELETE CASCADE ON UPDATE CASCADE,
  contributed_shares REAL NOT NULL CHECK (contributed_shares > 0.0),  -- Fractional shares this trade contributed to execution
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  UNIQUE (trade_id, execution_id)  -- Prevent duplicate linkages between same trade/execution pair
);

-- Indexes for new tables
CREATE INDEX idx_onchain_trades_symbol ON onchain_trades(symbol);
CREATE INDEX idx_schwab_executions_symbol ON schwab_executions(symbol);
CREATE INDEX idx_schwab_executions_status ON schwab_executions(status);

-- Indexes for trade_execution_links table (audit queries)
CREATE INDEX idx_trade_execution_links_trade_id ON trade_execution_links(trade_id);
CREATE INDEX idx_trade_execution_links_execution_id ON trade_execution_links(execution_id);
CREATE INDEX idx_trade_execution_links_created_at ON trade_execution_links(created_at);
CREATE INDEX idx_trade_execution_links_trade_exec ON trade_execution_links(trade_id, execution_id);

-- Data integrity constraints
-- Ensure only one in-progress execution per symbol (prevents race conditions)
CREATE UNIQUE INDEX idx_unique_in_progress_execution_per_symbol
ON schwab_executions(symbol)
WHERE status IN ('PENDING', 'SUBMITTED');

-- Ensure only one in-progress execution reference per symbol in accumulators
CREATE UNIQUE INDEX idx_unique_in_progress_execution_in_accumulator
ON trade_accumulators(pending_execution_id)
WHERE pending_execution_id IS NOT NULL;

/* NOTE: Storing underlying Schwab auth tokens is sensitive.
 * Ensure that this table is secured and access is controlled.
 * Consider encrypting the tokens if necessary.
 */
CREATE TABLE schwab_auth (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  access_token TEXT NOT NULL,
  access_token_fetched_at DATETIME NOT NULL,
  refresh_token TEXT NOT NULL,
  refresh_token_fetched_at DATETIME NOT NULL
);

-- Symbol locks table for per-symbol execution concurrency control
CREATE TABLE symbol_locks (
  symbol TEXT PRIMARY KEY NOT NULL,
  locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Event queue table for idempotent event processing
-- Ensures events are persisted before processing to prevent loss during restarts
CREATE TABLE event_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  tx_hash TEXT NOT NULL CHECK (length(tx_hash) = 66 AND tx_hash LIKE '0x%'),
  log_index INTEGER NOT NULL CHECK (log_index >= 0),
  block_number INTEGER NOT NULL CHECK (block_number >= 0),
  event_data TEXT NOT NULL,  -- JSON serialized event data
  processed BOOLEAN NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  processed_at TIMESTAMP,
  UNIQUE (tx_hash, log_index),  -- Prevent duplicate events
  CHECK (event_data != '')  -- Ensure event data is not empty
);

-- Indexes for event_queue table
CREATE INDEX idx_event_queue_processed ON event_queue(processed);
CREATE INDEX idx_event_queue_block_number ON event_queue(block_number);
CREATE INDEX idx_event_queue_created_at ON event_queue(created_at);

-- Trigger to automatically update last_updated column on trade_accumulators updates
CREATE TRIGGER IF NOT EXISTS update_trade_accumulators_last_updated
  AFTER UPDATE ON trade_accumulators
  FOR EACH ROW
  WHEN OLD.last_updated = NEW.last_updated
  BEGIN
    UPDATE trade_accumulators SET last_updated = CURRENT_TIMESTAMP WHERE rowid = NEW.rowid;
  END;
