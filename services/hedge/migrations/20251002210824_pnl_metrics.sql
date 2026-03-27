CREATE TABLE metrics_pnl (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  symbol TEXT NOT NULL CHECK (symbol != ''),
  timestamp TIMESTAMP NOT NULL,
  trade_type TEXT NOT NULL CHECK (trade_type IN ('ONCHAIN', 'OFFCHAIN')),
  trade_id INTEGER NOT NULL,
  trade_direction TEXT NOT NULL CHECK (trade_direction IN ('BUY', 'SELL')),
  quantity REAL NOT NULL CHECK (quantity > 0),
  price_per_share REAL NOT NULL CHECK (price_per_share > 0),
  realized_pnl REAL,
  cumulative_pnl REAL NOT NULL,
  net_position_after REAL NOT NULL,
  UNIQUE (trade_type, trade_id)
);

CREATE INDEX idx_metrics_pnl_symbol_timestamp ON metrics_pnl(symbol, timestamp);
CREATE INDEX idx_metrics_pnl_symbol ON metrics_pnl(symbol);
CREATE INDEX idx_metrics_pnl_timestamp ON metrics_pnl(timestamp);
