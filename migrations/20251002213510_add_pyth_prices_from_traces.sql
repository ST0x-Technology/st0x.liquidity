-- Add Pyth oracle price data columns extracted from transaction traces
-- These store the exact prices returned by Pyth oracle during on-chain execution

ALTER TABLE onchain_trades ADD COLUMN pyth_price REAL;
ALTER TABLE onchain_trades ADD COLUMN pyth_confidence REAL CHECK (pyth_confidence IS NULL OR pyth_confidence >= 0);
ALTER TABLE onchain_trades ADD COLUMN pyth_exponent INTEGER;
ALTER TABLE onchain_trades ADD COLUMN pyth_publish_time TIMESTAMP;
