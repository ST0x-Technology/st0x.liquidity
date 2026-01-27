-- Add block_timestamp column to onchain_trades table
ALTER TABLE onchain_trades ADD COLUMN block_timestamp TIMESTAMP;

-- Update existing records to use created_at timestamp as fallback
-- This is not ideal but prevents losing historical data
UPDATE onchain_trades SET block_timestamp = created_at;