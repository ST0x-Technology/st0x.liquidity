-- Add underlying_amount column for wrapped token support.
-- For wrapped tokens, this stores the underlying-equivalent amount used for hedging.
-- For unwrapped tokens, this equals the raw 'amount'.
--
-- This column is always set (never NULL) to avoid fallback behavior in financial calculations.

-- Step 1: Add column as nullable
ALTER TABLE onchain_trades ADD COLUMN underlying_amount REAL;

-- Step 2: Backfill existing rows with amount (unwrapped tokens)
UPDATE onchain_trades SET underlying_amount = amount WHERE underlying_amount IS NULL;
