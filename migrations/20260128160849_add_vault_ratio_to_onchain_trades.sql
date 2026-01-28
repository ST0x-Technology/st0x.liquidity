-- Add vault_ratio column for wrapped token audit trail.
--
-- vault_ratio stores the historical assets_per_share value from the ERC-4626 vault
-- at the time of the trade. This is for audit purposes only - actual underlying
-- amounts are always calculated at query time using the current vault ratio.
--
-- For unwrapped tokens, this column is NULL.

ALTER TABLE onchain_trades ADD COLUMN vault_ratio REAL CHECK (vault_ratio IS NULL OR vault_ratio > 0.0);
