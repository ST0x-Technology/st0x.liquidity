-- Add gas tracking columns to onchain_trades table for Base L2 transactions
-- Store only raw values from transaction receipt to maintain data normalization
-- Constrain to i64 range: 0 to i64::MAX (2^63 - 1)
ALTER TABLE onchain_trades ADD COLUMN gas_used INTEGER
    CHECK(gas_used IS NULL OR (gas_used >= 0 AND gas_used <= 9223372036854775807));

ALTER TABLE onchain_trades ADD COLUMN effective_gas_price INTEGER
    CHECK(effective_gas_price IS NULL OR (effective_gas_price >= 0 AND effective_gas_price <= 9223372036854775807));