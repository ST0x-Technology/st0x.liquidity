-- NULL marks a legacy Position fill whose wrapped-to-underlying basis was not
-- persisted. New rows carry the exact fixed-18 ERC-4626 ratio read at the
-- fill's confirmed block.
ALTER TABLE pnl_onchain_fill
ADD COLUMN underlying_per_wrapped_fixed18 TEXT;

-- The evidence arrives on the immediately following Position event. Retaining
-- its rowid keeps historical `as_of` reads from observing evidence that had not
-- been recorded yet.
ALTER TABLE pnl_onchain_fill
ADD COLUMN underlying_per_wrapped_event_rowid INTEGER;

CREATE INDEX pnl_onchain_fill_trade
ON pnl_onchain_fill(symbol, chain, tx_hash, log_index);
