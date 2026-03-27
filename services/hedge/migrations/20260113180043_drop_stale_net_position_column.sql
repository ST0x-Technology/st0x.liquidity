-- Drop the stale net_position column from trade_accumulators table.
--
-- This column was re-added in migration 20250919164756_broker_abstraction but
-- save_within_transaction() never updated it, causing it to become stale.
-- The correct net position should always be computed on-demand as:
--   (accumulated_long - accumulated_short)
--
-- This fixes the bug where check_all_accumulated_positions() would miss positions
-- that had stale net_position below threshold but actual accumulated values above
-- threshold.

ALTER TABLE trade_accumulators DROP COLUMN net_position;
