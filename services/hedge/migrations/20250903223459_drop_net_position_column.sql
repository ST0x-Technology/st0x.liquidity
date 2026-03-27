-- Drop redundant net_position column from trade_accumulators table
-- The net_position is now calculated on-demand as (accumulated_long - accumulated_short)

-- Drop the redundant net_position column
ALTER TABLE trade_accumulators DROP COLUMN net_position;

-- Create a view for convenience where net_position calculation is needed
CREATE VIEW trade_accumulators_with_net AS
SELECT 
    symbol,
    accumulated_long,
    accumulated_short,
    (accumulated_long - accumulated_short) as net_position,
    pending_execution_id,
    last_updated
FROM trade_accumulators;
