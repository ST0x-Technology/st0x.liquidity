-- Rename finalized_block to cutoff_block: the column records the ingestion
-- cutoff block, which may now be the 'safe' block rather than 'finalized'.
-- The semantic meaning (cutoff_block - last_processed_block = lag_blocks)
-- is unchanged; only the column name is updated to reflect the configurable tag.
ALTER TABLE block_lag_samples RENAME COLUMN finalized_block TO cutoff_block;
