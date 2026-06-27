-- Per-aggregate replay checkpoint for the rebalance stage-timing read model.
--
-- `last_sequence` is the highest `UsdcRebalance` event `sequence` that has been
-- folded into this row by a catch-up/rebuild pass. Startup catch-up replays only
-- events past it, so a restart re-folds the small tail since the last catch-up
-- rather than the whole event history. NULL means "never checkpointed" (existing
-- rows and rows the live reactor created without a sequence), which catch-up
-- treats as 0 and replays the full stream once before establishing a checkpoint.
ALTER TABLE rebalance_stage_timing ADD COLUMN last_sequence INTEGER;
