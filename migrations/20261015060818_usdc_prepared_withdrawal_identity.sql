-- Rebuild derived USDC state under schema v18. Immutable events remain intact;
-- legacy WithdrawalSubmitting events deserialize with prepared = None.
DELETE FROM snapshots WHERE aggregate_type = 'UsdcRebalance';
DELETE FROM usdc_rebalance_view;
