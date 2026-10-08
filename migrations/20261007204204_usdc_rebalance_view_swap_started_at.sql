-- Rebuild usdc_rebalance_view.started_at with the Relay swap states
-- (`SwapQuoted`, `SwapDepositPrepared`, `SwapDeposited`, aggregate v14). With
-- no arm for them a transfer in one of those states has started_at NULL and is
-- hidden from the dashboard's `started_at IS NOT NULL` listing. Generated
-- columns cannot be altered in place, so the table is dropped and recreated;
-- StoreBuilder catches it back up from the immutable event streams at startup.

DROP TABLE IF EXISTS usdc_rebalance_view;
CREATE TABLE usdc_rebalance_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    started_at_raw TEXT GENERATED ALWAYS AS (coalesce(
        json_extract(payload, '$.Live.Converting.initiated_at'),
        json_extract(payload, '$.Live.ConversionComplete.initiated_at'),
        json_extract(payload, '$.Live.ConversionFailed.initiated_at'),
        json_extract(payload, '$.Live.WithdrawalSubmitting.initiated_at'),
        json_extract(payload, '$.Live.Withdrawing.initiated_at'),
        json_extract(payload, '$.Live.WithdrawalComplete.initiated_at'),
        json_extract(payload, '$.Live.WithdrawalFailed.initiated_at'),
        json_extract(payload, '$.Live.BridgingSubmitting.initiated_at'),
        json_extract(payload, '$.Live.Bridging.initiated_at'),
        json_extract(payload, '$.Live.AwaitingAttestation.initiated_at'),
        json_extract(payload, '$.Live.Attested.initiated_at'),
        json_extract(payload, '$.Live.SwapQuoted.initiated_at'),
        json_extract(payload, '$.Live.SwapDepositPrepared.initiated_at'),
        json_extract(payload, '$.Live.SwapDeposited.initiated_at'),
        json_extract(payload, '$.Live.Bridged.initiated_at'),
        json_extract(payload, '$.Live.BridgingFailed.initiated_at'),
        json_extract(payload, '$.Live.DepositInitiated.initiated_at'),
        json_extract(payload, '$.Live.DepositConfirmed.initiated_at'),
        json_extract(payload, '$.Live.DepositFailed.initiated_at'),
        json_extract(payload, '$.Live.Reconciled.initiated_at')
    )) VIRTUAL,
    started_at TEXT GENERATED ALWAYS AS (
        CASE WHEN started_at_raw IS NOT NULL THEN
            substr(started_at_raw, 1, 19) || '.' ||
            substr(
                replace(replace(substr(started_at_raw, 20), '.', ''), 'Z', '') ||
                    '000000000',
                1,
                9
            )
        END
    ) STORED,
    terminal_at_raw TEXT GENERATED ALWAYS AS (coalesce(
        CASE
            WHEN json_extract(payload, '$.Live.ConversionComplete.direction') = 'BaseToAlpaca'
            THEN json_extract(payload, '$.Live.ConversionComplete.converted_at')
        END,
        json_extract(payload, '$.Live.ConversionFailed.failed_at'),
        json_extract(payload, '$.Live.WithdrawalFailed.failed_at'),
        json_extract(payload, '$.Live.BridgingFailed.failed_at'),
        CASE
            WHEN json_extract(payload, '$.Live.DepositConfirmed.direction') = 'AlpacaToBase'
            THEN json_extract(payload, '$.Live.DepositConfirmed.deposit_confirmed_at')
        END,
        json_extract(payload, '$.Live.DepositFailed.failed_at'),
        json_extract(payload, '$.Live.Reconciled.reconciled_at')
    )) VIRTUAL,
    terminal_at TEXT GENERATED ALWAYS AS (
        CASE WHEN terminal_at_raw IS NOT NULL THEN
            substr(terminal_at_raw, 1, 19) || '.' ||
            substr(
                replace(replace(substr(terminal_at_raw, 20), '.', ''), 'Z', '') ||
                    '000000000',
                1,
                9
            )
        END
    ) STORED
);

CREATE INDEX idx_usdc_rebalance_view_started_at
    ON usdc_rebalance_view (started_at DESC, view_id ASC)
    WHERE started_at IS NOT NULL;

CREATE INDEX idx_usdc_rebalance_view_terminal_at
    ON usdc_rebalance_view (terminal_at DESC, view_id ASC);
