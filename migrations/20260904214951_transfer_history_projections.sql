-- Materialize the three transfer aggregates so dashboard history can filter,
-- order, and paginate without replaying every event stream on every request.
-- StoreBuilder catches each table up from immutable events during startup, so
-- recreating the previously unused/stale views is safe.

DROP TABLE IF EXISTS tokenized_equity_mint_view;
CREATE TABLE tokenized_equity_mint_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    started_at_raw TEXT GENERATED ALWAYS AS (coalesce(
        json_extract(payload, '$.Live.MintRequested.requested_at'),
        json_extract(payload, '$.Live.MintAccepted.requested_at'),
        json_extract(payload, '$.Live.TokensReceived.requested_at'),
        json_extract(payload, '$.Live.WrapSubmitted.requested_at'),
        json_extract(payload, '$.Live.TokensWrapped.requested_at'),
        json_extract(payload, '$.Live.VaultDepositSubmitted.requested_at'),
        json_extract(payload, '$.Live.DepositedIntoRaindex.requested_at'),
        json_extract(payload, '$.Live.Failed.requested_at'),
        json_extract(payload, '$.Live.Reconciled.requested_at')
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
        json_extract(payload, '$.Live.DepositedIntoRaindex.deposited_at'),
        json_extract(payload, '$.Live.Failed.failed_at'),
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

CREATE INDEX idx_tokenized_equity_mint_view_started_at
    ON tokenized_equity_mint_view (started_at DESC, view_id ASC)
    WHERE started_at IS NOT NULL;

CREATE INDEX idx_tokenized_equity_mint_view_terminal_at
    ON tokenized_equity_mint_view (terminal_at DESC, view_id ASC);

DROP TABLE IF EXISTS equity_redemption_view;
CREATE TABLE equity_redemption_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    started_at_raw TEXT GENERATED ALWAYS AS (coalesce(
        json_extract(payload, '$.Live.VaultWithdrawPending.pending_at'),
        json_extract(payload, '$.Live.VaultWithdrawSubmitted.submitted_at'),
        json_extract(payload, '$.Live.WithdrawnFromRaindex.withdrawn_at'),
        json_extract(payload, '$.Live.UnwrapPending.withdrawn_at'),
        json_extract(payload, '$.Live.UnwrapSubmitted.withdrawn_at'),
        json_extract(payload, '$.Live.TokensUnwrapped.withdrawn_at'),
        json_extract(payload, '$.Live.SendPending.withdrawn_at'),
        json_extract(payload, '$.Live.TokensSent.sent_at'),
        json_extract(payload, '$.Live.Pending.sent_at'),
        json_extract(payload, '$.Live.Completed.started_at'),
        json_extract(payload, '$.Live.Failed.started_at'),
        json_extract(payload, '$.Live.Reconciled.started_at')
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
        json_extract(payload, '$.Live.Completed.completed_at'),
        json_extract(payload, '$.Live.Failed.failed_at'),
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

CREATE INDEX idx_equity_redemption_view_started_at
    ON equity_redemption_view (started_at DESC, view_id ASC)
    WHERE started_at IS NOT NULL;

CREATE INDEX idx_equity_redemption_view_terminal_at
    ON equity_redemption_view (terminal_at DESC, view_id ASC);

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
