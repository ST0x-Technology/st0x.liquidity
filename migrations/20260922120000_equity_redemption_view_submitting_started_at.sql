-- Rebuild equity_redemption_view.started_at to include the VaultWithdrawSubmitting
-- origin state. New redemptions now originate in `VaultWithdrawSubmitting`
-- (aggregate v7), but the started_at coalesce in
-- 20260904214951_transfer_history_projections.sql had no arm for its
-- `submitting_at`, so a redemption sat with started_at NULL -- and was therefore
-- hidden from the dashboard's `started_at IS NOT NULL` listing and pagination --
-- for the entire window before its withdrawal was broadcast, precisely when an
-- operator needs to see it. Generated columns cannot be altered in place, so the
-- table is dropped and recreated; StoreBuilder catches it back up from the
-- immutable event streams at startup, so this is safe.

DROP TABLE IF EXISTS equity_redemption_view;
CREATE TABLE equity_redemption_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    started_at_raw TEXT GENERATED ALWAYS AS (coalesce(
        json_extract(payload, '$.Live.VaultWithdrawSubmitting.submitting_at'),
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
