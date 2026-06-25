//! Redemption pipeline stage folding for the equity stage-timing read model.
//!
//! Holds [`StoredOperation::apply_redemption`] and [`redemption_observed_at`];
//! the shared read-model machinery, stored types, and stage helpers live in
//! the parent [`super`] module.

use chrono::{DateTime, Utc};

use st0x_finance::FractionalShares;

use super::{StoredOperation, StoredStageName, StoredStageOutcome, StoredStatus};
use crate::equity_redemption::EquityRedemptionEvent;

impl StoredOperation {
    /// Apply one redemption event to the accumulated state. Same idempotency
    /// contract as [`Self::apply_mint`].
    ///
    /// `sequence` is the event's stream position when known (only replay
    /// threads it; the live reactor always passes `None`, see call site).
    /// Used to distinguish a true legacy-genesis event (`sequence == Some(1)`,
    /// see `VaultWithdrawSubmitted`/`WithdrawnFromRaindex` below) from a
    /// genuinely mid-stream first observation, which must NOT be mistaken for
    /// one -- only the aggregate's actual first event can seed a known start.
    pub(super) fn apply_redemption(
        &mut self,
        event: &EquityRedemptionEvent,
        sequence: Option<i64>,
    ) {
        use EquityRedemptionEvent::*;
        use StoredStageName::*;

        match event {
            VaultWithdrawPending {
                symbol,
                quantity,
                pending_at,
                ..
            } => {
                self.symbol.get_or_insert_with(|| symbol.clone());
                self.quantity
                    .get_or_insert_with(|| FractionalShares::new(*quantity));
                // `VaultWithdrawPending` is always the genuine genesis event
                // (the aggregate's own `initialize()` command).
                self.started_at.get_or_insert(*pending_at);
                self.open_once(RedemptionWithdraw, *pending_at);
            }
            // Also carries symbol/quantity: hydrate them so an operation
            // first observed mid-stream (deploy/restart backfill) is not
            // left with `symbol: None, quantity: None` when the genesis
            // `VaultWithdrawPending` was never seen.
            VaultWithdrawSubmitted {
                symbol,
                quantity,
                submitted_at,
                ..
            } => {
                self.symbol.get_or_insert_with(|| symbol.clone());
                self.quantity
                    .get_or_insert_with(|| FractionalShares::new(*quantity));
                // Legacy genesis (see `EquityRedemption::originate`'s own
                // "old aggregates start with VaultWithdrawSubmitted" comment):
                // `submitted_at` IS the genuine start, not a mid-stream
                // observation, so seed both the operation start and the
                // withdraw stage's open run from it.
                if sequence == Some(1) {
                    self.started_at.get_or_insert(*submitted_at);
                    self.open_once(RedemptionWithdraw, *submitted_at);
                }
            }
            // `UnwrapSubmitted` can fire directly from `WithdrawnFromRaindex`
            // (see `EquityRedemption::evolve`'s `UnwrapSubmitted` arm, which
            // accepts both `WithdrawnFromRaindex` and `UnwrapPending` as
            // valid predecessors) -- a legacy/pre-split code path where
            // `UnwrapPending` never fires at all, so the genuine
            // `submitted_at -> unwrapped_at` interval would otherwise be lost
            // as Unmeasured. `open_once` no-ops when `UnwrapPending` already
            // opened the stage, so this is safe for the normal path too.
            UnwrapSubmitted { submitted_at, .. } => {
                self.open_once(RedemptionUnwrap, *submitted_at);
            }
            WithdrawnFromRaindex {
                symbol,
                quantity,
                withdrawn_at,
                ..
            } => {
                self.symbol.get_or_insert_with(|| symbol.clone());
                self.quantity
                    .get_or_insert_with(|| FractionalShares::new(*quantity));
                // Legacy genesis (see `EquityRedemption::originate`'s own
                // "old aggregates start with WithdrawnFromRaindex" comment):
                // `withdrawn_at` IS the genuine start (the withdraw already
                // happened by the time this stream begins), not a mid-stream
                // observation -- seed the operation start from it so
                // `total_ms` is measurable even though the withdraw stage
                // itself stays unmeasured below (no earlier event gives it a
                // duration).
                if sequence == Some(1) {
                    self.started_at.get_or_insert(*withdrawn_at);
                }
                // Mirrors USDC's `BridgingInitiated` handling (see
                // `rebalance::StoredOperation`'s fold): an operation first
                // observed here (mid-stream, `RedemptionWithdraw` never
                // opened) has no measurable withdraw duration, so record it
                // as unmeasured instead of silently dropping the stage.
                self.close_or_record_unmeasured(
                    RedemptionWithdraw,
                    *withdrawn_at,
                    StoredStageOutcome::Succeeded,
                );
            }
            UnwrapPending { pending_at } => {
                self.open_once(RedemptionUnwrap, *pending_at);
            }
            // Mid-stream-first case as above: no open `RedemptionUnwrap` run
            // when `WithdrawnFromRaindex`/`UnwrapPending` was never seen.
            TokensUnwrapped { unwrapped_at, .. } => {
                self.close_or_record_unmeasured(
                    RedemptionUnwrap,
                    *unwrapped_at,
                    StoredStageOutcome::Succeeded,
                );
            }
            SendPending { pending_at } => {
                self.open_once(RedemptionSend, *pending_at);
            }
            // Mid-stream-first case as above: `open_once` no-ops when
            // `SendPending` already opened the stage, so this only seeds the
            // send stage for an operation first observed at the submitted
            // event (deploy/restart backfill).
            SendSubmitted { submitted_at, .. } => {
                self.open_once(RedemptionSend, *submitted_at);
            }
            // Terminal failures: whichever stage is currently open (varies by
            // which of these fired) is closed as failed and the operation
            // ends. `TransferFailed` is emitted either by `SendTokens` (from
            // `SendPending`) or the operator `FailTransfer` command (from
            // `WithdrawnFromRaindex`/`TokensUnwrapped`/etc.); every source
            // state evolves to the terminal `Failed` state (see
            // `EquityRedemptionEvent::evolve`'s `TransferFailed` arm).
            // `DetectionFailed`, despite its doc comment's "keep inflight"
            // framing (which describes staying in the terminal `Failed`
            // state until an operator resolves it, not staying
            // non-terminal), also evolves straight to `Self::Failed`.
            TransferFailed { failed_at: at, .. }
            | DetectionFailed { failed_at: at, .. }
            | RedemptionRejected {
                rejected_at: at, ..
            } => {
                // `FailTransfer` (unlike `FailDetection`/`RejectRedemption`,
                // each valid from exactly one state with a stage already
                // open) is also valid from `WithdrawnFromRaindex` and
                // `TokensUnwrapped` -- states reached right after a stage
                // closes and before the next stage's start event arrives. In
                // those cases `close_open_stages` below has nothing open to
                // fail, and the operation would otherwise go `Failed` with no
                // failed stage in the breakdown. Derive and fail the stage
                // the pipeline stopped at instead.
                // Guarded on `self.status` too, not just `had_open_stage`:
                // once this arm has derived and failed a stage, every stage
                // has `ended_at: Some(_)`, so `had_open_stage` alone would be
                // `false` again on redelivery of the exact same event and
                // `next_missing_redemption_stage` would then derive the NEXT
                // stage in pipeline order (since the previously-derived one
                // now `has_run`), fabricating a second bogus Failed entry.
                // The operation is already terminal once `status` is
                // `Failed`, so redelivery must be a pure no-op here.
                let had_open_stage = self.stages.iter().any(|run| run.ended_at.is_none());
                self.close_open_stages(*at, StoredStageOutcome::Failed);
                if !had_open_stage && self.status != StoredStatus::Failed {
                    // `DetectionFailed`/`RedemptionRejected` are each valid
                    // from exactly one state (`TokensSent`/`Pending`
                    // respectively -- see `EquityRedemptionEvent::evolve`),
                    // so a mid-stream-first observation (no stage ever seen,
                    // `stages` empty) always means the pipeline reached
                    // `RedemptionDetection`/`RedemptionCompletion`. Deriving
                    // via `next_missing_redemption_stage` there would wrongly
                    // answer `RedemptionWithdraw` (the earliest stage with no
                    // run at all), since that helper only knows pipeline
                    // position, not which event fired. `TransferFailed` alone
                    // is valid from multiple states, so it still needs the
                    // derived lookup.
                    // `TransferFailed` alone (unlike `DetectionFailed`/
                    // `RedemptionRejected` above) is valid from either
                    // `WithdrawnFromRaindex` or `TokensUnwrapped`, and its
                    // payload carries nothing that says which. When it is
                    // ALSO the mid-stream-first event (`stages` empty, this
                    // branch), `next_missing_redemption_stage` always answers
                    // the earliest stage, `RedemptionWithdraw` -- a
                    // best-effort guess that is wrong whenever the true prior
                    // state was `TokensUnwrapped` (withdraw AND unwrap both
                    // actually succeeded). This is an accepted, inherent
                    // limitation: no event payload can disambiguate it, so it
                    // is deliberately not "fixed" by guessing further.
                    let derived_stage = match event {
                        DetectionFailed { .. } => Some(RedemptionDetection),
                        RedemptionRejected { .. } => Some(RedemptionCompletion),
                        _ => self.next_missing_redemption_stage(),
                    };
                    if let Some(stage) = derived_stage {
                        self.fail_unopened_stage(stage, *at);
                    }
                }
                self.status = StoredStatus::Failed;
            }
            // Mid-stream-first case as above: no open `RedemptionSend` run
            // when `SendPending` was never seen.
            TokensSent { sent_at, .. } => {
                self.close_or_record_unmeasured(
                    RedemptionSend,
                    *sent_at,
                    StoredStageOutcome::Succeeded,
                );
                self.open_once(RedemptionDetection, *sent_at);
            }
            // Mid-stream-first case as above: no open `RedemptionDetection`
            // run when `TokensSent` was never seen.
            Detected { detected_at, .. } => {
                self.close_or_record_unmeasured(
                    RedemptionDetection,
                    *detected_at,
                    StoredStageOutcome::Succeeded,
                );
                self.open_once(RedemptionCompletion, *detected_at);
            }
            // Mid-stream-first case as above: no open `RedemptionCompletion`
            // run when `Detected` was never seen.
            Completed { completed_at } => {
                self.close_or_record_unmeasured(
                    RedemptionCompletion,
                    *completed_at,
                    StoredStageOutcome::Succeeded,
                );
                self.status = StoredStatus::Completed;
                self.completed_at = Some(*completed_at);
            }
            // `RecoverProviderCompletion` is valid from `Failed{redemption_tx:
            // Some(_), ..}`, reached via two distinct prior failures: (1)
            // `DetectionFailed` from `TokensSent`, which closes the
            // already-open `RedemptionDetection` stage `Failed` (via
            // `close_open_stages` above) while `RedemptionCompletion` was
            // never opened; or (2) `RedemptionRejected` from `Pending` (i.e.
            // *after* `Detected` already opened `RedemptionCompletion`), which
            // closes `RedemptionCompletion` itself `Failed`. Either way,
            // recovery proves the operation actually completed, so flip
            // whichever stage a prior failure left `Failed` back to
            // `Unmeasured` first -- mirroring `recover_receipt_stage`'s
            // already-closed-run handling -- otherwise that stage would stay
            // permanently `Failed` on a `Completed` operation.
            ProviderCompletionRecovered { recovered_at, .. } => {
                self.close_open_stages(*recovered_at, StoredStageOutcome::Succeeded);
                for run in self
                    .stages
                    .iter_mut()
                    .filter(|run| run.outcome == StoredStageOutcome::Failed)
                {
                    run.outcome = StoredStageOutcome::Unmeasured;
                    run.duration_ms = None;
                }
                self.record_unmeasured(RedemptionCompletion, *recovered_at);
                self.status = StoredStatus::Completed;
                self.completed_at = Some(*recovered_at);
            }
            OperatorReconciled { reconciled_at, .. } => {
                // Reconcile is valid only from the terminal `Failed` state
                // (see `EquityRedemptionCommand::Reconcile`), so the failed
                // stage is already closed -- no stage manipulation needed
                // here, matching USDC's `OperatorReconciled` handling
                // exactly.
                self.status = StoredStatus::Completed;
                self.completed_at = Some(*reconciled_at);
                self.operator_reconciled = true;
            }
        }
    }

    /// The earliest redemption stage (in pipeline order) that has no run yet,
    /// used by `apply_redemption`'s `TransferFailed` arm to derive where the
    /// pipeline stopped when the failure arrives with no stage open.
    fn next_missing_redemption_stage(&self) -> Option<StoredStageName> {
        use StoredStageName::*;

        [
            RedemptionWithdraw,
            RedemptionUnwrap,
            RedemptionSend,
            RedemptionDetection,
            RedemptionCompletion,
        ]
        .into_iter()
        .find(|stage| !self.has_run(*stage))
    }
}

/// Redemption counterpart to [`super::mint::mint_observed_at`]; same rationale.
pub(super) fn redemption_observed_at(event: &EquityRedemptionEvent) -> DateTime<Utc> {
    use EquityRedemptionEvent::*;

    // Same combined-arm rationale as `mint_observed_at`.
    match event {
        VaultWithdrawPending { pending_at: at, .. }
        | VaultWithdrawSubmitted {
            submitted_at: at, ..
        }
        | WithdrawnFromRaindex {
            withdrawn_at: at, ..
        }
        | UnwrapPending { pending_at: at }
        | UnwrapSubmitted {
            submitted_at: at, ..
        }
        | TokensUnwrapped {
            unwrapped_at: at, ..
        }
        | SendPending { pending_at: at }
        | SendSubmitted {
            submitted_at: at, ..
        }
        | TransferFailed { failed_at: at, .. }
        | TokensSent { sent_at: at, .. }
        | DetectionFailed { failed_at: at, .. }
        | Detected {
            detected_at: at, ..
        }
        | RedemptionRejected {
            rejected_at: at, ..
        }
        | Completed { completed_at: at }
        | ProviderCompletionRecovered {
            recovered_at: at, ..
        }
        | OperatorReconciled {
            reconciled_at: at, ..
        } => *at,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, U256};
    use uuid::Uuid;

    use st0x_dto::{EquityOperationKind, EquityStageName, RebalanceTimingStatus, StageOutcome};
    use st0x_float_macro::float;
    use st0x_tokenization::TokenizationRequestId;

    use super::super::StoredKind;
    use super::super::tests::{
        fold_redemption, fold_redemption_from_genesis, redemption_happy_path, stage, symbol,
        timestamp,
    };
    use super::*;
    use crate::equity_redemption::DetectionFailure;

    #[test]
    fn redemption_happy_path_completes_all_five_stages() {
        let operation = fold_redemption(&redemption_happy_path());

        assert_eq!(operation.kind, EquityOperationKind::Redeem);
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.started_at, Some(timestamp(0)));
        assert_eq!(operation.completed_at, Some(timestamp(100)));
        assert_eq!(operation.total_ms, Some(100_000));

        for name in [
            EquityStageName::RedemptionWithdraw,
            EquityStageName::RedemptionUnwrap,
            EquityStageName::RedemptionSend,
            EquityStageName::RedemptionDetection,
            EquityStageName::RedemptionCompletion,
        ] {
            assert_eq!(
                stage(&operation, name).outcome,
                StageOutcome::Succeeded,
                "{name:?} did not succeed"
            );
        }
    }

    #[test]
    fn redemption_first_observed_at_withdrawn_from_raindex_hydrates_symbol_and_quantity() {
        // Deploy/restart backfill case: the read model first observes this
        // operation at `WithdrawnFromRaindex` because the genesis
        // `VaultWithdrawPending` predates the projection's `started_at`
        // window (or was otherwise never seen). `WithdrawnFromRaindex` still
        // carries `symbol`/`quantity`, so those fields -- and an Unmeasured
        // `RedemptionWithdraw` stage recording that the withdraw demonstrably
        // happened -- must be populated rather than left `None`/missing.
        let events = redemption_happy_path()[2..].to_vec();
        let operation = fold_redemption(&events);

        assert_eq!(operation.symbol, Some(symbol()));
        assert_eq!(operation.quantity, Some(FractionalShares::new(float!(5))));
        let withdraw = stage(&operation, EquityStageName::RedemptionWithdraw);
        assert_eq!(withdraw.outcome, StageOutcome::Unmeasured);
        assert_eq!(withdraw.duration_ms, None);
    }

    #[test]
    fn redemption_legacy_genesis_at_withdrawn_from_raindex_seeds_known_start() {
        // Distinct from the mid-stream-first case above: `EquityRedemption`
        // supports OLD aggregates that genuinely start their event stream at
        // `WithdrawnFromRaindex` (see its `originate` doc comment), so
        // `withdrawn_at` IS the real start, not an unknown one -- `started_at`
        // and `total_ms` must be populated, not left `None`.
        let events = redemption_happy_path()[2..].to_vec();
        let operation = fold_redemption_from_genesis(&events);

        assert_eq!(operation.started_at, Some(timestamp(20)));
        assert_eq!(operation.completed_at, Some(timestamp(100)));
        assert_eq!(operation.total_ms, Some(80_000));
        // The withdraw stage's own duration is still unmeasurable (no earlier
        // event gives it a start distinct from `withdrawn_at`), only the
        // operation-level start becomes known.
        let withdraw = stage(&operation, EquityStageName::RedemptionWithdraw);
        assert_eq!(withdraw.outcome, StageOutcome::Unmeasured);
        assert_eq!(withdraw.duration_ms, None);
    }

    #[test]
    fn redemption_legacy_genesis_at_vault_withdraw_submitted_seeds_known_start_and_stage() {
        // Same legacy support, one event earlier: `EquityRedemption` also
        // supports old aggregates starting at `VaultWithdrawSubmitted` (see
        // its `originate` doc comment). Unlike `WithdrawnFromRaindex`
        // genesis, `RedemptionWithdraw`'s full duration IS measurable here
        // (`submitted_at` -> the later `WithdrawnFromRaindex`).
        let events = redemption_happy_path()[1..].to_vec();
        let operation = fold_redemption_from_genesis(&events);

        assert_eq!(operation.started_at, Some(timestamp(5)));
        assert_eq!(operation.completed_at, Some(timestamp(100)));
        assert_eq!(operation.total_ms, Some(95_000));
        let withdraw = stage(&operation, EquityStageName::RedemptionWithdraw);
        assert_eq!(withdraw.outcome, StageOutcome::Succeeded);
        assert_eq!(withdraw.duration_ms, Some(15_000));
    }

    #[test]
    fn redemption_first_observed_at_tokens_unwrapped_records_unwrap_unmeasured() {
        // Same mid-stream-first case, one stage later: `WithdrawnFromRaindex`
        // was never seen either.
        let events = redemption_happy_path()[5..].to_vec();
        let operation = fold_redemption(&events);

        let unwrap = stage(&operation, EquityStageName::RedemptionUnwrap);
        assert_eq!(unwrap.outcome, StageOutcome::Unmeasured);
        assert_eq!(unwrap.duration_ms, None);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionSend).outcome,
            StageOutcome::Succeeded
        );
    }

    #[test]
    fn redemption_unwrap_submitted_without_unwrap_pending_still_measures_unwrap_duration() {
        // Legacy/pre-split path: `EquityRedemption::evolve`'s `UnwrapSubmitted`
        // arm accepts `WithdrawnFromRaindex` as a valid predecessor (not only
        // `UnwrapPending`), so some real streams never emit `UnwrapPending` at
        // all. Before this fix `UnwrapSubmitted` was a pure no-op marker, so
        // `RedemptionUnwrap` never opened and `TokensUnwrapped` recorded it as
        // Unmeasured even though `submitted_at -> unwrapped_at` is a genuine,
        // measurable interval.
        let events = vec![
            EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: symbol(),
                quantity: float!(5),
                token: Address::repeat_byte(0x22),
                wrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                actual_wrapped_amount: Some(U256::from(5_000_000_000_000_000_000_u128)),
                raindex_withdraw_tx: TxHash::random(),
                raindex_withdraw_block: Some(1),
                withdrawn_at: timestamp(20),
            },
            EquityRedemptionEvent::UnwrapSubmitted {
                unwrap_tx_hash: TxHash::random(),
                submitted_at: timestamp(25),
            },
            EquityRedemptionEvent::TokensUnwrapped {
                quantity: Some(float!(5)),
                underlying_token: Address::repeat_byte(0x33),
                unwrap_tx_hash: TxHash::random(),
                unwrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                unwrap_block: Some(2),
                unwrapped_at: timestamp(45),
            },
        ];
        let operation = fold_redemption(&events);

        let unwrap = stage(&operation, EquityStageName::RedemptionUnwrap);
        assert_eq!(unwrap.outcome, StageOutcome::Succeeded);
        assert_eq!(unwrap.duration_ms, Some(20_000));
    }

    #[test]
    fn catch_up_upgrades_unmeasured_placeholder_to_measured_duration() {
        // Simulates the gap `open_once`'s placeholder-upgrade fix targets:
        // the live reactor missed folding `UnwrapPending` (the event-sorcery
        // reactor-bridge logs and continues past a single reactor error), so
        // `TokensUnwrapped` arrives with no open `RedemptionUnwrap` run and
        // is recorded as an Unmeasured placeholder. A later restart's
        // `catch_up` replays the ENTIRE stream, so the real `UnwrapPending`
        // now arrives too, after the placeholder already exists. `open_once`
        // must recognize the placeholder as recoverable and backfill the
        // real duration instead of leaving the stage stuck Unmeasured
        // forever (once catch-up advances the checkpoint, this is the only
        // chance to repair it).
        let events = vec![
            EquityRedemptionEvent::TokensUnwrapped {
                quantity: Some(float!(5)),
                underlying_token: Address::repeat_byte(0x33),
                unwrap_tx_hash: TxHash::random(),
                unwrapped_amount: U256::from(5_000_000_000_000_000_000_u128),
                unwrap_block: Some(2),
                unwrapped_at: timestamp(45),
            },
            EquityRedemptionEvent::UnwrapPending {
                pending_at: timestamp(25),
            },
        ];
        let operation = fold_redemption(&events);

        let unwrap = stage(&operation, EquityStageName::RedemptionUnwrap);
        assert_eq!(unwrap.outcome, StageOutcome::Succeeded);
        assert_eq!(unwrap.started_at, timestamp(25));
        assert_eq!(unwrap.duration_ms, Some(20_000));
    }

    #[test]
    fn redemption_first_observed_at_tokens_sent_records_send_unmeasured() {
        let events = redemption_happy_path()[7..].to_vec();
        let operation = fold_redemption(&events);

        let send = stage(&operation, EquityStageName::RedemptionSend);
        assert_eq!(send.outcome, StageOutcome::Unmeasured);
        assert_eq!(send.duration_ms, None);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionDetection).outcome,
            StageOutcome::Succeeded
        );
    }

    #[test]
    fn redemption_first_observed_at_detected_records_detection_unmeasured() {
        let events = redemption_happy_path()[8..].to_vec();
        let operation = fold_redemption(&events);

        let detection = stage(&operation, EquityStageName::RedemptionDetection);
        assert_eq!(detection.outcome, StageOutcome::Unmeasured);
        assert_eq!(detection.duration_ms, None);
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
    }

    #[test]
    fn redemption_transfer_failed_closes_send_as_failed() {
        let mut events = redemption_happy_path()[..7].to_vec();
        events.push(EquityRedemptionEvent::TransferFailed {
            tx_hash: None,
            reason: None,
            failed_at: timestamp(55),
        });
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionSend).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn redemption_transfer_failed_first_observed_defaults_to_withdraw_stage() {
        // Regression-lock, not a behavior fix: `TransferFailed` is valid from
        // either `WithdrawnFromRaindex` or `TokensUnwrapped` (see the shared
        // failure arm's doc), so when it is the mid-stream-first event (no
        // stage ever observed, `self.stages` empty) the bare event payload
        // cannot say which of the two the pipeline actually reached --
        // `next_missing_redemption_stage` always answers the earliest stage,
        // `RedemptionWithdraw`, even though the true prior state may have
        // been `TokensUnwrapped` (withdraw AND unwrap both actually
        // succeeded). This is a known, accepted limitation documented at the
        // call site; this test locks in the CURRENT best-effort behavior so
        // a future change to it is deliberate, not an accidental regression.
        let events = vec![EquityRedemptionEvent::TransferFailed {
            tx_hash: None,
            reason: None,
            failed_at: timestamp(55),
        }];
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionWithdraw).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn redemption_transfer_failed_after_withdraw_derives_failed_unwrap_stage() {
        // `FailTransfer` is valid from `WithdrawnFromRaindex`, reached right
        // after `RedemptionWithdraw` closes and before `RedemptionUnwrap`
        // ever opens -- `close_open_stages` alone has nothing open to fail
        // here, so without the derived-stage fix the operation would go
        // `Failed` with every recorded stage showing `Succeeded`.
        let mut events = redemption_happy_path()[..3].to_vec();
        events.push(EquityRedemptionEvent::TransferFailed {
            tx_hash: None,
            reason: None,
            failed_at: timestamp(22),
        });
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionWithdraw).outcome,
            StageOutcome::Succeeded
        );
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionUnwrap).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn redelivering_transfer_failed_after_withdraw_does_not_fabricate_extra_failed_stages() {
        // Regression test: `catch_up` refolds an operation's entire event
        // stream from scratch on top of the already-fully-folded stored row
        // on every server restart (see `rebalance.rs`'s
        // `redelivering_full_event_sequence_is_idempotent`, the precedent
        // this mirrors). Reapplying the exact same gap-triggering sequence
        // must not derive a second bogus Failed stage on top of the one the
        // first application already derived.
        let mut events = redemption_happy_path()[..3].to_vec();
        events.push(EquityRedemptionEvent::TransferFailed {
            tx_hash: None,
            reason: None,
            failed_at: timestamp(22),
        });

        let mut operation = StoredOperation::new(
            Uuid::new_v4(),
            StoredKind::Redeem,
            redemption_observed_at(&events[0]),
        );
        for event in &events {
            operation.apply_redemption(event, None);
        }
        let once = operation.clone().into_dto();

        for event in &events {
            operation.apply_redemption(event, None);
        }
        let twice = operation.into_dto();

        assert_eq!(once.stages.len(), twice.stages.len());
        for (single, redelivered) in once.stages.iter().zip(twice.stages.iter()) {
            assert_eq!(single.stage, redelivered.stage);
            assert_eq!(single.outcome, redelivered.outcome);
        }
    }

    #[test]
    fn redemption_transfer_failed_after_unwrap_derives_failed_send_stage() {
        // Same gap as `WithdrawnFromRaindex`, one stage later:
        // `RedemptionUnwrap` has already closed and `RedemptionSend` never
        // opened.
        let mut events = redemption_happy_path()[..6].to_vec();
        events.push(EquityRedemptionEvent::TransferFailed {
            tx_hash: None,
            reason: None,
            failed_at: timestamp(47),
        });
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionUnwrap).outcome,
            StageOutcome::Succeeded
        );
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionSend).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn redemption_detection_failed_closes_detection_as_failed() {
        let mut events = redemption_happy_path()[..8].to_vec();
        events.push(EquityRedemptionEvent::DetectionFailed {
            failure: DetectionFailure::Timeout,
            failed_at: timestamp(80),
        });
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionDetection).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn redemption_detection_failed_first_observed_derives_detection_not_withdraw() {
        // Mid-stream-first observation (see `redemption_first_observed_at_*`
        // above): the projection's stream begins directly at
        // `DetectionFailed`, with no earlier stage ever recorded. Because
        // `DetectionFailed` is only ever valid from `TokensSent` (see
        // `EquityRedemptionCommand::FailDetection`), the derived-failed stage
        // must be `RedemptionDetection` -- not `next_missing_redemption_stage`'s
        // pipeline-position answer of `RedemptionWithdraw`, which would be
        // wrong here since no stage has run at all yet.
        let events = vec![EquityRedemptionEvent::DetectionFailed {
            failure: DetectionFailure::Timeout,
            failed_at: timestamp(80),
        }];
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionDetection).outcome,
            StageOutcome::Failed
        );
        assert!(
            operation
                .stages
                .iter()
                .all(|entry| entry.stage != EquityStageName::RedemptionWithdraw),
            "must not fabricate a Failed RedemptionWithdraw stage that never ran"
        );
    }

    #[test]
    fn redemption_rejected_closes_open_stage_as_failed() {
        let mut events = redemption_happy_path()[..2].to_vec();
        events.push(EquityRedemptionEvent::RedemptionRejected {
            reason: "rejected".to_string(),
            rejected_at: timestamp(10),
        });
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionWithdraw).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn redemption_provider_completion_recovered_marks_completion_unmeasured() {
        let mut events = redemption_happy_path()[..8].to_vec();
        events.push(EquityRedemptionEvent::DetectionFailed {
            failure: DetectionFailure::Timeout,
            failed_at: timestamp(80),
        });
        events.push(EquityRedemptionEvent::ProviderCompletionRecovered {
            tokenization_request_id: TokenizationRequestId::try_new("tok-2").unwrap(),
            recovered_at: timestamp(400),
        });
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.completed_at, Some(timestamp(400)));
        let completion = stage(&operation, EquityStageName::RedemptionCompletion);
        assert_eq!(completion.outcome, StageOutcome::Unmeasured);
        assert_eq!(completion.duration_ms, None);
        // `RedemptionDetection` was closed `Failed` by `DetectionFailed`
        // before `RedemptionCompletion` ever opened; recovery must flip it
        // back to `Unmeasured` too, not leave it permanently `Failed` on a
        // `Completed` operation.
        let detection = stage(&operation, EquityStageName::RedemptionDetection);
        assert_eq!(detection.outcome, StageOutcome::Unmeasured);
        assert_eq!(detection.duration_ms, None);
    }

    #[test]
    fn redemption_provider_completion_recovered_after_rejection_marks_completion_unmeasured() {
        // Unlike the `DetectionFailed` case above, `RedemptionRejected` fires
        // from `Pending` -- reached *after* `Detected` already opened
        // `RedemptionCompletion` -- so the shared failure arm closes
        // `RedemptionCompletion` itself `Failed` before recovery fires.
        // Recovery must flip that stale `Failed` outcome to `Unmeasured`
        // rather than leaving a `Completed` operation with a permanently
        // `Failed` `RedemptionCompletion` stage.
        let mut events = redemption_happy_path()[..9].to_vec();
        events.push(EquityRedemptionEvent::RedemptionRejected {
            reason: "rejected by operator".to_string(),
            rejected_at: timestamp(95),
        });
        events.push(EquityRedemptionEvent::ProviderCompletionRecovered {
            tokenization_request_id: TokenizationRequestId::try_new("tok-2").unwrap(),
            recovered_at: timestamp(400),
        });
        let operation = fold_redemption(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.completed_at, Some(timestamp(400)));
        let completion = stage(&operation, EquityStageName::RedemptionCompletion);
        assert_eq!(completion.outcome, StageOutcome::Unmeasured);
        assert_eq!(completion.duration_ms, None);
        assert_eq!(
            stage(&operation, EquityStageName::RedemptionDetection).outcome,
            StageOutcome::Succeeded,
            "detection succeeded before rejection; recovery must not touch it"
        );
    }

    #[test]
    fn redelivering_after_same_timestamp_open_and_fail_does_not_wrongly_upgrade_scrubbed_stage() {
        // Regression test for a shape-vs-marker bug: `DetectionFailed` fires
        // at the EXACT same timestamp as the `TokensSent` event that opened
        // `RedemptionDetection` (a stage's open and its terminal-failure
        // close can genuinely carry identical timestamps), so the closed
        // `RedemptionDetection` run ends up with `started_at == ended_at`.
        // `ProviderCompletionRecovered` then scrubs it from `Failed` back to
        // plain `Unmeasured` (NOT `Placeholder`) without touching
        // `started_at`/`ended_at`, leaving it with the EXACT same shape a
        // real upgradeable placeholder has. On a full-stream redelivery
        // (`catch_up` re-folding the whole stream, simulated here by
        // re-applying every event), the original `TokensSent` event re-fires
        // and calls `open_once(RedemptionDetection, ..)`. Before this fix,
        // `upgrade_placeholder_start` matched by shape alone and would
        // wrongly flip this scrubbed run to `Succeeded` with a fabricated
        // `duration_ms: Some(0)`, diverging from the single-pass fold (which
        // leaves it `Unmeasured`) and breaking redelivery idempotency.
        //
        // Unlike an equivalent mint-side scenario, `RedemptionCompletion`'s
        // recovery path (`record_unmeasured`, a fresh stage push) never
        // touches `RedemptionDetection` again, so nothing here masks the bug
        // the way mint's `recover_receipt_stage` (which unconditionally
        // re-flips its target stage to `Unmeasured` on every replay) would.
        let events = vec![
            EquityRedemptionEvent::TokensSent {
                redemption_wallet: Address::repeat_byte(0x44),
                redemption_tx: TxHash::random(),
                sent_at: timestamp(60),
            },
            EquityRedemptionEvent::DetectionFailed {
                failure: DetectionFailure::Timeout,
                failed_at: timestamp(60),
            },
            EquityRedemptionEvent::ProviderCompletionRecovered {
                tokenization_request_id: TokenizationRequestId::try_new("tok-2").unwrap(),
                recovered_at: timestamp(400),
            },
        ];

        let mut operation = StoredOperation::new(
            Uuid::new_v4(),
            StoredKind::Redeem,
            redemption_observed_at(&events[0]),
        );
        for event in &events {
            operation.apply_redemption(event, None);
        }
        // Simulates the restart: `catch_up` re-folds the whole stream onto
        // the already-live-folded state above.
        for event in &events {
            operation.apply_redemption(event, None);
        }
        let redelivered = operation.into_dto();

        let detection = stage(&redelivered, EquityStageName::RedemptionDetection);
        assert_eq!(detection.outcome, StageOutcome::Unmeasured);
        assert_eq!(detection.started_at, timestamp(60));
        assert_eq!(detection.duration_ms, None);
    }
}
