//! Mint pipeline stage folding for the equity stage-timing read model.
//!
//! Holds [`StoredOperation::apply_mint`] and [`mint_observed_at`]; the shared
//! read-model machinery, stored types, and stage helpers live in the parent
//! [`super`] module.

use chrono::{DateTime, Utc};

use st0x_finance::FractionalShares;

use super::{StoredOperation, StoredStageName, StoredStageOutcome, StoredStatus};
use crate::tokenized_equity_mint::TokenizedEquityMintEvent;

impl StoredOperation {
    /// Apply one mint event to the accumulated state.
    ///
    /// Idempotent under redelivery: every stage open goes through
    /// [`Self::open_once`] and every close/record-unmeasured arm is a no-op
    /// when the target run is already in its terminal shape, so replaying a
    /// fully-applied event sequence yields the same stage list and
    /// durations.
    pub(super) fn apply_mint(&mut self, event: &TokenizedEquityMintEvent) {
        use StoredStageName::*;
        use TokenizedEquityMintEvent::*;

        match event {
            MintRequested {
                symbol,
                quantity,
                requested_at,
                ..
            } => {
                self.symbol.get_or_insert_with(|| symbol.clone());
                self.quantity
                    .get_or_insert_with(|| FractionalShares::new(*quantity));
                // `MintRequested` is always the genuine genesis event -- it
                // is the aggregate's own `initialize()` command, so unlike
                // USDC's direction-gated seeding, no gating is needed here.
                self.started_at.get_or_insert(*requested_at);
                self.open_once(MintAcceptance, *requested_at);
            }
            // A mid-stream-first operation (genesis `MintRequested` never
            // seen) has no open `MintAcceptance` run, so record it as
            // unmeasured rather than silently dropping it.
            MintAccepted { accepted_at, .. } => {
                self.close_or_record_unmeasured(
                    MintAcceptance,
                    *accepted_at,
                    StoredStageOutcome::Succeeded,
                );
                self.open_once(MintReceipt, *accepted_at);
            }
            // Terminal failures: whichever stage is currently open (varies by
            // which of these fired -- pre-acceptance or vault-deposit) is
            // closed as failed and the operation ends. If this is the
            // operation's mid-stream-first event (deploy/restart backfill,
            // `self.stages` empty), `close_open_stages` has nothing to close,
            // so derive and fail the stage the event implies instead --
            // otherwise the operation would go `Failed` with zero stage
            // entries. `MintRejected` fires only from `MintRequested` (see
            // `TokenizedEquityMintCommand::RequestMint`'s immediate-rejection
            // doc), and `RaindexDepositFailed`'s two source states
            // (`TokensWrapped`/`VaultDepositSubmitted`) both map to the SAME
            // stage (`MintDeposit`, see its own `evolve` arm), so the
            // ambiguity there is harmless and the derivation below is a fixed
            // mapping, not a pipeline-position guess. `MintAcceptanceFailed`
            // is NOT a fixed mapping -- see its own arm below.
            MintRejected {
                rejected_at: at, ..
            }
            | RaindexDepositFailed { failed_at: at, .. } => {
                let had_open_stage = self.stages.iter().any(|run| run.ended_at.is_none());
                self.close_open_stages(*at, StoredStageOutcome::Failed);
                if !had_open_stage && self.status != StoredStatus::Failed {
                    let derived_stage = match event {
                        MintRejected { .. } => Some(MintAcceptance),
                        RaindexDepositFailed { .. } => Some(MintDeposit),
                        _ => None,
                    };
                    if let Some(stage) = derived_stage {
                        self.fail_unopened_stage(stage, *at);
                    }
                }
                self.status = StoredStatus::Failed;
            }
            // `MintAcceptanceFailed` fires from EITHER `MintRequested`
            // (pre-acceptance operator force-fail: `MintAcceptance` itself
            // failed, still open) OR `MintAccepted` (post-acceptance:
            // `MintAcceptance` already succeeded and `MintReceipt` -- never
            // opened, since `TokensReceived` never fired -- is what failed).
            // See `TokenizedEquityMintCommand::FailAcceptance`'s doc. Unlike
            // the fixed mapping above, these two source states map to TWO
            // DIFFERENT stages, so when this is the mid-stream-first event
            // (`self.stages` empty, no open stage to close), the bare event
            // payload cannot say which of the two actually happened -- the
            // same fundamental ambiguity as redemption's `TransferFailed`
            // (see `next_missing_redemption_stage`'s doc, an accepted, known
            // limitation). Default to `MintAcceptance`, the earlier of the
            // two, as a best-effort guess rather than silently dropping the
            // stage.
            MintAcceptanceFailed { failed_at: at, .. } => {
                let had_open_stage = self.stages.iter().any(|run| run.ended_at.is_none());
                self.close_open_stages(*at, StoredStageOutcome::Failed);
                if !had_open_stage && self.status != StoredStatus::Failed {
                    self.fail_unopened_stage(MintAcceptance, *at);
                }
                self.status = StoredStatus::Failed;
            }
            // Also carries symbol/quantity, unlike the other terminal
            // failures above: hydrate them so an operation first observed
            // here (mid-stream) is not left with `symbol: None,
            // quantity: None`, mirroring redemption's `WithdrawnFromRaindex`
            // handling. Same mid-stream-first derivation as above: fires only
            // from `TokensReceived`, so it always implies `MintWrap`.
            WrappingFailed {
                symbol,
                quantity,
                failed_at,
                ..
            } => {
                self.symbol.get_or_insert_with(|| symbol.clone());
                self.quantity
                    .get_or_insert_with(|| FractionalShares::new(*quantity));
                let had_open_stage = self.stages.iter().any(|run| run.ended_at.is_none());
                self.close_open_stages(*failed_at, StoredStageOutcome::Failed);
                if !had_open_stage && self.status != StoredStatus::Failed {
                    self.fail_unopened_stage(MintWrap, *failed_at);
                }
                self.status = StoredStatus::Failed;
            }
            // Mid-stream-first case as above: no open `MintReceipt` run when
            // `MintAccepted` was never seen.
            TokensReceived { received_at, .. } => {
                self.close_or_record_unmeasured(
                    MintReceipt,
                    *received_at,
                    StoredStageOutcome::Succeeded,
                );
                self.open_once(MintWrap, *received_at);
            }
            // Pure markers within their already-open stage.
            WrapSubmitted { .. } | VaultDepositSubmitted { .. } => {}
            // Mid-stream-first case as above: no open `MintWrap` run when
            // `TokensReceived` was never seen.
            TokensWrapped { wrapped_at, .. } => {
                self.close_or_record_unmeasured(
                    MintWrap,
                    *wrapped_at,
                    StoredStageOutcome::Succeeded,
                );
                self.open_once(MintDeposit, *wrapped_at);
            }
            // Mid-stream-first case as above: no open `MintDeposit` run when
            // `TokensWrapped` was never seen.
            DepositedIntoRaindex { deposited_at, .. } => {
                self.close_or_record_unmeasured(
                    MintDeposit,
                    *deposited_at,
                    StoredStageOutcome::Succeeded,
                );
                self.status = StoredStatus::Completed;
                self.completed_at = Some(*deposited_at);
            }
            ProviderCompletionRecovered { recovered_at, .. } => {
                // Mirrors USDC's `BridgingCompletionRecovered` (see
                // `rebalance::StoredOperation`'s fold): this recovers a mint
                // that failed at acceptance (tokens never received --
                // `recover_mint` refuses to dispatch this event otherwise), so
                // the aggregate lands in `TokensReceived`, the SAME
                // non-terminal state the normal `TokensReceived` event
                // produces -- not the terminal `DepositedIntoRaindex`. Real
                // wrap/deposit work still follows via `resume_mint`, so the
                // operation stays `InProgress` (no `completed_at`) and only
                // token receipt (not the deposit) is what was recovered. Adds
                // an Unmeasured `MintReceipt` run (its true duration is
                // unknown, discovered after the fact) and opens `MintWrap` so
                // the subsequent genuine `TokensWrapped` event has a run to
                // close -- matching what the normal `TokensReceived` arm
                // does. Recovery is also valid when the failure was
                // POST-acceptance (`MintAcceptanceFailed`), where
                // `MintReceipt` already closed `Failed` via that arm's own
                // `close_open_stages` call. But `MintAcceptanceFailed` can
                // ALSO fire PRE-acceptance (operator force-fail straight from
                // `MintRequested`), and `MintRejected` always fires
                // pre-acceptance -- both close `MintAcceptance` (not
                // `MintReceipt`) `Failed` instead. Scrub EVERY Failed-outcome
                // run back to Unmeasured (mirroring the redemption arm's
                // identical loop below) so no prior-failure shape leaves a
                // stage permanently `Failed` on a mint that later completes.
                //
                // Deliberately no `close_open_stages` call here (unlike the
                // terminal-failure arms above): by the time this event fires,
                // `MintRejected`/`MintAcceptanceFailed` has already closed
                // whatever was open via its own `close_open_stages`, so there
                // is nothing legitimately left open to close on a fresh fold.
                // On full-stream redelivery (a restart's `catch_up` re-folding
                // this event onto already-live-folded state), calling it here
                // would instead force-close the `MintWrap` run this exact
                // event opened during its OWN prior application (same
                // `started_at == recovered_at`), fabricating a spurious
                // `duration_ms: Some(0)` success and permanently losing the
                // real wrap timing (`open_once`/`record_unmeasured` below both
                // no-op once a run already exists).
                for run in self
                    .stages
                    .iter_mut()
                    .filter(|run| run.outcome == StoredStageOutcome::Failed)
                {
                    run.outcome = StoredStageOutcome::Unmeasured;
                    run.duration_ms = None;
                }
                // Still needed for the pre-acceptance `MintRejected` case:
                // `MintReceipt` never opened at all there, so the loop above
                // has nothing to scrub for it -- `recover_receipt_stage`'s
                // `None` branch adds a fresh Unmeasured entry instead. In the
                // POST-acceptance case the loop above already flipped
                // `MintReceipt` to Unmeasured, so this call is a harmless
                // no-op re-set of the same outcome.
                self.recover_receipt_stage(MintReceipt, *recovered_at);
                self.open_once(MintWrap, *recovered_at);
                self.status = StoredStatus::InProgress;
            }
            OperatorReconciled { reconciled_at, .. } => {
                // Reconcile is valid only from the terminal `Failed` state
                // (see `TokenizedEquityMintCommand::Reconcile`), so the
                // failed stage is already closed -- no stage manipulation
                // needed here, matching USDC's `OperatorReconciled` handling
                // exactly.
                self.status = StoredStatus::Completed;
                self.completed_at = Some(*reconciled_at);
                self.operator_reconciled = true;
            }
        }
    }
}

/// The timestamp at which the read model observed `event`, used to anchor a
/// freshly-seen mint operation's `first_seen_at` (the SQL window/sort key).
///
/// For `OperatorReconciled` this is `reconciled_at` -- when the reconciliation
/// actually happened -- NOT the original `requested_at` (which may be weeks
/// old). A recently-reconciled old operation must fall inside recent query
/// windows; anchoring it to the stale `requested_at` would hide exactly the
/// out-of-band recoveries operators want to see after a restart.
pub(super) fn mint_observed_at(event: &TokenizedEquityMintEvent) -> DateTime<Utc> {
    use TokenizedEquityMintEvent::*;

    // Every variant carries exactly one meaningful timestamp; combined into
    // one arm (rebinding each variant's differently-named field to `at`)
    // rather than 13 separate `Variant { field, .. } => *field` arms, whose
    // bodies are identical once the binding name is normalized.
    match event {
        MintRequested {
            requested_at: at, ..
        }
        | MintRejected {
            rejected_at: at, ..
        }
        | MintAccepted {
            accepted_at: at, ..
        }
        | MintAcceptanceFailed { failed_at: at, .. }
        | TokensReceived {
            received_at: at, ..
        }
        | WrapSubmitted {
            submitted_at: at, ..
        }
        | TokensWrapped { wrapped_at: at, .. }
        | WrappingFailed { failed_at: at, .. }
        | VaultDepositSubmitted {
            submitted_at: at, ..
        }
        | DepositedIntoRaindex {
            deposited_at: at, ..
        }
        | RaindexDepositFailed { failed_at: at, .. }
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
    use st0x_tokenization::{TokenizationRequestId, issuer_request_id};

    use super::super::StoredKind;
    use super::super::tests::{fold_mint, mint_happy_path, stage, symbol, timestamp};
    use super::*;

    #[test]
    fn mint_happy_path_completes_all_four_stages() {
        let operation = fold_mint(&mint_happy_path());

        assert_eq!(operation.kind, EquityOperationKind::Mint);
        assert_eq!(operation.symbol.as_ref(), Some(&symbol()));
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.started_at, Some(timestamp(0)));
        assert_eq!(operation.completed_at, Some(timestamp(70)));
        assert_eq!(operation.total_ms, Some(70_000));

        for name in [
            EquityStageName::MintAcceptance,
            EquityStageName::MintReceipt,
            EquityStageName::MintWrap,
            EquityStageName::MintDeposit,
        ] {
            assert_eq!(
                stage(&operation, name).outcome,
                StageOutcome::Succeeded,
                "{name:?} did not succeed"
            );
        }
    }

    #[test]
    fn mint_rejected_closes_acceptance_as_failed() {
        let events = vec![
            TokenizedEquityMintEvent::MintRequested {
                symbol: symbol(),
                quantity: float!(5),
                wallet: Address::repeat_byte(0x11),
                requested_at: timestamp(0),
            },
            TokenizedEquityMintEvent::MintRejected {
                reason: "rejected by provider".to_string(),
                rejected_at: timestamp(5),
            },
        ];
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::MintAcceptance).outcome,
            StageOutcome::Failed
        );
        assert_eq!(operation.total_ms, None);
    }

    #[test]
    fn mint_acceptance_failed_after_acceptance_closes_receipt_as_failed() {
        let events = vec![
            TokenizedEquityMintEvent::MintRequested {
                symbol: symbol(),
                quantity: float!(5),
                wallet: Address::repeat_byte(0x11),
                requested_at: timestamp(0),
            },
            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id: issuer_request_id("op"),
                tokenization_request_id: TokenizationRequestId::try_new("tok-1").unwrap(),
                accepted_at: timestamp(10),
            },
            TokenizedEquityMintEvent::MintAcceptanceFailed {
                reason: "poll timeout".to_string(),
                failed_at: timestamp(20),
            },
        ];
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::MintAcceptance).outcome,
            StageOutcome::Succeeded
        );
        assert_eq!(
            stage(&operation, EquityStageName::MintReceipt).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn mint_wrapping_failed_closes_wrap_as_failed() {
        let mut events = mint_happy_path()[..3].to_vec();
        events.push(TokenizedEquityMintEvent::WrappingFailed {
            symbol: symbol(),
            quantity: float!(5),
            reason: Some("ERC-4626 wrapping failed".to_string()),
            failed_at: timestamp(40),
        });
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::MintWrap).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn mint_raindex_deposit_failed_closes_deposit_as_failed() {
        let mut events = mint_happy_path()[..5].to_vec();
        events.push(TokenizedEquityMintEvent::RaindexDepositFailed {
            reason: "revert".to_string(),
            failed_at: timestamp(60),
        });
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::MintDeposit).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn mint_provider_completion_recovered_stays_in_progress_with_unmeasured_receipt() {
        // Recovery only ever fires for a mint that failed before tokens were
        // received (`recover_mint`'s `context.received_tokens` guard) -- here
        // `MintRejected` fires straight from `MintRequested`, before
        // `MintReceipt` ever opens, so `record_unmeasured` adds a fresh
        // Unmeasured entry for it. The aggregate lands in `TokensReceived`
        // (the same non-terminal state the normal `TokensReceived` event
        // produces), not the terminal `DepositedIntoRaindex`, so the
        // operation must stay `InProgress` with no `completed_at` -- the real
        // wrap/deposit work still has to happen.
        let mut events = mint_happy_path()[..1].to_vec();
        events.push(TokenizedEquityMintEvent::MintRejected {
            reason: "rejected by provider".to_string(),
            rejected_at: timestamp(5),
        });
        events.push(TokenizedEquityMintEvent::ProviderCompletionRecovered {
            issuer_request_id: issuer_request_id("op"),
            wallet: Address::repeat_byte(0x11),
            tokenization_request_id: TokenizationRequestId::try_new("tok-1").unwrap(),
            tx_hash: TxHash::random(),
            shares_minted: U256::from(5_000_000_000_000_000_000_u128),
            fees: None,
            recovered_at: timestamp(200),
        });
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::InProgress);
        assert_eq!(operation.completed_at, None);
        let receipt = stage(&operation, EquityStageName::MintReceipt);
        assert_eq!(receipt.outcome, StageOutcome::Unmeasured);
        assert_eq!(receipt.duration_ms, None);
        // `MintWrap` must already be open so the subsequent genuine
        // `TokensWrapped` event has a run to close.
        let wrap = stage(&operation, EquityStageName::MintWrap);
        assert_eq!(wrap.outcome, StageOutcome::Unmeasured);
        assert_eq!(wrap.ended_at, None);
    }

    #[test]
    fn mint_provider_completion_recovered_then_real_wrap_deposit_are_measured() {
        // The realistic post-recovery path: `resume_mint` drives genuine
        // wrap/deposit transactions after recovery. Both stages must end up
        // `Succeeded` with real measured durations, not missing (MintWrap
        // never opened) or stuck `Unmeasured` (MintDeposit's real close
        // silently discarded because an earlier bogus entry already closed
        // it) -- the corruption this fix prevents.
        let mut events = mint_happy_path()[..1].to_vec();
        events.push(TokenizedEquityMintEvent::MintRejected {
            reason: "rejected by provider".to_string(),
            rejected_at: timestamp(5),
        });
        events.push(TokenizedEquityMintEvent::ProviderCompletionRecovered {
            issuer_request_id: issuer_request_id("op"),
            wallet: Address::repeat_byte(0x11),
            tokenization_request_id: TokenizationRequestId::try_new("tok-1").unwrap(),
            tx_hash: TxHash::random(),
            shares_minted: U256::from(5_000_000_000_000_000_000_u128),
            fees: None,
            recovered_at: timestamp(200),
        });
        events.push(TokenizedEquityMintEvent::WrapSubmitted {
            wrap_tx_hash: TxHash::random(),
            submitted_at: timestamp(205),
        });
        events.push(TokenizedEquityMintEvent::TokensWrapped {
            wrap_tx_hash: TxHash::random(),
            wrapped_shares: U256::from(5_000_000_000_000_000_000_u128),
            wrapped_at: timestamp(220),
            wrap_block: Some(1),
        });
        events.push(TokenizedEquityMintEvent::VaultDepositSubmitted {
            vault_deposit_tx_hash: TxHash::random(),
            submitted_at: timestamp(225),
        });
        events.push(TokenizedEquityMintEvent::DepositedIntoRaindex {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: timestamp(240),
        });
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.completed_at, Some(timestamp(240)));
        let wrap = stage(&operation, EquityStageName::MintWrap);
        assert_eq!(wrap.outcome, StageOutcome::Succeeded);
        assert_eq!(wrap.duration_ms, Some(20_000));
        let deposit = stage(&operation, EquityStageName::MintDeposit);
        assert_eq!(deposit.outcome, StageOutcome::Succeeded);
        assert_eq!(deposit.duration_ms, Some(20_000));
    }

    #[test]
    fn redelivering_provider_completion_recovered_does_not_force_close_the_mint_wrap_it_opened() {
        // Regression test: like
        // `redelivering_transfer_failed_after_withdraw_does_not_fabricate_extra_failed_stages`,
        // a restart's `catch_up` re-folds an operation's entire event stream
        // from scratch on top of the already-live-folded stored row. If the
        // real wrap/deposit work (driven by `resume_mint`) has not completed
        // by the time of the restart, `MintWrap` is still open when the
        // redelivered `ProviderCompletionRecovered` event is re-applied.
        // Before this fix, `close_open_stages` in that arm would find the
        // `MintWrap` run this exact event opened during its OWN prior
        // application (`started_at == recovered_at`) and force-close it
        // `Succeeded` with a fabricated `duration_ms: Some(0)`, permanently
        // discarding the real wrap timing.
        let mut events = mint_happy_path()[..1].to_vec();
        events.push(TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "provider rejected".to_string(),
            failed_at: timestamp(5),
        });
        events.push(TokenizedEquityMintEvent::ProviderCompletionRecovered {
            issuer_request_id: issuer_request_id("op"),
            wallet: Address::repeat_byte(0x11),
            tokenization_request_id: TokenizationRequestId::try_new("tok-1").unwrap(),
            tx_hash: TxHash::random(),
            shares_minted: U256::from(5_000_000_000_000_000_000_u128),
            fees: None,
            recovered_at: timestamp(200),
        });

        let mut operation = StoredOperation::new(
            Uuid::new_v4(),
            StoredKind::Mint,
            mint_observed_at(&events[0]),
        );
        for event in &events {
            operation.apply_mint(event);
        }
        // Simulates the restart: `catch_up` re-folds the whole stream onto
        // the already-live-folded state above (the real wrap/deposit has not
        // happened yet, so `MintWrap` is still open).
        for event in &events {
            operation.apply_mint(event);
        }
        let redelivered = operation.into_dto();

        assert_eq!(redelivered.status, RebalanceTimingStatus::InProgress);
        let wrap = stage(&redelivered, EquityStageName::MintWrap);
        assert_eq!(wrap.outcome, StageOutcome::Unmeasured);
        assert_eq!(wrap.ended_at, None);
        assert_eq!(wrap.duration_ms, None);
    }

    #[test]
    fn mint_provider_completion_recovered_after_acceptance_failure_marks_receipt_unmeasured() {
        // Unlike the pre-acceptance `MintRejected` case above, `MintAccepted`
        // opens `MintReceipt` and `MintAcceptanceFailed` closes it `Failed`
        // via `close_open_stages` before recovery fires. Recovery must flip
        // that stale `Failed` outcome to `Unmeasured` rather than leaving the
        // recovered mint's receipt stage marked failed forever.
        let mut events = mint_happy_path()[..2].to_vec();
        events.push(TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "provider poll failed".to_string(),
            failed_at: timestamp(15),
        });
        events.push(TokenizedEquityMintEvent::ProviderCompletionRecovered {
            issuer_request_id: issuer_request_id("op"),
            wallet: Address::repeat_byte(0x11),
            tokenization_request_id: TokenizationRequestId::try_new("tok-1").unwrap(),
            tx_hash: TxHash::random(),
            shares_minted: U256::from(5_000_000_000_000_000_000_u128),
            fees: None,
            recovered_at: timestamp(200),
        });
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::InProgress);
        assert_eq!(operation.completed_at, None);
        let receipt = stage(&operation, EquityStageName::MintReceipt);
        assert_eq!(receipt.outcome, StageOutcome::Unmeasured);
        assert_eq!(receipt.duration_ms, None);
        let wrap = stage(&operation, EquityStageName::MintWrap);
        assert_eq!(wrap.outcome, StageOutcome::Unmeasured);
        assert_eq!(wrap.ended_at, None);
    }

    #[test]
    fn mint_provider_completion_recovered_after_pre_acceptance_rejection_unmeasures_acceptance() {
        // `MintRejected` fires straight from `MintRequested`, closing
        // `MintAcceptance` (not `MintReceipt`, which never opened) `Failed`
        // via `close_open_stages`. Before this fix, `ProviderCompletionRecovered`
        // only scrubbed `MintReceipt`, so `MintAcceptance` stayed permanently
        // `Failed` even once the operation goes on to genuinely `Complete`
        // via real wrap/deposit work. Recovery must scrub every Failed-outcome
        // stage, matching how the redemption `ProviderCompletionRecovered` arm
        // already handles this.
        let mut events = mint_happy_path()[..1].to_vec();
        events.push(TokenizedEquityMintEvent::MintRejected {
            reason: "rejected by provider".to_string(),
            rejected_at: timestamp(5),
        });
        events.push(TokenizedEquityMintEvent::ProviderCompletionRecovered {
            issuer_request_id: issuer_request_id("op"),
            wallet: Address::repeat_byte(0x11),
            tokenization_request_id: TokenizationRequestId::try_new("tok-1").unwrap(),
            tx_hash: TxHash::random(),
            shares_minted: U256::from(5_000_000_000_000_000_000_u128),
            fees: None,
            recovered_at: timestamp(200),
        });
        events.push(TokenizedEquityMintEvent::WrapSubmitted {
            wrap_tx_hash: TxHash::random(),
            submitted_at: timestamp(205),
        });
        events.push(TokenizedEquityMintEvent::TokensWrapped {
            wrap_tx_hash: TxHash::random(),
            wrapped_shares: U256::from(5_000_000_000_000_000_000_u128),
            wrapped_at: timestamp(220),
            wrap_block: Some(1),
        });
        events.push(TokenizedEquityMintEvent::VaultDepositSubmitted {
            vault_deposit_tx_hash: TxHash::random(),
            submitted_at: timestamp(225),
        });
        events.push(TokenizedEquityMintEvent::DepositedIntoRaindex {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: timestamp(240),
        });
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        let acceptance = stage(&operation, EquityStageName::MintAcceptance);
        assert_eq!(acceptance.outcome, StageOutcome::Unmeasured);
        assert_eq!(acceptance.duration_ms, None);
    }

    #[test]
    fn mint_operator_reconciled_completes_without_touching_stages() {
        let mut events = mint_happy_path()[..1].to_vec();
        events.push(TokenizedEquityMintEvent::MintRejected {
            reason: "rejected".to_string(),
            rejected_at: timestamp(5),
        });
        events.push(TokenizedEquityMintEvent::OperatorReconciled {
            reason: "funds recovered manually".to_string(),
            reconciled_at: timestamp(300),
        });
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.completed_at, Some(timestamp(300)));
        // Round-trip latency must exclude the operator-response window.
        assert_eq!(operation.total_ms, None);
        assert_eq!(
            stage(&operation, EquityStageName::MintAcceptance).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn mint_first_observed_at_mint_accepted_records_acceptance_unmeasured() {
        // Deploy/restart backfill case: the read model first observes this
        // operation at `MintAccepted` because the genesis `MintRequested`
        // predates the projection's window (or was otherwise never seen).
        // `MintAcceptance` must be recorded as an Unmeasured stage rather than
        // silently dropped, and the rest of the happy path still folds
        // normally on top of it.
        let events = mint_happy_path()[1..].to_vec();
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        let acceptance = stage(&operation, EquityStageName::MintAcceptance);
        assert_eq!(acceptance.outcome, StageOutcome::Unmeasured);
        assert_eq!(acceptance.duration_ms, None);
        assert_eq!(
            stage(&operation, EquityStageName::MintReceipt).outcome,
            StageOutcome::Succeeded
        );
    }

    #[test]
    fn mint_first_observed_at_tokens_received_records_receipt_unmeasured() {
        // Same mid-stream-first case, one stage later: `MintAccepted` was
        // never seen either.
        let events = mint_happy_path()[2..].to_vec();
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        let receipt = stage(&operation, EquityStageName::MintReceipt);
        assert_eq!(receipt.outcome, StageOutcome::Unmeasured);
        assert_eq!(receipt.duration_ms, None);
        assert_eq!(
            stage(&operation, EquityStageName::MintWrap).outcome,
            StageOutcome::Succeeded
        );
    }

    #[test]
    fn mint_first_observed_at_tokens_wrapped_records_wrap_unmeasured() {
        let events = mint_happy_path()[4..].to_vec();
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        let wrap = stage(&operation, EquityStageName::MintWrap);
        assert_eq!(wrap.outcome, StageOutcome::Unmeasured);
        assert_eq!(wrap.duration_ms, None);
        assert_eq!(
            stage(&operation, EquityStageName::MintDeposit).outcome,
            StageOutcome::Succeeded
        );
    }

    #[test]
    fn mint_first_observed_at_deposited_into_raindex_records_deposit_unmeasured() {
        let events = mint_happy_path()[6..].to_vec();
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        let deposit = stage(&operation, EquityStageName::MintDeposit);
        assert_eq!(deposit.outcome, StageOutcome::Unmeasured);
        assert_eq!(deposit.duration_ms, None);
    }

    #[test]
    fn mint_first_observed_at_wrapping_failed_hydrates_symbol_and_quantity() {
        // `WrappingFailed` carries `symbol`/`quantity` unlike the other
        // terminal mint failures, so an operation first observed here (the
        // genesis `MintRequested` never seen) must still hydrate them instead
        // of leaving `symbol: None, quantity: None`. It must also derive and
        // fail `MintWrap` (the stage it implies) rather than leaving the
        // operation `Failed` with an empty stage list -- `close_open_stages`
        // alone has nothing open to close since `self.stages` starts empty.
        let events = vec![TokenizedEquityMintEvent::WrappingFailed {
            symbol: symbol(),
            quantity: float!(5),
            reason: Some("ERC-4626 wrapping failed".to_string()),
            failed_at: timestamp(40),
        }];
        let operation = fold_mint(&events);

        assert_eq!(operation.symbol, Some(symbol()));
        assert_eq!(operation.quantity, Some(FractionalShares::new(float!(5))));
        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::MintWrap).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn mint_first_observed_at_raindex_deposit_failed_derives_failed_deposit_stage() {
        // Same mid-stream-first gap as `WrappingFailed` above, one stage
        // later: `RaindexDepositFailed` fires only from `TokensWrapped`, so a
        // first observation here (no earlier stage ever seen) must derive
        // and fail `MintDeposit` instead of leaving the operation `Failed`
        // with zero stage entries.
        let events = vec![TokenizedEquityMintEvent::RaindexDepositFailed {
            reason: "revert".to_string(),
            failed_at: timestamp(60),
        }];
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::MintDeposit).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn mint_first_observed_at_mint_rejected_derives_failed_acceptance_stage() {
        // `MintRejected` fires only from `MintRequested`, so a mid-stream-first
        // observation of it alone (no earlier stage ever seen) unambiguously
        // means `MintAcceptance` is what failed -- must derive and fail it
        // instead of leaving the operation `Failed` with zero stage entries.
        let events = vec![TokenizedEquityMintEvent::MintRejected {
            reason: "rejected by provider".to_string(),
            rejected_at: timestamp(5),
        }];
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::MintAcceptance).outcome,
            StageOutcome::Failed
        );
    }

    #[test]
    fn mint_first_observed_at_mint_acceptance_failed_defaults_to_acceptance_stage() {
        // Regression-lock, not a behavior fix: `MintAcceptanceFailed` is valid
        // from either `MintRequested` (pre-acceptance) or `MintAccepted`
        // (post-acceptance), which map to two DIFFERENT stages
        // (`MintAcceptance` vs `MintReceipt`). When it is the
        // mid-stream-first event (no stage ever observed, `self.stages`
        // empty), the bare event payload cannot say which of the two the
        // pipeline actually reached, so the best-effort default is
        // `MintAcceptance`, the earlier of the two -- even though the true
        // prior state may have been `MintAccepted` (acceptance actually
        // succeeded). This is a known, accepted limitation mirroring
        // redemption's `TransferFailed`; this test locks in the CURRENT
        // best-effort behavior so a future change to it is deliberate, not an
        // accidental regression.
        let events = vec![TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "poll timeout".to_string(),
            failed_at: timestamp(20),
        }];
        let operation = fold_mint(&events);

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(
            stage(&operation, EquityStageName::MintAcceptance).outcome,
            StageOutcome::Failed
        );
    }
}
