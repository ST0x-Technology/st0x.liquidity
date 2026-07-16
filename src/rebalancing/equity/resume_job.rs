//! Apalis job that resumes an interrupted tokenization aggregate
//! (mint or redemption) off the startup path.
//!
//! [`ResumeTokenizationAggregate`] is enqueued once per interrupted aggregate
//! at startup by `recover_interrupted_tokenization_aggregates` and dispatches
//! to [`CrossVenueEquityTransfer::resume_mint`] or
//! [`CrossVenueEquityTransfer::resume_redemption`]. Running the poll off-path
//! means a slow or down issuer cannot block the
//! [`crate::conductor::monitor::order_fills::OrderFillMonitor`] or
//! [`crate::conductor::monitor::inventory::InventoryMonitor`] from starting.
//!
//! Transient errors propagate as `Err` so apalis retries up to three times.
//! If all retries are exhausted the terminal failure is logged at `error!` but
//! does NOT open a recovering worker circuit, so hedging and fill detection
//! continue running. Aggregates already in a terminal state return `Ok(())`
//! (idempotent).

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use st0x_tokenization::IssuerRequestId;

use super::{CrossVenueEquityTransfer, MintError, RedemptionError};
use crate::conductor::job::{Job, JobQueue, Label};
use crate::equity_redemption::RedemptionAggregateId;

/// Apalis queue type for [`ResumeTokenizationAggregate`].
pub(crate) type ResumeTokenizationJobQueue = JobQueue<ResumeTokenizationAggregate>;

/// Which interrupted aggregate should be resumed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum ResumeTokenizationTarget {
    Mint(IssuerRequestId),
    Redemption(RedemptionAggregateId),
}

/// Apalis job payload. Holds the target aggregate to resume.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ResumeTokenizationAggregate {
    pub(crate) target: ResumeTokenizationTarget,
}

/// Dependencies the job needs.
pub(crate) struct ResumeTokenizationCtx {
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
}

/// Errors emitted by [`ResumeTokenizationAggregate::perform`].
#[derive(Debug, Error)]
pub(crate) enum ResumeTokenizationJobError {
    #[error(transparent)]
    Mint(#[from] MintError),
    #[error(transparent)]
    Redemption(#[from] RedemptionError),
}

impl Job<ResumeTokenizationCtx> for ResumeTokenizationAggregate {
    type Output = ();
    type Error = ResumeTokenizationJobError;

    const WORKER_NAME: &'static str = "resume-tokenization-aggregate-worker";
    const TERMINAL_FAILURE_MSG: &'static str = "Interrupted tokenization aggregate failed all resume retries; \
         the aggregate remains stuck. Operator action required.";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::ResumeTokenizationAggregate;

    fn label(&self) -> Label {
        match &self.target {
            ResumeTokenizationTarget::Mint(issuer_request_id) => Label::new(format!(
                "ResumeTokenizationAggregate:mint:{issuer_request_id}"
            )),
            ResumeTokenizationTarget::Redemption(aggregate_id) => Label::new(format!(
                "ResumeTokenizationAggregate:redemption:{aggregate_id}"
            )),
        }
    }

    async fn perform(&self, ctx: &ResumeTokenizationCtx) -> Result<Self::Output, Self::Error> {
        match &self.target {
            ResumeTokenizationTarget::Mint(issuer_request_id) => {
                ctx.transfer.resume_mint(issuer_request_id).await?;
            }
            ResumeTokenizationTarget::Redemption(aggregate_id) => {
                ctx.transfer.resume_redemption(aggregate_id).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, TxHash, U256};
    use serde_json::json;
    use st0x_event_sorcery::test_store;
    use st0x_float_macro::float;
    use st0x_raindex::{Raindex, RaindexVaultId};
    use st0x_tokenization::mock::{MockCompletionOutcome, MockDetectionOutcome, MockTokenizer};
    use st0x_tokenization::{issuer_request_id, tokenization_request_id};
    use st0x_wrapper::{MockWrapper, Wrapper};

    use super::*;
    use crate::equity_redemption::{
        EquityRedemption, EquityRedemptionCommand, redemption_aggregate_id,
    };
    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintCommand};
    use crate::vault_lookup::MockVaultLookup;

    /// Builds a [`ResumeTokenizationCtx`] backed by in-memory stores, plus
    /// the underlying stores for seeding aggregate state and the tokenizer
    /// for asserting call counts.
    async fn build_ctx() -> (
        ResumeTokenizationCtx,
        Arc<st0x_event_sorcery::Store<crate::tokenized_equity_mint::TokenizedEquityMint>>,
        Arc<st0x_event_sorcery::Store<EquityRedemption>>,
        Arc<MockTokenizer>,
    ) {
        build_ctx_with_tokenizer(Arc::new(MockTokenizer::new())).await
    }

    /// Like [`build_ctx`] but with a caller-provided tokenizer, so a redemption
    /// resume can configure detection/completion outcomes.
    async fn build_ctx_with_tokenizer(
        tokenizer: Arc<MockTokenizer>,
    ) -> (
        ResumeTokenizationCtx,
        Arc<st0x_event_sorcery::Store<crate::tokenized_equity_mint::TokenizedEquityMint>>,
        Arc<st0x_event_sorcery::Store<EquityRedemption>>,
        Arc<MockTokenizer>,
    ) {
        let (pool, _apalis_pool) = crate::test_utils::setup_test_pools().await;
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let vault_lookup =
            Arc::new(MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO)));

        let transfer_services = EquityTransferServices {
            raindex: raindex.clone(),
            vault_lookup: vault_lookup.clone(),
            tokenizer: tokenizer.clone(),
            wrapper: wrapper.clone(),
        };

        let mint_store = Arc::new(test_store(pool.clone(), transfer_services.clone()));
        let redemption_store = Arc::new(test_store(pool, transfer_services));

        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            raindex,
            vault_lookup,
            tokenizer.clone(),
            wrapper,
            Address::ZERO,
            mint_store.clone(),
            redemption_store.clone(),
        ));

        let ctx = ResumeTokenizationCtx { transfer };
        (ctx, mint_store, redemption_store, tokenizer)
    }

    /// `perform` on a `Mint` target with a terminal aggregate (`DepositedIntoRaindex`)
    /// returns `Ok(())` immediately without issuer contact.
    #[tokio::test]
    async fn perform_mint_target_returns_ok_for_terminal_aggregate() {
        let (ctx, mint_store, _, tokenizer) = build_ctx().await;
        let id = issuer_request_id("resume-mint-terminal");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        // Drive mint to DepositedIntoRaindex (terminal) via command chain.
        // MockTokenizer returns tokens immediately so Poll succeeds.
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();

        mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        // TokensReceived -> TokensWrapped
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: TxHash::ZERO,
                    wrapped_shares: U256::from(1u64),
                    wrap_block: 1,
                },
            )
            .await
            .unwrap();

        // TokensWrapped -> DepositedIntoRaindex
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash: TxHash::ZERO,
                },
            )
            .await
            .unwrap();

        // Capture seeding call count before the perform call.
        let calls_before = tokenizer.call_count();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id),
        };

        // Terminal aggregate: resume_mint returns Ok(()) immediately.
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            tokenizer.call_count(),
            calls_before,
            "terminal mint aggregate must not invoke any tokenizer method during resume"
        );
    }

    /// `perform` on a `Mint` target with a `Failed` aggregate returns `Ok(())`
    /// (both terminal states are no-ops per `resume_mint` semantics).
    #[tokio::test]
    async fn perform_mint_target_returns_ok_for_failed_aggregate() {
        let (ctx, mint_store, _, tokenizer) = build_ctx().await;
        let id = issuer_request_id("resume-mint-failed");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        // Drive to Failed via RequestMint + Poll + FailWrapping.
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();

        mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::FailWrapping {
                    reason: "test: force failed".to_string(),
                },
            )
            .await
            .unwrap();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id),
        };

        let calls_before = tokenizer.call_count();

        // Failed aggregate is terminal: resume_mint returns Ok(()).
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            tokenizer.call_count(),
            calls_before,
            "failed mint aggregate must not invoke any tokenizer method during resume"
        );
    }

    /// `perform` on a `Redemption` target with a `Completed` aggregate returns
    /// `Ok(())` immediately (terminal no-op).
    #[tokio::test]
    async fn perform_redemption_target_returns_ok_for_completed_aggregate() {
        let (ctx, _, redemption_store, tokenizer) = build_ctx().await;
        let id = redemption_aggregate_id("resume-redemption-completed");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        // Drive redemption to Completed: Redeem -> SubmitWithdraw ->
        // ConfirmWithdraw -> UnwrapTokens -> SubmitUnwrap -> ConfirmUnwrap ->
        // PrepareSend -> SendTokens -> (TokensSent). Then detect via DetectSend.
        // MockRaindex and MockWrapper complete synchronously.
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    token: Address::ZERO,
                    amount: U256::from(1_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();

        // Drive through intermediate states to SendPending.
        for cmd in [
            EquityRedemptionCommand::SubmitWithdraw,
            EquityRedemptionCommand::ConfirmWithdraw,
            EquityRedemptionCommand::UnwrapTokens,
            EquityRedemptionCommand::SubmitUnwrap,
            EquityRedemptionCommand::ConfirmUnwrap,
            EquityRedemptionCommand::PrepareSend,
        ] {
            redemption_store.send(&id, cmd).await.unwrap();
        }

        // SendTokens uses MockTokenizer.send_for_redemption which succeeds.
        // Drives SendPending -> TokensSent.
        redemption_store
            .send(&id, EquityRedemptionCommand::SendTokens)
            .await
            .unwrap();

        // TokensSent -> Pending (Alpaca detected the token transfer).
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: tokenization_request_id("test-req-id"),
                },
            )
            .await
            .unwrap();

        // Pending -> Completed (Alpaca completed the redemption).
        redemption_store
            .send(&id, EquityRedemptionCommand::Complete)
            .await
            .unwrap();

        // Capture seeding call count before the perform call.
        let calls_before = tokenizer.call_count();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(id),
        };

        // Completed aggregate is terminal: resume_redemption returns Ok(()).
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            tokenizer.call_count(),
            calls_before,
            "terminal redemption aggregate must not invoke any tokenizer method during resume"
        );
    }

    /// `perform` on a `Mint` target with a NON-TERMINAL (interrupted) aggregate
    /// must actually drive the resume: contact the issuer (tokenizer Poll) and
    /// advance the aggregate past its seeded state. Without this, a stubbed
    /// `Ok(())` body would pass every other test in this module (terminal tests
    /// assert call_count unchanged; missing tests assert error propagation).
    #[tokio::test]
    async fn perform_mint_target_resumes_interrupted_aggregate() {
        let (ctx, mint_store, _, tokenizer) = build_ctx().await;
        let id = issuer_request_id("resume-mint-interrupted");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        // RequestMint emits MintRequested + MintAccepted (MockTokenizer accepts),
        // leaving a non-terminal MintAccepted aggregate -- the interrupted state.
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();

        let seeded = mint_store.load(&id).await.unwrap();
        assert!(
            matches!(seeded, Some(TokenizedEquityMint::MintAccepted { .. })),
            "seed must be a non-terminal MintAccepted aggregate, got {seeded:?}"
        );

        let calls_before = tokenizer.call_count();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id.clone()),
        };
        Job::perform(&job, &ctx).await.unwrap();

        // The resume must have contacted the issuer (Poll) ...
        assert!(
            tokenizer.call_count() > calls_before,
            "resuming a MintAccepted aggregate must call the tokenizer (Poll); \
             call_count stayed at {calls_before}"
        );

        // ... and advanced the aggregate off MintAccepted to its terminal state.
        let resumed = mint_store.load(&id).await.unwrap();
        assert!(
            matches!(
                resumed,
                Some(TokenizedEquityMint::DepositedIntoRaindex { .. })
            ),
            "resume must drive the MintAccepted aggregate to DepositedIntoRaindex, \
             got {resumed:?}"
        );
    }

    /// `perform` on a `Mint` target with a non-existent aggregate propagates
    /// the error so apalis retries.
    #[tokio::test]
    async fn perform_mint_target_propagates_error_for_missing_aggregate() {
        let (ctx, _, _, _tokenizer) = build_ctx().await;
        let id = issuer_request_id("resume-mint-missing");

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id),
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();
        assert!(
            matches!(
                error,
                ResumeTokenizationJobError::Mint(MintError::EntityNotFound { .. })
            ),
            "missing mint aggregate must propagate EntityNotFound so apalis retries, got {error:?}"
        );
    }

    /// `perform` on a `Redemption` target with a non-existent aggregate
    /// propagates the error so apalis retries.
    #[tokio::test]
    async fn perform_redemption_target_propagates_error_for_missing_aggregate() {
        let (ctx, _, _, _tokenizer) = build_ctx().await;
        let id = redemption_aggregate_id("resume-redemption-missing");

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(id),
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();
        assert!(
            matches!(
                error,
                ResumeTokenizationJobError::Redemption(RedemptionError::EntityNotFound { .. })
            ),
            "missing redemption aggregate must propagate EntityNotFound so apalis retries, \
             got {error:?}"
        );
    }

    /// `perform` on a `Redemption` target with a NON-TERMINAL (interrupted)
    /// aggregate must drive the resume: contact the issuer (send/detect/complete)
    /// and advance the aggregate to its terminal state. Mirror of
    /// `perform_mint_target_resumes_interrupted_aggregate` for the redemption arm.
    #[tokio::test]
    async fn perform_redemption_target_resumes_interrupted_aggregate() {
        // Detection defaults to Detected; completion must be configured (the
        // default panics) so resume_redemption can reach Completed.
        let tokenizer = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let (ctx, _, redemption_store, tokenizer) = build_ctx_with_tokenizer(tokenizer).await;
        let id = redemption_aggregate_id("resume-redemption-interrupted");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        // Drive the redemption to a non-terminal SendPending state (Redeem ->
        // ... -> PrepareSend): an interrupted aggregate that has NOT yet sent
        // tokens to the issuer or completed.
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    token: Address::ZERO,
                    amount: U256::from(1_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();
        for cmd in [
            EquityRedemptionCommand::SubmitWithdraw,
            EquityRedemptionCommand::ConfirmWithdraw,
            EquityRedemptionCommand::UnwrapTokens,
            EquityRedemptionCommand::SubmitUnwrap,
            EquityRedemptionCommand::ConfirmUnwrap,
            EquityRedemptionCommand::PrepareSend,
        ] {
            redemption_store.send(&id, cmd).await.unwrap();
        }

        let seeded = redemption_store.load(&id).await.unwrap();
        assert!(
            matches!(seeded, Some(EquityRedemption::SendPending { .. })),
            "seed must be a non-terminal SendPending aggregate, got {seeded:?}"
        );

        let calls_before = tokenizer.call_count();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(id.clone()),
        };
        Job::perform(&job, &ctx).await.unwrap();

        // The resume must have contacted the issuer (send/detect/complete) ...
        assert!(
            tokenizer.call_count() > calls_before,
            "resuming a SendPending redemption must call the tokenizer; \
             call_count stayed at {calls_before}"
        );

        // ... and advanced the aggregate off SendPending to Completed.
        let resumed = redemption_store.load(&id).await.unwrap();
        assert!(
            matches!(resumed, Some(EquityRedemption::Completed { .. })),
            "resume must drive the SendPending redemption to Completed, got {resumed:?}"
        );
    }

    /// Job payload for both variants serializes and deserializes correctly.
    #[test]
    fn payload_roundtrips_through_json() {
        let mint_id = issuer_request_id("roundtrip-mint");
        let redemption_id = redemption_aggregate_id("roundtrip-redemption");

        let mint_job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(mint_id.clone()),
        };

        let expected_mint = json!({ "target": { "Mint": mint_id.to_string() } });
        assert_eq!(serde_json::to_value(&mint_job).unwrap(), expected_mint);

        let roundtripped_mint: ResumeTokenizationAggregate =
            serde_json::from_value(expected_mint).unwrap();
        assert_eq!(
            roundtripped_mint.target,
            ResumeTokenizationTarget::Mint(mint_id),
            "roundtripped mint target must match original"
        );

        let redemption_job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(redemption_id.clone()),
        };

        let expected_redemption = json!({ "target": { "Redemption": redemption_id.to_string() } });
        assert_eq!(
            serde_json::to_value(&redemption_job).unwrap(),
            expected_redemption
        );

        let roundtripped_redemption: ResumeTokenizationAggregate =
            serde_json::from_value(expected_redemption).unwrap();
        assert_eq!(
            roundtripped_redemption.target,
            ResumeTokenizationTarget::Redemption(redemption_id),
            "roundtripped redemption target must match original"
        );
    }
}
