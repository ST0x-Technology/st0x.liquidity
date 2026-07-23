//! Apalis jobs that drive tokenized equity transfers through their
//! lifecycles, one per direction: [`TransferEquityToMarketMaking`] for mints
//! (`TokenizedEquityMint`) and [`TransferEquityToHedging`] for redemptions
//! (`EquityRedemption`).
//!
//! Each job is keyed by an aggregate id chosen at enqueue time, so apalis
//! retries (and bot restarts that re-pick the row from the Jobs table) hit
//! the same aggregate. The worker calls the trait-erased resume entry point
//! on the equity transfer, which loads the aggregate via `Store::load` and
//! dispatches on its current state: an absent aggregate starts a fresh
//! transfer, an in-flight one resumes from its persisted step, and a terminal
//! one is a no-op. New transfers and mid-flight resumes share the same entry
//! point -- that uniformity is what makes recovery dispatch fall out of the
//! standard transfer lifecycle.
//!
//! The per-symbol `equity_in_progress` guard is cleared event-driven when the
//! aggregate reaches a terminal state (success or a recorded failure), not by
//! these workers. A transient failure that only schedules a retry keeps the
//! guard latched so automation does not re-arm a fresh transfer on top of a
//! partial one.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use st0x_config::EquitiesConfig;
use st0x_event_sorcery::Store;
use st0x_execution::{FractionalShares, Symbol};
use st0x_tokenization::IssuerRequestId;

use super::{CrossVenueEquityTransfer, MintTransferError, RedemptionError};
use crate::conductor::job::{Job, JobQueue, Label};
use crate::equity_redemption::RedemptionAggregateId;
use crate::rebalancing::trigger::GuardState;
use crate::tokenized_equity_mint::TokenizedEquityMint;

/// Apalis queue type for [`TransferEquityToMarketMaking`].
pub(crate) type TransferEquityToMarketMakingJobQueue = JobQueue<TransferEquityToMarketMaking>;

/// Trait-erased entry point for the hedging->market-making equity apalis
/// job. [`CrossVenueEquityTransfer`] is concrete, but the indirection gives
/// the job a mock seam, mirroring the USDC transfer jobs.
#[async_trait]
pub(crate) trait ResumeEquityToMarketMaking: Send + Sync + 'static {
    async fn resume_equity_to_market_making(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), MintTransferError>;
}

#[async_trait]
impl ResumeEquityToMarketMaking for CrossVenueEquityTransfer {
    async fn resume_equity_to_market_making(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), MintTransferError> {
        Self::resume_equity_to_market_making(self, issuer_request_id, symbol, quantity).await
    }
}

/// Dependencies the job needs to drive the mint.
pub(crate) struct TransferEquityToMarketMakingCtx {
    pub(crate) transfer: Arc<dyn ResumeEquityToMarketMaking>,
    /// Shared in-progress map. When a `PostReceipt` error surfaces and the
    /// aggregate is in a pre-wrap post-receipt state (`TokensReceived` or
    /// `WrapSubmitted`) AND `wrapped_equity_recovery` is enabled for the
    /// symbol, the job transitions this entry from `ActiveTransfer` to
    /// `HeldForRecovery` and returns `Ok(())` so apalis does not retry.
    /// `UnwrappedEquityRecovery` claims the `HeldForRecovery` slot and
    /// re-wraps + deposits the tokens.
    ///
    /// `TokensWrapped` and `VaultDepositSubmitted` are NOT handed off to
    /// recovery: the deposit stage is idempotent and apalis retrying the
    /// transfer job's `resume_mint` is the correct path for those states.
    ///
    /// When recovery is disabled for the symbol, `Err(PostReceipt)` is
    /// propagated so apalis retries the transfer job as before the PR —
    /// no stranding occurs.
    pub(crate) equity_in_progress: Arc<RwLock<HashMap<Symbol, GuardState>>>,
    /// Mint aggregate store. After a `PostReceipt` error the job loads the
    /// aggregate to confirm which post-receipt state it is in before
    /// transitioning the guard. Absent/pre-receipt/terminal states propagate
    /// `Err` so apalis retries normally.
    pub(crate) mint_store: Arc<Store<TokenizedEquityMint>>,
    /// Per-symbol equity asset configuration. Used to gate the `HeldForRecovery`
    /// handoff on `wrapped_equity_recovery = "enabled"` for the symbol — the
    /// same predicate the inventory reactor uses when dispatching recovery jobs.
    /// Keeping the check here ensures the two paths cannot disagree: if recovery
    /// is disabled, `Err(PostReceipt)` is returned instead and apalis retries.
    pub(crate) equities_config: EquitiesConfig,
}

/// Errors emitted by [`TransferEquityToMarketMaking::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferEquityToMarketMakingJobError {
    #[error(transparent)]
    Transfer(#[from] MintTransferError),
}

/// Apalis job payload. The `issuer_request_id` is generated at enqueue time
/// so retries resume the same aggregate (and Alpaca deduplicates the mint
/// request by the same id).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TransferEquityToMarketMaking {
    pub(crate) issuer_request_id: IssuerRequestId,
    pub(crate) symbol: Symbol,
    pub(crate) quantity: FractionalShares,
    /// Generation of the `ActiveTransfer` slot this job holds. Used by
    /// `mark_held_for_recovery` to detect if the timeout sweeper cleared this
    /// job's slot and a new transfer claimed it before the PostReceipt handler ran.
    ///
    /// Defaults to 0 on deserialization for rows enqueued before this field was
    /// added. A mismatch in `mark_held_for_recovery` is safe: it returns
    /// `GenerationMismatch -> Ok(())` rather than overwriting a new transfer's slot.
    #[serde(default)]
    pub(crate) generation: u64,
}

impl Job<TransferEquityToMarketMakingCtx> for TransferEquityToMarketMaking {
    type Output = ();
    type Error = TransferEquityToMarketMakingJobError;

    const WORKER_NAME: &'static str = "transfer-equity-to-market-making-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::TransferEquityToMarketMaking;

    fn label(&self) -> Label {
        Label::new(format!(
            "TransferEquityToMarketMaking:{}",
            self.issuer_request_id
        ))
    }

    async fn perform(
        &self,
        ctx: &TransferEquityToMarketMakingCtx,
    ) -> Result<Self::Output, Self::Error> {
        let result = ctx
            .transfer
            .resume_equity_to_market_making(&self.issuer_request_id, &self.symbol, self.quantity)
            .await;

        // Success needs no post-processing. Only a `PostReceipt` error (Alpaca
        // already delivered the tokens onchain) gets the recovery handoff below;
        // any other transfer error propagates for apalis to retry.
        let Err(transfer_error) = result else {
            return Ok(());
        };

        match transfer_error {
            MintTransferError::PostReceipt(mint_error) => {
                // PostReceipt means Alpaca already delivered tokens onchain.
                // Only pre-wrap states (TokensReceived, WrapSubmitted) hand
                // off to UnwrappedEquityRecovery: tokens are UNWRAPPED in the
                // base wallet and need re-wrap + deposit.
                //
                // TokensWrapped and VaultDepositSubmitted are NOT handed off:
                // the deposit stage is idempotent, and apalis retrying
                // resume_mint via the transfer job is the correct path.
                // WrappedEquityRecovery remains orphan-only (absent guard).
                //
                // The handoff is only done when recovery is enabled. When
                // disabled, propagate Err so apalis retries.
                let state = ctx
                    .mint_store
                    .load(&self.issuer_request_id)
                    .await
                    .inspect_err(|error| {
                        warn!(
                            target: "rebalance",
                            symbol = %self.symbol,
                            issuer_request_id = %self.issuer_request_id,
                            ?error,
                            "PostReceipt handler: failed to load mint aggregate state; \
                             treating as non-recoverable and propagating error for apalis retry"
                        );
                    });

                let is_post_receipt_recoverable = matches!(
                    state,
                    Ok(Some(
                        TokenizedEquityMint::TokensReceived { .. }
                            | TokenizedEquityMint::WrapSubmitted { .. } // TokensWrapped and VaultDepositSubmitted intentionally excluded:
                                                                        // the deposit stage is idempotent; apalis retrying resume_mint is
                                                                        // the correct path. WrappedEquityRecovery handles orphaned wrapped
                                                                        // tokens (absent guard) independently.
                    ))
                );

                let recovery_enabled =
                    ctx.equities_config
                        .symbols
                        .get(&self.symbol)
                        .is_some_and(|cfg| {
                            cfg.wrapped_equity_recovery == st0x_config::OperationMode::Enabled
                        });

                if is_post_receipt_recoverable && recovery_enabled {
                    match mark_held_for_recovery(
                        &ctx.equity_in_progress,
                        &self.symbol,
                        self.generation,
                    ) {
                        MarkHeldResult::Transitioned | MarkHeldResult::AlreadyHeld => {
                            warn!(
                                target: "rebalance",
                                symbol = %self.symbol,
                                issuer_request_id = %self.issuer_request_id,
                                ?mint_error,
                                "PostReceipt error with tokens in wallet (state: \
                                 TokensReceived / WrapSubmitted); transitioning guard to \
                                 HeldForRecovery. UnwrappedEquityRecovery will re-wrap and \
                                 deposit. Apalis will NOT retry this job."
                            );
                            Ok(())
                        }
                        MarkHeldResult::EntryAbsent => {
                            warn!(
                                target: "rebalance",
                                symbol = %self.symbol,
                                issuer_request_id = %self.issuer_request_id,
                                "PostReceipt handler: guard entry absent (cleared by timeout \
                                 sweeper?); propagating Err so apalis retries"
                            );
                            Err(TransferEquityToMarketMakingJobError::Transfer(
                                MintTransferError::PostReceipt(mint_error),
                            ))
                        }
                        MarkHeldResult::GenerationMismatch => {
                            // Timeout sweeper cleared this job's slot and a new transfer
                            // reclaimed it. No HeldForRecovery handoff is needed — the
                            // new transfer will drive its own lifecycle. Return Ok(()) so
                            // apalis marks this job Done rather than retrying it (which
                            // would operate on a stale aggregate state).
                            //
                            // Note: the guard is re-checked atomically inside
                            // mark_held_for_recovery under the write lock, so this
                            // decision is race-free with any concurrent transition.
                            warn!(
                                target: "rebalance",
                                symbol = %self.symbol,
                                issuer_request_id = %self.issuer_request_id,
                                "PostReceipt handler: generation mismatch (new transfer owns \
                                 the slot); returning Ok(()) — no recovery handoff"
                            );
                            Ok(())
                        }
                    }
                } else {
                    // Aggregate is absent, pre-receipt, already terminal,
                    // TokensWrapped/VaultDepositSubmitted, DB load failed, or
                    // recovery is disabled. Propagate Err so apalis retries or
                    // records a failed job.
                    Err(TransferEquityToMarketMakingJobError::Transfer(
                        MintTransferError::PostReceipt(mint_error),
                    ))
                }
            }
            other @ MintTransferError::PreReceipt(_) => {
                Err(TransferEquityToMarketMakingJobError::Transfer(other))
            }
        }
    }
}

/// Outcome of [`mark_held_for_recovery`].
enum MarkHeldResult {
    /// Successfully transitioned `ActiveTransfer` -> `HeldForRecovery`.
    Transitioned,
    /// Slot was already `HeldForRecovery` (idempotent double-call after retry).
    AlreadyHeld,
    /// Guard entry is absent (cleared by timeout sweeper before `PostReceipt`
    /// handler ran). Caller should propagate `Err` so apalis retries; the
    /// tokens remain in the wallet and the next inventory poll re-triggers
    /// recovery via the orphan path.
    EntryAbsent,
    /// The slot holds `ActiveTransfer` with a different generation: the timeout
    /// sweeper cleared this job's slot and a new transfer reclaimed it. The
    /// caller should return `Ok(())` — no handoff to recovery is needed because
    /// this job's tokens are either already handled or will be by the new transfer.
    GenerationMismatch,
}

/// Atomically transitions the guard from `ActiveTransfer` to `HeldForRecovery`.
///
/// Returns [`MarkHeldResult`] describing the outcome:
/// - `Transitioned`: slot updated; call site should return `Ok(())`.
/// - `AlreadyHeld`: slot was already held (idempotent); return `Ok(())`.
/// - `EntryAbsent`: guard was cleared externally; return `Err` for apalis retry.
/// - `GenerationMismatch`: a newer transfer owns the slot; return `Ok(())`.
fn mark_held_for_recovery(
    map: &RwLock<HashMap<Symbol, GuardState>>,
    symbol: &Symbol,
    expected_generation: u64,
) -> MarkHeldResult {
    let mut poisoned = false;
    let result = {
        let mut guard = match map.write() {
            Ok(guard) => guard,
            Err(poison) => {
                poisoned = true;
                poison.into_inner()
            }
        };
        match guard.get(symbol) {
            Some(GuardState::ActiveTransfer { generation })
                if *generation == expected_generation =>
            {
                guard.insert(symbol.clone(), GuardState::HeldForRecovery);
                MarkHeldResult::Transitioned
            }
            Some(GuardState::ActiveTransfer { .. }) => MarkHeldResult::GenerationMismatch,
            Some(GuardState::HeldForRecovery) => MarkHeldResult::AlreadyHeld,
            None => MarkHeldResult::EntryAbsent,
        }
    };

    if poisoned {
        warn!(
            target: "rebalance",
            %symbol,
            "mark_held_for_recovery: equity_in_progress lock poisoned; recovering inner guard"
        );
    }
    match result {
        MarkHeldResult::GenerationMismatch => {
            // A newer transfer claimed the slot after the timeout sweeper
            // cleared this job's slot. Leave the new claim untouched.
            warn!(
                target: "rebalance",
                %symbol,
                expected_generation,
                "mark_held_for_recovery: generation mismatch; a newer transfer owns the \
                 slot. No HeldForRecovery handoff."
            );
        }
        MarkHeldResult::AlreadyHeld => {
            warn!(
                target: "rebalance",
                %symbol,
                "mark_held_for_recovery: slot already HeldForRecovery; possible \
                 double-call after idempotent retry"
            );
        }
        MarkHeldResult::Transitioned | MarkHeldResult::EntryAbsent => {}
    }
    result
}

/// Apalis queue type for [`TransferEquityToHedging`].
pub(crate) type TransferEquityToHedgingJobQueue = JobQueue<TransferEquityToHedging>;

/// Trait-erased entry point for the market-making->hedging equity apalis
/// job. Sibling of [`ResumeEquityToMarketMaking`]; same mock-seam rationale.
#[async_trait]
pub(crate) trait ResumeEquityToHedging: Send + Sync + 'static {
    async fn resume_equity_to_hedging(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), RedemptionError>;
}

#[async_trait]
impl ResumeEquityToHedging for CrossVenueEquityTransfer {
    async fn resume_equity_to_hedging(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        quantity: FractionalShares,
    ) -> Result<(), RedemptionError> {
        Self::resume_equity_to_hedging(self, aggregate_id, symbol, quantity).await
    }
}

/// Dependencies the redemption job needs. Symmetric to
/// [`TransferEquityToMarketMakingCtx`].
pub(crate) struct TransferEquityToHedgingCtx {
    pub(crate) transfer: Arc<dyn ResumeEquityToHedging>,
}

/// Errors emitted by [`TransferEquityToHedging::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferEquityToHedgingJobError {
    #[error(transparent)]
    Transfer(#[from] RedemptionError),
}

/// Apalis job payload for the redemption direction. The `aggregate_id` is
/// generated at enqueue time so retries resume the same aggregate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TransferEquityToHedging {
    pub(crate) aggregate_id: RedemptionAggregateId,
    pub(crate) symbol: Symbol,
    pub(crate) quantity: FractionalShares,
}

impl Job<TransferEquityToHedgingCtx> for TransferEquityToHedging {
    type Output = ();
    type Error = TransferEquityToHedgingJobError;

    const WORKER_NAME: &'static str = "transfer-equity-to-hedging-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::TransferEquityToHedging;

    fn label(&self) -> Label {
        Label::new(format!("TransferEquityToHedging:{}", self.aggregate_id))
    }

    async fn perform(&self, ctx: &TransferEquityToHedgingCtx) -> Result<Self::Output, Self::Error> {
        ctx.transfer
            .resume_equity_to_hedging(&self.aggregate_id, &self.symbol, self.quantity)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use alloy::primitives::{Address, TxHash, U256};
    use serde_json::json;
    use st0x_config::{EquityAssetConfig, OperationMode};
    use st0x_event_sorcery::test_store;
    use st0x_float_macro::float;
    use st0x_tokenization::{issuer_request_id, tokenization_request_id};

    use super::*;
    use crate::equity_redemption::redemption_aggregate_id;
    use crate::rebalancing::equity::MintError;
    use crate::tokenized_equity_mint::TokenizedEquityMintCommand;

    /// Builds a test ctx with recovery enabled for AAPL and an empty guard map.
    async fn test_ctx(
        transfer: Arc<dyn ResumeEquityToMarketMaking>,
    ) -> TransferEquityToMarketMakingCtx {
        test_ctx_with_recovery(transfer, OperationMode::Enabled).await
    }

    /// Builds a test ctx with the given `wrapped_equity_recovery` mode for AAPL.
    async fn test_ctx_with_recovery(
        transfer: Arc<dyn ResumeEquityToMarketMaking>,
        recovery_mode: OperationMode,
    ) -> TransferEquityToMarketMakingCtx {
        let (pool, _apalis_pool) = crate::test_utils::setup_test_pools().await;
        let mint_store = Arc::new(test_store(pool, ()));

        let aapl_config = EquityAssetConfig {
            tokenized_equity: Address::ZERO,
            tokenized_equity_derivative: Address::ZERO,
            pyth_feed_id: None,
            vault_ids: vec![],
            trading: OperationMode::Disabled,
            rebalancing: OperationMode::Enabled,
            wrapped_equity_recovery: recovery_mode,
            extended_hours_counter_trading: OperationMode::Disabled,
            operational_limit: None,
        };
        let mut equities_config = EquitiesConfig::default();
        equities_config
            .symbols
            .insert(Symbol::new("AAPL").unwrap(), aapl_config);

        TransferEquityToMarketMakingCtx {
            transfer,
            equity_in_progress: Arc::new(RwLock::new(HashMap::new())),
            mint_store,
            equities_config,
        }
    }

    /// Records the resume call and returns a configurable outcome, so the
    /// job's `perform` can be tested without broker/onchain setup.
    struct RecordingResume {
        outcome: ResumeOutcome,
        captured: Mutex<Option<(IssuerRequestId, Symbol, FractionalShares)>>,
    }

    enum ResumeOutcome {
        Success,
        PreReceipt,
        PostReceipt,
    }

    impl RecordingResume {
        fn success() -> Self {
            Self {
                outcome: ResumeOutcome::Success,
                captured: Mutex::new(None),
            }
        }

        fn pre_receipt_failure() -> Self {
            Self {
                outcome: ResumeOutcome::PreReceipt,
                captured: Mutex::new(None),
            }
        }

        fn post_receipt_failure() -> Self {
            Self {
                outcome: ResumeOutcome::PostReceipt,
                captured: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl ResumeEquityToMarketMaking for RecordingResume {
        async fn resume_equity_to_market_making(
            &self,
            issuer_request_id: &IssuerRequestId,
            symbol: &Symbol,
            quantity: FractionalShares,
        ) -> Result<(), MintTransferError> {
            *self.captured.lock().unwrap() =
                Some((issuer_request_id.clone(), symbol.clone(), quantity));
            match self.outcome {
                ResumeOutcome::Success => Ok(()),
                ResumeOutcome::PreReceipt => {
                    Err(MintTransferError::PreReceipt(MintError::EntityNotFound {
                        issuer_request_id: issuer_request_id.clone(),
                        expected_state: "test-induced-pre",
                    }))
                }
                ResumeOutcome::PostReceipt => {
                    Err(MintTransferError::PostReceipt(MintError::EntityNotFound {
                        issuer_request_id: issuer_request_id.clone(),
                        expected_state: "test-induced-post",
                    }))
                }
            }
        }
    }

    #[tokio::test]
    async fn perform_forwards_id_symbol_and_quantity_to_resume() {
        let stub = Arc::new(RecordingResume::success());
        let ctx = test_ctx(Arc::clone(&stub) as Arc<dyn ResumeEquityToMarketMaking>).await;
        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_request_id("mint-forward"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            generation: 0,
        };

        Job::perform(&job, &ctx).await.unwrap();

        let captured = stub.captured.lock().unwrap().take();
        let (issuer_request_id, symbol, quantity) =
            captured.expect("perform must call resume_equity_to_market_making");
        assert_eq!(issuer_request_id, job.issuer_request_id);
        assert_eq!(symbol, job.symbol);
        assert_eq!(quantity, job.quantity);
    }

    #[tokio::test]
    async fn perform_propagates_pre_receipt_failure() {
        let ctx = test_ctx(Arc::new(RecordingResume::pre_receipt_failure())).await;
        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_request_id("mint-fail"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            generation: 0,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        // PreReceipt must propagate so apalis retries and the guard stays latched
        // until the aggregate reaches a terminal state.
        assert!(matches!(
            error,
            TransferEquityToMarketMakingJobError::Transfer(MintTransferError::PreReceipt(_))
        ));
    }

    /// PostReceipt with the aggregate ABSENT from the store (never persisted,
    /// or already cleaned up) must propagate as Err so apalis retries.
    /// Note: only `TokensReceived` and `WrapSubmitted` transition the guard to
    /// `HeldForRecovery` and return `Ok(())` when recovery is enabled.
    /// `TokensWrapped` and `VaultDepositSubmitted` instead propagate Err for
    /// apalis retry (the deposit stage is idempotent).
    #[tokio::test]
    async fn perform_post_receipt_error_propagates_when_aggregate_absent() {
        // The mint aggregate is absent in the store (never persisted) — the
        // error must propagate for apalis retry.
        let ctx = test_ctx(Arc::new(RecordingResume::post_receipt_failure())).await;
        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_request_id("test-post-receipt"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            generation: 0,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                TransferEquityToMarketMakingJobError::Transfer(MintTransferError::PostReceipt(_))
            ),
            "PostReceipt on absent aggregate must propagate for apalis retry, got {error:?}"
        );

        // Guard must not transition (it stays unchanged from before the call).
        assert!(
            !ctx.equity_in_progress
                .read()
                .unwrap()
                .contains_key(&Symbol::new("AAPL").unwrap()),
            "guard must not be modified when aggregate is absent from the store"
        );
    }

    /// Seeds the mint store to `TokensWrapped` state by driving the commands
    /// needed to reach that state without any real broker/RPC calls.
    async fn seed_tokens_wrapped(
        ctx: &TransferEquityToMarketMakingCtx,
        issuer_id: &IssuerRequestId,
        symbol: &Symbol,
    ) {
        ctx.mint_store
            .send(
                issuer_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("seed-tokens-wrapped"),
                },
            )
            .await
            .expect("RecordMintRequested must persist and accept the mint");

        ctx.mint_store
            .send(
                issuer_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::repeat_byte(0x11)),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .expect("RecordTokensReceived must transition to TokensReceived");

        ctx.mint_store
            .send(
                issuer_id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: TxHash::ZERO,
                    wrapped_shares: U256::from(5u64),
                    wrap_block: 1,
                },
            )
            .await
            .expect("WrapTokens must transition to TokensWrapped");
    }

    /// `PostReceipt` error with aggregate in `TokensWrapped` state must
    /// propagate `Err` so apalis retries the transfer job. `TokensWrapped` is
    /// excluded from the `HeldForRecovery` handoff: the deposit stage is
    /// idempotent and `WrappedEquityRecovery` is orphan-only (absent guard).
    #[tokio::test]
    async fn perform_post_receipt_with_tokens_wrapped_propagates_err_for_apalis_retry() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-wrapped");
        let ctx = test_ctx(Arc::new(RecordingResume::post_receipt_failure())).await;

        // Pre-seed the guard as ActiveTransfer (as the live transfer job would).
        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 0 });

        // Seed the mint store to TokensWrapped.
        seed_tokens_wrapped(&ctx, &issuer_id, &symbol).await;

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        // TokensWrapped propagates Err: apalis retries resume_mint (idempotent
        // vault deposit). WrappedEquityRecovery is orphan-only; no handoff.
        let error = Job::perform(&job, &ctx).await.unwrap_err();
        assert!(
            matches!(
                error,
                TransferEquityToMarketMakingJobError::Transfer(MintTransferError::PostReceipt(_))
            ),
            "TokensWrapped must propagate PostReceipt for apalis retry, got {error:?}"
        );

        // Guard must remain ActiveTransfer — no HeldForRecovery handoff.
        assert!(
            matches!(
                ctx.equity_in_progress.read().unwrap().get(&symbol),
                Some(GuardState::ActiveTransfer { .. })
            ),
            "TokensWrapped must not transition guard to HeldForRecovery"
        );
    }

    /// `PostReceipt` error with aggregate in `VaultDepositSubmitted` state must
    /// propagate `Err` so apalis retries the transfer job, which idempotently
    /// confirms the submitted deposit. `VaultDepositSubmitted` is intentionally
    /// excluded from the `HeldForRecovery` handoff: the deposit tx is already
    /// on-chain and the wallet balance is zero (tokens are in the vault), so
    /// `WrappedEquityRecovery` would find no balance and skip.
    #[tokio::test]
    async fn perform_post_receipt_with_vault_deposit_submitted_propagates_err_for_apalis_retry() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-deposit-submitted");
        let ctx = test_ctx(Arc::new(RecordingResume::post_receipt_failure())).await;

        // Pre-seed the guard as ActiveTransfer.
        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 0 });

        // Seed through TokensWrapped, then SubmitVaultDeposit to reach
        // VaultDepositSubmitted state.
        seed_tokens_wrapped(&ctx, &issuer_id, &symbol).await;

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::SubmitVaultDeposit {
                    vault_deposit_tx_hash: TxHash::ZERO,
                },
            )
            .await
            .expect("SubmitVaultDeposit must transition to VaultDepositSubmitted");

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        // VaultDepositSubmitted propagates Err so apalis retries the transfer job,
        // which idempotently confirms the submitted deposit. Recovery is not used
        // because the deposit tx is already on-chain and inventory won't show
        // a positive wrapped wallet balance once the deposit landed.
        let error = Job::perform(&job, &ctx).await.unwrap_err();
        assert!(
            matches!(
                error,
                TransferEquityToMarketMakingJobError::Transfer(MintTransferError::PostReceipt(_))
            ),
            "VaultDepositSubmitted must propagate PostReceipt for apalis retry, got {error:?}"
        );

        // Guard must remain ActiveTransfer — the apalis retry of the transfer job
        // handles the confirmation; no HeldForRecovery handoff.
        assert!(
            matches!(
                ctx.equity_in_progress.read().unwrap().get(&symbol),
                Some(GuardState::ActiveTransfer { .. })
            ),
            "VaultDepositSubmitted must not transition guard to HeldForRecovery"
        );
    }

    /// `PostReceipt` error with aggregate in `TokensReceived` state (wrap not
    /// attempted yet) must transition the guard to `HeldForRecovery` and return
    /// `Ok(())`. This is the production RAI-1070 failure path: the ERC-4626 wrap
    /// reverted, tokens remain UNWRAPPED in the base wallet, and
    /// `UnwrappedEquityRecovery` is the correct job to re-attempt the wrap+deposit.
    #[tokio::test]
    async fn perform_post_receipt_with_tokens_received_transitions_guard_to_held_for_recovery() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-tokens-received");
        let ctx = test_ctx(Arc::new(RecordingResume::post_receipt_failure())).await;

        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 0 });

        // Drive the aggregate to TokensReceived (RecordMintRequested +
        // RecordTokensReceived) but do NOT wrap.
        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("seed-tokens-received"),
                },
            )
            .await
            .expect("RecordMintRequested must persist and accept the mint");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::repeat_byte(0x11)),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .expect("RecordTokensReceived must transition to TokensReceived");

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        // Must return Ok(()) — apalis does not retry; UnwrappedEquityRecovery takes over.
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            ctx.equity_in_progress.read().unwrap().get(&symbol),
            Some(&GuardState::HeldForRecovery),
            "guard must transition to HeldForRecovery for TokensReceived so \
             UnwrappedEquityRecovery can resume the wrap+deposit"
        );
    }

    /// `PostReceipt` error with aggregate in `WrapSubmitted` state (wrap tx submitted
    /// but not yet confirmed) must transition the guard to `HeldForRecovery` and
    /// return `Ok(())`. This is the primary production scenario for RAI-1070: the
    /// wrap tx was submitted but confirmation failed; tokens are UNWRAPPED in the
    /// base wallet and `UnwrappedEquityRecovery` is the correct retry path.
    #[tokio::test]
    async fn perform_post_receipt_with_wrap_submitted_transitions_guard_to_held_for_recovery() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-wrap-submitted");
        let ctx = test_ctx(Arc::new(RecordingResume::post_receipt_failure())).await;

        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 0 });

        // Drive to TokensReceived then SubmitWrap to reach WrapSubmitted.
        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("seed-tokens-received"),
                },
            )
            .await
            .expect("RecordMintRequested must persist and accept the mint");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::repeat_byte(0x11)),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .expect("RecordTokensReceived must transition to TokensReceived");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: TxHash::ZERO,
                },
            )
            .await
            .expect("SubmitWrap must transition to WrapSubmitted");

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        // Must return Ok(()) — apalis does not retry; UnwrappedEquityRecovery takes over.
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            ctx.equity_in_progress.read().unwrap().get(&symbol),
            Some(&GuardState::HeldForRecovery),
            "guard must transition to HeldForRecovery for WrapSubmitted so \
             UnwrappedEquityRecovery can confirm/resume the wrap+deposit"
        );
    }

    /// `PostReceipt` error where the timeout sweeper cleared this job's slot and
    /// a NEW transfer reclaimed it (a different `generation`) must return `Ok(())`
    /// WITHOUT touching the new transfer's slot. `mark_held_for_recovery` detects
    /// the generation mismatch under the write lock, so no `HeldForRecovery`
    /// handoff happens and apalis marks this stale job Done instead of clobbering
    /// the live claim.
    #[tokio::test]
    async fn perform_post_receipt_generation_mismatch_leaves_new_claim_untouched() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-generation-mismatch");
        let ctx = test_ctx(Arc::new(RecordingResume::post_receipt_failure())).await;

        // This job holds generation 0.
        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 0 });

        // Drive the aggregate to TokensReceived (a recoverable pre-wrap state).
        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("seed-tokens-received"),
                },
            )
            .await
            .expect("RecordMintRequested must persist and accept the mint");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::repeat_byte(0x11)),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .expect("RecordTokensReceived must transition to TokensReceived");

        // Simulate the timeout sweeper clearing this job's slot and a NEW
        // transfer reclaiming it with a fresh generation before PostReceipt runs.
        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 1 });

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        // Must return Ok(()) so apalis marks the stale job Done -- no retry.
        Job::perform(&job, &ctx).await.unwrap();

        // The new transfer's slot (generation 1) must be left untouched: no
        // HeldForRecovery handoff, no removal.
        assert_eq!(
            ctx.equity_in_progress.read().unwrap().get(&symbol),
            Some(&GuardState::ActiveTransfer { generation: 1 }),
            "a generation mismatch must leave the new transfer's ActiveTransfer slot untouched",
        );
    }

    /// When `wrapped_equity_recovery` is disabled for the symbol, a `PostReceipt`
    /// error must propagate as `Err` so apalis retries — no stranding, no guard
    /// transition. The `HeldForRecovery` handoff is only valid when a recovery job
    /// will actually be enqueued to claim the slot.
    #[tokio::test]
    async fn perform_post_receipt_propagates_when_recovery_disabled() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-recovery-disabled");
        let ctx = test_ctx_with_recovery(
            Arc::new(RecordingResume::post_receipt_failure()),
            OperationMode::Disabled,
        )
        .await;

        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 0 });

        // Drive to TokensReceived so the aggregate IS in a recoverable state,
        // but recovery is disabled — the error must still propagate.
        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("seed-tokens-received"),
                },
            )
            .await
            .expect("RecordMintRequested must persist and accept the mint");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::repeat_byte(0x11)),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .expect("RecordTokensReceived must transition to TokensReceived");

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                TransferEquityToMarketMakingJobError::Transfer(MintTransferError::PostReceipt(_))
            ),
            "PostReceipt must propagate as Err when recovery is disabled, got {error:?}"
        );

        // Guard must remain ActiveTransfer — no handoff to recovery.
        assert!(
            matches!(
                ctx.equity_in_progress.read().unwrap().get(&symbol),
                Some(GuardState::ActiveTransfer { .. })
            ),
            "guard must not be modified to HeldForRecovery when recovery is disabled"
        );
    }

    /// `PostReceipt` error with the mint aggregate already in a terminal state
    /// (e.g. `Failed`) must propagate as `Err` — not transition the guard to
    /// `HeldForRecovery`. If this were allowed, a finished mint's symbol would be
    /// permanently blocked for future transfers.
    #[tokio::test]
    async fn perform_post_receipt_error_propagates_when_aggregate_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-terminal");
        let ctx = test_ctx_with_recovery(
            Arc::new(RecordingResume::post_receipt_failure()),
            OperationMode::Enabled,
        )
        .await;

        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 0 });

        // Drive the aggregate to TokensReceived, then fail it (terminal Failed state).
        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("seed-tokens-received"),
                },
            )
            .await
            .expect("RecordMintRequested must persist and accept the mint");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::repeat_byte(0x11)),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .expect("RecordTokensReceived must transition to TokensReceived");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::FailWrapping {
                    reason: "test: forced terminal state".to_string(),
                },
            )
            .await
            .expect("FailWrapping must transition to terminal Failed");

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        // Terminal aggregate must propagate Err — it is not a recoverable state.
        let error = Job::perform(&job, &ctx).await.unwrap_err();
        assert!(
            matches!(
                error,
                TransferEquityToMarketMakingJobError::Transfer(MintTransferError::PostReceipt(_))
            ),
            "PostReceipt on terminal aggregate must propagate Err, got {error:?}"
        );

        // Guard must remain ActiveTransfer — terminal aggregate propagates Err,
        // so no HeldForRecovery handoff occurs and the guard is unchanged.
        assert!(
            matches!(
                ctx.equity_in_progress.read().unwrap().get(&symbol),
                Some(GuardState::ActiveTransfer { .. })
            ),
            "terminal aggregate must leave guard unchanged as ActiveTransfer"
        );
    }

    /// `PostReceipt` error with guard already `HeldForRecovery` (idempotent
    /// re-entry after a retry) must still return `Ok(())` without double-
    /// transitioning or panicking.
    #[tokio::test]
    async fn perform_post_receipt_idempotent_when_guard_already_held_for_recovery() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-idempotent");
        let ctx = test_ctx(Arc::new(RecordingResume::post_receipt_failure())).await;

        // Pre-seed the guard as HeldForRecovery (as if a prior attempt already
        // ran and set the guard before crashing).
        ctx.equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::HeldForRecovery);

        // Drive to TokensReceived so the aggregate IS in a recoverable state.
        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("seed-tokens-received"),
                },
            )
            .await
            .expect("RecordMintRequested must persist and accept the mint");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::repeat_byte(0x11)),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .expect("RecordTokensReceived must transition to TokensReceived");

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        // Idempotent: guard already held, state is recoverable -> still Ok(()).
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            ctx.equity_in_progress.read().unwrap().get(&symbol),
            Some(&GuardState::HeldForRecovery),
            "idempotent re-entry must leave guard in HeldForRecovery"
        );
    }

    /// `PostReceipt` error with the guard absent (e.g. cleared by timeout
    /// sweeper) must propagate `Err` so apalis retries. Tokens remain in the
    /// wallet; the next inventory poll re-triggers recovery via orphan path.
    #[tokio::test]
    async fn perform_post_receipt_propagates_err_when_guard_entry_absent() {
        let symbol = Symbol::new("AAPL").unwrap();
        let issuer_id = issuer_request_id("post-receipt-guard-absent");
        let ctx = test_ctx(Arc::new(RecordingResume::post_receipt_failure())).await;

        // Guard is absent (not seeded) — simulates timeout sweeper clearing it.
        // Drive to TokensReceived so the aggregate IS in a recoverable state;
        // the absent guard is the only reason the handoff must fail.
        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: issuer_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("seed-tokens-received"),
                },
            )
            .await
            .expect("RecordMintRequested must persist and accept the mint");

        ctx.mint_store
            .send(
                &issuer_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(TxHash::repeat_byte(0x11)),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .expect("RecordTokensReceived must transition to TokensReceived");

        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_id.clone(),
            symbol: symbol.clone(),
            quantity: FractionalShares::new(float!(5)),
            generation: 0,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();
        assert!(
            matches!(
                error,
                TransferEquityToMarketMakingJobError::Transfer(MintTransferError::PostReceipt(_))
            ),
            "absent guard must propagate PostReceipt for apalis retry, got {error:?}"
        );

        assert!(
            !ctx.equity_in_progress.read().unwrap().contains_key(&symbol),
            "absent guard must remain absent after failed handoff"
        );
    }

    #[test]
    fn payload_roundtrips_through_json() {
        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_request_id("mint-roundtrip"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(2.5)),
            generation: 0,
        };

        let expected = json!({
            "issuer_request_id": issuer_request_id("mint-roundtrip").to_string(),
            "symbol": "AAPL",
            "quantity": "2.5",
            "generation": 0_u64,
        });

        assert_eq!(serde_json::to_value(&job).unwrap(), expected);

        let roundtripped: TransferEquityToMarketMaking = serde_json::from_value(expected).unwrap();

        assert_eq!(roundtripped.issuer_request_id, job.issuer_request_id);
        assert_eq!(roundtripped.symbol, job.symbol);
        assert_eq!(roundtripped.quantity, job.quantity);
        assert_eq!(roundtripped.generation, job.generation);

        // Old rows without the generation field must deserialize to generation=0.
        let legacy_payload = json!({
            "issuer_request_id": issuer_request_id("mint-roundtrip").to_string(),
            "symbol": "AAPL",
            "quantity": "2.5",
        });
        let legacy: TransferEquityToMarketMaking = serde_json::from_value(legacy_payload).unwrap();
        assert_eq!(legacy.generation, 0, "missing generation must default to 0");
    }

    /// Records the redemption resume call and returns a configurable outcome.
    struct RecordingRedemptionResume {
        fail: bool,
        captured: Mutex<Option<(RedemptionAggregateId, Symbol, FractionalShares)>>,
    }

    #[async_trait]
    impl ResumeEquityToHedging for RecordingRedemptionResume {
        async fn resume_equity_to_hedging(
            &self,
            aggregate_id: &RedemptionAggregateId,
            symbol: &Symbol,
            quantity: FractionalShares,
        ) -> Result<(), RedemptionError> {
            *self.captured.lock().unwrap() = Some((aggregate_id.clone(), symbol.clone(), quantity));

            if self.fail {
                Err(RedemptionError::EntityNotFound {
                    aggregate_id: aggregate_id.clone(),
                })
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn redemption_perform_forwards_id_symbol_and_quantity_to_resume() {
        let stub = Arc::new(RecordingRedemptionResume {
            fail: false,
            captured: Mutex::new(None),
        });
        let ctx = TransferEquityToHedgingCtx {
            transfer: stub.clone(),
        };
        let job = TransferEquityToHedging {
            aggregate_id: redemption_aggregate_id("redeem-forward"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
        };

        Job::perform(&job, &ctx).await.unwrap();

        let captured = stub.captured.lock().unwrap().take();
        let (aggregate_id, symbol, quantity) =
            captured.expect("perform must call resume_equity_to_hedging");
        assert_eq!(aggregate_id, job.aggregate_id);
        assert_eq!(symbol, job.symbol);
        assert_eq!(quantity, job.quantity);
    }

    #[tokio::test]
    async fn redemption_perform_propagates_resume_failure() {
        let ctx = TransferEquityToHedgingCtx {
            transfer: Arc::new(RecordingRedemptionResume {
                fail: true,
                captured: Mutex::new(None),
            }),
        };
        let job = TransferEquityToHedging {
            aggregate_id: redemption_aggregate_id("redeem-fail"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        // The failure must propagate (not be swallowed) so apalis retries and
        // the event-driven `equity_in_progress` guard stays latched until the
        // aggregate reaches a terminal state.
        assert!(matches!(
            error,
            TransferEquityToHedgingJobError::Transfer(RedemptionError::EntityNotFound { .. })
        ));
    }

    #[test]
    fn redemption_payload_roundtrips_through_json() {
        let job = TransferEquityToHedging {
            aggregate_id: redemption_aggregate_id("redeem-roundtrip"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(2.5)),
        };

        let serialized = serde_json::to_vec(&job).unwrap();
        let deserialized: TransferEquityToHedging = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.aggregate_id, job.aggregate_id);
        assert_eq!(deserialized.symbol, job.symbol);
        assert_eq!(deserialized.quantity, job.quantity);
    }
}
