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
//! Ordinary transient errors propagate so Apalis retries up to three times.
//! Inconclusive withdrawal broadcast/receipt outcomes instead enqueue an
//! uncapped durable replacement before returning success, because chain
//! finality cannot safely consume a finite worker retry budget. Terminal
//! failures are logged without tripping the conductor-wide fail-stop.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use st0x_event_sorcery::SendError;
use st0x_execution::Symbol;
use st0x_tokenization::IssuerRequestId;

use super::job::{
    PositionReservationAuthority, has_live_sibling_equity_transfer, restore_position_reservation,
};
use super::{
    CrossVenueEquityTransfer, MintError, RedemptionError, WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY,
};
#[cfg(test)]
use crate::bot_gas::BotGasReceiptCostEnqueuer;
use crate::bot_gas::redrive::{BotGasFailureClassifier, redrive_on_bot_gas_failure};
use crate::conductor::job::{
    BackpressureStreak, Job, JobQueue, Label, QueuePushError, TaskIdentity,
};
use crate::equity_redemption::RedemptionAggregateId;
use crate::position::{EquityTransferReservationId, Position, PositionCommand};
use crate::position_check::equity_transfer_retry_delay;

/// Apalis queue type for [`ResumeTokenizationAggregate`].
pub(crate) type ResumeTokenizationJobQueue = JobQueue<ResumeTokenizationAggregate>;

/// Delay before re-enqueueing a resume job after a bot-gas receipt cost
/// enqueue failure. Mirrors `BOT_GAS_ENQUEUE_REDRIVE_DELAY` in the equity
/// transfer jobs: the preceding on-chain step already succeeded, so this
/// only rides out a transient apalis/SQLite write failure before the resume
/// re-derives the same enqueue call from state.
const BOT_GAS_ENQUEUE_REDRIVE_DELAY: Duration = Duration::from_secs(30);

/// Which interrupted aggregate should be resumed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum ResumeTokenizationTarget {
    Mint(IssuerRequestId),
    Redemption(RedemptionAggregateId),
}

/// Names the aggregate the way an operator reads it in a refusal or a log:
/// the kind first, then the id it was persisted under.
impl fmt::Display for ResumeTokenizationTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mint(issuer_request_id) => write!(formatter, "mint {issuer_request_id}"),
            Self::Redemption(aggregate_id) => write!(formatter, "redemption {aggregate_id}"),
        }
    }
}

/// Apalis job payload. Holds the target aggregate to resume.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ResumeTokenizationAggregate {
    pub(crate) target: ResumeTokenizationTarget,
    /// Symbol whose durable Position reservation this resume owns. Legacy rows
    /// written before this field existed deserialize as `None` and are
    /// discarded fail-closed; startup enqueues a fresh symbol-bearing row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) symbol: Option<Symbol>,
    /// Count of consecutive broker rate-limit (429) reschedules leading up to
    /// this attempt (RAI-1494). `#[serde(default)]` so a row enqueued under
    /// the pre-this-change payload shape still deserializes to `0` instead of
    /// crashing the poll stream's `sqlx::Decode`.
    ///
    /// NOT YET wired to a reschedule: mirrors `TransferEquityToMarketMaking`'s
    /// identical gap (see its field doc) -- `resume_mint`/`resume_redemption`
    /// dispatch to the same `TokenizedEquityMint`/`EquityRedemption` command
    /// handlers, which convert tokenizer failures to
    /// `{ error_message: String }` fields via `error.to_string()`, discarding
    /// the original error type before it reaches this job. Field added now
    /// so the payload schema is ready when that follow-up lands.
    #[serde(default)]
    pub(crate) backpressure_streak: BackpressureStreak,
    /// Number of consecutive reservation-restoration deferrals. Persisted in
    /// the replacement payload so a long-lived hedge backs off across rows.
    #[serde(default)]
    pub(crate) position_reservation_retry_attempts: u32,
}

/// Dependencies the job needs.
///
/// `transfer` carries every hedged chain's services, so a resume resolves
/// the chain its record names rather than assuming the primary.
pub(crate) struct ResumeTokenizationCtx {
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
    /// Position authority holding the reservation that excludes concurrent
    /// hedges while this generic resume drives the aggregate.
    pub(crate) position_authority: PositionReservationAuthority,
    /// Used to delayed-redrive on a bot-gas receipt cost enqueue failure
    /// (ADR 0017 SS4: "failure in cost recording never blocks trading")
    /// instead of consuming the apalis retry budget. This is the startup
    /// crash-recovery job, running while SQLite write contention is at its
    /// worst -- see `redrive_on_bot_gas_failure`'s doc for why every job
    /// that can hit this failure must route through it.
    pub(crate) job_queue: ResumeTokenizationJobQueue,
}

/// Errors emitted by [`ResumeTokenizationAggregate::perform`].
#[derive(Debug, Error)]
pub(crate) enum ResumeTokenizationJobError {
    #[error(transparent)]
    Mint(#[from] MintError),
    #[error(transparent)]
    Redemption(#[from] RedemptionError),
    #[error(transparent)]
    PositionReservation(#[from] SendError<Position>),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
}

impl ResumeTokenizationJobError {
    fn is_reconciliation_pending(&self) -> bool {
        matches!(self, Self::Redemption(error) if error.is_reconciliation_pending())
    }
}

impl BotGasFailureClassifier for ResumeTokenizationJobError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::Mint(inner) => inner.is_bot_gas_enqueue_failure(),
            Self::Redemption(inner) => inner.is_bot_gas_enqueue_failure(),
            Self::PositionReservation(_) | Self::Enqueue(_) => false,
        }
    }
}

impl Job<ResumeTokenizationCtx> for ResumeTokenizationAggregate {
    type Output = ();
    type Error = ResumeTokenizationJobError;

    const WORKER_NAME: &'static str = "resume-tokenization-aggregate-worker";
    /// No perform-level bound (`None`): the redemption resume path drives
    /// `SendPending` -> `send_for_redemption`, an on-chain token transfer with
    /// no pre-send persisted guard. `perform_bounded` dropping it after the
    /// transfer broadcast but before `TokensSent` persists would, on retry,
    /// re-enter `SendPending` and send the tokens a second time. `resume_mint`
    /// (the other target) is idempotent, so leaving the whole job unbounded is
    /// safe; actual hangs are bounded by the HTTP/RPC transport timeouts.
    const PERFORM_TIMEOUT: Option<std::time::Duration> = None;
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
        let Some(symbol) = &self.symbol else {
            warn!(
                target: "tokenization",
                resume_target = %self.target,
                "Discarding legacy symbol-less tokenization resume row; startup owns the guarded replacement"
            );
            return Ok(());
        };
        let (position_store, position_threshold) = &ctx.position_authority;
        let reservation_id = match &self.target {
            ResumeTokenizationTarget::Mint(id) => EquityTransferReservationId::from_uuid(id.0),
            ResumeTokenizationTarget::Redemption(id) => {
                EquityTransferReservationId::from_uuid(id.0)
            }
        };
        if !restore_position_reservation(
            position_store,
            symbol,
            *position_threshold,
            reservation_id,
        )
        .await?
        {
            let retry_delay = equity_transfer_retry_delay(self.position_reservation_retry_attempts);
            let mut retry = self.clone();
            retry.position_reservation_retry_attempts =
                self.position_reservation_retry_attempts.saturating_add(1);
            let mut job_queue = ctx.job_queue.clone();
            job_queue.push_with_delay(retry, retry_delay).await?;
            return Ok(());
        }

        let result = match &self.target {
            ResumeTokenizationTarget::Mint(issuer_request_id) => ctx
                .transfer
                .resume_mint(issuer_request_id)
                .await
                .map_err(ResumeTokenizationJobError::from),
            ResumeTokenizationTarget::Redemption(aggregate_id) => ctx
                .transfer
                .resume_redemption(aggregate_id)
                .await
                .map_err(ResumeTokenizationJobError::from),
        };

        let Err(error) = result else {
            position_store
                .send(
                    symbol,
                    PositionCommand::ReleaseEquityTransfer { reservation_id },
                )
                .await?;
            return Ok(());
        };

        if error.is_reconciliation_pending() {
            warn!(
                target: "tokenization",
                resume_target = %self.target,
                "Withdrawal reconciliation remains inconclusive; scheduling a durable resume"
            );
            let mut job_queue = ctx.job_queue.clone();
            job_queue
                .push_with_delay(self.clone(), WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY)
                .await?;
            return Ok(());
        }

        // Bot-gas cost recording is best-effort (see `BotGasReceiptCostEnqueuer`'s
        // doc, ADR 0017 SS4): redrive through the shared mechanism rather than
        // consuming the apalis retry budget. See `ResumeTokenizationCtx::job_queue`'s
        // doc for why this matters especially for this startup crash-recovery job.
        redrive_on_bot_gas_failure(self, &ctx.job_queue, BOT_GAS_ENQUEUE_REDRIVE_DELAY, error).await
    }

    async fn on_terminal_attempt(
        &self,
        ctx: &ResumeTokenizationCtx,
        task_identity: &TaskIdentity,
    ) -> Result<(), apalis_core::error::BoxDynError> {
        let Some(symbol) = &self.symbol else {
            return Ok(());
        };
        let target_is_live = match &self.target {
            ResumeTokenizationTarget::Mint(id) => ctx
                .transfer
                .mint_store
                .load(id)
                .await?
                .is_some_and(|aggregate| !aggregate.is_terminal()),
            ResumeTokenizationTarget::Redemption(id) => ctx
                .transfer
                .redemption_store
                .load(id)
                .await?
                .is_some_and(|aggregate| !aggregate.is_terminal()),
        };
        if target_is_live {
            warn!(
                target: "tokenization",
                resume_target = %self.target,
                %task_identity,
                "Terminal resume attempt retained its reservation because the target aggregate is live"
            );
            return Ok(());
        }
        if has_live_sibling_equity_transfer::<Self>(
            ctx.job_queue.pool(),
            task_identity,
            |sibling| sibling.target == self.target,
        )
        .await?
        {
            warn!(
                target: "tokenization",
                resume_target = %self.target,
                %task_identity,
                "Terminal resume attempt retained its reservation because a sibling job row is live"
            );
            return Ok(());
        }

        let reservation_id = match &self.target {
            ResumeTokenizationTarget::Mint(id) => EquityTransferReservationId::from_uuid(id.0),
            ResumeTokenizationTarget::Redemption(id) => {
                EquityTransferReservationId::from_uuid(id.0)
            }
        };
        ctx.position_authority
            .0
            .send(
                symbol,
                PositionCommand::ReleaseEquityTransfer { reservation_id },
            )
            .await?;
        warn!(
            target: "tokenization",
            resume_target = %self.target,
            %task_identity,
            "Terminal resume attempt checked its exact Position reservation"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, TxHash, U256};
    use serde_json::json;
    use st0x_config::{ChainEquities, ExecutionThreshold};
    use st0x_event_sorcery::test_store;
    use st0x_evm::Chain;
    use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor};
    use st0x_float_macro::float;
    use st0x_raindex::{Raindex, RaindexVaultId};
    use st0x_tokenization::mock::{MockCompletionOutcome, MockDetectionOutcome, MockTokenizer};
    use st0x_tokenization::{ClientRequestId, issuer_request_id, tokenization_request_id};
    use st0x_wrapper::{MockWrapper, Wrapper};
    use std::collections::BTreeMap;

    use super::*;
    use crate::equity_redemption::{
        EquityRedemption, EquityRedemptionCommand, redemption_aggregate_id,
    };
    use crate::mint_authorization::ConfiguredMintAuthorizer;
    use crate::native_gas::ConfiguredGasReadiness;
    use crate::offchain::order::OffchainOrderId;
    use crate::onchain::mock::{ConfirmTxBehavior, MockRaindex};
    use crate::position::TradeId;
    use crate::rebalancing::equity::ChainEquityServices;
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintCommand};
    use crate::vault_lookup::MockVaultLookup;

    /// Regression guard: `ResumeTokenizationAggregate` must keep
    /// `PERFORM_TIMEOUT = None`. The redemption target drives `SendPending`
    /// -> `send_for_redemption`, an on-chain transfer with no pre-send guard;
    /// `perform_bounded` dropping it mid-send would re-send on retry. Any edit
    /// re-imposing a bound must first make that send re-entry-safe.
    #[test]
    fn resume_job_opts_out_of_perform_timeout() {
        assert_eq!(
            <ResumeTokenizationAggregate as Job<ResumeTokenizationCtx>>::PERFORM_TIMEOUT,
            None,
            "ResumeTokenizationAggregate must not be perform-bounded: the redemption \
             send has no safe re-entry if the future is dropped mid-flight",
        );
    }

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
        let raindex: Arc<dyn Raindex> = Arc::new(
            MockRaindex::new()
                .with_withdraw_transfer(Address::ZERO, U256::from(1_000_000_000_000_000_000_u128)),
        );
        build_ctx_with(tokenizer, raindex).await
    }

    async fn build_ctx_with(
        tokenizer: Arc<MockTokenizer>,
        raindex: Arc<dyn Raindex>,
    ) -> (
        ResumeTokenizationCtx,
        Arc<st0x_event_sorcery::Store<crate::tokenized_equity_mint::TokenizedEquityMint>>,
        Arc<st0x_event_sorcery::Store<EquityRedemption>>,
        Arc<MockTokenizer>,
    ) {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let vault_lookup =
            Arc::new(MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO)));

        let transfer_services = EquityTransferServices {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex,
                    vault_lookup,
                    tokenizer: tokenizer.clone(),
                    wrapper,
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: ChainEquities::default(),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };

        let mint_store = Arc::new(test_store(pool.clone(), transfer_services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), transfer_services.clone()));
        let position_store = Arc::new(test_store(pool, ()));

        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            transfer_services,
            mint_store.clone(),
            redemption_store.clone(),
        ));

        let ctx = ResumeTokenizationCtx {
            transfer,
            position_authority: (position_store, ExecutionThreshold::whole_share()),
            job_queue: ResumeTokenizationJobQueue::new(&apalis_pool),
        };
        (ctx, mint_store, redemption_store, tokenizer)
    }

    async fn submit_requested_mint(
        mint_store: &st0x_event_sorcery::Store<TokenizedEquityMint>,
        id: &IssuerRequestId,
    ) {
        mint_store
            .send(
                id,
                TokenizedEquityMintCommand::SubmitMintRequest {
                    issuer_request_id: id.clone(),
                },
            )
            .await
            .unwrap();
        let mint = mint_store.load(id).await.unwrap();
        assert!(
            matches!(mint, Some(TokenizedEquityMint::MintAccepted { .. })),
            "mint submission must persist acceptance: {mint:?}"
        );
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
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&mint_store, &id).await;

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
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
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
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&mint_store, &id).await;

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
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
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

        // Drive redemption to Completed: Redeem ->
        // RecordWithdrawSubmission -> ConfirmWithdraw -> UnwrapTokens ->
        // SubmitUnwrap -> ConfirmUnwrap -> PrepareSend -> SendTokens ->
        // (TokensSent). Then detect via DetectSend. Mock services complete
        // synchronously.
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    chain: Chain::Base,
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    token: Address::ZERO,
                    vault_id: st0x_raindex::RaindexVaultId(alloy::primitives::B256::ZERO),
                    amount: U256::from(1_000_000_000_000_000_000_u128),
                    from_block: 0,
                    prepared: crate::equity_redemption::prepared_withdrawal_for_test(),
                },
            )
            .await
            .unwrap();

        // Drive through intermediate states to SendPending.
        for cmd in [
            EquityRedemptionCommand::RecordWithdrawSubmission {
                tx_hash: alloy::primitives::TxHash::ZERO,
            },
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
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
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

        // Persist intent, then submit once to leave a non-terminal
        // MintAccepted aggregate -- the interrupted state.
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&mint_store, &id).await;

        let seeded = mint_store.load(&id).await.unwrap();
        assert!(
            matches!(seeded, Some(TokenizedEquityMint::MintAccepted { .. })),
            "seed must be a non-terminal MintAccepted aggregate, got {seeded:?}"
        );

        let calls_before = tokenizer.call_count();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id.clone()),
            symbol: Some(symbol.clone()),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
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

    #[tokio::test]
    async fn inconclusive_withdrawal_confirmation_durably_reschedules_resume() {
        let raindex =
            Arc::new(MockRaindex::new().with_confirm_behavior(ConfirmTxBehavior::Retryable));
        let tokenizer = Arc::new(MockTokenizer::new());
        let (ctx, _, redemption_store, _) = build_ctx_with(tokenizer, raindex.clone()).await;
        let id = redemption_aggregate_id("resume-withdrawal-reconciliation");
        let symbol = Symbol::new("AAPL").unwrap();
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    chain: Chain::Base,
                    symbol: symbol.clone(),
                    quantity: float!(1),
                    token: Address::ZERO,
                    vault_id: RaindexVaultId(B256::ZERO),
                    amount: U256::from(1_000_000_000_000_000_000_u128),
                    from_block: 0,
                    prepared: crate::equity_redemption::prepared_withdrawal_for_test(),
                },
            )
            .await
            .unwrap();
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(id.clone()),
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };

        Job::perform(&job, &ctx)
            .await
            .expect("inconclusive reconciliation must use a durable replacement row");

        assert_eq!(raindex.withdraw_submissions(), 1);
        assert!(matches!(
            redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::VaultWithdrawSubmitted {
                tx_hash: TxHash::ZERO,
                ..
            })
        ));
        let (payload, run_at): (Vec<u8>, i64) = sqlx_apalis::query_as(
            "SELECT job, run_at FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<ResumeTokenizationAggregate>())
        .fetch_one(ctx.job_queue.pool())
        .await
        .unwrap();
        let replacement: ResumeTokenizationAggregate = serde_json::from_slice(&payload).unwrap();
        assert!(matches!(
            replacement.target,
            ResumeTokenizationTarget::Redemption(replacement_id) if replacement_id == id
        ));
        let now = chrono::Utc::now().timestamp();
        assert!(
            run_at
                >= now + i64::try_from(WITHDRAWAL_RECONCILIATION_REDRIVE_DELAY.as_secs()).unwrap()
                    - 5
        );
    }

    #[tokio::test]
    async fn lost_withdrawal_broadcast_response_durably_reschedules_resume() {
        let raindex = Arc::new(MockRaindex::new().accepting_withdraw_then_losing_response());
        let tokenizer = Arc::new(MockTokenizer::new());
        let (ctx, _, redemption_store, _) = build_ctx_with(tokenizer, raindex.clone()).await;
        let id = redemption_aggregate_id("resume-lost-withdrawal-response");
        let symbol = Symbol::new("AAPL").unwrap();
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    chain: Chain::Base,
                    symbol: symbol.clone(),
                    quantity: float!(1),
                    token: Address::ZERO,
                    vault_id: RaindexVaultId(B256::ZERO),
                    amount: U256::from(1_000_000_000_000_000_000_u128),
                    from_block: 0,
                    prepared: crate::equity_redemption::prepared_withdrawal_for_test(),
                },
            )
            .await
            .unwrap();
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(id.clone()),
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };

        Job::perform(&job, &ctx)
            .await
            .expect("unknown broadcast outcome must use a durable replacement row");

        assert_eq!(raindex.withdraw_submissions(), 1);
        assert!(matches!(
            redemption_store.load(&id).await.unwrap(),
            Some(EquityRedemption::VaultWithdrawSubmitting { prepared, .. })
                if prepared.tx_hash() == TxHash::ZERO
        ));
        let pending_jobs: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<ResumeTokenizationAggregate>())
        .fetch_one(ctx.job_queue.pool())
        .await
        .unwrap();
        assert_eq!(pending_jobs, 1);
    }

    /// `ResumeTokenizationAggregate` is the startup crash-recovery job,
    /// running while SQLite write contention is at its worst. A bot-gas
    /// receipt cost enqueue failure hit during `resume_mint` must
    /// delayed-redrive (return `Ok(())` and push a replacement job), never
    /// dead-letter through apalis's tiny retry budget.
    #[tokio::test]
    async fn perform_mint_target_bot_gas_enqueue_failure_redrives_without_terminal_error() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let closed_apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        closed_apalis_pool.close().await;
        let bot_gas_queue =
            crate::bot_gas::RecordBotGasReceiptCostJobQueue::new(&closed_apalis_pool);

        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let vault_lookup =
            Arc::new(MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO)));
        let tokenizer = Arc::new(MockTokenizer::new());

        let transfer_services = EquityTransferServices {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex: raindex.clone(),
                    vault_lookup: vault_lookup.clone(),
                    tokenizer: tokenizer.clone(),
                    wrapper: wrapper.clone(),
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: ChainEquities::default(),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(bot_gas_queue.clone()),
        };
        let mint_store = Arc::new(test_store(pool.clone(), transfer_services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), transfer_services.clone()));
        let position_store = Arc::new(test_store(pool, ()));

        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            EquityTransferServices {
                bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Enabled(bot_gas_queue),
                ..transfer_services.clone()
            },
            mint_store.clone(),
            redemption_store,
        ));

        let id = issuer_request_id("resume-mint-bot-gas-failure");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        // Seed the mint straight to VaultDepositSubmitted so resume_mint's
        // confirm_tx + enqueue_bot_gas_cost is reached without needing the
        // full wrap flow.
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    chain: Chain::Base,
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        submit_requested_mint(&mint_store, &id).await;
        mint_store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();
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
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitVaultDeposit {
                    vault_deposit_tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        let ctx = ResumeTokenizationCtx {
            transfer,
            position_authority: (position_store, ExecutionThreshold::whole_share()),
            job_queue: ResumeTokenizationJobQueue::new(&apalis_pool),
        };
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id),
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };

        Job::perform(&job, &ctx)
            .await
            .expect("a bot-gas enqueue failure must not fail the resume job terminally");

        let (rescheduled,): (i64,) =
            sqlx_apalis::query_as("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<ResumeTokenizationAggregate>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            rescheduled, 1,
            "a bot-gas enqueue failure must re-enqueue a delayed replacement job"
        );
    }

    /// A redemption-send gas enqueue failure originates in the transfer
    /// manager after `TokensSent` has persisted. Startup recovery must
    /// delayed-redrive that exact error without calling the tokenizer again.
    #[tokio::test]
    async fn perform_redemption_send_bot_gas_enqueue_failure_redrives_without_resend() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let closed_apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        closed_apalis_pool.close().await;
        let bot_gas_queue =
            crate::bot_gas::RecordBotGasReceiptCostJobQueue::new(&closed_apalis_pool);
        let bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(bot_gas_queue);
        let raindex: Arc<dyn Raindex> = Arc::new(
            MockRaindex::new()
                .with_withdraw_transfer(Address::ZERO, U256::from(1_000_000_000_000_000_000_u128)),
        );
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let vault_lookup =
            Arc::new(MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO)));
        let tokenizer = Arc::new(MockTokenizer::new());
        let transfer_services = EquityTransferServices {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex,
                    vault_lookup,
                    tokenizer: tokenizer.clone(),
                    wrapper,
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: ChainEquities::default(),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };
        let mint_store = Arc::new(test_store(pool.clone(), transfer_services.clone()));
        let redemption_store = Arc::new(test_store(pool.clone(), transfer_services.clone()));
        let position_store = Arc::new(test_store(pool, ()));
        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            EquityTransferServices {
                bot_gas_enqueuer,
                ..transfer_services
            },
            mint_store,
            redemption_store.clone(),
        ));
        let id = redemption_aggregate_id("resume-redemption-send-bot-gas-failure");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();
        redemption_store
            .send(
                &id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    chain: Chain::Base,
                    quantity: float!(1.0),
                    token: Address::ZERO,
                    vault_id: st0x_raindex::RaindexVaultId(alloy::primitives::B256::ZERO),
                    amount: U256::from(1_000_000_000_000_000_000_u128),
                    from_block: 0,
                    prepared: crate::equity_redemption::prepared_withdrawal_for_test(),
                },
            )
            .await
            .unwrap();
        for command in [
            EquityRedemptionCommand::RecordWithdrawSubmission {
                tx_hash: alloy::primitives::TxHash::ZERO,
            },
            EquityRedemptionCommand::ConfirmWithdraw,
            EquityRedemptionCommand::UnwrapTokens,
            EquityRedemptionCommand::SubmitUnwrap,
            EquityRedemptionCommand::ConfirmUnwrap,
            EquityRedemptionCommand::PrepareSend,
            EquityRedemptionCommand::SendTokens,
        ] {
            redemption_store.send(&id, command).await.unwrap();
        }
        let calls_before_resume = tokenizer.call_count();
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(id.clone()),
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };
        let ctx = ResumeTokenizationCtx {
            transfer,
            position_authority: (position_store, ExecutionThreshold::whole_share()),
            job_queue: ResumeTokenizationJobQueue::new(&apalis_pool),
        };

        Job::perform(&job, &ctx)
            .await
            .expect("the persisted-send enqueue failure must delayed-redrive");

        assert_eq!(
            tokenizer.call_count(),
            calls_before_resume,
            "accounting redrive must not call the tokenizer or resend tokens"
        );
        let entity = redemption_store.load(&id).await.unwrap();
        assert!(matches!(entity, Some(EquityRedemption::TokensSent { .. })));
        let (payload, run_at): (Vec<u8>, i64) = sqlx_apalis::query_as(
            "SELECT job, run_at FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<ResumeTokenizationAggregate>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();

        let redriven: ResumeTokenizationAggregate = serde_json::from_slice(&payload).unwrap();
        assert!(matches!(
            redriven.target,
            ResumeTokenizationTarget::Redemption(redriven_id) if redriven_id == id
        ));
        let now = chrono::Utc::now().timestamp();
        assert!(
            run_at >= now + i64::try_from(BOT_GAS_ENQUEUE_REDRIVE_DELAY.as_secs()).unwrap() - 5,
            "replacement job must use the bot-gas redrive delay"
        );
    }

    #[tokio::test]
    async fn pending_hedge_backs_off_generic_resume_reservation_restoration() {
        let (ctx, _, _, tokenizer) = build_ctx().await;
        let id = issuer_request_id("resume-deferred-by-hedge");
        let symbol = Symbol::new("AAPL").unwrap();
        let position_store = &ctx.position_authority.0;
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        chain: Chain::Base,
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(10)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: chrono::Utc::now(),
                    block_number: None,
                },
            )
            .await
            .unwrap();
        position_store
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: OffchainOrderId::new(),
                    shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id.clone()),
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 3,
        };

        let calls_before = tokenizer.call_count();
        let scheduled_after = chrono::Utc::now().timestamp();
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            tokenizer.call_count(),
            calls_before,
            "the aggregate must not resume before its Position reservation is restored"
        );
        let (payload, run_at): (Vec<u8>, i64) = sqlx_apalis::query_as(
            "SELECT job, run_at FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<ResumeTokenizationAggregate>())
        .fetch_one(ctx.job_queue.pool())
        .await
        .unwrap();
        let retry: ResumeTokenizationAggregate = serde_json::from_slice(&payload).unwrap();
        assert_eq!(retry.target, ResumeTokenizationTarget::Mint(id));
        assert_eq!(retry.position_reservation_retry_attempts, 4);
        assert!(
            run_at >= scheduled_after + 8,
            "the fourth reservation retry must use the shared 8-second backoff"
        );
    }

    #[tokio::test]
    async fn legacy_symbol_less_resume_is_discarded_before_transfer() {
        let (ctx, _, _, _) = build_ctx().await;
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(issuer_request_id("legacy-symbol-less")),
            symbol: None,
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };

        Job::perform(&job, &ctx)
            .await
            .expect("a legacy symbol-less row must be discarded without invoking transfer");
    }

    /// `perform` on a `Mint` target with a non-existent aggregate propagates
    /// the error so apalis retries.
    #[tokio::test]
    async fn perform_mint_target_propagates_error_for_missing_aggregate() {
        let (ctx, _, _, _tokenizer) = build_ctx().await;
        let id = issuer_request_id("resume-mint-missing");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id),
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
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
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(id),
            symbol: Some(symbol),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
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

    #[tokio::test]
    async fn terminal_cleanup_releases_orphaned_reservation_idempotently() {
        let (ctx, _, _, _) = build_ctx().await;
        let id = issuer_request_id("orphaned-terminal-resume");
        let symbol = Symbol::new("AAPL").unwrap();
        let reservation_id = EquityTransferReservationId::from_uuid(id.0);
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ReserveEquityTransfer {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    reservation_id,
                },
            )
            .await
            .unwrap();
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ConfirmEquityTransfer { reservation_id },
            )
            .await
            .unwrap();
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id),
            symbol: Some(symbol.clone()),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };
        let task_identity =
            crate::conductor::job::TaskIdentity::for_test("orphaned-terminal-resume");

        Job::on_terminal_attempt(&job, &ctx, &task_identity)
            .await
            .unwrap();
        Job::on_terminal_attempt(&job, &ctx, &task_identity)
            .await
            .unwrap();

        let position = ctx
            .position_authority
            .0
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert!(
            position.equity_transfer_reservation.is_none(),
            "a terminal resume with no live aggregate or sibling must release its reservation"
        );
    }

    #[tokio::test]
    async fn terminal_cleanup_preserves_reservation_for_live_target() {
        let (ctx, mint_store, _, _) = build_ctx().await;
        let id = issuer_request_id("live-target-terminal-resume");
        let symbol = Symbol::new("AAPL").unwrap();
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1),
                    chain: Chain::Base,
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        let reservation_id = EquityTransferReservationId::from_uuid(id.0);
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ReserveEquityTransfer {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    reservation_id,
                },
            )
            .await
            .unwrap();
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ConfirmEquityTransfer { reservation_id },
            )
            .await
            .unwrap();
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id),
            symbol: Some(symbol.clone()),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };

        Job::on_terminal_attempt(
            &job,
            &ctx,
            &crate::conductor::job::TaskIdentity::for_test("live-target-terminal-resume"),
        )
        .await
        .unwrap();

        let position = ctx
            .position_authority
            .0
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            position.equity_transfer_reservation,
            Some(crate::position::EquityTransferReservation {
                status: crate::position::EquityTransferReservationStatus::Confirmed,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn terminal_cleanup_preserves_reservation_for_live_sibling() {
        let (ctx, _, _, _) = build_ctx().await;
        let id = issuer_request_id("live-sibling-terminal-resume");
        let symbol = Symbol::new("AAPL").unwrap();
        let reservation_id = EquityTransferReservationId::from_uuid(id.0);
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ReserveEquityTransfer {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    reservation_id,
                },
            )
            .await
            .unwrap();
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ConfirmEquityTransfer { reservation_id },
            )
            .await
            .unwrap();
        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id),
            symbol: Some(symbol.clone()),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };
        let mut queue = ctx.job_queue.clone();
        queue.push(job.clone()).await.unwrap();

        Job::on_terminal_attempt(
            &job,
            &ctx,
            &crate::conductor::job::TaskIdentity::for_test("current-terminal-resume"),
        )
        .await
        .unwrap();

        let position = ctx
            .position_authority
            .0
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            position.equity_transfer_reservation,
            Some(crate::position::EquityTransferReservation {
                status: crate::position::EquityTransferReservationStatus::Confirmed,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn perform_mint_requested_target_reconciles_without_resubmission() {
        let id = issuer_request_id("resume-mint-requested");
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();
        let mut provider_request = st0x_tokenization::TokenizationRequest::mock(
            st0x_tokenization::TokenizationRequestStatus::Pending,
        );
        provider_request.r#type = Some(st0x_tokenization::TokenizationRequestType::Mint);
        provider_request.underlying_symbol = symbol.clone();
        provider_request.quantity = st0x_execution::FractionalShares::new(float!(1.0));
        provider_request.wallet = Some(Address::ZERO);
        provider_request.client_request_id = Some(ClientRequestId::from(&id));
        let provider_request_id = provider_request.id.clone();
        let configured_tokenizer =
            Arc::new(MockTokenizer::new().with_pending_requests(vec![provider_request]));
        let (ctx, mint_store, _, tokenizer) = build_ctx_with_tokenizer(configured_tokenizer).await;

        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    chain: Chain::Base,
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();

        let reservation_id = EquityTransferReservationId::from_uuid(id.0);
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ReserveEquityTransfer {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    reservation_id,
                },
            )
            .await
            .unwrap();
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ConfirmEquityTransfer { reservation_id },
            )
            .await
            .unwrap();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(id.clone()),
            symbol: Some(symbol.clone()),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(tokenizer.mint_lookup_call_count(), 1);
        assert_eq!(tokenizer.mint_request_call_count(), 0);
        let reconciled = mint_store.load(&id).await.unwrap();
        assert!(
            matches!(reconciled,
                Some(TokenizedEquityMint::DepositedIntoRaindex {
                    ref issuer_request_id, ref tokenization_request_id, ..
                }) if issuer_request_id == &id && tokenization_request_id == &provider_request_id
            ),
            "reconciliation must preserve both provider identifiers: {reconciled:?}"
        );
        let position = ctx
            .position_authority
            .0
            .load(&symbol)
            .await
            .unwrap()
            .expect("reservation owner Position must remain");
        assert!(
            position.equity_transfer_reservation.is_none(),
            "a terminal generic resume must release its exact Position reservation"
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
                    chain: Chain::Base,
                    symbol: symbol.clone(),
                    quantity: float!(1.0),
                    token: Address::ZERO,
                    vault_id: st0x_raindex::RaindexVaultId(alloy::primitives::B256::ZERO),
                    amount: U256::from(1_000_000_000_000_000_000_u128),
                    from_block: 0,
                    prepared: crate::equity_redemption::prepared_withdrawal_for_test(),
                },
            )
            .await
            .unwrap();
        for cmd in [
            EquityRedemptionCommand::RecordWithdrawSubmission {
                tx_hash: alloy::primitives::TxHash::ZERO,
            },
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

        let reservation_id = EquityTransferReservationId::from_uuid(id.0);
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ReserveEquityTransfer {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    reservation_id,
                },
            )
            .await
            .unwrap();
        ctx.position_authority
            .0
            .send(
                &symbol,
                PositionCommand::ConfirmEquityTransfer { reservation_id },
            )
            .await
            .unwrap();

        let job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(id.clone()),
            symbol: Some(symbol.clone()),
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
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
        let position = ctx
            .position_authority
            .0
            .load(&symbol)
            .await
            .unwrap()
            .expect("reservation owner Position must remain");
        assert!(
            position.equity_transfer_reservation.is_none(),
            "a terminal generic redemption resume must release its exact reservation"
        );
    }

    /// Job payload for both variants serializes and deserializes correctly.
    #[test]
    fn payload_roundtrips_through_json() {
        let mint_id = issuer_request_id("roundtrip-mint");
        let redemption_id = redemption_aggregate_id("roundtrip-redemption");

        let mint_job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Mint(mint_id.clone()),
            symbol: None,
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };

        let expected_mint = json!({
            "target": { "Mint": mint_id.to_string() },
            "backpressure_streak": 0_u32,
            "position_reservation_retry_attempts": 0_u32,
        });
        assert_eq!(serde_json::to_value(&mint_job).unwrap(), expected_mint);

        let roundtripped_mint: ResumeTokenizationAggregate =
            serde_json::from_value(expected_mint).unwrap();
        assert_eq!(
            roundtripped_mint.target,
            ResumeTokenizationTarget::Mint(mint_id),
            "roundtripped mint target must match original"
        );
        assert_eq!(
            roundtripped_mint.backpressure_streak,
            BackpressureStreak::default()
        );
        assert_eq!(roundtripped_mint.position_reservation_retry_attempts, 0);

        let redemption_job = ResumeTokenizationAggregate {
            target: ResumeTokenizationTarget::Redemption(redemption_id.clone()),
            symbol: None,
            backpressure_streak: BackpressureStreak::default(),
            position_reservation_retry_attempts: 0,
        };

        let expected_redemption = json!({
            "target": { "Redemption": redemption_id.to_string() },
            "backpressure_streak": 0_u32,
            "position_reservation_retry_attempts": 0_u32,
        });
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
        assert_eq!(
            roundtripped_redemption.backpressure_streak,
            BackpressureStreak::default()
        );
        assert_eq!(
            roundtripped_redemption.position_reservation_retry_attempts,
            0
        );
    }

    /// Mandatory RAI-1494 test (M1): a row enqueued before `backpressure_streak`
    /// existed must still deserialize, defaulting the field to `0`.
    #[test]
    fn resume_tokenization_aggregate_payload_without_backpressure_streak_deserializes_to_zero() {
        let mint_id = issuer_request_id("legacy-mint");
        let legacy_payload = json!({ "target": { "Mint": mint_id.to_string() } });

        let job: ResumeTokenizationAggregate = serde_json::from_value(legacy_payload).unwrap();
        assert_eq!(job.backpressure_streak, BackpressureStreak::default());
        assert_eq!(job.position_reservation_retry_attempts, 0);
    }
}
