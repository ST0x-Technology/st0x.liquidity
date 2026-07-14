//! Apalis jobs that drive USDC transfers through the `UsdcRebalance`
//! lifecycle, one per direction.
//!
//! Each job is keyed by a `UsdcRebalanceId` chosen at enqueue time, so apalis
//! retries (and bot restarts that re-pick the row from the Jobs table) hit
//! the same aggregate. The worker calls the trait-erased `resume_*` entry
//! point on the cash transfer, which loads the aggregate via `Store::load`
//! and dispatches on its current state. New transfers and mid-flight resumes
//! share the same entry point — that uniformity is what makes recovery
//! dispatch fall out of the standard transfer lifecycle.
//!
//! The global `usdc_in_progress` guard is cleared event-driven when the
//! aggregate reaches a terminal state (success or a recorded failure), not by
//! this worker. A transient failure that only schedules a retry, or an
//! indeterminate failure that leaves the aggregate mid-flight (e.g. stalled at
//! `WithdrawalSubmitting`/`BridgingSubmitting`), keeps the guard latched so
//! automation does not re-arm a fresh transfer on top of a partial one.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, warn};

use st0x_evm::Wallet;
use st0x_execution::AlpacaWalletError;
use st0x_finance::Usdc;

use super::UsdcTransferError;
use super::manager::CrossVenueCashTransfer;
use crate::alerts::Notifier;
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::usdc_rebalance::UsdcRebalanceId;

const ATTESTATION_REDRIVE_DELAY: Duration = Duration::from_secs(60);

/// Delay before re-enqueueing a Base->Alpaca job after a revert-class burn
/// failure. 15 s is enough for a lagging load-balanced RPC node to catch up
/// while keeping the recovery cadence tight.
const BURN_REVERT_REDRIVE_DELAY: Duration = Duration::from_secs(15);

/// Delay before re-enqueueing a Base->Alpaca job after a per-attempt timeout.
/// 30 s gives a hung RPC time to settle before the job re-enters the resume
/// path. On re-pickup the resume FIRST checks the durably-recorded
/// `pending_burn_tx` via `burn_status` (mempool-aware): a still-pending burn is
/// adopted or waited on, never reburned, and a `Dropped` classification pages the
/// operator. Only when NO pending tx was recorded does it fall back to the
/// mempool-blind `find_recent_burn` scan -- which is reached automatically ONLY
/// for a genuine first-burn attempt (no prior broadcast). Any ambiguous burn
/// submission (timed-out/non-revert broadcast, or a broadcast whose hash failed
/// to record) instead fails closed terminally and latches at `BridgingSubmitting`
/// for operator reconciliation, so a possibly-in-flight burn is never
/// automatically reburned.
const TIMEOUT_REDRIVE_DELAY: Duration = Duration::from_secs(30);

/// Delay before re-enqueueing an Alpaca->Base job when on-chain settlement has
/// not yet completed. Two to three Ethereum block times (~30 s total) is enough
/// to give a lagging RPC node time to catch up after Alpaca marks the
/// withdrawal "Complete", while keeping the redrive cadence tight enough to
/// avoid materially delaying the transfer.
const SETTLEMENT_REDRIVE_DELAY: Duration = Duration::from_secs(30);

/// Delay before re-polling an Alpaca withdrawal when the poll outcome was
/// inconclusive (Alpaca API unreachable or returned an error). 30 seconds
/// gives Alpaca time to recover from a transient outage without hammering the
/// API during a prolonged failure. Distinct from `SETTLEMENT_REDRIVE_DELAY`
/// (Ethereum block timing) so the two can evolve independently.
const WITHDRAWAL_POLL_REDRIVE_DELAY: Duration = Duration::from_secs(30);

/// Delay before retrying a recovered Alpaca-to-Base job whose source
/// reservation is still waiting for a fresh hedging inventory observation.
const INVENTORY_RECOVERY_REDRIVE_DELAY: Duration = Duration::from_secs(30);

/// Duration after which repeated `WithdrawalPollInconclusive` redrives page
/// the operator via the notifier. The deadline is durable: it is derived from
/// `Withdrawing.initiated_at` (stored in the CQRS aggregate, survives restarts)
/// so the countdown is not reset by a bot restart.
///
/// 4 hours gives substantial headroom above the 30-minute internal poll timeout
/// (the longest a healthy Alpaca withdrawal can take), while ensuring a
/// permanently-stuck poll (rotated credentials, Alpaca API shape change, etc.)
/// is surfaced well before it becomes a multi-day outage.
const WITHDRAWAL_POLL_ALERT_DEADLINE: Duration = Duration::from_secs(4 * 60 * 60);

/// Redrive delay used AFTER the 4-hour operator alert deadline has elapsed.
/// Much longer than `WITHDRAWAL_POLL_REDRIVE_DELAY` (30 s) to prevent alert
/// fatigue: a permanently-stuck poll at 30 s cadence pages ~120 times/hour,
/// drowning out other alerts on the shared Telegram channel. At 30 minutes the
/// operator receives at most ~2 pages/hour while the guard stays held and
/// re-polling continues. The re-poll itself is idempotent (same transfer ID),
/// so a slower post-deadline cadence is harmless for funds in transit.
const WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY: Duration = Duration::from_secs(30 * 60);

/// Returns the warn-threshold attempt count at which an early operator alert
/// fires, or `None` when there is no room for a distinct early warning.
///
/// The threshold is set at `max/2 + 1` (integer division) so operators get
/// time to investigate before the circuit opens. For `max >= 3` this always
/// yields a threshold strictly less than `max`, giving one or more warn-only
/// attempts before the limit alert. For `max <= 2` the formula would produce
/// `threshold == max`, making the warn branch structurally unreachable (the
/// limit branch fires first in the if/else-if chain). In that case we return
/// `None` so callers skip the warn branch entirely rather than silently
/// dropping it.
fn warn_threshold(max_redrives: u32) -> Option<u32> {
    let threshold = max_redrives / 2 + 1;
    (threshold < max_redrives).then_some(threshold)
}

fn withdrawal_poll_deadline_elapsed(elapsed: Option<Duration>) -> Option<Duration> {
    elapsed.filter(|elapsed| *elapsed >= WITHDRAWAL_POLL_ALERT_DEADLINE)
}

/// Apalis queue type for [`TransferUsdcToHedging`].
pub(crate) type TransferUsdcToHedgingJobQueue = JobQueue<TransferUsdcToHedging>;

/// Apalis queue type for [`TransferUsdcToMarketMaking`].
pub(crate) type TransferUsdcToMarketMakingJobQueue = JobQueue<TransferUsdcToMarketMaking>;

/// Trait-erased entry point for the Base->Alpaca apalis job. Erasing the
/// `Chain` generic here lets the conductor build a single concrete `Ctx`
/// regardless of which wallet backend is wired in.
#[async_trait]
pub(crate) trait ResumeBaseToAlpaca: Send + Sync + 'static {
    async fn resume_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError>;
}

#[async_trait]
impl<Chain> ResumeBaseToAlpaca for CrossVenueCashTransfer<Chain>
where
    Chain: Wallet + Send + Sync + 'static,
{
    async fn resume_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        Self::resume_base_to_alpaca(self, id, amount).await
    }
}

/// Trait-erased entry point for the Alpaca->Base apalis job. Sibling of
/// [`ResumeBaseToAlpaca`]; same trait-erasure rationale.
#[async_trait]
pub(crate) trait ResumeAlpacaToBase: Send + Sync + 'static {
    async fn resume_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError>;
}

/// Whether a recovered Alpaca-to-Base transfer can safely enter its durable
/// aggregate resume path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlpacaToBaseResumePreparation {
    Ready,
    AwaitingFreshHedgingSnapshot,
}

/// Why a recovered Alpaca-to-Base job cannot enter its aggregate resume path.
#[derive(Debug, Error)]
pub(crate) enum AlpacaToBaseResumePreparationError {
    #[error(
        "Alpaca-to-Base job {requested} cannot resume while recovered reservation owner {owner} is active"
    )]
    ConflictingRecoveryOwner {
        requested: UsdcRebalanceId,
        owner: UsdcRebalanceId,
    },
}

/// Restores restart-only source inventory ownership before the transfer
/// manager resumes. Fresh transfers return [`AlpacaToBaseResumePreparation::Ready`].
#[async_trait]
pub(crate) trait PrepareAlpacaToBaseResume: Send + Sync + 'static {
    async fn prepare_alpaca_to_base_resume(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<AlpacaToBaseResumePreparation, AlpacaToBaseResumePreparationError>;
}

#[cfg(test)]
struct ReadyAlpacaToBaseResume;

#[cfg(test)]
#[async_trait]
impl PrepareAlpacaToBaseResume for ReadyAlpacaToBaseResume {
    async fn prepare_alpaca_to_base_resume(
        &self,
        _id: &UsdcRebalanceId,
    ) -> Result<AlpacaToBaseResumePreparation, AlpacaToBaseResumePreparationError> {
        Ok(AlpacaToBaseResumePreparation::Ready)
    }
}

#[async_trait]
impl<Chain> ResumeAlpacaToBase for CrossVenueCashTransfer<Chain>
where
    Chain: Wallet + Send + Sync + 'static,
{
    async fn resume_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        Self::resume_alpaca_to_base(self, id, amount).await
    }
}

/// Dependencies the job needs to resume the transfer.
pub(crate) struct TransferUsdcToHedgingCtx {
    pub(crate) transfer: Arc<dyn ResumeBaseToAlpaca>,
    /// Per-attempt wall-clock bound. A resume that exceeds this is aborted so
    /// a hung RPC fails the attempt (and retries) instead of wedging the
    /// single-concurrency worker forever.
    pub(crate) timeout: Duration,
    pub(crate) job_queue: TransferUsdcToHedgingJobQueue,
    /// Maximum consecutive revert-class burn failures reclassified as safe
    /// redrives before the circuit opens. From `RebalancingConfig`.
    pub(crate) max_burn_revert_redrives: u32,
    /// Alerting channel. `NoopNotifier` when `[alerts]` is unconfigured;
    /// `TelegramNotifier` otherwise. Never `None` — absence is explicit via
    /// `NoopNotifier` rather than a silent skip.
    pub(crate) notifier: Arc<dyn Notifier>,
}

/// Errors emitted by [`TransferUsdcToHedging::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferUsdcToHedgingJobError {
    #[error(transparent)]
    Transfer(#[from] UsdcTransferError),
    #[error(
        "Base->Alpaca transfer {id} burn revert redrive limit reached; \
         aggregate stalled at BridgingSubmitting, operator action required"
    )]
    BurnRevertLimitReached { id: UsdcRebalanceId },
    #[error(
        "Base->Alpaca transfer {id} per-attempt timeout redrive limit reached; \
         RPC permanently wedged, operator action required"
    )]
    TimeoutLimitReached { id: UsdcRebalanceId },
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
}

/// Apalis job payload. The `id` is generated at enqueue time so retries
/// resume the same aggregate. `revert_redrive_attempts` is a durable counter
/// so the redrive bound is preserved across restarts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TransferUsdcToHedging {
    pub(crate) id: UsdcRebalanceId,
    pub(crate) amount: Usdc,
    /// Shared redrive budget covering both burn-revert and per-attempt timeout
    /// redrives (hedging direction has both). Persisted in the apalis payload
    /// so the bound is durable across restarts.
    #[serde(default)]
    pub(crate) revert_redrive_attempts: u32,
}

impl Job<TransferUsdcToHedgingCtx> for TransferUsdcToHedging {
    type Output = ();
    type Error = TransferUsdcToHedgingJobError;

    const WORKER_NAME: &'static str = "transfer-usdc-to-hedging-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::TransferUsdcToHedging;

    fn label(&self) -> Label {
        Label::new(format!("TransferUsdcToHedging:{}", self.id))
    }

    async fn perform(&self, ctx: &TransferUsdcToHedgingCtx) -> Result<Self::Output, Self::Error> {
        // Per-attempt timeout wrapper (hedging only): abort a hung resume so the
        // attempt fails and retries instead of wedging the single-concurrency
        // worker. The inner result is then classified for redrive/terminal
        // handling.
        let resume = ctx.transfer.resume_base_to_alpaca(&self.id, self.amount);
        let Ok(result) = tokio::time::timeout(ctx.timeout, resume).await else {
            // A timeout fires while a burn tx may have been broadcast -- the RPC
            // just did not return the receipt in time. The redrive re-enters the
            // bridging resume, which FIRST checks the durably-recorded
            // `pending_burn_tx` via `burn_status` (mempool-aware): a still-pending
            // burn is adopted or waited on (never reburned) and a `Dropped`
            // classification pages the operator. The burn broadcast itself is
            // separately bounded by `BURN_BROADCAST_TIMEOUT` (<< this per-attempt
            // timeout) and fails closed on ambiguity, so this per-attempt timeout
            // realistically only fires during the post-record confirm/receipt
            // wait, where `pending_burn_tx` is already set -- the resume adopts it
            // rather than reburning. Count against the shared redrive budget so
            // repeated timeouts (e.g., a permanently hung RPC) eventually surface
            // for operator review.
            return self.handle_hedging_timeout_redrive(ctx).await;
        };

        match result {
            Ok(()) => {}
            Err(UsdcTransferError::AttestationTimedOut { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    delay = ?ATTESTATION_REDRIVE_DELAY,
                    "Rescheduling Base->Alpaca USDC transfer after attestation timeout"
                );
                let mut job_queue = ctx.job_queue.clone();
                job_queue
                    .push_with_delay(self.clone(), ATTESTATION_REDRIVE_DELAY)
                    .await?;
            }
            Err(UsdcTransferError::AttestationRetryDeadlineElapsed { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "Base->Alpaca USDC transfer attestation retry deadline elapsed; \
                     bridge marked failed for operator reconciliation"
                );
                let message = format!(
                    "USDC transfer {id} attestation retry deadline elapsed. \
                     Bridge marked failed; manual operator reconciliation required."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging deadline-elapsed alert");
                }
            }
            Err(UsdcTransferError::PreviouslyFailedAggregate { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "Base->Alpaca USDC transfer already in a terminal failed state; \
                     nothing to redrive, leaving for operator reconciliation"
                );
            }
            // Settlement-phase transient: the Base burn scan was inconclusive
            // (chain head not yet far enough past the scan lower bound) or another
            // settlement-phase RPC check failed transiently. The aggregate is in a
            // durable state (`BridgingSubmitting`), so this must delayed-redrive
            // rather than consume the apalis retry budget or trip the circuit --
            // an inconclusive scan is a normal self-heal outcome. Re-pushing
            // `self.clone()` unchanged means this redrive intentionally does NOT
            // consume the burn-revert budget (`revert_redrive_attempts`) and is
            // unbounded BY DESIGN: a scan-inconclusive / pending-settlement
            // condition resolves as the chain advances, and failing a transfer
            // for slow on-chain settlement would be wrong. Mirrors the
            // market-making settlement-wait arm.
            Err(UsdcTransferError::SettlementCheckTransient { id, .. }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    delay = ?SETTLEMENT_REDRIVE_DELAY,
                    "Rescheduling Base->Alpaca USDC transfer: settlement-phase RPC check \
                     failed transiently or burn scan inconclusive"
                );
                let mut job_queue = ctx.job_queue.clone();
                job_queue
                    .push_with_delay(self.clone(), SETTLEMENT_REDRIVE_DELAY)
                    .await?;
            }
            // Revert-class burn failures: safe to redrive because
            // `resume_bridging_submitting` scans for an existing burn before
            // re-burning (the scan lower bound is durably recorded in the
            // `BeginBridging` / `BridgingSubmitting` event). The safety
            // guarantee is the scan, NOT this classification.
            Err(UsdcTransferError::BurnRevert(_)) => {
                return self.handle_hedging_burn_revert_redrive(ctx).await;
            }
            // Fail-closed burn-submission terminals. The burn's on-chain fate is
            // unknown -- the broadcast may have landed (submit timed out / errored
            // non-revert), its hash was not durably recorded, or a recorded burn
            // was classified dropped. Returning `Ok(())` ends the apalis job with
            // NO retry, so the aggregate stays latched at `BridgingSubmitting`
            // (the guard stays held) and NO automatic reburn occurs -- an
            // automatic reburn could double-burn a still-pending burn. The
            // operator verifies on-chain and uses resume-/fail-usdc-transfer. The
            // alert fires exactly once because this attempt does not redrive.
            Err(UsdcTransferError::BurnTxDropped { id, burn_tx }) => {
                error!(
                    target: "rebalance",
                    %id,
                    %burn_tx,
                    "Base->Alpaca USDC transfer: recorded burn classified dropped; latched at \
                     BridgingSubmitting for operator reconciliation (no auto-reburn)"
                );
                let message = format!(
                    "USDC transfer {id}: recorded burn {burn_tx} classified dropped (not mined, \
                     absent from mempool past grace). Latched for operator reconciliation; \
                     verify on-chain before any reburn."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging dropped-burn alert");
                }
            }
            Err(UsdcTransferError::BurnRecordFailed { id, burn_tx }) => {
                error!(
                    target: "rebalance",
                    %id,
                    %burn_tx,
                    "Base->Alpaca USDC transfer: burn broadcast but its hash could not be durably \
                     recorded; latched at BridgingSubmitting for operator reconciliation \
                     (no auto-reburn)"
                );
                let message = format!(
                    "USDC transfer {id}: burn {burn_tx} broadcast but its hash could not be \
                     durably recorded; a burn is in flight. Latched for operator reconciliation; \
                     verify on-chain before any reburn."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging record-failed-burn alert");
                }
            }
            Err(
                UsdcTransferError::BurnSubmitInconclusive { id }
                | UsdcTransferError::BurnRecordTaskFailed { id },
            ) => {
                error!(
                    target: "rebalance",
                    %id,
                    "Base->Alpaca USDC transfer: burn submission inconclusive or its hash was \
                     not durably recorded; latched at BridgingSubmitting for operator \
                     reconciliation (no auto-reburn)"
                );
                let message = format!(
                    "USDC transfer {id}: burn submission inconclusive or its hash was not \
                     durably recorded; a burn may be in flight. Latched for operator \
                     reconciliation; verify on-chain before any reburn."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging inconclusive-burn alert");
                }
            }
            // Deterministic vault-liquidity revert: the withdraw is atomic
            // (nothing left the vault) and re-issuing it just reverts again
            // until the vault is refunded, burning gas per attempt. Returning
            // `Ok(())` ends the apalis job with NO retry, so the aggregate
            // stays latched at `WithdrawalSubmitting` and the alert fires
            // exactly once. The operator refunds the vault and redrives via
            // resume-usdc-transfer; the resume's withdrawal scan still guards
            // against a double-withdraw.
            Err(error @ UsdcTransferError::InsufficientVaultLiquidity { .. }) => {
                let id = &self.id;
                error!(
                    target: "rebalance",
                    %id,
                    %error,
                    "Base->Alpaca USDC transfer: inventory vault under-funded on withdraw; \
                     latched at WithdrawalSubmitting for operator reconciliation \
                     (no auto-retry)"
                );
                let message =
                    format!("USDC transfer {id}: {error}. Refund the vault, then redrive.");
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging vault-liquidity alert");
                }
            }
            Err(error) => {
                // Terminal non-redriven error: fire notifier before surfacing
                // to apalis so the operator is alerted before the circuit opens.
                //
                // KNOWN LIMITATION: this arm fires on every apalis attempt (up
                // to 4x with the default RetryPolicy::retries(3)). Because the
                // apalis retry uses the same serialized payload and `perform`
                // has no visibility into the current attempt number, suppressing
                // duplicates here is not feasible without threading apalis
                // attempt context through. The bounded-limit path
                // (BurnRevertLimitReached / TimeoutLimitReached) already fires
                // exactly once via the Ok-return redrive pattern; this generic
                // terminal arm is a best-effort alert that may duplicate.
                let id = &self.id;
                error!(
                    target: "rebalance",
                    %id,
                    %error,
                    "Base->Alpaca USDC transfer failed terminally; circuit will open"
                );
                let message = format!(
                    "USDC transfer {id} failed: {error}. \
                     Check if apalis will retry before acting."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging terminal-error alert");
                }
                return Err(error.into());
            }
        }

        Ok(())
    }
}

impl TransferUsdcToHedging {
    /// Handles a per-attempt timeout by either opening the circuit (when the
    /// redrive limit is reached) or scheduling a delayed redrive attempt.
    /// Extracted from `Job::perform` to mirror the market-making extraction and
    /// keep the perform body under the line-count lint threshold.
    async fn handle_hedging_timeout_redrive(
        &self,
        ctx: &TransferUsdcToHedgingCtx,
    ) -> Result<(), TransferUsdcToHedgingJobError> {
        let id = &self.id;

        // Check the stored counter BEFORE incrementing: the budget is exhausted
        // once the redrives already consumed reach the max, so the next attempt
        // would exceed it. Incrementing first then comparing `>` is equivalent but
        // obscures the boundary and leans on saturating arithmetic.
        if self.revert_redrive_attempts >= ctx.max_burn_revert_redrives {
            error!(
                target: "rebalance",
                %id,
                attempts = self.revert_redrive_attempts,
                timeout = ?ctx.timeout,
                "Base->Alpaca USDC transfer per-attempt timeout redrive limit reached; \
                 operator action required"
            );
            // Alert fires only on the last successful redrive (next_attempts == max),
            // not here. Apalis retries this Err up to 3 more times with the same
            // payload, so alerting here would fire up to 4x for the same event.
            //
            // The operator is paged for this exhausted-budget circuit-open via the
            // startup `recover_usdc_guard` stranded-alert path (the aggregate is
            // found at `BridgingSubmitting` with only an exhausted-`Failed` job
            // row). A live (non-restart) page on circuit-open is a known gap,
            // tracked as a follow-up.
            return Err(TransferUsdcToHedgingJobError::TimeoutLimitReached { id: id.clone() });
        }

        // Bound already checked above, so a plain `+ 1` cannot overflow.
        let next_attempts = self.revert_redrive_attempts + 1;

        // Last allowed redrive: fire the limit alert BEFORE enqueuing so operators
        // know the budget is exhausted. Returns Ok so apalis does not retry this
        // attempt -- the alert fires exactly once.
        if next_attempts == ctx.max_burn_revert_redrives {
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                timeout = ?ctx.timeout,
                "Base->Alpaca USDC transfer per-attempt timeout redrive limit reached; \
                 last redrive enqueued, operator action will be needed"
            );
            let message = format!(
                "USDC transfer {id} per-attempt timeout redrive limit reached after \
                 {next_attempts} attempts. Base->Alpaca transfer stalled; \
                 check aggregate state for current stage. Manual operator action required."
            );
            if let Err(error) = ctx.notifier.notify(&message).await {
                warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging timeout-limit alert");
            }
        } else if warn_threshold(ctx.max_burn_revert_redrives) == Some(next_attempts) {
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                max = ctx.max_burn_revert_redrives,
                delay = ?TIMEOUT_REDRIVE_DELAY,
                "Base->Alpaca USDC transfer timeout has retried multiple times; \
                 possible hung RPC or persistent network issue"
            );
            let message = format!(
                "USDC transfer {id} per-attempt timeout has retried {next_attempts} times \
                 (max: {}). Possible hung RPC or persistent network issue.",
                ctx.max_burn_revert_redrives
            );
            if let Err(error) = ctx.notifier.notify(&message).await {
                warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging timeout-warn alert");
            }
        } else {
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                delay = ?TIMEOUT_REDRIVE_DELAY,
                "Base->Alpaca USDC transfer timed out; re-entering scan-or-reburn \
                 path after delay (burn may have landed; resume will adopt it)"
            );
        }

        let updated = Self {
            revert_redrive_attempts: next_attempts,
            ..self.clone()
        };
        ctx.job_queue
            .clone()
            .push_with_delay(updated, TIMEOUT_REDRIVE_DELAY)
            .await?;
        Ok(())
    }

    /// Handles a revert-class burn error by either opening the circuit (when the
    /// redrive limit is reached) or scheduling a delayed redrive attempt. Symmetric
    /// to [`TransferUsdcToMarketMaking::handle_mm_burn_revert_redrive`].
    async fn handle_hedging_burn_revert_redrive(
        &self,
        ctx: &TransferUsdcToHedgingCtx,
    ) -> Result<(), TransferUsdcToHedgingJobError> {
        let id = &self.id;

        // Check the stored counter BEFORE incrementing: the budget is exhausted
        // once the redrives already consumed reach the max, so the next attempt
        // would exceed it. Incrementing first then comparing `>` is equivalent but
        // obscures the boundary and leans on saturating arithmetic.
        if self.revert_redrive_attempts >= ctx.max_burn_revert_redrives {
            error!(
                target: "rebalance",
                %id,
                attempts = self.revert_redrive_attempts,
                "Base->Alpaca USDC burn revert redrive limit reached; \
                 operator action required"
            );
            // Alert fires only on the last successful redrive (next_attempts == max),
            // not here. Apalis retries this Err up to 3 more times with the same
            // payload, so alerting here would fire up to 4x for the same event.
            //
            // The operator is paged for this exhausted-budget circuit-open via the
            // startup `recover_usdc_guard` stranded-alert path (the aggregate is
            // found at `BridgingSubmitting` with only an exhausted-`Failed` job
            // row). A live (non-restart) page on circuit-open is a known gap,
            // tracked as a follow-up.
            return Err(TransferUsdcToHedgingJobError::BurnRevertLimitReached { id: id.clone() });
        }

        // Bound already checked above, so a plain `+ 1` cannot overflow.
        let next_attempts = self.revert_redrive_attempts + 1;

        // Last allowed redrive: alert fires BEFORE enqueuing the final redrive
        // so the operator is notified as early as possible. The next failure
        // (next_attempts > max) returns BurnRevertLimitReached with no further
        // alert (apalis would retry the Err up to 3x, causing duplicate pages).
        // Returns Ok so apalis does not retry this attempt -- the alert fires
        // exactly once at this boundary.
        if next_attempts == ctx.max_burn_revert_redrives {
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                delay = ?BURN_REVERT_REDRIVE_DELAY,
                "Base->Alpaca USDC burn revert hit redrive limit; \
                 attempting the final redrive, operator action will be needed if it fails"
            );
            let message = format!(
                "USDC transfer {id} burn revert redrive limit reached after \
                 {next_attempts} attempts (max: {max}). Attempting the final redrive now; \
                 manual operator action will be required if it fails to enqueue or also reverts.",
                max = ctx.max_burn_revert_redrives
            );
            if let Err(error) = ctx.notifier.notify(&message).await {
                warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging burn-revert-limit alert");
            }
        } else if warn_threshold(ctx.max_burn_revert_redrives) == Some(next_attempts) {
            // Warn threshold: alert exactly once so operators can investigate
            // before the limit is reached, avoiding a silent infinite loop.
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                max = ctx.max_burn_revert_redrives,
                delay = ?BURN_REVERT_REDRIVE_DELAY,
                "Base->Alpaca USDC burn revert has retried multiple times; \
                 possible persistent RPC or contract issue"
            );
            let message = format!(
                "USDC transfer {id} burn revert has retried {next_attempts} times \
                 (max: {}). Possible transient or persistent RPC/contract issue.",
                ctx.max_burn_revert_redrives
            );
            if let Err(error) = ctx.notifier.notify(&message).await {
                warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging burn-revert-warn alert");
            }
        } else {
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                delay = ?BURN_REVERT_REDRIVE_DELAY,
                "Base->Alpaca USDC burn reverted (revert-class, no on-chain state \
                 change); re-entering scan-or-reburn path after delay"
            );
        }

        let updated = Self {
            revert_redrive_attempts: next_attempts,
            ..self.clone()
        };
        ctx.job_queue
            .clone()
            .push_with_delay(updated, BURN_REVERT_REDRIVE_DELAY)
            .await?;
        Ok(())
    }
}

/// Dependencies the Alpaca->Base job needs. Symmetric to
/// [`TransferUsdcToHedgingCtx`].
pub(crate) struct TransferUsdcToMarketMakingCtx {
    pub(crate) transfer: Arc<dyn ResumeAlpacaToBase>,
    pub(crate) resume_preparation: Arc<dyn PrepareAlpacaToBaseResume>,
    pub(crate) job_queue: TransferUsdcToMarketMakingJobQueue,
    /// Maximum consecutive revert-class burn failures before circuit opens.
    pub(crate) max_burn_revert_redrives: u32,
    /// Alerting channel. `NoopNotifier` when `[alerts]` is unconfigured;
    /// `TelegramNotifier` otherwise. Never `None` — absence is explicit via
    /// `NoopNotifier` rather than a silent skip.
    pub(crate) notifier: Arc<dyn Notifier>,
}

/// Errors emitted by [`TransferUsdcToMarketMaking::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferUsdcToMarketMakingJobError {
    #[error(transparent)]
    Transfer(#[from] UsdcTransferError),
    #[error(transparent)]
    ResumePreparation(#[from] AlpacaToBaseResumePreparationError),
    #[error(
        "Alpaca->Base transfer {id} burn revert redrive limit reached; \
         aggregate stalled at BridgingSubmitting, operator action required"
    )]
    BurnRevertLimitReached { id: UsdcRebalanceId },
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
}

/// Apalis job payload for the Alpaca->Base direction. The `id` is generated
/// at enqueue time so retries resume the same aggregate. `revert_redrive_attempts`
/// is a durable counter so the redrive bound is preserved across restarts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TransferUsdcToMarketMaking {
    pub(crate) id: UsdcRebalanceId,
    pub(crate) amount: Usdc,
    /// Burn-revert redrive budget (market-making direction: no per-attempt
    /// timeout, so this counter covers only burn-revert redrives). Persisted in
    /// the apalis payload so the bound is durable across restarts.
    #[serde(default)]
    pub(crate) revert_redrive_attempts: u32,
}

impl Job<TransferUsdcToMarketMakingCtx> for TransferUsdcToMarketMaking {
    type Output = ();
    type Error = TransferUsdcToMarketMakingJobError;

    const WORKER_NAME: &'static str = "transfer-usdc-to-market-making-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::TransferUsdcToMarketMaking;

    fn label(&self) -> Label {
        Label::new(format!("TransferUsdcToMarketMaking:{}", self.id))
    }

    async fn perform(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
    ) -> Result<Self::Output, Self::Error> {
        if ctx
            .resume_preparation
            .prepare_alpaca_to_base_resume(&self.id)
            .await?
            == AlpacaToBaseResumePreparation::AwaitingFreshHedgingSnapshot
        {
            warn!(
                target: "rebalance",
                id = %self.id,
                delay = ?INVENTORY_RECOVERY_REDRIVE_DELAY,
                "Waiting for fresh hedging inventory before resuming recovered Alpaca->Base transfer"
            );
            ctx.job_queue
                .clone()
                .push_with_delay(self.clone(), INVENTORY_RECOVERY_REDRIVE_DELAY)
                .await?;
            return Ok(());
        }

        // No per-attempt timeout here unlike the hedging direction. The
        // AlpacaToBase resume can pass through a long-running broker Converting
        // leg with no safe re-entry path if interrupted (unlike BaseToAlpaca
        // which has resume_converting recovery). The burn-revert-redrive path
        // below is the correct self-heal mechanism for the incident this PR
        // targets.
        //
        // KNOWN LIMITATION: A wedged (not erroring) RPC during resume of the burn
        // or attestation phase will stall this worker indefinitely with no
        // per-attempt timeout bound. The burn-revert redrive above handles the case
        // where the burn RETURNS a revert error; it does not handle a hung RPC that
        // never returns. A burn-leg-only timeout (gated to post-Converting stages
        // where re-entry is safe) is a follow-up.
        let result = ctx
            .transfer
            .resume_alpaca_to_base(&self.id, self.amount)
            .await;

        match result {
            Ok(()) => {}
            Err(UsdcTransferError::AttestationTimedOut { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    delay = ?ATTESTATION_REDRIVE_DELAY,
                    "Rescheduling Alpaca->Base USDC transfer after attestation timeout"
                );
                let mut job_queue = ctx.job_queue.clone();
                job_queue
                    .push_with_delay(self.clone(), ATTESTATION_REDRIVE_DELAY)
                    .await?;
            }
            // Settlement-wait errors: the withdrawal tx has not yet reached the
            // required on-chain confirmation depth, the Ethereum wallet has not yet
            // received the withdrawn USDC, or an RPC call in the settlement phase
            // (confirmation re-check, balance read, or burn scan) failed
            // transiently. These are all safe to delayed-redrive because the
            // aggregate is in a durable state (WithdrawalComplete or
            // BridgingSubmitting) -- they must NOT consume apalis retry budget
            // (only 3 retries, ~7 s total). Re-enqueue with
            // SETTLEMENT_REDRIVE_DELAY and return Ok so this attempt completes
            // cleanly; the delayed job resumes once settlement is likely complete.
            Err(
                ref settlement_err @ (UsdcTransferError::WithdrawalTxUnderconfirmed {
                    ref id, ..
                }
                | UsdcTransferError::WalletUsdcInsufficient { ref id, .. }
                | UsdcTransferError::SettlementCheckTransient { ref id, .. }),
            ) => {
                let reason = match settlement_err {
                    UsdcTransferError::WithdrawalTxUnderconfirmed { .. } => {
                        "withdrawal tx not yet sufficiently confirmed"
                    }
                    UsdcTransferError::WalletUsdcInsufficient { .. } => {
                        "market-maker wallet has insufficient USDC (withdrawal not yet settled)"
                    }
                    UsdcTransferError::SettlementCheckTransient { .. } => {
                        "settlement-phase RPC check failed transiently"
                    }
                    // The outer or-pattern matches only the three variants above.
                    // This arm is unreachable; it exists only to satisfy exhaustiveness.
                    _ => unreachable!("outer or-pattern limits to settlement variants"),
                };
                warn!(
                    target: "rebalance",
                    %id,
                    delay = ?SETTLEMENT_REDRIVE_DELAY,
                    "Rescheduling Alpaca->Base USDC transfer: {reason}"
                );
                let mut job_queue = ctx.job_queue.clone();
                job_queue
                    .push_with_delay(self.clone(), SETTLEMENT_REDRIVE_DELAY)
                    .await?;
            }
            Err(UsdcTransferError::AttestationRetryDeadlineElapsed { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "Alpaca->Base USDC transfer attestation retry deadline elapsed; \
                     bridge marked failed for operator reconciliation"
                );
                let message = format!(
                    "USDC transfer {id} attestation retry deadline elapsed. \
                     Bridge marked failed; manual operator reconciliation required."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making deadline-elapsed alert");
                }
            }
            Err(UsdcTransferError::PreviouslyFailedAggregate { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "Alpaca->Base USDC transfer already in a terminal failed state; \
                     nothing to redrive, leaving for operator reconciliation"
                );
            }
            // Ambient USDC in the market-maker wallet: the wallet-empty invariant
            // is broken and no burn can safely proceed. The aggregate has already
            // been moved to BridgingFailed via FailBridging; surface for operator
            // reconciliation (same pattern as AttestationRetryDeadlineElapsed).
            Err(UsdcTransferError::WalletUsdcAmbientBalance {
                id,
                balance,
                nominal,
            }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    %balance,
                    %nominal,
                    "Alpaca->Base USDC transfer failed: ambient USDC in market-maker wallet; \
                     bridge marked failed for operator reconciliation"
                );
                let message = format!(
                    "USDC transfer {id} failed: ambient USDC ({balance}) exceeds nominal ({nominal}). \
                     Wallet-empty invariant broken; bridge marked failed, manual operator reconciliation required."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making ambient-balance alert");
                }
            }
            // Indeterminate withdrawal poll: the Alpaca poll timed out or returned
            // a transport/API error without observing a terminal status. The
            // aggregate is in Withdrawing (guard held, AlpacaTransferId recorded)
            // -- NOT WithdrawalFailed. Re-polling is idempotent (reads only; never
            // re-initiates the withdrawal). Schedule an unbounded delayed redrive
            // (like SettlementCheckTransient): return Ok so apalis does not
            // consume the retry budget, and enqueue a new job after the delay so
            // the same transfer ID is re-polled. Before the alert deadline only
            // a warn log fires; at or after the deadline the operator is paged on
            // every redrive while the guard stays held and re-polling continues.
            Err(UsdcTransferError::WithdrawalPollInconclusive {
                id,
                initiated_at,
                source,
            }) => {
                self.handle_withdrawal_poll_inconclusive(ctx, id, initiated_at, source)
                    .await?;
            }
            // Revert-class burn failures: safe to redrive because
            // `resume_bridging_submitting` scans for an existing burn before
            // re-burning (the scan lower bound is durably recorded). The safety
            // guarantee is the scan, NOT this classification.
            Err(UsdcTransferError::BurnRevert(_)) => {
                return self.handle_mm_burn_revert_redrive(ctx).await;
            }
            // Fail-closed burn-submission terminals (see the hedging direction for
            // the full rationale). The burn's on-chain fate is unknown -- the
            // broadcast may have landed, its hash was not durably recorded, or a
            // recorded burn was classified dropped. Returning `Ok(())` ends the
            // apalis job with NO retry, so the aggregate stays latched at
            // `BridgingSubmitting` (guard held) and NO automatic reburn occurs.
            // The operator verifies on-chain and uses resume-/fail-usdc-transfer.
            Err(UsdcTransferError::BurnTxDropped { id, burn_tx }) => {
                error!(
                    target: "rebalance",
                    %id,
                    %burn_tx,
                    "Alpaca->Base USDC transfer: recorded burn classified dropped; latched at \
                     BridgingSubmitting for operator reconciliation (no auto-reburn)"
                );
                let message = format!(
                    "USDC transfer {id}: recorded burn {burn_tx} classified dropped (not mined, \
                     absent from mempool past grace). Latched for operator reconciliation; \
                     verify on-chain before any reburn."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making dropped-burn alert");
                }
            }
            Err(UsdcTransferError::BurnRecordFailed { id, burn_tx }) => {
                error!(
                    target: "rebalance",
                    %id,
                    %burn_tx,
                    "Alpaca->Base USDC transfer: burn broadcast but its hash could not be durably \
                     recorded; latched at BridgingSubmitting for operator reconciliation \
                     (no auto-reburn)"
                );
                let message = format!(
                    "USDC transfer {id}: burn {burn_tx} broadcast but its hash could not be \
                     durably recorded; a burn is in flight. Latched for operator reconciliation; \
                     verify on-chain before any reburn."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making record-failed-burn alert");
                }
            }
            Err(
                UsdcTransferError::BurnSubmitInconclusive { id }
                | UsdcTransferError::BurnRecordTaskFailed { id },
            ) => {
                error!(
                    target: "rebalance",
                    %id,
                    "Alpaca->Base USDC transfer: burn submission inconclusive or its hash was \
                     not durably recorded; latched at BridgingSubmitting for operator \
                     reconciliation (no auto-reburn)"
                );
                let message = format!(
                    "USDC transfer {id}: burn submission inconclusive or its hash was not \
                     durably recorded; a burn may be in flight. Latched for operator \
                     reconciliation; verify on-chain before any reburn."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making inconclusive-burn alert");
                }
            }
            Err(error) => {
                // Terminal non-redriven error: fire notifier before surfacing
                // to apalis so the operator is alerted before the circuit opens.
                //
                // KNOWN LIMITATION: this arm fires on every apalis attempt (up
                // to 4x with the default RetryPolicy::retries(3)). Because the
                // apalis retry uses the same serialized payload and `perform`
                // has no visibility into the current attempt number, suppressing
                // duplicates here is not feasible without threading apalis
                // attempt context through. The bounded-limit path
                // (BurnRevertLimitReached) already fires exactly once via the
                // Ok-return redrive pattern; this generic terminal arm is a
                // best-effort alert that may duplicate.
                let id = &self.id;
                error!(
                    target: "rebalance",
                    %id,
                    %error,
                    "Alpaca->Base USDC transfer failed terminally; circuit will open"
                );
                let message = format!(
                    "USDC transfer {id} failed: {error}. \
                     Check if apalis will retry before acting."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making terminal-error alert");
                }
                return Err(error.into());
            }
        }

        Ok(())
    }
}

impl TransferUsdcToMarketMaking {
    async fn handle_withdrawal_poll_inconclusive(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
        id: UsdcRebalanceId,
        initiated_at: DateTime<Utc>,
        source: AlpacaWalletError,
    ) -> Result<(), TransferUsdcToMarketMakingJobError> {
        // Mirror the `.ok()` pattern for `signed_duration_since`: if
        // `initiated_at` is in the future (e.g., clock skew after restart),
        // `to_std()` returns `Err` and we treat elapsed as `None`; the deadline
        // check is then false (no spurious alert).
        let elapsed = Utc::now().signed_duration_since(initiated_at).to_std().ok();

        // Once past the deadline, slow the redrive cadence from 30 s to 30 min
        // so the operator page repeats at ~2/hour rather than ~120/hour. The
        // re-poll is idempotent so a slower cadence is safe.
        let deadline_elapsed = withdrawal_poll_deadline_elapsed(elapsed);
        let redrive_delay = if deadline_elapsed.is_some() {
            WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY
        } else {
            WITHDRAWAL_POLL_REDRIVE_DELAY
        };

        warn!(
            target: "rebalance",
            %id,
            %source,
            ?elapsed,
            delay = ?redrive_delay,
            "Alpaca withdrawal polling inconclusive; rescheduling for re-poll \
             (aggregate stays in Withdrawing, guard held)"
        );

        if let Some(elapsed) = deadline_elapsed {
            let message = format!(
                "Alpaca->Base USDC transfer {id}: withdrawal polling inconclusive \
                 for {elapsed:?} (>{WITHDRAWAL_POLL_ALERT_DEADLINE:?}). Alpaca may \
                 be unreachable or credentials may have changed ({source}). Aggregate stays in \
                 Withdrawing (guard held). Use `stox transfer resume --kind usdc --id \
                 {id} --direction to-raindex` to manually re-poll, or investigate \
                 Alpaca connectivity."
            );
            if let Err(notify_err) = ctx.notifier.notify(&message).await {
                warn!(
                    target: "rebalance",
                    ?notify_err,
                    "Failed to deliver withdrawal-poll-deadline-elapsed alert"
                );
            }
        }

        ctx.job_queue
            .clone()
            .push_with_delay(self.clone(), redrive_delay)
            .await?;
        Ok(())
    }

    /// Handles a revert-class burn error by either opening the circuit (when the
    /// redrive limit is reached) or scheduling a delayed redrive attempt. Extracted
    /// to keep `Job::perform` under the line-count lint threshold.
    async fn handle_mm_burn_revert_redrive(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
    ) -> Result<(), TransferUsdcToMarketMakingJobError> {
        let id = &self.id;

        // Check the stored counter BEFORE incrementing: the budget is exhausted
        // once the redrives already consumed reach the max, so the next attempt
        // would exceed it. Incrementing first then comparing `>` is equivalent but
        // obscures the boundary and leans on saturating arithmetic.
        if self.revert_redrive_attempts >= ctx.max_burn_revert_redrives {
            error!(
                target: "rebalance",
                %id,
                attempts = self.revert_redrive_attempts,
                "Alpaca->Base USDC burn revert redrive limit reached; \
                 operator action required"
            );
            // Alert fires only on the last successful redrive (next_attempts == max),
            // not here. Apalis retries this Err up to 3 more times with the same
            // payload, so alerting here would fire up to 4x for the same event.
            //
            // The operator is paged for this exhausted-budget circuit-open via the
            // startup `recover_usdc_guard` stranded-alert path (the aggregate is
            // found at `BridgingSubmitting` with only an exhausted-`Failed` job
            // row). A live (non-restart) page on circuit-open is a known gap,
            // tracked as a follow-up.
            return Err(TransferUsdcToMarketMakingJobError::BurnRevertLimitReached {
                id: id.clone(),
            });
        }

        // Bound already checked above, so a plain `+ 1` cannot overflow.
        let next_attempts = self.revert_redrive_attempts + 1;

        // Last allowed redrive: alert fires BEFORE enqueuing the final redrive
        // so the operator is notified as early as possible. The next failure
        // (next_attempts > max) returns BurnRevertLimitReached with no further
        // alert (apalis would retry the Err up to 3x, causing duplicate pages).
        // Returns Ok so apalis does not retry this attempt -- the alert fires
        // exactly once at this boundary.
        if next_attempts == ctx.max_burn_revert_redrives {
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                delay = ?BURN_REVERT_REDRIVE_DELAY,
                "Alpaca->Base USDC burn revert hit redrive limit; \
                 attempting the final redrive, operator action will be needed if it fails"
            );
            let message = format!(
                "USDC transfer {id} burn revert redrive limit reached after \
                 {next_attempts} attempts (max: {max}). Attempting the final redrive now; \
                 manual operator action will be required if it fails to enqueue or also reverts.",
                max = ctx.max_burn_revert_redrives
            );
            if let Err(error) = ctx.notifier.notify(&message).await {
                warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making burn-revert-limit alert");
            }
        } else if warn_threshold(ctx.max_burn_revert_redrives) == Some(next_attempts) {
            // Warn threshold: alert exactly once so operators can investigate
            // before the limit is reached, avoiding a silent infinite loop.
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                max = ctx.max_burn_revert_redrives,
                delay = ?BURN_REVERT_REDRIVE_DELAY,
                "Alpaca->Base USDC burn revert has retried multiple times; \
                 possible persistent RPC or contract issue"
            );
            let message = format!(
                "USDC transfer {id} burn revert has retried {next_attempts} times \
                 (max: {}). Possible transient or persistent RPC/contract issue.",
                ctx.max_burn_revert_redrives
            );
            if let Err(error) = ctx.notifier.notify(&message).await {
                warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making burn-revert-warn alert");
            }
        } else {
            warn!(
                target: "rebalance",
                %id,
                attempts = next_attempts,
                delay = ?BURN_REVERT_REDRIVE_DELAY,
                "Alpaca->Base USDC burn reverted (revert-class, no on-chain state \
                 change); re-entering scan-or-reburn path after delay"
            );
        }

        let updated = Self {
            revert_redrive_attempts: next_attempts,
            ..self.clone()
        };
        ctx.job_queue
            .clone()
            .push_with_delay(updated, BURN_REVERT_REDRIVE_DELAY)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, U256};
    use chrono::{DateTime, Utc};
    use reqwest::StatusCode;
    use uuid::Uuid;

    use st0x_bridge::cctp::CctpError;
    use st0x_evm::EvmError;
    use st0x_execution::{AlpacaTransferId, AlpacaWalletError};
    use st0x_float_macro::float;

    use super::*;
    use crate::alerts::{CapturingNotifier, NoopNotifier};
    use crate::test_utils::setup_test_apalis_pool;

    #[test]
    fn withdrawal_poll_deadline_elapsed_includes_exact_boundary() {
        let elapsed = Some(WITHDRAWAL_POLL_ALERT_DEADLINE);

        assert_eq!(
            withdrawal_poll_deadline_elapsed(elapsed),
            Some(WITHDRAWAL_POLL_ALERT_DEADLINE),
            "the alert deadline comparison must be inclusive"
        );
    }

    /// Builds a `TransferUsdcToHedgingCtx` with test-safe defaults for the
    /// notifier and redrive-limit fields.
    fn hedging_ctx(
        transfer: Arc<dyn ResumeBaseToAlpaca>,
        pool: &apalis_sqlite::SqlitePool,
    ) -> TransferUsdcToHedgingCtx {
        TransferUsdcToHedgingCtx {
            transfer,
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(pool),
            max_burn_revert_redrives: 5,
            notifier: Arc::new(NoopNotifier),
        }
    }

    struct TimeoutBaseToAlpaca;

    #[async_trait]
    impl ResumeBaseToAlpaca for TimeoutBaseToAlpaca {
        async fn resume_base_to_alpaca(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::AttestationTimedOut { id: id.clone() })
        }
    }

    struct TimeoutAlpacaToBase;

    #[async_trait]
    impl ResumeAlpacaToBase for TimeoutAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::AttestationTimedOut { id: id.clone() })
        }
    }

    /// Models a hung RPC inside the transfer: the resume future never completes
    /// within the configured per-attempt timeout.
    struct HangingResume;

    #[async_trait]
    impl ResumeBaseToAlpaca for HangingResume {
        async fn resume_base_to_alpaca(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(())
        }
    }

    /// Terminal outcomes a resume can report that the job must treat as a clean
    /// `Ok(())` (no redrive, no error) because the aggregate is already in a
    /// durable terminal state needing only operator reconciliation.
    #[derive(Clone, Copy)]
    enum TerminalOutcome {
        DeadlineElapsed,
        PreviouslyFailed,
        AmbientBalance,
        /// Fail-closed burn-submission terminals: a burn may be in flight, so the
        /// job must NOT auto-redrive (a redrive could reburn).
        BurnSubmitInconclusive,
        BurnRecordFailed,
        BurnRecordTaskFailed,
        BurnTxDropped,
        /// Deterministic vault-liquidity revert: re-issuing the withdraw just
        /// reverts again until the vault is refunded, so the job must latch.
        InsufficientVaultLiquidity,
    }

    impl TerminalOutcome {
        fn into_error(self, id: &UsdcRebalanceId) -> UsdcTransferError {
            match self {
                Self::DeadlineElapsed => {
                    UsdcTransferError::AttestationRetryDeadlineElapsed { id: id.clone() }
                }
                Self::PreviouslyFailed => {
                    UsdcTransferError::PreviouslyFailedAggregate { id: id.clone() }
                }
                Self::AmbientBalance => UsdcTransferError::WalletUsdcAmbientBalance {
                    id: id.clone(),
                    balance: Usdc::new(float!(1)),
                    nominal: Usdc::new(float!(1)),
                },
                Self::BurnSubmitInconclusive => {
                    UsdcTransferError::BurnSubmitInconclusive { id: id.clone() }
                }
                Self::BurnRecordFailed => UsdcTransferError::BurnRecordFailed {
                    id: id.clone(),
                    burn_tx: TxHash::from([0xCD; 32]),
                },
                Self::BurnRecordTaskFailed => {
                    UsdcTransferError::BurnRecordTaskFailed { id: id.clone() }
                }
                Self::BurnTxDropped => UsdcTransferError::BurnTxDropped {
                    id: id.clone(),
                    burn_tx: TxHash::from([0xAB; 32]),
                },
                Self::InsufficientVaultLiquidity => UsdcTransferError::InsufficientVaultLiquidity {
                    token: Address::from([0xEE; 20]),
                    requested: U256::from(100),
                    received: U256::from(40),
                },
            }
        }
    }

    struct TerminalBaseToAlpaca(TerminalOutcome);

    #[async_trait]
    impl ResumeBaseToAlpaca for TerminalBaseToAlpaca {
        async fn resume_base_to_alpaca(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(self.0.into_error(id))
        }
    }

    struct TerminalAlpacaToBase(TerminalOutcome);

    #[async_trait]
    impl ResumeAlpacaToBase for TerminalAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(self.0.into_error(id))
        }
    }

    /// Stub that returns `WithdrawalPollInconclusive` for every resume call.
    /// `initiated_at` controls whether the deadline check in the job handler
    /// fires an operator alert (past deadline) or only logs a warning (before).
    struct InconclusiveAlpacaToBase {
        initiated_at: DateTime<Utc>,
    }

    impl InconclusiveAlpacaToBase {
        fn before_deadline() -> Self {
            Self {
                initiated_at: Utc::now(),
            }
        }

        fn future_initiated_at() -> Self {
            Self {
                initiated_at: Utc::now() + chrono::Duration::minutes(5),
            }
        }

        fn after_deadline() -> Self {
            Self {
                initiated_at: Utc::now()
                    - chrono::Duration::from_std(WITHDRAWAL_POLL_ALERT_DEADLINE).unwrap()
                    - chrono::Duration::seconds(1),
            }
        }

        /// Nominally at the deadline boundary. The handler computes elapsed later,
        /// so this exercises the job path at or just beyond the boundary; the pure
        /// helper test pins the exact `elapsed == WITHDRAWAL_POLL_ALERT_DEADLINE`
        /// comparison.
        fn at_deadline() -> Self {
            Self {
                initiated_at: Utc::now()
                    - chrono::Duration::from_std(WITHDRAWAL_POLL_ALERT_DEADLINE).unwrap(),
            }
        }
    }

    #[async_trait]
    impl ResumeAlpacaToBase for InconclusiveAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::WithdrawalPollInconclusive {
                id: id.clone(),
                initiated_at: self.initiated_at,
                source: AlpacaWalletError::TransferTimeout {
                    transfer_id: AlpacaTransferId::from(Uuid::new_v4()),
                    elapsed: Duration::from_secs(1800),
                },
            })
        }
    }

    async fn setup_queue_pool() -> apalis_sqlite::SqlitePool {
        setup_test_apalis_pool().await
    }

    async fn pending_job_count<Task>(pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs \
             WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<Task>())
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Returns the serialized payload (apalis stores it as a `serde_json` BLOB
    /// via `JsonCodec`) and the `run_at` unix-second timestamp of the single
    /// pending row of the given task type.
    async fn pending_job_row<Task>(pool: &apalis_sqlite::SqlitePool) -> (Vec<u8>, i64) {
        sqlx_apalis::query_as(
            "SELECT job, run_at FROM Jobs \
             WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<Task>())
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn hedging_job_reschedules_attestation_timeout() {
        let pool = setup_queue_pool().await;
        let ctx = hedging_ctx(Arc::new(TimeoutBaseToAlpaca), &pool);
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "attestation timeout should enqueue a delayed replacement job"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        let rescheduled: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount, got {} vs {}",
            rescheduled.amount,
            job.amount
        );
        assert!(
            run_at >= before + 55 && run_at <= after + 65,
            "redrive must be delayed by ~{ATTESTATION_REDRIVE_DELAY:?} -- neither immediate nor \
             excessive: run_at={run_at} before={before} after={after}"
        );
    }

    /// A hung resume (RPC wedge) must be aborted by the per-attempt timeout
    /// and reclassified as a safe redrive -- not propagated as a circuit-tripping
    /// error -- because the scan-or-reburn path will adopt any burn that landed
    /// during the hang. Verify Ok + one Pending row with TIMEOUT_REDRIVE_DELAY,
    /// carrying `revert_redrive_attempts = 1`.
    #[tokio::test]
    async fn perform_times_out_when_resume_hangs() {
        let pool = setup_queue_pool().await;
        // Short timeout to make the test fast. hedging_ctx uses 3600s by
        // default so override inline.
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(HangingResume),
            timeout: Duration::from_millis(50),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: Arc::new(NoopNotifier),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        Job::perform(&job, &ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "a per-attempt timeout must redrive rather than trip the circuit breaker"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        let rescheduled: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount"
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, 1,
            "revert_redrive_attempts must be incremented to 1 in the redrive payload"
        );
        assert!(
            run_at >= before + i64::try_from(TIMEOUT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at <= after + i64::try_from(TIMEOUT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{TIMEOUT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// After `max_burn_revert_redrives` consecutive timeouts the job must
    /// propagate `TimeoutLimitReached` so the circuit opens and the operator
    /// is alerted. No new Pending row must be created.
    #[tokio::test]
    async fn hedging_job_hits_redrive_limit_on_repeated_timeout() {
        let pool = setup_queue_pool().await;
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(HangingResume),
            timeout: Duration::from_millis(50),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: Arc::new(NoopNotifier),
        };
        // Simulate a job that has already used all its redrive budget.
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                TransferUsdcToHedgingJobError::TimeoutLimitReached { .. }
            ),
            "at the redrive limit a timeout must propagate TimeoutLimitReached, got {error:?}",
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "limit-reached must NOT enqueue a new pending job"
        );
    }

    /// Builds a `TransferUsdcToMarketMakingCtx` with test-safe defaults.
    fn market_making_ctx(
        transfer: Arc<dyn ResumeAlpacaToBase>,
        pool: &apalis_sqlite::SqlitePool,
    ) -> TransferUsdcToMarketMakingCtx {
        TransferUsdcToMarketMakingCtx {
            transfer,
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(pool),
            max_burn_revert_redrives: 5,
            notifier: Arc::new(NoopNotifier),
        }
    }

    struct AwaitingAlpacaToBaseResume;

    #[async_trait]
    impl PrepareAlpacaToBaseResume for AwaitingAlpacaToBaseResume {
        async fn prepare_alpaca_to_base_resume(
            &self,
            _id: &UsdcRebalanceId,
        ) -> Result<AlpacaToBaseResumePreparation, AlpacaToBaseResumePreparationError> {
            Ok(AlpacaToBaseResumePreparation::AwaitingFreshHedgingSnapshot)
        }
    }

    #[tokio::test]
    async fn market_making_job_waits_for_recovered_source_reservation() {
        let pool = setup_queue_pool().await;
        let transfer = Arc::new(RecordingResume {
            fail: false,
            captured: std::sync::Mutex::new(None),
        });
        let mut ctx = market_making_ctx(transfer.clone(), &pool);
        ctx.resume_preparation = Arc::new(AwaitingAlpacaToBaseResume);
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert!(
            transfer.captured.lock().unwrap().is_none(),
            "the transfer manager must not run before recovery restores source inventory"
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "the gated job must enqueue one delayed replacement"
        );
        let (_, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        assert!(
            run_at
                >= before + i64::try_from(INVENTORY_RECOVERY_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at
                    <= after
                        + i64::try_from(INVENTORY_RECOVERY_REDRIVE_DELAY.as_secs()).unwrap()
                        + 5,
            "recovery redrive must be delayed by ~{INVENTORY_RECOVERY_REDRIVE_DELAY:?}: \
             run_at={run_at} before={before} after={after}"
        );
    }

    #[tokio::test]
    async fn market_making_job_reschedules_attestation_timeout() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(Arc::new(TimeoutAlpacaToBase), &pool);
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "attestation timeout should enqueue a delayed replacement job"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount, got {} vs {}",
            rescheduled.amount,
            job.amount
        );
        assert!(
            run_at >= before + 55 && run_at <= after + 65,
            "redrive must be delayed by ~{ATTESTATION_REDRIVE_DELAY:?} -- neither immediate nor \
             excessive: run_at={run_at} before={before} after={after}"
        );
    }

    /// `WithdrawalPollInconclusive` before the alert deadline must schedule a
    /// delayed redrive (one Pending job row with `WITHDRAWAL_POLL_REDRIVE_DELAY`) and
    /// return `Ok` so the apalis retry budget is not consumed -- warn log only,
    /// no operator alert. `revert_redrive_attempts` must not be incremented
    /// (the re-poll redrive is unbounded and independent of the burn-revert budget).
    #[tokio::test]
    async fn market_making_job_redrives_on_withdrawal_poll_inconclusive() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(InconclusiveAlpacaToBase::before_deadline()),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "WithdrawalPollInconclusive must enqueue exactly one delayed redrive job"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount, got {} vs {}",
            rescheduled.amount,
            job.amount
        );
        assert!(
            run_at >= before + i64::try_from(WITHDRAWAL_POLL_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at
                    <= after + i64::try_from(WITHDRAWAL_POLL_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{WITHDRAWAL_POLL_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
        // No alert before the deadline: this is a normal transient outcome.
        assert!(
            notifier.messages().is_empty(),
            "WithdrawalPollInconclusive before deadline must not fire an operator alert, \
             got: {:?}",
            notifier.messages()
        );
        // The burn-revert budget must not be touched by a withdrawal re-poll.
        assert_eq!(
            rescheduled.revert_redrive_attempts, 0,
            "WithdrawalPollInconclusive must not increment revert_redrive_attempts: \
             the re-poll redrive is unbounded and independent of the burn-revert budget"
        );
    }

    /// A future `initiated_at` can happen after clock skew on restart. The
    /// elapsed calculation must treat it as `None`, which keeps the transfer on
    /// the pre-deadline cadence and avoids a spurious operator page.
    #[tokio::test]
    async fn market_making_job_redrives_future_initiated_at_without_alert() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(InconclusiveAlpacaToBase::future_initiated_at()),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            withdrawal_poll_deadline_elapsed(None),
            None,
            "future initiated_at maps to elapsed=None, so the deadline is not elapsed"
        );

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "future initiated_at must still enqueue one delayed redrive"
        );

        let (_payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        assert!(
            run_at >= before + i64::try_from(WITHDRAWAL_POLL_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at
                    <= after + i64::try_from(WITHDRAWAL_POLL_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "future initiated_at must use the pre-deadline delay -- \
             run_at={run_at} before={before} after={after}"
        );
        assert!(
            notifier.messages().is_empty(),
            "future initiated_at must not fire an operator alert, got: {:?}",
            notifier.messages()
        );
    }

    /// `WithdrawalPollInconclusive` at or after the alert deadline must fire an
    /// operator alert via the notifier while STILL scheduling the delayed redrive
    /// and returning `Ok`. The guard stays held and re-polling continues.
    #[tokio::test]
    async fn market_making_job_fires_alert_on_withdrawal_poll_deadline_elapsed() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(InconclusiveAlpacaToBase::after_deadline()),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        // perform must still return Ok: deadline-elapsed does NOT consume the
        // apalis retry budget or emit FailWithdrawal.
        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        // The delayed redrive must still be enqueued (re-polling continues),
        // but at the post-deadline cadence (30 min) to prevent alert fatigue.
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "deadline-elapsed must still enqueue a delayed redrive job (guard stays held)"
        );
        let (_payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        assert!(
            run_at
                >= before
                    + i64::try_from(WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY.as_secs()).unwrap()
                    - 5
                && run_at
                    <= after
                        + i64::try_from(WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY.as_secs())
                            .unwrap()
                        + 5,
            "post-deadline redrive must use the longer {WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY:?} \
             delay to prevent alert fatigue -- run_at={run_at} before={before} after={after}"
        );

        // Exactly one alert must fire with the correct operator instructions.
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "WithdrawalPollInconclusive past deadline must fire exactly one operator alert, \
             got: {messages:?}"
        );
        let alert = &messages[0];
        assert!(
            alert.contains(&job.id.to_string()),
            "alert must contain the transfer id so the operator can act on it; got: {alert:?}"
        );
        assert!(
            alert.contains("stox transfer resume"),
            "alert must contain the recovery command; got: {alert:?}"
        );
        assert!(
            alert.contains("to-raindex"),
            "alert must contain the --direction flag; got: {alert:?}"
        );
        assert!(
            alert.contains("timed out after 1800s"),
            "alert must contain the underlying Alpaca polling error; got: {alert:?}"
        );
    }

    /// `WithdrawalPollInconclusive` at the deadline boundary must fire the operator
    /// alert and use the post-deadline redrive delay.
    #[tokio::test]
    async fn market_making_job_fires_alert_at_exact_deadline_boundary() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(InconclusiveAlpacaToBase::at_deadline()),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        // perform must return Ok even at the exact boundary.
        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        // Redrive at post-deadline cadence (30 min).
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "exact-boundary must still enqueue a delayed redrive (guard held)"
        );
        let (_payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        assert!(
            run_at
                >= before
                    + i64::try_from(WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY.as_secs()).unwrap()
                    - 5
                && run_at
                    <= after
                        + i64::try_from(WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY.as_secs())
                            .unwrap()
                        + 5,
            "exact-boundary redrive must use the post-deadline delay -- run_at={run_at} before={before} after={after}"
        );

        // Alert must fire at the exact boundary (>=, not >).
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "WithdrawalPollInconclusive at exact deadline boundary must fire the operator alert \
             (>= comparison); got: {messages:?}"
        );
    }

    #[tokio::test]
    async fn hedging_job_treats_deadline_elapsed_as_clean_terminal() {
        let pool = setup_queue_pool().await;
        let ctx = hedging_ctx(
            Arc::new(TerminalBaseToAlpaca(TerminalOutcome::DeadlineElapsed)),
            &pool,
        );
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("deadline-elapsed must be a clean terminal outcome, not a job error");

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "a deadline-elapsed transfer is terminally failed; the job must not redrive it"
        );
    }

    #[tokio::test]
    async fn hedging_job_treats_previously_failed_as_clean_terminal() {
        let pool = setup_queue_pool().await;
        let ctx = hedging_ctx(
            Arc::new(TerminalBaseToAlpaca(TerminalOutcome::PreviouslyFailed)),
            &pool,
        );
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        job.perform(&ctx).await.expect(
            "a previously-failed aggregate must be a clean terminal outcome, not a job error",
        );

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "a previously-failed transfer must not be redriven and must not trip the breaker"
        );
    }

    #[tokio::test]
    async fn market_making_job_treats_deadline_elapsed_as_clean_terminal() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(
            Arc::new(TerminalAlpacaToBase(TerminalOutcome::DeadlineElapsed)),
            &pool,
        );
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("deadline-elapsed must be a clean terminal outcome, not a job error");

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "a deadline-elapsed transfer is terminally failed; the job must not redrive it"
        );
    }

    #[tokio::test]
    async fn market_making_job_treats_previously_failed_as_clean_terminal() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(
            Arc::new(TerminalAlpacaToBase(TerminalOutcome::PreviouslyFailed)),
            &pool,
        );
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        job.perform(&ctx).await.expect(
            "a previously-failed aggregate must be a clean terminal outcome, not a job error",
        );

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "a previously-failed transfer must not be redriven and must not trip the breaker"
        );
    }

    #[tokio::test]
    async fn market_making_job_treats_ambient_balance_as_clean_terminal() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(
            Arc::new(TerminalAlpacaToBase(TerminalOutcome::AmbientBalance)),
            &pool,
        );
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("ambient balance must be a clean terminal outcome, not a job error");

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "an ambient-balance failure must not be redriven and must not trip the breaker"
        );
    }

    /// Fail-closed terminals that must not trigger an apalis retry MUST end the
    /// job cleanly (`Ok`, never a retryable `Err`) with NO redrive and exactly
    /// one operator alert. Returning `Err` would trigger an apalis retry, which
    /// is wrong for burn-submission terminals (a redrive could reburn a
    /// possibly-in-flight burn) and for vault-liquidity terminals (a redrive
    /// re-issues a deterministically-reverting withdraw, burning gas).
    async fn assert_hedging_fail_closed(
        outcome: TerminalOutcome,
        label: &str,
        expect_burn_tx: Option<TxHash>,
    ) {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(TerminalBaseToAlpaca(outcome)),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        Job::perform(&job, &ctx).await.unwrap_or_else(|error| {
            panic!("{label} must end the job cleanly (Ok), never an apalis-retryable error; got: {error:?}")
        });

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "{label} must NOT redrive -- a redrive could reburn a possibly-in-flight burn \
             or re-issue a deterministically-reverting withdraw"
        );
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "{label} must fire exactly one operator alert"
        );
        assert!(
            messages[0].contains(&job.id.to_string()),
            "{label} alert must include the transfer id; got: {:?}",
            messages[0]
        );
        if let Some(burn_tx) = expect_burn_tx {
            assert!(
                messages[0].contains(&burn_tx.to_string()),
                "{label} alert must include the burn tx hash; got: {:?}",
                messages[0]
            );
        }
    }

    async fn assert_market_making_fail_closed(
        outcome: TerminalOutcome,
        label: &str,
        expect_burn_tx: Option<TxHash>,
    ) {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(TerminalAlpacaToBase(outcome)),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        Job::perform(&job, &ctx).await.unwrap_or_else(|error| {
            panic!("{label} must end the job cleanly (Ok), never an apalis-retryable error; got: {error:?}")
        });

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "{label} must NOT redrive -- an auto-redrive could reburn a possibly-in-flight burn"
        );
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "{label} must fire exactly one operator alert"
        );
        assert!(
            messages[0].contains(&job.id.to_string()),
            "{label} alert must include the transfer id; got: {:?}",
            messages[0]
        );
        if let Some(burn_tx) = expect_burn_tx {
            assert!(
                messages[0].contains(&burn_tx.to_string()),
                "{label} alert must include the burn tx hash; got: {:?}",
                messages[0]
            );
        }
    }

    #[tokio::test]
    async fn hedging_job_fails_closed_on_burn_submit_inconclusive() {
        assert_hedging_fail_closed(
            TerminalOutcome::BurnSubmitInconclusive,
            "BurnSubmitInconclusive (hedging)",
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn hedging_job_fails_closed_on_burn_record_failed() {
        assert_hedging_fail_closed(
            TerminalOutcome::BurnRecordFailed,
            "BurnRecordFailed (hedging)",
            Some(TxHash::from([0xCD; 32])),
        )
        .await;
    }

    #[tokio::test]
    async fn hedging_job_fails_closed_on_burn_record_task_failed() {
        assert_hedging_fail_closed(
            TerminalOutcome::BurnRecordTaskFailed,
            "BurnRecordTaskFailed (hedging)",
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn hedging_job_fails_closed_on_burn_tx_dropped() {
        assert_hedging_fail_closed(
            TerminalOutcome::BurnTxDropped,
            "BurnTxDropped (hedging)",
            Some(TxHash::from([0xAB; 32])),
        )
        .await;
    }

    /// An `InsufficientVaultLiquidity` withdraw revert is atomic (nothing left
    /// the vault) and deterministic: re-issuing the withdraw reverts again until
    /// the vault is refunded. The job must latch the aggregate at
    /// `WithdrawalSubmitting` (Ok, no redrive, one alert) instead of letting
    /// apalis retries burn gas re-submitting the same reverting withdraw.
    #[tokio::test]
    async fn hedging_job_latches_on_insufficient_vault_liquidity() {
        assert_hedging_fail_closed(
            TerminalOutcome::InsufficientVaultLiquidity,
            "InsufficientVaultLiquidity (hedging)",
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn market_making_job_fails_closed_on_burn_submit_inconclusive() {
        assert_market_making_fail_closed(
            TerminalOutcome::BurnSubmitInconclusive,
            "BurnSubmitInconclusive (market-making)",
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn market_making_job_fails_closed_on_burn_record_failed() {
        assert_market_making_fail_closed(
            TerminalOutcome::BurnRecordFailed,
            "BurnRecordFailed (market-making)",
            Some(TxHash::from([0xCD; 32])),
        )
        .await;
    }

    #[tokio::test]
    async fn market_making_job_fails_closed_on_burn_record_task_failed() {
        assert_market_making_fail_closed(
            TerminalOutcome::BurnRecordTaskFailed,
            "BurnRecordTaskFailed (market-making)",
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn market_making_job_fails_closed_on_burn_tx_dropped() {
        assert_market_making_fail_closed(
            TerminalOutcome::BurnTxDropped,
            "BurnTxDropped (market-making)",
            Some(TxHash::from([0xAB; 32])),
        )
        .await;
    }

    /// Records the resume call and returns a configurable outcome, so the
    /// Alpaca->Base job's `perform` can be tested without onchain/broker setup.
    struct RecordingResume {
        fail: bool,
        captured: std::sync::Mutex<Option<(UsdcRebalanceId, Usdc)>>,
    }

    #[async_trait]
    impl ResumeAlpacaToBase for RecordingResume {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            *self.captured.lock().unwrap() = Some((id.clone(), amount));
            if self.fail {
                Err(UsdcTransferError::WithdrawalFailed {
                    status: "test-induced".to_string(),
                })
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn market_making_perform_forwards_id_and_amount_to_resume() {
        let pool = setup_queue_pool().await;
        let stub = Arc::new(RecordingResume {
            fail: false,
            captured: std::sync::Mutex::new(None),
        });
        let ctx = market_making_ctx(stub.clone(), &pool);
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc::new(float!(250));
        let job = TransferUsdcToMarketMaking {
            id: id.clone(),
            amount,
            revert_redrive_attempts: 0,
        };

        Job::perform(&job, &ctx).await.unwrap();

        let captured = stub.captured.lock().unwrap().clone();
        assert_eq!(
            captured,
            Some((id, amount)),
            "perform must forward its id and amount to resume_alpaca_to_base",
        );
    }

    #[tokio::test]
    async fn market_making_perform_returns_ok_on_successful_resume() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(
            Arc::new(RecordingResume {
                fail: false,
                captured: std::sync::Mutex::new(None),
            }),
            &pool,
        );
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        Job::perform(&job, &ctx).await.unwrap();
    }

    #[tokio::test]
    async fn market_making_perform_propagates_resume_failure() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(
            Arc::new(RecordingResume {
                fail: true,
                captured: std::sync::Mutex::new(None),
            }),
            &pool,
        );
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        // The failure must propagate (not be swallowed) so apalis retries and the
        // event-driven `usdc_in_progress` guard stays latched until the aggregate
        // reaches a terminal state -- swallowing it would free the guard and let a
        // fresh transfer arm on top of a partial one.
        assert!(
            matches!(error, TransferUsdcToMarketMakingJobError::Transfer(_)),
            "perform must propagate the resume failure as a Transfer error, got {error:?}",
        );
    }

    /// The `#[serde(default)]` annotation on `revert_redrive_attempts` is
    /// load-bearing: on first deploy, all in-flight apalis job rows will lack
    /// the field in their serialized JSON payload. Verify that deserialization
    /// of a legacy payload (missing field) defaults to 0, not a parse error.
    #[test]
    fn hedging_job_deserializes_legacy_payload_without_redrive_attempts() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc::new(float!(100));
        let json = serde_json::json!({
            "id": id,
            "amount": amount
        })
        .to_string();

        let job: TransferUsdcToHedging = serde_json::from_str(&json).unwrap();
        assert_eq!(job.id, id, "deserialized id must match",);
        assert_eq!(
            job.revert_redrive_attempts, 0,
            "missing revert_redrive_attempts must default to 0 (serde(default))"
        );
    }

    /// Symmetric backward-compat test for the market-making direction.
    #[test]
    fn market_making_job_deserializes_legacy_payload_without_redrive_attempts() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc::new(float!(100));
        let json = serde_json::json!({
            "id": id,
            "amount": amount
        })
        .to_string();

        let job: TransferUsdcToMarketMaking = serde_json::from_str(&json).unwrap();
        assert_eq!(job.id, id, "deserialized id must match",);
        assert_eq!(
            job.revert_redrive_attempts, 0,
            "missing revert_redrive_attempts must default to 0 (serde(default))"
        );
    }

    /// Stubs that return settlement-wait errors (retryable, not consumer of
    /// apalis retry budget).
    struct UnderconfirmedWithdrawal;

    #[async_trait]
    impl ResumeAlpacaToBase for UnderconfirmedWithdrawal {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::WithdrawalTxUnderconfirmed {
                id: id.clone(),
                tx: TxHash::ZERO,
                required: 3,
                actual: 1,
            })
        }
    }

    struct InsufficientUsdcBalance;

    #[async_trait]
    impl ResumeAlpacaToBase for InsufficientUsdcBalance {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::WalletUsdcInsufficient {
                id: id.clone(),
                nominal: Usdc::new(float!(1)),
            })
        }
    }

    /// Hypothesis: WithdrawalTxUnderconfirmed re-enqueues with
    /// SETTLEMENT_REDRIVE_DELAY and returns Ok (job stays alive, no apalis
    /// retry budget consumed).
    #[tokio::test]
    async fn market_making_job_reschedules_underconfirmed_withdrawal() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(Arc::new(UnderconfirmedWithdrawal), &pool);
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "WithdrawalTxUnderconfirmed must re-enqueue a delayed replacement job"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount, got {} vs {}",
            rescheduled.amount,
            job.amount
        );
        assert!(
            run_at >= before + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at <= after + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{SETTLEMENT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// Hypothesis: WalletUsdcInsufficient re-enqueues with
    /// SETTLEMENT_REDRIVE_DELAY and returns Ok (job stays alive, no apalis
    /// retry budget consumed).
    #[tokio::test]
    async fn market_making_job_reschedules_insufficient_usdc_balance() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(Arc::new(InsufficientUsdcBalance), &pool);
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "WalletUsdcInsufficient must re-enqueue a delayed replacement job"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount, got {} vs {}",
            rescheduled.amount,
            job.amount
        );
        assert!(
            run_at >= before + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at <= after + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{SETTLEMENT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// Stubs for `SettlementCheckTransient` -- models an RPC failure during the
    /// settlement-phase confirmation re-check or the BridgingSubmitting scan.
    struct SettlementRpcFailure;

    #[async_trait]
    impl ResumeAlpacaToBase for SettlementRpcFailure {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::SettlementCheckTransient {
                id: id.clone(),
                source: Box::new(CctpError::ScanInconclusive { from_block: 42 }),
            })
        }
    }

    /// Hypothesis: SettlementCheckTransient (e.g. confirmation-check RPC failure)
    /// re-enqueues with SETTLEMENT_REDRIVE_DELAY and returns Ok -- the job stays
    /// alive without consuming the apalis retry budget.
    #[tokio::test]
    async fn market_making_job_reschedules_settlement_check_transient() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(Arc::new(SettlementRpcFailure), &pool);
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "SettlementCheckTransient must re-enqueue a delayed replacement job"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount, got {} vs {}",
            rescheduled.amount,
            job.amount
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, job.revert_redrive_attempts,
            "SettlementCheckTransient must not consume the revert-redrive budget"
        );
        assert!(
            run_at >= before + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at <= after + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{SETTLEMENT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// Stub for `SettlementCheckTransient` on the Base->Alpaca (hedging)
    /// direction -- models an inconclusive Base burn scan or a settlement-phase
    /// RPC failure surfaced by `resume_bridging_submitting`.
    struct SettlementRpcFailureBaseToAlpaca;

    #[async_trait]
    impl ResumeBaseToAlpaca for SettlementRpcFailureBaseToAlpaca {
        async fn resume_base_to_alpaca(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::SettlementCheckTransient {
                id: id.clone(),
                source: Box::new(CctpError::ScanInconclusive { from_block: 42 }),
            })
        }
    }

    /// Hypothesis: SettlementCheckTransient on the hedging direction (e.g. an
    /// inconclusive Base burn scan) re-enqueues with SETTLEMENT_REDRIVE_DELAY and
    /// returns Ok -- the job delayed-redrives without tripping the circuit, so the
    /// guard is not latched on a normal self-heal outcome.
    #[tokio::test]
    async fn hedging_job_reschedules_settlement_check_transient() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(SettlementRpcFailureBaseToAlpaca),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        Job::perform(&job, &ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "SettlementCheckTransient must re-enqueue a delayed replacement job"
        );
        assert_eq!(
            notifier.messages().len(),
            0,
            "an inconclusive settlement check is a normal self-heal outcome and must not \
             fire a terminal alert (which would page the operator and open the circuit)"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        let rescheduled: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount, got {} vs {}",
            rescheduled.amount,
            job.amount
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, job.revert_redrive_attempts,
            "SettlementCheckTransient must not consume the revert-redrive budget"
        );
        assert!(
            run_at >= before + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at <= after + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{SETTLEMENT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    // --- Burn-revert redrive tests ------------------------------------------------

    /// A notifier that always returns an error. Used to verify that a failing
    /// notifier does not abort the job -- errors are swallowed with a warning.
    struct FailingNotifier;

    #[async_trait]
    impl crate::alerts::Notifier for FailingNotifier {
        async fn notify(&self, _message: &str) -> Result<(), crate::alerts::NotifierError> {
            Err(crate::alerts::NotifierError::ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
            })
        }
    }

    /// A failing notifier must not abort the job. The notifier error is swallowed
    /// and logged as a warning; the job returns the same outcome it would have
    /// with a working notifier.
    #[tokio::test]
    async fn hedging_job_failing_notifier_does_not_abort_job() {
        let pool = setup_queue_pool().await;
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(BurnRevertResume),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 1,
            notifier: Arc::new(FailingNotifier),
        };
        // attempts=0 -> next=1 == max=1: limit alert fires (and is swallowed), redrive enqueued
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        // A failing notifier must not prevent the redrive from being enqueued
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "failing notifier must not prevent the redrive job from being enqueued"
        );
    }

    /// Returns a revert-class burn error. This simulates what
    /// `burn_recording_pending` emits when the burn EVM call reverts: `BurnRevert`,
    /// not `Cctp`. Only the burn call site emits `BurnRevert`; the mint path and
    /// other CCTP failures emit `Cctp`.
    fn revert_burn_error() -> UsdcTransferError {
        UsdcTransferError::BurnRevert(Box::new(CctpError::Evm(EvmError::Reverted {
            tx_hash: TxHash::ZERO,
        })))
    }

    /// Returns a non-revert `CctpError` (post-burn-success-but-undecodable).
    fn non_revert_burn_error() -> UsdcTransferError {
        UsdcTransferError::Cctp(Box::new(CctpError::MessageSentEventNotFound {
            tx_hash: TxHash::ZERO,
        }))
    }

    struct BurnRevertResume;

    #[async_trait]
    impl ResumeBaseToAlpaca for BurnRevertResume {
        async fn resume_base_to_alpaca(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(revert_burn_error())
        }
    }

    struct NonRevertBurnErrorResume;

    #[async_trait]
    impl ResumeBaseToAlpaca for NonRevertBurnErrorResume {
        async fn resume_base_to_alpaca(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(non_revert_burn_error())
        }
    }

    /// A revert-class burn error on the first attempt must return Ok and enqueue
    /// a delayed replacement job with `revert_redrive_attempts = 1`.
    /// The safety guarantee is `resume_bridging_submitting`'s scan-or-reburn
    /// path, not this classification.
    #[tokio::test]
    async fn hedging_job_redrives_burn_revert_first_attempt() {
        let pool = setup_queue_pool().await;
        let ctx = hedging_ctx(Arc::new(BurnRevertResume), &pool);
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        Job::perform(&job, &ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "a revert-class burn error must redrive rather than trip the circuit breaker"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        let rescheduled: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount"
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, 1,
            "revert_redrive_attempts must be incremented to 1 in the redrive payload"
        );
        assert!(
            run_at >= before + i64::try_from(BURN_REVERT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at
                    <= after + i64::try_from(BURN_REVERT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{BURN_REVERT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// After `max_burn_revert_redrives` redrives the job must propagate
    /// `BurnRevertLimitReached` so the circuit opens and the operator is alerted.
    #[tokio::test]
    async fn hedging_job_hits_redrive_limit_on_revert() {
        let pool = setup_queue_pool().await;
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(BurnRevertResume),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: Arc::new(NoopNotifier),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                TransferUsdcToHedgingJobError::BurnRevertLimitReached { .. }
            ),
            "at the redrive limit a burn revert must propagate BurnRevertLimitReached, got {error:?}",
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "limit-reached must NOT enqueue a new pending job"
        );
    }

    /// A non-revert `CctpError` (e.g. `MessageSentEventNotFound`) is NOT a
    /// safe-to-redrive error; it must propagate immediately as `Err` so the
    /// circuit opens.
    #[tokio::test]
    async fn hedging_job_does_not_redrive_non_revert_cctp_error() {
        let pool = setup_queue_pool().await;
        let ctx = hedging_ctx(Arc::new(NonRevertBurnErrorResume), &pool);
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(error, TransferUsdcToHedgingJobError::Transfer(_)),
            "a non-revert CCTP error must propagate as Transfer, not redrive; got {error:?}",
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "a non-revert error must NOT enqueue a pending job"
        );
    }

    /// A terminal non-redriven error must fire the notifier before the circuit
    /// opens, so the operator receives an alert.
    #[tokio::test]
    async fn hedging_job_fires_alert_on_terminal_error() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(NonRevertBurnErrorResume),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        Job::perform(&job, &ctx).await.unwrap_err();

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "exactly one alert must fire on a terminal non-redriven error"
        );
        assert!(
            messages[0].contains(&job.id.to_string()),
            "alert message must include the transfer id; got: {:?}",
            messages[0]
        );
    }

    /// The notifier must fire exactly once when `revert_redrive_attempts`
    /// reaches the warn threshold (max/2+1). With max=5, threshold=3: starting
    /// at attempts=2, next=3 fires exactly one alert and enqueues one pending job.
    #[tokio::test]
    async fn hedging_job_fires_alert_at_warn_threshold() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(BurnRevertResume),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        // attempts=2 -> next=3 == 5/2+1 == 3: exactly at threshold
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
        };

        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            notifier.messages().len(),
            1,
            "exactly one alert must fire at the warn threshold"
        );
        assert!(
            notifier.messages()[0].contains("retried"),
            "warn-threshold message must mention retry count; got: {:?}",
            notifier.messages()[0]
        );
        assert!(
            notifier.messages()[0].contains(&job.id.to_string()),
            "warn-threshold alert must include the transfer id; got: {:?}",
            notifier.messages()[0]
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "a delayed pending job must still be enqueued at the warn threshold"
        );
    }

    /// When `next_attempts == max_burn_revert_redrives` (last allowed redrive),
    /// the job must fire exactly one alert and enqueue the final delayed job.
    /// This ensures the alert fires exactly once (on the Ok-returning run, not on
    /// the Err-returning run that apalis retries up to 3 times).
    #[tokio::test]
    async fn hedging_job_fires_alert_at_last_redrive() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(BurnRevertResume),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: notifier.clone(),
        };
        // attempts=2 -> next=3 == max=3: last allowed redrive, alert fires
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
        };

        // Returns Ok (last redrive enqueued), NOT Err
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            notifier.messages().len(),
            1,
            "exactly one alert must fire on the last allowed redrive"
        );
        assert!(
            notifier.messages()[0].contains("limit reached"),
            "last-redrive message must say 'limit reached'; got: {:?}",
            notifier.messages()[0]
        );
        assert!(
            notifier.messages()[0].contains(&job.id.to_string()),
            "alert must include the transfer id; got: {:?}",
            notifier.messages()[0]
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "last redrive must enqueue one pending job"
        );
    }

    /// After the last redrive the job must return `BurnRevertLimitReached`
    /// with NO alert (apalis will retry this Err; alerting here fires multiple times).
    #[tokio::test]
    async fn hedging_job_errors_after_last_redrive_no_alert() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(BurnRevertResume),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: notifier.clone(),
        };
        // attempts=3 -> next=4 > max=3: over-limit, returns Err, no alert
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                TransferUsdcToHedgingJobError::BurnRevertLimitReached { .. }
            ),
            "over-limit must return BurnRevertLimitReached, got {error:?}",
        );
        assert_eq!(
            notifier.messages().len(),
            0,
            "over-limit must NOT fire an alert (would fire 4x due to apalis retries)"
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "over-limit must NOT enqueue a new pending job"
        );
    }

    /// With `max_burn_revert_redrives = 1`, `warn_threshold` returns `None` so
    /// no early-warning alert fires. Only the limit alert fires (on the first
    /// and only redrive attempt). Exactly one alert total, one pending job.
    #[tokio::test]
    async fn hedging_job_max_redrives_of_one_fires_exactly_one_limit_alert() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(BurnRevertResume),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 1,
            notifier: notifier.clone(),
        };
        // attempts=0 -> next=1 == max=1: last allowed redrive, limit alert fires
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        Job::perform(&job, &ctx).await.unwrap();

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "max=1: exactly one alert (limit) must fire, not two; got: {messages:?}"
        );
        assert!(
            messages[0].contains("limit reached"),
            "max=1: alert must say 'limit reached'; got: {:?}",
            messages[0]
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "max=1: the single redrive job must still be enqueued"
        );
    }

    /// With `max_burn_revert_redrives = 2`, `warn_threshold` returns `None` so
    /// no early-warning alert fires. The limit alert fires on attempt 2, the
    /// warn-threshold branch is skipped entirely (no room for a distinct warn).
    #[tokio::test]
    async fn hedging_job_max_redrives_of_two_no_warn_alert() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(BurnRevertResume),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 2,
            notifier: notifier.clone(),
        };
        // attempts=1 -> next=2 == max=2: last allowed redrive, limit alert fires
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 1,
        };

        Job::perform(&job, &ctx).await.unwrap();

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "max=2: exactly one alert (limit) must fire, no separate warn; got: {messages:?}"
        );
        assert!(
            messages[0].contains("limit reached"),
            "max=2: alert must say 'limit reached'; got: {:?}",
            messages[0]
        );
    }

    /// When `next_attempts == max_burn_revert_redrives` (last allowed timeout
    /// redrive), the job must fire exactly one alert and enqueue the final
    /// delayed job. Alert fires exactly once (Ok-path, not Err-path).
    #[tokio::test]
    async fn hedging_job_fires_alert_at_last_timeout_redrive() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(HangingResume),
            timeout: Duration::from_millis(50),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: notifier.clone(),
        };
        // attempts=2 -> next=3 == max=3: last allowed timeout redrive, alert fires
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
        };

        // Returns Ok (last redrive enqueued), NOT Err
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            notifier.messages().len(),
            1,
            "exactly one alert must fire on the last allowed timeout redrive"
        );
        assert!(
            notifier.messages()[0].contains("limit reached"),
            "last-timeout-redrive message must say 'limit reached'; got: {:?}",
            notifier.messages()[0]
        );
        assert!(
            notifier.messages()[0].contains(&job.id.to_string()),
            "alert must include the transfer id; got: {:?}",
            notifier.messages()[0]
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "last timeout redrive must enqueue one pending job"
        );
    }

    /// After the last timeout redrive the job returns `TimeoutLimitReached`
    /// with NO alert (would fire 4x due to apalis retries).
    #[tokio::test]
    async fn hedging_job_errors_after_last_timeout_redrive_no_alert() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(HangingResume),
            timeout: Duration::from_millis(50),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: notifier.clone(),
        };
        // attempts=3 -> next=4 > max=3: over-limit, returns Err, no alert
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                TransferUsdcToHedgingJobError::TimeoutLimitReached { .. }
            ),
            "over-limit must return TimeoutLimitReached, got {error:?}",
        );
        assert_eq!(
            notifier.messages().len(),
            0,
            "over-limit must NOT fire an alert (would fire 4x due to apalis retries)"
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "over-limit must NOT enqueue a new pending job"
        );
    }

    /// The notifier must fire exactly once when a timeout hits the warn threshold.
    /// With max=5, threshold=3: starting at attempts=2, next=3 fires exactly one alert.
    #[tokio::test]
    async fn hedging_job_fires_alert_at_timeout_warn_threshold() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(HangingResume),
            timeout: Duration::from_millis(50),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        // attempts=2 -> next=3 == 5/2+1: exactly at threshold
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
        };

        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            notifier.messages().len(),
            1,
            "exactly one alert must fire at the timeout warn threshold"
        );
        assert!(
            notifier.messages()[0].contains("retried"),
            "warn-threshold message must mention retry count; got: {:?}",
            notifier.messages()[0]
        );
        assert!(
            notifier.messages()[0].contains(&job.id.to_string()),
            "timeout warn-threshold alert must include the transfer id; got: {:?}",
            notifier.messages()[0]
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "a delayed pending job must still be enqueued at the timeout warn threshold"
        );
    }

    // --- Market-making burn-revert redrive tests ---------------------------------

    /// Stub that returns a revert-class error from `resume_alpaca_to_base`.
    struct BurnRevertAlpacaToBase;

    #[async_trait]
    impl ResumeAlpacaToBase for BurnRevertAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(revert_burn_error())
        }
    }

    /// A revert-class burn error on the first market-making attempt must return
    /// Ok and enqueue a delayed replacement job with `revert_redrive_attempts = 1`.
    #[tokio::test]
    async fn market_making_job_redrives_burn_revert_first_attempt() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(Arc::new(BurnRevertAlpacaToBase), &pool);
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        Job::perform(&job, &ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "a revert-class burn error must redrive rather than trip the circuit breaker"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            rescheduled.amount.eq(&job.amount).unwrap(),
            "the rescheduled job must carry the same amount"
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, 1,
            "revert_redrive_attempts must be incremented to 1 in the redrive payload"
        );
        assert!(
            run_at >= before + i64::try_from(BURN_REVERT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at
                    <= after + i64::try_from(BURN_REVERT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{BURN_REVERT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// After `max_burn_revert_redrives` redrives the market-making job must
    /// propagate `BurnRevertLimitReached` so the circuit opens.
    #[tokio::test]
    async fn market_making_job_hits_redrive_limit_on_revert() {
        let pool = setup_queue_pool().await;
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(BurnRevertAlpacaToBase),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: Arc::new(NoopNotifier),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                TransferUsdcToMarketMakingJobError::BurnRevertLimitReached { .. }
            ),
            "at the redrive limit a burn revert must propagate BurnRevertLimitReached, \
             got {error:?}",
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "limit-reached must NOT enqueue a new pending job"
        );
    }

    /// The notifier must fire exactly once when the market-making job hits the
    /// revert warn threshold.
    #[tokio::test]
    async fn market_making_job_fires_alert_at_warn_threshold() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(BurnRevertAlpacaToBase),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        // attempts=2 -> next=3 == 5/2+1: exactly at threshold
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
        };

        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            notifier.messages().len(),
            1,
            "exactly one alert must fire at the warn threshold"
        );
        assert!(
            notifier.messages()[0].contains("retried"),
            "warn-threshold message must mention retry count; got: {:?}",
            notifier.messages()[0]
        );
        assert!(
            notifier.messages()[0].contains(&job.id.to_string()),
            "warn-threshold alert must include the transfer id; got: {:?}",
            notifier.messages()[0]
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "a delayed pending job must still be enqueued at the warn threshold"
        );
    }

    /// When `next_attempts == max_burn_revert_redrives` (last allowed redrive),
    /// the market-making job must fire exactly one alert and enqueue the final
    /// delayed job. Alert fires exactly once (Ok-path).
    #[tokio::test]
    async fn market_making_job_fires_alert_at_last_redrive() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(BurnRevertAlpacaToBase),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: notifier.clone(),
        };
        // attempts=2 -> next=3 == max=3: last allowed redrive, alert fires
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
        };

        // Returns Ok (last redrive enqueued), NOT Err
        Job::perform(&job, &ctx).await.unwrap();

        assert_eq!(
            notifier.messages().len(),
            1,
            "exactly one alert must fire on the last allowed redrive"
        );
        assert!(
            notifier.messages()[0].contains("limit reached"),
            "last-redrive message must say 'limit reached'; got: {:?}",
            notifier.messages()[0]
        );
        assert!(
            notifier.messages()[0].contains(&job.id.to_string()),
            "alert must include the transfer id; got: {:?}",
            notifier.messages()[0]
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "last redrive must enqueue one pending job"
        );
    }

    /// After the last redrive the market-making job must return
    /// `BurnRevertLimitReached` with NO alert.
    #[tokio::test]
    async fn market_making_job_errors_after_last_redrive_no_alert() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(BurnRevertAlpacaToBase),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: notifier.clone(),
        };
        // attempts=3 -> next=4 > max=3: over-limit, Err, no alert
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                TransferUsdcToMarketMakingJobError::BurnRevertLimitReached { .. }
            ),
            "over-limit must return BurnRevertLimitReached, got {error:?}",
        );
        assert_eq!(
            notifier.messages().len(),
            0,
            "over-limit must NOT fire an alert (would fire 4x due to apalis retries)"
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "over-limit must NOT enqueue a new pending job"
        );
    }

    /// A terminal non-redriven error on the market-making job must fire the
    /// notifier before the circuit opens.
    #[tokio::test]
    async fn market_making_job_fires_alert_on_terminal_error() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());

        struct NonRevertAlpacaToBase;

        #[async_trait]
        impl ResumeAlpacaToBase for NonRevertAlpacaToBase {
            async fn resume_alpaca_to_base(
                &self,
                _id: &UsdcRebalanceId,
                _amount: Usdc,
            ) -> Result<(), UsdcTransferError> {
                Err(non_revert_burn_error())
            }
        }

        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(NonRevertAlpacaToBase),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        Job::perform(&job, &ctx).await.unwrap_err();

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "exactly one alert must fire on a terminal non-redriven error"
        );
        assert!(
            messages[0].contains(&job.id.to_string()),
            "alert message must include the transfer id; got: {:?}",
            messages[0]
        );
    }

    /// AttestationRetryDeadlineElapsed (hedging) must fire a notifier alert
    /// because it leaves the aggregate in an operator-reconciliation-bound state.
    #[tokio::test]
    async fn hedging_job_fires_alert_on_attestation_deadline_elapsed() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(TerminalBaseToAlpaca(TerminalOutcome::DeadlineElapsed)),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("deadline-elapsed is a clean terminal outcome");

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "attestation deadline elapsed must fire exactly one alert"
        );
        assert!(
            messages[0].contains(&job.id.to_string()),
            "alert must include the transfer id; got: {:?}",
            messages[0]
        );
    }

    /// AttestationRetryDeadlineElapsed (market-making) must fire a notifier alert.
    #[tokio::test]
    async fn market_making_job_fires_alert_on_attestation_deadline_elapsed() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(TerminalAlpacaToBase(TerminalOutcome::DeadlineElapsed)),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("deadline-elapsed is a clean terminal outcome");

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "attestation deadline elapsed must fire exactly one alert"
        );
        assert!(
            messages[0].contains(&job.id.to_string()),
            "alert must include the transfer id; got: {:?}",
            messages[0]
        );
    }

    /// A revert-class `CctpError` returned as `UsdcTransferError::Cctp` (as the
    /// MINT path emits after calling `FailBridging`) must NOT enter the burn-redrive
    /// path -- it must fall through to the terminal-error branch and propagate as Err.
    /// This is the HIGH #1 regression: before the `BurnRevert` variant,
    /// `is_burn_revert()` misrouted mint-side reverts into the redrive path,
    /// silently swallowing the operator alert.
    #[tokio::test]
    async fn hedging_job_cctp_revert_from_mint_path_goes_to_terminal_not_redrive() {
        struct MintPathRevert;

        #[async_trait]
        impl ResumeBaseToAlpaca for MintPathRevert {
            async fn resume_base_to_alpaca(
                &self,
                _id: &UsdcRebalanceId,
                _amount: Usdc,
            ) -> Result<(), UsdcTransferError> {
                // The mint path emits UsdcTransferError::Cctp(revert-class) after
                // FailBridging. Critically: NOT UsdcTransferError::BurnRevert.
                Err(UsdcTransferError::Cctp(Box::new(CctpError::Evm(
                    EvmError::Reverted {
                        tx_hash: TxHash::ZERO,
                    },
                ))))
            }
        }

        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(MintPathRevert),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        // Must error (terminal), not Ok (redrive)
        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(error, TransferUsdcToHedgingJobError::Transfer(_)),
            "a mint-path Cctp revert must propagate as terminal Transfer, not redrive; got {error:?}",
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "a mint-path Cctp revert must NOT enqueue a redrive job"
        );
        // Operator alert must fire (terminal path fires alert)
        assert_eq!(
            notifier.messages().len(),
            1,
            "exactly one alert must fire for a mint-path terminal error"
        );
        assert!(
            notifier.messages()[0].contains(&job.id.to_string()),
            "terminal alert must include the transfer id; got: {:?}",
            notifier.messages()[0]
        );
    }

    /// Symmetric to `hedging_job_cctp_revert_from_mint_path_goes_to_terminal_not_redrive`
    /// for the market-making (Alpaca->Base) direction.
    #[tokio::test]
    async fn market_making_job_cctp_revert_from_mint_path_goes_to_terminal_not_redrive() {
        struct MintPathRevertAlpacaToBase;

        #[async_trait]
        impl ResumeAlpacaToBase for MintPathRevertAlpacaToBase {
            async fn resume_alpaca_to_base(
                &self,
                _id: &UsdcRebalanceId,
                _amount: Usdc,
            ) -> Result<(), UsdcTransferError> {
                Err(UsdcTransferError::Cctp(Box::new(CctpError::Evm(
                    EvmError::Reverted {
                        tx_hash: TxHash::ZERO,
                    },
                ))))
            }
        }

        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(MintPathRevertAlpacaToBase),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(
            matches!(error, TransferUsdcToMarketMakingJobError::Transfer(_)),
            "a mint-path Cctp revert in market-making must propagate as terminal Transfer; got {error:?}",
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "a mint-path Cctp revert must NOT enqueue a redrive job"
        );
        assert_eq!(
            notifier.messages().len(),
            1,
            "exactly one alert must fire for a mint-path terminal error in market-making"
        );
        assert!(
            notifier.messages()[0].contains(&job.id.to_string()),
            "terminal alert must include the transfer id; got: {:?}",
            notifier.messages()[0]
        );
    }

    /// WalletUsdcAmbientBalance (market-making) must fire a notifier alert
    /// because it leaves the aggregate in an operator-reconciliation-bound state.
    #[tokio::test]
    async fn market_making_job_fires_alert_on_ambient_balance() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(TerminalAlpacaToBase(TerminalOutcome::AmbientBalance)),
            resume_preparation: Arc::new(ReadyAlpacaToBaseResume),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("ambient balance is a clean terminal outcome");

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "ambient balance must fire exactly one alert"
        );
        assert!(
            messages[0].contains(&job.id.to_string()),
            "alert must include the transfer id; got: {:?}",
            messages[0]
        );
    }
}
