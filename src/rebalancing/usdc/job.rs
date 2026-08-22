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
//!
//! The exceptions are the pre-flight refusals (`WalletUsdcAmbientPreflight`,
//! `WalletUsdcAmbientPreflightUnrepresentable`, and
//! `PreflightBalanceUnavailable`): they happen before the first aggregate
//! event, so no terminal event can ever clear the guard for them. Those arms
//! -- and only those arms -- release the guard from the worker.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use thiserror::Error;
use tracing::{error, warn};

use st0x_bridge::cctp::CctpError;
use st0x_event_sorcery::Store;
use st0x_evm::Wallet;
use st0x_execution::{AlpacaWalletError, Backpressure};
use st0x_finance::Usdc;

use super::UsdcTransferError;
use super::manager::CrossVenueCashTransfer;
use crate::alerts::Notifier;
use crate::bot_gas::redrive::{BotGasFailureClassifier, redrive_on_bot_gas_failure};
use crate::conductor::job::{
    BACKPRESSURE_ALERT_STREAK, BACKPRESSURE_RESCHEDULE_LIMIT, BackpressureOutcome,
    BackpressureStep, BackpressureStreak, Job, JobQueue, Label, QueuePushError,
    advance_backpressure, apply_backpressure_step, find_backpressure,
};
use crate::usdc_rebalance::{UsdcRebalance, UsdcRebalanceId, any_rebalance_holds_guard};

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

/// Duration after which repeated `MintRecoveryInconclusive` redrives page the
/// operator via the notifier. Mirrors `WITHDRAWAL_POLL_ALERT_DEADLINE`: the
/// deadline is durable, derived from whichever durable aggregate state the
/// transfer resumed from (`Bridging`, `AwaitingAttestation`, `Attested`, or a
/// post-burn `BridgingFailed`, all of which persist `initiated_at`), so the
/// countdown survives restarts.
///
/// 4 hours gives generous headroom above the ~2-minute internal probe window
/// (`MINT_RECOVERY_PROBES` x `MINT_RECOVERY_PROBE_INTERVAL` in the
/// `st0x-bridge` crate) that `recover_already_minted` runs per attempt, while
/// ensuring a durably degraded RPC endpoint or a nonce that never resolves
/// surfaces well before it becomes a multi-day silent outage.
const MINT_RECOVERY_ALERT_DEADLINE: Duration = Duration::from_secs(4 * 60 * 60);

/// Delay before re-probing an inconclusive CCTP mint recovery. It currently
/// matches `SETTLEMENT_REDRIVE_DELAY` (30 s), but remains distinct because
/// CCTP nonce polling and Ethereum settlement timing may evolve independently.
const MINT_RECOVERY_REDRIVE_DELAY: Duration = Duration::from_secs(30);

/// Redrive delay used AFTER the mint-recovery alert deadline has elapsed.
/// Mirrors `WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY`: slows the cadence
/// from `MINT_RECOVERY_REDRIVE_DELAY` (30 s) to prevent alert fatigue on the
/// shared Telegram channel, while the guard stays held and the re-probe --
/// idempotent against the same CCTP nonce -- keeps running.
const MINT_RECOVERY_POST_DEADLINE_REDRIVE_DELAY: Duration = Duration::from_secs(30 * 60);

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

/// Returns `elapsed` unchanged when it has reached or passed `deadline`, or
/// `None` otherwise. Shared by both the withdrawal-poll and mint-recovery
/// alert deadlines (`WITHDRAWAL_POLL_ALERT_DEADLINE` /
/// `MINT_RECOVERY_ALERT_DEADLINE`): each call site passes its own deadline
/// constant so the alert-triggering logic is written once.
fn deadline_elapsed(elapsed: Option<Duration>, deadline: Duration) -> Option<Duration> {
    elapsed.filter(|elapsed| *elapsed >= deadline)
}

/// Which cross-venue USDC transfer direction hit a post-burn CCTP mint
/// recovery that stayed inconclusive. The deadline math, notifier message
/// template, and redrive enqueue are identical between directions; only the
/// log label and the `stox transfer resume --direction` hint differ, so this
/// enum carries those two differences as data instead of duplicating the
/// surrounding logic per direction.
#[derive(Debug, Clone, Copy)]
enum MintRecoveryDirection {
    /// Base -> Alpaca (hedging).
    BaseToAlpaca,
    /// Alpaca -> Base (market making).
    AlpacaToBase,
}

impl MintRecoveryDirection {
    /// Human-readable label for the `warn!` log and operator alert, e.g.
    /// "Base->Alpaca".
    fn label(self) -> &'static str {
        match self {
            Self::BaseToAlpaca => "Base->Alpaca",
            Self::AlpacaToBase => "Alpaca->Base",
        }
    }

    /// The `stox transfer resume --direction` flag value naming this
    /// direction, for the manual-intervention hint in the alert message.
    fn cli_flag(self) -> &'static str {
        match self {
            Self::BaseToAlpaca => "to-alpaca",
            Self::AlpacaToBase => "to-raindex",
        }
    }
}

/// Reschedules a post-burn CCTP mint recovery that stayed inconclusive:
/// unbounded and budget-free, escalating to an operator alert once
/// `initiated_at` is past `MINT_RECOVERY_ALERT_DEADLINE`. Shared by
/// `TransferUsdcToHedging::handle_mint_recovery_inconclusive` and
/// `TransferUsdcToMarketMaking::handle_mint_recovery_inconclusive`, whose
/// logic was otherwise byte-for-byte identical between the two directions
/// (see `MintRecoveryDirection`'s doc for what actually differs).
///
/// The alert message reports `elapsed` as the transfer's total age since
/// `initiated_at`, NOT as "time the mint has been inconclusive": `initiated_at`
/// is stamped once at transfer start and carried unchanged through every
/// earlier phase (withdrawal, burn, attestation), so a transfer that spent
/// hours in an earlier phase before ever reaching mint recovery would
/// otherwise report a misleadingly large "inconclusive" duration on its very
/// first inconclusive probe. Phrasing it as total transfer age (with mint
/// recovery named as the CURRENT stuck stage) keeps the message honest while
/// still using `initiated_at` as the alert deadline -- anchoring the deadline
/// to when the mint phase itself began would need a new durable timestamp,
/// which is out of scope here (see the finding this addresses).
async fn schedule_mint_recovery_redrive<Task>(
    notifier: &Arc<dyn Notifier>,
    job_queue: &mut JobQueue<Task>,
    redriven_job: Task,
    direction: MintRecoveryDirection,
    id: UsdcRebalanceId,
    initiated_at: DateTime<Utc>,
    source: Box<CctpError>,
) -> Result<(), QueuePushError>
where
    Task: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
{
    // Mirror the `.ok()` pattern used for the withdrawal-poll deadline: a
    // future `initiated_at` (clock skew after restart) makes `to_std()`
    // return `Err`, treated as `None` so no spurious alert fires.
    let elapsed = Utc::now().signed_duration_since(initiated_at).to_std().ok();
    let alert_deadline_elapsed = deadline_elapsed(elapsed, MINT_RECOVERY_ALERT_DEADLINE);
    let redrive_delay = if alert_deadline_elapsed.is_some() {
        MINT_RECOVERY_POST_DEADLINE_REDRIVE_DELAY
    } else {
        MINT_RECOVERY_REDRIVE_DELAY
    };

    let label = direction.label();
    warn!(
        target: "rebalance",
        %id,
        %source,
        ?elapsed,
        delay = ?redrive_delay,
        "{label} USDC transfer: CCTP mint recovery inconclusive; rescheduling \
         (guard held, redrive continues)"
    );

    if let Some(elapsed) = alert_deadline_elapsed {
        let cli_flag = direction.cli_flag();
        let message = format!(
            "USDC transfer {id} ({label}): running for {elapsed:?} since it started \
             (>{MINT_RECOVERY_ALERT_DEADLINE:?}); currently stuck at CCTP mint recovery \
             -- nonce state unknown or mint receipt unreconstructible ({source}), the \
             mint may already have landed. Transfer stays mid-flight (guard held), \
             automatic redrive continues. Verify on-chain nonce/mint status before \
             acting; use `stox transfer resume --kind usdc --id {id} --direction \
             {cli_flag}` if the automatic redrive appears stuck."
        );
        if let Err(notify_err) = notifier.notify(&message).await {
            warn!(
                target: "rebalance",
                ?notify_err,
                "Failed to deliver mint-recovery-deadline-elapsed alert"
            );
        }
    }

    job_queue.push_with_delay(redriven_job, redrive_delay).await
}

/// Intercepts a bot-gas enqueue failure before either direction's
/// domain-specific error arms run.
///
/// Bot-gas cost recording is best-effort (see `BotGasReceiptCostEnqueuer`'s
/// doc, ADR 0017 SS4), so the failure is redriven through the shared mechanism
/// rather than consuming the apalis retry budget or opening the fail-stop
/// circuit. Whether the enqueue site runs before or after its
/// aggregate-advancing command (see `CrossVenueCashTransfer::enqueue_bot_gas_cost`'s
/// doc), the burn/withdraw/send resume paths scan-and-adopt rather than
/// re-executing the on-chain step, so redriving is safe either way.
///
/// `Break` carries the value the caller must return as-is; `Continue` carries
/// the untouched result for the caller's remaining arms.
async fn intercept_bot_gas_enqueue_failure<Ctx, TaskJob>(
    job: &TaskJob,
    job_queue: &JobQueue<TaskJob>,
    result: Result<(), UsdcTransferError>,
) -> ControlFlow<Result<(), TaskJob::Error>, Result<(), UsdcTransferError>>
where
    Ctx: Send + Sync + 'static,
    TaskJob: Job<Ctx> + Clone + Sync + Unpin,
    TaskJob::Error: From<UsdcTransferError> + BotGasFailureClassifier + std::fmt::Display,
{
    match result {
        Err(UsdcTransferError::BotGasEnqueue(push_error)) => ControlFlow::Break(
            redrive_on_bot_gas_failure(
                job,
                job_queue,
                SETTLEMENT_REDRIVE_DELAY,
                TaskJob::Error::from(UsdcTransferError::BotGasEnqueue(push_error)),
            )
            .await,
        ),
        other => ControlFlow::Continue(other),
    }
}

#[derive(Clone, Copy)]
enum BackpressureSite {
    Hedging,
    MarketMaking,
    WithdrawalPoll { deadline_elapsed: bool },
}

struct BackpressureLabels {
    direction: &'static str,
    context: &'static str,
    state: &'static str,
    alert: &'static str,
}

impl BackpressureSite {
    const fn labels(self) -> BackpressureLabels {
        match self {
            Self::Hedging => BackpressureLabels {
                direction: "Base->Alpaca",
                context: "broker rate-limiting",
                state: "mid-flight",
                alert: "hedging",
            },
            Self::MarketMaking => BackpressureLabels {
                direction: "Alpaca->Base",
                context: "broker rate-limiting",
                state: "mid-flight",
                alert: "market-making",
            },
            Self::WithdrawalPoll { .. } => BackpressureLabels {
                direction: "Alpaca->Base",
                context: "withdrawal poll broker rate-limiting",
                state: "in Withdrawing",
                alert: "withdrawal-poll",
            },
        }
    }

    const fn should_page_at_streak(self) -> bool {
        !matches!(
            self,
            Self::WithdrawalPoll {
                deadline_elapsed: true
            }
        )
    }
}

/// Shared log-and-notify tail for a backpressure `outcome`, used by every
/// call site that routes a classified 429 through `advance_backpressure`/
/// `apply_backpressure_step`: logs a loud, distinct `error!` and pages the
/// operator once on `DeadLettered` (unconditionally), or once when the
/// reschedule streak first crosses `BACKPRESSURE_ALERT_STREAK` on
/// `Rescheduled`. The typed `site` selects the direction, rate-limited
/// operation, held aggregate state, and alert-delivery label as one coherent
/// set so callers cannot accidentally combine labels from different transfer
/// stages. `streak_before_this_attempt` is only read on `DeadLettered` -- that
/// variant carries no streak of its own, so the caller's already-known
/// `backpressure_streak` (the value that made this attempt exhaust the budget)
/// is what gets logged.
async fn log_and_alert_backpressure_outcome(
    id: &UsdcRebalanceId,
    site: BackpressureSite,
    streak_before_this_attempt: BackpressureStreak,
    outcome: BackpressureOutcome,
    notifier: &Arc<dyn Notifier>,
) {
    let BackpressureLabels {
        direction,
        context,
        state,
        alert,
    } = site.labels();

    match outcome {
        BackpressureOutcome::DeadLettered => {
            let BackpressureStreak(streak) = streak_before_this_attempt;
            error!(
                target: "rebalance",
                %id,
                streak,
                limit = BACKPRESSURE_RESCHEDULE_LIMIT,
                "{direction} USDC transfer: {context} exceeded the reschedule \
                 budget; dead-lettering instead of opening the circuit breaker -- \
                 treat as a structurally-dead Alpaca integration needing manual \
                 reconciliation"
            );
            let message = format!(
                "USDC transfer {id}: {context} exceeded the \
                 {BACKPRESSURE_RESCHEDULE_LIMIT}-reschedule budget. Aggregate stays \
                 {state} (guard held); likely a structurally-dead Alpaca \
                 integration (suspended account, revoked key) needing manual \
                 reconciliation."
            );
            if let Err(error) = notifier.notify(&message).await {
                warn!(
                    target: "rebalance", ?error,
                    "Failed to deliver USDC {alert} backpressure dead-letter alert"
                );
            }
        }
        BackpressureOutcome::Rescheduled {
            next_streak: BackpressureStreak(streak),
            visible,
        } => {
            if visible {
                error!(
                    target: "rebalance",
                    %id,
                    streak,
                    "{direction} USDC transfer: {context} still rescheduling after \
                     sustained broker rate-limiting"
                );
            }
            if streak == BACKPRESSURE_ALERT_STREAK && site.should_page_at_streak() {
                let message = format!(
                    "USDC transfer {id}: {context} has persisted for {streak} consecutive \
                     reschedules. Aggregate stays {state} (guard held); investigate \
                     Alpaca connectivity/rate limits before the \
                     {BACKPRESSURE_RESCHEDULE_LIMIT}-attempt budget is exhausted."
                );
                if let Err(error) = notifier.notify(&message).await {
                    warn!(
                        target: "rebalance", ?error,
                        "Failed to deliver USDC {alert} sustained-backpressure alert"
                    );
                }
            }
        }
    }
}

async fn alert_withdrawal_poll_deadline_elapsed(
    id: &UsdcRebalanceId,
    elapsed: Duration,
    source: &AlpacaWalletError,
    notifier: &Arc<dyn Notifier>,
) {
    let message = format!(
        "Alpaca->Base USDC transfer {id}: withdrawal polling inconclusive \
         for {elapsed:?} (>{WITHDRAWAL_POLL_ALERT_DEADLINE:?}). Alpaca may \
         be unreachable or credentials may have changed ({source}). Aggregate stays in \
         Withdrawing (guard held). Use `stox transfer resume --kind usdc --id \
         {id} --direction to-raindex` to manually re-poll, or investigate \
         Alpaca connectivity."
    );
    if let Err(notify_err) = notifier.notify(&message).await {
        warn!(
            target: "rebalance",
            ?notify_err,
            "Failed to deliver withdrawal-poll-deadline-elapsed alert"
        );
    }
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

impl BotGasFailureClassifier for TransferUsdcToHedgingJobError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::Transfer(inner) => inner.is_bot_gas_enqueue_failure(),
            Self::BurnRevertLimitReached { .. }
            | Self::TimeoutLimitReached { .. }
            | Self::Enqueue(_) => false,
        }
    }
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
    /// Count of consecutive broker rate-limit (429) reschedules leading up to
    /// this attempt (RAI-1494). `#[serde(default)]` so a row enqueued under
    /// the pre-this-change payload shape still deserializes to `0` instead of
    /// crashing the poll stream's `sqlx::Decode`. Independent of
    /// `revert_redrive_attempts`, which covers a different failure class.
    #[serde(default)]
    pub(crate) backpressure_streak: BackpressureStreak,
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

        let result = match intercept_bot_gas_enqueue_failure(self, &ctx.job_queue, result).await {
            ControlFlow::Break(outcome) => return outcome,
            ControlFlow::Continue(result) => result,
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
            //
            // A post-burn CCTP mint recovery whose outcome could not be
            // determined is a DIFFERENT, dedicated variant --
            // `UsdcTransferError::MintRecoveryInconclusive` (see the arm below)
            // -- not wrapped in this one, so it gets its own deadline-based
            // operator alert instead of redriving here silently forever.
            Err(UsdcTransferError::SettlementCheckTransient { id, source }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    delay = ?SETTLEMENT_REDRIVE_DELAY,
                    ?source,
                    "Rescheduling Base->Alpaca USDC transfer: settlement-phase RPC check \
                     failed transiently or burn scan inconclusive"
                );
                let mut job_queue = ctx.job_queue.clone();
                job_queue
                    .push_with_delay(self.clone(), SETTLEMENT_REDRIVE_DELAY)
                    .await?;
            }
            // Post-burn CCTP mint recovery inconclusive: the nonce state is
            // unknown or the mint receipt could not be reconstructed (see
            // `CrossVenueCashTransfer::redrive_on_mint_recovery_inconclusive` in
            // manager.rs). The aggregate stays in whatever durable pre-mint
            // state it was already in, so this redrives unbounded and
            // budget-free BEFORE the deadline; at or after
            // `MINT_RECOVERY_ALERT_DEADLINE` the operator is paged on every
            // redrive while the guard stays held and redriving continues (at a
            // slower cadence, to avoid alert fatigue).
            Err(UsdcTransferError::MintRecoveryInconclusive {
                id,
                initiated_at,
                source,
            }) => {
                self.handle_mint_recovery_inconclusive(ctx, id, initiated_at, source)
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
            // The post-deposit conversion's fate is unknown and the order may
            // still fill, so retrying would race a live order and recording a
            // failure would terminalize the rebalance against one. Latched for
            // the operator, matching the Alpaca->Base leg.
            Err(UsdcTransferError::ConversionOutcomeUnresolved { id, source }) => {
                error!(
                    target: "rebalance",
                    %id, %source,
                    "Base->Alpaca USDC transfer: conversion outcome unresolved; the broker \
                     order may still be live. Latched for operator reconciliation \
                     (no auto-retry, no failure recorded)"
                );
                let message = format!(
                    "USDC transfer {id}: post-deposit conversion outcome unresolved ({source}). \
                     The broker order may still be live and may still fill; verify at Alpaca \
                     before forcing this rebalance either way."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging unresolved-conversion alert");
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
            Err(error) => return self.handle_terminal_or_backpressure_error(ctx, error).await,
        }

        Ok(())
    }
}

impl TransferUsdcToHedging {
    /// Handles the generic (unclassified-by-name) terminal error arm:
    /// reschedules with a classified delay on broker rate-limiting (429)
    /// instead of consuming the terminal retry budget and alerting, or falls
    /// through to the pre-existing terminal alert+propagate path for any
    /// other error. `TransferUsdcToHedging` is a "true retry" job (RAI-1494
    /// plan): `resume_base_to_alpaca` re-drives from the top on every attempt
    /// with no committed guard specific to this failure class, so every
    /// reschedule genuinely re-attempts the resume. Exception: the USDC->USD
    /// conversion placement sub-step never reaches this arm on a 429 -- it
    /// fails fast unconditionally (see `execute_usdc_to_usd_conversion`) and
    /// returns `ConversionPlacementFailed`, which `find_backpressure` never
    /// classifies, so it falls through to the plain terminal path below.
    async fn handle_terminal_or_backpressure_error(
        &self,
        ctx: &TransferUsdcToHedgingCtx,
        error: UsdcTransferError,
    ) -> Result<(), TransferUsdcToHedgingJobError> {
        if let Some(backpressure) = find_backpressure(&error) {
            let step = advance_backpressure(&backpressure, self.backpressure_streak);
            let mut job_queue = ctx.job_queue.clone();
            let outcome = apply_backpressure_step(step, &mut job_queue, |next_streak| Self {
                id: self.id.clone(),
                amount: self.amount,
                revert_redrive_attempts: self.revert_redrive_attempts,
                backpressure_streak: next_streak,
            })
            .await?;

            // Per the RAI-1494 plan's binding M2 decision: this is a
            // supervised worker, so dead-letter instead of propagating `Err`
            // into the shared supervised on-event path. RAI-1494 pass 3:
            // both dead-lettering and sustained rescheduling must page the
            // operator -- rerouting a 429 through this reschedule machinery
            // must not silently drop the alerting the pre-existing terminal
            // path gave every sustained failure.
            log_and_alert_backpressure_outcome(
                &self.id,
                BackpressureSite::Hedging,
                self.backpressure_streak,
                outcome,
                &ctx.notifier,
            )
            .await;

            return Ok(());
        }

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
            "USDC transfer {id} failed: {error}. Check if apalis will retry before acting."
        );
        if let Err(error) = ctx.notifier.notify(&message).await {
            warn!(target: "rebalance", ?error, "Failed to deliver USDC hedging terminal-error alert");
        }
        Err(error.into())
    }

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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
            ..self.clone()
        };
        ctx.job_queue
            .clone()
            .push_with_delay(updated, BURN_REVERT_REDRIVE_DELAY)
            .await?;
        Ok(())
    }

    /// Handles a post-burn CCTP mint recovery that stayed inconclusive: reschedule
    /// unbounded and budget-free, escalating to an operator alert once
    /// `initiated_at` is past `MINT_RECOVERY_ALERT_DEADLINE`. Symmetric to
    /// [`TransferUsdcToMarketMaking::handle_mint_recovery_inconclusive`].
    async fn handle_mint_recovery_inconclusive(
        &self,
        ctx: &TransferUsdcToHedgingCtx,
        id: UsdcRebalanceId,
        initiated_at: DateTime<Utc>,
        source: Box<CctpError>,
    ) -> Result<(), TransferUsdcToHedgingJobError> {
        schedule_mint_recovery_redrive(
            &ctx.notifier,
            &mut ctx.job_queue.clone(),
            self.clone(),
            MintRecoveryDirection::BaseToAlpaca,
            id,
            initiated_at,
            source,
        )
        .await?;
        Ok(())
    }
}

/// Dependencies the Alpaca->Base job needs. Symmetric to
/// [`TransferUsdcToHedgingCtx`].
pub(crate) struct TransferUsdcToMarketMakingCtx {
    pub(crate) transfer: Arc<dyn ResumeAlpacaToBase>,
    pub(crate) job_queue: TransferUsdcToMarketMakingJobQueue,
    /// Maximum consecutive revert-class burn failures before circuit opens.
    pub(crate) max_burn_revert_redrives: u32,
    /// Alerting channel. `NoopNotifier` when `[alerts]` is unconfigured;
    /// `TelegramNotifier` otherwise. Never `None` — absence is explicit via
    /// `NoopNotifier` rather than a silent skip.
    pub(crate) notifier: Arc<dyn Notifier>,
    /// Release-only handle for the trigger's single-rebalance guard, for the
    /// pre-flight refusals that emit no aggregate event (see
    /// [`UsdcGuardRelease`]). Every other outcome keeps the guard
    /// deliberately or clears it event-driven via the reactor.
    pub(crate) usdc_guard: Arc<dyn UsdcGuardRelease>,
    /// Cross-attempt pacing for the pre-flight alerts (see
    /// [`PreflightAlertGate`]): shared by every attempt through this ctx so
    /// refusals that repeat on every rebalancing check do not page once per
    /// check.
    pub(crate) preflight_alerts: Arc<PreflightAlertGate>,
}

/// A single balance-read blip is warn-only, but a sustained RPC outage halts
/// Alpaca->Base rebalancing silently; page on every N-th consecutive
/// pre-flight balance-read failure so the outage surfaces at a bounded rate.
const PREFLIGHT_UNAVAILABLE_ALERT_STREAK: u32 = 5;

/// Alert pacing for the pre-flight refusals. Both pre-flight outcomes repeat
/// on every rebalancing check (one check per fill and per snapshot) until an
/// operator acts, because the guard release lets each check re-arm and
/// refuse again -- unlike the settlement-time ambient failure, whose
/// aggregate holds the guard and therefore alerts exactly once. The ambient
/// refusal re-pages only when the observed balance changes; the balance-read
/// failure pages on every [`PREFLIGHT_UNAVAILABLE_ALERT_STREAK`]-th
/// consecutive failure. Any non-pre-flight outcome resets both, so the next
/// incident pages afresh.
#[derive(Default)]
pub(crate) struct PreflightAlertGate {
    last_paged_ambient: tokio::sync::Mutex<Option<Usdc>>,
    unavailable_streak: AtomicU32,
}

impl PreflightAlertGate {
    /// Whether this ambient refusal should page: the first one, or one whose
    /// balance differs from the last paged balance (the wallet was swept and
    /// re-dusted, or received more funds).
    async fn should_page_ambient(&self, balance: Usdc) -> bool {
        let mut last = self.last_paged_ambient.lock().await;
        if *last == Some(balance) {
            return false;
        }
        *last = Some(balance);
        true
    }

    /// Counts a consecutive balance-read failure; true on every
    /// [`PREFLIGHT_UNAVAILABLE_ALERT_STREAK`]-th so a sustained outage pages
    /// at a bounded rate.
    fn count_unavailable(&self) -> bool {
        let streak = self.unavailable_streak.fetch_add(1, Ordering::SeqCst) + 1;
        streak.is_multiple_of(PREFLIGHT_UNAVAILABLE_ALERT_STREAK)
    }

    /// Clears both gates. Called on any non-pre-flight outcome: the
    /// pre-flight passed, so the next refusal is a new incident.
    async fn reset(&self) {
        *self.last_paged_ambient.lock().await = None;
        self.unavailable_streak.store(0, Ordering::SeqCst);
    }
}

/// Settles the ambient pre-flight refusal (`WalletUsdcAmbientPreflight`):
/// warn on every attempt, page through the balance-keyed gate (the refusal
/// repeats on every rebalancing check until the wallet is swept), and
/// release the guard -- no aggregate exists to clear it event-driven.
async fn settle_preflight_ambient(
    ctx: &TransferUsdcToMarketMakingCtx,
    id: &UsdcRebalanceId,
    balance: Usdc,
    nominal: Usdc,
) {
    warn!(
        target: "rebalance",
        %id,
        %balance,
        %nominal,
        "Alpaca->Base USDC transfer refused pre-flight: ambient USDC in \
         market-maker wallet; no Alpaca call was made and no aggregate exists"
    );
    if ctx.preflight_alerts.should_page_ambient(balance).await {
        let message = format!(
            "USDC transfer {id} refused before start: market-maker wallet already \
             holds {balance} USDC (nominal {nominal}). No cash left Alpaca. \
             Sweep the wallet to unblock USDC rebalancing."
        );
        if let Err(error) = ctx.notifier.notify(&message).await {
            warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making pre-flight ambient alert");
        }
    }
    ctx.usdc_guard.release_unless_durably_held().await;
}

/// Settles the ambient sibling for a balance too large to represent
/// (`WalletUsdcAmbientPreflightUnrepresentable`): the wallet provably holds
/// USDC, so this pages and releases like the ambient refusal. Not deduped --
/// the case is near-impossible, and when it fires the loudest response is
/// the right one.
async fn settle_preflight_unrepresentable(
    ctx: &TransferUsdcToMarketMakingCtx,
    id: &UsdcRebalanceId,
    raw: U256,
    error: &UsdcTransferError,
) {
    error!(
        target: "rebalance",
        %id,
        %raw,
        "Alpaca->Base USDC transfer refused pre-flight: ambient USDC in \
         market-maker wallet with an unrepresentable balance; no Alpaca \
         call was made and no aggregate exists"
    );
    let message = format!("{error}");
    if let Err(error) = ctx.notifier.notify(&message).await {
        warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making pre-flight ambient alert");
    }
    ctx.usdc_guard.release_unless_durably_held().await;
}

/// Settles the pre-flight balance-read failure
/// (`PreflightBalanceUnavailable`): warn-only for a transient blip, but a
/// sustained outage halts Alpaca->Base rebalancing, so every
/// [`PREFLIGHT_UNAVAILABLE_ALERT_STREAK`]-th consecutive failure pages.
/// Releases the guard; the trigger's next cycle is the retry.
async fn settle_preflight_unavailable(
    ctx: &TransferUsdcToMarketMakingCtx,
    id: &UsdcRebalanceId,
    source: &UsdcTransferError,
) {
    warn!(
        target: "rebalance",
        %id,
        ?source,
        "Alpaca->Base USDC transfer refused pre-flight: wallet balance \
         could not be determined; the trigger retries on its next cycle"
    );
    if ctx.preflight_alerts.count_unavailable() {
        let message = format!(
            "USDC transfer pre-flight balance read has failed \
             {PREFLIGHT_UNAVAILABLE_ALERT_STREAK} consecutive times (latest \
             transfer {id}: {source}). Alpaca->Base USDC rebalancing is \
             halted until the RPC recovers."
        );
        if let Err(error) = ctx.notifier.notify(&message).await {
            warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making pre-flight outage alert");
        }
    }
    ctx.usdc_guard.release_unless_durably_held().await;
}

/// Release-only handle for the trigger's single-rebalance guard, given to the
/// worker for the outcomes that cannot clear it event-driven: pre-flight
/// refusals, which emit no aggregate event, so no terminal event will ever
/// clear the guard for them. Deliberately NOT the raw atomic: the guard is
/// process-global and startup recovery re-latches it for OTHER aggregates
/// (e.g. a post-burn failure awaiting manual reconciliation), so a blind
/// release could drop a latch that still protects funds. The durable check
/// covers exactly the persisted holders; a claim armed for a transfer that
/// has not persisted its first event yet is invisible to it. That window is
/// closed one layer up: every enqueue passes the trigger's
/// `in_flight_usdc_transfer` gate, which refuses to arm a new transfer while
/// any USDC transfer job row is still live, so a stale job's release cannot
/// admit a second concurrent transfer. The release-only trait also keeps any
/// future arm from claiming or blindly flipping the guard.
#[async_trait]
pub(crate) trait UsdcGuardRelease: Send + Sync + 'static {
    async fn release_unless_durably_held(&self);
}

/// Production [`UsdcGuardRelease`]: clears the guard only when no persisted
/// rebalance still holds it, keeping the latch on any doubt (fail closed,
/// mirroring startup guard recovery).
pub(crate) struct DurableCheckedGuardRelease {
    pub(crate) pool: SqlitePool,
    pub(crate) store: Arc<Store<UsdcRebalance>>,
    pub(crate) usdc_in_progress: Arc<AtomicBool>,
}

#[async_trait]
impl UsdcGuardRelease for DurableCheckedGuardRelease {
    async fn release_unless_durably_held(&self) {
        match any_rebalance_holds_guard(&self.pool, &self.store).await {
            Ok(false) => self.usdc_in_progress.store(false, Ordering::SeqCst),
            Ok(true) => warn!(
                target: "rebalance",
                "Guard stays latched after a pre-flight refusal: another \
                 persisted USDC rebalance still holds it"
            ),
            Err(error) => warn!(
                target: "rebalance",
                ?error,
                "Could not verify durable guard holders after a pre-flight \
                 refusal; keeping the guard latched (fail closed)"
            ),
        }
    }
}

/// Errors emitted by [`TransferUsdcToMarketMaking::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferUsdcToMarketMakingJobError {
    #[error(transparent)]
    Transfer(#[from] UsdcTransferError),
    #[error(
        "Alpaca->Base transfer {id} burn revert redrive limit reached; \
         aggregate stalled at BridgingSubmitting, operator action required"
    )]
    BurnRevertLimitReached { id: UsdcRebalanceId },
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
}

impl BotGasFailureClassifier for TransferUsdcToMarketMakingJobError {
    fn is_bot_gas_enqueue_failure(&self) -> bool {
        match self {
            Self::Transfer(inner) => inner.is_bot_gas_enqueue_failure(),
            Self::BurnRevertLimitReached { .. } | Self::Enqueue(_) => false,
        }
    }
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
    /// Count of consecutive broker rate-limit (429) reschedules leading up to
    /// this attempt (RAI-1494). `#[serde(default)]` so a row enqueued under
    /// the pre-this-change payload shape still deserializes to `0` instead of
    /// crashing the poll stream's `sqlx::Decode`. Independent of
    /// `revert_redrive_attempts`, which covers a different failure class.
    #[serde(default)]
    pub(crate) backpressure_streak: BackpressureStreak,
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

        let result = match intercept_bot_gas_enqueue_failure(self, &ctx.job_queue, result).await {
            ControlFlow::Break(outcome) => return outcome,
            ControlFlow::Continue(result) => result,
        };

        self.settle_transfer_outcome(ctx, result).await
    }
}

impl TransferUsdcToMarketMaking {
    /// Routes the transfer outcome to its recovery or terminal handling: the
    /// redrive waits (attestation, settlement), the pre-flight guard
    /// releases, the fail-closed burn-safety latches, backpressure, and the
    /// terminal failures. One arm per error contract. The match ends in a
    /// catch-all that routes to the terminal/backpressure handler, which
    /// latches the guard -- so any NEW pre-aggregate variant MUST get an
    /// explicit guard-releasing arm here, or its guard is latched with no
    /// terminal event to ever clear it. The compiler cannot flag that; the
    /// error type's docs mark the pre-aggregate variants.
    async fn settle_transfer_outcome(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
        result: Result<(), UsdcTransferError>,
    ) -> Result<(), TransferUsdcToMarketMakingJobError> {
        // Any non-pre-flight outcome proves the pre-flight passed, so the
        // alert gates reset and the next refusal pages as a fresh incident.
        // A wildcard is safe here: a future variant wrongly resetting the
        // gate can only cause an extra page, never a missed one.
        match &result {
            Err(
                UsdcTransferError::WalletUsdcAmbientPreflight { .. }
                | UsdcTransferError::WalletUsdcAmbientPreflightUnrepresentable { .. }
                | UsdcTransferError::PreflightBalanceUnavailable { .. },
            ) => {}
            _ => ctx.preflight_alerts.reset().await,
        }

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
                ref settlement_err @ UsdcTransferError::WithdrawalTxUnderconfirmed { ref id, .. },
            ) => {
                self.handle_settlement_wait_redrive(
                    ctx,
                    settlement_err,
                    id,
                    "withdrawal tx not yet sufficiently confirmed",
                )
                .await?;
            }
            Err(ref settlement_err @ UsdcTransferError::WalletUsdcInsufficient { ref id, .. }) => {
                self.handle_settlement_wait_redrive(
                    ctx,
                    settlement_err,
                    id,
                    "market-maker wallet has insufficient USDC (withdrawal not yet settled)",
                )
                .await?;
            }
            Err(
                ref settlement_err @ UsdcTransferError::SettlementCheckTransient { ref id, .. },
            ) => {
                self.handle_settlement_wait_redrive(
                    ctx,
                    settlement_err,
                    id,
                    "settlement-phase RPC check failed transiently",
                )
                .await?;
            }
            // Post-burn CCTP mint recovery inconclusive: the nonce state is
            // unknown or the mint receipt could not be reconstructed (see
            // `CrossVenueCashTransfer::redrive_on_mint_recovery_inconclusive` in
            // manager.rs). The aggregate stays in whatever durable pre-mint
            // state it was already in, so this redrives unbounded and
            // budget-free BEFORE the deadline; at or after
            // `MINT_RECOVERY_ALERT_DEADLINE` the operator is paged on every
            // redrive while the guard stays held and redriving continues (at a
            // slower cadence, to avoid alert fatigue).
            Err(UsdcTransferError::MintRecoveryInconclusive {
                id,
                initiated_at,
                source,
            }) => {
                self.handle_mint_recovery_inconclusive(ctx, id, initiated_at, source)
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
            // Pre-flight refusals (see the variants' docs): no aggregate
            // exists, so each settles worker-side -- log, page through its
            // alert gate, and release the guard -- and never redrives; the
            // trigger re-attempts on its own schedule.
            Err(UsdcTransferError::WalletUsdcAmbientPreflight {
                id,
                balance,
                nominal,
            }) => {
                settle_preflight_ambient(ctx, &id, balance, nominal).await;
            }
            Err(
                ref error @ UsdcTransferError::WalletUsdcAmbientPreflightUnrepresentable {
                    ref id,
                    raw,
                    ..
                },
            ) => {
                settle_preflight_unrepresentable(ctx, id, raw, error).await;
            }
            Err(UsdcTransferError::PreflightBalanceUnavailable { id, source }) => {
                settle_preflight_unavailable(ctx, &id, &source).await;
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
            // A classified broker rate-limit (429) on the withdrawal poll must
            // route through the same bounded backpressure machinery as every
            // other call site (RAI-1494), not the old unbounded inconclusive
            // redrive: without this, a sustained 429 here would never
            // increment `backpressure_streak`, honour `Retry-After`, or ever
            // dead-letter at `BACKPRESSURE_RESCHEDULE_LIMIT`. Any other
            // inconclusive poll error (timeout, network, non-429 API error)
            // keeps the pre-existing unbounded redrive via
            // `handle_withdrawal_poll_inconclusive`.
            Err(UsdcTransferError::WithdrawalPollInconclusive {
                id,
                initiated_at,
                source,
            }) => match source.backpressure() {
                None => {
                    self.handle_withdrawal_poll_inconclusive(ctx, id, initiated_at, source)
                        .await?;
                }
                Some(backpressure) => {
                    self.handle_withdrawal_poll_backpressure(
                        ctx,
                        id,
                        initiated_at,
                        source,
                        backpressure,
                    )
                    .await?;
                }
            },
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
            // The variants' Display strings carry the id, burn tx, and
            // situation, so one arm serves all four without restating them.
            Err(
                error @ (UsdcTransferError::BurnTxDropped { .. }
                | UsdcTransferError::BurnRecordFailed { .. }
                | UsdcTransferError::BurnSubmitInconclusive { .. }
                | UsdcTransferError::BurnRecordTaskFailed { .. }),
            ) => {
                error!(
                    target: "rebalance",
                    id = %self.id,
                    %error,
                    "Alpaca->Base USDC transfer: latched at BridgingSubmitting \
                     for operator reconciliation (no auto-reburn)"
                );
                let message = format!(
                    "{error}. Latched for operator reconciliation; verify \
                     on-chain before any reburn."
                );
                if let Err(error) = ctx.notifier.notify(&message).await {
                    warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making burn-safety alert");
                }
            }
            // Both outcomes are deterministic across retries and neither has a
            // safe automatic next step, so they latch for an operator instead
            // of burning the retry budget on an identical failure. Returning
            // `Ok` is what stops apalis re-entering the resume, which for an
            // unresolved conversion would reach the `Converting` arm and emit
            // `FailConversion` -- releasing the in-flight guard while the
            // broker order may still be live and still fill.
            Err(
                error @ (UsdcTransferError::ConversionBelowWithdrawalMinimum { .. }
                | UsdcTransferError::ConversionOutcomeUnresolved { .. }),
            ) => self.latch_conversion_for_reconciliation(ctx, &error).await,
            Err(error) => return self.handle_terminal_or_backpressure_error(ctx, error).await,
        }

        Ok(())
    }

    /// Ends the attempt without a retry for the two conversion outcomes that
    /// are deterministic across retries and have no safe automatic next step,
    /// alerting the operator instead.
    ///
    /// The caller returns `Ok` after this, which is the whole mechanism: an
    /// unresolved conversion re-entered by apalis would reach the `Converting`
    /// resume arm and emit `FailConversion`, releasing the in-flight guard
    /// while the broker order may still be live and still fill. A sub-minimum
    /// conversion would simply fail identically every attempt.
    async fn latch_conversion_for_reconciliation(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
        error: &UsdcTransferError,
    ) {
        let (context, detail) = match error {
            UsdcTransferError::ConversionBelowWithdrawalMinimum {
                converted, minimum, ..
            } => (
                "conversion settled below the broker's withdrawal minimum; the converted \
                 USDC is in the Alpaca crypto wallet and needs reconciliation",
                format!(
                    "settled at {converted}, below Alpaca's {minimum} withdrawal minimum. \
                     No withdrawal was attempted."
                ),
            ),
            _ => (
                "conversion outcome unresolved; the broker order may still be live. Latched \
                 with no failure recorded",
                format!(
                    "outcome unresolved ({error}). The order may still fill; verify at \
                     Alpaca before forcing this rebalance either way."
                ),
            ),
        };

        error!(target: "rebalance", id = %self.id, %error, "Alpaca->Base USDC transfer: {context}");

        let message = format!("USDC transfer {}: {detail}", self.id);
        if let Err(notify_error) = ctx.notifier.notify(&message).await {
            warn!(target: "rebalance", ?notify_error, "Failed to deliver USDC market-making conversion-latch alert");
        }
    }

    /// `reason` is supplied by the caller, where the concrete settlement
    /// variant is already known statically. Re-deriving it here from the
    /// widened `&UsdcTransferError` would need an `unreachable!` fallback that
    /// a future variant added to the caller's or-pattern could turn into a
    /// worker-killing panic.
    async fn handle_settlement_wait_redrive(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
        settlement_err: &UsdcTransferError,
        id: &UsdcRebalanceId,
        reason: &'static str,
    ) -> Result<(), TransferUsdcToMarketMakingJobError> {
        // `settlement_err` is logged in full (not just the generic
        // `reason` string) so an operator watching this redrive sees
        // the underlying cause.
        //
        // A post-burn CCTP mint recovery whose outcome could not be
        // determined is a DIFFERENT, dedicated variant --
        // `UsdcTransferError::MintRecoveryInconclusive` -- not wrapped in
        // this one, so it gets its own deadline-based operator alert
        // instead of redriving here silently forever.
        warn!(
            target: "rebalance",
            %id,
            delay = ?SETTLEMENT_REDRIVE_DELAY,
            ?settlement_err,
            "Rescheduling Alpaca->Base USDC transfer: {reason}"
        );
        let mut job_queue = ctx.job_queue.clone();
        job_queue
            .push_with_delay(self.clone(), SETTLEMENT_REDRIVE_DELAY)
            .await?;
        Ok(())
    }

    /// Handles the generic (unclassified-by-name) terminal error arm:
    /// reschedules with a classified delay on broker rate-limiting (429)
    /// instead of consuming the terminal retry budget and alerting, or falls
    /// through to the pre-existing terminal alert+propagate path for any
    /// other error. `TransferUsdcToMarketMaking` is a "true retry" job
    /// (RAI-1494 plan): `resume_alpaca_to_base` re-drives from the top on
    /// every attempt with no committed guard specific to this failure class,
    /// so every reschedule genuinely re-attempts the resume. Exception: the
    /// USD->USDC conversion placement sub-step never reaches this arm on a
    /// 429 -- it fails fast unconditionally (see
    /// `execute_usd_to_usdc_conversion`) and returns
    /// `ConversionPlacementFailed`, which `find_backpressure` never
    /// classifies, so it falls through to the plain terminal path below.
    async fn handle_terminal_or_backpressure_error(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
        error: UsdcTransferError,
    ) -> Result<(), TransferUsdcToMarketMakingJobError> {
        if let Some(backpressure) = find_backpressure(&error) {
            let step = advance_backpressure(&backpressure, self.backpressure_streak);
            let mut job_queue = ctx.job_queue.clone();
            let outcome = apply_backpressure_step(step, &mut job_queue, |next_streak| Self {
                id: self.id.clone(),
                amount: self.amount,
                revert_redrive_attempts: self.revert_redrive_attempts,
                backpressure_streak: next_streak,
            })
            .await?;

            // Per the RAI-1494 plan's binding M2 decision: this is a
            // supervised worker, so dead-letter instead of propagating `Err`
            // into the shared supervised on-event path. RAI-1494 pass 3:
            // both dead-lettering and sustained rescheduling must page the
            // operator -- rerouting a 429 through this reschedule machinery
            // must not silently drop the alerting the pre-existing terminal
            // path gave every sustained failure.
            log_and_alert_backpressure_outcome(
                &self.id,
                BackpressureSite::MarketMaking,
                self.backpressure_streak,
                outcome,
                &ctx.notifier,
            )
            .await;

            return Ok(());
        }

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
            "USDC transfer {id} failed: {error}. Check if apalis will retry before acting."
        );
        if let Err(error) = ctx.notifier.notify(&message).await {
            warn!(target: "rebalance", ?error, "Failed to deliver USDC market-making terminal-error alert");
        }
        Err(error.into())
    }

    /// Bounded backpressure path for a CLASSIFIED broker rate-limit (429) on the
    /// withdrawal poll. Routes through the same machinery as every other call
    /// site (RAI-1494) rather than the unbounded inconclusive redrive in
    /// [`Self::handle_withdrawal_poll_inconclusive`]: without this, a sustained
    /// 429 here would never increment `backpressure_streak`, honour
    /// `Retry-After`, or ever dead-letter at `BACKPRESSURE_RESCHEDULE_LIMIT`.
    async fn handle_withdrawal_poll_backpressure(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
        id: UsdcRebalanceId,
        initiated_at: DateTime<Utc>,
        source: AlpacaWalletError,
        backpressure: Backpressure,
    ) -> Result<(), TransferUsdcToMarketMakingJobError> {
        let elapsed = Utc::now().signed_duration_since(initiated_at).to_std().ok();
        let deadline_elapsed = deadline_elapsed(elapsed, WITHDRAWAL_POLL_ALERT_DEADLINE);
        let mut step = advance_backpressure(&backpressure, self.backpressure_streak);
        if deadline_elapsed.is_some()
            && let BackpressureStep::Reschedule { delay, .. } = &mut step
        {
            *delay = (*delay).max(WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY);
        }
        let mut job_queue = ctx.job_queue.clone();
        let outcome = apply_backpressure_step(step, &mut job_queue, |next_streak| Self {
            id: id.clone(),
            amount: self.amount,
            revert_redrive_attempts: self.revert_redrive_attempts,
            backpressure_streak: next_streak,
        })
        .await?;

        if let Some(elapsed) = deadline_elapsed {
            alert_withdrawal_poll_deadline_elapsed(&id, elapsed, &source, &ctx.notifier).await;
        }

        // Per the RAI-1494 plan's binding M2 decision: dead-letter instead of
        // propagating `Err` into the shared supervised on-event path. The
        // aggregate stays in Withdrawing (guard held); the pre-existing 4-hour
        // alert deadline path (`handle_withdrawal_poll_inconclusive`) does not
        // run on this bounded backpressure path, so this pages the operator
        // directly instead (RAI-1494 pass 3) rather than staying silent until a
        // manual restart is noticed, and pages once more when sustained
        // backpressure crosses a deadline comparable to that same 4h alert.
        log_and_alert_backpressure_outcome(
            &id,
            BackpressureSite::WithdrawalPoll {
                deadline_elapsed: deadline_elapsed.is_some(),
            },
            self.backpressure_streak,
            outcome,
            &ctx.notifier,
        )
        .await;

        Ok(())
    }

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
        let alert_deadline_elapsed = deadline_elapsed(elapsed, WITHDRAWAL_POLL_ALERT_DEADLINE);
        let redrive_delay = if alert_deadline_elapsed.is_some() {
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

        if let Some(elapsed) = alert_deadline_elapsed {
            alert_withdrawal_poll_deadline_elapsed(&id, elapsed, &source, &ctx.notifier).await;
        }

        // Reset backpressure_streak: this arm now only handles NON-backpressure
        // inconclusive polls (a classified 429 is routed through the bounded
        // backpressure machinery before reaching here), so any prior streak is
        // unrelated to this redrive cause.
        let updated = Self {
            backpressure_streak: BackpressureStreak::default(),
            ..self.clone()
        };
        ctx.job_queue
            .clone()
            .push_with_delay(updated, redrive_delay)
            .await?;
        Ok(())
    }

    /// Handles a post-burn CCTP mint recovery that stayed inconclusive: reschedule
    /// unbounded and budget-free, escalating to an operator alert once
    /// `initiated_at` is past `MINT_RECOVERY_ALERT_DEADLINE`. Symmetric to
    /// [`TransferUsdcToHedging::handle_mint_recovery_inconclusive`].
    async fn handle_mint_recovery_inconclusive(
        &self,
        ctx: &TransferUsdcToMarketMakingCtx,
        id: UsdcRebalanceId,
        initiated_at: DateTime<Utc>,
        source: Box<CctpError>,
    ) -> Result<(), TransferUsdcToMarketMakingJobError> {
        schedule_mint_recovery_redrive(
            &ctx.notifier,
            &mut ctx.job_queue.clone(),
            self.clone(),
            MintRecoveryDirection::AlpacaToBase,
            id,
            initiated_at,
            source,
        )
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
            backpressure_streak: BackpressureStreak::default(),
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
    use alloy::transports::TransportErrorKind;
    use chrono::{DateTime, Utc};
    use reqwest::StatusCode;
    use uuid::{Uuid, uuid};

    use st0x_evm::EvmError;
    use st0x_execution::{
        AlpacaBrokerApiError, AlpacaTransferId, AlpacaWalletError, DeadlineCancel,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::alerts::{CapturingNotifier, NoopNotifier};
    use crate::test_utils::setup_test_apalis_pool;

    /// Builds a `QueuePushError` without touching a pool. The classification
    /// under test depends only on the variant shape, not on the underlying
    /// sqlx failure.
    fn queue_push_error() -> QueuePushError {
        QueuePushError(apalis_core::backend::TaskSinkError::PushError(
            sqlx_apalis::Error::PoolClosed,
        ))
    }

    fn rebalance_id() -> UsdcRebalanceId {
        UsdcRebalanceId(Uuid::new_v4())
    }

    /// The classification gates whether a failure bypasses the apalis retry
    /// budget and the fail-stop circuit, so each variant is asserted directly
    /// rather than only transitively through `perform`.
    #[test]
    fn hedging_job_error_classifies_only_wrapped_bot_gas_enqueue_failures() {
        assert!(
            TransferUsdcToHedgingJobError::Transfer(UsdcTransferError::BotGasEnqueue(
                queue_push_error()
            ))
            .is_bot_gas_enqueue_failure(),
            "a wrapped BotGasEnqueue must redrive instead of consuming retry budget"
        );

        let id = rebalance_id();
        assert!(
            !TransferUsdcToHedgingJobError::Transfer(UsdcTransferError::AttestationTimedOut { id })
                .is_bot_gas_enqueue_failure(),
            "a non-bot-gas transfer error must not be classified as a bot-gas failure"
        );
        assert!(
            !TransferUsdcToHedgingJobError::BurnRevertLimitReached { id: rebalance_id() }
                .is_bot_gas_enqueue_failure(),
            "burn-revert exhaustion is operator-actionable, not best-effort bookkeeping"
        );
        assert!(
            !TransferUsdcToHedgingJobError::TimeoutLimitReached { id: rebalance_id() }
                .is_bot_gas_enqueue_failure(),
            "timeout exhaustion is operator-actionable, not best-effort bookkeeping"
        );
        assert!(
            !TransferUsdcToHedgingJobError::Enqueue(queue_push_error())
                .is_bot_gas_enqueue_failure(),
            "a failure re-enqueueing the transfer job itself is not a bot-gas failure"
        );
    }

    #[test]
    fn market_making_job_error_classifies_only_wrapped_bot_gas_enqueue_failures() {
        assert!(
            TransferUsdcToMarketMakingJobError::Transfer(UsdcTransferError::BotGasEnqueue(
                queue_push_error()
            ))
            .is_bot_gas_enqueue_failure(),
            "a wrapped BotGasEnqueue must redrive instead of consuming retry budget"
        );

        let id = rebalance_id();
        assert!(
            !TransferUsdcToMarketMakingJobError::Transfer(UsdcTransferError::AttestationTimedOut {
                id
            })
            .is_bot_gas_enqueue_failure(),
            "a non-bot-gas transfer error must not be classified as a bot-gas failure"
        );
        assert!(
            !TransferUsdcToMarketMakingJobError::BurnRevertLimitReached { id: rebalance_id() }
                .is_bot_gas_enqueue_failure(),
            "burn-revert exhaustion is operator-actionable, not best-effort bookkeeping"
        );
        assert!(
            !TransferUsdcToMarketMakingJobError::Enqueue(queue_push_error())
                .is_bot_gas_enqueue_failure(),
            "a failure re-enqueueing the transfer job itself is not a bot-gas failure"
        );
    }

    #[test]
    fn withdrawal_poll_deadline_elapsed_includes_exact_boundary() {
        let elapsed = Some(WITHDRAWAL_POLL_ALERT_DEADLINE);

        assert_eq!(
            deadline_elapsed(elapsed, WITHDRAWAL_POLL_ALERT_DEADLINE),
            Some(WITHDRAWAL_POLL_ALERT_DEADLINE),
            "the alert deadline comparison must be inclusive"
        );
    }

    #[test]
    fn mint_recovery_deadline_elapsed_includes_exact_boundary() {
        let elapsed = Some(MINT_RECOVERY_ALERT_DEADLINE);

        assert_eq!(
            deadline_elapsed(elapsed, MINT_RECOVERY_ALERT_DEADLINE),
            Some(MINT_RECOVERY_ALERT_DEADLINE),
            "the alert deadline comparison must be inclusive"
        );
    }

    #[test]
    fn mint_recovery_deadline_elapsed_none_input_stays_none() {
        assert_eq!(
            deadline_elapsed(None, MINT_RECOVERY_ALERT_DEADLINE),
            None,
            "a None elapsed (e.g. future initiated_at from clock skew) must not elapse"
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

    struct NoopGuardRelease;

    #[async_trait]
    impl UsdcGuardRelease for NoopGuardRelease {
        async fn release_unless_durably_held(&self) {}
    }

    /// Records whether the worker asked for a guard release, standing in for
    /// the durable-state-checked production impl.
    #[derive(Default)]
    struct RecordingGuardRelease {
        released: AtomicBool,
    }

    #[async_trait]
    impl UsdcGuardRelease for RecordingGuardRelease {
        async fn release_unless_durably_held(&self) {
            self.released.store(true, Ordering::SeqCst);
        }
    }

    struct AmbientPreflightAlpacaToBase;

    #[async_trait]
    impl ResumeAlpacaToBase for AmbientPreflightAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::WalletUsdcAmbientPreflight {
                id: id.clone(),
                balance: Usdc::new(float!(50)),
                nominal: amount,
            })
        }
    }

    /// Ambient pre-flight refusal with a caller-chosen balance, for the
    /// alert-dedup tests that need the observed balance to change.
    struct AmbientPreflightWithBalance(Usdc);

    #[async_trait]
    impl ResumeAlpacaToBase for AmbientPreflightWithBalance {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::WalletUsdcAmbientPreflight {
                id: id.clone(),
                balance: self.0,
                nominal: amount,
            })
        }
    }

    struct UnrepresentableAmbientAlpacaToBase;

    #[async_trait]
    impl ResumeAlpacaToBase for UnrepresentableAmbientAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(
                UsdcTransferError::WalletUsdcAmbientPreflightUnrepresentable {
                    id: id.clone(),
                    raw: alloy::primitives::U256::MAX,
                    source: Box::new(UsdcTransferError::Cctp(Box::new(CctpError::RpcTransport(
                        TransportErrorKind::backend_gone(),
                    )))),
                },
            )
        }
    }

    struct OkAlpacaToBase;

    #[async_trait]
    impl ResumeAlpacaToBase for OkAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Ok(())
        }
    }

    struct BalanceUnavailableAlpacaToBase;

    #[async_trait]
    impl ResumeAlpacaToBase for BalanceUnavailableAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::PreflightBalanceUnavailable {
                id: id.clone(),
                source: Box::new(UsdcTransferError::Cctp(Box::new(CctpError::RpcTransport(
                    TransportErrorKind::backend_gone(),
                )))),
            })
        }
    }

    fn wallet_429() -> UsdcTransferError {
        UsdcTransferError::AlpacaWallet(AlpacaWalletError::ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: "rate limited".to_string(),
            retry_after: Some(Duration::from_millis(1)),
        })
    }

    fn wallet_500() -> UsdcTransferError {
        UsdcTransferError::AlpacaWallet(AlpacaWalletError::ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "boom".to_string(),
            retry_after: None,
        })
    }

    struct RateLimitedBaseToAlpaca;

    #[async_trait]
    impl ResumeBaseToAlpaca for RateLimitedBaseToAlpaca {
        async fn resume_base_to_alpaca(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(wallet_429())
        }
    }

    struct RateLimitedAlpacaToBase;

    #[async_trait]
    impl ResumeAlpacaToBase for RateLimitedAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(wallet_429())
        }
    }

    struct FailingBaseToAlpaca;

    #[async_trait]
    impl ResumeBaseToAlpaca for FailingBaseToAlpaca {
        async fn resume_base_to_alpaca(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(wallet_500())
        }
    }

    struct FailingAlpacaToBase;

    #[async_trait]
    impl ResumeAlpacaToBase for FailingAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(wallet_500())
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
        /// The conversion order may still be live: a redrive would race it and
        /// could reach the resume's failure transition, releasing the in-flight
        /// guard while real money can still move.
        ConversionOutcomeUnresolved,
        /// Deterministic across retries -- the converted amount cannot grow --
        /// and the withdrawal it blocks needs an operator, not a retry.
        ConversionBelowWithdrawalMinimum,
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
                Self::ConversionOutcomeUnresolved => {
                    UsdcTransferError::ConversionOutcomeUnresolved {
                        id: id.clone(),
                        source: Box::new(AlpacaBrokerApiError::ConversionCancelNotSettled {
                            order_id: uuid!("61e7b016-9c91-4a97-b912-615c9d365c9d"),
                            cancel: DeadlineCancel::Accepted,
                            filled_quantity: None,
                        }),
                    }
                }
                Self::ConversionBelowWithdrawalMinimum => {
                    UsdcTransferError::ConversionBelowWithdrawalMinimum {
                        id: id.clone(),
                        converted: Usdc::new(float!(20)),
                        minimum: *st0x_config::ALPACA_MINIMUM_WITHDRAWAL,
                    }
                }
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

    /// Stub that returns `MintRecoveryInconclusive` for every resume call in
    /// either direction. `initiated_at` controls whether the deadline check in
    /// the job handler fires an operator alert (past deadline) or only logs a
    /// warning (before).
    struct MintRecoveryInconclusiveStub {
        initiated_at: DateTime<Utc>,
    }

    impl MintRecoveryInconclusiveStub {
        fn before_deadline() -> Self {
            Self {
                initiated_at: Utc::now(),
            }
        }

        fn after_deadline() -> Self {
            Self {
                initiated_at: Utc::now()
                    - chrono::Duration::from_std(MINT_RECOVERY_ALERT_DEADLINE).unwrap()
                    - chrono::Duration::seconds(1),
            }
        }

        fn error(&self, id: &UsdcRebalanceId) -> UsdcTransferError {
            UsdcTransferError::MintRecoveryInconclusive {
                id: id.clone(),
                initiated_at: self.initiated_at,
                source: Box::new(CctpError::ScanInconclusive { from_block: 99 }),
            }
        }
    }

    #[async_trait]
    impl ResumeAlpacaToBase for MintRecoveryInconclusiveStub {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(self.error(id))
        }
    }

    #[async_trait]
    impl ResumeBaseToAlpaca for MintRecoveryInconclusiveStub {
        async fn resume_base_to_alpaca(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(self.error(id))
        }
    }

    /// Withdrawal poll returning a CLASSIFIED broker rate-limit (429), unlike
    /// [`InconclusiveAlpacaToBase`]'s unclassifiable timeout -- pins that a
    /// 429 here routes through the bounded backpressure machinery
    /// (RAI-1494), not the old unbounded `WITHDRAWAL_POLL_REDRIVE_DELAY` path.
    struct RateLimitedWithdrawalPollAlpacaToBase {
        initiated_at: DateTime<Utc>,
    }

    impl RateLimitedWithdrawalPollAlpacaToBase {
        fn before_deadline() -> Self {
            Self {
                initiated_at: Utc::now(),
            }
        }

        fn after_deadline() -> Self {
            Self {
                initiated_at: Utc::now()
                    - chrono::Duration::from_std(WITHDRAWAL_POLL_ALERT_DEADLINE).unwrap()
                    - chrono::Duration::seconds(1),
            }
        }
    }

    #[async_trait]
    impl ResumeAlpacaToBase for RateLimitedWithdrawalPollAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            Err(UsdcTransferError::WithdrawalPollInconclusive {
                id: id.clone(),
                initiated_at: self.initiated_at,
                source: AlpacaWalletError::ApiError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    message: "rate limited".to_string(),
                    retry_after: Some(Duration::from_millis(1)),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(pool),
            max_burn_revert_redrives: 5,
            notifier: Arc::new(NoopNotifier),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        }
    }

    #[tokio::test]
    async fn market_making_job_reschedules_attestation_timeout() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(Arc::new(TimeoutAlpacaToBase), &pool);
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
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

    #[test]
    fn transfer_usdc_to_hedging_payload_without_backpressure_streak_deserializes_to_zero() {
        let payload = serde_json::json!({
            "id": UsdcRebalanceId(Uuid::new_v4()),
            "amount": Usdc::new(float!(100)),
            "revert_redrive_attempts": 0,
        });

        let job: TransferUsdcToHedging = serde_json::from_value(payload).unwrap();
        assert_eq!(job.backpressure_streak, BackpressureStreak::default());
    }

    #[tokio::test]
    async fn hedging_job_429_reschedules_with_incremented_streak() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(RateLimitedBaseToAlpaca),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        // `revert_redrive_attempts` starts nonzero and distinct from
        // `backpressure_streak` so a copy-paste swap of which counter
        // receives which value at the `handle_terminal_or_backpressure_error`
        // struct-literal construction site would fail this assertion instead
        // of passing coincidentally (both fields would otherwise start at the
        // same value, 0, and a swap would be invisible).
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
            backpressure_streak: BackpressureStreak::default(),
        };

        job.perform(&ctx).await.unwrap();

        let (payload, _run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        let rescheduled: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(rescheduled.backpressure_streak, BackpressureStreak(1));
        assert_eq!(
            rescheduled.revert_redrive_attempts, 2,
            "a backpressure reschedule must not touch the unrelated revert-redrive budget"
        );
        // RAI-1494 pass 3: a lone 429, far below BACKPRESSURE_ALERT_STREAK,
        // must not page the operator.
        assert!(
            notifier.messages().is_empty(),
            "a single 429 far below the alert streak must not fire an operator alert, got: {:?}",
            notifier.messages()
        );
    }

    #[tokio::test]
    async fn hedging_job_429_past_reschedule_limit_dead_letters_without_propagating_err() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(RateLimitedBaseToAlpaca),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(BACKPRESSURE_RESCHEDULE_LIMIT),
        };

        job.perform(&ctx).await.unwrap();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            0,
            "an exhausted backpressure streak must dead-letter, not reschedule"
        );
        // RAI-1494 pass 3: dead-lettering after exhausting the reschedule
        // budget must page the operator -- rerouting a 429 through this
        // machinery must not silently drop the alerting a terminal failure
        // used to always get.
        assert_eq!(
            notifier.messages().len(),
            1,
            "dead-lettering a sustained 429 must page the operator exactly once, got: {:?}",
            notifier.messages()
        );
    }

    /// Sustained backpressure crossing `BACKPRESSURE_ALERT_STREAK` must page
    /// the operator once, well before the full `BACKPRESSURE_RESCHEDULE_LIMIT`
    /// dead-letter -- otherwise a sustained 429 silently degrades the
    /// pre-existing paging SLA a terminal failure used to get on every
    /// attempt (RAI-1494 pass 3).
    #[tokio::test]
    async fn hedging_job_429_crossing_alert_streak_pages_operator_once() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(RateLimitedBaseToAlpaca),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(BACKPRESSURE_ALERT_STREAK - 1),
        };

        job.perform(&ctx).await.unwrap();

        let (payload, _run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        let rescheduled: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.backpressure_streak,
            BackpressureStreak(BACKPRESSURE_ALERT_STREAK)
        );
        assert_eq!(
            notifier.messages().len(),
            1,
            "crossing BACKPRESSURE_ALERT_STREAK must page the operator exactly once, got: {:?}",
            notifier.messages()
        );
    }

    #[tokio::test]
    async fn hedging_job_non_backpressure_error_still_fails_terminally() {
        let pool = setup_queue_pool().await;
        let ctx = hedging_ctx(Arc::new(FailingBaseToAlpaca), &pool);
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let error = job.perform(&ctx).await.unwrap_err();
        let TransferUsdcToHedgingJobError::Transfer(UsdcTransferError::AlpacaWallet(
            AlpacaWalletError::ApiError { status, .. },
        )) = error
        else {
            panic!("expected the non-backpressure error to propagate unchanged, got {error:?}");
        };
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn transfer_usdc_to_market_making_payload_without_backpressure_streak_deserializes_to_zero() {
        let payload = serde_json::json!({
            "id": UsdcRebalanceId(Uuid::new_v4()),
            "amount": Usdc::new(float!(100)),
            "revert_redrive_attempts": 0,
        });

        let job: TransferUsdcToMarketMaking = serde_json::from_value(payload).unwrap();
        assert_eq!(job.backpressure_streak, BackpressureStreak::default());
    }

    /// Hypothesis: a pre-flight ambient refusal requests the durable-checked
    /// guard release from the worker. The refusal emits NO aggregate event,
    /// so no terminal event will ever clear the guard event-driven; without
    /// this release the trigger stays wedged ("already in progress") until
    /// restart. The job must also alert the operator to sweep the wallet and
    /// must NOT redrive (a retry cannot remove the ambient balance; the
    /// trigger re-attempts on its own schedule).
    #[tokio::test]
    async fn market_making_job_releases_guard_and_alerts_on_preflight_ambient_refusal() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let guard_release = Arc::new(RecordingGuardRelease::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(AmbientPreflightAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: guard_release.clone(),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(1000)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        job.perform(&ctx).await.unwrap();

        assert!(
            guard_release.released.load(Ordering::SeqCst),
            "the pre-flight refusal must request the guard release: no \
             aggregate event exists to clear the guard event-driven"
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "a pre-flight refusal must NOT redrive: retrying cannot remove \
             the ambient balance"
        );
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "exactly one operator alert must fire; got: {messages:?}"
        );
        assert!(
            messages[0].contains("Sweep the wallet"),
            "the alert must tell the operator to sweep the wallet; got: {}",
            messages[0]
        );
    }

    /// The production release clears the latch when no persisted rebalance
    /// holds the guard: the pre-flight refusal wrote nothing durable, so the
    /// guard must reflect durable state alone.
    #[tokio::test]
    async fn durable_checked_release_clears_guard_when_no_holder() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = st0x_event_sorcery::test_store::<UsdcRebalance>(pool.clone(), ());
        let latch = Arc::new(AtomicBool::new(true));

        DurableCheckedGuardRelease {
            pool,
            store: Arc::new(store),
            usdc_in_progress: latch.clone(),
        }
        .release_unless_durably_held()
        .await;

        assert!(
            !latch.load(Ordering::SeqCst),
            "with no durable holder the release must clear the latch"
        );
    }

    /// The production release must NOT clear the latch while a persisted
    /// rebalance still holds the guard: a stale pre-crash job row can reach
    /// the pre-flight refusal while startup recovery has re-latched the
    /// guard for a different, unreconciled aggregate.
    #[tokio::test]
    async fn durable_checked_release_keeps_guard_for_post_burn_holder() {
        use crate::usdc_rebalance::UsdcRebalanceCommand::*;

        let pool = crate::test_utils::setup_test_db().await;
        let store = st0x_event_sorcery::test_store::<UsdcRebalance>(pool.clone(), ());
        let burn_tx = TxHash::repeat_byte(0x11);

        let id = UsdcRebalanceId(Uuid::new_v4());
        for command in [
            Initiate {
                direction: crate::usdc_rebalance::RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(400.0)),
                withdrawal: crate::usdc_rebalance::TransferRef::OnchainTx(burn_tx),
            },
            ConfirmWithdrawal {
                withdrawal_tx: None,
            },
            InitiateBridging { burn_tx },
            FailBridging {
                reason: "x".to_string(),
            },
        ] {
            store.send(&id, command).await.unwrap();
        }

        let latch = Arc::new(AtomicBool::new(true));
        DurableCheckedGuardRelease {
            pool,
            store: Arc::new(store),
            usdc_in_progress: latch.clone(),
        }
        .release_unless_durably_held()
        .await;

        assert!(
            latch.load(Ordering::SeqCst),
            "the release must keep the latch while a persisted rebalance \
             still holds the guard (fail closed)"
        );
    }

    /// Hypothesis: a pre-flight balance-read failure releases the guard and
    /// does NOT redrive or page: nothing started, no aggregate exists, and
    /// the trigger's next cycle is the retry (a transient RPC blip must not
    /// alert-spam the operator).
    #[tokio::test]
    async fn market_making_job_releases_guard_without_alert_on_preflight_balance_failure() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let guard_release = Arc::new(RecordingGuardRelease::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(BalanceUnavailableAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: guard_release.clone(),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(1000)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        job.perform(&ctx).await.unwrap();

        assert!(
            guard_release.released.load(Ordering::SeqCst),
            "a pre-flight balance failure must request the guard release: \
             nothing started and no aggregate exists"
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "a pre-flight balance failure must NOT redrive; the trigger's \
             next cycle is the retry"
        );
        assert!(
            notifier.messages().is_empty(),
            "a transient balance-read failure must not page the operator; \
             got: {:?}",
            notifier.messages()
        );
    }

    /// The ambient refusal repeats on every rebalancing check until the
    /// wallet is swept, so the page dedups on the observed balance: same
    /// balance pages once, a changed balance pages again.
    #[tokio::test]
    async fn market_making_preflight_ambient_alert_pages_once_per_balance() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let gate = Arc::new(PreflightAlertGate::default());
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(1000)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let ctx_with_balance = |balance: Usdc| TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(AmbientPreflightWithBalance(balance)),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: gate.clone(),
        };

        let dusted = ctx_with_balance(Usdc::new(float!(50)));
        job.perform(&dusted).await.unwrap();
        job.perform(&dusted).await.unwrap();
        assert_eq!(
            notifier.messages().len(),
            1,
            "a repeated refusal on the same balance must page exactly once"
        );

        let more_dust = ctx_with_balance(Usdc::new(float!(75)));
        job.perform(&more_dust).await.unwrap();
        assert_eq!(
            notifier.messages().len(),
            2,
            "a changed ambient balance is a new incident and must page again"
        );
    }

    /// A single balance-read blip stays warn-only, but a sustained outage
    /// halts Alpaca->Base rebalancing: every
    /// `PREFLIGHT_UNAVAILABLE_ALERT_STREAK`-th consecutive failure pages.
    #[tokio::test]
    async fn market_making_preflight_outage_pages_on_streak_threshold() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(BalanceUnavailableAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(1000)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        for _ in 0..4 {
            job.perform(&ctx).await.unwrap();
        }
        assert!(
            notifier.messages().is_empty(),
            "below the streak threshold the outage must stay warn-only; \
             got: {:?}",
            notifier.messages()
        );

        job.perform(&ctx).await.unwrap();
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "the fifth consecutive failure must page the operator"
        );
        assert!(
            messages[0].contains("halted until the RPC recovers"),
            "the page must state that rebalancing is halted; got: {}",
            messages[0]
        );
    }

    /// A successful pre-flight between failures proves the outage ended, so
    /// the streak resets and the next failures start counting from zero.
    #[tokio::test]
    async fn market_making_preflight_outage_streak_resets_on_success() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let gate = Arc::new(PreflightAlertGate::default());
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(1000)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let failing = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(BalanceUnavailableAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: gate.clone(),
        };
        let succeeding = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(OkAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: gate.clone(),
        };

        for _ in 0..3 {
            job.perform(&failing).await.unwrap();
        }
        job.perform(&succeeding).await.unwrap();
        for _ in 0..4 {
            job.perform(&failing).await.unwrap();
        }

        assert!(
            notifier.messages().is_empty(),
            "a success between failures must reset the streak; got: {:?}",
            notifier.messages()
        );
    }

    /// The unrepresentable-balance refusal is the ambient sibling: page the
    /// operator with the raw balance, release the guard, never redrive.
    #[tokio::test]
    async fn market_making_job_pages_and_releases_on_unrepresentable_ambient() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let guard_release = Arc::new(RecordingGuardRelease::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(UnrepresentableAmbientAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: guard_release.clone(),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(1000)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        job.perform(&ctx).await.unwrap();

        assert!(
            guard_release.released.load(Ordering::SeqCst),
            "the unrepresentable ambient refusal is pre-aggregate and must \
             release the guard"
        );
        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "the refusal must NOT redrive: retrying cannot shrink the balance"
        );
        let messages = notifier.messages();
        assert_eq!(messages.len(), 1, "the refusal must page the operator");
        assert!(
            messages[0].contains("raw balance"),
            "the page must carry the raw balance; got: {}",
            messages[0]
        );
    }

    #[tokio::test]
    async fn market_making_job_429_reschedules_with_incremented_streak() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RateLimitedAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        // See the hedging-direction sibling test: a nonzero, distinct
        // `revert_redrive_attempts` closes the swap-risk gap between the two
        // same-typed counters (RAI-1494 review finding).
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
            backpressure_streak: BackpressureStreak::default(),
        };

        job.perform(&ctx).await.unwrap();

        let (payload, _run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(rescheduled.backpressure_streak, BackpressureStreak(1));
        assert_eq!(
            rescheduled.revert_redrive_attempts, 3,
            "a backpressure reschedule must not touch the unrelated revert-redrive budget"
        );
        // RAI-1494 pass 3: a lone 429, far below BACKPRESSURE_ALERT_STREAK,
        // must not page the operator.
        assert!(
            notifier.messages().is_empty(),
            "a single 429 far below the alert streak must not fire an operator alert, got: {:?}",
            notifier.messages()
        );
    }

    #[tokio::test]
    async fn market_making_job_429_past_reschedule_limit_dead_letters_without_propagating_err() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RateLimitedAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(BACKPRESSURE_RESCHEDULE_LIMIT),
        };

        job.perform(&ctx).await.unwrap();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "an exhausted backpressure streak must dead-letter, not reschedule"
        );
        // RAI-1494 pass 3: dead-lettering after exhausting the reschedule
        // budget must page the operator -- rerouting a 429 through this
        // machinery must not silently drop the alerting a terminal failure
        // used to always get.
        assert_eq!(
            notifier.messages().len(),
            1,
            "dead-lettering a sustained 429 must page the operator exactly once, got: {:?}",
            notifier.messages()
        );
    }

    /// Sustained backpressure crossing `BACKPRESSURE_ALERT_STREAK` must page
    /// the operator once, well before the full `BACKPRESSURE_RESCHEDULE_LIMIT`
    /// dead-letter (RAI-1494 pass 3), mirroring the hedging-direction sibling.
    #[tokio::test]
    async fn market_making_job_429_crossing_alert_streak_pages_operator_once() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RateLimitedAlpacaToBase),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(BACKPRESSURE_ALERT_STREAK - 1),
        };

        job.perform(&ctx).await.unwrap();

        let (payload, _run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.backpressure_streak,
            BackpressureStreak(BACKPRESSURE_ALERT_STREAK)
        );
        assert_eq!(
            notifier.messages().len(),
            1,
            "crossing BACKPRESSURE_ALERT_STREAK must page the operator exactly once, got: {:?}",
            notifier.messages()
        );
    }

    #[tokio::test]
    async fn market_making_job_non_backpressure_error_still_fails_terminally() {
        let pool = setup_queue_pool().await;
        let ctx = market_making_ctx(Arc::new(FailingAlpacaToBase), &pool);
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let error = job.perform(&ctx).await.unwrap_err();
        let TransferUsdcToMarketMakingJobError::Transfer(UsdcTransferError::AlpacaWallet(
            AlpacaWalletError::ApiError { status, .. },
        )) = error
        else {
            panic!("expected the non-backpressure error to propagate unchanged, got {error:?}");
        };
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        // `backpressure_streak` starts nonzero: this non-429 inconclusive
        // poll error routes through `handle_withdrawal_poll_inconclusive`
        // (not the backpressure branch), which must reset the streak since
        // it is now unrelated (RAI-1494 review finding: closes the
        // swap-risk gap between the two same-typed counters).
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(4),
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
        assert_eq!(
            rescheduled.backpressure_streak,
            BackpressureStreak::default(),
            "a non-429 inconclusive poll redrive is unrelated to backpressure and must \
             reset the streak"
        );
    }

    /// A classified 429 on the withdrawal poll (unlike the plain-timeout
    /// `InconclusiveAlpacaToBase` case above) must route through the bounded
    /// backpressure machinery (RAI-1494): increment `backpressure_streak`
    /// and use `decide_backpressure`'s delay, not the old unbounded
    /// `WITHDRAWAL_POLL_REDRIVE_DELAY` redrive.
    #[tokio::test]
    async fn market_making_job_withdrawal_poll_429_reschedules_through_backpressure_machinery() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RateLimitedWithdrawalPollAlpacaToBase::before_deadline()),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        // `RateLimitedWithdrawalPollAlpacaToBase` returns `retry_after:
        // Some(Duration::from_millis(1))`, which `decide_backpressure`
        // deterministically floors to exactly `MIN_BACKPRESSURE_DELAY` (1s) --
        // captured before the call so the assertion below can pin an exact
        // window, not just a one-sided upper bound that would also pass for
        // an unintended zero-delay reschedule.
        let before_now = Utc::now().timestamp();

        job.perform(&ctx).await.unwrap();

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.backpressure_streak,
            BackpressureStreak(1),
            "a classified 429 on the withdrawal poll must route through the bounded \
             backpressure machinery (incrementing the streak), not the old unbounded \
             inconclusive redrive"
        );
        assert!(
            notifier.messages().is_empty(),
            "a single 429 below the reschedule limit must not fire an operator alert, got: {:?}",
            notifier.messages()
        );
        // `decide_backpressure` floors the delay at MIN_BACKPRESSURE_DELAY (1s),
        // far below the old unbounded WITHDRAWAL_POLL_REDRIVE_DELAY (30s) --
        // pin the exact expected window (not just "somewhere under 30s") so a
        // regression to a different delay computation would fail this test.
        assert!(
            (before_now + 1..before_now + 3).contains(&run_at),
            "a classified 429 must use the deterministic MIN_BACKPRESSURE_DELAY (1s) \
             floor, not the old {WITHDRAWAL_POLL_REDRIVE_DELAY:?} unbounded redrive \
             delay or any other value; got run_at={run_at}, before_now={before_now}"
        );
    }

    /// Once the withdrawal-poll 429 streak exhausts `BACKPRESSURE_RESCHEDULE_LIMIT`,
    /// the job must dead-letter (loud log, `Ok(())`) instead of rescheduling
    /// again or propagating `Err` -- symmetric with every other backpressure
    /// call site.
    #[tokio::test]
    async fn market_making_job_withdrawal_poll_429_past_reschedule_limit_dead_letters_without_propagating_err()
     {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RateLimitedWithdrawalPollAlpacaToBase::before_deadline()),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(BACKPRESSURE_RESCHEDULE_LIMIT),
        };

        job.perform(&ctx).await.unwrap();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            0,
            "an exhausted backpressure streak on the withdrawal poll must dead-letter, \
             not reschedule"
        );
        // RAI-1494 pass 3: dead-lettering the withdrawal-poll backpressure path
        // must page the operator directly -- the pre-existing 4h
        // `handle_withdrawal_poll_inconclusive` alert path does not run here,
        // so this arm must not silently drop paging altogether.
        assert_eq!(
            notifier.messages().len(),
            1,
            "dead-lettering a sustained withdrawal-poll 429 must page the operator \
             exactly once, got: {:?}",
            notifier.messages()
        );
    }

    /// Sustained withdrawal-poll backpressure crossing `BACKPRESSURE_ALERT_STREAK`
    /// must page the operator once, well before the full
    /// `BACKPRESSURE_RESCHEDULE_LIMIT` dead-letter (RAI-1494 pass 3) --
    /// otherwise the bounded backpressure path silently degrades the
    /// pre-existing 4h withdrawal-poll paging SLA to ~8-42h.
    #[tokio::test]
    async fn market_making_job_withdrawal_poll_429_crossing_alert_streak_pages_operator_once() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RateLimitedWithdrawalPollAlpacaToBase::before_deadline()),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(BACKPRESSURE_ALERT_STREAK - 1),
        };

        job.perform(&ctx).await.unwrap();

        let (payload, _run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.backpressure_streak,
            BackpressureStreak(BACKPRESSURE_ALERT_STREAK)
        );
        assert_eq!(
            notifier.messages().len(),
            1,
            "crossing BACKPRESSURE_ALERT_STREAK on the withdrawal poll must page the \
             operator exactly once, got: {:?}",
            notifier.messages()
        );
    }

    #[tokio::test]
    async fn market_making_job_withdrawal_poll_429_preserves_wall_clock_alert_and_cadence() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RateLimitedWithdrawalPollAlpacaToBase::after_deadline()),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(BACKPRESSURE_ALERT_STREAK - 1),
        };
        let before_now = Utc::now().timestamp();

        job.perform(&ctx).await.unwrap();

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.backpressure_streak,
            BackpressureStreak(BACKPRESSURE_ALERT_STREAK)
        );
        assert_eq!(
            notifier.messages().len(),
            1,
            "the wall-clock page must replace, not duplicate, the streak-threshold page"
        );

        let expected_delay =
            i64::try_from(WITHDRAWAL_POLL_POST_DEADLINE_REDRIVE_DELAY.as_secs()).unwrap();
        assert!(
            (before_now + expected_delay..before_now + expected_delay + 2).contains(&run_at),
            "post-deadline backpressure must preserve the 30-minute alert cadence; \
             got run_at={run_at}, before_now={before_now}"
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            deadline_elapsed(None, WITHDRAWAL_POLL_ALERT_DEADLINE),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
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

    /// `MintRecoveryInconclusive` before the alert deadline must schedule a
    /// delayed redrive (one Pending row with `MINT_RECOVERY_REDRIVE_DELAY`) and
    /// return `Ok` so the apalis retry budget is not consumed -- warn log
    /// only, no operator alert. `revert_redrive_attempts` must not be
    /// incremented (the redrive is unbounded and independent of the
    /// burn-revert budget). Market-making (Alpaca->Base) direction.
    #[tokio::test]
    async fn market_making_job_redrives_on_mint_recovery_inconclusive() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(MintRecoveryInconclusiveStub::before_deadline()),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "MintRecoveryInconclusive must enqueue exactly one delayed redrive job"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert!(
            run_at >= before + i64::try_from(MINT_RECOVERY_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at
                    <= after + i64::try_from(MINT_RECOVERY_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{MINT_RECOVERY_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
        assert!(
            notifier.messages().is_empty(),
            "MintRecoveryInconclusive before deadline must not fire an operator alert, got: {:?}",
            notifier.messages()
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, 0,
            "MintRecoveryInconclusive must not increment revert_redrive_attempts: the redrive \
             is unbounded and independent of the burn-revert budget"
        );
    }

    /// `MintRecoveryInconclusive` at or after the alert deadline must fire an
    /// operator alert via the notifier while STILL scheduling the delayed
    /// redrive (at the slower post-deadline cadence) and returning `Ok`. The
    /// guard stays held and redriving continues -- funds are already burned,
    /// so silently giving up is not an option. Market-making (Alpaca->Base)
    /// direction.
    #[tokio::test]
    async fn market_making_job_fires_alert_on_mint_recovery_deadline_elapsed() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(MintRecoveryInconclusiveStub::after_deadline()),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "deadline-elapsed must still enqueue a delayed redrive job (guard stays held, \
             redriving does not stop)"
        );
        let (_payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        assert!(
            run_at
                >= before
                    + i64::try_from(MINT_RECOVERY_POST_DEADLINE_REDRIVE_DELAY.as_secs()).unwrap()
                    - 5
                && run_at
                    <= after
                        + i64::try_from(MINT_RECOVERY_POST_DEADLINE_REDRIVE_DELAY.as_secs())
                            .unwrap()
                        + 5,
            "post-deadline redrive must use the longer {MINT_RECOVERY_POST_DEADLINE_REDRIVE_DELAY:?} \
             delay to prevent alert fatigue -- run_at={run_at} before={before} after={after}"
        );

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "MintRecoveryInconclusive past deadline must fire exactly one operator alert, \
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
        let expected_source = CctpError::ScanInconclusive { from_block: 99 }.to_string();
        assert!(
            alert.contains(&expected_source),
            "alert must contain the underlying CctpError source {expected_source:?}; \
             got: {alert:?}"
        );
        // `initiated_at` is the transfer's START, carried unchanged through every
        // earlier phase, NOT the moment mint recovery became inconclusive -- mint
        // recovery is the LAST phase, so `elapsed` can already exceed the alert
        // deadline on the very first inconclusive probe. The message must report
        // it honestly as total transfer age with mint recovery named as the
        // current stuck stage, not as "mint recovery inconclusive for {elapsed}"
        // (which would overstate how long the mint itself has been stuck).
        assert!(
            alert.contains("running for")
                && alert.contains("currently stuck at CCTP mint recovery"),
            "alert must report elapsed as total transfer age (mint recovery is the current \
             stage), not as the duration the mint itself has been inconclusive; got: {alert:?}"
        );
    }

    /// Mirrors `market_making_job_redrives_on_mint_recovery_inconclusive` for the
    /// hedging (Base->Alpaca) direction: a `MintRecoveryInconclusive` before the
    /// deadline must redrive budget-free with no alert.
    #[tokio::test]
    async fn hedging_job_redrives_on_mint_recovery_inconclusive() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(MintRecoveryInconclusiveStub::before_deadline()),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let before = Utc::now().timestamp();
        Job::perform(&job, &ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "MintRecoveryInconclusive must enqueue exactly one delayed redrive job"
        );
        assert!(
            notifier.messages().is_empty(),
            "MintRecoveryInconclusive before deadline must not fire an operator alert, got: {:?}",
            notifier.messages()
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        let rescheduled: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, 0,
            "MintRecoveryInconclusive must not increment revert_redrive_attempts"
        );
        assert!(
            run_at >= before + i64::try_from(MINT_RECOVERY_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at
                    <= after + i64::try_from(MINT_RECOVERY_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{MINT_RECOVERY_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// Mirrors `market_making_job_fires_alert_on_mint_recovery_deadline_elapsed`
    /// for the hedging (Base->Alpaca) direction.
    #[tokio::test]
    async fn hedging_job_fires_alert_on_mint_recovery_deadline_elapsed() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(MintRecoveryInconclusiveStub::after_deadline()),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let before = Utc::now().timestamp();
        Job::perform(&job, &ctx).await.unwrap();
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "deadline-elapsed must still enqueue a delayed redrive job (guard stays held, \
             redriving does not stop)"
        );
        let (_payload, run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        assert!(
            run_at
                >= before
                    + i64::try_from(MINT_RECOVERY_POST_DEADLINE_REDRIVE_DELAY.as_secs()).unwrap()
                    - 5
                && run_at
                    <= after
                        + i64::try_from(MINT_RECOVERY_POST_DEADLINE_REDRIVE_DELAY.as_secs())
                            .unwrap()
                        + 5,
            "post-deadline redrive must use the longer {MINT_RECOVERY_POST_DEADLINE_REDRIVE_DELAY:?} \
             delay -- run_at={run_at} before={before} after={after}"
        );

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "MintRecoveryInconclusive past deadline must fire exactly one operator alert, \
             got: {messages:?}"
        );
        let alert = &messages[0];
        assert!(
            alert.contains(&job.id.to_string()),
            "alert must contain the transfer id so the operator can act on it; got: {alert:?}"
        );
        assert!(
            alert.contains("to-alpaca"),
            "alert must contain the --direction flag; got: {alert:?}"
        );
        // See the matching assertion in
        // `market_making_job_fires_alert_on_mint_recovery_deadline_elapsed` for why
        // this must be phrased as total transfer age, not mint-inconclusive duration.
        assert!(
            alert.contains("running for")
                && alert.contains("currently stuck at CCTP mint recovery"),
            "alert must report elapsed as total transfer age (mint recovery is the current \
             stage), not as the duration the mint itself has been inconclusive; got: {alert:?}"
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
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

    /// An unresolved conversion outcome must latch on both legs. A redrive
    /// would re-enter the resume while the broker order may still be live,
    /// and on the Alpaca->Base leg that reaches the `Converting` arm and
    /// emits `FailConversion`, releasing the in-flight guard.
    #[tokio::test]
    async fn hedging_job_latches_on_unresolved_conversion_outcome() {
        assert_hedging_fail_closed(
            TerminalOutcome::ConversionOutcomeUnresolved,
            "ConversionOutcomeUnresolved (hedging)",
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn market_making_job_latches_on_unresolved_conversion_outcome() {
        assert_market_making_fail_closed(
            TerminalOutcome::ConversionOutcomeUnresolved,
            "ConversionOutcomeUnresolved (market-making)",
            None,
        )
        .await;
    }

    /// Every retry converts the same sub-minimum amount and is refused
    /// identically, so retrying only burns the budget an operator alert is
    /// worth more than.
    #[tokio::test]
    async fn market_making_job_latches_on_conversion_below_withdrawal_minimum() {
        assert_market_making_fail_closed(
            TerminalOutcome::ConversionBelowWithdrawalMinimum,
            "ConversionBelowWithdrawalMinimum (market-making)",
            None,
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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

    /// Stub for `UsdcTransferError::BotGasEnqueue` on the Alpaca->Base
    /// (market-making) direction. Wraps a genuine `QueuePushError` produced
    /// by pushing to a closed pool -- a real push failure, not a synthesized
    /// enum variant -- so the test exercises the same error shape production
    /// code hits.
    struct BotGasEnqueueFailureAlpacaToBase(TransferUsdcToMarketMakingJobQueue);

    #[async_trait]
    impl ResumeAlpacaToBase for BotGasEnqueueFailureAlpacaToBase {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            let mut queue = self.0.clone();
            let error = queue
                .push(TransferUsdcToMarketMaking {
                    id: id.clone(),
                    amount,
                    revert_redrive_attempts: 0,
                    backpressure_streak: BackpressureStreak::default(),
                })
                .await
                .expect_err("push to a closed pool must fail");
            Err(UsdcTransferError::BotGasEnqueue(error))
        }
    }

    /// Acceptance criterion (ADR 0017 SS4): a bot-gas receipt cost enqueue
    /// failure must delayed-redrive like `SettlementCheckTransient`, not fall
    /// into the generic terminal arm -- otherwise a bookkeeping write can
    /// consume the apalis retry budget and open this supervised worker's
    /// fail-stop circuit.
    #[tokio::test]
    async fn market_making_job_reschedules_bot_gas_enqueue_failure() {
        let pool = setup_queue_pool().await;
        let closed_pool = setup_queue_pool().await;
        closed_pool.close().await;
        let closed_queue = TransferUsdcToMarketMakingJobQueue::new(&closed_pool);
        let ctx = market_making_ctx(
            Arc::new(BotGasEnqueueFailureAlpacaToBase(closed_queue)),
            &pool,
        );
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx)
            .await
            .expect("a bot-gas enqueue failure must not fail the job terminally");
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToMarketMaking>(&pool).await,
            1,
            "a bot-gas enqueue failure must re-enqueue a delayed replacement job"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToMarketMaking>(&pool).await;
        let rescheduled: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, job.revert_redrive_attempts,
            "a bot-gas enqueue failure must not consume the revert-redrive budget"
        );
        assert!(
            run_at >= before + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at <= after + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{SETTLEMENT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// Stub for `UsdcTransferError::BotGasEnqueue` on the Base->Alpaca
    /// (hedging) direction. Same real-`QueuePushError` approach as
    /// `BotGasEnqueueFailureAlpacaToBase`.
    struct BotGasEnqueueFailureBaseToAlpaca(TransferUsdcToHedgingJobQueue);

    #[async_trait]
    impl ResumeBaseToAlpaca for BotGasEnqueueFailureBaseToAlpaca {
        async fn resume_base_to_alpaca(
            &self,
            id: &UsdcRebalanceId,
            amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            let mut queue = self.0.clone();
            let error = queue
                .push(TransferUsdcToHedging {
                    id: id.clone(),
                    amount,
                    revert_redrive_attempts: 0,
                    backpressure_streak: BackpressureStreak::default(),
                })
                .await
                .expect_err("push to a closed pool must fail");
            Err(UsdcTransferError::BotGasEnqueue(error))
        }
    }

    /// Acceptance criterion (ADR 0017 SS4), hedging direction: a bot-gas
    /// receipt cost enqueue failure must delayed-redrive without consuming
    /// the apalis retry budget or firing a terminal alert.
    #[tokio::test]
    async fn hedging_job_reschedules_bot_gas_enqueue_failure() {
        let pool = setup_queue_pool().await;
        let closed_pool = setup_queue_pool().await;
        closed_pool.close().await;
        let closed_queue = TransferUsdcToHedgingJobQueue::new(&closed_pool);
        let notifier = Arc::new(CapturingNotifier::default());
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(BotGasEnqueueFailureBaseToAlpaca(closed_queue)),
            timeout: Duration::from_secs(3600),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
        };

        let before = Utc::now().timestamp();
        Job::perform(&job, &ctx)
            .await
            .expect("a bot-gas enqueue failure must not fail the job terminally");
        let after = Utc::now().timestamp();

        assert_eq!(
            pending_job_count::<TransferUsdcToHedging>(&pool).await,
            1,
            "a bot-gas enqueue failure must re-enqueue a delayed replacement job"
        );
        assert_eq!(
            notifier.messages().len(),
            0,
            "a bot-gas enqueue failure is a best-effort accounting write and must not fire a \
             terminal alert (which would page the operator and open the circuit)"
        );

        let (payload, run_at) = pending_job_row::<TransferUsdcToHedging>(&pool).await;
        let rescheduled: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.id, job.id,
            "the rescheduled job must resume the same aggregate id"
        );
        assert_eq!(
            rescheduled.revert_redrive_attempts, job.revert_redrive_attempts,
            "a bot-gas enqueue failure must not consume the revert-redrive budget"
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
                body: "injected test failure".to_string(),
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
            backpressure_streak: BackpressureStreak::default(),
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
        // `backpressure_streak` starts nonzero (a prior 429 streak that this
        // unrelated burn-revert redrive must clear) so a copy-paste swap of
        // which counter gets reset vs incremented at this struct-literal
        // construction site would fail the assertion below instead of
        // passing coincidentally (RAI-1494 review finding).
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(4),
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
        assert_eq!(
            rescheduled.backpressure_streak,
            BackpressureStreak::default(),
            "a burn-revert redrive is unrelated to backpressure and must reset the streak"
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
        // See the hedging-direction sibling test: a nonzero starting
        // `backpressure_streak` closes the swap-risk gap between the two
        // same-typed counters (RAI-1494 review finding).
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak(4),
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
        assert_eq!(
            rescheduled.backpressure_streak,
            BackpressureStreak::default(),
            "a burn-revert redrive is unrelated to backpressure and must reset the streak"
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: Arc::new(NoopNotifier),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        // attempts=2 -> next=3 == 5/2+1: exactly at threshold
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        // attempts=2 -> next=3 == max=3: last allowed redrive, alert fires
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 2,
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 3,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        // attempts=3 -> next=4 > max=3: over-limit, Err, no alert
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 3,
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
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
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
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
    /// because it leaves the aggregate in an operator-reconciliation-bound
    /// state. The failure already emitted `FailBridging`, so the reactor
    /// clears the guard event-driven -- the worker must NOT release it, or a
    /// second rebalance could start on top of the unreconciled one.
    #[tokio::test]
    async fn market_making_job_fires_alert_on_ambient_balance() {
        let pool = setup_queue_pool().await;
        let notifier = Arc::new(CapturingNotifier::default());
        let guard_release = Arc::new(RecordingGuardRelease::default());
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(TerminalAlpacaToBase(TerminalOutcome::AmbientBalance)),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
            max_burn_revert_redrives: 5,
            notifier: notifier.clone(),
            usdc_guard: guard_release.clone(),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
            revert_redrive_attempts: 0,
            backpressure_streak: BackpressureStreak::default(),
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
        assert!(
            !guard_release.released.load(Ordering::SeqCst),
            "a post-flight ambient failure must leave the guard for the \
             event-driven reactor, never release it from the worker"
        );
    }
}
