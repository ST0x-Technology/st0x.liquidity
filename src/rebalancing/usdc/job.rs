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
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use st0x_evm::Wallet;
use st0x_finance::Usdc;

use super::UsdcTransferError;
use super::manager::CrossVenueCashTransfer;
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::usdc_rebalance::UsdcRebalanceId;

const ATTESTATION_REDRIVE_DELAY: Duration = Duration::from_secs(60);

/// Delay before re-enqueueing an Alpaca->Base job when on-chain settlement has
/// not yet completed. Two to three Ethereum block times (~30 s total) is enough
/// to give a lagging RPC node time to catch up after Alpaca marks the
/// withdrawal "Complete", while keeping the redrive cadence tight enough to
/// avoid materially delaying the transfer.
const SETTLEMENT_REDRIVE_DELAY: Duration = Duration::from_secs(30);

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
}

/// Errors emitted by [`TransferUsdcToHedging::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferUsdcToHedgingJobError {
    #[error(transparent)]
    Transfer(#[from] UsdcTransferError),
    #[error("Base->Alpaca transfer {id} attempt exceeded the {timeout:?} per-attempt timeout")]
    Timeout {
        id: UsdcRebalanceId,
        timeout: Duration,
    },
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
}

/// Apalis job payload. The `id` is generated at enqueue time so retries
/// resume the same aggregate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TransferUsdcToHedging {
    pub(crate) id: UsdcRebalanceId,
    pub(crate) amount: Usdc,
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
        let result = match tokio::time::timeout(ctx.timeout, resume).await {
            Ok(result) => result,
            Err(_elapsed) => {
                return Err(TransferUsdcToHedgingJobError::Timeout {
                    id: self.id.clone(),
                    timeout: ctx.timeout,
                });
            }
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
            }
            Err(UsdcTransferError::PreviouslyFailedAggregate { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "Base->Alpaca USDC transfer already in a terminal failed state; \
                     nothing to redrive, leaving for operator reconciliation"
                );
            }
            Err(error) => return Err(error.into()),
        }

        Ok(())
    }
}

/// Dependencies the Alpaca->Base job needs. Symmetric to
/// [`TransferUsdcToHedgingCtx`].
pub(crate) struct TransferUsdcToMarketMakingCtx {
    pub(crate) transfer: Arc<dyn ResumeAlpacaToBase>,
    pub(crate) job_queue: TransferUsdcToMarketMakingJobQueue,
}

/// Errors emitted by [`TransferUsdcToMarketMaking::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferUsdcToMarketMakingJobError {
    #[error(transparent)]
    Transfer(#[from] UsdcTransferError),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
}

/// Apalis job payload for the Alpaca->Base direction. The `id` is generated
/// at enqueue time so retries resume the same aggregate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TransferUsdcToMarketMaking {
    pub(crate) id: UsdcRebalanceId,
    pub(crate) amount: Usdc,
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
        match ctx
            .transfer
            .resume_alpaca_to_base(&self.id, self.amount)
            .await
        {
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
            }
            Err(UsdcTransferError::PreviouslyFailedAggregate { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    "Alpaca->Base USDC transfer already in a terminal failed state; \
                     nothing to redrive, leaving for operator reconciliation"
                );
            }
            Err(error) => return Err(error.into()),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, U256};
    use chrono::Utc;
    use uuid::Uuid;

    use st0x_float_macro::float;

    use super::*;
    use crate::test_utils::setup_test_apalis_pool;

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
        let job_queue = TransferUsdcToHedgingJobQueue::new(&pool);
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(TimeoutBaseToAlpaca),
            timeout: Duration::from_secs(3600),
            job_queue,
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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

    #[tokio::test]
    async fn perform_times_out_when_resume_hangs() {
        let pool = setup_queue_pool().await;
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(HangingResume),
            timeout: Duration::from_millis(50),
            job_queue: TransferUsdcToHedgingJobQueue::new(&pool),
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        assert!(matches!(
            error,
            TransferUsdcToHedgingJobError::Timeout { .. }
        ));
    }

    #[tokio::test]
    async fn market_making_job_reschedules_attestation_timeout() {
        let pool = setup_queue_pool().await;
        let job_queue = TransferUsdcToMarketMakingJobQueue::new(&pool);
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(TimeoutAlpacaToBase),
            job_queue,
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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

    #[tokio::test]
    async fn hedging_job_treats_deadline_elapsed_as_clean_terminal() {
        let pool = setup_queue_pool().await;
        let job_queue = TransferUsdcToHedgingJobQueue::new(&pool);
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(TerminalBaseToAlpaca(TerminalOutcome::DeadlineElapsed)),
            timeout: Duration::from_secs(3600),
            job_queue,
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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
        let job_queue = TransferUsdcToHedgingJobQueue::new(&pool);
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(TerminalBaseToAlpaca(TerminalOutcome::PreviouslyFailed)),
            timeout: Duration::from_secs(3600),
            job_queue,
        };
        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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
        let job_queue = TransferUsdcToMarketMakingJobQueue::new(&pool);
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(TerminalAlpacaToBase(TerminalOutcome::DeadlineElapsed)),
            job_queue,
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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
        let job_queue = TransferUsdcToMarketMakingJobQueue::new(&pool);
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(TerminalAlpacaToBase(TerminalOutcome::PreviouslyFailed)),
            job_queue,
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: stub.clone(),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
        };
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = Usdc::new(float!(250));
        let job = TransferUsdcToMarketMaking {
            id: id.clone(),
            amount,
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
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RecordingResume {
                fail: false,
                captured: std::sync::Mutex::new(None),
            }),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
        };

        Job::perform(&job, &ctx).await.unwrap();
    }

    #[tokio::test]
    async fn market_making_perform_propagates_resume_failure() {
        let pool = setup_queue_pool().await;
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RecordingResume {
                fail: true,
                captured: std::sync::Mutex::new(None),
            }),
            job_queue: TransferUsdcToMarketMakingJobQueue::new(&pool),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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
                required: U256::from(1_000_000u64),
                actual: U256::ZERO,
            })
        }
    }

    /// Hypothesis: WithdrawalTxUnderconfirmed re-enqueues with
    /// SETTLEMENT_REDRIVE_DELAY and returns Ok (job stays alive, no apalis
    /// retry budget consumed).
    #[tokio::test]
    async fn market_making_job_reschedules_underconfirmed_withdrawal() {
        let pool = setup_queue_pool().await;
        let job_queue = TransferUsdcToMarketMakingJobQueue::new(&pool);
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(UnderconfirmedWithdrawal),
            job_queue,
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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
        let job_queue = TransferUsdcToMarketMakingJobQueue::new(&pool);
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(InsufficientUsdcBalance),
            job_queue,
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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
            use st0x_bridge::cctp::CctpError;
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
        let job_queue = TransferUsdcToMarketMakingJobQueue::new(&pool);
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(SettlementRpcFailure),
            job_queue,
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
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
        assert!(
            run_at >= before + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at <= after + i64::try_from(SETTLEMENT_REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{SETTLEMENT_REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }
}
