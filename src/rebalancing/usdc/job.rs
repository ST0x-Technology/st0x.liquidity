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

use st0x_evm::Wallet;
use st0x_finance::Usdc;

use super::UsdcTransferError;
use super::manager::CrossVenueCashTransfer;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::usdc_rebalance::UsdcRebalanceId;

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
        let resume = ctx.transfer.resume_base_to_alpaca(&self.id, self.amount);

        match tokio::time::timeout(ctx.timeout, resume).await {
            Ok(result) => Ok(result?),
            Err(_elapsed) => Err(TransferUsdcToHedgingJobError::Timeout {
                id: self.id.clone(),
                timeout: ctx.timeout,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use st0x_float_macro::float;

    use super::*;

    /// Models a hung RPC inside the transfer: the resume future never
    /// completes within the configured per-attempt timeout.
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

    #[tokio::test]
    async fn perform_times_out_when_resume_hangs() {
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::new(HangingResume),
            timeout: Duration::from_millis(50),
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
        let stub = Arc::new(RecordingResume {
            fail: false,
            captured: std::sync::Mutex::new(None),
        });
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: stub.clone(),
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
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RecordingResume {
                fail: false,
                captured: std::sync::Mutex::new(None),
            }),
        };
        let job = TransferUsdcToMarketMaking {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(100)),
        };

        Job::perform(&job, &ctx).await.unwrap();
    }

    #[tokio::test]
    async fn market_making_perform_propagates_resume_failure() {
        let ctx = TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(RecordingResume {
                fail: true,
                captured: std::sync::Mutex::new(None),
            }),
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
}

/// Dependencies the Alpaca->Base job needs. Symmetric to
/// [`TransferUsdcToHedgingCtx`].
pub(crate) struct TransferUsdcToMarketMakingCtx {
    pub(crate) transfer: Arc<dyn ResumeAlpacaToBase>,
}

/// Errors emitted by [`TransferUsdcToMarketMaking::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferUsdcToMarketMakingJobError {
    #[error(transparent)]
    Transfer(#[from] UsdcTransferError),
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
        ctx.transfer
            .resume_alpaca_to_base(&self.id, self.amount)
            .await?;
        Ok(())
    }
}
