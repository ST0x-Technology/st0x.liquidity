//! Apalis job that drives a `BaseToAlpaca` USDC transfer through the
//! `UsdcRebalance` lifecycle.
//!
//! Each job is keyed by a `UsdcRebalanceId` chosen at enqueue time, so apalis
//! retries (and bot restarts that re-pick the row from the Jobs table) hit
//! the same aggregate. The worker calls
//! [`ResumeBaseToAlpaca::resume_base_to_alpaca`] on the trait-erased
//! transfer, which loads the aggregate via `Store::load` and dispatches on
//! its current state. New transfers and mid-flight resumes share the same
//! entry point — that uniformity is what makes recovery dispatch fall out of
//! the standard transfer lifecycle.
//!
//! On failure the worker clears the `usdc_in_progress` flag so the next
//! imbalance check can fire a fresh rebalance once the operator has dealt
//! with whatever caused the failure.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use st0x_evm::Wallet;
use st0x_finance::Usdc;

use super::UsdcTransferError;
use super::manager::CrossVenueCashTransfer;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::usdc_rebalance::UsdcRebalanceId;

/// Apalis queue type for [`TransferUsdcToHedging`].
pub(crate) type TransferUsdcToHedgingJobQueue = JobQueue<TransferUsdcToHedging>;

/// Trait-erased entry point the apalis job calls. Erasing the `Chain` generic
/// here lets the conductor build a single concrete `Ctx` regardless of which
/// wallet backend is wired in.
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

/// Dependencies the job needs to resume the transfer and clear the global
/// USDC-in-progress flag on failure.
pub(crate) struct TransferUsdcToHedgingCtx {
    pub(crate) transfer: Arc<dyn ResumeBaseToAlpaca>,
    pub(crate) usdc_in_progress: Arc<AtomicBool>,
}

/// Errors emitted by [`TransferUsdcToHedging::perform`].
#[derive(Debug, Error)]
pub(crate) enum TransferUsdcToHedgingJobError {
    #[error(transparent)]
    Transfer(#[from] UsdcTransferError),
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
        let result = ctx
            .transfer
            .resume_base_to_alpaca(&self.id, self.amount)
            .await;

        if let Err(error) = &result {
            ctx.usdc_in_progress.store(false, Ordering::SeqCst);
            warn!(
                target: "rebalance",
                id = %self.id,
                %error,
                "TransferUsdcToHedging failed; cleared usdc_in_progress flag",
            );
        }

        result?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;

    use uuid::Uuid;

    use super::*;

    /// Stub implementation that records calls and returns either Ok or a
    /// canned error. Lets us drive [`TransferUsdcToHedging::perform`] without
    /// the full CCTP/Alpaca/raindex setup.
    struct StubResume {
        call_count: Mutex<usize>,
        outcome: StubOutcome,
    }

    enum StubOutcome {
        Ok,
        Err,
    }

    impl StubResume {
        fn new(outcome: StubOutcome) -> Self {
            Self {
                call_count: Mutex::new(0),
                outcome,
            }
        }
    }

    #[async_trait]
    impl ResumeBaseToAlpaca for StubResume {
        async fn resume_base_to_alpaca(
            &self,
            _id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            *self.call_count.lock().unwrap() += 1;
            match self.outcome {
                StubOutcome::Ok => Ok(()),
                StubOutcome::Err => Err(UsdcTransferError::PreviouslyFailedAggregate {
                    id: UsdcRebalanceId(Uuid::new_v4()),
                }),
            }
        }
    }

    fn make_job() -> TransferUsdcToHedging {
        TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::from_str("1000").unwrap(),
        }
    }

    #[tokio::test]
    async fn perform_clears_usdc_in_progress_on_failure() {
        let stub = Arc::new(StubResume::new(StubOutcome::Err));
        let usdc_in_progress = Arc::new(AtomicBool::new(true));
        let ctx = TransferUsdcToHedgingCtx {
            transfer: stub.clone(),
            usdc_in_progress: usdc_in_progress.clone(),
        };

        let error = make_job().perform(&ctx).await.unwrap_err();
        assert!(matches!(
            error,
            TransferUsdcToHedgingJobError::Transfer(
                UsdcTransferError::PreviouslyFailedAggregate { .. }
            )
        ));
        assert!(
            !usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress should be cleared so the next imbalance check can retrigger"
        );
        assert_eq!(*stub.call_count.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn perform_preserves_usdc_in_progress_on_success() {
        let stub = Arc::new(StubResume::new(StubOutcome::Ok));
        let usdc_in_progress = Arc::new(AtomicBool::new(true));
        let ctx = TransferUsdcToHedgingCtx {
            transfer: stub.clone(),
            usdc_in_progress: usdc_in_progress.clone(),
        };

        make_job().perform(&ctx).await.unwrap();
        assert!(
            usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress should remain set on success; the next event clears it"
        );
        assert_eq!(*stub.call_count.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn perform_forwards_id_and_amount_to_transfer() {
        struct RecordingResume {
            captured: Mutex<Option<(UsdcRebalanceId, Usdc)>>,
        }

        #[async_trait]
        impl ResumeBaseToAlpaca for RecordingResume {
            async fn resume_base_to_alpaca(
                &self,
                id: &UsdcRebalanceId,
                amount: Usdc,
            ) -> Result<(), UsdcTransferError> {
                *self.captured.lock().unwrap() = Some((id.clone(), amount));
                Ok(())
            }
        }

        let recorder = Arc::new(RecordingResume {
            captured: Mutex::new(None),
        });
        let ctx = TransferUsdcToHedgingCtx {
            transfer: recorder.clone(),
            usdc_in_progress: Arc::new(AtomicBool::new(false)),
        };
        let job = make_job();
        let expected_id = job.id.clone();
        let expected_amount = job.amount;

        job.perform(&ctx).await.unwrap();

        let captured = recorder
            .captured
            .lock()
            .unwrap()
            .clone()
            .expect("resume should have been called");
        assert_eq!(captured.0, expected_id);
        assert_eq!(captured.1, expected_amount);
    }
}
