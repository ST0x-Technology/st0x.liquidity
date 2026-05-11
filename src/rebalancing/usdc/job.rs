//! Apalis-backed job for the Base-to-Alpaca USDC transfer.
//!
//! Wraps [`CrossVenueCashTransfer::resume_base_to_alpaca`] so the workflow is
//! durable across crashes: each in-flight transfer corresponds to a queued
//! [`TransferUsdcToHedging`] task that can pick up from any persisted
//! [`UsdcRebalance`] state when retried.
//!
//! [`CrossVenueCashTransfer::resume_base_to_alpaca`]: super::manager::CrossVenueCashTransfer::resume_base_to_alpaca
//! [`UsdcRebalance`]: crate::usdc_rebalance::UsdcRebalance

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use st0x_evm::Wallet;
use st0x_finance::Usdc;

use super::CrossVenueCashTransfer;
use super::UsdcTransferError;
#[cfg(any(test, feature = "test-support"))]
use crate::conductor::job::JobKind;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::usdc_rebalance::UsdcRebalanceId;

pub(crate) type TransferUsdcToHedgingJobQueue = JobQueue<TransferUsdcToHedging>;

/// Object-safe wrapper over the single [`CrossVenueCashTransfer`] entry
/// point the Base-to-Alpaca job needs. Lets the job context hold an
/// `Arc<dyn>` instead of carrying the wallet's `Chain` type parameter.
#[async_trait]
pub(crate) trait BaseToAlpacaTransfer: Send + Sync + 'static {
    async fn resume(&self, id: &UsdcRebalanceId, amount: Usdc) -> Result<(), UsdcTransferError>;
}

#[async_trait]
impl<Chain> BaseToAlpacaTransfer for CrossVenueCashTransfer<Chain>
where
    Chain: Wallet + Send + Sync + 'static,
{
    async fn resume(&self, id: &UsdcRebalanceId, amount: Usdc) -> Result<(), UsdcTransferError> {
        self.resume_base_to_alpaca(id, amount).await
    }
}

/// Shared dependencies for [`TransferUsdcToHedging`].
pub(crate) struct TransferUsdcToHedgingCtx {
    pub(crate) transfer: Arc<dyn BaseToAlpacaTransfer>,
}

/// Drives a single Base-to-Alpaca USDC rebalance from initial enqueue
/// through to terminal state, resuming from the persisted aggregate state
/// on each retry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TransferUsdcToHedging {
    pub(crate) id: UsdcRebalanceId,
    pub(crate) amount: Usdc,
}

impl Job<TransferUsdcToHedgingCtx> for TransferUsdcToHedging {
    type Output = ();
    type Error = UsdcTransferError;

    const WORKER_NAME: &'static str = "transfer-usdc-to-hedging-worker";
    const TERMINAL_FAILURE_MSG: &'static str = "Base-to-Alpaca USDC transfer failed after retries";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: JobKind = JobKind::TransferUsdcToHedging;

    fn label(&self) -> Label {
        Label::new(format!("TransferUsdcToHedging:{}", self.id))
    }

    async fn perform(&self, ctx: &TransferUsdcToHedgingCtx) -> Result<Self::Output, Self::Error> {
        ctx.transfer.resume(&self.id, self.amount).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    use st0x_float_macro::float;

    use super::*;

    struct CapturingTransfer {
        calls: AtomicUsize,
        last: Mutex<Option<(UsdcRebalanceId, Usdc)>>,
        result: Mutex<Result<(), UsdcTransferError>>,
    }

    impl CapturingTransfer {
        fn new(result: Result<(), UsdcTransferError>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                last: Mutex::new(None),
                result: Mutex::new(result),
            }
        }
    }

    #[async_trait]
    impl BaseToAlpacaTransfer for CapturingTransfer {
        async fn resume(
            &self,
            id: &UsdcRebalanceId,
            amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last.lock().unwrap() = Some((id.clone(), amount));
            let mut result_slot = self.result.lock().unwrap();
            std::mem::replace(&mut *result_slot, Ok(()))
        }
    }

    #[tokio::test]
    async fn perform_delegates_to_resume_base_to_alpaca_with_payload() {
        let transfer = Arc::new(CapturingTransfer::new(Ok(())));
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::clone(&transfer) as Arc<dyn BaseToAlpacaTransfer>,
        };

        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(123)),
        };

        job.perform(&ctx).await.unwrap();

        assert_eq!(transfer.calls.load(Ordering::SeqCst), 1);
        let last = transfer.last.lock().unwrap().clone().unwrap();
        assert_eq!(last.0, job.id);
        assert_eq!(last.1, job.amount);
    }

    #[tokio::test]
    async fn perform_propagates_resume_errors_to_apalis_retry_layer() {
        let transfer = Arc::new(CapturingTransfer::new(Err(
            UsdcTransferError::WithdrawalFailed {
                status: "test".to_string(),
            },
        )));
        let ctx = TransferUsdcToHedgingCtx {
            transfer: Arc::clone(&transfer) as Arc<dyn BaseToAlpacaTransfer>,
        };

        let job = TransferUsdcToHedging {
            id: UsdcRebalanceId(Uuid::new_v4()),
            amount: Usdc::new(float!(50)),
        };

        let err = job.perform(&ctx).await.unwrap_err();
        assert!(matches!(err, UsdcTransferError::WithdrawalFailed { .. }));
    }
}
