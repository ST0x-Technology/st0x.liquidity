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
        ctx.transfer
            .resume_base_to_alpaca(&self.id, self.amount)
            .await?;
        Ok(())
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
