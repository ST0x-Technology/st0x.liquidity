//! Apalis job that drives tokenized equity mints (hedging -> market-making)
//! through the `TokenizedEquityMint` lifecycle.
//!
//! The job is keyed by an `IssuerRequestId` chosen at enqueue time, so apalis
//! retries (and bot restarts that re-pick the row from the Jobs table) hit
//! the same aggregate. The worker calls the trait-erased resume entry point
//! on the equity transfer, which loads the aggregate via `Store::load` and
//! dispatches on its current state: an absent aggregate starts a fresh mint,
//! an in-flight one resumes from its persisted step, and a terminal one is a
//! no-op. New transfers and mid-flight resumes share the same entry point --
//! that uniformity is what makes recovery dispatch fall out of the standard
//! transfer lifecycle.
//!
//! The per-symbol `equity_in_progress` guard is cleared event-driven when the
//! aggregate reaches a terminal state (success or a recorded failure), not by
//! this worker. A transient failure that only schedules a retry keeps the
//! guard latched so automation does not re-arm a fresh mint on top of a
//! partial one.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use st0x_execution::{FractionalShares, Symbol};

use super::{CrossVenueEquityTransfer, MintTransferError};
use crate::conductor::job::{Job, JobQueue, Label};
use crate::tokenized_equity_mint::IssuerRequestId;

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
        ctx.transfer
            .resume_equity_to_market_making(&self.issuer_request_id, &self.symbol, self.quantity)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;
    use st0x_float_macro::float;

    use super::*;
    use crate::rebalancing::equity::MintError;
    use crate::tokenized_equity_mint::issuer_request_id;

    /// Records the resume call and returns a configurable outcome, so the
    /// job's `perform` can be tested without broker/onchain setup.
    struct RecordingResume {
        fail: bool,
        captured: Mutex<Option<(IssuerRequestId, Symbol, FractionalShares)>>,
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

            if self.fail {
                Err(MintTransferError::PreReceipt(MintError::EntityNotFound {
                    issuer_request_id: issuer_request_id.clone(),
                    expected_state: "test-induced",
                }))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn perform_forwards_id_symbol_and_quantity_to_resume() {
        let stub = Arc::new(RecordingResume {
            fail: false,
            captured: Mutex::new(None),
        });
        let ctx = TransferEquityToMarketMakingCtx {
            transfer: stub.clone(),
        };
        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_request_id("mint-forward"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
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
    async fn perform_propagates_resume_failure() {
        let ctx = TransferEquityToMarketMakingCtx {
            transfer: Arc::new(RecordingResume {
                fail: true,
                captured: Mutex::new(None),
            }),
        };
        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_request_id("mint-fail"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
        };

        let error = Job::perform(&job, &ctx).await.unwrap_err();

        // The failure must propagate (not be swallowed) so apalis retries and
        // the event-driven `equity_in_progress` guard stays latched until the
        // aggregate reaches a terminal state -- swallowing it would free the
        // guard and let a fresh mint arm on top of a partial one.
        assert!(matches!(
            error,
            TransferEquityToMarketMakingJobError::Transfer(MintTransferError::PreReceipt(_))
        ));
    }

    #[test]
    fn payload_roundtrips_through_json() {
        let job = TransferEquityToMarketMaking {
            issuer_request_id: issuer_request_id("mint-roundtrip"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(2.5)),
        };

        let expected = json!({
            "issuer_request_id": issuer_request_id("mint-roundtrip").to_string(),
            "symbol": "AAPL",
            "quantity": "2.5",
        });

        assert_eq!(serde_json::to_value(&job).unwrap(), expected);

        let roundtripped: TransferEquityToMarketMaking = serde_json::from_value(expected).unwrap();

        assert_eq!(roundtripped.issuer_request_id, job.issuer_request_id);
        assert_eq!(roundtripped.symbol, job.symbol);
        assert_eq!(roundtripped.quantity, job.quantity);
    }
}
