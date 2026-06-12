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

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use st0x_execution::{FractionalShares, Symbol};

use super::{CrossVenueEquityTransfer, MintTransferError, RedemptionError};
use crate::conductor::job::{Job, JobQueue, Label};
use crate::equity_redemption::RedemptionAggregateId;
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

    use serde_json::json;
    use st0x_float_macro::float;

    use super::*;
    use crate::equity_redemption::redemption_aggregate_id;
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
