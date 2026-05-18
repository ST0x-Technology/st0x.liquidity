//! Apalis job that drives wrapped-equity recovery.
//!
//! Enqueued per symbol with a positive `BaseWalletWrappedEquity` balance.
//! The job:
//!
//! 1. Tries to claim the `equity_in_progress` guard for the symbol. If
//!    another task already holds it the job exits at debug log; the next
//!    inventory poll re-evaluates.
//! 2. Consults the inventory view's active mint / active redemption maps
//!    to decide which path applies.
//! 3. Initializes a `WrappedEquityRecovery` aggregate via the supplied
//!    `Store`, records the dispatch decision, and -- for the orphan path
//!    -- drives the deposit through to `RecoveryCompleted`.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use st0x_event_sorcery::{SendError, Store};
use st0x_execution::Symbol;

#[cfg(any(test, feature = "test-support"))]
use crate::conductor::job::JobKind;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::inventory::BroadcastingInventory;
use crate::rebalancing::equity::CrossVenueEquityTransfer;

use super::aggregate::{WrappedEquityRecovery, WrappedEquityRecoveryError};

pub(crate) type WrappedEquityRecoveryJobQueue = JobQueue<WrappedEquityRecoveryJob>;

/// Dependencies the recovery job needs to look up active aggregates,
/// claim the per-symbol concurrency guard, drive existing transfers
/// forward, and emit aggregate commands.
pub(crate) struct WrappedEquityRecoveryCtx {
    /// Snapshot of the current inventory view, used to read the
    /// `active_mints` / `active_redemptions` maps and the wallet balance.
    pub(crate) inventory: Arc<BroadcastingInventory>,
    /// Aggregate store for emitting recovery events.
    pub(crate) store: Arc<Store<WrappedEquityRecovery>>,
    /// Drives the underlying mint / redemption forward when an active
    /// aggregate exists.
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
    /// Shared with the rebalancing trigger so a recovery dispatch can't
    /// race a normal transfer for the same symbol.
    pub(crate) equity_in_progress: Arc<RwLock<HashSet<Symbol>>>,
}

#[derive(Debug, Error)]
pub(crate) enum WrappedEquityRecoveryJobError {
    #[error("recovery aggregate error: {0}")]
    Aggregate(#[from] SendError<WrappedEquityRecovery>),

    #[error(transparent)]
    Domain(#[from] WrappedEquityRecoveryError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WrappedEquityRecoveryJob {
    pub(crate) symbol: Symbol,
}

impl Job<WrappedEquityRecoveryCtx> for WrappedEquityRecoveryJob {
    type Output = ();
    type Error = WrappedEquityRecoveryJobError;

    const WORKER_NAME: &'static str = "wrapped-equity-recovery-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: JobKind = JobKind::WrappedEquityRecovery;

    fn label(&self) -> Label {
        Label::new(format!("WrappedEquityRecovery:{}", self.symbol))
    }

    async fn perform(&self, ctx: &WrappedEquityRecoveryCtx) -> Result<Self::Output, Self::Error> {
        // Reference every field so the type signature is exercised; the real
        // dispatch logic lands with the implementation step of RAI-87.
        let _inventory = &ctx.inventory;
        let _store = &ctx.store;
        let _transfer = &ctx.transfer;
        let _equity_in_progress = &ctx.equity_in_progress;
        todo!(
            "WrappedEquityRecoveryJob::perform: claim guard, decide path, drive aggregate. RAI-87"
        )
    }
}
