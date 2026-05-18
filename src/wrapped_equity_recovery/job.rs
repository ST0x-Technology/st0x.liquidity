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
use tracing::{debug, error, info, warn};

use st0x_event_sorcery::{SendError, Store};
use st0x_execution::Symbol;

#[cfg(any(test, feature = "test-support"))]
use crate::conductor::job::JobKind;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::inventory::{BroadcastingInventory, view::InFlightEquityLocation};
use crate::onchain::raindex::Raindex;
use crate::rebalancing::equity::CrossVenueEquityTransfer;
use crate::tokenized_equity_mint::TOKENIZED_EQUITY_DECIMALS;
use crate::wrapper::Wrapper;

use super::aggregate::{
    RecoveryOutcome, WrappedEquityRecovery, WrappedEquityRecoveryCommand,
    WrappedEquityRecoveryError, WrappedEquityRecoveryId,
};

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
    /// Raindex service for the orphan deposit path.
    pub(crate) raindex: Arc<dyn Raindex>,
    /// Wrapper service for symbol -> wtSTOCK address resolution.
    pub(crate) wrapper: Arc<dyn Wrapper>,
}

#[derive(Debug, Error)]
pub(crate) enum WrappedEquityRecoveryJobError {
    #[error("recovery aggregate error: {0}")]
    Aggregate(#[from] SendError<WrappedEquityRecovery>),

    #[error(transparent)]
    Domain(#[from] WrappedEquityRecoveryError),

    #[error("raindex error: {0}")]
    Raindex(#[from] crate::onchain::raindex::RaindexError),

    #[error("wrapper error: {0}")]
    Wrapper(#[from] crate::wrapper::WrapperError),

    #[error("shares conversion error: {0}")]
    Shares(#[from] st0x_execution::SharesConversionError),
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
        let symbol = self.symbol.clone();

        let Some(guard) = claim_guard(&ctx.equity_in_progress, &symbol) else {
            debug!(
                target: "rebalance",
                %symbol,
                "Skipping wrapped equity recovery: equity_in_progress already held",
            );
            return Ok(());
        };

        let snapshot = read_recovery_snapshot(&ctx.inventory, &symbol).await;
        let Some(snapshot) = snapshot else {
            debug!(
                target: "rebalance",
                %symbol,
                "Skipping wrapped equity recovery: no positive balance in inventory view",
            );
            drop(guard);
            return Ok(());
        };

        let recovery_id = WrappedEquityRecoveryId(uuid::Uuid::new_v4());

        ctx.store
            .send(
                &recovery_id,
                WrappedEquityRecoveryCommand::Detect {
                    symbol: symbol.clone(),
                    shares: snapshot.shares,
                },
            )
            .await?;

        info!(
            target: "rebalance",
            %symbol,
            %recovery_id,
            shares = %snapshot.shares,
            "Wrapped equity recovery: dispatched detection",
        );

        let outcome = match snapshot.dispatch {
            DispatchDecision::ActiveMint(mint_id) => {
                dispatch_mint(ctx, &recovery_id, &mint_id).await
            }
            DispatchDecision::ActiveRedemption(redemption_id) => {
                dispatch_redemption(ctx, &recovery_id, &redemption_id).await
            }
            DispatchDecision::Orphan => dispatch_orphan(ctx, &recovery_id, &symbol).await,
        };

        match outcome {
            Ok(outcome) => {
                ctx.store
                    .send(
                        &recovery_id,
                        WrappedEquityRecoveryCommand::CompleteRecovery { outcome },
                    )
                    .await?;
                info!(target: "rebalance", %symbol, %recovery_id, "Wrapped equity recovery completed");
            }
            Err(error) => {
                let reason = format!("{error}");
                error!(
                    target: "rebalance",
                    %symbol,
                    %recovery_id,
                    %reason,
                    "Wrapped equity recovery failed",
                );
                ctx.store
                    .send(
                        &recovery_id,
                        WrappedEquityRecoveryCommand::FailRecovery { reason },
                    )
                    .await?;
                return Err(error);
            }
        }

        drop(guard);
        Ok(())
    }
}

struct RecoverySnapshot {
    shares: st0x_execution::FractionalShares,
    dispatch: DispatchDecision,
}

enum DispatchDecision {
    ActiveMint(crate::tokenized_equity_mint::IssuerRequestId),
    ActiveRedemption(crate::equity_redemption::RedemptionAggregateId),
    Orphan,
}

async fn read_recovery_snapshot(
    inventory: &BroadcastingInventory,
    symbol: &Symbol,
) -> Option<RecoverySnapshot> {
    let view = inventory.read().await;
    let shares = view.inflight_equity_at(symbol, InFlightEquityLocation::BaseWalletWrapped)?;
    if shares == st0x_execution::FractionalShares::ZERO {
        return None;
    }
    let dispatch = decide_dispatch(&view, symbol);
    drop(view);
    Some(RecoverySnapshot { shares, dispatch })
}

fn decide_dispatch(
    view: &crate::inventory::view::InventoryView,
    symbol: &Symbol,
) -> DispatchDecision {
    view.active_mint(symbol).map_or_else(
        || {
            view.active_redemption(symbol)
                .map_or(DispatchDecision::Orphan, |redemption_id| {
                    DispatchDecision::ActiveRedemption(redemption_id.clone())
                })
        },
        |mint_id| DispatchDecision::ActiveMint(mint_id.clone()),
    )
}

/// RAII helper that inserts `symbol` into `equity_in_progress` and removes it
/// on drop. Returns `None` if another task already holds the guard.
struct InProgressGuard {
    symbol: Symbol,
    set: Arc<RwLock<HashSet<Symbol>>>,
}

impl Drop for InProgressGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.set.write() {
            guard.remove(&self.symbol);
        }
    }
}

fn claim_guard(set: &Arc<RwLock<HashSet<Symbol>>>, symbol: &Symbol) -> Option<InProgressGuard> {
    let mut guard = set.write().ok()?;
    if guard.contains(symbol) {
        return None;
    }
    guard.insert(symbol.clone());
    drop(guard);
    Some(InProgressGuard {
        symbol: symbol.clone(),
        set: Arc::clone(set),
    })
}

async fn dispatch_mint(
    ctx: &WrappedEquityRecoveryCtx,
    recovery_id: &WrappedEquityRecoveryId,
    mint_id: &crate::tokenized_equity_mint::IssuerRequestId,
) -> Result<RecoveryOutcome, WrappedEquityRecoveryJobError> {
    ctx.store
        .send(
            recovery_id,
            WrappedEquityRecoveryCommand::DispatchToMint {
                mint_id: mint_id.clone(),
            },
        )
        .await?;

    if let Err(error) = ctx.transfer.resume_mint(mint_id).await {
        warn!(
            target: "rebalance",
            %mint_id,
            ?error,
            "Wrapped equity recovery: resume_mint failed",
        );
        return Err(WrappedEquityRecoveryJobError::Domain(
            WrappedEquityRecoveryError::InvalidTransition {
                state: format!("resume_mint error: {error}"),
            },
        ));
    }

    Ok(RecoveryOutcome::MintResumed {
        mint_id: mint_id.clone(),
    })
}

async fn dispatch_redemption(
    ctx: &WrappedEquityRecoveryCtx,
    recovery_id: &WrappedEquityRecoveryId,
    redemption_id: &crate::equity_redemption::RedemptionAggregateId,
) -> Result<RecoveryOutcome, WrappedEquityRecoveryJobError> {
    ctx.store
        .send(
            recovery_id,
            WrappedEquityRecoveryCommand::DispatchToRedemption {
                redemption_id: redemption_id.clone(),
            },
        )
        .await?;

    if let Err(error) = ctx.transfer.resume_redemption(redemption_id).await {
        warn!(
            target: "rebalance",
            %redemption_id,
            ?error,
            "Wrapped equity recovery: resume_redemption failed",
        );
        return Err(WrappedEquityRecoveryJobError::Domain(
            WrappedEquityRecoveryError::InvalidTransition {
                state: format!("resume_redemption error: {error}"),
            },
        ));
    }

    Ok(RecoveryOutcome::RedemptionResumed {
        redemption_id: redemption_id.clone(),
    })
}

async fn dispatch_orphan(
    ctx: &WrappedEquityRecoveryCtx,
    recovery_id: &WrappedEquityRecoveryId,
    symbol: &Symbol,
) -> Result<RecoveryOutcome, WrappedEquityRecoveryJobError> {
    use st0x_execution::SharesBlockchain;

    let wrapped_token = ctx.wrapper.lookup_derivative(symbol)?;
    let vault_id = ctx.raindex.lookup_vault_id(wrapped_token).await?;

    let shares = {
        let view = ctx.inventory.read().await;
        view.inflight_equity_at(symbol, InFlightEquityLocation::BaseWalletWrapped)
            .ok_or_else(|| {
                WrappedEquityRecoveryJobError::Domain(
                    WrappedEquityRecoveryError::InvalidTransition {
                        state: "missing inflight wrapped equity at orphan dispatch".to_string(),
                    },
                )
            })?
    };
    let raw = shares.to_u256_18_decimals()?;

    let tx_hash = ctx
        .raindex
        .submit_deposit(wrapped_token, vault_id, raw, TOKENIZED_EQUITY_DECIMALS)
        .await?;

    ctx.store
        .send(
            recovery_id,
            WrappedEquityRecoveryCommand::SubmitOrphanDeposit {
                vault_deposit_tx_hash: tx_hash,
            },
        )
        .await?;

    ctx.raindex.confirm_tx(tx_hash).await?;

    ctx.store
        .send(
            recovery_id,
            WrappedEquityRecoveryCommand::ConfirmOrphanDeposit {
                vault_deposit_tx_hash: tx_hash,
            },
        )
        .await?;

    Ok(RecoveryOutcome::OrphanDeposited {
        vault_deposit_tx_hash: tx_hash,
    })
}
