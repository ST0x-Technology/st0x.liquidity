//! Startup wiring for the trading-pipeline and equity-recovery job queues.
//!
//! [`setup_trading_job_queues`] is called once during conductor startup, before
//! the apalis monitor spawns, to create every fill-to-hedge and equity-recovery
//! queue, reset orphaned `Running` rows left by a previous crash, recover
//! in-flight submitted orders, and bootstrap the position-check queue. Extracted
//! from the `conductor` module to keep that module focused on lifecycle
//! orchestration.

use std::sync::Arc;
use std::time::Duration;

use st0x_event_sorcery::{Projection, Store};
use st0x_execution::SupportedExecutor;

use super::{
    build_unwrapped_equity_recovery_ctx, build_wrapped_equity_recovery_ctx,
    requeue_equity_recovery_orphans, requeue_trading_orphans,
};
use crate::equity_redemption::EquityRedemption;
use crate::inventory::BroadcastingInventory;
use crate::offchain::order::{
    HandleOrderRejectionJobQueue, OffchainOrder, PollOrderStatusJobQueue,
    ReconcileOrderFillJobQueue, recover_submitted_offchain_orders,
};
use crate::portfolio_snapshot::{PortfolioSnapshotJobQueue, bootstrap_portfolio_snapshot};
use crate::position_check::{CheckPositionsJobQueue, bootstrap_check_positions};
use crate::rebalancing::RebalancingService;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::trading::offchain::hedge::HedgeJobQueue;
use crate::trading::onchain::trade_accountant::DexTradeAccountingJobQueue;
use crate::unwrapped_equity_recovery::{
    UnwrappedEquityRecovery, UnwrappedEquityRecoveryCtx, UnwrappedEquityRecoveryJobQueue,
};
use crate::wrapped_equity_recovery::{
    WrappedEquityRecovery, WrappedEquityRecoveryCtx, WrappedEquityRecoveryJobQueue,
};

/// Equity-recovery stores and related state passed to [`setup_trading_job_queues`].
/// Bundles the optional rebalancing-side inputs needed to build the equity
/// recovery contexts, keeping the function argument count within the lint limit.
pub(super) struct EquityRecoveryInputs {
    pub(super) wrapped_store: Option<Arc<Store<WrappedEquityRecovery>>>,
    pub(super) unwrapped_store: Option<Arc<Store<UnwrappedEquityRecovery>>>,
    pub(super) rebalancing_service: Option<Arc<RebalancingService>>,
    pub(super) mint_store: Option<Arc<Store<TokenizedEquityMint>>>,
    pub(super) redemption_store: Option<Arc<Store<EquityRedemption>>>,
    pub(super) inventory: Arc<BroadcastingInventory>,
    pub(super) inventory_poll_interval: Duration,
}

/// All trading-related and equity-recovery job queues and contexts produced
/// at startup. Returned by [`setup_trading_job_queues`].
pub(super) struct TradingJobQueues {
    pub(super) hedge_queue: HedgeJobQueue,
    pub(super) poll_status_queue: PollOrderStatusJobQueue,
    pub(super) reconcile_queue: ReconcileOrderFillJobQueue,
    pub(super) rejection_queue: HandleOrderRejectionJobQueue,
    pub(super) wrapped_equity_recovery_queue: WrappedEquityRecoveryJobQueue,
    pub(super) unwrapped_equity_recovery_queue: UnwrappedEquityRecoveryJobQueue,
    pub(super) wrapped_equity_recovery_ctx: Option<Arc<WrappedEquityRecoveryCtx>>,
    pub(super) unwrapped_equity_recovery_ctx: Option<Arc<UnwrappedEquityRecoveryCtx>>,
    pub(super) check_positions_queue: CheckPositionsJobQueue,
    pub(super) portfolio_snapshot_queue: PortfolioSnapshotJobQueue,
}

/// Creates all trading-side and equity-recovery job queues, resets any orphaned
/// `Running` rows left by a previous crash, recovers in-flight submitted orders,
/// and bootstraps the position-check queue. Called once during conductor startup,
/// before the apalis monitor spawns, so every `Running` row is orphaned by
/// definition.
pub(super) async fn setup_trading_job_queues(
    cqrs_pool: &sqlx::SqlitePool,
    apalis_pool: &apalis_sqlite::SqlitePool,
    job_queue: &DexTradeAccountingJobQueue,
    equity_recovery: EquityRecoveryInputs,
    offchain_order_projection: &Arc<Projection<OffchainOrder>>,
    executor_type: SupportedExecutor,
) -> anyhow::Result<TradingJobQueues> {
    let EquityRecoveryInputs {
        wrapped_store: wrapped_equity_recovery_store,
        unwrapped_store: unwrapped_equity_recovery_store,
        rebalancing_service,
        mint_store,
        redemption_store,
        inventory,
        inventory_poll_interval,
    } = equity_recovery;
    let hedge_queue: HedgeJobQueue = crate::conductor::job::JobQueue::new(apalis_pool);
    let mut poll_status_queue = PollOrderStatusJobQueue::new(apalis_pool);
    let reconcile_queue = ReconcileOrderFillJobQueue::new(apalis_pool);
    let rejection_queue = HandleOrderRejectionJobQueue::new(apalis_pool);

    // A previous process may have died mid-job anywhere on the
    // fill-to-hedge path, leaving apalis `Running` rows no live worker
    // owns. Reset them before the monitor spawns, exactly like the
    // transfer/backfill/recovery queues above. Trade accounting is the
    // one that cannot be recovered any other way: once the backfill
    // checkpoint advances, its job row is the only record of the fill.
    requeue_trading_orphans(job_queue, "trade accounting").await?;
    requeue_trading_orphans(&hedge_queue, "hedge placement").await?;
    requeue_trading_orphans(&poll_status_queue, "order status polling").await?;
    requeue_trading_orphans(&reconcile_queue, "order fill reconciliation").await?;
    requeue_trading_orphans(&rejection_queue, "order rejection handling").await?;

    let wrapped_equity_recovery_queue = WrappedEquityRecoveryJobQueue::new(apalis_pool);
    let unwrapped_equity_recovery_queue = UnwrappedEquityRecoveryJobQueue::new(apalis_pool);

    let wrapped_equity_recovery_ctx = build_wrapped_equity_recovery_ctx(
        wrapped_equity_recovery_store,
        rebalancing_service.clone(),
        mint_store.clone(),
        redemption_store.clone(),
        inventory.clone(),
        wrapped_equity_recovery_queue.clone(),
        inventory_poll_interval,
    );

    let unwrapped_equity_recovery_ctx = build_unwrapped_equity_recovery_ctx(
        unwrapped_equity_recovery_store,
        rebalancing_service,
        mint_store,
        redemption_store,
        inventory,
        unwrapped_equity_recovery_queue.clone(),
        inventory_poll_interval,
    );

    requeue_equity_recovery_orphans(
        wrapped_equity_recovery_ctx.as_ref(),
        unwrapped_equity_recovery_ctx.as_ref(),
        &wrapped_equity_recovery_queue,
        &unwrapped_equity_recovery_queue,
    )
    .await?;

    let check_positions_queue = CheckPositionsJobQueue::new(apalis_pool);

    recover_submitted_offchain_orders(
        offchain_order_projection,
        &mut poll_status_queue,
        executor_type,
    )
    .await?;

    bootstrap_check_positions(apalis_pool, &check_positions_queue).await?;

    let portfolio_snapshot_queue = PortfolioSnapshotJobQueue::new(apalis_pool);
    bootstrap_portfolio_snapshot(cqrs_pool, apalis_pool, &portfolio_snapshot_queue).await?;

    Ok(TradingJobQueues {
        hedge_queue,
        poll_status_queue,
        reconcile_queue,
        rejection_queue,
        wrapped_equity_recovery_queue,
        unwrapped_equity_recovery_queue,
        wrapped_equity_recovery_ctx,
        unwrapped_equity_recovery_ctx,
        check_positions_queue,
        portfolio_snapshot_queue,
    })
}
