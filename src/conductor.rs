//! Orchestrates the bot lifecycle: startup sequencing, runtime task management,
//! and trade processing. [`Conductor::run`] is the entry point.

mod builder;
mod exit;
pub(crate) mod job;
mod manifest;
pub(crate) mod monitor;

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use anyhow::Context;
use apalis::prelude::Status;
use chrono::Utc;
use sqlx::SqlitePool;
use sqlx_apalis::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use task_supervisor::SupervisorHandle;
use tokio::sync::{Mutex, broadcast};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use st0x_config::{AssetsConfig, BrokerCtx, Ctx, CtxError, ExecutionThreshold, RebalancingCtx};
use st0x_dto::Statement;
use st0x_event_sorcery::{
    Projection, Store, StoreBuilder, compact_events, incremental_vacuum, load_all_ids, load_entity,
};
use st0x_evm::Wallet;
use st0x_execution::{
    ClientOrderId, CounterTradePreflight, CounterTradeReservation, CounterTradeSkipReason,
    ExecutionError, Executor, FractionalShares, MarketOrder, Symbol, TryIntoExecutor,
};
use st0x_raindex::{RaindexService, RaindexVaultId};
use st0x_wrapper::WrapperService;

use crate::alpaca_wallet::AlpacaWalletService;
use crate::conductor::exit::{ConductorExit, MonitorTaskError};
use crate::dashboard::Broadcaster;
use crate::equity_redemption::{
    EquityRedemption, interrupted_redemption_ids, symbols_with_stuck_redemptions,
};
use crate::inventory::{
    BroadcastingInventory, Inventory, InventoryProjection, InventorySnapshot, Venue,
};
use crate::offchain::order::{
    ExecutorOrderPlacer, HandleOrderRejectionJobQueue, OffchainOrder, OffchainOrderCommand,
    OffchainOrderId, OrderPlacer, PollOrderStatus, PollOrderStatusJobQueue,
    ReconcileOrderFillJobQueue, recover_submitted_offchain_orders,
};
use crate::onchain::OnchainTrade;
use crate::onchain::USDC_BASE;
#[cfg(test)]
use crate::onchain::accumulator::check_all_positions;
use crate::onchain::accumulator::{ExecutionCtx, check_execution_readiness};
use crate::onchain::approvals::{build_approval_targets, grant_startup_approvals};
use crate::onchain::backfill::BackfillJobQueue;
use crate::onchain::trade::{RaindexTradeEvent, extract_owned_vaults, extract_vaults_from_clear};
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand, OnChainTradeId};
use crate::position::{Position, PositionCommand, TradeId};
use crate::position_check::{CheckPositionsJobQueue, bootstrap_check_positions};
use crate::rebalancing::equity::{
    CrossVenueEquityTransfer, EquityTransferServices, TransferEquityToHedgingCtx,
    TransferEquityToMarketMakingCtx,
};
use crate::rebalancing::usdc::{TransferUsdcToHedgingCtx, TransferUsdcToMarketMakingCtx};
use crate::rebalancing::{
    RebalancerServices, RebalancingSchedulers, RebalancingService, RebalancingServiceConfig,
    to_wrapped_equities,
};
use crate::symbol::cache::SymbolCache;
use crate::tokenization::Tokenizer;
use crate::tokenization::alpaca::AlpacaTokenizationService;
use crate::tokenized_equity_mint::{TokenizedEquityMint, interrupted_mint_ids};
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::{DexTradeAccountingJobQueue, TradeAccountingError};
use crate::unwrapped_equity_recovery::{
    UnwrappedEquityRecovery, UnwrappedEquityRecoveryCtx, UnwrappedEquityRecoveryJobQueue,
    UnwrappedEquityRecoveryServices,
};
use crate::vault_lookup::{VaultLookup, VaultRegistryLookup};
use crate::vault_registry::{
    SeedVaultRegistry, SeedVaultRegistryCtx, SeedVaultRegistryJobQueue, VaultRegistry,
    VaultRegistryCommand, VaultRegistryId,
};
use crate::wrapped_equity_recovery::{
    WrappedEquityRecovery, WrappedEquityRecoveryCtx, WrappedEquityRecoveryJobQueue,
    WrappedEquityRecoveryServices,
};

pub(crate) use builder::CqrsFrameworks;
use manifest::QueryManifest;

/// CQRS/event-store and apalis worker pools over the same SQLite database.
#[derive(Clone)]
pub(crate) struct DatabasePools {
    pub(crate) cqrs: SqlitePool,
    pub(crate) apalis: apalis_sqlite::SqlitePool,
}

/// Opens an apalis-side pool (sqlx 0.8) against the same database as the
/// CQRS pool (sqlx 0.9). apalis workers read the `Jobs` table through this
/// pool; the event store writes events through the CQRS pool.
pub(crate) async fn connect_apalis_pool(
    database_url: &str,
) -> Result<apalis_sqlite::SqlitePool, apalis_sqlite::SqlxError> {
    let options: SqliteConnectOptions = st0x_config::effective_sqlite_url(database_url)
        .parse::<SqliteConnectOptions>()?
        .create_if_missing(true)
        .auto_vacuum(SqliteAutoVacuum::Incremental)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(10));

    apalis_sqlite::SqlitePool::connect_with(options).await
}

/// Sets up apalis SQLite storage tables, tolerating pre-existing
/// application migrations in the shared `_sqlx_migrations` table.
pub(crate) async fn setup_apalis_tables(
    pool: &apalis_sqlite::SqlitePool,
) -> Result<(), apalis_sqlite::SqlxError> {
    apalis_sqlite::SqliteStorage::migrations()
        .set_ignore_missing(true)
        .run(pool)
        .await?;

    Ok(())
}

/// Bundles CQRS frameworks used throughout the trade processing pipeline.
pub(crate) struct TradeProcessingCqrs {
    pub(crate) onchain_trade: Arc<Store<OnChainTrade>>,
    pub(crate) position: Arc<Store<Position>>,
    pub(crate) position_projection: Arc<Projection<Position>>,
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    pub(crate) execution_threshold: ExecutionThreshold,
    pub(crate) assets: AssetsConfig,
    pub(crate) counter_trade_submission_lock: Arc<Mutex<()>>,
    /// Queue every newly-submitted offchain order is enrolled into so the
    /// per-order poll job picks up where the broker left off. Without this
    /// hook the order stays `Submitted` forever -- the system has no other
    /// trigger to ask the broker about an in-flight order.
    pub(crate) poll_status_queue: PollOrderStatusJobQueue,
}

/// Orchestrates the bot's runtime by composing long-running supervised tasks
/// (e.g. order fill monitoring) with one-shot persistent jobs (e.g. trade
/// accounting) into a unified lifecycle. Waits for either the supervisor or
/// the apalis monitor to exit, then aborts all remaining tasks.
pub(crate) struct Conductor {
    /// Manages long-running tasks (order fill monitor, position monitor,
    /// executor maintenance) with automatic restart.
    supervisor: SupervisorHandle,
    /// Runs the apalis job queue workers that process trade accounting jobs.
    monitor: JoinHandle<Result<(), MonitorTaskError>>,
    /// Periodically removes terminal rows from the persistent job queue.
    job_cleanup: JoinHandle<()>,
    /// Cancelled by the outer shutdown handler to trigger graceful drain.
    shutdown_token: CancellationToken,
    /// Independent token for apalis — cancelled explicitly in Phase 2 after
    /// producers are stopped.
    apalis_shutdown_token: CancellationToken,
}

/// Resets a transfer queue's orphaned `Running`/`Queued` rows back to `Pending`
/// at startup -- run before workers spawn, so any such row is orphaned by a dead
/// process by definition. apalis cannot rescue these on its own (the
/// deterministic worker name keeps the heartbeat fresh, so the row never ages
/// out), and the re-arm / enqueue-dedup paths treat a non-terminal row as
/// in-flight, so a wedged row would silently suppress every future transfer in
/// that direction. Fails fast on a reset error -- the bot must not come up
/// looking healthy with crash-safe recovery disabled.
///
/// A previous process may have died mid-transfer, leaving an apalis `Running`
/// row that no live worker owns. Resets orphaned rows for every transfer
/// queue whose worker is wired (its ctx is present, i.e. rebalancing is
/// enabled) before the monitor spawns, so any `Running` row is orphaned by
/// definition. The USDC queues are wedge-prone symmetrically: Base->Alpaca
/// via the enqueue dedup, Alpaca->Base because `rearm_stranded_transfers`
/// treats a `Running` row as in-flight and skips it. The equity mint queue
/// is wedge-prone via its per-symbol enqueue dedup.
async fn requeue_wired_transfer_orphans(
    schedulers: &RebalancingSchedulers,
    usdc_to_hedging_ctx: Option<&Arc<TransferUsdcToHedgingCtx>>,
    usdc_to_market_making_ctx: Option<&Arc<TransferUsdcToMarketMakingCtx>>,
    equity_to_market_making_ctx: Option<&Arc<TransferEquityToMarketMakingCtx>>,
    equity_to_hedging_ctx: Option<&Arc<TransferEquityToHedgingCtx>>,
) -> anyhow::Result<()> {
    if usdc_to_hedging_ctx.is_some() {
        requeue_transfer_orphans(&schedulers.transfer_usdc_to_hedging, "Base->Alpaca").await?;
    }

    if usdc_to_market_making_ctx.is_some() {
        requeue_transfer_orphans(&schedulers.transfer_usdc_to_market_making, "Alpaca->Base")
            .await?;
    }

    if equity_to_market_making_ctx.is_some() {
        requeue_transfer_orphans(
            &schedulers.transfer_equity_to_market_making,
            "equity hedging->market-making",
        )
        .await?;
    }

    if equity_to_hedging_ctx.is_some() {
        requeue_transfer_orphans(
            &schedulers.transfer_equity_to_hedging,
            "equity market-making->hedging",
        )
        .await?;
    }

    Ok(())
}

async fn requeue_startup_orphans(
    schedulers: &RebalancingSchedulers,
    backfill_queue: &BackfillJobQueue,
    usdc_to_hedging_ctx: Option<&Arc<TransferUsdcToHedgingCtx>>,
    usdc_to_market_making_ctx: Option<&Arc<TransferUsdcToMarketMakingCtx>>,
    equity_to_market_making_ctx: Option<&Arc<TransferEquityToMarketMakingCtx>>,
    equity_to_hedging_ctx: Option<&Arc<TransferEquityToHedgingCtx>>,
) -> anyhow::Result<()> {
    requeue_wired_transfer_orphans(
        schedulers,
        usdc_to_hedging_ctx,
        usdc_to_market_making_ctx,
        equity_to_market_making_ctx,
        equity_to_hedging_ctx,
    )
    .await?;
    requeue_backfill_orphans(backfill_queue).await
}

async fn requeue_transfer_orphans<Task>(
    queue: &job::JobQueue<Task>,
    direction_label: &str,
) -> anyhow::Result<()>
where
    Task: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Unpin + 'static,
{
    let count = queue.requeue_orphaned().await.with_context(|| {
        format!("failed to re-queue orphaned {direction_label} transfer jobs at startup")
    })?;

    if count > 0 {
        info!(
            target: "rebalance",
            count,
            direction = direction_label,
            "Re-queued orphaned transfer job(s) for crash-safe resume",
        );
    }

    Ok(())
}

/// Resets recovery queue rows that were left in-flight by a previous process.
/// Recovery jobs are durable and crash-resumable, but only if the apalis row is
/// made runnable again before the deterministic worker id starts heartbeating.
async fn requeue_recovery_orphans<Task>(
    queue: &job::JobQueue<Task>,
    recovery_label: &str,
) -> anyhow::Result<()>
where
    Task: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Unpin + 'static,
{
    let count = queue.requeue_orphaned().await.with_context(|| {
        format!("failed to re-queue orphaned {recovery_label} recovery jobs at startup")
    })?;

    if count > 0 {
        info!(
            target: "rebalance",
            count,
            recovery = recovery_label,
            "Re-queued orphaned recovery job(s) for crash-safe resume",
        );
    }

    Ok(())
}

async fn requeue_backfill_orphans(backfill_queue: &BackfillJobQueue) -> anyhow::Result<()> {
    let count = backfill_queue
        .requeue_orphaned()
        .await
        .context("failed to re-queue orphaned backfill jobs at startup")?;

    if count > 0 {
        info!(
            target: "backfill",
            count,
            "Re-queued orphaned backfill range job(s) for crash-safe resume",
        );
    }

    Ok(())
}

async fn requeue_equity_recovery_orphans(
    wrapped_ctx: Option<&Arc<WrappedEquityRecoveryCtx>>,
    unwrapped_ctx: Option<&Arc<UnwrappedEquityRecoveryCtx>>,
    wrapped_queue: &WrappedEquityRecoveryJobQueue,
    unwrapped_queue: &UnwrappedEquityRecoveryJobQueue,
) -> anyhow::Result<()> {
    if wrapped_ctx.is_some() {
        requeue_recovery_orphans(wrapped_queue, "wrapped equity").await?;
    }
    if unwrapped_ctx.is_some() {
        requeue_recovery_orphans(unwrapped_queue, "unwrapped equity").await?;
    }
    Ok(())
}

/// Resets a trading-pipeline queue's orphaned `Running`/`Queued` rows back to
/// `Pending` at startup, before workers spawn. The trading queues cover the
/// fill-to-hedge path: trade accounting, hedge placement, status polling,
/// fill reconciliation, and rejection handling. A row wedged by a dead
/// process is unrecoverable otherwise (see `JobQueue::requeue_orphaned` for
/// why apalis never ages it out). Trade accounting is the critical case: its
/// job row is the only remaining record of an observed fill once the backfill
/// checkpoint has advanced past the fill's block, so a wedged row there is
/// permanently unhedged exposure that no rescan will ever surface again.
async fn requeue_trading_orphans<Task>(
    queue: &job::JobQueue<Task>,
    queue_label: &str,
) -> anyhow::Result<()>
where
    Task: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Unpin + 'static,
{
    let count = queue
        .requeue_orphaned()
        .await
        .with_context(|| format!("failed to re-queue orphaned {queue_label} jobs at startup"))?;

    if count > 0 {
        info!(
            target: "hedge",
            count,
            queue = queue_label,
            "Re-queued orphaned trading job(s) for crash-safe resume",
        );
    }

    Ok(())
}

impl Conductor {
    pub(crate) async fn run<E>(
        executor_ctx: impl TryIntoExecutor<Executor = E>,
        ctx: Ctx,
        pools: DatabasePools,
        event_sender: broadcast::Sender<Statement>,
        inventory: Arc<BroadcastingInventory>,
        shutdown_token: CancellationToken,
        recovery_cell: Arc<tokio::sync::OnceCell<crate::api::RecoveryHandle>>,
        #[cfg(any(test, feature = "test-support"))]
        failure_injector: crate::conductor::job::FailureInjector,
    ) -> anyhow::Result<()>
    where
        E: Executor + Clone + Send + 'static,
        TradeAccountingError: From<E::Error>,
        crate::offchain::order::JobError: From<E::Error>,
    {
        let DatabasePools {
            cqrs: pool,
            apalis: apalis_pool,
        } = pools;
        let executor = executor_ctx.try_into_executor().await?;

        // Single HTTP transport: drives continuous eth_getLogs fill polling
        // (via the OrderFillMonitor + backfill worker) and all read-only
        // contract calls. No WebSocket -- see `monitor::order_fills`.
        let provider = ProviderBuilder::new().connect_http(ctx.evm.rpc_url.clone());

        // The HTTP transport connects lazily, so probe it once at startup to
        // fail fast on a misconfigured or unreachable RPC rather than only
        // surfacing it as repeated poll-loop retries. systemd's bounded
        // restart loop handles a genuinely-down endpoint from here.
        provider
            .get_block_number()
            .await
            .context("failed to reach RPC endpoint at startup")?;
        let cache = SymbolCache::default();

        setup_apalis_tables(&apalis_pool).await?;
        let job_queue = DexTradeAccountingJobQueue::new(&apalis_pool);
        let backfill_queue = BackfillJobQueue::new(&apalis_pool);
        let schedulers = RebalancingSchedulers::new(&apalis_pool);

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await?;

        let (vault_registry, vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await?;

        let seed_vault_registry_queue = SeedVaultRegistryJobQueue::new(&apalis_pool);
        let seed_vault_registry_ctx = Arc::new(
            SeedVaultRegistryCtx::from_config(vault_registry.clone(), &ctx).map_err(|err| *err)?,
        );

        // Run the seeding logic inline at startup so downstream wiring
        // (RaindexService, trade accounting, inventory polling) starts
        // with a populated registry. The same code path is registered
        // below as an apalis worker so the queue retries on failure if
        // seeding is re-triggered later (e.g. from a recovery flow).
        crate::conductor::job::Job::perform(&SeedVaultRegistry, &seed_vault_registry_ctx).await?;

        // Grant one-time idempotent MAX approvals to the trusted spenders (our
        // ERC-4626 wrapper vaults and the Raindex orderbook) before any worker
        // or rebalancer runs, so wrap/deposit never reverts with
        // ERC20InsufficientAllowance. Fails fast -- the bot must not come up
        // healthy with these missing. Only runs when a wallet is configured;
        // without one the bot cannot submit on-chain transactions at all.
        grant_startup_token_approvals(&ctx).await?;

        let rebalancing = match ctx.rebalancing_ctx() {
            Ok(ctx) => Some(ctx.clone()),
            Err(CtxError::NotRebalancing) => None,
            Err(error) => return Err(error.into()),
        };

        let PositionAndRebalancing {
            position,
            position_projection,
            snapshot,
            wallet_polling,
            tokenizer,
            service: rebalancing_service,
            recovery_transfer,
            wrapped_equity_recovery_store,
            unwrapped_equity_recovery_store,
            mint_store,
            redemption_store,
            transfer_usdc_to_hedging_ctx,
            transfer_usdc_to_market_making_ctx,
            transfer_equity_to_market_making_ctx,
            transfer_equity_to_hedging_ctx,
        } = PositionAndRebalancing::setup(
            rebalancing,
            &ctx,
            &pool,
            inventory.clone(),
            event_sender,
            vault_registry.clone(),
            vault_registry_projection,
            schedulers.clone(),
        )
        .await?;

        requeue_startup_orphans(
            &schedulers,
            &backfill_queue,
            transfer_usdc_to_hedging_ctx.as_ref(),
            transfer_usdc_to_market_making_ctx.as_ref(),
            transfer_equity_to_market_making_ctx.as_ref(),
            transfer_equity_to_hedging_ctx.as_ref(),
        )
        .await?;

        // Hydrate the in-memory InventoryView from persisted snapshot
        // state so the runtime projection has the same data as the
        // database. Without this, the first post-restart poll may emit
        // no events (unchanged values are deduplicated), leaving the
        // view empty and potentially causing incorrect rebalancing.
        hydrate_inventory_from_snapshot(&pool, &inventory).await;
        if let Some(service) = &rebalancing_service {
            service.enqueue_recovery_for_current_wallet_balances().await;
        }

        let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer(executor.clone()));

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await?;

        recover_orphaned_pending_offchain_orders(&position, &position_projection, &offchain_order)
            .await?;

        let frameworks = CqrsFrameworks {
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            offchain_order_projection,
            vault_registry,
            snapshot,
        };

        let hedge_queue = crate::conductor::job::JobQueue::new(&apalis_pool);
        let mut poll_status_queue = PollOrderStatusJobQueue::new(&apalis_pool);
        let reconcile_queue = ReconcileOrderFillJobQueue::new(&apalis_pool);
        let rejection_queue = HandleOrderRejectionJobQueue::new(&apalis_pool);

        // A previous process may have died mid-job anywhere on the
        // fill-to-hedge path, leaving apalis `Running` rows no live worker
        // owns. Reset them before the monitor spawns, exactly like the
        // transfer/backfill/recovery queues above. Trade accounting is the
        // one that cannot be recovered any other way: once the backfill
        // checkpoint advances, its job row is the only record of the fill.
        requeue_trading_orphans(&job_queue, "trade accounting").await?;
        requeue_trading_orphans(&hedge_queue, "hedge placement").await?;
        requeue_trading_orphans(&poll_status_queue, "order status polling").await?;
        requeue_trading_orphans(&reconcile_queue, "order fill reconciliation").await?;
        requeue_trading_orphans(&rejection_queue, "order rejection handling").await?;

        let wrapped_equity_recovery_queue = WrappedEquityRecoveryJobQueue::new(&apalis_pool);
        let unwrapped_equity_recovery_queue = UnwrappedEquityRecoveryJobQueue::new(&apalis_pool);

        let wrapped_equity_recovery_ctx = build_wrapped_equity_recovery_ctx(
            wrapped_equity_recovery_store,
            rebalancing_service.clone(),
            mint_store.clone(),
            redemption_store.clone(),
            inventory.clone(),
            wrapped_equity_recovery_queue.clone(),
            Duration::from_secs(ctx.inventory_poll_interval),
        );

        let unwrapped_equity_recovery_ctx = build_unwrapped_equity_recovery_ctx(
            unwrapped_equity_recovery_store,
            rebalancing_service.clone(),
            mint_store,
            redemption_store,
            inventory.clone(),
            unwrapped_equity_recovery_queue.clone(),
            Duration::from_secs(ctx.inventory_poll_interval),
        );

        requeue_equity_recovery_orphans(
            wrapped_equity_recovery_ctx.as_ref(),
            unwrapped_equity_recovery_ctx.as_ref(),
            &wrapped_equity_recovery_queue,
            &unwrapped_equity_recovery_queue,
        )
        .await?;

        let check_positions_queue = CheckPositionsJobQueue::new(&apalis_pool);

        recover_submitted_offchain_orders(
            &frameworks.offchain_order_projection,
            &mut poll_status_queue,
            executor.to_supported_executor(),
        )
        .await?;

        bootstrap_check_positions(&apalis_pool, &check_positions_queue).await?;

        let job_cleanup = spawn_finished_job_cleanup(
            pool.clone(),
            apalis_pool.clone(),
            Duration::from_secs(ctx.apalis_finished_job_cleanup_interval_secs),
        );

        let conductor_ctx = builder::ConductorCtx {
            ctx: ctx.clone(),
            cache,
            provider,
            executor,
            execution_threshold: ctx.execution_threshold,
            frameworks,
            pool,
            wallet_polling,
            tokenizer,
            shutdown_token: shutdown_token.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector,
        };

        // Clone before the builder consumes it; the recovery handle needs the
        // rebalancing service to rebuild tracking during recheck recovery.
        let recovery_rebalancing_service = rebalancing_service.clone();

        let mut conductor = builder::spawn()
            .context(conductor_ctx)
            .job_queue(job_queue)
            .backfill_queue(backfill_queue)
            .hedge_queue(hedge_queue)
            .poll_status_queue(poll_status_queue)
            .reconcile_queue(reconcile_queue)
            .rejection_queue(rejection_queue)
            .check_positions_queue(check_positions_queue)
            .wrapped_equity_recovery_queue(wrapped_equity_recovery_queue)
            .maybe_wrapped_equity_recovery_ctx(wrapped_equity_recovery_ctx)
            .unwrapped_equity_recovery_queue(unwrapped_equity_recovery_queue)
            .maybe_unwrapped_equity_recovery_ctx(unwrapped_equity_recovery_ctx)
            .equity_check_scheduler(schedulers.equity)
            .usdc_check_scheduler(schedulers.usdc)
            .transfer_usdc_to_hedging_queue(schedulers.transfer_usdc_to_hedging)
            .maybe_transfer_usdc_to_hedging_ctx(transfer_usdc_to_hedging_ctx)
            .transfer_usdc_to_market_making_queue(schedulers.transfer_usdc_to_market_making)
            .maybe_transfer_usdc_to_market_making_ctx(transfer_usdc_to_market_making_ctx)
            .transfer_equity_to_market_making_queue(schedulers.transfer_equity_to_market_making)
            .maybe_transfer_equity_to_market_making_ctx(transfer_equity_to_market_making_ctx)
            .transfer_equity_to_hedging_queue(schedulers.transfer_equity_to_hedging)
            .maybe_transfer_equity_to_hedging_ctx(transfer_equity_to_hedging_ctx)
            .maybe_rebalancing_service(rebalancing_service)
            .seed_vault_registry_queue(seed_vault_registry_queue)
            .seed_vault_registry_ctx(seed_vault_registry_ctx)
            .job_cleanup(job_cleanup)
            .call();

        // Publish the recovery handle only after all startup work
        // (inventory hydration, orphan recovery, store builds) completes
        // so /transfers/resume returns 503 until the conductor is ready.
        if let (Some(transfer), Some(rebalancing_service)) =
            (recovery_transfer, recovery_rebalancing_service)
        {
            let _ = recovery_cell.set(crate::api::RecoveryHandle {
                transfer,
                rebalancing_service,
            });
        }

        info!("Conductor is running");
        let result = conductor.wait_for_completion().await;
        if result.is_err() {
            conductor.abort_all();
        }
        result
    }

    pub(crate) async fn wait_for_completion(&mut self) -> anyhow::Result<()> {
        let exit = tokio::select! {
            biased;
            () = self.shutdown_token.cancelled() => ConductorExit::GracefulShutdown,
            result = self.supervisor.wait() => ConductorExit::Supervisor(result),
            result = &mut self.monitor => ConductorExit::Monitor(result),
            result = &mut self.job_cleanup => ConductorExit::JobCleanup(result),
        };

        match exit {
            ConductorExit::GracefulShutdown => self.drain_gracefully().await,
            other => {
                other.handle()?;
                Ok(())
            }
        }
    }

    /// Two-phase graceful drain: stop producers, then wait for consumers.
    async fn drain_gracefully(&mut self) -> anyhow::Result<()> {
        // Phase 1: stop supervised tasks (OrderFillMonitor, PositionMonitor)
        // so they stop enqueuing new jobs.
        info!(target: "shutdown", "Phase 1: stopping producers (supervisor)");
        if let Err(error) = self.supervisor.shutdown() {
            error!(target: "shutdown", %error, "Failed to shutdown supervisor");
        }

        // Wait for the supervisor to fully exit so producers are actually
        // stopped before Phase 2 begins draining consumers.
        if let Err(error) = self.supervisor.wait().await {
            warn!(target: "shutdown", %error, "Supervisor exited with error during drain");
        }

        // Abort remaining non-critical background tasks. Equity and USDC
        // transfers run as apalis jobs, so their in-flight operations drain
        // with the rest of the queue in Phase 2.
        self.abort_background_tasks();

        // Phase 2: now that producers are stopped, signal apalis to drain
        // in-flight jobs. The apalis token is independent of the outer
        // shutdown token -- we cancel it explicitly after producers stop.
        info!(target: "shutdown", "Phase 2: draining in-flight jobs");
        self.apalis_shutdown_token.cancel();
        check_monitor_drain_result((&mut self.monitor).await)
    }

    /// Abort all conductor tasks (force shutdown fallback).
    pub(crate) fn abort_all(&self) {
        info!(target: "shutdown", "Aborting all conductor tasks");
        if let Err(error) = self.supervisor.shutdown() {
            error!(%error, "Failed to shutdown supervisor");
        }
        self.monitor.abort();
        self.abort_background_tasks();
    }

    fn abort_background_tasks(&self) {
        self.job_cleanup.abort();
    }
}

/// Builds the [`WrappedEquityRecoveryCtx`] when every dependency the recovery
/// job needs is present; returns `None` (recovery wiring absent) if any is not.
fn build_wrapped_equity_recovery_ctx(
    store: Option<Arc<Store<WrappedEquityRecovery>>>,
    service: Option<Arc<RebalancingService>>,
    mint_store: Option<Arc<Store<TokenizedEquityMint>>>,
    redemption_store: Option<Arc<Store<EquityRedemption>>>,
    inventory: Arc<BroadcastingInventory>,
    queue: WrappedEquityRecoveryJobQueue,
    reschedule_interval: Duration,
) -> Option<Arc<WrappedEquityRecoveryCtx>> {
    let (Some(store), Some(service), Some(mint_store), Some(redemption_store)) =
        (store, service, mint_store, redemption_store)
    else {
        return None;
    };

    Some(Arc::new(WrappedEquityRecoveryCtx {
        inventory,
        store,
        mint_store,
        redemption_store,
        equity_in_progress: service.equity_in_progress.clone(),
        queue,
        reschedule_interval,
    }))
}

/// Builds the [`UnwrappedEquityRecoveryCtx`] when every dependency the recovery
/// job needs is present; returns `None` (recovery wiring absent) if any is not.
fn build_unwrapped_equity_recovery_ctx(
    store: Option<Arc<Store<UnwrappedEquityRecovery>>>,
    service: Option<Arc<RebalancingService>>,
    mint_store: Option<Arc<Store<TokenizedEquityMint>>>,
    redemption_store: Option<Arc<Store<EquityRedemption>>>,
    inventory: Arc<BroadcastingInventory>,
    queue: UnwrappedEquityRecoveryJobQueue,
    reschedule_interval: Duration,
) -> Option<Arc<UnwrappedEquityRecoveryCtx>> {
    let (Some(store), Some(service), Some(mint_store), Some(redemption_store)) =
        (store, service, mint_store, redemption_store)
    else {
        return None;
    };

    Some(Arc::new(UnwrappedEquityRecoveryCtx {
        inventory,
        store,
        mint_store,
        redemption_store,
        equity_in_progress: service.equity_in_progress.clone(),
        queue,
        reschedule_interval,
    }))
}

fn base_wallet_equity_recovery_enabled(ctx: &Ctx, symbol: &Symbol) -> bool {
    ctx.is_trading_enabled(symbol)
        || ctx.is_rebalancing_enabled(symbol)
        || ctx.is_wrapped_equity_recovery_enabled(symbol)
}

fn base_wallet_unwrapped_equity_token_addresses(ctx: &Ctx) -> HashMap<Symbol, Address> {
    ctx.assets
        .equities
        .symbols
        .iter()
        .filter(|(symbol, _)| base_wallet_equity_recovery_enabled(ctx, symbol))
        .map(|(symbol, config)| (symbol.clone(), config.tokenized_equity))
        .collect()
}

fn base_wallet_wrapped_equity_token_addresses(ctx: &Ctx) -> HashMap<Symbol, Address> {
    ctx.assets
        .equities
        .symbols
        .iter()
        .filter(|(symbol, _)| base_wallet_equity_recovery_enabled(ctx, symbol))
        .map(|(symbol, config)| (symbol.clone(), config.tokenized_equity_derivative))
        .collect()
}

/// Grants one-time idempotent MAX ERC20 approvals to the trusted spenders at
/// startup: each configured equity's underlying -> wrapper vault and wrapped ->
/// orderbook, plus USDC -> orderbook. Resolves token addresses through a
/// [`WrapperService`] built from the equity config, and submits through the
/// base wallet so confirmations and nonce handling match every other on-chain
/// write.
///
/// Skips entirely when no wallet is configured -- a standalone bot without a
/// wallet never wraps or deposits, so it has no allowances to grant.
async fn grant_startup_token_approvals(ctx: &Ctx) -> anyhow::Result<()> {
    let base_wallet = match ctx.wallet() {
        Ok(wallet_ctx) => wallet_ctx.base_wallet().clone(),
        Err(CtxError::WalletNotConfigured) => {
            info!(
                target: "orderbook",
                "No wallet configured -- skipping startup token approvals"
            );
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    let wrapper = WrapperService::new(
        base_wallet.clone(),
        to_wrapped_equities(&ctx.assets.equities.symbols),
    );

    let symbols = ctx
        .assets
        .equities
        .symbols
        .keys()
        .filter(|symbol| ctx.is_trading_enabled(symbol) || ctx.is_rebalancing_enabled(symbol))
        .cloned();

    let targets = build_approval_targets(&wrapper, symbols, ctx.evm.orderbook, USDC_BASE)?;

    grant_startup_approvals(&base_wallet, &targets).await?;

    info!(
        target: "orderbook",
        target_count = targets.len(),
        "Startup token approvals ensured"
    );

    Ok(())
}

/// Context for vault discovery operations during trade processing.
pub(crate) struct VaultDiscoveryCtx<'a> {
    pub(crate) vault_registry: &'a Store<VaultRegistry>,
    pub(crate) orderbook: Address,
    pub(crate) order_owner: Address,
}

#[cfg(test)]
pub(crate) struct AccumulatedPositionExecutionCtx<'a> {
    pub(crate) position: &'a Store<Position>,
    pub(crate) position_projection: &'a Projection<Position>,
    pub(crate) offchain_order: &'a Arc<Store<OffchainOrder>>,
    pub(crate) counter_trade_submission_lock: &'a Mutex<()>,
    pub(crate) threshold: &'a ExecutionThreshold,
    pub(crate) assets: &'a AssetsConfig,
}

fn check_monitor_drain_result(
    join_result: Result<Result<(), MonitorTaskError>, JoinError>,
) -> anyhow::Result<()> {
    let monitor_result = match join_result {
        Ok(inner) => inner,
        Err(join_error) => {
            error!(%join_error, "Monitor task panicked during drain");
            return Err(
                anyhow::Error::from(join_error).context("Monitor task panicked during drain")
            );
        }
    };

    match monitor_result {
        Ok(()) => {
            info!("All jobs drained, shutdown complete");
            Ok(())
        }
        Err(error) => {
            error!(%error, "Monitor error during drain");
            Err(error.into())
        }
    }
}

fn spawn_finished_job_cleanup(
    pool: SqlitePool,
    apalis_pool: apalis_sqlite::SqlitePool,
    cleanup_interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            interval.tick().await;

            let cleanup_result = sqlx_apalis::query(
                "DELETE FROM Jobs \
                 WHERE status = ? \
                 OR status = ? \
                 OR (status = ? AND max_attempts <= attempts)",
            )
            .bind(Status::Done.to_string())
            .bind(Status::Killed.to_string())
            .bind(Status::Failed.to_string())
            .execute(&apalis_pool)
            .await
            .map(|outcome| outcome.rows_affected());

            match cleanup_result {
                Ok(deleted) => {
                    debug!(deleted, "Cleaned up finished job queue rows");
                }
                Err(error) => {
                    error!(%error, "Failed to clean up finished job queue rows");
                }
            }

            match compact_inventory_snapshot_events(&pool).await {
                Ok(deleted) => {
                    debug!(deleted, "Compacted inventory snapshot events");
                }
                Err(error) => {
                    error!(%error, "Failed to compact inventory snapshot events");
                }
            }
        }
    })
}

async fn compact_inventory_snapshot_events(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let deleted = compact_events::<InventorySnapshot>(pool).await?;
    if deleted > 0 {
        incremental_vacuum(pool, 100).await?;
    }
    Ok(deleted)
}

/// Replay persisted [`InventorySnapshot`] state into the in-memory
/// [`BroadcastingInventory`] so the runtime view starts warm.
async fn hydrate_inventory_from_snapshot(
    pool: &SqlitePool,
    inventory: &Arc<BroadcastingInventory>,
) {
    let ids = match load_all_ids::<InventorySnapshot>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(?error, "Failed to load InventorySnapshot IDs for hydration");
            return;
        }
    };

    for id in &ids {
        hydrate_single_snapshot(pool, inventory, id).await;
    }
}

async fn hydrate_single_snapshot(
    pool: &SqlitePool,
    inventory: &Arc<BroadcastingInventory>,
    id: &crate::inventory::snapshot::InventorySnapshotId,
) {
    let Ok(Some(snapshot)) = load_entity::<InventorySnapshot>(pool, id).await else {
        return;
    };

    let event_count = snapshot.hydrate_inventory(inventory).await;
    if event_count == 0 {
        return;
    }

    info!(%id, event_count, "Hydrated InventoryView from persisted snapshot");
}

struct RebalancingInfrastructure {
    position: Arc<Store<Position>>,
    position_projection: Arc<Projection<Position>>,
    snapshot: Arc<Store<InventorySnapshot>>,
    tokenizer: Arc<dyn Tokenizer>,
    service: Arc<RebalancingService>,
    recovery_transfer: Arc<CrossVenueEquityTransfer>,
    wrapped_equity_recovery_store: Arc<Store<WrappedEquityRecovery>>,
    unwrapped_equity_recovery_store: Arc<Store<UnwrappedEquityRecovery>>,
    mint_store: Arc<Store<TokenizedEquityMint>>,
    redemption_store: Arc<Store<EquityRedemption>>,
    transfer_usdc_to_hedging_ctx: Arc<TransferUsdcToHedgingCtx>,
    transfer_usdc_to_market_making_ctx: Arc<TransferUsdcToMarketMakingCtx>,
    transfer_equity_to_market_making_ctx: Arc<TransferEquityToMarketMakingCtx>,
    transfer_equity_to_hedging_ctx: Arc<TransferEquityToHedgingCtx>,
}

/// Shared infrastructure dependencies needed to spawn rebalancing.
struct RebalancingDeps {
    pool: SqlitePool,
    ctx: Ctx,
    inventory: Arc<BroadcastingInventory>,
    event_sender: broadcast::Sender<Statement>,
    vault_registry: Arc<Store<VaultRegistry>>,
    vault_registry_projection: Arc<Projection<VaultRegistry>>,
    schedulers: RebalancingSchedulers,
}

/// Position + rebalancing-adjacent infrastructure produced during conductor
/// startup. When rebalancing is disabled, the optional fields are `None` and
/// the position aggregate is built standalone; otherwise the rebalancing
/// setup owns position construction and surfaces its handles here.
struct PositionAndRebalancing {
    position: Arc<Store<Position>>,
    position_projection: Arc<Projection<Position>>,
    snapshot: Arc<Store<InventorySnapshot>>,
    wallet_polling: Option<crate::inventory::WalletPollingCtx>,
    tokenizer: Option<Arc<dyn Tokenizer>>,
    service: Option<Arc<RebalancingService>>,
    recovery_transfer: Option<Arc<CrossVenueEquityTransfer>>,
    wrapped_equity_recovery_store: Option<Arc<Store<WrappedEquityRecovery>>>,
    unwrapped_equity_recovery_store: Option<Arc<Store<UnwrappedEquityRecovery>>>,
    mint_store: Option<Arc<Store<TokenizedEquityMint>>>,
    redemption_store: Option<Arc<Store<EquityRedemption>>>,
    transfer_usdc_to_hedging_ctx: Option<Arc<TransferUsdcToHedgingCtx>>,
    transfer_usdc_to_market_making_ctx: Option<Arc<TransferUsdcToMarketMakingCtx>>,
    transfer_equity_to_market_making_ctx: Option<Arc<TransferEquityToMarketMakingCtx>>,
    transfer_equity_to_hedging_ctx: Option<Arc<TransferEquityToHedgingCtx>>,
}

impl PositionAndRebalancing {
    async fn setup(
        rebalancing: Option<RebalancingCtx>,
        ctx: &Ctx,
        pool: &SqlitePool,
        inventory: Arc<BroadcastingInventory>,
        event_sender: broadcast::Sender<Statement>,
        vault_registry: Arc<Store<VaultRegistry>>,
        vault_registry_projection: Arc<Projection<VaultRegistry>>,
        schedulers: RebalancingSchedulers,
    ) -> anyhow::Result<Self> {
        if let Some(rebalancing_ctx) = rebalancing {
            let wallet_ctx = ctx.wallet()?;
            let ethereum_wallet = wallet_ctx.ethereum_wallet().clone();
            let base_wallet = wallet_ctx.base_wallet().clone();

            let infra = spawn_rebalancing_infrastructure(
                rebalancing_ctx,
                ctx.redemption_wallet()?,
                ethereum_wallet.clone(),
                base_wallet.clone(),
                RebalancingDeps {
                    pool: pool.clone(),
                    ctx: ctx.clone(),
                    inventory: inventory.clone(),
                    event_sender,
                    vault_registry,
                    vault_registry_projection,
                    schedulers,
                },
            )
            .await?;

            let wallet_polling = crate::inventory::WalletPollingCtx {
                ethereum: Arc::new(ethereum_wallet),
                base: Arc::new(base_wallet),
                unwrapped_equity_token_addresses: base_wallet_unwrapped_equity_token_addresses(ctx),
                wrapped_equity_token_addresses: base_wallet_wrapped_equity_token_addresses(ctx),
            };

            Ok(Self {
                position: infra.position,
                position_projection: infra.position_projection,
                snapshot: infra.snapshot,
                wallet_polling: Some(wallet_polling),
                tokenizer: Some(infra.tokenizer),
                service: Some(infra.service),
                recovery_transfer: Some(infra.recovery_transfer),
                wrapped_equity_recovery_store: Some(infra.wrapped_equity_recovery_store),
                unwrapped_equity_recovery_store: Some(infra.unwrapped_equity_recovery_store),
                mint_store: Some(infra.mint_store),
                redemption_store: Some(infra.redemption_store),
                transfer_usdc_to_hedging_ctx: Some(infra.transfer_usdc_to_hedging_ctx),
                transfer_usdc_to_market_making_ctx: Some(infra.transfer_usdc_to_market_making_ctx),
                transfer_equity_to_market_making_ctx: Some(
                    infra.transfer_equity_to_market_making_ctx,
                ),
                transfer_equity_to_hedging_ctx: Some(infra.transfer_equity_to_hedging_ctx),
            })
        } else {
            let broadcaster = Arc::new(Broadcaster::new(event_sender, pool.clone()));
            let (position, position_projection) = build_position_cqrs(pool, broadcaster).await?;

            // Without the service, the projection is the only subscriber
            // keeping the dashboard view in sync.
            let inventory_projection = Arc::new(InventoryProjection::new(inventory));
            let snapshot = StoreBuilder::<InventorySnapshot>::new(pool.clone())
                .with(inventory_projection)
                .build(())
                .await?;

            Ok(Self {
                position,
                position_projection,
                snapshot,
                wallet_polling: None,
                tokenizer: None,
                service: None,
                recovery_transfer: None,
                wrapped_equity_recovery_store: None,
                unwrapped_equity_recovery_store: None,
                mint_store: None,
                redemption_store: None,
                transfer_usdc_to_hedging_ctx: None,
                transfer_usdc_to_market_making_ctx: None,
                transfer_equity_to_market_making_ctx: None,
                transfer_equity_to_hedging_ctx: None,
            })
        }
    }
}

fn spawn_rebalancing_infrastructure<Chain: Wallet + Clone>(
    rebalancing_ctx: RebalancingCtx,
    redemption_wallet: Address,
    ethereum_wallet: Chain,
    base_wallet: Chain,
    deps: RebalancingDeps,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = anyhow::Result<RebalancingInfrastructure>> + Send>,
> {
    Box::pin(async move {
        info!("Initializing rebalancing infrastructure");

        let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &deps.ctx.broker else {
            anyhow::bail!("rebalancing requires Alpaca Broker API configuration");
        };

        let market_maker_wallet = base_wallet.address();

        let vault_lookup: Arc<dyn VaultLookup> = Arc::new(VaultRegistryLookup::new(
            deps.vault_registry_projection,
            deps.ctx.evm.orderbook,
            market_maker_wallet,
        ));

        let raindex_service = Arc::new(RaindexService::new(
            base_wallet.clone(),
            deps.ctx.evm.orderbook,
            market_maker_wallet,
        ));

        let tokenization = Arc::new(AlpacaTokenizationService::new(
            alpaca_auth.base_url().to_string(),
            alpaca_auth.account_id,
            alpaca_auth.api_key.clone(),
            alpaca_auth.api_secret.clone(),
            base_wallet.clone(),
            Some(redemption_wallet),
        ));

        let tokenizer: Arc<dyn Tokenizer> = tokenization;

        let wrapper = Arc::new(WrapperService::new(
            base_wallet.clone(),
            to_wrapped_equities(&deps.ctx.assets.equities.symbols),
        ));

        let equity_transfer_services = EquityTransferServices {
            raindex: raindex_service.clone(),
            vault_lookup: vault_lookup.clone(),
            tokenizer: tokenizer.clone(),
            wrapper: wrapper.clone(),
        };

        let transfer_usdc_to_hedging_queue = deps.schedulers.transfer_usdc_to_hedging.clone();
        let transfer_usdc_to_market_making_queue =
            deps.schedulers.transfer_usdc_to_market_making.clone();

        let rebalancing_service = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                equity: rebalancing_ctx.equity,
                usdc: rebalancing_ctx.usdc,
                transfer_timeout: rebalancing_ctx.transfer_timeout,
                assets: deps.ctx.assets.clone(),
            },
            deps.vault_registry,
            deps.ctx.evm.orderbook,
            market_maker_wallet,
            deps.inventory.clone(),
            wrapper.clone(),
            deps.schedulers,
        ));

        let broadcaster = Arc::new(Broadcaster::new(deps.event_sender, deps.pool.clone()));
        let manifest = QueryManifest::new(rebalancing_service.clone(), broadcaster);

        let built = manifest
            .build(deps.pool.clone(), equity_transfer_services)
            .await?;

        rebalancing_service
            .recover_pending_offchain_order_symbols(&built.position_projection)
            .await?;

        rebalancing_service
            .set_stores(built.mint.clone(), built.redemption.clone())
            .await;

        let recovery_transfer = Arc::new(CrossVenueEquityTransfer::new(
            raindex_service.clone(),
            vault_lookup.clone(),
            tokenizer.clone(),
            wrapper.clone(),
            market_maker_wallet,
            built.mint.clone(),
            built.redemption.clone(),
        ));

        // Built outside `QueryManifest` because the recovery aggregate's
        // services include `recovery_transfer`, which depends on the
        // mint/redemption stores produced by the manifest above.
        let wrapped_equity_recovery_store =
            StoreBuilder::<WrappedEquityRecovery>::new(deps.pool.clone())
                .build(WrappedEquityRecoveryServices {
                    raindex: raindex_service.clone(),
                    vault_lookup: vault_lookup.clone(),
                    wrapper: wrapper.clone(),
                    transfer: recovery_transfer.clone(),
                })
                .await?;

        let unwrapped_equity_recovery_store =
            StoreBuilder::<UnwrappedEquityRecovery>::new(deps.pool.clone())
                .build(UnwrappedEquityRecoveryServices {
                    raindex: raindex_service.clone(),
                    vault_lookup: vault_lookup.clone(),
                    wrapper: wrapper.clone(),
                    transfer: recovery_transfer.clone(),
                    wallet: base_wallet.address(),
                })
                .await?;

        recover_interrupted_tokenization_aggregates(
            &deps.pool,
            &rebalancing_service,
            deps.inventory.as_ref(),
            &recovery_transfer,
            built.mint.clone(),
            built.redemption.clone(),
        )
        .await?;

        rebalancing_service
            .recover_usdc_guard(&deps.pool, &built.usdc)
            .await?;

        let alpaca_wallet = Arc::new(AlpacaWalletService::new(
            alpaca_auth.base_url().to_string(),
            alpaca_auth.account_id,
            alpaca_auth.api_key.clone(),
            alpaca_auth.api_secret.clone(),
        ));

        let services = RebalancerServices::new(
            rebalancing_ctx.clone(),
            alpaca_auth.clone(),
            Arc::clone(&alpaca_wallet),
            ethereum_wallet,
            base_wallet,
            raindex_service,
        )
        .await?;

        let usdc_vault_id = deps
            .ctx
            .assets
            .cash
            .as_ref()
            .and_then(|cash| cash.vault_ids.first().copied())
            .ok_or(CtxError::MissingCashVaultId)?;

        let mint_store = built.mint;
        let redemption_store = built.redemption;

        let usdc_handles = services.into_usdc_transfer_handles(
            market_maker_wallet,
            RaindexVaultId(usdc_vault_id),
            built.usdc,
        );

        let transfer_usdc_to_hedging_ctx = Arc::new(TransferUsdcToHedgingCtx {
            transfer: usdc_handles.resume_base_to_alpaca,
            timeout: rebalancing_ctx.transfer_attempt_timeout,
            job_queue: transfer_usdc_to_hedging_queue,
        });

        let transfer_usdc_to_market_making_ctx = Arc::new(TransferUsdcToMarketMakingCtx {
            transfer: usdc_handles.resume_alpaca_to_base,
            job_queue: transfer_usdc_to_market_making_queue,
        });

        let transfer_equity_to_market_making_ctx = Arc::new(TransferEquityToMarketMakingCtx {
            transfer: recovery_transfer.clone(),
        });

        let transfer_equity_to_hedging_ctx = Arc::new(TransferEquityToHedgingCtx {
            transfer: recovery_transfer.clone(),
        });

        Ok(RebalancingInfrastructure {
            position: built.position,
            position_projection: built.position_projection,
            snapshot: built.snapshot,
            tokenizer,
            service: rebalancing_service,
            recovery_transfer,
            wrapped_equity_recovery_store,
            unwrapped_equity_recovery_store,
            mint_store,
            redemption_store,
            transfer_usdc_to_hedging_ctx,
            transfer_usdc_to_market_making_ctx,
            transfer_equity_to_market_making_ctx,
            transfer_equity_to_hedging_ctx,
        })
    })
}

/// Recovers inflight state from event history at startup.
///
/// Redemptions that ended in `DetectionFailed` or `RedemptionRejected` have
/// tokens physically in Alpaca's wallet with no snapshot source. Setting their
/// inflight directly prevents the system from re-triggering operations for
/// tokens it no longer holds.
async fn recover_stuck_redemptions(
    pool: &SqlitePool,
    inventory: &BroadcastingInventory,
) -> anyhow::Result<()> {
    let stuck_redemptions = symbols_with_stuck_redemptions(pool).await?;

    if stuck_redemptions.is_empty() {
        return Ok(());
    }

    let mut view = inventory.write().await;
    for (symbol, quantity) in &stuck_redemptions {
        *view = view.clone().update_equity(
            symbol,
            Inventory::set_inflight(Venue::MarketMaking, *quantity),
            Utc::now(),
        )?;
    }
    drop(view);

    info!(
        stuck = ?stuck_redemptions,
        "Recovered inflight from event history"
    );

    Ok(())
}

async fn recover_interrupted_tokenization_aggregates(
    pool: &SqlitePool,
    rebalancing_service: &RebalancingService,
    inventory: &BroadcastingInventory,
    transfer: &CrossVenueEquityTransfer,
    mint_store: Arc<Store<TokenizedEquityMint>>,
    redemption_store: Arc<Store<EquityRedemption>>,
) -> anyhow::Result<()> {
    let interrupted_mints = interrupted_mint_ids(pool).await?;
    let interrupted_redemptions = interrupted_redemption_ids(pool).await?;

    for mint_id in &interrupted_mints {
        let Some(mint) = mint_store.load(mint_id).await? else {
            return Err(anyhow::anyhow!(
                "Interrupted mint aggregate {mint_id} missing from store"
            ));
        };

        rebalancing_service
            .recover_mint_state(mint_id, &mint)
            .await?;
    }

    for redemption_id in &interrupted_redemptions {
        let Some(redemption) = redemption_store.load(redemption_id).await? else {
            return Err(anyhow::anyhow!(
                "Interrupted redemption aggregate {redemption_id} missing from store"
            ));
        };

        rebalancing_service
            .recover_redemption_state(redemption_id, &redemption)
            .await?;
    }

    recover_stuck_redemptions(pool, inventory).await?;
    resume_interrupted_transfers(transfer, &interrupted_mints, &interrupted_redemptions).await;

    Ok(())
}

async fn resume_interrupted_transfers(
    transfer: &CrossVenueEquityTransfer,
    interrupted_mints: &[crate::tokenized_equity_mint::IssuerRequestId],
    interrupted_redemptions: &[crate::equity_redemption::RedemptionAggregateId],
) {
    let mut failed_mints = 0usize;
    let mut failed_redemptions = 0usize;

    for mint_id in interrupted_mints {
        failed_mints += usize::from(
            transfer
                .resume_mint(mint_id)
                .await
                .inspect_err(|error| {
                    error!(%mint_id, ?error, "Failed to resume mint -- skipping");
                })
                .is_err(),
        );
    }

    for redemption_id in interrupted_redemptions {
        failed_redemptions += usize::from(
            transfer
                .resume_redemption(redemption_id)
                .await
                .inspect_err(|error| {
                    error!(%redemption_id, ?error, "Failed to resume redemption -- skipping");
                })
                .is_err(),
        );
    }

    info!(
        mint_count = interrupted_mints.len(),
        redemption_count = interrupted_redemptions.len(),
        failed_mints,
        failed_redemptions,
        "Interrupted tokenization transfer resume completed"
    );
}

/// Recovers positions whose `pending_offchain_order_id` references an
/// offchain order that has already reached a terminal state (Filled or
/// Failed) but whose completion was never propagated back to the position
/// aggregate.
///
/// This can happen when `complete_offchain_order_fill` succeeds but the
/// subsequent `complete_position_order` call fails -- the two operations
/// are not atomic. Since the order poller only polls `Submitted` orders,
/// it will never retry the failed position update, leaving the position
/// permanently stuck.
async fn recover_orphaned_pending_offchain_orders(
    position: &Arc<Store<Position>>,
    position_projection: &Arc<Projection<Position>>,
    offchain_order: &Arc<Store<OffchainOrder>>,
) -> anyhow::Result<()> {
    let positions = position_projection.load_all().await?;

    let orphaned: Vec<_> = positions
        .into_iter()
        .filter_map(|(symbol, pos)| {
            pos.pending_offchain_order_id
                .map(|order_id| (symbol, order_id))
        })
        .collect();

    if orphaned.is_empty() {
        return Ok(());
    }

    info!(
        count = orphaned.len(),
        "Found positions with pending offchain orders, checking for orphans"
    );

    for (symbol, order_id) in orphaned {
        recover_single_orphaned_order(position, offchain_order, &symbol, order_id).await?;
    }

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
async fn recover_single_orphaned_order(
    position: &Arc<Store<Position>>,
    offchain_order: &Arc<Store<OffchainOrder>>,
    symbol: &Symbol,
    order_id: OffchainOrderId,
) -> anyhow::Result<()> {
    let Some(order) = offchain_order.load(&order_id).await? else {
        warn!(%symbol, %order_id, "Pending offchain order not found in store -- recovering");
        position
            .send(
                symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: order_id,
                    error: "pending offchain order has no offchain order aggregate".to_string(),
                },
            )
            .await?;
        info!(%symbol, %order_id, "Orphaned missing pending order recovered");
        return Ok(());
    };

    let command = match order {
        OffchainOrder::Filled {
            ref executor_order_id,
            price,
            filled_at,
            ..
        } => {
            info!(%symbol, %order_id, "Orphaned order already Filled -- recovering");
            PositionCommand::CompleteOffChainOrder {
                offchain_order_id: order_id,
                shares_filled: order.shares(),
                direction: order.direction(),
                executor_order_id: executor_order_id.clone(),
                price,
                broker_timestamp: filled_at,
            }
        }

        OffchainOrder::Failed { .. } => {
            info!(%symbol, %order_id, "Orphaned order already Failed -- recovering");
            PositionCommand::FailOffChainOrder {
                offchain_order_id: order_id,
                error: "recovered from orphaned state on startup".to_string(),
            }
        }

        OffchainOrder::Submitted { .. } | OffchainOrder::PartiallyFilled { .. } => {
            debug!(%symbol, %order_id, "Pending offchain order still in progress");
            return Ok(());
        }

        OffchainOrder::Pending { .. } => {
            warn!(%symbol, %order_id, "Offchain order still Pending -- may not have been submitted");
            return Ok(());
        }
    };

    position.send(symbol, command).await?;
    info!(%symbol, %order_id, "Orphaned pending order recovered");

    Ok(())
}

/// Constructs the position CQRS framework with its view query
/// (without rebalancing trigger). Used when rebalancing is disabled.
async fn build_position_cqrs(
    pool: &SqlitePool,
    broadcaster: Arc<Broadcaster>,
) -> anyhow::Result<(Arc<Store<Position>>, Arc<Projection<Position>>)> {
    Ok(StoreBuilder::<Position>::new(pool.clone())
        .with(broadcaster)
        .build(())
        .await?)
}

/// Discovers vaults from a trade and emits VaultRegistryCommands.
///
/// This function is called AFTER trade conversion succeeds, using the trade's
/// already-resolved symbol. It extracts vault information from the queued event
/// and registers vaults owned by the specified order_owner.
///
/// Vaults are classified as:
/// - USDC vault: token == USDC_BASE
/// - Equity vault: token matches the trade's symbol (via cache lookup)
pub(crate) async fn discover_vaults_for_trade(
    trade_event: &EmittedOnChain<RaindexTradeEvent>,
    trade: &OnchainTrade,
    context: &VaultDiscoveryCtx<'_>,
) -> Result<(), TradeAccountingError> {
    let tx_hash = trade_event.tx_hash;
    let base_symbol = trade.symbol.base();
    let expected_equity_token = trade.equity_token;

    let owned_vaults = match &trade_event.event {
        RaindexTradeEvent::ClearV3(clear_event) => extract_vaults_from_clear(clear_event),
        RaindexTradeEvent::TakeOrderV3(take_event) => extract_owned_vaults(
            &take_event.config.order,
            take_event.config.inputIOIndex,
            take_event.config.outputIOIndex,
        ),
    };

    let our_vaults = owned_vaults
        .into_iter()
        .filter(|vault| vault.owner == context.order_owner);

    let vault_registry_id = VaultRegistryId {
        orderbook: context.orderbook,
        owner: context.order_owner,
    };

    for owned_vault in our_vaults {
        let vault = owned_vault.vault;

        let command = if vault.token == USDC_BASE {
            VaultRegistryCommand::DiscoverUsdcVault {
                vault_id: vault.vault_id,
                discovered_in: tx_hash,
            }
        } else if vault.token == expected_equity_token {
            VaultRegistryCommand::DiscoverEquityVault {
                token: vault.token,
                vault_id: vault.vault_id,
                discovered_in: tx_hash,
                symbol: base_symbol.clone(),
            }
        } else {
            debug!(
                vault_id = %vault.vault_id,
                token = %vault.token,
                usdc = %USDC_BASE,
                expected_equity_token = %expected_equity_token,
                "Vault token does not match USDC or expected equity token, skipping"
            );
            continue;
        };

        context
            .vault_registry
            .send(&vault_registry_id, command)
            .await?;
    }

    Ok(())
}

/// Returns `true` if the witness was accepted, `false` if rejected.
async fn execute_witness_trade(
    onchain_trade: &Store<OnChainTrade>,
    trade: &OnchainTrade,
    block_number: u64,
) -> bool {
    let trade_id = OnChainTradeId {
        tx_hash: trade.tx_hash,
        log_index: trade.log_index,
    };

    let amount = trade.amount.inner();
    let price_usdc = trade.price.value();

    let Some(block_timestamp) = trade.block_timestamp else {
        warn!(
            tx_hash = ?trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            "Missing block_timestamp for OnChainTrade::Witness"
        );
        return false;
    };

    let command = OnChainTradeCommand::Witness {
        symbol: trade.symbol.base().clone(),
        amount,
        direction: trade.direction,
        price_usdc,
        block_number,
        block_timestamp,
    };

    match onchain_trade.send(&trade_id, command).await {
        Ok(()) => {
            debug!(
                tx_hash = ?trade.tx_hash,
                log_index = trade.log_index,
                symbol = %trade.symbol,
                "Successfully executed OnChainTrade::Witness command"
            );
            true
        }
        Err(error) => {
            warn!(
                tx_hash = ?trade.tx_hash,
                log_index = trade.log_index,
                symbol = %trade.symbol,
                "OnChainTrade::Witness rejected: {error}"
            );
            false
        }
    }
}

async fn execute_enrich_trade(onchain_trade: &Store<OnChainTrade>, trade: &OnchainTrade) {
    let (Some(gas_used), Some(effective_gas_price), Some(pyth_price)) = (
        trade.gas_used,
        trade.effective_gas_price,
        trade.pyth_price.clone(),
    ) else {
        warn!(
            tx_hash = ?trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            gas_used = ?trade.gas_used,
            effective_gas_price = ?trade.effective_gas_price,
            pyth_price = ?trade.pyth_price,
            "Cannot enrich trade: missing gas_used, effective_gas_price, or pyth_price"
        );
        return;
    };

    let trade_id = OnChainTradeId {
        tx_hash: trade.tx_hash,
        log_index: trade.log_index,
    };

    let command = OnChainTradeCommand::Enrich {
        gas_used,
        effective_gas_price,
        pyth_price,
    };

    match onchain_trade.send(&trade_id, command).await {
        Ok(()) => debug!(
            tx_hash = ?trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            "Successfully executed OnChainTrade::Enrich command"
        ),
        Err(error) => error!(
            tx_hash = ?trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            "Failed to execute OnChainTrade::Enrich command: {error}"
        ),
    }
}

async fn execute_acknowledge_fill(
    position: &Store<Position>,
    trade: &OnchainTrade,
    threshold: ExecutionThreshold,
) {
    let base_symbol = trade.symbol.base();

    let amount = trade.amount.inner();
    let price_usdc = trade.price.value();

    let Some(block_timestamp) = trade.block_timestamp else {
        warn!(
            tx_hash = ?trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            "Missing block_timestamp for Position::AcknowledgeOnChainFill"
        );
        return;
    };

    let command = PositionCommand::AcknowledgeOnChainFill {
        symbol: base_symbol.clone(),
        threshold,
        trade_id: TradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        },
        amount: FractionalShares::new(amount),
        direction: trade.direction,
        price_usdc,
        block_timestamp,
    };

    match position.send(base_symbol, command).await {
        Ok(()) => debug!(
            tx_hash = ?trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            "Successfully executed Position::AcknowledgeOnChainFill command"
        ),
        Err(error) => error!(
            tx_hash = ?trade.tx_hash,
            log_index = trade.log_index,
            symbol = %trade.symbol,
            "Failed to execute Position::AcknowledgeOnChainFill command: {error}"
        ),
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
pub(crate) async fn process_queued_trade<E: Executor>(
    executor: &E,
    trade_event: &EmittedOnChain<RaindexTradeEvent>,
    trade: OnchainTrade,
    cqrs: &TradeProcessingCqrs,
    asset_enabled: bool,
) -> Result<Option<OffchainOrderId>, TradeAccountingError>
where
    TradeAccountingError: From<E::Error>,
{
    let trade_id = OnChainTradeId {
        tx_hash: trade.tx_hash,
        log_index: trade.log_index,
    };

    if let Ok(Some(_)) = cqrs.onchain_trade.load(&trade_id).await {
        debug!(
            ?trade_id,
            symbol = %trade.symbol,
            "Trade already processed (duplicate event), skipping"
        );
        return Ok(None);
    }

    let witnessed =
        execute_witness_trade(&cqrs.onchain_trade, &trade, trade_event.block_number).await;

    if !witnessed {
        return Ok(None);
    }

    execute_enrich_trade(&cqrs.onchain_trade, &trade).await;

    execute_acknowledge_fill(&cqrs.position, &trade, cqrs.execution_threshold).await;

    let base_symbol = trade.symbol.base();

    let executor_type = executor.to_supported_executor();

    let Some(mut execution) = check_execution_readiness(
        executor,
        &cqrs.position_projection,
        base_symbol,
        executor_type,
        &cqrs.assets,
        asset_enabled,
    )
    .await?
    else {
        return Ok(None);
    };

    let _counter_trade_submission_guard = cqrs.counter_trade_submission_lock.lock().await;

    match preflight_counter_trade_submission(executor, &execution, None).await? {
        CounterTradeSubmissionCheck::Skipped => return Ok(None),
        CounterTradeSubmissionCheck::Allowed { reservation } => {
            clamp_shares_to_reservation(&mut execution, reservation.as_ref());
        }
    }

    place_offchain_order(&execution, cqrs).await
}

#[derive(Default)]
struct CounterTradeBatchBudget {
    reserved_buying_power_cents: i64,
    remaining_equity: HashMap<Symbol, FractionalShares>,
}

impl CounterTradeBatchBudget {
    fn check_reservation(
        &self,
        reservation: &CounterTradeReservation,
    ) -> Result<Option<CounterTradeSkipReason>, ExecutionError> {
        match reservation {
            CounterTradeReservation::Equity {
                required,
                available,
                symbol,
            } => {
                let remaining = self
                    .remaining_equity
                    .get(symbol)
                    .copied()
                    .unwrap_or(*available);

                if !remaining
                    .inner()
                    .gte(required.inner().inner())
                    .map_err(ExecutionError::from)?
                {
                    return Ok(Some(CounterTradeSkipReason::InsufficientEquity {
                        required: *required,
                        available: remaining,
                    }));
                }

                Ok(None)
            }
            CounterTradeReservation::BuyingPower {
                estimated_cost_cents,
                available_buying_power_cents,
            } => {
                let remaining = available_buying_power_cents
                    .checked_sub(self.reserved_buying_power_cents)
                    .ok_or(ExecutionError::BuyingPowerReservationOverflow {
                        current_reserved_cents: self.reserved_buying_power_cents,
                        additional_cents: *available_buying_power_cents,
                    })?;

                if remaining < *estimated_cost_cents {
                    return Ok(Some(CounterTradeSkipReason::InsufficientBuyingPower {
                        estimated_cost_cents: *estimated_cost_cents,
                        available_buying_power_cents: remaining,
                    }));
                }

                Ok(None)
            }
        }
    }
}

#[cfg(test)]
impl CounterTradeBatchBudget {
    fn reserve_buying_power(&mut self, estimated_cost_cents: i64) -> Result<(), ExecutionError> {
        self.reserved_buying_power_cents = self
            .reserved_buying_power_cents
            .checked_add(estimated_cost_cents)
            .ok_or(ExecutionError::BuyingPowerReservationOverflow {
                current_reserved_cents: self.reserved_buying_power_cents,
                additional_cents: estimated_cost_cents,
            })?;

        Ok(())
    }

    fn commit_reservation(
        &mut self,
        reservation: &CounterTradeReservation,
    ) -> Result<Option<CounterTradeSkipReason>, ExecutionError> {
        if let Some(reason) = self.check_reservation(reservation)? {
            return Ok(Some(reason));
        }

        match reservation {
            CounterTradeReservation::Equity {
                symbol,
                required,
                available,
            } => {
                let remaining = self
                    .remaining_equity
                    .entry(symbol.clone())
                    .or_insert(*available);
                *remaining = (*remaining - required.inner()).map_err(ExecutionError::from)?;
                Ok(None)
            }
            CounterTradeReservation::BuyingPower {
                estimated_cost_cents,
                ..
            } => {
                self.reserve_buying_power(*estimated_cost_cents)?;
                Ok(None)
            }
        }
    }
}

enum CounterTradeSubmissionCheck {
    Allowed {
        reservation: Option<CounterTradeReservation>,
    },
    Skipped,
}

/// When the preflight returns an equity reservation with fewer shares than
/// requested, clamp the execution context to the allowed amount. This enables
/// partial hedging: sell what's available instead of skipping entirely.
pub(crate) fn clamp_shares_to_reservation(
    execution: &mut ExecutionCtx,
    reservation: Option<&CounterTradeReservation>,
) {
    match reservation {
        Some(CounterTradeReservation::Equity { required, .. }) if *required < execution.shares => {
            info!(
                target: "hedge",
                symbol = %execution.symbol,
                requested = %execution.shares,
                allowed = %required,
                "Partial hedge: reducing order to available inventory"
            );
            execution.shares = *required;
        }

        // Equity with required == execution.shares: full inventory, no clamping needed.
        // BuyingPower: buy-side reservations control dollar amounts, not share counts.
        Some(
            CounterTradeReservation::Equity { .. } | CounterTradeReservation::BuyingPower { .. },
        )
        | None => {}
    }
}

fn log_counter_trade_skip(
    execution: &ExecutionCtx,
    source: &'static str,
    reason: &CounterTradeSkipReason,
) {
    match reason {
        CounterTradeSkipReason::InsufficientEquity {
            required,
            available,
        } => {
            warn!(
                symbol = %execution.symbol,
                shares = %execution.shares,
                direction = ?execution.direction,
                source,
                required_shares = %required,
                available_shares = %available,
                "Skipping counter trade before broker submission: insufficient offchain equity"
            );
        }
        CounterTradeSkipReason::InsufficientBuyingPower {
            estimated_cost_cents,
            available_buying_power_cents,
        } => {
            warn!(
                symbol = %execution.symbol,
                shares = %execution.shares,
                direction = ?execution.direction,
                source,
                estimated_cost_cents,
                available_buying_power_cents,
                "Skipping counter trade before broker submission: insufficient buying power"
            );
        }
    }
}

async fn preflight_counter_trade_submission<E: Executor>(
    executor: &E,
    execution: &ExecutionCtx,
    batch_budget: Option<&CounterTradeBatchBudget>,
) -> Result<CounterTradeSubmissionCheck, TradeAccountingError>
where
    TradeAccountingError: From<E::Error>,
{
    // Preflight does not submit; the client_order_id is irrelevant for the
    // check but the type requires it. Use a fresh value so callers cannot
    // accidentally reuse it as a real placement key.
    let order = MarketOrder {
        symbol: execution.symbol.clone(),
        shares: execution.shares,
        direction: execution.direction,
        client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
    };

    match executor.preflight_counter_trade(order).await? {
        CounterTradePreflight::Allowed { reservation } => {
            if let (Some(batch_budget), Some(reservation)) = (batch_budget, reservation.as_ref())
                && let Some(reason) = batch_budget.check_reservation(reservation)?
            {
                log_counter_trade_skip(execution, "reservation_budget", &reason);
                return Ok(CounterTradeSubmissionCheck::Skipped);
            }

            Ok(CounterTradeSubmissionCheck::Allowed { reservation })
        }
        CounterTradePreflight::Skipped(reason) => {
            log_counter_trade_skip(execution, "broker_preflight", &reason);
            Ok(CounterTradeSubmissionCheck::Skipped)
        }
    }
}

async fn place_offchain_order(
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, TradeAccountingError> {
    // Always mint a fresh OffchainOrderId for the new aggregate — the
    // prior aggregate may be in `Failed` state and cannot be revived. The
    // broker-side dedupe relies on `last_failed_offchain_order_id` being
    // reused as `client_order_id`, not on aggregate identity.
    let offchain_order_id = OffchainOrderId::new();

    if !execute_place_offchain_order(execution, cqrs, offchain_order_id).await {
        return Ok(None);
    }

    execute_create_offchain_order(execution, cqrs, offchain_order_id).await;

    let loaded = cqrs
        .offchain_order
        .load(&offchain_order_id)
        .await
        .inspect_err(|error| {
            error!(
                %offchain_order_id,
                symbol = %execution.symbol,
                %error,
                "Failed to load offchain order after Place; cannot determine post-broker state"
            );
        })?;

    dispatch_post_place_state(loaded, execution, cqrs, offchain_order_id).await
}

/// Routes the freshly-loaded `OffchainOrder` post-`Place` to either the
/// rejection-cleanup or poll-enqueue path. Returns the order id wrapped in
/// `Some` when the order is real and recorded, `None` when no usable order
/// exists (Place silently failed or persisted state is a lifecycle bug), or
/// propagates infrastructure errors so DB / queue failures cannot be
/// silently swallowed.
async fn dispatch_post_place_state(
    loaded: Option<OffchainOrder>,
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
    offchain_order_id: OffchainOrderId,
) -> Result<Option<OffchainOrderId>, TradeAccountingError> {
    use OffchainOrder::{Failed, Filled, PartiallyFilled, Pending, Submitted};
    match loaded {
        Some(Failed { error, .. }) => {
            cqrs.position
                .send(
                    &execution.symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error,
                    },
                )
                .await?;

            Ok(Some(offchain_order_id))
        }

        Some(Submitted { .. } | PartiallyFilled { .. }) => {
            let mut queue = cqrs.poll_status_queue.clone();

            queue
                .push(PollOrderStatus { offchain_order_id })
                .await
                .inspect_err(|error| {
                    error!(
                        %offchain_order_id,
                        symbol = %execution.symbol,
                        %error,
                        "Failed to enqueue PollOrderStatus job for newly-submitted order"
                    );
                })?;

            Ok(Some(offchain_order_id))
        }

        // `OffchainOrder::initialize` for Place always emits Placed plus
        // either Submitted or Failed atomically, so a persisted Pending or
        // Filled state directly after Place would be a lifecycle bug. None
        // means the prior `execute_create_offchain_order` swallowed a Place
        // failure -- report no order placed so the caller does not pretend
        // a phantom id is real, and clear the position pending state.
        None => {
            cqrs.position
                .send(
                    &execution.symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error: "Offchain order missing after Place".to_string(),
                    },
                )
                .await?;

            Ok(None)
        }

        Some(Pending { .. } | Filled { .. }) => {
            cqrs.position
                .send(
                    &execution.symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error: "Offchain order in unexpected state directly after Place"
                            .to_string(),
                    },
                )
                .await?;

            Ok(None)
        }
    }
}

/// Returns `true` if the Position aggregate accepted the order, `false` if it
/// was rejected (e.g. already has a pending execution).
#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
async fn execute_place_offchain_order(
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
    offchain_order_id: OffchainOrderId,
) -> bool {
    let command = PositionCommand::PlaceOffChainOrder {
        offchain_order_id,
        shares: execution.shares,
        direction: execution.direction,
        executor: execution.executor,
        threshold: cqrs.execution_threshold,
    };

    match cqrs.position.send(&execution.symbol, command).await {
        Ok(()) => {
            debug!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "Position::PlaceOffChainOrder succeeded"
            );
            true
        }
        Err(error) => {
            warn!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "Position::PlaceOffChainOrder rejected: {error}"
            );
            false
        }
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
async fn execute_create_offchain_order(
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
    offchain_order_id: OffchainOrderId,
) {
    // Derive the broker-side client_order_id from the live position aggregate
    // (read after PlaceOffChainOrder claimed it), reusing a prior failed
    // attempt's stashed OffchainOrderId as the idempotency anchor so the broker
    // dedupes the retry. Fall back to this attempt's id when there is no anchor.
    let anchor = match cqrs.position.load(&execution.symbol).await {
        Ok(position) => position.and_then(|position| position.last_failed_offchain_order_id),
        Err(error) => {
            warn!(
                %offchain_order_id,
                symbol = %execution.symbol,
                %error,
                "Failed to load position for the idempotency anchor; placing under a fresh key"
            );
            None
        }
    };
    let client_order_id_source = anchor.unwrap_or(offchain_order_id);
    let command = OffchainOrderCommand::Place {
        symbol: execution.symbol.clone(),
        shares: execution.shares,
        direction: execution.direction,
        executor: execution.executor,
        client_order_id: ClientOrderId::from_uuid(client_order_id_source.as_uuid()),
    };

    match cqrs.offchain_order.send(&offchain_order_id, command).await {
        Ok(()) => debug!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "OffchainOrder::Place succeeded"
        ),
        Err(error) => error!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "OffchainOrder::Place failed: {error}"
        ),
    }
}

#[cfg(test)]
#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
pub(crate) async fn check_and_execute_accumulated_positions<E>(
    executor: &E,
    execution_ctx: AccumulatedPositionExecutionCtx<'_>,
    is_trading_enabled: impl Fn(&Symbol) -> bool,
) -> Result<(), TradeAccountingError>
where
    E: Executor + Clone + Send + 'static,
    TradeAccountingError: From<E::Error>,
{
    let AccumulatedPositionExecutionCtx {
        position,
        position_projection,
        offchain_order,
        counter_trade_submission_lock,
        threshold,
        assets,
    } = execution_ctx;

    let _counter_trade_submission_guard = counter_trade_submission_lock.lock().await;
    let executor_type = executor.to_supported_executor();
    let ready_positions = check_all_positions(
        executor,
        position_projection,
        executor_type,
        assets,
        is_trading_enabled,
    )
    .await?;

    if ready_positions.is_empty() {
        debug!("No accumulated positions ready for execution");
        return Ok(());
    }

    info!(
        ready_positions = ready_positions.len(),
        "Found accumulated positions ready for execution"
    );

    let mut batch_budget = CounterTradeBatchBudget::default();

    for mut execution in ready_positions {
        let reservation =
            match preflight_counter_trade_submission(executor, &execution, Some(&batch_budget))
                .await?
            {
                CounterTradeSubmissionCheck::Allowed { reservation } => reservation,
                CounterTradeSubmissionCheck::Skipped => continue,
            };

        clamp_shares_to_reservation(&mut execution, reservation.as_ref());

        let offchain_order_id = OffchainOrderId::new();

        debug!(
            symbol = %execution.symbol,
            shares = %execution.shares,
            direction = ?execution.direction,
            %offchain_order_id,
            "Executing accumulated position"
        );

        let command = PositionCommand::PlaceOffChainOrder {
            offchain_order_id,
            shares: execution.shares,
            direction: execution.direction,
            executor: execution.executor,
            threshold: *threshold,
        };

        if let Err(error) = position.send(&execution.symbol, command).await {
            warn!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "Position::PlaceOffChainOrder rejected (likely pending execution), \
                 skipping OffchainOrder creation: {error}"
            );
            continue;
        }

        debug!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "Position::PlaceOffChainOrder succeeded"
        );

        // Derive the broker-side client_order_id from the live position aggregate,
        // reusing a prior failed attempt's stashed OffchainOrderId as the
        // idempotency anchor (mirroring `execute_create_offchain_order`) so this
        // path exercises the same broker dedupe the production reactor relies on.
        let anchor = match position.load(&execution.symbol).await {
            Ok(loaded) => loaded.and_then(|loaded| loaded.last_failed_offchain_order_id),
            Err(error) => {
                warn!(
                    %offchain_order_id,
                    symbol = %execution.symbol,
                    %error,
                    "Failed to load position for the idempotency anchor; placing under a fresh key"
                );
                None
            }
        };
        let client_order_id_source = anchor.unwrap_or(offchain_order_id);

        let command = OffchainOrderCommand::Place {
            symbol: execution.symbol.clone(),
            shares: execution.shares,
            direction: execution.direction,
            executor: execution.executor,
            client_order_id: ClientOrderId::from_uuid(client_order_id_source.as_uuid()),
        };

        let place_result = offchain_order.send(&offchain_order_id, command).await;

        match &place_result {
            Ok(()) => debug!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "OffchainOrder::Place succeeded"
            ),
            Err(error) => error!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "OffchainOrder::Place failed: {error}"
            ),
        }

        let mut broker_rejected_immediately = false;

        if let Ok(Some(OffchainOrder::Failed { error, .. })) =
            offchain_order.load(&offchain_order_id).await
        {
            broker_rejected_immediately = true;
            position
                .send(
                    &execution.symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error,
                    },
                )
                .await?;
        }

        if place_result.is_ok()
            && !broker_rejected_immediately
            && let Some(reservation) = reservation.as_ref()
            && let Some(reason) = batch_budget.commit_reservation(reservation)?
        {
            log_counter_trade_skip(&execution, "reservation_commit", &reason);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, TxHash, U256, address, bytes, fixed_bytes};
    use apalis::prelude::Status;
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use task_supervisor::SupervisorBuilder;
    use tokio::sync::broadcast;

    use st0x_config::{
        AssetsConfig, EquitiesConfig, EquityAssetConfig, ExecutionThreshold, OperationMode,
        create_test_ctx_with_order_owner,
    };
    use st0x_dto::Statement;
    use st0x_event_sorcery::{StoreBuilder, test_store};
    use st0x_execution::{
        Direction, EquityPosition, ExecutorOrderId, Inventory as ExecutionInventory, MarketOrder,
        MockExecutor, Positive, Symbol,
    };
    use st0x_finance::{Usd, Usdc};
    use st0x_float_macro::float;
    use st0x_wrapper::{MockWrapper, RATIO_ONE, UnderlyingPerWrapped};

    use super::*;
    use crate::bindings::IRaindexV6::{
        ClearConfigV2, ClearV3, EvaluableV4, IOV2, OrderV4, TakeOrderConfigV4, TakeOrderV3,
    };
    use crate::conductor::builder::CqrsFrameworks;
    use crate::inventory::view::Operator;
    use crate::inventory::{ImbalanceThreshold, Inventory, InventoryView, Venue};
    use crate::offchain::order::OrderPlacementResult;
    use crate::onchain::trade::OnchainTrade;
    use crate::rebalancing::equity::{TransferEquityToHedging, TransferEquityToMarketMaking};
    use crate::rebalancing::{RebalancingSchedulers, RebalancingService};
    use crate::test_utils::{
        OnchainTradeBuilder, get_test_log, get_test_order, rebalancing_enabled_equities,
        setup_test_db, setup_test_pools,
    };
    use crate::trading::onchain::inclusion::EmittedOnChain;
    use crate::trading::onchain::trade_accountant::AccountForDexTrade;
    use crate::unwrapped_equity_recovery::UnwrappedEquityRecoveryJob;
    use crate::unwrapped_equity_recovery::aggregate::UnwrappedEquityRecoveryId;

    fn one_to_one_ratio() -> UnderlyingPerWrapped {
        UnderlyingPerWrapped::new(RATIO_ONE).unwrap()
    }

    fn conductor_with_job_cleanup(job_cleanup: JoinHandle<()>) -> Conductor {
        let supervisor = SupervisorBuilder::default().build().run();
        let monitor = tokio::spawn(pending::<Result<(), MonitorTaskError>>());

        Conductor {
            supervisor,
            monitor,
            job_cleanup,
            shutdown_token: CancellationToken::new(),
            apalis_shutdown_token: CancellationToken::new(),
        }
    }

    fn test_broadcaster(pool: &SqlitePool) -> (Arc<Broadcaster>, broadcast::Receiver<Statement>) {
        let (sender, receiver) = broadcast::channel(16);
        (Arc::new(Broadcaster::new(sender, pool.clone())), receiver)
    }

    async fn insert_finished_job(apalis_pool: &apalis_sqlite::SqlitePool, id: &str) {
        sqlx_apalis::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority) \
             VALUES (?, ?, 'test', ?, 1, 25, 0, 0)",
        )
        .bind(vec![0_u8])
        .bind(id)
        .bind(Status::Done.to_string())
        .execute(apalis_pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn requeue_recovery_orphans_resets_unwrapped_recovery_rows() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let mut queue = UnwrappedEquityRecoveryJobQueue::new(&apalis_pool);
        let job_type = std::any::type_name::<UnwrappedEquityRecoveryJob>();

        queue
            .push(UnwrappedEquityRecoveryJob {
                symbol: Symbol::new("AAPL").unwrap(),
                recovery_id: UnwrappedEquityRecoveryId(uuid::Uuid::new_v4()),
            })
            .await
            .unwrap();

        sqlx_apalis::query("UPDATE Jobs SET status = ?, lock_at = 1 WHERE job_type = ?")
            .bind(Status::Running.to_string())
            .bind(job_type)
            .execute(&apalis_pool)
            .await
            .unwrap();

        requeue_recovery_orphans(&queue, "unwrapped equity")
            .await
            .unwrap();

        let (status, lock_by): (String, Option<String>) =
            sqlx_apalis::query_as("SELECT status, lock_by FROM Jobs WHERE job_type = ?")
                .bind(job_type)
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(status, Status::Pending.to_string());
        assert_eq!(lock_by, None);
    }

    /// A crash mid trade-accounting job leaves a `Running` row that apalis
    /// never rescues (the deterministic worker name keeps its heartbeat
    /// fresh), and the backfill checkpoint has already advanced past the
    /// fill -- the row is the only remaining record of the trade. The
    /// startup requeue must make it runnable again.
    #[tokio::test]
    async fn requeue_trading_orphans_resets_running_accounting_rows() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let queue = DexTradeAccountingJobQueue::new(&apalis_pool);
        let job_type = std::any::type_name::<AccountForDexTrade>();

        sqlx_apalis::query(
            "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority) \
             VALUES (?, ?, ?, ?, 1, 25, 0, 0)",
        )
        .bind(vec![0_u8])
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(job_type)
        .bind(Status::Running.to_string())
        .execute(&apalis_pool)
        .await
        .unwrap();

        requeue_trading_orphans(&queue, "trade accounting")
            .await
            .unwrap();

        let (status, lock_by): (String, Option<String>) =
            sqlx_apalis::query_as("SELECT status, lock_by FROM Jobs WHERE job_type = ?")
                .bind(job_type)
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(status, Status::Pending.to_string());
        assert_eq!(lock_by, None);
    }

    async fn job_count(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs")
            .fetch_one(apalis_pool)
            .await
            .unwrap()
    }

    async fn pending_job_count<Job>(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<Job>())
        .fetch_one(apalis_pool)
        .await
        .unwrap()
    }

    async fn wait_for_job_count(apalis_pool: &apalis_sqlite::SqlitePool, expected_count: i64) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let count = job_count(apalis_pool).await;
                if count == expected_count {
                    return;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn wait_for_completion_reports_unexpected_job_cleanup_exit() {
        let job_cleanup = tokio::spawn(async {});
        let mut conductor = conductor_with_job_cleanup(job_cleanup);

        let error = conductor.wait_for_completion().await.unwrap_err();
        conductor.abort_all();

        assert!(
            error
                .to_string()
                .contains("Job cleanup exited unexpectedly"),
            "expected unexpected job cleanup exit, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn wait_for_completion_reports_job_cleanup_failure() {
        let job_cleanup = tokio::spawn(async {
            panic!("job cleanup failure for test");
        });
        let mut conductor = conductor_with_job_cleanup(job_cleanup);

        let error = conductor.wait_for_completion().await.unwrap_err();
        conductor.abort_all();

        assert!(
            error.to_string().contains("Job cleanup failed"),
            "expected job cleanup failure context, got: {error:#}"
        );
        assert!(
            error.source().is_some(),
            "expected preserved JoinError source"
        );
    }

    #[tokio::test]
    async fn spawn_finished_job_cleanup_runs_until_aborted() {
        let (pool, apalis_pool) = setup_test_pools().await;
        insert_finished_job(&apalis_pool, "done").await;

        let handle =
            spawn_finished_job_cleanup(pool.clone(), apalis_pool.clone(), Duration::from_secs(60));

        wait_for_job_count(&apalis_pool, 0).await;
        handle.abort();
        let join_error = handle.await.unwrap_err();

        assert!(
            join_error.is_cancelled(),
            "expected cleanup task to be cancelled, got: {join_error}"
        );
    }

    #[test]
    fn base_wallet_unwrapped_equity_token_addresses_skips_disabled_assets() {
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        let spym = Symbol::new("SPYM").unwrap();
        let coin = Symbol::new("COIN").unwrap();
        let aapl_token = Address::random();
        let tsla_token = Address::random();
        let spym_token = Address::random();
        let coin_token = Address::random();

        let mut symbols = HashMap::new();
        symbols.insert(
            aapl.clone(),
            EquityAssetConfig {
                tokenized_equity: aapl_token,
                tokenized_equity_derivative: Address::random(),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            tsla.clone(),
            EquityAssetConfig {
                tokenized_equity: tsla_token,
                tokenized_equity_derivative: Address::random(),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            spym.clone(),
            EquityAssetConfig {
                tokenized_equity: spym_token,
                tokenized_equity_derivative: Address::random(),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            coin.clone(),
            EquityAssetConfig {
                tokenized_equity: coin_token,
                tokenized_equity_derivative: Address::random(),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let ctx = Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    symbols,
                    operational_limit: None,
                },
                cash: None,
            },
            ..create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ))
        };

        let actual = base_wallet_unwrapped_equity_token_addresses(&ctx);

        assert_eq!(
            actual.len(),
            3,
            "Trading-enabled, rebalancing-enabled, and recovery-enabled assets should be polled"
        );
        assert_eq!(actual.get(&aapl), Some(&aapl_token));
        assert_eq!(actual.get(&tsla), Some(&tsla_token));
        assert_eq!(actual.get(&coin), Some(&coin_token));
        assert!(
            !actual.contains_key(&spym),
            "Disabled assets should be excluded from Base-wallet unwrapped equity polling"
        );
    }

    #[test]
    fn base_wallet_wrapped_equity_token_addresses_skips_disabled_assets() {
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        let spym = Symbol::new("SPYM").unwrap();
        let coin = Symbol::new("COIN").unwrap();
        let aapl_wrapped_token = Address::random();
        let tsla_wrapped_token = Address::random();
        let spym_wrapped_token = Address::random();
        let coin_wrapped_token = Address::random();

        let mut symbols = HashMap::new();
        symbols.insert(
            aapl.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: aapl_wrapped_token,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            tsla.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: tsla_wrapped_token,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            spym.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: spym_wrapped_token,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            coin.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: coin_wrapped_token,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let ctx = Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    symbols,
                    operational_limit: None,
                },
                cash: None,
            },
            ..create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ))
        };

        let actual = base_wallet_wrapped_equity_token_addresses(&ctx);

        assert_eq!(
            actual.len(),
            3,
            "Trading-enabled, rebalancing-enabled, and recovery-enabled assets should be polled"
        );
        assert_eq!(actual.get(&aapl), Some(&aapl_wrapped_token));
        assert_eq!(actual.get(&tsla), Some(&tsla_wrapped_token));
        assert_eq!(actual.get(&coin), Some(&coin_wrapped_token));
        assert!(
            !actual.contains_key(&spym),
            "Disabled assets should be excluded from Base-wallet wrapped equity polling"
        );
    }

    const TEST_ORDERBOOK: Address = address!("0x1234567890123456789012345678901234567890");
    const ORDER_OWNER: Address = address!("0xdddddddddddddddddddddddddddddddddddddddd");
    const OTHER_OWNER: Address = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
    const TEST_EQUITY_TOKEN: Address = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    const TEST_VAULT_ID: B256 =
        fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

    fn create_order_with_usdc_and_equity_vaults(owner: Address) -> OrderV4 {
        OrderV4 {
            owner,
            evaluable: EvaluableV4 {
                interpreter: address!("0x2222222222222222222222222222222222222222"),
                store: address!("0x3333333333333333333333333333333333333333"),
                bytecode: bytes!("0x00"),
            },
            nonce: fixed_bytes!(
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ),
            validInputs: vec![
                IOV2 {
                    token: USDC_BASE,
                    vaultId: TEST_VAULT_ID,
                },
                IOV2 {
                    token: TEST_EQUITY_TOKEN,
                    vaultId: TEST_VAULT_ID,
                },
            ],
            validOutputs: vec![
                IOV2 {
                    token: USDC_BASE,
                    vaultId: TEST_VAULT_ID,
                },
                IOV2 {
                    token: TEST_EQUITY_TOKEN,
                    vaultId: TEST_VAULT_ID,
                },
            ],
        }
    }

    fn create_emitted_clear_event(
        alice: OrderV4,
        bob: OrderV4,
    ) -> EmittedOnChain<RaindexTradeEvent> {
        let clear_event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice,
            bob,
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        EmittedOnChain::from_log(
            RaindexTradeEvent::ClearV3(Box::new(clear_event)),
            &get_test_log(),
        )
        .unwrap()
    }

    fn create_emitted_take_event(order: OrderV4) -> EmittedOnChain<RaindexTradeEvent> {
        let take_event = TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: vec![],
            },
            input: B256::ZERO,
            output: B256::ZERO,
        };

        EmittedOnChain::from_log(
            RaindexTradeEvent::TakeOrderV3(Box::new(take_event)),
            &get_test_log(),
        )
        .unwrap()
    }

    async fn load_vault_registry(vault_registry: &Store<VaultRegistry>) -> Option<VaultRegistry> {
        let registry_id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: ORDER_OWNER,
        };

        vault_registry.load(&registry_id).await.unwrap()
    }

    fn create_test_trade(symbol: &str) -> OnchainTrade {
        let tokenized_symbol = format!("wt{symbol}");
        OnchainTradeBuilder::default()
            .with_symbol(&tokenized_symbol)
            .with_equity_token(TEST_EQUITY_TOKEN)
            .build()
    }

    fn test_trade_with_amount(amount: Float, log_index: u64) -> OnchainTrade {
        OnchainTradeBuilder::default()
            .with_symbol("wtAAPL")
            .with_equity_token(TEST_EQUITY_TOKEN)
            .with_amount(amount)
            .with_log_index(log_index)
            .build()
    }

    fn test_trade_with_amount_and_direction(
        amount: Float,
        log_index: u64,
        direction: Direction,
    ) -> OnchainTrade {
        let mut trade = test_trade_with_amount(amount, log_index);
        trade.direction = direction;
        trade
    }

    async fn acknowledge_fill(
        position: &Store<Position>,
        symbol: &str,
        amount: &str,
        direction: Direction,
        log_index: u64,
    ) {
        let symbol = Symbol::new(symbol).unwrap();

        position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: B256::ZERO,
                        log_index,
                    },
                    amount: FractionalShares::new(Float::parse(amount.to_string()).unwrap()),
                    direction,
                    price_usdc: float!(150),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
    }

    fn create_vault_discovery_context(
        vault_registry: &Store<VaultRegistry>,
    ) -> VaultDiscoveryCtx<'_> {
        VaultDiscoveryCtx {
            vault_registry,
            orderbook: TEST_ORDERBOOK,
            order_owner: ORDER_OWNER,
        }
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_discovers_usdc_vault() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_emitted_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let registry = load_vault_registry(&vault_registry)
            .await
            .expect("VaultRegistry should exist after discovery");

        assert!(
            !registry.usdc_vaults.is_empty(),
            "Expected USDC vault to be discovered"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_discovers_equity_vault() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_emitted_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let registry = load_vault_registry(&vault_registry)
            .await
            .expect("VaultRegistry should exist after discovery");

        assert!(
            !registry.equity_vaults.is_empty(),
            "Expected equity vault to be discovered"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_from_take_event() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());

        let order = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let queued_event = create_emitted_take_event(order);
        let trade = create_test_trade("MSFT");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed when cache is populated");

        let registry = load_vault_registry(&vault_registry)
            .await
            .expect("VaultRegistry should exist after discovery");

        assert!(
            !registry.usdc_vaults.is_empty(),
            "Expected USDC vault to be discovered from take order"
        );
        assert!(
            !registry.equity_vaults.is_empty(),
            "Expected equity vault to be discovered from take order"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_filters_non_owner_vaults() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_emitted_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed even when no vaults match");

        let registry = load_vault_registry(&vault_registry).await;

        assert!(
            registry.is_none(),
            "Expected no vault registry when vaults don't belong to order_owner"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_uses_correct_aggregate_id() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_emitted_clear_event(alice, bob);
        let trade = create_test_trade("AAPL");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed");

        let registry = load_vault_registry(&vault_registry).await;

        assert!(
            registry.is_some(),
            "VaultRegistry should exist at the expected aggregate ID (orderbook:owner)"
        );
    }

    #[tokio::test]
    async fn test_discover_vaults_for_trade_uses_trade_symbol_for_equity() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());

        let alice = create_order_with_usdc_and_equity_vaults(ORDER_OWNER);
        let bob = create_order_with_usdc_and_equity_vaults(OTHER_OWNER);
        let queued_event = create_emitted_clear_event(alice, bob);
        let trade = create_test_trade("GOOG");

        let context = create_vault_discovery_context(&vault_registry);
        discover_vaults_for_trade(&queued_event, &trade, &context)
            .await
            .expect("Should succeed");

        let registry = load_vault_registry(&vault_registry)
            .await
            .expect("VaultRegistry should exist after discovery");

        let goog_symbol = Symbol::new("GOOG").unwrap();
        let has_goog_vault = registry
            .equity_vaults
            .values()
            .flat_map(|vaults| vaults.values())
            .any(|vault| vault.symbol == goog_symbol);

        assert!(
            has_goog_vault,
            "Equity vault should use the trade's symbol (GOOG), got vaults: {:?}",
            registry
                .equity_vaults
                .values()
                .flat_map(|vaults| vaults.values())
                .map(|vault| &vault.symbol)
                .collect::<Vec<_>>()
        );
    }

    fn succeeding_order_placer() -> Arc<dyn OrderPlacer> {
        struct TestOrderPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for TestOrderPlacer {
            async fn place_market_order(
                &self,
                order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("TEST_BROKER_ORD"),
                    placed_shares: order.shares,
                })
            }
        }

        Arc::new(TestOrderPlacer)
    }

    async fn create_cqrs_frameworks_with_order_placer(
        pool: &SqlitePool,
        order_placer: Arc<dyn OrderPlacer>,
    ) -> (CqrsFrameworks, Arc<Projection<OffchainOrder>>) {
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .unwrap();

        let (vault_registry, _) = StoreBuilder::<VaultRegistry>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let snapshot = StoreBuilder::<InventorySnapshot>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        (
            CqrsFrameworks {
                onchain_trade,
                position,
                position_projection: position_projection.clone(),
                offchain_order,
                offchain_order_projection: offchain_order_projection.clone(),
                vault_registry,
                snapshot,
            },
            offchain_order_projection,
        )
    }

    fn trade_processing_cqrs_with_threshold(
        frameworks: &CqrsFrameworks,
        threshold: ExecutionThreshold,
        apalis_pool: &apalis_sqlite::SqlitePool,
    ) -> TradeProcessingCqrs {
        TradeProcessingCqrs {
            onchain_trade: frameworks.onchain_trade.clone(),
            position: frameworks.position.clone(),
            position_projection: frameworks.position_projection.clone(),
            offchain_order: frameworks.offchain_order.clone(),
            execution_threshold: threshold,
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            counter_trade_submission_lock: Arc::new(Mutex::new(())),
            poll_status_queue: PollOrderStatusJobQueue::new(apalis_pool),
        }
    }

    fn make_trade_event(log_index: u64) -> EmittedOnChain<RaindexTradeEvent> {
        let event = ClearV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            alice: get_test_order(),
            bob: get_test_order(),
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let mut log = get_test_log();
        log.log_index = Some(log_index);
        let mut hash_bytes = [0u8; 32];
        hash_bytes[31] = u8::try_from(log_index).unwrap_or(0);
        log.transaction_hash = Some(B256::from(hash_bytes));

        EmittedOnChain::from_log(RaindexTradeEvent::ClearV3(Box::new(event)), &log).unwrap()
    }

    #[tokio::test]
    async fn trade_below_threshold_does_not_place_order() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(10);
        let trade = test_trade_with_amount(float!(0.5), 10);

        let result =
            process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true).await;

        assert_eq!(
            result.unwrap(),
            None,
            "0.5 shares should not trigger execution with 1-share threshold"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.net.inner().eq(float!(0.5)).unwrap(),
            "Position net should reflect the accumulated trade"
        );
        assert!(
            position.pending_offchain_order_id.is_none(),
            "No offchain order should be pending"
        );
    }

    #[tokio::test]
    async fn trade_above_threshold_places_offchain_order() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(20);
        let trade = test_trade_with_amount(float!(1.5), 20);

        let result =
            process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true).await;

        let offchain_order_id = result
            .unwrap()
            .expect("1.5 shares should trigger execution with 1-share threshold");

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(position.net.inner().eq(float!(1.5)).unwrap());
        assert_eq!(
            position.pending_offchain_order_id,
            Some(offchain_order_id),
            "Position should track the pending offchain order"
        );

        let offchain_order = offchain_order_projection
            .load(&offchain_order_id)
            .await
            .expect("offchain order should not be in failed lifecycle state")
            .expect("offchain order view should exist");

        assert!(
            matches!(offchain_order, OffchainOrder::Submitted { .. }),
            "Offchain order should be Submitted after successful placement, got: {offchain_order:?}"
        );
    }

    #[tokio::test]
    async fn trade_above_threshold_skips_counter_trade_without_offchain_inventory() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(21);
        let trade = test_trade_with_amount_and_direction(float!(1.5), 21, Direction::Buy);
        let executor = MockExecutor::new().with_inventory(ExecutionInventory {
            positions: vec![],
            usd_balance_cents: 100_000,
            cash_buying_power_cents: Some(100_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        });

        let result = process_queued_trade(&executor, &trade_event, trade, &cqrs, true)
            .await
            .unwrap();

        assert_eq!(
            result, None,
            "Counter trade should be skipped when the broker cannot preflight a sell"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_none(),
            "Skipped counter trades must not leave the position pending"
        );
        assert!(
            offchain_order_projection
                .load_all()
                .await
                .unwrap()
                .is_empty(),
            "Skipped counter trades must not create offchain orders"
        );
    }

    #[tokio::test]
    async fn trade_above_threshold_places_partial_hedge_with_available_inventory() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(22);
        // Onchain buy of 5 shares -> position net = +5 -> hedge needs to sell 5
        let trade = test_trade_with_amount_and_direction(float!(5.0), 22, Direction::Buy);

        // Broker only has 3 AAPL shares available
        let executor = MockExecutor::new().with_inventory(ExecutionInventory {
            positions: vec![EquityPosition {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(float!(3.0)),
                market_value: None,
            }],
            usd_balance_cents: 100_000,
            cash_buying_power_cents: Some(100_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        });

        let offchain_order_id = process_queued_trade(&executor, &trade_event, trade, &cqrs, true)
            .await
            .unwrap()
            .expect("Should place a partial hedge order, not skip entirely");

        let offchain_order = offchain_order_projection
            .load(&offchain_order_id)
            .await
            .expect("offchain order should not be in failed lifecycle state")
            .expect("offchain order view should exist");

        match offchain_order {
            OffchainOrder::Submitted { shares, .. } => {
                assert_eq!(
                    shares,
                    Positive::new(FractionalShares::new(float!(3.0))).unwrap(),
                    "Order should be placed with available shares (3), not requested (5)"
                );
            }
            other => panic!("Expected Submitted with capped shares, got: {other:?}"),
        }

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position.pending_offchain_order_id,
            Some(offchain_order_id),
            "Position should track the pending partial hedge order"
        );
    }

    #[tokio::test]
    async fn trade_above_threshold_still_skips_with_zero_inventory() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(23);
        let trade = test_trade_with_amount_and_direction(float!(1.5), 23, Direction::Buy);

        // Broker has zero AAPL shares
        let executor = MockExecutor::new().with_inventory(ExecutionInventory {
            positions: vec![EquityPosition {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(float!(0)),
                market_value: None,
            }],
            usd_balance_cents: 100_000,
            cash_buying_power_cents: Some(100_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        });

        let result = process_queued_trade(&executor, &trade_event, trade, &cqrs, true)
            .await
            .unwrap();

        assert_eq!(
            result, None,
            "Counter trade should still be skipped when broker has zero shares"
        );

        assert!(
            offchain_order_projection
                .load_all()
                .await
                .unwrap()
                .is_empty(),
            "No offchain orders should be created when inventory is zero"
        );
    }

    #[tokio::test]
    async fn multiple_trades_accumulate_then_trigger() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event_1 = make_trade_event(30);
        let trade_1 = test_trade_with_amount(float!(0.5), 30);

        let result_1 =
            process_queued_trade(&MockExecutor::new(), &trade_event_1, trade_1, &cqrs, true).await;

        assert_eq!(
            result_1.unwrap(),
            None,
            "First trade of 0.5 shares should not trigger"
        );

        let trade_event_2 = make_trade_event(31);
        let trade_2 = test_trade_with_amount(float!(0.7), 31);

        let result_2 =
            process_queued_trade(&MockExecutor::new(), &trade_event_2, trade_2, &cqrs, true).await;

        assert!(
            result_2.unwrap().is_some(),
            "Accumulated 1.2 shares should trigger execution with 1-share threshold"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(position.net.inner().eq(float!(1.2)).unwrap());
        assert!(position.pending_offchain_order_id.is_some());
    }

    #[tokio::test]
    async fn pending_order_blocks_new_execution() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event_1 = make_trade_event(40);
        let trade_1 = test_trade_with_amount(float!(1.5), 40);

        let first_order_id =
            process_queued_trade(&MockExecutor::new(), &trade_event_1, trade_1, &cqrs, true)
                .await
                .unwrap()
                .expect("first trade should place an order");

        let trade_event_2 = make_trade_event(41);
        let trade_2 = test_trade_with_amount(float!(1.5), 41);

        let result_2 =
            process_queued_trade(&MockExecutor::new(), &trade_event_2, trade_2, &cqrs, true).await;

        assert_eq!(
            result_2.unwrap(),
            None,
            "Second trade should not trigger execution while first order is pending"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.net.inner().eq(float!(3.0)).unwrap(),
            "Both fills should be accumulated"
        );
        assert_eq!(
            position.pending_offchain_order_id,
            Some(first_order_id),
            "Only the first order should be pending"
        );
    }

    #[tokio::test]
    async fn periodic_checker_executes_after_order_completion() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        // Process first trade -> places order
        let trade_event_1 = make_trade_event(50);
        let trade_1 = test_trade_with_amount(float!(1.5), 50);

        let first_order_id =
            process_queued_trade(&MockExecutor::new(), &trade_event_1, trade_1, &cqrs, true)
                .await
                .unwrap()
                .expect("first trade should place an order");

        // Process second trade -> blocked by pending order
        let trade_event_2 = make_trade_event(51);
        let trade_2 = test_trade_with_amount(float!(1.5), 51);

        process_queued_trade(&MockExecutor::new(), &trade_event_2, trade_2, &cqrs, true)
            .await
            .unwrap();

        // Complete the first order via CQRS
        let symbol = Symbol::new("AAPL").unwrap();

        cqrs.position
            .send(
                &symbol,
                PositionCommand::CompleteOffChainOrder {
                    offchain_order_id: first_order_id,
                    shares_filled: Positive::new(FractionalShares::new(float!(1.5))).unwrap(),
                    direction: Direction::Sell,
                    executor_order_id: ExecutorOrderId::new("TEST_BROKER_ORD"),
                    price: Usd::new(float!(150)),
                    broker_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Verify position is unblocked
        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_none(),
            "Order should be cleared after completion"
        );

        // Run the periodic checker - it should find the remaining net and place a new order
        let executor = st0x_execution::MockExecutor::new();

        check_and_execute_accumulated_positions(
            &executor,
            AccumulatedPositionExecutionCtx {
                position: &cqrs.position,
                position_projection: &cqrs.position_projection,
                offchain_order: &cqrs.offchain_order,
                counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
                threshold: &cqrs.execution_threshold,
                assets: &cqrs.assets,
            },
            |_| true,
        )
        .await
        .unwrap();

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_some(),
            "Periodic checker should have placed a new order for remaining net position"
        );
        assert_ne!(
            position.pending_offchain_order_id.unwrap(),
            first_order_id,
            "New order should have a different ID than the completed one"
        );
    }

    #[tokio::test]
    async fn periodic_checker_skips_counter_trade_without_buying_power() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        acknowledge_fill(&cqrs.position, "AAPL", "1", Direction::Sell, 1).await;

        // cash_buying_power_cents deliberately differs from usd_balance_cents
        // to verify buying power is display-only and does not affect trade decisions.
        let executor = MockExecutor::new()
            .with_inventory(ExecutionInventory {
                positions: vec![],
                usd_balance_cents: 10_000,
                cash_buying_power_cents: Some(1_000_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            })
            .with_preflight_price(float!(100));

        check_and_execute_accumulated_positions(
            &executor,
            AccumulatedPositionExecutionCtx {
                position: &cqrs.position,
                position_projection: &cqrs.position_projection,
                offchain_order: &cqrs.offchain_order,
                counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
                threshold: &cqrs.execution_threshold,
                assets: &cqrs.assets,
            },
            |_| true,
        )
        .await
        .unwrap();

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_none(),
            "Skipped accumulated counter trades must not leave the position pending"
        );
        assert!(
            offchain_order_projection
                .load_all()
                .await
                .unwrap()
                .is_empty(),
            "Skipped accumulated counter trades must not create offchain orders"
        );
    }

    #[tokio::test]
    async fn periodic_checker_reserves_buying_power_across_batch() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        acknowledge_fill(&cqrs.position, "AAPL", "1", Direction::Sell, 1).await;
        acknowledge_fill(&cqrs.position, "MSFT", "1", Direction::Sell, 2).await;

        let executor = MockExecutor::new()
            .with_inventory(ExecutionInventory {
                positions: vec![EquityPosition {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: FractionalShares::new(float!(5)),
                    market_value: None,
                }],
                usd_balance_cents: 15_000,
                cash_buying_power_cents: Some(15_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            })
            .with_preflight_price(float!(100));

        check_and_execute_accumulated_positions(
            &executor,
            AccumulatedPositionExecutionCtx {
                position: &cqrs.position,
                position_projection: &cqrs.position_projection,
                offchain_order: &cqrs.offchain_order,
                counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
                threshold: &cqrs.execution_threshold,
                assets: &cqrs.assets,
            },
            |_| true,
        )
        .await
        .unwrap();

        let pending_positions = cqrs
            .position_projection
            .load_all()
            .await
            .unwrap()
            .into_iter()
            .map(|(_symbol, position)| position)
            .filter(|position| position.pending_offchain_order_id.is_some())
            .count();

        assert_eq!(
            pending_positions, 1,
            "Buying-power reservations should allow only one accumulated buy in the batch"
        );
        assert_eq!(
            offchain_order_projection.load_all().await.unwrap().len(),
            1,
            "Only one offchain order should be created when batch buying power is exhausted"
        );
    }

    #[tokio::test]
    async fn periodic_checker_places_partial_hedge_with_limited_inventory() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        // Onchain buy -> positive net -> hedge needs to sell
        acknowledge_fill(&cqrs.position, "AAPL", "5", Direction::Buy, 1).await;

        // Broker only holds 2 AAPL shares (less than the 5 needed)
        let executor = MockExecutor::new().with_inventory(ExecutionInventory {
            positions: vec![EquityPosition {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(float!(2.0)),
                market_value: None,
            }],
            usd_balance_cents: 100_000,
            cash_buying_power_cents: Some(100_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        });

        check_and_execute_accumulated_positions(
            &executor,
            AccumulatedPositionExecutionCtx {
                position: &cqrs.position,
                position_projection: &cqrs.position_projection,
                offchain_order: &cqrs.offchain_order,
                counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
                threshold: &cqrs.execution_threshold,
                assets: &cqrs.assets,
            },
            |_| true,
        )
        .await
        .unwrap();

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_some(),
            "Periodic checker should place a partial hedge order instead of skipping"
        );

        let offchain_order_id = position.pending_offchain_order_id.unwrap();

        let offchain_order = offchain_order_projection
            .load(&offchain_order_id)
            .await
            .expect("offchain order should not be in failed lifecycle state")
            .expect("offchain order view should exist");

        match offchain_order {
            OffchainOrder::Submitted { shares, .. } => {
                assert_eq!(
                    shares,
                    Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
                    "Batch order should be capped to available inventory (2), not requested (5)"
                );
            }
            other => panic!("Expected Submitted with capped shares, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn periodic_checker_reuses_buying_power_after_immediate_broker_rejection() {
        struct FailOnceOrderPlacer {
            attempts: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl OrderPlacer for FailOnceOrderPlacer {
            async fn place_market_order(
                &self,
                order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("Broker rejected first order".into());
                }

                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("TEST_BROKER_ORD"),
                    placed_shares: order.shares,
                })
            }
        }

        let (pool, apalis_pool) = setup_test_pools().await;
        let order_placer: Arc<dyn OrderPlacer> = Arc::new(FailOnceOrderPlacer {
            attempts: AtomicUsize::new(0),
        });
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, order_placer).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        acknowledge_fill(&cqrs.position, "AAPL", "1", Direction::Sell, 1).await;
        acknowledge_fill(&cqrs.position, "MSFT", "1", Direction::Sell, 2).await;

        let executor = MockExecutor::new()
            .with_inventory(ExecutionInventory {
                positions: vec![],
                usd_balance_cents: 15_000,
                cash_buying_power_cents: Some(15_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            })
            .with_preflight_price(float!(100));

        check_and_execute_accumulated_positions(
            &executor,
            AccumulatedPositionExecutionCtx {
                position: &cqrs.position,
                position_projection: &cqrs.position_projection,
                offchain_order: &cqrs.offchain_order,
                counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
                threshold: &cqrs.execution_threshold,
                assets: &cqrs.assets,
            },
            |_| true,
        )
        .await
        .unwrap();

        let pending_positions = cqrs
            .position_projection
            .load_all()
            .await
            .unwrap()
            .into_iter()
            .map(|(_symbol, position)| position)
            .filter(|position| position.pending_offchain_order_id.is_some())
            .count();

        assert_eq!(
            pending_positions, 1,
            "Buying power released after immediate rejection should let the next accumulated buy proceed"
        );
        assert_eq!(
            offchain_order_projection.load_all().await.unwrap().len(),
            2,
            "Both accumulated orders should be attempted when the first rejection releases its reservation"
        );
    }

    /// Builds an `InventoryView` one onchain share below the 20/80 imbalance used
    /// in mint assertions: the position fill in
    /// `position_events_reach_rebalancing_service` adds one onchain share before
    /// the trigger enqueues the mint job.
    ///
    /// With a 50% target +/- 20% deviation, 20% < 30% lower bound -> TooMuchOffchain.
    fn imbalanced_inventory(symbol: &Symbol) -> InventoryView {
        InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::ZERO,
            )
            .with_usdc(Usdc::new(float!(1000000)), Usdc::new(float!(1000000)))
            .update_equity(
                symbol,
                Inventory::available(
                    Venue::MarketMaking,
                    Operator::Add,
                    FractionalShares::new(float!(19)),
                ),
                chrono::Utc::now(),
            )
            .unwrap()
            .update_equity(
                symbol,
                Inventory::available(
                    Venue::Hedging,
                    Operator::Add,
                    FractionalShares::new(float!(80)),
                ),
                chrono::Utc::now(),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn position_events_reach_rebalancing_service() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let orderbook = address!("0x0000000000000000000000000000000000000001");
        let order_owner = address!("0x0000000000000000000000000000000000000002");
        let test_token = address!("0x1234567890123456789012345678901234567890");

        // Seed vault registry so the trigger can resolve the token address.
        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());
        vault_registry
            .send(
                &VaultRegistryId {
                    orderbook,
                    owner: order_owner,
                },
                VaultRegistryCommand::SeedEquityVaultFromConfig {
                    token: test_token,
                    vault_id: fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    symbol: symbol.clone(),
                },
            )
            .await
            .unwrap();

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            imbalanced_inventory(&symbol),
            event_sender,
        ));

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: Some(ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                }),
                transfer_timeout: Duration::from_secs(30 * 60),
                assets: AssetsConfig {
                    equities: rebalancing_enabled_equities(&["AAPL"]),
                    cash: None,
                },
            },
            vault_registry,
            orderbook,
            order_owner,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
        ));
        let reactor = trigger.clone();

        let (position_store, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();

        // Acknowledge a fill -> fires position events -> trigger should react.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 99,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        crate::rebalancing::drain_pending_jobs(&reactor)
            .await
            .unwrap();

        let job_payload: Vec<u8> = sqlx::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ? LIMIT 1",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .fetch_one(&pool)
        .await
        .expect("Expected a TransferEquityToMarketMaking job enqueued by the trigger");

        let job: TransferEquityToMarketMaking =
            serde_json::from_slice(&job_payload).expect("deserialize enqueued mint job");

        assert_eq!(job.symbol, symbol);
        assert_eq!(
            job.quantity,
            FractionalShares::new(float!(30)),
            "20 onchain / 80 offchain imbalance should mint 30 shares to reach target"
        );
        assert!(
            !job.issuer_request_id.0.is_nil(),
            "enqueue must assign a fresh issuer_request_id"
        );
    }

    #[tokio::test]
    async fn inventory_updated_after_fill_via_cqrs() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let orderbook = address!("0x0000000000000000000000000000000000000001");
        let order_owner = address!("0x0000000000000000000000000000000000000002");

        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        // Seed inventory with 50 offchain shares and USDC. CQRS will add 50 onchain.
        let initial_inventory = InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::ZERO,
            )
            .with_usdc(Usdc::new(float!(1000000)), Usdc::new(float!(1000000)))
            .update_equity(
                &symbol,
                Inventory::available(
                    Venue::Hedging,
                    Operator::Add,
                    FractionalShares::new(float!(50)),
                ),
                chrono::Utc::now(),
            )
            .unwrap();

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(initial_inventory, event_sender));

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                equity: threshold,
                usdc: Some(threshold),
                transfer_timeout: Duration::from_secs(30 * 60),
                assets: AssetsConfig {
                    equities: rebalancing_enabled_equities(&["AAPL"]),
                    cash: None,
                },
            },
            vault_registry,
            orderbook,
            order_owner,
            Arc::clone(&inventory),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
        ));
        let reactor = trigger.clone();

        let (position_store, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();

        // Add 50 onchain shares via CQRS -> trigger applies to inventory.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(50)),
                    direction: Direction::Buy,
                    price_usdc: float!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // 50 onchain / 50 offchain = 50% ratio, within 30%-70% bounds -> balanced.
        assert!(
            inventory
                .read()
                .await
                .check_equity_imbalance(&symbol, &threshold, &one_to_one_ratio())
                .unwrap()
                .is_none(),
            "50/50 inventory should be balanced (no imbalance detected)"
        );
    }

    #[tokio::test]
    async fn balanced_position_event_does_not_trigger_rebalancing() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let orderbook = address!("0x0000000000000000000000000000000000000001");
        let order_owner = address!("0x0000000000000000000000000000000000000002");

        // Start balanced: 50 onchain, 50 offchain.
        let initial_inventory = InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::ZERO,
            )
            .with_usdc(
                Usdc::new(Float::zero().unwrap()),
                Usdc::new(Float::zero().unwrap()),
            )
            .update_equity(
                &symbol,
                Inventory::available(
                    Venue::MarketMaking,
                    Operator::Add,
                    FractionalShares::new(float!(50)),
                ),
                chrono::Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(
                    Venue::Hedging,
                    Operator::Add,
                    FractionalShares::new(float!(50)),
                ),
                chrono::Utc::now(),
            )
            .unwrap()
            .update_usdc(
                Inventory::available(Venue::MarketMaking, Operator::Add, Usdc::new(float!(50))),
                chrono::Utc::now(),
            )
            .unwrap()
            .update_usdc(
                Inventory::available(Venue::Hedging, Operator::Add, Usdc::new(float!(50))),
                chrono::Utc::now(),
            )
            .unwrap();

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(initial_inventory, event_sender));

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: Some(ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                }),
                transfer_timeout: Duration::from_secs(30 * 60),
                assets: AssetsConfig {
                    equities: rebalancing_enabled_equities(&["AAPL"]),
                    cash: None,
                },
            },
            vault_registry,
            orderbook,
            order_owner,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
        ));
        let reactor = trigger.clone();

        let (position_store, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(reactor.clone())
            .build(())
            .await
            .unwrap();

        // Small onchain fill: 55/105 = 52.4%, within 30%-70%.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 99,
                    },
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        crate::rebalancing::drain_pending_jobs(&reactor)
            .await
            .unwrap();

        assert_eq!(
            pending_job_count::<TransferEquityToMarketMaking>(&apalis_pool).await,
            0,
            "Balanced position event should not enqueue a mint job"
        );
        assert_eq!(
            pending_job_count::<TransferEquityToHedging>(&apalis_pool).await,
            0,
            "Balanced position event should not enqueue a redemption job"
        );
    }

    /// Sets up an initialized position with 65% onchain / 35% offchain inventory
    /// (within the 30%-70% threshold bounds), a seeded vault registry, and the
    /// trigger wired into the position store via `build_position_cqrs`.
    async fn setup_near_upper_threshold_position() -> (
        Arc<Store<Position>>,
        SqlitePool,
        apalis_sqlite::SqlitePool,
        Symbol,
        Arc<RebalancingService>,
    ) {
        let (pool, apalis_pool) = setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let orderbook = address!("0x0000000000000000000000000000000000000001");
        let order_owner = address!("0x0000000000000000000000000000000000000002");
        let test_token = address!("0x1234567890123456789012345678901234567890");

        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());
        vault_registry
            .send(
                &VaultRegistryId {
                    orderbook,
                    owner: order_owner,
                },
                VaultRegistryCommand::SeedEquityVaultFromConfig {
                    token: test_token,
                    vault_id: fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    symbol: symbol.clone(),
                },
            )
            .await
            .unwrap();

        // 65 onchain, 35 offchain = 65% < 70% upper bound -> within bounds.
        let initial_inventory = InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::ZERO,
            )
            .with_usdc(Usdc::new(float!(1000000)), Usdc::new(float!(1000000)))
            .update_equity(
                &symbol,
                Inventory::available(
                    Venue::MarketMaking,
                    Operator::Add,
                    FractionalShares::new(float!(65)),
                ),
                chrono::Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(
                    Venue::Hedging,
                    Operator::Add,
                    FractionalShares::new(float!(35)),
                ),
                chrono::Utc::now(),
            )
            .unwrap();

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(initial_inventory, event_sender));

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingService::new(
            RebalancingServiceConfig {
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: Some(ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                }),
                transfer_timeout: Duration::from_secs(30 * 60),
                assets: AssetsConfig {
                    equities: rebalancing_enabled_equities(&["AAPL"]),
                    cash: None,
                },
            },
            vault_registry,
            orderbook,
            order_owner,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
        ));
        let reactor = trigger.clone();

        let (position_store, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();

        (position_store, pool, apalis_pool, symbol, reactor)
    }

    #[tokio::test]
    async fn position_event_causing_imbalance_triggers_redemption() {
        let (position_store, _pool, apalis_pool, symbol, reactor) =
            setup_near_upper_threshold_position().await;

        assert_eq!(
            pending_job_count::<TransferEquityToHedging>(&apalis_pool).await,
            0,
            "No commands executed yet, no redemption job should be enqueued"
        );

        // +20 onchain: 85 onchain, 35 offchain, total=120, target=60, excess=25.
        // 85/120 = 70.8% > 70% upper bound -> triggers Redemption.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 99,
                    },
                    amount: FractionalShares::new(float!(20)),
                    direction: Direction::Buy,
                    price_usdc: float!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        crate::rebalancing::drain_pending_jobs(&reactor)
            .await
            .unwrap();

        assert_eq!(
            pending_job_count::<TransferEquityToHedging>(&apalis_pool).await,
            1,
            "Expected a TransferEquityToHedging job enqueued by the trigger \
             on the imbalancing position event"
        );
    }

    #[tokio::test]
    async fn in_progress_guard_blocks_duplicate_trigger() {
        let (position_store, _pool, apalis_pool, symbol, reactor) =
            setup_near_upper_threshold_position().await;

        // First fill pushes over: 85/120 = 70.8% > 70% -> triggers Redemption(25).
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(20)),
                    direction: Direction::Buy,
                    price_usdc: float!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        crate::rebalancing::drain_pending_jobs(&reactor)
            .await
            .unwrap();

        assert_eq!(
            pending_job_count::<TransferEquityToHedging>(&apalis_pool).await,
            1,
            "First imbalancing fill should enqueue a redemption job"
        );

        // Second fill while in-progress NOT cleared -> no second trigger.
        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 2,
                    },
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        crate::rebalancing::drain_pending_jobs(&reactor)
            .await
            .unwrap();

        assert_eq!(
            pending_job_count::<TransferEquityToHedging>(&apalis_pool).await,
            1,
            "In-progress guard should block a duplicate redemption job"
        );
    }

    #[tokio::test]
    async fn restart_hydrates_position_view_from_persisted_events() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        // First "session": create position store, execute commands.
        {
            let (broadcaster, _receiver) = test_broadcaster(&pool);
            let (position_store, position_projection) =
                build_position_cqrs(&pool, broadcaster).await.unwrap();

            position_store
                .send(
                    &symbol,
                    PositionCommand::AcknowledgeOnChainFill {
                        symbol: symbol.clone(),
                        threshold: ExecutionThreshold::whole_share(),
                        trade_id: TradeId {
                            tx_hash: TxHash::random(),
                            log_index: 0,
                        },
                        amount: FractionalShares::new(float!(10)),
                        direction: Direction::Buy,
                        price_usdc: float!(150.0),
                        block_timestamp: chrono::Utc::now(),
                    },
                )
                .await
                .unwrap();

            let position = position_projection
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after first session");

            assert_eq!(position.net, FractionalShares::new(float!(10)));
            assert_eq!(position.accumulated_long, FractionalShares::new(float!(10)));
        }
        // position_store dropped here, simulating shutdown.

        // Second "session": create NEW position store against the same pool.
        {
            let (broadcaster, _receiver) = test_broadcaster(&pool);
            let (_position_store, position_projection) =
                build_position_cqrs(&pool, broadcaster).await.unwrap();

            let position = position_projection
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after restart");

            assert_eq!(
                position.net,
                FractionalShares::new(float!(10)),
                "Net position should survive restart via persisted view"
            );
            assert_eq!(
                position.accumulated_long,
                FractionalShares::new(float!(10)),
                "Accumulated long should survive restart via persisted view"
            );
        }
    }

    #[tokio::test]
    async fn standalone_position_cqrs_broadcasts_live_position_updates() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let (broadcaster, mut receiver) = test_broadcaster(&pool);
        let (position_store, _position_projection) =
            build_position_cqrs(&pool, broadcaster).await.unwrap();

        position_store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(3)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let message = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .expect("position command should broadcast dashboard update")
            .expect("position command should broadcast dashboard update");

        match message {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
                assert!(position.net.eq(float!(3)).unwrap());
                assert!(
                    position
                        .last_price_usdc
                        .expect("position update should include last price")
                        .eq(float!(150))
                        .unwrap()
                );
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn disabled_rebalancing_setup_broadcasts_live_position_updates() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let (event_sender, mut event_receiver) = broadcast::channel(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender.clone(),
        ));
        let (vault_registry, vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let position_and_rebalancing = PositionAndRebalancing::setup(
            None,
            &ctx,
            &pool,
            inventory,
            event_sender,
            vault_registry,
            vault_registry_projection,
            RebalancingSchedulers::new(&apalis_pool),
        )
        .await
        .unwrap();

        position_and_rebalancing
            .position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(3)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let message =
            tokio::time::timeout(std::time::Duration::from_secs(1), event_receiver.recv())
                .await
                .expect("position command should broadcast through setup event sender")
                .expect("position command should broadcast through setup event sender");

        match message {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
                assert!(position.net.eq(float!(3)).unwrap());
                assert!(
                    position
                        .last_price_usdc
                        .expect("position update should include last price")
                        .eq(float!(150))
                        .unwrap()
                );
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn broker_rejection_does_not_leave_position_stuck() {
        fn failing_order_placer() -> Arc<dyn OrderPlacer> {
            struct RejectingOrderPlacer;

            #[async_trait::async_trait]
            impl OrderPlacer for RejectingOrderPlacer {
                async fn place_market_order(
                    &self,
                    _order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Err("API error (403 Forbidden): trade denied due to pattern day trading protection".into())
                }
            }

            Arc::new(RejectingOrderPlacer)
        }

        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, failing_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(70);
        let trade = test_trade_with_amount(float!(1.5), 70);

        process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true)
            .await
            .unwrap();

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        // The broker rejected the order. The position must NOT be stuck with a
        // phantom pending order that will never complete.
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position should not have a pending order after broker rejection, \
             but got {:?}. This leaves the position permanently stuck.",
            position.pending_offchain_order_id
        );

        // After clearing the stuck state, the periodic position checker should
        // find the position still has a net imbalance and place a new order.
        // Rebuild CQRS with a broker that accepts orders (simulating the
        // transient PDT restriction being lifted).
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let executor = st0x_execution::MockExecutor::new();

        check_and_execute_accumulated_positions(
            &executor,
            AccumulatedPositionExecutionCtx {
                position: &cqrs.position,
                position_projection: &cqrs.position_projection,
                offchain_order: &cqrs.offchain_order,
                counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
                threshold: &cqrs.execution_threshold,
                assets: &cqrs.assets,
            },
            |_| true,
        )
        .await
        .unwrap();

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");

        assert!(
            position.pending_offchain_order_id.is_some(),
            "Periodic checker should retry execution after broker rejection is cleared"
        );
    }

    #[tokio::test]
    async fn periodic_checker_clears_position_on_broker_rejection() {
        fn rejecting_order_placer() -> Arc<dyn OrderPlacer> {
            struct RejectingOrderPlacer;

            #[async_trait::async_trait]
            impl OrderPlacer for RejectingOrderPlacer {
                async fn place_market_order(
                    &self,
                    _order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Err("Broker rejected: insufficient buying power".into())
                }
            }

            Arc::new(RejectingOrderPlacer)
        }

        let (pool, apalis_pool) = setup_test_pools().await;

        // Build CQRS with a rejecting broker
        let (frameworks, _) =
            create_cqrs_frameworks_with_order_placer(&pool, rejecting_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        // Accumulate a position by acknowledging an onchain fill
        let symbol = Symbol::new("AAPL").unwrap();
        cqrs.position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: B256::ZERO,
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Verify position is ready (has net shares, no pending order)
        let position = cqrs
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert!(position.pending_offchain_order_id.is_none());

        // Run periodic checker - broker will reject the order
        let executor = MockExecutor::new();
        check_and_execute_accumulated_positions(
            &executor,
            AccumulatedPositionExecutionCtx {
                position: &cqrs.position,
                position_projection: &cqrs.position_projection,
                offchain_order: &cqrs.offchain_order,
                counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
                threshold: &cqrs.execution_threshold,
                assets: &cqrs.assets,
            },
            |_| true,
        )
        .await
        .unwrap();

        // Position must not be stuck with a phantom pending order
        let position = cqrs
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position.pending_offchain_order_id, None,
            "Periodic checker should clear pending order after broker rejection"
        );
    }

    #[tokio::test]
    async fn enrichment_fields_present_emits_enrich_event() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(50);

        let pyth_price = crate::onchain_trade::PythPrice {
            value: "150250000".to_string(),
            expo: -6,
            conf: "50000".to_string(),
            publish_time: chrono::Utc::now(),
        };

        let trade = OnchainTradeBuilder::default()
            .with_symbol("wtAAPL")
            .with_equity_token(TEST_EQUITY_TOKEN)
            .with_amount(float!("1.5"))
            .with_log_index(50)
            .with_enrichment(50000, 1_000_000_000, pyth_price)
            .build();

        process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true)
            .await
            .unwrap();

        let trade_id = OnChainTradeId {
            tx_hash: fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 50,
        };

        let onchain_trade = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("OnChainTrade should exist after processing");

        assert!(
            onchain_trade.enrichment.is_some(),
            "Expected enrichment to be present when enrichment fields were provided"
        );
    }

    #[tokio::test]
    async fn missing_enrichment_fields_skips_enrich_event() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(60);

        let trade = test_trade_with_amount(float!("1.5"), 60);

        process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true)
            .await
            .unwrap();

        let trade_id = OnChainTradeId {
            tx_hash: fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 60,
        };

        let onchain_trade = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("OnChainTrade should exist after processing");

        assert!(
            onchain_trade.enrichment.is_none(),
            "Should not have enrichment when enrichment fields were None"
        );
    }

    #[tokio::test]
    async fn recover_orphaned_pending_clears_filled_order() {
        let pool = setup_test_db().await;
        let order_placer = crate::offchain::order::noop_order_placer();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .unwrap();

        let symbol = Symbol::new("RKLB").unwrap();
        let threshold = ExecutionThreshold::whole_share();
        let offchain_order_id = OffchainOrderId::new();
        let shares = Positive::new(st0x_execution::FractionalShares::new(float!(0.5))).unwrap();

        // Step 1: Initialize position via an onchain fill
        position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        tx_hash: fixed_bytes!(
                            "0000000000000000000000000000000000000000000000000000000000000001"
                        ),
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(78),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        // Step 2: Place an offchain order on the position (sets pending)
        position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    threshold,
                },
            )
            .await
            .unwrap();

        // Step 3: Create the offchain order aggregate (Place -> Submitted -> Filled)
        offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                },
            )
            .await
            .unwrap();

        offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(78)),
                },
            )
            .await
            .unwrap();

        // Verify position is stuck: pending_offchain_order_id is set
        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(pos.pending_offchain_order_id, Some(offchain_order_id));

        // Step 4: Run recovery
        recover_orphaned_pending_offchain_orders(&position, &position_projection, &offchain_order)
            .await
            .unwrap();

        // Verify recovery cleared the pending order
        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            pos.pending_offchain_order_id, None,
            "Recovery should clear pending_offchain_order_id for filled orders"
        );
    }

    #[tokio::test]
    async fn recover_orphaned_pending_clears_missing_order_aggregate() {
        let pool = setup_test_db().await;
        let order_placer = crate::offchain::order::noop_order_placer();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .unwrap();

        let symbol = Symbol::new("MSTR").unwrap();
        let threshold = ExecutionThreshold::whole_share();
        let offchain_order_id = OffchainOrderId::new();
        let shares = Positive::new(st0x_execution::FractionalShares::new(float!(0.5))).unwrap();

        position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        tx_hash: fixed_bytes!(
                            "0000000000000000000000000000000000000000000000000000000000000004"
                        ),
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(420),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    threshold,
                },
            )
            .await
            .unwrap();

        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(pos.pending_offchain_order_id, Some(offchain_order_id));

        recover_orphaned_pending_offchain_orders(&position, &position_projection, &offchain_order)
            .await
            .unwrap();

        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            pos.pending_offchain_order_id, None,
            "Recovery should clear a pending order id when the offchain order aggregate is missing"
        );
    }

    #[tokio::test]
    async fn recover_orphaned_pending_skips_submitted_order() {
        let pool = setup_test_db().await;

        struct BlockingPlacer;

        #[async_trait::async_trait]
        impl crate::offchain::order::OrderPlacer for BlockingPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-submitted"),
                    placed_shares: Positive::new(st0x_execution::FractionalShares::new(float!(
                        0.5
                    )))
                    .unwrap(),
                })
            }
        }

        let order_placer: Arc<dyn crate::offchain::order::OrderPlacer> = Arc::new(BlockingPlacer);

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .unwrap();

        let symbol = Symbol::new("SGOV").unwrap();
        let threshold = ExecutionThreshold::whole_share();
        let offchain_order_id = OffchainOrderId::new();
        let shares = Positive::new(st0x_execution::FractionalShares::new(float!(0.5))).unwrap();

        // Initialize position
        position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        tx_hash: fixed_bytes!(
                            "0000000000000000000000000000000000000000000000000000000000000002"
                        ),
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(100),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        // Place offchain order on position
        position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    threshold,
                },
            )
            .await
            .unwrap();

        // Create offchain order in Submitted state (Place triggers submission)
        offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                },
            )
            .await
            .unwrap();

        // Verify: order is Submitted, position has pending
        let order = offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(order, OffchainOrder::Submitted { .. }));

        // Run recovery
        recover_orphaned_pending_offchain_orders(&position, &position_projection, &offchain_order)
            .await
            .unwrap();

        // Verify: pending_offchain_order_id is NOT cleared
        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            pos.pending_offchain_order_id,
            Some(offchain_order_id),
            "Recovery must not clear pending order that is still Submitted"
        );
    }

    #[tokio::test]
    async fn recover_orphaned_pending_clears_failed_order() {
        let pool = setup_test_db().await;
        let order_placer = crate::offchain::order::noop_order_placer();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .unwrap();

        let symbol = Symbol::new("TSLA").unwrap();
        let threshold = ExecutionThreshold::whole_share();
        let offchain_order_id = OffchainOrderId::new();
        let shares = Positive::new(st0x_execution::FractionalShares::new(float!(0.5))).unwrap();

        // Initialize position
        position
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        tx_hash: fixed_bytes!(
                            "0000000000000000000000000000000000000000000000000000000000000003"
                        ),
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(400),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        // Place offchain order on position
        position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    threshold,
                },
            )
            .await
            .unwrap();

        // Create offchain order that transitions to Failed
        offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                },
            )
            .await
            .unwrap();

        offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkFailed {
                    error: "insufficient qty".to_string(),
                },
            )
            .await
            .unwrap();

        // Verify position is stuck
        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(pos.pending_offchain_order_id, Some(offchain_order_id));

        // Run recovery
        recover_orphaned_pending_offchain_orders(&position, &position_projection, &offchain_order)
            .await
            .unwrap();

        // Verify recovery cleared the pending order
        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            pos.pending_offchain_order_id, None,
            "Recovery should clear pending_offchain_order_id for failed orders"
        );
    }

    /// Drives a Position into the `pending_offchain_order_id = Some(_)` state
    /// for testing post-Place dispatch logic. Returns the offchain order id
    /// that the position is expecting.
    async fn drive_position_to_pending(
        frameworks: &CqrsFrameworks,
        symbol: &Symbol,
        shares: Positive<FractionalShares>,
    ) -> OffchainOrderId {
        let onchain = OnchainTradeBuilder::new()
            .with_symbol(&format!("wt{symbol}"))
            .with_amount(shares.inner().inner())
            .build();
        frameworks
            .position
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: onchain.tx_hash,
                        log_index: onchain.log_index,
                    },
                    amount: onchain.amount,
                    direction: Direction::Buy,
                    price_usdc: onchain.price.value(),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        let offchain_order_id = OffchainOrderId::new();
        frameworks
            .position
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        offchain_order_id
    }

    fn execution_ctx_for(
        symbol: &Symbol,
        shares: Positive<FractionalShares>,
    ) -> crate::onchain::accumulator::ExecutionCtx {
        crate::onchain::accumulator::ExecutionCtx {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares,
            executor: st0x_execution::SupportedExecutor::DryRun,
        }
    }

    #[tokio::test]
    async fn dispatch_post_place_state_none_clears_position_pending() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let offchain_order_id = drive_position_to_pending(&frameworks, &symbol, shares).await;

        // Sanity: pending is set before dispatch
        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(position.pending_offchain_order_id, Some(offchain_order_id));

        let execution = execution_ctx_for(&symbol, shares);
        let result = dispatch_post_place_state(None, &execution, &cqrs, offchain_order_id)
            .await
            .unwrap();
        assert_eq!(
            result, None,
            "None state must report no order placed so caller does not pretend a phantom \
             id is real"
        );

        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            position.pending_offchain_order_id, None,
            "None state must clear the position's pending offchain order id"
        );
    }

    #[tokio::test]
    async fn dispatch_post_place_state_pending_clears_position_pending() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let offchain_order_id = drive_position_to_pending(&frameworks, &symbol, shares).await;

        let lifecycle_bug_state = OffchainOrder::Pending {
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: st0x_execution::SupportedExecutor::DryRun,
            placed_at: Utc::now(),
        };

        let execution = execution_ctx_for(&symbol, shares);
        let result = dispatch_post_place_state(
            Some(lifecycle_bug_state),
            &execution,
            &cqrs,
            offchain_order_id,
        )
        .await
        .unwrap();
        assert_eq!(
            result, None,
            "Lifecycle-bug Pending state must report no order placed"
        );

        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Lifecycle-bug Pending state must clear the position's pending offchain order id"
        );
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_cleanly_when_token_cancelled() {
        let supervisor = SupervisorBuilder::default().build().run();
        let shutdown_token = CancellationToken::new();
        let apalis_shutdown_token = CancellationToken::new();
        let apalis_token_clone = apalis_shutdown_token.clone();

        // Monitor that completes once its shutdown signal fires.
        let monitor = tokio::spawn(async move {
            apalis_token_clone.cancelled().await;
            Ok::<(), MonitorTaskError>(())
        });

        let job_cleanup = tokio::spawn(pending::<()>());

        let mut conductor = Conductor {
            supervisor,
            monitor,
            job_cleanup,
            shutdown_token: shutdown_token.clone(),
            apalis_shutdown_token,
        };

        shutdown_token.cancel();
        conductor.wait_for_completion().await.unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_propagates_monitor_error() {
        let supervisor = SupervisorBuilder::default().build().run();
        let shutdown_token = CancellationToken::new();
        let apalis_shutdown_token = CancellationToken::new();
        let apalis_token_clone = apalis_shutdown_token.clone();

        // Monitor returns an error after shutdown signal.
        let monitor = tokio::spawn(async move {
            apalis_token_clone.cancelled().await;
            Err(MonitorTaskError::TerminalJobFailure)
        });

        let job_cleanup = tokio::spawn(pending::<()>());

        let mut conductor = Conductor {
            supervisor,
            monitor,
            job_cleanup,
            shutdown_token: shutdown_token.clone(),
            apalis_shutdown_token,
        };

        shutdown_token.cancel();
        let result = conductor.wait_for_completion().await;
        assert!(
            result.is_err(),
            "monitor error during drain should propagate"
        );
    }

    #[test]
    fn check_monitor_drain_result_ok() {
        check_monitor_drain_result(Ok(Ok(()))).unwrap();
    }

    #[test]
    fn check_monitor_drain_result_propagates_monitor_error() {
        let error =
            check_monitor_drain_result(Ok(Err(MonitorTaskError::TerminalJobFailure))).unwrap_err();

        assert!(
            error.to_string().contains("Apalis worker failed"),
            "should propagate MonitorTaskError, got: {error}"
        );
    }

    #[tokio::test]
    async fn dispatch_post_place_state_filled_clears_position_pending() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let offchain_order_id = drive_position_to_pending(&frameworks, &symbol, shares).await;

        let lifecycle_bug_state = OffchainOrder::Filled {
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: st0x_execution::SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("ORD_TEST"),
            price: Usd::new(float!(150)),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
            filled_at: Utc::now(),
        };

        let execution = execution_ctx_for(&symbol, shares);
        let result = dispatch_post_place_state(
            Some(lifecycle_bug_state),
            &execution,
            &cqrs,
            offchain_order_id,
        )
        .await
        .unwrap();
        assert_eq!(
            result, None,
            "Lifecycle-bug Filled state must report no order placed"
        );

        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Lifecycle-bug Filled state must clear the position's pending offchain order id"
        );
    }

    #[tokio::test]
    async fn dispatch_post_place_state_failed_clears_position_pending() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let offchain_order_id = drive_position_to_pending(&frameworks, &symbol, shares).await;

        let failed_state = OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: st0x_execution::SupportedExecutor::DryRun,
            error: "broker rejected".to_string(),
            placed_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let execution = execution_ctx_for(&symbol, shares);
        let result =
            dispatch_post_place_state(Some(failed_state), &execution, &cqrs, offchain_order_id)
                .await
                .unwrap();
        assert_eq!(
            result,
            Some(offchain_order_id),
            "Failed state must report the order id so caller records it"
        );

        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Failed state must clear the position's pending offchain order id"
        );
    }

    #[tokio::test]
    async fn check_monitor_drain_result_propagates_join_error() {
        let handle = tokio::spawn(async { panic!("test panic") });
        // Abort to get a JoinError::Cancelled.
        handle.abort();
        let join_error = handle.await.unwrap_err();

        check_monitor_drain_result(Err(join_error)).unwrap_err();
    }
}
