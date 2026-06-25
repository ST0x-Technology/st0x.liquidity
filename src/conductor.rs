//! Orchestrates the bot lifecycle: startup sequencing, runtime task management,
//! and trade processing. [`Conductor::run`] is the entry point.

mod builder;
mod exit;
pub(crate) mod job;
mod manifest;
pub(crate) mod monitor;
mod trading_queues;

use alloy::primitives::{Address, B256};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
use alloy::rpc::client::ClientBuilder;
use anyhow::Context;
use apalis::prelude::Status;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use sqlx_apalis::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use task_supervisor::SupervisorHandle;
use tokio::sync::{Mutex, broadcast};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use st0x_config::{
    AssetsConfig, BrokerCtx, Ctx, CtxError, EvmCtx, ExecutionThreshold, RebalancingCtx,
};
use st0x_dto::Statement;
use st0x_event_sorcery::{
    AggregateError, LifecycleError, Projection, SendError, Store, StoreBuilder, compact_events,
    incremental_vacuum, load_all_ids, load_entity,
};
use st0x_evm::{USDC_BASE, Wallet};
use st0x_execution::{
    AlpacaBrokerApi, AlpacaWalletService, ClientOrderId, CounterTradePreflight,
    CounterTradeReservation, CounterTradeSkipReason, ExecutionError, Executor, FractionalShares,
    MarketOrder, Symbol, TryIntoExecutor,
};
use st0x_raindex::{RaindexService, RaindexVaultId};
use st0x_wrapper::WrapperService;

use crate::alerts::{NoopNotifier, NotifierError, TelegramNotifier};
use crate::conductor::exit::{ConductorExit, MonitorTaskError};
use crate::conductor::monitor::order_fills::{CutoffProbe, probe_cutoff_block_support};
use crate::dashboard::Broadcaster;
use crate::equity_redemption::{
    EquityRedemption, interrupted_redemption_ids, symbols_with_stuck_redemptions,
};
use crate::inventory::{
    BroadcastingInventory, Inventory, InventoryProjection, InventorySnapshot, Venue,
};
use crate::offchain::order::{
    ExecutorOrderPlacer, OffchainOrder, OffchainOrderId, OrderPlacer, PollOrderStatus,
    PollOrderStatusJobQueue, client_order_id_for_placement, place_offchain_order_at_broker,
};
#[cfg(test)]
use crate::offchain::order::{OffchainOrderCommand, noop_order_placer};
use crate::onchain::OnchainTrade;
#[cfg(test)]
use crate::onchain::accumulator::check_all_positions;
use crate::onchain::accumulator::{ExecutionCtx, check_execution_readiness};
use crate::onchain::approvals::{build_approval_targets, grant_startup_approvals};
use crate::onchain::backfill::BackfillJobQueue;
use crate::onchain::trade::{RaindexTradeEvent, extract_owned_vaults, extract_vaults_from_clear};
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand, OnChainTradeError, OnChainTradeId};
use crate::performance::HedgeLatencyProjection;
use crate::performance::rebalance::RebalanceTimingProjection;
use crate::performance::reliability::LifecycleFailureProjection;
use crate::position::{Position, PositionCommand, PositionError, TradeId};
use crate::rebalancing::equity::{
    CrossVenueEquityTransfer, ResumeTokenizationAggregate, ResumeTokenizationCtx,
    ResumeTokenizationJobQueue, ResumeTokenizationTarget, TransferEquityToHedgingCtx,
    TransferEquityToMarketMakingCtx,
};
use crate::rebalancing::trigger::GuardState;
use crate::rebalancing::usdc::{
    TransferUsdcToHedging, TransferUsdcToHedgingCtx, TransferUsdcToMarketMaking,
    TransferUsdcToMarketMakingCtx, UsdcSettlementParams,
};
use crate::rebalancing::{
    RebalancerServices, RebalancingSchedulers, RebalancingService, RebalancingServiceConfig,
    to_wrapped_equities,
};
use crate::symbol::cache::SymbolCache;
use crate::telemetry::broker::InstrumentedAlpacaBroker;
use crate::telemetry::executor::InstrumentedExecutor;
use crate::telemetry::rpc::RpcTelemetryLayer;
use crate::telemetry::{TelemetrySender, spawn_dependency_call_writer};
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
use trading_queues::{EquityRecoveryInputs, TradingJobQueues, setup_trading_job_queues};

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
    /// Places the broker order for a newly-recorded offchain order. Lifted out
    /// of the (now pure) `OffchainOrder::Place` handler: the side effect lives
    /// in the placement path, not the command handler.
    pub(crate) order_placer: Arc<dyn OrderPlacer>,
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
    /// Drains dependency-call telemetry samples into SQLite in batches.
    telemetry_writer: JoinHandle<()>,
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

/// Wires the telemetry channel, instrumented executor, and RPC provider together
/// and spawns the background writer that drains samples into SQLite.
///
/// Returns the instrumented executor, provider (ready for use), the writer task
/// handle, and the original telemetry sender. Probes the RPC endpoint once to
/// fail fast on misconfiguration before the rest of startup proceeds: basic
/// reachability plus a usable configured cutoff block tag (`safe` by default).
/// An endpoint that errors on the tag, or aliases `finalized` to the chain tip
/// (disabling reorg protection when `finalized` is configured), is rejected
/// here rather than running silently degraded; a fresh chain with no block for
/// the tag yet is allowed through. The executor, RPC layer, and the returned
/// sender each hold a clone of the telemetry channel; the writer exits only
/// when all three are dropped.
/// Provider type returned by `ProviderBuilder::new().connect_client(...)` over
/// an HTTP transport. Named here to make `setup_instrumentation`'s return type
/// concrete without coupling callers to `alloy` internals.
type HttpProvider = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider,
>;

async fn setup_instrumentation<E>(
    executor_ctx: impl TryIntoExecutor<Executor = E>,
    evm: &EvmCtx,
    pool: SqlitePool,
) -> anyhow::Result<(
    InstrumentedExecutor<E>,
    HttpProvider,
    JoinHandle<()>,
    TelemetrySender,
)>
where
    E: Executor,
{
    // Telemetry channel: the RPC layer and instrumented executor emit
    // dependency-call samples through it; the writer task batches them
    // into SQLite in the background.
    let (telemetry, telemetry_receiver) = TelemetrySender::channel();

    let executor =
        InstrumentedExecutor::new(executor_ctx.try_into_executor().await?, telemetry.clone());

    // Single HTTP transport: drives continuous eth_getLogs fill polling
    // (via the OrderFillMonitor + backfill worker) and all read-only
    // contract calls. No WebSocket -- see `monitor::order_fills`. The
    // telemetry layer wraps the transport itself, so every JSON-RPC call
    // from any provider handle is timed.
    let rpc_client = ClientBuilder::default()
        .layer(RpcTelemetryLayer::new(telemetry.clone()))
        .http(evm.rpc_url.clone());
    let provider = ProviderBuilder::new().connect_client(rpc_client);

    // The HTTP transport connects lazily, so probe it once at startup to
    // fail fast on a misconfigured or unreachable RPC rather than only
    // surfacing it as repeated poll-loop retries. systemd's bounded
    // restart loop handles a genuinely-down endpoint from here.
    let chain_tip = provider
        .get_block_number()
        .await
        .context("failed to reach RPC endpoint at startup")?;

    // Probe the configured cutoff block tag at startup to surface a
    // misconfigured or unsupported endpoint before the fill monitor starts
    // polling. A null response is allowed through (cold start); an error or
    // a detected `finalized`-aliasing-to-`latest` returns Err and fails startup.
    match probe_cutoff_block_support(&provider, chain_tip, evm.ingestion_cutoff)
        .await
        .context("RPC endpoint cannot serve the configured cutoff block tag at startup")?
    {
        CutoffProbe::Supported | CutoffProbe::NotYetAvailable => {}
    }

    // Spawn the writer before returning the sender: the executor, RPC layer,
    // and the returned sender each hold a clone. When all three are dropped,
    // the channel closes and the writer task exits cleanly.
    let telemetry_writer = spawn_dependency_call_writer(pool, telemetry_receiver);

    Ok((executor, provider, telemetry_writer, telemetry))
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

        let (executor, provider, telemetry_writer, telemetry) =
            setup_instrumentation(executor_ctx, &ctx.evm, pool.clone()).await?;
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
            unwrapped_equity_recovery_services,
            mint_store,
            redemption_store,
            transfer_usdc_to_hedging_ctx,
            transfer_usdc_to_market_making_ctx,
            transfer_equity_to_market_making_ctx,
            transfer_equity_to_hedging_ctx,
            resume_tokenization_queue,
        } = PositionAndRebalancing::setup(
            rebalancing,
            RebalancingDeps {
                pool: pool.clone(),
                apalis_pool: apalis_pool.clone(),
                ctx: ctx.clone(),
                inventory: inventory.clone(),
                event_sender,
                vault_registry: vault_registry.clone(),
                vault_registry_projection,
                schedulers: schedulers.clone(),
                telemetry: telemetry.clone(),
            },
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

        hydrate_startup_inventory_and_read_models(&pool, &inventory, rebalancing_service.as_ref())
            .await?;

        // The HedgeLatencyProjection subscribes to BOTH Position and
        // OffchainOrder (see its deps!). This instance, on the OffchainOrder
        // store, handles the broker-acceptance event (`Accepted`/legacy
        // `Submitted`) to stamp submitted_at. The cycle's filled_at/failed_at are
        // stamped by the SAME projection registered on the Position store (the
        // Position stream carries the broker's own fill timestamp). Do NOT
        // consolidate the two registrations: routing OffchainOrder Filled/Failed
        // here would drop the authoritative broker timestamp.
        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .with(Arc::new(HedgeLatencyProjection::new(pool.clone())))
                .with(Arc::new(LifecycleFailureProjection::new(pool.clone())))
                .build(())
                .await?;

        // Startup recovery runs before any job worker starts, so no concurrent
        // placement can race its broker re-drive -- it intentionally runs without
        // `counter_trade_submission_lock` (which the builder constructs later). The
        // periodic `CheckPositions` sweep, which CAN race live placements, holds
        // the lock across the same call.
        recover_orphaned_pending_offchain_orders(
            &position,
            &position_projection,
            &offchain_order,
            &ExecutorOrderPlacer(executor.clone()),
        )
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

        let TradingJobQueues {
            hedge_queue,
            poll_status_queue,
            reconcile_queue,
            rejection_queue,
            wrapped_equity_recovery_queue,
            unwrapped_equity_recovery_queue,
            wrapped_equity_recovery_ctx,
            unwrapped_equity_recovery_ctx,
            check_positions_queue,
        } = setup_trading_job_queues(
            &apalis_pool,
            &job_queue,
            EquityRecoveryInputs {
                wrapped_store: wrapped_equity_recovery_store,
                unwrapped_store: unwrapped_equity_recovery_store,
                unwrapped_services: unwrapped_equity_recovery_services,
                rebalancing_service: rebalancing_service.clone(),
                mint_store: mint_store.clone(),
                redemption_store: redemption_store.clone(),
                inventory: inventory.clone(),
                inventory_poll_interval: Duration::from_secs(ctx.inventory_poll_interval),
            },
            &frameworks.offchain_order_projection,
            executor.to_supported_executor(),
        )
        .await?;

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

        // Build ResumeTokenizationCtx only when BOTH the queue and the
        // recovery_transfer are present (rebalancing enabled). zip() makes the
        // dual-Some requirement explicit: if either is None the ctx is None,
        // and the compiler flags any mismatch if one arm is accidentally set
        // without the other.
        let resume_tokenization_ctx = resume_tokenization_queue
            .as_ref()
            .zip(recovery_transfer.as_ref())
            .map(|(_, transfer)| {
                Arc::new(ResumeTokenizationCtx {
                    transfer: transfer.clone(),
                })
            });

        // Provide a fallback empty queue when rebalancing is disabled, so the
        // builder always receives a concrete queue (it is never consumed).
        let resume_tokenization_queue = resume_tokenization_queue
            .unwrap_or_else(|| ResumeTokenizationJobQueue::new(&apalis_pool));

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
            .resume_tokenization_queue(resume_tokenization_queue)
            .maybe_resume_tokenization_ctx(resume_tokenization_ctx)
            .job_cleanup(job_cleanup)
            .telemetry_writer(telemetry_writer)
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

        // Job cleanup is not a drain target, so stop it now. The telemetry
        // writer stays up through Phase 2 so dependency samples emitted by
        // draining apalis jobs are still persisted; it is aborted only once
        // the drain has completed.
        self.job_cleanup.abort();

        // Phase 2: now that producers are stopped, signal apalis to drain
        // in-flight jobs. The apalis token is independent of the outer
        // shutdown token -- we cancel it explicitly after producers stop.
        info!(target: "shutdown", "Phase 2: draining in-flight jobs");
        self.apalis_shutdown_token.cancel();
        let monitor_result = check_monitor_drain_result((&mut self.monitor).await);
        self.telemetry_writer.abort();
        monitor_result
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
        self.telemetry_writer.abort();
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
    services: Option<UnwrappedEquityRecoveryServices>,
    service: Option<Arc<RebalancingService>>,
    mint_store: Option<Arc<Store<TokenizedEquityMint>>>,
    redemption_store: Option<Arc<Store<EquityRedemption>>>,
    inventory: Arc<BroadcastingInventory>,
    queue: UnwrappedEquityRecoveryJobQueue,
    reschedule_interval: Duration,
) -> Option<Arc<UnwrappedEquityRecoveryCtx>> {
    let (Some(store), Some(services), Some(service), Some(mint_store), Some(redemption_store)) =
        (store, services, service, mint_store, redemption_store)
    else {
        return None;
    };

    Some(Arc::new(UnwrappedEquityRecoveryCtx {
        inventory,
        store,
        services,
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
    pub(crate) order_placer: &'a dyn OrderPlacer,
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

            // USDC transfer-job rows are the durable startup re-arm
            // idempotency + redrive-budget signal. Pruning them while the
            // aggregate is still mid-flight (e.g. `BridgingSubmitting`) resets
            // the redrive bound to a fresh budget on restart and silences the
            // stranded-transfer operator alert. Transfer jobs are low-volume,
            // so retaining their finished rows is cheap; exclude both transfer
            // job types from cleanup.
            let cleanup_result = sqlx_apalis::query(
                "DELETE FROM Jobs \
                 WHERE job_type NOT IN (?, ?) \
                 AND ( \
                     status = ? \
                     OR status = ? \
                     OR (status = ? AND max_attempts <= attempts) \
                 )",
            )
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
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

/// Brings the in-memory startup state into line with the event log before the
/// job workers and reactors go live: hydrates the `InventoryView` from the
/// persisted snapshot (without this, the first post-restart poll deduplicates
/// unchanged values and leaves the view empty, risking incorrect rebalancing),
/// enqueues recovery for any current wallet balances, and catches the
/// lifecycle-failure read model up to the log before its `OffchainOrder`
/// reactor is registered.
async fn hydrate_startup_inventory_and_read_models(
    pool: &SqlitePool,
    inventory: &Arc<BroadcastingInventory>,
    rebalancing_service: Option<&Arc<RebalancingService>>,
) -> anyhow::Result<()> {
    hydrate_inventory_from_snapshot(pool, inventory).await;
    if let Some(service) = rebalancing_service {
        service.enqueue_recovery_for_current_wallet_balances().await;
    }
    catch_up_lifecycle_failures(pool).await?;
    Ok(())
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
    unwrapped_equity_recovery_services: UnwrappedEquityRecoveryServices,
    mint_store: Arc<Store<TokenizedEquityMint>>,
    redemption_store: Arc<Store<EquityRedemption>>,
    transfer_usdc_to_hedging_ctx: Arc<TransferUsdcToHedgingCtx>,
    transfer_usdc_to_market_making_ctx: Arc<TransferUsdcToMarketMakingCtx>,
    transfer_equity_to_market_making_ctx: Arc<TransferEquityToMarketMakingCtx>,
    transfer_equity_to_hedging_ctx: Arc<TransferEquityToHedgingCtx>,
    resume_tokenization_queue: ResumeTokenizationJobQueue,
}

/// Shared infrastructure dependencies needed to spawn rebalancing.
struct RebalancingDeps {
    pool: SqlitePool,
    apalis_pool: apalis_sqlite::SqlitePool,
    ctx: Ctx,
    inventory: Arc<BroadcastingInventory>,
    event_sender: broadcast::Sender<Statement>,
    vault_registry: Arc<Store<VaultRegistry>>,
    vault_registry_projection: Arc<Projection<VaultRegistry>>,
    schedulers: RebalancingSchedulers,
    telemetry: TelemetrySender,
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
    unwrapped_equity_recovery_services: Option<UnwrappedEquityRecoveryServices>,
    mint_store: Option<Arc<Store<TokenizedEquityMint>>>,
    redemption_store: Option<Arc<Store<EquityRedemption>>>,
    transfer_usdc_to_hedging_ctx: Option<Arc<TransferUsdcToHedgingCtx>>,
    transfer_usdc_to_market_making_ctx: Option<Arc<TransferUsdcToMarketMakingCtx>>,
    transfer_equity_to_market_making_ctx: Option<Arc<TransferEquityToMarketMakingCtx>>,
    transfer_equity_to_hedging_ctx: Option<Arc<TransferEquityToHedgingCtx>>,
    resume_tokenization_queue: Option<ResumeTokenizationJobQueue>,
}

impl PositionAndRebalancing {
    async fn setup(
        rebalancing: Option<RebalancingCtx>,
        deps: RebalancingDeps,
    ) -> anyhow::Result<Self> {
        if let Some(rebalancing_ctx) = rebalancing {
            let wallet_ctx = deps.ctx.wallet()?;
            let ethereum_wallet = wallet_ctx.ethereum_wallet().clone();
            let base_wallet = wallet_ctx.base_wallet().clone();
            let redemption_wallet = deps.ctx.redemption_wallet()?;

            // Computed before `deps` is moved into the spawn call, since
            // `WalletPollingCtx` below also needs the config behind `deps.ctx`.
            let unwrapped_equity_token_addresses =
                base_wallet_unwrapped_equity_token_addresses(&deps.ctx);
            let wrapped_equity_token_addresses =
                base_wallet_wrapped_equity_token_addresses(&deps.ctx);

            let infra = spawn_rebalancing_infrastructure(
                rebalancing_ctx,
                redemption_wallet,
                ethereum_wallet.clone(),
                base_wallet.clone(),
                deps,
            )
            .await?;

            let wallet_polling = crate::inventory::WalletPollingCtx {
                ethereum: Arc::new(ethereum_wallet),
                base: Arc::new(base_wallet),
                unwrapped_equity_token_addresses,
                wrapped_equity_token_addresses,
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
                unwrapped_equity_recovery_services: Some(infra.unwrapped_equity_recovery_services),
                mint_store: Some(infra.mint_store),
                redemption_store: Some(infra.redemption_store),
                transfer_usdc_to_hedging_ctx: Some(infra.transfer_usdc_to_hedging_ctx),
                transfer_usdc_to_market_making_ctx: Some(infra.transfer_usdc_to_market_making_ctx),
                transfer_equity_to_market_making_ctx: Some(
                    infra.transfer_equity_to_market_making_ctx,
                ),
                transfer_equity_to_hedging_ctx: Some(infra.transfer_equity_to_hedging_ctx),
                resume_tokenization_queue: Some(infra.resume_tokenization_queue),
            })
        } else {
            let RebalancingDeps {
                pool,
                inventory,
                event_sender,
                ..
            } = deps;

            let broadcaster = Arc::new(Broadcaster::new(event_sender, pool.clone()));
            let (position, position_projection) = build_position_cqrs(&pool, broadcaster).await?;

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
                unwrapped_equity_recovery_services: None,
                mint_store: None,
                redemption_store: None,
                transfer_usdc_to_hedging_ctx: None,
                transfer_usdc_to_market_making_ctx: None,
                transfer_equity_to_market_making_ctx: None,
                transfer_equity_to_hedging_ctx: None,
                resume_tokenization_queue: None,
            })
        }
    }
}

/// Builds the USDC alerting notifier.
///
/// Returns `Ok(Arc<NoopNotifier>)` when `[alerts]` is absent — silence is
/// the correct behaviour for an unconfigured optional channel.
///
/// Returns `Err` when `[alerts]` IS present but `TelegramNotifier` fails to
/// initialise. The caller must propagate this so the server fails to start:
/// an operator who configured `[alerts]` believes alerts are active; silently
/// falling back to Noop would suppress all redrive-limit and terminal-error
/// pages with no runtime indication.
fn build_usdc_notifier(
    alerts: Option<&st0x_config::AlertsCtx>,
) -> Result<Arc<dyn crate::alerts::Notifier>, NotifierError> {
    let Some(alerts) = alerts else {
        debug!("USDC alerting: [alerts] section absent, using NoopNotifier");
        return Ok(Arc::new(NoopNotifier));
    };
    let notifier =
        TelegramNotifier::new(&alerts.bot_token, alerts.chat_id, alerts.message_thread_id)?;
    info!("USDC alerting: Telegram notifier configured");
    Ok(Arc::new(notifier))
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

        let transfer_usdc_to_hedging_queue = deps.schedulers.transfer_usdc_to_hedging.clone();
        let transfer_usdc_to_market_making_queue =
            deps.schedulers.transfer_usdc_to_market_making.clone();

        let issuance_freeze_reader = Arc::new(st0x_issuance_client::IssuanceClient::new(
            deps.ctx.issuance.base_url.clone(),
            deps.ctx.issuance.api_key.header_value(),
        )?);

        let usdc_notifier = build_usdc_notifier(deps.ctx.alerts.as_ref())?;

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
            usdc_notifier.clone(),
        ));

        rebalancing_service
            .set_freeze_status_reader(issuance_freeze_reader)
            .await;

        let broadcaster = Arc::new(Broadcaster::new(deps.event_sender, deps.pool.clone()));
        let hedge_latency = Arc::new(HedgeLatencyProjection::new(deps.pool.clone()));
        let rebalance_timing = Arc::new(RebalanceTimingProjection::new(deps.pool.clone()));
        let lifecycle_failure = Arc::new(LifecycleFailureProjection::new(deps.pool.clone()));
        // Catch the rebalance stage-timing read model up to the event log before
        // the reactor goes live, so a restart replays whatever the forward-only
        // live path missed in the previous run. Reads only the un-folded tail
        // past each operation's checkpoint, so this does not re-fold history.
        let rebalance_timing_replayed = rebalance_timing.catch_up().await?;
        info!(target: "rebalance", replayed = rebalance_timing_replayed,
            "Rebalance stage-timing read model caught up to the event log at startup");

        let manifest = QueryManifest::new(
            rebalancing_service.clone(),
            broadcaster,
            hedge_latency,
            rebalance_timing,
            lifecycle_failure,
        );

        let built = manifest.build(deps.pool.clone()).await?;

        rebalancing_service
            .recover_pending_offchain_order_symbols(&built.position_projection)
            .await?;

        rebalancing_service
            .set_stores(
                built.mint.clone(),
                built.redemption.clone(),
                built.usdc.clone(),
            )
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
        let (
            wrapped_equity_recovery_store,
            unwrapped_equity_recovery_store,
            unwrapped_equity_recovery_services,
        ) = build_equity_recovery_stores(
            &deps.pool,
            raindex_service.clone(),
            vault_lookup.clone(),
            wrapper.clone(),
            recovery_transfer.clone(),
            base_wallet.address(),
        )
        .await?;

        let mut resume_tokenization_queue = ResumeTokenizationJobQueue::new(&deps.apalis_pool);

        recover_interrupted_tokenization_aggregates(
            &deps.pool,
            &rebalancing_service,
            deps.inventory.as_ref(),
            built.mint.clone(),
            built.redemption.clone(),
            &mut resume_tokenization_queue,
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

        // Wrap with telemetry before threading down so rebalancer Alpaca calls
        // emit broker dependency samples (mirrors hedge executor wrapping).
        let broker = InstrumentedAlpacaBroker::new(
            AlpacaBrokerApi::try_from_ctx(alpaca_auth.clone()).await?,
            deps.telemetry.clone(),
        );

        let services = RebalancerServices::new(
            broker,
            Arc::clone(&alpaca_wallet),
            ethereum_wallet,
            base_wallet,
            raindex_service,
            UsdcSettlementParams {
                attestation_retry_deadline: rebalancing_ctx.attestation_retry_deadline,
                required_confirmations: deps.ctx.evm.required_confirmations,
                #[cfg(feature = "test-support")]
                circle_api_base: rebalancing_ctx.circle_api_base.clone(),
                #[cfg(feature = "test-support")]
                token_messenger: rebalancing_ctx.token_messenger,
                #[cfg(feature = "test-support")]
                message_transmitter: rebalancing_ctx.message_transmitter,
            },
        )?;

        let usdc_vault_id = deps
            .ctx
            .assets
            .cash
            .as_ref()
            .and_then(|cash| cash.vault_ids.first().copied())
            .ok_or(CtxError::MissingCashVaultId)?;

        let usdc_handles = services.into_usdc_transfer_handles(
            market_maker_wallet,
            RaindexVaultId(usdc_vault_id),
            built.usdc,
        );

        let transfer_usdc_to_market_making_ctx = Arc::new(TransferUsdcToMarketMakingCtx {
            transfer: usdc_handles.resume_alpaca_to_base,
            job_queue: transfer_usdc_to_market_making_queue,
            max_burn_revert_redrives: rebalancing_ctx.max_burn_revert_redrives,
            notifier: usdc_notifier.clone(),
        });

        let transfer_usdc_to_hedging_ctx = Arc::new(TransferUsdcToHedgingCtx {
            transfer: usdc_handles.resume_base_to_alpaca,
            timeout: rebalancing_ctx.transfer_attempt_timeout,
            job_queue: transfer_usdc_to_hedging_queue,
            max_burn_revert_redrives: rebalancing_ctx.max_burn_revert_redrives,
            notifier: usdc_notifier,
        });

        let transfer_equity_to_market_making_ctx = Arc::new(TransferEquityToMarketMakingCtx {
            transfer: recovery_transfer.clone(),
            equity_in_progress: rebalancing_service.equity_in_progress.clone(),
            mint_store: built.mint.clone(),
            equities_config: deps.ctx.assets.equities.clone(),
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
            unwrapped_equity_recovery_services,
            mint_store: built.mint,
            redemption_store: built.redemption,
            transfer_usdc_to_hedging_ctx,
            transfer_usdc_to_market_making_ctx,
            transfer_equity_to_market_making_ctx,
            transfer_equity_to_hedging_ctx,
            resume_tokenization_queue,
        })
    })
}

/// Catches the lifecycle-failure read model up to the event log before its
/// reactor goes live. Runs in BOTH trading modes: the projection subscribes to
/// OffchainOrder, which is active even in standalone/hedging-only mode, so a
/// hedging-only deploy must replay its failure history too -- not just the
/// rebalancing path. Idempotent via the dedup insert, so re-running cannot
/// duplicate rows.
async fn catch_up_lifecycle_failures(pool: &SqlitePool) -> anyhow::Result<()> {
    let replayed = LifecycleFailureProjection::new(pool.clone())
        .catch_up()
        .await?;
    info!(
        target: "reliability",
        replayed,
        "Lifecycle-failure read model caught up to the event log at startup"
    );

    Ok(())
}

/// Builds the wrapped and unwrapped equity-recovery aggregate stores.
///
/// Both share the recovery `transfer` and the raindex/vault/wrapper
/// dependencies; the unwrapped store additionally needs the base `wallet`
/// address to settle unwraps. Kept together because they are the recovery
/// counterpart built from the same `recovery_transfer`.
async fn build_equity_recovery_stores<Chain: Wallet + Clone>(
    pool: &SqlitePool,
    raindex: Arc<RaindexService<Chain>>,
    vault_lookup: Arc<dyn VaultLookup>,
    wrapper: Arc<WrapperService<Chain>>,
    transfer: Arc<CrossVenueEquityTransfer>,
    wallet: Address,
) -> anyhow::Result<(
    Arc<Store<WrappedEquityRecovery>>,
    Arc<Store<UnwrappedEquityRecovery>>,
    UnwrappedEquityRecoveryServices,
)> {
    let wrapped_store = StoreBuilder::<WrappedEquityRecovery>::new(pool.clone())
        .build(WrappedEquityRecoveryServices {
            raindex: raindex.clone(),
            vault_lookup: vault_lookup.clone(),
            wrapper: wrapper.clone(),
            transfer: transfer.clone(),
        })
        .await?;

    // The unwrapped-recovery aggregate is a pure event recorder; its onchain
    // side effects run in the recovery job, which holds these services on its
    // ctx rather than on the store.
    let unwrapped_services = UnwrappedEquityRecoveryServices {
        raindex,
        vault_lookup,
        wrapper,
        transfer,
        wallet,
    };

    let unwrapped_store = StoreBuilder::<UnwrappedEquityRecovery>::new(pool.clone())
        .build(())
        .await?;

    Ok((wrapped_store, unwrapped_store, unwrapped_services))
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
    mint_store: Arc<Store<TokenizedEquityMint>>,
    redemption_store: Arc<Store<EquityRedemption>>,
    resume_queue: &mut ResumeTokenizationJobQueue,
) -> anyhow::Result<()> {
    // First promote any Running/Queued orphans (from a previous process that
    // crashed mid-job) back to Pending. Then discard ALL pending rows.
    // Order matters: if cancel_all_pending ran first, the orphan-reset step
    // would re-activate those rows as Pending AFTER the cancel, leaving
    // duplicate Pending rows for the same aggregates.
    //
    // Intentionally soft-fail (warn, not ?) unlike every other orphan-requeue
    // call at startup. Propagating would re-introduce the startup-gating that
    // moving resume off the startup path removes: the whole point of the async
    // resume worker is that a slow or failing issuer cannot block monitoring
    // from starting.
    //
    // When requeue_orphaned fails, a Running orphan is NOT promoted to Pending
    // and therefore survives the subsequent cancel_all_pending. The fresh push
    // below then leaves a Running+Pending PAIR for the same aggregate, not a
    // plain duplicate Pending. Because the resume worker uses a deterministic
    // WORKER_NAME, a restarted process keeps the orphan's heartbeat fresh, so
    // apalis never re-enqueues or re-executes that Running row -- it stays stuck,
    // and only the fresh Pending row runs. This is safe ONLY because resume_mint
    // and resume_redemption are idempotent: the fresh run redoes any side effect
    // the crashed attempt left partially applied. Idempotency is a load-bearing
    // invariant here -- any future non-idempotent side-effect in resume would
    // break this design.
    if let Err(error) = resume_queue.requeue_orphaned().await {
        warn!(
            target: "tokenization",
            %error,
            "Failed to reset orphaned resume-tokenization rows; a Running orphan \
             from a crashed process survives as a stuck row (the deterministic \
             worker keeps its heartbeat fresh, so apalis never re-runs it) while \
             the fresh Pending job executes. Tolerated because resume is idempotent."
        );
    }
    resume_queue.cancel_all_pending().await;

    // If the process dies after cancel_all_pending() but before the first push()
    // below, the interrupted aggregates have neither a Pending nor a Running row,
    // so they are not resumed until the next restart re-derives them from the
    // events table (interrupted_mint_ids / interrupted_redemption_ids read the
    // event store, not the queue). This self-heals on reboot.

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

        // Exclude pre-wrap mints (TokensReceived, WrapSubmitted) that are
        // HeldForRecovery from direct resume. UnwrappedEquityRecovery owns
        // those and will re-wrap + deposit the tokens. Calling resume_mint
        // on the same aggregate would race with the recovery job (both would
        // try to submit the wrap tx). TokensWrapped and VaultDepositSubmitted
        // mints are always safe to resume (vault deposit is idempotent and
        // the aggregate/inventory guard both converge to Done).
        //
        // If cancel_all_pending silently failed above, a stale Pending row for
        // this aggregate may still exist. The duplicate Pending row is tolerated
        // because resume_mint is idempotent.
        if !is_pre_wrap_held_for_recovery(&mint, &rebalancing_service.equity_in_progress) {
            resume_queue
                .push(ResumeTokenizationAggregate {
                    target: ResumeTokenizationTarget::Mint(mint_id.clone()),
                })
                .await?;
        }
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

        // If cancel_all_pending silently failed above, a stale Pending row for
        // this aggregate may still exist. The duplicate Pending row is tolerated
        // because resume_redemption is idempotent.
        resume_queue
            .push(ResumeTokenizationAggregate {
                target: ResumeTokenizationTarget::Redemption(redemption_id.clone()),
            })
            .await?;
    }

    recover_stuck_redemptions(pool, inventory).await?;

    Ok(())
}

/// Returns `true` when `mint` is a pre-wrap post-receipt state
/// (`TokensReceived` or `WrapSubmitted`) AND its symbol's guard is
/// `HeldForRecovery`. These mints are excluded from direct `resume_mints`
/// because `UnwrappedEquityRecovery` owns the slot and will re-wrap and
/// deposit the tokens; calling `resume_mint` concurrently would race.
fn is_pre_wrap_held_for_recovery(
    mint: &TokenizedEquityMint,
    equity_in_progress: &RwLock<HashMap<Symbol, GuardState>>,
) -> bool {
    if let TokenizedEquityMint::TokensReceived { symbol, .. }
    | TokenizedEquityMint::WrapSubmitted { symbol, .. } = mint
    {
        let guard = match equity_in_progress.read() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        match guard.get(symbol) {
            Some(GuardState::HeldForRecovery) => true,
            Some(GuardState::ActiveTransfer { .. }) | None => false,
        }
    } else {
        false
    }
}

/// Recovers positions whose `pending_offchain_order_id` references an offchain
/// order that still needs reconciling back into the position aggregate. Two
/// cases:
///
/// - **Terminal order, un-propagated:** the order reached `Filled`/`Failed` but
///   the position update never landed (e.g. `complete_offchain_order_fill`
///   succeeded and the subsequent `complete_position_order` failed -- the two
///   are not atomic). The poller only polls `Submitted` orders, so it never
///   retries the position update and the position would stay stuck.
/// - **Still `Pending`:** a crash between `Place` (which records intent and
///   enters `Pending` before the broker call) and the broker outcome left the
///   order un-driven. The broker placement is re-driven; the broker dedupes on
///   `client_order_id`, so an in-flight order is adopted rather than placed
///   twice.
///
/// Called at startup (`Conductor::start`) and on every periodic `CheckPositions`
/// scan. A re-drive that reaches `Submitted` does not enqueue a poll itself: at
/// startup the subsequent `recover_submitted_offchain_orders` covers it, and the
/// periodic scan re-runs that same catch-up immediately after this call.
pub(crate) async fn recover_orphaned_pending_offchain_orders(
    position: &Arc<Store<Position>>,
    position_projection: &Arc<Projection<Position>>,
    offchain_order: &Arc<Store<OffchainOrder>>,
    order_placer: &dyn OrderPlacer,
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
        recover_single_orphaned_order(position, offchain_order, order_placer, &symbol, order_id)
            .await?;
    }

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
async fn recover_single_orphaned_order(
    position: &Arc<Store<Position>>,
    offchain_order: &Arc<Store<OffchainOrder>>,
    order_placer: &dyn OrderPlacer,
    symbol: &Symbol,
    order_id: OffchainOrderId,
) -> anyhow::Result<()> {
    let order = offchain_order.load(&order_id).await?;

    if resolve_terminal_claimed_order(position, symbol, order_id, order.as_ref()).await? {
        info!(%symbol, %order_id, "Orphaned pending order recovered");
        return Ok(());
    }

    // Only live orders remain -- terminal and absent ones were resolved above.
    match order {
        Some(OffchainOrder::Submitted { .. } | OffchainOrder::PartiallyFilled { .. }) => {
            debug!(%symbol, %order_id, "Pending offchain order still in progress");
            Ok(())
        }

        Some(OffchainOrder::Pending {
            symbol: order_symbol,
            shares,
            direction,
            executor,
            ..
        }) => {
            warn!(
                %symbol, %order_id,
                "Offchain order still Pending on startup -- re-driving the broker placement"
            );
            // A crash between `Place` (which now records intent and enters
            // Pending before the broker call) and the broker outcome leaves a
            // Pending orphan. Re-drive the durable placement: the pure `Place`
            // is a no-op on the existing aggregate, and the broker dedupes on
            // `client_order_id` (so it adopts the in-flight order rather than
            // placing a second time). Reconstruct that key exactly as the
            // placement paths derive it -- the prior-failure anchor, else this
            // order's own id. A resulting `Submitted` is polled to a fill by the
            // `recover_submitted_offchain_orders` catch-up that follows this
            // recovery (at startup, and re-run by the periodic scan at runtime);
            // only a terminal `Failed` needs the position claim cleared here.
            let anchor = position
                .load(symbol)
                .await?
                .and_then(|position| position.last_failed_offchain_order_id);
            let client_order_id = client_order_id_for_placement(order_id, anchor);

            let recovered = place_offchain_order_at_broker(
                offchain_order,
                order_placer,
                &order_id,
                order_symbol,
                shares,
                direction,
                executor,
                client_order_id,
            )
            .await?;

            // The broker rejected the re-drive: clear the position claim. A
            // re-drive that reaches a live broker state needs a `PollOrderStatus`
            // job, which the periodic submitted-order catch-up enqueues.
            if let Some(OffchainOrder::Failed { error, .. }) = recovered {
                position
                    .send(
                        symbol,
                        PositionCommand::FailOffChainOrder {
                            offchain_order_id: order_id,
                            error,
                        },
                    )
                    .await?;
            }
            Ok(())
        }

        // Terminal and absent orders are resolved by
        // `resolve_terminal_claimed_order` above.
        Some(OffchainOrder::Filled { .. } | OffchainOrder::Failed { .. }) | None => Ok(()),
    }
}

/// Propagates a terminal (or absent) offchain order's outcome into the position
/// that still claims it via `pending_offchain_order_id`, clearing the claim
/// in-process.
///
/// A position can hold a claim for an order that already reached `Filled` /
/// `Failed` -- or whose aggregate is missing entirely -- because the order
/// update and the position update are not atomic and the poller only polls
/// `Submitted` orders. Left alone the claim sits stuck until the next
/// `CheckPositions` sweep, so this emits the matching `CompleteOffChainOrder` /
/// `FailOffChainOrder` to resolve it immediately.
///
/// Returns `true` when a terminal/absent order was resolved, `false` when the
/// order is still live (`Pending` / `Submitted` / `PartiallyFilled`) and the
/// caller must drive it further.
async fn resolve_terminal_claimed_order(
    position: &Store<Position>,
    symbol: &Symbol,
    order_id: OffchainOrderId,
    order: Option<&OffchainOrder>,
) -> Result<bool, SendError<Position>> {
    use OffchainOrder::{Failed, Filled, PartiallyFilled, Pending, Submitted};

    let command = match order {
        None => {
            warn!(%symbol, %order_id, "Claimed offchain order missing from store -- recovering");
            PositionCommand::FailOffChainOrder {
                offchain_order_id: order_id,
                error: "pending offchain order has no offchain order aggregate".to_string(),
            }
        }

        Some(Filled {
            shares,
            direction,
            executor_order_id,
            price,
            filled_at,
            ..
        }) => {
            info!(%symbol, %order_id, "Claimed offchain order already Filled -- recovering");
            PositionCommand::CompleteOffChainOrder {
                offchain_order_id: order_id,
                shares_filled: *shares,
                direction: *direction,
                executor_order_id: executor_order_id.clone(),
                price: *price,
                broker_timestamp: *filled_at,
            }
        }

        Some(Failed { error, .. }) => {
            info!(%symbol, %order_id, "Claimed offchain order already Failed -- recovering");
            PositionCommand::FailOffChainOrder {
                offchain_order_id: order_id,
                error: error.clone(),
            }
        }

        Some(Pending { .. } | Submitted { .. } | PartiallyFilled { .. }) => return Ok(false),
    };

    position.send(symbol, command).await?;
    Ok(true)
}

/// Constructs the position CQRS framework with its view query
/// (without rebalancing trigger). Used when rebalancing is disabled.
async fn build_position_cqrs(
    pool: &SqlitePool,
    broadcaster: Arc<Broadcaster>,
) -> anyhow::Result<(Arc<Store<Position>>, Arc<Projection<Position>>)> {
    Ok(StoreBuilder::<Position>::new(pool.clone())
        .with(broadcaster)
        .with(Arc::new(HedgeLatencyProjection::new(pool.clone())))
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
/// Executes `OnChainTrade::Witness`, distinguishing domain rejections from
/// infrastructure failures per the error contract in docs/conductor.md.
///
/// Returns `Ok(false)` only for the duplicate-style domain rejections
/// (already filled/enriched) -- permanent, expected, and safe to skip.
/// Every other error (database unavailable, aggregate conflict, lifecycle
/// bug) propagates so the apalis job retries instead of marking the fill
/// Done: with the backfill checkpoint already advanced, a swallowed
/// witness failure is a permanently lost fill and silent unhedged
/// exposure.
pub(crate) async fn execute_witness_trade(
    onchain_trade: &Store<OnChainTrade>,
    trade: &OnchainTrade,
    block_number: u64,
    block_hash: Option<B256>,
    block_timestamp: DateTime<Utc>,
) -> Result<bool, SendError<OnChainTrade>> {
    let trade_id = OnChainTradeId {
        tx_hash: trade.tx_hash,
        log_index: trade.log_index,
    };

    let amount = trade.amount.inner();
    let price_usdc = trade.price.value();

    let command = OnChainTradeCommand::Witness {
        symbol: trade.symbol.base().clone(),
        amount,
        direction: trade.direction,
        price_usdc,
        block_number,
        block_hash,
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
            Ok(true)
        }
        // A Witness command on an existing aggregate can only be rejected with
        // AlreadyFilled (AlreadyEnriched is only ever produced by an Enrich
        // command), so that is the one duplicate-skip case here.
        Err(AggregateError::UserError(LifecycleError::Apply(OnChainTradeError::AlreadyFilled))) => {
            warn!(
                tx_hash = ?trade.tx_hash,
                log_index = trade.log_index,
                symbol = %trade.symbol,
                "OnChainTrade::Witness rejected as duplicate: already filled"
            );
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

pub(crate) async fn execute_enrich_trade(
    onchain_trade: &Store<OnChainTrade>,
    trade: &OnchainTrade,
) {
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

/// Drives `Position::AcknowledgeOnChainFill`, treating the idempotent
/// re-drive as success rather than an error.
///
/// Returns `Ok(())` when the position reflects the fill -- either this call
/// applied it or a previous attempt already did (`DuplicateTrade`, the
/// resume path's crash window between the position write and the
/// acknowledgement marker). Every other error propagates so the apalis job
/// retries instead of silently dropping the fill.
pub(crate) async fn execute_acknowledge_fill(
    position: &Store<Position>,
    trade: &OnchainTrade,
    threshold: ExecutionThreshold,
    block_timestamp: DateTime<Utc>,
) -> Result<(), SendError<Position>> {
    let base_symbol = trade.symbol.base();

    let amount = trade.amount.inner();
    let price_usdc = trade.price.value();

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
        Ok(()) => {
            debug!(
                tx_hash = ?trade.tx_hash,
                log_index = trade.log_index,
                symbol = %trade.symbol,
                "Successfully executed Position::AcknowledgeOnChainFill command"
            );
            Ok(())
        }
        Err(AggregateError::UserError(LifecycleError::Apply(PositionError::DuplicateTrade {
            ref trade_id,
        }))) => {
            info!(
                tx_hash = ?trade.tx_hash,
                log_index = trade.log_index,
                symbol = %trade.symbol,
                %trade_id,
                "Position already applied this fill; resuming without re-counting"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Outcome of the acknowledge marker write on the `OnChainTrade` aggregate.
pub(crate) enum FillMarkOutcome {
    /// The acknowledgement marker is durable -- freshly written, or already
    /// present from a prior attempt (an idempotent resume). The fill may be
    /// settled and hedged.
    Marked,
    /// A reorg invalidated this fill between the `process_queued_trade`
    /// reorg-skip guard and the marker write, so `Acknowledge` was rejected with
    /// `CannotAcknowledgeReorgedFill`. The marker is NOT durable: the fill must
    /// not be settled (its marker-durable precondition is unmet) or hedged
    /// (a broker order for a fill that no longer exists on the canonical chain
    /// is wrong-way exposure). The position impact the acknowledge already
    /// applied is left for the reorg reversal flow (built across the rest of the
    /// reorg stack) to reverse.
    Reorged,
}

/// Marks the trade acknowledged on the `OnChainTrade` aggregate after the
/// position write succeeded, completing the exactly-once pair (ADR 0005).
/// A re-driven marker (`AlreadyAcknowledged`) is idempotent and reports
/// [`FillMarkOutcome::Marked`]; a fill reorged mid-pair reports
/// [`FillMarkOutcome::Reorged`] so the caller skips settle and hedge;
/// infrastructure errors propagate so the apalis retry re-drives the
/// idempotent acknowledge pair.
pub(crate) async fn execute_mark_acknowledged(
    onchain_trade: &Store<OnChainTrade>,
    trade_id: &OnChainTradeId,
) -> Result<FillMarkOutcome, SendError<OnChainTrade>> {
    match onchain_trade
        .send(trade_id, OnChainTradeCommand::Acknowledge)
        .await
    {
        // `AlreadyAcknowledged`: a prior attempt already acknowledged (resume).
        Ok(())
        | Err(AggregateError::UserError(LifecycleError::Apply(
            OnChainTradeError::AlreadyAcknowledged,
        ))) => Ok(FillMarkOutcome::Marked),
        // The fill was reorged between the `process_queued_trade` reorg-skip
        // guard and this send. Surface it so the caller terminates cleanly
        // without settling or hedging an invalidated fill, instead of swallowing
        // it as success and letting the pipeline place a broker hedge.
        Err(AggregateError::UserError(LifecycleError::Apply(
            OnChainTradeError::CannotAcknowledgeReorgedFill,
        ))) => Ok(FillMarkOutcome::Reorged),
        Err(error) => Err(error),
    }
}

/// Prunes the fill from the position's pending-acknowledgement set once its
/// `OnChainTrade` marker is durable, completing the exactly-once sequence
/// (ADR 0010). Pruning a trade not in the set emits no events (a no-op), so a
/// re-driven settle is idempotent; infrastructure errors propagate so the
/// apalis retry re-drives the settle until the entry self-heals.
pub(crate) async fn execute_settle_fill(
    position: &Store<Position>,
    trade: &OnchainTrade,
) -> Result<(), SendError<Position>> {
    let base_symbol = trade.symbol.base();

    let command = PositionCommand::SettleOnChainFill {
        trade_id: TradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        },
    };

    position.send(base_symbol, command).await
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

    // A fill we cannot timestamp can be neither witnessed nor acknowledged.
    // Fail loudly so the apalis retry surfaces it as an operator-visible
    // failure instead of completing the job and dropping the fill silently.
    // The timestamp is baked into the job payload, so retries cannot resolve
    // it -- they burn the (small) retry budget before the terminal-failure
    // alert fires, which is an acceptable cost for not silently losing a fill.
    let Some(block_timestamp) = trade.block_timestamp else {
        error!(
            ?trade_id,
            symbol = %trade.symbol,
            "Missing block_timestamp; cannot account for fill"
        );
        return Err(TradeAccountingError::MissingBlockTimestamp {
            trade_id: trade_id.clone(),
        });
    };

    // The dedupe is step-grained (ADR 0005): only a trade the position
    // has acknowledged is fully processed. A trade that exists but is
    // not acknowledged means a previous delivery died between the
    // witness and acknowledge writes -- resume the acknowledge pair
    // instead of skipping the fill into permanent loss.
    match cqrs.onchain_trade.load(&trade_id).await {
        Ok(Some(state)) if state.is_acknowledged() => {
            debug!(
                ?trade_id,
                symbol = %trade.symbol,
                "Trade already processed (duplicate event), skipping"
            );
            // Self-heal a marker-without-settle leak (ADR 0010): a crash
            // between MARK and SETTLE leaves the trade marked but still in
            // the pending set. The marker is durable, so prune it now. A
            // no-op when already pruned.
            execute_settle_fill(&cqrs.position, &trade).await?;
            return Ok(None);
        }
        // A reorg invalidated this fill's block before it was acknowledged.
        // Skip it as terminal: applying its position impact would account for a
        // fill that no longer exists, and `Acknowledge` now rejects a reorged
        // trade -- a retried job would otherwise apply impact then fail the
        // acknowledge and retry forever.
        Ok(Some(state)) if state.is_reorged() => {
            warn!(
                ?trade_id,
                symbol = %trade.symbol,
                "Trade was reorged before acknowledgement; skipping fill accounting"
            );
            return Ok(None);
        }
        Ok(Some(state)) => {
            info!(
                ?trade_id,
                symbol = %trade.symbol,
                "Trade witnessed but not acknowledged; resuming fill accounting"
            );

            // A crash in the witness->enrich window leaves the trade
            // witnessed but un-enriched. Enrich now so the acknowledged
            // trade is not permanently stuck with `enrichment: None` --
            // mirroring the fresh-trade path. Best-effort: enrichment is
            // observability data, not financial state.
            if !state.is_enriched() {
                execute_enrich_trade(&cqrs.onchain_trade, &trade).await;
            }
        }
        Ok(None) => {
            let witnessed = execute_witness_trade(
                &cqrs.onchain_trade,
                &trade,
                trade_event.block_number,
                trade_event.block_hash,
                block_timestamp,
            )
            .await?;

            if !witnessed {
                return Ok(None);
            }

            execute_enrich_trade(&cqrs.onchain_trade, &trade).await;
        }
        // A failed read must not masquerade as "not a duplicate": the
        // witness write would also fail on a healthy dedupe path, but
        // propagating here keeps the retry contract explicit.
        Err(error) => return Err(error.into()),
    }

    execute_acknowledge_fill(
        &cqrs.position,
        &trade,
        cqrs.execution_threshold,
        block_timestamp,
    )
    .await?;
    // A reorg can land between the load-time skip guard above and this marker
    // write. When it does, the acknowledge marker is not durable, so the fill
    // must not be settled or hedged -- placing a broker order for a fill that no
    // longer exists on the canonical chain is wrong-way exposure. Bail before
    // settle and hedge; the position impact the acknowledge above applied is left
    // for the reorg reversal flow (built across the rest of the reorg stack) to
    // reverse.
    if matches!(
        execute_mark_acknowledged(&cqrs.onchain_trade, &trade_id).await?,
        FillMarkOutcome::Reorged
    ) {
        warn!(
            ?trade_id,
            symbol = %trade.symbol,
            "Fill reorged during the acknowledge pair; skipping settle and hedge"
        );
        return Ok(None);
    }
    // Prune the pending-acknowledgement entry now that the marker is durable
    // (ADR 0010), before any early return below, so the threshold-not-met and
    // placement-skip paths still settle.
    execute_settle_fill(&cqrs.position, &trade).await?;

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
        // `PendingExecution`: the position is already claimed by a prior
        // placement attempt that may not have completed (e.g. it placed the
        // broker order but crashed/was retried before enqueuing the poll).
        // Reconcile that claimed order instead of dropping it -- otherwise it
        // sits `Submitted`-but-unpolled until the next restart. Mirrors the
        // hedge job's `recover_pending_poll_status`.
        return recover_claimed_offchain_order(execution, cqrs).await;
    }

    execute_create_offchain_order(execution, cqrs, offchain_order_id).await?;

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

/// Recovers the order a position is already claimed by when a fresh placement
/// hits `PendingExecution`. A prior attempt claimed the position but may not
/// have finished: re-enqueue the poll if the order reached the broker
/// (`Submitted`/`PartiallyFilled`), or re-drive the idempotent placement if it
/// is still `Pending`. Symmetric to the hedge job's `recover_pending_poll_status`
/// so a stuck order is recovered at runtime instead of sitting unpolled until
/// the next restart. Terminal or absent orders propagate their outcome into the
/// position via `resolve_terminal_claimed_order`, clearing the stale claim
/// in-process instead of waiting for the next `CheckPositions` sweep.
async fn recover_claimed_offchain_order(
    execution: &ExecutionCtx,
    cqrs: &TradeProcessingCqrs,
) -> Result<Option<OffchainOrderId>, TradeAccountingError> {
    use OffchainOrder::{Failed, Filled, PartiallyFilled, Pending, Submitted};

    let Some(pending_id) = cqrs
        .position
        .load(&execution.symbol)
        .await?
        .and_then(|position| position.pending_offchain_order_id)
    else {
        return Ok(None);
    };

    let order = cqrs.offchain_order.load(&pending_id).await?;

    if resolve_terminal_claimed_order(
        &cqrs.position,
        &execution.symbol,
        pending_id,
        order.as_ref(),
    )
    .await?
    {
        return Ok(None);
    }

    match order {
        Some(Submitted { .. } | PartiallyFilled { .. }) => {
            cqrs.poll_status_queue
                .clone()
                .push(PollOrderStatus {
                    offchain_order_id: pending_id,
                })
                .await
                .inspect_err(|error| {
                    error!(
                        offchain_order_id = %pending_id,
                        symbol = %execution.symbol,
                        %error,
                        "Failed to re-enqueue PollOrderStatus for a claimed pending order"
                    );
                })?;
            Ok(Some(pending_id))
        }
        Some(Pending {
            symbol,
            shares,
            direction,
            executor,
            ..
        }) => {
            let anchor = cqrs
                .position
                .load(&symbol)
                .await?
                .and_then(|position| position.last_failed_offchain_order_id);
            let client_order_id = client_order_id_for_placement(pending_id, anchor);

            let placed = place_offchain_order_at_broker(
                &cqrs.offchain_order,
                cqrs.order_placer.as_ref(),
                &pending_id,
                symbol,
                shares,
                direction,
                executor,
                client_order_id,
            )
            .await?;

            dispatch_post_place_state(placed, execution, cqrs, pending_id).await
        }
        // Terminal and absent orders were resolved by
        // `resolve_terminal_claimed_order` above.
        Some(Filled { .. } | Failed { .. }) | None => Ok(None),
    }
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

        // No order exists after a successful Place -- a serious inconsistency,
        // since the aggregate should have been created. Report no order placed
        // so the caller does not treat a phantom id as real, and clear the
        // position pending state (there is no order to track).
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

        // With the pure `Place` handler, `Place` only records intent (entering
        // `Pending`) and the durable placement path commits the broker outcome
        // separately. `place_offchain_order_at_broker` returns only once the
        // order has left `Pending`, and a commit failure propagates out of
        // `execute_create_offchain_order` before we reach here -- so `Pending` or
        // `Filled` at this point is unexpected. Surface it as a retryable error
        // rather than clearing the position claim, which would strand a
        // possibly-live broker order.
        Some(state @ (Pending { .. } | Filled { .. })) => {
            Err(TradeAccountingError::UnexpectedPostPlaceState {
                offchain_order_id,
                state,
            })
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
) -> Result<(), TradeAccountingError> {
    // Derive the broker-side client_order_id from the live position aggregate
    // (read after PlaceOffChainOrder claimed it), reusing a prior failed
    // attempt's stashed OffchainOrderId as the idempotency anchor so the broker
    // dedupes the retry. Fail fast on a load error rather than placing under a
    // fresh key: a transient load failure while an anchor exists would submit a
    // SECOND live broker order for shares the prior attempt already placed (the
    // anchor exists precisely to dedupe that retry) -- a double-hedge the
    // fail-fast rule exists to prevent. Retrying the job reloads the anchor.
    let anchor = cqrs
        .position
        .load(&execution.symbol)
        .await
        .inspect_err(|error| {
            error!(
                %offchain_order_id,
                symbol = %execution.symbol,
                %error,
                "Failed to load position for the idempotency anchor; failing the \
                 job so a retry reloads it before placing"
            );
        })?
        .and_then(|position| position.last_failed_offchain_order_id);
    let client_order_id = client_order_id_for_placement(offchain_order_id, anchor);

    // Propagate a placement failure instead of swallowing it. If the broker
    // accepted the order but committing `MarkAccepted` failed, the aggregate is
    // left `Pending` with a live broker order; returning the error fails the
    // accounting job so apalis retries, the position claim is preserved, and the
    // idempotent re-drive (or startup orphan recovery) reconciles it. Swallowing
    // here would let the caller observe `Pending` and force-fail the position,
    // stranding the live broker order.
    place_offchain_order_at_broker(
        &cqrs.offchain_order,
        cqrs.order_placer.as_ref(),
        &offchain_order_id,
        execution.symbol.clone(),
        execution.shares,
        execution.direction,
        execution.executor,
        client_order_id,
    )
    .await
    .inspect(|_| {
        debug!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "OffchainOrder placement completed"
        );
    })
    .inspect_err(|error| {
        error!(
            %offchain_order_id,
            symbol = %execution.symbol,
            %error,
            "OffchainOrder placement failed"
        );
    })?;

    Ok(())
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
        order_placer,
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
                // Financial-integrity: never place under a fresh idempotency key when the
                // anchor cannot be read. A stashed `last_failed_offchain_order_id` may
                // front a live broker order, so a fresh key would double-hedge (this is
                // why `execute_create_offchain_order` fails fast here). The position is
                // already claimed above but no OffchainOrder aggregate exists yet, so the
                // runtime orphaned-pending recovery (ADR 0014) clears the stale claim and
                // a later CheckPositions scan re-attempts the placement.
                warn!(
                    %offchain_order_id,
                    symbol = %execution.symbol,
                    %error,
                    "Failed to load position for the idempotency anchor; deferring \
                     placement to runtime recovery instead of using a fresh key"
                );
                continue;
            }
        };
        let client_order_id = client_order_id_for_placement(offchain_order_id, anchor);

        let place_result = place_offchain_order_at_broker(
            offchain_order,
            order_placer,
            &offchain_order_id,
            execution.symbol.clone(),
            execution.shares,
            execution.direction,
            execution.executor,
            client_order_id,
        )
        .await;

        let mut broker_rejected_immediately = false;

        match &place_result {
            Ok(Some(OffchainOrder::Failed { error, .. })) => {
                broker_rejected_immediately = true;
                position
                    .send(
                        &execution.symbol,
                        PositionCommand::FailOffChainOrder {
                            offchain_order_id,
                            error: error.clone(),
                        },
                    )
                    .await?;
            }
            Ok(_) => debug!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "OffchainOrder placement completed"
            ),
            Err(error) => error!(
                %offchain_order_id,
                symbol = %execution.symbol,
                "OffchainOrder placement failed: {error}"
            ),
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
    use sqlx::{ConnectOptions, SqlitePool};
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};
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
    use crate::equity_redemption::{EquityRedemptionCommand, redemption_aggregate_id};
    use crate::inventory::view::Operator;
    use crate::inventory::{ImbalanceThreshold, Inventory, InventoryView, Venue};
    use crate::offchain::order::OrderPlacementResult;
    use crate::onchain::trade::OnchainTrade;
    use crate::rebalancing::equity::{
        ResumeTokenizationAggregate, ResumeTokenizationJobQueue, ResumeTokenizationTarget,
        TransferEquityToHedging, TransferEquityToMarketMaking,
    };
    use crate::rebalancing::{RebalancingSchedulers, RebalancingService};
    use crate::test_utils::{
        OnchainTradeBuilder, get_test_log, get_test_order, rebalancing_enabled_equities,
        setup_test_db, setup_test_pools,
    };
    use crate::tokenization::mock::MockTokenizer;
    use crate::tokenized_equity_mint::{
        TokenizationRequestId, TokenizedEquityMintCommand, issuer_request_id,
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
            telemetry_writer: tokio::spawn(pending::<()>()),
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

    async fn insert_job_row(
        apalis_pool: &apalis_sqlite::SqlitePool,
        id: &str,
        job_type: &str,
        status: Status,
        attempts: i64,
        max_attempts: i64,
    ) {
        sqlx_apalis::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority) \
             VALUES (?, ?, ?, ?, ?, ?, 0, 0)",
        )
        .bind(vec![0_u8])
        .bind(id)
        .bind(job_type)
        .bind(status.to_string())
        .bind(attempts)
        .bind(max_attempts)
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

    struct InterruptedAggregateFixture {
        pool: sqlx::SqlitePool,
        apalis_pool: apalis_sqlite::SqlitePool,
        mint_id: crate::tokenized_equity_mint::IssuerRequestId,
        redemption_id: crate::equity_redemption::RedemptionAggregateId,
        tokenizer: Arc<crate::tokenization::mock::MockTokenizer>,
        rebalancing_service: RebalancingService,
        inventory: Arc<BroadcastingInventory>,
        resume_queue: crate::rebalancing::equity::ResumeTokenizationJobQueue,
    }

    /// Shared setup for the three `recover_interrupted_tokenization_aggregates`
    /// tests. Seeds one mint (MintAccepted state) and one redemption
    /// (VaultWithdrawPending state) into an in-memory database, then builds the
    /// `RebalancingService` and `ResumeTokenizationJobQueue` that the recovery
    /// function requires.
    async fn seed_interrupted_aggregates_and_build_service(
        wallet_byte: u8,
        mint_label: &str,
        redemption_label: &str,
    ) -> InterruptedAggregateFixture {
        let (pool, apalis_pool) = setup_test_pools().await;

        let mint_id = issuer_request_id(mint_label);
        let redemption_id = redemption_aggregate_id(redemption_label);

        let tokenizer = Arc::new(MockTokenizer::new());

        let seeding_mint_store = Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ()));
        let seeding_redemption_store = Arc::new(test_store::<EquityRedemption>(pool.clone(), ()));

        seeding_mint_store
            .send(
                &mint_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: mint_id.clone(),
                    symbol: st0x_execution::Symbol::new("AAPL").unwrap(),
                    quantity: st0x_float_macro::float!(10.0),
                    wallet: alloy::primitives::Address::from([wallet_byte; 20]),
                    tokenization_request_id: TokenizationRequestId(
                        "TOK-interrupted-mint".to_string(),
                    ),
                },
            )
            .await
            .unwrap();

        seeding_redemption_store
            .send(
                &redemption_id,
                EquityRedemptionCommand::Redeem {
                    symbol: st0x_execution::Symbol::new("AAPL").unwrap(),
                    quantity: st0x_float_macro::float!(5.0),
                    token: alloy::primitives::Address::from([wallet_byte; 20]),
                    amount: alloy::primitives::U256::from(5_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            crate::inventory::InventoryView::default(),
            event_sender,
        ));
        let vault_registry: Arc<Store<VaultRegistry>> = Arc::new(test_store(pool.clone(), ()));
        let rebalancing_service = RebalancingService::new(
            RebalancingServiceConfig {
                equity: crate::inventory::ImbalanceThreshold {
                    target: st0x_float_macro::float!(0.5),
                    deviation: st0x_float_macro::float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(60),
                assets: AssetsConfig {
                    equities: rebalancing_enabled_equities(&["AAPL"]),
                    cash: None,
                },
            },
            vault_registry,
            alloy::primitives::Address::ZERO,
            alloy::primitives::Address::ZERO,
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        );

        let resume_queue = ResumeTokenizationJobQueue::new(&apalis_pool);

        InterruptedAggregateFixture {
            pool,
            apalis_pool,
            mint_id,
            redemption_id,
            tokenizer,
            rebalancing_service,
            inventory,
            resume_queue,
        }
    }

    /// Regression: `recover_interrupted_tokenization_aggregates` must enqueue
    /// a `ResumeTokenizationAggregate` job for each interrupted aggregate and
    /// return immediately without calling any issuer (tokenizer) method.
    ///
    /// The previous implementation called `resume_interrupted_transfers` inline
    /// which awaited `poll_for_redemption` / `poll_mint_until_complete`, blocking
    /// startup for up to 30 minutes when the issuer was down.
    #[tokio::test]
    async fn issuer_down_does_not_block_tokenization_resume() {
        let InterruptedAggregateFixture {
            pool,
            apalis_pool,
            mint_id,
            redemption_id,
            tokenizer,
            rebalancing_service,
            inventory,
            mut resume_queue,
        } = seed_interrupted_aggregates_and_build_service(
            1,
            "test-interrupted-mint",
            "test-interrupted-redemption",
        )
        .await;

        // The recover function only calls store.load() (event replay); it does
        // not invoke tokenizer methods. Use the same services for the function
        // call -- MockTokenizer methods are never called during load().
        let mint_store = Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ()));
        let redemption_store = Arc::new(test_store::<EquityRedemption>(pool.clone(), ()));
        let calls_before = tokenizer.call_count();

        // Must complete in under 1 second: no issuer poll blocking.
        tokio::time::timeout(
            Duration::from_secs(1),
            recover_interrupted_tokenization_aggregates(
                &pool,
                &rebalancing_service,
                inventory.as_ref(),
                mint_store,
                redemption_store,
                &mut resume_queue,
            ),
        )
        .await
        .expect("recover_interrupted_tokenization_aggregates must not block on issuer")
        .expect("recover_interrupted_tokenization_aggregates must succeed");

        // Recovery must not invoke the issuer at all -- it only replays stored
        // events. Issuer calls happen later when the apalis worker runs the job.
        assert_eq!(
            tokenizer.call_count(),
            calls_before,
            "recover_interrupted_tokenization_aggregates must not call the issuer"
        );

        // Both interrupted aggregates must be enqueued -- assert the payloads
        // (one targeting the mint, one the redemption), not just the count, so a
        // regression that enqueued e.g. two mint jobs would be caught.
        let payloads: Vec<Vec<u8>> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<ResumeTokenizationAggregate>())
        .fetch_all(&apalis_pool)
        .await
        .unwrap();

        let targets: Vec<ResumeTokenizationTarget> = payloads
            .iter()
            .map(|job| {
                serde_json::from_slice::<ResumeTokenizationAggregate>(job)
                    .expect("queued resume job must deserialize")
                    .target
            })
            .collect();

        assert_eq!(
            targets.len(),
            2,
            "expected 2 pending ResumeTokenizationAggregate jobs (one mint + one redemption), \
             got {targets:?}"
        );
        assert!(
            targets.contains(&ResumeTokenizationTarget::Mint(mint_id.clone())),
            "a queued resume job must target the interrupted mint {mint_id}, got {targets:?}"
        );
        assert!(
            targets.contains(&ResumeTokenizationTarget::Redemption(redemption_id.clone())),
            "a queued resume job must target the interrupted redemption {redemption_id}, \
             got {targets:?}"
        );
    }

    /// `recover_interrupted_tokenization_aggregates` called twice (simulating a
    /// crash-restart cycle) must not accumulate duplicate pending jobs.
    #[tokio::test]
    async fn recover_interrupted_tokenization_aggregates_does_not_duplicate_jobs_on_restart() {
        let InterruptedAggregateFixture {
            pool,
            apalis_pool,
            mint_id: _,
            redemption_id: _,
            tokenizer: _,
            rebalancing_service,
            inventory,
            mut resume_queue,
        } = seed_interrupted_aggregates_and_build_service(
            2,
            "dup-test-mint",
            "dup-test-redemption",
        )
        .await;

        // First call -- simulates initial startup.
        recover_interrupted_tokenization_aggregates(
            &pool,
            &rebalancing_service,
            inventory.as_ref(),
            Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ())),
            Arc::new(test_store::<EquityRedemption>(pool.clone(), ())),
            &mut resume_queue,
        )
        .await
        .unwrap();

        let after_first = pending_job_count::<ResumeTokenizationAggregate>(&apalis_pool).await;
        assert_eq!(
            after_first, 2,
            "first call must enqueue exactly 2 pending jobs, got {after_first}"
        );

        // Second call -- simulates crash-restart. Must not add more pending rows.
        recover_interrupted_tokenization_aggregates(
            &pool,
            &rebalancing_service,
            inventory.as_ref(),
            Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ())),
            Arc::new(test_store::<EquityRedemption>(pool.clone(), ())),
            &mut resume_queue,
        )
        .await
        .unwrap();

        let after_second = pending_job_count::<ResumeTokenizationAggregate>(&apalis_pool).await;
        assert_eq!(
            after_second, 2,
            "second call (crash-restart) must not duplicate pending jobs,              expected 2, got {after_second}"
        );
    }

    /// Extension of the above: a crash mid-job leaves a `Running` row (not
    /// `Pending`). `cancel_all_pending` alone cannot clean it up; the orphan
    /// must be promoted to `Pending` first and then cancelled. Without the
    /// `requeue_orphaned` call inside the function, the `Running` row would
    /// survive `cancel_all_pending`, then get promoted by `requeue_orphaned`
    /// later (in the old startup ordering), creating a duplicate `Pending` row.
    #[tokio::test]
    async fn recover_interrupted_tokenization_aggregates_does_not_duplicate_running_orphan() {
        let InterruptedAggregateFixture {
            pool,
            apalis_pool,
            mint_id: _,
            redemption_id: _,
            tokenizer: _,
            rebalancing_service,
            inventory,
            mut resume_queue,
        } = seed_interrupted_aggregates_and_build_service(
            3,
            "running-orphan-mint",
            "running-orphan-redemption",
        )
        .await;

        // First call: simulate a previous startup that enqueued jobs.
        recover_interrupted_tokenization_aggregates(
            &pool,
            &rebalancing_service,
            inventory.as_ref(),
            Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ())),
            Arc::new(test_store::<EquityRedemption>(pool.clone(), ())),
            &mut resume_queue,
        )
        .await
        .unwrap();

        // Simulate a crash mid-job: force both Pending rows to Running state.
        let job_type = std::any::type_name::<ResumeTokenizationAggregate>();
        sqlx_apalis::query("UPDATE Jobs SET status = ?, lock_at = 1 WHERE job_type = ?")
            .bind(Status::Running.to_string())
            .bind(job_type)
            .execute(&apalis_pool)
            .await
            .unwrap();

        // Second call: must not create duplicate Pending rows even though the
        // previous rows are in Running state (not Pending).
        recover_interrupted_tokenization_aggregates(
            &pool,
            &rebalancing_service,
            inventory.as_ref(),
            Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ())),
            Arc::new(test_store::<EquityRedemption>(pool.clone(), ())),
            &mut resume_queue,
        )
        .await
        .unwrap();

        let after_restart = pending_job_count::<ResumeTokenizationAggregate>(&apalis_pool).await;
        assert_eq!(
            after_restart, 2,
            "crash-restart with Running orphan must not duplicate pending jobs,              expected 2, got {after_restart}"
        );
    }

    /// Integration test: `recover_interrupted_tokenization_aggregates` must NOT
    /// enqueue a resume job for a mint in a pre-wrap state (`TokensReceived`)
    /// when `wrapped_equity_recovery` is enabled. `recover_mint_state` sets the
    /// guard to `HeldForRecovery` automatically, and `is_pre_wrap_held_for_recovery`
    /// then gates the push. `UnwrappedEquityRecovery` owns those aggregates and
    /// would race with a concurrent resume. Control case: same state with
    /// recovery disabled (guard stays `ActiveTransfer`) DOES produce a job.
    #[tokio::test]
    async fn recover_interrupted_tokenization_aggregates_excludes_held_for_recovery_pre_wrap_mint()
    {
        let symbol = Symbol::new("AAPL").unwrap();

        // Helper that builds a single-symbol EquitiesConfig with a configurable
        // wrapped_equity_recovery mode.
        let make_equities_config = |wrapped_equity_recovery_mode: OperationMode| EquitiesConfig {
            operational_limit: None,
            symbols: std::iter::once((
                symbol.clone(),
                EquityAssetConfig {
                    tokenized_equity: alloy::primitives::Address::ZERO,
                    tokenized_equity_derivative: alloy::primitives::Address::ZERO,
                    pyth_feed_id: None,
                    vault_ids: Vec::new(),
                    trading: OperationMode::Disabled,
                    rebalancing: OperationMode::Enabled,
                    wrapped_equity_recovery: wrapped_equity_recovery_mode,
                    operational_limit: None,
                },
            ))
            .collect(),
        };

        // --- HeldForRecovery case: zero jobs expected ---
        // With wrapped_equity_recovery ENABLED, recover_mint_state sets the guard
        // to HeldForRecovery automatically for TokensReceived state, so
        // is_pre_wrap_held_for_recovery blocks the resume push.
        let (pool, apalis_pool) = setup_test_pools().await;
        let mint_id = issuer_request_id("pre-wrap-held-mint");

        let seeding_mint_store = Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ()));

        // RecordMintRequested -> MintAccepted state.
        // RecordTokensReceived -> TokensReceived state (pre-wrap).
        seeding_mint_store
            .send(
                &mint_id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: mint_id.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5.0),
                    wallet: alloy::primitives::Address::from([3u8; 20]),
                    tokenization_request_id: TokenizationRequestId("TOK-pre-wrap-held".to_string()),
                },
            )
            .await
            .unwrap();

        seeding_mint_store
            .send(
                &mint_id,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(fixed_bytes!(
                        "0x1111111111111111111111111111111111111111111111111111111111111111"
                    )),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .unwrap();

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            crate::inventory::InventoryView::default(),
            event_sender,
        ));
        let vault_registry: Arc<Store<VaultRegistry>> = Arc::new(test_store(pool.clone(), ()));
        let rebalancing_service = RebalancingService::new(
            RebalancingServiceConfig {
                equity: crate::inventory::ImbalanceThreshold {
                    target: st0x_float_macro::float!(0.5),
                    deviation: st0x_float_macro::float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(60),
                assets: AssetsConfig {
                    // wrapped_equity_recovery ENABLED: recover_mint_state will set
                    // HeldForRecovery on TokensReceived, blocking the resume push.
                    equities: make_equities_config(OperationMode::Enabled),
                    cash: None,
                },
            },
            vault_registry,
            alloy::primitives::Address::ZERO,
            alloy::primitives::Address::ZERO,
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        );

        let mint_store = Arc::new(test_store::<TokenizedEquityMint>(pool.clone(), ()));
        let redemption_store = Arc::new(test_store::<EquityRedemption>(pool.clone(), ()));
        let mut resume_queue = ResumeTokenizationJobQueue::new(&apalis_pool);

        recover_interrupted_tokenization_aggregates(
            &pool,
            &rebalancing_service,
            inventory.as_ref(),
            mint_store,
            redemption_store,
            &mut resume_queue,
        )
        .await
        .unwrap();

        let held_jobs = pending_job_count::<ResumeTokenizationAggregate>(&apalis_pool).await;
        assert_eq!(
            held_jobs, 0,
            "HeldForRecovery + TokensReceived must be excluded from resume jobs, \
             got {held_jobs} pending jobs"
        );

        // --- Control case: recovery DISABLED keeps ActiveTransfer, job IS enqueued ---
        let (pool2, apalis_pool2) = setup_test_pools().await;
        let mint_id2 = issuer_request_id("pre-wrap-active-mint");

        let seeding_mint_store2 = Arc::new(test_store::<TokenizedEquityMint>(pool2.clone(), ()));

        seeding_mint_store2
            .send(
                &mint_id2,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: mint_id2.clone(),
                    symbol: symbol.clone(),
                    quantity: float!(5.0),
                    wallet: alloy::primitives::Address::from([4u8; 20]),
                    tokenization_request_id: TokenizationRequestId(
                        "TOK-pre-wrap-active".to_string(),
                    ),
                },
            )
            .await
            .unwrap();

        seeding_mint_store2
            .send(
                &mint_id2,
                TokenizedEquityMintCommand::RecordTokensReceived {
                    tx_hash: Some(fixed_bytes!(
                        "0x2222222222222222222222222222222222222222222222222222222222222222"
                    )),
                    token_symbol: Some(format!("t{symbol}")),
                    fees: None,
                },
            )
            .await
            .unwrap();

        let (event_sender2, _) = broadcast::channel::<Statement>(16);
        let inventory2 = Arc::new(BroadcastingInventory::new(
            crate::inventory::InventoryView::default(),
            event_sender2.clone(),
        ));
        let vault_registry2: Arc<Store<VaultRegistry>> = Arc::new(test_store(pool2.clone(), ()));
        let rebalancing_service2 = RebalancingService::new(
            RebalancingServiceConfig {
                equity: crate::inventory::ImbalanceThreshold {
                    target: st0x_float_macro::float!(0.5),
                    deviation: st0x_float_macro::float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(60),
                assets: AssetsConfig {
                    // wrapped_equity_recovery DISABLED: recover_mint_state keeps
                    // ActiveTransfer, so the pre-wrap exclusion does NOT fire.
                    equities: make_equities_config(OperationMode::Disabled),
                    cash: None,
                },
            },
            vault_registry2,
            alloy::primitives::Address::ZERO,
            alloy::primitives::Address::ZERO,
            inventory2.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool2),
            Arc::new(crate::alerts::NoopNotifier),
        );

        let mint_store2 = Arc::new(test_store::<TokenizedEquityMint>(pool2.clone(), ()));
        let redemption_store2 = Arc::new(test_store::<EquityRedemption>(pool2.clone(), ()));
        let mut resume_queue2 = ResumeTokenizationJobQueue::new(&apalis_pool2);

        recover_interrupted_tokenization_aggregates(
            &pool2,
            &rebalancing_service2,
            inventory2.as_ref(),
            mint_store2,
            redemption_store2,
            &mut resume_queue2,
        )
        .await
        .unwrap();

        let active_jobs = pending_job_count::<ResumeTokenizationAggregate>(&apalis_pool2).await;
        assert_eq!(
            active_jobs, 1,
            "ActiveTransfer (recovery disabled) + TokensReceived must produce 1 resume job, \
             got {active_jobs}"
        );
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

        // The lock owner must exist: Jobs.lock_by is a foreign key into Workers.
        sqlx_apalis::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES (?, 'test', 'test')",
        )
        .bind("order-fill-worker-0")
        .execute(&apalis_pool)
        .await
        .unwrap();

        // Insert with a real lock owner, exactly as apalis leaves a crashed
        // Running row, so clearing lock_by/lock_at is actually exercised --
        // asserting lock_by is None afterwards would pass vacuously otherwise.
        sqlx_apalis::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority, lock_by, lock_at) \
             VALUES (?, ?, ?, ?, 1, 25, 0, 0, ?, ?)",
        )
        .bind(vec![0_u8])
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(job_type)
        .bind(Status::Running.to_string())
        .bind("order-fill-worker-0")
        .bind(0_i64)
        .execute(&apalis_pool)
        .await
        .unwrap();

        requeue_trading_orphans(&queue, "trade accounting")
            .await
            .unwrap();

        let (status, lock_by, attempts): (String, Option<String>, i64) =
            sqlx_apalis::query_as("SELECT status, lock_by, attempts FROM Jobs WHERE job_type = ?")
                .bind(job_type)
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(status, Status::Pending.to_string());
        assert_eq!(lock_by, None);
        assert_eq!(attempts, 1, "requeue must preserve the attempt count");
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

    #[tokio::test]
    async fn spawn_finished_job_cleanup_retains_usdc_transfer_rows() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let hedging_type = std::any::type_name::<TransferUsdcToHedging>();
        let market_making_type = std::any::type_name::<TransferUsdcToMarketMaking>();

        // Non-transfer finished rows must be pruned (Done and exhausted Failed).
        insert_job_row(&apalis_pool, "other-done", "test", Status::Done, 1, 25).await;
        insert_job_row(&apalis_pool, "other-failed", "test", Status::Failed, 25, 25).await;

        // USDC transfer finished rows must survive cleanup: they are the durable
        // startup re-arm idempotency + redrive-budget signal.
        insert_job_row(
            &apalis_pool,
            "hedging-done",
            hedging_type,
            Status::Done,
            1,
            25,
        )
        .await;
        insert_job_row(
            &apalis_pool,
            "market-making-failed",
            market_making_type,
            Status::Failed,
            25,
            25,
        )
        .await;

        let handle =
            spawn_finished_job_cleanup(pool.clone(), apalis_pool.clone(), Duration::from_secs(60));

        // The two non-transfer finished rows are pruned; the two transfer rows remain.
        wait_for_job_count(&apalis_pool, 2).await;
        handle.abort();

        let mut remaining: Vec<String> = sqlx_apalis::query_scalar("SELECT job_type FROM Jobs")
            .fetch_all(&apalis_pool)
            .await
            .unwrap();
        remaining.sort();
        let mut expected = vec![hedging_type.to_string(), market_making_type.to_string()];
        expected.sort();
        assert_eq!(
            remaining, expected,
            "USDC transfer finished rows must survive cleanup regardless of Done/Failed status"
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

    async fn create_cqrs_frameworks(
        pool: &SqlitePool,
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
                .build(())
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
            order_placer: succeeding_order_placer(),
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
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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

    /// The emitted log's block hash must thread end-to-end through ingestion
    /// into the witnessed fill, where reorg detection later reads it to tell a
    /// re-observation on a different fork from a duplicate. The aggregate is the
    /// system of record for the fill, so assert the hash survives there.
    #[tokio::test]
    async fn ingestion_threads_block_hash_into_witnessed_fill() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let block_hash = B256::from([0xcd; 32]);
        let mut trade_event = make_trade_event(70);
        trade_event.block_hash = Some(block_hash);

        let trade = test_trade_with_amount(float!(1.5), 70);
        let trade_id = OnChainTradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        };

        process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true)
            .await
            .unwrap()
            .expect("1.5 shares should witness and hedge the fill");

        let witnessed = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("the witnessed trade aggregate must exist after ingestion");

        assert_eq!(
            witnessed.block_hash,
            Some(block_hash),
            "the emitted log's block hash must thread into the witnessed fill"
        );
    }

    /// A database write lock during the Witness write must surface as a
    /// retryable error, not silently drop the fill. Pre-fix,
    /// `execute_witness_trade` blanket-matched every `SendError` as a
    /// domain rejection and returned `false`, so the accounting job was
    /// marked Done with the checkpoint already advanced -- a SQLITE_BUSY
    /// blip (the documented bot-vs-reporter contention scenario) became
    /// silent, permanently unhedged exposure. The lock is held by a
    /// second raw connection via `BEGIN IMMEDIATE`, exactly how another
    /// process contends for the WAL write lock; the pool's busy timeout
    /// is scaled down so the test waits milliseconds, not production's
    /// 10 seconds -- the surfacing code path is identical.
    #[tokio::test]
    async fn db_write_lock_during_witness_propagates_error_for_retry() {
        let (pool, apalis_pool, db_path, _dir) =
            crate::test_utils::setup_file_backed_test_db(Duration::from_millis(250)).await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let mut locker = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&db_path)
            .connect()
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut locker)
            .await
            .unwrap();

        let result = process_queued_trade(
            &MockExecutor::new(),
            &make_trade_event(40),
            test_trade_with_amount(float!(1.5), 40),
            &cqrs,
            true,
        )
        .await;

        assert!(
            matches!(result, Err(TradeAccountingError::OnChainTradeCommand(_))),
            "A write-locked database during Witness must propagate a \
             retryable error so apalis re-runs the job; got: {result:?}",
        );

        sqlx::query("ROLLBACK").execute(&mut locker).await.unwrap();

        // The apalis retry re-delivers the same job against the healthy
        // database; the fill must flow through exactly once.
        process_queued_trade(
            &MockExecutor::new(),
            &make_trade_event(40),
            test_trade_with_amount(float!(1.5), 40),
            &cqrs,
            true,
        )
        .await
        .unwrap()
        .expect("retry must place the hedge for the recovered fill");

        let (onchain_fills,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("OnChainTradeEvent::Filled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            onchain_fills, 1,
            "Exactly one witnessed fill after the lock cleared and the \
             retry ran"
        );

        // The witness count alone does not prove the fill reached the position.
        // Assert the full pipeline ran exactly once after recovery: one
        // position fill, the net updated, and a hedge placed.
        let (position_fills,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            position_fills, 1,
            "The recovered fill must reach the position exactly once, not just \
             the witness layer"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("the position must exist after the recovered fill");
        assert!(
            position.net.inner().eq(float!(1.5)).unwrap(),
            "The position net must reflect the recovered fill"
        );
        assert!(
            position.pending_offchain_order_id.is_some(),
            "The recovered fill must have placed a pending hedge"
        );
    }

    /// A failed dedup read (not just a failed write) must propagate as a
    /// retryable error rather than being mistaken for "not a duplicate" and
    /// re-processing the fill.
    #[tokio::test]
    async fn dedup_load_failure_propagates_as_retryable_error() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        // Close the pool so the dedup load -- the first DB access in
        // process_queued_trade -- fails before any write is attempted.
        pool.close().await;

        let result = process_queued_trade(
            &MockExecutor::new(),
            &make_trade_event(41),
            test_trade_with_amount(float!(1.5), 41),
            &cqrs,
            true,
        )
        .await;
        assert!(
            matches!(result, Err(TradeAccountingError::OnChainTradeCommand(_))),
            "A failed dedup read must propagate a retryable error, never be \
             treated as not-a-duplicate; got: {result:?}",
        );
    }

    /// Reproduction for the fill-loss half of the Witness/Acknowledge
    /// gap (ADR 0005): a crash between the Witness write and the
    /// Position acknowledge leaves a genuinely reachable production
    /// state -- the trade exists in the event store, the position never
    /// learned of the fill. The re-delivered accounting job (a real path
    /// since the startup orphan requeue) must complete the position
    /// update and place the hedge, not dedupe-skip and lose the fill
    /// forever. Both halves of the test run production code: the
    /// persisted prefix is the exact first write `process_queued_trade`
    /// makes, and the retry is `process_queued_trade` itself.
    #[tokio::test]
    async fn redelivery_after_crash_between_witness_and_acknowledge_recovers_fill() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );
        let symbol = Symbol::new("AAPL").unwrap();

        let trade_event = make_trade_event(60);
        let witnessing_trade = test_trade_with_amount(float!(1.5), 60);
        let block_timestamp = witnessing_trade
            .block_timestamp
            .expect("test trade carries a block timestamp");

        // The exact first write process_queued_trade makes -- and then
        // nothing: models the kill between the two aggregate writes.
        let witnessed = execute_witness_trade(
            &cqrs.onchain_trade,
            &witnessing_trade,
            trade_event.block_number,
            trade_event.block_hash,
            block_timestamp,
        )
        .await
        .unwrap();
        assert!(witnessed, "premise: the witness write must succeed");
        assert!(
            cqrs.position_projection
                .load(&symbol)
                .await
                .unwrap()
                .is_none(),
            "premise: the position must not have learned of the fill yet"
        );

        // The re-delivered job after restart.
        let recovered_hedge_id = process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            test_trade_with_amount(float!(1.5), 60),
            &cqrs,
            true,
        )
        .await
        .unwrap()
        .expect(
            "the re-delivered accounting job must place the hedge and return \
             its id, not dedupe-skip the fill and return Ok(None)",
        );

        let position = cqrs
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect(
                "the re-delivered job must drive the fill into the position \
                 instead of dedupe-skipping it into permanent loss",
            );
        assert!(
            position.net.inner().eq(float!(1.5)).unwrap(),
            "The fill must be accounted exactly once"
        );

        let orders = offchain_order_projection.load_all().await.unwrap();
        assert_eq!(
            orders.len(),
            1,
            "The recovered fill must hedge exactly once"
        );
        assert_eq!(
            orders[0].0, recovered_hedge_id,
            "process_queued_trade must return the id of the hedge it placed"
        );
        assert_eq!(
            position.pending_offchain_order_id,
            Some(recovered_hedge_id),
            "The position must track the recovered hedge"
        );

        let (position_fills,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            position_fills, 1,
            "Exactly one position fill event after recovery"
        );

        // The crash window this fix closes is the gap between the position
        // write and the acknowledgement marker. Proving the fill reached
        // the position is not enough: without the marker, every later
        // re-delivery re-enters the resume path and re-drives the fill,
        // which the position then rejects as a duplicate. Assert the marker
        // was actually written -- both as a persisted event and as
        // acknowledged state on the replayed aggregate.
        let (acknowledged_markers,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("OnChainTradeEvent::Acknowledged")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            acknowledged_markers, 1,
            "The resume path must write exactly one Acknowledged marker"
        );

        let witnessed_trade = test_trade_with_amount(float!(1.5), 60);
        let trade_id = OnChainTradeId {
            tx_hash: witnessed_trade.tx_hash,
            log_index: witnessed_trade.log_index,
        };
        let recovered_trade = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("the witnessed trade aggregate must exist after recovery");
        assert!(
            recovered_trade.is_acknowledged(),
            "The OnChainTrade must be marked acknowledged so the dedupe \
             treats the fill as fully processed on subsequent deliveries"
        );

        // A third delivery after recovery must be a true no-op: the marker
        // written above makes the dedupe short-circuit to `Ok(None)`
        // without re-driving the fill. Proving the marker exists is not
        // enough -- this proves it is read back and honored.
        let outcome = process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            test_trade_with_amount(float!(1.5), 60),
            &cqrs,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome, None,
            "a delivery after the marker is written must dedupe-skip, not re-drive"
        );

        let (position_fills_after,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            position_fills_after, 1,
            "the dedupe short-circuit must not write a second fill event"
        );
    }

    /// A crash in the witness->enrich window must not leave the fill
    /// permanently un-enriched. The resume path enriches before
    /// acknowledging when the trade carries enrichment data, so the
    /// recovered trade ends up both enriched and acknowledged -- enrichment
    /// stays best-effort (ADR 0005) but the resume path must not skip it
    /// when the data is present.
    #[tokio::test]
    async fn resume_path_enriches_trade_unenriched_at_crash() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(60);
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
            .with_log_index(60)
            .with_enrichment(50000, 1_000_000_000, pyth_price)
            .build();
        let block_timestamp = trade
            .block_timestamp
            .expect("test trade carries a block timestamp");
        let trade_id = OnChainTradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        };

        // Witness only -- model the crash before enrichment ran.
        let witnessed = execute_witness_trade(
            &cqrs.onchain_trade,
            &trade,
            trade_event.block_number,
            trade_event.block_hash,
            block_timestamp,
        )
        .await
        .unwrap();
        assert!(witnessed, "premise: the witness write must succeed");
        let witnessed_state = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("witnessed aggregate exists");
        assert!(
            !witnessed_state.is_enriched(),
            "premise: the crash left the trade un-enriched"
        );

        process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true)
            .await
            .unwrap();

        let recovered = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("the aggregate exists after recovery");
        assert!(
            recovered.is_enriched(),
            "the resume path must enrich a trade that crashed before enrichment"
        );
        assert!(
            recovered.is_acknowledged(),
            "the resume path must still acknowledge the recovered fill"
        );
    }

    #[tokio::test]
    async fn reorged_trade_skipped_by_resume_path() {
        // A trade reorged before acknowledgement must be skipped by the resume
        // path: applying its position impact would account for a fill that no
        // longer exists, and `Acknowledge` rejects a reorged trade -- a retried
        // job would otherwise apply impact and then loop on that rejection.
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(61);
        let trade = OnchainTradeBuilder::default()
            .with_symbol("wtAAPL")
            .with_equity_token(TEST_EQUITY_TOKEN)
            .with_amount(float!("1.5"))
            .with_log_index(61)
            .build();
        let block_timestamp = trade
            .block_timestamp
            .expect("test trade carries a block timestamp");
        let trade_id = OnChainTradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        };

        // Witness the fill, then reorg it before acknowledgement -- the window
        // where a stale AccountForDexTrade job is still queued.
        let witnessed = execute_witness_trade(
            &cqrs.onchain_trade,
            &trade,
            trade_event.block_number,
            trade_event.block_hash,
            block_timestamp,
        )
        .await
        .unwrap();
        assert!(witnessed, "premise: the witness write must succeed");
        cqrs.onchain_trade
            .send(
                &trade_id,
                OnChainTradeCommand::RecordReorg { reorg_depth: 3 },
            )
            .await
            .unwrap();

        let result = process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true)
            .await
            .expect("a reorged trade must be skipped cleanly, not error and retry");

        assert_eq!(result, None, "a reorged trade must enqueue no hedge");
        let state = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("the reorged aggregate still exists");
        assert!(
            !state.is_acknowledged(),
            "a reorged trade must not be acknowledged -- its impact is reversed via the reorg path",
        );
    }

    /// End-to-end assurance that a reorged fill is never hedged by the
    /// trade-accounting pipeline. The 1.5-share fill is above the hedging
    /// threshold, so absent the reorg `process_queued_trade` would place a
    /// broker hedge (cf. `trade_above_threshold_places_offchain_order`).
    /// Reorging its block before the queued job runs makes
    /// `process_queued_trade` hit its load-time reorg-skip guard and
    /// short-circuit before the acknowledge/settle/hedge path, so no offchain
    /// order is placed.
    ///
    /// This exercises the load-time guard, not the mid-pipeline race (a reorg
    /// landing between the load and the acknowledge-marker write), which cannot
    /// be injected deterministically here and is unit-covered by
    /// `execute_mark_acknowledged_signals_reorged_trade`. `MockExecutor` records
    /// no placed orders, so "not hedged" is asserted via the absent
    /// offchain-order id and the absence of any position impact / pending hedge.
    #[tokio::test]
    async fn process_queued_trade_does_not_hedge_a_reorged_fill() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(63);
        let trade = test_trade_with_amount(float!(1.5), 63);
        let block_timestamp = trade
            .block_timestamp
            .expect("test trade carries a block timestamp");
        let trade_id = OnChainTradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        };

        // Witness the above-threshold fill, then reorg its block away before the
        // queued AccountForDexTrade job runs.
        let witnessed = execute_witness_trade(
            &cqrs.onchain_trade,
            &trade,
            trade_event.block_number,
            trade_event.block_hash,
            block_timestamp,
        )
        .await
        .unwrap();
        assert!(witnessed, "premise: the witness write must succeed");
        cqrs.onchain_trade
            .send(
                &trade_id,
                OnChainTradeCommand::RecordReorg { reorg_depth: 3 },
            )
            .await
            .unwrap();

        let result = process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true)
            .await
            .expect("a reorged fill must be skipped cleanly, not error and retry");

        assert_eq!(
            result, None,
            "a reorged fill must not be hedged -- no offchain order id is returned"
        );

        // The skip short-circuits before the acknowledge/settle/hedge path, so
        // the position is never created and no offchain hedge is pending.
        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap();
        assert!(
            position.is_none(),
            "a reorged fill must short-circuit before the hedge path, leaving no position or \
             pending offchain order",
        );
    }

    /// Composed coverage for the second crash window (ADR 0005): a crash
    /// between the position write (`execute_acknowledge_fill`) and the
    /// `OnChainTrade` acknowledgement marker (`execute_mark_acknowledged`)
    /// leaves the fill applied to the position but the trade un-acknowledged
    /// and the hedge unplaced. The re-delivered job, entering through
    /// `process_queued_trade` itself, must thread the two idempotent steps
    /// together: re-drive the acknowledge pair (the position rejects the
    /// duplicate via the single-slot guard, the marker is written) and place
    /// the hedge exactly once -- never double-count the fill nor place a
    /// second hedge. The isolated idempotency contracts are covered by
    /// `execute_acknowledge_fill_redrive_is_idempotent` and
    /// `execute_mark_acknowledged_is_idempotent`; this test proves the
    /// composed retry path.
    #[tokio::test]
    async fn redelivery_after_crash_between_position_write_and_marker_recovers_fill() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );
        let symbol = Symbol::new("AAPL").unwrap();

        let trade_event = make_trade_event(60);
        let trade = test_trade_with_amount(float!(1.5), 60);
        let block_timestamp = trade
            .block_timestamp
            .expect("test trade carries a block timestamp");
        let trade_id = OnChainTradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        };

        // Model the kill in the second crash window: the trade is witnessed
        // and the position write landed, but the acknowledgement marker and
        // the hedge never ran.
        execute_witness_trade(
            &cqrs.onchain_trade,
            &trade,
            trade_event.block_number,
            trade_event.block_hash,
            block_timestamp,
        )
        .await
        .unwrap();
        execute_acknowledge_fill(
            &cqrs.position,
            &trade,
            cqrs.execution_threshold,
            block_timestamp,
        )
        .await
        .expect("premise: the position write must land before the crash");
        let crashed_state = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("premise: the trade is witnessed");
        assert!(
            !crashed_state.is_acknowledged(),
            "premise: the acknowledgement marker must not be written yet"
        );

        // The re-delivered job after restart, entering through the real
        // pipeline entrypoint.
        let recovered_hedge_id = process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            test_trade_with_amount(float!(1.5), 60),
            &cqrs,
            true,
        )
        .await
        .unwrap()
        .expect(
            "the re-delivered job must resume the acknowledge pair and place \
             the hedge, not skip a witnessed-but-unacknowledged fill",
        );

        let position = cqrs
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("the position must exist after recovery");
        assert!(
            position.net.inner().eq(float!(1.5)).unwrap(),
            "the fill must be accounted exactly once across the crash window"
        );

        let recovered = cqrs
            .onchain_trade
            .load(&trade_id)
            .await
            .unwrap()
            .expect("the trade aggregate exists after recovery");
        assert!(
            recovered.is_acknowledged(),
            "the resume path must complete the acknowledgement marker"
        );

        let orders = offchain_order_projection.load_all().await.unwrap();
        assert_eq!(
            orders.len(),
            1,
            "the recovered fill must hedge exactly once, not twice"
        );
        assert_eq!(
            orders[0].0, recovered_hedge_id,
            "process_queued_trade must return the id of the hedge it placed"
        );
        assert_eq!(
            position.pending_offchain_order_id,
            Some(recovered_hedge_id),
            "the position must track the recovered hedge"
        );

        let (position_fills,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            position_fills, 1,
            "exactly one position fill event after recovery"
        );
    }

    /// `execute_mark_acknowledged` absorbs the aggregate's
    /// `AlreadyAcknowledged` rejection as `Ok(())`, so a re-driven marker
    /// (the second crash window) is idempotent rather than a propagated
    /// error that would fail the apalis job on every retry after the first
    /// marker write.
    #[tokio::test]
    async fn execute_mark_acknowledged_is_idempotent() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(60);
        let trade = test_trade_with_amount(float!(1.5), 60);
        let block_timestamp = trade
            .block_timestamp
            .expect("test trade carries a block timestamp");
        let trade_id = OnChainTradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        };

        execute_witness_trade(
            &cqrs.onchain_trade,
            &trade,
            trade_event.block_number,
            trade_event.block_hash,
            block_timestamp,
        )
        .await
        .unwrap();

        assert!(
            matches!(
                execute_mark_acknowledged(&cqrs.onchain_trade, &trade_id).await,
                Ok(FillMarkOutcome::Marked)
            ),
            "the first marker write must succeed and mark the fill"
        );
        assert!(
            matches!(
                execute_mark_acknowledged(&cqrs.onchain_trade, &trade_id).await,
                Ok(FillMarkOutcome::Marked)
            ),
            "re-driving the marker must be idempotent (Marked), not propagate an error"
        );
    }

    #[tokio::test]
    async fn execute_mark_acknowledged_signals_reorged_trade() {
        // The `process_queued_trade` reorg-skip guard normally prevents a reorged
        // trade from reaching the acknowledge marker, but a reorg landing between
        // that guard and this send makes `Acknowledge` return
        // `CannotAcknowledgeReorgedFill`. The marker write must surface that as
        // `FillMarkOutcome::Reorged` so `process_queued_trade` skips settle and
        // hedge instead of swallowing it and placing a broker order for the
        // invalidated fill.
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(62);
        let trade = test_trade_with_amount(float!(1.5), 62);
        let block_timestamp = trade
            .block_timestamp
            .expect("test trade carries a block timestamp");
        let trade_id = OnChainTradeId {
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
        };

        execute_witness_trade(
            &cqrs.onchain_trade,
            &trade,
            trade_event.block_number,
            trade_event.block_hash,
            block_timestamp,
        )
        .await
        .unwrap();
        cqrs.onchain_trade
            .send(
                &trade_id,
                OnChainTradeCommand::RecordReorg { reorg_depth: 3 },
            )
            .await
            .unwrap();

        assert!(
            matches!(
                execute_mark_acknowledged(&cqrs.onchain_trade, &trade_id).await,
                Ok(FillMarkOutcome::Reorged)
            ),
            "marking a reorged trade must signal Reorged so the caller skips settle and hedge"
        );
    }

    /// Exactly-once across multiple same-symbol fills: once a fill is
    /// acknowledged (marker written), re-delivering it -- even after a
    /// later fill for the same symbol advanced the single-slot
    /// `last_acknowledged_trade_id` -- is a safe no-op caught by the
    /// upstream `is_acknowledged()` marker, not a double-count. The single
    /// slot only holds the most recent fill, so the marker is the backstop
    /// for these longer-range duplicates (ADR 0005).
    #[tokio::test]
    async fn redelivery_of_marked_fill_after_later_fill_is_deduped() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        // Fill A then fill B, both fully processed. B advances the
        // single-slot guard to B's trade_id, displacing A.
        let event_a = make_trade_event(60);
        process_queued_trade(
            &MockExecutor::new(),
            &event_a,
            test_trade_with_amount(float!(1.0), 60),
            &cqrs,
            true,
        )
        .await
        .unwrap();

        let event_b = make_trade_event(61);
        process_queued_trade(
            &MockExecutor::new(),
            &event_b,
            test_trade_with_amount(float!(2.0), 61),
            &cqrs,
            true,
        )
        .await
        .unwrap();

        let (fills_before,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fills_before, 2,
            "premise: two distinct fills each accounted exactly once"
        );

        // Re-deliver A after B displaced the slot. The single-slot guard no
        // longer holds A, so correctness rests entirely on the upstream
        // marker short-circuiting the dedupe.
        let outcome = process_queued_trade(
            &MockExecutor::new(),
            &event_a,
            test_trade_with_amount(float!(1.0), 60),
            &cqrs,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome, None,
            "a marked fill re-delivered after a later fill must dedupe-skip"
        );

        let (fills_after,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fills_after, 2,
            "re-delivering the marked fill must not re-apply it (no double-count)"
        );
    }

    /// Exactly-once across a CRASH + later fill -- the scenario the single
    /// slot cannot handle (ADR 0010). Fill A is witnessed and applied to the
    /// position but its marker never lands (crash before MARK). A later fill B
    /// advances the single-slot guard to B, so the slot no longer holds A.
    /// Re-delivering A through the real pipeline must be rejected by the
    /// position's pending-acknowledgement set -- not double-counted. This is the
    /// cross-process / process-tx-CLI double-count the slot alone misses; with
    /// only the single slot, re-delivering A would emit a third
    /// `OnChainOrderFilled`.
    #[tokio::test]
    async fn redelivery_of_unmarked_fill_after_later_fill_is_rejected() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        // Fill A: witnessed and applied to the position, but crash before MARK
        // (and SETTLE). A is durably in the pending-acknowledgement set.
        let event_a = make_trade_event(60);
        let trade_a = test_trade_with_amount(float!(1.0), 60);
        let block_timestamp_a = trade_a
            .block_timestamp
            .expect("test trade carries a block timestamp");
        execute_witness_trade(
            &cqrs.onchain_trade,
            &trade_a,
            event_a.block_number,
            event_a.block_hash,
            block_timestamp_a,
        )
        .await
        .unwrap();
        execute_acknowledge_fill(
            &cqrs.position,
            &trade_a,
            cqrs.execution_threshold,
            block_timestamp_a,
        )
        .await
        .expect("premise: A's position write must land before the crash");

        // Fill B: processed end-to-end. Advances the single slot to B,
        // displacing A; A remains in the pending set.
        let event_b = make_trade_event(61);
        process_queued_trade(
            &MockExecutor::new(),
            &event_b,
            test_trade_with_amount(float!(2.0), 61),
            &cqrs,
            true,
        )
        .await
        .unwrap();

        let (fills_before,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(fills_before, 2, "premise: A and B each applied once");

        // Re-deliver A (resume path) after B displaced the slot. The pending
        // set still holds A, so the re-drive must be rejected -- not counted
        // a second time.
        process_queued_trade(
            &MockExecutor::new(),
            &event_a,
            test_trade_with_amount(float!(1.0), 60),
            &cqrs,
            true,
        )
        .await
        .unwrap();

        let (fills_after,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fills_after, 2,
            "re-delivering the unmarked, slot-displaced fill must not re-apply \
             it (no double-count)"
        );
    }

    /// The resume path's idempotency contract in isolation: re-driving a
    /// fill the position already applied (the crash window between the
    /// position write and the acknowledgement marker) must succeed without
    /// counting the fill twice. `execute_acknowledge_fill` maps the
    /// position's `DuplicateTrade` rejection to `Ok(())`.
    #[tokio::test]
    async fn execute_acknowledge_fill_redrive_is_idempotent() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );
        let symbol = Symbol::new("AAPL").unwrap();
        let trade = test_trade_with_amount(float!(1.5), 60);
        let block_timestamp = trade
            .block_timestamp
            .expect("test trade carries a block timestamp");

        execute_acknowledge_fill(
            &cqrs.position,
            &trade,
            cqrs.execution_threshold,
            block_timestamp,
        )
        .await
        .expect("the first acknowledge must apply the fill");

        execute_acknowledge_fill(
            &cqrs.position,
            &trade,
            cqrs.execution_threshold,
            block_timestamp,
        )
        .await
        .expect("re-driving the same fill must succeed, not propagate an error");

        let position = cqrs
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("the position must exist after acknowledge");
        assert!(
            position.net.inner().eq(float!(1.5)).unwrap(),
            "the re-drive must not double-count the fill"
        );
    }

    /// A fill missing its block timestamp can be neither witnessed nor
    /// acknowledged, so `process_queued_trade` must fail loudly with a
    /// retryable error rather than completing the job and dropping the fill
    /// silently.
    #[tokio::test]
    async fn process_queued_trade_rejects_missing_block_timestamp() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(60);
        let mut trade = test_trade_with_amount(float!(1.5), 60);
        trade.block_timestamp = None;

        let error = process_queued_trade(&MockExecutor::new(), &trade_event, trade, &cqrs, true)
            .await
            .unwrap_err();
        assert!(
            matches!(error, TradeAccountingError::MissingBlockTimestamp { .. }),
            "a fill without a block timestamp must fail loudly, not drop silently; got {error:?}"
        );
    }

    /// A broker outage during trade accounting must record the fill and
    /// defer the hedge, never lose it: the first attempt errors at the
    /// broker preflight check (after witness and acknowledge persisted; the
    /// mock's market-hours check passes, so the failure surfaces one step
    /// later in `preflight_counter_trade`),
    /// the apalis retry during the sustained outage dedupe-skips cleanly
    /// instead of burning the retry budget against the dead broker, and
    /// the first healthy position rescan places the hedge. One-sided
    /// availability must produce visible deferred exposure, not silent
    /// loss.
    #[tokio::test]
    async fn broker_outage_records_fill_and_defers_hedge_to_rescan() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let failing_executor = MockExecutor::with_failure("connection refused");

        let result = process_queued_trade(
            &failing_executor,
            &make_trade_event(50),
            test_trade_with_amount(float!(1.5), 50),
            &cqrs,
            true,
        )
        .await;
        assert!(
            matches!(result, Err(TradeAccountingError::Execution(_))),
            "First attempt must error at the broker preflight check; got: {result:?}",
        );

        let retry = process_queued_trade(
            &failing_executor,
            &make_trade_event(50),
            test_trade_with_amount(float!(1.5), 50),
            &cqrs,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            retry, None,
            "The retry during a sustained outage must dedupe-skip cleanly \
             rather than re-erroring and exhausting the retry budget"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position must exist after the outage attempt");
        assert!(
            position.net.inner().eq(float!(1.5)).unwrap(),
            "The fill must be recorded exactly once despite the outage"
        );
        assert_eq!(
            position.pending_offchain_order_id, None,
            "No hedge can have been claimed against a dead broker"
        );
        assert_eq!(
            offchain_order_projection.load_all().await.unwrap().len(),
            0,
            "No offchain order may exist during the outage"
        );

        check_and_execute_accumulated_positions(
            &MockExecutor::new(),
            AccumulatedPositionExecutionCtx {
                position: &cqrs.position,
                position_projection: &cqrs.position_projection,
                offchain_order: &cqrs.offchain_order,
                order_placer: cqrs.order_placer.as_ref(),
                counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
                threshold: &cqrs.execution_threshold,
                assets: &cqrs.assets,
            },
            |_| true,
        )
        .await
        .unwrap();

        let recovered = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position must exist after the rescan");
        let orders = offchain_order_projection.load_all().await.unwrap();
        assert_eq!(orders.len(), 1, "Exactly one hedge after recovery");
        assert_eq!(
            recovered.pending_offchain_order_id,
            Some(orders[0].0),
            "The first healthy rescan must place the deferred hedge"
        );
    }

    /// Re-delivering the identical (tx_hash, log_index) trade through the
    /// accounting pipeline must be single-effect: one OnChainTrade fill
    /// event, one Position fill transition, one offchain order -- the
    /// duplicate short-circuits at the aggregate-load guard. This is the
    /// dedup invariant every duplicate-input source (replayed logs,
    /// duplicate apalis jobs, restart re-backfill) funnels through.
    #[tokio::test]
    async fn duplicate_trade_event_is_single_effect() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let trade_event = make_trade_event(30);

        let first = process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            test_trade_with_amount(float!(1.5), 30),
            &cqrs,
            true,
        )
        .await
        .unwrap();
        let offchain_order_id =
            first.expect("1.5 shares should trigger execution with 1-share threshold");

        let second = process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            test_trade_with_amount(float!(1.5), 30),
            &cqrs,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            second, None,
            "Duplicate trade must short-circuit without placing another order"
        );

        let (onchain_fills,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("OnChainTradeEvent::Filled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(onchain_fills, 1, "Duplicate must not re-witness the trade");

        let (position_fills,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            position_fills, 1,
            "Duplicate must not double-count the position"
        );

        let position = cqrs
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("position should exist");
        assert!(
            position.net.inner().eq(float!(1.5)).unwrap(),
            "Position net must reflect the trade exactly once"
        );
        assert_eq!(
            position.pending_offchain_order_id,
            Some(offchain_order_id),
            "The pending order must still be the first attempt's"
        );

        let all_orders = offchain_order_projection.load_all().await.unwrap();
        assert_eq!(
            all_orders.len(),
            1,
            "Exactly one offchain order should exist after the duplicate, got {}",
            all_orders.len(),
        );
    }

    #[tokio::test]
    async fn trade_above_threshold_skips_counter_trade_without_offchain_inventory() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
                order_placer: cqrs.order_placer.as_ref(),
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
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
                order_placer: cqrs.order_placer.as_ref(),
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
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
                order_placer: cqrs.order_placer.as_ref(),
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
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
                order_placer: cqrs.order_placer.as_ref(),
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
        let (frameworks, offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let mut cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );
        cqrs.order_placer = order_placer;

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
                order_placer: cqrs.order_placer.as_ref(),
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
            Arc::new(crate::alerts::NoopNotifier),
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
            Arc::new(crate::alerts::NoopNotifier),
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
            Arc::new(crate::alerts::NoopNotifier),
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
            Arc::new(crate::alerts::NoopNotifier),
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
            RebalancingDeps {
                pool: pool.clone(),
                apalis_pool: apalis_pool.clone(),
                ctx: ctx.clone(),
                inventory,
                event_sender,
                vault_registry,
                vault_registry_projection,
                schedulers: RebalancingSchedulers::new(&apalis_pool),
                telemetry: TelemetrySender::disabled(),
            },
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
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
        let mut cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );
        cqrs.order_placer = failing_order_placer();

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
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
                order_placer: cqrs.order_placer.as_ref(),
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
        let (frameworks, _) = create_cqrs_frameworks(&pool).await;
        let mut cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );
        cqrs.order_placer = rejecting_order_placer();

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
                order_placer: cqrs.order_placer.as_ref(),
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
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
        let (frameworks, _offchain_order_projection) = create_cqrs_frameworks(&pool).await;
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
        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(())
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
                },
            )
            .await
            .unwrap();

        offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("test-order"),
                    placed_shares: shares,
                    submitted_at: Utc::now(),
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
        recover_orphaned_pending_offchain_orders(
            &position,
            &position_projection,
            &offchain_order,
            noop_order_placer().as_ref(),
        )
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
        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(())
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

        recover_orphaned_pending_offchain_orders(
            &position,
            &position_projection,
            &offchain_order,
            noop_order_placer().as_ref(),
        )
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
                .build(())
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

        // Drive the order to Submitted via the durable placement path; the
        // placer accepts, so the order leaves Pending for Submitted.
        place_offchain_order_at_broker(
            &offchain_order,
            order_placer.as_ref(),
            &offchain_order_id,
            symbol.clone(),
            shares,
            Direction::Sell,
            st0x_execution::SupportedExecutor::AlpacaBrokerApi,
            ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
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
        recover_orphaned_pending_offchain_orders(
            &position,
            &position_projection,
            &offchain_order,
            noop_order_placer().as_ref(),
        )
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
    async fn recover_orphaned_pending_redrives_pending_order() {
        let pool = setup_test_db().await;

        // A broker that accepts: re-driving the Pending orphan reaches Submitted.
        let order_placer = noop_order_placer();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let symbol = Symbol::new("SGOV").unwrap();
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
                            "0000000000000000000000000000000000000000000000000000000000000003"
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

        // Leave the order in Pending: a crash during placement persists `Placed`
        // (Pending) but never records the broker outcome.
        offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Pending { .. }
        ));

        recover_orphaned_pending_offchain_orders(
            &position,
            &position_projection,
            &offchain_order,
            order_placer.as_ref(),
        )
        .await
        .unwrap();

        // The sweep re-drove the placement in place: the same aggregate advanced
        // to Submitted (the broker accepted), not failed or left Pending.
        assert!(matches!(
            offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Submitted { .. }
        ));
        // The position claim is intact -- the order recovered in place, so the
        // sweep did not fall back to failing the claim and re-hedging.
        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(pos.pending_offchain_order_id, Some(offchain_order_id));
    }

    #[tokio::test]
    async fn recover_orphaned_pending_redrives_and_clears_position_on_rejection() {
        let pool = setup_test_db().await;

        // A broker that rejects: re-driving the Pending orphan fails the order,
        // and the sweep must clear the position claim so hedging can retry.
        fn rejecting_order_placer() -> Arc<dyn OrderPlacer> {
            struct RejectingPlacer;

            #[async_trait::async_trait]
            impl OrderPlacer for RejectingPlacer {
                async fn place_market_order(
                    &self,
                    _order: MarketOrder,
                ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
                {
                    Err("Broker rejected: insufficient buying power".into())
                }
            }

            Arc::new(RejectingPlacer)
        }

        let order_placer = rejecting_order_placer();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let symbol = Symbol::new("SGOV").unwrap();
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
                    price_usdc: float!(100),
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

        // Leave the order in Pending: a crash during placement persists `Placed`
        // (Pending) but never records the broker outcome.
        offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                },
            )
            .await
            .unwrap();

        recover_orphaned_pending_offchain_orders(
            &position,
            &position_projection,
            &offchain_order,
            order_placer.as_ref(),
        )
        .await
        .unwrap();

        // The re-driven placement was rejected, so the order is Failed and the
        // position claim is cleared for a fresh hedge attempt.
        assert!(matches!(
            offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Failed { .. }
        ));
        let pos = position_projection.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            pos.pending_offchain_order_id, None,
            "a rejected re-drive must clear the position claim so hedging can retry"
        );
    }

    #[tokio::test]
    async fn recover_orphaned_pending_clears_failed_order() {
        let pool = setup_test_db().await;
        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(())
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
        recover_orphaned_pending_offchain_orders(
            &position,
            &position_projection,
            &offchain_order,
            noop_order_placer().as_ref(),
        )
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
        let (frameworks, _projection) = create_cqrs_frameworks(&pool).await;
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
    async fn dispatch_post_place_state_pending_preserves_position_claim() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let offchain_order_id = drive_position_to_pending(&frameworks, &symbol, shares).await;

        // After the pure-Place extraction, a `Pending` order observed here means
        // the broker outcome commit failed and the error was propagated, leaving
        // a possibly-live broker order. Dispatch must NOT clear the claim (which
        // would strand it) -- it surfaces a retryable error instead.
        let pending_state = OffchainOrder::Pending {
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: st0x_execution::SupportedExecutor::DryRun,
            placed_at: Utc::now(),
        };

        let execution = execution_ctx_for(&symbol, shares);
        let error =
            dispatch_post_place_state(Some(pending_state), &execution, &cqrs, offchain_order_id)
                .await
                .unwrap_err();
        assert!(
            matches!(
                error,
                TradeAccountingError::UnexpectedPostPlaceState {
                    state: OffchainOrder::Pending { .. },
                    ..
                }
            ),
            "Pending after Place must surface as a retryable error, got {error:?}"
        );

        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            position.pending_offchain_order_id,
            Some(offchain_order_id),
            "a Pending order after Place must keep its position claim so a live broker order \
             is not stranded"
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
            telemetry_writer: tokio::spawn(pending::<()>()),
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
            telemetry_writer: tokio::spawn(pending::<()>()),
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
    async fn dispatch_post_place_state_filled_preserves_position_claim() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let offchain_order_id = drive_position_to_pending(&frameworks, &symbol, shares).await;

        // `place_offchain_order_at_broker` never returns `Filled`, so observing it
        // here is unexpected. Dispatch must surface a retryable error rather than
        // clear the claim -- it must not fail an order that has already filled.
        let filled_state = OffchainOrder::Filled {
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
        let error =
            dispatch_post_place_state(Some(filled_state), &execution, &cqrs, offchain_order_id)
                .await
                .unwrap_err();
        assert!(
            matches!(
                error,
                TradeAccountingError::UnexpectedPostPlaceState {
                    state: OffchainOrder::Filled { .. },
                    ..
                }
            ),
            "Filled after Place must surface as a retryable error, got {error:?}"
        );

        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            position.pending_offchain_order_id,
            Some(offchain_order_id),
            "an unexpected Filled state after Place must not clear the position claim"
        );
    }

    #[tokio::test]
    async fn dispatch_post_place_state_failed_clears_position_pending() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _projection) = create_cqrs_frameworks(&pool).await;
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
    async fn recover_claimed_offchain_order_clears_filled_terminal_claim() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (frameworks, _projection) = create_cqrs_frameworks(&pool).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &apalis_pool,
        );

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();

        // A prior placement attempt claimed the position and the broker filled the
        // order, but the position update never landed -- the claim is stuck while
        // the order is already terminal.
        let offchain_order_id = drive_position_to_pending(&frameworks, &symbol, shares).await;

        frameworks
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::DryRun,
                },
            )
            .await
            .unwrap();
        frameworks
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("ORD_TERMINAL"),
                    placed_shares: shares,
                    submitted_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        frameworks
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150)),
                },
            )
            .await
            .unwrap();

        // Sanity: the order is Filled yet the position still holds the claim.
        assert!(matches!(
            frameworks
                .offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Filled { .. }
        ));
        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(position.pending_offchain_order_id, Some(offchain_order_id));

        let execution = execution_ctx_for(&symbol, shares);
        let result = recover_claimed_offchain_order(&execution, &cqrs)
            .await
            .unwrap();
        assert_eq!(
            result, None,
            "a Filled terminal order reports no new placement from the PendingExecution \
             recovery path"
        );

        let position = frameworks
            .position_projection
            .load(&symbol)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            position.pending_offchain_order_id, None,
            "recovery must complete the position and clear the stale claim in-process"
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

    /// `is_pre_wrap_held_for_recovery` excludes `TokensReceived` and
    /// `WrapSubmitted` mints whose guard is `HeldForRecovery` from
    /// `resume_mints`. All other states are included regardless.
    #[test]
    fn resume_exclusion_predicate_excludes_only_pre_wrap_held_for_recovery() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let equity_in_progress: Arc<RwLock<HashMap<Symbol, GuardState>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let tokens_received = TokenizedEquityMint::TokensReceived {
            symbol: symbol.clone(),
            quantity: float!(5),
            wallet: Address::ZERO,
            issuer_request_id: issuer_request_id("exc-tokens-received"),
            tokenization_request_id: TokenizationRequestId("TOK-TR".to_string()),
            tx_hash: TxHash::ZERO,
            shares_minted: U256::from(5u64),
            fees: None,
            requested_at: now,
            accepted_at: now,
            received_at: now,
        };
        let wrap_submitted = TokenizedEquityMint::WrapSubmitted {
            symbol: symbol.clone(),
            quantity: float!(5),
            wallet: Address::ZERO,
            issuer_request_id: issuer_request_id("exc-wrap-submitted"),
            tokenization_request_id: TokenizationRequestId("TOK-WS".to_string()),
            tx_hash: TxHash::ZERO,
            shares_minted: U256::from(5u64),
            fees: None,
            requested_at: now,
            accepted_at: now,
            received_at: now,
            wrap_tx_hash: TxHash::ZERO,
        };
        let tokens_wrapped = TokenizedEquityMint::TokensWrapped {
            symbol: symbol.clone(),
            quantity: float!(5),
            wallet: Address::ZERO,
            issuer_request_id: issuer_request_id("exc-tokens-wrapped"),
            tokenization_request_id: TokenizationRequestId("TOK-TW".to_string()),
            tx_hash: TxHash::ZERO,
            shares_minted: U256::from(5u64),
            requested_at: now,
            accepted_at: now,
            received_at: now,
            wrap_tx_hash: TxHash::ZERO,
            wrapped_shares: U256::from(5u64),
            wrap_block: None,
            wrapped_at: now,
        };

        // Without HeldForRecovery -- no mint is excluded.
        equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation: 0 });
        assert!(
            !is_pre_wrap_held_for_recovery(&tokens_received, &equity_in_progress),
            "TokensReceived + ActiveTransfer must NOT be excluded"
        );
        assert!(
            !is_pre_wrap_held_for_recovery(&wrap_submitted, &equity_in_progress),
            "WrapSubmitted + ActiveTransfer must NOT be excluded"
        );

        // With HeldForRecovery -- only pre-wrap states are excluded.
        equity_in_progress
            .write()
            .unwrap()
            .insert(symbol, GuardState::HeldForRecovery);
        assert!(
            is_pre_wrap_held_for_recovery(&tokens_received, &equity_in_progress),
            "TokensReceived + HeldForRecovery must be excluded from resume_mints"
        );
        assert!(
            is_pre_wrap_held_for_recovery(&wrap_submitted, &equity_in_progress),
            "WrapSubmitted + HeldForRecovery must be excluded from resume_mints"
        );
        assert!(
            !is_pre_wrap_held_for_recovery(&tokens_wrapped, &equity_in_progress),
            "TokensWrapped must NEVER be excluded (deposit idempotent)"
        );
    }

    /// When `[alerts]` is absent, `build_usdc_notifier` returns a `NoopNotifier`
    /// that silently discards notifications without error.
    #[tokio::test]
    async fn build_usdc_notifier_returns_ok_noop_when_alerts_absent() {
        let notifier = build_usdc_notifier(None).unwrap();
        notifier
            .notify("test message")
            .await
            .expect("NoopNotifier must not error on notify");
    }

    /// When `[alerts]` IS present, `build_usdc_notifier` constructs a real
    /// Telegram notifier and returns `Ok` -- it must NOT fail startup for a
    /// well-formed config, and it must NOT silently fall back to `NoopNotifier`
    /// (which would suppress every redrive-limit and terminal-error page).
    ///
    /// The error branch (`Err(NotifierError::ClientBuild)`) is only reachable on
    /// a reqwest/TLS backend init failure; it cannot be triggered deterministically
    /// from the `AlertsCtx` inputs, so it is not unit-testable here. At the call
    /// site the `?` propagation fails startup, which is the behaviour that matters.
    /// `notify()` is deliberately not exercised: the present-branch notifier posts
    /// to the live Telegram API, which a unit test must never reach.
    #[tokio::test]
    async fn build_usdc_notifier_returns_ok_telegram_when_alerts_present() {
        let alerts = st0x_config::AlertsCtx {
            chat_id: 123,
            bot_token: "test-bot-token".to_string(),
            low_balance_threshold_wei: U256::from(0u64),
            poll_interval: std::time::Duration::from_secs(60),
            realert_interval: std::time::Duration::from_secs(3600),
            message_thread_id: Some(42),
        };

        build_usdc_notifier(Some(&alerts))
            .expect("a well-formed [alerts] config must yield a notifier, not a startup error");
    }
}
