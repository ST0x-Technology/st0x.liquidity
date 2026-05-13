//! Orchestrates the bot lifecycle: startup sequencing, runtime task management,
//! and trade processing. [`Conductor::run`] is the entry point.

mod builder;
mod exit;
pub(crate) mod job;
mod manifest;
pub(crate) mod monitor;

use alloy::primitives::Address;
use alloy::providers::{ProviderBuilder, WsConnect};
use anyhow::Context;
use apalis::prelude::Status;
use chrono::Utc;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use task_supervisor::SupervisorHandle;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

use st0x_dto::Statement;
use st0x_event_sorcery::{
    Projection, Store, StoreBuilder, compact_events, incremental_vacuum, load_all_ids, load_entity,
};
use st0x_evm::Wallet;
use st0x_execution::{
    CounterTradePreflight, CounterTradeReservation, CounterTradeSkipReason, ExecutionError,
    Executor, FractionalShares, MarketOrder, Symbol, TryIntoExecutor,
};

use crate::alpaca_wallet::AlpacaWalletService;
use crate::conductor::exit::{ConductorExit, MonitorTaskError};
use crate::config::{AssetsConfig, BrokerCtx, Ctx, CtxError};
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
use crate::onchain::backfill::BackfillJobQueue;
use crate::onchain::raindex::{RaindexService, RaindexVaultId};
use crate::onchain::trade::{RaindexTradeEvent, extract_owned_vaults, extract_vaults_from_clear};
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand, OnChainTradeId};
use crate::position::{Position, PositionCommand, TradeId};
use crate::rebalancing::equity::{CrossVenueEquityTransfer, EquityTransferServices};
use crate::rebalancing::{
    RebalancerServices, RebalancingCqrsFrameworks, RebalancingCtx, RebalancingTrigger,
    RebalancingTriggerConfig,
};
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
use crate::tokenization::Tokenizer;
use crate::tokenization::alpaca::AlpacaTokenizationService;
use crate::tokenized_equity_mint::{TokenizedEquityMint, interrupted_mint_ids};
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::{DexTradeAccountingJobQueue, TradeAccountingError};
use crate::vault_registry::{VaultRegistry, VaultRegistryCommand, VaultRegistryId};
use crate::wrapper::WrapperService;

pub(crate) use builder::CqrsFrameworks;
use manifest::QueryManifest;

/// Sets up apalis SQLite storage tables, tolerating pre-existing
/// application migrations in the shared `_sqlx_migrations` table.
pub(crate) async fn setup_apalis_tables(pool: &SqlitePool) -> Result<(), sqlx::Error> {
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
    /// Manages long-running tasks (order fill monitor, position monitor)
    /// with automatic restart.
    supervisor: SupervisorHandle,
    /// Runs the apalis job queue workers that process trade accounting jobs.
    monitor: JoinHandle<Result<(), MonitorTaskError>>,
    /// Periodic executor upkeep (e.g. Schwab token refresh). Absent when
    /// the executor requires no background maintenance.
    executor_maintenance: Option<JoinHandle<()>>,
    /// Periodic rebalancing loop. Absent when rebalancing is not configured.
    rebalancer: Option<JoinHandle<()>>,
    /// Periodically removes terminal rows from the persistent job queue.
    job_cleanup: JoinHandle<()>,
}

fn base_wallet_unwrapped_equity_token_addresses(ctx: &Ctx) -> HashMap<Symbol, Address> {
    ctx.assets
        .equities
        .symbols
        .iter()
        .filter(|(symbol, _)| ctx.is_trading_enabled(symbol) || ctx.is_rebalancing_enabled(symbol))
        .map(|(symbol, config)| (symbol.clone(), config.tokenized_equity))
        .collect()
}

fn base_wallet_wrapped_equity_token_addresses(ctx: &Ctx) -> HashMap<Symbol, Address> {
    ctx.assets
        .equities
        .symbols
        .iter()
        .filter(|(symbol, _)| ctx.is_trading_enabled(symbol) || ctx.is_rebalancing_enabled(symbol))
        .map(|(symbol, config)| (symbol.clone(), config.tokenized_equity_derivative))
        .collect()
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

impl Conductor {
    pub(crate) async fn run<E>(
        executor_ctx: impl TryIntoExecutor<Executor = E>,
        ctx: Ctx,
        pool: SqlitePool,
        event_sender: broadcast::Sender<Statement>,
        inventory: Arc<BroadcastingInventory>,
    ) -> anyhow::Result<()>
    where
        E: Executor + Clone + Send + 'static,
        TradeAccountingError: From<E::Error>,
        crate::offchain::order::JobError: From<E::Error>,
    {
        let executor = executor_ctx.try_into_executor().await?;
        let executor_maintenance = executor.run_executor_maintenance().await;

        let ws = WsConnect::new(ctx.evm.ws_rpc_url.as_str());
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .context("failed to connect websocket provider")?;
        let cache = SymbolCache::default();

        setup_apalis_tables(&pool).await?;
        let job_queue = DexTradeAccountingJobQueue::new(&pool);
        let backfill_queue = BackfillJobQueue::new(&pool);

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await?;

        let (vault_registry, vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await?;

        seed_vault_registry_from_config(&vault_registry, &ctx).await?;

        let rebalancing = match ctx.rebalancing_ctx() {
            Ok(ctx) => Some(ctx.clone()),
            Err(CtxError::NotRebalancing) => None,
            Err(error) => return Err(error.into()),
        };

        let PositionAndRebalancing {
            position,
            position_projection,
            snapshot,
            rebalancer,
            wallet_polling,
            tokenizer,
        } = PositionAndRebalancing::setup(
            rebalancing,
            &ctx,
            &pool,
            inventory.clone(),
            event_sender,
            vault_registry.clone(),
            vault_registry_projection.clone(),
        )
        .await?;

        // Hydrate the in-memory InventoryView from persisted snapshot
        // state so the runtime projection has the same data as the
        // database. Without this, the first post-restart poll may emit
        // no events (unchanged values are deduplicated), leaving the
        // view empty and potentially causing incorrect rebalancing.
        hydrate_inventory_from_snapshot(&pool, &inventory).await;

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
            vault_registry_projection,
            snapshot,
        };

        let hedge_queue = crate::conductor::job::JobQueue::new(&pool);
        let mut poll_status_queue = PollOrderStatusJobQueue::new(&pool);
        let reconcile_queue = ReconcileOrderFillJobQueue::new(&pool);
        let rejection_queue = HandleOrderRejectionJobQueue::new(&pool);

        recover_submitted_offchain_orders(
            &frameworks.offchain_order_projection,
            &mut poll_status_queue,
            executor.to_supported_executor(),
        )
        .await?;

        let job_cleanup = spawn_finished_job_cleanup(
            pool.clone(),
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
            #[cfg(any(test, feature = "test-support"))]
            failure_injector: ctx.failure_injector.clone(),
        };

        let mut conductor = builder::spawn()
            .context(conductor_ctx)
            .job_queue(job_queue)
            .backfill_queue(backfill_queue)
            .hedge_queue(hedge_queue)
            .poll_status_queue(poll_status_queue)
            .reconcile_queue(reconcile_queue)
            .rejection_queue(rejection_queue)
            .maybe_executor_maintenance(executor_maintenance)
            .maybe_rebalancer(rebalancer)
            .job_cleanup(job_cleanup)
            .call();

        info!("Conductor is running");
        let result = conductor.wait_for_completion().await;
        conductor.abort_all();
        result
    }
}

impl Conductor {
    pub(crate) async fn wait_for_completion(&mut self) -> anyhow::Result<()> {
        let exit = tokio::select! {
            result = self.supervisor.wait() => ConductorExit::Supervisor(result),
            result = &mut self.monitor => ConductorExit::Monitor(result),
            result = &mut self.job_cleanup => ConductorExit::JobCleanup(result),
        };

        exit.handle()?;
        Ok(())
    }

    pub(crate) fn abort_all(&self) {
        info!("Aborting all conductor tasks");
        if let Err(error) = self.supervisor.shutdown() {
            error!(%error, "Failed to shutdown supervisor");
        }
        self.monitor.abort();

        if let Some(ref handle) = self.rebalancer {
            handle.abort();
        }
        if let Some(ref handle) = self.executor_maintenance {
            handle.abort();
        }
        self.job_cleanup.abort();
    }
}

fn spawn_finished_job_cleanup(pool: SqlitePool, cleanup_interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            interval.tick().await;

            let cleanup_result = sqlx::query(
                "DELETE FROM Jobs \
                 WHERE status = ? \
                 OR status = ? \
                 OR (status = ? AND max_attempts <= attempts)",
            )
            .bind(Status::Done.to_string())
            .bind(Status::Killed.to_string())
            .bind(Status::Failed.to_string())
            .execute(&pool)
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

    let events = snapshot.hydration_events();
    if events.is_empty() {
        return;
    }

    apply_hydration_events(inventory, &events).await;
    info!(%id, event_count = events.len(), "Hydrated InventoryView from persisted snapshot");
}

async fn apply_hydration_events(
    inventory: &Arc<BroadcastingInventory>,
    events: &[crate::inventory::snapshot::InventorySnapshotEvent],
) {
    let now = chrono::Utc::now();
    let mut view = inventory.write().await;

    for event in events {
        if let Ok(updated) = view.clone().apply_snapshot_event(event, now) {
            *view = updated;
        }
    }
}

struct RebalancingInfrastructure {
    position: Arc<Store<Position>>,
    position_projection: Arc<Projection<Position>>,
    snapshot: Arc<Store<InventorySnapshot>>,
    rebalancer: JoinHandle<()>,
    tokenizer: Arc<dyn Tokenizer>,
}

/// Shared infrastructure dependencies needed to spawn rebalancing.
struct RebalancingDeps {
    pool: SqlitePool,
    ctx: Ctx,
    inventory: Arc<BroadcastingInventory>,
    event_sender: broadcast::Sender<Statement>,
    vault_registry: Arc<Store<VaultRegistry>>,
    vault_registry_projection: Arc<Projection<VaultRegistry>>,
}

/// Position + rebalancing-adjacent infrastructure produced during conductor
/// startup. When rebalancing is disabled, the optional fields are `None` and
/// the position aggregate is built standalone; otherwise the rebalancer owns
/// position construction and surfaces its handles here for the conductor to
/// supervise.
struct PositionAndRebalancing {
    position: Arc<Store<Position>>,
    position_projection: Arc<Projection<Position>>,
    snapshot: Arc<Store<InventorySnapshot>>,
    rebalancer: Option<JoinHandle<()>>,
    wallet_polling: Option<crate::inventory::WalletPollingCtx>,
    tokenizer: Option<Arc<dyn Tokenizer>>,
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
                rebalancer: Some(infra.rebalancer),
                wallet_polling: Some(wallet_polling),
                tokenizer: Some(infra.tokenizer),
            })
        } else {
            let (position, position_projection) = build_position_cqrs(pool).await?;

            // Without the trigger, the projection is the only subscriber
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
                rebalancer: None,
                wallet_polling: None,
                tokenizer: None,
            })
        }
    }
}

/// Pre-seeds the vault registry with vault IDs from config.
///
/// Assets with a `vault_id` in config get their vaults registered
/// immediately at startup, so rebalancing can work even before any
/// onchain trades have been observed.
async fn seed_vault_registry_from_config(
    vault_registry: &Store<VaultRegistry>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    // Pre-flight validation: ensure every rebalancing-enabled equity has at
    // least one vault_id before performing any writes to the vault registry.
    for (symbol, equity_config) in &ctx.assets.equities.symbols {
        if equity_config.vault_ids.is_empty() && ctx.is_rebalancing_enabled(symbol) {
            return Err(CtxError::MissingEquityVaultId {
                symbol: symbol.clone(),
            }
            .into());
        }
    }

    let vault_registry_id = VaultRegistryId {
        orderbook: ctx.evm.orderbook,
        owner: ctx.order_owner(),
    };

    for (symbol, equity_config) in &ctx.assets.equities.symbols {
        for vault_id in &equity_config.vault_ids {
            debug!(
                %symbol,
                %vault_id,
                token = %equity_config.tokenized_equity_derivative,
                "Seeding equity vault from config"
            );

            vault_registry
                .send(
                    &vault_registry_id,
                    VaultRegistryCommand::SeedEquityVaultFromConfig {
                        token: equity_config.tokenized_equity_derivative,
                        vault_id: *vault_id,
                        symbol: symbol.clone(),
                    },
                )
                .await?;
        }
    }

    if let Some(cash) = &ctx.assets.cash {
        for vault_id in &cash.vault_ids {
            info!(%vault_id, "Seeding USDC vault from config");

            vault_registry
                .send(
                    &vault_registry_id,
                    VaultRegistryCommand::SeedUsdcVaultFromConfig {
                        vault_id: *vault_id,
                    },
                )
                .await?;
        }
    }

    Ok(())
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

        const OPERATION_CHANNEL_CAPACITY: usize = 100;
        let (operation_sender, operation_receiver) = mpsc::channel(OPERATION_CHANNEL_CAPACITY);

        let raindex_service = Arc::new(RaindexService::new(
            base_wallet.clone(),
            deps.ctx.evm.orderbook,
            deps.vault_registry_projection,
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
            deps.ctx.assets.equities.symbols.clone(),
        ));

        let equity_transfer_services = EquityTransferServices {
            raindex: raindex_service.clone(),
            tokenizer: tokenizer.clone(),
            wrapper: wrapper.clone(),
        };

        let disabled_assets = deps
            .ctx
            .assets
            .equities
            .symbols
            .keys()
            .filter(|symbol| !deps.ctx.is_rebalancing_enabled(symbol))
            .cloned()
            .collect();

        let rebalancing_trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: rebalancing_ctx.equity,
                usdc: rebalancing_ctx.usdc,
                transfer_timeout: rebalancing_ctx.transfer_timeout,
                assets: deps.ctx.assets.clone(),
                disabled_assets,
            },
            deps.vault_registry,
            deps.ctx.evm.orderbook,
            market_maker_wallet,
            deps.inventory.clone(),
            operation_sender,
            wrapper.clone(),
        ));

        let equity_in_progress = Arc::clone(&rebalancing_trigger.equity_in_progress);
        let usdc_in_progress = Arc::clone(&rebalancing_trigger.usdc_in_progress);

        let broadcaster = Arc::new(Broadcaster::new(deps.event_sender, deps.pool.clone()));
        let manifest = QueryManifest::new(rebalancing_trigger.clone(), broadcaster);

        let built = manifest
            .build(deps.pool.clone(), equity_transfer_services)
            .await?;

        rebalancing_trigger
            .set_stores(built.mint.clone(), built.redemption.clone())
            .await;

        let recovery_transfer = CrossVenueEquityTransfer::new(
            raindex_service.clone(),
            tokenizer.clone(),
            wrapper.clone(),
            market_maker_wallet,
            built.mint.clone(),
            built.redemption.clone(),
        );

        recover_interrupted_tokenization_aggregates(
            &deps.pool,
            &rebalancing_trigger,
            deps.inventory.as_ref(),
            &recovery_transfer,
            built.mint.clone(),
            built.redemption.clone(),
        )
        .await?;

        let frameworks = RebalancingCqrsFrameworks {
            mint: built.mint,
            redemption: built.redemption,
            usdc: built.usdc,
        };

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
            deps.ctx.assets.equities.symbols.clone(),
            ethereum_wallet,
            base_wallet,
            raindex_service,
            Arc::clone(&tokenizer),
        )
        .await?;

        let usdc_vault_id = deps
            .ctx
            .assets
            .cash
            .as_ref()
            .and_then(|cash| cash.vault_ids.first().copied())
            .ok_or(CtxError::MissingCashVaultId)?;

        let handle = services.spawn(
            market_maker_wallet,
            RaindexVaultId(usdc_vault_id),
            operation_receiver,
            frameworks,
            equity_in_progress,
            usdc_in_progress,
        );

        Ok(RebalancingInfrastructure {
            position: built.position,
            position_projection: built.position_projection,
            snapshot: built.snapshot,
            rebalancer: handle,
            tokenizer,
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
    rebalancing_trigger: &RebalancingTrigger,
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

        rebalancing_trigger
            .recover_mint_state(mint_id, &mint)
            .await?;
    }

    for redemption_id in &interrupted_redemptions {
        let Some(redemption) = redemption_store.load(redemption_id).await? else {
            return Err(anyhow::anyhow!(
                "Interrupted redemption aggregate {redemption_id} missing from store"
            ));
        };

        rebalancing_trigger
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
        warn!(%symbol, %order_id, "Pending offchain order not found in store -- skipping");
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
) -> anyhow::Result<(Arc<Store<Position>>, Arc<Projection<Position>>)> {
    Ok(StoreBuilder::<Position>::new(pool.clone())
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

    let Some(execution) = check_execution_readiness(
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

    if matches!(
        preflight_counter_trade_submission(executor, &execution, None).await?,
        CounterTradeSubmissionCheck::Skipped
    ) {
        return Ok(None);
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
        // Read by the #[cfg(test)] batch-execution path;
        // the production single-trade path only checks Skipped.
        #[cfg_attr(not(test), allow(dead_code))]
        reservation: Option<CounterTradeReservation>,
    },
    Skipped,
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
    let order = MarketOrder {
        symbol: execution.symbol.clone(),
        shares: execution.shares,
        direction: execution.direction,
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
    let command = OffchainOrderCommand::Place {
        symbol: execution.symbol.clone(),
        shares: execution.shares,
        direction: execution.direction,
        executor: execution.executor,
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

    for execution in ready_positions {
        let reservation =
            match preflight_counter_trade_submission(executor, &execution, Some(&batch_budget))
                .await?
            {
                CounterTradeSubmissionCheck::Allowed { reservation } => reservation,
                CounterTradeSubmissionCheck::Skipped => continue,
            };

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

        let command = OffchainOrderCommand::Place {
            symbol: execution.symbol.clone(),
            shares: execution.shares,
            direction: execution.direction,
            executor: execution.executor,
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
    use std::collections::HashSet;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use task_supervisor::SupervisorBuilder;
    use tokio::sync::broadcast;

    use st0x_dto::Statement;
    use st0x_event_sorcery::{StoreBuilder, test_store};
    use st0x_execution::{
        Direction, EquityPosition, ExecutorOrderId, Inventory as ExecutionInventory, MarketOrder,
        MockExecutor, Positive, Symbol,
    };
    use st0x_finance::{Usd, Usdc};
    use st0x_float_macro::float;

    use super::*;
    use crate::bindings::IOrderBookV6::{
        ClearConfigV2, ClearV3, EvaluableV4, IOV2, OrderV4, TakeOrderConfigV4, TakeOrderV3,
    };
    use crate::conductor::builder::CqrsFrameworks;
    use crate::config::tests::create_test_ctx_with_order_owner;
    use crate::config::{AssetsConfig, EquitiesConfig, EquityAssetConfig, OperationMode};
    use crate::inventory::view::Operator;
    use crate::inventory::{ImbalanceThreshold, Inventory, InventoryView, Venue};
    use crate::offchain::order::OrderPlacementResult;
    use crate::onchain::trade::OnchainTrade;
    use crate::rebalancing::{RebalancingTrigger, TriggeredOperation};
    use crate::test_utils::{OnchainTradeBuilder, get_test_log, get_test_order, setup_test_db};
    use crate::threshold::ExecutionThreshold;
    use crate::trading::onchain::inclusion::EmittedOnChain;
    use crate::wrapper::mock::MockWrapper;
    use crate::wrapper::{RATIO_ONE, UnderlyingPerWrapped};

    fn one_to_one_ratio() -> UnderlyingPerWrapped {
        UnderlyingPerWrapped::new(RATIO_ONE).unwrap()
    }

    fn conductor_with_job_cleanup(job_cleanup: JoinHandle<()>) -> Conductor {
        let supervisor = SupervisorBuilder::default().build().run();
        let monitor = tokio::spawn(pending::<Result<(), MonitorTaskError>>());

        Conductor {
            supervisor,
            monitor,
            executor_maintenance: None,
            rebalancer: None,
            job_cleanup,
        }
    }

    async fn insert_finished_job(pool: &SqlitePool, id: &str) {
        sqlx::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority) \
             VALUES (?, ?, 'test', ?, 1, 25, 0, 0)",
        )
        .bind(vec![0_u8])
        .bind(id)
        .bind(Status::Done.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    async fn job_count(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn wait_for_job_count(pool: &SqlitePool, expected_count: i64) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let count = job_count(pool).await;
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
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();
        insert_finished_job(&pool, "done").await;

        let handle = spawn_finished_job_cleanup(pool.clone(), Duration::from_secs(60));

        wait_for_job_count(&pool, 0).await;
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
        let aapl_token = Address::random();
        let tsla_token = Address::random();
        let spym_token = Address::random();

        let mut symbols = HashMap::new();
        symbols.insert(
            aapl.clone(),
            EquityAssetConfig {
                tokenized_equity: aapl_token,
                tokenized_equity_derivative: Address::random(),
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            tsla.clone(),
            EquityAssetConfig {
                tokenized_equity: tsla_token,
                tokenized_equity_derivative: Address::random(),
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            spym.clone(),
            EquityAssetConfig {
                tokenized_equity: spym_token,
                tokenized_equity_derivative: Address::random(),
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
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
            2,
            "Only trading-enabled or rebalancing-enabled assets should be polled"
        );
        assert_eq!(actual.get(&aapl), Some(&aapl_token));
        assert_eq!(actual.get(&tsla), Some(&tsla_token));
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
        let aapl_wrapped_token = Address::random();
        let tsla_wrapped_token = Address::random();
        let spym_wrapped_token = Address::random();

        let mut symbols = HashMap::new();
        symbols.insert(
            aapl.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: aapl_wrapped_token,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            tsla.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: tsla_wrapped_token,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            spym.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: spym_wrapped_token,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
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
            2,
            "Only trading-enabled or rebalancing-enabled assets should be polled"
        );
        assert_eq!(actual.get(&aapl), Some(&aapl_wrapped_token));
        assert_eq!(actual.get(&tsla), Some(&tsla_wrapped_token));
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

        let (vault_registry, vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
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
                vault_registry_projection,
                snapshot,
            },
            offchain_order_projection,
        )
    }

    async fn trade_processing_cqrs_with_threshold(
        frameworks: &CqrsFrameworks,
        threshold: ExecutionThreshold,
        pool: &SqlitePool,
    ) -> TradeProcessingCqrs {
        // Tests reuse this helper; constructing PollOrderStatusJobQueue is
        // cheap but pushing into it requires apalis tables to exist.
        setup_apalis_tables(pool).await.unwrap();
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
            poll_status_queue: PollOrderStatusJobQueue::new(pool),
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
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

        let trade_event = make_trade_event(21);
        let trade = test_trade_with_amount_and_direction(float!(1.5), 21, Direction::Buy);
        let executor = MockExecutor::new().with_inventory(ExecutionInventory {
            positions: vec![],
            usd_balance_cents: 100_000,
            margin_safe_buying_power_cents: Some(100_000),
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
    async fn multiple_trades_accumulate_then_trigger() {
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

        acknowledge_fill(&cqrs.position, "AAPL", "1", Direction::Sell, 1).await;

        // margin_safe_buying_power_cents deliberately differs from usd_balance_cents
        // to verify buying power is display-only and does not affect trade decisions.
        let executor = MockExecutor::new()
            .with_inventory(ExecutionInventory {
                positions: vec![],
                usd_balance_cents: 10_000,
                margin_safe_buying_power_cents: Some(1_000_000),
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
        let pool = setup_test_db().await;
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
                margin_safe_buying_power_cents: Some(15_000),
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

        let pool = setup_test_db().await;
        let order_placer: Arc<dyn OrderPlacer> = Arc::new(FailOnceOrderPlacer {
            attempts: AtomicUsize::new(0),
        });
        let (frameworks, offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, order_placer).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

        acknowledge_fill(&cqrs.position, "AAPL", "1", Direction::Sell, 1).await;
        acknowledge_fill(&cqrs.position, "MSFT", "1", Direction::Sell, 2).await;

        let executor = MockExecutor::new()
            .with_inventory(ExecutionInventory {
                positions: vec![],
                usd_balance_cents: 15_000,
                margin_safe_buying_power_cents: Some(15_000),
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

    /// Builds an `InventoryView` with an equity imbalance: 20% onchain, 80% offchain.
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
                    FractionalShares::new(float!(20)),
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
    async fn position_events_reach_rebalancing_trigger() {
        let pool = setup_test_db().await;
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
        let (operation_sender, mut operation_receiver) = mpsc::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
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
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
                disabled_assets: HashSet::new(),
            },
            vault_registry,
            orderbook,
            order_owner,
            inventory,
            operation_sender,
            Arc::new(MockWrapper::new()),
        ));

        let (position_store, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(Arc::clone(&trigger))
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

        let triggered = operation_receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Mint { .. })),
            "Expected Mint operation from trigger on position event, got {triggered:?}"
        );
    }

    #[tokio::test]
    async fn inventory_updated_after_fill_via_cqrs() {
        let pool = setup_test_db().await;
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
        let (operation_sender, _operation_receiver) = mpsc::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: threshold,
                usdc: Some(threshold),
                transfer_timeout: Duration::from_secs(30 * 60),
                assets: AssetsConfig {
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
                disabled_assets: HashSet::new(),
            },
            vault_registry,
            orderbook,
            order_owner,
            Arc::clone(&inventory),
            operation_sender,
            Arc::new(MockWrapper::new()),
        ));

        let (position_store, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(Arc::clone(&trigger))
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
        let pool = setup_test_db().await;
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
        let (operation_sender, mut receiver) = mpsc::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
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
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
                disabled_assets: HashSet::new(),
            },
            vault_registry,
            orderbook,
            order_owner,
            inventory,
            operation_sender,
            Arc::new(MockWrapper::new()),
        ));

        let (position_store, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(trigger.clone())
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

        assert!(
            matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "Balanced position event should not trigger rebalancing"
        );
    }

    /// Sets up an initialized position with 65% onchain / 35% offchain inventory
    /// (within the 30%-70% threshold bounds), a seeded vault registry, and the
    /// trigger wired into the position store via `build_position_cqrs`.
    async fn setup_near_upper_threshold_position() -> (
        Arc<Store<Position>>,
        mpsc::Receiver<TriggeredOperation>,
        Symbol,
    ) {
        let pool = setup_test_db().await;
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
        let (operation_sender, receiver) = mpsc::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
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
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
                disabled_assets: HashSet::new(),
            },
            vault_registry,
            orderbook,
            order_owner,
            inventory,
            operation_sender,
            Arc::new(MockWrapper::new()),
        ));

        let (position_store, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(trigger)
            .build(())
            .await
            .unwrap();

        (position_store, receiver, symbol)
    }

    #[tokio::test]
    async fn position_event_causing_imbalance_triggers_redemption() {
        let (position_store, mut receiver, symbol) = setup_near_upper_threshold_position().await;
        let test_token = address!("0x1234567890123456789012345678901234567890");

        // No position commands yet -> no triggers fired.
        assert!(
            matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "No commands executed yet, channel should be empty"
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

        let triggered = receiver.try_recv().unwrap();
        let TriggeredOperation::Redemption {
            symbol: redeemed_symbol,
            quantity,
            wrapped_token,
            ..
        } = triggered
        else {
            panic!("Expected Redemption, got {triggered:?}");
        };
        assert_eq!(redeemed_symbol, symbol);
        assert_eq!(quantity, FractionalShares::new(float!(25)));
        assert_eq!(wrapped_token, test_token);
    }

    #[tokio::test]
    async fn in_progress_guard_blocks_duplicate_trigger() {
        let (position_store, mut receiver, symbol) = setup_near_upper_threshold_position().await;
        let test_token = address!("0x1234567890123456789012345678901234567890");

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

        let first = receiver.try_recv().unwrap();
        let TriggeredOperation::Redemption {
            symbol: redeemed_symbol,
            quantity,
            wrapped_token,
            ..
        } = first
        else {
            panic!("Expected Redemption, got {first:?}");
        };
        assert_eq!(redeemed_symbol, symbol);
        assert_eq!(quantity, FractionalShares::new(float!(25)));
        assert_eq!(wrapped_token, test_token);

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

        assert!(
            matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "In-progress guard should block duplicate trigger"
        );
    }

    #[tokio::test]
    async fn restart_hydrates_position_view_from_persisted_events() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();

        // First "session": create position store, execute commands.
        {
            let (position_store, position_projection) = build_position_cqrs(&pool).await.unwrap();

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
            let (_position_store, position_projection) = build_position_cqrs(&pool).await.unwrap();

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

        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, failing_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
            &pool,
        )
        .await;

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

        let pool = setup_test_db().await;

        // Build CQRS with a rejecting broker
        let (frameworks, _) =
            create_cqrs_frameworks_with_order_placer(&pool, rejecting_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
    async fn seed_vault_registry_rejects_missing_vault_id_without_side_effects() {
        let pool = setup_test_db().await;
        let vault_registry: Store<VaultRegistry> = test_store(pool.clone(), ());

        // Two equities with rebalancing enabled: one has a vault_id, one does not.
        // The pre-flight validation must reject before any seeds are applied.
        let mut symbols = HashMap::new();

        symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_ids: vec![fixed_bytes!(
                    "0x0000000000000000000000000000000000000000000000000000000000000001"
                )],
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        symbols.insert(
            Symbol::new("TSLA").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let ctx = Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    operational_limit: None,
                    symbols,
                },
                cash: None,
            },
            ..create_test_ctx_with_order_owner(Address::ZERO)
        };

        let error = seed_vault_registry_from_config(&vault_registry, &ctx)
            .await
            .expect_err("should fail when vault_id is missing for TSLA");

        let ctx_error = error
            .downcast_ref::<CtxError>()
            .expect("error should be CtxError");

        assert!(
            matches!(ctx_error, CtxError::MissingEquityVaultId { symbol } if symbol.to_string() == "TSLA"),
            "expected MissingEquityVaultId for TSLA, got: {ctx_error:?}"
        );

        let registry = load_vault_registry(&vault_registry).await;

        assert!(
            registry.is_none(),
            "Vault registry should have no state after validation failure"
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
        let pool = setup_test_db().await;
        let (frameworks, _projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, _projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
    async fn dispatch_post_place_state_filled_clears_position_pending() {
        let pool = setup_test_db().await;
        let (frameworks, _projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
        let pool = setup_test_db().await;
        let (frameworks, _projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs = trade_processing_cqrs_with_threshold(
            &frameworks,
            ExecutionThreshold::whole_share(),
            &pool,
        )
        .await;

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
}
