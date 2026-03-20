//! Orchestrates the bot lifecycle: startup sequencing, runtime task management,
//! and trade processing. [`Conductor::run`] is the entry point.

mod builder;
pub(crate) mod job;
mod manifest;
mod order_fill_monitor;

use alloy::primitives::Address;
use alloy::providers::{ProviderBuilder, WsConnect};
use apalis_sqlite::SqliteStorage;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use task_supervisor::SupervisorHandle;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use st0x_dto::ServerMessage;
use st0x_event_sorcery::{Projection, Store, StoreBuilder};
use st0x_evm::Wallet;
use st0x_execution::{Executor, FractionalShares, Symbol};

use crate::alpaca_wallet::AlpacaWalletService;
use crate::bindings::IOrderBookV6::IOrderBookV6Instance;
use crate::config::{AssetsConfig, Ctx, CtxError};
use crate::dashboard::EventBroadcaster;
use crate::inventory::{BroadcastingInventory, InventoryPollingService, InventorySnapshot};
use crate::offchain::order_poller::OrderStatusPoller;
use crate::offchain_order::{
    ExecutorOrderPlacer, OffchainOrder, OffchainOrderCommand, OffchainOrderId, OrderPlacer,
};
use crate::onchain::OnchainTrade;
use crate::onchain::USDC_BASE;
use crate::onchain::accumulator::{ExecutionCtx, check_all_positions, check_execution_readiness};
use crate::onchain::backfill::backfill_events;
use crate::onchain::raindex::{RaindexService, RaindexVaultId};
use crate::onchain::trade::{RaindexTradeEvent, extract_owned_vaults, extract_vaults_from_clear};
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand, OnChainTradeId};
use crate::position::{Position, PositionCommand, TradeId};
use crate::rebalancing::equity::EquityTransferServices;
use crate::rebalancing::{
    RebalancerServices, RebalancingCqrsFrameworks, RebalancingCtx, RebalancingTrigger,
    RebalancingTriggerConfig,
};
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
use crate::tokenization::Tokenizer;
use crate::tokenization::alpaca::AlpacaTokenizationService;
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::{DexTradeAccountingJobQueue, TradeAccountingError};
use crate::vault_registry::{VaultRegistry, VaultRegistryCommand, VaultRegistryId};
use crate::wrapper::WrapperService;

use self::manifest::QueryManifest;
pub(crate) use builder::CqrsFrameworks;

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
}

pub(crate) struct Conductor {
    supervisor: SupervisorHandle,
    monitor: JoinHandle<()>,
    order_poller: JoinHandle<()>,
    position_checker: JoinHandle<()>,
    executor_maintenance: Option<JoinHandle<()>>,
    rebalancer: Option<JoinHandle<()>>,
    inventory_poller: Option<JoinHandle<()>>,
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

impl Conductor {
    pub(crate) async fn run<E>(
        executor: E,
        ctx: Ctx,
        pool: SqlitePool,
        executor_maintenance: Option<JoinHandle<()>>,
        event_sender: broadcast::Sender<ServerMessage>,
        inventory: Arc<BroadcastingInventory>,
    ) -> anyhow::Result<()>
    where
        E: Executor + Clone + Send + 'static,
        TradeAccountingError: From<E::Error>,
    {
        // Phase 1: connect WS and set up apalis tables (parallel)
        let ws = WsConnect::new(ctx.evm.ws_rpc_url.as_str());
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let cache = SymbolCache::default();
        let orderbook = IOrderBookV6Instance::new(ctx.evm.orderbook, &provider);

        setup_apalis_tables(&pool).await?;
        let job_queue: DexTradeAccountingJobQueue = SqliteStorage::new(&pool);

        let mut clear_stream = orderbook.ClearV3_filter().watch().await?.into_stream();
        let mut take_stream = orderbook.TakeOrderV3_filter().watch().await?.into_stream();

        // Phase 2: determine cutoff block from WS subscription
        let cutoff_block = get_cutoff_block(&mut clear_stream, &mut take_stream, &provider).await?;

        // Phase 3: backfill historical events to the job queue
        if let Some(end_block) = cutoff_block.checked_sub(1) {
            backfill_events(
                &provider,
                &ctx.evm,
                end_block,
                backon::ExponentialBuilder::default(),
                job_queue.clone(),
            )
            .await?;
        }

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

        let (position, position_projection, snapshot, rebalancer, wallet_polling) =
            if let Some(rebalancing_ctx) = rebalancing {
                let ethereum_wallet = rebalancing_ctx.ethereum_wallet().clone();
                let base_wallet = rebalancing_ctx.base_wallet().clone();
                let infra = spawn_rebalancing_infrastructure(
                    rebalancing_ctx,
                    ethereum_wallet.clone(),
                    base_wallet.clone(),
                    RebalancingDeps {
                        pool: pool.clone(),
                        ctx: ctx.clone(),
                        inventory: inventory.clone(),
                        event_sender,
                        vault_registry: vault_registry.clone(),
                        vault_registry_projection: vault_registry_projection.clone(),
                    },
                )
                .await?;

                let wallet_polling = crate::inventory::WalletPollingCtx {
                    ethereum: Arc::new(ethereum_wallet),
                    base: Arc::new(base_wallet),
                    alpaca_wallet: infra.alpaca_wallet,
                    unwrapped_equity_token_addresses: base_wallet_unwrapped_equity_token_addresses(
                        &ctx,
                    ),
                    wrapped_equity_token_addresses: base_wallet_wrapped_equity_token_addresses(
                        &ctx,
                    ),
                };

                (
                    infra.position,
                    infra.position_projection,
                    infra.snapshot,
                    Some(infra.rebalancer),
                    Some(wallet_polling),
                )
            } else {
                let (position, position_projection) = build_position_cqrs(&pool).await?;
                let snapshot = StoreBuilder::<InventorySnapshot>::new(pool.clone())
                    .build(())
                    .await?;
                (position, position_projection, snapshot, None, None)
            };

        let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer(executor.clone()));

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
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

        let dex_streams = order_fill_monitor::DexEventStreams {
            clear: Box::pin(clear_stream),
            take: Box::pin(take_stream),
        };

        let conductor_ctx = builder::ConductorCtx {
            ctx: ctx.clone(),
            cache,
            provider,
            executor,
            execution_threshold: ctx.execution_threshold,
            frameworks,
            poll_notify: Arc::new(tokio::sync::Notify::new()),
            wallet_polling,
        };

        let mut conductor = builder::spawn()
            .context(conductor_ctx)
            .job_queue(job_queue)
            .dex_streams(dex_streams)
            .maybe_executor_maintenance(executor_maintenance)
            .maybe_rebalancer(rebalancer)
            .call();

        info!("Conductor running");
        let result = conductor.wait_for_completion().await;
        conductor.abort_all();
        result
    }
}

impl Conductor {
    pub(crate) async fn wait_for_completion(&mut self) -> anyhow::Result<()> {
        tokio::select! {
            result = self.supervisor.wait() => {
                result?;
                info!("Supervisor exited");
            }
            result = &mut self.monitor => {
                if let Err(join_error) = result {
                    if !join_error.is_cancelled() {
                        return Err(anyhow::anyhow!("Apalis monitor failed: {join_error}"));
                    }
                }
                info!("Apalis monitor exited");
            }
            result = &mut self.order_poller => {
                if let Err(join_error) = result {
                    if !join_error.is_cancelled() {
                        return Err(anyhow::anyhow!("Order poller failed: {join_error}"));
                    }
                }
                info!("Order poller exited");
            }
            result = &mut self.position_checker => {
                if let Err(join_error) = result {
                    if !join_error.is_cancelled() {
                        return Err(anyhow::anyhow!("Position checker failed: {join_error}"));
                    }
                }
                info!("Position checker exited");
            }
        }

        Ok(())
    }

    pub(crate) fn abort_all(&mut self) {
        info!("Aborting all conductor tasks");
        if let Err(error) = self.supervisor.shutdown() {
            error!(%error, "Failed to shutdown supervisor");
        }
        self.monitor.abort();
        self.order_poller.abort();
        self.position_checker.abort();

        if let Some(ref handle) = self.rebalancer {
            handle.abort();
        }
        if let Some(ref handle) = self.inventory_poller {
            handle.abort();
        }
        if let Some(ref handle) = self.executor_maintenance {
            handle.abort();
        }
    }
}

struct RebalancingInfrastructure {
    position: Arc<Store<Position>>,
    position_projection: Arc<Projection<Position>>,
    snapshot: Arc<Store<InventorySnapshot>>,
    rebalancer: JoinHandle<()>,
    alpaca_wallet: Arc<AlpacaWalletService>,
}

/// Shared infrastructure dependencies needed to spawn rebalancing.
struct RebalancingDeps {
    pool: SqlitePool,
    ctx: Ctx,
    inventory: Arc<BroadcastingInventory>,
    event_sender: broadcast::Sender<ServerMessage>,
    vault_registry: Arc<Store<VaultRegistry>>,
    vault_registry_projection: Arc<Projection<VaultRegistry>>,
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
    // Pre-flight validation: ensure every rebalancing-enabled equity has a
    // vault_id before performing any writes to the vault registry.
    for (symbol, equity_config) in &ctx.assets.equities.symbols {
        if equity_config.vault_id.is_none() && ctx.is_rebalancing_enabled(symbol) {
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
        let Some(vault_id) = equity_config.vault_id else {
            continue;
        };

        info!(
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
                    vault_id,
                    symbol: symbol.clone(),
                },
            )
            .await?;
    }

    if let Some(cash) = &ctx.assets.cash
        && let Some(vault_id) = cash.vault_id
    {
        info!(%vault_id, "Seeding USDC vault from config");

        vault_registry
            .send(
                &vault_registry_id,
                VaultRegistryCommand::SeedUsdcVaultFromConfig { vault_id },
            )
            .await?;
    }

    Ok(())
}

fn spawn_rebalancing_infrastructure<Chain: Wallet + Clone>(
    rebalancing_ctx: RebalancingCtx,
    ethereum_wallet: Chain,
    base_wallet: Chain,
    deps: RebalancingDeps,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = anyhow::Result<RebalancingInfrastructure>> + Send>,
> {
    Box::pin(async move {
        info!("Initializing rebalancing infrastructure");

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
            rebalancing_ctx.alpaca_broker_auth.base_url().to_string(),
            rebalancing_ctx.alpaca_broker_auth.account_id,
            rebalancing_ctx.alpaca_broker_auth.api_key.clone(),
            rebalancing_ctx.alpaca_broker_auth.api_secret.clone(),
            base_wallet.clone(),
            rebalancing_ctx.redemption_wallet,
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
                assets: deps.ctx.assets.clone(),
                disabled_assets,
            },
            deps.vault_registry,
            deps.ctx.evm.orderbook,
            market_maker_wallet,
            deps.inventory.clone(),
            operation_sender,
            wrapper,
        ));

        let event_broadcaster = Arc::new(EventBroadcaster::new(deps.event_sender));
        let manifest = QueryManifest::new(rebalancing_trigger, event_broadcaster);

        let built = manifest
            .build(deps.pool.clone(), equity_transfer_services)
            .await?;

        let frameworks = RebalancingCqrsFrameworks {
            mint: built.mint,
            redemption: built.redemption,
            usdc: built.usdc,
        };

        let alpaca_wallet = Arc::new(AlpacaWalletService::new(
            rebalancing_ctx.alpaca_broker_auth.base_url().to_string(),
            rebalancing_ctx.alpaca_broker_auth.account_id,
            rebalancing_ctx.alpaca_broker_auth.api_key.clone(),
            rebalancing_ctx.alpaca_broker_auth.api_secret.clone(),
        ));

        let services = RebalancerServices::new(
            rebalancing_ctx.clone(),
            Arc::clone(&alpaca_wallet),
            deps.ctx.assets.equities.symbols.clone(),
            ethereum_wallet,
            base_wallet,
            raindex_service,
            tokenizer,
        )
        .await?;

        let usdc_vault_id = deps
            .ctx
            .assets
            .cash
            .as_ref()
            .and_then(|cash| cash.vault_id)
            .ok_or(CtxError::MissingCashVaultId)?;

        let handle = services.spawn(
            market_maker_wallet,
            RaindexVaultId(usdc_vault_id),
            operation_receiver,
            frameworks,
        );

        Ok(RebalancingInfrastructure {
            position: built.position,
            position_projection: built.position_projection,
            snapshot: built.snapshot,
            rebalancer: handle,
            alpaca_wallet,
        })
    })
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

/// Determines the block number at which the WS subscription starts.
///
/// Waits up to 5 seconds for the first event on either stream. If an event
/// arrives, its block number is the cutoff. If no events arrive within the
/// timeout, falls back to the provider's current block number.
async fn get_cutoff_block<S1, S2, P>(
    clear_stream: &mut S1,
    take_stream: &mut S2,
    provider: &P,
) -> anyhow::Result<u64>
where
    S1: futures_util::Stream<
            Item = Result<
                (
                    crate::bindings::IOrderBookV6::ClearV3,
                    alloy::rpc::types::Log,
                ),
                alloy::sol_types::Error,
            >,
        > + Unpin,
    S2: futures_util::Stream<
            Item = Result<
                (
                    crate::bindings::IOrderBookV6::TakeOrderV3,
                    alloy::rpc::types::Log,
                ),
                alloy::sol_types::Error,
            >,
        > + Unpin,
    P: alloy::providers::Provider + Clone,
{
    use futures_util::StreamExt;

    info!("Waiting for first WebSocket event to determine cutoff block...");

    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        let block_number = tokio::select! {
            Some(result) = clear_stream.next() => {
                result?.1.block_number
            }
            Some(result) = take_stream.next() => {
                result?.1.block_number
            }
            () = &mut timeout => {
                let current_block = provider.get_block_number().await?;
                info!("No events within timeout, using current block {current_block} as cutoff");
                return Ok(current_block);
            }
        };

        if let Some(block) = block_number {
            info!("First event at block {block}, using as cutoff");
            return Ok(block);
        }

        warn!("Event missing block number, waiting for next event");
    }
}

fn spawn_order_poller<E: Executor + Clone + Send + 'static>(
    ctx: &Ctx,
    executor: E,
    offchain_order_projection: Projection<OffchainOrder>,
    offchain_order: Arc<Store<OffchainOrder>>,
    position: Arc<Store<Position>>,
) -> JoinHandle<()> {
    let poller_ctx = ctx.get_order_poller_ctx();
    info!(
        "Starting order status poller with interval: {:?}, max jitter: {:?}",
        poller_ctx.polling_interval, poller_ctx.max_jitter
    );

    let poller = OrderStatusPoller::new(
        poller_ctx,
        executor,
        offchain_order_projection,
        offchain_order,
        position,
    );
    tokio::spawn(async move {
        if let Err(error) = poller.run().await {
            error!("Order poller failed: {error}");
        } else {
            info!("Order poller completed successfully");
        }
    })
}

#[bon::builder]
fn spawn_periodic_accumulated_position_check<E>(
    executor: E,
    position: Arc<Store<Position>>,
    position_projection: Arc<Projection<Position>>,
    offchain_order: Arc<Store<OffchainOrder>>,
    execution_threshold: ExecutionThreshold,
    check_interval: Duration,
    ctx: Ctx,
) -> JoinHandle<()>
where
    E: Executor + Clone + Send + 'static,
    TradeAccountingError: From<E::Error>,
{
    info!("Starting periodic accumulated position checker");

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(check_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            debug!("Running periodic accumulated position check");
            if let Err(error) = check_and_execute_accumulated_positions(
                &executor,
                &position,
                &position_projection,
                &offchain_order,
                &execution_threshold,
                &ctx.assets,
                |symbol| ctx.is_trading_enabled(symbol),
            )
            .await
            {
                error!("Periodic accumulated position check failed: {error}");
            }
        }
    })
}

fn spawn_inventory_poller<Chain, Exe>(
    service: InventoryPollingService<Chain, Exe>,
    poll_interval: std::time::Duration,
    poll_notify: Arc<tokio::sync::Notify>,
) -> JoinHandle<()>
where
    Chain: st0x_evm::Evm,
    Exe: Executor + Clone + Send + 'static,
{
    info!("Starting inventory poller");

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {}
                () = poll_notify.notified() => {
                    debug!("Inventory poll triggered by notification");
                }
            }

            debug!("Running inventory poll");
            if let Err(error) = service.poll_and_record().await {
                error!(%error, "Inventory polling failed");
            }
        }
    })
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
            warn!(
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

async fn execute_witness_trade(
    onchain_trade: &Store<OnChainTrade>,
    trade: &OnchainTrade,
    block_number: u64,
) {
    let trade_id = OnChainTradeId {
        tx_hash: trade.tx_hash,
        log_index: trade.log_index,
    };

    let amount = trade.amount.inner();
    let price_usdc = trade.price.value();

    let Some(block_timestamp) = trade.block_timestamp else {
        error!(
            "Missing block_timestamp for OnChainTrade::Witness: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        );
        return;
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
        Ok(()) => info!(
            "Successfully executed OnChainTrade::Witness command: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        ),
        Err(error) => error!(
            "Failed to execute OnChainTrade::Witness command: {error}, tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
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
        Ok(()) => info!(
            "Successfully executed OnChainTrade::Enrich command: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
        ),
        Err(error) => error!(
            "Failed to execute OnChainTrade::Enrich command: {error}, tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
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
        error!(
            "Missing block_timestamp for Position::AcknowledgeOnChainFill: tx_hash={:?}, log_index={}",
            trade.tx_hash, trade.log_index
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
        Ok(()) => info!(
            "Successfully executed Position::AcknowledgeOnChainFill command: tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
        Err(error) => error!(
            "Failed to execute Position::AcknowledgeOnChainFill command: {error}, tx_hash={:?}, log_index={}, symbol={}",
            trade.tx_hash, trade.log_index, trade.symbol
        ),
    }
}

pub(crate) async fn process_queued_trade<E: Executor>(
    executor: &E,
    trade_event: &EmittedOnChain<RaindexTradeEvent>,
    trade: OnchainTrade,
    cqrs: &TradeProcessingCqrs,
    asset_enabled: bool,
) -> Result<Option<OffchainOrderId>, TradeAccountingError> {
    // Update Position aggregate FIRST so threshold check sees current state
    execute_acknowledge_fill(&cqrs.position, &trade, cqrs.execution_threshold).await;

    execute_witness_trade(&cqrs.onchain_trade, &trade, trade_event.block_number).await;
    execute_enrich_trade(&cqrs.onchain_trade, &trade).await;

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

    place_offchain_order(&execution, cqrs).await
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

    let aggregate = cqrs.offchain_order.load(&offchain_order_id).await;

    if let Ok(Some(OffchainOrder::Failed { error, .. })) = aggregate {
        warn!(
            %offchain_order_id,
            symbol = %execution.symbol,
            %error,
            "Broker rejected order, clearing position pending state"
        );
        execute_fail_offchain_order_position(&cqrs.position, offchain_order_id, execution, error)
            .await;
    }

    Ok(Some(offchain_order_id))
}

async fn execute_fail_offchain_order_position(
    position: &Store<Position>,
    offchain_order_id: OffchainOrderId,
    execution: &ExecutionCtx,
    error: String,
) {
    let command = PositionCommand::FailOffChainOrder {
        offchain_order_id,
        error,
    };

    match position.send(&execution.symbol, command).await {
        Ok(()) => info!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "Position::FailOffChainOrder succeeded"
        ),
        Err(error) => error!(
            %offchain_order_id,
            symbol = %execution.symbol,
            "Position::FailOffChainOrder failed: {error}"
        ),
    }
}

/// Returns `true` if the Position aggregate accepted the order, `false` if it
/// was rejected (e.g. already has a pending execution).
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
            info!(
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
        Ok(()) => info!(
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

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
pub(crate) async fn check_and_execute_accumulated_positions<E>(
    executor: &E,
    position: &Store<Position>,
    position_projection: &Projection<Position>,
    offchain_order: &Arc<Store<OffchainOrder>>,
    threshold: &ExecutionThreshold,
    assets: &AssetsConfig,
    is_trading_enabled: impl Fn(&Symbol) -> bool,
) -> Result<(), TradeAccountingError>
where
    E: Executor + Clone + Send + 'static,
    TradeAccountingError: From<E::Error>,
{
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
        "Found {} accumulated positions ready for execution",
        ready_positions.len()
    );

    for execution in ready_positions {
        let offchain_order_id = OffchainOrderId::new();

        info!(
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

        info!(
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

        match offchain_order.send(&offchain_order_id, command).await {
            Ok(()) => info!(
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

        if let Ok(Some(OffchainOrder::Failed { error, .. })) =
            offchain_order.load(&offchain_order_id).await
        {
            warn!(
                %offchain_order_id,
                symbol = %execution.symbol,
                %error,
                "Broker rejected order, clearing position pending state"
            );
            execute_fail_offchain_order_position(position, offchain_order_id, &execution, error)
                .await;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, TxHash, U256, address, bytes, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use rain_math_float::Float;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    use st0x_dto::ServerMessage;
    use st0x_event_sorcery::{StoreBuilder, test_store};
    use st0x_execution::{
        Direction, ExecutorOrderId, MarketOrder, MockExecutor, Positive, Symbol,
    };
    use st0x_finance::{Usd, Usdc};

    use super::*;
    use crate::bindings::IOrderBookV6::{
        ClearConfigV2, ClearV3, EvaluableV4, IOV2, OrderV4, TakeOrderConfigV4, TakeOrderV3,
    };
    use crate::conductor::builder::CqrsFrameworks;
    use crate::config::tests::create_test_ctx_with_order_owner;
    use crate::config::{AssetsConfig, EquitiesConfig, EquityAssetConfig, OperationMode};
    use crate::inventory::view::Operator;
    use crate::inventory::{ImbalanceThreshold, Inventory, InventoryView, Venue};
    use crate::onchain::trade::OnchainTrade;
    use crate::rebalancing::{RebalancingTrigger, TriggeredOperation};
    use crate::test_utils::{OnchainTradeBuilder, get_test_log, get_test_order, setup_test_db};
    use crate::threshold::ExecutionThreshold;
    use crate::trading::onchain::inclusion::EmittedOnChain;
    use crate::wrapper::mock::MockWrapper;
    use crate::wrapper::{RATIO_ONE, UnderlyingPerWrapped};
    use st0x_float_macro::float;

    fn one_to_one_ratio() -> UnderlyingPerWrapped {
        UnderlyingPerWrapped::new(RATIO_ONE).unwrap()
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
                vault_id: None,
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
                vault_id: None,
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
                vault_id: None,
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
                vault_id: None,
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
                vault_id: None,
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
                vault_id: None,
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




    #[tokio::test]
    async fn test_get_cutoff_block_with_timeout() {
        let asserter = Asserter::new();

        asserter.push_success(&serde_json::Value::from(12345u64));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let mut clear_stream = futures_util::stream::empty();
        let mut take_stream = futures_util::stream::empty();

        let cutoff_block = get_cutoff_block(&mut clear_stream, &mut take_stream, &provider)
            .await
            .unwrap();

        assert_eq!(cutoff_block, 12345);
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
            registry.usdc_vault.is_some(),
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
            registry.usdc_vault.is_some(),
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
            .any(|vault| vault.symbol == goog_symbol);

        assert!(
            has_goog_vault,
            "Equity vault should use the trade's symbol (GOOG), got vaults: {:?}",
            registry.equity_vaults.values().map(|vault| &vault.symbol).collect::<Vec<_>>()
        );
    }

    fn succeeding_order_placer() -> Arc<dyn OrderPlacer> {
        struct TestOrderPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for TestOrderPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>> {
                Ok(ExecutorOrderId::new("TEST_BROKER_ORD"))
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

    fn trade_processing_cqrs_with_threshold(
        frameworks: &CqrsFrameworks,
        threshold: ExecutionThreshold,
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
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let trade_event = make_trade_event(10);
        let trade = test_trade_with_amount(float!(0.5), 10);

        let result = process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            trade,
            &cqrs,
            true,
        )
        .await;

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
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let trade_event = make_trade_event(20);
        let trade = test_trade_with_amount(float!(1.5), 20);

        let result = process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            trade,
            &cqrs,
            true,
        )
        .await;

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
    async fn multiple_trades_accumulate_then_trigger() {
        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, succeeding_order_placer()).await;
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let trade_event_1 = make_trade_event(30);
        let trade_1 = test_trade_with_amount(float!(0.5), 30);

        let result_1 = process_queued_trade(
            &MockExecutor::new(),
            &trade_event_1,
            trade_1,
            &cqrs,
            true,
        )
        .await;

        assert_eq!(
            result_1.unwrap(),
            None,
            "First trade of 0.5 shares should not trigger"
        );

        let trade_event_2 = make_trade_event(31);
        let trade_2 = test_trade_with_amount(float!(0.7), 31);

        let result_2 = process_queued_trade(
            &MockExecutor::new(),
            &trade_event_2,
            trade_2,
            &cqrs,
            true,
        )
        .await;

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
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let trade_event_1 = make_trade_event(40);
        let trade_1 = test_trade_with_amount(float!(1.5), 40);

        let first_order_id = process_queued_trade(
            &MockExecutor::new(),
            &trade_event_1,
            trade_1,
            &cqrs,
            true,
        )
        .await
        .unwrap()
        .expect("first trade should place an order");

        let trade_event_2 = make_trade_event(41);
        let trade_2 = test_trade_with_amount(float!(1.5), 41);

        let result_2 = process_queued_trade(
            &MockExecutor::new(),
            &trade_event_2,
            trade_2,
            &cqrs,
            true,
        )
        .await;

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
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        // Process first trade -> places order
        let trade_event_1 = make_trade_event(50);
        let trade_1 = test_trade_with_amount(float!(1.5), 50);

        let first_order_id = process_queued_trade(
            &MockExecutor::new(),
            &trade_event_1,
            trade_1,
            &cqrs,
            true,
        )
        .await
        .unwrap()
        .expect("first trade should place an order");

        // Process second trade -> blocked by pending order
        let trade_event_2 = make_trade_event(51);
        let trade_2 = test_trade_with_amount(float!(1.5), 51);

        process_queued_trade(
            &MockExecutor::new(),
            &trade_event_2,
            trade_2,
            &cqrs,
            true,
        )
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
            &cqrs.position,
            &cqrs.position_projection,
            &cqrs.offchain_order,
            &cqrs.execution_threshold,
            &cqrs.assets,
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

        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
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

        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(initial_inventory, event_sender));
        let (operation_sender, _operation_receiver) = mpsc::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let trigger = Arc::new(RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: threshold,
                usdc: Some(threshold),

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

        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
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

        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
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
                ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>>
                {
                    Err("API error (403 Forbidden): trade denied due to pattern day trading protection".into())
                }
            }

            Arc::new(RejectingOrderPlacer)
        }

        let pool = setup_test_db().await;
        let (frameworks, _offchain_order_projection) =
            create_cqrs_frameworks_with_order_placer(&pool, failing_order_placer()).await;
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let trade_event = make_trade_event(70);
        let trade = test_trade_with_amount(float!(1.5), 70);

        process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            trade,
            &cqrs,
            true,
        )
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
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let executor = st0x_execution::MockExecutor::new();

        check_and_execute_accumulated_positions(
            &executor,
            &cqrs.position,
            &cqrs.position_projection,
            &cqrs.offchain_order,
            &cqrs.execution_threshold,
            &cqrs.assets,
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
                ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>>
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
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

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
            &cqrs.position,
            &cqrs.position_projection,
            &cqrs.offchain_order,
            &cqrs.execution_threshold,
            &cqrs.assets,
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
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

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

        process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            trade,
            &cqrs,
            true,
        )
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
        let cqrs =
            trade_processing_cqrs_with_threshold(&frameworks, ExecutionThreshold::whole_share());

        let trade_event = make_trade_event(60);

        let trade = test_trade_with_amount(float!("1.5"), 60);

        process_queued_trade(
            &MockExecutor::new(),
            &trade_event,
            trade,
            &cqrs,
            true,
        )
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
                vault_id: Some(fixed_bytes!(
                    "0x0000000000000000000000000000000000000000000000000000000000000001"
                )),
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
                vault_id: None,
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
}
