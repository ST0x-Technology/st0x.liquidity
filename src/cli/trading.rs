//! Trading order execution and transaction processing CLI commands.

use alloy::primitives::TxHash;
use alloy::providers::Provider;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use st0x_event_sorcery::{Store, StoreBuilder};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::alpaca_broker_api::{AlpacaLimitOrder, AlpacaLimitPrice};
use st0x_execution::{
    ClientOrderId, Direction, Executor, ExecutorOrderId, FractionalShares, MarketOrder,
    MockExecutor, MockExecutorCtx, OrderPlacement, OrderState, Positive, Symbol, TimeInForce,
    TryIntoExecutor,
};

use crate::offchain::order::{
    OffchainOrderCommand, OffchainOrderId, OrderPlacementResult, OrderPlacer,
    build_offchain_order_cqrs,
};
use crate::onchain::accumulator::check_execution_readiness;
use crate::onchain::pyth::PythFeedIds;
use crate::onchain::{OnChainError, OnchainTrade, TradeValidationError};
use crate::position::{Position, PositionCommand, TradeId};
use crate::symbol::cache::SymbolCache;
use st0x_config::ExecutionThreshold;
use st0x_config::{BrokerCtx, Ctx};
use st0x_float_serde::format_float_with_fallback;

/// OrderPlacer for the CLI that delegates to the broker-specific executor
/// constructed from config.
struct CliOrderPlacer {
    ctx: Ctx,
    pool: SqlitePool,
}

#[async_trait]
impl OrderPlacer for CliOrderPlacer {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
        let placement =
            execute_broker_order(&self.ctx, &self.pool, order, None, &mut std::io::sink()).await?;
        Ok(OrderPlacementResult {
            executor_order_id: ExecutorOrderId::new(&placement.order_id),
            placed_shares: placement.shares,
        })
    }
}

pub(super) fn create_order_placer(ctx: &Ctx, pool: &SqlitePool) -> Arc<dyn OrderPlacer> {
    Arc::new(CliOrderPlacer {
        ctx: ctx.clone(),
        pool: pool.clone(),
    })
}

pub(super) enum CliOrderKind {
    Market {
        time_in_force: Option<TimeInForce>,
    },
    AlpacaLimit {
        limit_price: AlpacaLimitPrice,
        extended_hours: bool,
    },
}

pub(super) struct CliOrderRequest {
    pub(super) symbol: Symbol,
    pub(super) shares: Positive<FractionalShares>,
    pub(super) direction: Direction,
    pub(super) kind: CliOrderKind,
}

impl CliOrderRequest {
    pub(super) fn from_cli_args(
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        time_in_force: Option<TimeInForce>,
        limit_price: Option<AlpacaLimitPrice>,
        extended_hours: bool,
    ) -> anyhow::Result<Self> {
        let kind = if let Some(limit_price) = limit_price {
            if time_in_force.is_some() {
                anyhow::bail!("--time-in-force is not supported with --limit-price");
            }

            CliOrderKind::AlpacaLimit {
                limit_price,
                extended_hours,
            }
        } else {
            if extended_hours {
                anyhow::bail!("--extended-hours requires --limit-price");
            }

            CliOrderKind::Market { time_in_force }
        };

        Ok(Self {
            symbol,
            shares,
            direction,
            kind,
        })
    }

    fn limit_price(&self) -> Option<&AlpacaLimitPrice> {
        match &self.kind {
            CliOrderKind::Market { .. } => None,
            CliOrderKind::AlpacaLimit { limit_price, .. } => Some(limit_price),
        }
    }

    fn extended_hours(&self) -> bool {
        match &self.kind {
            CliOrderKind::Market { .. } => false,
            CliOrderKind::AlpacaLimit { extended_hours, .. } => *extended_hours,
        }
    }
}

pub(super) async fn order_status_command<W: Write>(
    stdout: &mut W,
    order_id: &str,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔍 Checking order status for ID: {order_id}")?;

    let state = get_broker_order_status(ctx, pool, order_id, stdout).await?;

    match state {
        OrderState::Pending => {
            writeln!(stdout, "⏳ Order Status: PENDING")?;
            writeln!(
                stdout,
                "   The order has been created but not yet submitted."
            )?;
        }
        OrderState::Submitted { order_id } => {
            writeln!(stdout, "📤 Order Status: SUBMITTED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
            writeln!(
                stdout,
                "   The order has been submitted and is waiting to be filled."
            )?;
        }
        OrderState::PartiallyFilled { order_id, .. } => {
            writeln!(stdout, "⏳ Order Status: PARTIALLY FILLED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
        }
        OrderState::Cancelled { order_id, .. } => {
            writeln!(stdout, "🚫 Order Status: CANCELLED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
        }
        OrderState::Filled {
            executed_at,
            order_id,
            price,
        } => {
            writeln!(stdout, "✅ Order Status: FILLED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
            writeln!(stdout, "   Executed At: {executed_at}")?;
            writeln!(
                stdout,
                "   Fill Price: ${}",
                format_float_with_fallback(&price.into())
            )?;
        }
        OrderState::Failed {
            failed_at,
            error_reason,
            ..
        } => {
            writeln!(stdout, "❌ Order Status: FAILED")?;
            writeln!(stdout, "   Failed At: {failed_at}")?;
            if let Some(reason) = error_reason {
                writeln!(stdout, "   Reason: {reason}")?;
            }
        }
    }

    Ok(())
}

async fn get_broker_order_status<W: Write>(
    ctx: &Ctx,
    _pool: &SqlitePool,
    order_id: &str,
    _stdout: &mut W,
) -> anyhow::Result<OrderState> {
    match &ctx.broker {
        BrokerCtx::AlpacaBrokerApi(alpaca_auth) => {
            let broker = alpaca_auth.clone().try_into_executor().await?;
            Ok(broker.get_order_status(&order_id.to_string()).await?)
        }
        BrokerCtx::DryRun => {
            let broker = MockExecutorCtx.try_into_executor().await?;
            Ok(broker.get_order_status(&order_id.to_string()).await?)
        }
    }
}

pub(super) async fn execute_order_with_writers<W: Write>(
    request: CliOrderRequest,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let symbol_display = request.symbol.to_string();
    let quantity_display = request.shares.to_string();

    info!(
        symbol = %symbol_display,
        direction = ?request.direction,
        quantity = %quantity_display,
        limit_price = ?request.limit_price(),
        extended_hours = request.extended_hours(),
        "Received order request"
    );

    let execution = match &request.kind {
        CliOrderKind::AlpacaLimit { .. } => execute_alpaca_limit_order(&request, ctx, stdout).await,
        CliOrderKind::Market { .. } => execute_market_order(&request, ctx, pool, stdout).await,
    };

    match execution {
        Ok(placement) => {
            write_order_success(
                stdout,
                &placement,
                request.limit_price(),
                request.extended_hours(),
            )?;
        }
        Err(error) => {
            error!(
                symbol = %symbol_display,
                direction = ?request.direction,
                quantity = %quantity_display,
                error = ?error,
                "Failed to place order"
            );
            writeln!(stdout, "❌ Failed to place order: {error}")?;
            return Err(error);
        }
    }

    Ok(())
}

async fn execute_market_order<W: Write>(
    request: &CliOrderRequest,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<OrderPlacement<String>> {
    let time_in_force = match &request.kind {
        CliOrderKind::Market { time_in_force } => *time_in_force,
        CliOrderKind::AlpacaLimit { .. } => {
            anyhow::bail!("internal error: expected market order request")
        }
    };

    let market_order = MarketOrder {
        symbol: request.symbol.clone(),
        shares: request.shares,
        direction: request.direction,
        // Manual CLI placement; generate a fresh idempotency key so an
        // operator-issued retry cannot accidentally dedupe against an
        // unrelated earlier placement.
        client_order_id: ClientOrderId::cli(Uuid::new_v4()),
    };

    execute_broker_order(ctx, pool, market_order, time_in_force, stdout).await
}

async fn execute_alpaca_limit_order<W: Write>(
    request: &CliOrderRequest,
    ctx: &Ctx,
    stdout: &mut W,
) -> anyhow::Result<OrderPlacement<String>> {
    let (limit_price, extended_hours) = match &request.kind {
        CliOrderKind::AlpacaLimit {
            limit_price,
            extended_hours,
        } => (limit_price.clone(), *extended_hours),
        CliOrderKind::Market { .. } => {
            anyhow::bail!("internal error: expected Alpaca limit order request")
        }
    };

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("--limit-price is only supported with alpaca-broker-api");
    };

    writeln!(stdout, "🔄 Executing Alpaca Broker API limit order...")?;

    let broker = alpaca_auth.clone().try_into_executor().await?;
    let placement = broker
        .place_alpaca_limit_order(AlpacaLimitOrder {
            symbol: request.symbol.clone(),
            shares: request.shares,
            direction: request.direction,
            limit_price,
            extended_hours,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        })
        .await?;

    writeln!(
        stdout,
        "✅ Alpaca Broker API limit order placed with ID: {}",
        placement.order_id
    )?;

    Ok(placement)
}

fn write_order_success<W: Write>(
    stdout: &mut W,
    placement: &OrderPlacement<String>,
    limit_price: Option<&AlpacaLimitPrice>,
    extended_hours: bool,
) -> anyhow::Result<()> {
    info!(
        symbol = %placement.symbol,
        direction = ?placement.direction,
        quantity = %placement.shares,
        order_id = %placement.order_id,
        "Order placed successfully"
    );
    writeln!(stdout, "✅ Order placed successfully")?;
    writeln!(stdout, "   Symbol: {}", placement.symbol)?;
    writeln!(stdout, "   Action: {:?}", placement.direction)?;
    writeln!(stdout, "   Quantity: {}", placement.shares)?;
    writeln!(stdout, "   Order ID: {}", placement.order_id)?;

    if let Some(limit_price) = limit_price {
        writeln!(stdout, "   Order Type: limit")?;
        writeln!(
            stdout,
            "   Limit Price: ${}",
            format_float_with_fallback(&limit_price.as_price().inner().inner())
        )?;
        writeln!(
            stdout,
            "   Extended Hours: {}",
            if extended_hours { "yes" } else { "no" }
        )?;
    }

    Ok(())
}

pub(super) async fn process_tx_with_provider<W: Write, P: Provider + Clone + 'static>(
    tx_hash: TxHash,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
    provider: &P,
    cache: &SymbolCache,
    order_placer: Arc<dyn OrderPlacer>,
) -> anyhow::Result<()> {
    let evm_ctx = &ctx.evm;
    let pyth_feed_ids = PythFeedIds::new(ctx.pyth_feed_ids());
    let order_owner = ctx.order_owner();
    let read_evm = ReadOnlyEvm::new(provider.clone());

    match OnchainTrade::try_from_tx_hash(
        tx_hash,
        &read_evm,
        cache,
        evm_ctx,
        &pyth_feed_ids,
        order_owner,
    )
    .await
    {
        Ok(Some(onchain_trade)) => {
            process_found_trade(onchain_trade, ctx, pool, stdout, order_placer).await?;
        }
        Ok(None) => {
            writeln!(
                stdout,
                "❌ No tradeable events found in transaction {tx_hash}"
            )?;
            writeln!(
                stdout,
                "   This transaction may not contain orderbook events matching the configured order hash."
            )?;
        }
        Err(OnChainError::Validation(TradeValidationError::TransactionNotFound(hash))) => {
            writeln!(stdout, "❌ Transaction not found: {hash}")?;
            writeln!(
                stdout,
                "   Please verify the transaction hash and ensure the RPC endpoint is correct."
            )?;
        }
        Err(error) => {
            writeln!(stdout, "❌ Error processing transaction: {error}")?;
            return Err(error.into());
        }
    }

    Ok(())
}

pub(super) async fn execute_broker_order<W: Write>(
    ctx: &Ctx,
    _pool: &SqlitePool,
    market_order: MarketOrder,
    time_in_force: Option<TimeInForce>,
    stdout: &mut W,
) -> anyhow::Result<OrderPlacement<String>> {
    match &ctx.broker {
        BrokerCtx::AlpacaBrokerApi(alpaca_auth) => {
            writeln!(stdout, "🔄 Executing Alpaca Broker API order...")?;
            let mut auth = alpaca_auth.clone();
            if let Some(tif) = time_in_force {
                auth.time_in_force = tif;
            }
            let broker = auth.try_into_executor().await?;
            let placement = broker.place_market_order(market_order).await?;
            writeln!(
                stdout,
                "✅ Alpaca Broker API order placed with ID: {}",
                placement.order_id
            )?;
            Ok(placement)
        }
        BrokerCtx::DryRun => {
            writeln!(stdout, "🔄 Executing dry-run order...")?;
            if time_in_force.is_some() {
                writeln!(
                    stdout,
                    "⚠️  --time-in-force is ignored for dry-run \
                     (only supported by Alpaca Broker API)"
                )?;
            }
            let broker = MockExecutorCtx.try_into_executor().await?;
            let placement = broker.place_market_order(market_order).await?;
            writeln!(
                stdout,
                "✅ Dry-run order placed with ID: {}",
                placement.order_id
            )?;
            Ok(placement)
        }
    }
}

/// Poll cadence for the dividend-bump buy-fill wait. Mirrors the tokenization
/// poll loop in `alpaca_tokenize_command`: a market buy normally fills quickly,
/// but the composite flow must not tokenize against an unfilled order.
const BUY_FILL_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
const BUY_FILL_MAX_ATTEMPTS: u32 = 150;

/// Places a market buy and blocks until the broker reports it filled, so the
/// dividend NAV bump can act on the acquired shares instead of racing an
/// unfilled order into the tokenization step.
pub(super) async fn execute_market_buy_until_filled<W: Write>(
    ctx: &Ctx,
    symbol: Symbol,
    shares: Positive<FractionalShares>,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let market_order = MarketOrder {
        symbol,
        shares,
        direction: Direction::Buy,
        client_order_id: ClientOrderId::cli(Uuid::new_v4()),
    };

    match &ctx.broker {
        BrokerCtx::AlpacaBrokerApi(alpaca_auth) => {
            let broker = alpaca_auth.clone().try_into_executor().await?;
            place_market_order_until_filled(&broker, market_order, stdout).await
        }
        BrokerCtx::DryRun => {
            let broker = MockExecutorCtx.try_into_executor().await?;
            place_market_order_until_filled(&broker, market_order, stdout).await
        }
    }
}

async fn place_market_order_until_filled<Exec: Executor, W: Write>(
    broker: &Exec,
    market_order: MarketOrder,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let placement = broker.place_market_order(market_order).await?;
    writeln!(stdout, "   Buy order placed: {}", placement.order_id)?;

    for attempt in 1..=BUY_FILL_MAX_ATTEMPTS {
        match broker.get_order_status(&placement.order_id).await? {
            OrderState::Filled { order_id, .. } => {
                writeln!(stdout, "   Buy filled (order {order_id})")?;
                return Ok(());
            }
            OrderState::Failed { error_reason, .. } => {
                anyhow::bail!("buy order failed: {error_reason:?}");
            }
            OrderState::Cancelled { .. } => {
                anyhow::bail!("buy order was cancelled by the broker");
            }
            OrderState::PartiallyFilled { .. }
            | OrderState::Pending
            | OrderState::Submitted { .. } => {
                if attempt % 10 == 0 {
                    writeln!(
                        stdout,
                        "   Waiting for buy fill... (attempt {attempt}/{BUY_FILL_MAX_ATTEMPTS})"
                    )?;
                }
                tokio::time::sleep(BUY_FILL_POLL_INTERVAL).await;
            }
        }
    }

    anyhow::bail!("timed out waiting for the buy order to fill")
}

pub(super) async fn process_found_trade<W: Write>(
    onchain_trade: OnchainTrade,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
    order_placer: Arc<dyn OrderPlacer>,
) -> anyhow::Result<()> {
    display_trade_details(&onchain_trade, stdout)?;

    writeln!(stdout, "🔄 Processing trade with TradeAccumulator...")?;

    let (position_store, position_projection) = StoreBuilder::<Position>::new(pool.clone())
        .build(())
        .await?;
    let (offchain_order_store, _) = build_offchain_order_cqrs(pool, order_placer).await?;

    update_position_aggregate(&position_store, &onchain_trade, ctx.execution_threshold).await;

    let executor_type = ctx.broker.to_supported_executor();
    let base_symbol = onchain_trade.symbol.base();

    // CLI test command uses MockExecutor (market always open)
    let trading_enabled = ctx.is_trading_enabled(base_symbol);

    if !trading_enabled {
        writeln!(
            stdout,
            "Trading disabled by configuration for {base_symbol}"
        )?;
        return Ok(());
    }

    let executor = MockExecutor::new();
    let Some(params) = check_execution_readiness(
        &executor,
        &position_projection,
        base_symbol,
        executor_type,
        &ctx.assets,
        trading_enabled,
    )
    .await?
    else {
        writeln!(
            stdout,
            "Trade accumulated but did not trigger execution yet."
        )?;
        writeln!(
            stdout,
            "   (Waiting to accumulate enough shares for a whole share execution)"
        )?;
        return Ok(());
    };

    let offchain_order_id = OffchainOrderId::new();

    writeln!(
        stdout,
        "Trade triggered execution for {executor_type:?} (ID: {offchain_order_id})"
    )?;

    if let Err(error) = position_store
        .send(
            &params.symbol,
            PositionCommand::PlaceOffChainOrder {
                offchain_order_id,
                shares: params.shares,
                direction: params.direction,
                executor: params.executor,
                threshold: ctx.execution_threshold,
            },
        )
        .await
    {
        error!(%offchain_order_id, symbol = %params.symbol, "Failed to execute Position::PlaceOffChainOrder: {error}");
    }

    // Reuse a prior failed attempt's stashed OffchainOrderId as the broker-side
    // idempotency anchor (mirroring the automated `execute_create_offchain_order`
    // path) so a CLI retry after a transient broker failure dedupes instead of
    // placing a second order. Fall back to this attempt's id when there is none.
    let anchor = match position_store.load(&params.symbol).await {
        Ok(position) => position.and_then(|position| position.last_failed_offchain_order_id),
        Err(error) => {
            error!(%offchain_order_id, symbol = %params.symbol, %error, "Failed to load position for the idempotency anchor; placing under a fresh key");
            None
        }
    };
    let client_order_id_source = anchor.unwrap_or(offchain_order_id);

    if let Err(error) = offchain_order_store
        .send(
            &offchain_order_id,
            OffchainOrderCommand::Place {
                symbol: params.symbol.clone(),
                shares: params.shares,
                direction: params.direction,
                executor: params.executor,
                client_order_id: ClientOrderId::from_uuid(client_order_id_source.as_uuid()),
            },
        )
        .await
    {
        error!(%offchain_order_id, "Failed to execute OffchainOrder::Place: {error}");
    }

    writeln!(stdout, "Trade processing completed!")?;

    Ok(())
}

async fn update_position_aggregate(
    position_store: &Store<Position>,
    onchain_trade: &OnchainTrade,
    execution_threshold: ExecutionThreshold,
) {
    let base_symbol = onchain_trade.symbol.base();

    acknowledge_fill(
        position_store,
        base_symbol,
        onchain_trade,
        execution_threshold,
    )
    .await;
}

fn extract_fill_params(
    onchain_trade: &OnchainTrade,
) -> Option<(FractionalShares, Float, DateTime<Utc>)> {
    let Some(block_timestamp) = onchain_trade.block_timestamp else {
        error!(
            tx_hash = %onchain_trade.tx_hash,
            log_index = onchain_trade.log_index,
            "Missing block timestamp, cannot acknowledge onchain fill"
        );
        return None;
    };

    let amount = onchain_trade.amount;
    let price_usdc = onchain_trade.price.value();

    Some((amount, price_usdc, block_timestamp))
}

async fn acknowledge_fill(
    position_store: &Store<Position>,
    symbol: &Symbol,
    onchain_trade: &OnchainTrade,
    execution_threshold: ExecutionThreshold,
) {
    let base_symbol = onchain_trade.symbol.base();

    let Some((amount, price_usdc, block_timestamp)) = extract_fill_params(onchain_trade) else {
        return;
    };

    if let Err(error) = position_store
        .send(
            symbol,
            PositionCommand::AcknowledgeOnChainFill {
                symbol: base_symbol.clone(),
                threshold: execution_threshold,
                trade_id: TradeId {
                    tx_hash: onchain_trade.tx_hash,
                    log_index: onchain_trade.log_index,
                },
                amount,
                direction: onchain_trade.direction,
                price_usdc,
                block_timestamp,
            },
        )
        .await
    {
        error!(
            symbol = %base_symbol,
            execution_threshold = ?execution_threshold,
            tx_hash = %onchain_trade.tx_hash,
            log_index = onchain_trade.log_index,
            block_timestamp = ?onchain_trade.block_timestamp,
            %error,
            "Failed to acknowledge onchain fill in position aggregate"
        );
    }
}

fn display_trade_details<W: Write>(
    onchain_trade: &OnchainTrade,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let ticker = onchain_trade.symbol.base().to_string();

    writeln!(stdout, "✅ Found opposite-side trade opportunity:")?;
    writeln!(stdout, "   Transaction: {}", onchain_trade.tx_hash)?;
    writeln!(stdout, "   Log Index: {}", onchain_trade.log_index)?;
    writeln!(stdout, "   Symbol: {ticker}")?;
    writeln!(stdout, "   Direction: {:?}", onchain_trade.direction)?;
    writeln!(stdout, "   Quantity: {}", onchain_trade.amount)?;
    writeln!(
        stdout,
        "   Price per Share: ${}",
        format_float_with_fallback(&onchain_trade.price.value())
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use httpmock::MockServer;
    use regex::Regex;
    use serde_json::json;
    use url::Url;
    use uuid::uuid;

    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        AssetsConfig, BrokerCtx, EquitiesConfig, EvmCtx, ExecutionThreshold, IngestionCutoff,
        LogLevel, TradingMode,
    };
    use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode};

    use super::*;
    use crate::test_utils::{positive_shares, setup_test_db};

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_base_test_ctx() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            board_port: 8081,
            evm: EvmCtx {
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
                required_confirmations: 0,
                ingestion_cutoff: IngestionCutoff::Safe,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            order_fill_poll_interval: 5,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            alerts: None,
            trading_mode: TradingMode::Standalone,
            order_owner: Address::ZERO,
            wallet: None,
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            travel_rule: None,
            rest_api: None,
            issuance: create_test_issuance_ctx(),
            redemption_wallet: None,
        }
    }

    fn create_alpaca_broker_api_test_ctx(mock_server: &MockServer) -> Ctx {
        let mut ctx = create_base_test_ctx();
        ctx.broker = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(AlpacaBrokerApiMode::Mock(mock_server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
            time_in_force: TimeInForce::Day,
        });
        ctx
    }

    fn cli_order_request(
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        time_in_force: Option<TimeInForce>,
        limit_price: Option<AlpacaLimitPrice>,
        extended_hours: bool,
    ) -> anyhow::Result<CliOrderRequest> {
        CliOrderRequest::from_cli_args(
            symbol,
            shares,
            direction,
            time_in_force,
            limit_price,
            extended_hours,
        )
    }

    fn positive_limit_price(value: &str) -> AlpacaLimitPrice {
        AlpacaLimitPrice::try_new(
            Positive::new(st0x_execution::Usd::new(
                Float::parse(value.to_string()).unwrap(),
            ))
            .unwrap(),
        )
        .unwrap()
    }

    macro_rules! execute_order_with_writers {
        (
            $symbol:expr,
            $shares:expr,
            $direction:expr,
            $time_in_force:expr,
            $limit_price:expr,
            $extended_hours:expr,
            $ctx:expr,
            $pool:expr,
            $stdout:expr $(,)?
        ) => {
            async {
                let request = cli_order_request(
                    $symbol,
                    $shares,
                    $direction,
                    $time_in_force,
                    $limit_price,
                    $extended_hours,
                )?;
                super::execute_order_with_writers(request, $ctx, $pool, $stdout).await
            }
        };
    }

    fn setup_alpaca_broker_market_order_mocks<'a>(
        server: &'a MockServer,
        symbol: &'a str,
        quantity: &'a str,
        side: &'a str,
    ) -> (httpmock::Mock<'a>, httpmock::Mock<'a>, httpmock::Mock<'a>) {
        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE"
                }));
        });

        let asset_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!("/v1/assets/{symbol}"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": symbol,
                    "status": "active",
                    "tradable": true,
                    "overnight_tradable": true
                }));
        });

        // The broker-side `client_order_id` is a fresh UUID per CLI invocation,
        // so its exact value cannot be pinned. Assert the value is a well-formed
        // CLI idempotency key (`cli-{uuid}`) so a placement can never be issued
        // with a missing or malformed key (the chaos tests use the broker mock,
        // not this httpmock, so they do not cover it); `json_body_includes`
        // covers the static fields.
        let client_order_id_pattern = Regex::new(
            r#""client_order_id":"cli-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}""#,
        )
        .unwrap();

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .body_matches(client_order_id_pattern.clone())
                .json_body_includes(
                    json!({
                        "symbol": symbol,
                        "qty": quantity,
                        "side": side,
                        "type": "market",
                        "time_in_force": "day",
                        "extended_hours": false
                    })
                    .to_string(),
                );
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": symbol,
                    "qty": quantity,
                    "side": side,
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        (account_mock, asset_mock, order_mock)
    }

    fn setup_alpaca_broker_limit_order_mocks(
        server: &MockServer,
    ) -> (httpmock::Mock<'_>, httpmock::Mock<'_>, httpmock::Mock<'_>) {
        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE"
                }));
        });

        let asset_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/assets/AAPL");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "status": "active",
                    "tradable": true,
                    "overnight_tradable": true
                }));
        });

        let order_mock = server.mock(|when, then| {
            // Match on method + path only: the request body now carries a
            // generated `client_order_id`, and the field-level body shape is
            // verified by the execution crate's own order tests.
            when.method(httpmock::Method::POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "AAPL",
                    "qty": "10",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        (account_mock, asset_mock, order_mock)
    }

    #[tokio::test]
    async fn test_execute_order_buy_success() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let (account_mock, asset_mock, order_mock) =
            setup_alpaca_broker_market_order_mocks(&server, "AAPL", "100", "buy");

        execute_order_with_writers!(
            Symbol::new("AAPL").unwrap(),
            positive_shares("100"),
            Direction::Buy,
            None,
            None,
            false,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap();

        account_mock.assert();
        asset_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_order_sell_success() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let (account_mock, asset_mock, order_mock) =
            setup_alpaca_broker_market_order_mocks(&server, "TSLA", "50", "sell");

        execute_order_with_writers!(
            Symbol::new("TSLA").unwrap(),
            positive_shares("50"),
            Direction::Sell,
            None,
            None,
            false,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap();

        account_mock.assert();
        asset_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_order_failure_stdout_contains_error() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;

        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE"
                }));
        });

        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/assets/AAPL");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "status": "active",
                    "tradable": true,
                    "overnight_tradable": true
                }));
        });

        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "Insufficient funds"}));
        });

        let mut stdout_buffer = Vec::new();
        execute_order_with_writers!(
            Symbol::new("AAPL").unwrap(),
            positive_shares("100"),
            Direction::Buy,
            None,
            None,
            false,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap_err();

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("❌ Failed to place order"),
            "Expected failure message, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_time_in_force_warning_for_dry_run() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;

        let mut stdout_buffer = Vec::new();
        execute_order_with_writers!(
            Symbol::new("AAPL").unwrap(),
            positive_shares("100"),
            Direction::Buy,
            Some(TimeInForce::Day),
            None,
            false,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("--time-in-force is ignored"),
            "Expected warning about ignored --time-in-force, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_limit_order_uses_alpaca_broker_api_path() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let (account_mock, asset_mock, order_mock) = setup_alpaca_broker_limit_order_mocks(&server);

        let mut stdout_buffer = Vec::new();
        execute_order_with_writers!(
            Symbol::new("AAPL").unwrap(),
            positive_shares("10"),
            Direction::Buy,
            None,
            Some(positive_limit_price("195.25")),
            true,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap();

        account_mock.assert();
        asset_mock.assert();
        order_mock.assert();

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("Order Type: limit"),
            "unexpected output: {output}"
        );
        assert!(
            output.contains("Extended Hours: yes"),
            "unexpected output: {output}"
        );
        assert!(
            output.contains("Order ID: 61e7b016-9c91-4a97-b912-615c9d365c9d"),
            "unexpected output: {output}"
        );
    }

    #[tokio::test]
    async fn test_extended_hours_requires_limit_price() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;

        let mut stdout_buffer = Vec::new();
        let error = execute_order_with_writers!(
            Symbol::new("AAPL").unwrap(),
            positive_shares("10"),
            Direction::Buy,
            None,
            None,
            true,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), "--extended-hours requires --limit-price");
    }

    #[tokio::test]
    async fn test_limit_order_rejected_for_dry_run() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;

        let mut stdout_buffer = Vec::new();
        let error = execute_order_with_writers!(
            Symbol::new("AAPL").unwrap(),
            positive_shares("10"),
            Direction::Buy,
            None,
            Some(positive_limit_price("195.25")),
            false,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "--limit-price is only supported with alpaca-broker-api"
        );
    }

    #[tokio::test]
    async fn test_limit_order_rejects_market_on_close() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;

        let mut stdout_buffer = Vec::new();
        let error = execute_order_with_writers!(
            Symbol::new("AAPL").unwrap(),
            positive_shares("10"),
            Direction::Buy,
            Some(TimeInForce::MarketOnClose),
            Some(positive_limit_price("195.25")),
            false,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "--time-in-force is not supported with --limit-price"
        );
    }

    #[tokio::test]
    async fn market_buy_returns_once_the_broker_reports_filled() {
        // MockExecutor reports Filled by default, so the poll resolves on the
        // first status check.
        let broker = MockExecutor::new();
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("10"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        };
        let mut stdout = Vec::new();

        place_market_order_until_filled(&broker, order, &mut stdout)
            .await
            .expect("a filled order must resolve the buy-fill wait");

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Buy filled"),
            "the buy-fill wait must report the fill, got: {output}"
        );
    }

    #[tokio::test]
    async fn market_buy_fails_when_the_broker_rejects_the_order() {
        let broker = MockExecutor::new().with_order_status(OrderState::Failed {
            failed_at: Utc::now(),
            error_reason: Some("rejected".to_string()),
            shares_filled: None,
            avg_price: None,
        });
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("10"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        };
        let mut stdout = Vec::new();

        let error = place_market_order_until_filled(&broker, order, &mut stdout)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("buy order failed"),
            "a rejected order must fail the buy-fill wait, got: {error}"
        );
    }
}
