//! Trading order execution and transaction processing CLI commands.

use alloy::primitives::TxHash;
use alloy::providers::Provider;
use async_trait::async_trait;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use st0x_config::{BrokerCtx, Ctx};
use st0x_event_sorcery::{Store, StoreBuilder};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::alpaca_broker_api::{AlpacaLimitOrder, AlpacaLimitPrice};
use st0x_execution::{
    ClientOrderId, Direction, Executor, ExecutorOrderId, FractionalShares, MarketOrder,
    MockExecutor, MockExecutorCtx, OrderPlacement, OrderState, Positive, Symbol, TimeInForce,
    TryIntoExecutor,
};
use st0x_float_serde::format_float_with_fallback;

use crate::conductor::{
    FillAccountingOutcome, account_for_onchain_fill, execute_mark_acknowledged,
    execute_settle_fill, is_expected_place_offchain_order_rejection,
};
use crate::offchain::order::{
    OffchainOrder, OffchainOrderId, OffchainOrderPlacement, OrderPlacementResult, OrderPlacer,
    TerminalPositionFinalization, client_order_id_for_placement, place_offchain_order_at_broker,
    position_command_for_finalization, terminal_position_finalization,
};
use crate::onchain::accumulator::check_execution_readiness;
use crate::onchain::pyth::PythFeedIds;
use crate::onchain::{OnChainError, OnchainTrade, TradeValidationError};
use crate::onchain_trade::{OnChainTrade, OnChainTradeId};
use crate::position::{Position, PositionCommand};
use crate::symbol::cache::SymbolCache;

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
            is_extended_hours: placement.extended_hours,
            limit_price: placement.limit_price,
        })
    }

    async fn place_limit_order(
        &self,
        _order: st0x_execution::LimitOrder,
    ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
        Err("CLI does not support automated limit order placement".into())
    }

    async fn cancel_order(
        &self,
        _executor_order_id: &ExecutorOrderId,
    ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
        Err("CLI does not support order cancellation".into())
    }

    async fn get_order_status(
        &self,
        _executor_order_id: &ExecutorOrderId,
    ) -> Result<OrderState, Box<dyn std::error::Error + Send + Sync>> {
        Err("CLI does not support reading order status via OrderPlacer".into())
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

    write_order_status(stdout, state)?;

    Ok(())
}

fn write_order_status<W: Write>(stdout: &mut W, state: OrderState) -> anyhow::Result<()> {
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
        OrderState::PartiallyFilled {
            order_id,
            shares_filled,
            avg_price,
            partially_filled_at,
        } => {
            writeln!(stdout, "🟡 Order Status: PARTIALLY FILLED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
            writeln!(stdout, "   Partially Filled At: {partially_filled_at}")?;
            writeln!(stdout, "   Shares Filled: {shares_filled}")?;
            if let Some(price) = avg_price {
                writeln!(stdout, "   Avg Fill Price: ${price}")?;
            }
        }
        OrderState::Filled {
            executed_at,
            order_id,
            price,
        } => {
            writeln!(stdout, "✅ Order Status: FILLED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
            writeln!(stdout, "   Executed At: {executed_at}")?;
            writeln!(stdout, "   Fill Price: ${price}")?;
        }
        OrderState::Cancelled {
            cancelled_at,
            order_id,
            shares_filled,
            avg_price,
        } => {
            writeln!(stdout, "🚫 Order Status: CANCELLED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
            writeln!(stdout, "   Cancelled At: {cancelled_at}")?;
            if shares_filled != FractionalShares::ZERO {
                writeln!(stdout, "   Shares Filled: {shares_filled}")?;
            }
            if let Some(avg_price) = avg_price {
                writeln!(stdout, "   Avg Fill Price: ${avg_price}")?;
            }
        }
        OrderState::Failed {
            failed_at,
            error_reason,
            shares_filled,
            avg_price,
        } => {
            writeln!(stdout, "❌ Order Status: FAILED")?;
            writeln!(stdout, "   Failed At: {failed_at}")?;
            if let Some(reason) = error_reason {
                writeln!(stdout, "   Reason: {reason}")?;
            }
            if let Some(shares_filled) = shares_filled {
                writeln!(stdout, "   Shares Filled: {shares_filled}")?;
            }
            if let Some(avg_price) = avg_price {
                writeln!(stdout, "   Avg Fill Price: ${avg_price}")?;
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
            OrderState::Cancelled {
                shares_filled,
                avg_price,
                ..
            } => {
                if shares_filled == FractionalShares::ZERO {
                    anyhow::bail!("buy order was cancelled by the broker");
                }

                let Some(avg_price) = avg_price else {
                    anyhow::bail!("buy order was cancelled after partial fill of {shares_filled}");
                };

                anyhow::bail!(
                    "buy order was cancelled after partial fill of {shares_filled} \
                     at average price ${avg_price}"
                );
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

    let trade_id = OnChainTradeId {
        tx_hash: onchain_trade.tx_hash,
        log_index: onchain_trade.log_index,
    };

    // Build stores. CLI path: per-invocation instances are fine (single-instance
    // rule applies to the server path only; see AGENTS.md).
    let onchain_trade_store = StoreBuilder::<OnChainTrade>::new(pool.clone())
        .build(())
        .await?;
    let (position_store, position_projection) = StoreBuilder::<Position>::new(pool.clone())
        .build(())
        .await?;
    let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
        .build(order_placer.clone())
        .await?;

    let Some(block_number) = onchain_trade.block_number else {
        anyhow::bail!("Fill {trade_id}: missing block_number, cannot witness fill");
    };

    let FillAccountingOutcome::Accounted { trade_id } = account_for_onchain_fill(
        pool,
        &onchain_trade_store,
        &position_store,
        &onchain_trade,
        block_number,
        ctx.execution_threshold,
    )
    .await?
    else {
        writeln!(
            stdout,
            "Fill {trade_id} is already fully accounted. Nothing to do; \
             the normal pipeline will hedge any unhedged position exposure."
        )?;
        return Ok(());
    };

    let executor_type = ctx.broker.to_supported_executor();
    let base_symbol = onchain_trade.symbol.base();

    match reconcile_existing_pending_order(
        &offchain_order_store,
        &position_store,
        base_symbol,
        stdout,
    )
    .await?
    {
        CliPendingReconciliation::NoPending | CliPendingReconciliation::Cleared => {}
        CliPendingReconciliation::InFlight => {
            mark_and_settle_fill(
                &onchain_trade_store,
                &position_store,
                &trade_id,
                &onchain_trade,
            )
            .await?;
            return Ok(());
        }
    }

    let trading_enabled = ctx.assets.is_trading_enabled(base_symbol);

    if !trading_enabled {
        writeln!(
            stdout,
            "Trading disabled by configuration for {base_symbol}"
        )?;
        mark_and_settle_fill(
            &onchain_trade_store,
            &position_store,
            &trade_id,
            &onchain_trade,
        )
        .await?;
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
        mark_and_settle_fill(
            &onchain_trade_store,
            &position_store,
            &trade_id,
            &onchain_trade,
        )
        .await?;
        return Ok(());
    };

    let offchain_order_id = OffchainOrderId::new();

    writeln!(
        stdout,
        "Trade triggered execution for {executor_type:?} (ID: {offchain_order_id})"
    )?;

    let anchor = position_store
        .load(&params.symbol)
        .await
        .inspect_err(|error| {
            error!(
                %offchain_order_id,
                symbol = %params.symbol,
                %error,
                "Failed to load position for the idempotency anchor; refusing \
                 placement until it can be read"
            );
        })?
        .and_then(|position| position.last_failed_offchain_order_id);

    match position_store
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
        Ok(()) => {}
        Err(error) if is_expected_place_offchain_order_rejection(&error) => {
            info!(
                %offchain_order_id,
                symbol = %params.symbol,
                "Position::PlaceOffChainOrder rejected by domain state: {error}"
            );
            mark_and_settle_fill(
                &onchain_trade_store,
                &position_store,
                &trade_id,
                &onchain_trade,
            )
            .await?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    }

    let client_order_id = client_order_id_for_placement(offchain_order_id, anchor);

    place_offchain_order_at_broker(
        &offchain_order_store,
        order_placer.as_ref(),
        &offchain_order_id,
        OffchainOrderPlacement::market(
            params.symbol.clone(),
            params.shares,
            params.direction,
            params.executor,
            client_order_id,
        ),
    )
    .await?;

    reconcile_post_place_state(
        &offchain_order_store,
        &position_store,
        &params.symbol,
        offchain_order_id,
        stdout,
    )
    .await?;

    mark_and_settle_fill(
        &onchain_trade_store,
        &position_store,
        &trade_id,
        &onchain_trade,
    )
    .await?;
    Ok(())
}

async fn mark_and_settle_fill(
    onchain_trade_store: &Store<OnChainTrade>,
    position_store: &Store<Position>,
    trade_id: &OnChainTradeId,
    onchain_trade: &OnchainTrade,
) -> anyhow::Result<()> {
    execute_mark_acknowledged(onchain_trade_store, trade_id).await?;
    execute_settle_fill(position_store, onchain_trade).await?;

    Ok(())
}

enum CliPendingReconciliation {
    NoPending,
    InFlight,
    Cleared,
}

#[derive(Clone, Copy)]
enum CliPendingClearNextStep {
    ContinueThisRun,
    NormalPipelineNextCycle,
}

async fn reconcile_existing_pending_order<W: Write>(
    offchain_order_store: &Store<OffchainOrder>,
    position_store: &Store<Position>,
    symbol: &Symbol,
    stdout: &mut W,
) -> anyhow::Result<CliPendingReconciliation> {
    let Some(position) = position_store.load(symbol).await? else {
        return Ok(CliPendingReconciliation::NoPending);
    };

    let Some(offchain_order_id) = position.pending_offchain_order_id else {
        return Ok(CliPendingReconciliation::NoPending);
    };

    let loaded_order = offchain_order_store
        .load(&offchain_order_id)
        .await
        .inspect_err(|error| {
            error!(
                %offchain_order_id,
                %symbol,
                %error,
                "Failed to load existing pending offchain order; cannot safely acknowledge fill"
            );
        })?;

    reconcile_loaded_post_place_state(
        loaded_order,
        position_store,
        symbol,
        offchain_order_id,
        CliPendingClearNextStep::ContinueThisRun,
        stdout,
    )
    .await
}

// Mirrors dispatch_post_place_state in the normal pipeline: inspect the
// persisted offchain order state and clear the position pending marker on
// failure so the normal pipeline can re-hedge on its next cycle.
async fn reconcile_post_place_state<W: Write>(
    offchain_order_store: &Store<OffchainOrder>,
    position_store: &Store<Position>,
    symbol: &Symbol,
    offchain_order_id: OffchainOrderId,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let loaded_order = offchain_order_store
        .load(&offchain_order_id)
        .await
        .inspect_err(|error| {
            error!(
                %offchain_order_id,
                %symbol,
                %error,
                "Failed to load offchain order after Place; cannot determine post-broker state"
            );
        })?;

    reconcile_loaded_post_place_state(
        loaded_order,
        position_store,
        symbol,
        offchain_order_id,
        CliPendingClearNextStep::NormalPipelineNextCycle,
        stdout,
    )
    .await
    .map(|_| ())
}

async fn reconcile_loaded_post_place_state<W: Write>(
    loaded_order: Option<OffchainOrder>,
    position_store: &Store<Position>,
    symbol: &Symbol,
    offchain_order_id: OffchainOrderId,
    next_step: CliPendingClearNextStep,
    stdout: &mut W,
) -> anyhow::Result<CliPendingReconciliation> {
    match loaded_order {
        Some(OffchainOrder::Failed { error, .. }) => {
            // Broker placement failed: clear pending_offchain_order_id so the
            // position is not permanently stuck and the normal pipeline can retry.
            position_store
                .send(
                    symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error,
                    },
                )
                .await?;
            write_pending_clear_message(stdout, "Hedge placement failed", next_step)?;
            Ok(CliPendingReconciliation::Cleared)
        }
        Some(OffchainOrder::Submitted { .. } | OffchainOrder::PartiallyFilled { .. }) => {
            // Order submitted to the broker. The CLI does not enqueue a live
            // polling job; the order will be reconciled to a terminal state by
            // the order-status recovery sweep on the next bot startup.
            writeln!(stdout, "Trade processing completed!")?;
            Ok(CliPendingReconciliation::InFlight)
        }
        Some(OffchainOrder::Cancelling { .. }) => {
            writeln!(
                stdout,
                "Pending hedge cancellation is still in progress; not placing another hedge."
            )?;
            Ok(CliPendingReconciliation::InFlight)
        }
        None => {
            position_store
                .send(
                    symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error: "Offchain order missing after Place".to_owned(),
                    },
                )
                .await?;
            write_pending_clear_message(
                stdout,
                "Hedge placement produced unexpected state",
                next_step,
            )?;
            Ok(CliPendingReconciliation::Cleared)
        }
        Some(OffchainOrder::Pending { .. }) => {
            anyhow::bail!(
                "offchain order {offchain_order_id} for {symbol} is in an unexpected \
                 post-placement state; refusing to clear the position claim"
            );
        }
        Some(order @ (OffchainOrder::Filled { .. } | OffchainOrder::Cancelled { .. })) => {
            reconcile_terminal_post_place_state(
                &order,
                position_store,
                symbol,
                offchain_order_id,
                next_step,
                stdout,
            )
            .await
        }
    }
}

async fn reconcile_terminal_post_place_state<W: Write>(
    order: &OffchainOrder,
    position_store: &Store<Position>,
    symbol: &Symbol,
    offchain_order_id: OffchainOrderId,
    next_step: CliPendingClearNextStep,
    stdout: &mut W,
) -> anyhow::Result<CliPendingReconciliation> {
    let Some(finalization) = terminal_position_finalization(order) else {
        anyhow::bail!(
            "offchain order {offchain_order_id} for {symbol} is terminal but did not \
             produce a position finalization; refusing to clear the position claim"
        );
    };

    let command = match finalization {
        TerminalPositionFinalization::UnpricedFill { shares_filled } => {
            anyhow::bail!(
                "offchain order {offchain_order_id} for {symbol} has {shares_filled} filled \
                 shares without an average price; refusing to clear the position claim"
            );
        }
        finalization => {
            let Some(command) = position_command_for_finalization(finalization, offchain_order_id)
            else {
                anyhow::bail!(
                    "offchain order {offchain_order_id} for {symbol} could not be mapped to a \
                     position finalization command; refusing to clear the position claim"
                );
            };
            command
        }
    };

    position_store.send(symbol, command).await?;
    write_pending_clear_message(stdout, "Existing pending hedge finalized", next_step)?;

    Ok(CliPendingReconciliation::Cleared)
}

fn write_pending_clear_message<W: Write>(
    stdout: &mut W,
    reason: &str,
    next_step: CliPendingClearNextStep,
) -> anyhow::Result<()> {
    let next_step_message = match next_step {
        CliPendingClearNextStep::ContinueThisRun => {
            "process-tx will re-evaluate the position in this run."
        }
        CliPendingClearNextStep::NormalPipelineNextCycle => {
            "The normal pipeline will re-hedge on the next cycle."
        }
    };

    writeln!(
        stdout,
        "{reason}; pending order cleared. {next_step_message}"
    )?;

    Ok(())
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
    use alloy::primitives::{Address, B256, U256, address};
    use chrono::Utc;
    use httpmock::MockServer;
    use rain_math_float::Float;
    use regex::Regex;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;
    use url::Url;
    use uuid::uuid;

    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        AssetsConfig, BrokerCtx, EquitiesConfig, EquityAssetConfig, EvmCtx, ExecutionThreshold,
        IngestionCutoff, LogLevel, OperationMode, TradingMode,
    };
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, CancellationOutcome,
        ExecutionError, InventoryResult, LimitOrder, SupportedExecutor,
    };

    use super::*;
    use crate::bindings::IRaindexV6::{ClearConfigV2, ClearV3};
    use crate::conductor::{
        TradeProcessingCqrs, execute_acknowledge_fill, execute_mark_acknowledged,
        process_queued_trade,
    };
    use crate::offchain::order::{CancellationReason, PollOrderStatusJobQueue, noop_order_placer};
    use crate::onchain::trade::RaindexTradeEvent;
    use crate::onchain_trade::{OnChainTrade as OnChainTradeCqrs, OnChainTradeCommand};
    use crate::test_utils::{
        OnchainTradeBuilder, get_test_order, positive_shares, setup_test_db, setup_test_pools,
    };
    use crate::trading::onchain::inclusion::EmittedOnChain;
    use crate::trading::onchain::trade_accountant::TradeAccountingError;

    /// `OrderPlacer` that always returns a broker error, used to drive the
    /// `OffchainOrder::Failed` path in `process_found_trade` tests.
    struct FailingOrderPlacer;

    #[async_trait]
    impl OrderPlacer for FailingOrderPlacer {
        async fn place_market_order(
            &self,
            _order: MarketOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Err("broker rejected the order".into())
        }

        async fn place_limit_order(
            &self,
            _order: LimitOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Err("broker rejected the order".into())
        }

        async fn cancel_order(
            &self,
            _executor_order_id: &ExecutorOrderId,
        ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
            Err("broker rejected the cancellation".into())
        }
    }

    /// `OrderPlacer` that always returns a successful placement, used to drive
    /// the `OffchainOrder::Submitted` happy-path in `process_found_trade` tests.
    struct SucceedingOrderPlacer;

    #[async_trait]
    impl OrderPlacer for SucceedingOrderPlacer {
        async fn place_market_order(
            &self,
            order: MarketOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Ok(OrderPlacementResult {
                executor_order_id: ExecutorOrderId::new("test-broker-order-id"),
                placed_shares: order.shares,
                is_extended_hours: false,
                limit_price: None,
            })
        }

        async fn place_limit_order(
            &self,
            order: LimitOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Ok(OrderPlacementResult {
                executor_order_id: ExecutorOrderId::new("test-broker-order-id"),
                placed_shares: order.shares,
                is_extended_hours: order.extended_hours,
                limit_price: Some(order.limit_price),
            })
        }

        async fn cancel_order(
            &self,
            _executor_order_id: &ExecutorOrderId,
        ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
            Err("unexpected cancellation from CLI test order placer".into())
        }
    }

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[derive(Clone)]
    struct SequencedStatusExecutor {
        statuses: Arc<Vec<OrderState>>,
        status_calls: Arc<AtomicUsize>,
    }

    impl SequencedStatusExecutor {
        fn new(statuses: Vec<OrderState>) -> Self {
            Self {
                statuses: Arc::new(statuses),
                status_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn status_calls(&self) -> usize {
            self.status_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Executor for SequencedStatusExecutor {
        type Error = ExecutionError;
        type OrderId = String;
        type Ctx = ();

        async fn try_from_ctx(_ctx: Self::Ctx) -> Result<Self, Self::Error> {
            Ok(Self::new(vec![OrderState::Pending]))
        }

        async fn is_market_open(&self) -> Result<bool, Self::Error> {
            Ok(true)
        }

        async fn place_market_order(
            &self,
            order: MarketOrder,
        ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
            Ok(OrderPlacement {
                order_id: "sequenced-order-id".to_string(),
                symbol: order.symbol,
                shares: order.shares,
                direction: order.direction,
                placed_at: Utc::now(),
                extended_hours: false,
                limit_price: None,
            })
        }

        async fn get_order_status(
            &self,
            _order_id: &Self::OrderId,
        ) -> Result<OrderState, Self::Error> {
            let call_index = self.status_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .statuses
                .get(call_index)
                .or_else(|| self.statuses.last())
                .cloned()
                .unwrap_or(OrderState::Pending))
        }

        fn to_supported_executor(&self) -> SupportedExecutor {
            SupportedExecutor::DryRun
        }

        fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
            Ok(order_id_str.to_string())
        }

        async fn get_inventory(&self) -> Result<InventoryResult, Self::Error> {
            Ok(InventoryResult::Unimplemented)
        }

        async fn place_limit_order(
            &self,
            order: LimitOrder,
        ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
            Ok(OrderPlacement {
                order_id: "sequenced-limit-order-id".to_string(),
                symbol: order.symbol,
                shares: order.shares,
                direction: order.direction,
                placed_at: Utc::now(),
                extended_hours: order.extended_hours,
                limit_price: Some(order.limit_price),
            })
        }

        async fn cancel_order(
            &self,
            _order_id: &Self::OrderId,
        ) -> Result<CancellationOutcome, Self::Error> {
            Ok(CancellationOutcome::Requested)
        }
    }

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
            when.method(httpmock::Method::POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                // CLI limit orders now carry a fresh `cli-{uuid}` client_order_id
                // (added for broker-side idempotency), so match the static fields
                // partially and assert the key is a well-formed CLI idempotency
                // key rather than pinning its random value.
                .body_matches(client_order_id_cli_pattern())
                .json_body_includes(
                    json!({
                        "symbol": "AAPL",
                        "qty": "10",
                        "side": "buy",
                        "type": "limit",
                        "limit_price": "195.25",
                        "time_in_force": "day",
                        "extended_hours": true
                    })
                    .to_string(),
                );
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

    /// Regex asserting a request body carries a well-formed `cli-{uuid}`
    /// `client_order_id`, shared by the market- and limit-order mocks.
    fn client_order_id_cli_pattern() -> Regex {
        Regex::new(
            r#""client_order_id":"cli-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}""#,
        )
        .unwrap()
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

    #[tokio::test(start_paused = true)]
    async fn market_buy_keeps_polling_after_partial_fill_until_filled() {
        let broker = SequencedStatusExecutor::new(vec![
            OrderState::PartiallyFilled {
                order_id: ExecutorOrderId::new("sequenced-order-id"),
                shares_filled: FractionalShares::new(Float::parse("3.5".to_string()).unwrap()),
                avg_price: Some(st0x_execution::Usd::new(
                    Float::parse("195.25".to_string()).unwrap(),
                )),
                partially_filled_at: Utc::now(),
            },
            OrderState::Filled {
                executed_at: Utc::now(),
                order_id: ExecutorOrderId::new("sequenced-order-id"),
                price: st0x_execution::Usd::new(Float::parse("195.30".to_string()).unwrap()),
            },
        ]);
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("10"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        };
        let mut stdout = Vec::new();

        place_market_order_until_filled(&broker, order, &mut stdout)
            .await
            .expect("partial fill should keep polling until the broker reports Filled");

        let output = String::from_utf8(stdout).unwrap();
        assert_eq!(
            broker.status_calls(),
            2,
            "the buy-fill wait must poll again after PartiallyFilled"
        );
        assert!(
            output.contains("Buy filled"),
            "the buy-fill wait must report the final fill, got: {output}"
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

    // ------------------------------------------------------------------ //
    // ADR-0005 exactly-once fill accounting tests for process_found_trade  //
    // ------------------------------------------------------------------ //

    /// `process-tx` on an acknowledged fill must fail closed: output the "already
    /// fully accounted" message, return immediately, and place NO broker order.
    /// Position-level exposure is the normal pipeline's responsibility.
    #[tokio::test]
    async fn process_tx_skips_accounting_on_acknowledged_fill() {
        let pool = setup_test_db().await;

        // Enable trading so check_execution_readiness would trigger if we fell
        // through — confirming that the early return fires before hedge placement.
        let mut ctx = create_base_test_ctx();
        ctx.assets.equities.symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: vec![],
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let order_placer = create_order_placer(&ctx, &pool);

        let onchain_trade = OnchainTradeBuilder::default().build();
        let block_timestamp = onchain_trade.block_timestamp.unwrap();

        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        // Pre-seed 1: witness + acknowledge the OnChainTrade aggregate.
        let onchain_store = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        onchain_store
            .send(
                &trade_id,
                OnChainTradeCommand::Witness {
                    symbol: onchain_trade.symbol.base().clone(),
                    amount: onchain_trade.amount.inner(),
                    direction: onchain_trade.direction,
                    price_usdc: onchain_trade.price.value(),
                    block_number: 1,
                    block_timestamp,
                },
            )
            .await
            .unwrap();

        onchain_store
            .send(&trade_id, OnChainTradeCommand::Acknowledge)
            .await
            .unwrap();

        // Pre-seed 2: apply the fill to the Position aggregate so there is live
        // unhedged exposure that could trigger hedge placement if we fell through.
        let (pre_position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        execute_acknowledge_fill(
            &pre_position_store,
            &onchain_trade,
            ctx.execution_threshold,
            block_timestamp,
        )
        .await
        .unwrap();

        let mut stdout = Vec::new();
        process_found_trade(onchain_trade, &ctx, &pool, &mut stdout, order_placer)
            .await
            .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("already fully accounted"),
            "acknowledged fill must output the already-accounted message, got: {output}"
        );

        // No second OnChainOrderFilled event: the pre-seeded fill must not be
        // re-counted by the re-run.
        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "re-running process-tx on an acknowledged fill must not emit a second fill event"
        );

        // No OffchainOrder placed: fail-closed must not place a spurious hedge
        // driven by the live position exposure.
        let (order_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE event_type LIKE 'OffchainOrderEvent%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            order_count, 0,
            "re-running process-tx on an acknowledged fill must place no broker order"
        );
    }

    /// `process-tx` must resume the acknowledge step when the fill was witnessed
    /// but not yet acknowledged (crash-recovery window). After the call, the
    /// `OnChainTrade` record must be acknowledged and exactly one position fill
    /// event must exist. Trading is disabled in this test context so hedge
    /// placement is not reached.
    #[tokio::test]
    async fn process_tx_resumes_witnessed_but_unacknowledged_fill() {
        let pool = setup_test_db().await;
        let ctx = create_base_test_ctx();
        let order_placer = create_order_placer(&ctx, &pool);

        let onchain_trade = OnchainTradeBuilder::default().with_block_number(1).build();
        let block_timestamp = onchain_trade.block_timestamp.unwrap();

        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        // Pre-seed: witness only (not acknowledged — simulates crash window).
        let store = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        store
            .send(
                &trade_id,
                OnChainTradeCommand::Witness {
                    symbol: onchain_trade.symbol.base().clone(),
                    amount: onchain_trade.amount.inner(),
                    direction: onchain_trade.direction,
                    price_usdc: onchain_trade.price.value(),
                    block_number: 1,
                    block_timestamp,
                },
            )
            .await
            .unwrap();

        // Call process_found_trade: must resume and complete the acknowledge step.
        process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut std::io::sink(),
            order_placer,
        )
        .await
        .unwrap();

        // Verify the trade is now fully acknowledged.
        let state = store
            .load(&trade_id)
            .await
            .unwrap()
            .expect("OnChainTrade record must exist after resume");
        assert!(
            state.is_acknowledged(),
            "Fill must be acknowledged after process_found_trade resumes the crash-recovery path"
        );

        // Exactly one OnChainOrderFilled event must exist — the fill accounting
        // ran once during resume, not zero times (dropped) or twice (double-counted).
        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "Resume must apply exactly one fill to the position aggregate"
        );
    }

    /// `process-tx` on a genuinely new fill must witness it in the
    /// `OnChainTrade` aggregate, acknowledge it in the `Position` aggregate,
    /// and mark it acknowledged — leaving exactly one position fill event.
    #[tokio::test]
    async fn process_tx_witnesses_and_acknowledges_new_fill() {
        let pool = setup_test_db().await;
        let ctx = create_base_test_ctx();
        let order_placer = create_order_placer(&ctx, &pool);

        // block_number is required for the Witness step on a new fill.
        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();

        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut std::io::sink(),
            order_placer,
        )
        .await
        .unwrap();

        // The OnChainTrade aggregate must be acknowledged.
        let store = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let state = store
            .load(&trade_id)
            .await
            .unwrap()
            .expect("OnChainTrade record must exist after processing a new fill");
        assert!(
            state.is_acknowledged(),
            "Fill must be acknowledged in the OnChainTrade aggregate"
        );

        // Exactly one OnChainOrderFilled position event must be in the DB.
        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "Exactly one fill must be applied to the position for a new fill"
        );
    }

    /// After the CLI applies a fill via `process_found_trade`, the normal
    /// pipeline re-detecting the same fill (via `process_queued_trade`) must
    /// return `Ok(None)` — skipping cleanly — and must NOT emit a second
    /// `OnChainOrderFilled` event. This is the primary double-count guard test.
    #[tokio::test]
    async fn process_tx_then_normal_path_does_not_double_count() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let ctx = create_base_test_ctx();
        let order_placer = create_order_placer(&ctx, &pool);

        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();

        // Step 1: CLI applies the fill.
        process_found_trade(
            onchain_trade.clone(),
            &ctx,
            &pool,
            &mut std::io::sink(),
            order_placer.clone(),
        )
        .await
        .unwrap();

        // Step 2: Construct TradeProcessingCqrs backed by the same pool so the
        // acknowledged OnChainTrade record written by the CLI is visible.
        let onchain_trade_store = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position_store, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();

        let cqrs = TradeProcessingCqrs {
            pool: pool.clone(),
            onchain_trade: onchain_trade_store,
            position: position_store,
            position_projection,
            offchain_order: offchain_order_store,
            order_placer,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            counter_trade_submission_lock: Arc::new(Mutex::new(())),
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            hedge_queue: crate::trading::offchain::hedge::HedgeJobQueue::new(&apalis_pool),
        };

        // The trade_event payload is never accessed because process_queued_trade
        // returns Ok(None) immediately at the is_acknowledged() guard, before
        // reaching the witness step that would use block_number.
        let trade_event = EmittedOnChain {
            event: RaindexTradeEvent::ClearV3(Box::new(ClearV3 {
                sender: alloy::primitives::Address::ZERO,
                alice: get_test_order(),
                bob: get_test_order(),
                clearConfig: ClearConfigV2 {
                    aliceInputIOIndex: U256::ZERO,
                    aliceOutputIOIndex: U256::ZERO,
                    bobInputIOIndex: U256::ZERO,
                    bobOutputIOIndex: U256::ZERO,
                    aliceBountyVaultId: B256::ZERO,
                    bobBountyVaultId: B256::ZERO,
                },
            })),
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
            block_number: 42,
            block_timestamp: onchain_trade.block_timestamp,
        };

        // Step 3: Normal pipeline re-detects the same fill.
        let result = process_queued_trade(
            &st0x_execution::MockExecutor::new(),
            &trade_event,
            onchain_trade,
            &cqrs,
            true,
        )
        .await
        .unwrap();

        assert_eq!(
            result, None,
            "Normal pipeline must skip an already-acknowledged fill"
        );

        // Exactly one OnChainOrderFilled position event: CLI + pipeline together
        // must not double-count.
        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "CLI then normal pipeline must produce exactly one position fill event, not two"
        );
    }

    /// A new fill with no `block_number` must return an error containing
    /// "missing block_number" so the operator sees a loud failure rather than
    /// a silent skip.
    #[tokio::test]
    async fn process_tx_fails_on_missing_block_number() {
        let pool = setup_test_db().await;
        let ctx = create_base_test_ctx();
        let order_placer = create_order_placer(&ctx, &pool);

        let onchain_trade = OnchainTradeBuilder::default()
            .with_block_number(None)
            .build();

        let error = process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut std::io::sink(),
            order_placer,
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("missing block_number"),
            "missing block_number must produce an explicit error, got: {error}"
        );
    }

    /// A new fill with no `block_timestamp` must return an error so the operator
    /// sees a loud failure rather than a silent skip.
    #[tokio::test]
    async fn process_tx_fails_on_missing_block_timestamp() {
        let pool = setup_test_db().await;
        let ctx = create_base_test_ctx();
        let order_placer = create_order_placer(&ctx, &pool);

        // block_number is set so the new-fill branch is reached; block_timestamp
        // is None so the bail fires before the witness step.
        let onchain_trade = OnchainTradeBuilder::default()
            .with_block_number(42)
            .with_block_timestamp(None)
            .build();
        let expected_trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        let error = process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut std::io::sink(),
            order_placer,
        )
        .await
        .unwrap_err();

        let trade_accounting_error = error
            .downcast_ref::<TradeAccountingError>()
            .expect("missing block_timestamp should bubble up as TradeAccountingError");
        assert!(
            matches!(
                trade_accounting_error,
                TradeAccountingError::MissingBlockTimestamp { trade_id }
                    if trade_id == &expected_trade_id
            ),
            "missing block_timestamp must produce \
             TradeAccountingError::MissingBlockTimestamp for {expected_trade_id}, \
             got: {trade_accounting_error}"
        );
    }

    /// When broker placement fails (resulting in `OffchainOrder::Failed`),
    /// `process_found_trade` must send `PositionCommand::FailOffChainOrder` to
    /// clear `pending_offchain_order_id`, leaving the position unpending so
    /// the normal pipeline can re-hedge on its next cycle.
    #[tokio::test]
    async fn process_tx_clears_pending_order_on_failed_placement() {
        let pool = setup_test_db().await;

        // Enable trading so check_execution_readiness can trigger hedge placement.
        let mut ctx = create_base_test_ctx();
        ctx.assets.equities.symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: vec![],
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let order_placer: Arc<dyn OrderPlacer> = Arc::new(FailingOrderPlacer);
        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();

        let mut stdout = Vec::new();
        process_found_trade(onchain_trade, &ctx, &pool, &mut stdout, order_placer)
            .await
            .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Hedge placement failed"),
            "failed placement must report the outcome to the operator, got: {output}"
        );
        assert!(
            output.contains("The normal pipeline will re-hedge on the next cycle."),
            "post-place cleanup should defer re-hedging to the normal pipeline, got: {output}"
        );

        // pending_offchain_order_id must be cleared: the position must not be
        // permanently stuck after a failed broker placement.
        let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let position = position_store
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("Position must exist after fill accounting");

        assert!(
            position.pending_offchain_order_id.is_none(),
            "pending_offchain_order_id must be cleared after a failed broker placement"
        );
    }

    #[tokio::test]
    async fn existing_pending_cleanup_reports_process_tx_continues_this_run() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let block_timestamp = Utc::now();

        let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let onchain_trade = OnchainTradeBuilder::default()
            .with_block_number(42)
            .with_block_timestamp(Some(block_timestamp))
            .build();

        execute_acknowledge_fill(
            &position_store,
            &onchain_trade,
            ExecutionThreshold::whole_share(),
            block_timestamp,
        )
        .await
        .unwrap();

        position_store
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: positive_shares("1"),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        let failed_order = OffchainOrder::Failed {
            symbol: symbol.clone(),
            shares: positive_shares("1"),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            retained_fill: None,
            executor_order_id: None,
            error: "previous placement failed".to_string(),
            placed_at: block_timestamp,
            failed_at: block_timestamp,
        };

        let mut stdout = Vec::new();
        reconcile_loaded_post_place_state(
            Some(failed_order),
            &position_store,
            &symbol,
            offchain_order_id,
            CliPendingClearNextStep::ContinueThisRun,
            &mut stdout,
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("process-tx will re-evaluate the position in this run."),
            "existing pending cleanup should report that process-tx continues, got: {output}"
        );
        assert!(
            !output.contains("normal pipeline"),
            "existing pending cleanup should not imply re-hedging waits for the normal pipeline, \
             got: {output}"
        );
    }

    #[tokio::test]
    async fn process_tx_clears_pending_order_when_post_place_order_missing() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();
        let block_timestamp = onchain_trade
            .block_timestamp
            .expect("test trade should have a block timestamp");

        let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();

        execute_acknowledge_fill(
            &position_store,
            &onchain_trade,
            ExecutionThreshold::whole_share(),
            block_timestamp,
        )
        .await
        .unwrap();

        position_store
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: positive_shares("1"),
                    direction: Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        reconcile_post_place_state(
            &offchain_order_store,
            &position_store,
            &symbol,
            offchain_order_id,
            &mut stdout,
        )
        .await
        .unwrap();

        let position = position_store
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist after setup");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "missing OffchainOrder state must clear the position's pending id"
        );

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("pending order cleared"),
            "missing order cleanup must report the cleared pending marker, got: {output}"
        );
    }

    #[tokio::test]
    async fn existing_cancelling_pending_order_remains_in_flight() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();
        let block_timestamp = onchain_trade
            .block_timestamp
            .expect("test trade should have a block timestamp");

        let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        execute_acknowledge_fill(
            &position_store,
            &onchain_trade,
            ExecutionThreshold::whole_share(),
            block_timestamp,
        )
        .await
        .unwrap();

        position_store
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: positive_shares("1"),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        let cancelling_order = OffchainOrder::Cancelling {
            symbol: symbol.clone(),
            shares: positive_shares("1"),
            retained_fill: None,
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("broker-order-id"),
            reason: CancellationReason::MarketOpenReplacement,
            placed_at: block_timestamp,
            submitted_at: block_timestamp,
            cancel_requested_at: block_timestamp,
            market_session: st0x_execution::MarketSession::Regular,
        };

        let mut stdout = Vec::new();
        let outcome = reconcile_loaded_post_place_state(
            Some(cancelling_order),
            &position_store,
            &symbol,
            offchain_order_id,
            CliPendingClearNextStep::ContinueThisRun,
            &mut stdout,
        )
        .await
        .unwrap();

        assert!(
            matches!(outcome, CliPendingReconciliation::InFlight),
            "cancelling orders must remain in flight until broker cancellation confirms"
        );

        let position = position_store
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist after setup");
        assert_eq!(
            position.pending_offchain_order_id,
            Some(offchain_order_id),
            "Cancelling must leave the position claim in place"
        );

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("cancellation is still in progress"),
            "cancelling order reconciliation must explain why no new hedge is placed, \
             got: {output}"
        );
    }

    #[tokio::test]
    async fn existing_cancelled_pending_order_clears_position_claim() {
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();
        let block_timestamp = onchain_trade
            .block_timestamp
            .expect("test trade should have a block timestamp");

        let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        execute_acknowledge_fill(
            &position_store,
            &onchain_trade,
            ExecutionThreshold::whole_share(),
            block_timestamp,
        )
        .await
        .unwrap();

        position_store
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: positive_shares("1"),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        let cancelled_order = OffchainOrder::Cancelled {
            symbol: symbol.clone(),
            shares: positive_shares("1"),
            retained_fill: None,
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("broker-order-id"),
            reason: CancellationReason::MarketOpenReplacement,
            placed_at: block_timestamp,
            cancelled_at: block_timestamp,
        };

        let mut stdout = Vec::new();
        let outcome = reconcile_loaded_post_place_state(
            Some(cancelled_order),
            &position_store,
            &symbol,
            offchain_order_id,
            CliPendingClearNextStep::ContinueThisRun,
            &mut stdout,
        )
        .await
        .unwrap();

        assert!(
            matches!(outcome, CliPendingReconciliation::Cleared),
            "cancelled orders with no retained fill must clear the pending claim"
        );

        let position = position_store
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist after setup");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Cancelled must clear the position claim through CancelOffChainOrder"
        );

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Existing pending hedge finalized"),
            "cancelled order reconciliation must report the finalized pending order, \
             got: {output}"
        );
    }

    /// When a second client shares the same pool and witnesses the fill first,
    /// `process_found_trade` must resume the acknowledge step and complete it —
    /// not silently drop the fill. The fill must be accounted exactly once.
    ///
    /// This covers the concurrent-witnessed sub-path: the outer load sees
    /// `Some(Witnessed)` from the peer writer's record and resumes.
    #[tokio::test]
    async fn process_tx_concurrent_witness_resumes_acknowledge() {
        let pool = setup_test_db().await;
        let ctx = create_base_test_ctx();
        let order_placer = create_order_placer(&ctx, &pool);

        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();
        let block_timestamp = onchain_trade.block_timestamp.unwrap();

        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        // Simulate a concurrent writer (e.g. the normal pipeline) that witnesses
        // the fill via its own store instance backed by the same pool. When
        // process_found_trade's internal store loads the aggregate, it will see
        // Some(Witnessed) and resume rather than re-witness.
        let store_a = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        store_a
            .send(
                &trade_id,
                OnChainTradeCommand::Witness {
                    symbol: onchain_trade.symbol.base().clone(),
                    amount: onchain_trade.amount.inner(),
                    direction: onchain_trade.direction,
                    price_usdc: onchain_trade.price.value(),
                    block_number: 42,
                    block_timestamp,
                },
            )
            .await
            .unwrap();

        // Call process_found_trade: must resume the acknowledge step and complete
        // it rather than silently dropping the fill.
        process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut std::io::sink(),
            order_placer,
        )
        .await
        .unwrap();

        // The trade must be fully acknowledged after the resume.
        let state = store_a
            .load(&trade_id)
            .await
            .unwrap()
            .expect("OnChainTrade record must exist after concurrent resume");
        assert!(
            state.is_acknowledged(),
            "Fill must be acknowledged after process_found_trade resumes from concurrent witness"
        );

        // Exactly one fill event: the concurrent write-then-resume must not
        // double-count or drop the fill.
        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "Concurrent witness + resume must apply exactly one fill to the position"
        );
    }

    /// When a concurrent writer has already fully acknowledged the fill,
    /// `process_found_trade` must fail closed: return early without placing a
    /// hedge or emitting a second fill event.
    ///
    /// This covers the concurrent-acknowledged sub-path: the outer load sees
    /// `Some(Acknowledged)` from the peer writer's record and exits immediately.
    #[tokio::test]
    async fn process_tx_concurrent_acknowledged_fails_closed() {
        let pool = setup_test_db().await;

        // Enable trading so that if we fell through to hedge placement we would
        // know — confirming the early return fires before any of that.
        let mut ctx = create_base_test_ctx();
        ctx.assets.equities.symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: vec![],
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let order_placer = create_order_placer(&ctx, &pool);

        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();
        let block_timestamp = onchain_trade.block_timestamp.unwrap();

        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        // Simulate a concurrent writer that has fully processed the fill.
        let store_a = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        store_a
            .send(
                &trade_id,
                OnChainTradeCommand::Witness {
                    symbol: onchain_trade.symbol.base().clone(),
                    amount: onchain_trade.amount.inner(),
                    direction: onchain_trade.direction,
                    price_usdc: onchain_trade.price.value(),
                    block_number: 42,
                    block_timestamp,
                },
            )
            .await
            .unwrap();

        store_a
            .send(&trade_id, OnChainTradeCommand::Acknowledge)
            .await
            .unwrap();

        // Apply the fill to the position so there is live unhedged exposure that
        // could trigger hedge placement if we fell through.
        let (pre_position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        execute_acknowledge_fill(
            &pre_position_store,
            &onchain_trade,
            ctx.execution_threshold,
            block_timestamp,
        )
        .await
        .unwrap();

        let mut stdout = Vec::new();
        process_found_trade(onchain_trade, &ctx, &pool, &mut stdout, order_placer)
            .await
            .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("already fully accounted"),
            "concurrent-acknowledged fill must output the already-accounted message, got: {output}"
        );

        // No second fill event and no spurious hedge order from the concurrent path.
        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind("PositionEvent::OnChainOrderFilled")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "Concurrent acknowledged + second process-tx must not emit a second fill event"
        );

        let (order_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE event_type LIKE 'OffchainOrderEvent%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            order_count, 0,
            "Fail-closed on concurrent-acknowledged fill must place no broker order"
        );
    }

    /// Regression test for the crash-window double-count bug:
    ///
    /// 1. Fill A is witnessed and acknowledged in Position (slot = A), but the
    ///    process crashes BEFORE `mark_acknowledged` runs — OnChainTrade A stays
    ///    Witnessed.
    /// 2. Fill B arrives and is fully processed (slot advances to B).
    /// 3. `process-tx` is retried for A.
    ///
    /// Without the durable `position_fill_already_recorded` guard, the resume
    /// path would call `execute_acknowledge_fill(A)` again. Because the slot now
    /// holds B (not A), `PositionError::DuplicateTrade` does NOT fire and A is
    /// counted a second time — corrupting the net position.
    ///
    /// After the fix, the retry must:
    /// - Apply fill A exactly once (total fill events = 2: one A + one B).
    /// - Mark OnChainTrade A acknowledged.
    #[tokio::test]
    async fn process_tx_does_not_double_count_witnessed_fill_after_newer_fill_acknowledged() {
        let pool = setup_test_db().await;
        let ctx = create_base_test_ctx();
        let order_placer = create_order_placer(&ctx, &pool);

        // Fill A and fill B: same tx_hash, different log_index so they have
        // distinct (tx_hash, log_index) identities.
        let fill_a = OnchainTradeBuilder::default().with_block_number(10).build();
        let fill_b = OnchainTradeBuilder::default()
            .with_log_index(2)
            .with_block_number(11)
            .build();

        let block_timestamp_a = fill_a.block_timestamp.unwrap();
        let block_timestamp_b = fill_b.block_timestamp.unwrap();

        let trade_id_a = OnChainTradeId {
            tx_hash: fill_a.tx_hash,
            log_index: fill_a.log_index,
        };
        let trade_id_b = OnChainTradeId {
            tx_hash: fill_b.tx_hash,
            log_index: fill_b.log_index,
        };

        let onchain_store = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        // Step 1: Witness fill A in OnChainTrade.
        onchain_store
            .send(
                &trade_id_a,
                OnChainTradeCommand::Witness {
                    symbol: fill_a.symbol.base().clone(),
                    amount: fill_a.amount.inner(),
                    direction: fill_a.direction,
                    price_usdc: fill_a.price.value(),
                    block_number: 10,
                    block_timestamp: block_timestamp_a,
                },
            )
            .await
            .unwrap();

        // Step 2: Acknowledge fill A in Position (slot = A).
        execute_acknowledge_fill(
            &position_store,
            &fill_a,
            ctx.execution_threshold,
            block_timestamp_a,
        )
        .await
        .unwrap();

        // Simulate crash: do NOT call execute_mark_acknowledged for fill A.
        // OnChainTrade A stays Witnessed; Position already has A applied.

        // Step 3: Fully process fill B (witness + acknowledge + mark).
        onchain_store
            .send(
                &trade_id_b,
                OnChainTradeCommand::Witness {
                    symbol: fill_b.symbol.base().clone(),
                    amount: fill_b.amount.inner(),
                    direction: fill_b.direction,
                    price_usdc: fill_b.price.value(),
                    block_number: 11,
                    block_timestamp: block_timestamp_b,
                },
            )
            .await
            .unwrap();

        execute_acknowledge_fill(
            &position_store,
            &fill_b,
            ctx.execution_threshold,
            block_timestamp_b,
        )
        .await
        .unwrap();

        execute_mark_acknowledged(&onchain_store, &trade_id_b)
            .await
            .unwrap();

        // At this point:
        // - Position has fill_a and fill_b applied (last_slot = fill_b's trade_id).
        // - OnChainTrade fill_a is Witnessed (not Acknowledged).
        // - OnChainTrade fill_b is Acknowledged.
        // Without the durable guard, retrying process-tx for fill_a would
        // re-apply it: last_slot (B) != A, so DuplicateTrade does NOT fire.

        // Step 4: Retry process-tx for fill A (crash-recovery scenario).
        process_found_trade(fill_a, &ctx, &pool, &mut std::io::sink(), order_placer)
            .await
            .unwrap();

        // Assertion 1: OnChainTrade A must now be acknowledged.
        let state_a = onchain_store
            .load(&trade_id_a)
            .await
            .unwrap()
            .expect("OnChainTrade fill_a must exist after process_tx retry");
        assert!(
            state_a.is_acknowledged(),
            "fill_a must be acknowledged after process_tx retry"
        );

        // Assertion 2: Exactly two OnChainOrderFilled events — fill_a once and
        // fill_b once. Any value other than 2 means double-counting or a dropped
        // fill.
        let (fill_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE event_type = 'PositionEvent::OnChainOrderFilled'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            fill_count, 2,
            "process_tx retry on a crash-window fill must not double-count: \
             expected exactly 2 fill events (fill_a once + fill_b once), got {fill_count}"
        );
    }

    /// Regression test for the None-path (fresh-witness) double-count hole:
    ///
    /// A legacy fill whose Position record was written (e.g. via a prior direct
    /// `execute_acknowledge_fill` call) but whose OnChainTrade record was NEVER
    /// created causes `process_found_trade` to take the `None` branch. Without
    /// the unified durable guard the fresh-witness arm would call
    /// `execute_acknowledge_fill` again; because the Position slot already
    /// advanced to a newer fill (B), `DuplicateTrade` does NOT fire and fill A
    /// is counted a second time.
    ///
    /// After the fix the unified `position_fill_already_recorded` guard runs on
    /// every path — including the `None` path — and blocks the re-apply.
    #[tokio::test]
    async fn process_tx_none_path_does_not_recount_legacy_position_fill() {
        let pool = setup_test_db().await;
        let ctx = create_base_test_ctx();
        let order_placer = create_order_placer(&ctx, &pool);

        // Fill A and fill B have distinct (tx_hash, log_index) identities.
        let fill_a = OnchainTradeBuilder::default().with_block_number(10).build();
        let fill_b = OnchainTradeBuilder::default()
            .with_log_index(2)
            .with_block_number(11)
            .build();

        let block_timestamp_a = fill_a.block_timestamp.unwrap();
        let block_timestamp_b = fill_b.block_timestamp.unwrap();

        let trade_id_a = OnChainTradeId {
            tx_hash: fill_a.tx_hash,
            log_index: fill_a.log_index,
        };
        let trade_id_b = OnChainTradeId {
            tx_hash: fill_b.tx_hash,
            log_index: fill_b.log_index,
        };

        let onchain_store = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        // Step 1: Apply fill A to Position ONLY — no OnChainTrade witness record.
        // This simulates a legacy fill whose OnChainTrade record was never created.
        // process_found_trade will load None for trade_id_a and take the None path.
        execute_acknowledge_fill(
            &position_store,
            &fill_a,
            ctx.execution_threshold,
            block_timestamp_a,
        )
        .await
        .unwrap();

        // Step 2: Fully process fill B so the Position slot advances beyond A.
        // Now last_acknowledged_trade_id = B, so a re-apply of A bypasses the
        // single-slot DuplicateTrade guard without the durable check.
        onchain_store
            .send(
                &trade_id_b,
                OnChainTradeCommand::Witness {
                    symbol: fill_b.symbol.base().clone(),
                    amount: fill_b.amount.inner(),
                    direction: fill_b.direction,
                    price_usdc: fill_b.price.value(),
                    block_number: 11,
                    block_timestamp: block_timestamp_b,
                },
            )
            .await
            .unwrap();

        execute_acknowledge_fill(
            &position_store,
            &fill_b,
            ctx.execution_threshold,
            block_timestamp_b,
        )
        .await
        .unwrap();

        execute_mark_acknowledged(&onchain_store, &trade_id_b)
            .await
            .unwrap();

        // Step 3: Run process_found_trade for fill A. It takes the None branch
        // (no OnChainTrade record), witnesses A, then the authoritative guard must
        // detect A already in Position and skip the re-apply.
        process_found_trade(fill_a, &ctx, &pool, &mut std::io::sink(), order_placer)
            .await
            .unwrap();

        // Assertion 1: OnChainTrade A must be acknowledged (witness + mark ran).
        let state_a = onchain_store
            .load(&trade_id_a)
            .await
            .unwrap()
            .expect("OnChainTrade fill_a must exist after process_found_trade");
        assert!(
            state_a.is_acknowledged(),
            "fill_a must be acknowledged after process_found_trade takes the None path"
        );

        // Assertion 2: Exactly two fill events — fill_a once + fill_b once. Any
        // value other than 2 means fill_a was double-counted on the None path.
        let (fill_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events \
             WHERE event_type = 'PositionEvent::OnChainOrderFilled'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            fill_count, 2,
            "None-path process_tx must not re-apply a fill already in Position: \
             expected 2 fill events (fill_a once + fill_b once), got {fill_count}"
        );
    }

    /// The happy path: a new fill with trading enabled and a broker that accepts
    /// the order must print 'Trade processing completed!' and leave the position
    /// with `pending_offchain_order_id` set (order submitted, not cleared).
    ///
    /// This is the only test that exercises the
    /// `Some(OffchainOrder::Submitted | PartiallyFilled)` arm in
    /// `process_found_trade`.
    #[tokio::test]
    async fn process_tx_submitted_hedge_sets_pending_order_id() {
        let pool = setup_test_db().await;

        // Enable trading so check_execution_readiness can trigger hedge placement.
        let mut ctx = create_base_test_ctx();
        ctx.assets.equities.symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: vec![],
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);

        // 1 share buy -> net +1 -> is_ready_for_execution returns (Sell, 1).
        let onchain_trade = OnchainTradeBuilder::default().with_block_number(42).build();

        let mut stdout = Vec::new();
        process_found_trade(onchain_trade, &ctx, &pool, &mut stdout, order_placer)
            .await
            .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Trade processing completed!"),
            "successful broker submission must print the completion message, got: {output}"
        );

        // pending_offchain_order_id must be set: the order is submitted to the
        // broker and in flight; only the order-status sweep will clear it.
        let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let position = position_store
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap()
            .expect("Position must exist after fill accounting");

        let pending_order_id = position
            .pending_offchain_order_id
            .expect("pending_offchain_order_id must be set after successful broker submission");

        let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();
        let offchain_order = offchain_order_store
            .load(&pending_order_id)
            .await
            .unwrap()
            .expect("pending_offchain_order_id must refer to a persisted offchain order");
        assert!(
            matches!(offchain_order, OffchainOrder::Submitted { .. }),
            "pending_offchain_order_id must point to the submitted broker order, got: {offchain_order:?}"
        );

        let (offchain_event_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_id = ? AND event_type LIKE 'OffchainOrderEvent%'",
        )
        .bind(pending_order_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            offchain_event_count >= 1,
            "submitted hedge should persist at least one OffchainOrder event for {pending_order_id}"
        );

        let (fill_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                .bind(crate::position::PositionEvent::ON_CHAIN_ORDER_FILLED_EVENT_TYPE)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            fill_count, 1,
            "successful broker submission must account the onchain fill exactly once"
        );
    }

    #[tokio::test]
    async fn market_buy_fails_when_the_broker_cancels_the_order() {
        let broker = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at: Utc::now(),
            order_id: ExecutorOrderId::new("some-broker-order-id"),
            shares_filled: FractionalShares::ZERO,
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
            error.to_string().contains("buy order was cancelled"),
            "a cancelled order must fail the buy-fill wait, got: {error}"
        );
    }

    #[tokio::test]
    async fn market_buy_cancelled_after_partial_fill_reports_details() {
        let broker = MockExecutor::new().with_order_status(OrderState::Cancelled {
            cancelled_at: Utc::now(),
            order_id: ExecutorOrderId::new("some-broker-order-id"),
            shares_filled: FractionalShares::new(Float::parse("1.5".to_string()).unwrap()),
            avg_price: Some(st0x_execution::Usd::new(
                Float::parse("195.25".to_string()).unwrap(),
            )),
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
        let error = error.to_string();

        assert!(
            error.contains("cancelled after partial fill"),
            "a cancelled partial fill must be explicit, got: {error}"
        );
        assert!(
            error.contains("average price"),
            "a cancelled partial fill with price must report it, got: {error}"
        );
    }

    #[test]
    fn write_order_status_displays_partially_filled_state() {
        let mut stdout = Vec::new();

        write_order_status(
            &mut stdout,
            OrderState::PartiallyFilled {
                order_id: ExecutorOrderId::new("some-broker-order-id"),
                shares_filled: FractionalShares::new(Float::parse("1.5".to_string()).unwrap()),
                avg_price: Some(st0x_execution::Usd::new(
                    Float::parse("195.25".to_string()).unwrap(),
                )),
                partially_filled_at: Utc::now(),
            },
        )
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("PARTIALLY FILLED"),
            "status output must include the partial-fill arm, got: {output}"
        );
        assert!(
            output.contains("some-broker-order-id"),
            "status output must include the broker order id, got: {output}"
        );
    }

    #[test]
    fn write_order_status_displays_cancelled_state() {
        let mut stdout = Vec::new();

        write_order_status(
            &mut stdout,
            OrderState::Cancelled {
                cancelled_at: Utc::now(),
                order_id: ExecutorOrderId::new("some-broker-order-id"),
                shares_filled: FractionalShares::ZERO,
                avg_price: None,
            },
        )
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("CANCELLED"),
            "status output must include the cancelled arm, got: {output}"
        );
        assert!(
            output.contains("some-broker-order-id"),
            "status output must include the broker order id, got: {output}"
        );
    }
}
