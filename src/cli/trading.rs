//! Trading order execution and transaction processing CLI commands.

use alloy::primitives::TxHash;
use alloy::providers::Provider;
use anyhow::Context;
use async_trait::async_trait;
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

use crate::conductor::{
    execute_acknowledge_fill, execute_enrich_trade, execute_mark_acknowledged, execute_settle_fill,
    execute_witness_trade,
};
use crate::offchain::order::{
    OffchainOrder, OffchainOrderId, OrderPlacementResult, OrderPlacer,
    client_order_id_for_placement, place_offchain_order_at_broker,
};
use crate::onchain::accumulator::check_execution_readiness;
use crate::onchain::pyth::PythFeedIds;
use crate::onchain::{OnChainError, OnchainTrade, TradeValidationError};
use crate::onchain_trade::{OnChainTrade, OnChainTradeId};
use crate::position::{Position, PositionCommand};
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
                format_float_with_fallback(&price)
            )?;
        }
        OrderState::Failed {
            failed_at,
            error_reason,
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
        .place_limit_order(AlpacaLimitOrder {
            symbol: request.symbol.clone(),
            shares: request.shares,
            direction: request.direction,
            limit_price,
            extended_hours,
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
            OrderState::Pending | OrderState::Submitted { .. } => {
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
    let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
        .build(())
        .await?;
    let onchain_trade_store = StoreBuilder::<OnChainTrade>::new(pool.clone())
        .build(())
        .await?;

    // Refuse a fill the bot has already recorded (witnessed or acknowledged),
    // then account this unrecorded fill through the same exactly-once
    // witness -> acknowledge -> mark -> settle sequence the automated pipeline
    // uses, so it is recorded in the OnChainTrade log and a re-run is refused
    // rather than double-counted. process-tx runs in a separate process from the
    // bot, so an in-process lock cannot serialize them; the persisted
    // pending-acknowledgement set (ADR 0010) makes the position itself reject an
    // out-of-order cross-process re-drive, and the recovery-CLI no-concurrent-bot
    // contract remains the operational backstop.
    refuse_if_fill_already_recorded(
        &onchain_trade_store,
        &position_store,
        &onchain_trade,
        stdout,
    )
    .await?;
    account_fill_exactly_once(
        &onchain_trade_store,
        &position_store,
        &onchain_trade,
        ctx.execution_threshold,
    )
    .await?;

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
        error!(
            %offchain_order_id,
            symbol = %params.symbol,
            "Failed to execute Position::PlaceOffChainOrder: {error}"
        );
    }

    // Reuse a prior failed attempt's stashed OffchainOrderId as the broker-side
    // idempotency anchor (mirroring the automated `execute_create_offchain_order`
    // path) so a CLI retry after a transient broker failure dedupes instead of
    // placing a second order. Fail fast on a load error rather than placing under
    // a fresh key: a transient load failure while an anchor exists would submit a
    // SECOND live broker order for shares the prior attempt already placed -- a
    // double-hedge the fail-fast rule exists to prevent. Re-running the CLI
    // reloads the anchor.
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
    let client_order_id = client_order_id_for_placement(offchain_order_id, anchor);

    match place_offchain_order_at_broker(
        &offchain_order_store,
        order_placer.as_ref(),
        &offchain_order_id,
        params.symbol.clone(),
        params.shares,
        params.direction,
        params.executor,
        client_order_id,
    )
    .await
    {
        // A broker rejection comes back as `Ok(Some(Failed))`, not `Err`. Mirror
        // the automated path (`check_and_execute_accumulated_positions`): clear
        // the position's pending claim so the symbol is not stranded permanently
        // claimed -- which would block all future hedging for it -- and surface
        // the rejection to the operator.
        Ok(Some(OffchainOrder::Failed { error, .. })) => {
            error!(
                %offchain_order_id,
                symbol = %params.symbol,
                %error,
                "Broker rejected the order; clearing the position claim"
            );
            if let Err(send_error) = position_store
                .send(
                    &params.symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error,
                    },
                )
                .await
            {
                error!(
                    %offchain_order_id,
                    symbol = %params.symbol,
                    %send_error,
                    "Failed to clear the position claim after broker rejection"
                );
            }
        }
        Ok(_) => {}
        Err(error) => {
            error!(%offchain_order_id, "Failed to place OffchainOrder: {error}");
        }
    }

    writeln!(stdout, "Trade processing completed!")?;

    Ok(())
}

/// Fails closed when the fill at `(tx_hash, log_index)` is already recorded in
/// the `OnChainTrade` log -- in ANY state. A recorded fill has already been
/// accounted (acknowledged), or is mid-accounting (witnessed but not yet
/// acknowledged) by whichever path recorded it -- the automated pipeline OR a
/// prior `process-tx` run. The `Position` now rejects an out-of-order re-drive
/// via its persisted pending-acknowledgement set (ADR 0010), but `process-tx`
/// still refuses a recorded fill outright rather than racing the bot for it:
/// it only accounts fills with no `OnChainTrade` record.
async fn refuse_if_fill_already_recorded<W: Write>(
    onchain_trade_store: &Store<OnChainTrade>,
    position_store: &Store<Position>,
    onchain_trade: &OnchainTrade,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let trade_id = OnChainTradeId {
        tx_hash: onchain_trade.tx_hash,
        log_index: onchain_trade.log_index,
    };

    let Some(recorded) = onchain_trade_store.load(&trade_id).await? else {
        return Ok(());
    };

    if recorded.is_acknowledged() {
        // Self-heal a marker-without-settle leak (ADR 0010): if a prior run
        // marked this fill but crashed before pruning it from the pending set,
        // the entry lingers. The fill is fully accounted, so prune before
        // refusing. A no-op when already pruned.
        execute_settle_fill(position_store, onchain_trade)
            .await
            .context("failed to prune the acknowledged fill from the pending set")?;
        writeln!(
            stdout,
            "❌ Fill {trade_id} is already accounted; refusing to re-apply it"
        )?;
        anyhow::bail!(
            "fill {trade_id} is already recorded and acknowledged in the onchain trade log; \
             re-applying it would double-count the position. The bot already accounted this \
             fill -- no manual processing is needed."
        );
    }

    writeln!(
        stdout,
        "❌ Fill {trade_id} is already witnessed but not acknowledged; refusing to re-apply it"
    )?;
    anyhow::bail!(
        "fill {trade_id} is recorded in the onchain trade log but not yet acknowledged: a prior \
         process-tx run or an in-flight automated job witnessed it. Re-applying it here could \
         double-count the position. Verify whether the bot is currently accounting this symbol \
         (if so, it will finish); otherwise reconcile the position manually -- the record cannot \
         tell which process wrote it. process-tx only accounts fills with no onchain trade record."
    );
}

/// Accounts an unrecorded fill through the automated pipeline's exactly-once
/// sequence (ADR 0005 + ADR 0010): witness the fill into the `OnChainTrade`
/// log, enrich it, drive `Position::AcknowledgeOnChainFill` (which records the
/// fill in the position's pending-acknowledgement set in the same atomic
/// batch), mark the trade acknowledged, then settle -- pruning the pending
/// entry now that the marker is durable. The witness comes FIRST so a crash in
/// the witness->acknowledge window leaves a recorded-but-unacknowledged fill
/// that `refuse_if_fill_already_recorded` blocks on a CLI re-run. A fill that
/// cannot be anchored to a block cannot be witnessed, so this fails closed
/// rather than accounting it unrecorded.
///
/// A crash between the acknowledge and the mark leaves the fill durably in the
/// position's pending-acknowledgement set, so a later re-drive -- even by the
/// bot, even after a newer fill, even cross-process -- is rejected as a
/// duplicate rather than double-counted (ADR 0010). A crash between the mark
/// and the settle leaks one pending entry, pruned by the next re-delivery (the
/// bot's resume skip branch) or the next CLI re-run (the refuse branch); the
/// leak is harmless to correctness and self-heals.
async fn account_fill_exactly_once(
    onchain_trade_store: &Store<OnChainTrade>,
    position_store: &Store<Position>,
    onchain_trade: &OnchainTrade,
    execution_threshold: ExecutionThreshold,
) -> anyhow::Result<()> {
    let trade_id = OnChainTradeId {
        tx_hash: onchain_trade.tx_hash,
        log_index: onchain_trade.log_index,
    };

    let Some(block_number) = onchain_trade.block_number else {
        anyhow::bail!(
            "cannot account fill {trade_id}: the transaction carries no block number, so the \
             fill cannot be witnessed into the onchain trade log"
        );
    };
    let Some(block_timestamp) = onchain_trade.block_timestamp else {
        anyhow::bail!(
            "cannot account fill {trade_id}: the transaction carries no block timestamp, so the \
             fill cannot be witnessed into the onchain trade log"
        );
    };

    // process-tx reconstructs the fill from the transaction and does not carry
    // the originating block hash (OnchainTrade stores only block number and
    // timestamp), so a manually recovered fill is witnessed without a reorg
    // hash. Such a fill is past reorg risk by the time an operator accounts it.
    execute_witness_trade(
        onchain_trade_store,
        onchain_trade,
        block_number,
        None,
        block_timestamp,
    )
    .await
    .context("failed to witness the fill in the onchain trade log")?;
    // Enrich best-effort with the gas/Pyth data already on the trade, matching
    // the automated path: the fill is marked acknowledged below so the bot never
    // re-processes it, making this the only chance to record its enrichment.
    execute_enrich_trade(onchain_trade_store, onchain_trade).await;
    execute_acknowledge_fill(
        position_store,
        onchain_trade,
        execution_threshold,
        block_timestamp,
    )
    .await
    .context("failed to acknowledge the fill in the position aggregate")?;
    execute_mark_acknowledged(onchain_trade_store, &trade_id)
        .await
        .context("failed to mark the fill acknowledged in the onchain trade log")?;
    execute_settle_fill(position_store, onchain_trade)
        .await
        .context("failed to settle the fill acknowledgement in the position aggregate")?;

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
    use alloy::primitives::{Address, address};
    use chrono::Utc;
    use httpmock::MockServer;
    use rain_math_float::Float;
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
    use crate::onchain_trade::OnChainTradeCommand;
    use crate::position::TradeId;
    use crate::test_utils::{OnchainTradeBuilder, positive_shares, setup_test_db};

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
            when.method(httpmock::Method::POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "10",
                    "side": "buy",
                    "type": "limit",
                    "limit_price": "195.25",
                    "time_in_force": "day",
                    "extended_hours": true
                }));
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

    /// A fill the bot already accounted (witnessed + acknowledged on the
    /// `OnChainTrade` log) must be refused by `process-tx`: re-applying it
    /// would double-count the position past the single-slot
    /// `last_acknowledged_trade_id` guard.
    #[tokio::test]
    async fn process_found_trade_refuses_an_already_acknowledged_fill() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let onchain_trade = OnchainTradeBuilder::default().build();

        let base_symbol = onchain_trade.symbol.base().clone();
        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };
        let amount = onchain_trade.amount.inner();
        let price_usdc = onchain_trade.price.value();
        let direction = onchain_trade.direction;
        let block_timestamp = onchain_trade.block_timestamp.unwrap();

        // Simulate the automated path having already accounted this fill.
        let onchain_trade_store = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        onchain_trade_store
            .send(
                &trade_id,
                OnChainTradeCommand::Witness {
                    symbol: base_symbol.clone(),
                    amount,
                    direction,
                    price_usdc,
                    block_number: 1,
                    block_hash: None,
                    block_timestamp,
                },
            )
            .await
            .unwrap();
        onchain_trade_store
            .send(&trade_id, OnChainTradeCommand::Acknowledge)
            .await
            .unwrap();

        let order_placer = create_order_placer(&ctx, &pool);
        let mut stdout = Vec::new();
        let error = process_found_trade(onchain_trade, &ctx, &pool, &mut stdout, order_placer)
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "fill {trade_id} is already recorded and acknowledged in the onchain trade log; \
                 re-applying it would double-count the position. The bot already accounted this \
                 fill -- no manual processing is needed."
            ),
        );

        // The position must never have been touched -- the fill is not
        // re-counted.
        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert!(
            projection.load(&base_symbol).await.unwrap().is_none(),
            "an already-accounted fill must not create or mutate the position",
        );
    }

    /// A fill the bot witnessed but has not yet acknowledged (the ADR-0005 crash
    /// window between the position write and its acknowledgement marker) is owned
    /// by the automated pipeline. process-tx must refuse it -- the single-slot
    /// position guard cannot dedupe an out-of-order re-application once a newer
    /// fill advanced the slot, so re-applying would double-count.
    #[tokio::test]
    async fn process_found_trade_refuses_a_witnessed_but_unacknowledged_fill() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let onchain_trade = OnchainTradeBuilder::default().build();

        let base_symbol = onchain_trade.symbol.base().clone();
        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };
        let amount = onchain_trade.amount.inner();
        let price_usdc = onchain_trade.price.value();
        let direction = onchain_trade.direction;
        let block_timestamp = onchain_trade.block_timestamp.unwrap();

        // Witness only -- no Acknowledge: the crash-window state.
        let onchain_trade_store = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        onchain_trade_store
            .send(
                &trade_id,
                OnChainTradeCommand::Witness {
                    symbol: base_symbol.clone(),
                    amount,
                    direction,
                    price_usdc,
                    block_number: 1,
                    block_hash: None,
                    block_timestamp,
                },
            )
            .await
            .unwrap();

        let order_placer = create_order_placer(&ctx, &pool);
        let mut stdout = Vec::new();
        let error = process_found_trade(onchain_trade, &ctx, &pool, &mut stdout, order_placer)
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "fill {trade_id} is recorded in the onchain trade log but not yet acknowledged: \
                 a prior process-tx run or an in-flight automated job witnessed it. Re-applying \
                 it here could double-count the position. Verify whether the bot is currently \
                 accounting this symbol (if so, it will finish); otherwise reconcile the position \
                 manually -- the record cannot tell which process wrote it. process-tx only \
                 accounts fills with no onchain trade record."
            ),
        );

        // The position must never have been touched.
        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert!(
            projection.load(&base_symbol).await.unwrap().is_none(),
            "a witnessed-but-unacknowledged fill must not create or mutate the position",
        );
    }

    /// A fill with no prior `OnChainTrade` record is the normal `process-tx`
    /// recovery case and must still be accounted into the position.
    #[tokio::test]
    async fn process_found_trade_accounts_a_fill_not_yet_recorded() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let onchain_trade = OnchainTradeBuilder::default().build();

        let base_symbol = onchain_trade.symbol.base().clone();
        let expected_net = onchain_trade.amount;
        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        let order_placer = create_order_placer(&ctx, &pool);
        let mut stdout = Vec::new();
        process_found_trade(onchain_trade, &ctx, &pool, &mut stdout, order_placer)
            .await
            .unwrap();

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let view = projection.load(&base_symbol).await.unwrap().unwrap();
        assert_eq!(
            view.net, expected_net,
            "an unrecorded fill must still be accounted into the position",
        );

        // process-tx records the fill in the OnChainTrade log via the exactly-once
        // witness -> acknowledge -> mark pair, so a re-run is refused instead of
        // double-counted (and the bot's later pickup of the same fill skips it as
        // already acknowledged).
        let onchain_trade_store = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let recorded = onchain_trade_store
            .load(&trade_id)
            .await
            .unwrap()
            .expect("process-tx must witness the fill into the OnChainTrade log");
        assert!(
            recorded.is_acknowledged(),
            "the witnessed fill must be marked acknowledged",
        );
    }

    /// The exactly-once sequence prunes its own pending-acknowledgement entry:
    /// after a successful `process-tx` run the set is empty (SETTLE ran),
    /// keeping the set bounded (ADR 0010).
    #[tokio::test]
    async fn account_fill_exactly_once_leaves_pending_set_empty() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let onchain_trade = OnchainTradeBuilder::default().build();
        let base_symbol = onchain_trade.symbol.base().clone();

        let mut stdout = Vec::new();
        process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut stdout,
            create_order_placer(&ctx, &pool),
        )
        .await
        .unwrap();

        let (position_store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let position = position_store
            .load(&base_symbol)
            .await
            .unwrap()
            .expect("the position exists after accounting the fill");
        assert!(
            position.pending_acknowledged_trade_ids.is_empty(),
            "SETTLE must prune the entry, leaving the pending set empty; got: {:?}",
            position.pending_acknowledged_trade_ids,
        );
    }

    /// A CLI crash between MARK and SETTLE leaks one pending-acknowledgement
    /// entry. The next `process-tx` re-run on that fill refuses it (already
    /// accounted) AND prunes the leak, so the set self-heals and stays bounded
    /// (ADR 0010).
    #[tokio::test]
    async fn cli_rerun_on_acknowledged_fill_prunes_leak() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let onchain_trade = OnchainTradeBuilder::default().build();
        let base_symbol = onchain_trade.symbol.base().clone();
        let block_number = onchain_trade
            .block_number
            .expect("builder default carries a block number");
        let block_timestamp = onchain_trade
            .block_timestamp
            .expect("builder default carries a block timestamp");
        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };
        let position_trade_id = TradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        let (position_store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let onchain_trade_store = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        // Simulate a crash between MARK and SETTLE: witness, acknowledge, and
        // mark the fill, but never settle -- leaving its entry in the set.
        execute_witness_trade(
            &onchain_trade_store,
            &onchain_trade,
            block_number,
            None,
            block_timestamp,
        )
        .await
        .unwrap();
        execute_acknowledge_fill(
            &position_store,
            &onchain_trade,
            ctx.execution_threshold,
            block_timestamp,
        )
        .await
        .unwrap();
        execute_mark_acknowledged(&onchain_trade_store, &trade_id)
            .await
            .unwrap();

        let leaked = position_store
            .load(&base_symbol)
            .await
            .unwrap()
            .expect("the position exists");
        assert!(
            leaked
                .pending_acknowledged_trade_ids
                .contains(&position_trade_id),
            "premise: the un-settled fill leaks into the pending set",
        );

        // Re-run process-tx on the now-acknowledged fill: it must refuse AND
        // prune the leaked entry.
        let mut stdout = Vec::new();
        let error = process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut stdout,
            create_order_placer(&ctx, &pool),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "fill {trade_id} is already recorded and acknowledged in the onchain trade log; \
                 re-applying it would double-count the position. The bot already accounted this \
                 fill -- no manual processing is needed."
            ),
        );

        let healed = position_store
            .load(&base_symbol)
            .await
            .unwrap()
            .expect("the position exists");
        assert!(
            healed.pending_acknowledged_trade_ids.is_empty(),
            "the refuse path must prune the leaked entry; got: {:?}",
            healed.pending_acknowledged_trade_ids,
        );
    }

    /// process-tx is idempotent on a bot-missed fill: the first run witnesses,
    /// acknowledges, and marks it; a second run on the same fill is refused (it is
    /// now recorded + acknowledged) and the net is unchanged -- not double-counted.
    #[tokio::test]
    async fn process_found_trade_is_idempotent_on_a_bot_missed_fill() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let onchain_trade = OnchainTradeBuilder::default().build();
        let base_symbol = onchain_trade.symbol.base().clone();
        let expected_net = onchain_trade.amount;
        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        // First run accounts the previously-unrecorded fill.
        let mut first = Vec::new();
        process_found_trade(
            onchain_trade.clone(),
            &ctx,
            &pool,
            &mut first,
            create_order_placer(&ctx, &pool),
        )
        .await
        .unwrap();

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let net_after_first = projection.load(&base_symbol).await.unwrap().unwrap().net;
        assert_eq!(net_after_first, expected_net);

        // Second run on the same fill is refused -- now recorded + acknowledged.
        let mut second = Vec::new();
        let error = process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut second,
            create_order_placer(&ctx, &pool),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "fill {trade_id} is already recorded and acknowledged in the onchain trade log; \
                 re-applying it would double-count the position. The bot already accounted this \
                 fill -- no manual processing is needed."
            ),
        );

        // The net is unchanged -- the fill was not counted a second time.
        let net_after_second = projection.load(&base_symbol).await.unwrap().unwrap().net;
        assert_eq!(
            net_after_second, net_after_first,
            "a re-run on an already-accounted fill must not double-count",
        );
    }

    /// The motivating financial failure mode: fill A is fully accounted, then a
    /// newer fill B advances the position's single-slot `last_acknowledged_trade_id`.
    /// Re-running process-tx on A must still be refused -- the position guard alone
    /// no longer recognizes A (its slot points at B), so without the OnChainTrade
    /// witness check A would be counted a second time.
    #[tokio::test]
    async fn process_found_trade_refuses_acknowledged_fill_after_a_newer_fill() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let fill_a = OnchainTradeBuilder::default().with_log_index(1).build();
        let fill_b = OnchainTradeBuilder::default().with_log_index(2).build();

        let base_symbol = fill_a.symbol.base().clone();
        let trade_id_a = OnChainTradeId {
            tx_hash: fill_a.tx_hash,
            log_index: fill_a.log_index,
        };

        // Fully account A on the OnChainTrade log (witness + acknowledge).
        let onchain_trade_store = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        onchain_trade_store
            .send(
                &trade_id_a,
                OnChainTradeCommand::Witness {
                    symbol: base_symbol.clone(),
                    amount: fill_a.amount.inner(),
                    direction: fill_a.direction,
                    price_usdc: fill_a.price.value(),
                    block_number: 1,
                    block_hash: None,
                    block_timestamp: fill_a.block_timestamp.unwrap(),
                },
            )
            .await
            .unwrap();
        onchain_trade_store
            .send(&trade_id_a, OnChainTradeCommand::Acknowledge)
            .await
            .unwrap();

        // Apply A then the newer B to the position, so its single slot points at B.
        let (position_store, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        for fill in [&fill_a, &fill_b] {
            position_store
                .send(
                    &base_symbol,
                    PositionCommand::AcknowledgeOnChainFill {
                        symbol: base_symbol.clone(),
                        threshold: ExecutionThreshold::whole_share(),
                        trade_id: TradeId {
                            tx_hash: fill.tx_hash,
                            log_index: fill.log_index,
                        },
                        amount: fill.amount,
                        direction: fill.direction,
                        price_usdc: fill.price.value(),
                        block_timestamp: fill.block_timestamp.unwrap(),
                    },
                )
                .await
                .unwrap();
        }
        let net_before = projection.load(&base_symbol).await.unwrap().unwrap().net;

        // Re-run process-tx on the older fill A: the position slot points at B, but
        // A is acknowledged on the OnChainTrade log, so it must be refused.
        let order_placer = create_order_placer(&ctx, &pool);
        let mut stdout = Vec::new();
        let error = process_found_trade(fill_a, &ctx, &pool, &mut stdout, order_placer)
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "fill {trade_id_a} is already recorded and acknowledged in the onchain trade log; \
                 re-applying it would double-count the position. The bot already accounted this \
                 fill -- no manual processing is needed."
            ),
        );

        // The net must be unchanged -- A was not counted a second time.
        let net_after = projection.load(&base_symbol).await.unwrap().unwrap().net;
        assert_eq!(
            net_after, net_before,
            "re-running process-tx on an older acknowledged fill must not change the net",
        );

        // The refuse path self-heals a marker-without-settle leak: A is
        // acknowledged on the OnChainTrade log, so refuse_if_fill_already_recorded
        // prunes A from the position's pending-acknowledgement set before bailing,
        // leaving the still-in-flight newer fill B untouched (ADR 0010).
        let pending = position_store
            .load(&base_symbol)
            .await
            .unwrap()
            .expect("position exists after applying A and B")
            .pending_acknowledged_trade_ids;
        assert!(
            !pending.contains(&TradeId {
                tx_hash: trade_id_a.tx_hash,
                log_index: trade_id_a.log_index,
            }),
            "refusing an acknowledged fill must prune it from the pending set (self-heal)",
        );
        assert!(
            pending.contains(&TradeId {
                tx_hash: fill_b.tx_hash,
                log_index: fill_b.log_index,
            }),
            "the untouched newer fill B must remain in the pending set",
        );
    }

    /// A fill that cannot be anchored to a block must not be silently accounted
    /// unrecorded: account_fill_exactly_once fails closed on a missing block
    /// number (the fill cannot be witnessed into the onchain trade log).
    #[tokio::test]
    async fn process_found_trade_fails_closed_when_block_number_absent() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let onchain_trade = OnchainTradeBuilder::default()
            .with_block_number(None)
            .build();
        let base_symbol = onchain_trade.symbol.base().clone();
        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        let mut stdout = Vec::new();
        let error = process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut stdout,
            create_order_placer(&ctx, &pool),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "cannot account fill {trade_id}: the transaction carries no block number, so the \
                 fill cannot be witnessed into the onchain trade log"
            ),
        );

        // Fail-closed before any write: no position, no witness.
        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert!(
            projection.load(&base_symbol).await.unwrap().is_none(),
            "a fill that cannot be witnessed must not touch the position",
        );
    }

    /// Symmetric fail-closed guard for a missing block timestamp.
    #[tokio::test]
    async fn process_found_trade_fails_closed_when_block_timestamp_absent() {
        let ctx = create_base_test_ctx();
        let pool = setup_test_db().await;
        let onchain_trade = OnchainTradeBuilder::default()
            .with_block_timestamp(None)
            .build();
        let base_symbol = onchain_trade.symbol.base().clone();
        let trade_id = OnChainTradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };

        let mut stdout = Vec::new();
        let error = process_found_trade(
            onchain_trade,
            &ctx,
            &pool,
            &mut stdout,
            create_order_placer(&ctx, &pool),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "cannot account fill {trade_id}: the transaction carries no block timestamp, so \
                 the fill cannot be witnessed into the onchain trade log"
            ),
        );

        let (_position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        assert!(
            projection.load(&base_symbol).await.unwrap().is_none(),
            "a fill that cannot be witnessed must not touch the position",
        );
    }
}
