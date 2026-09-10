//! Trading order execution and transaction processing CLI commands.

use alloy::primitives::TxHash;
use alloy::providers::Provider;
use async_trait::async_trait;
use reqwest::StatusCode;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use st0x_config::{BrokerCtx, Ctx};
use st0x_execution::alpaca_broker_api::{AlpacaLimitOrder, AlpacaLimitPrice};
use st0x_execution::{
    ALPACA_MAX_DECIMAL_PLACES, AlpacaBrokerApiError, CancellationOutcome, ClientOrderId, Direction,
    Executor, ExecutorOrderId, FractionalShares, MarketOrder, MarketSession,
    OrderFailureTerminality, OrderPlacement, OrderState, Positive, Symbol, TimeInForce,
    TryIntoExecutor,
};
use st0x_float_serde::format_float_with_fallback;
use st0x_hedge::operator::offchain::order::{OrderPlacementResult, OrderPlacer};
use st0x_hedge::operator::process_tx::{HedgeDisposition, ProcessTxOutcome};
use st0x_registry::SymbolCache;

use super::backpressure_retry::{BACKPRESSURE_RETRY_MAX_ATTEMPTS, retry_on_backpressure};

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
        let placement = execute_broker_order(
            &self.ctx,
            &self.pool,
            order,
            None,
            SessionValidation::Bypass,
            &mut std::io::sink(),
        )
        .await?;
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
    ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
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

#[derive(Debug)]
pub(super) enum CliOrderKind {
    Market {
        time_in_force: Option<TimeInForce>,
    },
    AlpacaLimit {
        limit_price: AlpacaLimitPrice,
        extended_hours: bool,
    },
}

#[derive(Debug)]
pub(super) struct CliOrderRequest {
    pub(super) symbol: Symbol,
    pub(super) shares: Positive<FractionalShares>,
    pub(super) direction: Direction,
    pub(super) kind: CliOrderKind,
    /// An operator-supplied idempotency key from `--client-order-id`.
    /// `None` generates a fresh key; passing the key a previous attempt
    /// printed lets a rerun adopt the already-accepted order (via the
    /// broker's duplicate reconciliation) instead of creating a second
    /// one after a lost response.
    pub(super) client_order_id: Option<Uuid>,
}

impl CliOrderRequest {
    pub(super) fn from_cli_args(
        symbol: Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        time_in_force: Option<TimeInForce>,
        limit_price: Option<AlpacaLimitPrice>,
        extended_hours: bool,
        client_order_id: Option<Uuid>,
    ) -> anyhow::Result<Self> {
        // Reject over-precise quantities instead of the automated path's
        // silent floor: a manual operator typing more than Alpaca's 9
        // decimal places made either a typo or a wrong assumption, and
        // placing a DIFFERENT quantity than the one typed would mask it.
        let (_, lossless) = shares
            .inner()
            .inner()
            .to_fixed_decimal_lossy(ALPACA_MAX_DECIMAL_PLACES)?;
        if !lossless {
            anyhow::bail!(
                "quantity {shares} exceeds Alpaca's maximum of {ALPACA_MAX_DECIMAL_PLACES} \
                 decimal places"
            );
        }

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
            client_order_id,
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

    /// The effective time-in-force label for the evidence block. Limit
    /// orders are hard-coded `day` by the placement path; market orders
    /// without an explicit `--time-in-force` submit with the broker ctx's
    /// configured `time_in_force` (see `execute_broker_order`), so the
    /// label resolves from the same source. Dry-run ignores time-in-force
    /// entirely (a warning says so at placement) and submits a plain
    /// market order, so its label is always `day` -- even when an
    /// explicit value was passed and ignored.
    fn time_in_force_label(&self, ctx: &Ctx) -> &'static str {
        let cli_time_in_force = match &self.kind {
            CliOrderKind::AlpacaLimit { .. } => return "day",
            CliOrderKind::Market { time_in_force } => *time_in_force,
        };

        let effective = match (cli_time_in_force, &ctx.broker) {
            (Some(explicit), _) => explicit,
            (None, BrokerCtx::AlpacaBrokerApi(alpaca)) => alpaca.time_in_force,
        };

        match effective {
            TimeInForce::Day => "day",
            TimeInForce::MarketOnClose => "market-on-close (cls)",
        }
    }
}

/// Pre-submission session validation for manual orders: the overnight
/// session accepts only limit `day` orders flagged `extended_hours = true`,
/// so a non-conforming request is rejected BEFORE the HTTP call with the
/// contract spelled out, instead of surfacing as a broker rejection.
///
/// The one market-order exception is market-on-close: Alpaca queues a cls
/// order submitted after 19:00 ET for the next regular close (see
/// `TimeInForce::MarketOnClose`), so it passes through. Callers must
/// resolve the market time-in-force to the EFFECTIVE value (the broker
/// ctx's configured one when the CLI option is absent) before validating.
fn validate_order_kind_for_session(
    kind: &CliOrderKind,
    session: MarketSession,
) -> anyhow::Result<()> {
    // Exhaustive on purpose: a new session variant must decide its order
    // contract here instead of silently passing validation.
    match session {
        MarketSession::Regular | MarketSession::Extended | MarketSession::Closed => {
            return Ok(());
        }
        MarketSession::Overnight => {}
    }

    match kind {
        CliOrderKind::Market {
            time_in_force: Some(TimeInForce::Day) | None,
        } => anyhow::bail!(
            "the overnight session (20:00-04:00 ET) accepts only limit orders; retry with \
             --limit-price and --extended-hours, or queue for the next regular close with \
             --time-in-force market-on-close"
        ),
        CliOrderKind::AlpacaLimit {
            extended_hours: false,
            ..
        } => anyhow::bail!(
            "overnight orders require extended_hours = true; retry with --extended-hours"
        ),
        CliOrderKind::Market {
            time_in_force: Some(TimeInForce::MarketOnClose),
        }
        | CliOrderKind::AlpacaLimit {
            extended_hours: true,
            ..
        } => Ok(()),
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
            shares_filled,
            price,
        } => {
            writeln!(stdout, "✅ Order Status: FILLED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
            writeln!(stdout, "   Executed At: {executed_at}")?;
            writeln!(stdout, "   Shares Filled: {shares_filled}")?;
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
            terminality,
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
            match terminality {
                OrderFailureTerminality::Terminal => writeln!(
                    stdout,
                    "   Terminality: TERMINAL -- the broker order cannot resume; \
                     a fresh order is needed"
                )?,
                OrderFailureTerminality::NotTerminal => writeln!(
                    stdout,
                    "   Terminality: NOT TERMINAL -- the broker order may still \
                     resume or fill"
                )?,
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
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker;
    let broker = alpaca_auth.clone().try_into_executor().await?;
    let order_id = order_id.to_string();

    Ok(retry_on_backpressure(
        || broker.get_order_status(&order_id),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await?)
}

/// Cancels an open broker order by the id the broker assigned at placement
/// and reports the outcome. Two broker responses are results, not errors,
/// because retrying the cancel can never succeed: a 404 means the broker does
/// not know the id at all, and a 422 means the order is known but no longer
/// cancelable (already filled or cancelled).
pub(super) async fn cancel_broker_order<W: Write>(
    ctx: &Ctx,
    order_id: Uuid,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker;
    let broker = alpaca_auth.clone().try_into_executor().await?;
    let broker_order_id = order_id.to_string();
    let cancellation = retry_on_backpressure(
        || broker.cancel_order(&broker_order_id),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await;

    let outcome = match cancellation {
        Ok(outcome) => outcome,
        Err(AlpacaBrokerApiError::ApiError { status, .. })
            if status == StatusCode::UNPROCESSABLE_ENTITY =>
        {
            writeln!(
                stdout,
                "Order {order_id} is no longer cancelable (already filled or cancelled)"
            )?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    match outcome {
        CancellationOutcome::Requested => {
            writeln!(stdout, "Cancellation requested for order {order_id}")?;
        }
        CancellationOutcome::OrderNotFound => {
            writeln!(stdout, "Order {order_id} unknown to the broker")?;
        }
    }

    Ok(())
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
            write_order_success(stdout, &placement, request.time_in_force_label(ctx), ctx)?;
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

    // A fresh key per invocation, unless the operator re-supplied the key
    // a previous attempt printed: then the broker's duplicate
    // reconciliation adopts the already-accepted order instead of
    // creating a second one after a lost response.
    let client_order_id = ClientOrderId::cli(request.client_order_id.unwrap_or_else(Uuid::new_v4));

    let market_order = MarketOrder {
        symbol: request.symbol.clone(),
        shares: request.shares,
        direction: request.direction,
        client_order_id,
    };

    execute_broker_order(
        ctx,
        pool,
        market_order,
        time_in_force,
        SessionValidation::Enforce,
        stdout,
    )
    .await
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

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker;

    writeln!(stdout, "🔄 Executing Alpaca Broker API limit order...")?;

    let broker = alpaca_auth.clone().try_into_executor().await?;

    let session =
        retry_on_backpressure(|| broker.market_session(), BACKPRESSURE_RETRY_MAX_ATTEMPTS).await?;
    validate_order_kind_for_session(&request.kind, session)?;
    if session == MarketSession::Overnight {
        writeln!(
            stdout,
            "🌙 Overnight session: placing a limit day order with extended_hours=true"
        )?;
    }

    // Same reuse semantics as the market path: an operator-supplied key
    // makes a rerun adopt the accepted order instead of duplicating it.
    let client_order_id = ClientOrderId::cli(request.client_order_id.unwrap_or_else(Uuid::new_v4));
    writeln!(stdout, "   Client Order ID: {client_order_id}")?;

    let order = AlpacaLimitOrder {
        symbol: request.symbol.clone(),
        shares: request.shares,
        direction: request.direction,
        limit_price,
        extended_hours,
        client_order_id,
    };
    let placement = retry_on_backpressure(
        || broker.place_alpaca_limit_order(order.clone()),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await?;

    writeln!(
        stdout,
        "✅ Alpaca Broker API limit order placed with ID: {}",
        placement.order_id
    )?;

    Ok(placement)
}

/// Prints the placement evidence block: everything an Alpaca sign-off
/// reviewer (or a runbook operator) needs to identify the order -- broker
/// timestamps in UTC and ET, the account, the broker-held order terms, and
/// both order ids.
///
/// Limit terms come from the PLACEMENT, not the request: a reused
/// `client_order_id` can adopt an existing broker order whose limit price
/// and extended-hours state differ from this request, and the evidence
/// must report what the broker actually holds.
fn write_order_success<W: Write>(
    stdout: &mut W,
    placement: &OrderPlacement<String>,
    time_in_force_label: &str,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    info!(
        symbol = %placement.symbol,
        direction = ?placement.direction,
        quantity = %placement.shares,
        order_id = %placement.order_id,
        "Order placed successfully"
    );
    writeln!(stdout, "✅ Order placed successfully")?;
    writeln!(
        stdout,
        "   Placed at (UTC): {}",
        placement.placed_at.format("%Y-%m-%d %H:%M:%S%.3f")
    )?;
    writeln!(
        stdout,
        "   Placed at (ET):  {}",
        placement
            .placed_at
            .with_timezone(&chrono_tz::America::New_York)
            .format("%Y-%m-%d %H:%M:%S %Z")
    )?;
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker;
    writeln!(stdout, "   Account ID: {}", alpaca_auth.account_id)?;
    writeln!(stdout, "   Symbol: {}", placement.symbol)?;
    writeln!(stdout, "   Action: {:?}", placement.direction)?;
    writeln!(stdout, "   Quantity: {}", placement.shares)?;
    writeln!(stdout, "   Time in Force: {time_in_force_label}")?;
    writeln!(stdout, "   Order ID: {}", placement.order_id)?;

    if let Some(limit_price) = placement.limit_price {
        writeln!(stdout, "   Order Type: limit")?;
        writeln!(
            stdout,
            "   Limit Price: ${}",
            format_float_with_fallback(&limit_price.inner().inner())
        )?;
        writeln!(
            stdout,
            "   Extended Hours: {}",
            if placement.extended_hours {
                "yes"
            } else {
                "no"
            }
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
    // The CLI runs outside the bot: no reactors to reach, so standalone stores.
    let stores =
        st0x_hedge::operator::process_tx::ProcessTxStores::standalone(pool, order_placer.clone())
            .await?;
    let outcome = st0x_hedge::operator::process_tx::process_tx(
        tx_hash,
        ctx,
        pool,
        provider,
        cache,
        &stores,
        order_placer,
        None,
    )
    .await?;
    render_process_tx_outcome(tx_hash, &outcome, stdout)
}

fn render_process_tx_outcome<W: Write>(
    tx_hash: TxHash,
    outcome: &ProcessTxOutcome,
    stdout: &mut W,
) -> anyhow::Result<()> {
    match outcome {
        ProcessTxOutcome::NoTradeableEvents => {
            writeln!(stdout, "No tradeable events found in transaction {tx_hash}")?;
            writeln!(
                stdout,
                "This transaction may not contain orderbook events matching the configured order hash."
            )?;
        }
        ProcessTxOutcome::TransactionNotFound { tx_hash } => {
            writeln!(stdout, "Transaction not found: {tx_hash}")?;
        }
        ProcessTxOutcome::AlreadyAccounted => {
            writeln!(
                stdout,
                "Fill is already fully accounted. Nothing to do; the normal pipeline will hedge any unhedged position exposure."
            )?;
        }
        ProcessTxOutcome::PendingHedgeInFlight => {
            writeln!(
                stdout,
                "An existing pending hedge is in flight; settled the fill without placing a new hedge."
            )?;
        }
        ProcessTxOutcome::BelowExecutionThreshold => {
            writeln!(
                stdout,
                "Trade accumulated but did not trigger execution yet (waiting to accumulate enough shares for a whole share execution)."
            )?;
        }
        ProcessTxOutcome::TradingDisabled { symbol } => {
            writeln!(stdout, "Trading disabled by configuration for {symbol}")?;
        }
        ProcessTxOutcome::PlacementRejected { symbol } => {
            writeln!(
                stdout,
                "Placement for {symbol} was rejected by domain state; a concurrent placement already claimed the position. Settled the fill."
            )?;
        }
        ProcessTxOutcome::HedgePlaced {
            symbol,
            offchain_order_id,
            shares,
            direction,
            disposition,
        } => {
            writeln!(
                stdout,
                "Placed {direction:?} hedge for {shares} {symbol} (order {offchain_order_id})"
            )?;
            match disposition {
                HedgeDisposition::InFlight => writeln!(
                    stdout,
                    "Order submitted; it will be reconciled to a terminal state by the order-status recovery sweep on the next bot startup."
                )?,
                HedgeDisposition::ClearedForRetry => writeln!(
                    stdout,
                    "Hedge placement failed or the order vanished; pending order cleared so the normal pipeline can re-hedge."
                )?,
                HedgeDisposition::Finalized => writeln!(
                    stdout,
                    "The order reached a terminal broker state and the position was finalized."
                )?,
            }
        }
    }
    Ok(())
}

/// Whether `execute_broker_order` enforces the pre-submission session
/// contract.
#[derive(Clone, Copy)]
pub(super) enum SessionValidation {
    /// Interactive placements: reject a non-conforming order BEFORE the
    /// HTTP call, with the retry flags spelled out for the operator.
    Enforce,
    /// The `OrderPlacer` hedge path (`process-tx`): it cannot pass the
    /// retry flags the rejection message names, and a day market hedge
    /// placed overnight queues at Alpaca for the next regular open, so
    /// the pre-gate behavior is kept and no session lookup runs.
    Bypass,
}

pub(super) async fn execute_broker_order<W: Write>(
    ctx: &Ctx,
    _pool: &SqlitePool,
    market_order: MarketOrder,
    time_in_force: Option<TimeInForce>,
    session_validation: SessionValidation,
    stdout: &mut W,
) -> anyhow::Result<OrderPlacement<String>> {
    writeln!(
        stdout,
        "   Client Order ID: {}",
        market_order.client_order_id
    )?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker;

    writeln!(stdout, "🔄 Executing Alpaca Broker API order...")?;
    let mut auth = alpaca_auth.clone();
    if let Some(tif) = time_in_force {
        auth.time_in_force = tif;
    }
    // The submitted order carries auth.time_in_force, so session
    // validation must see that effective value: a configured
    // market-on-close with no CLI override queues overnight
    // instead of being rejected.
    let effective_time_in_force = auth.time_in_force;
    let broker = auth.try_into_executor().await?;

    match session_validation {
        SessionValidation::Enforce => {
            let session =
                retry_on_backpressure(|| broker.market_session(), BACKPRESSURE_RETRY_MAX_ATTEMPTS)
                    .await?;
            validate_order_kind_for_session(
                &CliOrderKind::Market {
                    time_in_force: Some(effective_time_in_force),
                },
                session,
            )?;
        }
        SessionValidation::Bypass => {}
    }

    let placement = retry_on_backpressure(
        || broker.place_market_order(market_order.clone()),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await?;
    writeln!(
        stdout,
        "✅ Alpaca Broker API order placed with ID: {}",
        placement.order_id
    )?;
    Ok(placement)
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
) -> anyhow::Result<Positive<FractionalShares>> {
    let market_order = MarketOrder {
        symbol,
        shares,
        direction: Direction::Buy,
        client_order_id: ClientOrderId::cli(Uuid::new_v4()),
    };

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker;
    let broker = alpaca_auth.clone().try_into_executor().await?;

    place_market_order_until_filled(&broker, market_order, stdout).await
}

async fn place_market_order_until_filled<Exec: Executor, W: Write>(
    broker: &Exec,
    market_order: MarketOrder,
    stdout: &mut W,
) -> anyhow::Result<Positive<FractionalShares>> {
    let requested_shares = market_order.shares;
    let placement = retry_on_backpressure(
        || broker.place_market_order(market_order.clone()),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await?;
    writeln!(stdout, "   Buy order placed: {}", placement.order_id)?;

    for attempt in 1..=BUY_FILL_MAX_ATTEMPTS {
        let order_state = retry_on_backpressure(
            || broker.get_order_status(&placement.order_id),
            BACKPRESSURE_RETRY_MAX_ATTEMPTS,
        )
        .await?;
        match order_state {
            OrderState::Filled {
                order_id,
                shares_filled,
                ..
            } => {
                writeln!(stdout, "   Buy filled (order {order_id})")?;
                let requested_matches_placed = requested_shares
                    .inner()
                    .inner()
                    .eq(placement.shares.inner().inner())?;
                let placed_matches_filled = placement
                    .shares
                    .inner()
                    .inner()
                    .eq(shares_filled.inner().inner())?;
                if !requested_matches_placed || !placed_matches_filled {
                    writeln!(stdout, "   Requested quantity: {requested_shares}")?;
                    writeln!(stdout, "   Placed quantity: {}", placement.shares)?;
                    writeln!(stdout, "   Filled quantity: {shares_filled}")?;
                }
                return Ok(shares_filled);
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

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use chrono::Utc;
    use httpmock::MockServer;
    use proptest::prelude::*;
    use rain_math_float::Float;
    use regex::Regex;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::uuid;

    use st0x_config::ChainRegistry;
    use st0x_config::HedgingAssets;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        BrokerCtx, ExecutionThreshold, InventoryMode, LogFormat, LogLevel, TradingChain,
    };
    use st0x_execution::alpaca_broker_api::AlpacaBrokerMock;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, CancellationOutcome,
        CounterTradePreflight, ExecutionError, InventoryResult, LimitOrder, MockExecutor, Positive,
        SupportedExecutor, Usd,
    };
    use st0x_hedge::operator::offchain::order::OffchainOrderId;
    use st0x_hedge::operator::test_utils::{
        mock_alpaca_broker_ctx, try_positive_shares, try_setup_test_db,
    };

    use super::*;

    fn positive_shares(value: &str) -> Positive<FractionalShares> {
        try_positive_shares(value).expect("test shares must be valid and positive")
    }

    async fn setup_test_db() -> SqlitePool {
        try_setup_test_db()
            .await
            .expect("test database setup must succeed")
    }

    fn positive_fractional(value: &str) -> Positive<FractionalShares> {
        Positive::new(FractionalShares::new(
            Float::parse(value.to_string()).unwrap(),
        ))
        .unwrap()
    }

    /// Pins the placement evidence block literally: the sign-off sheet's
    /// reviewer reads these lines as evidence, so a silently dropped or
    /// reformatted field (timestamps, account id, time-in-force) must fail a
    /// test, not surface on a demo call.
    #[tokio::test]
    async fn write_order_success_prints_the_full_evidence_block() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);

        let placed_at = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 24, 1, 30, 0).unwrap();
        let placement = OrderPlacement {
            order_id: "broker-order-1".to_string(),
            symbol: Symbol::new("RKLB").unwrap(),
            shares: positive_shares("10"),
            direction: Direction::Buy,
            placed_at,
            extended_hours: true,
            limit_price: Some(*positive_limit_price("24.10").as_price()),
        };
        let mut stdout = Vec::new();
        write_order_success(&mut stdout, &placement, "day", &ctx).unwrap();

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "✅ Order placed successfully\n\
             \x20  Placed at (UTC): 2026-08-24 01:30:00.000\n\
             \x20  Placed at (ET):  2026-08-23 21:30:00 EDT\n\
             \x20  Account ID: 904837e3-3b76-47ec-b432-046db621571b\n\
             \x20  Symbol: RKLB\n\
             \x20  Action: Buy\n\
             \x20  Quantity: 10\n\
             \x20  Time in Force: day\n\
             \x20  Order ID: broker-order-1\n\
             \x20  Order Type: limit\n\
             \x20  Limit Price: $24.1\n\
             \x20  Extended Hours: yes\n"
        );
    }

    #[test]
    fn time_in_force_label_reports_the_configured_market_on_close() {
        // No --time-in-force: execute_broker_order submits with the broker
        // ctx's configured value, so the evidence label must match it.
        let server = MockServer::start();
        let mut ctx = create_alpaca_broker_api_test_ctx(&server);
        let BrokerCtx::AlpacaBrokerApi(ref mut alpaca) = ctx.broker;
        alpaca.time_in_force = TimeInForce::MarketOnClose;

        let request = cli_order_request(
            Symbol::new("RKLB").unwrap(),
            positive_shares("10"),
            Direction::Buy,
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(request.time_in_force_label(&ctx), "market-on-close (cls)");
    }

    #[test]
    fn time_in_force_label_prefers_the_explicit_cli_value() {
        // An explicit --time-in-force overrides the configured value in
        // execute_broker_order; the label follows the same precedence.
        let server = MockServer::start();
        let mut ctx = create_alpaca_broker_api_test_ctx(&server);
        let BrokerCtx::AlpacaBrokerApi(ref mut alpaca) = ctx.broker;
        alpaca.time_in_force = TimeInForce::MarketOnClose;

        let request = cli_order_request(
            Symbol::new("RKLB").unwrap(),
            positive_shares("10"),
            Direction::Buy,
            Some(TimeInForce::Day),
            None,
            false,
        )
        .unwrap();

        assert_eq!(request.time_in_force_label(&ctx), "day");
    }

    #[test]
    fn from_cli_args_rejects_quantity_over_nine_decimal_places() {
        let error = CliOrderRequest::from_cli_args(
            Symbol::new("RKLB").unwrap(),
            positive_fractional("0.1234567891"),
            Direction::Buy,
            None,
            None,
            false,
            None,
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "quantity 0.1234567891 exceeds Alpaca's maximum of 9 decimal places"
        );
    }

    #[test]
    fn from_cli_args_accepts_quantity_at_nine_decimal_places() {
        let request = CliOrderRequest::from_cli_args(
            Symbol::new("RKLB").unwrap(),
            positive_fractional("0.123456789"),
            Direction::Buy,
            None,
            None,
            false,
            None,
        )
        .unwrap();

        assert_eq!(request.shares, positive_fractional("0.123456789"));
    }

    proptest! {
        // Both sides of the 9-decimal boundary: a quantity whose LAST
        // fractional digit is nonzero has exactly `leading.len() + 1`
        // decimal places, so acceptance flips precisely at nine.
        #[test]
        fn quantity_precision_boundary_flips_at_nine_decimals(
            integer in 0u32..1_000,
            leading in proptest::collection::vec(0u8..=9, 0..=11),
            last in 1u8..=9,
        ) {
            let decimal_places = leading.len() + 1;
            let mut fraction: String = leading
                .iter()
                .map(ToString::to_string)
                .collect();
            fraction.push_str(&last.to_string());
            let quantity = format!("{integer}.{fraction}");

            let result = CliOrderRequest::from_cli_args(
                Symbol::new("RKLB").unwrap(),
                positive_fractional(&quantity),
                Direction::Buy,
                None,
                None,
                false,
                None,
            );

            if decimal_places <= 9 {
                let request = result.unwrap();
                prop_assert_eq!(request.shares, positive_fractional(&quantity));
            } else {
                let error = result.unwrap_err();
                prop_assert_eq!(
                    error.to_string(),
                    format!(
                        "quantity {quantity} exceeds Alpaca's maximum of 9 decimal places"
                    )
                );
            }
        }
    }

    #[test]
    fn from_cli_args_accepts_trailing_zeros_beyond_nine_decimal_places() {
        // Trailing zeros carry no precision: the VALUE is representable
        // at nine decimals, so the lossless check accepts it.
        let request = CliOrderRequest::from_cli_args(
            Symbol::new("RKLB").unwrap(),
            positive_fractional("0.1000000000"),
            Direction::Buy,
            None,
            None,
            false,
            None,
        )
        .unwrap();

        assert_eq!(request.shares, positive_fractional("0.1"));
    }

    #[test]
    fn overnight_session_rejects_day_market_orders() {
        for time_in_force in [None, Some(TimeInForce::Day)] {
            let error = validate_order_kind_for_session(
                &CliOrderKind::Market { time_in_force },
                MarketSession::Overnight,
            )
            .unwrap_err();

            assert_eq!(
                error.to_string(),
                "the overnight session (20:00-04:00 ET) accepts only limit orders; retry with \
                 --limit-price and --extended-hours, or queue for the next regular close with \
                 --time-in-force market-on-close"
            );
        }
    }

    #[test]
    fn overnight_session_accepts_market_on_close_orders() {
        // Alpaca queues a cls order submitted after 19:00 ET for the next
        // regular close, so the overnight session must let it through.
        validate_order_kind_for_session(
            &CliOrderKind::Market {
                time_in_force: Some(TimeInForce::MarketOnClose),
            },
            MarketSession::Overnight,
        )
        .unwrap();
    }

    #[test]
    fn overnight_session_rejects_limit_without_extended_hours() {
        let limit_price = "24.10".parse::<AlpacaLimitPrice>().unwrap();
        let error = validate_order_kind_for_session(
            &CliOrderKind::AlpacaLimit {
                limit_price,
                extended_hours: false,
            },
            MarketSession::Overnight,
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "overnight orders require extended_hours = true; retry with --extended-hours"
        );
    }

    #[test]
    fn overnight_session_accepts_extended_hours_limit() {
        let limit_price = "24.10".parse::<AlpacaLimitPrice>().unwrap();
        validate_order_kind_for_session(
            &CliOrderKind::AlpacaLimit {
                limit_price,
                extended_hours: true,
            },
            MarketSession::Overnight,
        )
        .unwrap();
    }

    #[test]
    fn other_sessions_accept_every_order_kind() {
        for session in [
            MarketSession::Regular,
            MarketSession::Extended,
            MarketSession::Closed,
        ] {
            validate_order_kind_for_session(
                &CliOrderKind::Market {
                    time_in_force: Some(TimeInForce::MarketOnClose),
                },
                session,
            )
            .unwrap();

            let limit_price = "24.10".parse::<AlpacaLimitPrice>().unwrap();
            validate_order_kind_for_session(
                &CliOrderKind::AlpacaLimit {
                    limit_price,
                    extended_hours: false,
                },
                session,
            )
            .unwrap();
        }
    }
    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[derive(Clone)]
    struct SequencedStatusExecutor {
        statuses: Arc<Vec<OrderState>>,
        status_calls: Arc<AtomicUsize>,
        placed_shares: Option<Positive<FractionalShares>>,
    }

    impl SequencedStatusExecutor {
        fn new(statuses: Vec<OrderState>) -> Self {
            Self {
                statuses: Arc::new(statuses),
                status_calls: Arc::new(AtomicUsize::new(0)),
                placed_shares: None,
            }
        }

        fn with_placed_shares(mut self, placed_shares: Positive<FractionalShares>) -> Self {
            self.placed_shares = Some(placed_shares);
            self
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
            let shares = self.placed_shares.unwrap_or(order.shares);
            Ok(OrderPlacement {
                order_id: "sequenced-order-id".to_string(),
                symbol: order.symbol,
                shares,
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

        async fn preflight_counter_trade_at_price(
            &self,
            order: MarketOrder,
            _reference_price: Positive<Usd>,
        ) -> Result<CounterTradePreflight, Self::Error> {
            self.preflight_counter_trade(order).await
        }
    }

    fn create_base_test_ctx() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            file_logging: None,
            log_format: LogFormat::Text,
            log_query_url_template: None,
            server_port: 8080,
            board_port: 8081,
            chains: ChainRegistry::single_trading_chain(
                TradingChain::test()
                    .orderbook(address!("0x1234567890123456789012345678901234567890"))
                    .inventory(InventoryMode::Managed {
                        inventory: address!("0x1234567890123456789012345678901234567890"),
                    })
                    .vault_owner(Address::ZERO)
                    .deployment_block(1)
                    .call(),
            ),
            order_polling_interval_secs: 15,
            order_polling_max_jitter_secs: 5,
            position_check_interval_secs: 60,
            inventory_poll_interval_secs: 60,
            inventory_divergence_threshold: std::num::NonZeroU32::MIN,
            hedge_order_gate_reconciliation_timeout_secs: std::num::NonZeroU64::MIN,
            extended_hours_reprice_timeout_secs: std::num::NonZeroU64::new(300),
            close_flatten_reprice_timeout_secs: 60,
            extended_hours_close_flatten_window_secs: 900,
            close_flatten_cross_max_bps: 400,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: st0x_config::test_alpaca_broker_ctx(),
            telemetry: None,
            alerts: None,
            startup_notices: Vec::new(),
            pricing: None,
            rebalancing: st0x_config::default_test_rebalancing_ctx(),
            order_owner: Address::ZERO,
            wallet: None,
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: HedgingAssets::default(),
            travel_rule: None,
            rest_api: None,
            ops_api: None,
            issuance: create_test_issuance_ctx(),
            bot_gas_valuation: None,
            orchestrator: None,
        }
    }

    fn create_alpaca_broker_api_test_ctx(mock_server: &MockServer) -> Ctx {
        let mut ctx = create_base_test_ctx();
        ctx.broker = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            auth: st0x_execution::AlpacaBrokerAuth::Basic {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
            },
            account_id: TEST_ACCOUNT_ID,
            mode: Some(AlpacaBrokerApiMode::Mock(mock_server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
            time_in_force: TimeInForce::Day,
        });
        ctx
    }

    #[tokio::test]
    async fn order_status_command_reports_the_broker_status() {
        let broker_mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![(
                Symbol::new("AAPL").unwrap(),
                Float::parse("150.00".to_string()).unwrap(),
            )])
            .symbol_positions(vec![])
            .call()
            .await;
        let mut ctx = create_base_test_ctx();
        ctx.broker = mock_alpaca_broker_ctx(broker_mock.base_url());
        let pool = setup_test_db().await;

        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("1"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        };
        let placement = execute_broker_order(
            &ctx,
            &pool,
            market_order,
            None,
            SessionValidation::Bypass,
            &mut Vec::new(),
        )
        .await
        .unwrap();

        let mut stdout = Vec::new();
        order_status_command(&mut stdout, &placement.order_id, &ctx, &pool)
            .await
            .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Order Status:"),
            "expected a broker status report, got: {output}"
        );
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
            None,
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

    fn mock_active_account(server: &MockServer) -> httpmock::Mock<'_> {
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE"
                }));
        })
    }

    /// Serves an all-day-open calendar entry for today so the pre-submission
    /// session check classifies Regular regardless of when the test runs.
    /// Returns the mock so tests can assert the session check actually
    /// queried the calendar before placement.
    fn mock_regular_session_calendar(server: &MockServer) -> httpmock::Mock<'_> {
        let today = Utc::now()
            .with_timezone(&chrono_tz::America::New_York)
            .date_naive();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/calendar");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "date": today.format("%Y-%m-%d").to_string(),
                    "open": "00:00",
                    "close": "23:59",
                    "session_open": "0000",
                    "session_close": "2359"
                }]));
        })
    }

    fn setup_alpaca_broker_market_order_mocks<'a>(
        server: &'a MockServer,
        symbol: &'a str,
        quantity: &'a str,
        side: &'a str,
    ) -> (
        httpmock::Mock<'a>,
        httpmock::Mock<'a>,
        httpmock::Mock<'a>,
        httpmock::Mock<'a>,
    ) {
        let account_mock = mock_active_account(server);
        let calendar_mock = mock_regular_session_calendar(server);

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
                    "attributes": ["overnight_tradable"]
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

        (account_mock, calendar_mock, asset_mock, order_mock)
    }

    fn setup_alpaca_broker_limit_order_mocks(
        server: &MockServer,
    ) -> (
        httpmock::Mock<'_>,
        httpmock::Mock<'_>,
        httpmock::Mock<'_>,
        httpmock::Mock<'_>,
    ) {
        let account_mock = mock_active_account(server);
        let calendar_mock = mock_regular_session_calendar(server);

        let asset_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/assets/AAPL");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "status": "active",
                    "tradable": true,
                    "attributes": ["overnight_tradable"]
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

        (account_mock, calendar_mock, asset_mock, order_mock)
    }

    /// Regex asserting a request body carries a well-formed `cli-{uuid}`
    /// `client_order_id`, shared by the market- and limit-order mocks.
    fn client_order_id_cli_pattern() -> Regex {
        Regex::new(
            r#""client_order_id":"cli-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}""#,
        )
        .unwrap()
    }

    fn mock_tradable_asset<'a>(server: &'a MockServer, symbol: &'a str) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!("/v1/assets/{symbol}"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": symbol,
                    "status": "active",
                    "tradable": true,
                    "attributes": ["overnight_tradable"]
                }));
        })
    }

    #[tokio::test]
    async fn explicit_client_order_id_is_sent_verbatim() {
        // --client-order-id makes the idempotency key deterministic, so a
        // rerun after a lost response re-sends the SAME key instead of a
        // fresh UUID that would create a second order.
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let _account_mock = mock_active_account(&server);
        let _calendar_mock = mock_regular_session_calendar(&server);
        let _asset_mock = mock_tradable_asset(&server, "AAPL");

        let reused = uuid!("4d6d9f40-5434-4f77-89f6-7156e375b739");
        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body_includes(
                    json!({"client_order_id": format!("cli-{reused}")}).to_string(),
                );
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let request = CliOrderRequest::from_cli_args(
            Symbol::new("AAPL").unwrap(),
            positive_shares("100"),
            Direction::Buy,
            None,
            None,
            false,
            Some(reused),
        )
        .unwrap();
        super::execute_order_with_writers(request, &ctx, &pool, &mut std::io::sink())
            .await
            .unwrap();

        order_mock.assert();
    }

    #[tokio::test]
    async fn rerun_with_the_same_client_order_id_adopts_the_accepted_order() {
        // The lost-response regression: the first attempt was accepted
        // but its response never arrived. A rerun with the printed key
        // hits the duplicate 422 and adopts the broker's recorded order
        // instead of creating a second one.
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let _account_mock = mock_active_account(&server);
        let _calendar_mock = mock_regular_session_calendar(&server);
        let _asset_mock = mock_tradable_asset(&server, "AAPL");

        let reused = uuid!("4d6d9f40-5434-4f77-89f6-7156e375b739");
        let place_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40_010_001,
                    "message": "client_order_id must be unique"
                }));
        });
        // The adopted order deliberately differs from the request on every
        // reportable term: a distinct broker order id (NOT the account id,
        // so the Order ID assertion cannot be satisfied by the Account ID
        // line), a smaller accepted quantity, and adopted extended-hours
        // limit terms the plain market request never asked for.
        let lookup_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(
                    "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id",
                )
                .query_param("client_order_id", format!("cli-{reused}"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "AAPL",
                    "qty": "50",
                    "side": "buy",
                    "status": "new",
                    "extended_hours": true,
                    "limit_price": "24.20",
                    "filled_avg_price": null
                }));
        });

        let request = CliOrderRequest::from_cli_args(
            Symbol::new("AAPL").unwrap(),
            positive_shares("100"),
            Direction::Buy,
            None,
            None,
            false,
            Some(reused),
        )
        .unwrap();
        let mut stdout = Vec::new();
        super::execute_order_with_writers(request, &ctx, &pool, &mut stdout)
            .await
            .unwrap();

        place_mock.assert();
        lookup_mock.assert();
        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("   Order ID: 61e7b016-9c91-4a97-b912-615c9d365c9d\n"),
            "the ADOPTED broker order id must be reported on its exact line, got:\n{output}"
        );
        assert!(
            output.contains("   Quantity: 50\n"),
            "the broker-accepted quantity must be reported, not the request's 100:\n{output}"
        );
        assert!(
            output.contains("   Limit Price: $24.2\n"),
            "the adopted limit price must be reported for a market request:\n{output}"
        );
        assert!(
            output.contains("   Extended Hours: yes\n"),
            "the adopted extended-hours state must be reported honestly:\n{output}"
        );
    }

    #[tokio::test]
    async fn test_execute_order_buy_success() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let (account_mock, calendar_mock, asset_mock, order_mock) =
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
        calendar_mock.assert();
        asset_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_order_sell_success() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let (account_mock, calendar_mock, asset_mock, order_mock) =
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
        calendar_mock.assert();
        asset_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn order_placer_hedge_path_bypasses_the_session_gate() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let (account_mock, calendar_mock, asset_mock, order_mock) =
            setup_alpaca_broker_market_order_mocks(&server, "AAPL", "100", "buy");

        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("100"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        };
        execute_broker_order(
            &ctx,
            &pool,
            market_order,
            None,
            SessionValidation::Bypass,
            &mut std::io::sink(),
        )
        .await
        .unwrap();

        account_mock.assert();
        // The hedge path places without consulting the calendar: the
        // session gate (whose rejection message names CLI retry flags
        // the OrderPlacer cannot pass) applies only to interactive
        // placements.
        calendar_mock.assert_calls(0);
        asset_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn cancel_broker_order_reports_requested_on_success() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let account_mock = mock_active_account(&server);

        let order_id = uuid!("61e7b016-9c91-4a97-b912-615c9d365c9d");
        let delete_mock = server.mock(|when, then| {
            when.method(httpmock::Method::DELETE).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(204);
        });

        let mut stdout = Vec::new();
        cancel_broker_order(&ctx, order_id, &mut stdout)
            .await
            .unwrap();

        account_mock.assert();
        delete_mock.assert();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("Cancellation requested for order {order_id}\n")
        );
    }

    #[tokio::test]
    async fn cancel_broker_order_reports_not_found_on_404() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let account_mock = mock_active_account(&server);

        let order_id = uuid!("61e7b016-9c91-4a97-b912-615c9d365c9d");
        let delete_mock = server.mock(|when, then| {
            when.method(httpmock::Method::DELETE).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(404)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "Resource does not exist" }));
        });

        let mut stdout = Vec::new();
        cancel_broker_order(&ctx, order_id, &mut stdout)
            .await
            .unwrap();

        account_mock.assert();
        delete_mock.assert();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("Order {order_id} unknown to the broker\n")
        );
    }

    /// An order that just filled or was already cancelled is KNOWN to the
    /// broker, so the DELETE answers 422 ("order is not cancelable"), not
    /// 404. The CLI must report that as an outcome and exit 0 -- the most
    /// likely operator scenario is the limit order filling before the cancel
    /// lands, which is not an error.
    #[tokio::test]
    async fn cancel_broker_order_reports_no_longer_cancelable_on_422() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let account_mock = mock_active_account(&server);

        let order_id = uuid!("61e7b016-9c91-4a97-b912-615c9d365c9d");
        let delete_mock = server.mock(|when, then| {
            when.method(httpmock::Method::DELETE).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(422)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "order is not cancelable" }));
        });

        let mut stdout = Vec::new();
        cancel_broker_order(&ctx, order_id, &mut stdout)
            .await
            .unwrap();

        account_mock.assert();
        delete_mock.assert();
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("Order {order_id} is no longer cancelable (already filled or cancelled)\n")
        );
    }

    /// A sustained 429 on the cancel DELETE must be retried in place with the
    /// bounded CLI budget and then propagate as an error: exactly
    /// `BACKPRESSURE_RETRY_MAX_ATTEMPTS` DELETEs, no more. This pins the
    /// cancel path's wiring through the real client (`Retry-After` capture,
    /// `find_backpressure` classification); the retry-then-succeed half is
    /// pinned generically in `backpressure_retry`'s unit tests, since
    /// `httpmock` cannot sequence a 429 followed by a 204.
    #[tokio::test]
    async fn cancel_broker_order_retries_429_up_to_budget_then_errors() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let account_mock = mock_active_account(&server);

        let order_id = uuid!("61e7b016-9c91-4a97-b912-615c9d365c9d");
        let delete_mock = server.mock(|when, then| {
            when.method(httpmock::Method::DELETE).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(429)
                .header("content-type", "application/json")
                .header("Retry-After", "0")
                .json_body(json!({ "message": "rate limited" }));
        });

        let mut stdout = Vec::new();
        let error = cancel_broker_order(&ctx, order_id, &mut stdout)
            .await
            .unwrap_err();

        account_mock.assert();
        assert_eq!(
            delete_mock.calls(),
            BACKPRESSURE_RETRY_MAX_ATTEMPTS as usize,
            "must stop after exactly the bounded CLI retry budget"
        );
        assert!(
            matches!(
                error.downcast_ref::<AlpacaBrokerApiError>(),
                Some(AlpacaBrokerApiError::ApiError { status, .. })
                    if *status == StatusCode::TOO_MANY_REQUESTS
            ),
            "expected the 429 ApiError to propagate unchanged, got {error:?}"
        );
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "",
            "no outcome line may be printed when the cancel ultimately fails"
        );
    }

    #[tokio::test]
    async fn test_execute_order_failure_stdout_contains_error() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;

        mock_active_account(&server);
        // Without the calendar mock the session check fails first and the
        // test would assert the failure banner without ever reaching the
        // order POST it claims to exercise.
        let calendar_mock = mock_regular_session_calendar(&server);

        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/assets/AAPL");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "status": "active",
                    "tradable": true,
                    "attributes": ["overnight_tradable"]
                }));
        });

        let order_mock = server.mock(|when, then| {
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

        calendar_mock.assert();
        order_mock.assert();
        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("❌ Failed to place order"),
            "Expected failure message, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_limit_order_uses_alpaca_broker_api_path() {
        let server = MockServer::start();
        let ctx = create_alpaca_broker_api_test_ctx(&server);
        let pool = setup_test_db().await;
        let (account_mock, calendar_mock, asset_mock, order_mock) =
            setup_alpaca_broker_limit_order_mocks(&server);

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
        calendar_mock.assert();
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

        let shares_filled = place_market_order_until_filled(&broker, order, &mut stdout)
            .await
            .expect("a filled order must resolve the buy-fill wait");

        assert_eq!(shares_filled, positive_shares("10"));

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
                shares_filled: positive_shares("10"),
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

        let shares_filled = place_market_order_until_filled(&broker, order, &mut stdout)
            .await
            .expect("partial fill should keep polling until the broker reports Filled");

        let output = String::from_utf8(stdout).unwrap();
        assert_eq!(
            broker.status_calls(),
            2,
            "the buy-fill wait must poll again after PartiallyFilled"
        );
        assert_eq!(shares_filled, positive_shares("10"));
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
            terminality: st0x_execution::OrderFailureTerminality::Terminal,
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

    #[tokio::test]
    async fn market_buy_returns_filled_quantity_and_reports_quantity_adjustments() {
        let filled_shares = positive_shares("0.0041");
        let broker = SequencedStatusExecutor::new(vec![OrderState::Filled {
            executed_at: Utc::now(),
            order_id: ExecutorOrderId::new("sequenced-order-id"),
            shares_filled: filled_shares,
            price: st0x_execution::Usd::new(Float::parse("199.50".to_string()).unwrap()),
        }])
        .with_placed_shares(positive_shares("0.004115451"));
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("0.004115451077565126"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        };
        let mut stdout = Vec::new();

        let returned_shares = place_market_order_until_filled(&broker, order, &mut stdout)
            .await
            .unwrap();

        assert_eq!(returned_shares, filled_shares);
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            concat!(
                "   Buy order placed: sequenced-order-id\n",
                "   Buy filled (order sequenced-order-id)\n",
                "   Requested quantity: 0.004115451077565126\n",
                "   Placed quantity: 0.004115451\n",
                "   Filled quantity: 0.0041\n",
            )
        );
    }

    /// Executor error carrying an optional Alpaca 429 as its `source`, so
    /// `find_backpressure` classifies it exactly like a real
    /// `AlpacaBrokerApiError` -- used to pin that
    /// `place_market_order_until_filled` retries a classified 429 in place
    /// (RAI-1494) instead of failing the buy-fill wait immediately.
    #[derive(Debug, thiserror::Error)]
    #[error("test executor error")]
    struct BackpressureTestError {
        #[source]
        source: Option<st0x_execution::AlpacaBrokerApiError>,
    }

    fn backpressure_429_error() -> BackpressureTestError {
        BackpressureTestError {
            source: Some(st0x_execution::AlpacaBrokerApiError::ApiError {
                status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                alpaca_code: None,
                message: "rate limited".to_string(),
                retry_after: Some(std::time::Duration::from_millis(1)),
            }),
        }
    }

    /// Executor whose `place_market_order` returns a classified 429 for the
    /// first `placement_429s` calls before succeeding, and whose
    /// `get_order_status` returns a classified 429 for the first
    /// `status_429s` calls before reporting `Filled`.
    #[derive(Clone)]
    struct BackpressureThenFilledExecutor {
        placement_429s: usize,
        status_429s: usize,
        placement_calls: Arc<AtomicUsize>,
        status_calls: Arc<AtomicUsize>,
    }

    impl BackpressureThenFilledExecutor {
        fn new(placement_429s: usize, status_429s: usize) -> Self {
            Self {
                placement_429s,
                status_429s,
                placement_calls: Arc::new(AtomicUsize::new(0)),
                status_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait::async_trait]
    impl Executor for BackpressureThenFilledExecutor {
        type Error = BackpressureTestError;
        type OrderId = String;
        type Ctx = ();

        async fn try_from_ctx(_ctx: Self::Ctx) -> Result<Self, Self::Error> {
            Ok(Self::new(0, 0))
        }

        async fn is_market_open(&self) -> Result<bool, Self::Error> {
            Ok(true)
        }

        async fn place_market_order(
            &self,
            order: MarketOrder,
        ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
            let call_index = self.placement_calls.fetch_add(1, Ordering::SeqCst);
            if call_index < self.placement_429s {
                return Err(backpressure_429_error());
            }
            Ok(OrderPlacement {
                order_id: "backpressure-order-id".to_string(),
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
            if call_index < self.status_429s {
                return Err(backpressure_429_error());
            }
            Ok(OrderState::Filled {
                executed_at: Utc::now(),
                order_id: ExecutorOrderId::new("backpressure-order-id"),
                shares_filled: positive_shares("10"),
                price: st0x_execution::Usd::new(Float::parse("195.30".to_string()).unwrap()),
            })
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
            _order: LimitOrder,
        ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
            unimplemented!("not exercised by the buy-fill backpressure tests")
        }

        async fn cancel_order(
            &self,
            _order_id: &Self::OrderId,
        ) -> Result<CancellationOutcome, Self::Error> {
            unimplemented!("not exercised by the buy-fill backpressure tests")
        }

        async fn preflight_counter_trade_at_price(
            &self,
            order: MarketOrder,
            _reference_price: Positive<Usd>,
        ) -> Result<CounterTradePreflight, Self::Error> {
            self.preflight_counter_trade(order).await
        }
    }

    #[tokio::test]
    async fn market_buy_retries_placement_after_a_429_then_succeeds() {
        let broker = BackpressureThenFilledExecutor::new(2, 0);
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("10"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        };
        let mut stdout = Vec::new();

        place_market_order_until_filled(&broker, order, &mut stdout)
            .await
            .expect("a classified 429 on placement must be retried, not fail the buy-fill wait");

        assert_eq!(
            broker.placement_calls.load(Ordering::SeqCst),
            3,
            "must retry placement exactly twice after the first two 429s before succeeding"
        );
    }

    #[tokio::test]
    async fn market_buy_retries_status_poll_after_a_429_then_succeeds() {
        let broker = BackpressureThenFilledExecutor::new(0, 2);
        let order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: positive_shares("10"),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::cli(Uuid::new_v4()),
        };
        let mut stdout = Vec::new();

        place_market_order_until_filled(&broker, order, &mut stdout)
            .await
            .expect(
                "a classified 429 on the status poll must be retried, not fail the buy-fill wait",
            );

        assert_eq!(
            broker.status_calls.load(Ordering::SeqCst),
            3,
            "must retry the status poll exactly twice after the first two 429s before succeeding"
        );
    }

    #[tokio::test]
    async fn market_buy_placement_fails_when_429s_exceed_the_retry_budget() {
        let broker =
            BackpressureThenFilledExecutor::new(BACKPRESSURE_RETRY_MAX_ATTEMPTS as usize, 0);
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

        assert_eq!(
            broker.placement_calls.load(Ordering::SeqCst),
            BACKPRESSURE_RETRY_MAX_ATTEMPTS as usize,
            "must stop retrying after exactly the bounded attempt budget"
        );
        let backpressure_error = error
            .downcast_ref::<BackpressureTestError>()
            .expect("the exhausted 429 must surface as the executor's own error type unwrapped");
        assert!(
            matches!(
                backpressure_error.source,
                Some(st0x_execution::AlpacaBrokerApiError::ApiError {
                    status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                    ..
                })
            ),
            "the exhausted 429 must retain its classified source, got: {backpressure_error:?}"
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
    fn write_order_status_displays_filled_quantity() {
        let mut stdout = Vec::new();

        write_order_status(
            &mut stdout,
            OrderState::Filled {
                executed_at: Utc::now(),
                order_id: ExecutorOrderId::new("some-broker-order-id"),
                shares_filled: positive_shares("1.5"),
                price: Usd::new(Float::parse("195.25".to_string()).unwrap()),
            },
        )
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Shares Filled: 1.5"),
            "status output must include the executed quantity, got: {output}"
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

    #[test]
    fn write_order_status_displays_terminal_failure() {
        let mut stdout = Vec::new();

        write_order_status(
            &mut stdout,
            OrderState::Failed {
                failed_at: Utc::now(),
                error_reason: Some("order expired".to_string()),
                shares_filled: None,
                avg_price: None,
                terminality: OrderFailureTerminality::Terminal,
            },
        )
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("TERMINAL"),
            "a Terminal failure must be labeled TERMINAL, got: {output}"
        );
        assert!(
            !output.contains("NOT TERMINAL"),
            "a Terminal failure must not also read NOT TERMINAL, got: {output}"
        );
        assert!(
            output.contains("fresh order is needed"),
            "a Terminal failure must tell the operator to place a fresh order, got: {output}"
        );
    }

    #[test]
    fn write_order_status_displays_not_terminal_failure() {
        let mut stdout = Vec::new();

        write_order_status(
            &mut stdout,
            OrderState::Failed {
                failed_at: Utc::now(),
                error_reason: Some("order suspended".to_string()),
                shares_filled: None,
                avg_price: None,
                terminality: OrderFailureTerminality::NotTerminal,
            },
        )
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("NOT TERMINAL"),
            "a NotTerminal failure must be labeled NOT TERMINAL, got: {output}"
        );
        assert!(
            output.contains("may still resume or fill"),
            "a NotTerminal failure must tell the operator the order may still \
             resolve, got: {output}"
        );
    }

    #[test]
    fn render_process_tx_outcome_covers_every_arm() {
        let tx_hash = TxHash::repeat_byte(0x11);
        let not_found = TxHash::repeat_byte(0x22);
        let symbol = || Symbol::new("MSTR").expect("test symbol must be valid");

        let mut cases: Vec<(ProcessTxOutcome, String)> = vec![
            (
                ProcessTxOutcome::NoTradeableEvents,
                format!("No tradeable events found in transaction {tx_hash}\nThis transaction may not contain orderbook events matching the configured order hash.\n"),
            ),
            (
                ProcessTxOutcome::TransactionNotFound { tx_hash: not_found },
                format!("Transaction not found: {not_found}\n"),
            ),
            (
                ProcessTxOutcome::AlreadyAccounted,
                "Fill is already fully accounted. Nothing to do; the normal pipeline will hedge any unhedged position exposure.\n".to_string(),
            ),
            (
                ProcessTxOutcome::PendingHedgeInFlight,
                "An existing pending hedge is in flight; settled the fill without placing a new hedge.\n".to_string(),
            ),
            (
                ProcessTxOutcome::BelowExecutionThreshold,
                "Trade accumulated but did not trigger execution yet (waiting to accumulate enough shares for a whole share execution).\n".to_string(),
            ),
            (
                ProcessTxOutcome::TradingDisabled { symbol: symbol() },
                format!("Trading disabled by configuration for {}\n", symbol()),
            ),
            (
                ProcessTxOutcome::PlacementRejected { symbol: symbol() },
                format!("Placement for {} was rejected by domain state; a concurrent placement already claimed the position. Settled the fill.\n", symbol()),
            ),
        ];

        for (disposition, disposition_line) in [
            (
                HedgeDisposition::InFlight,
                "Order submitted; it will be reconciled to a terminal state by the order-status recovery sweep on the next bot startup.",
            ),
            (
                HedgeDisposition::ClearedForRetry,
                "Hedge placement failed or the order vanished; pending order cleared so the normal pipeline can re-hedge.",
            ),
            (
                HedgeDisposition::Finalized,
                "The order reached a terminal broker state and the position was finalized.",
            ),
        ] {
            let order_id = OffchainOrderId::new();
            let shares = positive_shares("1.5");
            let expected = format!(
                "Placed {:?} hedge for {shares} {} (order {order_id})\n{disposition_line}\n",
                Direction::Buy,
                symbol()
            );
            cases.push((
                ProcessTxOutcome::HedgePlaced {
                    symbol: symbol(),
                    offchain_order_id: order_id,
                    shares,
                    direction: Direction::Buy,
                    disposition,
                },
                expected,
            ));
        }

        for (outcome, expected) in cases {
            let mut buf = Vec::new();
            render_process_tx_outcome(tx_hash, &outcome, &mut buf).expect("render must succeed");
            assert_eq!(
                String::from_utf8(buf).expect("output must be valid UTF-8"),
                expected,
                "unexpected output for {outcome:?}"
            );
        }
    }
}
