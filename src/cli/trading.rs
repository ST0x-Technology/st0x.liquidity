//! Trading order execution and transaction processing CLI commands.

use alloy::primitives::TxHash;
use alloy::providers::Provider;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::persist::GenericQuery;
use rust_decimal::Decimal;
use sqlite_es::{SqliteCqrs, SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use tracing::{error, info};

use st0x_execution::{
    ArithmeticError, Direction, Executor, ExecutorOrderId, FractionalShares, MarketOrder,
    MockExecutorCtx, OrderPlacement, OrderState, Positive, Symbol, TryIntoExecutor,
};

use super::auth::ensure_schwab_authentication;
use crate::config::{BrokerCtx, Ctx};
use crate::lifecycle::Lifecycle;
use crate::offchain_order::{
    OffchainOrder, OffchainOrderCommand, OffchainOrderId, OrderPlacer, build_offchain_order_cqrs,
};
use crate::onchain::accumulator::check_execution_readiness;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::{OnChainError, OnchainTrade, TradeValidationError};
use crate::position::{Position, PositionCommand, TradeId};
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;

/// OrderPlacer for the CLI that delegates to the broker-specific executor
/// constructed from config. Handles Schwab auth, symbol mapping, etc.
struct CliOrderPlacer {
    ctx: Ctx,
    pool: SqlitePool,
}

#[async_trait]
impl OrderPlacer for CliOrderPlacer {
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<ExecutorOrderId, Box<dyn std::error::Error + Send + Sync>> {
        let placement =
            execute_broker_order(&self.ctx, &self.pool, order, &mut std::io::sink()).await?;
        Ok(ExecutorOrderId::new(&placement.order_id))
    }
}

pub(super) fn create_order_placer(ctx: &Ctx, pool: &SqlitePool) -> Arc<dyn OrderPlacer> {
    Arc::new(CliOrderPlacer {
        ctx: ctx.clone(),
        pool: pool.clone(),
    })
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
            price_cents,
        } => {
            let dollars = price_cents / 100;
            let cents = price_cents % 100;
            writeln!(stdout, "✅ Order Status: FILLED")?;
            writeln!(stdout, "   Order ID: {order_id}")?;
            writeln!(stdout, "   Executed At: {executed_at}")?;
            writeln!(stdout, "   Fill Price: ${dollars}.{cents:02}")?;
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
    pool: &SqlitePool,
    order_id: &str,
    stdout: &mut W,
) -> anyhow::Result<OrderState> {
    match &ctx.broker {
        BrokerCtx::Schwab(schwab_auth) => {
            ensure_schwab_authentication(pool, &ctx.broker, stdout).await?;
            let schwab_ctx = schwab_auth.to_schwab_ctx(pool.clone());
            let broker = schwab_ctx.try_into_executor().await?;
            Ok(broker.get_order_status(&order_id.to_string()).await?)
        }
        BrokerCtx::AlpacaTradingApi(alpaca_auth) => {
            let broker = alpaca_auth.clone().try_into_executor().await?;
            Ok(broker.get_order_status(&order_id.to_string()).await?)
        }
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
    symbol: Symbol,
    quantity: u64,
    direction: Direction,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let market_order = MarketOrder {
        symbol: symbol.clone(),
        shares: Positive::new(FractionalShares::new(Decimal::from(quantity)))?,
        direction,
    };

    info!("Created order: symbol={symbol}, direction={direction:?}, quantity={quantity}");

    match execute_broker_order(ctx, pool, market_order, stdout).await {
        Ok(placement) => {
            info!(
                symbol = %symbol,
                direction = ?direction,
                quantity = quantity,
                order_id = %placement.order_id,
                "Order placed successfully"
            );
            writeln!(stdout, "✅ Order placed successfully")?;
            writeln!(stdout, "   Symbol: {symbol}")?;
            writeln!(stdout, "   Action: {direction:?}")?;
            writeln!(stdout, "   Quantity: {quantity}")?;
        }
        Err(error) => {
            error!(
                symbol = %symbol,
                direction = ?direction,
                quantity = quantity,
                error = ?error,
                "Failed to place order"
            );
            writeln!(stdout, "❌ Failed to place order: {error}")?;
            return Err(error);
        }
    }

    Ok(())
}

pub(super) async fn process_tx_with_provider<W: Write, P: Provider + Clone>(
    tx_hash: TxHash,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
    provider: &P,
    cache: &SymbolCache,
    order_placer: Arc<dyn OrderPlacer>,
) -> anyhow::Result<()> {
    let evm = &ctx.evm;
    let feed_id_cache = FeedIdCache::new();
    let order_owner = ctx.order_owner()?;

    match OnchainTrade::try_from_tx_hash(tx_hash, provider, cache, evm, &feed_id_cache, order_owner)
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
    pool: &SqlitePool,
    market_order: MarketOrder,
    stdout: &mut W,
) -> anyhow::Result<OrderPlacement<String>> {
    match &ctx.broker {
        BrokerCtx::Schwab(schwab_auth) => {
            ensure_schwab_authentication(pool, &ctx.broker, stdout).await?;
            writeln!(stdout, "🔄 Executing Schwab order...")?;
            let schwab_ctx = schwab_auth.to_schwab_ctx(pool.clone());
            let broker = schwab_ctx.try_into_executor().await?;
            let placement = broker.place_market_order(market_order).await?;
            writeln!(
                stdout,
                "✅ Schwab order placed with ID: {}",
                placement.order_id
            )?;
            Ok(placement)
        }
        BrokerCtx::AlpacaTradingApi(alpaca_auth) => {
            writeln!(stdout, "🔄 Executing Alpaca Trading API order...")?;
            let broker = alpaca_auth.clone().try_into_executor().await?;
            let placement = broker.place_market_order(market_order).await?;
            writeln!(
                stdout,
                "✅ Alpaca Trading API order placed with ID: {}",
                placement.order_id
            )?;
            Ok(placement)
        }
        BrokerCtx::AlpacaBrokerApi(alpaca_auth) => {
            writeln!(stdout, "🔄 Executing Alpaca Broker API order...")?;
            let broker = alpaca_auth.clone().try_into_executor().await?;
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

pub(super) async fn process_found_trade<W: Write>(
    onchain_trade: OnchainTrade,
    ctx: &Ctx,
    pool: &SqlitePool,
    stdout: &mut W,
    order_placer: Arc<dyn OrderPlacer>,
) -> anyhow::Result<()> {
    display_trade_details(&onchain_trade, stdout)?;

    writeln!(stdout, "🔄 Processing trade with TradeAccumulator...")?;

    let position_view_repo = Arc::new(SqliteViewRepository::new(
        pool.clone(),
        "position_view".to_string(),
    ));
    let position_query = GenericQuery::new(position_view_repo.clone());
    let position_cqrs: Arc<SqliteCqrs<Lifecycle<Position>>> =
        Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![Box::new(GenericQuery::new(position_view_repo))],
            (),
        ));
    let (offchain_order_cqrs, _) = build_offchain_order_cqrs(pool, order_placer);

    update_position_aggregate(&position_cqrs, &onchain_trade, ctx.execution_threshold).await;

    let executor_type = ctx.broker.to_supported_executor();
    let base_symbol = onchain_trade.symbol.base();

    let Some(params) =
        check_execution_readiness(&position_query, base_symbol, executor_type).await?
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

    let aggregate_id = params.symbol.to_string();
    if let Err(error) = position_cqrs
        .execute(
            &aggregate_id,
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

    let agg_id = offchain_order_id.to_string();

    if let Err(error) = offchain_order_cqrs
        .execute(
            &agg_id,
            OffchainOrderCommand::Place {
                symbol: params.symbol.clone(),
                shares: params.shares,
                direction: params.direction,
                executor: params.executor,
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
    position_cqrs: &SqliteCqrs<Lifecycle<Position, ArithmeticError<FractionalShares>>>,
    onchain_trade: &OnchainTrade,
    execution_threshold: ExecutionThreshold,
) {
    let base_symbol = onchain_trade.symbol.base();
    let aggregate_id = Position::aggregate_id(base_symbol);

    acknowledge_fill(
        position_cqrs,
        &aggregate_id,
        onchain_trade,
        execution_threshold,
    )
    .await;
}

fn extract_fill_params(
    onchain_trade: &OnchainTrade,
) -> Option<(FractionalShares, Decimal, DateTime<Utc>)> {
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
    position_cqrs: &SqliteCqrs<Lifecycle<Position, ArithmeticError<FractionalShares>>>,
    aggregate_id: &str,
    onchain_trade: &OnchainTrade,
    execution_threshold: ExecutionThreshold,
) {
    let base_symbol = onchain_trade.symbol.base();

    let Some((amount, price_usdc, block_timestamp)) = extract_fill_params(onchain_trade) else {
        return;
    };

    if let Err(error) = position_cqrs
        .execute(
            aggregate_id,
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
    let ticker = onchain_trade.symbol.extract_base();

    writeln!(stdout, "✅ Found opposite-side trade opportunity:")?;
    writeln!(stdout, "   Transaction: {}", onchain_trade.tx_hash)?;
    writeln!(stdout, "   Log Index: {}", onchain_trade.log_index)?;
    writeln!(stdout, "   Symbol: {ticker}")?;
    writeln!(stdout, "   Direction: {:?}", onchain_trade.direction)?;
    writeln!(stdout, "   Quantity: {}", onchain_trade.amount)?;
    writeln!(
        stdout,
        "   Price per Share: ${:.2}",
        onchain_trade.price.value()
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, FixedBytes, address};
    use httpmock::MockServer;
    use serde_json::json;
    use url::Url;

    use super::*;
    use crate::config::{LogLevel, SchwabAuth};
    use crate::onchain::EvmCtx;
    use crate::test_utils::{setup_test_db, setup_test_tokens};
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    fn create_schwab_test_ctx(mock_server: &MockServer) -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Some(Address::ZERO),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerCtx::Schwab(SchwabAuth {
                app_key: "test_app_key".to_string(),
                app_secret: "test_app_secret".to_string(),
                redirect_uri: Some(Url::parse("https://127.0.0.1").expect("valid test URL")),
                base_url: Some(Url::parse(&mock_server.base_url()).expect("valid mock URL")),
                account_index: Some(0),
                encryption_key: TEST_ENCRYPTION_KEY,
            }),
            telemetry: None,
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn get_schwab_auth(ctx: &Ctx) -> &SchwabAuth {
        match &ctx.broker {
            BrokerCtx::Schwab(auth) => auth,
            _ => panic!("Expected Schwab broker ctx"),
        }
    }

    fn setup_schwab_order_mocks(server: &MockServer) -> (httpmock::Mock<'_>, httpmock::Mock<'_>) {
        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer test_access_token");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        (account_mock, order_mock)
    }

    #[tokio::test]
    async fn test_execute_order_buy_success() {
        let server = MockServer::start();
        let ctx = create_schwab_test_ctx(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&ctx)).await;

        let (account_mock, order_mock) = setup_schwab_order_mocks(&server);

        execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap();

        account_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_order_sell_success() {
        let server = MockServer::start();
        let ctx = create_schwab_test_ctx(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&ctx)).await;

        let (account_mock, order_mock) = setup_schwab_order_mocks(&server);

        execute_order_with_writers(
            Symbol::new("TSLA").unwrap(),
            50,
            Direction::Sell,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap();

        account_mock.assert();
        order_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_order_api_failure() {
        let server = MockServer::start();
        let ctx = create_schwab_test_ctx(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&ctx)).await;

        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "Invalid order"}));
        });

        execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut std::io::sink(),
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn test_execute_order_stdout_contains_details() {
        let server = MockServer::start();
        let ctx = create_schwab_test_ctx(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&ctx)).await;

        setup_schwab_order_mocks(&server);

        let mut stdout_buffer = Vec::new();
        execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &ctx,
            &pool,
            &mut stdout_buffer,
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(output.contains("AAPL"), "Output should contain symbol");
        assert!(output.contains("100"), "Output should contain quantity");
    }

    #[tokio::test]
    async fn test_execute_order_failure_stdout_contains_error() {
        let server = MockServer::start();
        let ctx = create_schwab_test_ctx(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&ctx)).await;

        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({"error": "Insufficient funds"}));
        });

        let mut stdout_buffer = Vec::new();
        execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
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
}
