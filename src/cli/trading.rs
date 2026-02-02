//! Trading order execution and transaction processing CLI commands.

use std::io::Write;
use std::sync::Arc;

use alloy::primitives::B256;
use alloy::providers::Provider;
use rust_decimal::Decimal;
use sqlite_es::sqlite_cqrs;
use sqlx::SqlitePool;
use st0x_execution::schwab::SchwabConfig;
use st0x_execution::{
    Direction, Executor, FractionalShares, MarketOrder, MockExecutorConfig, OrderPlacement,
    OrderState, Positive, Symbol, TryIntoExecutor,
};
use tracing::{error, info};

use crate::dual_write::DualWriteContext;
use crate::env::{BrokerConfig, Config};
use crate::error::OnChainError;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::{OnchainTrade, accumulator};
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;

use super::auth::ensure_schwab_authentication;

pub(super) async fn order_status_command<W: Write>(
    stdout: &mut W,
    order_id: &str,
    config: &Config,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔍 Checking order status for ID: {order_id}")?;

    let state = get_broker_order_status(config, pool, order_id, stdout).await?;

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
    config: &Config,
    pool: &SqlitePool,
    order_id: &str,
    stdout: &mut W,
) -> anyhow::Result<OrderState> {
    match &config.broker {
        BrokerConfig::Schwab(schwab_auth) => {
            ensure_schwab_authentication(pool, &config.broker, stdout).await?;
            let schwab_config = SchwabConfig {
                auth: schwab_auth.clone(),
                pool: pool.clone(),
            };
            let broker = schwab_config.try_into_executor().await?;
            Ok(broker.get_order_status(&order_id.to_string()).await?)
        }
        BrokerConfig::AlpacaTradingApi(alpaca_auth) => {
            let broker = alpaca_auth.clone().try_into_executor().await?;
            Ok(broker.get_order_status(&order_id.to_string()).await?)
        }
        BrokerConfig::AlpacaBrokerApi(alpaca_auth) => {
            let broker = alpaca_auth.clone().try_into_executor().await?;
            Ok(broker.get_order_status(&order_id.to_string()).await?)
        }
        BrokerConfig::DryRun => {
            let broker = MockExecutorConfig.try_into_executor().await?;
            Ok(broker.get_order_status(&order_id.to_string()).await?)
        }
    }
}

pub(super) async fn execute_order_with_writers<W: Write>(
    symbol: Symbol,
    quantity: u64,
    direction: Direction,
    config: &Config,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let market_order = MarketOrder {
        symbol: symbol.clone(),
        shares: Positive::new(FractionalShares::new(Decimal::from(quantity)))?,
        direction,
    };

    info!("Created order: symbol={symbol}, direction={direction:?}, quantity={quantity}");

    match execute_broker_order(config, pool, market_order, stdout).await {
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
        Err(e) => {
            error!(
                symbol = %symbol,
                direction = ?direction,
                quantity = quantity,
                error = ?e,
                "Failed to place order"
            );
            writeln!(stdout, "❌ Failed to place order: {e}")?;
            return Err(e);
        }
    }

    Ok(())
}

pub(super) async fn process_tx_with_provider<W: Write, P: Provider + Clone>(
    tx_hash: B256,
    config: &Config,
    pool: &SqlitePool,
    stdout: &mut W,
    provider: &P,
    cache: &SymbolCache,
) -> anyhow::Result<()> {
    let evm = &config.evm;
    let feed_id_cache = FeedIdCache::new();
    let order_owner = config.order_owner()?;

    match OnchainTrade::try_from_tx_hash(tx_hash, provider, cache, evm, &feed_id_cache, order_owner)
        .await
    {
        Ok(Some(onchain_trade)) => {
            process_found_trade(onchain_trade, config, pool, stdout).await?;
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
        Err(OnChainError::Validation(crate::error::TradeValidationError::TransactionNotFound(
            hash,
        ))) => {
            writeln!(stdout, "❌ Transaction not found: {hash}")?;
            writeln!(
                stdout,
                "   Please verify the transaction hash and ensure the RPC endpoint is correct."
            )?;
        }
        Err(e) => {
            writeln!(stdout, "❌ Error processing transaction: {e}")?;
            return Err(e.into());
        }
    }

    Ok(())
}

pub(super) async fn execute_broker_order<W: Write>(
    config: &Config,
    pool: &SqlitePool,
    market_order: MarketOrder,
    stdout: &mut W,
) -> anyhow::Result<OrderPlacement<String>> {
    match &config.broker {
        BrokerConfig::Schwab(schwab_auth) => {
            ensure_schwab_authentication(pool, &config.broker, stdout).await?;
            writeln!(stdout, "🔄 Executing Schwab order...")?;
            let schwab_config = SchwabConfig {
                auth: schwab_auth.clone(),
                pool: pool.clone(),
            };
            let broker = schwab_config.try_into_executor().await?;
            let placement = broker.place_market_order(market_order).await?;
            writeln!(
                stdout,
                "✅ Schwab order placed with ID: {}",
                placement.order_id
            )?;
            Ok(placement)
        }
        BrokerConfig::AlpacaTradingApi(alpaca_auth) => {
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
        BrokerConfig::AlpacaBrokerApi(alpaca_auth) => {
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
        BrokerConfig::DryRun => {
            writeln!(stdout, "🔄 Executing dry-run order...")?;
            let broker = MockExecutorConfig.try_into_executor().await?;
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
    config: &Config,
    pool: &SqlitePool,
    stdout: &mut W,
) -> anyhow::Result<()> {
    display_trade_details(&onchain_trade, stdout)?;

    writeln!(stdout, "🔄 Processing trade with TradeAccumulator...")?;

    let dual_write_context = DualWriteContext::with_threshold(
        pool.clone(),
        Arc::new(sqlite_cqrs(pool.clone(), vec![], ())),
        Arc::new(sqlite_cqrs(pool.clone(), vec![], ())),
        Arc::new(sqlite_cqrs(pool.clone(), vec![], ())),
        config.execution_threshold,
    );

    update_position_aggregate(
        &dual_write_context,
        &onchain_trade,
        config.execution_threshold,
    )
    .await;

    let mut sql_tx = pool.begin().await?;
    let execution = accumulator::process_onchain_trade(
        &mut sql_tx,
        &dual_write_context,
        onchain_trade,
        config.broker.to_supported_executor(),
    )
    .await?;
    sql_tx.commit().await?;

    if let Some(execution) = execution.execution {
        let execution_id = execution
            .id
            .ok_or_else(|| anyhow::anyhow!("OffchainExecution missing ID after accumulation"))?;
        writeln!(
            stdout,
            "✅ Trade triggered execution for {:?} (ID: {execution_id})",
            config.broker.to_supported_executor()
        )?;

        let market_order = MarketOrder {
            symbol: execution.symbol,
            shares: execution.shares,
            direction: execution.direction,
        };

        let placement = execute_broker_order(config, pool, market_order, stdout).await?;

        let submitted_state = OrderState::Submitted {
            order_id: placement.order_id.clone(),
        };

        let mut sql_tx = pool.begin().await?;
        submitted_state
            .store_update(&mut sql_tx, execution_id)
            .await?;
        sql_tx.commit().await?;
        writeln!(stdout, "🎯 Trade processing completed!")?;
    } else {
        writeln!(
            stdout,
            "📊 Trade accumulated but did not trigger execution yet."
        )?;
        writeln!(
            stdout,
            "   (Waiting to accumulate enough shares for a whole share execution)"
        )?;
    }

    Ok(())
}

async fn update_position_aggregate(
    dual_write_context: &DualWriteContext,
    onchain_trade: &OnchainTrade,
    execution_threshold: ExecutionThreshold,
) {
    if let Err(e) = crate::dual_write::initialize_position(
        dual_write_context,
        onchain_trade.symbol.base(),
        execution_threshold,
    )
    .await
    {
        error!(
            symbol = %onchain_trade.symbol.base(),
            execution_threshold = ?execution_threshold,
            tx_hash = %onchain_trade.tx_hash,
            log_index = onchain_trade.log_index,
            error = ?e,
            "Failed to initialize position aggregate"
        );
    }

    if let Err(e) =
        crate::dual_write::acknowledge_onchain_fill(dual_write_context, onchain_trade).await
    {
        error!(
            symbol = %onchain_trade.symbol.base(),
            execution_threshold = ?execution_threshold,
            tx_hash = %onchain_trade.tx_hash,
            log_index = onchain_trade.log_index,
            block_timestamp = ?onchain_trade.block_timestamp,
            error = ?e,
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
    use st0x_execution::schwab::SchwabAuthConfig;

    use super::*;
    use crate::env::LogLevel;
    use crate::onchain::EvmConfig;
    use crate::test_utils::{setup_test_db, setup_test_tokens};
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    fn create_schwab_test_config(mock_server: &MockServer) -> Config {
        Config {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmConfig {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Some(Address::ZERO),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::Schwab(SchwabAuthConfig {
                app_key: "test_app_key".to_string(),
                app_secret: "test_app_secret".to_string(),
                redirect_uri: Some(url::Url::parse("https://127.0.0.1").expect("valid test URL")),
                base_url: Some(url::Url::parse(&mock_server.base_url()).expect("valid mock URL")),
                account_index: Some(0),
                encryption_key: TEST_ENCRYPTION_KEY,
            }),
            hyperdx: None,
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn get_schwab_auth(config: &Config) -> &SchwabAuthConfig {
        match &config.broker {
            BrokerConfig::Schwab(auth) => auth,
            _ => panic!("Expected Schwab broker config"),
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
        let config = create_schwab_test_config(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&config)).await;

        let (account_mock, order_mock) = setup_schwab_order_mocks(&server);

        execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &config,
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
        let config = create_schwab_test_config(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&config)).await;

        let (account_mock, order_mock) = setup_schwab_order_mocks(&server);

        execute_order_with_writers(
            Symbol::new("TSLA").unwrap(),
            50,
            Direction::Sell,
            &config,
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
        let config = create_schwab_test_config(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&config)).await;

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

        let result = execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &config,
            &pool,
            &mut std::io::sink(),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_order_stdout_contains_details() {
        let server = MockServer::start();
        let config = create_schwab_test_config(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&config)).await;

        setup_schwab_order_mocks(&server);

        let mut stdout_buffer = Vec::new();
        execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &config,
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
        let config = create_schwab_test_config(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, get_schwab_auth(&config)).await;

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
        let result = execute_order_with_writers(
            Symbol::new("AAPL").unwrap(),
            100,
            Direction::Buy,
            &config,
            &pool,
            &mut stdout_buffer,
        )
        .await;

        assert!(result.is_err());
        let output = String::from_utf8(stdout_buffer).unwrap();
        assert!(
            output.contains("❌ Failed to place order"),
            "Expected failure message, got: {output}"
        );
    }
}
