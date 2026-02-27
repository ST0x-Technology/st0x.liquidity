use alloy::primitives::{Address, B256, U256};
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::collections::HashMap;
use tokio::task::JoinHandle;

use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};
use st0x_hedge::bindings::IOrderBookV6;
use st0x_hedge::config::{BrokerCtx, Ctx, LogLevel, OperationalLimits};
use st0x_hedge::{EvmCtx, TradingMode};

use e2e_tests::common::{assert_broker_state, assert_cqrs_state};
use e2e_tests::services::alpaca_broker;
use e2e_tests::services::alpaca_broker::AlpacaBrokerMock;
use e2e_tests::services::base_chain::{self, TakeOrderResult};

pub(crate) use std::time::Duration;

pub(crate) use alloy::providers::Provider;
pub(crate) use st0x_exact_decimal::ExactDecimal;

pub(crate) fn ed(value: &str) -> ExactDecimal {
    match ExactDecimal::parse(value) {
        Ok(val) => val,
        Err(error) => panic!("ed({value:?}) failed: {error}"),
    }
}

pub(crate) use st0x_event_sorcery::Projection;
pub(crate) use st0x_execution::{FractionalShares, Positive, Symbol};
pub(crate) use st0x_hedge::ExecutionThreshold;
pub(crate) use st0x_hedge::{OffchainOrder, Position};

pub(crate) use e2e_tests::common::{
    DEFAULT_POLL_TIMEOUT_SECS, ExpectedPosition, connect_db, count_events,
    poll_for_aggregate_events_containing, poll_for_events, sleep_or_crash, spawn_bot,
    wait_for_processing,
};
pub(crate) use e2e_tests::services::TestInfra;
pub(crate) use e2e_tests::services::base_chain::TakeDirection;

/// Builds a `Ctx` pointing at the given chain, broker, and database path.
#[bon::builder]
pub(crate) fn build_ctx<P: Provider + Clone>(
    chain: &base_chain::BaseChain<P>,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    deployment_block: u64,
) -> anyhow::Result<Ctx> {
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
        api_key: alpaca_broker::TEST_API_KEY.to_owned(),
        api_secret: alpaca_broker::TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    });
    let execution_threshold = broker_ctx.execution_threshold()?;

    Ok(Ctx {
        database_url: db_path.display().to_string(),
        log_level: LogLevel::Debug,
        server_port: 0,
        operational_limits: OperationalLimits::Disabled,
        evm: EvmCtx {
            ws_rpc_url: chain.ws_endpoint()?,
            orderbook: chain.orderbook_addr,
            deployment_block,
        },
        order_polling_interval: 1,
        order_polling_max_jitter: 0,
        position_check_interval: 2,
        inventory_poll_interval: 2,
        broker: broker_ctx,
        telemetry: None,
        trading_mode: TradingMode::Standalone {
            order_owner: chain.owner,
        },
        execution_threshold,
        equities: HashMap::new(),
    })
}

/// Polls until the Position projection for `symbol` has `net == 0`,
/// indicating the position is fully hedged.
pub(crate) async fn poll_for_hedged_position(
    bot: &mut JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    symbol: &str,
) {
    let url = format!("sqlite:{}", db_path.display());
    let timeout = Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS);
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("hedged position for {symbol}");
    let target_symbol = Symbol::force_new(symbol.to_owned());

    loop {
        sleep_or_crash(bot, &context).await;

        let Ok(pool) = SqlitePool::connect(&url).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out after {timeout:?} waiting for {context} (database not ready)",
            );
            continue;
        };

        let is_hedged = {
            let projection = Projection::<Position>::sqlite(pool.clone());
            projection
                .load(&target_symbol)
                .await
                .ok()
                .flatten()
                .is_some_and(|position| position.net == FractionalShares::ZERO)
        };

        pool.close().await;

        if is_hedged {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context}",
        );
    }
}

/// Counts rows in the `event_queue` table.
pub(crate) async fn count_queued_events(pool: &SqlitePool) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_queue")
        .fetch_one(pool)
        .await?;

    Ok(row.0)
}

/// Counts processed rows in the `event_queue` table.
pub(crate) async fn count_processed_queue_events(pool: &SqlitePool) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_queue WHERE processed = 1")
        .fetch_one(pool)
        .await?;

    Ok(row.0)
}

/// Comprehensive end-to-end assertions covering broker state, onchain
/// vaults, and CQRS events/views for one or more symbols.
///
/// This is the primary hedging assertion entry point. Hedging e2e tests
/// should call this after the bot has finished processing.
pub(crate) async fn assert_full_hedging_flow<P: Provider>(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    broker: &AlpacaBrokerMock,
    database_url: &str,
) -> anyhow::Result<()> {
    assert_broker_state(expected_positions, broker);

    for take_result in take_results {
        assert_onchain_vaults(provider, orderbook_addr, owner, take_result).await?;
    }

    assert_cqrs_state(expected_positions, take_results.len(), database_url).await?;

    Ok(())
}

fn vault_token_decimals(token: Address) -> u8 {
    if token == base_chain::USDC_BASE {
        6
    } else {
        18
    }
}

async fn assert_onchain_vaults<P: Provider>(
    provider: &P,
    orderbook_addr: Address,
    owner: Address,
    take_result: &TakeOrderResult,
) -> anyhow::Result<()> {
    let orderbook = IOrderBookV6::IOrderBookV6Instance::new(orderbook_addr, provider);

    let output_balance_now = orderbook
        .vaultBalance2(owner, take_result.output_token, take_result.output_vault_id)
        .call()
        .await?;
    assert_eq!(
        output_balance_now, take_result.output_vault_balance_after_take,
        "Output vault current balance should match recorded post-take balance"
    );

    let output_decimals = vault_token_decimals(take_result.output_token);
    let output_before_units = Float::from_raw(take_result.output_vault_balance_before_take)
        .to_fixed_decimal_lossy(output_decimals)?
        .0;
    let output_after_units = Float::from_raw(take_result.output_vault_balance_after_take)
        .to_fixed_decimal_lossy(output_decimals)?
        .0;
    let expected_output_delta_units =
        Float::from_raw(take_result.output_vault_delta_from_take_event)
            .to_fixed_decimal_lossy(output_decimals)?
            .0;
    let output_delta_units = output_before_units
        .checked_sub(output_after_units)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Output vault should not increase across take (before={}, after={})",
                Float::from_raw(take_result.output_vault_balance_before_take)
                    .format_with_scientific(false)
                    .unwrap_or_else(|_| "???".to_string()),
                Float::from_raw(take_result.output_vault_balance_after_take)
                    .format_with_scientific(false)
                    .unwrap_or_else(|_| "???".to_string())
            )
        })?;
    assert_eq!(
        output_delta_units, expected_output_delta_units,
        "Output vault delta should match TakeOrderV3 fill amount"
    );
    assert_eq!(
        output_after_units,
        U256::ZERO,
        "Output vault should be fully consumed"
    );
    assert_eq!(
        output_balance_now,
        B256::ZERO,
        "Output vault should be fully consumed, got {}",
        Float::from_raw(output_balance_now)
            .format_with_scientific(false)
            .unwrap_or_else(|_| "???".to_string())
    );

    let input_balance_now = orderbook
        .vaultBalance2(owner, take_result.input_token, take_result.input_vault_id)
        .call()
        .await?;
    assert_eq!(
        input_balance_now, take_result.input_vault_balance_after_take,
        "Input vault current balance should match recorded post-take balance"
    );

    let input_decimals = vault_token_decimals(take_result.input_token);
    let input_before_units = Float::from_raw(take_result.input_vault_balance_before_take)
        .to_fixed_decimal_lossy(input_decimals)?
        .0;
    let input_after_units = Float::from_raw(take_result.input_vault_balance_after_take)
        .to_fixed_decimal_lossy(input_decimals)?
        .0;
    let expected_input_delta_units = Float::from_raw(take_result.input_vault_delta_from_take_event)
        .to_fixed_decimal_lossy(input_decimals)?
        .0;
    let input_delta_units = input_after_units
        .checked_sub(input_before_units)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Input vault should not decrease across take (before={}, after={})",
                Float::from_raw(take_result.input_vault_balance_before_take)
                    .format_with_scientific(false)
                    .unwrap_or_else(|_| "???".to_string()),
                Float::from_raw(take_result.input_vault_balance_after_take)
                    .format_with_scientific(false)
                    .unwrap_or_else(|_| "???".to_string())
            )
        })?;
    assert_eq!(
        input_delta_units, expected_input_delta_units,
        "Input vault delta should match TakeOrderV3 fill amount"
    );

    Ok(())
}
