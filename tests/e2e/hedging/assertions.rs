use alloy::primitives::{Address, B256, U256};
pub(crate) use alloy::providers::Provider;
use rain_math_float::Float;
use sqlx::SqlitePool;
pub(crate) use std::time::Duration;
use tokio::task::JoinHandle;

pub(crate) use st0x_event_sorcery::Projection;
use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, TEST_API_KEY, TEST_API_SECRET};
use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};
pub(crate) use st0x_execution::{FractionalShares, Positive, Symbol};
pub(crate) use st0x_hedge::ExecutionThreshold;
use st0x_hedge::TradingMode;
use st0x_hedge::bindings::IOrderBookV6;
use st0x_hedge::config::{AssetsConfig, BrokerCtx, Ctx};
pub(crate) use st0x_hedge::{OffchainOrder, Position};

pub(crate) use crate::assert::ExpectedPosition;
use crate::assert::{assert_broker_state, assert_cqrs_state};
pub(crate) use crate::base_chain::TakeDirection;
use crate::base_chain::{self, TakeOrderResult};
pub(crate) use crate::poll::{
    DEFAULT_POLL_TIMEOUT_SECS, connect_db, count_done_jobs, count_events, count_jobs,
    poll_for_aggregate_events_containing, poll_for_events, sleep_or_crash, spawn_bot,
    wait_for_processing,
};
pub(crate) use crate::test_infra::TestInfra;

/// Builds a `Ctx` pointing at the given chain, broker, and database path.
#[bon::builder]
pub(crate) fn build_ctx<P: Provider + Clone>(
    chain: &base_chain::BaseChain<P>,
    broker: &AlpacaBrokerMock,
    db_path: &std::path::Path,
    deployment_block: u64,
    assets: AssetsConfig,
    execution_threshold_override: Option<st0x_hedge::ExecutionThreshold>,
) -> anyhow::Result<Ctx> {
    let broker_ctx = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
        api_key: TEST_API_KEY.to_owned(),
        api_secret: TEST_API_SECRET.to_owned(),
        account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
        mode: Some(AlpacaBrokerApiMode::Mock(broker.base_url())),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: TimeInForce::Day,
    });

    Ctx::for_test()
        .database_url(db_path.display().to_string())
        .ws_rpc_url(chain.ws_endpoint()?)
        .orderbook(chain.orderbook)
        .deployment_block(deployment_block)
        .broker(broker_ctx)
        .trading_mode(TradingMode::Standalone {
            order_owner: chain.owner,
        })
        .assets(assets)
        .maybe_execution_threshold_override(execution_threshold_override)
        .call()
        .map_err(Into::into)
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
    let target_symbol = Symbol::new(symbol.to_owned()).unwrap();

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
            match projection.load(&target_symbol).await {
                Ok(maybe_position) => {
                    maybe_position.is_some_and(|position| position.net == FractionalShares::ZERO)
                }
                Err(load_error) => panic!("failed to load Position projection: {load_error}"),
            }
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

/// Comprehensive end-to-end assertions covering broker state, onchain
/// vaults, and CQRS events/views for one or more symbols.
///
/// This is the primary hedging assertion entry point. Hedging e2e tests
/// should call this after the bot has finished processing.
pub(crate) async fn assert_full_hedging_flow<P: Provider>(
    expected_positions: &[ExpectedPosition],
    take_results: &[TakeOrderResult],
    provider: &P,
    orderbook: Address,
    owner: Address,
    broker: &AlpacaBrokerMock,
    database_url: &str,
) -> anyhow::Result<()> {
    assert_broker_state(expected_positions, broker);

    for take_result in take_results {
        assert_onchain_vaults(provider, orderbook, owner, take_result).await?;
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
    orderbook: Address,
    owner: Address,
    take_result: &TakeOrderResult,
) -> anyhow::Result<()> {
    let fmt = |raw: B256| -> String {
        Float::from_raw(raw)
            .format_with_scientific(false)
            .unwrap_or_else(|_| format!("{raw}"))
    };

    let orderbook = IOrderBookV6::IOrderBookV6Instance::new(orderbook, provider);

    let output_balance_now = orderbook
        .vaultBalance2(owner, take_result.output_token, take_result.output_vault_id)
        .call()
        .await?;

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
                fmt(take_result.output_vault_balance_before_take),
                fmt(take_result.output_vault_balance_after_take),
            )
        })?;

    assert_eq!(
        output_delta_units, expected_output_delta_units,
        "Output vault delta should match TakeOrderV3 fill amount \
         (delta={output_delta_units}, expected={expected_output_delta_units})",
    );

    // Dedicated vault (entire balance was the order's deposit): should be
    // fully consumed. Shared vault (pre-existing balance): only the delta
    // matters, verified above.
    let was_dedicated_vault = output_before_units == expected_output_delta_units;
    if was_dedicated_vault {
        assert_eq!(
            output_balance_now,
            B256::ZERO,
            "Dedicated output vault should be fully consumed, got {}",
            fmt(output_balance_now),
        );
    } else {
        assert!(
            output_balance_now <= take_result.output_vault_balance_before_take,
            "Shared output vault balance should not exceed pre-take value \
             (now={}, before={})",
            fmt(output_balance_now),
            fmt(take_result.output_vault_balance_before_take),
        );
    }

    // Input vault may have been modified by concurrent operations (e.g. USDC
    // rebalancer withdrawing from a shared vault). Verify the balance hasn't
    // grown beyond the recorded post-take value — concurrent withdrawals are
    // expected, concurrent deposits are not.
    let input_balance_now = orderbook
        .vaultBalance2(owner, take_result.input_token, take_result.input_vault_id)
        .call()
        .await?;

    assert!(
        input_balance_now <= take_result.input_vault_balance_after_take,
        "Input vault balance should not exceed recorded post-take value \
         (now={}, recorded={}). Unexpected deposit into vault?",
        fmt(input_balance_now),
        fmt(take_result.input_vault_balance_after_take),
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
                fmt(take_result.input_vault_balance_before_take),
                fmt(take_result.input_vault_balance_after_take),
            )
        })?;

    assert_eq!(
        input_delta_units, expected_input_delta_units,
        "Input vault delta should match TakeOrderV3 fill amount"
    );

    Ok(())
}
