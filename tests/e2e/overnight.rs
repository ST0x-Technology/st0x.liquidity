//! E2E overnight hedging scenarios (RAI-1954).
//!
//! Every test lands the session clock inside an overnight session through
//! `AlpacaBrokerApiMode::MockAt` (ADR 0021) and anchors its calendar
//! fixtures to the same effective date. Quote and order timestamps stay on
//! the real clock per the two-clock coherence rule. The mock's overnight
//! contract enforcement is switched on in every fill scenario, so a placed
//! order being *accepted* is itself proof the executor sent a limit day
//! order with `extended_hours: true`.

use chrono::NaiveDate;
use serde_json::json;
use st0x_config::{AssetsConfig, OperationMode};
use st0x_execution::Symbol;
use st0x_float_macro::float;

use crate::hedging::assertions::*;
use crate::poll::poll_for_events;
use crate::test_infra::{TestInfra, clock_offset_secs_to_et};
use base_chain::TakeDirection;
use st0x_execution::alpaca_broker_api::overnight_weeknight_calendar;
use std::time::Duration;

use crate::base_chain;

/// Tuesday, an ordinary weeknight whose next calendar day trades.
fn effective_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 9, 8).expect("fixed fixture date")
}

fn overnight_enabled(mut assets: AssetsConfig) -> AssetsConfig {
    for config in assets.equities.symbols.values_mut() {
        config.overnight_counter_trading = OperationMode::Enabled;
    }
    assets
}

/// Anchors the broker mock inside the effective overnight evening:
/// weeknight calendar for the effective date, a fresh (real-clock)
/// indicative quote around the fill price, and the contract enforcement
/// that makes acceptance prove the overnight order shape.
fn arm_overnight_mock(
    infra: &TestInfra<impl alloy::providers::Provider + Clone>,
    symbol: &str,
    bid: &str,
    ask: &str,
) -> i64 {
    let date = effective_date();
    infra
        .broker_service
        .set_calendar_entries(overnight_weeknight_calendar(date));
    infra.broker_service.set_overnight_quote(
        Symbol::new(symbol).expect("test symbol"),
        json!({
            "t": chrono::Utc::now().to_rfc3339(),
            "bp": bid,
            "ap": ask,
        }),
    );
    infra
        .broker_service
        .set_overnight_contract_enforcement(true);

    clock_offset_secs_to_et(date, 21, 0)
}

/// Overnight sell fill with a fractional quantity: an onchain buy of
/// equity accumulates a long that the bot hedges with a fractional
/// overnight limit sell, filled and reconciled exactly once.
#[test_log::test(tokio::test)]
async fn overnight_sell_fill_fractional() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let buy_amount = float!(12.5);

    let infra = TestInfra::start(
        vec![(equity_symbol, broker_fill_price)],
        vec![(equity_symbol, float!(100.0))],
    )
    .await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");

    let expected_position = ExpectedPosition::builder()
        .symbol(equity_symbol)
        .amount(buy_amount)
        .direction(TakeDirection::BuyEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(buy_amount)
        .expected_accumulated_short(float!(0))
        .expected_net(float!(0))
        .build();

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(overnight_enabled(infra.assets_config()))
        .session_clock_offset_secs(offset)
        .overnight_knobs(true)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(buy_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_full_hedging_flow(
        &[expected_position],
        &[take_result],
        &infra.base_chain.provider,
        infra.base_chain.orderbook,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot.abort();
    Ok(())
}

/// Overnight buy fill with a whole quantity: an onchain sell of equity
/// accumulates a short hedged by an overnight limit buy priced from the
/// crossed indicative ask.
#[test_log::test(tokio::test)]
async fn overnight_buy_fill_whole() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");

    let expected_position = ExpectedPosition::builder()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(0))
        .expected_accumulated_short(sell_amount)
        .expected_net(float!(0))
        .build();

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(overnight_enabled(infra.assets_config()))
        .session_clock_offset_secs(offset)
        .overnight_knobs(true)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_full_hedging_flow(
        &[expected_position],
        &[take_result],
        &infra.base_chain.provider,
        infra.base_chain.orderbook,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot.abort();
    Ok(())
}

/// An asset the feed marks not overnight-tradable defers with no broker
/// call: the onchain trade is accounted, the position stands, and no order
/// of any kind reaches the mock.
#[test_log::test(tokio::test)]
async fn overnight_ineligible_asset_defers_without_a_broker_call() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let infra = TestInfra::start(vec![(equity_symbol, float!(150.25))], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");

    // Attributes without "overnight_tradable": the startup sync snapshots
    // the asset as ineligible for overnight.
    infra.broker_service.set_asset_payload(
        Symbol::new(equity_symbol)?,
        json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "symbol": equity_symbol,
            "status": "active",
            "tradable": true,
            "fractionable": true,
            "attributes": ["fractional_eh_enabled"],
        }),
    );

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(overnight_enabled(infra.assets_config()))
        .session_clock_offset_secs(offset)
        .overnight_knobs(true)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(float!(5.0))
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Filled", 1).await;

    // Several scan intervals: every one must defer with no broker call.
    tokio::time::sleep(Duration::from_secs(7)).await;

    assert!(
        infra.broker_service.orders().is_empty(),
        "an overnight-ineligible asset must never reach the broker"
    );

    bot.abort();
    Ok(())
}

/// A halted asset defers; after the halt lifts and the bot restarts (the
/// startup eligibility sync reads the updated attributes -- scheduled
/// re-syncs run at 19:45 ET real time, unreachable in a test), the standing
/// exposure hedges and fills. The restart doubles as crash-resume coverage:
/// the deferred position survives the process boundary and is hedged
/// exactly once.
#[test_log::test(tokio::test)]
async fn overnight_halted_asset_hedges_after_the_halt_lifts_across_restart() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");

    infra.broker_service.set_asset_payload(
        Symbol::new(equity_symbol)?,
        json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "symbol": equity_symbol,
            "status": "active",
            "tradable": true,
            "fractionable": true,
            "attributes": [
                "fractional_eh_enabled",
                "overnight_tradable",
                "overnight_halted",
            ],
        }),
    );

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let build = |infra: &TestInfra<_>| {
        build_ctx()
            .chain(&infra.base_chain)
            .broker(&infra.broker_service)
            .db_path(&infra.db_path)
            .deployment_block(current_block)
            .assets(overnight_enabled(infra.assets_config()))
            .session_clock_offset_secs(offset)
            .overnight_knobs(true)
            .call()
    };
    let mut bot = spawn_bot(build(&infra)?);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Filled", 1).await;
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(
        infra.broker_service.orders().is_empty(),
        "a halted asset must defer with no broker call"
    );

    // The halt lifts; only a restart's startup sync can observe it.
    bot.abort();
    infra.broker_service.set_asset_payload(
        Symbol::new(equity_symbol)?,
        json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "symbol": equity_symbol,
            "status": "active",
            "tradable": true,
            "fractionable": true,
            "attributes": ["fractional_eh_enabled", "overnight_tradable"],
        }),
    );

    let mut bot = spawn_bot(build(&infra)?);
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    let expected_position = ExpectedPosition::builder()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(0))
        .expected_accumulated_short(sell_amount)
        .expected_net(float!(0))
        .build();
    assert_full_hedging_flow(
        &[expected_position],
        &[take_result],
        &infra.base_chain.provider,
        infra.base_chain.orderbook,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot.abort();
    Ok(())
}
