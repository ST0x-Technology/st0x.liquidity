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
use rain_math_float::Float;
use serde_json::json;
use st0x_config::{AssetsConfig, OperationMode};
use st0x_execution::Symbol;
use st0x_execution::alpaca_broker_api::OrderStatus;
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

/// Fault-scenario closing assertion. A recovered fault legitimately leaves
/// non-filled siblings behind (a cancelled original, a rejected first
/// attempt), which the strict `assert_full_hedging_flow` refuses -- it
/// demands the pristine single-order success shape. This asserts what a
/// recovery actually promises: the position ends fully hedged, exactly one
/// broker order filled, and it filled at the mock's configured price.
async fn assert_fault_recovery(
    infra: &TestInfra<impl alloy::providers::Provider + Clone>,
    bot: &tokio::task::JoinHandle<anyhow::Result<()>>,
    symbol: &str,
    broker_fill_price: Float,
) {
    poll_for_dust_hedged_position(bot, &infra.db_path, symbol).await;

    let filled: Vec<_> = infra
        .broker_service
        .orders()
        .into_iter()
        .filter(|order| matches!(order.status, OrderStatus::Filled))
        .collect();
    assert_eq!(
        filled.len(),
        1,
        "a recovered fault must produce exactly one broker fill"
    );
    let fill_price = filled[0]
        .filled_price
        .expect("a filled order carries its fill price");
    assert!(
        fill_price.eq(broker_fill_price).expect("comparable prices"),
        "the fill must land at the mock's configured price"
    );
}

/// Like `poll_for_hedged_position`, but tolerating the sub-precision
/// remainder a 9dp-quantized hedge legitimately leaves on an 18dp onchain
/// position -- the issue's "one broker-executable remainder": the residual
/// is NOT broker-executable, so `net == 0` exactly is unreachable and the
/// hedged end state is |net| within the per-order truncation epsilon
/// (the same tolerance `assert_decimal_eq!` applies in `assert.rs`).
async fn poll_for_dust_hedged_position(
    bot: &tokio::task::JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    symbol: &str,
) {
    let epsilon = Float::parse("0.000000002".to_string()).expect("two orders' truncation dust");
    let negative_epsilon = (-epsilon).expect("negatable epsilon");
    let url = format!("sqlite:{}", db_path.display());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let target_symbol = Symbol::new(symbol).expect("test symbol");

    loop {
        assert!(
            !bot.is_finished(),
            "bot crashed while waiting for the dust-hedged position"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;

        let Ok(pool) = sqlx::SqlitePool::connect(&url).await else {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for the database"
            );
            continue;
        };
        let hedged = Projection::<Position>::sqlite(pool)
            .load(&target_symbol)
            .await
            .expect("Position projection loads")
            .is_some_and(|position| {
                let net = position.net.inner();
                net.lte(epsilon).expect("comparable")
                    && net.gte(negative_epsilon).expect("comparable")
            });
        if hedged {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {symbol} to hedge to within truncation dust"
        );
    }
}

/// The 1952 reprice cycle through real queues: an unfilled overnight limit
/// is cancelled on the `overnight_reprice_timeout_secs` cadence
/// (`OvernightRepriceTimeout`), the poller confirms through the mock's real
/// cancel route, the released exposure re-places from a fresh quote, and
/// the replacement fills. Exactly one final fill; the cancelled original
/// retains no phantom execution.
#[test_log::test(tokio::test)]
async fn overnight_reprice_cycle_cancels_and_replaces_an_unfilled_limit() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    // Effectively never fills: the first order must outlive the 3s reprice
    // timeout and be cancelled unfilled. The delay drops to zero once the
    // cancellation is observed -- a static delay shorter than the cadence
    // would fill before the reprice, and one longer would cancel every
    // replacement too, looping forever.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 999);

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(overnight_enabled(infra.assets_config()))
        .session_clock_offset_secs(offset)
        .overnight_knobs(true)
        .overnight_reprice_secs_override(3)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // The cycle: first order cancelled on the cadence...
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Cancelled", 1).await;
    // ...then the replacement fills promptly (inside its own cadence).
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_fault_recovery(&infra, &bot, equity_symbol, broker_fill_price).await;

    bot.abort();
    Ok(())
}

/// A broker rejection releases the position exactly once, and the next scan
/// re-hedges it to a fill: one Failed order, one Filled order, one final
/// reconciliation.
#[test_log::test(tokio::test)]
async fn overnight_rejection_releases_once_and_rehedges() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    infra
        .broker_service
        .set_mode(st0x_execution::alpaca_broker_api::MockMode::OrderRejected);

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
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // The rejection lands as a terminal failure and releases the claim.
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Failed", 1).await;

    // The broker recovers; the next scan re-hedges to a fill.
    infra
        .broker_service
        .set_mode(st0x_execution::alpaca_broker_api::MockMode::HappyPath);
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_fault_recovery(&infra, &bot, equity_symbol, broker_fill_price).await;

    bot.abort();
    Ok(())
}

/// The overnight feed answers 429 for a few requests: every affected scan
/// defers (no placement against a missing reference), and the hedge lands
/// once the feed recovers -- backpressure waits, it never dead-letters.
#[test_log::test(tokio::test)]
async fn overnight_feed_backpressure_defers_then_recovers() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    infra.broker_service.set_overnight_quote_failures(3, 429);

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

/// A placement whose response is lost after the broker recorded the order:
/// the mock records, answers 503, and the redrive's identical
/// `client_order_id` hits the duplicate 422 -- the executor adopts the
/// existing broker order instead of placing a second one. One broker
/// order, one fill, one reconciliation.
#[test_log::test(tokio::test)]
async fn overnight_lost_placement_response_adopts_by_client_order_id() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    infra.broker_service.set_transient_placement_failures(1);

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
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_eq!(
        infra.broker_service.orders().len(),
        1,
        "the redrive must adopt the recorded order, never place a second one"
    );

    assert_fault_recovery(&infra, &bot, equity_symbol, broker_fill_price).await;

    bot.abort();
    Ok(())
}

/// The consecutive-sell race: the reprice cycle cancels a live overnight
/// sell, the venue's net-short guard clears LATER than the terminal status
/// the poller read, and the replacement sell 422s with "open closing
/// position orders". The `ConsecutiveSellPending` classification must treat
/// it as transient and retry to success -- one final fill, no duplicate
/// exposure, no dead-letter.
#[test_log::test(tokio::test)]
async fn overnight_replacement_sell_survives_the_net_short_guard_race() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let buy_amount = float!(3.0);

    let infra = TestInfra::start(
        vec![(equity_symbol, broker_fill_price)],
        vec![(equity_symbol, float!(100.0))],
    )
    .await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    // Never fills until the race is set up (dropped to zero after the
    // cancellation, same reasoning as the reprice-cycle scenario).
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 999);
    infra.broker_service.set_consecutive_sell_guard(true);
    infra
        .broker_service
        .set_consecutive_sell_guard_lag_attempts(1);

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(overnight_enabled(infra.assets_config()))
        .session_clock_offset_secs(offset)
        .overnight_knobs(true)
        .overnight_reprice_secs_override(3)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(buy_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    // Reprice cancels the first sell; the replacement races the lagging
    // guard, retries, and fills.
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Cancelled", 1).await;
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_fault_recovery(&infra, &bot, equity_symbol, broker_fill_price).await;

    bot.abort();
    Ok(())
}

/// Overnight-enabled assets that also enable extended hours, for the
/// session-transition restart where the relaunched bot must re-hedge
/// through the extended chain.
fn both_sessions_enabled(mut assets: AssetsConfig) -> AssetsConfig {
    for config in assets.equities.symbols.values_mut() {
        config.overnight_counter_trading = OperationMode::Enabled;
        config.extended_hours_counter_trading = OperationMode::Enabled;
    }
    assets
}

/// Restart window 1: killed while the placement acknowledgement is in
/// flight (the broker recorded the order; the bot never saw the response).
/// The relaunched bot re-drives the pending claim, the identical
/// `client_order_id` hits the duplicate 422, and the executor adopts the
/// recorded order -- one broker order, one fill, no double submit, at the
/// overnight session.
#[test_log::test(tokio::test)]
async fn overnight_crash_mid_placement_adopts_on_restart() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let latency =
        crate::chaos::LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    latency
        .delay_order_placements(Duration::from_secs(20), 1)
        .await;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(overnight_enabled(infra.assets_config()))
        .session_clock_offset_secs(offset)
        .overnight_knobs(true)
        .broker_url_override(latency.endpoint.clone())
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    crate::chaos_crash::poll_for_recorded_broker_order(&mut bot, &infra.broker_service).await;

    // Crash while the acknowledgement is still held by the proxy.
    bot.abort();
    let _ = bot.await;

    // Premise checks (the chaos_crash discipline): the crash must have
    // landed after `Placed` (Pending) and the pending claim, but before
    // any broker outcome -- otherwise CI jitter would silently turn this
    // into a plain post-acknowledgement restart.
    let pool = crate::poll::connect_db(&infra.db_path).await?;
    let offchain_events_at_crash = crate::poll::count_events(&pool, "OffchainOrder").await?;
    let position_events_at_crash = crate::poll::fetch_events_by_type(&pool, "Position").await?;
    pool.close().await;
    assert_eq!(
        offchain_events_at_crash, 1,
        "the crash must land between Placed and the broker outcome"
    );
    let claim_count = position_events_at_crash
        .iter()
        .filter(|event| event.event_type == "PositionEvent::OffChainOrderPlaced")
        .count();
    assert_eq!(
        claim_count, 1,
        "the pending-placement claim must already be persisted at the crash point"
    );

    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(overnight_enabled(infra.assets_config()))
        .session_clock_offset_secs(offset)
        .overnight_knobs(true)
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events(&mut bot2, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_eq!(
        infra.broker_service.orders().len(),
        1,
        "recovery must adopt the recorded order, never place a second one"
    );
    assert_fault_recovery(&infra, &bot2, equity_symbol, broker_fill_price).await;

    bot2.abort();
    Ok(())
}

/// Restart window 2: killed while the overnight order sits Submitted. The
/// relaunched bot's recovery re-enrolls the status poll; the fill is
/// observed only after the restart and reconciles exactly once.
#[test_log::test(tokio::test)]
async fn overnight_crash_while_submitted_resumes_polling() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 999);

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

    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Accepted", 1).await;
    bot.abort();
    let _ = bot.await;

    // The fill becomes available only after the restart.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);

    let mut bot2 = spawn_bot(build(&infra)?);
    poll_for_events(&mut bot2, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_fault_recovery(&infra, &bot2, equity_symbol, broker_fill_price).await;

    bot2.abort();
    Ok(())
}

/// Restart window 3: killed while the reprice cancellation is pending
/// (`Cancelling`, broker not yet settled). The relaunched poller drives
/// the cancellation terminal, the position releases, and the replacement
/// hedge fills -- exactly one fill, no stranded claim.
#[test_log::test(tokio::test)]
async fn overnight_crash_while_cancelling_confirms_on_restart() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(5.0);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 999);
    // The pending_cancel window outlives the crash: the DELETE lands, but
    // terminal Canceled needs polls only the relaunched bot will make.
    infra.broker_service.set_cancel_settle_polls(15);

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
            .overnight_reprice_secs_override(3)
            .call()
    };
    let mut bot = spawn_bot(build(&infra)?);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::CancelRequested",
        1,
    )
    .await;
    bot.abort();
    let _ = bot.await;

    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);

    let mut bot2 = spawn_bot(build(&infra)?);
    poll_for_events(
        &mut bot2,
        &infra.db_path,
        "OffchainOrderEvent::Cancelled",
        1,
    )
    .await;
    poll_for_events(&mut bot2, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_fault_recovery(&infra, &bot2, equity_symbol, broker_fill_price).await;

    bot2.abort();
    Ok(())
}

/// Restart window 4: killed inside the overnight session with a live
/// overnight limit; relaunched after the 04:00 boundary (the session
/// clock lands at 05:00 the next morning). The first Extended scan
/// converges the survivor with `PreMarketOpenReplacement` -- the e2e twin
/// of 1952's scan-layer restart test -- and the released exposure
/// re-hedges through the extended chain (mark-referenced limit) to a
/// fill. Never two live orders, exactly one final fill.
#[test_log::test(tokio::test)]
async fn overnight_crash_across_the_0400_transition_converges() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);
    let buy_amount = float!(5.0);

    let infra = TestInfra::start(
        vec![(equity_symbol, broker_fill_price)],
        vec![(equity_symbol, float!(100.0))],
    )
    .await?;
    let overnight_offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 999);

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(both_sessions_enabled(infra.assets_config()))
        .session_clock_offset_secs(overnight_offset)
        .overnight_knobs(true)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(buy_amount)
        .price(float!(155.00))
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Accepted", 1).await;
    bot.abort();
    let _ = bot.await;

    // The relaunch lands the session clock past the 04:00 boundary: the
    // next morning's pre-market, on the effective calendar's second day.
    // The fill delay stays prohibitive until the boundary cancel is
    // observed -- dropping it now would let the relaunched poller fill
    // the ORIGINAL overnight order (a legitimate fill-beats-cancel
    // outcome, but not this scenario).
    let morning_offset = clock_offset_secs_to_et(effective_date() + chrono::Days::new(1), 5, 0);

    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(both_sessions_enabled(infra.assets_config()))
        .session_clock_offset_secs(morning_offset)
        .overnight_knobs(true)
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events(
        &mut bot2,
        &infra.db_path,
        "OffchainOrderEvent::Cancelled",
        1,
    )
    .await;

    // The boundary sweep, not a timeout, converged the survivor.
    let pool = crate::poll::connect_db(&infra.db_path).await?;
    let cancelled_events = crate::poll::fetch_events_by_type(&pool, "OffchainOrder").await?;
    pool.close().await;
    let boundary_cancel = cancelled_events.iter().any(|event| {
        event.event_type == "OffchainOrderEvent::Cancelled"
            && event
                .payload
                .to_string()
                .contains("PreMarketOpenReplacement")
    });
    assert!(
        boundary_cancel,
        "the survivor must be converged by the pre-market boundary sweep"
    );

    // Only now may the replacement fill.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);
    poll_for_events(&mut bot2, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    assert_fault_recovery(&infra, &bot2, equity_symbol, broker_fill_price).await;

    bot2.abort();
    Ok(())
}

// ---------- Step 6: boundary and calendar scenarios ----------

/// Like `arm_overnight_mock`, but with an explicit calendar shape for the
/// boundary scenarios; the caller computes its own session-clock offset.
fn arm_overnight_mock_with_calendar(
    infra: &TestInfra<impl alloy::providers::Provider + Clone>,
    symbol: &str,
    entries: Vec<st0x_execution::alpaca_broker_api::CalendarEntry>,
) {
    infra.broker_service.set_calendar_entries(entries);
    infra.broker_service.set_overnight_quote(
        Symbol::new(symbol).expect("test symbol"),
        json!({
            "t": chrono::Utc::now().to_rfc3339(),
            "bp": "150.00",
            "ap": "150.50",
        }),
    );
    infra
        .broker_service
        .set_overnight_contract_enforcement(true);
}

/// Drives one take through a bot whose session clock sits at `offset` and
/// asserts the exposure hedges to a fill (the evening opened overnight).
async fn assert_evening_hedges_overnight(
    infra: &TestInfra<impl alloy::providers::Provider + Clone>,
    offset: i64,
    symbol: &str,
    broker_fill_price: Float,
) -> anyhow::Result<()> {
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
        .symbol(symbol)
        .amount(float!(5.0))
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;
    assert_fault_recovery(infra, &bot, symbol, broker_fill_price).await;

    bot.abort();
    Ok(())
}

/// Drives one take through a bot whose session clock sits at `offset` and
/// asserts the evening classifies Closed: the trade is accounted, and no
/// order of any kind reaches the broker across several scan intervals.
async fn assert_evening_places_nothing(
    infra: &TestInfra<impl alloy::providers::Provider + Clone>,
    offset: i64,
    symbol: &str,
) -> anyhow::Result<()> {
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
        .symbol(symbol)
        .amount(float!(5.0))
        .price(float!(155.00))
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Filled", 1).await;
    tokio::time::sleep(Duration::from_secs(7)).await;

    assert!(
        infra.broker_service.orders().is_empty(),
        "a Closed evening must place no order of any kind"
    );

    bot.abort();
    Ok(())
}

/// The 20:00 weeknight entry: a live extended-hours day order from the
/// prior session is cancelled broker-side at 20:00 (Alpaca's day-order
/// expiry, simulated with a direct DELETE the bot never requested). The
/// relaunched bot's poller observes it as an UNREQUESTED cancellation,
/// releases the position, and re-hedges through the overnight path.
#[test_log::test(tokio::test)]
async fn overnight_2000_entry_rehedges_after_the_broker_auto_cancel() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);

    let infra = TestInfra::start(
        vec![(equity_symbol, broker_fill_price)],
        vec![(equity_symbol, float!(100.0))],
    )
    .await?;
    let evening_offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    let extended_offset = clock_offset_secs_to_et(effective_date(), 18, 0);
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 999);

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let build = |offset: i64, infra: &TestInfra<_>| {
        build_ctx()
            .chain(&infra.base_chain)
            .broker(&infra.broker_service)
            .db_path(&infra.db_path)
            .deployment_block(current_block)
            .assets(both_sessions_enabled(infra.assets_config()))
            .session_clock_offset_secs(offset)
            .overnight_knobs(true)
            .call()
    };

    // The prior extended session: a live day limit that will not fill.
    let mut bot = spawn_bot(build(extended_offset, &infra)?);
    tokio::time::sleep(Duration::from_secs(2)).await;
    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(float!(3.0))
        .price(float!(155.00))
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Accepted", 1).await;
    bot.abort();
    let _ = bot.await;

    // 20:00 ET: the venue cancels the unfilled day order on its own.
    let expired_order_id = infra.broker_service.orders()[0].order_id.clone();
    let cancelled = reqwest::Client::new()
        .delete(format!(
            "{}/v1/trading/accounts/{}/orders/{}",
            infra.broker_service.base_url(),
            st0x_execution::alpaca_broker_api::TEST_ACCOUNT_ID,
            expired_order_id
        ))
        .send()
        .await?;
    assert_eq!(cancelled.status().as_u16(), 204);

    // The overnight session: the relaunched poller observes the broker-side
    // cancellation and the released exposure re-hedges overnight.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);
    let mut bot2 = spawn_bot(build(evening_offset, &infra)?);

    poll_for_events(
        &mut bot2,
        &infra.db_path,
        "OffchainOrderEvent::Cancelled",
        1,
    )
    .await;
    let pool = crate::poll::connect_db(&infra.db_path).await?;
    let events = crate::poll::fetch_events_by_type(&pool, "OffchainOrder").await?;
    pool.close().await;
    assert!(
        events.iter().any(|event| {
            event.event_type == "OffchainOrderEvent::Cancelled"
                && event.payload.to_string().contains("Unrequested")
        }),
        "the auto-cancel must land as an UNREQUESTED cancellation"
    );

    poll_for_events(&mut bot2, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;
    assert_fault_recovery(&infra, &bot2, equity_symbol, broker_fill_price).await;

    bot2.abort();
    Ok(())
}

/// The 09:30 convergence: an overnight survivor met by a Regular tick is
/// cancelled for market-open replacement and the released exposure
/// re-hedges as a regular market order.
#[test_log::test(tokio::test)]
async fn overnight_survivor_converges_to_a_market_order_at_0930() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);

    let infra = TestInfra::start(
        vec![(equity_symbol, broker_fill_price)],
        vec![(equity_symbol, float!(100.0))],
    )
    .await?;
    let overnight_offset = arm_overnight_mock(&infra, equity_symbol, "150.00", "150.50");
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 999);

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let build = |offset: i64, infra: &TestInfra<_>| {
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

    let mut bot = spawn_bot(build(overnight_offset, &infra)?);
    tokio::time::sleep(Duration::from_secs(2)).await;
    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(float!(3.0))
        .price(float!(155.00))
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Accepted", 1).await;
    bot.abort();
    let _ = bot.await;

    // Regular hours, next morning: the market-open sweep converges the
    // survivor and the replacement places as a market order -- which the
    // overnight contract enforcement must no longer gate (it models the
    // venue's overnight-session validation, and the session is over).
    infra
        .broker_service
        .set_overnight_contract_enforcement(false);
    let regular_offset = clock_offset_secs_to_et(effective_date() + chrono::Days::new(1), 9, 35);
    let mut bot2 = spawn_bot(build(regular_offset, &infra)?);

    poll_for_events(
        &mut bot2,
        &infra.db_path,
        "OffchainOrderEvent::Cancelled",
        1,
    )
    .await;
    let pool = crate::poll::connect_db(&infra.db_path).await?;
    let events = crate::poll::fetch_events_by_type(&pool, "OffchainOrder").await?;
    pool.close().await;
    assert!(
        events.iter().any(|event| {
            event.event_type == "OffchainOrderEvent::Cancelled"
                && event.payload.to_string().contains("MarketOpenReplacement")
        }),
        "the survivor must be converged by the market-open sweep"
    );

    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);
    poll_for_events(&mut bot2, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;
    assert_fault_recovery(&infra, &bot2, equity_symbol, broker_fill_price).await;

    bot2.abort();
    Ok(())
}

/// Friday 20:00 ET: the next calendar day does not trade, so no overnight
/// session opens and no order is placed before the weekend gap.
#[test_log::test(tokio::test)]
async fn friday_evening_places_no_overnight_order() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let infra = TestInfra::start(vec![(equity_symbol, float!(150.25))], vec![]).await?;
    let friday = NaiveDate::from_ymd_opt(2026, 9, 11).expect("a Friday");
    arm_overnight_mock_with_calendar(
        &infra,
        equity_symbol,
        st0x_execution::alpaca_broker_api::friday_close_calendar(friday),
    );

    let offset = clock_offset_secs_to_et(friday, 21, 0);
    assert_evening_places_nothing(&infra, offset, equity_symbol).await
}

/// Sunday 20:00 ET: Sunday itself never trades, but Monday does, so the
/// D+1 predicate opens the overnight session and the exposure hedges.
#[test_log::test(tokio::test)]
async fn sunday_evening_opens_the_overnight_session() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);
    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let monday = NaiveDate::from_ymd_opt(2026, 9, 14).expect("a Monday");
    arm_overnight_mock_with_calendar(
        &infra,
        equity_symbol,
        st0x_execution::alpaca_broker_api::sunday_open_calendar(monday),
    );

    let sunday = NaiveDate::from_ymd_opt(2026, 9, 13).expect("a Sunday");
    let offset = clock_offset_secs_to_et(sunday, 21, 0);
    assert_evening_hedges_overnight(&infra, offset, equity_symbol, broker_fill_price).await
}

/// The evening before a full market holiday stays Closed: no overnight
/// session runs immediately before a holiday.
#[test_log::test(tokio::test)]
async fn holiday_eve_evening_places_no_overnight_order() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let infra = TestInfra::start(vec![(equity_symbol, float!(150.25))], vec![]).await?;
    let thanksgiving_eve = NaiveDate::from_ymd_opt(2026, 11, 25).expect("a Wednesday");
    arm_overnight_mock_with_calendar(
        &infra,
        equity_symbol,
        st0x_execution::alpaca_broker_api::holiday_eve_calendar(thanksgiving_eve),
    );

    let offset = clock_offset_secs_to_et(thanksgiving_eve, 21, 0);
    assert_evening_places_nothing(&infra, offset, equity_symbol).await
}

/// An ordinary half-day (early close, next day trades): the early close
/// leaves a Closed gap, but the overnight session still opens at the
/// fixed 20:00 and the exposure hedges.
#[test_log::test(tokio::test)]
async fn ordinary_half_day_evening_opens_overnight() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);
    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let half_day = NaiveDate::from_ymd_opt(2026, 9, 9).expect("a Wednesday");
    arm_overnight_mock_with_calendar(
        &infra,
        equity_symbol,
        st0x_execution::alpaca_broker_api::half_day_calendar(half_day, true),
    );

    let offset = clock_offset_secs_to_et(half_day, 21, 0);
    assert_evening_hedges_overnight(&infra, offset, equity_symbol, broker_fill_price).await
}

/// A pre-holiday half-day: the early close's evening has no trading next
/// day, so it stays Closed and nothing is placed.
#[test_log::test(tokio::test)]
async fn pre_holiday_half_day_evening_stays_closed() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let infra = TestInfra::start(vec![(equity_symbol, float!(150.25))], vec![]).await?;
    let christmas_eve = NaiveDate::from_ymd_opt(2026, 12, 24).expect("a Thursday");
    arm_overnight_mock_with_calendar(
        &infra,
        equity_symbol,
        st0x_execution::alpaca_broker_api::half_day_calendar(christmas_eve, false),
    );

    let offset = clock_offset_secs_to_et(christmas_eve, 21, 0);
    assert_evening_places_nothing(&infra, offset, equity_symbol).await
}

/// The DST fall-back Sunday (2026-11-01, clocks left EDT at 02:00 ET):
/// the classifier must place the 20:00 ET overnight open under the NEW
/// UTC offset. US transitions land on Sundays at 02:00, so no session
/// ever spans a shift -- correctly classifying the transition evening IS
/// the DST behavior, and the session opens like any Sunday.
#[test_log::test(tokio::test)]
async fn dst_fall_back_sunday_still_opens_overnight() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let broker_fill_price = float!(150.25);
    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let monday_after_fall_back = NaiveDate::from_ymd_opt(2026, 11, 2).expect("a Monday");
    arm_overnight_mock_with_calendar(
        &infra,
        equity_symbol,
        st0x_execution::alpaca_broker_api::sunday_open_calendar(monday_after_fall_back),
    );

    let fall_back_sunday = NaiveDate::from_ymd_opt(2026, 11, 1).expect("the fall-back Sunday");
    let offset = clock_offset_secs_to_et(fall_back_sunday, 21, 0);
    assert_evening_hedges_overnight(&infra, offset, equity_symbol, broker_fill_price).await
}
