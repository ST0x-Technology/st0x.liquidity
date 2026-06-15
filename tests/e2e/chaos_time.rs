//! Chaos tests for time-based faults.
//!
//! Injects the time faults that are realistically controllable from
//! outside the process: a transient market-hours (calendar) data outage
//! during fill processing, and a session/credential expiry (401) that
//! rejects a placement before the broker records anything. System-wide
//! clock jumps are not injectable in-process; the decision-relevant
//! clock reads are either parameterized (`is_market_open_at(now)`,
//! exercised by market-hours unit tests across session boundaries),
//! monotonic (`Instant`-based caches, tokio intervals), or DB-scheduled
//! (apalis delays), so wall-clock jumps cannot corrupt them by
//! construction.

use st0x_float_macro::float;
use std::time::Duration;

use crate::hedging::assertions::*;
use crate::poll::{
    connect_db, fetch_events_by_type, poll_for_calendar_failures_consumed,
    poll_for_events_with_timeout, spawn_bot,
};

/// Top-level hypothesis: a market-hours data outage spanning the inline
/// readiness check must defer the hedge -- never lose it. The inline
/// check fails while the calendar endpoint is down, the retry
/// dedupe-skips (the trade is already witnessed), and the
/// self-rescheduling `CheckPositions` scan hedges on its first tick after
/// the calendar recovers.
///
/// Scenario: the calendar is held down across the entire fill-processing
/// window (armed well past the point the test restores it), so the
/// deferral is provable rather than racing calendar recovery. The test
/// asserts, in order: the trade is witnessed; the down calendar was
/// actually hit by a readiness check (a failure drained); no hedge exists
/// while the calendar is down (the hedge was deferred, not placed
/// inline); and -- only after the calendar is explicitly restored -- a
/// single hedge appears, placed by the position scan.
#[test_log::test(tokio::test)]
async fn calendar_outage_during_fill_defers_hedge_to_position_scan() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Hold the calendar down for the whole fill-processing window. The
    // armed count is far larger than the inline check plus the handful of
    // CheckPositions ticks that fire before the test restores service, so
    // the calendar cannot recover on its own and let the hedge slip
    // through while we assert it was deferred.
    let armed_calendar_failures = 100;
    infra
        .broker_service
        .set_calendar_failures(armed_calendar_failures);

    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // The fill is witnessed inside the accounting job; the inline
    // readiness check then hits the down calendar and defers the hedge.
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OnChainTradeEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

    // Premise: a readiness check actually hit the down calendar. If the
    // calendar were cached or the check skipped, no failure would drain
    // and this would time out loudly.
    poll_for_calendar_failures_consumed(
        &mut bot,
        &infra.broker_service,
        armed_calendar_failures,
        Duration::from_secs(30),
    )
    .await;

    // The hedge is deferred, not placed: with the calendar still down, no
    // readiness check (inline or scan) can pass, so no broker order can
    // exist yet. This is the assertion the prior version lacked -- it
    // proves the deferred path was taken rather than the hedge eventually
    // completing by some route.
    let orders_while_down = infra.broker_service.orders();
    assert!(
        orders_while_down.is_empty(),
        "Hedge must be deferred while the calendar is down; found {} order(s)",
        orders_while_down.len(),
    );

    // Calendar recovers. The self-rescheduling CheckPositions scan must
    // hedge the witnessed-but-unhedged position on its next tick.
    infra.broker_service.set_calendar_failures(0);

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let (onchain_fills,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
            .bind("OnChainTradeEvent::Filled")
            .fetch_one(&pool)
            .await?;
    pool.close().await;
    assert_eq!(
        onchain_fills, 1,
        "Exactly one witnessed trade despite the readiness retry; got {onchain_fills}",
    );

    let orders = infra.broker_service.orders();
    let order_count = orders.len();
    assert_eq!(
        order_count, 1,
        "Exactly one hedge once the calendar recovered; got {order_count}",
    );

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

/// Top-level hypothesis: a placement rejected with a 401 *before* the
/// broker records anything -- session or credential expiry mid-request
/// -- must recover with exactly one fresh order, not a phantom dedupe.
///
/// Scenario: unlike the recorded-then-lost cases (5xx-after-record,
/// timed-out acknowledgement), here the broker never processed the
/// placement, so the anchor-keyed retry must NOT find an existing order
/// under its `client_order_id` -- it must place fresh and succeed. The
/// failed first attempt goes `Failed`, the position stashes the anchor,
/// the `CheckPositions` scan re-enqueues, and the retry's placement
/// sails through.
#[test_log::test(tokio::test)]
async fn unauthorized_placement_then_recovery_places_exactly_one_fresh_order() -> anyhow::Result<()>
{
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    // The full hedging-flow assertion is deliberately not used in this
    // test: the recovery path leaves one intentionally-Failed aggregate,
    // which that assertion rejects. The targeted assertions below plus
    // the hedged-position poll cover the end state.

    // One 401 rejected before the mock records anything.
    infra.broker_service.set_unauthorized_placement_failures(1);

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let _take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

    // Premise: the 401 was actually served.
    let unconsumed = infra
        .broker_service
        .unauthorized_placement_failures_remaining();
    assert_eq!(
        unconsumed, 0,
        "Expected the armed 401 to be consumed by the first placement; \
         {unconsumed} remain",
    );

    // The recovery must have run through the Failed -> anchor ->
    // re-enqueue path, not succeeded first try.
    let pool = connect_db(&infra.db_path).await?;
    let offchain_order_events = fetch_events_by_type(&pool, "OffchainOrder").await?;
    pool.close().await;

    let failed_count = offchain_order_events
        .iter()
        .filter(|event| event.event_type == "OffchainOrderEvent::Failed")
        .count();
    assert_eq!(
        failed_count, 1,
        "Expected exactly one OffchainOrder failure (the 401) before \
         recovery; got {failed_count}",
    );

    let distinct_aggregates: std::collections::HashSet<&str> = offchain_order_events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert_eq!(
        distinct_aggregates.len(),
        2,
        "Expected exactly two OffchainOrder aggregates -- the rejected first \
         attempt and the single fresh retry. A single 401 is armed and the \
         pending-order guard blocks concurrent re-placement, so more than two \
         means a retry storm spawning a fresh aggregate per scan; got {}",
        distinct_aggregates.len(),
    );

    // Fresh placement, not adoption: the broker never recorded the first
    // attempt, so exactly one order exists and it must be the retry's.
    let orders = infra.broker_service.orders();
    let order_count = orders.len();
    assert_eq!(
        order_count, 1,
        "Expected exactly one broker order after the 401 recovery; got \
         {order_count}",
    );

    poll_for_hedged_position(&mut bot, &infra.db_path, equity_symbol).await;

    bot.abort();
    Ok(())
}
