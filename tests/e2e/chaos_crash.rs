//! Chaos tests for crash and restart recovery.
//!
//! Kills the bot task at critical in-flight points -- mid broker
//! placement, while an order sits Submitted -- and restarts a fresh
//! conductor against the same SQLite database. Asserts the recovered
//! state converges to exactly one hedge per fill: no double-effects,
//! no lost events, no operations stuck in a non-terminal state.
//!
//! The existing restart tests (`resumption_after_shutdown`,
//! `crash_recovery_eventual_consistency` in `hedging`) crash at quiet
//! points, after a trade fully completes. These tests crash in the
//! middle of the side-effect window, where recovery must reconstruct
//! intent from partial state.

use std::time::Duration;

use st0x_float_macro::float;

use crate::chaos::LatencyProxy;
use crate::hedging::assertions::*;
use crate::poll::{
    connect_db, count_events, fetch_events_by_type, poll_for_events_with_timeout, spawn_bot,
};

/// Top-level hypothesis: a crash while a broker placement is in flight
/// -- the broker received and recorded the order, the acknowledgement
/// never arrived, and no `OffchainOrder` event was persisted -- must
/// not double-submit when the bot restarts.
///
/// Mechanism under test: the hedge job persists the position's pending
/// claim (`PlaceOffChainOrder`) before calling the broker, and the
/// broker call happens inside the `OffchainOrder` aggregate's `Place`
/// command *before* any event is emitted. Killing the bot while the
/// [`LatencyProxy`] holds the placement response therefore leaves a
/// position claimed by an order aggregate that has zero events. On
/// restart, `recover_orphaned_pending_offchain_orders` finds the claim,
/// observes the aggregate is missing, and issues `FailOffChainOrder`,
/// which stashes the orphaned id as the position's idempotency anchor.
/// The `CheckPositions` scan then re-enqueues a hedge whose
/// `client_order_id` derives from that anchor, the broker rejects the
/// duplicate with a 422, and the executor adopts the order the broker
/// already accepted. Net result: exactly one broker order.
#[test_log::test(tokio::test)]
async fn crash_during_in_flight_placement_recovers_without_double_submit() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let latency = LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;

    // Hold the placement acknowledgement long enough to abort the bot
    // while the response is in flight. The broker mock records the
    // order the moment the request arrives.
    latency
        .delay_order_placements(Duration::from_secs(20), 1)
        .await;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .broker_url_override(latency.endpoint.clone())
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

    poll_for_recorded_broker_order(&mut bot, &infra.broker_service).await;

    // Crash while the acknowledgement is still held by the proxy.
    bot.abort();
    let _ = bot.await;

    // Pin the crash point: the broker has the order, the position holds
    // a pending claim, and the order aggregate has zero events. If the
    // placement pipeline ever starts persisting events before the
    // broker call, this premise check fails loudly instead of letting
    // the test validate a different scenario.
    let pool = connect_db(&infra.db_path).await?;
    let offchain_events_at_crash = count_events(&pool, "OffchainOrder").await?;
    pool.close().await;
    assert_eq!(
        offchain_events_at_crash, 0,
        "Expected the crash to land before any OffchainOrder event was \
         persisted; found {offchain_events_at_crash}",
    );

    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events_with_timeout(
        &mut bot2,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

    // The startup sweep must have recovered the orphaned claim -- this
    // is what proves the recovery path ran rather than the test passing
    // trivially.
    let pool = connect_db(&infra.db_path).await?;
    let position_events = fetch_events_by_type(&pool, "Position").await?;
    pool.close().await;

    let orphan_failures = position_events
        .iter()
        .filter(|event| event.event_type == "PositionEvent::OffChainOrderFailed")
        .count();
    assert_eq!(
        orphan_failures, 1,
        "Expected exactly one PositionEvent::OffChainOrderFailed from the \
         startup orphan sweep; got {orphan_failures}",
    );

    let orders = infra.broker_service.orders();
    let order_count = orders.len();
    assert_eq!(
        order_count, 1,
        "Expected exactly one order on the broker after crashing mid \
         placement; got {order_count}. More than one means the restarted \
         bot re-placed without the idempotency anchor and the broker \
         could not dedupe the second submission.",
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

    bot2.abort();
    Ok(())
}

/// Top-level hypothesis: a crash while a hedge order sits `Submitted`
/// at the broker -- placed and acknowledged, fill still pending -- must
/// resume status polling after restart and drive the order to `Filled`
/// without placing a second hedge.
///
/// Mechanism under test: the broker mock is configured to keep the
/// order "new" across status polls, so the bot is killed in the window
/// where the `OffchainOrder` aggregate is `Submitted` and only a
/// `PollOrderStatus` job (lost with the dead process's in-flight work)
/// would ever complete it. On restart,
/// `recover_submitted_offchain_orders` scans for non-terminal orders
/// and re-enqueues the poll; the mock then fills on the next status
/// check, and `ReconcileOrderFill` completes both aggregates.
#[test_log::test(tokio::test)]
async fn crash_while_order_submitted_resumes_polling_and_fills() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    // Keep the broker order "new" indefinitely so the crash window --
    // order Submitted, fill pending -- stays open as long as needed.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 1000);

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

    let take_result = infra
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
        "OffchainOrderEvent::Submitted",
        1,
        Duration::from_secs(60),
    )
    .await;

    // Crash while the order awaits its fill.
    bot.abort();
    let _ = bot.await;

    // Pin the crash point: submitted, not filled.
    let pool = connect_db(&infra.db_path).await?;
    let filled_at_crash: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM events WHERE event_type = 'OffchainOrderEvent::Filled'",
    )
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    assert_eq!(
        filled_at_crash.0, 0,
        "Expected the crash to land before the order filled; the fill \
         delay knob did not hold the order open",
    );

    // Let the broker fill on the next status poll after restart.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);

    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events_with_timeout(
        &mut bot2,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let offchain_order_events = fetch_events_by_type(&pool, "OffchainOrder").await?;

    let non_terminal: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM offchain_order_view \
         WHERE status IN ('Pending', 'Submitted', 'PartiallyFilled')",
    )
    .fetch_one(&pool)
    .await?;
    pool.close().await;

    let distinct_aggregates: std::collections::HashSet<&str> = offchain_order_events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert_eq!(
        distinct_aggregates.len(),
        1,
        "Expected the restart to resume the submitted order, not place a \
         second hedge; found {} OffchainOrder aggregates",
        distinct_aggregates.len(),
    );

    assert_eq!(
        non_terminal.0, 0,
        "Recovery left {} offchain orders in a non-terminal state -- the \
         PollOrderStatus re-enqueue was skipped and the order would sit \
         Submitted forever",
        non_terminal.0,
    );

    let orders = infra.broker_service.orders();
    let order_count = orders.len();
    assert_eq!(
        order_count, 1,
        "Expected exactly one order on the broker after resuming a \
         Submitted order across a crash; got {order_count}",
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

    bot2.abort();
    Ok(())
}

/// Polls until the broker mock has recorded exactly one order,
/// panicking if the bot dies or the 60s deadline passes first. Used to
/// time a crash to the moment a placement has reached the broker while
/// its acknowledgement is still in flight.
async fn poll_for_recorded_broker_order(
    bot: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    broker: &st0x_execution::alpaca_broker_api::AlpacaBrokerMock,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    loop {
        crate::poll::sleep_or_crash(bot, "broker-recorded order").await;

        if broker.orders().len() == 1 {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for the broker mock to record the placement",
        );
    }
}
