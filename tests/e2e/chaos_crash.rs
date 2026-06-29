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

use crate::chaos::{ChaosProxy, LatencyProxy};
use crate::hedging::assertions::*;
use crate::poll::{
    connect_db, count_events, fetch_events_by_type, poll_for_backfill_checkpoint,
    poll_for_events_with_timeout, poll_for_running_job, spawn_bot,
};

/// Top-level hypothesis: a crash while a broker placement is in flight
/// -- the broker received the order but its outcome
/// (`MarkAccepted`/`MarkFailed`) was never committed -- must not
/// double-submit when the bot restarts.
///
/// Mechanism under test: the hedge job persists the position's pending
/// claim (`PlaceOffChainOrder`) before placement, and the durable
/// `place_offchain_order_at_broker` path emits `Placed` (entering
/// `Pending`) *before* calling the broker. Killing the bot while the
/// [`LatencyProxy`] holds the placement response therefore leaves the
/// order aggregate in `Pending`, the position still claimed, and the
/// broker order possibly live. On restart,
/// `recover_orphaned_pending_offchain_orders` finds the `Pending` orphan
/// and re-drives `place_offchain_order_at_broker`: `Place` is a no-op on
/// the existing aggregate, and the broker dedupes the retry on
/// `client_order_id` (a 422 the executor reconciles by adopting the order
/// it already accepted), driving it to `Submitted`. Net result: exactly
/// one broker order.
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

    // Pin the crash point: the broker has the order, the position holds a
    // pending claim, and the order aggregate holds exactly its `Placed` event.
    // The now-pure `Place` records intent and enters `Pending` BEFORE the broker
    // call, so a crash during that in-flight call leaves one event (`Placed`) and
    // no broker outcome yet. If the pipeline's event ordering ever changes again,
    // this premise check fails loudly instead of validating a different scenario.
    let pool = connect_db(&infra.db_path).await?;
    let offchain_events_at_crash = count_events(&pool, "OffchainOrder").await?;
    let position_events_at_crash = fetch_events_by_type(&pool, "Position").await?;
    pool.close().await;
    assert_eq!(
        offchain_events_at_crash, 1,
        "Expected the crash to land after `Placed` (Pending) but before the \
         broker outcome was recorded; found {offchain_events_at_crash} events",
    );
    // Symmetric premise: the pending placement claim -- the orphan the restart
    // sweep re-drives -- must already exist. Without this, a crash that landed
    // before the claim was written would still satisfy the events check above
    // and silently exercise fresh-hedge placement instead of in-place recovery.
    let claim_count = position_events_at_crash
        .iter()
        .filter(|event| event.event_type == "PositionEvent::OffChainOrderPlaced")
        .count();
    assert_eq!(
        claim_count, 1,
        "Expected exactly one persisted pending-placement claim at the crash \
         point; got {claim_count}",
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

    // The startup sweep re-drove the orphaned Pending placement rather than
    // failing it: the pure `Place` is a no-op on the existing aggregate and the
    // broker dedupes the in-flight order, so the SAME aggregate advances to
    // Submitted and then Filled. A Pending order that reached Filled at all
    // proves the re-drive ran (nothing else completes a Pending order); the
    // absence of any orphan failure proves it recovered in place rather than
    // falling back to fail-and-re-hedge.
    let pool = connect_db(&infra.db_path).await?;
    let position_events = fetch_events_by_type(&pool, "Position").await?;
    pool.close().await;

    let orphan_failures = position_events
        .iter()
        .filter(|event| event.event_type == "PositionEvent::OffChainOrderFailed")
        .count();
    assert_eq!(
        orphan_failures, 0,
        "Expected the orphan sweep to re-drive the Pending order in place, not \
         fail it; got {orphan_failures} PositionEvent::OffChainOrderFailed",
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

    // The sweep re-drove the SAME Pending aggregate in place, so there is
    // exactly one OffchainOrder aggregate -- the recovered orphan, not a fresh
    // retry. Assert the aggregate count directly: broker-level dedupe (the
    // 422-adopt path) would keep the broker order count at one even if recovery
    // had spawned a second aggregate, so more than one here would be a CQRS-level
    // double-count the broker dedupe otherwise masks.
    let pool = connect_db(&infra.db_path).await?;
    let offchain_order_events = fetch_events_by_type(&pool, "OffchainOrder").await?;
    pool.close().await;
    let distinct_aggregates: std::collections::HashSet<&str> = offchain_order_events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert_eq!(
        distinct_aggregates.len(),
        1,
        "Expected exactly one OffchainOrder aggregate after recovery (the \
         retry); more means a CQRS-level double-count",
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
    let _ = bot2.await;
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
        "OffchainOrderEvent::Accepted",
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
    let _ = bot2.await;
    Ok(())
}

/// Top-level hypothesis: a crash in the middle of the trade-accounting
/// job -- fill observed, job enqueued, backfill checkpoint already
/// advanced past the fill's block, but no CQRS event persisted yet --
/// must not lose the fill. The job row is the only remaining record of
/// the trade at that point.
///
/// Mechanism under test: the [`ChaosProxy`] holds the
/// `eth_getTransactionReceipt` response, which is the first RPC call
/// `AccountForDexTrade::perform` makes, pinning the job in `Running`
/// with zero events persisted. The bot is killed there. apalis cannot
/// rescue the orphaned `Running` row on its own: the deterministic
/// worker name keeps the row's heartbeat fresh after restart, so it
/// never ages out, and the advanced checkpoint means no rescan will
/// ever surface the fill again. Startup must reset the orphaned row to
/// `Pending` (`requeue_trading_orphans`) so the re-spawned worker
/// re-runs the job and the fill flows through Witness -> Position ->
/// hedge exactly once.
#[test_log::test(tokio::test)]
async fn crash_mid_accounting_job_recovers_the_fill_after_restart() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let chaos = ChaosProxy::start(infra.base_chain.endpoint().parse()?).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .rpc_url_override(chaos.endpoint.clone())
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pin the accounting job mid-perform: the receipt fetch is its first
    // RPC call, so holding the response keeps the job Running with zero
    // CQRS events persisted. The hold must outlast the worst-case time to
    // reach the abort below -- the Running-job poll and the checkpoint poll
    // can each take their full 60s timeout (120s combined) -- or a slow run
    // would deliver the receipt before the crash and let the job complete,
    // defeating the test. The receipt is never actually awaited (the crash
    // lands first), so the larger hold does not slow the test.
    chaos
        .delay_transaction_receipts(Duration::from_secs(150), 4)
        .await;

    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let take_block = infra.base_chain.provider.get_block_number().await?;

    poll_for_running_job(
        &mut bot,
        &infra.db_path,
        std::any::type_name::<st0x_hedge::AccountForDexTrade>(),
        Duration::from_secs(60),
    )
    .await;

    // The checkpoint must be past the fill before the crash: that is
    // what makes the wedged job row the only remaining record of the
    // trade, so nothing short of rescuing the row can recover the fill.
    poll_for_backfill_checkpoint(
        &mut bot,
        &infra.db_path,
        take_block,
        Duration::from_secs(60),
    )
    .await;

    bot.abort();
    let _ = bot.await;

    // Pin the crash point: job observed and enqueued, no event persisted. This
    // is the pre-witness window, where the existence dedup does not short-circuit
    // the requeued re-run, so requeue alone recovers the fill. The post-witness
    // window (witnessed but not acknowledged) is recovered by the exactly-once
    // acknowledgement marker and is covered by that work, not this test.
    let pool = connect_db(&infra.db_path).await?;
    let onchain_events_at_crash = count_events(&pool, "OnChainTrade").await?;
    pool.close().await;
    assert_eq!(
        onchain_events_at_crash, 0,
        "Expected the crash to land before the trade was witnessed; \
         found {onchain_events_at_crash} OnChainTrade events",
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

    let orders = infra.broker_service.orders();
    let order_count = orders.len();
    assert_eq!(
        order_count, 1,
        "Expected exactly one hedge on the broker for the recovered \
         fill; got {order_count}",
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

/// Polls until the broker mock has recorded at least one order,
/// panicking if the bot dies or the 60s deadline passes first. Used to
/// time a crash to the moment a placement has reached the broker while
/// its acknowledgement is still in flight. Returns on the first recorded
/// order so a double-submit surfaces via the caller's exact-count
/// assertion rather than as a misleading poll timeout here.
async fn poll_for_recorded_broker_order(
    bot: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    broker: &st0x_execution::alpaca_broker_api::AlpacaBrokerMock,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    loop {
        crate::poll::sleep_or_crash(bot, "broker-recorded order").await;

        if !broker.orders().is_empty() {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for the broker mock to record the placement",
        );
    }
}
