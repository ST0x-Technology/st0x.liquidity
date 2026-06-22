//! Chaos tests for Alpaca broker fault tolerance.
//!
//! Drives the bot through scenarios where the broker misbehaves while
//! the chain stays healthy: a 5xx returned after recording the order, a
//! response held past the client's request timeout, and full outages
//! (connection refused) hitting before a hedge exists or while one sits
//! Submitted. Asserts the bot never double-submits, never silently
//! drops a fill, keeps unhedged exposure operator-visible, and recovers
//! to exactly one broker order once the broker returns.

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use alloy::providers::Provider;

use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, HTTP_REQUEST_TIMEOUT};
use st0x_float_macro::float;

use crate::chaos::LatencyProxy;
use crate::hedging::assertions::*;
use crate::poll::{connect_db, fetch_events_by_type, poll_for_events_with_timeout, spawn_bot};

/// Response arrives before the client times out -- no transport failure.
/// Held 5s below the timeout (not 1s) so CI scheduling jitter cannot push the
/// response past the boundary and flip this success case into a spurious
/// transport-timeout failure.
const PLACEMENT_RESPONSE_JUST_UNDER_TIMEOUT: Duration =
    Duration::from_secs(HTTP_REQUEST_TIMEOUT.as_secs() - 5);

/// Minimal delay that exceeds the client timeout and triggers recovery.
const PLACEMENT_RESPONSE_JUST_OVER_TIMEOUT: Duration =
    Duration::from_secs(HTTP_REQUEST_TIMEOUT.as_secs() + 1);

/// Comfortable margin above the timeout for slow CI hosts.
const PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT: Duration =
    Duration::from_secs(HTTP_REQUEST_TIMEOUT.as_secs() + 5);

struct BrokerDedupeRecoveryExpectation {
    expected_failures: usize,
    min_distinct_aggregates: usize,
}

async fn assert_broker_dedupe_recovery(
    db_path: &Path,
    broker: &AlpacaBrokerMock,
    expectation: BrokerDedupeRecoveryExpectation,
) {
    let pool = connect_db(db_path).await.unwrap();
    let offchain_order_events = fetch_events_by_type(&pool, "OffchainOrder").await.unwrap();
    pool.close().await;

    let failed_count = offchain_order_events
        .iter()
        .filter(|event| event.event_type == "OffchainOrderEvent::Failed")
        .count();
    assert_eq!(
        failed_count, expectation.expected_failures,
        "Expected exactly {} OffchainOrder failure(s) before recovery; got {failed_count}",
        expectation.expected_failures,
    );

    let distinct_aggregates: HashSet<&str> = offchain_order_events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert!(
        distinct_aggregates.len() >= expectation.min_distinct_aggregates,
        "Expected at least {} OffchainOrder aggregate(s); got {}",
        expectation.min_distinct_aggregates,
        distinct_aggregates.len(),
    );

    let order_count = broker.orders().len();
    assert_eq!(
        order_count, 1,
        "Expected exactly one order on the broker after dedupe recovery; got {order_count}. \
         More than one means the bot retried without an idempotency key and the broker has no \
         way to dedupe the second submission.",
    );
}

async fn assert_single_successful_hedge_without_recovery(
    db_path: &Path,
    broker: &AlpacaBrokerMock,
) {
    let pool = connect_db(db_path).await.unwrap();
    let offchain_order_events = fetch_events_by_type(&pool, "OffchainOrder").await.unwrap();
    pool.close().await;

    let failed_count = offchain_order_events
        .iter()
        .filter(|event| event.event_type == "OffchainOrderEvent::Failed")
        .count();
    assert_eq!(
        failed_count, 0,
        "Expected no OffchainOrder failures when the response arrives before the client \
         timeout; got {failed_count}",
    );

    let distinct_aggregates: HashSet<&str> = offchain_order_events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert_eq!(
        distinct_aggregates.len(),
        1,
        "Expected exactly one OffchainOrder aggregate without a recovery retry; got {}",
        distinct_aggregates.len(),
    );

    let order_count = broker.orders().len();
    assert_eq!(
        order_count, 1,
        "Expected exactly one broker order after a successful first placement; got {order_count}",
    );
}

async fn drive_sell_equity_hedge<P: Provider + Clone>(
    infra: &TestInfra<P>,
    latency: &LatencyProxy,
    poll_timeout: Duration,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let sell_amount = float!(10.75);

    let current_block = infra.base_chain.provider.get_block_number().await.unwrap();
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .broker_url_override(latency.endpoint.clone())
        .call()
        .unwrap();
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
        .await
        .unwrap();

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        poll_timeout,
    )
    .await;

    bot
}

/// Top-level hypothesis: an Alpaca placement that the broker processed but
/// failed to acknowledge (5xx in flight) must not produce two orders when the
/// bot recovers.
///
/// Mechanism under test: the mock records the first placement then returns 503.
/// The placement's `OffchainOrder` aggregate goes `Failed` and the position
/// stashes that attempt's id as its idempotency anchor; the periodic
/// `CheckPositions` scan then re-enqueues a fresh `PlaceHedge` (recovery is
/// driven by the scan, not by apalis retrying the failed job, which returned
/// `Ok` after recording the failure). The retry derives its broker-side
/// `client_order_id` from the live anchor, so the broker recognizes the
/// duplicate and rejects it with a 422 ("client_order_id must be unique"); the
/// executor reconciles by looking the order up by `client_order_id` and
/// adopting the one the broker already accepted. Net result: exactly one broker
/// order despite the lost-in-flight response.
#[test_log::test(tokio::test)]
async fn transient_alpaca_5xx_after_record_does_not_double_submit() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    // Arm the mock to record the first placement then fail its response with a
    // 503. The placement is marked Failed (the hedge job returns Ok, not an
    // apalis retry) and the CheckPositions scan re-enqueues a fresh hedge; one
    // transient failure is enough to drive the dedupe-on-retry recovery path.
    infra.broker_service.set_transient_placement_failures(1);

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

    assert_broker_dedupe_recovery(
        &infra.db_path,
        &infra.broker_service,
        BrokerDedupeRecoveryExpectation {
            expected_failures: 1,
            min_distinct_aggregates: 2,
        },
    )
    .await;

    bot.abort();
    Ok(())
}

/// Top-level hypothesis: a placement whose acknowledgement arrives just
/// before the Alpaca HTTP client's timeout succeeds on the first attempt
/// without entering the dedupe recovery path.
#[test_log::test(tokio::test)]
async fn placement_response_just_under_client_timeout_succeeds_without_retry() -> anyhow::Result<()>
{
    let broker_fill_price = float!(150.25);
    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;
    let latency = LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;

    latency
        .delay_order_placements(PLACEMENT_RESPONSE_JUST_UNDER_TIMEOUT, 1)
        .await;

    let poll_timeout = PLACEMENT_RESPONSE_JUST_UNDER_TIMEOUT + Duration::from_secs(60);
    let bot = drive_sell_equity_hedge(&infra, &latency, poll_timeout).await;

    assert_single_successful_hedge_without_recovery(&infra.db_path, &infra.broker_service).await;

    bot.abort();
    Ok(())
}

/// Top-level hypothesis: an Alpaca placement whose response is held just
/// past the client's request timeout must not produce two orders when the
/// bot recovers -- the broker executed the placement on time, only the
/// acknowledgement was late.
///
/// Uses the minimal delay (`31s`) above the hardcoded `30s` client timeout
/// to pin the transport-error boundary rather than a comfortable margin.
#[test_log::test(tokio::test)]
async fn placement_response_just_over_client_timeout_does_not_double_submit() -> anyhow::Result<()>
{
    let broker_fill_price = float!(150.25);
    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;
    let latency = LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;

    latency
        .delay_order_placements(PLACEMENT_RESPONSE_JUST_OVER_TIMEOUT, 1)
        .await;

    let poll_timeout = PLACEMENT_RESPONSE_JUST_OVER_TIMEOUT * 2 + Duration::from_secs(60);
    let bot = drive_sell_equity_hedge(&infra, &latency, poll_timeout).await;

    assert_broker_dedupe_recovery(
        &infra.db_path,
        &infra.broker_service,
        BrokerDedupeRecoveryExpectation {
            expected_failures: 1,
            min_distinct_aggregates: 2,
        },
    )
    .await;

    bot.abort();
    Ok(())
}

/// Top-level hypothesis: a fill ingested while the broker is completely
/// down -- connection refused, not slow -- must be recorded and visibly
/// unhedged, never silently dropped, and the periodic `CheckPositions`
/// rescan must place exactly one hedge once the broker returns.
///
/// Scenario: the [`LatencyProxy`]'s serve loop is severed after startup
/// (the bot needs the broker reachable to construct its executor), so
/// every broker call is refused while the chain keeps producing fills.
/// The accounting job witnesses and acknowledges the fill before its
/// broker readiness check errors; the retry dedupe-skips; several
/// `CheckPositions` ticks fire against the dead broker and must swallow
/// the errors without enqueueing anything. Mid-outage the position view
/// shows the unhedged exposure (the durable operator-visible signal the
/// dashboard renders). After restore, the first healthy rescan hedges.
#[test_log::test(tokio::test)]
async fn broker_outage_defers_hedge_until_rescan_after_restore() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let mut latency = LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;

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

    latency.sever();

    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // The fill must be recorded even with the broker dark.
    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Filled", 1).await;

    // Let several CheckPositions ticks (2s interval) fire against the
    // dead broker -- the negative assertions below are only meaningful
    // if the scan actually ran during the outage.
    wait_for_processing(&mut bot, 6).await;

    let mid_outage_orders = infra.broker_service.orders().len();
    assert_eq!(
        mid_outage_orders, 0,
        "Nothing can have reached the broker during the outage; got \
         {mid_outage_orders} orders",
    );

    let pool = connect_db(&infra.db_path).await?;
    let offchain_events = count_events(&pool, "OffchainOrder").await?;
    // CheckPositions is a self-rescheduling job: each completed tick is a `Done`
    // row, so a non-zero count proves the scan actually ran (and swallowed the
    // broker error) during the outage -- without this the negative assertions
    // below could pass vacuously if the scan never fired.
    let (scan_runs,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Done'")
            .bind(st0x_hedge::check_positions_job_type())
            .fetch_one(&pool)
            .await?;
    let position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new(equity_symbol)?)
        .await?
        .expect("position must exist mid-outage");
    pool.close().await;

    assert!(
        scan_runs >= 1,
        "At least one CheckPositions scan must have run against the dead broker \
         for the deferral assertions to be meaningful; got {scan_runs}",
    );
    assert_eq!(
        offchain_events, 0,
        "No offchain order may exist while the broker is down"
    );
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(sell_amount),
        "The position must visibly carry the unhedged fill"
    );
    assert_eq!(
        position.pending_offchain_order_id, None,
        "No hedge can be claimed against a dead broker"
    );

    latency.restore().await?;

    poll_for_events_with_timeout(
        &mut bot,
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
        "Exactly one hedge after the broker returned; got {order_count}",
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

/// Top-level hypothesis: an Alpaca placement whose response is held well
/// past the client's request timeout must not produce two orders when the
/// bot recovers.
///
/// Mechanism under test: a [`LatencyProxy`] in front of the broker mock
/// forwards the first placement immediately (the mock records the order)
/// and holds the response past the Alpaca client's request timeout.
/// Unlike the 5xx scenario above, the bot never receives an HTTP response
/// at all -- this exercises the transport-error (`HttpClient`) failure
/// path rather than the `ApiError` path.
#[test_log::test(tokio::test)]
async fn delayed_placement_response_past_client_timeout_does_not_double_submit()
-> anyhow::Result<()> {
    let broker_fill_price = float!(150.25);
    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;
    let latency = LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;

    latency
        .delay_order_placements(PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT, 1)
        .await;

    let poll_timeout = PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT * 2 + Duration::from_secs(60);
    let bot = drive_sell_equity_hedge(&infra, &latency, poll_timeout).await;

    assert_broker_dedupe_recovery(
        &infra.db_path,
        &infra.broker_service,
        BrokerDedupeRecoveryExpectation {
            expected_failures: 1,
            min_distinct_aggregates: 2,
        },
    )
    .await;

    bot.abort();
    Ok(())
}

/// Top-level hypothesis: when both the initial placement *and* the dedupe
/// retry receive delayed responses past the client timeout, recovery still
/// completes with exactly one broker order once the delay budget is
/// exhausted and the duplicate 422 can be reconciled.
///
/// Mechanism under test: the first POST times out after the broker records
/// the order. The retry reuses the stashed idempotency anchor; the broker
/// responds with 422 immediately, but the proxy holds that response too, so
/// the bot sees a second transport timeout. A third attempt hits an
/// un-delayed 422, the executor reconciles by `client_order_id` lookup,
/// and hedging completes without a second broker order.
#[test_log::test(tokio::test)]
async fn two_delayed_placement_responses_past_client_timeout_do_not_double_submit()
-> anyhow::Result<()> {
    let broker_fill_price = float!(150.25);
    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;
    let latency = LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;

    latency
        .delay_order_placements(PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT, 2)
        .await;

    let poll_timeout = PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT * 3 + Duration::from_secs(90);
    let bot = drive_sell_equity_hedge(&infra, &latency, poll_timeout).await;

    assert_broker_dedupe_recovery(
        &infra.db_path,
        &infra.broker_service,
        BrokerDedupeRecoveryExpectation {
            expected_failures: 2,
            min_distinct_aggregates: 3,
        },
    )
    .await;

    bot.abort();
    Ok(())
}

#[test]
fn alpaca_client_timeout_constants_bracket_placement_delay_boundaries() {
    assert!(
        PLACEMENT_RESPONSE_JUST_UNDER_TIMEOUT < HTTP_REQUEST_TIMEOUT,
        "just-under delay must arrive before the client times out",
    );
    assert!(
        PLACEMENT_RESPONSE_JUST_OVER_TIMEOUT > HTTP_REQUEST_TIMEOUT,
        "just-over delay must exceed the client timeout",
    );
    assert!(
        PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT > PLACEMENT_RESPONSE_JUST_OVER_TIMEOUT,
        "well-over delay must remain above the timeout boundary",
    );
}

/// Top-level hypothesis: a broker outage while a hedge sits `Submitted`
/// must end in a loud halt -- never a silently un-polled order -- and a
/// restart with the broker restored must resume polling to exactly one
/// filled broker order.
///
/// Scenario: the broker mock holds the order "new" indefinitely, the
/// proxy is severed while the order awaits its fill, and `PollOrderStatus`
/// exhausts its retry budget against the refused connections. The
/// fail-stop circuit breaker then halts the bot (the deliberate
/// fail-stop posture: a one-sided trade must page an operator, not
/// accumulate silent exposure). On restart with the broker back,
/// `recover_submitted_offchain_orders` re-enqueues the poll and the
/// order completes.
#[test_log::test(tokio::test)]
async fn broker_outage_with_submitted_order_halts_bot_and_restart_recovers() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let mut latency = LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;

    // Keep the order un-filled so the outage lands while it sits
    // Submitted, awaiting status polls.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 10_000);

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

    poll_for_aggregate_events_containing(&mut bot, &infra.db_path, "OffchainOrder", "Accepted", 1)
        .await;

    latency.sever();

    // PollOrderStatus exhausts its retries against refused connections;
    // the fail-stop breaker must halt the bot loudly.
    let join_result = tokio::time::timeout(Duration::from_secs(60), &mut bot)
        .await
        .expect("Bot must halt within 60s of the broker going dark with an order in flight");
    let error = join_result
        .expect("Bot task must fail, not panic")
        .expect_err("Bot must fail-stop, not keep running with an un-pollable order");
    let chain = format!("{error:#}");
    assert!(
        chain.contains("Apalis worker failed after retries"),
        "Bot error chain should contain the terminal job failure, got: {chain}",
    );

    // Broker comes back; let the order fill on the next poll.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 0);
    latency.restore().await?;

    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .broker_url_override(latency.endpoint.clone())
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
        "Exactly one broker order after the restart resumed polling; got \
         {order_count}",
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
