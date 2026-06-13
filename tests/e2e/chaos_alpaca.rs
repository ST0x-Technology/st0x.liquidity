//! Chaos tests for Alpaca broker fault tolerance.
//!
//! Drives the bot through scenarios where the broker processed a
//! placement but the bot never received a usable acknowledgement: the
//! Alpaca broker mock returns a 5xx after recording the order, or a
//! latency proxy holds the response past the client's request timeout.
//! Asserts the bot does not double-submit hedges when retrying a
//! placement whose response was lost or late in flight.

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
