//! Chaos tests for Alpaca broker fault tolerance.
//!
//! Drives the bot through scenarios where the Alpaca broker mock returns
//! a 5xx after recording the order in its internal state. Asserts the
//! bot does not double-submit hedges when retrying a placement whose
//! response was lost in flight.

use st0x_float_macro::float;
use std::time::Duration;

use crate::hedging::assertions::*;
use crate::poll::{connect_db, fetch_events_by_type, poll_for_events_with_timeout, spawn_bot};

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

    // Assert the recovery path actually ran, so this can't pass trivially: the
    // first attempt must have Failed (the lost-in-flight 503), and a second
    // OffchainOrder aggregate must exist (the CheckPositions re-enqueue). The
    // broker dedupe is only exercised when both happened.
    let pool = connect_db(&infra.db_path).await?;
    let offchain_order_events = fetch_events_by_type(&pool, "OffchainOrder").await?;
    pool.close().await;

    let failed_count = offchain_order_events
        .iter()
        .filter(|event| event.event_type == "OffchainOrderEvent::Failed")
        .count();
    assert_eq!(
        failed_count, 1,
        "Expected exactly one OffchainOrder failure (the transient 503) before \
         recovery; got {failed_count}",
    );

    let distinct_aggregates: std::collections::HashSet<&str> = offchain_order_events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert!(
        distinct_aggregates.len() >= 2,
        "Expected at least two OffchainOrder aggregates -- the failed first \
         attempt and the re-enqueued retry that reconciled the broker's \
         existing order; got {}",
        distinct_aggregates.len(),
    );

    let orders = infra.broker_service.orders();
    let order_count = orders.len();
    assert_eq!(
        order_count, 1,
        "Expected exactly one order on the broker after a transient 5xx \
         on the placement response; got {order_count}. More than one \
         means the bot retried without an idempotency key and the \
         broker has no way to dedupe the second submission.",
    );

    bot.abort();
    Ok(())
}
