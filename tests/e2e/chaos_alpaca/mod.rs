//! Chaos tests for Alpaca broker fault tolerance.
//!
//! Drives the bot through scenarios where the Alpaca broker mock returns
//! a 5xx after recording the order in its internal state. Asserts the
//! bot does not double-submit hedges when retrying a placement whose
//! response was lost in flight.

use std::time::Duration;

use st0x_float_macro::float;

use crate::hedging::assertions::*;
use crate::poll::{poll_for_events_with_timeout, spawn_bot};

/// Top-level hypothesis: an Alpaca placement that the broker
/// processed but failed to acknowledge (5xx in flight) must not produce
/// two orders when the bot retries.
///
/// Scenario: the bot detects a SellEquity onchain take and enqueues a
/// `PlaceHedge` job. The mock is armed to record the first placement
/// then return 503; apalis retries the job. On master the retry triggers
/// a second `place_order` call from a fresh `OffchainOrder` aggregate
/// path, which the mock records as a separate order -- final order
/// count on the broker is 2 even though the bot believes it placed
/// only one. The upstack fix must wire an idempotency key
/// (`client_order_id`) end-to-end so the broker can dedupe duplicate
/// submissions.
#[test_log::test(tokio::test)]
async fn transient_alpaca_5xx_after_record_does_not_double_submit() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    // Arm the mock to fail the next placement response after recording
    // the order. Apalis retries with backon; one transient failure is
    // enough to expose the double-submit bug.
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
