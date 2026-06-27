//! Cross-cutting chaos tests wiring multiple fault injectors at once.
//!
//! Exercises recovery when onchain ingestion and offchain placement both
//! encounter adversarial upstream behaviour during the same hedge flow.

use std::time::Duration;

use st0x_execution::alpaca_broker_api::HTTP_REQUEST_TIMEOUT;
use st0x_float_macro::float;

use crate::chaos::{ChaosProxy, LatencyProxy};
use crate::hedging::assertions::*;
use crate::poll::{connect_db, fetch_events_by_type, poll_for_events_with_timeout, spawn_bot};

/// One placement response held well past the broker HTTP timeout, forcing the
/// timeout-recovery path. Derived from the client's `HTTP_REQUEST_TIMEOUT` so it
/// tracks any change to that single source of truth.
const PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT: Duration =
    Duration::from_secs(HTTP_REQUEST_TIMEOUT.as_secs() + 5);

/// Top-level hypothesis: a hedge must complete with exactly one broker
/// order even when onchain backfill sees transient empty `eth_getLogs`
/// responses *and* the first placement acknowledgement is held past the
/// Alpaca client's request timeout.
///
/// Mechanism under test: the take fires before the bot starts, so fill
/// detection depends on the initial `eth_getLogs` backfill. The RPC chaos
/// proxy returns empty results modelling a lagging load-balancer node; the
/// tip check retries until the fill is observed. Separately, the broker
/// latency proxy holds the first placement response past the HTTP client
/// timeout; dedupe-on-retry must still leave exactly one broker order.
#[test_log::test(tokio::test)]
async fn empty_get_logs_and_delayed_placement_together_do_not_drop_or_double_hedge()
-> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let chaos = ChaosProxy::start(infra.base_chain.endpoint().parse()?).await?;
    let latency = LatencyProxy::start(infra.broker_service.base_url().parse()?).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    chaos.empty_get_logs(4).await;
    latency
        .delay_order_placements(PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT, 1)
        .await;

    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .rpc_url_override(chaos.endpoint.clone())
        .broker_url_override(latency.endpoint.clone())
        .call()?;
    let mut bot = spawn_bot(ctx);

    let poll_timeout = PLACEMENT_RESPONSE_WELL_OVER_TIMEOUT * 2 + Duration::from_secs(120);
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        poll_timeout,
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let offchain_order_events = fetch_events_by_type(&pool, "OffchainOrder").await?;
    pool.close().await;

    let failed_count = offchain_order_events
        .iter()
        .filter(|event| event.event_type == "OffchainOrderEvent::Failed")
        .count();
    assert_eq!(
        failed_count, 1,
        "Expected exactly one timed-out placement before broker dedupe recovery; got {failed_count}",
    );

    let distinct_aggregates: std::collections::HashSet<&str> = offchain_order_events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert!(
        distinct_aggregates.len() >= 2,
        "Expected at least two OffchainOrder aggregates -- the CheckPositions re-enqueue that drives \
         dedupe recovery must produce a second attempt; got {}. Without this the test can pass \
         vacuously through the plain success path.",
        distinct_aggregates.len(),
    );

    let order_count = infra.broker_service.orders().len();
    assert_eq!(
        order_count, 1,
        "Expected exactly one broker order after combined RPC and placement chaos; got {order_count}",
    );

    bot.abort();
    Ok(())
}
