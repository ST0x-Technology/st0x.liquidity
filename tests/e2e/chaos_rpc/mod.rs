//! Chaos tests for RPC fault tolerance.
//!
//! These tests wire the bot's onchain provider through a [`ChaosProxy`]
//! so the test harness can inject transient RPC failures (dropped
//! responses, 5xx errors, empty `eth_getLogs` results modelling a
//! load-balancer that routed to a lagging node) and assert the bot's
//! recovery logic delivers the correct end state without losing or
//! duplicating events.

use std::time::Duration;

use st0x_float_macro::float;

use crate::chaos::ChaosProxy;
use crate::hedging::assertions::*;
use crate::poll::{poll_for_events_with_timeout, spawn_bot};

/// Top-level hypothesis: a SellEquity onchain trade observed
/// only in the bot's initial `eth_getLogs` backfill window still produces
/// `OffchainOrderEvent::Filled` even when the first batch of backfill
/// responses come back empty -- the load-balancer-inconsistency case.
///
/// Scenario: the take fires *before* the bot is up. When the bot starts,
/// its initial backfill from `deployment_block` to chain head is the
/// only thing that can surface the take -- the WS subscription only
/// delivers events that happen after the subscribe handshake. The
/// chaos proxy is armed to return empty `eth_getLogs` results for the
/// next batches, modelling a load-balancer node that has not finished
/// indexing the block range covering the take. On master, backfill
/// trusts the empty response, advances its checkpoint past the
/// take's block, and `OffchainOrderEvent::Filled` is never reached --
/// the test times out at `poll_for_events_with_timeout`. The upstack
/// implementation must add a read-after-write tip check (or equivalent
/// reconciliation) so the empty response is detected as unauthoritative
/// and retried.
#[test_log::test(tokio::test)]
async fn transient_empty_get_logs_during_backfill_does_not_drop_events() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let chaos = ChaosProxy::start(infra.base_chain.ws_endpoint()?).await?;

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

    // Take the order BEFORE the bot starts. The WS subscribe path will
    // not deliver this event (it predates the subscription), so the bot
    // must catch it via `eth_getLogs` backfill -- exactly the path the
    // chaos proxy targets.
    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // Arm the proxy to nuke the next batch of `eth_getLogs` responses
    // before the bot can run its initial backfill. The bot's first two
    // `eth_getLogs` calls (one for ClearV3, one for TakeOrderV3) cover
    // the deployment_block..head range, so 4 empty responses cover the
    // initial sync plus a paranoid retry margin.
    chaos.empty_get_logs(4).await;

    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .ws_rpc_url_override(chaos.endpoint.clone())
        .call()?;
    let mut bot = spawn_bot(ctx);

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

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
