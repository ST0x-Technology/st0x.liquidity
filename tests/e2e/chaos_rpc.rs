//! Chaos tests for RPC fault tolerance.
//!
//! These tests wire the bot's onchain provider through a [`ChaosProxy`]
//! so the test harness can inject transient RPC failures (dropped
//! responses, 5xx errors, empty `eth_getLogs` results modelling a
//! load-balancer that routed to a lagging node, delayed responses
//! modelling a slow node) and assert the bot's recovery logic delivers
//! the correct end state without losing or duplicating events.

use std::time::Duration;

use st0x_float_macro::float;

use crate::chaos::ChaosProxy;
use crate::hedging::assertions::*;
use crate::poll::{poll_for_events_with_timeout, spawn_bot};

/// Top-level hypothesis: a SellEquity onchain trade observed
/// only in the bot's initial `eth_getLogs` poll window still produces
/// `OffchainOrderEvent::Filled` even when the first batch of `getLogs`
/// responses come back empty -- the load-balancer-inconsistency case.
///
/// Scenario: the take fires *before* the bot is up. Ingestion is a
/// checkpoint-driven `eth_getLogs` poll, so the initial range from
/// `deployment_block` to the confirmed tip is the only thing that can
/// surface the take. The chaos proxy is armed to return empty
/// `eth_getLogs` results for the next batches, modelling a load-balancer
/// node that has not finished indexing the block range covering the
/// take. Without the read-after-write tip check, the poller would trust
/// the empty response, advance its checkpoint past the take's block, and
/// `OffchainOrderEvent::Filled` would never be reached -- the test would
/// time out at `poll_for_events_with_timeout`. The tip check detects the
/// empty response as unauthoritative (the responding node's
/// `eth_blockNumber` is stale) and retries.
#[test_log::test(tokio::test)]
async fn transient_empty_get_logs_during_backfill_does_not_drop_events() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let chaos = ChaosProxy::start(infra.base_chain.endpoint().parse()?).await?;

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

    // Take the order BEFORE the bot starts. The bot must catch it via
    // its checkpoint-driven `eth_getLogs` poll from deployment_block --
    // exactly the path the chaos proxy targets.
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
        .rpc_url_override(chaos.endpoint.clone())
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

/// Top-level hypothesis: high latency on `eth_getLogs` responses must not
/// drop or double-process fills. Each delayed response eventually arrives
/// with the correct payload; the checkpoint-driven poller must wait for
/// it rather than re-requesting the same range concurrently (which would
/// surface the fill twice) or advancing past it (which would lose it).
///
/// Scenario: the take fires *before* the bot is up, so the initial
/// backfill poll is the only thing that can surface it. The chaos proxy
/// is armed to hold the next batch of `eth_getLogs` responses for
/// several seconds each -- well within any transport timeout, modelling
/// a slow-but-correct upstream node. The end state must be exactly one
/// hedge: `assert_full_hedging_flow` checks broker orders, onchain
/// vaults, and CQRS state against a single take.
#[test_log::test(tokio::test)]
async fn delayed_get_logs_responses_do_not_drop_or_duplicate_events() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;
    let chaos = ChaosProxy::start(infra.base_chain.endpoint().parse()?).await?;

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

    // Take the order BEFORE the bot starts. The bot must catch it via
    // its checkpoint-driven `eth_getLogs` poll from deployment_block --
    // exactly the path the delayed responses target.
    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // Hold the next batch of `eth_getLogs` responses for 3s each. The
    // bot's initial backfill issues one call per event kind (ClearV3,
    // TakeOrderV3) over the deployment_block..head range, so 4 delayed
    // responses cover the initial sync plus the next poll round.
    chaos.delay_get_logs(Duration::from_secs(3), 4).await;

    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .rpc_url_override(chaos.endpoint.clone())
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
