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
use crate::poll::{connect_db, poll_for_all_jobs_done, poll_for_events_with_timeout, spawn_bot};

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

/// Top-level hypothesis: an RPC node re-serving logs the bot already
/// ingested in an earlier poll round -- after the backfill checkpoint
/// has advanced past them -- must not double-process the fill. The
/// duplicate enters the system as a real duplicate `AccountForDexTrade`
/// job (there is no enqueue-time dedup); the single-effect guard is the
/// `(tx_hash, log_index)`-keyed OnChainTrade aggregate.
///
/// Scenario: the first take is ingested normally through the chaos
/// proxy, which caches every non-empty `eth_getLogs` result by event
/// signature. The proxy is then armed to merge those cached logs into
/// later `eth_getLogs` responses, modelling a load-balanced or caching
/// node serving stale results that ignore the requested block range. A
/// second take on another symbol keeps the chain and poller moving. The
/// premise assertion checks the duplicate job actually entered the
/// queue -- without that the test could pass trivially -- and the end
/// state must show exactly one effect per take across broker orders,
/// onchain vaults, and CQRS state.
#[test_log::test(tokio::test)]
async fn replayed_get_logs_across_checkpoint_advances_do_not_double_process() -> anyhow::Result<()>
{
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price), ("TSLA", broker_fill_price)],
        vec![],
    )
    .await?;
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

    let take1 = infra
        .base_chain
        .take_order()
        .symbol("AAPL")
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

    // The proxy has now cached take 1's TakeOrderV3 logs. Arm it to
    // re-serve them in the next poll round's responses -- one armed
    // count per event-kind query (ClearV3 + TakeOrderV3) in the round.
    chaos.replay_get_logs(2).await;

    let take2 = infra
        .base_chain
        .take_order()
        .symbol("TSLA")
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        2,
        Duration::from_secs(120),
    )
    .await;

    // Premise: the replayed log really entered the queue as a duplicate
    // accounting job -- two takes plus at least one replayed duplicate.
    let pool = connect_db(&infra.db_path).await?;
    let (accounting_jobs,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
        .bind(std::any::type_name::<st0x_hedge::AccountForDexTrade>())
        .fetch_one(&pool)
        .await?;
    assert!(
        accounting_jobs >= 3,
        "Expected the replayed log to enqueue a duplicate accounting job \
         (2 takes + >=1 replay); found only {accounting_jobs} jobs",
    );

    // Drain every queue so the replayed duplicate accounting job has
    // definitively run before the single-effect snapshot below. The duplicate
    // is a no-op on a correct dedup guard, so it emits no
    // `OffchainOrderEvent::Filled` and the fill poll above never waited for it.
    // Without this barrier a broken guard could write the second fill *after*
    // the snapshot and the assertion would still pass, shipping the bug.
    poll_for_all_jobs_done(&mut bot, &infra.db_path, accounting_jobs).await;

    // Single effect: exactly one witnessed trade per take despite the
    // duplicate job.
    let (onchain_fills,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
            .bind("OnChainTradeEvent::Filled")
            .fetch_one(&pool)
            .await?;
    pool.close().await;
    assert_eq!(
        onchain_fills, 2,
        "Expected exactly one OnChainTrade fill per take; got {onchain_fills}",
    );

    let expected_positions = [
        ExpectedPosition::builder()
            .symbol("AAPL")
            .amount(sell_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(onchain_price)
            .broker_fill_price(broker_fill_price)
            .expected_accumulated_long(float!(0))
            .expected_accumulated_short(sell_amount)
            .expected_net(float!(0))
            .build(),
        ExpectedPosition::builder()
            .symbol("TSLA")
            .amount(sell_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(onchain_price)
            .broker_fill_price(broker_fill_price)
            .expected_accumulated_long(float!(0))
            .expected_accumulated_short(sell_amount)
            .expected_net(float!(0))
            .build(),
    ];

    assert_full_hedging_flow(
        &expected_positions,
        &[take1, take2],
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
