//! North-star e2e for first-class reorg handling.
//!
//! This test asserts the end state the reorg epic converges on: a fill the bot
//! has witnessed and hedged is re-mined under a different block by a chain
//! reorg, the bot detects the fork change, reverses the stale block's position
//! impact, and re-applies the same fill on the new canonical block -- restoring
//! the position to its correct value rather than leaving it flat, all without
//! deleting history.
//!
//! The reorg is injected through the [`ChaosProxy`] fork-replay perturbation: a
//! load-balanced RPC node on a competing fork re-serves the already-ingested
//! fill's log under a different `blockHash` than the one the bot persisted on
//! the `OnChainTrade` at ingestion. The bot's backfill detects the block-hash
//! mismatch and runs the exactly-once reverse-then-reapply: it reverses the
//! stale-block impact (`OnChainTradeEvent::Reorged` + `PositionEvent::Reorged`)
//! and then re-applies the same fill on the new canonical block
//! (`OnChainTradeEvent::ReWitnessed`, then a second acknowledgement that
//! re-drives the position). The re-mined fill is still canonical, so the net
//! returns to exactly what it was before the reorg. Every step is append-only
//! -- the original `Filled` events survive and the reversal and re-application
//! are recorded as new events, never by deleting a row.

use alloy::providers::Provider;
use std::time::Duration;

use st0x_float_macro::float;

use crate::chaos::ChaosProxy;
use crate::hedging::assertions::{
    Position, Projection, Symbol, TakeDirection, TestInfra, build_ctx, spawn_bot,
};
use crate::poll::{connect_db, count_events_of_type, poll_for_events};

#[test_log::test(tokio::test)]
async fn reorg_reverts_witnessed_fill() -> anyhow::Result<()> {
    let symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let amount = float!(10.0);

    let infra = TestInfra::start(vec![(symbol, broker_fill_price)], vec![]).await?;
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

    // Ingest a fill and let the bot witness and hedge it. Witnessing serves the
    // fill's log through the proxy, which caches it for the fork-replay below to
    // re-serve under a competing block hash.
    infra
        .base_chain
        .take_order()
        .symbol(symbol)
        .amount(amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Filled", 1).await;
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    // Capture the position's net now that the fill is witnessed and hedged. This
    // is the value the reverse-then-reapply must restore: the re-mined fill is
    // still canonical, so reversing the stale block and re-applying it on the new
    // canonical block leaves the position exactly where it started.
    let pre_reorg_pool = connect_db(&infra.db_path).await?;
    let pre_reorg_net = Projection::<Position>::sqlite(pre_reorg_pool.clone())
        .load(&Symbol::new(symbol)?)
        .await?
        .expect("Position should exist after the witnessed-and-hedged fill")
        .net;
    pre_reorg_pool.close().await;

    // Simulate the reorg: a forked RPC node re-serves the witnessed fill's log
    // under a competing block hash. The count covers both event-signature polls
    // across several rounds; the reversal is exactly-once, so re-detecting the
    // same fork on later rounds is an idempotent no-op.
    chaos.fork_replay_get_logs(10).await;

    // Detection runs the exactly-once reverse-then-reapply. First the reversal:
    // the stale block's impact is backed out of both aggregates.
    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Reorged", 1).await;
    poll_for_events(&mut bot, &infra.db_path, "PositionEvent::Reorged", 1).await;

    // Then the re-application onto the new canonical block: `ReWitnessed` clears
    // the reorg markers and the second acknowledgement re-drives the position. The
    // second `OnChainOrderFilled` is the position write that restores net, so gate
    // the post-reorg load on it -- once it lands, net is back to its final value.
    poll_for_events(
        &mut bot,
        &infra.db_path,
        "OnChainTradeEvent::ReWitnessed",
        1,
    )
    .await;
    poll_for_events(
        &mut bot,
        &infra.db_path,
        "PositionEvent::OnChainOrderFilled",
        2,
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;

    // The reverse-then-reapply is append-only and asserted against the event
    // stream: nothing is deleted, the reversal and the re-application are each
    // recorded as new events. (onchain_trade_view has no writer, so reorg state is
    // read from the event log / Position aggregate, not from a view.)
    assert_eq!(
        count_events_of_type(&pool, "OnChainTradeEvent::Filled").await?,
        1,
        "the original witness must be preserved, not deleted",
    );
    assert_eq!(
        count_events_of_type(&pool, "OnChainTradeEvent::Reorged").await?,
        1,
        "the reorged fill must be marked via a Reorged event exactly once, not removed",
    );
    assert_eq!(
        count_events_of_type(&pool, "OnChainTradeEvent::ReWitnessed").await?,
        1,
        "the re-mined fill must be re-witnessed onto the new canonical block exactly once",
    );
    assert_eq!(
        count_events_of_type(&pool, "PositionEvent::Reorged").await?,
        1,
        "the reversal must back out the fill's position impact exactly once",
    );
    assert_eq!(
        count_events_of_type(&pool, "PositionEvent::OnChainOrderFilled").await?,
        2,
        "the position must record the original fill plus exactly one re-application",
    );

    // The core invariant: the reverse-then-reapply is net-preserving. The re-mined
    // fill is still canonical, so after reversing the stale block and re-applying
    // on the new canonical block the position returns to its pre-reorg net -- it is
    // never left flat by the reversal alone.
    let post_reorg_net = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new(symbol)?)
        .await?
        .expect("Position should still exist after the reverse-then-reapply")
        .net;
    assert_eq!(
        post_reorg_net, pre_reorg_net,
        "reverse-then-reapply must restore the position to its pre-reorg net \
         (pre={pre_reorg_net}, post={post_reorg_net})",
    );

    pool.close().await;
    bot.abort();
    Ok(())
}
