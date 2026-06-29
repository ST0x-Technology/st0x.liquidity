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
//!
//! [`reorg_reverts_dropped_fill`] is the companion case: a fill that the reorg
//! DROPS rather than re-mines. It never re-appears, so the correct recovery is
//! reverse-ONLY (no re-application), restoring the position to its pre-fill net.
//! That test pins a KNOWN GAP and is expected to fail against the current
//! detector (which only flags re-observed fills); see its own doc comment.

use alloy::providers::Provider;
use std::time::Duration;

use st0x_float_macro::float;

use crate::chaos::ChaosProxy;
use crate::hedging::assertions::{
    FractionalShares, Position, Projection, Symbol, TakeDirection, TestInfra, build_ctx,
    poll_for_position_net, spawn_bot,
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

/// Pins the DROPPED-fill reorg gap: a fill the bot has witnessed and hedged is
/// orphaned by a reorg and -- unlike the re-mined case in
/// [`reorg_reverts_witnessed_fill`] -- never re-appears on the canonical chain.
///
/// The correct end state is a reverse-ONLY recovery: the fill's position impact
/// is backed out (`OnChainTradeEvent::Reorged` + `PositionEvent::Reorged`, each
/// exactly once), the original `Filled` events are preserved (append-only), and
/// there is NO re-application -- `OnChainTradeEvent::ReWitnessed` stays at zero
/// and the position's `OnChainOrderFilled` stays at the original one. The net
/// returns to the PRE-FILL value rather than the post-fill value: the dropped
/// fill is gone for good and is never replayed.
///
/// The drop is injected through the [`ChaosProxy::fork_drop_get_logs`]
/// perturbation: a forked RPC node serves the fill's block height under a
/// competing block hash but no longer carries the witnessed fill's own
/// `(tx_hash, log_index)` -- the fill is orphaned and never re-observed.
///
/// This test is EXPECTED TO FAIL against the current production detector. The
/// authoritative reorg detector, `detect_block_hash_reorgs`, only flags a fill
/// whose SAME `(tx_hash, log_index)` is re-observed on a different `block_hash`
/// (the re-mined case). A fill that is dropped and never re-appears is never
/// re-observed, so it is never compared and never flagged; the `removed: true`
/// path that would otherwise catch it is inert because `eth_getLogs` range
/// queries never return removed logs, and the forward scan never regresses
/// below the checkpoint. The bot therefore emits no `Reorged` events and the
/// `poll_for_events` waits below time out -- the dropped fill leaves a naked
/// hedge in production. It should go green once absence / ancestry
/// re-verification of unfinalized witnessed fills is added to the design.
#[test_log::test(tokio::test)]
async fn reorg_reverts_dropped_fill() -> anyhow::Result<()> {
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

    // Capture the PRE-FILL net. Before the first fill the symbol has no position
    // at all -- the aggregate is created by the witness, so `Position::load`
    // returns `None` and the net is zero by definition. Bind it directly rather
    // than reading the database: the bot may still be running migrations this
    // early (the poll helpers below tolerate a not-yet-ready DB for the same
    // reason). A reverse-only reorg (drop, no re-application) must restore
    // exactly this pre-fill net: the dropped fill is backed out and never
    // replayed.
    let pre_fill_net = FractionalShares::ZERO;

    // Ingest a fill and let the bot witness and hedge it. Witnessing serves the
    // fill's log through the proxy, which caches it for the drop perturbation
    // below to re-serve as an orphan-block marker under a competing block hash.
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

    // Simulate the DROP: a forked RPC node serves the fill's block height under a
    // competing block hash but no longer carries the witnessed fill -- the fill
    // is orphaned and never re-appears. The count covers both event-signature
    // polls across several rounds.
    chaos.fork_drop_get_logs(10).await;

    // The bot must detect the orphaned fill and reverse it exactly once across
    // both aggregates. This is where the test fails against current production:
    // the dropped fill is never re-observed, so `detect_block_hash_reorgs` never
    // flags it and these waits time out.
    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Reorged", 1).await;
    poll_for_events(&mut bot, &infra.db_path, "PositionEvent::Reorged", 1).await;

    // `poll_for_events` gates on the `events` table, but the final net assertion
    // reads `net` via `Projection::load`, which reads the `position_view`
    // materialized view. The view is updated by a reactor dispatched after the
    // event commit, in a separate transaction, so the `PositionEvent::Reorged`
    // row can be visible in `events` before the view reflects the reversal --
    // reading `net` here without waiting would observe the still-applied fill
    // net, not the reverse-only result. Wait until the reversal lands in the
    // view's net before asserting. (The witnessed case gates on a later position
    // event, which gives the view time to catch up; the dropped case has no such
    // later event, so it must wait explicitly.)
    poll_for_position_net(&mut bot, &infra.db_path, symbol, pre_fill_net).await;

    let pool = connect_db(&infra.db_path).await?;

    // The reversal is append-only and asserted against the event stream: the
    // original witness survives, the reversal is a new event, nothing is deleted.
    assert_eq!(
        count_events_of_type(&pool, "OnChainTradeEvent::Filled").await?,
        1,
        "the original witness must be preserved, not deleted",
    );
    assert_eq!(
        count_events_of_type(&pool, "OnChainTradeEvent::Reorged").await?,
        1,
        "the dropped fill must be marked reorged exactly once, not removed",
    );
    assert_eq!(
        count_events_of_type(&pool, "PositionEvent::Reorged").await?,
        1,
        "the reversal must back out the fill's position impact exactly once",
    );

    // A dropped fill is gone for good: unlike the re-mined case there is no
    // re-witness and no second position fill -- the original is the only one.
    assert_eq!(
        count_events_of_type(&pool, "OnChainTradeEvent::ReWitnessed").await?,
        0,
        "a dropped fill must never be re-witnessed -- it does not re-appear",
    );
    assert_eq!(
        count_events_of_type(&pool, "PositionEvent::OnChainOrderFilled").await?,
        1,
        "the dropped fill must be applied exactly once and never re-applied",
    );

    // The core invariant: reverse-only restores the position to its PRE-FILL net.
    // The fill is backed out and never re-applied, so the position returns to
    // exactly where it was before the fill -- not to the post-fill value.
    let post_reorg_net = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new(symbol)?)
        .await?
        .expect("Position should still exist after the reversal")
        .net;
    assert_eq!(
        post_reorg_net, pre_fill_net,
        "reverse-only must restore the position to its pre-fill net \
         (pre={pre_fill_net}, post={post_reorg_net})",
    );

    pool.close().await;
    bot.abort();
    Ok(())
}
