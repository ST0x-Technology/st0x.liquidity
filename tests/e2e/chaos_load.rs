//! Chaos tests for resource exhaustion via queue backpressure.
//!
//! Every apalis worker runs with `concurrency(1)`, so the bounded
//! resource under load is worker throughput: a burst of fills becomes a
//! backlog of `AccountForDexTrade` jobs draining serially while the
//! `Position` aggregate accumulates fills behind a pending hedge and
//! the `CheckPositions` scan re-hedges the remainder. These tests
//! assert graceful degradation under that backpressure: no silent data
//! loss, no duplicates, no stuck queues -- convergence regardless of
//! drain speed.
//!
//! OOM, file-descriptor, and CPU exhaustion are not realistically
//! injectable inside a cargo test (allocator aborts kill the whole
//! harness; rlimits are process-global and take Anvil and the mock
//! servers down indiscriminately; CPU burn destabilizes sibling
//! tests). OOM's observable production consequence -- a SIGKILL with
//! the queue mid-drain -- is covered here by the crash-mid-burst
//! scenario.

use st0x_execution::alpaca_broker_api::OrderSide;
use st0x_float_macro::float;
use std::time::Duration;

use crate::hedging::assertions::*;
use crate::poll::{
    connect_db, count_events_of_type, poll_for_all_jobs_done, poll_for_broker_fills,
    poll_for_events, spawn_bot,
};

/// Asserts no offchain order is left in a non-terminal state: `Pending`
/// means a hedge was never re-driven, `Submitted`/`PartiallyFilled`
/// mean status polling was dropped -- either way the queue is stuck.
async fn assert_no_stuck_offchain_orders(db_path: &std::path::Path) -> anyhow::Result<()> {
    let pool = connect_db(db_path).await?;
    let non_terminal: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM offchain_order_view \
         WHERE status IN ('Pending', 'Submitted', 'PartiallyFilled')",
    )
    .fetch_one(&pool)
    .await?;
    pool.close().await;

    assert_eq!(
        non_terminal.0, 0,
        "Backpressure left {} offchain orders stuck in a non-terminal state",
        non_terminal.0,
    );
    Ok(())
}

/// Counts witnessed onchain fills; under bursts every assertion on
/// effects must use exact event counts, never broker order counts
/// (trades legitimately batch into fewer orders). Counts the `Filled`
/// event specifically -- each trade also carries an `Acknowledged`
/// marker (ADR 0005).
async fn assert_witnessed_trades(db_path: &std::path::Path, expected: i64) -> anyhow::Result<()> {
    let pool = connect_db(db_path).await?;
    let witnessed = count_events_of_type(&pool, "OnChainTradeEvent::Filled").await?;
    pool.close().await;

    assert_eq!(
        witnessed, expected,
        "Expected exactly {expected} witnessed trades (one per take, no \
         loss, no duplicates); got {witnessed}",
    );
    Ok(())
}

/// Top-level hypothesis: a burst of fills landing in one backfill range
/// -- N jobs hitting the single-threaded accounting worker at once,
/// with the first hedge held pending at the broker -- must drain with
/// exactly one effect per fill and no stuck queue.
///
/// Scenario: all takes fire BEFORE the bot starts, so the initial
/// checkpoint-driven poll covers every fill in one `BackfillRange` and
/// enqueues the whole burst at once. The broker mock holds the first
/// hedge order unfilled across several status polls, forcing later
/// fills onto the accumulate-while-pending path and the `CheckPositions`
/// re-hedge. Convergence is asserted via broker fill totals (trades may
/// legitimately batch into fewer orders), exact witnessed-trade counts,
/// a fully hedged position, and an empty job queue.
#[test_log::test(tokio::test)]
async fn backfill_burst_hedges_every_take_exactly_once() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let take_amount = float!(2.5);
    let take_count = 8;

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    // Hold each broker order "new" across a few status polls so the
    // backlog accumulates behind a pending hedge instead of degenerating
    // into independent quick hedges.
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new(equity_symbol)?, 4);

    let current_block = infra.base_chain.provider.get_block_number().await?;

    for _ in 0..take_count {
        infra
            .base_chain
            .take_order()
            .symbol(equity_symbol)
            .amount(take_amount)
            .price(onchain_price)
            .direction(TakeDirection::SellEquity)
            .call()
            .await?;
    }

    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot = spawn_bot(ctx);

    // Onchain sells hedge as broker buys; 8 takes of 2.5 shares must
    // fill 20 shares in total, however they batch into orders.
    poll_for_broker_fills(
        &mut bot,
        &infra.broker_service,
        equity_symbol,
        OrderSide::Buy,
        FractionalShares::new(float!(20)),
        Duration::from_secs(120),
    )
    .await;

    poll_for_hedged_position(&mut bot, &infra.db_path, equity_symbol).await;
    poll_for_all_jobs_done(&mut bot, &infra.db_path, i64::from(take_count)).await;

    assert_witnessed_trades(&infra.db_path, i64::from(take_count)).await?;
    assert_no_stuck_offchain_orders(&infra.db_path).await?;

    bot.abort();
    Ok(())
}

/// Top-level hypothesis: a live rapid-fire burst spread across two hot
/// symbols sharing the single accounting worker must not starve either
/// symbol or lose fills across poll-tick boundaries.
///
/// Scenario: orders are prepared upfront (separate owner-account
/// transactions), then taken back-to-back with no pacing while the bot
/// is live -- fills land across multiple 1s monitor ticks, exercising
/// the overlap guard's interplay with the advancing checkpoint under a
/// sustained backlog. Each symbol must converge to a fully hedged
/// position with exactly one witnessed trade per take.
#[test_log::test(tokio::test)]
async fn live_burst_across_two_symbols_loses_nothing() -> anyhow::Result<()> {
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let aapl_amount = float!(2.0);
    let tsla_amount = float!(1.5);
    let takes_per_symbol = 4;

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price), ("TSLA", broker_fill_price)],
        vec![],
    )
    .await?;

    let mut prepared_orders = Vec::new();
    for symbol_and_amount in [("AAPL", aapl_amount), ("TSLA", tsla_amount)] {
        for _ in 0..takes_per_symbol {
            prepared_orders.push(
                infra
                    .base_chain
                    .setup_order()
                    .symbol(symbol_and_amount.0)
                    .amount(symbol_and_amount.1)
                    .price(onchain_price)
                    .direction(TakeDirection::SellEquity)
                    .call()
                    .await?,
            );
        }
    }

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

    // Rapid-fire: no pacing between takes; fills spread across poll
    // ticks purely by transaction timing.
    for prepared in &prepared_orders {
        infra.base_chain.take_prepared_order(prepared).await?;
    }

    poll_for_broker_fills(
        &mut bot,
        &infra.broker_service,
        "AAPL",
        OrderSide::Buy,
        FractionalShares::new(float!(8)),
        Duration::from_secs(120),
    )
    .await;
    poll_for_broker_fills(
        &mut bot,
        &infra.broker_service,
        "TSLA",
        OrderSide::Buy,
        FractionalShares::new(float!(6)),
        Duration::from_secs(120),
    )
    .await;

    poll_for_hedged_position(&mut bot, &infra.db_path, "AAPL").await;
    poll_for_hedged_position(&mut bot, &infra.db_path, "TSLA").await;
    poll_for_all_jobs_done(&mut bot, &infra.db_path, i64::from(takes_per_symbol) * 2).await;

    assert_witnessed_trades(&infra.db_path, i64::from(takes_per_symbol) * 2).await?;
    assert_no_stuck_offchain_orders(&infra.db_path).await?;

    bot.abort();
    Ok(())
}

/// Top-level hypothesis: an abrupt kill with the job queue mid-drain --
/// the observable consequence of an OOM kill -- must not lose or
/// double-process any fill once the bot restarts on the same database.
///
/// Scenario: the burst lands in one backfill range, the bot is killed
/// as soon as the first couple of trades are witnessed (queue still
/// mid-drain: rows Pending or Running, possibly a hedge in flight), and
/// a fresh conductor restarts against the same SQLite file. Recovery
/// must come from the startup orphan requeue plus the
/// `(tx_hash, log_index)` dedupe and the `client_order_id` idempotency
/// anchor: exactly one witnessed trade per take, exact broker fill
/// totals, nothing stuck.
#[test_log::test(tokio::test)]
async fn kill_mid_burst_recovers_every_take_exactly_once() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let take_amount = float!(2.5);
    let take_count = 8;

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    for _ in 0..take_count {
        infra
            .base_chain
            .take_order()
            .symbol(equity_symbol)
            .amount(take_amount)
            .price(onchain_price)
            .direction(TakeDirection::SellEquity)
            .call()
            .await?;
    }

    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot = spawn_bot(ctx);

    // Kill while the queue is still mid-drain: a couple of trades
    // witnessed, the rest of the burst still Pending/Running.
    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Filled", 2).await;
    bot.abort();
    let _ = bot.await;

    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_broker_fills(
        &mut bot2,
        &infra.broker_service,
        equity_symbol,
        OrderSide::Buy,
        FractionalShares::new(float!(20)),
        Duration::from_secs(120),
    )
    .await;

    poll_for_hedged_position(&mut bot2, &infra.db_path, equity_symbol).await;
    poll_for_all_jobs_done(&mut bot2, &infra.db_path, i64::from(take_count)).await;

    assert_witnessed_trades(&infra.db_path, i64::from(take_count)).await?;
    assert_no_stuck_offchain_orders(&infra.db_path).await?;

    bot2.abort();
    Ok(())
}
