//! Chaos tests for database unavailability.
//!
//! Injects SQLite write-lock contention -- a second connection holding
//! `BEGIN IMMEDIATE` against the bot's database file, exactly how a
//! co-located process (e.g. the reporter) contends for the WAL write
//! lock. Asserts the layered posture: transient contention inside the
//! pool's busy timeout rides through with zero data loss, a sustained
//! lock at startup fails fast with a clear database error instead of
//! coming up half-alive, and a restart on the released database resumes
//! via the checkpoint with exactly-once hedging.

use st0x_float_macro::float;
use std::time::Duration;

use crate::chaos::DbLock;
use crate::hedging::assertions::*;
use crate::poll::{poll_for_events, poll_for_events_with_timeout, spawn_bot};

/// Top-level hypothesis: a write lock held for less than the pool's 10s
/// busy timeout (the documented bot-vs-reporter contention scenario)
/// must cause zero data loss and zero duplicates -- every blocked write
/// waits the lock out and proceeds.
///
/// Scenario: the lock is engaged immediately after the take fires, so
/// the fill pipeline (backfill enqueue, checkpoint save, CQRS writes,
/// order placement bookkeeping) runs while the write lock is
/// contended. `configure_sqlite_pool` arms every pooled connection
/// with WAL + a 10s busy timeout; a 5s hold stays inside that budget,
/// so SQLite absorbs the contention silently and the end state must be
/// exactly one hedge.
#[test_log::test(tokio::test)]
async fn transient_write_lock_within_busy_timeout_rides_through() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

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

    let take_result = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // Contend for the write lock while the fill pipeline processes the
    // take. 5s < the pool's 10s busy timeout, so every blocked write
    // must ride it out rather than error.
    let lock = DbLock::acquire(&infra.db_path).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    lock.release().await?;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

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

/// Top-level hypothesis: a database that stays write-locked past the
/// busy timeout must make startup halt cleanly with an error that names
/// the database -- not come up half-alive -- and a restart on the
/// released database must resume from the checkpoint and hedge
/// exactly once.
///
/// Scenario: phase A warms the database (one take fully hedged) and
/// stops the bot. Phase B fires a second take onchain (touching no
/// SQLite), engages the lock, and starts a fresh bot: its unconditional
/// startup writes (orphan requeues, CheckPositions bootstrap) block for
/// the 10s busy timeout, error with SQLITE_BUSY, and the startup error
/// propagates out of the bot task. Phase C releases the lock and starts
/// the bot again: the backfill checkpoint never advanced past take #2,
/// so it is re-ingested and hedged exactly once.
#[test_log::test(tokio::test)]
async fn sustained_lock_at_startup_fails_fast_and_restart_resumes() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let sell_amount = float!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    // Phase A: warm run -- one take hedged end-to-end, then stop.
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let take1 = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;
    bot.abort();
    let _ = bot.await;

    // Phase B: take #2 lands onchain while the bot is down, then the
    // database is locked and a fresh bot starts against it.
    let take2 = infra
        .base_chain
        .take_order()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let lock = DbLock::acquire(&infra.db_path).await?;

    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let bot2 = spawn_bot(ctx2);

    // The first unconditional startup write blocks for the 10s busy
    // timeout and then errors; the error must propagate out of the bot
    // task rather than leave it running half-alive.
    let startup_result = tokio::time::timeout(Duration::from_secs(60), bot2)
        .await
        .expect("bot must halt within 60s when the database stays locked")
        .expect("bot task must not panic");

    let startup_error =
        startup_result.expect_err("startup against a write-locked database must fail, not come up");
    let error_chain = format!("{startup_error:#}");
    assert!(
        error_chain.contains("database is locked"),
        "Startup failure must clearly name the database lock; got: {error_chain}",
    );

    lock.release().await?;

    // Phase C: restart on the released database; take #2 must be
    // re-ingested from the checkpoint and hedged exactly once.
    let ctx3 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .assets(infra.assets_config())
        .call()?;
    let mut bot3 = spawn_bot(ctx3);

    poll_for_events_with_timeout(
        &mut bot3,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        2,
        Duration::from_secs(120),
    )
    .await;

    let expected_position = ExpectedPosition::builder()
        .symbol(equity_symbol)
        .amount(float!(21.5))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(0))
        .expected_accumulated_short(float!(21.5))
        .expected_net(float!(0))
        .build();

    assert_full_hedging_flow(
        &[expected_position],
        &[take1, take2],
        &infra.base_chain.provider,
        infra.base_chain.orderbook,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot3.abort();
    Ok(())
}
