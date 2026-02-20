//! E2E hedging tests exercising the full bot lifecycle.
//!
//! Each test starts a real Anvil fork, a mock broker, and launches the bot
//! via `launch()`. Tests verify that the entire pipeline -- from onchain
//! event detection through CQRS processing to offchain order fills -- works
//! correctly under various hedging conditions.
//!
//! Every hedging test calls `assert_full_pipeline` which checks broker state,
//! onchain vault balances, and all CQRS events/views comprehensively.

mod common;
mod services;

use std::time::Duration;

use alloy::providers::Provider;
use rust_decimal_macros::dec;
use st0x_event_sorcery::Projection;
use st0x_execution::{FractionalShares, Positive, Symbol};
use st0x_hedge::config::ExecutionThreshold;
use st0x_hedge::{OffchainOrder, Position};

use common::{
    ExpectedPosition, assert_full_hedging_flow, build_ctx, connect_db, count_all_domain_events,
    count_events, count_processed_queue_events, count_queued_events, spawn_bot,
    wait_for_processing,
};
use services::TestInfra;
use services::base_chain::TakeDirection;

#[test_log::test(tokio::test)]
async fn e2e_hedging_via_launch() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.25);
    let sell_amount = dec!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    let expected_position = ExpectedPosition {
        symbol: equity_symbol,
        amount: sell_amount,
        direction: TakeDirection::SellEquity,
        onchain_price,
        broker_fill_price,
        expected_accumulated_long: dec!(0),
        expected_accumulated_short: sell_amount,
        // post-hedge net should go back to 0.
        expected_net: dec!(0),
    };

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
    )?;
    let bot = spawn_bot(ctx);

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

    wait_for_processing(&bot, 8).await;

    assert_full_hedging_flow(
        &[expected_position],
        &[take_result],
        &infra.base_chain.provider,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot.abort();
    Ok(())
}

#[test_log::test(tokio::test)]
async fn multi_asset_sustained_load() -> anyhow::Result<()> {
    let aapl_onchain = dec!(190.00);
    let aapl_broker = dec!(185.50);
    let tsla_onchain = dec!(250.00);
    let tsla_broker = dec!(245.00);
    let msft_onchain = dec!(415.00);
    let msft_broker = dec!(410.75);
    let trade_amount = dec!(5.25);

    let infra = TestInfra::start(
        vec![
            ("AAPL", aapl_broker),
            ("TSLA", tsla_broker),
            ("MSFT", msft_broker),
        ],
        vec![],
    )
    .await?;

    // Mix of directions: AAPL sell, TSLA buy, MSFT sell
    let expected_positions = [
        ExpectedPosition {
            symbol: "AAPL",
            amount: trade_amount,
            direction: TakeDirection::SellEquity,
            onchain_price: aapl_onchain,
            broker_fill_price: aapl_broker,
            expected_accumulated_long: dec!(0),
            expected_accumulated_short: trade_amount,
            expected_net: dec!(0),
        },
        ExpectedPosition {
            symbol: "TSLA",
            amount: trade_amount,
            direction: TakeDirection::BuyEquity,
            onchain_price: tsla_onchain,
            broker_fill_price: tsla_broker,
            expected_accumulated_long: trade_amount,
            expected_accumulated_short: dec!(0),
            expected_net: dec!(0),
        },
        ExpectedPosition {
            symbol: "MSFT",
            amount: trade_amount,
            direction: TakeDirection::SellEquity,
            onchain_price: msft_onchain,
            broker_fill_price: msft_broker,
            expected_accumulated_long: dec!(0),
            expected_accumulated_short: trade_amount,
            expected_net: dec!(0),
        },
    ];

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
    )?;
    let bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Space trades so each gets individually hedged before the next
    // position event arrives on the same symbol.
    let mut take_results = Vec::new();
    for expected_position in &expected_positions {
        take_results.push(
            infra
                .base_chain
                .take_order()
                .symbol(expected_position.symbol)
                .amount(trade_amount)
                .price(expected_position.onchain_price)
                .direction(expected_position.direction)
                .call()
                .await?,
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    wait_for_processing(&bot, 12).await;

    assert_full_hedging_flow(
        &expected_positions,
        &take_results,
        &infra.base_chain.provider,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot.abort();
    Ok(())
}

#[test_log::test(tokio::test)]
async fn backfilling() -> anyhow::Result<()> {
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.00);
    let sell_amount = dec!(4.5);
    let trade_count: i64 = 3;

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    // Record the block BEFORE any take-orders (subtract 1 for safety margin)
    let pre_trade_block = infra
        .base_chain
        .provider
        .get_block_number()
        .await?
        .saturating_sub(1);

    // Execute 3 take-orders BEFORE starting the bot
    let mut take_results = Vec::new();
    for _ in 0..trade_count {
        take_results.push(
            infra
                .base_chain
                .take_order()
                .symbol("AAPL")
                .amount(sell_amount)
                .price(onchain_price)
                .direction(TakeDirection::SellEquity)
                .call()
                .await?,
        );
    }

    // Mine an extra block to ensure all trades are finalized
    infra.base_chain.mine_blocks(1).await?;

    // Start bot with deployment_block set to BEFORE the first take-order
    let ctx = build_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        pre_trade_block,
    )?;
    let bot = spawn_bot(ctx);

    // Wait for backfill + processing. With 3 rapid backfilled trades, only
    // 1 offchain order is placed initially (the bot won't place a second
    // while the first is pending). After the first fill, the remaining net
    // needs the position checker's cycle (2s in tests) to detect and hedge.
    wait_for_processing(&bot, 12).await;

    // Verify all historical events were picked up via backfill
    let pool = connect_db(&infra.db_path).await?;
    let queued = count_queued_events(&pool).await?;
    assert!(
        queued >= trade_count,
        "Expected at least {trade_count} queued events from backfill, got {queued}"
    );
    let processed = count_processed_queue_events(&pool).await?;
    assert_eq!(processed, queued, "All queued events should be processed");
    pool.close().await;

    // Full pipeline: 3 sells of 4.5 each, all hedged -> net = 0
    let expected_position = ExpectedPosition {
        symbol: "AAPL",
        amount: dec!(13.5),
        direction: TakeDirection::SellEquity,
        onchain_price,
        broker_fill_price,
        expected_accumulated_long: dec!(0),
        expected_accumulated_short: dec!(13.5),
        expected_net: dec!(0),
    };

    assert_full_hedging_flow(
        &[expected_position],
        &take_results,
        &infra.base_chain.provider,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot.abort();
    Ok(())
}

#[test_log::test(tokio::test)]
async fn resumption_after_shutdown() -> anyhow::Result<()> {
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.00);
    let sell_amount = dec!(8.3);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    // Phase 1: Start bot, process 1 trade, wait for fill
    let ctx = build_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
    )?;
    let bot = spawn_bot(ctx);
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

    wait_for_processing(&bot, 10).await;

    // Snapshot DB state before shutdown
    let pool = connect_db(&infra.db_path).await?;
    let pre_shutdown_domain_events = count_all_domain_events(&pool).await?;
    pool.close().await;

    // Abort the bot (simulates graceful shutdown)
    bot.abort();
    let _ = bot.await;

    // Phase 2: Execute 1 more take-order while bot is down
    let take2 = infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // Restart bot with same db_path
    let ctx2 = build_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
    )?;
    let bot2 = spawn_bot(ctx2);

    // Wait for backfill to pick up missed events + processing
    wait_for_processing(&bot2, 12).await;

    // Verify no duplicate CQRS events
    let pool = connect_db(&infra.db_path).await?;
    let post_restart_domain_events = count_all_domain_events(&pool).await?;
    assert!(
        post_restart_domain_events > pre_shutdown_domain_events,
        "Should have new events after restart: pre={pre_shutdown_domain_events}, \
         post={post_restart_domain_events}"
    );
    pool.close().await;

    // Full pipeline: 2 sells of 8.3 each, both hedged
    let expected_position = ExpectedPosition {
        symbol: "AAPL",
        amount: dec!(16.6),
        direction: TakeDirection::SellEquity,
        onchain_price,
        broker_fill_price,
        expected_accumulated_long: dec!(0),
        expected_accumulated_short: dec!(16.6),
        expected_net: dec!(0),
    };

    assert_full_hedging_flow(
        &[expected_position],
        &[take1, take2],
        &infra.base_chain.provider,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot2.abort();
    Ok(())
}

#[test_log::test(tokio::test)]
async fn crash_recovery_eventual_consistency() -> anyhow::Result<()> {
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.00);
    let sell_amount = dec!(6.75);

    let expected_positions = [
        ExpectedPosition {
            symbol: "AAPL",
            amount: sell_amount,
            direction: TakeDirection::SellEquity,
            onchain_price,
            broker_fill_price,
            expected_accumulated_long: dec!(0),
            expected_accumulated_short: sell_amount,
            expected_net: dec!(0),
        },
        ExpectedPosition {
            symbol: "TSLA",
            amount: sell_amount,
            direction: TakeDirection::SellEquity,
            onchain_price,
            broker_fill_price,
            expected_accumulated_long: dec!(0),
            expected_accumulated_short: sell_amount,
            expected_net: dec!(0),
        },
    ];

    // ── Reference run: uninterrupted ────────────────────────────────

    let ref_infra = TestInfra::start(
        vec![("AAPL", broker_fill_price), ("TSLA", broker_fill_price)],
        vec![],
    )
    .await?;

    let ref_block = ref_infra.base_chain.provider.get_block_number().await?;
    let ref_ctx = build_ctx(
        &ref_infra.base_chain,
        &ref_infra.broker_service,
        &ref_infra.db_path,
        ref_block,
    )?;
    let ref_bot = spawn_bot(ref_ctx);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut ref_take_results = Vec::new();
    for expected_position in &expected_positions {
        ref_take_results.push(
            ref_infra
                .base_chain
                .take_order()
                .symbol(expected_position.symbol)
                .amount(sell_amount)
                .price(onchain_price)
                .direction(expected_position.direction)
                .call()
                .await?,
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    wait_for_processing(&ref_bot, 10).await;

    // Assert reference run passes full pipeline
    assert_full_hedging_flow(
        &expected_positions,
        &ref_take_results,
        &ref_infra.base_chain.provider,
        ref_infra.base_chain.orderbook_addr,
        ref_infra.base_chain.owner,
        &ref_infra.broker_service,
        &ref_infra.db_path.display().to_string(),
    )
    .await?;

    let ref_pool = connect_db(&ref_infra.db_path).await?;
    let ref_domain_events = count_all_domain_events(&ref_pool).await?;
    ref_pool.close().await;
    ref_bot.abort();
    let _ = ref_bot.await;

    // ── Crash run: same trades, with interruption ───────────────────

    let crash_infra = TestInfra::start(
        vec![("AAPL", broker_fill_price), ("TSLA", broker_fill_price)],
        vec![],
    )
    .await?;

    let crash_block = crash_infra.base_chain.provider.get_block_number().await?;

    // Phase 1: process first trade, then crash
    let ctx1 = build_ctx(
        &crash_infra.base_chain,
        &crash_infra.broker_service,
        &crash_infra.db_path,
        crash_block,
    )?;
    let bot1 = spawn_bot(ctx1);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let crash_take1 = crash_infra
        .base_chain
        .take_order()
        .symbol(expected_positions[0].symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(expected_positions[0].direction)
        .call()
        .await?;
    wait_for_processing(&bot1, 10).await;
    bot1.abort();
    let _ = bot1.await;

    // Phase 2: submit remaining trade and restart
    let crash_take2 = crash_infra
        .base_chain
        .take_order()
        .symbol(expected_positions[1].symbol)
        .amount(sell_amount)
        .price(onchain_price)
        .direction(expected_positions[1].direction)
        .call()
        .await?;

    let ctx2 = build_ctx(
        &crash_infra.base_chain,
        &crash_infra.broker_service,
        &crash_infra.db_path,
        crash_block,
    )?;
    let bot2 = spawn_bot(ctx2);

    wait_for_processing(&bot2, 12).await;

    // Assert crash run also passes full pipeline (convergence)
    assert_full_hedging_flow(
        &expected_positions,
        &[crash_take1, crash_take2],
        &crash_infra.base_chain.provider,
        crash_infra.base_chain.orderbook_addr,
        crash_infra.base_chain.owner,
        &crash_infra.broker_service,
        &crash_infra.db_path.display().to_string(),
    )
    .await?;

    // Domain event count should be at least as many as reference (crash run
    // may generate additional events from re-initialization, position checker
    // detecting pending states on restart, etc.)
    let crash_pool = connect_db(&crash_infra.db_path).await?;
    let crash_domain_events = count_all_domain_events(&crash_pool).await?;
    crash_pool.close().await;

    assert!(
        crash_domain_events >= ref_domain_events,
        "Crash run should have at least as many events as reference: \
         crash={crash_domain_events}, reference={ref_domain_events}"
    );

    bot2.abort();
    Ok(())
}

#[test_log::test(tokio::test)]
async fn market_hours_transitions() -> anyhow::Result<()> {
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.00);
    let sell_amount = dec!(12.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    // Start with market CLOSED
    infra.broker_service.set_market_closed();

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
    )?;
    let bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Execute take-order -- events enqueue and positions accumulate
    let take_result = infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // Wait for event processing but not order placement (market closed)
    wait_for_processing(&bot, 8).await;

    // Assert: position accumulated but NO offchain orders placed
    let pool = connect_db(&infra.db_path).await?;
    let position = Projection::<Position>::sqlite(pool.clone())?
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should exist even when market is closed");
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(sell_amount),
        "Should accumulate short even when market closed"
    );

    let offchain_orders = Projection::<OffchainOrder>::sqlite(pool.clone())?
        .load_all()
        .await?;
    assert!(
        offchain_orders.is_empty(),
        "No offchain orders should be placed when market is closed"
    );

    let onchain_events = count_events(&pool, "OnChainTrade").await?;
    assert!(
        onchain_events >= 1,
        "OnChainTrade events should be persisted even with market closed"
    );
    pool.close().await;

    // Switch calendar to market OPEN
    infra.broker_service.set_market_open();

    // Wait for the position checker's periodic cycle (2s in tests) plus processing.
    // The checker detects the pending position and places orders.
    wait_for_processing(&bot, 8).await;

    // Full pipeline assertions after market opens
    let expected_position = ExpectedPosition {
        symbol: "AAPL",
        amount: sell_amount,
        direction: TakeDirection::SellEquity,
        onchain_price,
        broker_fill_price,
        expected_accumulated_long: dec!(0),
        expected_accumulated_short: sell_amount,
        expected_net: dec!(0),
    };

    assert_full_hedging_flow(
        &[expected_position],
        &[take_result],
        &infra.base_chain.provider,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    // Verify no duplicate event processing
    let pool = connect_db(&infra.db_path).await?;
    let queued = count_queued_events(&pool).await?;
    let processed = count_processed_queue_events(&pool).await?;
    assert_eq!(
        queued, processed,
        "All queued events should be processed exactly once"
    );
    pool.close().await;

    bot.abort();
    Ok(())
}

/// Opposing trades cancel out, so no offchain hedge is placed.
///
/// Uses a high execution threshold so individual trades don't trigger
/// hedging. After a SellEquity and BuyEquity of equal size, net = 0
/// and no offchain orders should exist.
#[test_log::test(tokio::test)]
async fn opposing_trades_no_hedge() -> anyhow::Result<()> {
    // Price must have an exact decimal reciprocal (1/200 = 0.005) so the
    // BuyEquity ioRatio round-trips without precision artifacts. Otherwise
    // sell 14.75 + buy 14.75 won't net to exactly zero onchain.
    let onchain_price = dec!(200.00);
    let broker_fill_price = dec!(195.00);
    let trade_amount = dec!(14.75);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    let expected_position = ExpectedPosition {
        symbol: "AAPL",
        amount: dec!(0),
        direction: TakeDirection::NetZero,
        onchain_price,
        broker_fill_price,
        expected_accumulated_long: trade_amount,
        expected_accumulated_short: trade_amount,
        expected_net: dec!(0),
    };

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let mut ctx = build_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
    )?;

    // High threshold: 200 shares -- well above any single trade, so
    // individual trades won't trigger hedging.
    let high_threshold = Positive::<FractionalShares>::new(FractionalShares::new(dec!(200)))?;
    ctx.execution_threshold = ExecutionThreshold::Shares(high_threshold);

    let bot = spawn_bot(ctx);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // SellEquity 14.75 shares -- below 200-share threshold
    let take_result_sell = infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // BuyEquity 14.75 shares -- net goes to 0
    let take_result_buy = infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    wait_for_processing(&bot, 8).await;

    assert_full_hedging_flow(
        &[expected_position],
        &[take_result_sell, take_result_buy],
        &infra.base_chain.provider,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path.display().to_string(),
    )
    .await?;

    bot.abort();
    Ok(())
}

// ── Chain reorganization (stub) ──────────────────────────────────────

#[test_log::test(tokio::test)]
#[ignore = "Requires Anvil reorg support (removed=true WebSocket notifications)"]
async fn chain_reorganization() -> anyhow::Result<()> {
    // TODO: Anvil's evm_snapshot/evm_revert don't generate proper reorg
    // notifications on WebSocket subscriptions. When Anvil adds anvil_reorg
    // support that triggers removed=true log events, implement:
    //
    // 1. Start bot, process a take-order
    // 2. Snapshot chain state before the take-order
    // 3. Revert to snapshot (simulating reorg that removes the trade)
    // 4. Mine new blocks with different transactions
    // 5. Assert bot detects reorg and handles removed events
    // 6. Assert final state only reflects the replacement transactions
    Ok(())
}
