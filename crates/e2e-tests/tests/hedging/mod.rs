//! E2E hedging tests exercising the full bot lifecycle.
//!
//! Each test starts a real Anvil fork, a mock broker, and launches the bot
//! via `launch()`. Tests verify that the entire pipeline -- from onchain
//! event detection through CQRS processing to offchain order fills -- works
//! correctly under various hedging conditions.
//!
//! Every hedging test calls `assert_full_hedging_flow` which checks broker state,
//! onchain vault balances, and all CQRS events/views comprehensively.

mod utils;

use self::utils::*;

#[test_log::test(tokio::test)]
async fn e2e_hedging_via_launch() -> anyhow::Result<()> {
    let equity_symbol = "AAPL";
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.25);
    let sell_amount = dec!(10.75);

    let infra = TestInfra::start(vec![(equity_symbol, broker_fill_price)], vec![]).await?;

    let expected_position = ExpectedPosition::builder()
        .symbol(equity_symbol)
        .amount(sell_amount)
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(0))
        .expected_accumulated_short(sell_amount)
        .expected_net(dec!(0))
        .build();

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    // Wait for bot's WebSocket + initial setup before submitting orders.
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

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

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

/// Control test: a direct high-precision sell-side Raindex `ioRatio` literal
/// should still hedge successfully.
///
/// This keeps a high-scale decimal in the order expression without going
/// through buy-side reciprocal generation, so it isolates reciprocal/dust bugs
/// from direct price literal handling in the hedging path.
#[test_log::test(tokio::test)]
async fn direct_high_precision_sell_price_still_hedges() -> anyhow::Result<()> {
    let onchain_price = Decimal::from_str("112.50000000000000000000000002")?;
    let broker_fill_price = dec!(113.57);
    let trade_amount = dec!(12.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    let pool = connect_db(&infra.db_path).await?;

    let offchain_order_events = count_events(&pool, "OffchainOrder").await?;
    assert_eq!(
        offchain_order_events, 3,
        "Expected exact OffchainOrder success event sequence for a single hedge",
    );

    let position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should exist after the trade");

    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(trade_amount),
        "Sell-side onchain fill should accumulate short shares",
    );
    assert_eq!(
        position.net,
        FractionalShares::ZERO,
        "Position should be fully hedged after offchain fill",
    );
    assert_eq!(
        position.last_price_usdc.map(|price| price.round_dp(2)),
        Some(dec!(112.50)),
        "High-precision direct onchain price should still round to expected cents",
    );

    pool.close().await;
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
        ExpectedPosition::builder()
            .symbol("AAPL")
            .amount(trade_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(aapl_onchain)
            .broker_fill_price(aapl_broker)
            .expected_accumulated_long(dec!(0))
            .expected_accumulated_short(trade_amount)
            .expected_net(dec!(0))
            .build(),
        ExpectedPosition::builder()
            .symbol("TSLA")
            .amount(trade_amount)
            .direction(TakeDirection::BuyEquity)
            .onchain_price(tsla_onchain)
            .broker_fill_price(tsla_broker)
            .expected_accumulated_long(trade_amount)
            .expected_accumulated_short(dec!(0))
            .expected_net(dec!(0))
            .build(),
        ExpectedPosition::builder()
            .symbol("MSFT")
            .amount(trade_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(msft_onchain)
            .broker_fill_price(msft_broker)
            .expected_accumulated_long(dec!(0))
            .expected_accumulated_short(trade_amount)
            .expected_net(dec!(0))
            .build(),
    ];

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

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

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 3).await;

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
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(pre_trade_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    // With 3 rapid backfilled trades, the number of offchain fill events
    // is non-deterministic (depends on position checker batching). Poll
    // for the position reaching net=0 instead of counting fill events.
    poll_for_hedged_position(&mut bot, &infra.db_path, "AAPL").await;

    // Verify all historical events were picked up via backfill
    let pool = connect_db(&infra.db_path).await?;
    let queued = count_queued_events(&pool).await?;
    assert_eq!(
        queued, trade_count,
        "Expected exact queued event count from backfill",
    );
    let processed = count_processed_queue_events(&pool).await?;
    assert_eq!(processed, queued, "All queued events should be processed");
    pool.close().await;

    let expected_position = ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(dec!(13.5))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(0))
        .expected_accumulated_short(dec!(13.5))
        .expected_net(dec!(0))
        .build();

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
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
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

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    let pool = connect_db(&infra.db_path).await?;
    let pre_shutdown_onchain_events = count_events(&pool, "OnChainTrade").await?;
    let pre_shutdown_position_events = count_events(&pool, "Position").await?;
    let pre_shutdown_offchain_events = count_events(&pool, "OffchainOrder").await?;
    pool.close().await;

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

    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events(&mut bot2, &infra.db_path, "OffchainOrderEvent::Filled", 2).await;

    // Restart should process new events (the take-order while bot was down)
    let pool = connect_db(&infra.db_path).await?;
    let post_restart_onchain_events = count_events(&pool, "OnChainTrade").await?;
    let post_restart_position_events = count_events(&pool, "Position").await?;
    let post_restart_offchain_events = count_events(&pool, "OffchainOrder").await?;
    assert_eq!(
        post_restart_onchain_events,
        pre_shutdown_onchain_events + 1,
        "Restart should persist exactly one new OnChainTrade event",
    );
    assert_eq!(
        post_restart_position_events,
        pre_shutdown_position_events + 3,
        "Restart should emit exact Position success transition events for one hedge",
    );
    assert_eq!(
        post_restart_offchain_events,
        pre_shutdown_offchain_events + 3,
        "Restart should emit exact OffchainOrder success event sequence for one hedge",
    );
    pool.close().await;

    let expected_position = ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(dec!(16.6))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(0))
        .expected_accumulated_short(dec!(16.6))
        .expected_net(dec!(0))
        .build();

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
        ExpectedPosition::builder()
            .symbol("AAPL")
            .amount(sell_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(onchain_price)
            .broker_fill_price(broker_fill_price)
            .expected_accumulated_long(dec!(0))
            .expected_accumulated_short(sell_amount)
            .expected_net(dec!(0))
            .build(),
        ExpectedPosition::builder()
            .symbol("TSLA")
            .amount(sell_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(onchain_price)
            .broker_fill_price(broker_fill_price)
            .expected_accumulated_long(dec!(0))
            .expected_accumulated_short(sell_amount)
            .expected_net(dec!(0))
            .build(),
    ];

    // ── Reference run: uninterrupted ────────────────────────────────

    let ref_infra = TestInfra::start(
        vec![("AAPL", broker_fill_price), ("TSLA", broker_fill_price)],
        vec![],
    )
    .await?;

    let ref_block = ref_infra.base_chain.provider.get_block_number().await?;
    let ref_ctx = build_ctx()
        .chain(&ref_infra.base_chain)
        .broker(&ref_infra.broker_service)
        .db_path(&ref_infra.db_path)
        .deployment_block(ref_block)
        .call()?;
    let mut ref_bot = spawn_bot(ref_ctx);

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

    poll_for_events(
        &mut ref_bot,
        &ref_infra.db_path,
        "OffchainOrderEvent::Filled",
        2,
    )
    .await;

    // Also wait for the downstream Position events so the event count
    // snapshot captures the full pipeline (not just the offchain fill).
    poll_for_events(
        &mut ref_bot,
        &ref_infra.db_path,
        "PositionEvent::OffChainOrderFilled",
        2,
    )
    .await;

    // Abort the bot immediately after the pipeline completes to stop
    // background tasks (inventory poller) from emitting additional
    // events that would inflate the reference count non-deterministically.
    ref_bot.abort();
    let _ = ref_bot.await;

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
    let ref_queued_events = count_queued_events(&ref_pool).await?;
    let ref_processed_queue_events = count_processed_queue_events(&ref_pool).await?;
    let ref_onchain_events = count_events(&ref_pool, "OnChainTrade").await?;
    let ref_offchain_events = count_events(&ref_pool, "OffchainOrder").await?;
    ref_pool.close().await;

    // ── Crash run: same trades, with interruption ───────────────────

    let crash_infra = TestInfra::start(
        vec![("AAPL", broker_fill_price), ("TSLA", broker_fill_price)],
        vec![],
    )
    .await?;

    let crash_block = crash_infra.base_chain.provider.get_block_number().await?;

    // Phase 1: process first trade, then crash
    let ctx1 = build_ctx()
        .chain(&crash_infra.base_chain)
        .broker(&crash_infra.broker_service)
        .db_path(&crash_infra.db_path)
        .deployment_block(crash_block)
        .call()?;
    let mut bot1 = spawn_bot(ctx1);

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
    poll_for_events(
        &mut bot1,
        &crash_infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
    )
    .await;
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

    let ctx2 = build_ctx()
        .chain(&crash_infra.base_chain)
        .broker(&crash_infra.broker_service)
        .db_path(&crash_infra.db_path)
        .deployment_block(crash_block)
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events(
        &mut bot2,
        &crash_infra.db_path,
        "OffchainOrderEvent::Filled",
        2,
    )
    .await;

    poll_for_events(
        &mut bot2,
        &crash_infra.db_path,
        "PositionEvent::OffChainOrderFilled",
        2,
    )
    .await;

    // Abort immediately to stop background events from accumulating
    bot2.abort();
    let _ = bot2.await;

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

    let crash_pool = connect_db(&crash_infra.db_path).await?;
    let crash_queued_events = count_queued_events(&crash_pool).await?;
    let crash_processed_queue_events = count_processed_queue_events(&crash_pool).await?;
    let crash_onchain_events = count_events(&crash_pool, "OnChainTrade").await?;
    let crash_offchain_events = count_events(&crash_pool, "OffchainOrder").await?;
    crash_pool.close().await;

    assert_eq!(
        crash_queued_events, ref_queued_events,
        "Crash recovery should enqueue the exact same number of onchain events as reference",
    );
    assert_eq!(
        crash_processed_queue_events, ref_processed_queue_events,
        "Crash recovery should process the exact same number of queued events as reference",
    );
    assert_eq!(
        crash_onchain_events, ref_onchain_events,
        "Crash recovery should persist the exact same OnChainTrade event count as reference",
    );
    assert_eq!(
        crash_offchain_events, ref_offchain_events,
        "Crash recovery should persist the exact same OffchainOrder event count as reference",
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn market_hours_transitions() -> anyhow::Result<()> {
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.00);
    let sell_amount = dec!(12.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    infra.broker_service.set_market_closed();

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let take_result = infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // Wait for onchain trade processing (no offchain order — market closed)
    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Filled", 1).await;

    let pool = connect_db(&infra.db_path).await?;
    let position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should exist even when market is closed");
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(sell_amount),
        "Should accumulate short even when market closed"
    );

    let offchain_orders = Projection::<OffchainOrder>::sqlite(pool.clone())
        .load_all()
        .await?;
    assert!(
        offchain_orders.is_empty(),
        "No offchain orders should be placed when market is closed"
    );

    let offchain_order_events = count_events(&pool, "OffchainOrder").await?;
    assert_eq!(
        offchain_order_events, 0,
        "No offchain order events should be emitted when market is closed"
    );

    let onchain_events = count_events(&pool, "OnChainTrade").await?;
    assert_eq!(
        onchain_events, 1,
        "Exactly one OnChainTrade event should be persisted while market is closed"
    );
    pool.close().await;

    infra.broker_service.set_market_open();

    // The position checker detects the pending position and places orders.
    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    let expected_position = ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(sell_amount)
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(0))
        .expected_accumulated_short(sell_amount)
        .expected_net(dec!(0))
        .build();

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

    let expected_position = ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(dec!(0))
        .direction(TakeDirection::NetZero)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(trade_amount)
        .expected_accumulated_short(trade_amount)
        .expected_net(dec!(0))
        .build();

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let mut ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;

    // High threshold: 200 shares -- well above any single trade, so
    // individual trades won't trigger hedging.
    let high_threshold = Positive::<FractionalShares>::new(FractionalShares::new(dec!(200)))?;
    ctx.execution_threshold = ExecutionThreshold::Shares(high_threshold);

    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

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

    let take_result_buy = infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    poll_for_events(
        &mut bot,
        &infra.db_path,
        "PositionEvent::OnChainOrderFilled",
        2,
    )
    .await;

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

/// Verifies that when the broker rejects order placement (HTTP 422), the
/// position still accumulates onchain shares but the offchain order
/// transitions to Failed and no broker orders are created.
#[test_log::test(tokio::test)]
async fn broker_placement_fails() -> anyhow::Result<()> {
    let onchain_price = dec!(150.00);
    let broker_fill_price = dec!(150.00);
    let sell_amount = dec!(7.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    infra
        .broker_service
        .set_mode(e2e_tests::services::alpaca_broker::MockMode::PlacementFails);

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_aggregate_events_containing(&mut bot, &infra.db_path, "OffchainOrder", "Failed", 1)
        .await;

    let pool = connect_db(&infra.db_path).await?;

    let position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should exist after onchain trade");
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(sell_amount),
        "Position should accumulate short shares even when broker fails"
    );

    // At least one offchain order should exist, all in Failed state.
    // The position checker retries placement every cycle (2s in tests),
    // so multiple failed orders may accumulate during the wait window.
    let offchain_orders = Projection::<OffchainOrder>::sqlite(pool.clone())
        .load_all()
        .await?;
    assert!(
        !offchain_orders.is_empty(),
        "At least one offchain order should be created"
    );
    for (order_id, order) in &offchain_orders {
        assert!(
            matches!(order, OffchainOrder::Failed { .. }),
            "Offchain order {order_id} should be in Failed state, got: {order:?}"
        );
    }

    // Each failed order produces 2 events (Placed + Failed)
    let offchain_order_events = count_events(&pool, "OffchainOrder").await?;
    let expected_events = i64::try_from(offchain_orders.len())? * 2;
    assert_eq!(
        offchain_order_events, expected_events,
        "Each failed order should have Placed + Failed events"
    );

    let broker_orders = infra.broker_service.orders();
    assert!(
        broker_orders.is_empty(),
        "No broker orders should exist when placement fails"
    );

    pool.close().await;
    bot.abort();
    Ok(())
}

/// Verifies that when the broker accepts order placement but polling returns
/// "rejected", the offchain order transitions through Placed -> Submitted ->
/// Failed. Unlike `broker_placement_fails` (HTTP 422 at placement), here
/// the broker order actually exists and is polled before failing.
#[test_log::test(tokio::test)]
async fn broker_order_rejected() -> anyhow::Result<()> {
    let onchain_price = dec!(150.00);
    let broker_fill_price = dec!(150.00);
    let sell_amount = dec!(5.25);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    infra
        .broker_service
        .set_mode(e2e_tests::services::alpaca_broker::MockMode::OrderRejected);

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_aggregate_events_containing(&mut bot, &infra.db_path, "OffchainOrder", "Failed", 1)
        .await;

    let pool = connect_db(&infra.db_path).await?;

    let position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should exist after onchain trade");
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(sell_amount),
        "Position should accumulate short shares even when broker rejects"
    );

    // Offchain order(s) should exist in Failed state. The position checker
    // may retry placement each cycle, producing multiple failed orders.
    let offchain_orders = Projection::<OffchainOrder>::sqlite(pool.clone())
        .load_all()
        .await?;
    assert!(
        !offchain_orders.is_empty(),
        "At least one offchain order should be created"
    );
    for (order_id, order) in &offchain_orders {
        assert!(
            matches!(order, OffchainOrder::Failed { .. }),
            "Offchain order {order_id} should be in Failed state, got: {order:?}"
        );
    }

    // Unlike PlacementFails, here orders ARE placed on the broker. The mock
    // accepts placement (returning "new") then rejects on polling.
    let broker_orders = infra.broker_service.orders();
    assert!(
        !broker_orders.is_empty(),
        "Broker orders should exist (placement succeeded before rejection)"
    );
    for order in &broker_orders {
        assert_eq!(
            order.status, "rejected",
            "Broker order {} should be rejected",
            order.order_id
        );
    }

    // Each rejected order goes through Placed -> Submitted -> Failed (3 events)
    let offchain_order_events = count_events(&pool, "OffchainOrder").await?;
    let expected_events = i64::try_from(offchain_orders.len())? * 3;
    assert_eq!(
        offchain_order_events, expected_events,
        "Each rejected order should have Placed + Submitted + Failed events"
    );

    pool.close().await;
    bot.abort();
    Ok(())
}

/// Verifies that the bot correctly handles orders that take multiple poll
/// cycles to fill (simulating real broker latency). The order stays in
/// "new" status for 3 polls before transitioning to "filled".
#[test_log::test(tokio::test)]
async fn delayed_fill() -> anyhow::Result<()> {
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.25);
    let sell_amount = dec!(10.75);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    // Order stays "new" for 3 polls before filling
    infra
        .broker_service
        .set_mode(e2e_tests::services::alpaca_broker::MockMode::DelayedFill {
            polls_before_fill: 3,
        });

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    let pool = connect_db(&infra.db_path).await?;

    // Position should be fully hedged (net = 0)
    let position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should exist");
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(sell_amount),
    );
    assert_eq!(
        position.net,
        FractionalShares::ZERO,
        "Position should be fully hedged after delayed fill"
    );

    // The offchain order should be filled (not stuck in Submitted)
    let offchain_orders = Projection::<OffchainOrder>::sqlite(pool.clone())
        .load_all()
        .await?;
    assert_eq!(
        offchain_orders.len(),
        1,
        "Should have exactly one offchain order"
    );
    for (order_id, order) in &offchain_orders {
        assert!(
            matches!(order, OffchainOrder::Filled { .. }),
            "Offchain order {order_id} should be Filled, got: {order:?}"
        );
    }

    // Broker order should be filled with correct price
    let broker_orders = infra.broker_service.orders();
    assert_eq!(
        broker_orders.len(),
        1,
        "Should have exactly one broker order"
    );
    assert_eq!(broker_orders[0].status, "filled");
    assert_eq!(
        broker_orders[0].filled_price.as_deref(),
        Some("150.25"),
        "Should fill at configured broker price"
    );

    // Order was polled multiple times before filling
    assert_eq!(
        broker_orders[0].poll_count, 3,
        "Broker order should have exactly 3 status polls before delayed fill",
    );

    pool.close().await;
    bot.abort();
    Ok(())
}

/// Verifies the full pipeline handles very small fractional amounts
/// (sub-penny scale). A single milliShare (0.001) at $2500.00 exercises
/// the 18-decimal onchain conversion, Rain float encoding, CQRS event
/// persistence, and broker fill at sub-penny share scale.
///
/// The price must be high enough that 0.001 shares x price exceeds
/// the Alpaca $2.00 execution threshold ($2500 x 0.001 = $2.50).
#[test_log::test(tokio::test)]
async fn small_fractional_amounts() -> anyhow::Result<()> {
    let onchain_price = dec!(2500.00);
    let broker_fill_price = dec!(2490.00);
    let tiny_amount = dec!(0.001);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    let expected_position = ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(tiny_amount)
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(0))
        .expected_accumulated_short(tiny_amount)
        .expected_net(dec!(0))
        .build();

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let take_result = infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(tiny_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

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

/// Verifies that broker fills arriving out of submission order are
/// handled correctly. AAPL is submitted first but delayed 5 polls;
/// TSLA is submitted second but fills immediately. Both should end
/// fully hedged regardless of fill ordering.
#[test_log::test(tokio::test)]
async fn out_of_order_fills() -> anyhow::Result<()> {
    let onchain_price = dec!(155.00);
    let aapl_broker = dec!(150.25);
    let tsla_broker = dec!(245.00);
    let trade_amount = dec!(5.25);

    let infra =
        TestInfra::start(vec![("AAPL", aapl_broker), ("TSLA", tsla_broker)], vec![]).await?;

    // AAPL orders stay "new" for 5 polls before filling
    infra
        .broker_service
        .set_symbol_fill_delay(Symbol::new("AAPL")?, 5);

    let expected_positions = [
        ExpectedPosition::builder()
            .symbol("AAPL")
            .amount(trade_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(onchain_price)
            .broker_fill_price(aapl_broker)
            .expected_accumulated_long(dec!(0))
            .expected_accumulated_short(trade_amount)
            .expected_net(dec!(0))
            .build(),
        ExpectedPosition::builder()
            .symbol("TSLA")
            .amount(trade_amount)
            .direction(TakeDirection::SellEquity)
            .onchain_price(onchain_price)
            .broker_fill_price(tsla_broker)
            .expected_accumulated_long(dec!(0))
            .expected_accumulated_short(trade_amount)
            .expected_net(dec!(0))
            .build(),
    ];

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Submit AAPL first (will be delayed), then TSLA (fills immediately)
    let mut take_results = Vec::new();
    for expected_position in &expected_positions {
        take_results.push(
            infra
                .base_chain
                .take_order()
                .symbol(expected_position.symbol)
                .amount(trade_amount)
                .price(onchain_price)
                .direction(expected_position.direction)
                .call()
                .await?,
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 2).await;

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

    // Verify fill ordering: TSLA should have filled before AAPL
    let broker_orders = infra.broker_service.orders();
    let aapl_order = broker_orders
        .iter()
        .find(|order| order.symbol == "AAPL")
        .expect("AAPL order should exist");
    let tsla_order = broker_orders
        .iter()
        .find(|order| order.symbol == "TSLA")
        .expect("TSLA order should exist");

    assert_eq!(
        tsla_order.poll_count, 1,
        "TSLA should fill immediately with a single poll",
    );
    assert_eq!(
        aapl_order.poll_count, 5,
        "AAPL should fill after the configured 5-poll delay",
    );

    bot.abort();
    Ok(())
}

/// Verifies idempotent event processing: re-backfilling the same onchain
/// events (by restarting with the same `deployment_block`) must not
/// create duplicate queue entries or OnChainTrade aggregate events.
/// Validates the `UNIQUE(tx_hash, log_index)` dedup on the event queue
/// and CQRS aggregate idempotency for onchain trade processing.
///
/// The position checker may legitimately emit new events on restart
/// (e.g., re-checking accumulated positions), so we only assert
/// strict equality on queue-level and onchain-aggregate-level counts,
/// and verify the position projection converges to the same state.
#[test_log::test(tokio::test)]
async fn duplicate_event_delivery() -> anyhow::Result<()> {
    let onchain_price = dec!(155.00);
    let broker_fill_price = dec!(150.00);
    let sell_amount = dec!(8.3);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    // Phase 1: process 1 trade, wait for full hedging
    let ctx = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    infra
        .base_chain
        .take_order()
        .symbol("AAPL")
        .amount(sell_amount)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    poll_for_events(&mut bot, &infra.db_path, "OffchainOrderEvent::Filled", 1).await;

    // Snapshot counts before restart
    let pool = connect_db(&infra.db_path).await?;
    let pre_queued = count_queued_events(&pool).await?;
    let pre_processed = count_processed_queue_events(&pool).await?;
    let pre_onchain_events = count_events(&pool, "OnChainTrade").await?;

    let pre_position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should exist after first run");
    pool.close().await;

    bot.abort();
    let _ = bot.await;

    // Phase 2: restart bot with SAME deployment_block (re-backfills same events)
    let ctx2 = build_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    // Wait for backfill + processing of duplicate events
    wait_for_processing(&mut bot2, 10).await;

    let pool = connect_db(&infra.db_path).await?;

    // Queue-level dedup: no new rows from re-backfilling the same events
    let post_queued = count_queued_events(&pool).await?;
    let post_processed = count_processed_queue_events(&pool).await?;
    assert_eq!(
        pre_queued, post_queued,
        "Queue count should be unchanged after re-backfill: \
         pre={pre_queued}, post={post_queued}"
    );
    assert_eq!(
        pre_processed, post_processed,
        "Processed count should be unchanged after re-backfill: \
         pre={pre_processed}, post={post_processed}"
    );

    // OnChainTrade aggregate events: CQRS prevents duplicate events on
    // the same aggregate (same tx_hash:log_index ID)
    let post_onchain_events = count_events(&pool, "OnChainTrade").await?;
    assert_eq!(
        pre_onchain_events, post_onchain_events,
        "OnChainTrade event count should be unchanged: \
         pre={pre_onchain_events}, post={post_onchain_events}"
    );

    // Position projection converges to same final state
    let post_position = Projection::<Position>::sqlite(pool.clone())
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should still exist after restart");
    assert_eq!(
        pre_position.net, post_position.net,
        "Position net should be unchanged"
    );
    assert_eq!(
        pre_position.accumulated_short, post_position.accumulated_short,
        "Position accumulated_short should be unchanged"
    );
    assert_eq!(
        pre_position.accumulated_long, post_position.accumulated_long,
        "Position accumulated_long should be unchanged"
    );

    pool.close().await;
    bot2.abort();
    Ok(())
}
