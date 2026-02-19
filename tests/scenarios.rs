//! E2E scenario tests exercising the full bot lifecycle.
//!
//! Each scenario starts a real Anvil fork, a mock broker, and launches the
//! bot via `launch()`. The tests verify that the entire pipeline — from
//! onchain event detection through CQRS processing to offchain order fills —
//! works correctly under various conditions.
//!
//! Every test calls `assert_full_pipeline` which checks broker state,
//! onchain vault balances, and all CQRS events/views comprehensively.

mod common;
mod services;

use std::time::Duration;

use alloy::providers::Provider;
use rust_decimal::Decimal;
use serial_test::serial;

use st0x_event_sorcery::Projection;
use st0x_execution::{FractionalShares, Symbol};
use st0x_hedge::{OffchainOrder, Position};

use common::{
    E2eScenario, assert_full_pipeline, build_ctx, connect_db, count_all_domain_events,
    count_events, count_processed_queue_events, count_queued_events, spawn_bot,
    wait_for_processing,
};
use services::alpaca_broker::AlpacaBrokerMock;
use services::base_chain::{self, TakeDirection};

/// Single-asset happy path hedging
#[tokio::test]
#[serial]
async fn e2e_hedging_via_launch() -> anyhow::Result<()> {
    let scenario = E2eScenario {
        symbol: "AAPL",
        amount: "1.0",
        direction: TakeDirection::SellEquity,
        fill_price: "150.25",
        expected_accumulated_long: "0",
        expected_accumulated_short: "1.0",
        expected_net: "0",
    };

    let mut chain = base_chain::BaseChain::start("https://mainnet.base.org").await?;
    chain.deploy_equity_token(scenario.symbol).await?;

    let broker = AlpacaBrokerMock::start()
        .with_fill_price(scenario.fill_price)
        .build()
        .await;

    let current_block = chain.provider.get_block_number().await?;
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("e2e.sqlite");
    let database_url = db_path.display().to_string();

    let ctx = build_ctx(&chain, &broker, &db_path, current_block)?;
    let bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let take_result = chain
        .take_order(scenario.symbol, scenario.amount, scenario.direction)
        .await?;

    wait_for_processing(&bot, 10).await;

    assert_full_pipeline(
        &[scenario],
        &[take_result],
        &chain.provider,
        chain.orderbook_addr,
        chain.owner,
        &broker,
        &database_url,
    )
    .await?;

    bot.abort();
    Ok(())
}

// ── Scenario 2: Multi-asset sustained load ─────────────────────────

#[tokio::test]
#[serial]
async fn multi_asset_sustained_load() -> anyhow::Result<()> {
    let mut chain = base_chain::BaseChain::start("https://developer-access-mainnet.base.org").await?;
    chain.deploy_equity_token("AAPL").await?;
    chain.deploy_equity_token("TSLA").await?;
    chain.deploy_equity_token("MSFT").await?;

    let broker = AlpacaBrokerMock::start()
        .with_symbol_fill_price("AAPL", "185.50")
        .with_symbol_fill_price("TSLA", "245.00")
        .with_symbol_fill_price("MSFT", "410.75")
        .build()
        .await;

    let current_block = chain.provider.get_block_number().await?;
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("e2e.sqlite");
    let database_url = db_path.display().to_string();

    let ctx = build_ctx(&chain, &broker, &db_path, current_block)?;
    let bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let scenarios = [
        E2eScenario {
            symbol: "AAPL",
            amount: "1.0",
            direction: TakeDirection::SellEquity,
            fill_price: "185.50",
            expected_accumulated_long: "0",
            expected_accumulated_short: "1.0",
            expected_net: "0",
        },
        E2eScenario {
            symbol: "TSLA",
            amount: "1.0",
            direction: TakeDirection::SellEquity,
            fill_price: "245.00",
            expected_accumulated_long: "0",
            expected_accumulated_short: "1.0",
            expected_net: "0",
        },
        E2eScenario {
            symbol: "MSFT",
            amount: "1.0",
            direction: TakeDirection::SellEquity,
            fill_price: "410.75",
            expected_accumulated_long: "0",
            expected_accumulated_short: "1.0",
            expected_net: "0",
        },
    ];

    // Space trades so each gets individually hedged before the next
    // position event arrives on the same symbol.
    let mut take_results = Vec::new();
    for scenario in &scenarios {
        take_results.push(
            chain
                .take_order(scenario.symbol, "1.0", scenario.direction)
                .await?,
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    wait_for_processing(&bot, 20).await;

    assert_full_pipeline(
        &scenarios,
        &take_results,
        &chain.provider,
        chain.orderbook_addr,
        chain.owner,
        &broker,
        &database_url,
    )
    .await?;

    bot.abort();
    Ok(())
}

// ── Scenario 3: Backfilling ────────────────────────────────────────

#[tokio::test]
#[serial]
async fn backfilling() -> anyhow::Result<()> {
    let mut chain = base_chain::BaseChain::start("https://base-rpc.publicnode.com").await?;
    chain.deploy_equity_token("AAPL").await?;

    let broker = AlpacaBrokerMock::start()
        .with_fill_price("150.00")
        .build()
        .await;

    // Record the block BEFORE any take-orders (subtract 1 for safety margin)
    let pre_trade_block = chain.provider.get_block_number().await?.saturating_sub(1);

    // Execute 3 take-orders BEFORE starting the bot
    let trade_count: i64 = 3;
    let mut take_results = Vec::new();
    for _ in 0..trade_count {
        take_results.push(
            chain
                .take_order("AAPL", "1.0", TakeDirection::SellEquity)
                .await?,
        );
    }

    // Mine an extra block to ensure all trades are finalized
    chain.mine_blocks(1).await?;

    // Start bot with deployment_block set to BEFORE the first take-order
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("e2e.sqlite");
    let database_url = db_path.display().to_string();
    let ctx = build_ctx(&chain, &broker, &db_path, pre_trade_block)?;
    let bot = spawn_bot(ctx);

    // Wait for backfill + processing. With 3 rapid backfilled trades, only
    // 1 offchain order is placed initially (the bot won't place a second
    // while the first is pending). After the first fill, the remaining net
    // needs the position checker's cycle (2s in tests) to detect and hedge.
    wait_for_processing(&bot, 10).await;

    // Verify all historical events were picked up via backfill
    let pool = connect_db(&db_path).await?;
    let queued = count_queued_events(&pool).await?;
    assert!(
        queued >= trade_count,
        "Expected at least {trade_count} queued events from backfill, got {queued}"
    );
    let processed = count_processed_queue_events(&pool).await?;
    assert_eq!(processed, queued, "All queued events should be processed");
    pool.close().await;

    // Full pipeline: 3 sells, each hedged -> net = 0
    let scenario = E2eScenario {
        symbol: "AAPL",
        amount: "3.0",
        direction: TakeDirection::SellEquity,
        fill_price: "150.00",
        expected_accumulated_long: "0",
        expected_accumulated_short: "3.0",
        expected_net: "0",
    };

    assert_full_pipeline(
        &[scenario],
        &take_results,
        &chain.provider,
        chain.orderbook_addr,
        chain.owner,
        &broker,
        &database_url,
    )
    .await?;

    bot.abort();
    Ok(())
}

// ── Scenario 4: Resumption after graceful shutdown ─────────────────

#[tokio::test]
#[serial]
async fn resumption_after_shutdown() -> anyhow::Result<()> {
    let mut chain = base_chain::BaseChain::start("https://base.drpc.org").await?;
    chain.deploy_equity_token("AAPL").await?;

    let broker = AlpacaBrokerMock::start()
        .with_fill_price("150.00")
        .build()
        .await;

    let current_block = chain.provider.get_block_number().await?;
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("e2e.sqlite");
    let database_url = db_path.display().to_string();

    // Phase 1: Start bot, process 1 trade, wait for fill
    let ctx = build_ctx(&chain, &broker, &db_path, current_block)?;
    let bot = spawn_bot(ctx);
    tokio::time::sleep(Duration::from_secs(3)).await;

    let take1 = chain
        .take_order("AAPL", "1.0", TakeDirection::SellEquity)
        .await?;

    wait_for_processing(&bot, 15).await;

    // Snapshot DB state before shutdown
    let pool = connect_db(&db_path).await?;
    let pre_shutdown_domain_events = count_all_domain_events(&pool).await?;
    pool.close().await;

    // Abort the bot (simulates graceful shutdown)
    bot.abort();
    let _ = bot.await;

    // Phase 2: Execute 1 more take-order while bot is down
    let take2 = chain
        .take_order("AAPL", "1.0", TakeDirection::SellEquity)
        .await?;

    // Restart bot with same db_path
    let ctx2 = build_ctx(&chain, &broker, &db_path, current_block)?;
    let bot2 = spawn_bot(ctx2);

    // Wait for backfill to pick up missed events + processing
    wait_for_processing(&bot2, 20).await;

    // Verify no duplicate CQRS events
    let pool = connect_db(&db_path).await?;
    let post_restart_domain_events = count_all_domain_events(&pool).await?;
    assert!(
        post_restart_domain_events > pre_shutdown_domain_events,
        "Should have new events after restart: pre={pre_shutdown_domain_events}, \
         post={post_restart_domain_events}"
    );
    pool.close().await;

    // Full pipeline: 2 sells total (1 pre + 1 post shutdown), both hedged
    let scenario = E2eScenario {
        symbol: "AAPL",
        amount: "2.0",
        direction: TakeDirection::SellEquity,
        fill_price: "150.00",
        expected_accumulated_long: "0",
        expected_accumulated_short: "2.0",
        expected_net: "0",
    };

    assert_full_pipeline(
        &[scenario],
        &[take1, take2],
        &chain.provider,
        chain.orderbook_addr,
        chain.owner,
        &broker,
        &database_url,
    )
    .await?;

    bot2.abort();
    Ok(())
}

// ── Scenario 5: Crash recovery with eventual consistency ───────────

#[tokio::test]
#[serial]
async fn crash_recovery_eventual_consistency() -> anyhow::Result<()> {
    let trade_sequence: Vec<(&str, &str, TakeDirection)> = vec![
        ("AAPL", "1.0", TakeDirection::SellEquity),
        ("TSLA", "1.0", TakeDirection::SellEquity),
    ];

    let scenarios = [
        E2eScenario {
            symbol: "AAPL",
            amount: "1.0",
            direction: TakeDirection::SellEquity,
            fill_price: "150.00",
            expected_accumulated_long: "0",
            expected_accumulated_short: "1.0",
            expected_net: "0",
        },
        E2eScenario {
            symbol: "TSLA",
            amount: "1.0",
            direction: TakeDirection::SellEquity,
            fill_price: "150.00",
            expected_accumulated_long: "0",
            expected_accumulated_short: "1.0",
            expected_net: "0",
        },
    ];

    // ── Reference run: uninterrupted ────────────────────────────────

    let mut ref_chain = base_chain::BaseChain::start("https://base.public.blockpi.network/v1/rpc/public").await?;
    ref_chain.deploy_equity_token("AAPL").await?;
    ref_chain.deploy_equity_token("TSLA").await?;

    let ref_broker = AlpacaBrokerMock::start()
        .with_fill_price("150.00")
        .build()
        .await;

    let ref_block = ref_chain.provider.get_block_number().await?;
    let ref_db_dir = tempfile::tempdir()?;
    let ref_db_path = ref_db_dir.path().join("e2e.sqlite");
    let ref_database_url = ref_db_path.display().to_string();
    let ref_ctx = build_ctx(&ref_chain, &ref_broker, &ref_db_path, ref_block)?;
    let ref_bot = spawn_bot(ref_ctx);
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut ref_take_results = Vec::new();
    for (symbol, amount, direction) in &trade_sequence {
        ref_take_results.push(ref_chain.take_order(symbol, amount, *direction).await?);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    wait_for_processing(&ref_bot, 15).await;

    // Assert reference run passes full pipeline
    assert_full_pipeline(
        &scenarios,
        &ref_take_results,
        &ref_chain.provider,
        ref_chain.orderbook_addr,
        ref_chain.owner,
        &ref_broker,
        &ref_database_url,
    )
    .await?;

    let ref_pool = connect_db(&ref_db_path).await?;
    let ref_domain_events = count_all_domain_events(&ref_pool).await?;
    ref_pool.close().await;
    ref_bot.abort();
    let _ = ref_bot.await;

    // ── Crash run: same trades, with interruption ───────────────────

    let mut crash_chain = base_chain::BaseChain::start("https://base.public.blockpi.network/v1/rpc/public").await?;
    crash_chain.deploy_equity_token("AAPL").await?;
    crash_chain.deploy_equity_token("TSLA").await?;

    let crash_broker = AlpacaBrokerMock::start()
        .with_fill_price("150.00")
        .build()
        .await;

    let crash_block = crash_chain.provider.get_block_number().await?;
    let crash_db_dir = tempfile::tempdir()?;
    let crash_db_path = crash_db_dir.path().join("e2e.sqlite");
    let crash_database_url = crash_db_path.display().to_string();

    // Phase 1: process first trade, then crash
    let ctx1 = build_ctx(&crash_chain, &crash_broker, &crash_db_path, crash_block)?;
    let bot1 = spawn_bot(ctx1);
    tokio::time::sleep(Duration::from_secs(3)).await;

    let crash_take1 = crash_chain
        .take_order(
            trade_sequence[0].0,
            trade_sequence[0].1,
            trade_sequence[0].2,
        )
        .await?;
    wait_for_processing(&bot1, 15).await;
    bot1.abort();
    let _ = bot1.await;

    // Phase 2: submit remaining trade and restart
    let crash_take2 = crash_chain
        .take_order(
            trade_sequence[1].0,
            trade_sequence[1].1,
            trade_sequence[1].2,
        )
        .await?;

    let ctx2 = build_ctx(&crash_chain, &crash_broker, &crash_db_path, crash_block)?;
    let bot2 = spawn_bot(ctx2);

    wait_for_processing(&bot2, 20).await;

    // Assert crash run also passes full pipeline (convergence)
    assert_full_pipeline(
        &scenarios,
        &[crash_take1, crash_take2],
        &crash_chain.provider,
        crash_chain.orderbook_addr,
        crash_chain.owner,
        &crash_broker,
        &crash_database_url,
    )
    .await?;

    // Domain event count should be at least as many as reference (crash run
    // may generate additional events from re-initialization, position checker
    // detecting pending states on restart, etc.)
    let crash_pool = connect_db(&crash_db_path).await?;
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

// ── Scenario 6: Market hours transitions ───────────────────────────

#[tokio::test]
#[serial]
async fn market_hours_transitions() -> anyhow::Result<()> {
    let mut chain = base_chain::BaseChain::start("https://endpoints.omniatech.io/v1/base/mainnet/public").await?;
    chain.deploy_equity_token("AAPL").await?;

    // Start with market CLOSED
    let broker = AlpacaBrokerMock::start()
        .with_fill_price("150.00")
        .with_market_closed()
        .build()
        .await;

    let current_block = chain.provider.get_block_number().await?;
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("e2e.sqlite");
    let database_url = db_path.display().to_string();
    let ctx = build_ctx(&chain, &broker, &db_path, current_block)?;
    let bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Execute take-order — events enqueue and positions accumulate
    let take_result = chain
        .take_order("AAPL", "1.0", TakeDirection::SellEquity)
        .await?;

    // Wait for event processing but not order placement (market closed)
    wait_for_processing(&bot, 10).await;

    // Assert: position accumulated but NO offchain orders placed
    let pool = connect_db(&db_path).await?;
    let position = Projection::<Position>::sqlite(pool.clone())?
        .load(&Symbol::new("AAPL")?)
        .await?
        .expect("Position should exist even when market is closed");
    assert_eq!(
        position.accumulated_short,
        FractionalShares::new(Decimal::new(1, 0)),
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
    broker.set_market_open();

    // Wait for the position checker's periodic cycle (2s in tests) plus processing.
    // The checker detects the pending position and places orders.
    wait_for_processing(&bot, 10).await;

    // Full pipeline assertions after market opens
    let scenario = E2eScenario {
        symbol: "AAPL",
        amount: "1.0",
        direction: TakeDirection::SellEquity,
        fill_price: "150.00",
        expected_accumulated_long: "0",
        expected_accumulated_short: "1.0",
        expected_net: "0",
    };

    assert_full_pipeline(
        &[scenario],
        &[take_result],
        &chain.provider,
        chain.orderbook_addr,
        chain.owner,
        &broker,
        &database_url,
    )
    .await?;

    // Verify no duplicate event processing
    let pool = connect_db(&db_path).await?;
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

// ── Scenario 7: Inventory auto-rebalancing ─────────────────────────
// Requires constructing RebalancingCtx which has pub(crate) fields.
// Since integration tests can't access pub(crate) internals, this test
// uses Ctx::from_toml() with TOML strings that enable rebalancing.
// TODO: Implement when the rebalancing e2e infrastructure is ready.
// The rebalancing flow requires:
// 1. AlpacaBrokerMock + AlpacaTokenizationMock + CctpAttestationMock
// 2. A fresh signer for market_maker_wallet (different from redemption_wallet)
// 3. Onchain wallet funding and vault setup
// 4. Aggressive imbalance thresholds to trigger rebalancing quickly
// 5. The 60s inventory poller cycle
//
// This is deferred because:
// - RebalancingCtx::new() is pub(crate), requiring Ctx::from_toml()
// - The bot needs real onchain wallet state for rebalancing checks
// - Mock services need to coordinate mint/redeem flows end-to-end

// ── Scenario 8: Chain reorganization (stub) ────────────────────────

#[tokio::test]
#[serial]
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
