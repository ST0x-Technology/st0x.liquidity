//! North-star e2e for first-class reorg handling.
//!
//! Verifies the end state of reorg handling: a fill the bot has already
//! witnessed and hedged is dropped by a chain reorg, the bot detects the fork
//! change, and the event-sourced views reverse the fill's position impact
//! append-only -- the original fill and its reversal both survive, nothing is
//! deleted.
//!
//! `#[ignore]`d until the chaos-proxy fork simulation that
//! [`simulate_reorg_dropping_fill`] depends on exists; that simulation drives
//! the reorg this test asserts against.

use alloy::providers::Provider;
use std::time::Duration;

use st0x_float_macro::float;

use crate::hedging::assertions::{TakeDirection, TestInfra, build_ctx, spawn_bot};
use crate::poll::{connect_db, count_events_of_type, poll_for_events};

#[test_log::test(tokio::test)]
#[ignore = "north-star: needs the chaos-proxy fork simulation (terminal reorg slice)"]
async fn reorg_reverts_witnessed_fill() -> anyhow::Result<()> {
    let symbol = "AAPL";
    let onchain_price = float!(155.00);
    let broker_fill_price = float!(150.25);
    let amount = float!(10.0);

    let infra = TestInfra::start(vec![(symbol, broker_fill_price)], vec![]).await?;

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

    // Ingest a fill and let the bot witness and hedge it.
    let take_result = infra
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

    // Simulate a reorg that drops the fill's block from the canonical chain.
    // The terminal slice implements this via a chaos proxy that forks the chain
    // and serves a competing history without the take order's block.
    simulate_reorg_dropping_fill(&infra, &take_result)?;

    // Detection emits RecordReorg, producing the reversal events.
    poll_for_events(&mut bot, &infra.db_path, "OnChainTradeEvent::Reorged", 1).await;
    poll_for_events(&mut bot, &infra.db_path, "PositionEvent::Reorged", 1).await;

    let pool = connect_db(&infra.db_path).await?;

    // The reversal is append-only: the original fill survives, the reorg is a
    // new event, and the views expose a `reorged` flag rather than deleting rows.
    assert_eq!(
        count_events_of_type(&pool, "OnChainTradeEvent::Filled").await?,
        1,
        "the original fill must be preserved, not deleted",
    );
    assert_eq!(
        count_events_of_type(&pool, "PositionEvent::Reorged").await?,
        1,
        "the reorg must reverse the fill's position impact exactly once",
    );

    let reorged_trades: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM onchain_trade_view \
         WHERE json_extract(payload, '$.Live.reorged') = 1",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        reorged_trades, 1,
        "the reorged fill must be flagged in onchain_trade_view, not removed",
    );

    bot.abort();
    Ok(())
}

/// Forks the chain so the take order's block is no longer canonical, modelling a
/// reorg that drops an already-witnessed fill.
///
/// Unimplemented until the terminal slice lands the chaos-proxy fork
/// simulation. The north-star test above is `#[ignore]`d, so this stub never
/// executes.
fn simulate_reorg_dropping_fill<Node>(
    _infra: &TestInfra<Node>,
    _take_result: &crate::base_chain::TakeOrderResult,
) -> anyhow::Result<()> {
    todo!("fork the chain via the chaos proxy to drop the witnessed fill")
}
