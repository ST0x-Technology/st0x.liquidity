//! Phase 1 milestone: profitability evaluation end-to-end.
//!
//! Deploys a FixedPrice order on Anvil, seeds mock market data with a
//! known quote, starts the taker bot, and verifies the evaluation loop
//! processes the order without crashing. The bot's evaluation interval
//! is set to 1 second in test config, so waiting a few seconds after
//! discovery proves the loop ran at least once.

use alloy::primitives::{Bytes, U256, utils::parse_units};
use rain_math_float::Float;
use std::collections::HashMap;

use st0x_execution::{AlpacaMarketDataMock, Symbol};
use st0x_shared::EquityTokenAddresses;

use crate::poll::{poll_for_events, spawn_bot};
use crate::test_infra::TakerTestInfra;

/// Builds CBOR-encoded RainMetaDocumentV1 metadata from a Rainlang source.
fn encode_rainlang_metadata(rainlang: &str) -> Bytes {
    let magic: [u8; 8] = [0xff, 0x0a, 0x89, 0xc6, 0x74, 0xee, 0x78, 0x74];

    let mut buf = Vec::new();
    buf.extend_from_slice(&magic);

    let map = ciborium::Value::Map(vec![(
        ciborium::Value::Integer(0.into()),
        ciborium::Value::Bytes(rainlang.as_bytes().to_vec()),
    )]);
    ciborium::into_writer(&map, &mut buf).unwrap();

    Bytes::from(buf)
}

fn float_val(value: u32) -> Float {
    Float::from_fixed_decimal_lossy(U256::from(value), 0)
        .unwrap()
        .0
}

/// Full pipeline: deploy a FixedPrice sell order, seed mock market data
/// with a quote that makes the order profitable, start the bot, verify
/// discovery, then verify the evaluation loop runs through at least one
/// cycle without crashing.
///
/// Scenario A: owner sells wtAAPL for USDC at io_ratio=185 (USDC per wtAAPL).
/// Market bid=$190 -> bot buy at $185, sell at $190 -> profitable.
#[tokio::test]
async fn profitable_order_evaluated_without_crash() {
    let infra = TakerTestInfra::start().await.unwrap();

    // Deploy a FixedPrice sell order with CBOR metadata
    let max_amount: U256 = parse_units("10", 18).unwrap().into();
    let expression = format!("_ _: {max_amount} 185;:;");
    let rainlang_source = "_ _: 1000 185;:;";
    let meta = encode_rainlang_metadata(rainlang_source);

    infra
        .rain
        .add_order(&expression, meta, infra.usdc_addr, infra.wt_aapl_addr)
        .await
        .unwrap();

    // Seed mock market data: bid=$190, ask=$191 -> Scenario A is profitable
    let mock_market = AlpacaMarketDataMock::new();
    let aapl = Symbol::new("AAPL").unwrap();
    mock_market.set_quote(aapl.clone(), float_val(190), float_val(191));

    let mut equities = HashMap::new();
    equities.insert(
        aapl,
        EquityTokenAddresses {
            wrapped: infra.wt_aapl_addr,
            unwrapped: infra.wt_aapl_addr,
        },
    );

    let ws_url = infra
        .rain
        .chain
        .ws_endpoint()
        .expect("Anvil should provide WS endpoint");
    let private_key = format!("{:x}", infra.rain.chain.owner_key);

    let ctx = st0x_taker::Ctx::for_test()
        .database_url(infra.database_url())
        .ws_rpc_url(ws_url)
        .orderbook(infra.rain.orderbook)
        .usdc_address(infra.usdc_addr)
        .deployment_block(0)
        .excluded_owner(alloy::primitives::Address::repeat_byte(0xFF))
        .private_key(private_key)
        .equities(equities)
        .evaluation_interval_secs(1)
        .call();

    let mut bot = spawn_bot(ctx, mock_market);

    // Wait for the bot to discover and classify the order
    poll_for_events(&mut bot, &infra.db_path, "TrackedOrderEvent::Discovered", 1).await;

    // The evaluation loop runs every 1 second. Wait for at least 2 cycles
    // to prove the loop runs successfully with seeded quotes. If the
    // evaluation panicked or errored fatally, the bot task would exit and
    // the select! in sleep_or_crash would catch it.
    for _ in 0..10 {
        crate::poll::sleep_or_crash(&mut bot, "profitability evaluation cycle").await;
    }

    // Bot is still alive after multiple evaluation cycles — the eval loop
    // processed the FixedPrice order with seeded market data successfully.
    bot.abort();
}

/// Deploy an order where the market price makes it unprofitable, verify
/// the evaluation loop still runs without crashing.
///
/// Scenario A: io_ratio=185, market bid=$180 -> unprofitable.
#[tokio::test]
async fn unprofitable_order_evaluated_without_crash() {
    let infra = TakerTestInfra::start().await.unwrap();

    let max_amount: U256 = parse_units("10", 18).unwrap().into();
    let expression = format!("_ _: {max_amount} 185;:;");
    let rainlang_source = "_ _: 1000 185;:;";
    let meta = encode_rainlang_metadata(rainlang_source);

    infra
        .rain
        .add_order(&expression, meta, infra.usdc_addr, infra.wt_aapl_addr)
        .await
        .unwrap();

    // Seed mock market data: bid=$180, ask=$181 -> Scenario A is unprofitable
    let mock_market = AlpacaMarketDataMock::new();
    let aapl = Symbol::new("AAPL").unwrap();
    mock_market.set_quote(aapl.clone(), float_val(180), float_val(181));

    let mut equities = HashMap::new();
    equities.insert(
        aapl,
        EquityTokenAddresses {
            wrapped: infra.wt_aapl_addr,
            unwrapped: infra.wt_aapl_addr,
        },
    );

    let ws_url = infra
        .rain
        .chain
        .ws_endpoint()
        .expect("Anvil should provide WS endpoint");
    let private_key = format!("{:x}", infra.rain.chain.owner_key);

    let ctx = st0x_taker::Ctx::for_test()
        .database_url(infra.database_url())
        .ws_rpc_url(ws_url)
        .orderbook(infra.rain.orderbook)
        .usdc_address(infra.usdc_addr)
        .deployment_block(0)
        .excluded_owner(alloy::primitives::Address::repeat_byte(0xFF))
        .private_key(private_key)
        .equities(equities)
        .evaluation_interval_secs(1)
        .call();

    let mut bot = spawn_bot(ctx, mock_market);

    poll_for_events(&mut bot, &infra.db_path, "TrackedOrderEvent::Discovered", 1).await;

    // Wait for evaluation cycles — unprofitable verdicts are debug-logged
    // but should not crash the bot
    for _ in 0..10 {
        crate::poll::sleep_or_crash(&mut bot, "unprofitable evaluation cycle").await;
    }

    bot.abort();
}
