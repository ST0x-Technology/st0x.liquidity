//! Phase 1 milestone: order discovery and classification end-to-end.
//!
//! Verifies the full pipeline: deploy an order on Anvil, start the taker
//! bot, and assert that it discovers and classifies the order correctly.

use alloy::primitives::{Bytes, U256, utils::parse_units};
use alloy::providers::Provider;
use std::collections::HashMap;
use std::time::Duration;

use st0x_execution::{AlpacaMarketDataMock, Symbol};
use st0x_shared::EquityTokenAddresses;

use crate::poll::{connect_db, fetch_all_events, poll_for_events, spawn_bot};
use crate::test_infra::TakerTestInfra;

/// Build a `Ctx` from the test infrastructure.
fn build_ctx(
    infra: &TakerTestInfra<impl Provider + Clone>,
    deployment_block: u64,
    excluded_owner: alloy::primitives::Address,
) -> st0x_taker::Ctx {
    let aapl = Symbol::new("AAPL").unwrap();
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

    st0x_taker::Ctx::for_test()
        .database_url(infra.database_url())
        .ws_rpc_url(ws_url)
        .orderbook(infra.rain.orderbook)
        .usdc_address(infra.usdc_addr)
        .deployment_block(deployment_block)
        .excluded_owner(excluded_owner)
        .private_key(private_key)
        .equities(equities)
        .evaluation_interval_secs(1)
        .call()
}

/// Smoke test: verify test infrastructure starts and can deploy orders.
#[tokio::test]
async fn test_infra_starts_and_deploys_order() {
    let infra = TakerTestInfra::start().await.unwrap();

    infra.rain.chain.provider.get_block_number().await.unwrap();

    let max_amount: U256 = parse_units("10", 18).unwrap().into();
    let expression = format!("_ _: {max_amount} 185;:;");

    let result = infra
        .rain
        .add_order(
            &expression,
            Bytes::new(),
            infra.usdc_addr,
            infra.wt_aapl_addr,
        )
        .await
        .unwrap();

    assert!(result.block > 0);
    assert!(!result.order_hash.is_zero());
}

/// Full pipeline: place a sell order on Anvil with metadata, start
/// the taker bot, verify it discovers the order and classifies it
/// as FixedPrice.
#[tokio::test]
async fn order_discovered_and_classified_via_bot() {
    let infra = TakerTestInfra::start().await.unwrap();
    // Place a fixed-price sell order with CBOR metadata so it gets
    // classified as FixedPrice by the bot's classification pipeline.
    let max_amount: U256 = parse_units("10", 18).unwrap().into();
    let expression = format!("_ _: {max_amount} 185;:;");

    let rainlang_source = "_ _: 1000 185;:;";
    let meta = encode_rainlang_metadata(rainlang_source);

    let add_result = infra
        .rain
        .add_order(&expression, meta, infra.usdc_addr, infra.wt_aapl_addr)
        .await
        .unwrap();

    assert!(add_result.block > 0, "Order should be mined");

    // Start the taker bot — it will backfill from deployment_block=0,
    // discover the order, classify it, and begin the evaluation loop.
    // The excluded_owner is NOT the Anvil owner, so the order should
    // be tracked.
    // Use a non-existent address as excluded_owner so orders from the
    // Anvil owner ARE tracked.
    let ctx = build_ctx(&infra, 0, alloy::primitives::Address::repeat_byte(0xFF));
    let mut bot = spawn_bot(ctx, AlpacaMarketDataMock::new());

    // Wait for the bot to discover and classify the order.
    // The TrackedOrder aggregate emits a "Discovered" event.
    poll_for_events(&mut bot, &infra.db_path, "TrackedOrderEvent::Discovered", 1).await;

    // Verify the CQRS event store has exactly the expected events.
    let pool = connect_db(&infra.db_path).await.unwrap();
    let events = fetch_all_events(&pool).await.unwrap();

    // Should have exactly 1 event: the Discover for our order
    assert_eq!(
        events.len(),
        1,
        "Expected exactly 1 event, got {}: {events:?}",
        events.len()
    );

    let event = &events[0];
    assert_eq!(event.aggregate_type, "TrackedOrder");
    assert_eq!(event.event_type, "TrackedOrderEvent::Discovered");

    // The aggregate_id should be the order hash (hex-encoded)
    assert!(
        !event.aggregate_id.is_empty(),
        "aggregate_id should be the order hash"
    );

    // Verify the event payload contains expected classification data.
    // The payload is the serialized TrackedOrderEvent::Discovered variant.
    let payload = &event.payload;
    let discovered = &payload["Discovered"];

    assert_eq!(
        discovered["scenario"].as_str().unwrap(),
        "A",
        "Owner outputs wtAAPL -> Scenario A"
    );

    assert_eq!(
        discovered["symbol"].as_str().unwrap(),
        "AAPL",
        "Symbol should be AAPL"
    );

    // Classification should have produced FixedPrice with the Rainlang
    // source from the metadata, NOT Unknown.
    let order_type = &discovered["order_type"];
    assert!(
        order_type["FixedPrice"].is_object(),
        "Expected FixedPrice classification, got: {order_type}"
    );
    assert_eq!(
        order_type["FixedPrice"]["rainlang"].as_str().unwrap(),
        rainlang_source,
        "Rainlang source should match metadata"
    );

    assert_eq!(
        discovered["discovered_block"].as_u64().unwrap(),
        add_result.block,
        "Discovered block should match the order's block"
    );

    pool.close().await;
    bot.abort();
}

/// Places an order from the excluded owner address. The bot should
/// NOT track it — no events should appear in the database.
#[tokio::test]
async fn excluded_owner_order_not_tracked() {
    let infra = TakerTestInfra::start().await.unwrap();

    let max_amount: U256 = parse_units("10", 18).unwrap().into();
    let expression = format!("_ _: {max_amount} 185;:;");

    infra
        .rain
        .add_order(
            &expression,
            Bytes::new(),
            infra.usdc_addr,
            infra.wt_aapl_addr,
        )
        .await
        .unwrap();

    // Use the Anvil owner as excluded_owner so their order gets filtered.
    let ctx = build_ctx(&infra, 0, infra.rain.chain.owner);
    let bot = spawn_bot(ctx, AlpacaMarketDataMock::new());

    // Give the bot time to backfill and process. If the order were
    // tracked, it would appear within a few seconds.
    // NOTE: Sleep-based negative assertion is inherently racy. A
    // deterministic approach (e.g., polling for backfill-complete)
    // would be more reliable but adds complexity for Phase 1.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let pool = connect_db(&infra.db_path).await.unwrap();
    let events = fetch_all_events(&pool).await.unwrap();

    assert!(
        events.is_empty(),
        "Excluded owner's order should produce no events, got: {events:?}"
    );

    pool.close().await;
    bot.abort();
}

/// Builds CBOR-encoded RainMetaDocumentV1 metadata from a Rainlang source.
fn encode_rainlang_metadata(rainlang: &str) -> Bytes {
    // RainMetaDocumentV1 magic prefix
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
