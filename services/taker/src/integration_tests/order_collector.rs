//! Integration tests for the order collector pipeline.
//!
//! Deploys a real Rain OrderBook on Anvil, places and removes orders,
//! then verifies the backfill pipeline correctly parses events into
//! TrackedOrder commands and persists state.

use alloy::primitives::{Address, Bytes, U256, utils::parse_units};
use alloy::providers::Provider;
use std::sync::Arc;

use st0x_event_sorcery::{Projection, Store, StoreBuilder};
use st0x_execution::Symbol;
use st0x_shared::test_support::RainOrderBook;

use crate::classification::RAIN_META_DOCUMENT_V1_MAGIC;
use crate::order_collector::{BlockCursor, EventProcessor};
use crate::tracked_order::{OrderFilter, OrderHash, OrderType, Scenario, TrackedOrder};
use crate::{fetch_batch_logs, process_backfill_logs};

/// Starts a [`RainOrderBook`] and deploys USDC + wtAAPL tokens for order tests.
/// Returns `(rain, usdc_addr, wt_aapl_addr)`.
async fn setup() -> (RainOrderBook<impl Provider + Clone>, Address, Address) {
    let rain = RainOrderBook::start().await.unwrap();

    let usdc_addr = rain
        .deploy_erc20(
            "USD Coin",
            "USDC",
            6,
            parse_units("1000000", 6).unwrap().into(),
        )
        .await
        .unwrap();

    let wt_aapl_addr = rain
        .deploy_erc20(
            "Wrapped AAPL",
            "wtAAPL",
            18,
            parse_units("1000000", 18).unwrap().into(),
        )
        .await
        .unwrap();

    (rain, usdc_addr, wt_aapl_addr)
}

/// Adds a sell order: owner outputs wtAAPL, inputs USDC (Scenario A).
async fn add_sell_order(
    rain: &RainOrderBook<impl Provider + Clone>,
    usdc_addr: Address,
    wt_aapl_addr: Address,
) -> (u64, OrderHash) {
    let max_amount: U256 = parse_units("10", 18).unwrap().into();
    let expression = format!("_ _: {max_amount} 100;:;");

    let result = rain
        .add_order(&expression, Bytes::new(), usdc_addr, wt_aapl_addr)
        .await
        .unwrap();

    (result.block, OrderHash::new(result.order_hash))
}

/// Adds a buy order: owner outputs USDC, inputs wtAAPL (Scenario B).
async fn add_buy_order(
    rain: &RainOrderBook<impl Provider + Clone>,
    usdc_addr: Address,
    wt_aapl_addr: Address,
) -> (u64, OrderHash) {
    let max_amount: U256 = parse_units("1000", 6).unwrap().into();
    let expression = format!("_ _: {max_amount} 100;:;");

    let result = rain
        .add_order(&expression, Bytes::new(), wt_aapl_addr, usdc_addr)
        .await
        .unwrap();

    (result.block, OrderHash::new(result.order_hash))
}

/// Adds an order with tokens not in the filter config.
async fn add_order_with_unknown_tokens(
    rain: &RainOrderBook<impl Provider + Clone>,
) -> (u64, OrderHash) {
    let result = rain
        .add_order(
            "_ _: 1000 100;:;",
            Bytes::new(),
            Address::repeat_byte(0x77),
            Address::repeat_byte(0x88),
        )
        .await
        .unwrap();

    (result.block, OrderHash::new(result.order_hash))
}

fn test_filter(owner: Address, usdc_addr: Address, wt_aapl_addr: Address) -> OrderFilter {
    let symbol = Symbol::new("AAPL").unwrap();
    OrderFilter::new(
        // Excluded owner: use a random address that is NOT the anvil owner
        owner,
        usdc_addr,
        vec![(symbol, wt_aapl_addr)],
    )
}

async fn setup_cqrs() -> (
    Arc<Store<TrackedOrder>>,
    Arc<Projection<TrackedOrder>>,
    sqlx::SqlitePool,
) {
    let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
    sqlx::migrate!().run(&pool).await.unwrap();

    let (store, projection) = StoreBuilder::<TrackedOrder>::new(pool.clone())
        .build(())
        .await
        .unwrap();

    (store, projection, pool)
}

// ── Integration: subscribe to AddOrderV3/RemoveOrderV3, verify events
//    parsed into TrackedOrder commands ────────────────────────────────

#[tokio::test]
async fn backfill_add_order_creates_active_tracked_order() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, _projection, pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    let (add_block, order_hash) = add_sell_order(&rain, usdc_addr, wt_aapl_addr).await;

    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, add_block, add_block)
            .await
            .unwrap();

    assert_eq!(add_logs.len(), 1, "Expected exactly one AddOrderV3 log");
    assert!(remove_logs.is_empty());
    assert!(take_logs.is_empty());

    let processed =
        process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
            .await
            .unwrap();
    assert_eq!(processed, 1);

    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner,
        symbol,
        scenario,
        output_token,
        input_token,
        order_type,
        max_output,
        remaining_output,
        discovered_block,
        ..
    }) = &tracked
    else {
        panic!("Expected Active tracked order, got: {tracked:?}");
    };

    assert_eq!(*owner, rain.chain.owner);
    assert_eq!(symbol.to_string(), "AAPL");
    assert_eq!(
        *scenario,
        Scenario::A,
        "Seller outputs wtAAPL -> Scenario A"
    );
    assert_eq!(*output_token, wt_aapl_addr);
    assert_eq!(*input_token, usdc_addr);
    assert_eq!(*order_type, OrderType::Unknown);
    assert_eq!(*max_output, U256::MAX);
    assert_eq!(*remaining_output, U256::MAX);
    assert_eq!(*discovered_block, add_block);

    let cursor = BlockCursor::new(&pool);
    assert_eq!(
        cursor.last_block().await.unwrap(),
        None,
        "process_backfill_logs does not touch the cursor"
    );
}

#[tokio::test]
async fn backfill_remove_order_transitions_to_removed() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, _projection, _pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    // Place then remove an order onchain
    let max_amount: U256 = parse_units("10", 18).unwrap().into();
    let expression = format!("_ _: {max_amount} 100;:;");
    let add_result = rain
        .add_order(&expression, Bytes::new(), usdc_addr, wt_aapl_addr)
        .await
        .unwrap();

    let order_hash = OrderHash::new(add_result.order_hash);
    let add_block = add_result.block;

    let remove_block = rain.remove_order(&add_result.order).await.unwrap();

    // Backfill the add event first
    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, add_block, add_block)
            .await
            .unwrap();
    process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
        .await
        .unwrap();

    // Verify Active before removal
    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner,
        symbol,
        scenario,
        ..
    }) = &tracked
    else {
        panic!("Expected Active before remove, got: {tracked:?}");
    };
    assert_eq!(*owner, rain.chain.owner);
    assert_eq!(symbol.to_string(), "AAPL");
    assert_eq!(*scenario, Scenario::A);

    // Backfill the remove event
    let (add_logs, remove_logs, take_logs, meta_logs) = fetch_batch_logs(
        &rain.chain.provider,
        rain.orderbook,
        remove_block,
        remove_block,
    )
    .await
    .unwrap();

    assert_eq!(
        remove_logs.len(),
        1,
        "Expected exactly one RemoveOrderV3 log"
    );

    process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
        .await
        .unwrap();

    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Removed { owner, symbol, .. }) = &tracked else {
        panic!("Expected Removed tracked order, got: {tracked:?}");
    };
    assert_eq!(*owner, rain.chain.owner);
    assert_eq!(symbol.to_string(), "AAPL");
}

#[tokio::test]
async fn backfill_excluded_owner_order_is_not_tracked() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, _projection, _pool) = setup_cqrs().await;

    // Use the anvil owner as the excluded owner so their order gets filtered out
    let filter = test_filter(rain.chain.owner, usdc_addr, wt_aapl_addr);

    let bot_address = Address::repeat_byte(0xB0);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    let (add_block, order_hash) = add_sell_order(&rain, usdc_addr, wt_aapl_addr).await;

    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, add_block, add_block)
            .await
            .unwrap();

    process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
        .await
        .unwrap();

    let tracked = store.load(&order_hash).await.unwrap();
    assert!(
        tracked.is_none(),
        "Excluded owner's order should not be tracked"
    );
}

// ── Full pipeline: place order on chain, backfill, verify TrackedOrder
//    reaches Active state in DB ──────────────────────────────────────

#[tokio::test]
async fn order_discovery_reaches_active_state_in_db() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, projection, pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    let (add_block, order_hash) = add_sell_order(&rain, usdc_addr, wt_aapl_addr).await;

    let current_block = rain.chain.provider.get_block_number().await.unwrap();
    let cursor = BlockCursor::new(&pool);

    assert_eq!(
        cursor.last_block().await.unwrap(),
        None,
        "Fresh DB: no cursor"
    );

    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, 0, current_block)
            .await
            .unwrap();

    let processed =
        process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
            .await
            .unwrap();
    assert!(processed >= 1, "Should have processed at least 1 event");

    cursor.update(current_block).await.unwrap();

    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner,
        symbol,
        scenario,
        output_token,
        input_token,
        order_type,
        max_output,
        remaining_output,
        discovered_block,
        ..
    }) = &tracked
    else {
        panic!("TrackedOrder should be Active in event store, got: {tracked:?}");
    };

    assert_eq!(*owner, rain.chain.owner);
    assert_eq!(symbol.to_string(), "AAPL");
    assert_eq!(*scenario, Scenario::A);
    assert_eq!(*output_token, wt_aapl_addr);
    assert_eq!(*input_token, usdc_addr);
    assert_eq!(*order_type, OrderType::Unknown);
    assert_eq!(*max_output, U256::MAX);
    assert_eq!(*remaining_output, U256::MAX);
    assert_eq!(*discovered_block, add_block);

    let projected = projection.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner: proj_owner,
        symbol: proj_symbol,
        scenario: proj_scenario,
        output_token: proj_output,
        input_token: proj_input,
        discovered_block: proj_block,
        ..
    }) = &projected
    else {
        panic!("TrackedOrder should be Active in projection, got: {projected:?}");
    };

    assert_eq!(*proj_owner, rain.chain.owner);
    assert_eq!(proj_symbol.to_string(), "AAPL");
    assert_eq!(*proj_scenario, Scenario::A);
    assert_eq!(*proj_output, wt_aapl_addr);
    assert_eq!(*proj_input, usdc_addr);
    assert_eq!(*proj_block, add_block);

    assert_eq!(
        cursor.last_block().await.unwrap(),
        Some(current_block),
        "Block cursor should be at current block after backfill"
    );
}

#[tokio::test]
async fn multiple_orders_all_discovered() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, projection, pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    let (_block1, hash1) = add_sell_order(&rain, usdc_addr, wt_aapl_addr).await;
    let (_block2, hash2) = add_sell_order(&rain, usdc_addr, wt_aapl_addr).await;

    let current_block = rain.chain.provider.get_block_number().await.unwrap();

    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, 0, current_block)
            .await
            .unwrap();

    assert_eq!(add_logs.len(), 2, "Expected two AddOrderV3 logs");

    let processed =
        process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
            .await
            .unwrap();
    assert_eq!(processed, 2);

    let tracked1 = store.load(&hash1).await.unwrap();
    let Some(TrackedOrder::Active {
        owner: owner1,
        symbol: symbol1,
        scenario: scenario1,
        output_token: out1,
        input_token: in1,
        ..
    }) = &tracked1
    else {
        panic!("First order should be Active, got: {tracked1:?}");
    };
    assert_eq!(*owner1, rain.chain.owner);
    assert_eq!(symbol1.to_string(), "AAPL");
    assert_eq!(*scenario1, Scenario::A);
    assert_eq!(*out1, wt_aapl_addr);
    assert_eq!(*in1, usdc_addr);

    let tracked2 = store.load(&hash2).await.unwrap();
    let Some(TrackedOrder::Active {
        owner: owner2,
        symbol: symbol2,
        scenario: scenario2,
        output_token: out2,
        input_token: in2,
        ..
    }) = &tracked2
    else {
        panic!("Second order should be Active, got: {tracked2:?}");
    };
    assert_eq!(*owner2, rain.chain.owner);
    assert_eq!(symbol2.to_string(), "AAPL");
    assert_eq!(*scenario2, Scenario::A);
    assert_eq!(*out2, wt_aapl_addr);
    assert_eq!(*in2, usdc_addr);

    let all_orders = projection.load_all().await.unwrap();
    let active_count = all_orders
        .iter()
        .filter(|(_, order)| matches!(order, TrackedOrder::Active { .. }))
        .count();
    assert_eq!(active_count, 2, "Projection should show 2 active orders");

    let cursor = BlockCursor::new(&pool);
    cursor.update(current_block).await.unwrap();
    assert_eq!(cursor.last_block().await.unwrap(), Some(current_block));
}

// ── Scenario B, token filtering, and idempotency ─────────────────

#[tokio::test]
async fn scenario_b_buy_order_discovered_with_correct_tokens() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, _projection, _pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    let (add_block, order_hash) = add_buy_order(&rain, usdc_addr, wt_aapl_addr).await;

    let current_block = rain.chain.provider.get_block_number().await.unwrap();
    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, 0, current_block)
            .await
            .unwrap();

    let processed =
        process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
            .await
            .unwrap();
    assert_eq!(processed, 1);

    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner,
        symbol,
        scenario,
        output_token,
        input_token,
        discovered_block,
        ..
    }) = &tracked
    else {
        panic!("Expected Active order, got: {tracked:?}");
    };

    assert_eq!(*owner, rain.chain.owner);
    assert_eq!(symbol.to_string(), "AAPL");
    assert_eq!(*scenario, Scenario::B, "Owner outputs USDC -> Scenario B");
    assert_eq!(*output_token, usdc_addr, "Scenario B output is USDC");
    assert_eq!(*input_token, wt_aapl_addr, "Scenario B input is wtAAPL");
    assert_eq!(*discovered_block, add_block);
}

#[tokio::test]
async fn unsupported_token_pair_silently_skipped() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, _projection, _pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    let (add_block, order_hash) = add_order_with_unknown_tokens(&rain).await;

    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, add_block, add_block)
            .await
            .unwrap();

    assert_eq!(add_logs.len(), 1, "AddOrderV3 was emitted on chain");

    process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
        .await
        .unwrap();

    let tracked = store.load(&order_hash).await.unwrap();
    assert!(
        tracked.is_none(),
        "Order with unsupported tokens should not be tracked, got: {tracked:?}"
    );
}

#[tokio::test]
async fn duplicate_backfill_is_idempotent() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, projection, _pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    let (add_block, order_hash) = add_sell_order(&rain, usdc_addr, wt_aapl_addr).await;

    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, add_block, add_block)
            .await
            .unwrap();

    // First backfill
    let processed =
        process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
            .await
            .unwrap();
    assert_eq!(processed, 1);

    // Second backfill of the exact same logs (simulates restart overlap)
    let processed_again =
        process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
            .await
            .unwrap();
    assert_eq!(processed_again, 1, "Logs still counted even if idempotent");

    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner,
        symbol,
        scenario,
        max_output,
        remaining_output,
        ..
    }) = &tracked
    else {
        panic!("Expected Active order after double backfill, got: {tracked:?}");
    };

    assert_eq!(*owner, rain.chain.owner);
    assert_eq!(symbol.to_string(), "AAPL");
    assert_eq!(*scenario, Scenario::A);
    assert_eq!(*max_output, U256::MAX);
    assert_eq!(
        *remaining_output,
        U256::MAX,
        "Duplicate Discover should not change remaining_output"
    );

    let all_orders = projection.load_all().await.unwrap();
    assert_eq!(
        all_orders.len(),
        1,
        "Projection should have exactly 1 order, not duplicated"
    );
}

// ── Classification e2e ───────────────────────────────────────────

/// Builds CBOR-encoded RainMetaDocumentV1 metadata from a Rainlang source.
///
/// Includes key 1 (`RainlangSourceV1` magic) and key 2 (content type)
/// to match the production metadata format that `extract_rainlang_source`
/// validates against.
fn encode_rainlang_metadata(rainlang: &str) -> Bytes {
    use crate::classification::RAINLANG_SOURCE_V1_MAGIC;

    let mut buf = Vec::new();
    buf.extend_from_slice(&RAIN_META_DOCUMENT_V1_MAGIC);

    let map = ciborium::Value::Map(vec![
        (
            ciborium::Value::Integer(0.into()),
            ciborium::Value::Bytes(rainlang.as_bytes().to_vec()),
        ),
        (
            ciborium::Value::Integer(1.into()),
            ciborium::Value::Integer(RAINLANG_SOURCE_V1_MAGIC.into()),
        ),
        (
            ciborium::Value::Integer(2.into()),
            ciborium::Value::Text("application/octet-stream".to_owned()),
        ),
    ]);
    ciborium::into_writer(&map, &mut buf).unwrap();

    Bytes::from(buf)
}

/// Adds a sell order with CBOR-encoded Rainlang metadata.
async fn add_sell_order_with_meta(
    rain: &RainOrderBook<impl Provider + Clone>,
    usdc_addr: Address,
    wt_aapl_addr: Address,
    rainlang_source: &str,
) -> (u64, OrderHash) {
    let max_amount: U256 = parse_units("10", 18).unwrap().into();
    let expression = format!("_ _: {max_amount} 100;:;");
    let meta = encode_rainlang_metadata(rainlang_source);

    let result = rain
        .add_order(&expression, meta, usdc_addr, wt_aapl_addr)
        .await
        .unwrap();

    (result.block, OrderHash::new(result.order_hash))
}

#[tokio::test]
async fn backfill_with_metadata_classifies_as_fixed_price() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, _projection, pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    let (add_block, order_hash) =
        add_sell_order_with_meta(&rain, usdc_addr, wt_aapl_addr, "_ _: 1000 185e18;:;").await;

    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, add_block, add_block)
            .await
            .unwrap();

    assert!(!meta_logs.is_empty(), "MetaV1_2 should be emitted");

    let processed =
        process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
            .await
            .unwrap();
    assert_eq!(processed, 1);

    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner,
        symbol,
        scenario,
        output_token,
        input_token,
        order_type,
        discovered_block,
        ..
    }) = &tracked
    else {
        panic!("Expected Active order, got: {tracked:?}");
    };

    assert_eq!(*owner, rain.chain.owner);
    assert_eq!(symbol.to_string(), "AAPL");
    assert_eq!(*scenario, Scenario::A);
    assert_eq!(*output_token, wt_aapl_addr);
    assert_eq!(*input_token, usdc_addr);
    assert_eq!(*discovered_block, add_block);
    match order_type {
        OrderType::FixedPrice { rainlang } => {
            assert_eq!(rainlang, "_ _: 1000 185e18;:;");
        }
        other => panic!("Expected FixedPrice, got: {other:?}"),
    }

    let cursor = BlockCursor::new(&pool);
    assert_eq!(cursor.last_block().await.unwrap(), None);
}

#[tokio::test]
async fn late_classification_updates_unknown_order() {
    let (rain, usdc_addr, wt_aapl_addr) = setup().await;
    let (store, _projection, _pool) = setup_cqrs().await;

    let bot_address = Address::repeat_byte(0xB0);
    let filter = test_filter(Address::repeat_byte(0xFF), usdc_addr, wt_aapl_addr);
    let processor = EventProcessor::new(store.clone(), filter, bot_address);

    // Add order WITHOUT metadata — gets discovered as Unknown
    let (add_block, order_hash) = add_sell_order(&rain, usdc_addr, wt_aapl_addr).await;

    let (add_logs, remove_logs, take_logs, meta_logs) =
        fetch_batch_logs(&rain.chain.provider, rain.orderbook, add_block, add_block)
            .await
            .unwrap();

    assert!(meta_logs.is_empty(), "No MetaV1_2 for order without meta");

    process_backfill_logs(&processor, &add_logs, &remove_logs, &take_logs, &meta_logs)
        .await
        .unwrap();

    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner,
        symbol,
        scenario,
        output_token,
        input_token,
        order_type,
        discovered_block,
        ..
    }) = &tracked
    else {
        panic!("Expected Active order, got: {tracked:?}");
    };

    assert_eq!(*owner, rain.chain.owner);
    assert_eq!(symbol.to_string(), "AAPL");
    assert_eq!(*scenario, Scenario::A);
    assert_eq!(*output_token, wt_aapl_addr);
    assert_eq!(*input_token, usdc_addr);
    assert_eq!(*discovered_block, add_block);
    assert_eq!(*order_type, OrderType::Unknown);

    // Simulate late MetaV1_2: classify the order after discovery
    let rainlang_source = "_ _: 1000 185e18;:;";
    processor
        .classify_order(
            order_hash.into_inner(),
            &encode_rainlang_metadata(rainlang_source),
        )
        .await
        .unwrap();

    // Verify classification updated while all other fields preserved
    let tracked = store.load(&order_hash).await.unwrap();
    let Some(TrackedOrder::Active {
        owner: owner_after,
        symbol: symbol_after,
        scenario: scenario_after,
        output_token: output_after,
        input_token: input_after,
        order_type: type_after,
        discovered_block: block_after,
        ..
    }) = &tracked
    else {
        panic!("Expected Active order after classification, got: {tracked:?}");
    };

    match type_after {
        OrderType::FixedPrice { rainlang } => {
            assert_eq!(rainlang, rainlang_source);
        }
        other => panic!("Expected FixedPrice after late classification, got: {other:?}"),
    }

    assert_eq!(*owner_after, rain.chain.owner);
    assert_eq!(symbol_after.to_string(), "AAPL");
    assert_eq!(*scenario_after, Scenario::A);
    assert_eq!(*output_after, wt_aapl_addr);
    assert_eq!(*input_after, usdc_addr);
    assert_eq!(*block_after, add_block);
}
