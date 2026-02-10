use alloy::primitives::{Address, TxHash, address, fixed_bytes};
use chrono::Utc;
use cqrs_es::persist::GenericQuery;
use httpmock::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::json;
use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

use super::fetch_events;
use crate::alpaca_tokenization::tests::{
    TEST_REDEMPTION_WALLET, create_test_service_from_mock, setup_anvil, tokenization_mint_path,
    tokenization_requests_path,
};
use crate::inventory::{ImbalanceThreshold, InventoryView};
use crate::lifecycle::{Lifecycle, Never};
use crate::offchain_order::{OffchainOrder, PriceCents};
use crate::position::{Position, PositionAggregate, PositionCommand};
use crate::rebalancing::mint::mock::MockMint;
use crate::rebalancing::redemption::mock::MockRedeem;
use crate::rebalancing::usdc::mock::MockUsdcRebalance;
use crate::rebalancing::{MintManager, Rebalancer, RebalancingTrigger, RebalancingTriggerConfig};
use crate::test_utils::setup_test_db;
use crate::threshold::{ExecutionThreshold, Usdc};
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::vault_registry::{VaultRegistry, VaultRegistryCommand};
use st0x_execution::{
    Direction, ExecutorOrderId, FractionalShares, Positive, SupportedExecutor, Symbol,
};

const TEST_ORDERBOOK: Address = address!("0x0000000000000000000000000000000000000001");
const TEST_ORDER_OWNER: Address = address!("0x0000000000000000000000000000000000000002");
const TEST_TOKEN: Address = address!("0x1234567890123456789012345678901234567890");

fn shares(n: i64) -> FractionalShares {
    FractionalShares::new(Decimal::from(n))
}

async fn seed_vault_registry(pool: &SqlitePool, symbol: &Symbol) {
    let cqrs = sqlite_cqrs::<Lifecycle<VaultRegistry, Never>>(pool.clone(), vec![], ());
    let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_ORDER_OWNER);

    cqrs.execute(
        &aggregate_id,
        VaultRegistryCommand::DiscoverEquityVault {
            token: TEST_TOKEN,
            vault_id: fixed_bytes!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            ),
            discovered_in: TxHash::ZERO,
            symbol: symbol.clone(),
        },
    )
    .await
    .unwrap();
}

fn test_trigger_config() -> RebalancingTriggerConfig {
    RebalancingTriggerConfig {
        equity_threshold: ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        },
        usdc_threshold: ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.2),
        },
    }
}

/// Mirrors the private `build_position_cqrs()` from `src/conductor/mod.rs`,
/// wiring the RebalancingTrigger as a Position CQRS query processor so that
/// position events flow through trigger.dispatch() -> inventory update.
fn build_position_cqrs_with_trigger(
    pool: &SqlitePool,
    trigger: &Arc<RebalancingTrigger>,
) -> Arc<sqlite_es::SqliteCqrs<PositionAggregate>> {
    let view_repo = Arc::new(
        SqliteViewRepository::<PositionAggregate, PositionAggregate>::new(
            pool.clone(),
            "position_view".to_string(),
        ),
    );

    let queries: Vec<Box<dyn cqrs_es::Query<PositionAggregate>>> = vec![
        Box::new(GenericQuery::new(view_repo)),
        Box::new(Arc::clone(trigger)),
    ];

    Arc::new(sqlite_cqrs(pool.clone(), queries, ()))
}

fn sample_pending_response(id: &str) -> serde_json::Value {
    json!({
        "tokenization_request_id": id,
        "type": "mint",
        "status": "pending",
        "underlying_symbol": "AAPL",
        "token_symbol": "tAAPL",
        "qty": "30.0",
        "issuer": "st0x",
        "network": "base",
        "wallet_address": "0x0000000000000000000000000000000000000000",
        "issuer_request_id": "issuer_123",
        "created_at": "2024-01-15T10:30:00Z"
    })
}

fn sample_completed_response(id: &str) -> serde_json::Value {
    json!({
        "tokenization_request_id": id,
        "type": "mint",
        "status": "completed",
        "underlying_symbol": "AAPL",
        "token_symbol": "tAAPL",
        "qty": "30.0",
        "issuer": "st0x",
        "network": "base",
        "wallet_address": "0x0000000000000000000000000000000000000000",
        "issuer_request_id": "issuer_123",
        "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        "created_at": "2024-01-15T10:30:00Z"
    })
}

fn assert_event_types(events: &[super::StoredEvent], aggregate_type: &str, expected: &[&str]) {
    let actual: Vec<&str> = events
        .iter()
        .filter(|e| e.aggregate_type == aggregate_type)
        .map(|e| e.event_type.as_str())
        .collect();
    assert_eq!(actual, expected, "Event type mismatch for {aggregate_type}");
}

type PositionCqrs = sqlite_es::SqliteCqrs<PositionAggregate>;

fn onchain_fill_command(
    amount: FractionalShares,
    direction: Direction,
    log_index: u64,
) -> PositionCommand {
    PositionCommand::AcknowledgeOnChainFill {
        trade_id: crate::position::TradeId {
            tx_hash: TxHash::random(),
            log_index,
        },
        amount,
        direction,
        price_usdc: dec!(150.0),
        block_timestamp: Utc::now(),
    }
}

/// Builds inventory state: 20 onchain, 80 offchain = 20% ratio (below 30% lower bound).
/// VaultRegistry is NOT seeded, so the trigger silently skips even though inventory is imbalanced.
async fn build_imbalanced_equity_inventory(position_cqrs: &PositionCqrs, aggregate_id: &str) {
    position_cqrs
        .execute(
            aggregate_id,
            onchain_fill_command(shares(100), Direction::Buy, 0),
        )
        .await
        .unwrap();

    position_cqrs
        .execute(
            aggregate_id,
            onchain_fill_command(shares(80), Direction::Sell, 1),
        )
        .await
        .unwrap();

    let offchain_order_id = OffchainOrder::aggregate_id();
    position_cqrs
        .execute(
            aggregate_id,
            PositionCommand::PlaceOffChainOrder {
                offchain_order_id,
                shares: Positive::new(shares(80)).unwrap(),
                direction: Direction::Buy,
                executor: SupportedExecutor::AlpacaTradingApi,
                threshold: ExecutionThreshold::whole_share(),
            },
        )
        .await
        .unwrap();

    position_cqrs
        .execute(
            aggregate_id,
            PositionCommand::CompleteOffChainOrder {
                offchain_order_id,
                shares_filled: shares(80),
                direction: Direction::Buy,
                executor_order_id: ExecutorOrderId::new("ORD1"),
                price_cents: PriceCents(15000),
                broker_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();
}

/// Test 4: Position CQRS events -> RebalancingTrigger -> Rebalancer -> MintManager -> Aggregate
///
/// Drives InventoryView state through real Position CQRS commands (not direct
/// `apply_position_event` calls), verifying the production wiring where
/// RebalancingTrigger receives events as a Query processor, updates inventory,
/// detects imbalance, and sends a TriggeredOperation to the Rebalancer.
#[tokio::test]
async fn position_events_trigger_equity_mint_through_rebalancer() {
    let pool = setup_test_db().await;
    let symbol = Symbol::new("AAPL").unwrap();
    let aggregate_id = Position::aggregate_id(&symbol);

    let inventory = Arc::new(RwLock::new(
        InventoryView::default().with_equity(symbol.clone()),
    ));
    let (sender, receiver) = mpsc::channel(10);

    let trigger = Arc::new(RebalancingTrigger::new(
        test_trigger_config(),
        pool.clone(),
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
    ));

    let position_cqrs = build_position_cqrs_with_trigger(&pool, &trigger);

    // Build inventory: 20 onchain, 80 offchain = 20% ratio -> TooMuchOffchain.
    // Without VaultRegistry seeded, the trigger silently skips Mint operations.
    build_imbalanced_equity_inventory(&position_cqrs, &aggregate_id).await;

    // Now seed VaultRegistry so the next Position event triggers a real Mint.
    seed_vault_registry(&pool, &symbol).await;

    let server = MockServer::start();
    let (_anvil, endpoint, key) = setup_anvil();
    let service = Arc::new(
        create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
    );

    let mint_cqrs = Arc::new(sqlite_cqrs::<Lifecycle<TokenizedEquityMint, Never>>(
        pool.clone(),
        vec![],
        (),
    ));
    let mint_mgr = MintManager::new(service, mint_cqrs);

    let mint_mock = server.mock(|when, then| {
        when.method(POST).path(tokenization_mint_path());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(sample_pending_response("mint_int_test"));
    });

    let poll_mock = server.mock(|when, then| {
        when.method(GET).path(tokenization_requests_path());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([sample_completed_response("mint_int_test")]));
    });

    let rebalancer = Rebalancer::new(
        Arc::new(mint_mgr),
        Arc::new(MockRedeem::new()),
        Arc::new(MockUsdcRebalance::new()),
        receiver,
        Address::ZERO,
    );

    // One more onchain sell triggers the CQRS -> trigger -> Mint flow now that
    // VaultRegistry is seeded. Inventory: 19 onchain, 80 offchain = 19.2%.
    position_cqrs
        .execute(
            &aggregate_id,
            onchain_fill_command(shares(1), Direction::Sell, 2),
        )
        .await
        .unwrap();

    // Both trigger and position_cqrs hold Arc<RebalancingTrigger> which owns the
    // mpsc sender. Both must be dropped to close the channel so rebalancer.run()
    // can exit after processing all queued operations.
    drop(trigger);
    drop(position_cqrs);

    rebalancer.run().await;

    mint_mock.assert();
    poll_mock.assert();

    let events = fetch_events(&pool).await;
    assert_event_types(
        &events,
        "Position",
        &[
            "PositionEvent::OnChainOrderFilled",
            "PositionEvent::OnChainOrderFilled",
            "PositionEvent::OffChainOrderPlaced",
            "PositionEvent::OffChainOrderFilled",
            "PositionEvent::OnChainOrderFilled",
        ],
    );
    assert_event_types(
        &events,
        "VaultRegistry",
        &["VaultRegistryEvent::EquityVaultDiscovered"],
    );
    assert_event_types(
        &events,
        "TokenizedEquityMint",
        &[
            "TokenizedEquityMintEvent::MintRequested",
            "TokenizedEquityMintEvent::MintAccepted",
            "TokenizedEquityMintEvent::TokensReceived",
            "TokenizedEquityMintEvent::MintCompleted",
        ],
    );

    // All mint events should share the same aggregate_id (a UUID assigned by the rebalancer).
    let mint_ids: Vec<_> = events
        .iter()
        .filter(|e| e.aggregate_type == "TokenizedEquityMint")
        .map(|e| &e.aggregate_id)
        .collect();
    assert!(
        mint_ids.windows(2).all(|w| w[0] == w[1]),
        "All mint events should share the same aggregate_id"
    );
}

/// Test 5: Trigger -> Rebalancer -> MockUsdcRebalance (USDC transfer flow)
///
/// Uses MockUsdcRebalance since the real UsdcRebalanceManager requires CCTP bridge
/// and vault services that perform actual Ethereum transactions. This still tests the
/// trigger -> channel -> rebalancer -> manager dispatch chain.
#[tokio::test]
async fn trigger_rebalancer_usdc_dispatch() {
    let pool = setup_test_db().await;

    // 100 onchain, 900 offchain = 10% onchain ratio -> below 30% -> TooMuchOffchain
    // Excess = target_onchain - onchain = 500 - 100 = 400 USDC (above $51 minimum)
    let inventory = InventoryView::default().with_usdc(Usdc(dec!(100)), Usdc(dec!(900)));
    let inventory = Arc::new(RwLock::new(inventory));

    let (sender, receiver) = mpsc::channel(10);

    let trigger = RebalancingTrigger::new(
        test_trigger_config(),
        pool.clone(),
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
    );

    let mint = Arc::new(MockMint::new());
    let redeem = Arc::new(MockRedeem::new());
    let usdc = Arc::new(MockUsdcRebalance::new());

    let rebalancer = Rebalancer::new(
        Arc::clone(&mint),
        Arc::clone(&redeem),
        Arc::clone(&usdc),
        receiver,
        Address::ZERO,
    );

    // Trigger detects the USDC imbalance and sends a UsdcAlpacaToBase operation.
    trigger.check_and_trigger_usdc().await;

    // Close the channel so the rebalancer exits after processing.
    drop(trigger);

    // Rebalancer receives the operation and dispatches to MockUsdcRebalance.
    rebalancer.run().await;

    assert_eq!(
        usdc.alpaca_to_base_calls(),
        1,
        "Expected USDC manager to be called once for alpaca_to_base"
    );

    let call = usdc
        .last_alpaca_to_base_call()
        .expect("Expected a captured call");
    assert_eq!(
        call.amount,
        Usdc(dec!(400)),
        "Expected excess of $400 (target $500 - actual $100)"
    );

    assert_eq!(mint.calls(), 0, "Mint should not have been called");
    assert_eq!(redeem.calls(), 0, "Redeem should not have been called");
    assert_eq!(
        usdc.base_to_alpaca_calls(),
        0,
        "base_to_alpaca should not have been called"
    );
}
