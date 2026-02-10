use alloy::primitives::{Address, TxHash, address, fixed_bytes};
use chrono::Utc;
use cqrs_es::persist::GenericQuery;
use httpmock::prelude::*;
use rust_decimal_macros::dec;
use serde_json::json;
use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

use super::{ExpectedEvent, assert_events, fetch_events};
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

type PositionCqrs = sqlite_es::SqliteCqrs<PositionAggregate>;

enum Imbalance<'a> {
    Equity {
        position_cqrs: &'a PositionCqrs,
        aggregate_id: &'a str,
        onchain: rust_decimal::Decimal,
        offchain: rust_decimal::Decimal,
    },
    Usdc {
        inventory: &'a Arc<RwLock<InventoryView>>,
        onchain: Usdc,
        offchain: Usdc,
    },
}

async fn build_imbalanced_inventory(imbalance: Imbalance<'_>) {
    match imbalance {
        Imbalance::Equity {
            position_cqrs,
            aggregate_id,
            onchain,
            offchain,
        } => {
            position_cqrs
                .execute(
                    aggregate_id,
                    PositionCommand::AcknowledgeOnChainFill {
                        trade_id: crate::position::TradeId {
                            tx_hash: TxHash::random(),
                            log_index: 0,
                        },
                        amount: FractionalShares::new(onchain),
                        direction: Direction::Buy,
                        price_usdc: dec!(150.0),
                        block_timestamp: Utc::now(),
                    },
                )
                .await
                .unwrap();

            let offchain_order_id = OffchainOrder::aggregate_id();

            position_cqrs
                .execute(
                    aggregate_id,
                    PositionCommand::PlaceOffChainOrder {
                        offchain_order_id,
                        shares: Positive::new(FractionalShares::new(offchain)).unwrap(),
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
                        shares_filled: FractionalShares::new(offchain),
                        direction: Direction::Buy,
                        executor_order_id: ExecutorOrderId::new("ORD1"),
                        price_cents: PriceCents(15000),
                        broker_timestamp: Utc::now(),
                    },
                )
                .await
                .unwrap();
        }

        Imbalance::Usdc {
            inventory,
            onchain,
            offchain,
        } => {
            let mut guard = inventory.write().await;
            let taken = std::mem::take(&mut *guard);
            *guard = taken.with_usdc(onchain, offchain);
        }
    }
}

/// Asserts the full event sequence for an equity mint pipeline: position events,
/// vault discovery, and the 4-step mint lifecycle.
async fn assert_mint_pipeline_events(pool: &SqlitePool, position_agg_id: &str, vault_agg_id: &str) {
    // Extract the mint aggregate_id (UUID assigned by the rebalancer at runtime).
    let events = fetch_events(pool).await;
    let mint_agg_id = events
        .iter()
        .find(|e| e.aggregate_type == "TokenizedEquityMint")
        .expect("Expected at least one TokenizedEquityMint event")
        .aggregate_id
        .clone();

    let e = |agg_type, agg_id: &str, event_type| ExpectedEvent::new(agg_type, agg_id, event_type);

    assert_events(
        pool,
        &[
            e(
                "Position",
                position_agg_id,
                "PositionEvent::OnChainOrderFilled",
            ),
            e(
                "Position",
                position_agg_id,
                "PositionEvent::OffChainOrderPlaced",
            ),
            e(
                "Position",
                position_agg_id,
                "PositionEvent::OffChainOrderFilled",
            ),
            e(
                "VaultRegistry",
                vault_agg_id,
                "VaultRegistryEvent::EquityVaultDiscovered",
            ),
            e(
                "Position",
                position_agg_id,
                "PositionEvent::OnChainOrderFilled",
            ),
            e(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::MintRequested",
            ),
            e(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::MintAccepted",
            ),
            e(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::TokensReceived",
            ),
            e(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::MintCompleted",
            ),
        ],
    )
    .await;
}

/// Verifies the full equity mint rebalancing pipeline: position CQRS commands
/// flow through the RebalancingTrigger (registered as a Query processor),
/// update the InventoryView, detect an equity imbalance, and dispatch a Mint
/// operation through the Rebalancer to the MintManager which drives the
/// TokenizedEquityMint aggregate to completion via the Alpaca tokenization API.
#[tokio::test]
async fn equity_offchain_imbalance_triggers_mint() {
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
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        aggregate_id: &aggregate_id,
        onchain: dec!(20),
        offchain: dec!(80),
    })
    .await;

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
            PositionCommand::AcknowledgeOnChainFill {
                trade_id: crate::position::TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 2,
                },
                amount: FractionalShares::new(dec!(1)),
                direction: Direction::Sell,
                price_usdc: dec!(150.0),
                block_timestamp: Utc::now(),
            },
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

    let vault_agg_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_ORDER_OWNER);
    assert_mint_pipeline_events(&pool, &aggregate_id, &vault_agg_id).await;
}

/// Verifies the equity redemption rebalancing pipeline: too much onchain equity
/// triggers a Redemption operation dispatched to MockRedeem. 79 onchain + 20
/// offchain is built without VaultRegistry, then after seeding, a Buy of 1 more
/// share (total 80 onchain / 20 offchain = 80% ratio > 70% upper) triggers the
/// Redemption with excess = 80 - 50 = 30 shares.
#[tokio::test]
async fn equity_onchain_imbalance_triggers_redemption() {
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

    // Build inventory: 79 onchain, 20 offchain = 79.8% ratio -> TooMuchOnchain.
    // Without VaultRegistry seeded, the trigger silently skips Redemption operations.
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        aggregate_id: &aggregate_id,
        onchain: dec!(79),
        offchain: dec!(20),
    })
    .await;

    // Now seed VaultRegistry so the next Position event triggers a real Redemption.
    seed_vault_registry(&pool, &symbol).await;

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

    // One more onchain buy triggers the CQRS -> trigger -> Redemption flow.
    // Inventory: 80 onchain, 20 offchain = 80%, excess = 80 - 50 = 30 shares.
    position_cqrs
        .execute(
            &aggregate_id,
            PositionCommand::AcknowledgeOnChainFill {
                trade_id: crate::position::TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 2,
                },
                amount: FractionalShares::new(dec!(1)),
                direction: Direction::Buy,
                price_usdc: dec!(150.0),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();

    drop(trigger);
    drop(position_cqrs);

    rebalancer.run().await;

    assert_eq!(redeem.calls(), 1, "Expected MockRedeem to be called once");

    let call = redeem.last_call().expect("Expected a captured redeem call");
    assert_eq!(call.symbol, symbol);
    assert_eq!(
        call.quantity,
        FractionalShares::new(dec!(30)),
        "Expected excess of 30 shares (target 50 - actual 80 inverted: 80 - 50)"
    );
    assert_eq!(
        call.token, TEST_TOKEN,
        "Token should match VaultRegistry entry"
    );

    assert_eq!(mint.calls(), 0, "Mint should not have been called");
    assert_eq!(
        usdc.alpaca_to_base_calls(),
        0,
        "USDC alpaca_to_base should not have been called"
    );
    assert_eq!(
        usdc.base_to_alpaca_calls(),
        0,
        "USDC base_to_alpaca should not have been called"
    );
}

/// Verifies USDC rebalancing dispatch: a USDC imbalance triggers the
/// RebalancingTrigger to send a UsdcAlpacaToBase operation through the channel
/// to the Rebalancer, which dispatches to the USDC manager. Uses mocked managers
/// since the real USDC flow requires CCTP bridge and vault transactions.
#[tokio::test]
async fn usdc_offchain_imbalance_triggers_alpaca_to_base() {
    let pool = setup_test_db().await;

    // 100 onchain, 900 offchain = 10% onchain ratio -> below 30% -> TooMuchOffchain
    // Excess = target_onchain - onchain = 500 - 100 = 400 USDC (above $51 minimum)
    let inventory = Arc::new(RwLock::new(InventoryView::default()));

    build_imbalanced_inventory(Imbalance::Usdc {
        inventory: &inventory,
        onchain: Usdc(dec!(100)),
        offchain: Usdc(dec!(900)),
    })
    .await;

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

/// Verifies USDC onchain imbalance dispatch: 900 onchain / 100 offchain = 90%
/// onchain ratio (above 70% upper bound) triggers a UsdcBaseToAlpaca operation
/// with excess = 900 - 500 = $400.
#[tokio::test]
async fn usdc_onchain_imbalance_triggers_base_to_alpaca() {
    let pool = setup_test_db().await;

    // 900 onchain, 100 offchain = 90% onchain ratio -> above 70% -> TooMuchOnchain
    // Excess = onchain - target_onchain = 900 - 500 = 400 USDC
    let inventory = Arc::new(RwLock::new(InventoryView::default()));

    build_imbalanced_inventory(Imbalance::Usdc {
        inventory: &inventory,
        onchain: Usdc(dec!(900)),
        offchain: Usdc(dec!(100)),
    })
    .await;

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

    // Trigger detects the USDC onchain imbalance and sends a UsdcBaseToAlpaca operation.
    trigger.check_and_trigger_usdc().await;

    drop(trigger);

    rebalancer.run().await;

    assert_eq!(
        usdc.base_to_alpaca_calls(),
        1,
        "Expected USDC manager to be called once for base_to_alpaca"
    );

    let call = usdc
        .last_base_to_alpaca_call()
        .expect("Expected a captured call");
    assert_eq!(
        call.amount,
        Usdc(dec!(400)),
        "Expected excess of $400 (actual $900 - target $500)"
    );

    assert_eq!(mint.calls(), 0, "Mint should not have been called");
    assert_eq!(redeem.calls(), 0, "Redeem should not have been called");
    assert_eq!(
        usdc.alpaca_to_base_calls(),
        0,
        "alpaca_to_base should not have been called"
    );
}
