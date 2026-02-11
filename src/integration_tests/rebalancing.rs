use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, TxHash, U256, address, keccak256};
use alloy::providers::ProviderBuilder;
use alloy::providers::ext::AnvilApi as _;
use alloy::signers::local::PrivateKeySigner;
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
use crate::bindings::{IERC20, TestERC20};
use crate::equity_redemption::EquityRedemption;
use crate::inventory::{ImbalanceThreshold, InventoryView};
use crate::lifecycle::{Lifecycle, Never};
use crate::offchain_order::{OffchainOrder, PriceCents};
use crate::position::{Position, PositionAggregate, PositionCommand};
use crate::rebalancing::mint::mock::MockMint;
use crate::rebalancing::redemption::mock::MockRedeem;
use crate::rebalancing::usdc::mock::MockUsdcRebalance;
use crate::rebalancing::{
    MintManager, Rebalancer, RebalancingTrigger, RebalancingTriggerConfig, RedemptionManager,
};
use crate::test_utils::setup_test_db;
use crate::threshold::{ExecutionThreshold, Usdc};
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::vault_registry::{VaultRegistry, VaultRegistryAggregate, VaultRegistryCommand};
use st0x_execution::{
    Direction, ExecutorOrderId, FractionalShares, Positive, SupportedExecutor, Symbol,
};

const TEST_ORDERBOOK: Address = address!("0x0000000000000000000000000000000000000001");
const TEST_ORDER_OWNER: Address = address!("0x0000000000000000000000000000000000000002");
/// Seeds the VaultRegistry with the given token address and a deterministic
/// vault ID derived from the symbol.
async fn seed_vault_registry(pool: &SqlitePool, symbol: &Symbol, token: Address) {
    let vault_id = B256::from(keccak256(symbol.to_string().as_bytes()));

    let cqrs = sqlite_cqrs::<VaultRegistryAggregate>(pool.clone(), vec![], ());
    let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_ORDER_OWNER);

    cqrs.execute(
        &aggregate_id,
        VaultRegistryCommand::DiscoverEquityVault {
            token,
            vault_id,
            discovered_in: TxHash::ZERO,
            symbol: symbol.clone(),
        },
    )
    .await
    .unwrap();
}

/// Uses Anvil snapshot/revert to discover the deterministic tx_hash that will
/// be produced by an ERC20 transfer. Anvil is deterministic: same sender +
/// nonce + calldata = same tx_hash.
async fn discover_deterministic_tx_hash(
    provider: &impl alloy::providers::Provider,
    token: Address,
    recipient: Address,
    amount: U256,
) -> TxHash {
    let snapshot_id = provider.anvil_snapshot().await.unwrap();

    let erc20 = IERC20::new(token, provider);
    let receipt = erc20
        .transfer(recipient, amount)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    let tx_hash = receipt.transaction_hash;

    provider.anvil_revert(snapshot_id).await.unwrap();

    tx_hash
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
    let token = Address::from_slice(&keccak256(symbol.to_string().as_bytes())[..20]);
    seed_vault_registry(&pool, &symbol, token).await;

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
        when.method(POST)
            .path(tokenization_mint_path())
            .json_body_partial(r#"{"underlying_symbol":"AAPL","qty":"30.5","wallet_address":"0x0000000000000000000000000000000000000000"}"#);
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

    // Extract the mint aggregate_id (UUID assigned by the rebalancer at runtime).
    let events = fetch_events(&pool).await;
    let mint_agg_id = events
        .iter()
        .find(|e| e.aggregate_type == "TokenizedEquityMint")
        .expect("Expected at least one TokenizedEquityMint event")
        .aggregate_id
        .clone();
    let vault_agg_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_ORDER_OWNER);

    assert_events(
        &pool,
        &[
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OnChainOrderFilled",
            ),
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OffChainOrderPlaced",
            ),
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OffChainOrderFilled",
            ),
            ExpectedEvent::new(
                "VaultRegistry",
                &vault_agg_id,
                "VaultRegistryEvent::EquityVaultDiscovered",
            ),
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OnChainOrderFilled",
            ),
            ExpectedEvent::new(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::MintRequested",
            ),
            ExpectedEvent::new(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::MintAccepted",
            ),
            ExpectedEvent::new(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::TokensReceived",
            ),
            ExpectedEvent::new(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::MintCompleted",
            ),
        ],
    )
    .await;
}

/// Verifies the full equity redemption rebalancing pipeline: position CQRS
/// commands flow through the RebalancingTrigger, detect too much onchain equity,
/// and dispatch a Redemption operation through the Rebalancer to the real
/// RedemptionManager. The manager sends tokens on Anvil, then drives the
/// EquityRedemption aggregate through TokensSent -> Detected -> Completed via
/// the mocked Alpaca tokenization API.
///
/// Uses Anvil snapshot/revert to discover the deterministic tx_hash before
/// setting up httpmock responses, so the mock detection endpoint can match
/// the exact hash produced by the real onchain transfer.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn equity_onchain_imbalance_triggers_redemption() {
    let pool = setup_test_db().await;
    let symbol = Symbol::new("AAPL").unwrap();
    let aggregate_id = Position::aggregate_id(&symbol);
    let server = MockServer::start();
    let (_anvil, endpoint, key) = setup_anvil();

    // Deploy TestERC20 and mint exactly 30 * 10^18 tokens (the redemption amount:
    // 80 onchain - 50 target = 30 shares excess).
    let signer = PrivateKeySigner::from_bytes(&key).unwrap();
    let wallet = EthereumWallet::from(signer.clone());
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(&endpoint)
        .await
        .unwrap();

    let token_contract = TestERC20::deploy(&provider).await.unwrap();
    let token_address = *token_contract.address();
    let transfer_amount = U256::from(30_000_000_000_000_000_000_u128);

    token_contract
        .mint(signer.address(), transfer_amount)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // Anvil is deterministic: same sender + nonce + calldata = same tx_hash.
    // Execute the transfer once to capture the hash, then revert so the real
    // RedemptionManager can execute the same transfer and get the same hash.
    let expected_tx_hash = discover_deterministic_tx_hash(
        &provider,
        token_address,
        TEST_REDEMPTION_WALLET,
        transfer_amount,
    )
    .await;

    let detection_mock = server.mock(|when, then| {
        when.method(GET)
            .path(tokenization_requests_path())
            .query_param("type", "redeem");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([{
                "tokenization_request_id": "redeem_int_test",
                "type": "redeem",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "30.0",
                "issuer": "st0x",
                "network": "base",
                "tx_hash": expected_tx_hash,
                "created_at": "2024-01-15T10:30:00Z"
            }]));
    });

    let completion_mock = server.mock(|when, then| {
        when.method(GET).path(tokenization_requests_path());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([{
                "tokenization_request_id": "redeem_int_test",
                "type": "redeem",
                "status": "completed",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "30.0",
                "issuer": "st0x",
                "network": "base",
                "tx_hash": expected_tx_hash,
                "created_at": "2024-01-15T10:30:00Z"
            }]));
    });

    let service = Arc::new(
        create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
    );

    let redemption_cqrs = Arc::new(sqlite_cqrs::<Lifecycle<EquityRedemption, Never>>(
        pool.clone(),
        vec![],
        (),
    ));
    let redeem_mgr = RedemptionManager::new(service, redemption_cqrs);

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

    // Seed VaultRegistry with the deployed TestERC20 address so the trigger
    // passes the real token address to the RedemptionManager.
    seed_vault_registry(&pool, &symbol, token_address).await;

    let mint = Arc::new(MockMint::new());
    let usdc = Arc::new(MockUsdcRebalance::new());

    let rebalancer = Rebalancer::new(
        Arc::clone(&mint),
        Arc::new(redeem_mgr),
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

    // Verify onchain balances after the ERC20 transfer
    let erc20 = IERC20::new(token_address, &provider);
    let sender_balance = erc20.balanceOf(signer.address()).call().await.unwrap();
    assert_eq!(
        sender_balance,
        U256::ZERO,
        "Sender should have 0 tokens after redemption transfer"
    );
    let recipient_balance = erc20
        .balanceOf(TEST_REDEMPTION_WALLET)
        .call()
        .await
        .unwrap();
    assert_eq!(
        recipient_balance, transfer_amount,
        "Redemption wallet should have received the tokens"
    );

    detection_mock.assert();
    completion_mock.assert();

    // Extract the redemption aggregate_id (UUID assigned by the rebalancer).
    let events = fetch_events(&pool).await;
    let redemption_agg_id = events
        .iter()
        .find(|e| e.aggregate_type == "EquityRedemption")
        .expect("Expected at least one EquityRedemption event")
        .aggregate_id
        .clone();
    let vault_agg_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_ORDER_OWNER);

    assert_events(
        &pool,
        &[
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OnChainOrderFilled",
            ),
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OffChainOrderPlaced",
            ),
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OffChainOrderFilled",
            ),
            ExpectedEvent::new(
                "VaultRegistry",
                &vault_agg_id,
                "VaultRegistryEvent::EquityVaultDiscovered",
            ),
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OnChainOrderFilled",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::TokensSent",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::Detected",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::Completed",
            ),
        ],
    )
    .await;

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
