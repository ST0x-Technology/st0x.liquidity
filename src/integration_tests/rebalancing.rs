//! Integration tests for the inventory rebalancing pipeline: position changes
//! flow through the RebalancingTrigger (wired as a CQRS query processor),
//! update the InventoryView, detect equity or USDC imbalances, and dispatch
//! operations through the Rebalancer to drive mints, redemptions, and USDC
//! transfers to completion.

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, TxHash, U256, address, keccak256};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use chrono::Utc;
use httpmock::Mock;
use httpmock::prelude::*;
use serde_json::json;
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

use rain_math_float::Float;
use st0x_event_sorcery::{Store, StoreBuilder, test_store};
use st0x_execution::{
    Direction, ExecutorOrderId, FractionalShares, Positive, SupportedExecutor, Symbol,
};

use super::{ExpectedEvent, assert_events, fetch_events};
use crate::bindings::{IERC20, TestERC20};
use crate::config::{
    AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, OperationMode,
};
use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId};
use crate::inventory::{ImbalanceThreshold, InventoryView, Venue};
use crate::offchain_order::{Dollars, OffchainOrderId};
use crate::onchain::mock::MockRaindex;
use crate::onchain::raindex::Raindex;
use crate::position::{Position, PositionCommand, TradeId};
use crate::rebalancing::equity::mock::MockCrossVenueEquityTransfer;
use crate::rebalancing::equity::{CrossVenueEquityTransfer, Equity, EquityTransferServices};
use crate::rebalancing::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use crate::rebalancing::usdc::mock::MockUsdcRebalance;
use crate::rebalancing::{
    Rebalancer, RebalancingTrigger, RebalancingTriggerConfig, TriggeredOperation,
};
use crate::test_utils::setup_test_db;
use crate::threshold::{ExecutionThreshold, Usdc};
use crate::tokenization::Tokenizer;
use crate::tokenization::alpaca::tests::{
    TEST_REDEMPTION_WALLET, create_test_service_from_mock, setup_anvil, tokenization_mint_path,
    tokenization_requests_path,
};
use crate::tokenization::mock::MockTokenizer;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::vault_registry::{VaultRegistry, VaultRegistryCommand, VaultRegistryId};
use crate::wrapper::mock::MockWrapper;

const TEST_ORDERBOOK: Address = address!("0x0000000000000000000000000000000000000001");
const TEST_ORDER_OWNER: Address = address!("0x0000000000000000000000000000000000000002");
/// Seeds the VaultRegistry with the given token address and a deterministic
/// vault ID derived from the symbol.
async fn seed_vault_registry(pool: &SqlitePool, symbol: &Symbol, token: Address) {
    let vault_id = B256::from(keccak256(symbol.to_string().as_bytes()));

    let cqrs = test_store::<VaultRegistry>(pool.clone(), ());
    let id = VaultRegistryId {
        orderbook: TEST_ORDERBOOK,
        owner: TEST_ORDER_OWNER,
    };

    cqrs.send(
        &id,
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
    provider: &impl Provider,
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
        equity: ImbalanceThreshold {
            target: float!("0.5"),
            deviation: float!("0.2"),
        },
        usdc: Some(ImbalanceThreshold {
            target: float!("0.5"),
            deviation: float!("0.2"),
        }),
        assets: AssetsConfig {
            equities: EquitiesConfig {
                symbols: HashMap::from([(
                    Symbol::new("AAPL").unwrap(),
                    EquityAssetConfig {
                        tokenized_equity: Address::ZERO,
                        tokenized_equity_derivative: Address::ZERO,
                        vault_id: None,
                        trading: OperationMode::Disabled,
                        rebalancing: OperationMode::Enabled,
                        operational_limit: None,
                    },
                )]),
            },
            cash: Some(CashAssetConfig {
                vault_id: None,
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
            }),
        },
        disabled_assets: HashSet::new(),
    }
}

/// Mirrors the private `build_position_cqrs()` from `src/conductor/mod.rs`,
/// wiring the RebalancingTrigger as a Position CQRS query processor so that
/// position events flow through trigger.dispatch() -> inventory update.
async fn build_position_cqrs_with_trigger(
    pool: &SqlitePool,
    trigger: &Arc<RebalancingTrigger>,
) -> Arc<Store<Position>> {
    let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
        .with(Arc::clone(trigger))
        .build(())
        .await
        .unwrap();

    store
}

/// Shared state for equity rebalancing tests (mint and redemption) that
/// wires up the Position CQRS with a RebalancingTrigger as a query processor.
struct EquityTriggerFixture {
    pool: SqlitePool,
    symbol: Symbol,
    aggregate_id: String,
    trigger: Arc<RebalancingTrigger>,
    inventory: Arc<RwLock<InventoryView>>,
    position_cqrs: Arc<Store<Position>>,
    receiver: mpsc::Receiver<TriggeredOperation>,
}

async fn setup_equity_trigger() -> EquityTriggerFixture {
    let pool = setup_test_db().await;
    let symbol = Symbol::new("AAPL").unwrap();
    let aggregate_id = symbol.to_string();

    let inventory = Arc::new(RwLock::new(
        InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::ZERO,
            )
            .with_usdc(Usdc(float!("1000000")), Usdc(float!("1000000"))),
    ));
    let (sender, receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));

    let wrapper = Arc::new(MockWrapper::new());

    let trigger = Arc::new(RebalancingTrigger::new(
        test_trigger_config(),
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
    ));

    let position_cqrs = build_position_cqrs_with_trigger(&pool, &trigger).await;

    EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id,
        trigger,
        inventory,
        position_cqrs,
        receiver,
    }
}

/// Creates httpmock responses for the Alpaca tokenization API detection and
/// completion polling endpoints, matching the given tx_hash.
fn setup_redemption_mocks(server: &MockServer, expected_tx_hash: TxHash) -> (Mock<'_>, Mock<'_>) {
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

    (detection_mock, completion_mock)
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

fn sample_completed_response(id: &str, tx_hash: TxHash) -> serde_json::Value {
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
        "tx_hash": tx_hash,
        "created_at": "2024-01-15T10:30:00Z"
    })
}

enum Imbalance<'a> {
    Equity {
        position_cqrs: &'a Store<Position>,
        symbol: &'a Symbol,
        onchain: Float,
        offchain: Float,
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
            symbol,
            onchain,
            offchain,
        } => {
            position_cqrs
                .send(
                    symbol,
                    PositionCommand::AcknowledgeOnChainFill {
                        symbol: symbol.clone(),
                        threshold: ExecutionThreshold::whole_share(),
                        trade_id: TradeId {
                            tx_hash: TxHash::random(),
                            log_index: 0,
                        },
                        amount: FractionalShares::new(onchain),
                        direction: Direction::Buy,
                        price_usdc: float!("150.0"),
                        block_timestamp: Utc::now(),
                    },
                )
                .await
                .unwrap();

            let offchain_order_id = OffchainOrderId::new();

            position_cqrs
                .send(
                    symbol,
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
                .send(
                    symbol,
                    PositionCommand::CompleteOffChainOrder {
                        offchain_order_id,
                        shares_filled: Positive::new(FractionalShares::new(offchain)).unwrap(),
                        direction: Direction::Buy,
                        executor_order_id: ExecutorOrderId::new("ORD1"),
                        price: Dollars(float!("150")),
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

fn build_equity_transfer_with_wrapper(
    pool: &SqlitePool,
    raindex: Arc<dyn crate::onchain::raindex::Raindex>,
    tokenizer: Arc<dyn Tokenizer>,
    mock_wrapper: MockWrapper,
    wallet: Address,
) -> Arc<CrossVenueEquityTransfer> {
    let wrapper: Arc<dyn crate::wrapper::Wrapper> = Arc::new(mock_wrapper);

    let equity_services = EquityTransferServices {
        raindex: Arc::clone(&raindex),
        tokenizer: Arc::clone(&tokenizer),
        wrapper: Arc::clone(&wrapper),
    };
    let mint_store = Arc::new(test_store::<TokenizedEquityMint>(
        pool.clone(),
        equity_services.clone(),
    ));
    let redemption_store = Arc::new(test_store::<EquityRedemption>(
        pool.clone(),
        equity_services,
    ));
    Arc::new(CrossVenueEquityTransfer::new(
        raindex,
        tokenizer,
        wrapper,
        wallet,
        mint_store,
        redemption_store,
    ))
}

/// Builds `CrossVenueEquityTransfer` with mint and redemption stores wired to
/// the `RebalancingTrigger` as a query processor. This mirrors the production
/// wiring in `Conductor` via `QueryManifest::build()`, ensuring mint/redemption
/// lifecycle events flow through the trigger and update inflight state.
async fn build_equity_transfer_with_trigger(
    pool: &SqlitePool,
    raindex: Arc<dyn crate::onchain::raindex::Raindex>,
    tokenizer: Arc<dyn Tokenizer>,
    mock_wrapper: MockWrapper,
    wallet: Address,
    trigger: &Arc<RebalancingTrigger>,
) -> Arc<CrossVenueEquityTransfer> {
    let wrapper: Arc<dyn crate::wrapper::Wrapper> = Arc::new(mock_wrapper);

    let equity_services = EquityTransferServices {
        raindex: Arc::clone(&raindex),
        tokenizer: Arc::clone(&tokenizer),
        wrapper: Arc::clone(&wrapper),
    };

    let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
        .with(Arc::clone(trigger))
        .build(equity_services.clone())
        .await
        .unwrap();

    let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
        .with(Arc::clone(trigger))
        .build(equity_services)
        .await
        .unwrap();

    Arc::new(CrossVenueEquityTransfer::new(
        raindex,
        tokenizer,
        wrapper,
        wallet,
        mint_store,
        redemption_store,
    ))
}

/// Verifies the full equity mint rebalancing pipeline: position CQRS commands
/// flow through the RebalancingTrigger (registered as a Query processor),
/// update the InventoryView, detect an equity imbalance, and dispatch a Mint
/// operation through the Rebalancer to the CrossVenueEquityTransfer which
/// drives the TokenizedEquityMint aggregate to completion via the Alpaca
/// tokenization API.
#[tokio::test]
async fn equity_offchain_imbalance_triggers_mint() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id,
        trigger,
        inventory: _,
        position_cqrs,
        receiver,
    } = setup_equity_trigger().await;

    // Build inventory: 20 onchain, 80 offchain = 20% ratio -> TooMuchOffchain.
    // Without VaultRegistry seeded, the trigger silently skips Mint operations.
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!("20"),
        offchain: float!("80"),
    })
    .await;

    let server = MockServer::start();
    let (_anvil, endpoint, key) = setup_anvil();

    // Deploy a real ERC20 on Anvil so that verify_mint_tx can find the
    // transaction receipt and confirm the token balance.
    let signer = PrivateKeySigner::from_bytes(&key).unwrap();
    let wallet = EthereumWallet::from(signer.clone());
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(&endpoint)
        .await
        .unwrap();

    let token_contract = TestERC20::deploy(&provider).await.unwrap();
    let token_address = *token_contract.address();

    // The imbalance produces a 30.5 share mint (19 onchain, 80 offchain,
    // target 49.5). Mint the equivalent amount to the signer's address so
    // balanceOf passes the verification check.
    let mint_amount = U256::from(30_500_000_000_000_000_000_u128);
    let mint_receipt = token_contract
        .mint(signer.address(), mint_amount)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    let mint_tx_hash = mint_receipt.transaction_hash;

    // Seed VaultRegistry so the next Position event triggers a real Mint.
    seed_vault_registry(&pool, &symbol, token_address).await;

    let tokenizer: Arc<dyn Tokenizer> = Arc::new(
        create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
    );
    let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

    // Configure MockWrapper to return the real TestERC20 address for
    // lookup_tokenized_equity so that verify_mint_tx checks the correct contract.
    let mock_wrapper = MockWrapper::new().with_unwrapped_token(token_address);
    let equity_transfer = build_equity_transfer_with_wrapper(
        &pool,
        raindex,
        tokenizer,
        mock_wrapper,
        signer.address(),
    );

    let wallet_hex = format!("{:#x}", signer.address());

    // json_body_partial acts as an implicit assertion: the mock only matches if
    // the request contains these exact fields. mint_mock.assert() below then
    // verifies the mock was called, confirming the correct qty was sent.
    let mint_mock = server.mock(|when, then| {
        when.method(POST)
            .path(tokenization_mint_path())
            .json_body_includes(
                json!({
                    "underlying_symbol": "AAPL",
                    "qty": "30.5",
                    "wallet_address": wallet_hex,
                })
                .to_string(),
            );
        then.status(200)
            .header("content-type", "application/json")
            .json_body(sample_pending_response("mint_int_test"));
    });

    let poll_mock = server.mock(|when, then| {
        when.method(GET).path(tokenization_requests_path());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([sample_completed_response(
                "mint_int_test",
                mint_tx_hash
            )]));
    });

    let mock_equity = Arc::new(MockCrossVenueEquityTransfer::new());
    let mock_usdc = Arc::new(MockUsdcRebalance::new());

    let rebalancer = Rebalancer::new(
        equity_transfer as _,
        mock_equity as _,
        Arc::clone(&mock_usdc) as _,
        mock_usdc as _,
        receiver,
    );

    // One more onchain sell triggers the CQRS -> trigger -> Mint flow now that
    // VaultRegistry is seeded. Inventory: 19 onchain, 80 offchain = 19.2%.
    position_cqrs
        .send(
            &symbol,
            PositionCommand::AcknowledgeOnChainFill {
                symbol: symbol.clone(),
                threshold: ExecutionThreshold::whole_share(),
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 2,
                },
                amount: FractionalShares::new(float!("1")),
                direction: Direction::Sell,
                price_usdc: float!("150.0"),
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

    let events = fetch_events(&pool).await;
    let mint_agg_id = events
        .iter()
        .find(|event| event.aggregate_type == "TokenizedEquityMint")
        .expect("Expected at least one TokenizedEquityMint event")
        .aggregate_id
        .clone();
    let vault_agg_id = VaultRegistryId {
        orderbook: TEST_ORDERBOOK,
        owner: TEST_ORDER_OWNER,
    }
    .to_string();

    let events = assert_events(
        &pool,
        &[
            ExpectedEvent::new("Position", &aggregate_id, "PositionEvent::Initialized"),
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
                "TokenizedEquityMintEvent::TokensWrapped",
            ),
            ExpectedEvent::new(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::DepositedIntoRaindex",
            ),
        ],
    )
    .await;

    let mint_requested = &events[6].payload["MintRequested"];
    assert_eq!(
        mint_requested["symbol"].as_str().unwrap(),
        "AAPL",
        "MintRequested should target the correct symbol"
    );

    let mint_accepted = &events[7].payload["MintAccepted"];
    assert_eq!(
        mint_accepted["tokenization_request_id"].as_str().unwrap(),
        "mint_int_test",
        "MintAccepted should capture the request ID from the API response"
    );
}

/// Verifies the full equity redemption rebalancing pipeline: position CQRS
/// commands flow through the RebalancingTrigger, detect too much onchain equity,
/// and dispatch a Redemption operation through the Rebalancer to the real
/// CrossVenueEquityTransfer. The transfer sends tokens on Anvil, then drives the
/// EquityRedemption aggregate through TokensSent -> Detected -> Completed via
/// the mocked Alpaca tokenization API.
///
/// Uses Anvil snapshot/revert to discover the deterministic tx_hash before
/// setting up httpmock responses, so the mock detection endpoint can match
/// the exact hash produced by the real onchain transfer.
#[tokio::test]
async fn equity_onchain_imbalance_triggers_redemption() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id,
        trigger,
        inventory: _,
        position_cqrs,
        receiver,
    } = setup_equity_trigger().await;
    let server = MockServer::start();
    let (_anvil, endpoint, key) = setup_anvil();

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

    let expected_tx_hash = discover_deterministic_tx_hash(
        &provider,
        token_address,
        TEST_REDEMPTION_WALLET,
        transfer_amount,
    )
    .await;

    let (detection_mock, completion_mock) = setup_redemption_mocks(&server, expected_tx_hash);
    let tokenizer: Arc<dyn Tokenizer> = Arc::new(
        create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
    );
    let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new().with_token(token_address));
    let wrapper = MockWrapper::new().with_unwrapped_token(token_address);
    let equity_transfer =
        build_equity_transfer_with_wrapper(&pool, raindex, tokenizer, wrapper, Address::ZERO);

    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!("79"),
        offchain: float!("20"),
    })
    .await;
    seed_vault_registry(&pool, &symbol, token_address).await;

    let mock_equity = Arc::new(MockCrossVenueEquityTransfer::new());
    let usdc = Arc::new(MockUsdcRebalance::new());
    let rebalancer = Rebalancer::new(
        Arc::clone(&mock_equity) as _,
        equity_transfer as _,
        Arc::clone(&usdc) as _,
        usdc as _,
        receiver,
    );

    position_cqrs
        .send(
            &symbol,
            PositionCommand::AcknowledgeOnChainFill {
                symbol: symbol.clone(),
                threshold: ExecutionThreshold::whole_share(),
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 2,
                },
                amount: FractionalShares::new(float!("1")),
                direction: Direction::Buy,
                price_usdc: float!("150.0"),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();

    drop(trigger);
    drop(position_cqrs);
    rebalancer.run().await;

    let erc20 = IERC20::new(token_address, &provider);
    assert_eq!(
        erc20.balanceOf(signer.address()).call().await.unwrap(),
        U256::ZERO,
        "Sender should have 0 tokens after redemption transfer"
    );
    assert_eq!(
        erc20
            .balanceOf(TEST_REDEMPTION_WALLET)
            .call()
            .await
            .unwrap(),
        transfer_amount,
        "Redemption wallet should have received the tokens"
    );

    detection_mock.assert();
    completion_mock.assert();

    let events = fetch_events(&pool).await;
    let redemption_agg_id = events
        .iter()
        .find(|event| event.aggregate_type == "EquityRedemption")
        .expect("Expected at least one EquityRedemption event")
        .aggregate_id
        .clone();
    let vault_agg_id = VaultRegistryId {
        orderbook: TEST_ORDERBOOK,
        owner: TEST_ORDER_OWNER,
    }
    .to_string();

    let events = assert_events(
        &pool,
        &[
            ExpectedEvent::new("Position", &aggregate_id, "PositionEvent::Initialized"),
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
                "EquityRedemptionEvent::WithdrawnFromRaindex",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::TokensUnwrapped",
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

    assert_eq!(
        events[6].payload["WithdrawnFromRaindex"]["symbol"]
            .as_str()
            .unwrap(),
        "AAPL",
        "WithdrawnFromRaindex should target the correct symbol"
    );
    assert_eq!(
        events[8].payload["TokensSent"]["redemption_tx"]
            .as_str()
            .unwrap(),
        format!("{expected_tx_hash:#x}"),
        "TokensSent redemption_tx should match the deterministic Anvil hash"
    );
    assert_eq!(
        events[9].payload["Detected"]["tokenization_request_id"]
            .as_str()
            .unwrap(),
        "redeem_int_test",
        "Detected should capture the request ID from the API response"
    );
    assert_eq!(
        mock_equity.mint_calls(),
        0,
        "Mint should not have been called"
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
        onchain: Usdc(float!("100")),
        offchain: Usdc(float!("900")),
    })
    .await;

    let (sender, receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingTrigger::new(
        test_trigger_config(),
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
    );

    let mock_equity = Arc::new(MockCrossVenueEquityTransfer::new());
    let usdc = Arc::new(MockUsdcRebalance::new());

    let rebalancer = Rebalancer::new(
        Arc::clone(&mock_equity) as _,
        mock_equity as _,
        Arc::clone(&usdc) as _,
        usdc.clone() as _,
        receiver,
    );

    trigger.check_and_trigger_usdc().await;

    // Close the channel so the rebalancer exits after processing.
    drop(trigger);

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
        Usdc(float!("400")),
        "Expected excess of $400 (target $500 - actual $100)"
    );

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
        onchain: Usdc(float!("900")),
        offchain: Usdc(float!("100")),
    })
    .await;

    let (sender, receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingTrigger::new(
        test_trigger_config(),
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
    );

    let mock_equity = Arc::new(MockCrossVenueEquityTransfer::new());
    let usdc = Arc::new(MockUsdcRebalance::new());

    let rebalancer = Rebalancer::new(
        Arc::clone(&mock_equity) as _,
        mock_equity as _,
        Arc::clone(&usdc) as _,
        usdc.clone() as _,
        receiver,
    );

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
        Usdc(float!("400")),
        "Expected excess of $400 (actual $900 - target $500)"
    );

    assert_eq!(
        usdc.alpaca_to_base_calls(),
        0,
        "alpaca_to_base should not have been called"
    );
}

/// Verifies that setting `usdc: None` in RebalancingTriggerConfig
/// disables USDC rebalancing entirely: even with a severe imbalance,
/// `check_and_trigger_usdc` dispatches no operations.
#[tokio::test]
async fn usdc_none_disables_usdc_rebalancing() {
    let pool = setup_test_db().await;

    let inventory = Arc::new(RwLock::new(InventoryView::default()));

    build_imbalanced_inventory(Imbalance::Usdc {
        inventory: &inventory,
        onchain: Usdc(float!("100")),
        offchain: Usdc(float!("900")),
    })
    .await;

    let (sender, mut receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingTrigger::new(
        RebalancingTriggerConfig {
            usdc: None,
            ..test_trigger_config()
        },
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
    );

    trigger.check_and_trigger_usdc().await;

    drop(trigger);

    assert!(
        receiver.try_recv().is_err(),
        "No USDC operation should be dispatched when usdc threshold is None"
    );
}

/// Tests that when the Alpaca mint API returns an HTTP error, the
/// `TokenizedEquityMint` aggregate returns an error without emitting any
/// events. The rebalancer swallows the error, so no mint events appear
/// in the event store.
#[tokio::test]
async fn mint_api_failure_produces_rejected_event() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id,
        trigger,
        inventory: _,
        position_cqrs,
        receiver,
    } = setup_equity_trigger().await;

    // Build inventory: 20 onchain, 80 offchain = 20% ratio -> TooMuchOffchain
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!("20"),
        offchain: float!("80"),
    })
    .await;

    let token = Address::from_slice(&keccak256(symbol.to_string().as_bytes())[..20]);
    seed_vault_registry(&pool, &symbol, token).await;

    let server = MockServer::start();
    let (_anvil, endpoint, key) = setup_anvil();
    let tokenizer: Arc<dyn Tokenizer> = Arc::new(
        create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
    );
    let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
    let equity_transfer = build_equity_transfer_with_wrapper(
        &pool,
        raindex,
        tokenizer,
        MockWrapper::new(),
        Address::ZERO,
    );

    let mint_mock = server.mock(|when, then| {
        when.method(POST).path(tokenization_mint_path());
        then.status(500).body("Internal Server Error");
    });

    let mock_equity = Arc::new(MockCrossVenueEquityTransfer::new());
    let mock_usdc = Arc::new(MockUsdcRebalance::new());

    let rebalancer = Rebalancer::new(
        equity_transfer as _,
        mock_equity as _,
        Arc::clone(&mock_usdc) as _,
        mock_usdc as _,
        receiver,
    );

    // One more sell triggers the CQRS -> trigger -> Mint flow
    position_cqrs
        .send(
            &symbol,
            PositionCommand::AcknowledgeOnChainFill {
                symbol: symbol.clone(),
                threshold: ExecutionThreshold::whole_share(),
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 2,
                },
                amount: FractionalShares::new(float!("1")),
                direction: Direction::Sell,
                price_usdc: float!("150.0"),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();

    drop(trigger);
    drop(position_cqrs);

    rebalancer.run().await;

    mint_mock.assert();

    let vault_agg_id = VaultRegistryId {
        orderbook: TEST_ORDERBOOK,
        owner: TEST_ORDER_OWNER,
    }
    .to_string();

    // When the mint API returns HTTP 500, the TokenizedEquityMint aggregate
    // returns Err(RequestFailed) without emitting any events. The rebalancer
    // swallows the error (logs it), so no TokenizedEquityMint events appear.
    assert_events(
        &pool,
        &[
            ExpectedEvent::new("Position", &aggregate_id, "PositionEvent::Initialized"),
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
        ],
    )
    .await;

    let events = fetch_events(&pool).await;
    assert!(
        !events
            .iter()
            .any(|event| event.aggregate_type == "TokenizedEquityMint"),
        "No TokenizedEquityMint events should be emitted when the API fails"
    );
}

/// Tests that operational limits cap USDC rebalancing amounts, requiring
/// multiple trigger cycles to resolve a large imbalance. With a $100 cap and
/// $400 excess, the first trigger produces a $100 transfer. After updating
/// inventory to reflect the transfer, the second trigger fires again with the
/// remaining imbalance, and so on until the inventory is balanced.
#[tokio::test]
async fn usdc_operational_limits_cap_across_trigger_cycles() {
    let pool = setup_test_db().await;

    // 50 onchain, 950 offchain = 5% ratio -> TooMuchOffchain
    // Excess to reach 50% target = 500 - 50 = 450 USDC
    let inventory = Arc::new(RwLock::new(
        InventoryView::default().with_usdc(Usdc(float!("50")), Usdc(float!("950"))),
    ));

    let assets = AssetsConfig {
        equities: EquitiesConfig::default(),
        cash: Some(CashAssetConfig {
            vault_id: None,
            rebalancing: OperationMode::Enabled,
            operational_limit: Some(Positive::new(Usdc(float!("100"))).unwrap()),
        }),
    };

    let config = RebalancingTriggerConfig {
        equity: ImbalanceThreshold {
            target: float!("0.5"),
            deviation: float!("0.2"),
        },
        usdc: Some(ImbalanceThreshold {
            target: float!("0.5"),
            deviation: float!("0.2"),
        }),
        assets,
        disabled_assets: HashSet::new(),
    };

    let (sender, mut receiver) = mpsc::channel(10);
    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingTrigger::new(
        config,
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
    );

    // Cycle 1: excess = 450, capped to 100
    trigger.check_and_trigger_usdc().await;
    let op1 = receiver.try_recv().expect("First trigger should fire");
    match op1 {
        TriggeredOperation::UsdcAlpacaToBase { amount } => {
            assert_eq!(amount, Usdc(float!("100")), "First transfer capped to $100");
        }
        _ => panic!("Expected UsdcAlpacaToBase, got {op1:?}"),
    }
    trigger.clear_usdc_in_progress();

    // Simulate first transfer: 150 onchain, 850 offchain = 15% ratio
    // Still below 30% lower bound, excess = 500 - 150 = 350
    {
        let mut guard = inventory.write().await;
        let taken = std::mem::take(&mut *guard);
        *guard = taken.with_usdc(Usdc(float!("150")), Usdc(float!("850")));
    }

    // Cycle 2: excess = 350, capped to 100
    trigger.check_and_trigger_usdc().await;
    let op2 = receiver.try_recv().expect("Second trigger should fire");
    match op2 {
        TriggeredOperation::UsdcAlpacaToBase { amount } => {
            assert_eq!(
                amount,
                Usdc(float!("100")),
                "Second transfer capped to $100"
            );
        }
        _ => panic!("Expected UsdcAlpacaToBase, got {op2:?}"),
    }
    trigger.clear_usdc_in_progress();

    // Simulate second transfer: 250 onchain, 750 offchain = 25% ratio
    // Still below 30% lower bound, excess = 500 - 250 = 250
    {
        let mut guard = inventory.write().await;
        let taken = std::mem::take(&mut *guard);
        *guard = taken.with_usdc(Usdc(float!("250")), Usdc(float!("750")));
    }

    // Cycle 3: excess = 250, capped to 100
    trigger.check_and_trigger_usdc().await;
    let op3 = receiver.try_recv().expect("Third trigger should fire");
    match op3 {
        TriggeredOperation::UsdcAlpacaToBase { amount } => {
            assert_eq!(amount, Usdc(float!("100")), "Third transfer capped to $100");
        }
        _ => panic!("Expected UsdcAlpacaToBase, got {op3:?}"),
    }
    trigger.clear_usdc_in_progress();

    // Simulate third transfer: 350 onchain, 650 offchain = 35% ratio
    // Now within [30%, 70%] band -> balanced, no more trigger
    {
        let mut guard = inventory.write().await;
        let taken = std::mem::take(&mut *guard);
        *guard = taken.with_usdc(Usdc(float!("350")), Usdc(float!("650")));
    }

    trigger.check_and_trigger_usdc().await;
    assert!(
        matches!(
            receiver.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty
        ),
        "Balanced inventory should not trigger"
    );
}

/// Tests that the USDC in-progress guard blocks concurrent triggers. When a
/// USDC operation is in progress, subsequent trigger attempts are silently
/// skipped. After the guard is released (operation completes or fails), the
/// trigger fires again.
#[tokio::test]
async fn usdc_in_progress_blocks_concurrent_triggers() {
    let pool = setup_test_db().await;

    // Large imbalance: 100 onchain, 900 offchain
    let inventory = Arc::new(RwLock::new(
        InventoryView::default().with_usdc(Usdc(float!("100")), Usdc(float!("900"))),
    ));

    let assets = AssetsConfig {
        equities: EquitiesConfig::default(),
        cash: Some(CashAssetConfig {
            vault_id: None,
            rebalancing: OperationMode::Enabled,
            operational_limit: Some(Positive::new(Usdc(float!("100"))).unwrap()),
        }),
    };
    let config = RebalancingTriggerConfig {
        equity: ImbalanceThreshold {
            target: float!("0.5"),
            deviation: float!("0.2"),
        },
        usdc: Some(ImbalanceThreshold {
            target: float!("0.5"),
            deviation: float!("0.2"),
        }),
        assets,
        disabled_assets: HashSet::new(),
    };

    let (sender, mut receiver) = mpsc::channel(10);
    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingTrigger::new(
        config,
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
    );

    // First trigger fires: excess = 400, capped to 100
    trigger.check_and_trigger_usdc().await;
    let op1 = receiver
        .try_recv()
        .expect("First trigger should produce an operation");
    match op1 {
        TriggeredOperation::UsdcAlpacaToBase { amount } => {
            assert_eq!(amount, Usdc(float!("100")), "First transfer capped to $100");
        }
        _ => panic!("Expected UsdcAlpacaToBase, got {op1:?}"),
    }

    // Without clearing in_progress, second trigger is blocked
    trigger.check_and_trigger_usdc().await;
    assert!(
        matches!(
            receiver.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty
        ),
        "In-progress guard should block second trigger"
    );

    // Clear in-progress (simulates operation completion/failure)
    trigger.clear_usdc_in_progress();

    // Trigger fires again: same inventory, same excess = 400, capped to 100
    trigger.check_and_trigger_usdc().await;
    let op2 = receiver
        .try_recv()
        .expect("After clearing in_progress, trigger should fire again");
    match op2 {
        TriggeredOperation::UsdcAlpacaToBase { amount } => {
            assert_eq!(
                amount,
                Usdc(float!("100")),
                "Retry transfer also capped to $100"
            );
        }
        _ => panic!("Expected UsdcAlpacaToBase, got {op2:?}"),
    }
}

/// Tests that threshold configuration controls trigger sensitivity: the same
/// USDC inventory (35% onchain) is within bounds for a wide threshold but
/// outside bounds for a tight threshold, causing only the tight config to
/// dispatch a rebalancing operation.
#[tokio::test]
async fn threshold_config_controls_trigger_sensitivity() {
    let pool = setup_test_db().await;

    // Inventory: 350 onchain / 650 offchain = 35% onchain ratio.
    // Wide config (deviation=0.4, bounds: 10%-90%): 35% is within bounds -> no trigger.
    // Tight config (deviation=0.1, bounds: 40%-60%): 35% is below 40% -> triggers.

    // Scenario 1: Wide threshold - no trigger
    {
        let inventory = Arc::new(RwLock::new(InventoryView::default()));

        build_imbalanced_inventory(Imbalance::Usdc {
            inventory: &inventory,
            onchain: Usdc(float!("350")),
            offchain: Usdc(float!("650")),
        })
        .await;

        let (sender, mut receiver) = mpsc::channel(10);
        let wide_config = RebalancingTriggerConfig {
            equity: ImbalanceThreshold {
                target: float!("0.5"),
                deviation: float!("0.4"),
            },
            usdc: Some(ImbalanceThreshold {
                target: float!("0.5"),
                deviation: float!("0.4"),
            }),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: Some(CashAssetConfig {
                    vault_id: None,
                    rebalancing: OperationMode::Enabled,
                    operational_limit: None,
                }),
            },
            disabled_assets: HashSet::new(),
        };
        let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
        let wrapper = Arc::new(MockWrapper::new());
        let trigger = RebalancingTrigger::new(
            wide_config,
            vault_registry,
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            Arc::clone(&inventory),
            sender,
            wrapper,
        );

        trigger.check_and_trigger_usdc().await;
        drop(trigger);

        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Disconnected
            ),
            "Wide threshold (10%-90%) should not trigger at 35% onchain ratio"
        );
    }

    // Scenario 2: Tight threshold - triggers
    {
        let inventory = Arc::new(RwLock::new(InventoryView::default()));

        build_imbalanced_inventory(Imbalance::Usdc {
            inventory: &inventory,
            onchain: Usdc(float!("350")),
            offchain: Usdc(float!("650")),
        })
        .await;

        let (sender, mut receiver) = mpsc::channel(10);
        let tight_config = RebalancingTriggerConfig {
            equity: ImbalanceThreshold {
                target: float!("0.5"),
                deviation: float!("0.1"),
            },
            usdc: Some(ImbalanceThreshold {
                target: float!("0.5"),
                deviation: float!("0.1"),
            }),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: Some(CashAssetConfig {
                    vault_id: None,
                    rebalancing: OperationMode::Enabled,
                    operational_limit: None,
                }),
            },
            disabled_assets: HashSet::new(),
        };
        let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
        let wrapper = Arc::new(MockWrapper::new());
        let trigger = RebalancingTrigger::new(
            tight_config,
            vault_registry,
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            Arc::clone(&inventory),
            sender,
            wrapper,
        );

        trigger.check_and_trigger_usdc().await;
        drop(trigger);

        let operation = receiver
            .try_recv()
            .expect("Tight threshold (40%-60%) should trigger at 35% onchain ratio");

        // Excess = target_onchain - actual_onchain = 500 - 350 = $150
        match operation {
            TriggeredOperation::UsdcAlpacaToBase { amount } => {
                assert_eq!(
                    amount,
                    Usdc(float!("150")),
                    "Excess should be $150 (target $500 - actual $350)"
                );
            }
            _ => panic!("Expected UsdcAlpacaToBase operation"),
        }
    }
}

/// Verifies that a dispatched mint operation sets offchain inflight balance
/// in the InventoryView. The trigger dispatches a Mint, and MintAccepted flows
/// through `on_mint` which calls `Inventory::transfer(Hedging, Start, qty)`.
/// This moves shares from offchain available to offchain inflight, recording
/// the pending tokenization as in-flight equity.
#[tokio::test]
async fn mint_accepted_sets_offchain_inflight() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id: _,
        trigger,
        inventory,
        position_cqrs,
        mut receiver,
    } = setup_equity_trigger().await;

    // Build inventory: 20 onchain, 80 offchain = 20% ratio -> TooMuchOffchain
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!("20"),
        offchain: float!("80"),
    })
    .await;

    // Confirm initial state: offchain has 80 available, 0 inflight
    let inv = inventory.read().await;
    let initial_offchain_available = inv.equity_available(&symbol, Venue::Hedging).unwrap();
    let initial_offchain_inflight = inv.equity_inflight(&symbol, Venue::Hedging).unwrap();
    drop(inv);

    assert!(
        initial_offchain_available.inner().eq(float!("80")).unwrap(),
        "Initial offchain available should be 80"
    );
    assert!(
        initial_offchain_inflight.inner().is_zero().unwrap(),
        "Initial offchain inflight should be 0"
    );

    let server = MockServer::start();
    let (_anvil, endpoint, key) = setup_anvil();

    let signer = PrivateKeySigner::from_bytes(&key).unwrap();
    let wallet = EthereumWallet::from(signer.clone());
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(&endpoint)
        .await
        .unwrap();
    let token_contract = TestERC20::deploy(&provider).await.unwrap();
    let token_address = *token_contract.address();

    seed_vault_registry(&pool, &symbol, token_address).await;

    let tokenizer: Arc<dyn Tokenizer> = Arc::new(
        create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
    );
    let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
    let mock_wrapper = MockWrapper::new().with_unwrapped_token(token_address);

    // Wire mint/redemption stores with trigger so lifecycle events update
    // inflight state through the trigger's Reactor
    let equity_transfer = build_equity_transfer_with_trigger(
        &pool,
        raindex,
        tokenizer,
        mock_wrapper,
        signer.address(),
        &trigger,
    )
    .await;

    // Mock the mint API to accept but never complete (stays pending forever)
    server.mock(|when, then| {
        when.method(POST).path(tokenization_mint_path());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(sample_pending_response("inflight_test"));
    });

    // Mock the poll endpoint to keep returning pending
    server.mock(|when, then| {
        when.method(GET).path(tokenization_requests_path());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([sample_pending_response("inflight_test")]));
    });

    // Trigger the imbalance via a position command to dispatch the Mint
    position_cqrs
        .send(
            &symbol,
            PositionCommand::AcknowledgeOnChainFill {
                symbol: symbol.clone(),
                threshold: ExecutionThreshold::whole_share(),
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 2,
                },
                amount: FractionalShares::new(float!("1")),
                direction: Direction::Sell,
                price_usdc: float!("150.0"),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();

    // Receive the dispatched Mint operation and get the quantity
    let operation = receiver
        .try_recv()
        .expect("Trigger should dispatch a Mint operation for the imbalance");
    let TriggeredOperation::Mint {
        symbol: mint_symbol,
        quantity: mint_quantity,
    } = operation
    else {
        panic!("Expected Mint operation, got {operation:?}");
    };
    assert_eq!(mint_symbol, symbol);

    // Execute the mint transfer. This calls the Alpaca API, gets MintAccepted,
    // which triggers on_mint -> Inventory::transfer(Hedging, Start, qty).
    // The poll will return pending, so the mint aggregate will time out or loop,
    // but MintAccepted has already fired by then. We spawn and cancel to
    // get just the MintAccepted event through.
    let transfer_handle = tokio::spawn({
        let equity_transfer = Arc::clone(&equity_transfer);
        let mint_symbol = mint_symbol.clone();
        async move {
            let _ = CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
                equity_transfer.as_ref(),
                Equity {
                    symbol: mint_symbol,
                    quantity: mint_quantity,
                },
            )
            .await;
        }
    });

    // Wait for MintAccepted to propagate: poll until inflight becomes non-zero
    // rather than relying on a fixed sleep duration that can race on busy CI.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        let inv = inventory.read().await;
        let inflight = inv.equity_inflight(&symbol, Venue::Hedging);
        drop(inv);

        if let Some(inflight) = inflight
            && inflight.inner().gt(float!("0")).unwrap()
        {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for MintAccepted to set inflight balance"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    transfer_handle.abort();

    // After MintAccepted, offchain inflight should have increased by the
    // mint quantity and available decreased by the same amount
    let inv = inventory.read().await;
    let offchain_inflight = inv.equity_inflight(&symbol, Venue::Hedging).unwrap();
    let offchain_available = inv.equity_available(&symbol, Venue::Hedging).unwrap();
    drop(inv);

    assert!(
        offchain_inflight.inner().gt(float!("0")).unwrap(),
        "Offchain inflight should be non-zero after MintAccepted, got {offchain_inflight:?}"
    );

    // The mint quantity was dispatched from available to inflight
    let mint_qty_float = mint_quantity.inner();
    assert!(
        offchain_inflight.inner().eq(mint_qty_float).unwrap(),
        "Offchain inflight should equal mint quantity {mint_quantity:?}, \
         got {offchain_inflight:?}"
    );

    // available should have decreased: original 80 - mint_quantity
    let expected_available = (float!("80") - mint_qty_float).unwrap();
    assert!(
        offchain_available.inner().eq(expected_available).unwrap(),
        "Offchain available should be 80 - {mint_quantity:?} = {expected_available:?}, \
         got {offchain_available:?}"
    );
}

/// Verifies that a completed mint clears the offchain inflight balance and
/// increases the onchain available balance. With mint stores wired to the
/// trigger (mirroring Conductor's `QueryManifest::build()`), the full mint
/// lifecycle flows through `on_mint`. `MintAccepted` sets offchain inflight,
/// and `TokensReceived` clears offchain inflight while adding to onchain
/// available -- proving the full transfer lifecycle updates InventoryView
/// correctly.
#[tokio::test]
async fn completed_mint_clears_inflight_and_updates_inventory() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id: _,
        trigger,
        inventory,
        position_cqrs,
        mut receiver,
    } = setup_equity_trigger().await;

    // Build inventory: 20 onchain, 80 offchain = 20% ratio -> TooMuchOffchain
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!("20"),
        offchain: float!("80"),
    })
    .await;

    let server = MockServer::start();
    let (_anvil, endpoint, key) = setup_anvil();

    let signer = PrivateKeySigner::from_bytes(&key).unwrap();
    let wallet = EthereumWallet::from(signer.clone());
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(&endpoint)
        .await
        .unwrap();

    let token_contract = TestERC20::deploy(&provider).await.unwrap();
    let token_address = *token_contract.address();

    // Mint tokens so balanceOf verification passes during verify_mint_tx
    let mint_amount = U256::from(30_500_000_000_000_000_000_u128);
    let mint_receipt = token_contract
        .mint(signer.address(), mint_amount)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    let mint_tx_hash = mint_receipt.transaction_hash;

    seed_vault_registry(&pool, &symbol, token_address).await;

    let tokenizer: Arc<dyn Tokenizer> = Arc::new(
        create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
    );
    let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
    let mock_wrapper = MockWrapper::new().with_unwrapped_token(token_address);

    let equity_transfer = build_equity_transfer_with_trigger(
        &pool,
        raindex,
        tokenizer,
        mock_wrapper,
        signer.address(),
        &trigger,
    )
    .await;

    let wallet_hex = format!("{:#x}", signer.address());

    let mint_mock = server.mock(|when, then| {
        when.method(POST)
            .path(tokenization_mint_path())
            .json_body_includes(
                json!({
                    "underlying_symbol": "AAPL",
                    "qty": "30.5",
                    "wallet_address": wallet_hex,
                })
                .to_string(),
            );
        then.status(200)
            .header("content-type", "application/json")
            .json_body(sample_pending_response("completed_mint_test"));
    });

    let poll_mock = server.mock(|when, then| {
        when.method(GET).path(tokenization_requests_path());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([sample_completed_response(
                "completed_mint_test",
                mint_tx_hash
            )]));
    });

    // Trigger the mint via a position command
    position_cqrs
        .send(
            &symbol,
            PositionCommand::AcknowledgeOnChainFill {
                symbol: symbol.clone(),
                threshold: ExecutionThreshold::whole_share(),
                trade_id: TradeId {
                    tx_hash: TxHash::random(),
                    log_index: 2,
                },
                amount: FractionalShares::new(float!("1")),
                direction: Direction::Sell,
                price_usdc: float!("150.0"),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();

    // Receive the dispatched Mint operation
    let operation = receiver
        .try_recv()
        .expect("Trigger should dispatch a Mint operation");
    let TriggeredOperation::Mint {
        symbol: mint_symbol,
        quantity: mint_quantity,
    } = operation
    else {
        panic!("Expected Mint operation, got {operation:?}");
    };

    // Record initial onchain available before the mint executes
    let initial_onchain_available = {
        let inv = inventory.read().await;
        inv.equity_available(&symbol, Venue::MarketMaking).unwrap()
    };

    // Execute the full mint lifecycle through CrossVenueTransfer
    CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
        equity_transfer.as_ref(),
        Equity {
            symbol: mint_symbol,
            quantity: mint_quantity,
        },
    )
    .await
    .unwrap();

    mint_mock.assert();
    poll_mock.assert();

    // Drop the equity transfer to release Arc refs held by wired stores
    drop(equity_transfer);

    // After TokensReceived, offchain inflight should be cleared (back to 0)
    let inv = inventory.read().await;
    let offchain_inflight = inv.equity_inflight(&symbol, Venue::Hedging).unwrap();
    let onchain_available = inv.equity_available(&symbol, Venue::MarketMaking).unwrap();
    drop(inv);

    assert!(
        offchain_inflight.inner().is_zero().unwrap(),
        "Offchain inflight should be 0 after mint completes, got {offchain_inflight:?}"
    );

    // Onchain available should have increased by the mint quantity
    let mint_qty_float = mint_quantity.inner();
    let expected_onchain = (initial_onchain_available.inner() + mint_qty_float).unwrap();
    assert!(
        onchain_available.inner().eq(expected_onchain).unwrap(),
        "Onchain available should have increased by {mint_quantity:?}: \
         expected {expected_onchain:?}, got {onchain_available:?}"
    );
}

/// TransferFailed during redemption cancels inflight and restores available.
///
/// When tokens are withdrawn from Raindex (WithdrawnFromRaindex), inflight
/// is set at MarketMaking. If the subsequent send to Alpaca's redemption
/// wallet fails (TransferFailed), inflight must be cancelled back to
/// available — tokens never left our wallet.
#[tokio::test]
async fn transfer_failed_cancels_redemption_inflight() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id: _,
        trigger,
        inventory,
        position_cqrs,
        receiver: _,
    } = setup_equity_trigger().await;

    // Build inventory: 80 onchain, 20 offchain = 80% ratio -> TooMuchOnchain
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!("80"),
        offchain: float!("20"),
    })
    .await;

    let token_address = Address::random();
    seed_vault_registry(&pool, &symbol, token_address).await;

    // Build a redemption store wired to the trigger so events flow through
    let tokenizer: Arc<dyn Tokenizer> = Arc::new(MockTokenizer::new().with_send_failure());

    let equity_services = EquityTransferServices {
        raindex: Arc::new(MockRaindex::new()),
        tokenizer,
        wrapper: Arc::new(MockWrapper::new()),
    };

    let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
        .with(Arc::clone(&trigger))
        .build(equity_services)
        .await
        .unwrap();

    let redemption_id = RedemptionAggregateId::new("redemption-transfer-failed");

    // Redeem: withdraws tokens from Raindex vault -> WithdrawnFromRaindex
    redemption_store
        .send(
            &redemption_id,
            EquityRedemptionCommand::Redeem {
                symbol: symbol.clone(),
                quantity: float!("10"),
                token: token_address,
                amount: U256::from(10_000_000_000_000_000_000_u128),
            },
        )
        .await
        .unwrap();

    // After WithdrawnFromRaindex, inflight should be set at MarketMaking
    let inflight_after_withdraw = inventory
        .read()
        .await
        .equity_inflight(&symbol, Venue::MarketMaking)
        .unwrap();
    assert!(
        !inflight_after_withdraw.inner().is_zero().unwrap(),
        "Inflight should be non-zero after WithdrawnFromRaindex, got {inflight_after_withdraw:?}"
    );

    // UnwrapTokens -> TokensUnwrapped (no inventory change)
    redemption_store
        .send(&redemption_id, EquityRedemptionCommand::UnwrapTokens)
        .await
        .unwrap();

    // SendTokens: mock tokenizer fails -> TransferFailed event
    // The aggregate emits TransferFailed, trigger cancels inflight
    redemption_store
        .send(&redemption_id, EquityRedemptionCommand::SendTokens)
        .await
        .unwrap();

    // After TransferFailed, inflight should be cleared
    let inv = inventory.read().await;
    let inflight_after_fail = inv.equity_inflight(&symbol, Venue::MarketMaking).unwrap();
    let available_after_fail = inv.equity_available(&symbol, Venue::MarketMaking).unwrap();
    drop(inv);

    assert!(
        inflight_after_fail.inner().is_zero().unwrap(),
        "Inflight should be zero after TransferFailed, got {inflight_after_fail:?}"
    );

    // Available should have the tokens back (80 original)
    assert!(
        available_after_fail.inner().eq(float!("80")).unwrap(),
        "Available should be restored to 80 after TransferFailed, got {available_after_fail:?}"
    );
}
