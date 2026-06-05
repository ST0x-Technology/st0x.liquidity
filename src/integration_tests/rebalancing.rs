//! Integration tests for the inventory rebalancing pipeline: position changes
//! flow through the RebalancingService (wired as a CQRS query processor),
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
use std::time::Duration;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{broadcast, mpsc};

use rain_math_float::Float;
use st0x_config::{
    AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, ExecutionThreshold,
    OperationMode,
};
use st0x_dto::Statement;
use st0x_event_sorcery::{Store, StoreBuilder, test_store};
use st0x_execution::{
    Direction, ExecutorOrderId, FractionalShares, Positive, SupportedExecutor, Symbol,
};
use st0x_finance::{Usd, Usdc};
use st0x_float_macro::float;
use st0x_raindex::{Raindex, RaindexVaultId};
use st0x_wrapper::MockWrapper;

use super::{ExpectedEvent, assert_events, fetch_events};
use crate::bindings::{IERC20, TestERC20};
use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId};
use crate::inventory::{BroadcastingInventory, ImbalanceThreshold, InventoryView, Venue};
use crate::offchain::order::OffchainOrderId;
use crate::onchain::mock::MockRaindex;
use crate::position::{Position, PositionCommand, TradeId};
use crate::rebalancing::equity::mock::MockCrossVenueEquityTransfer;
use crate::rebalancing::equity::{CrossVenueEquityTransfer, Equity, EquityTransferServices};
use crate::rebalancing::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use crate::rebalancing::usdc::{TransferUsdcToHedging, TransferUsdcToMarketMaking};
use crate::rebalancing::{
    Rebalancer, RebalancingSchedulers, RebalancingService, RebalancingServiceConfig,
    TriggeredOperation, drain_pending_jobs,
};
use crate::test_utils::setup_test_db;
use crate::tokenization::Tokenizer;
use crate::tokenization::alpaca::tests::{
    TEST_REDEMPTION_WALLET, create_test_service_from_mock, setup_anvil, tokenization_mint_path,
    tokenization_requests_path,
};
use crate::tokenization::mock::MockTokenizer;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::vault_lookup::{MockVaultLookup, VaultLookup};
use crate::vault_registry::{VaultRegistry, VaultRegistryCommand, VaultRegistryId};

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
        VaultRegistryCommand::SeedEquityVaultFromConfig {
            token,
            vault_id,
            symbol: symbol.clone(),
        },
    )
    .await
    .unwrap();
}

fn mock_vault_lookup_for_symbol(symbol: &Symbol, token: Address) -> Arc<dyn VaultLookup> {
    Arc::new(
        MockVaultLookup::new()
            .with_symbol_token(symbol.clone(), token)
            .with_vault(
                token,
                RaindexVaultId(B256::from(keccak256(symbol.to_string().as_bytes()))),
            ),
    )
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

fn test_trigger_config() -> RebalancingServiceConfig {
    RebalancingServiceConfig {
        equity: ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        },
        usdc: Some(ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        }),
        transfer_timeout: Duration::from_secs(30 * 60),
        assets: AssetsConfig {
            equities: EquitiesConfig {
                operational_limit: None,
                symbols: HashMap::from([(
                    Symbol::new("AAPL").unwrap(),
                    EquityAssetConfig {
                        tokenized_equity: Address::ZERO,
                        tokenized_equity_derivative: Address::ZERO,
                        vault_ids: Vec::new(),
                        trading: OperationMode::Disabled,
                        rebalancing: OperationMode::Enabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        operational_limit: None,
                    },
                )]),
            },
            cash: Some(CashAssetConfig {
                vault_ids: Vec::new(),
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
                reserved: None,
            }),
        },
        disabled_assets: HashSet::new(),
    }
}

/// Mirrors the private `build_position_cqrs()` from `src/conductor/mod.rs`,
/// wiring the `RebalancingService` as a Position CQRS query processor so
/// that position events flow through it into inventory bookkeeping +
/// follow-up check enqueueing.
async fn build_position_cqrs_with_service(
    pool: &SqlitePool,
    service: &Arc<RebalancingService>,
) -> Arc<Store<Position>> {
    let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
        .with(Arc::clone(service))
        .build(())
        .await
        .unwrap();

    store
}

/// Shared state for equity rebalancing tests (mint and redemption) that
/// wires up the Position CQRS with a `RebalancingService` as a query
/// processor.
struct EquityTriggerFixture {
    pool: SqlitePool,
    symbol: Symbol,
    aggregate_id: String,
    service: Arc<RebalancingService>,
    inventory: Arc<BroadcastingInventory>,
    position_cqrs: Arc<Store<Position>>,
    receiver: mpsc::Receiver<TriggeredOperation>,
}

async fn setup_equity_trigger() -> EquityTriggerFixture {
    let pool = setup_test_db().await;
    let symbol = Symbol::new("AAPL").unwrap();
    let aggregate_id = symbol.to_string();

    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default()
            .with_equity(
                symbol.clone(),
                FractionalShares::ZERO,
                FractionalShares::ZERO,
            )
            .with_usdc(Usdc::new(float!(1000000)), Usdc::new(float!(1000000))),
        event_sender,
    ));
    let (sender, receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));

    let wrapper = Arc::new(MockWrapper::new());

    let service = Arc::new(RebalancingService::new(
        test_trigger_config(),
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    ));

    let position_cqrs = build_position_cqrs_with_service(&pool, &service).await;

    EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id,
        service,
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
        inventory: &'a Arc<BroadcastingInventory>,
        onchain: Usdc,
        offchain: Usdc,
    },
}

/// Direction of a USDC transfer enqueued by the trigger, recovered from the
/// apalis Jobs table for assertion in integration tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnqueuedUsdcOperation {
    AlpacaToBase { amount: Usdc },
    BaseToAlpaca { amount: Usdc },
}

/// Drains every pending USDC transfer row (both directions) from the apalis
/// Jobs table, marks them Done, and returns them in `run_at` order.
async fn drain_pending_usdc_transfer_jobs(pool: &SqlitePool) -> Vec<EnqueuedUsdcOperation> {
    let to_hedging_type = std::any::type_name::<TransferUsdcToHedging>();
    let to_market_making_type = std::any::type_name::<TransferUsdcToMarketMaking>();

    let rows: Vec<(String, Vec<u8>, String)> = sqlx::query_as(
        "SELECT id, job, job_type FROM Jobs \
         WHERE status = 'Pending' AND (job_type = ? OR job_type = ?) \
         ORDER BY run_at",
    )
    .bind(to_hedging_type)
    .bind(to_market_making_type)
    .fetch_all(pool)
    .await
    .expect("query pending USDC transfer jobs");

    let mut operations = Vec::with_capacity(rows.len());
    for (row_id, payload, job_type) in rows {
        let operation = if job_type == to_hedging_type {
            let job: TransferUsdcToHedging =
                serde_json::from_slice(&payload).expect("deserialize TransferUsdcToHedging");
            EnqueuedUsdcOperation::BaseToAlpaca { amount: job.amount }
        } else {
            let job: TransferUsdcToMarketMaking =
                serde_json::from_slice(&payload).expect("deserialize TransferUsdcToMarketMaking");
            EnqueuedUsdcOperation::AlpacaToBase { amount: job.amount }
        };

        sqlx::query("UPDATE Jobs SET status = 'Done' WHERE id = ?")
            .bind(&row_id)
            .execute(pool)
            .await
            .expect("mark drained USDC transfer job Done");

        operations.push(operation);
    }

    operations
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
                        price_usdc: float!(150.0),
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
                        executor: SupportedExecutor::AlpacaBrokerApi,
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
                        price: Usd::new(float!(150)),
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
            let withdrawable_cash_cents = Usd::new(offchain.inner())
                .to_cents()
                .expect("test USDC balances should be cent-denominated");
            let mut guard = inventory.write().await;
            let taken = std::mem::take(&mut *guard);
            *guard = taken
                .with_usdc(onchain, offchain)
                .with_withdrawable_cash_cents(withdrawable_cash_cents);
        }
    }
}

fn build_equity_transfer_with_wrapper(
    pool: &SqlitePool,
    raindex: Arc<dyn Raindex>,
    vault_lookup: Arc<dyn VaultLookup>,
    tokenizer: Arc<dyn Tokenizer>,
    mock_wrapper: MockWrapper,
    wallet: Address,
) -> Arc<CrossVenueEquityTransfer> {
    let wrapper: Arc<dyn st0x_wrapper::Wrapper> = Arc::new(mock_wrapper);

    let equity_services = EquityTransferServices {
        raindex: Arc::clone(&raindex),
        vault_lookup: Arc::clone(&vault_lookup),
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
        vault_lookup,
        tokenizer,
        wrapper,
        wallet,
        mint_store,
        redemption_store,
    ))
}

/// Builds `CrossVenueEquityTransfer` with mint and redemption stores wired to
/// the `RebalancingService` as a query processor. This mirrors the production
/// wiring in `Conductor` via `QueryManifest::build()`, ensuring mint/redemption
/// lifecycle events flow through the trigger and update inflight state.
async fn build_equity_transfer_with_service(
    pool: &SqlitePool,
    raindex: Arc<dyn Raindex>,
    vault_lookup: Arc<dyn VaultLookup>,
    tokenizer: Arc<dyn Tokenizer>,
    mock_wrapper: MockWrapper,
    wallet: Address,
    service: &Arc<RebalancingService>,
) -> Arc<CrossVenueEquityTransfer> {
    let wrapper: Arc<dyn st0x_wrapper::Wrapper> = Arc::new(mock_wrapper);

    let equity_services = EquityTransferServices {
        raindex: Arc::clone(&raindex),
        vault_lookup: Arc::clone(&vault_lookup),
        tokenizer: Arc::clone(&tokenizer),
        wrapper: Arc::clone(&wrapper),
    };

    let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
        .with(Arc::clone(service))
        .build(equity_services.clone())
        .await
        .unwrap();

    let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
        .with(Arc::clone(service))
        .build(equity_services)
        .await
        .unwrap();

    Arc::new(CrossVenueEquityTransfer::new(
        raindex,
        vault_lookup,
        tokenizer,
        wrapper,
        wallet,
        mint_store,
        redemption_store,
    ))
}

/// Verifies the full equity mint rebalancing pipeline: position CQRS commands
/// flow through the RebalancingService (registered as a Query processor),
/// update the InventoryView, detect an equity imbalance, and dispatch a Mint
/// operation through the Rebalancer to the CrossVenueEquityTransfer which
/// drives the TokenizedEquityMint aggregate to completion via the Alpaca
/// tokenization API.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn equity_offchain_imbalance_triggers_mint() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id,
        service,
        inventory: _,
        position_cqrs,
        receiver,
    } = setup_equity_trigger().await;

    // Build inventory: 20 onchain, 80 offchain = 20% ratio -> TooMuchOffchain.
    // Without VaultRegistry seeded, the trigger silently skips Mint operations.
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!(20),
        offchain: float!(80),
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
    let mock_wrapper = MockWrapper::new()
        .with_tokenized_shares(token_address)
        .with_wrapped_token(token_address);
    let equity_transfer = build_equity_transfer_with_wrapper(
        &pool,
        raindex,
        mock_vault_lookup_for_symbol(&symbol, token_address),
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

    let rebalancer = Rebalancer::new(
        equity_transfer as _,
        mock_equity as _,
        receiver,
        Arc::new(std::sync::RwLock::new(std::collections::HashSet::new())),
        tokio_util::sync::CancellationToken::new(),
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
                amount: FractionalShares::new(float!(1)),
                direction: Direction::Sell,
                price_usdc: float!(150.0),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();
    drain_pending_jobs(&service).await.unwrap();

    // Both trigger and position_cqrs hold Arc<RebalancingService> which owns the
    // mpsc sender. Both must be dropped to close the channel so rebalancer.run()
    // can exit after processing all queued operations.
    drop(service);
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
                "VaultRegistryEvent::EquityVaultSeededFromConfig",
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
                "TokenizedEquityMintEvent::WrapSubmitted",
            ),
            ExpectedEvent::new(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::TokensWrapped",
            ),
            ExpectedEvent::new(
                "TokenizedEquityMint",
                &mint_agg_id,
                "TokenizedEquityMintEvent::VaultDepositSubmitted",
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
/// commands flow through the RebalancingService, detect too much onchain equity,
/// and dispatch a Redemption operation through the Rebalancer to the real
/// CrossVenueEquityTransfer. The transfer sends tokens on Anvil, then drives the
/// EquityRedemption aggregate through TokensSent -> Detected -> Completed via
/// the mocked Alpaca tokenization API.
///
/// Uses Anvil snapshot/revert to discover the deterministic tx_hash before
/// setting up httpmock responses, so the mock detection endpoint can match
/// the exact hash produced by the real onchain transfer.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn equity_onchain_imbalance_triggers_redemption() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id,
        service,
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
    let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
    let wrapper = MockWrapper::new()
        .with_tokenized_shares(token_address)
        .with_wrapped_token(token_address);
    let equity_transfer = build_equity_transfer_with_wrapper(
        &pool,
        raindex,
        mock_vault_lookup_for_symbol(&symbol, token_address),
        tokenizer,
        wrapper,
        Address::ZERO,
    );

    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!(79),
        offchain: float!(20),
    })
    .await;
    seed_vault_registry(&pool, &symbol, token_address).await;

    let mock_equity = Arc::new(MockCrossVenueEquityTransfer::new());
    let rebalancer = Rebalancer::new(
        Arc::clone(&mock_equity) as _,
        equity_transfer as _,
        receiver,
        Arc::new(std::sync::RwLock::new(std::collections::HashSet::new())),
        tokio_util::sync::CancellationToken::new(),
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
                amount: FractionalShares::new(float!(1)),
                direction: Direction::Buy,
                price_usdc: float!(150.0),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();
    drain_pending_jobs(&service).await.unwrap();

    drop(service);
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
                "VaultRegistryEvent::EquityVaultSeededFromConfig",
            ),
            ExpectedEvent::new(
                "Position",
                &aggregate_id,
                "PositionEvent::OnChainOrderFilled",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::VaultWithdrawPending",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::VaultWithdrawSubmitted",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::WithdrawnFromRaindex",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::UnwrapPending",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::UnwrapSubmitted",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::TokensUnwrapped",
            ),
            ExpectedEvent::new(
                "EquityRedemption",
                &redemption_agg_id,
                "EquityRedemptionEvent::SendPending",
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
        events[6].payload["VaultWithdrawPending"]["symbol"]
            .as_str()
            .unwrap(),
        "AAPL",
        "VaultWithdrawPending should target the correct symbol"
    );
    assert_eq!(
        events[13].payload["TokensSent"]["redemption_tx"]
            .as_str()
            .unwrap(),
        format!("{expected_tx_hash:#x}"),
        "TokensSent redemption_tx should match the deterministic Anvil hash"
    );
    assert_eq!(
        events[14].payload["Detected"]["tokenization_request_id"]
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

/// Verifies USDC rebalancing dispatch: a USDC imbalance for the Alpaca->Base
/// direction enqueues a `TransferUsdcToMarketMaking` apalis job with the
/// expected amount. No work flows through the Rebalancer mpsc channel.
#[tokio::test]
async fn usdc_offchain_imbalance_triggers_alpaca_to_base() {
    let pool = setup_test_db().await;

    // 100 onchain, 900 offchain = 10% onchain ratio -> below 30% -> TooMuchOffchain
    // Excess = target_onchain - onchain = 500 - 100 = 400 USDC (above $51 minimum)
    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default(),
        event_sender,
    ));

    build_imbalanced_inventory(Imbalance::Usdc {
        inventory: &inventory,
        onchain: Usdc::new(float!(100)),
        offchain: Usdc::new(float!(900)),
    })
    .await;

    let (sender, _receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingService::new(
        test_trigger_config(),
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    );

    trigger.check_and_trigger_usdc().await;

    let job_type = std::any::type_name::<TransferUsdcToMarketMaking>();

    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM Jobs \
         WHERE status = 'Pending' AND job_type = ?",
    )
    .bind(job_type)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        pending, 1,
        "Expected exactly one pending TransferUsdcToMarketMaking job"
    );

    let payload: Vec<u8> = sqlx::query_scalar(
        "SELECT job FROM Jobs \
         WHERE status = 'Pending' AND job_type = ?",
    )
    .bind(job_type)
    .fetch_one(&pool)
    .await
    .unwrap();

    let job: TransferUsdcToMarketMaking =
        serde_json::from_slice(&payload).expect("deserialize TransferUsdcToMarketMaking");
    assert_eq!(
        job.amount,
        Usdc::new(float!(400)),
        "Expected excess of $400 (target $500 - actual $100)"
    );

    let opposite: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(
        opposite, 0,
        "Base->Alpaca queue should not have been touched"
    );
}

/// Verifies USDC onchain imbalance dispatch: 900 onchain / 100 offchain = 90%
/// onchain ratio (above 70% upper bound) triggers a BaseToAlpaca operation
/// with excess = 900 - 500 = $400.
#[tokio::test]
async fn usdc_onchain_imbalance_triggers_base_to_alpaca() {
    let pool = setup_test_db().await;

    // 900 onchain, 100 offchain = 90% onchain ratio -> above 70% -> TooMuchOnchain
    // Excess = onchain - target_onchain = 900 - 500 = 400 USDC
    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default(),
        event_sender,
    ));

    build_imbalanced_inventory(Imbalance::Usdc {
        inventory: &inventory,
        onchain: Usdc::new(float!(900)),
        offchain: Usdc::new(float!(100)),
    })
    .await;

    let (sender, _receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingService::new(
        test_trigger_config(),
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    );

    trigger.check_and_trigger_usdc().await;

    // Base->Alpaca is dispatched via the TransferUsdcToHedging apalis job
    // queue, not the Rebalancer mpsc channel. Assert exactly one pending row
    // with the expected payload.
    let job_type = std::any::type_name::<TransferUsdcToHedging>();

    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM Jobs \
         WHERE status = 'Pending' AND job_type = ?",
    )
    .bind(job_type)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        pending, 1,
        "Expected exactly one pending TransferUsdcToHedging job"
    );

    let payload: Vec<u8> = sqlx::query_scalar(
        "SELECT job FROM Jobs \
         WHERE status = 'Pending' AND job_type = ?",
    )
    .bind(job_type)
    .fetch_one(&pool)
    .await
    .unwrap();

    let job: TransferUsdcToHedging =
        serde_json::from_slice(&payload).expect("deserialize TransferUsdcToHedging payload");
    assert_eq!(
        job.amount,
        Usdc::new(float!(400)),
        "Expected excess of $400 (actual $900 - target $500)"
    );

    let opposite: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(
        opposite, 0,
        "Alpaca->Base queue should not have been touched"
    );
}

/// Verifies that the configured cash reserve does NOT shift the rebalancing
/// ratio. Polling subtracts the reserve from `OffchainUsd` for dashboard and
/// spending-cap purposes, but the imbalance check uses gross offchain cash
/// (`offchain_gross_usd_cents`) so the reserve cannot make the system look
/// artificially onchain-heavy and pull cash onchain unnecessarily.
///
/// The broker reports $500 gross, reserve is $300. Polling emits
/// `OffchainUsd` with available = $200 and `gross_usd_cents = Some(50000)`.
/// Rebalancing uses gross: 500 onchain / 500 gross offchain = 50% -> balanced
/// -> no operation triggered.
#[tokio::test]
async fn cash_reserve_does_not_shift_rebalancing_ratio() {
    use alloy::providers::{ProviderBuilder, mock::Asserter};

    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::ReadOnlyEvm;
    use st0x_execution::{Inventory as ExecutorInventory, MockExecutor};
    use st0x_finance::Usd;
    use st0x_raindex::RaindexService;

    use crate::inventory::InventoryPollingService;
    use crate::inventory::snapshot::{InventorySnapshotCommand, InventorySnapshotId};

    let pool = setup_test_db().await;

    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default(),
        event_sender,
    ));

    let (sender, receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    // Trigger config mirrors prod: reserved is configured here so the
    // imbalance check exercises the (reserved=Some, gross=Some) path
    // end-to-end, matching what the polling side will write.
    let mut trigger_config = test_trigger_config();
    if let Some(cash) = trigger_config.assets.cash.as_mut() {
        cash.reserved = Some(Positive::new(Usd::new(float!(300))).unwrap());
    }
    let service = Arc::new(RebalancingService::new(
        trigger_config,
        Arc::clone(&vault_registry),
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    ));

    // Build snapshot store with the service as the CQRS subscriber — mirrors
    // production wiring in QueryManifest::build. Snapshot events from polling
    // flow through the service's on_snapshot, updating the
    // BroadcastingInventory and scheduling follow-up imbalance checks.
    let snapshot_id = InventorySnapshotId {
        orderbook: TEST_ORDERBOOK,
        owner: TEST_ORDER_OWNER,
    };

    let snapshot_store =
        StoreBuilder::<crate::inventory::snapshot::InventorySnapshot>::new(pool.clone())
            .with(Arc::clone(&service))
            .build(())
            .await
            .unwrap();

    // Set onchain $500 directly via snapshot command (onchain polling is
    // skipped because vault registry is empty — no vaults registered).
    snapshot_store
        .send(
            &snapshot_id,
            InventorySnapshotCommand::OnchainUsdc {
                usdc_balance: Usdc::new(float!(500)),
            },
        )
        .await
        .unwrap();

    // Broker reports $500 gross, reserve = $300 -> available = $200.
    let executor = MockExecutor::new().with_inventory(ExecutorInventory {
        positions: vec![],
        usd_balance_cents: 50_000,
        cash_buying_power_cents: Some(50_000),
        alpaca_usdc: None,
        cash_withdrawable_cents: None,
    });

    let asserter = Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    let raindex_service = Arc::new(RaindexService::new(
        ReadOnlyEvm::new(provider),
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
    ));

    let reserved_cash = Usd::new(float!(300));

    let polling_service = InventoryPollingService::new(
        raindex_service,
        executor,
        vault_registry,
        snapshot_id,
        snapshot_store.clone(),
        None,
        None,
        reserved_cash,
    );

    // Poll offchain balances — compute_available_cash subtracts $300 reserve
    // from the $500 broker balance, emitting OffchainUsd with $200 available
    // and gross_usd_cents = Some(50000). The trigger reactor applies the
    // event to the BroadcastingInventory.
    polling_service.poll_and_record().await.unwrap();

    // Verify the reserve subtraction landed in the inventory view.
    let view = inventory.read().await;
    assert_eq!(
        view.usdc_available(Venue::Hedging),
        Some(Usdc::new(float!(200))),
        "Offchain available should be $500 gross - $300 reserved = $200"
    );
    drop(view);

    // Trigger checks thresholds. The reserve subtraction lives in
    // `usdc_available`; the imbalance check uses gross offchain cash so the
    // ratio stays balanced (500/500) despite the reserve.
    service.check_and_trigger_usdc().await;

    // Drop trigger holders so subsequent assertions read final queue state.
    drop(service);
    drop(snapshot_store);
    drop(polling_service);
    drop(receiver);

    let to_hedging: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .fetch_one(&pool)
            .await
            .unwrap();

    let to_market_making: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(
        to_hedging, 0,
        "Gross offchain cash is used for the rebalancing ratio, so a $300 \
         reserve must not trigger base_to_alpaca on a balanced 500/500 split"
    );
    assert_eq!(
        to_market_making, 0,
        "alpaca_to_base should not have been called"
    );
}

/// Verifies that without reserve, the same 500/500 split is balanced
/// and no rebalancing triggers. This is the counterpart to
/// `cash_reserve_does_not_shift_rebalancing_ratio`.
#[tokio::test]
async fn balanced_usdc_without_reserve_triggers_no_rebalancing() {
    let pool = setup_test_db().await;

    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default(),
        event_sender,
    ));

    build_imbalanced_inventory(Imbalance::Usdc {
        inventory: &inventory,
        onchain: Usdc::new(float!(500)),
        offchain: Usdc::new(float!(500)),
    })
    .await;

    let (sender, mut receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingService::new(
        test_trigger_config(),
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    );

    trigger.check_and_trigger_usdc().await;

    drop(trigger);

    assert!(
        matches!(receiver.try_recv(), Err(TryRecvError::Disconnected)),
        "Balanced 500/500 split should not trigger any USDC rebalancing"
    );
}

/// Verifies that when a reserve is configured and the broker stops
/// reporting `cash_withdrawable_cents` (e.g. transient Alpaca outage), an
/// existing offchain imbalance does not trigger an Alpaca-to-Base
/// transfer: the system refuses to act because reserve-safety cannot be
/// proven. Companion to `cash_reserve_does_not_shift_rebalancing_ratio`
/// which covers the balanced case.
#[tokio::test]
async fn usdc_alpaca_to_base_skips_when_withdrawable_cash_missing_with_reserve() {
    let pool = setup_test_db().await;

    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default(),
        event_sender,
    ));

    // Imbalanced 100 onchain / 500 offchain (17% / 83%) is well outside the
    // 30%-70% band. gross is set (production invariant after first poll
    // when a reserve is configured), but the broker did not report
    // withdrawable cash.
    {
        let mut guard = inventory.write().await;
        let taken = std::mem::take(&mut *guard);
        *guard = taken
            .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
            .with_offchain_gross_usd_cents(50_000);
    }

    let mut config = test_trigger_config();
    if let Some(cash) = config.assets.cash.as_mut() {
        cash.reserved = Some(Positive::new(Usd::new(float!(100))).unwrap());
    }

    let (sender, mut receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingService::new(
        config,
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    );

    trigger.check_and_trigger_usdc().await;

    drop(trigger);

    assert!(
        matches!(receiver.try_recv(), Err(TryRecvError::Disconnected)),
        "Missing withdrawable cash + reserve configured must suppress Alpaca-to-Base rebalancing even with a real imbalance"
    );
}

/// Verifies that setting `usdc: None` in `RebalancingServiceConfig`
/// disables USDC rebalancing entirely: even with a severe imbalance,
/// `check_and_trigger_usdc` dispatches no operations.
#[tokio::test]
async fn usdc_none_disables_usdc_rebalancing() {
    let pool = setup_test_db().await;

    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default(),
        event_sender,
    ));

    build_imbalanced_inventory(Imbalance::Usdc {
        inventory: &inventory,
        onchain: Usdc::new(float!(100)),
        offchain: Usdc::new(float!(900)),
    })
    .await;

    let (sender, mut receiver) = mpsc::channel(10);

    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingService::new(
        RebalancingServiceConfig {
            usdc: None,
            ..test_trigger_config()
        },
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    );

    trigger.check_and_trigger_usdc().await;

    drop(trigger);

    assert!(
        matches!(receiver.try_recv(), Err(TryRecvError::Disconnected)),
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
        service,
        inventory: _,
        position_cqrs,
        receiver,
    } = setup_equity_trigger().await;

    // Build inventory: 20 onchain, 80 offchain = 20% ratio -> TooMuchOffchain
    build_imbalanced_inventory(Imbalance::Equity {
        position_cqrs: &position_cqrs,
        symbol: &symbol,
        onchain: float!(20),
        offchain: float!(80),
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
        mock_vault_lookup_for_symbol(&symbol, token),
        tokenizer,
        MockWrapper::new(),
        Address::ZERO,
    );

    let mint_mock = server.mock(|when, then| {
        when.method(POST).path(tokenization_mint_path());
        then.status(500).body("Internal Server Error");
    });

    let mock_equity = Arc::new(MockCrossVenueEquityTransfer::new());

    let rebalancer = Rebalancer::new(
        equity_transfer as _,
        mock_equity as _,
        receiver,
        Arc::new(std::sync::RwLock::new(std::collections::HashSet::new())),
        tokio_util::sync::CancellationToken::new(),
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
                amount: FractionalShares::new(float!(1)),
                direction: Direction::Sell,
                price_usdc: float!(150.0),
                block_timestamp: Utc::now(),
            },
        )
        .await
        .unwrap();
    drain_pending_jobs(&service).await.unwrap();

    drop(service);
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
                "VaultRegistryEvent::EquityVaultSeededFromConfig",
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
    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default()
            .with_usdc(Usdc::new(float!(50)), Usdc::new(float!(950)))
            .with_withdrawable_cash_cents(95_000),
        event_sender,
    ));

    let assets = AssetsConfig {
        equities: EquitiesConfig::default(),
        cash: Some(CashAssetConfig {
            vault_ids: Vec::new(),
            rebalancing: OperationMode::Enabled,
            operational_limit: Some(Positive::new(Usdc::new(float!(100))).unwrap()),
            reserved: None,
        }),
    };

    let config = RebalancingServiceConfig {
        equity: ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        },
        usdc: Some(ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        }),
        transfer_timeout: Duration::from_secs(30 * 60),
        assets,
        disabled_assets: HashSet::new(),
    };

    let (sender, _receiver) = mpsc::channel(10);
    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingService::new(
        config,
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    );

    // Cycle 1: excess = 450, capped to 100
    trigger.check_and_trigger_usdc().await;
    let cycle1 = drain_pending_usdc_transfer_jobs(&pool).await;
    assert_eq!(
        cycle1.as_slice(),
        [EnqueuedUsdcOperation::AlpacaToBase {
            amount: Usdc::new(float!(100))
        }],
        "First transfer capped to $100",
    );
    trigger.clear_usdc_in_progress();

    // Simulate first transfer: 150 onchain, 850 offchain = 15% ratio
    // Still below 30% lower bound, excess = 500 - 150 = 350
    {
        let mut guard = inventory.write().await;
        let taken = std::mem::take(&mut *guard);
        *guard = taken
            .with_usdc(Usdc::new(float!(150)), Usdc::new(float!(850)))
            .with_withdrawable_cash_cents(85_000);
    }

    // Cycle 2: excess = 350, capped to 100
    trigger.check_and_trigger_usdc().await;
    let cycle2 = drain_pending_usdc_transfer_jobs(&pool).await;
    assert_eq!(
        cycle2.as_slice(),
        [EnqueuedUsdcOperation::AlpacaToBase {
            amount: Usdc::new(float!(100))
        }],
        "Second transfer capped to $100",
    );
    trigger.clear_usdc_in_progress();

    // Simulate second transfer: 250 onchain, 750 offchain = 25% ratio
    // Still below 30% lower bound, excess = 500 - 250 = 250
    {
        let mut guard = inventory.write().await;
        let taken = std::mem::take(&mut *guard);
        *guard = taken
            .with_usdc(Usdc::new(float!(250)), Usdc::new(float!(750)))
            .with_withdrawable_cash_cents(75_000);
    }

    // Cycle 3: excess = 250, capped to 100
    trigger.check_and_trigger_usdc().await;
    let cycle3 = drain_pending_usdc_transfer_jobs(&pool).await;
    assert_eq!(
        cycle3.as_slice(),
        [EnqueuedUsdcOperation::AlpacaToBase {
            amount: Usdc::new(float!(100))
        }],
        "Third transfer capped to $100",
    );
    trigger.clear_usdc_in_progress();

    // Simulate third transfer: 350 onchain, 650 offchain = 35% ratio
    // Now within [30%, 70%] band -> balanced, no more trigger
    {
        let mut guard = inventory.write().await;
        let taken = std::mem::take(&mut *guard);
        *guard = taken
            .with_usdc(Usdc::new(float!(350)), Usdc::new(float!(650)))
            .with_withdrawable_cash_cents(65_000);
    }

    trigger.check_and_trigger_usdc().await;
    assert!(
        drain_pending_usdc_transfer_jobs(&pool).await.is_empty(),
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
    let (event_sender, _) = broadcast::channel::<Statement>(16);
    let inventory = Arc::new(BroadcastingInventory::new(
        InventoryView::default()
            .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(900)))
            .with_withdrawable_cash_cents(90_000),
        event_sender,
    ));

    let assets = AssetsConfig {
        equities: EquitiesConfig::default(),
        cash: Some(CashAssetConfig {
            vault_ids: Vec::new(),
            rebalancing: OperationMode::Enabled,
            operational_limit: Some(Positive::new(Usdc::new(float!(100))).unwrap()),
            reserved: None,
        }),
    };
    let config = RebalancingServiceConfig {
        equity: ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        },
        usdc: Some(ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        }),
        transfer_timeout: Duration::from_secs(30 * 60),
        assets,
        disabled_assets: HashSet::new(),
    };

    let (sender, _receiver) = mpsc::channel(10);
    let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
    let wrapper = Arc::new(MockWrapper::new());

    let trigger = RebalancingService::new(
        config,
        vault_registry,
        TEST_ORDERBOOK,
        TEST_ORDER_OWNER,
        Arc::clone(&inventory),
        sender,
        wrapper,
        RebalancingSchedulers::new(&pool),
    );

    // First trigger fires: excess = 400, capped to 100
    trigger.check_and_trigger_usdc().await;
    assert_eq!(
        drain_pending_usdc_transfer_jobs(&pool).await.as_slice(),
        [EnqueuedUsdcOperation::AlpacaToBase {
            amount: Usdc::new(float!(100))
        }],
        "First transfer capped to $100",
    );

    // Without clearing in_progress, second trigger is blocked
    trigger.check_and_trigger_usdc().await;
    assert!(
        drain_pending_usdc_transfer_jobs(&pool).await.is_empty(),
        "In-progress guard should block second trigger"
    );

    // Clear in-progress (simulates operation completion/failure)
    trigger.clear_usdc_in_progress();

    // Trigger fires again: same inventory, same excess = 400, capped to 100
    trigger.check_and_trigger_usdc().await;
    assert_eq!(
        drain_pending_usdc_transfer_jobs(&pool).await.as_slice(),
        [EnqueuedUsdcOperation::AlpacaToBase {
            amount: Usdc::new(float!(100))
        }],
        "Retry transfer also capped to $100",
    );
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
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));

        build_imbalanced_inventory(Imbalance::Usdc {
            inventory: &inventory,
            onchain: Usdc::new(float!(350)),
            offchain: Usdc::new(float!(650)),
        })
        .await;

        let (sender, mut receiver) = mpsc::channel(10);
        let wide_config = RebalancingServiceConfig {
            equity: ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.4),
            },
            usdc: Some(ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.4),
            }),
            transfer_timeout: Duration::from_secs(30 * 60),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: Some(CashAssetConfig {
                    vault_ids: Vec::new(),
                    rebalancing: OperationMode::Enabled,
                    operational_limit: None,
                    reserved: None,
                }),
            },
            disabled_assets: HashSet::new(),
        };
        let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
        let wrapper = Arc::new(MockWrapper::new());
        let trigger = RebalancingService::new(
            wide_config,
            vault_registry,
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            Arc::clone(&inventory),
            sender,
            wrapper,
            RebalancingSchedulers::new(&pool),
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
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));

        build_imbalanced_inventory(Imbalance::Usdc {
            inventory: &inventory,
            onchain: Usdc::new(float!(350)),
            offchain: Usdc::new(float!(650)),
        })
        .await;

        let (sender, _receiver) = mpsc::channel(10);
        let tight_config = RebalancingServiceConfig {
            equity: ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.1),
            },
            usdc: Some(ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.1),
            }),
            transfer_timeout: Duration::from_secs(30 * 60),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: Some(CashAssetConfig {
                    vault_ids: Vec::new(),
                    rebalancing: OperationMode::Enabled,
                    operational_limit: None,
                    reserved: None,
                }),
            },
            disabled_assets: HashSet::new(),
        };
        let vault_registry = Arc::new(test_store::<VaultRegistry>(pool.clone(), ()));
        let wrapper = Arc::new(MockWrapper::new());
        let trigger = RebalancingService::new(
            tight_config,
            vault_registry,
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            Arc::clone(&inventory),
            sender,
            wrapper,
            RebalancingSchedulers::new(&pool),
        );

        trigger.check_and_trigger_usdc().await;
        drop(trigger);

        // Excess = target_onchain - actual_onchain = 500 - 350 = $150
        assert_eq!(
            drain_pending_usdc_transfer_jobs(&pool).await.as_slice(),
            [EnqueuedUsdcOperation::AlpacaToBase {
                amount: Usdc::new(float!(150))
            }],
            "Tight threshold (40%-60%) should trigger at 35% onchain ratio with $150 excess",
        );
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
        service,
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
    let mock_wrapper = MockWrapper::new()
        .with_tokenized_shares(token_address)
        .with_wrapped_token(token_address);

    // Wire mint/redemption stores with trigger so lifecycle events update
    // inflight state through the trigger's Reactor
    let equity_transfer = build_equity_transfer_with_service(
        &pool,
        raindex,
        mock_vault_lookup_for_symbol(&symbol, token_address),
        tokenizer,
        mock_wrapper,
        signer.address(),
        &service,
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
    drain_pending_jobs(&service).await.unwrap();

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
        service,
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
    let mock_wrapper = MockWrapper::new()
        .with_tokenized_shares(token_address)
        .with_wrapped_token(token_address);

    let equity_transfer = build_equity_transfer_with_service(
        &pool,
        raindex,
        mock_vault_lookup_for_symbol(&symbol, token_address),
        tokenizer,
        mock_wrapper,
        signer.address(),
        &service,
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
    drain_pending_jobs(&service).await.unwrap();

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
/// available -- tokens never left our wallet.
#[tokio::test]
async fn transfer_failed_cancels_redemption_inflight() {
    let EquityTriggerFixture {
        pool,
        symbol,
        aggregate_id: _,
        service,
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
        vault_lookup: mock_vault_lookup_for_symbol(&symbol, token_address),
        tokenizer,
        wrapper: Arc::new(MockWrapper::new()),
    };

    let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
        .with(Arc::clone(&service))
        .build(equity_services)
        .await
        .unwrap();

    let redemption_id = RedemptionAggregateId::new("redemption-transfer-failed");

    // Redeem: creates VaultWithdrawPending
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

    // After VaultWithdrawPending, inflight should be set at MarketMaking
    let inflight_after_withdraw = inventory
        .read()
        .await
        .equity_inflight(&symbol, Venue::MarketMaking)
        .unwrap();
    assert!(
        !inflight_after_withdraw.inner().is_zero().unwrap(),
        "Inflight should be non-zero after VaultWithdrawPending, got {inflight_after_withdraw:?}"
    );

    redemption_store
        .send(&redemption_id, EquityRedemptionCommand::SubmitWithdraw)
        .await
        .unwrap();

    redemption_store
        .send(&redemption_id, EquityRedemptionCommand::ConfirmWithdraw)
        .await
        .unwrap();

    // UnwrapTokens -> UnwrapPending, SubmitUnwrap -> UnwrapSubmitted,
    // ConfirmUnwrap -> TokensUnwrapped
    redemption_store
        .send(&redemption_id, EquityRedemptionCommand::UnwrapTokens)
        .await
        .unwrap();

    redemption_store
        .send(&redemption_id, EquityRedemptionCommand::SubmitUnwrap)
        .await
        .unwrap();

    redemption_store
        .send(&redemption_id, EquityRedemptionCommand::ConfirmUnwrap)
        .await
        .unwrap();

    // PrepareSend -> SendPending
    redemption_store
        .send(&redemption_id, EquityRedemptionCommand::PrepareSend)
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
