//! E2E rebalancing tests exercising inventory imbalance detection and
//! corrective actions (mint/redeem tokenized equities, USDC CCTP bridge).
//!
//! Each test starts a real Anvil fork, mock broker, and launches the bot
//! with rebalancing enabled. Tests verify that the inventory poller detects
//! imbalances and triggers appropriate rebalancing operations.
//!
//! # Infrastructure requirements
//!
//! The full rebalancing pipeline requires onchain infrastructure beyond
//! plain ERC-20 tokens:
//!
//! - **Equity mint/redemption**: The trigger calls `convertToAssets()` on
//!   ERC-4626 vault tokens to determine the vault ratio. Plain test ERC-20
//!   tokens don't implement this interface, so the equity trigger cannot
//!   fire. Requires deploying ERC-4626 vault wrappers on Anvil.
//!
//! - **USDC bridging**: Requires CCTP MessageTransmitter and TokenMessenger
//!   contracts deployed on both Ethereum and Base Anvil forks, plus a mock
//!   attestation service.
//!
//! All rebalancing tests deploy ERC-4626 vault wrappers so the trigger's
//! `convertToAssets()` call succeeds. The `inventory_snapshot_events_emitted`
//! test verifies the first stage of the pipeline (inventory polling +
//! snapshot events).

mod common;
mod services;

use std::time::Duration;

use alloy::primitives::{Address, B256, U256, utils::parse_units};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;

use common::{
    ExpectedPosition, assert_broker_state, assert_cqrs_state, assert_event_subsequence,
    assert_full_pipeline, assert_single_clean_aggregate, build_rebalancing_ctx,
    build_usdc_rebalancing_ctx, connect_db, count_events, fetch_events_by_type, spawn_bot,
    wait_for_processing,
};
use services::alpaca_broker::AlpacaBrokerMock;
use services::base_chain::{self, IERC20, TakeDirection, USDC_BASE};
use services::cctp_attestation::CctpAttestationMock;
use services::cctp_attester;
use services::ethereum_chain;
use st0x_hedge::UsdcRebalancing;
use st0x_hedge::bindings::IOrderBookV5;

const BASE_RPC_URL: &str =
    "https://api.developer.coinbase.com/rpc/v1/base/DD1T1ZCY6bZ09lHv19TjBHi1saRZCHFF";
const ETHEREUM_RPC_URL: &str =
    "https://api.developer.coinbase.com/rpc/v1/ethereum/DD1T1ZCY6bZ09lHv19TjBHi1saRZCHFF";

// /// Processes several one-directional SellEquity trades to create an
// /// inventory imbalance (offchain accumulates hedged equity while onchain
// /// vault depletes). Verifies the inventory poller runs and emits
// /// `InventorySnapshot` events.
// #[tokio::test]
// async fn inventory_snapshot_events_emitted() -> anyhow::Result<()> {
//     let mut chain = base_chain::BaseChain::start(BASE_RPC_URL).await?;
//     let (vault_addr, underlying_addr) = chain.deploy_equity_vault("AAPL").await?;
//
//     let broker = AlpacaBrokerMock::start().build().await;
//
//     let current_block = chain.provider.get_block_number().await?;
//     let db_dir = tempfile::tempdir()?;
//     let db_path = db_dir.path().join("e2e.sqlite");
//
//     let ctx = build_rebalancing_ctx(
//         &chain,
//         &broker,
//         &db_path,
//         current_block,
//         &[("AAPL", vault_addr, underlying_addr)],
//         UsdcRebalancing::Disabled,
//         None,
//     )?;
//     let bot = spawn_bot(ctx);
//
//     tokio::time::sleep(Duration::from_secs(2)).await;
//
//     // Process several sells to create inventory pressure.
//     for _ in 0..3 {
//         chain
//             .take_order("AAPL", "7.5", TakeDirection::SellEquity)
//             .await?;
//         tokio::time::sleep(Duration::from_secs(3)).await;
//     }
//
//     // Wait for hedging + inventory poller cycles (poll interval is 15s)
//     wait_for_processing(&bot, 20).await;
//
//     // Verify inventory snapshot events were emitted by the poller
//     let pool = connect_db(&db_path).await?;
//
//     let onchain_events = count_events(&pool, "OnChainTrade").await?;
//     assert!(
//         onchain_events >= 3,
//         "Expected at least 3 OnChainTrade events, got {onchain_events}"
//     );
//
//     let inventory_events = count_events(&pool, "InventorySnapshot").await?;
//     assert!(
//         inventory_events >= 1,
//         "Expected at least 1 InventorySnapshot event from the inventory poller, \
//          got {inventory_events}"
//     );
//
//     pool.close().await;
//     bot.abort();
//     Ok(())
// }
//
// /// Equity mint triggered by offchain-heavy imbalance.
// ///
// /// SellEquity trades cause the bot to sell equity onchain (vault depletes)
// /// and buy equity offchain (broker position grows). This creates a
// /// TooMuchOffchain equity imbalance that should trigger a mint operation
// /// to tokenize offchain shares and deposit them into the Raindex vault.
// ///
// /// The test deploys an ERC-4626 vault wrapping a test ERC20 so the
// /// trigger's `convertToAssets()` call succeeds. Tokenization API
// /// endpoints are mocked on the broker server (the conductor resolves
// /// all Alpaca services from the same base URL).
// ///
// /// We assert the full mint pipeline fires: MintRequested, MintAccepted,
// /// TokensReceived, TokensWrapped, and DepositedIntoRaindex. This
// /// verifies the complete flow from tokenization request through
// /// ERC-4626 wrapping and Raindex vault deposit.
// #[tokio::test]
// async fn equity_imbalance_triggers_mint() -> anyhow::Result<()> {
//     let mut chain = base_chain::BaseChain::start(BASE_RPC_URL).await?;
//
//     // Deploy ERC-4626 vault for AAPL. The owner already has underlying
//     // tokens + unlimited vault approval from deploy_equity_vault(), so
//     // the wrapping step (ERC-4626 deposit) after Alpaca "mints" will
//     // succeed using the owner key as the market maker.
//     let (vault_addr, underlying_addr) = chain.deploy_equity_vault("AAPL").await?;
//
//     // Start broker mock with tokenization endpoints registered
//     let broker = AlpacaBrokerMock::start().build().await;
//     broker.register_tokenization_endpoints();
//
//     let current_block = chain.provider.get_block_number().await?;
//     let db_dir = tempfile::tempdir()?;
//     let db_path = db_dir.path().join("e2e.sqlite");
//     let database_url = db_path.display().to_string();
//
//     let ctx = build_rebalancing_ctx(
//         &chain,
//         &broker,
//         &db_path,
//         current_block,
//         &[("AAPL", vault_addr, underlying_addr)],
//         UsdcRebalancing::Disabled,
//         None,
//     )?;
//     let bot = spawn_bot(ctx);
//
//     tokio::time::sleep(Duration::from_secs(2)).await;
//
//     // Process several SellEquity trades to create offchain-heavy imbalance.
//     // Each sell depletes the onchain vault and accumulates offchain equity.
//     // Inter-trade delays ensure hedging completes before the next trade.
//     let mut take_results = Vec::new();
//     for _ in 0..3 {
//         take_results.push(
//             chain
//                 .take_order("AAPL", "7.5", TakeDirection::SellEquity)
//                 .await?,
//         );
//         tokio::time::sleep(Duration::from_secs(3)).await;
//     }
//
//     // Wait for: hedging + inventory poller + trigger evaluation +
//     // tokenization request + polling (2 polls at 10s each)
//     wait_for_processing(&bot, 40).await;
//
//     // ── Layer 1: Hedging pipeline (broker + CQRS) ────────────────────
//     // Skip vault depletion assertions because the mint pipeline deposits
//     // wrapped tokens back into the output vault, making it non-zero.
//     let scenarios = [ExpectedPosition {
//         symbol: "AAPL",
//         amount: "22.5",
//         direction: TakeDirection::SellEquity,
//         fill_price: "150.00",
//         expected_accumulated_long: "0",
//         expected_accumulated_short: "22.5",
//         expected_net: "0",
//     }];
//
//     assert_broker_state(&scenarios, &broker);
//     assert_cqrs_state(&scenarios, take_results.len(), &database_url).await?;
//
//     // ── Layer 2: Inventory snapshots ─────────────────────────────────
//     let pool = connect_db(&db_path).await?;
//
//     let inventory_events = count_events(&pool, "InventorySnapshot").await?;
//     assert!(
//         inventory_events >= 1,
//         "Expected at least 1 InventorySnapshot event, got {inventory_events}"
//     );
//
//     // ── Layer 3: Mint pipeline (events) ──────────────────────────────
//     let mint_events = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
//     assert!(
//         !mint_events.is_empty(),
//         "Expected at least 1 TokenizedEquityMint event (mint trigger fired), got 0"
//     );
//
//     assert_event_subsequence(
//         &mint_events,
//         &[
//             "TokenizedEquityMintEvent::MintRequested",
//             "TokenizedEquityMintEvent::MintAccepted",
//             "TokenizedEquityMintEvent::TokensReceived",
//             "TokenizedEquityMintEvent::TokensWrapped",
//             "TokenizedEquityMintEvent::DepositedIntoRaindex",
//         ],
//     );
//
//     assert_single_clean_aggregate(&mint_events, &["Failed", "Rejected"]);
//
//     // ── Layer 3: Mint pipeline (broker state) ────────────────────────
//     let mint_requests: Vec<_> = broker
//         .tokenization_requests()
//         .into_iter()
//         .filter(|req| req.request_type == "mint" && req.symbol == "AAPL")
//         .collect();
//     assert!(
//         !mint_requests.is_empty(),
//         "Expected at least 1 mint tokenization request for AAPL on the broker"
//     );
//     assert!(
//         mint_requests.iter().any(|req| req.status == "completed"),
//         "Expected at least one completed mint request, got: {:?}",
//         mint_requests.iter().map(|r| &r.status).collect::<Vec<_>>()
//     );
//
//     pool.close().await;
//     bot.abort();
//     Ok(())
// }
//
// /// Equity redemption triggered by onchain-heavy imbalance.
// ///
// /// BuyEquity trades cause the bot to receive equity onchain (vault fills)
// /// and sell equity offchain (broker position shrinks). This creates a
// /// TooMuchOnchain equity imbalance that should trigger a redemption
// /// operation to unwrap and transfer tokens to the redemption wallet.
// ///
// /// Expected CQRS event flow:
// /// - `EquityRedemption`: WithdrawnFromRaindex -> TokensUnwrapped
// ///   -> TokensSent -> Detected -> Completed
// ///
// /// Requires: ERC-4626 vault wrapper on Anvil (for trigger ratio check),
// /// Raindex vault with sufficient balance for withdrawal, and tokenization
// /// API mock endpoints for redemption detection/completion polling.
// #[tokio::test]
// async fn equity_imbalance_triggers_redemption() -> anyhow::Result<()> {
//     let mut chain = base_chain::BaseChain::start(BASE_RPC_URL).await?;
//     let (vault_addr, underlying_addr) = chain.deploy_equity_vault("AAPL").await?;
//
//     let broker = AlpacaBrokerMock::start().build().await;
//     broker.register_redemption_tokenization_endpoints();
//
//     // Create a known redemption wallet so the watcher can detect
//     // ERC-20 transfers to it and auto-add redemption requests.
//     let redemption_wallet = Address::random();
//     let _watcher = broker.start_redemption_watcher(chain.provider.clone(), redemption_wallet);
//
//     let current_block = chain.provider.get_block_number().await?;
//     let db_dir = tempfile::tempdir()?;
//     let db_path = db_dir.path().join("e2e.sqlite");
//     let database_url = db_path.display().to_string();
//
//     let ctx = build_rebalancing_ctx(
//         &chain,
//         &broker,
//         &db_path,
//         current_block,
//         &[("AAPL", vault_addr, underlying_addr)],
//         UsdcRebalancing::Disabled,
//         Some(redemption_wallet),
//     )?;
//     let bot = spawn_bot(ctx);
//
//     tokio::time::sleep(Duration::from_secs(2)).await;
//
//     // BuyEquity trades: equity accumulates onchain while the bot sells
//     // offchain to hedge. Each trade deposits equity into the order's
//     // input vault, but the VaultRegistry only tracks the LAST vault per
//     // token. With 3 trades of 7.5 shares each, only 7.5 is visible
//     // onchain while offchain shows -22.5 (net = -15). To create a
//     // genuine TooMuchOnchain imbalance, we pre-fund the last vault with
//     // additional wrapped tokens after the trades.
//     let mut take_results = Vec::new();
//     for _ in 0..3 {
//         take_results.push(
//             chain
//                 .take_order("AAPL", "7.5", TakeDirection::BuyEquity)
//                 .await?,
//         );
//         tokio::time::sleep(Duration::from_secs(3)).await;
//     }
//
//     // Deposit extra wrapped equity tokens into the last trade's input vault.
//     // This is the vault the VaultRegistry tracks. After deposit:
//     //   onchain = 7.5 + 100 = 107.5, offchain = -22.5
//     //   total = 85, ratio = 107.5/85 ≈ 1.26 → TooMuchOnchain → Redemption
//     let equity_vault_id = take_results.last().unwrap().input_vault_id;
//     let extra_equity: U256 = parse_units("100", 18)?.into();
//     chain
//         .deposit_into_raindex_vault(vault_addr, equity_vault_id, extra_equity, 18)
//         .await?;
//
//     // Wait for: hedging + inventory poller + trigger evaluation +
//     // redemption pipeline (withdraw + unwrap + send + detection + completion)
//     wait_for_processing(&bot, 40).await;
//
//     // ── Layer 1: Hedging pipeline (broker + CQRS) ────────────────────
//     // Skip vault assertions because the redemption pipeline withdraws from
//     // the vault that BuyEquity trades fill, altering its expected balance.
//     let scenarios = [ExpectedPosition {
//         symbol: "AAPL",
//         amount: "22.5",
//         direction: TakeDirection::BuyEquity,
//         fill_price: "150.00",
//         expected_accumulated_long: "22.5",
//         expected_accumulated_short: "0",
//         expected_net: "0",
//     }];
//
//     assert_broker_state(&scenarios, &broker);
//     assert_cqrs_state(&scenarios, take_results.len(), &database_url).await?;
//
//     // ── Layer 2: Inventory snapshots ─────────────────────────────────
//     let pool = connect_db(&db_path).await?;
//
//     let inventory_events = count_events(&pool, "InventorySnapshot").await?;
//     assert!(
//         inventory_events >= 1,
//         "Expected at least 1 InventorySnapshot event, got {inventory_events}"
//     );
//
//     // ── Layer 3: Redemption pipeline (events) ────────────────────────
//     let redemption_events = fetch_events_by_type(&pool, "EquityRedemption").await?;
//     assert!(
//         !redemption_events.is_empty(),
//         "Expected at least 1 EquityRedemption event (redemption trigger fired), got 0"
//     );
//
//     assert_event_subsequence(
//         &redemption_events,
//         &[
//             "EquityRedemptionEvent::WithdrawnFromRaindex",
//             "EquityRedemptionEvent::TokensUnwrapped",
//             "EquityRedemptionEvent::TokensSent",
//             "EquityRedemptionEvent::Detected",
//             "EquityRedemptionEvent::Completed",
//         ],
//     );
//
//     assert_single_clean_aggregate(&redemption_events, &["Failed", "Rejected"]);
//
//     // Verify the first event's payload contains the correct symbol.
//     let first_payload = &redemption_events[0].payload;
//     assert_eq!(
//         first_payload
//             .get("WithdrawnFromRaindex")
//             .and_then(|val| val.get("symbol"))
//             .and_then(|val| val.as_str()),
//         Some("AAPL"),
//         "First redemption event should be for AAPL, got: {first_payload}"
//     );
//
//     // ── Layer 3: Redemption pipeline (onchain) ───────────────────────
//     // The redemption pipeline sends underlying tokens to the redemption
//     // wallet. Verify the wallet received tokens.
//     let underlying_balance = IERC20::new(underlying_addr, &chain.provider)
//         .balanceOf(redemption_wallet)
//         .call()
//         .await?;
//     assert!(
//         underlying_balance > U256::ZERO,
//         "Redemption wallet should hold underlying tokens after redemption"
//     );
//
//     // ── Layer 3: Redemption pipeline (broker state) ──────────────────
//     let redeem_requests: Vec<_> = broker
//         .tokenization_requests()
//         .into_iter()
//         .filter(|req| req.request_type == "redeem")
//         .collect();
//     assert!(
//         !redeem_requests.is_empty(),
//         "Expected at least 1 redeem tokenization request on the broker"
//     );
//     assert!(
//         redeem_requests.iter().any(|req| req.status == "completed"),
//         "Expected at least one completed redeem request, got: {:?}",
//         redeem_requests
//             .iter()
//             .map(|r| &r.status)
//             .collect::<Vec<_>>(),
//     );
//
//     // ── Layer 3: Redemption pipeline (CQRS projection) ──────────────
//     // The equity_redemption_view projection stores the aggregate state.
//     // By this point the pipeline should have reached Completed.
//     let (payload_json,): (String,) =
//         sqlx::query_as("SELECT payload FROM equity_redemption_view LIMIT 1")
//             .fetch_one(&pool)
//             .await?;
//     let payload: serde_json::Value = serde_json::from_str(&payload_json)?;
//     // Projection stores Lifecycle<EquityRedemption>, serialized as
//     // {"Live": {"Completed": {"symbol": "AAPL", ...}}}
//     let live = payload
//         .get("Live")
//         .unwrap_or_else(|| panic!("equity_redemption_view payload should be Live, got: {payload}"));
//     let completed = live.get("Completed").unwrap_or_else(|| {
//         panic!("equity_redemption_view should be in Completed state, got: {live}")
//     });
//     assert_eq!(
//         completed.get("symbol").and_then(|val| val.as_str()),
//         Some("AAPL"),
//         "Completed redemption should be for AAPL, got: {completed}"
//     );
//
//     pool.close().await;
//     bot.abort();
//     Ok(())
// }
//
// /// USDC rebalancing from Alpaca (offchain) to Base (onchain).
// ///
// /// BuyEquity trades cause the bot to spend USDC onchain (paying for equity)
// /// and receive USDC offchain (from selling equity on the broker). This
// /// creates a TooMuchOffchain USDC imbalance that should trigger a transfer
// /// from Alpaca to Base via CCTP bridge.
// ///
// /// Expected CQRS event flow:
// /// - `UsdcRebalance`: ConversionInitiated -> ConversionConfirmed
// ///   -> Initiated -> WithdrawalConfirmed -> BridgingInitiated
// ///   -> BridgeAttestationReceived -> Bridged -> DepositInitiated
// ///   -> DepositConfirmed
// ///
// /// Infrastructure: Forks both Base and Ethereum mainnet via Anvil so CCTP
// /// contracts are live at production addresses. Adds a test attester to
// /// `MessageTransmitterV2` on both chains and runs a background watcher
// /// that auto-signs attestations for `MessageSent` events.
// #[tokio::test]
// async fn usdc_imbalance_triggers_alpaca_to_base() -> anyhow::Result<()> {
//     // Fork both chains
//     let mut base_chain = base_chain::BaseChain::start(BASE_RPC_URL).await?;
//     let ethereum_chain = ethereum_chain::EthereumChain::start(ETHEREUM_RPC_URL).await?;
//
//     let (vault_addr, underlying_addr) = base_chain.deploy_equity_vault("AAPL").await?;
//
//     // Set up test attester on both chains' MessageTransmitterV2
//     let attester_key = B256::random();
//     let attester_signer = PrivateKeySigner::from_bytes(&attester_key)?;
//     let attester_address = attester_signer.address();
//
//     cctp_attester::setup_test_attester(&base_chain.provider, attester_address).await?;
//     cctp_attester::setup_test_attester(&ethereum_chain.provider, attester_address).await?;
//
//     // Start CCTP attestation mock with background watcher
//     let attestation_mock = CctpAttestationMock::start().await;
//     let _attestation_watcher = attestation_mock.start_watcher(
//         ethereum_chain.provider.clone(),
//         base_chain.provider.clone(),
//         attester_key,
//     );
//
//     // Start broker mock with wallet + tokenization endpoints
//     let broker = AlpacaBrokerMock::start().build().await;
//     broker.register_tokenization_endpoints();
//     broker.register_wallet_endpoints(base_chain.owner);
//
//     // Create a USDC vault on Raindex (deposit destination for bridged USDC)
//     let usdc_amount: U256 = parse_units("10000", 6)?.into();
//     let usdc_vault_id = base_chain.create_usdc_vault(usdc_amount).await?;
//
//     let current_block = base_chain.provider.get_block_number().await?;
//     let db_dir = tempfile::tempdir()?;
//     let db_path = db_dir.path().join("e2e.sqlite");
//     let database_url = db_path.display().to_string();
//
//     let ctx = build_usdc_rebalancing_ctx(
//         &base_chain,
//         &ethereum_chain,
//         &broker,
//         &attestation_mock.base_url(),
//         &db_path,
//         current_block,
//         &[("AAPL", vault_addr, underlying_addr)],
//         usdc_vault_id,
//     )?;
//     let bot = spawn_bot(ctx);
//
//     tokio::time::sleep(Duration::from_secs(2)).await;
//
//     // BuyEquity trades: bot spends USDC onchain (to buy equity), receives
//     // USDC offchain (from selling equity on broker). Creates TooMuchOffchain
//     // USDC imbalance triggering AlpacaToBase direction.
//     // Inter-trade delays ensure hedging completes before the next trade.
//     let mut take_results = Vec::new();
//     for _ in 0..3 {
//         take_results.push(
//             base_chain
//                 .take_order("AAPL", "7.5", TakeDirection::BuyEquity)
//                 .await?,
//         );
//         tokio::time::sleep(Duration::from_secs(3)).await;
//     }
//
//     // Wait for: hedging + inventory poller + trigger + full USDC pipeline
//     // (conversion + withdrawal + bridge + deposit)
//     wait_for_processing(&bot, 60).await;
//
//     // ── Layer 1: Full hedging pipeline (broker + vaults + CQRS) ──────
//     let scenario = ExpectedPosition {
//         symbol: "AAPL",
//         amount: "22.5",
//         direction: TakeDirection::BuyEquity,
//         fill_price: "150.00",
//         expected_accumulated_long: "22.5",
//         expected_accumulated_short: "0",
//         expected_net: "0",
//     };
//
//     assert_full_pipeline(
//         &[scenario],
//         &take_results,
//         &base_chain.provider,
//         base_chain.orderbook_addr,
//         base_chain.owner,
//         &broker,
//         &database_url,
//     )
//     .await?;
//
//     // ── Layer 2: Inventory snapshots ─────────────────────────────────
//     let pool = connect_db(&db_path).await?;
//
//     let inventory_events = count_events(&pool, "InventorySnapshot").await?;
//     assert!(
//         inventory_events >= 1,
//         "Expected at least 1 InventorySnapshot event, got {inventory_events}"
//     );
//
//     // ── Layer 3: USDC rebalance pipeline (events) ────────────────────
//     let usdc_events = fetch_events_by_type(&pool, "UsdcRebalance").await?;
//     assert!(
//         !usdc_events.is_empty(),
//         "Expected at least 1 UsdcRebalance event (USDC trigger fired), got 0"
//     );
//
//     // Verify the AlpacaToBase pipeline stages fired in order.
//     // Conversion happens first (USD -> USDC), then withdrawal + bridge + deposit.
//     assert_event_subsequence(
//         &usdc_events,
//         &[
//             "UsdcRebalanceEvent::ConversionInitiated",
//             "UsdcRebalanceEvent::ConversionConfirmed",
//             "UsdcRebalanceEvent::Initiated",
//             "UsdcRebalanceEvent::WithdrawalConfirmed",
//             "UsdcRebalanceEvent::BridgingInitiated",
//             "UsdcRebalanceEvent::BridgeAttestationReceived",
//             "UsdcRebalanceEvent::Bridged",
//             "UsdcRebalanceEvent::DepositInitiated",
//             "UsdcRebalanceEvent::DepositConfirmed",
//         ],
//     );
//
//     assert_single_clean_aggregate(&usdc_events, &["Failed"]);
//
//     // Verify the Initiated event payload records AlpacaToBase direction.
//     let initiated_event = usdc_events
//         .iter()
//         .find(|event| event.event_type == "UsdcRebalanceEvent::Initiated")
//         .expect("Missing Initiated event");
//     let initiated_payload = initiated_event
//         .payload
//         .get("Initiated")
//         .expect("Initiated event payload missing Initiated wrapper");
//     assert_eq!(
//         initiated_payload
//             .get("direction")
//             .and_then(|val| val.as_str()),
//         Some("AlpacaToBase"),
//         "Initiated event should record AlpacaToBase direction, got: {initiated_payload}"
//     );
//
//     // Verify the Bridged event records a nonzero amount received.
//     let bridged_event = usdc_events
//         .iter()
//         .find(|event| event.event_type == "UsdcRebalanceEvent::Bridged")
//         .expect("Missing Bridged event");
//     let bridged_payload = bridged_event
//         .payload
//         .get("Bridged")
//         .expect("Bridged event payload missing Bridged wrapper");
//     let amount_received = bridged_payload
//         .get("amount_received")
//         .and_then(|val| val.as_str())
//         .expect("Bridged event missing amount_received");
//     assert_ne!(
//         amount_received, "0",
//         "Bridged amount_received should be nonzero"
//     );
//
//     // ── Layer 3: USDC rebalance pipeline (broker state) ──────────────
//     // AlpacaToBase conversion: USD -> USDC = buy USDCUSD.
//     let usdcusd_orders: Vec<_> = broker
//         .orders()
//         .into_iter()
//         .filter(|order| order.symbol == "USDCUSD")
//         .collect();
//     assert!(
//         !usdcusd_orders.is_empty(),
//         "Expected a USDCUSD conversion order on the broker"
//     );
//     assert!(
//         usdcusd_orders.iter().any(|order| order.side == "buy"),
//         "AlpacaToBase conversion should be a buy (USD -> USDC), got sides: {:?}",
//         usdcusd_orders.iter().map(|o| &o.side).collect::<Vec<_>>(),
//     );
//
//     // AlpacaToBase direction: bot requests withdrawal from Alpaca wallet.
//     let wallet_transfers = broker.wallet_transfers();
//     let outgoing_transfers: Vec<_> = wallet_transfers
//         .iter()
//         .filter(|transfer| transfer.direction == "OUTGOING")
//         .collect();
//     assert!(
//         !outgoing_transfers.is_empty(),
//         "Expected at least 1 OUTGOING wallet transfer (Alpaca withdrawal)"
//     );
//     assert!(
//         outgoing_transfers
//             .iter()
//             .all(|transfer| transfer.status == "COMPLETE"),
//         "All OUTGOING wallet transfers should be COMPLETE, got: {:?}",
//         outgoing_transfers
//             .iter()
//             .map(|t| &t.status)
//             .collect::<Vec<_>>(),
//     );
//
//     // AlpacaToBase should NOT have any incoming transfers.
//     assert!(
//         !wallet_transfers
//             .iter()
//             .any(|transfer| transfer.direction == "INCOMING"),
//         "AlpacaToBase should not have INCOMING wallet transfers"
//     );
//
//     // ── Layer 3: USDC rebalance pipeline (onchain) ───────────────────
//     // The USDC vault on Raindex should reflect the bridge deposit.
//     let orderbook =
//         IOrderBookV5::IOrderBookV5Instance::new(base_chain.orderbook_addr, &base_chain.provider);
//     let vault_balance = orderbook
//         .vaultBalance2(base_chain.owner, USDC_BASE, usdc_vault_id)
//         .call()
//         .await?;
//     assert_ne!(
//         vault_balance,
//         B256::ZERO,
//         "USDC vault should have a nonzero balance after AlpacaToBase rebalance"
//     );
//
//     pool.close().await;
//     bot.abort();
//     Ok(())
// }
//
// /// USDC rebalancing from Base (onchain) to Alpaca (offchain).
// ///
// /// SellEquity trades cause the bot to receive USDC onchain (from selling
// /// equity) and spend USDC offchain (buying equity on the broker to hedge).
// /// This creates a TooMuchOnchain USDC imbalance that should trigger a
// /// transfer from Base to Alpaca via CCTP bridge.
// ///
// /// Expected CQRS event flow:
// /// - `UsdcRebalance`: Initiated -> WithdrawalConfirmed -> BridgingInitiated
// ///   -> BridgeAttestationReceived -> Bridged -> DepositInitiated
// ///   -> DepositConfirmed -> ConversionInitiated -> ConversionConfirmed
// ///
// /// Infrastructure: Same as `usdc_imbalance_triggers_alpaca_to_base` but the
// /// Raindex USDC vault is pre-funded so the bot can withdraw from it.
// #[tokio::test]
// async fn usdc_imbalance_triggers_base_to_alpaca() -> anyhow::Result<()> {
//     // Fork both chains
//     let mut base_chain = base_chain::BaseChain::start(BASE_RPC_URL).await?;
//     let ethereum_chain = ethereum_chain::EthereumChain::start(ETHEREUM_RPC_URL).await?;
//
//     let (vault_addr, underlying_addr) = base_chain.deploy_equity_vault("AAPL").await?;
//
//     // Set up test attester on both chains' MessageTransmitterV2
//     let attester_key = B256::random();
//     let attester_signer = PrivateKeySigner::from_bytes(&attester_key)?;
//     let attester_address = attester_signer.address();
//
//     cctp_attester::setup_test_attester(&base_chain.provider, attester_address).await?;
//     cctp_attester::setup_test_attester(&ethereum_chain.provider, attester_address).await?;
//
//     // Start CCTP attestation mock with background watcher
//     let attestation_mock = CctpAttestationMock::start().await;
//     let _attestation_watcher = attestation_mock.start_watcher(
//         ethereum_chain.provider.clone(),
//         base_chain.provider.clone(),
//         attester_key,
//     );
//
//     // Start broker mock with wallet + tokenization endpoints
//     let broker = AlpacaBrokerMock::start().build().await;
//     broker.register_tokenization_endpoints();
//     broker.register_wallet_endpoints(base_chain.owner);
//
//     // Pre-fund the USDC vault so the bot can withdraw from it.
//     // BaseToAlpaca direction: bot withdraws USDC from Raindex, bridges to
//     // Ethereum, deposits to Alpaca.
//     let usdc_amount: U256 = parse_units("100000", 6)?.into();
//     let usdc_vault_id = base_chain.create_usdc_vault(usdc_amount).await?;
//
//     let current_block = base_chain.provider.get_block_number().await?;
//     let db_dir = tempfile::tempdir()?;
//     let db_path = db_dir.path().join("e2e.sqlite");
//     let database_url = db_path.display().to_string();
//
//     let ctx = build_usdc_rebalancing_ctx(
//         &base_chain,
//         &ethereum_chain,
//         &broker,
//         &attestation_mock.base_url(),
//         &db_path,
//         current_block,
//         &[("AAPL", vault_addr, underlying_addr)],
//         usdc_vault_id,
//     )?;
//     let bot = spawn_bot(ctx);
//
//     tokio::time::sleep(Duration::from_secs(2)).await;
//
//     // SellEquity trades: bot receives USDC onchain (from selling equity),
//     // spends USDC offchain (buying equity on broker to hedge). Creates
//     // TooMuchOnchain USDC imbalance triggering BaseToAlpaca direction.
//     // Inter-trade delays ensure hedging completes before the next trade.
//     let mut take_results = Vec::new();
//     for _ in 0..3 {
//         take_results.push(
//             base_chain
//                 .take_order("AAPL", "7.5", TakeDirection::SellEquity)
//                 .await?,
//         );
//         tokio::time::sleep(Duration::from_secs(3)).await;
//     }
//
//     // Wait for: hedging + inventory poller + trigger + full USDC pipeline
//     // (withdraw from Raindex + bridge + deposit to Alpaca + conversion)
//     wait_for_processing(&bot, 60).await;
//
//     // ── Layer 1: Full hedging pipeline (broker + vaults + CQRS) ──────
//     let scenario = ExpectedPosition {
//         symbol: "AAPL",
//         amount: "22.5",
//         direction: TakeDirection::SellEquity,
//         fill_price: "150.00",
//         expected_accumulated_long: "0",
//         expected_accumulated_short: "22.5",
//         expected_net: "0",
//     };
//
//     assert_full_pipeline(
//         &[scenario],
//         &take_results,
//         &base_chain.provider,
//         base_chain.orderbook_addr,
//         base_chain.owner,
//         &broker,
//         &database_url,
//     )
//     .await?;
//
//     // ── Layer 2: Inventory snapshots ─────────────────────────────────
//     let pool = connect_db(&db_path).await?;
//
//     let inventory_events = count_events(&pool, "InventorySnapshot").await?;
//     assert!(
//         inventory_events >= 1,
//         "Expected at least 1 InventorySnapshot event, got {inventory_events}"
//     );
//
//     // ── Layer 3: USDC rebalance pipeline (events) ────────────────────
//     let usdc_events = fetch_events_by_type(&pool, "UsdcRebalance").await?;
//     assert!(
//         !usdc_events.is_empty(),
//         "Expected at least 1 UsdcRebalance event (USDC trigger fired), got 0"
//     );
//
//     // Verify the BaseToAlpaca pipeline stages fired in order.
//     // Withdrawal + bridge happen first, then conversion (USDC -> USD) at the end.
//     assert_event_subsequence(
//         &usdc_events,
//         &[
//             "UsdcRebalanceEvent::Initiated",
//             "UsdcRebalanceEvent::WithdrawalConfirmed",
//             "UsdcRebalanceEvent::BridgingInitiated",
//             "UsdcRebalanceEvent::BridgeAttestationReceived",
//             "UsdcRebalanceEvent::Bridged",
//             "UsdcRebalanceEvent::DepositInitiated",
//             "UsdcRebalanceEvent::DepositConfirmed",
//             "UsdcRebalanceEvent::ConversionInitiated",
//             "UsdcRebalanceEvent::ConversionConfirmed",
//         ],
//     );
//
//     assert_single_clean_aggregate(&usdc_events, &["Failed"]);
//
//     // Verify the Initiated event payload records BaseToAlpaca direction.
//     let initiated_event = usdc_events
//         .iter()
//         .find(|event| event.event_type == "UsdcRebalanceEvent::Initiated")
//         .expect("Missing Initiated event");
//     let initiated_payload = initiated_event
//         .payload
//         .get("Initiated")
//         .expect("Initiated event payload missing Initiated wrapper");
//     assert_eq!(
//         initiated_payload
//             .get("direction")
//             .and_then(|val| val.as_str()),
//         Some("BaseToAlpaca"),
//         "Initiated event should record BaseToAlpaca direction, got: {initiated_payload}"
//     );
//
//     // Verify the Bridged event records a nonzero amount received.
//     let bridged_event = usdc_events
//         .iter()
//         .find(|event| event.event_type == "UsdcRebalanceEvent::Bridged")
//         .expect("Missing Bridged event");
//     let bridged_payload = bridged_event
//         .payload
//         .get("Bridged")
//         .expect("Bridged event payload missing Bridged wrapper");
//     let amount_received = bridged_payload
//         .get("amount_received")
//         .and_then(|val| val.as_str())
//         .expect("Bridged event missing amount_received");
//     assert_ne!(
//         amount_received, "0",
//         "Bridged amount_received should be nonzero"
//     );
//
//     // ── Layer 3: USDC rebalance pipeline (broker state) ──────────────
//     // BaseToAlpaca conversion: USDC -> USD = sell USDCUSD.
//     let usdcusd_orders: Vec<_> = broker
//         .orders()
//         .into_iter()
//         .filter(|order| order.symbol == "USDCUSD")
//         .collect();
//     assert!(
//         !usdcusd_orders.is_empty(),
//         "Expected a USDCUSD conversion order on the broker"
//     );
//     assert!(
//         usdcusd_orders.iter().any(|order| order.side == "sell"),
//         "BaseToAlpaca conversion should be a sell (USDC -> USD), got sides: {:?}",
//         usdcusd_orders.iter().map(|o| &o.side).collect::<Vec<_>>(),
//     );
//
//     // BaseToAlpaca direction: bot deposits USDC into Alpaca wallet.
//     let wallet_transfers = broker.wallet_transfers();
//     let incoming_transfers: Vec<_> = wallet_transfers
//         .iter()
//         .filter(|transfer| transfer.direction == "INCOMING")
//         .collect();
//     assert!(
//         !incoming_transfers.is_empty(),
//         "Expected at least 1 INCOMING wallet transfer (Alpaca deposit)"
//     );
//     assert!(
//         incoming_transfers
//             .iter()
//             .all(|transfer| transfer.status == "COMPLETE"),
//         "All INCOMING wallet transfers should be COMPLETE, got: {:?}",
//         incoming_transfers
//             .iter()
//             .map(|t| &t.status)
//             .collect::<Vec<_>>(),
//     );
//
//     // BaseToAlpaca should NOT have any outgoing transfers.
//     assert!(
//         !wallet_transfers
//             .iter()
//             .any(|transfer| transfer.direction == "OUTGOING"),
//         "BaseToAlpaca should not have OUTGOING wallet transfers"
//     );
//
//     // ── Layer 3: USDC rebalance pipeline (onchain) ───────────────────
//     // BaseToAlpaca direction: USDC was withdrawn from the Raindex vault,
//     // so the balance should be less than the initial deposit. We verify
//     // the vault still has some balance (not fully drained).
//     let orderbook =
//         IOrderBookV5::IOrderBookV5Instance::new(base_chain.orderbook_addr, &base_chain.provider);
//     let vault_balance = orderbook
//         .vaultBalance2(base_chain.owner, USDC_BASE, usdc_vault_id)
//         .call()
//         .await?;
//     assert_ne!(
//         vault_balance,
//         B256::ZERO,
//         "USDC vault should still have some balance (not fully drained)"
//     );
//
//     pool.close().await;
//     bot.abort();
//     Ok(())
// }
