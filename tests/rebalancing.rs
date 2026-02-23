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
use rust_decimal_macros::dec;

use crate::services::alpaca_tokenization::REDEMPTION_WALLET;
use common::{
    ExpectedPosition, MintAssertions, assert_broker_state, assert_cqrs_state,
    assert_event_subsequence, assert_full_hedging_flow, assert_rebalancing_state,
    assert_single_clean_aggregate, build_rebalancing_ctx, build_usdc_rebalancing_ctx, connect_db,
    count_events, fetch_events_by_type, spawn_bot, wait_for_processing,
};
use services::TestInfra;
use services::alpaca_broker::AlpacaBrokerMock;
use services::base_chain::{self, IERC20, TakeDirection, USDC_BASE};
use services::cctp_attestation::CctpAttestationMock;
use services::cctp_attester;
use services::ethereum_chain;
use st0x_hedge::UsdcRebalancing;
use st0x_hedge::bindings::IOrderBookV5;

/// Equity mint triggered by offchain-heavy imbalance.
///
/// SellEquity trades cause the bot to sell equity onchain (vault depletes)
/// and buy equity offchain (broker position grows). This creates a
/// TooMuchOffchain equity imbalance that should trigger a mint operation
/// to tokenize offchain shares and deposit them into the Raindex vault.
///
/// The test deploys an ERC-4626 vault wrapping a test ERC20 so the
/// trigger's `convertToAssets()` call succeeds. Tokenization API
/// endpoints are mocked on the broker server (the conductor resolves
/// all Alpaca services from the same base URL).
///
/// We assert the full mint pipeline fires: MintRequested, MintAccepted,
/// TokensReceived, TokensWrapped, and DepositedIntoRaindex. This
/// verifies the complete flow from tokenization request through
/// ERC-4626 wrapping and Raindex vault deposit.
#[test_log::test(tokio::test)]
async fn equity_imbalance_triggers_mint() -> anyhow::Result<()> {
    let onchain_price = dec!(150.00);
    let broker_fill_price = dec!(150.00);
    let amount_per_trade = dec!(7.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

    // BEFORE bot starts: set up all orders from owner account.
    // No concurrent bot transactions yet, so no nonce collisions.
    let mut prepared_orders = Vec::new();
    for _ in 0..3 {
        prepared_orders.push(
            infra
                .base_chain
                .setup_order()
                .symbol("AAPL")
                .amount(amount_per_trade)
                .price(onchain_price)
                .direction(TakeDirection::SellEquity)
                .call()
                .await?,
        );
    }

    // Capture block AFTER setup so the bot sees the orders
    let current_block = infra.base_chain.provider.get_block_number().await?;

    let ctx = build_rebalancing_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
        UsdcRebalancing::Disabled,
        None,
    )?;
    let mut bot = spawn_bot(ctx);

    // Wait long enough for the bot's 5-second coordination phase
    // (WebSocket first-event timeout + backfill + CQRS setup) to
    // complete. With take_prepared_order, the take only has 2
    // transactions (~2s), so it would arrive during coordination
    // if we only waited 2s. Waiting 8s ensures the bot is fully
    // set up and the live event processor is running.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // AFTER bot starts: take orders from taker account only.
    // The taker has its own provider/nonces, no collision with bot.
    let mut take_results = Vec::new();
    for prepared in &prepared_orders {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    wait_for_processing(&mut bot, 80).await;

    let expected_positions = [ExpectedPosition {
        symbol: "AAPL",
        amount: dec!(22.5),
        direction: TakeDirection::SellEquity,
        onchain_price,
        broker_fill_price,
        expected_accumulated_long: dec!(0),
        expected_accumulated_short: dec!(22.5),
        expected_net: dec!(0),
    }];

    let database_url = infra.db_path.display().to_string();
    assert_broker_state(&expected_positions, &infra.broker_service);
    assert_cqrs_state(&expected_positions, take_results.len(), &database_url).await?;

    assert_rebalancing_state(
        &infra.db_path,
        3,
        Some(MintAssertions {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
        }),
    )
    .await?;

    bot.abort();
    Ok(())
}

/// Equity redemption triggered by onchain-heavy imbalance.
///
/// BuyEquity trades cause the bot to receive equity onchain (vault fills)
/// and sell equity offchain (broker position shrinks). This creates a
/// TooMuchOnchain equity imbalance that should trigger a redemption
/// operation to unwrap and transfer tokens to the redemption wallet.
///
/// Expected CQRS event flow:
/// - `EquityRedemption`: WithdrawnFromRaindex -> TokensUnwrapped
///   -> TokensSent -> Detected -> Completed
///
/// Requires: ERC-4626 vault wrapper on Anvil (for trigger ratio check),
/// Raindex vault with sufficient balance for withdrawal, and tokenization
/// API mock endpoints for redemption detection/completion polling.
#[test_log::test(tokio::test)]
async fn equity_imbalance_triggers_redemption() -> anyhow::Result<()> {
    let onchain_price = dec!(125);
    let broker_fill_price = dec!(125.55);
    let trade_amount = dec!(10);

    let infra =
        TestInfra::start(vec![("AAPL", broker_fill_price)], vec![("AAPL", dec!(20))]).await?;

    // BEFORE bot starts: set up order and deposit extra equity from owner.
    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    // Deposit extra wrapped equity into the order's input vault.
    // The VaultRegistry tracks this vault. After the take + deposit:
    //   onchain = trade_amount + 100 = ~110, offchain = -trade_amount
    //   ratio >> 0.6 threshold -> TooMuchOnchain -> Redemption
    let vault_addr = infra.equity_addresses[0].1;
    let underlying_addr = infra.equity_addresses[0].2;
    let equity_vault_id = prepared.input_vault_id;
    let extra_equity: U256 = parse_units("100", 18)?.into();
    infra
        .base_chain
        .deposit_into_raindex_vault(vault_addr, equity_vault_id, extra_equity, 18)
        .await?;

    // Capture block AFTER all owner setup so the bot sees everything
    let current_block = infra.base_chain.provider.get_block_number().await?;

    let ctx = build_rebalancing_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
        UsdcRebalancing::Disabled,
        Some(REDEMPTION_WALLET),
    )?;
    let mut bot = spawn_bot(ctx);

    // Wait long enough for the bot's 5-second coordination phase
    // (WebSocket first-event timeout + backfill + CQRS setup) to
    // complete before we submit the take.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // AFTER bot starts: take from taker account only
    let _take_result = infra.base_chain.take_prepared_order(&prepared).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Wait for: hedging + inventory poller (15s) + trigger evaluation +
    // redemption pipeline (withdraw + unwrap + send + detection + completion)
    wait_for_processing(&mut bot, 80).await;

    let expected_positions = [ExpectedPosition {
        symbol: "AAPL",
        amount: trade_amount,
        direction: TakeDirection::BuyEquity,
        onchain_price,
        broker_fill_price,
        expected_accumulated_long: dec!(10),
        expected_accumulated_short: dec!(0),
        expected_net: dec!(0),
    }];

    let database_url = infra.db_path.display().to_string();
    assert_broker_state(&expected_positions, &infra.broker_service);
    assert_cqrs_state(&expected_positions, 1, &database_url).await?;

    // ── Layer 2: Inventory snapshots ─────────────────────────────────
    let pool = connect_db(&infra.db_path).await?;

    let inventory_events = count_events(&pool, "InventorySnapshot").await?;
    assert!(
        inventory_events >= 1,
        "Expected at least 1 InventorySnapshot event, got {inventory_events}"
    );

    // ── Layer 3: Redemption pipeline (events) ────────────────────────
    let redemption_events = fetch_events_by_type(&pool, "EquityRedemption").await?;
    assert!(
        !redemption_events.is_empty(),
        "Expected at least 1 EquityRedemption event (redemption trigger fired), got 0"
    );

    assert_event_subsequence(
        &redemption_events,
        &[
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            "EquityRedemptionEvent::TokensUnwrapped",
            "EquityRedemptionEvent::TokensSent",
            "EquityRedemptionEvent::Detected",
            "EquityRedemptionEvent::Completed",
        ],
    );

    assert_single_clean_aggregate(&redemption_events, &["Failed", "Rejected"]);

    // Verify the first event's payload contains the correct symbol.
    let first_payload = &redemption_events[0].payload;
    assert_eq!(
        first_payload
            .get("WithdrawnFromRaindex")
            .and_then(|val| val.get("symbol"))
            .and_then(|val| val.as_str()),
        Some("AAPL"),
        "First redemption event should be for AAPL, got: {first_payload}"
    );

    // ── Layer 3: Redemption pipeline (onchain) ───────────────────────
    // The redemption pipeline sends underlying tokens to the redemption
    // wallet. Verify the wallet received tokens.
    let underlying_balance = IERC20::new(underlying_addr, &infra.base_chain.provider)
        .balanceOf(REDEMPTION_WALLET)
        .call()
        .await?;
    assert!(
        underlying_balance > U256::ZERO,
        "Redemption wallet should hold underlying tokens after redemption"
    );

    // ── Layer 3: Redemption pipeline (tokenization state) ────────────
    let redeem_requests: Vec<_> = infra
        .tokenization_service
        .tokenization_requests()
        .into_iter()
        .filter(|req| req.request_type == "redeem")
        .collect();
    assert!(
        !redeem_requests.is_empty(),
        "Expected at least 1 redeem tokenization request"
    );
    assert!(
        redeem_requests.iter().any(|req| req.status == "completed"),
        "Expected at least one completed redeem request, got: {:?}",
        redeem_requests
            .iter()
            .map(|req| &req.status)
            .collect::<Vec<_>>(),
    );

    // ── Layer 3: Redemption pipeline (CQRS projection) ──────────────
    // The equity_redemption_view projection stores the aggregate state.
    // By this point the pipeline should have reached Completed.
    let (payload_json,): (String,) =
        sqlx::query_as("SELECT payload FROM equity_redemption_view LIMIT 1")
            .fetch_one(&pool)
            .await?;
    let payload: serde_json::Value = serde_json::from_str(&payload_json)?;
    // Projection stores Lifecycle<EquityRedemption>, serialized as
    // {"Live": {"Completed": {"symbol": "AAPL", ...}}}
    let live = payload
        .get("Live")
        .unwrap_or_else(|| panic!("equity_redemption_view payload should be Live, got: {payload}"));
    let completed = live.get("Completed").unwrap_or_else(|| {
        panic!("equity_redemption_view should be in Completed state, got: {live}")
    });
    assert_eq!(
        completed.get("symbol").and_then(|val| val.as_str()),
        Some("AAPL"),
        "Completed redemption should be for AAPL, got: {completed}"
    );

    pool.close().await;
    bot.abort();
    Ok(())
}
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
//     let broker = AlpacaBrokerMock::start().call().await;
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
//     let mut bot = spawn_bot(ctx);
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
//     wait_for_processing(&mut bot, 60).await;
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
//     let broker = AlpacaBrokerMock::start().call().await;
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
//     let mut bot = spawn_bot(ctx);
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
//     wait_for_processing(&mut bot, 60).await;
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
