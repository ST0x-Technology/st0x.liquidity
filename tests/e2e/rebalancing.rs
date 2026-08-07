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

pub(crate) mod assertions;

use alloy::primitives::{Address, B256, Signature, TxHash};
use alloy::providers::ext::AnvilApi as _;
use alloy::sol_types::SolCall;
use rain_math_float::Float;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use st0x_finance::{FractionalShares, Positive, Usd};
use st0x_float_macro::float;
use st0x_hedge::ImbalanceThreshold;
use st0x_hedge::OperationMode;
use st0x_hedge::bindings::{IRaindexV6, IST0xOrchestratorV1};
use st0x_hedge::cli::seed_mint_at_tokens_wrapped_for_test;

use self::assertions::*;
use crate::assert::{StoredEvent, assert_single_clean_aggregate};

fn issuer_request_id(label: &str) -> String {
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, label.as_bytes()).to_string()
}

fn aggregate_id_for_event(events: &[crate::assert::StoredEvent], event_type: &str) -> String {
    events
        .iter()
        .find(|event| event.event_type == event_type)
        .unwrap_or_else(|| panic!("Missing event type {event_type}"))
        .aggregate_id
        .clone()
}

fn count_tokenization_requests(
    requests: &[st0x_hedge::mock_api::MockTokenizationRequestSnapshot],
    request_type: TokenizationRequestType,
    symbol: &str,
    status: st0x_hedge::mock_api::TokenizationStatus,
) -> usize {
    requests
        .iter()
        .filter(|request| {
            request.request_type == request_type
                && request.symbol == symbol
                && request.status == status
        })
        .count()
}

#[derive(Debug, sqlx::FromRow)]
struct StoredSnapshot {
    last_sequence: i64,
    payload: String,
}

/// Verifies the recorded facts of `BotGasReceiptCostEvent::Recorded` events
/// on Base, not just that the expected number of events landed: each
/// expected `operation_category` (e.g. `"wrap"`, `"vault_deposit"`) must
/// appear with `chain == "base"`, a well-formed non-zero `tx_hash`, and a
/// strictly positive `usd_cost` -- otherwise the ledger could silently
/// record a zero-value or wrong-chain cost fact while the raw event count
/// still passed.
async fn assert_bot_gas_receipt_costs_recorded(
    db_path: &std::path::Path,
    expected_categories: &[&str],
) -> anyhow::Result<()> {
    let pool = crate::poll::connect_db(db_path).await?;
    let events: Vec<StoredEvent> = crate::poll::fetch_events_by_type(&pool, "BotGasReceiptCost")
        .await?
        .into_iter()
        .filter(|event| event.event_type == "BotGasReceiptCostEvent::Recorded")
        .collect();
    pool.close().await;

    assert_eq!(
        events.len(),
        expected_categories.len(),
        "expected exactly {} recorded bot-gas cost facts, got: {events:?}",
        expected_categories.len()
    );

    let mut recorded_categories: Vec<String> = Vec::new();
    for event in &events {
        let cost = &event.payload["Recorded"]["cost"];

        assert_eq!(
            cost["chain"].as_str(),
            Some("base"),
            "bot-gas cost fact must be recorded on Base, got: {cost:?}"
        );

        let tx_hash = cost["tx_hash"]
            .as_str()
            .unwrap_or_else(|| panic!("bot-gas cost fact missing tx_hash: {cost:?}"));
        assert!(
            tx_hash.starts_with("0x") && tx_hash != format!("0x{}", "0".repeat(64)),
            "bot-gas cost fact must carry a well-formed non-zero tx_hash, got: {tx_hash}"
        );

        let usd_cost: f64 = cost["usd_cost"]
            .as_str()
            .unwrap_or_else(|| panic!("bot-gas cost fact missing usd_cost: {cost:?}"))
            .parse()
            .unwrap_or_else(|error| panic!("usd_cost must parse as a decimal: {error}"));
        assert!(
            usd_cost > 0.0,
            "bot-gas cost fact's usd_cost must be strictly positive, got: {usd_cost}"
        );

        let category = cost["operation_category"]
            .as_str()
            .unwrap_or_else(|| panic!("bot-gas cost fact missing operation_category: {cost:?}"))
            .to_owned();
        recorded_categories.push(category);
    }

    recorded_categories.sort_unstable();
    let mut expected_sorted: Vec<String> = expected_categories
        .iter()
        .map(|category| (*category).to_owned())
        .collect();
    expected_sorted.sort_unstable();
    assert_eq!(
        recorded_categories, expected_sorted,
        "unexpected set of recorded bot-gas operation categories"
    );

    Ok(())
}

/// USDC-path counterpart to `assert_bot_gas_receipt_costs_recorded`: unlike
/// every equity/vault path (Base-only), a USDC AlpacaToBase/BaseToAlpaca
/// rebalance is the only flow that exercises `BotGasChain::Ethereum` (the
/// CCTP burn and the Alpaca-deposit wallet transfer), so the reorg-safe
/// receipt/block/valuation pipeline needs proving on BOTH chains against
/// real anvil providers, not just via `Asserter` unit tests. Asserts at
/// least one well-formed, positively-valued recorded cost fact per chain,
/// rather than pinning an exact category set: which categories land on which
/// chain depends on whether CCTP adopts an already-relayed mint, so a strict
/// count would be flaky.
async fn assert_bot_gas_receipt_costs_recorded_on_both_chains(
    db_path: &std::path::Path,
) -> anyhow::Result<()> {
    let pool = crate::poll::connect_db(db_path).await?;
    let events: Vec<StoredEvent> = crate::poll::fetch_events_by_type(&pool, "BotGasReceiptCost")
        .await?
        .into_iter()
        .filter(|event| event.event_type == "BotGasReceiptCostEvent::Recorded")
        .collect();
    pool.close().await;

    assert!(
        !events.is_empty(),
        "expected at least one recorded bot-gas cost fact for the USDC rebalance"
    );

    let mut chains_seen: Vec<String> = Vec::new();
    for event in &events {
        let cost = &event.payload["Recorded"]["cost"];

        let chain = cost["chain"]
            .as_str()
            .unwrap_or_else(|| panic!("bot-gas cost fact missing chain: {cost:?}"))
            .to_owned();

        let tx_hash = cost["tx_hash"]
            .as_str()
            .unwrap_or_else(|| panic!("bot-gas cost fact missing tx_hash: {cost:?}"));
        assert!(
            tx_hash.starts_with("0x") && tx_hash != format!("0x{}", "0".repeat(64)),
            "bot-gas cost fact must carry a well-formed non-zero tx_hash, got: {tx_hash}"
        );

        let usd_cost: f64 = cost["usd_cost"]
            .as_str()
            .unwrap_or_else(|| panic!("bot-gas cost fact missing usd_cost: {cost:?}"))
            .parse()
            .unwrap_or_else(|error| panic!("usd_cost must parse as a decimal: {error}"));
        assert!(
            usd_cost > 0.0,
            "bot-gas cost fact's usd_cost must be strictly positive, got: {usd_cost}"
        );

        chains_seen.push(chain);
    }

    for expected_chain in ["base", "ethereum"] {
        assert!(
            chains_seen.iter().any(|chain| chain == expected_chain),
            "expected at least one recorded bot-gas cost fact on chain \
             {expected_chain:?}, got chains: {chains_seen:?}"
        );
    }

    Ok(())
}

async fn latest_inventory_snapshot(
    pool: &sqlx::SqlitePool,
) -> anyhow::Result<Option<StoredSnapshot>> {
    let snapshot = sqlx::query_as::<_, StoredSnapshot>(
        "SELECT last_sequence, payload \
         FROM snapshots \
         WHERE aggregate_type = 'InventorySnapshot' \
         ORDER BY last_sequence DESC \
         LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;

    Ok(snapshot)
}

fn snapshot_has_inflight_mint(snapshot: &StoredSnapshot, symbol: &str) -> anyhow::Result<bool> {
    let payload: serde_json::Value = serde_json::from_str(&snapshot.payload)?;
    let live = payload.get("Live").unwrap_or(&payload);
    let has_symbol = live
        .get("inflight_mints")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|mints| mints.contains_key(symbol));

    Ok(has_symbol)
}

fn snapshot_inflight_mint(
    snapshot: &StoredSnapshot,
    symbol: &str,
) -> anyhow::Result<Option<FractionalShares>> {
    let payload: serde_json::Value = serde_json::from_str(&snapshot.payload)?;
    let live = payload.get("Live").unwrap_or(&payload);
    let quantity = live
        .get("inflight_mints")
        .and_then(serde_json::Value::as_object)
        .and_then(|mints| mints.get(symbol))
        .and_then(serde_json::Value::as_str)
        .map(str::parse::<FractionalShares>)
        .transpose()?;

    Ok(quantity)
}

fn single_pending_request_quantity(
    requests: &[st0x_hedge::mock_api::MockTokenizationRequestSnapshot],
    request_type: TokenizationRequestType,
    symbol: &str,
) -> FractionalShares {
    let quantities: Vec<_> = requests
        .iter()
        .filter(|request| {
            request.request_type == request_type
                && request.symbol == symbol
                && request.status == st0x_hedge::mock_api::TokenizationStatus::Pending
        })
        .map(|request| FractionalShares::new(request.quantity))
        .collect();

    assert_eq!(
        quantities.len(),
        1,
        "Expected exactly one pending {request_type:?} request for {symbol}, got {quantities:?}",
    );

    quantities[0]
}

fn inflight_mint_quantities(
    events: &[crate::assert::StoredEvent],
    symbol: &str,
) -> anyhow::Result<Vec<FractionalShares>> {
    events
        .iter()
        .filter(|event| event.event_type == "InventorySnapshotEvent::InflightEquity")
        .filter_map(|event| {
            event
                .payload
                .get("InflightEquity")?
                .get("mints")?
                .get(symbol)?
                .as_str()
        })
        .map(|quantity| quantity.parse::<FractionalShares>().map_err(Into::into))
        .collect()
}

async fn poll_for_inflight_mint_event_count_after(
    bot: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    symbol: &str,
    expected_quantity: FractionalShares,
    previous_count: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("new InflightEquity event for {symbol} quantity {expected_quantity}");

    loop {
        sleep_or_crash(bot, &context).await;

        if let Ok(pool) = connect_db(db_path).await {
            let events = fetch_events_by_type(&pool, "InventorySnapshot").await?;
            pool.close().await;

            let matching_count = inflight_mint_quantities(&events, symbol)?
                .into_iter()
                .filter(|quantity| *quantity == expected_quantity)
                .count();

            if matching_count > previous_count {
                return Ok(());
            }
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context}",
        );
    }
}

async fn poll_for_inflight_mint_snapshot(
    bot: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    db_path: &std::path::Path,
    symbol: &str,
    timeout: Duration,
) -> anyhow::Result<StoredSnapshot> {
    let deadline = tokio::time::Instant::now() + timeout;
    let context = format!("InventorySnapshot snapshot with inflight mint for {symbol}");

    loop {
        sleep_or_crash(bot, &context).await;

        if let Ok(pool) = connect_db(db_path).await {
            let snapshot = latest_inventory_snapshot(&pool).await?;
            pool.close().await;

            if let Some(snapshot) = snapshot
                && snapshot_has_inflight_mint(&snapshot, symbol)?
            {
                return Ok(snapshot);
            }
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out after {timeout:?} waiting for {context}",
        );
    }
}

/// Control test: direct high-precision sell-side Raindex prices should not
/// break equity rebalancing. This avoids buy-side reciprocal math and verifies
/// that direct decimal price literals do not block the mint pipeline.

#[test_log::test(tokio::test)]
async fn equity_mint_handles_direct_high_precision_sell_price() -> anyhow::Result<()> {
    let onchain_price = float!(&"112.50000000000000000000000002".to_string());
    let broker_fill_price = float!(110);
    let amount_per_trade = float!(7.5);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(7.5))],
    )
    .await?;

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

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared_orders[0].input_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared_orders[0].output_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(10)).await;

    // Take all orders without delay so the inventory poller sees the
    // full imbalance in one pass and triggers a single mint cycle.
    let mut take_results = Vec::new();
    for prepared in &prepared_orders {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
    }

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "TokenizedEquityMintEvent::DepositedIntoRaindex",
        1,
        Duration::from_secs(120),
    )
    .await;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(float!(22.5))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(0))
        .expected_accumulated_short(float!(22.5))
        .expected_net(float!(0))
        .build()];

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&take_results)
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .rebalance_type(EquityRebalanceType::Mint {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
        })
        .call()
        .await?;

    bot.abort();
    Ok(())
}

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
    let onchain_price = float!(150.00);
    let broker_fill_price = float!(148.00);
    let amount_per_trade = float!(7.5);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(30))],
    )
    .await?;
    let wrapped_token = infra.equity_addresses[0].1;
    let unwrapped_token = infra.equity_addresses[0].2;

    // Keep the owner's direct wrapped and unwrapped balances distinct so the
    // BaseWalletUnwrappedEquity snapshot assertion proves we polled the
    // unwrapped token.
    let balance_skew: U256 = parse_units("1", 18)?.into();
    crate::base_chain::IERC20::new(unwrapped_token, &infra.base_chain.provider)
        .transfer(infra.base_chain.taker, balance_skew)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Set up orders before bot starts (owner nonces, no collision).
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

    let owner_unwrapped_balance_before_takes =
        crate::base_chain::IERC20::new(unwrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;
    let owner_wrapped_balance_before_takes =
        crate::base_chain::IERC20::new(wrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;
    let owner_unwrapped_shares_before_takes =
        st0x_execution::FractionalShares::from_u256_18_decimals(
            owner_unwrapped_balance_before_takes,
        )?;
    let owner_wrapped_shares_before_takes =
        st0x_execution::FractionalShares::from_u256_18_decimals(
            owner_wrapped_balance_before_takes,
        )?;

    // Capture block AFTER setup so the bot sees the orders
    let current_block = infra.base_chain.provider.get_block_number().await?;

    let cash_vault_id = prepared_orders[0].input_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared_orders[0].output_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;
    let mut bot = spawn_bot(ctx);

    assert_initial_base_wallet_unwrapped_and_wrapped_equity_snapshot(
        &mut bot,
        &infra.db_path,
        "AAPL",
        owner_unwrapped_shares_before_takes,
        owner_wrapped_shares_before_takes,
    )
    .await?;

    // Take all orders without delay so the inventory poller sees the
    // full imbalance in one pass and triggers a single mint cycle.
    let mut take_results = Vec::new();
    for prepared in &prepared_orders {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
    }

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "TokenizedEquityMintEvent::DepositedIntoRaindex",
        1,
        Duration::from_secs(120),
    )
    .await;

    // ADR 0017: the mint's wrap and vault-deposit confirmations
    // each enqueue a one-shot `RecordBotGasReceiptCost` job; wait for both to
    // drain into recorded cost facts before asserting the rest of the flow.
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "BotGasReceiptCostEvent::Recorded",
        2,
        Duration::from_secs(120),
    )
    .await;

    assert_bot_gas_receipt_costs_recorded(&infra.db_path, &["wrap", "vault_deposit"]).await?;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(float!(22.5))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(0))
        .expected_accumulated_short(float!(22.5))
        .expected_net(float!(0))
        .build()];

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&take_results)
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .rebalance_type(EquityRebalanceType::Mint {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
        })
        .call()
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
    let onchain_price = float!(112.50);
    let broker_fill_price = float!(113.60);
    let trade_amount = float!(12.5);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(20))],
    )
    .await?;

    // Set up order and deposit extra equity before bot starts.
    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    // Extra equity keeps the onchain/offchain ratio balanced (0.5) before the
    // take so the trigger doesn't fire at startup. After the BuyEquity fill
    // adds 12.5 shares onchain and the hedge sell removes 12.5 offchain, the
    // ratio jumps to ~0.81 > 0.6 threshold -> Redemption.
    let vault_addr = infra.equity_addresses[0].1;
    let underlying_addr = infra.equity_addresses[0].2;
    let redemption_wallet_balance_before =
        crate::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;
    let equity_vault_id = prepared.input_vault_id;
    let extra_equity: U256 = parse_units("20", 18)?.into();
    infra
        .base_chain
        .deposit_into_raindex_vault(vault_addr, equity_vault_id, extra_equity, 18)
        .await?;

    // Capture block AFTER all owner setup so the bot sees everything
    let current_block = infra.base_chain.provider.get_block_number().await?;

    let cash_vault_id = prepared.output_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared.input_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let mut bot = spawn_bot(ctx);

    // Wait for bot's coordination phase before submitting takes.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Take from taker account.
    let take_result = infra.base_chain.take_prepared_order(&prepared).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "EquityRedemptionEvent::Completed",
        1,
        Duration::from_secs(120),
    )
    .await;
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(trade_amount)
        .direction(TakeDirection::BuyEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(12.5))
        .expected_accumulated_short(float!(0))
        .expected_net(float!(0))
        .build()];

    let redemption_wallet_balance_after =
        crate::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&[take_result])
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .rebalance_type(EquityRebalanceType::Redeem {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
            redemption_wallet_balance_before,
            redemption_wallet_balance_after,
        })
        .call()
        .await?;

    bot.abort();
    Ok(())
}

/// Regression repro for buy-side reciprocal precision dust in the redemption
/// path using the normal harness behavior (`inv(price)` in Rainlang).
///
/// This test intentionally uses a BuyEquity price whose reciprocal is
/// non-terminating (`1 / 115`) to exercise the precision-dust path without
/// overriding the generated `ioRatio`. Correct behavior is still that
/// redemption completes successfully.
#[test_log::test(tokio::test)]
async fn equity_redemption_buy_inv_repeating_reciprocal_regression() -> anyhow::Result<()> {
    let onchain_price = float!(115);
    let broker_fill_price = float!(113.57);
    let trade_amount = float!(12.5);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(20))],
    )
    .await?;

    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    let vault_addr = infra.equity_addresses[0].1;
    let underlying_addr = infra.equity_addresses[0].2;
    let redemption_wallet_balance_before =
        crate::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;
    // Extra equity keeps the onchain/offchain ratio balanced (0.5) before the
    // take so the trigger doesn't fire at startup. After the BuyEquity fill
    // adds shares onchain and the hedge sell removes shares offchain, the
    // ratio exceeds the 0.6 threshold -> Redemption.
    let equity_vault_id = prepared.input_vault_id;
    let extra_equity: U256 = parse_units("20", 18)?.into();
    infra
        .base_chain
        .deposit_into_raindex_vault(vault_addr, equity_vault_id, extra_equity, 18)
        .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    let cash_vault_id = prepared.output_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared.input_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(10)).await;

    let take_result = infra.base_chain.take_prepared_order(&prepared).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "EquityRedemptionEvent::Completed",
        1,
        Duration::from_secs(120),
    )
    .await;

    // Redemption is driven by the inventory poller observing the
    // post-take vault balance, so it can complete before the bot has
    // processed the TakeOrderV3 event and placed its hedge. Wait for
    // the position to settle so broker assertions are deterministic.
    poll_for_hedge_completion(&mut bot, &infra.db_path, "AAPL", Duration::from_secs(30)).await;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(trade_amount)
        .direction(TakeDirection::BuyEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(trade_amount)
        .expected_accumulated_short(float!(0))
        .expected_net(float!(0))
        .build()];

    let redemption_wallet_balance_after =
        crate::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&[take_result])
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .rebalance_type(EquityRebalanceType::Redeem {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
            redemption_wallet_balance_before,
            redemption_wallet_balance_after,
        })
        .call()
        .await?;

    bot.abort();
    Ok(())
}

/// Diagnostic repro using the historical harness behavior: precompute the
/// buy-side reciprocal in `Float` and inject it as a Rainlang literal.
///
/// This uses the same rebalancing redemption setup as the `inv(price)` repro
/// but replaces the generated Rainlang expression with a direct reciprocal
/// string, matching the old harness path.
#[test_log::test(tokio::test)]
async fn equity_redemption_buy_literal_reciprocal_regression() -> anyhow::Result<()> {
    let onchain_price = float!(112);
    let broker_fill_price = float!(113.57);
    let trade_amount = float!(12.5);

    let reciprocal_literal = (float!(1.0) / onchain_price)
        .unwrap()
        .format_with_scientific(false)
        .unwrap();
    let usdc_total = (trade_amount * onchain_price).unwrap();
    let usdc_total_rounded = crate::assert::round_float(usdc_total, 6)?;
    let usdc_total_str = usdc_total_rounded
        .format_with_scientific(false)
        .map_err(|err| anyhow::anyhow!("format failed: {err:?}"))?;
    let max_amount_base: U256 = parse_units(&usdc_total_str, 6)?.into();
    let rain_expression_override = format!("_ _: {max_amount_base} {reciprocal_literal};:;");

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(20))],
    )
    .await?;

    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .rain_expression_override(rain_expression_override)
        .call()
        .await?;

    let vault_addr = infra.equity_addresses[0].1;
    let underlying_addr = infra.equity_addresses[0].2;
    let redemption_wallet_balance_before =
        crate::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;
    // Extra equity keeps the onchain/offchain ratio balanced (0.5) before the
    // take so the trigger doesn't fire at startup.
    let equity_vault_id = prepared.input_vault_id;
    let extra_equity: U256 = parse_units("20", 18)?.into();
    infra
        .base_chain
        .deposit_into_raindex_vault(vault_addr, equity_vault_id, extra_equity, 18)
        .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    let cash_vault_id = prepared.output_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared.input_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(10)).await;

    let take_result = infra.base_chain.take_prepared_order(&prepared).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "EquityRedemptionEvent::Completed",
        1,
        Duration::from_secs(120),
    )
    .await;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(trade_amount)
        .direction(TakeDirection::BuyEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(trade_amount)
        .expected_accumulated_short(float!(0))
        .expected_net(float!(0))
        .build()];

    let redemption_wallet_balance_after =
        crate::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&[take_result])
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .rebalance_type(EquityRebalanceType::Redeem {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
            redemption_wallet_balance_before,
            redemption_wallet_balance_after,
        })
        .call()
        .await?;

    bot.abort();
    Ok(())
}

/// USDC rebalancing from Alpaca (offchain) to Base (onchain).
///
/// BuyEquity trades cause the bot to spend USDC onchain (paying for equity)
/// and receive USDC offchain (from selling equity on the broker). This
/// creates a TooMuchOffchain USDC imbalance that should trigger a transfer
/// from Alpaca to Base via CCTP bridge.
///
/// Expected CQRS event flow:
/// - `UsdcRebalance`: ConversionInitiated -> ConversionConfirmed
///   -> Initiated -> WithdrawalConfirmed -> BridgingSubmitting
///   -> PendingBurnRecorded -> BridgingInitiated -> BridgeAttestationReceived
///   -> Bridged -> DepositInitiated -> DepositConfirmed
///
/// Infrastructure: Deploys CCTP contracts fresh on two plain Anvil chains
/// (no mainnet fork required). A background watcher auto-signs attestations
/// for `MessageSent` events using a test attester key.
#[test_log::test(tokio::test)]
async fn usdc_imbalance_triggers_alpaca_to_base() -> anyhow::Result<()> {
    let onchain_price = float!(158.39);
    let broker_fill_price = float!(155.00);
    let amount_per_trade = float!(7.5);
    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(30))],
    )
    .await?;
    let cctp = CctpInfra::start(&infra).await?;
    let eth_balance_provider = alloy::providers::ProviderBuilder::new()
        .connect(&cctp.ethereum_endpoint)
        .await?;

    // USDC vault sized so the onchain/offchain ratio stays above 0.4
    // before takes (70k / 170k = 0.41) but drops below after BuyEquity
    // drains ~3.6k USDC (66.4k / 170k = 0.39 < 0.4 threshold).
    let usdc_amount: U256 = parse_units("70000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

    // Set up BuyEquity orders BEFORE bot starts
    let mut prepared_orders = Vec::new();
    for _ in 0..3 {
        prepared_orders.push(
            infra
                .base_chain
                .setup_order()
                .symbol("AAPL")
                .amount(amount_per_trade)
                .price(onchain_price)
                .direction(TakeDirection::BuyEquity)
                .call()
                .await?,
        );
    }

    // Drain residual owner-wallet USDC after vault + order setup so wallet
    // polling sees the production-like baseline (0). Without this, the
    // `BaseWalletUsdc` snapshot would carry the leftover from
    // `deploy_usdc_at`'s 1B-USDC seeding and trip the wallet-inflight
    // suppression even though no transfer is in flight.
    infra.base_chain.set_owner_usdc_balance(U256::ZERO).await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;

    let ctx = build_usdc_rebalancing_ctx()
        .base_chain(&infra.base_chain)
        .ethereum_endpoint(&cctp.ethereum_endpoint)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .usdc_vault_id(usdc_vault_id)
        .cctp(cctp.cctp_overrides())
        .wrapped_equity_recovery(OperationMode::Disabled)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(10)).await;

    // AFTER bot starts: take orders from taker account
    let mut take_results = Vec::new();
    for (index, prepared) in prepared_orders.iter().enumerate() {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
        if index + 1 < prepared_orders.len() {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    // The BuyEquity orders use their own USDC output vaults (not the
    // pre-funded usdc_vault_id), so takes don't modify the pre-funded
    // vault. The chain query is safe here: the AlpacaToBase deposit
    // flow is long enough (CCTP bridge + attestation) that the vault
    // balance won't change before we snapshot.
    let usdc_vault_balance_before_rebalance =
        st0x_hedge::bindings::IRaindexV6::IRaindexV6Instance::new(
            infra.base_chain.orderbook,
            &infra.base_chain.provider,
        )
        .vaultBalance2(
            infra.base_chain.owner,
            crate::base_chain::USDC_BASE,
            usdc_vault_id,
        )
        .call()
        .await?;
    let ethereum_usdc_balance_before_rebalance =
        crate::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "UsdcRebalanceEvent::DepositConfirmed",
        1,
        Duration::from_secs(120),
    )
    .await;
    let ethereum_usdc_balance_after_rebalance =
        crate::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    let total_amount = (amount_per_trade * float!(3)).unwrap();

    poll_for_hedge_completion(&mut bot, &infra.db_path, "AAPL", Duration::from_secs(30)).await;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(total_amount)
        .direction(TakeDirection::BuyEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(total_amount)
        .expected_accumulated_short(float!(0))
        .expected_net(float!(0))
        .build()];

    assert_usdc_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&take_results)
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .attestation(&infra.attestation_service)
        .db_path(&infra.db_path)
        .usdc_vault_id(usdc_vault_id)
        .usdc_vault_balance_before_rebalance(usdc_vault_balance_before_rebalance)
        .ethereum_usdc_balance_before_rebalance(ethereum_usdc_balance_before_rebalance)
        .ethereum_usdc_balance_after_rebalance(ethereum_usdc_balance_after_rebalance)
        .rebalance_type(UsdcRebalanceType::AlpacaToBase)
        .call()
        .await?;

    assert_ethereum_usdc_event_exists(&infra.db_path).await?;
    assert_base_wallet_usdc_event_exists(&infra.db_path).await?;

    // ADR 0017: each of the CCTP burn/mint and USDC wallet-transfer
    // confirmations enqueues a one-shot `RecordBotGasReceiptCost` job; wait
    // for both to drain into recorded cost facts before asserting on them --
    // otherwise a single unsynchronized read below only proves the jobs were
    // enqueued (via `DepositConfirmed` above), not that they completed.
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "BotGasReceiptCostEvent::Recorded",
        2,
        Duration::from_secs(120),
    )
    .await;

    assert_bot_gas_receipt_costs_recorded_on_both_chains(&infra.db_path).await?;

    bot.abort();
    Ok(())
}

/// USDC rebalancing from Base (onchain) to Alpaca (offchain).
///
/// SellEquity trades cause the bot to receive USDC onchain (from selling
/// equity) and spend USDC offchain (buying equity on the broker to hedge).
/// This creates a TooMuchOnchain USDC imbalance that should trigger a
/// transfer from Base to Alpaca via CCTP bridge.
///
/// Expected CQRS event flow:
/// - `UsdcRebalance`: WithdrawalSubmitting -> Initiated -> WithdrawalConfirmed
///   -> BridgingSubmitting -> PendingBurnRecorded -> BridgingInitiated
///   -> BridgeAttestationReceived -> Bridged -> DepositInitiated
///   -> DepositConfirmed -> ConversionInitiated -> ConversionConfirmed
///
/// Infrastructure: Same as `usdc_imbalance_triggers_alpaca_to_base` but the
/// Raindex USDC vault is pre-funded so the bot can withdraw from it.
#[test_log::test(tokio::test)]
async fn usdc_imbalance_triggers_base_to_alpaca() -> anyhow::Result<()> {
    let onchain_price = float!(158.39);
    let broker_fill_price = float!(155.00);
    let amount_per_trade = float!(7.5);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(7.5))],
    )
    .await?;
    let cctp = CctpInfra::start(&infra).await?;

    // Pre-fund the USDC vault so the ratio stays below 0.6 before
    // takes (145k / 245k = 0.59) but exceeds after SellEquity fills
    // add ~3.6k USDC (148.6k / 245k = 0.61 > 0.6 threshold).
    let usdc_amount: U256 = parse_units("145000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

    // Drain residual owner-wallet USDC so wallet polling sees the
    // production-like baseline (0). Without this, `BaseWalletUsdc`
    // snapshots would carry the leftover from `deploy_usdc_at`'s
    // 1B-USDC seeding and trip the wallet-inflight suppression even
    // though no transfer is in flight.
    infra.base_chain.set_owner_usdc_balance(U256::ZERO).await?;

    // Set up SellEquity orders BEFORE bot starts.
    // All orders share the same USDC input vault as the pre-funded vault
    // so the VaultRegistry discovers the pre-funded vault via TakeOrderV3,
    // and the inventory poller reads the correct ($100k) balance.
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
                .usdc_vault_id(usdc_vault_id)
                .call()
                .await?,
        );
    }

    // Start deposit watcher on Ethereum chain. The BaseToAlpaca flow
    // mints USDC via CCTP to the bot's Ethereum wallet (same key as the
    // Base chain owner). The watcher detects the Transfer event and
    // registers an INCOMING wallet transfer in mock state so the bot's
    // deposit polling succeeds.
    let eth_deposit_provider = alloy::providers::ProviderBuilder::new()
        .connect(&cctp.ethereum_endpoint)
        .await?;
    let eth_balance_provider = alloy::providers::ProviderBuilder::new()
        .connect(&cctp.ethereum_endpoint)
        .await?;
    let _deposit_watcher = infra
        .broker_service
        .start_deposit_watcher(eth_deposit_provider, USDC_ETHEREUM, infra.base_chain.owner)
        .await?;

    // Snapshot Ethereum USDC balance before the bot starts. Once the
    // bot detects the USDC surplus from takes it may start the CCTP
    // bridge immediately, minting USDC on Ethereum. Querying after
    // takes is racey — the "before" snapshot would capture a
    // partially-bridged state and double-count the mint.
    let ethereum_usdc_balance_before_rebalance =
        crate::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let reserved = Positive::new(Usd::new(float!(3000)))?;
    let ctx = build_usdc_rebalancing_ctx()
        .base_chain(&infra.base_chain)
        .ethereum_endpoint(&cctp.ethereum_endpoint)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .usdc_vault_id(usdc_vault_id)
        .cctp(cctp.cctp_overrides())
        .reserved(reserved)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(10)).await;

    // AFTER bot starts: take orders from taker account
    let mut take_results = Vec::new();
    for (index, prepared) in prepared_orders.iter().enumerate() {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
        if index + 1 < prepared_orders.len() {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    // Compute the pre-trade USDC vault balance from take event data.
    // The live chain query (`vaultBalance2`) is racey because the
    // inventory poller may detect the USDC surplus and trigger a
    // vault withdrawal before the test re-queries the chain.
    let usdc_vault_balance_before_rebalance = take_results
        .iter()
        .find(|result| {
            result.input_token == crate::base_chain::USDC_BASE
                && result.input_vault_id == usdc_vault_id
        })
        .expect("at least one take should touch the USDC input vault")
        .input_vault_balance_before_take;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "UsdcRebalanceEvent::ConversionConfirmed",
        1,
        Duration::from_secs(120),
    )
    .await;
    let ethereum_usdc_balance_after_rebalance =
        crate::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    let total_amount = (amount_per_trade * float!(3)).unwrap();

    // CheckPositions batches hedges across scan cycles. The
    // last onchain fill may arrive during the USDC rebalance, so
    // its hedge completes after the rebalance event. Wait for all
    // hedges to fill by polling until the position net reaches zero.
    poll_for_hedge_completion(&mut bot, &infra.db_path, "AAPL", Duration::from_secs(30)).await;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(total_amount)
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(0))
        .expected_accumulated_short(total_amount)
        .expected_net(float!(0))
        .build()];

    assert_usdc_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&take_results)
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .attestation(&infra.attestation_service)
        .db_path(&infra.db_path)
        .usdc_vault_id(usdc_vault_id)
        .usdc_vault_balance_before_rebalance(usdc_vault_balance_before_rebalance)
        .ethereum_usdc_balance_before_rebalance(ethereum_usdc_balance_before_rebalance)
        .ethereum_usdc_balance_after_rebalance(ethereum_usdc_balance_after_rebalance)
        .rebalance_type(UsdcRebalanceType::BaseToAlpaca)
        .call()
        .await?;

    assert_offchain_usd_reflects_reserve(&mut bot, &infra.db_path, &infra.broker_service, reserved)
        .await?;

    bot.abort();
    Ok(())
}

/// Redemption rejected by Alpaca preserves inflight via sticky marker.
///
/// When a redemption request is rejected by Alpaca, the bot marks the
/// `(symbol, MarketMaking)` pair as sticky inflight. Subsequent
/// `InflightEquity` polls that find no pending requests for that symbol
/// must NOT zero the inflight -- the tokens are physically in Alpaca's
/// redemption wallet with no snapshot source tracking them.
///
/// Without sticky inflight, the system would lose track of those assets
/// entirely and make incorrect rebalancing decisions.
#[test_log::test(tokio::test)]
async fn redemption_rejected_preserves_inflight_via_sticky() -> anyhow::Result<()> {
    let onchain_price = float!("112.50");
    let broker_fill_price = float!("113.60");
    let trade_amount = float!("12.5");

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!("20"))],
    )
    .await?;

    // Configure the mock to reject redemption requests instead of completing.
    infra
        .tokenization_service
        .set_redemption_outcome(RedemptionOutcome::Reject);

    // Set up a BuyEquity order that will create TooMuchOnchain imbalance.
    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    // Deposit extra equity to keep the ratio balanced before the take.
    // After BuyEquity fill adds 12.5 onchain and hedge sell removes 12.5
    // offchain, the ratio exceeds threshold -> triggers redemption.
    let vault_addr = infra.equity_addresses[0].1;
    let equity_vault_id = prepared.input_vault_id;
    let extra_equity: U256 = parse_units("20", 18)?.into();
    infra
        .base_chain
        .deposit_into_raindex_vault(vault_addr, equity_vault_id, extra_equity, 18)
        .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared.output_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared.input_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

    // Take from taker account to create the imbalance.
    let _take_result = infra.base_chain.take_prepared_order(&prepared).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Wait for the redemption to reach RedemptionRejected terminal state.
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "EquityRedemptionEvent::RedemptionRejected",
        1,
        Duration::from_secs(120),
    )
    .await;

    // Wait for 2+ full poll cycles (15s each) after the rejection.
    //
    // We intentionally do NOT wait for new InflightEquity events here: the
    // snapshot aggregate deduplicates — it only emits an event when
    // inflight_redemptions changes. The sticky mechanism keeps the rejected
    // request in the inflight list, so the aggregate sees the same
    // {AAPL: 6.25} every poll and emits nothing new. Waiting on a count
    // increase would time out. A plain sleep is the correct approach: we
    // need enough time for any duplicate redemption to surface if sticky
    // fails, not evidence that inflight changed.
    tokio::time::sleep(Duration::from_secs(50)).await;

    let pool = connect_db(&infra.db_path).await?;

    // Verify the redemption event sequence includes RedemptionRejected.
    let redeem_events = fetch_events_by_type(&pool, "EquityRedemption").await?;
    assert_event_subsequence(
        &redeem_events,
        &[
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            "EquityRedemptionEvent::UnwrapPending",
            "EquityRedemptionEvent::UnwrapSubmitted",
            "EquityRedemptionEvent::TokensUnwrapped",
            "EquityRedemptionEvent::SendPending",
            "EquityRedemptionEvent::TokensSent",
            "EquityRedemptionEvent::Detected",
            "EquityRedemptionEvent::RedemptionRejected",
        ],
    );

    // The critical assertion: no second WithdrawnFromRaindex event should
    // exist. After RedemptionRejected, the sticky marker prevents
    // InflightEquity polls from zeroing inflight for the symbol. Without
    // sticky, the poll would zero inflight, the system would see a new
    // imbalance, and trigger another (incorrect) redemption.
    let withdrawn_count = redeem_events
        .iter()
        .filter(|event| event.event_type == "EquityRedemptionEvent::WithdrawnFromRaindex")
        .count();
    assert_eq!(
        withdrawn_count, 1,
        "Expected exactly 1 WithdrawnFromRaindex event (sticky inflight should prevent \
         the system from seeing a new imbalance and triggering another redemption), \
         got {withdrawn_count}",
    );

    // Verify the mock shows the rejected redemption request.
    let redeem_requests: Vec<_> = infra
        .tokenization_service
        .tokenization_requests()
        .into_iter()
        .filter(|req| req.request_type == TokenizationRequestType::Redeem && req.symbol == "AAPL")
        .collect();
    assert_eq!(
        redeem_requests.len(),
        1,
        "Expected exactly 1 redeem request for AAPL"
    );
    assert_eq!(
        redeem_requests[0].status,
        st0x_hedge::mock_api::TokenizationStatus::Rejected,
        "Redeem request should have Rejected status"
    );

    pool.close().await;
    bot.abort();
    Ok(())
}

/// Pending tokenization requests from foreign wallets are filtered out.
///
/// When multiple conductors share an Alpaca account, each conductor's
/// `poll_inflight_equity` should only count pending requests matching its
/// own wallet address. A foreign request (different wallet) must not
/// inflate the inflight state.
#[test_log::test(tokio::test)]
async fn pending_requests_filtered_by_wallet() -> anyhow::Result<()> {
    let onchain_price = float!("150.00");
    let broker_fill_price = float!("148.00");
    let amount_per_trade = float!("7.5");

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(7.5))],
    )
    .await?;

    // Set up SellEquity orders to create TooMuchOffchain imbalance -> mint.
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

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared_orders[0].input_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared_orders[0].output_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

    // Take all orders to create the imbalance.
    for prepared in &prepared_orders {
        infra.base_chain.take_prepared_order(prepared).await?;
    }

    // Inject a foreign pending mint request from a different wallet.
    // This simulates another conductor sharing the same Alpaca account.
    // If the bot doesn't filter by wallet, it would see 1000 extra shares
    // of inflight and make incorrect rebalancing decisions.
    let foreign_wallet = Address::random();
    infra.tokenization_service.inject_pending_request(
        "AAPL",
        float!("1000"),
        foreign_wallet,
        TokenizationRequestType::Mint,
    );

    // Wait for the mint to complete successfully.
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "TokenizedEquityMintEvent::DepositedIntoRaindex",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;

    // Verify the mint completed and the foreign pending request did not
    // inflate the conductor's own inflight polling.
    let mint_events = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
    assert_event_subsequence(
        &mint_events,
        &[
            "TokenizedEquityMintEvent::MintRequested",
            "TokenizedEquityMintEvent::MintAccepted",
            "TokenizedEquityMintEvent::TokensReceived",
            "TokenizedEquityMintEvent::WrapSubmitted",
            "TokenizedEquityMintEvent::TokensWrapped",
            "TokenizedEquityMintEvent::VaultDepositSubmitted",
            "TokenizedEquityMintEvent::DepositedIntoRaindex",
        ],
    );

    // The number of mint aggregates is timing-sensitive because later fills
    // can legitimately trigger a follow-up mint after the first cycle
    // completes. Assert on the inflight snapshots instead: the foreign
    // wallet's 1000-share request must never appear in our conductor's
    // polled inflight state.
    let snapshot_events = fetch_events_by_type(&pool, "InventorySnapshot").await?;
    let foreign_request_quantity = FractionalShares::new(float!("1000"));
    let observed_inflight_mints: Vec<FractionalShares> = snapshot_events
        .iter()
        .filter(|event| event.event_type == "InventorySnapshotEvent::InflightEquity")
        .filter_map(|event| {
            event
                .payload
                .get("InflightEquity")?
                .get("mints")?
                .get("AAPL")?
                .as_str()
        })
        .map(|quantity| {
            quantity
                .parse::<FractionalShares>()
                .unwrap_or_else(|error| {
                    panic!("Failed to parse InflightEquity AAPL quantity '{quantity}': {error:?}")
                })
        })
        .collect();

    assert_eq!(
        observed_inflight_mints
            .iter()
            .filter(|quantity| **quantity >= foreign_request_quantity)
            .count(),
        0,
        "Foreign wallet request should never appear in inflight polling: \
         {observed_inflight_mints:?}",
    );

    // Verify the foreign request still exists in the mock but was not
    // consumed by the bot.
    let all_requests = infra.tokenization_service.tokenization_requests();
    let foreign_requests: Vec<_> = all_requests
        .iter()
        .filter(|req| req.request_type == TokenizationRequestType::Mint && req.symbol == "AAPL")
        .collect();
    assert!(
        foreign_requests.len() >= 2,
        "Expected at least 2 mint requests for AAPL (1 from bot + 1 foreign), \
         got {}",
        foreign_requests.len(),
    );

    pool.close().await;
    bot.abort();
    Ok(())
}

/// Inflight polling picks up bot-owned pending tokenization requests end-to-end.
///
/// The conductor's inventory poller calls `list_pending_requests()` on
/// every poll cycle. When pending requests match an active mint aggregate,
/// the poller aggregates them into an `InflightEquity` snapshot command,
/// which the CQRS aggregate turns into an `InflightEquity` event.
#[test_log::test(tokio::test)]
async fn inflight_polling_emits_events_for_pending_requests() -> anyhow::Result<()> {
    let broker_fill_price = float!("150.00");
    let onchain_price = float!("150.00");
    let amount_per_trade = float!("5");

    let infra =
        TestInfra::start(vec![("AAPL", broker_fill_price)], vec![("AAPL", float!(5))]).await?;
    infra
        .tokenization_service
        .set_polls_until_complete(usize::MAX);

    // We need at least one order set up so the vault registry gets seeded
    // and the inventory poller has valid vault IDs to query.
    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(amount_per_trade)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared.input_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared.output_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let mut bot = spawn_bot(ctx);

    infra.base_chain.take_prepared_order(&prepared).await?;

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "TokenizedEquityMintEvent::MintAccepted",
        1,
        Duration::from_secs(120),
    )
    .await;

    let expected_quantity = single_pending_request_quantity(
        &infra.tokenization_service.tokenization_requests(),
        TokenizationRequestType::Mint,
        "AAPL",
    );

    poll_for_inflight_mint_event_count_after(
        &mut bot,
        &infra.db_path,
        "AAPL",
        expected_quantity,
        0,
        Duration::from_secs(30),
    )
    .await?;

    poll_for_snapshot_field(
        &mut bot,
        &infra.db_path,
        "InventorySnapshot",
        "inflight_mints",
        Duration::from_secs(30),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;

    // Verify the snapshot contains the bot-owned pending mint for AAPL.
    let snapshot_payload: (String,) = sqlx::query_as(
        "SELECT payload FROM snapshots \
         WHERE aggregate_type = 'InventorySnapshot' LIMIT 1",
    )
    .fetch_one(&pool)
    .await?;
    let snapshot: serde_json::Value = serde_json::from_str(&snapshot_payload.0)?;

    let live = snapshot
        .get("Live")
        .expect("Snapshot should be in Live state");
    let mints = live
        .get("inflight_mints")
        .expect("Snapshot should contain inflight_mints");
    let aapl_quantity = mints
        .get("AAPL")
        .expect("inflight_mints should contain AAPL from the bot-owned pending request");
    let aapl_quantity = aapl_quantity
        .as_str()
        .expect("AAPL inflight mint quantity should be serialized as a string")
        .parse::<FractionalShares>()?;
    assert_eq!(
        aapl_quantity, expected_quantity,
        "inflight_mints quantity for AAPL should match the bot-owned \
         pending request"
    );

    pool.close().await;
    bot.abort();
    Ok(())
}

/// A mint accepted before shutdown must resume and complete after restart.
#[test_log::test(tokio::test)]
async fn interrupted_mint_resumes_after_restart() -> anyhow::Result<()> {
    let onchain_price = float!(150.00);
    let broker_fill_price = float!(148.00);
    let amount_per_trade = float!(7.5);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(7.5))],
    )
    .await?;
    infra.tokenization_service.set_polls_until_complete(4);

    let wrapped_token = infra.equity_addresses[0].1;
    let unwrapped_token = infra.equity_addresses[0].2;

    let balance_skew: U256 = parse_units("1", 18)?.into();
    crate::base_chain::IERC20::new(unwrapped_token, &infra.base_chain.provider)
        .transfer(infra.base_chain.taker, balance_skew)
        .send()
        .await?
        .get_receipt()
        .await?;

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

    let owner_unwrapped_balance_before_takes =
        crate::base_chain::IERC20::new(unwrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;
    let owner_wrapped_balance_before_takes =
        crate::base_chain::IERC20::new(wrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;
    let owner_unwrapped_shares_before_takes =
        st0x_execution::FractionalShares::from_u256_18_decimals(
            owner_unwrapped_balance_before_takes,
        )?;
    let owner_wrapped_shares_before_takes =
        st0x_execution::FractionalShares::from_u256_18_decimals(
            owner_wrapped_balance_before_takes,
        )?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared_orders[0].input_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared_orders[0].output_vault_id)]);

    let ctx1 = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;
    let mut bot1 = spawn_bot(ctx1);

    assert_initial_base_wallet_unwrapped_and_wrapped_equity_snapshot(
        &mut bot1,
        &infra.db_path,
        "AAPL",
        owner_unwrapped_shares_before_takes,
        owner_wrapped_shares_before_takes,
    )
    .await?;

    let mut take_results = Vec::new();
    for prepared in &prepared_orders {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
    }

    poll_for_events_with_timeout(
        &mut bot1,
        &infra.db_path,
        "TokenizedEquityMintEvent::MintAccepted",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let mint_events_before_restart = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
    let resumed_aggregate_id = aggregate_id_for_event(
        &mint_events_before_restart,
        "TokenizedEquityMintEvent::MintAccepted",
    );
    pool.close().await;

    let requests_before_restart = infra.tokenization_service.tokenization_requests();
    let pending_mints_before_restart = count_tokenization_requests(
        &requests_before_restart,
        TokenizationRequestType::Mint,
        "AAPL",
        st0x_hedge::mock_api::TokenizationStatus::Pending,
    );
    assert!(
        pending_mints_before_restart >= 1,
        "Expected the mint request to still be pending before restart"
    );

    bot1.abort();
    let _ = bot1.await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let ctx2 = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events_with_timeout(
        &mut bot2,
        &infra.db_path,
        "TokenizedEquityMintEvent::DepositedIntoRaindex",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let mint_events_after_restart = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
    assert_single_clean_aggregate(&mint_events_after_restart, &["Failed", "Rejected"]);
    assert_event_subsequence(
        &mint_events_after_restart,
        &[
            "TokenizedEquityMintEvent::MintRequested",
            "TokenizedEquityMintEvent::MintAccepted",
            "TokenizedEquityMintEvent::TokensReceived",
            "TokenizedEquityMintEvent::WrapSubmitted",
            "TokenizedEquityMintEvent::TokensWrapped",
            "TokenizedEquityMintEvent::VaultDepositSubmitted",
            "TokenizedEquityMintEvent::DepositedIntoRaindex",
        ],
    );
    assert_eq!(
        aggregate_id_for_event(
            &mint_events_after_restart,
            "TokenizedEquityMintEvent::DepositedIntoRaindex",
        ),
        resumed_aggregate_id,
        "Expected the interrupted mint aggregate to complete after restart",
    );
    pool.close().await;

    let requests_after_restart = infra.tokenization_service.tokenization_requests();
    let completed_mints_after_restart = count_tokenization_requests(
        &requests_after_restart,
        TokenizationRequestType::Mint,
        "AAPL",
        st0x_hedge::mock_api::TokenizationStatus::Completed,
    );
    assert!(
        completed_mints_after_restart >= 1,
        "Expected a completed mint request after restart"
    );

    // The finalized-block fill monitor ingests fills a few blocks behind the
    // tip, so the per-fill hedges settle shortly after the mint completes.
    // Wait for every hedge fill before the one-shot broker-state assertion.
    poll_for_events_with_timeout(
        &mut bot2,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        i64::try_from(take_results.len())?,
        Duration::from_secs(120),
    )
    .await;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(float!(22.5))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(0))
        .expected_accumulated_short(float!(22.5))
        .expected_net(float!(0))
        .build()];

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&take_results)
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .rebalance_type(EquityRebalanceType::Mint {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
        })
        .call()
        .await?;

    bot2.abort();
    Ok(())
}

/// A redemption detected before shutdown must resume and complete after restart.
#[test_log::test(tokio::test)]
async fn interrupted_redemption_resumes_after_restart() -> anyhow::Result<()> {
    let onchain_price = float!(112.50);
    let broker_fill_price = float!(113.60);
    let trade_amount = float!(12.5);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!("20"))],
    )
    .await?;
    infra.tokenization_service.set_polls_until_complete(4);

    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(trade_amount)
        .price(onchain_price)
        .direction(TakeDirection::BuyEquity)
        .call()
        .await?;

    let vault_addr = infra.equity_addresses[0].1;
    let underlying_addr = infra.equity_addresses[0].2;
    let redemption_wallet_balance_before =
        crate::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;
    let equity_vault_id = prepared.input_vault_id;
    let extra_equity: U256 = parse_units("20", 18)?.into();
    infra
        .base_chain
        .deposit_into_raindex_vault(vault_addr, equity_vault_id, extra_equity, 18)
        .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared.output_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared.input_vault_id)]);

    let ctx1 = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;
    let mut bot1 = spawn_bot(ctx1);

    tokio::time::sleep(Duration::from_secs(10)).await;

    let take_result = infra.base_chain.take_prepared_order(&prepared).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    poll_for_events_with_timeout(
        &mut bot1,
        &infra.db_path,
        "EquityRedemptionEvent::Detected",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let redemption_events_before_restart = fetch_events_by_type(&pool, "EquityRedemption").await?;
    let resumed_aggregate_id = aggregate_id_for_event(
        &redemption_events_before_restart,
        "EquityRedemptionEvent::Detected",
    );
    pool.close().await;

    let requests_before_restart = infra.tokenization_service.tokenization_requests();
    let pending_redemptions_before_restart = count_tokenization_requests(
        &requests_before_restart,
        TokenizationRequestType::Redeem,
        "AAPL",
        st0x_hedge::mock_api::TokenizationStatus::Pending,
    );
    assert!(
        pending_redemptions_before_restart >= 1,
        "Expected the redemption request to still be pending before restart"
    );

    bot1.abort();
    let _ = bot1.await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let ctx2 = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events_with_timeout(
        &mut bot2,
        &infra.db_path,
        "EquityRedemptionEvent::Completed",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let redemption_events_after_restart = fetch_events_by_type(&pool, "EquityRedemption").await?;
    assert_single_clean_aggregate(&redemption_events_after_restart, &["Failed", "Rejected"]);
    assert_event_subsequence(
        &redemption_events_after_restart,
        &[
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            "EquityRedemptionEvent::UnwrapPending",
            "EquityRedemptionEvent::UnwrapSubmitted",
            "EquityRedemptionEvent::TokensUnwrapped",
            "EquityRedemptionEvent::SendPending",
            "EquityRedemptionEvent::TokensSent",
            "EquityRedemptionEvent::Detected",
            "EquityRedemptionEvent::Completed",
        ],
    );
    assert_eq!(
        aggregate_id_for_event(
            &redemption_events_after_restart,
            "EquityRedemptionEvent::Completed",
        ),
        resumed_aggregate_id,
        "Expected the interrupted redemption aggregate to complete after restart",
    );
    pool.close().await;

    let requests_after_restart = infra.tokenization_service.tokenization_requests();
    let completed_redemptions_after_restart = count_tokenization_requests(
        &requests_after_restart,
        TokenizationRequestType::Redeem,
        "AAPL",
        st0x_hedge::mock_api::TokenizationStatus::Completed,
    );
    assert!(
        completed_redemptions_after_restart >= 1,
        "Expected a completed redemption request after restart"
    );

    // The finalized-block fill monitor ingests fills a few blocks behind the
    // tip, so the hedge settles shortly after the redemption completes. Wait
    // for the hedge fill before the one-shot broker-state assertion.
    poll_for_events_with_timeout(
        &mut bot2,
        &infra.db_path,
        "OffchainOrderEvent::Filled",
        1,
        Duration::from_secs(120),
    )
    .await;

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(trade_amount)
        .direction(TakeDirection::BuyEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(float!(12.5))
        .expected_accumulated_short(float!(0))
        .expected_net(float!(0))
        .build()];

    let redemption_wallet_balance_after =
        crate::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&[take_result])
        .provider(&infra.base_chain.provider)
        .orderbook(infra.base_chain.orderbook)
        .owner(infra.base_chain.owner)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .rebalance_type(EquityRebalanceType::Redeem {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
            redemption_wallet_balance_before,
            redemption_wallet_balance_after,
        })
        .call()
        .await?;

    bot2.abort();
    Ok(())
}

/// Inflight state survives bot restart via CQRS snapshots.
///
/// When the bot restarts against the same database, the CQRS aggregate
/// restores compacted `InventorySnapshot` state from the latest snapshot.
///
/// This test:
/// 1. Starts a bot, lets it poll and persist inflight state with a pending
///    mint
/// 2. Stops the bot
/// 3. Starts a new bot on the same database
/// 4. Verifies the second bot starts successfully and the inflight state
///    is still present after restart
#[test_log::test(tokio::test)]
async fn inflight_state_survives_restart() -> anyhow::Result<()> {
    let broker_fill_price = float!("150.00");
    let onchain_price = float!("150.00");
    let amount_per_trade = float!("5");

    let infra =
        TestInfra::start(vec![("AAPL", broker_fill_price)], vec![("AAPL", float!(5))]).await?;
    infra
        .tokenization_service
        .set_polls_until_complete(usize::MAX);

    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(amount_per_trade)
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared.input_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared.output_vault_id)]);

    // --- First bot: let it poll and snapshot inflight equity ---
    let ctx1 = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let mut bot1 = spawn_bot(ctx1);

    infra.base_chain.take_prepared_order(&prepared).await?;

    poll_for_events_with_timeout(
        &mut bot1,
        &infra.db_path,
        "TokenizedEquityMintEvent::MintAccepted",
        1,
        Duration::from_secs(120),
    )
    .await;

    let expected_quantity = single_pending_request_quantity(
        &infra.tokenization_service.tokenization_requests(),
        TokenizationRequestType::Mint,
        "AAPL",
    );

    poll_for_inflight_mint_event_count_after(
        &mut bot1,
        &infra.db_path,
        "AAPL",
        expected_quantity,
        0,
        Duration::from_secs(30),
    )
    .await?;

    // Wait for the first bot to persist inflight state. InventorySnapshot
    // events are compactable, so the durable assertion is the snapshot row.
    let snapshot_before_restart =
        poll_for_inflight_mint_snapshot(&mut bot1, &infra.db_path, "AAPL", Duration::from_secs(30))
            .await?;
    let pool = connect_db(&infra.db_path).await?;
    let events_before_restart = fetch_events_by_type(&pool, "InventorySnapshot").await?;
    pool.close().await;
    let observed_before_restart = inflight_mint_quantities(&events_before_restart, "AAPL")?;
    assert!(
        observed_before_restart.contains(&expected_quantity),
        "First bot should persist exact inflight mint quantity before restart: \
         {observed_before_restart:?}",
    );

    // Stop the first bot.
    bot1.abort();
    let join_error = bot1.await.unwrap_err();
    assert!(
        join_error.is_cancelled(),
        "First bot should be cancelled, got: {join_error}"
    );
    // Allow the abort to propagate and release the database.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Second bot: restart on the same database ---
    let ctx2 = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let bot2 = spawn_bot(ctx2);

    // Recovery resumes the accepted mint before the inventory monitor starts.
    // Keep the provider request pending and assert the compacted snapshot is
    // still present while startup is in that recovery phase.
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Verify the second bot is still alive (didn't crash on replay).
    assert!(
        !bot2.is_finished(),
        "Second bot should still be running after replaying events"
    );

    let pool = connect_db(&infra.db_path).await?;
    let snapshot_after_restart = latest_inventory_snapshot(&pool)
        .await?
        .expect("InventorySnapshot snapshot should exist after restart");

    assert!(
        snapshot_after_restart.last_sequence >= snapshot_before_restart.last_sequence,
        "InventorySnapshot sequence should not move backwards across restart \
         (before: {}, after: {})",
        snapshot_before_restart.last_sequence,
        snapshot_after_restart.last_sequence
    );
    assert_eq!(
        snapshot_inflight_mint(&snapshot_after_restart, "AAPL")?,
        Some(expected_quantity),
        "Inflight mint state should survive restart via the compacted snapshot"
    );

    pool.close().await;
    bot2.abort();
    Ok(())
}

/// Wrapped equity sitting in the bot wallet outside the Raindex vault, with
/// no aggregate owning the symbol's in-flight slot, must be recovered into
/// the configured Raindex vault automatically.
///
/// `deploy_equity_vault` deposits half of the underlying supply into the
/// vault and mints shares to the owner; `fund_taker_with_equity` moves only
/// a slice of those shares to the taker. The leftover wtSTOCK in the bot
/// wallet is what this test treats as orphaned.
///
/// Asserts: the bot wallet is drained of wtSTOCK, the configured Raindex
/// vault is topped up by at least the original wallet balance, and the
/// recovery aggregate's event stream contains
/// `Detected -> OrphanDepositSubmitted -> OrphanDeposited`.
#[test_log::test(tokio::test)]
async fn wrapped_equity_in_bot_wallet_recovers_into_raindex() -> anyhow::Result<()> {
    let onchain_price = float!("150.00");
    let broker_fill_price = float!("150.00");

    // Match the broker position to the equity deposited into the Raindex
    // vault by `setup_order` below so the inventory comes up balanced. Without
    // a matching offchain position the regular rebalancing trigger fires
    // first and holds the symbol's `equity_in_progress` guard, blocking the
    // recovery job long enough to time out.
    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!("1"))],
    )
    .await?;

    let wrapped_token = infra.equity_addresses[0].1;

    // Register a Raindex vault for AAPL via setup_order so the bot knows
    // where to deposit recovered wtSTOCK. The order is never taken; we only
    // need the vault ID.
    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!("1"))
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let owner_wrapped_balance_before =
        crate::base_chain::IERC20::new(wrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    assert!(
        owner_wrapped_balance_before > U256::ZERO,
        "Test precondition violated: owner wallet should hold wtSTOCK after vault deployment",
    );

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared.input_vault_id;
    let equity_vault_id = prepared.output_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), equity_vault_id)]);

    let orderbook =
        IRaindexV6::IRaindexV6Instance::new(infra.base_chain.orderbook, &infra.base_chain.provider);

    let vault_balance_before_raw = orderbook
        .vaultBalance2(infra.base_chain.owner, wrapped_token, equity_vault_id)
        .call()
        .await?;

    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Enabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let mut bot = spawn_bot(ctx);

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "WrappedEquityRecoveryEvent::OrphanDeposited",
        1,
        Duration::from_secs(60),
    )
    .await;

    let owner_wrapped_balance_after =
        crate::base_chain::IERC20::new(wrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    assert_eq!(
        owner_wrapped_balance_after,
        U256::ZERO,
        "Recovery should have moved every wtSTOCK share out of the owner wallet \
         (started with {owner_wrapped_balance_before}, still holding \
         {owner_wrapped_balance_after})",
    );

    let vault_balance_after_raw = orderbook
        .vaultBalance2(infra.base_chain.owner, wrapped_token, equity_vault_id)
        .call()
        .await?;

    // The orderbook stores vault balances as rain.math.float Floats packed
    // into bytes32, so raw byte arithmetic is meaningless. Decode each side
    // to a U256 with 18 decimals (matching the wtSTOCK ERC-20 precision)
    // and compare the delta to the wallet's recovered amount.
    let (vault_balance_before, _lossless_before) =
        Float::from_raw(vault_balance_before_raw).to_fixed_decimal_lossy(18)?;
    let (vault_balance_after, _lossless_after) =
        Float::from_raw(vault_balance_after_raw).to_fixed_decimal_lossy(18)?;
    let vault_delta = vault_balance_after.saturating_sub(vault_balance_before);

    assert!(
        vault_delta >= owner_wrapped_balance_before,
        "Recovery should have deposited at least {owner_wrapped_balance_before} wtSTOCK into the \
         configured Raindex vault (before={vault_balance_before}, after={vault_balance_after}, \
         delta={vault_delta})",
    );

    let pool = connect_db(&infra.db_path).await?;
    let recovery_events = fetch_events_by_type(&pool, "WrappedEquityRecovery").await?;
    pool.close().await;

    assert_event_subsequence(
        &recovery_events,
        &[
            "WrappedEquityRecoveryEvent::Detected",
            "WrappedEquityRecoveryEvent::OrphanDepositSubmitted",
            "WrappedEquityRecoveryEvent::OrphanDeposited",
        ],
    );
    assert_single_clean_aggregate(
        &recovery_events,
        &["WrappedEquityRecoveryEvent::RecoveryFailed"],
    );

    bot.abort();
    Ok(())
}

/// Orphan recovery for unwrapped equity (tSTOCK) found on the bot wallet.
///
/// Mirrors `wrapped_equity_in_bot_wallet_recovers_into_raindex` but for the
/// unwrapped path: the wallet starts with leftover tSTOCK (unwrapped) from
/// vault deployment, no in-flight mint or redemption owns it, so the
/// UnwrappedEquityRecoveryJob takes the orphan path and runs the full
/// four-step sequence (wrap submit + confirm, then deposit submit + confirm).
///
/// Asserts: the bot wallet ends up with zero tSTOCK and zero wtSTOCK (both
/// drained), the configured Raindex vault is topped up by at least the
/// originally-unwrapped balance, and the recovery aggregate's event stream
/// records the full orphan sequence `Detected -> OrphanWrapSubmitted ->
/// OrphanWrapped -> OrphanDepositSubmitted -> OrphanDeposited`.
#[test_log::test(tokio::test)]
async fn unwrapped_equity_in_bot_wallet_recovers_into_raindex() -> anyhow::Result<()> {
    let onchain_price = float!("150.00");
    let broker_fill_price = float!("150.00");

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!("1"))],
    )
    .await?;

    let wrapped_token = infra.equity_addresses[0].1;
    let unwrapped_token = infra.equity_addresses[0].2;

    // Register a Raindex vault for AAPL via setup_order so the bot knows
    // where to deposit recovered shares. The order is never taken; we only
    // need the vault ID and the side-effect of leaving tSTOCK in the owner
    // wallet from deploy_equity_vault.
    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!("1"))
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let owner_unwrapped_balance_before =
        crate::base_chain::IERC20::new(unwrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;
    let owner_wrapped_balance_before =
        crate::base_chain::IERC20::new(wrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    assert!(
        owner_unwrapped_balance_before > U256::ZERO,
        "Test precondition violated: owner wallet should hold tSTOCK after vault deployment",
    );
    assert!(
        owner_wrapped_balance_before > U256::ZERO,
        "Test precondition violated: owner wallet should hold wtSTOCK after vault deployment",
    );

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared.input_vault_id;
    let equity_vault_id = prepared.output_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), equity_vault_id)]);

    let orderbook =
        IRaindexV6::IRaindexV6Instance::new(infra.base_chain.orderbook, &infra.base_chain.provider);

    let vault_balance_before_raw = orderbook
        .vaultBalance2(infra.base_chain.owner, wrapped_token, equity_vault_id)
        .call()
        .await?;

    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        // Suppress equity rebalancing with an unreachable imbalance deviation so
        // this test isolates recovery. Equity rebalancing must stay `Enabled`
        // (the wallet poller only emits the events that dispatch recovery jobs
        // for rebalancing-enabled symbols), but with it actively transferring,
        // the wtSTOCK the recovery deposits into the vault creates an imbalance
        // that fires a Raindex->Alpaca transfer. That transfer holds the per-symbol
        // `equity_in_progress` guard and starves the wrapped recovery (and
        // drains the vault the assertions check). A huge deviation keeps the
        // imbalance check from ever triggering a transfer. The recovery<->
        // rebalancing interaction itself is a separate integration concern.
        .equity_imbalance(ImbalanceThreshold::new(float!(0.5), float!(100))?)
        .wrapped_equity_recovery(OperationMode::Enabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let mut bot = spawn_bot(ctx);

    // The bot wallet holds BOTH unwrapped tSTOCK and pre-existing wtSTOCK for
    // AAPL, so two recovery jobs are enqueued: the unwrapped recovery (wraps the
    // tSTOCK and deposits exactly that wrapped amount) and the wrapped recovery
    // (deposits the pre-existing wtSTOCK). Both claim the same per-symbol
    // `equity_in_progress` guard, so they run serially -- whichever wins the
    // guard first runs to completion while the other silently skips, and the
    // skipped one is only re-dispatched on the next inventory poll once the
    // guard is free. We must therefore wait for BOTH terminal events before
    // asserting the wallet is fully drained; waiting only for the unwrapped
    // terminal races the wrapped recovery, which has not run yet.
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "UnwrappedEquityRecoveryEvent::OrphanDeposited",
        1,
        Duration::from_secs(60),
    )
    .await;
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "WrappedEquityRecoveryEvent::OrphanDeposited",
        1,
        Duration::from_secs(60),
    )
    .await;

    let owner_unwrapped_balance_after =
        crate::base_chain::IERC20::new(unwrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;
    let owner_wrapped_balance_after =
        crate::base_chain::IERC20::new(wrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    assert_eq!(
        owner_unwrapped_balance_after,
        U256::ZERO,
        "Recovery should have wrapped every tSTOCK out of the owner wallet \
         (started with {owner_unwrapped_balance_before}, still holding \
         {owner_unwrapped_balance_after})",
    );

    assert_eq!(
        owner_wrapped_balance_after,
        U256::ZERO,
        "Recovery should have deposited every wtSTOCK into the vault, leaving \
         the owner wallet empty (still holding {owner_wrapped_balance_after})",
    );

    let vault_balance_after_raw = orderbook
        .vaultBalance2(infra.base_chain.owner, wrapped_token, equity_vault_id)
        .call()
        .await?;

    let (vault_balance_before, _lossless_before) =
        Float::from_raw(vault_balance_before_raw).to_fixed_decimal_lossy(18)?;
    let (vault_balance_after, _lossless_after) =
        Float::from_raw(vault_balance_after_raw).to_fixed_decimal_lossy(18)?;
    let vault_delta = vault_balance_after.saturating_sub(vault_balance_before);

    // Vault delta must cover BOTH the pre-existing wtSTOCK (which the wrapped
    // recovery deposits) AND the wtSTOCK produced by wrapping the tSTOCK (which
    // the unwrapped recovery is responsible for). Asserting the lower bound at
    // `owner_unwrapped_balance_before` alone would pass even if the unwrapped
    // path did nothing, because the wrapped path would still deposit the
    // pre-existing balance.
    let expected_min_delta = owner_unwrapped_balance_before
        .checked_add(owner_wrapped_balance_before)
        .expect("snapshot sum fits in U256");
    assert!(
        vault_delta >= expected_min_delta,
        "Recovery should have deposited at least {expected_min_delta} into the configured \
         Raindex vault (pre-existing wtSTOCK={owner_wrapped_balance_before} plus the \
         wrapped equivalent of {owner_unwrapped_balance_before} recovered tSTOCK) \
         (before={vault_balance_before}, after={vault_balance_after}, delta={vault_delta})",
    );

    let pool = connect_db(&infra.db_path).await?;
    let unwrapped_recovery_events = fetch_events_by_type(&pool, "UnwrappedEquityRecovery").await?;
    let wrapped_recovery_events = fetch_events_by_type(&pool, "WrappedEquityRecovery").await?;
    pool.close().await;

    assert_event_subsequence(
        &unwrapped_recovery_events,
        &[
            "UnwrappedEquityRecoveryEvent::Detected",
            "UnwrappedEquityRecoveryEvent::OrphanWrapSubmitted",
            "UnwrappedEquityRecoveryEvent::OrphanWrapped",
            "UnwrappedEquityRecoveryEvent::OrphanDepositSubmitted",
            "UnwrappedEquityRecoveryEvent::OrphanDeposited",
        ],
    );
    assert_single_clean_aggregate(
        &unwrapped_recovery_events,
        &["UnwrappedEquityRecoveryEvent::RecoveryFailed"],
    );

    assert_event_subsequence(
        &wrapped_recovery_events,
        &[
            "WrappedEquityRecoveryEvent::Detected",
            "WrappedEquityRecoveryEvent::OrphanDepositSubmitted",
            "WrappedEquityRecoveryEvent::OrphanDeposited",
        ],
    );
    assert_single_clean_aggregate(
        &wrapped_recovery_events,
        &["WrappedEquityRecoveryEvent::RecoveryFailed"],
    );

    bot.abort();
    Ok(())
}

/// Active-mint counterpart to the orphan recovery test: a TokenizedEquityMint
/// aggregate persisted in `TokensWrapped` state must be driven to
/// `DepositedIntoRaindex` once the bot starts. The wtSTOCK left over from
/// `deploy_equity_vault` supplies the wallet balance that the mint
/// "wrapped"; the bot's recovery path (either startup `resume_interrupted`
/// or the inventory-triggered WrappedEquityRecovery job, whichever races
/// first) finishes the Raindex deposit.
///
/// Asserts the terminal scenario rather than which path finished it: the mint
/// reaches `DepositedIntoRaindex`, the owner wallet is drained of wtSTOCK, and
/// the configured Raindex vault holds the deposited shares.
#[test_log::test(tokio::test)]
async fn active_mint_in_tokens_wrapped_recovers_into_raindex_vault() -> anyhow::Result<()> {
    let onchain_price = float!("150.00");
    let broker_fill_price = float!("150.00");

    // Balance the broker against the equity vault deposited by `setup_order`
    // so the rebalancing trigger doesn't race the seeded mint's startup
    // recovery for the symbol's `equity_in_progress` guard.
    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!("1"))],
    )
    .await?;
    let wrapped_token = infra.equity_addresses[0].1;

    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!("1"))
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    let owner_wrapped_balance_before =
        crate::base_chain::IERC20::new(wrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    assert!(
        owner_wrapped_balance_before > U256::ZERO,
        "Test precondition: owner wallet should hold wtSTOCK after vault deployment",
    );

    // Seed a TokenizedEquityMint at TokensWrapped pointing to the wtSTOCK
    // already sitting in the owner wallet. The seeded `wrapped_shares` must
    // be <= the wallet balance so the on-chain Raindex deposit can clear.
    // The DB file doesn't exist yet (the bot creates it on first launch), so
    // open it with `create_if_missing` and apply migrations before seeding.
    let seeded_shares = owner_wrapped_balance_before;

    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&infra.db_path)
                .create_if_missing(true),
        )
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let recovery_mint_id = issuer_request_id("ISS-RECOVERY-E2E");

    seed_mint_at_tokens_wrapped_for_test(
        &pool,
        &recovery_mint_id,
        "AAPL",
        infra.base_chain.owner,
        TxHash::random(),
        seeded_shares,
        float!("1"),
    )
    .await?;

    pool.close().await;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared.input_vault_id;
    let equity_vault_id = prepared.output_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), equity_vault_id)]);

    let orderbook =
        IRaindexV6::IRaindexV6Instance::new(infra.base_chain.orderbook, &infra.base_chain.provider);

    let vault_balance_before = orderbook
        .vaultBalance2(infra.base_chain.owner, wrapped_token, equity_vault_id)
        .call()
        .await?;

    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Enabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(infra.issuance_base_url.clone())
        .call()?;

    let mut bot = spawn_bot(ctx);

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "TokenizedEquityMintEvent::DepositedIntoRaindex",
        1,
        Duration::from_secs(60),
    )
    .await;

    let owner_wrapped_balance_after =
        crate::base_chain::IERC20::new(wrapped_token, &infra.base_chain.provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

    assert_eq!(
        owner_wrapped_balance_after,
        U256::ZERO,
        "Recovery should drain the bot wallet of wtSTOCK \
         (started with {owner_wrapped_balance_before}, still holding {owner_wrapped_balance_after})",
    );

    let vault_balance_after = orderbook
        .vaultBalance2(infra.base_chain.owner, wrapped_token, equity_vault_id)
        .call()
        .await?;

    let vault_balance_before_shares = Float::from_raw(vault_balance_before)
        .to_fixed_decimal_lossy(18)?
        .0;
    let vault_balance_after_shares = Float::from_raw(vault_balance_after)
        .to_fixed_decimal_lossy(18)?
        .0;
    let vault_delta_shares = vault_balance_after_shares - vault_balance_before_shares;

    assert!(
        vault_delta_shares >= owner_wrapped_balance_before,
        "Recovery should deposit at least the wallet snapshot into the configured \
         Raindex vault (vault delta is {vault_delta_shares}, expected at least \
         {owner_wrapped_balance_before})",
    );

    let pool = connect_db(&infra.db_path).await?;
    let mint_events = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
    pool.close().await;

    let seeded_mint_events: Vec<StoredEvent> = mint_events
        .into_iter()
        .filter(|event| event.aggregate_id == recovery_mint_id)
        .collect();

    assert_event_subsequence(
        &seeded_mint_events,
        &[
            "TokenizedEquityMintEvent::TokensWrapped",
            "TokenizedEquityMintEvent::DepositedIntoRaindex",
        ],
    );

    bot.abort();
    Ok(())
}

/// Base->Alpaca crash-recovery hypothesis: the system survives a crash mid
/// USDC transfer.
///
/// Setup runs a Base->Alpaca USDC rebalance, kills the bot after
/// `BridgingInitiated` (vault withdrawal done, CCTP burn submitted, USDC not
/// yet on Ethereum), and restarts a fresh conductor against the same SQLite
/// db_path. The assertions require that the same UsdcRebalance aggregate
/// reaches `ConversionConfirmed` without forking into a second aggregate or
/// emitting any *Failed events.
///
/// The test fails on the current mpsc-driven Rebalancer (the in-flight
/// transfer dies with the bot and never resumes), and passes once the
/// Base->Alpaca direction is durably handled by the TransferUsdcToHedging
/// apalis job.
#[test_log::test(tokio::test)]
async fn interrupted_usdc_base_to_alpaca_resumes_after_restart() -> anyhow::Result<()> {
    let onchain_price = float!(158.39);
    let broker_fill_price = float!(155.00);
    let amount_per_trade = float!(7.5);

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        vec![("AAPL", float!(7.5))],
    )
    .await?;
    let cctp = CctpInfra::start(&infra).await?;

    let usdc_amount: U256 = parse_units("145000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;
    infra.base_chain.set_owner_usdc_balance(U256::ZERO).await?;

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
                .usdc_vault_id(usdc_vault_id)
                .call()
                .await?,
        );
    }

    let eth_deposit_provider = alloy::providers::ProviderBuilder::new()
        .connect(&cctp.ethereum_endpoint)
        .await?;
    let _deposit_watcher = infra
        .broker_service
        .start_deposit_watcher(eth_deposit_provider, USDC_ETHEREUM, infra.base_chain.owner)
        .await?;

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let reserved = Positive::new(Usd::new(float!(3000)))?;
    let ctx1 = build_usdc_rebalancing_ctx()
        .base_chain(&infra.base_chain)
        .ethereum_endpoint(&cctp.ethereum_endpoint)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .usdc_vault_id(usdc_vault_id)
        .cctp(cctp.cctp_overrides())
        .reserved(reserved)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .call()?;
    let mut bot1 = spawn_bot(ctx1);

    tokio::time::sleep(Duration::from_secs(10)).await;

    for (index, prepared) in prepared_orders.iter().enumerate() {
        let _ = infra.base_chain.take_prepared_order(prepared).await?;
        if index + 1 < prepared_orders.len() {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    // Wait for the bot to advance the aggregate past the vault withdrawal
    // into the CCTP bridge phase. From here on the job's idempotent resume
    // path is the only thing that can reach ConversionConfirmed.
    poll_for_events_with_timeout(
        &mut bot1,
        &infra.db_path,
        "UsdcRebalanceEvent::BridgingInitiated",
        1,
        Duration::from_secs(120),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let events_before_restart = fetch_events_by_type(&pool, "UsdcRebalance").await?;
    let resumed_aggregate_id = aggregate_id_for_event(
        &events_before_restart,
        "UsdcRebalanceEvent::BridgingInitiated",
    );
    pool.close().await;

    bot1.abort();
    let _ = bot1.await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let ctx2 = build_usdc_rebalancing_ctx()
        .base_chain(&infra.base_chain)
        .ethereum_endpoint(&cctp.ethereum_endpoint)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .usdc_vault_id(usdc_vault_id)
        .cctp(cctp.cctp_overrides())
        .reserved(reserved)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .call()?;
    let mut bot2 = spawn_bot(ctx2);

    poll_for_events_with_timeout(
        &mut bot2,
        &infra.db_path,
        "UsdcRebalanceEvent::ConversionConfirmed",
        1,
        Duration::from_secs(180),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;
    let events_after_restart = fetch_events_by_type(&pool, "UsdcRebalance").await?;

    // Load-bearing assertion: the resumed transfer must NOT re-issue the
    // vault withdrawal. The pre-712 Rebalancer-mpsc path detects the
    // still-out-of-balance inventory on restart and triggers a fresh
    // UsdcRebalance aggregate, which fires `WithdrawalConfirmed` a second
    // time. The apalis-job path re-enters the existing aggregate at its
    // last persisted state and never re-issues the withdrawal.
    let withdrawal_confirmed_count = events_after_restart
        .iter()
        .filter(|event| event.event_type == "UsdcRebalanceEvent::WithdrawalConfirmed")
        .count();
    assert_eq!(
        withdrawal_confirmed_count, 1,
        "Expected exactly one WithdrawalConfirmed event across the full run \
         (one per UsdcRebalance lifecycle); got {withdrawal_confirmed_count}. \
         More than one means the bot started a fresh transfer instead of \
         resuming the interrupted one, which means the apalis durability \
         contract isn't holding.",
    );

    // Apalis-driven resume produces a Done TransferUsdcToHedging row
    // in the Jobs table whose serialized payload references the same
    // aggregate id that BridgingInitiated was emitted under. Asserted via
    // string match on job_type so the test can sit downstack of the PR
    // that introduces the type and still fail to compile-time-reference
    // it.
    let raw_rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT job_type, job FROM Jobs \
         WHERE job_type LIKE '%TransferUsdcToHedging%' \
         AND status = 'Done'",
    )
    .fetch_all(&pool)
    .await?;
    pool.close().await;

    let successful_job_rows: Vec<(String, String)> = raw_rows
        .into_iter()
        .map(|(job_type, job)| (job_type, String::from_utf8_lossy(&job).into_owned()))
        .collect();

    assert!(
        !successful_job_rows.is_empty(),
        "Expected at least one Done TransferUsdcToHedging row in \
         the Jobs table after restart; the apalis-job-row durability \
         contract is the only mechanism that produces such a row. None \
         found, which means the resume happened via some other path \
         (Rebalancer-mpsc + inventory poller) -- exactly the pre-712 \
         behaviour this test must reject.",
    );
    assert!(
        successful_job_rows
            .iter()
            .any(|(_, job)| job.contains(resumed_aggregate_id.as_str())),
        "Expected the Done TransferUsdcToHedging row to reference \
         the resumed_aggregate_id ({resumed_aggregate_id}). Job rows \
         found: {successful_job_rows:?}",
    );

    assert_single_clean_aggregate(
        &events_after_restart,
        &[
            "WithdrawalFailed",
            "BridgingFailed",
            "DepositFailed",
            "ConversionFailed",
        ],
    );
    assert_event_subsequence(
        &events_after_restart,
        &[
            "UsdcRebalanceEvent::Initiated",
            "UsdcRebalanceEvent::WithdrawalConfirmed",
            "UsdcRebalanceEvent::BridgingInitiated",
            "UsdcRebalanceEvent::BridgeAttestationReceived",
            "UsdcRebalanceEvent::Bridged",
            "UsdcRebalanceEvent::DepositInitiated",
            "UsdcRebalanceEvent::DepositConfirmed",
            "UsdcRebalanceEvent::ConversionInitiated",
            "UsdcRebalanceEvent::ConversionConfirmed",
        ],
    );
    assert_eq!(
        aggregate_id_for_event(
            &events_after_restart,
            "UsdcRebalanceEvent::ConversionConfirmed",
        ),
        resumed_aggregate_id,
        "Expected the interrupted UsdcRebalance aggregate to complete after restart",
    );

    bot2.abort();
    let _ = bot2.await;
    Ok(())
}

/// The constant digest the stub orchestrator reports. The authorization
/// assertions require the delivered signature to recover to the bot wallet
/// over exactly this word.
const E2E_ORCHESTRATOR_DIGEST: [u8; 32] = [0xd1; 32];

/// Runtime bytecode that returns the given 32-byte word for a well-formed
/// `mintAuthDigest(address,address,uint256,bytes32)` call and reverts on
/// anything else.
///
/// Stands in for the orchestrator's `mintAuthDigest` view on the e2e chain
/// (no ST0xOrchestrator artifact is available to deploy here): the
/// mint-authorization service reads the digest from the configured
/// orchestrator address and signs whatever it returns, so a constant is
/// enough to drive the full sign-and-deliver path. The stub still rejects
/// a wrong selector or mis-sized calldata; argument VALUES cannot be
/// pinned statically (the nonce is random per mint) -- their exact
/// encoding is pinned by the mint_authorization unit tests.
///
/// Assembly:
/// `PUSH1 0 CALLDATALOAD PUSH1 224 SHR PUSH4 <selector> EQ PUSH1 20 JUMPI`
/// `PUSH1 0 PUSH1 0 REVERT` (wrong selector)
/// `JUMPDEST CALLDATASIZE PUSH1 132 EQ PUSH1 33 JUMPI`
/// `PUSH1 0 PUSH1 0 REVERT` (calldata is not selector + 4 words)
/// `JUMPDEST PUSH32 <digest> PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN`
fn digest_stub_bytecode(digest: [u8; 32]) -> Vec<u8> {
    let selector = IST0xOrchestratorV1::mintAuthDigestCall::SELECTOR;

    let mut code = Vec::with_capacity(75);
    code.extend_from_slice(&[0x60, 0x00, 0x35, 0x60, 0xe0, 0x1c, 0x63]);
    code.extend_from_slice(&selector);
    code.extend_from_slice(&[0x14, 0x60, 0x14, 0x57, 0x60, 0x00, 0x60, 0x00, 0xfd]);
    code.extend_from_slice(&[
        0x5b, 0x36, 0x60, 0x84, 0x14, 0x60, 0x21, 0x57, 0x60, 0x00, 0x60, 0x00, 0xfd,
    ]);
    code.extend_from_slice(&[0x5b, 0x7f]);
    code.extend_from_slice(&digest);
    code.extend_from_slice(&[0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3]);
    code
}

/// Test-owned issuance mock reporting `vault_mode` for every asset, so each
/// mint-authorization flow controls the minting path (the shared infra mock
/// is always vault-direct). The caller registers its own POST-authorization
/// mock on the returned server BEFORE driving the flow -- mock matching is
/// server-side, handles are only needed for call-count assertions.
///
/// Sync on purpose: httpmock's `When`/`Then` builders are `Rc`-based, so an
/// async fn touching them is `!Send` (`clippy::future_not_send`); the
/// blocking httpmock API manages its own runtime and is safe inside tokio
/// tests (the turnkey suite's established pattern).
fn issuance_mock_with_vault_mode(vault_mode: &'static str) -> httpmock::MockServer {
    let issuance = httpmock::MockServer::start();
    issuance.mock(move |when, then| {
        when.method(httpmock::Method::GET)
            .path_prefix("/tokenized-assets/")
            .path_suffix("/status");
        then.status(200).json_body(serde_json::json!({
            "underlying": "AAPL",
            "status": "enabled",
            "vault_mode": vault_mode,
        }));
    });
    issuance
}

/// Test-owned issuance stand-in for the orchestrator-mode flow: reports
/// `vault_mode: "orchestrator"` for every asset and answers every
/// authorization delivery with a 200, recording each delivery's
/// `(tokenization_request_id, body)`.
///
/// A plain axum handler rather than an httpmock matcher closure: httpmock
/// does not guarantee a single predicate evaluation per request (matching
/// and closest-match diagnostics can re-run predicates), so recording
/// inside a matcher could duplicate entries -- and duplicates cannot be
/// deduplicated away, because a byte-identical redelivery is a real retry
/// the assertions must see. The handler runs exactly once per received
/// request.
async fn spawn_recording_issuance() -> anyhow::Result<(url::Url, Arc<Mutex<Vec<(String, String)>>>)>
{
    use axum::extract::Path;

    let recordings = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&recordings);

    let app = axum::Router::new()
        .route(
            "/tokenized-assets/{underlying}/status",
            axum::routing::get(|Path(underlying): Path<String>| async move {
                axum::Json(serde_json::json!({
                    "underlying": underlying,
                    "status": "enabled",
                    "vault_mode": "orchestrator",
                }))
            }),
        )
        .route(
            "/internal/mints/{tokenization_request_id}/authorization",
            axum::routing::post(
                move |Path(tokenization_request_id): Path<String>, body: String| {
                    let recorder = Arc::clone(&recorder);
                    async move {
                        recorder
                            .lock()
                            .unwrap()
                            .push((tokenization_request_id, body));
                        axum::Json(serde_json::json!({
                            "issuer_request_id": "e2e-issuer-id",
                            "status": "authorized",
                        }))
                    }
                },
            ),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?).parse()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok((url, recordings))
}

/// Shared driver for the two mint-authorization flows: starts the infra,
/// deploys the digest-stub orchestrator, prepares and takes three
/// sell-equity orders against a bot configured with `[orchestrator]`
/// pointing at that stub and `[issuance]` at the given base URL, and waits
/// for the first mint to complete end to end. Which path the mint takes is
/// decided solely by the issuance service's `vault_mode` -- the
/// orchestrator config is deliberately present in BOTH flows.
async fn drive_mint_with_orchestrator_configured(
    issuance_base_url: url::Url,
) -> anyhow::Result<(
    TestInfra<impl alloy::providers::Provider + Clone>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
)> {
    let infra = TestInfra::start(vec![("AAPL", float!(110))], vec![("AAPL", float!(7.5))]).await?;

    // Stand-in orchestrator contract: a well-formed `mintAuthDigest` call
    // returns a constant digest; anything else reverts.
    let orchestrator_address = Address::repeat_byte(0x77);
    infra
        .base_chain
        .provider
        .anvil_set_code(
            orchestrator_address,
            digest_stub_bytecode(E2E_ORCHESTRATOR_DIGEST).into(),
        )
        .await?;

    let mut prepared_orders = Vec::new();
    for _ in 0..3 {
        prepared_orders.push(
            infra
                .base_chain
                .setup_order()
                .symbol("AAPL")
                .amount(float!(7.5))
                .price(float!(112))
                .direction(TakeDirection::SellEquity)
                .call()
                .await?,
        );
    }

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared_orders[0].input_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared_orders[0].output_vault_id)]);
    let ctx = build_rebalancing_ctx()
        .chain(&infra.base_chain)
        .broker(&infra.broker_service)
        .db_path(&infra.db_path)
        .deployment_block(current_block)
        .equity_tokens(&infra.equity_addresses)
        .equity_vault_ids(&equity_vault_ids)
        .cash_vault_id(cash_vault_id)
        .usdc_rebalancing(UsdcRebalancing::Disabled)
        .cash_rebalancing(OperationMode::Disabled)
        .wrapped_equity_recovery(OperationMode::Disabled)
        .redemption_wallet(REDEMPTION_WALLET)
        .issuance_base_url(issuance_base_url)
        .orchestrator(st0x_config::OrchestratorConfig {
            address: orchestrator_address,
        })
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(10)).await;

    for prepared in &prepared_orders {
        infra.base_chain.take_prepared_order(prepared).await?;
    }

    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "TokenizedEquityMintEvent::DepositedIntoRaindex",
        1,
        Duration::from_secs(120),
    )
    .await;

    Ok((infra, bot))
}

/// Orchestrator-mode mint (RAI-1243): the bot must sign a MintAuthV1 over
/// the orchestrator-reported digest BEFORE tokens arrive, deliver it to
/// issuance exactly once with its acknowledgement recorded, and the mint
/// must still complete end to end.
#[test_log::test(tokio::test)]
async fn equity_mint_orchestrator_mode_delivers_exactly_one_authorization() -> anyhow::Result<()> {
    // The recording issuance stand-in captures every delivery's
    // `(tokenization_request_id, body)` so the assertions below can
    // correlate deliveries to the completed mint (with three takes the
    // saga legitimately starts further mints, so global counts would race)
    // and verify the delivered signature.
    let (issuance_base_url, recorded_deliveries) = spawn_recording_issuance().await?;

    let (infra, mut bot) = drive_mint_with_orchestrator_configured(issuance_base_url).await?;

    let pool = connect_db(&infra.db_path).await?;
    let completed_mint_id = fetch_events_by_type(&pool, "TokenizedEquityMint")
        .await?
        .iter()
        .find(|event| event.event_type == "TokenizedEquityMintEvent::DepositedIntoRaindex")
        .map(|event| event.aggregate_id.clone())
        .expect("DepositedIntoRaindex must be persisted");

    // The acknowledgement always trails the mint: the saga's `Poll` holds
    // the per-aggregate lock for the whole Alpaca wait, so the delivery
    // job's `RecordMintAuthorizationDelivered` queues behind it and lands
    // after `TokensReceived`. Wait for the COMPLETED mint's own
    // acknowledgement -- a follow-up mint's ack could satisfy any global
    // count first.
    let ack_context = format!("MintAuthorizationDelivered for {completed_mint_id}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        crate::poll::sleep_or_crash(&mut bot, &ack_context).await;
        let (acknowledged,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE aggregate_id = ? \
             AND event_type = 'TokenizedEquityMintEvent::MintAuthorizationDelivered'",
        )
        .bind(&completed_mint_id)
        .fetch_one(&pool)
        .await?;
        if acknowledged >= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for {ack_context}"
        );
    }

    let events = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
    let mint_events: Vec<_> = events
        .iter()
        .filter(|event| event.aggregate_id == completed_mint_id)
        .collect();
    let position_of = |event_type: &str| {
        mint_events
            .iter()
            .position(|event| event.event_type == event_type)
    };

    // Signing is the deterministic gate: the saga signs before it starts
    // polling Alpaca, so the signed event must precede token receipt.
    let signed = position_of("TokenizedEquityMintEvent::MintAuthorizationSigned")
        .expect("MintAuthorizationSigned must be persisted");
    let received = position_of("TokenizedEquityMintEvent::TokensReceived")
        .expect("TokensReceived must be persisted");
    assert!(
        signed < received,
        "authorization must be signed before tokens arrive; \
         got signed={signed}, received={received}"
    );
    let signed_count = mint_events
        .iter()
        .filter(|event| event.event_type == "TokenizedEquityMintEvent::MintAuthorizationSigned")
        .count();
    assert_eq!(
        signed_count, 1,
        "exactly one authorization may be signed per mint"
    );

    // Correlate the wire deliveries to the completed mint via the path's
    // tokenization request id: exactly one delivery, carrying exactly the
    // persisted nonce and signature (retries would redeliver
    // byte-identically, but the happy path must not retry at all).
    let accepted = &mint_events[position_of("TokenizedEquityMintEvent::MintAccepted")
        .expect("MintAccepted must be persisted")];
    let tokenization_request_id = accepted.payload["MintAccepted"]["tokenization_request_id"]
        .as_str()
        .expect("MintAccepted must carry the tokenization request id")
        .to_owned();
    let signed_payload = &mint_events[signed].payload["MintAuthorizationSigned"];
    let recorded = recorded_deliveries.lock().unwrap().clone();
    let mint_deliveries: Vec<_> = recorded
        .iter()
        .filter(|(delivered_for, _)| *delivered_for == tokenization_request_id)
        .collect();
    assert_eq!(
        mint_deliveries.len(),
        1,
        "exactly one delivery may reach issuance for the completed mint; \
         all recorded deliveries: {recorded:?}"
    );

    // The delivered body must be the persisted authorization: a 32-byte
    // nonce, and a 65-byte signature by the bot wallet over exactly the
    // digest the stub orchestrator reports.
    let (_, delivered_body) = mint_deliveries[0];
    let delivered: serde_json::Value = serde_json::from_str(delivered_body)?;
    assert_eq!(
        delivered["nonce"], signed_payload["nonce"],
        "the delivered nonce must be the persisted one"
    );
    assert_eq!(
        delivered["signature"], signed_payload["signature"],
        "the delivered signature must be the persisted one"
    );
    let nonce_bytes = alloy::hex::decode(
        delivered["nonce"]
            .as_str()
            .expect("delivered nonce must be a hex string"),
    )?;
    assert_eq!(nonce_bytes.len(), 32, "the nonce must be 32 bytes");
    let signature_bytes = alloy::hex::decode(
        delivered["signature"]
            .as_str()
            .expect("delivered signature must be a hex string"),
    )?;
    assert_eq!(signature_bytes.len(), 65, "the signature must be 65 bytes");
    let recovered = Signature::from_raw(&signature_bytes)?
        .recover_address_from_prehash(&B256::from(E2E_ORCHESTRATOR_DIGEST))?;
    assert_eq!(
        recovered, infra.base_chain.owner,
        "the delivered signature must recover to the bot wallet over the \
         orchestrator-reported digest"
    );

    bot.abort();
    Ok(())
}

/// Vault-direct assets take the pre-authorization path untouched even when
/// an `[orchestrator]` section is configured and the contract is live: the
/// per-asset `vault_mode` from issuance -- not the config's presence -- is
/// what gates signing. No authorization is signed or delivered.
#[test_log::test(tokio::test)]
async fn equity_mint_vault_direct_mode_delivers_no_authorization() -> anyhow::Result<()> {
    let issuance = issuance_mock_with_vault_mode("vault_direct");
    let authorization_deliveries = issuance
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST)
                .path_prefix("/internal/mints/")
                .path_suffix("/authorization");
            then.status(200).json_body(serde_json::json!({
                "issuer_request_id": "e2e-issuer-id",
                "status": "authorized",
            }));
        })
        .await;

    let (infra, bot) =
        drive_mint_with_orchestrator_configured(issuance.base_url().parse()?).await?;

    authorization_deliveries.assert_calls_async(0).await;

    let pool = connect_db(&infra.db_path).await?;
    let events = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
    assert!(
        !events
            .iter()
            .any(|event| event.event_type == "TokenizedEquityMintEvent::MintAuthorizationSigned"),
        "a vault-direct mint must never sign an authorization"
    );

    bot.abort();
    Ok(())
}
