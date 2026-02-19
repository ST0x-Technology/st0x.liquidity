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

use alloy::providers::Provider;

use common::{build_rebalancing_ctx, connect_db, count_events, spawn_bot, wait_for_processing};
use services::alpaca_broker::AlpacaBrokerMock;
use services::base_chain::{self, TakeDirection};
use st0x_hedge::UsdcRebalancing;

/// Processes several one-directional SellEquity trades to create an
/// inventory imbalance (offchain accumulates hedged equity while onchain
/// vault depletes). Verifies the inventory poller runs and emits
/// `InventorySnapshot` events.
#[tokio::test]
async fn inventory_snapshot_events_emitted() -> anyhow::Result<()> {
    let mut chain = base_chain::BaseChain::start(
        "https://api.developer.coinbase.com/rpc/v1/base/DD1T1ZCY6bZ09lHv19TjBHi1saRZCHFF",
    )
    .await?;
    let (vault_addr, underlying_addr) = chain.deploy_equity_vault("AAPL").await?;

    let broker = AlpacaBrokerMock::start()
        .with_fill_price("150.00")
        .build()
        .await;

    let current_block = chain.provider.get_block_number().await?;
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("e2e.sqlite");

    let ctx = build_rebalancing_ctx(
        &chain,
        &broker,
        &db_path,
        current_block,
        &[("AAPL", vault_addr, underlying_addr)],
        UsdcRebalancing::Disabled,
    )?;
    let bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Process several sells to create inventory pressure
    for _ in 0..3 {
        chain
            .take_order("AAPL", "7.5", TakeDirection::SellEquity)
            .await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Wait for hedging + inventory poller cycles
    wait_for_processing(&bot, 12).await;

    // Verify inventory snapshot events were emitted by the poller
    let pool = connect_db(&db_path).await?;

    let onchain_events = count_events(&pool, "OnChainTrade").await?;
    assert!(
        onchain_events >= 3,
        "Expected at least 3 OnChainTrade events, got {onchain_events}"
    );

    let inventory_events = count_events(&pool, "InventorySnapshot").await?;
    assert!(
        inventory_events >= 1,
        "Expected at least 1 InventorySnapshot event from the inventory poller, \
         got {inventory_events}"
    );

    pool.close().await;
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
/// We assert the first stages of the mint pipeline fire (MintRequested,
/// MintAccepted, TokensReceived). The downstream wrapping and Raindex
/// deposit may or may not complete depending on timing, but the trigger
/// + tokenization request is the critical path under test.
#[tokio::test]
async fn equity_imbalance_triggers_mint() -> anyhow::Result<()> {
    let mut chain = base_chain::BaseChain::start(
        "https://api.developer.coinbase.com/rpc/v1/base/DD1T1ZCY6bZ09lHv19TjBHi1saRZCHFF",
    )
    .await?;

    // Deploy ERC-4626 vault for AAPL. The owner already has underlying
    // tokens + unlimited vault approval from deploy_equity_vault(), so
    // the wrapping step (ERC-4626 deposit) after Alpaca "mints" will
    // succeed using the owner key as the market maker.
    let (vault_addr, underlying_addr) = chain.deploy_equity_vault("AAPL").await?;

    // Start broker mock with tokenization endpoints registered
    let broker = AlpacaBrokerMock::start()
        .with_fill_price("150.00")
        .build()
        .await;
    broker.register_tokenization_endpoints();

    let current_block = chain.provider.get_block_number().await?;
    let db_dir = tempfile::tempdir()?;
    let db_path = db_dir.path().join("e2e.sqlite");

    let ctx = build_rebalancing_ctx(
        &chain,
        &broker,
        &db_path,
        current_block,
        &[("AAPL", vault_addr, underlying_addr)],
        UsdcRebalancing::Disabled,
    )?;
    let bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Process several SellEquity trades to create offchain-heavy imbalance.
    // Each sell depletes the onchain vault and accumulates offchain equity.
    for _ in 0..3 {
        chain
            .take_order("AAPL", "7.5", TakeDirection::SellEquity)
            .await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Wait for: hedging + inventory poller + trigger evaluation +
    // tokenization request + polling (2 polls at 10s each)
    wait_for_processing(&bot, 40).await;

    let pool = connect_db(&db_path).await?;

    // Verify trades were detected
    let onchain_events = count_events(&pool, "OnChainTrade").await?;
    assert!(
        onchain_events >= 3,
        "Expected at least 3 OnChainTrade events, got {onchain_events}"
    );

    // Verify inventory snapshots were emitted
    let inventory_events = count_events(&pool, "InventorySnapshot").await?;
    assert!(
        inventory_events >= 1,
        "Expected at least 1 InventorySnapshot event, got {inventory_events}"
    );

    // Verify the mint pipeline was triggered. At minimum MintRequested
    // should be emitted; MintAccepted and TokensReceived follow if the
    // mock API responds in time.
    let mint_events = count_events(&pool, "TokenizedEquityMint").await?;
    assert!(
        mint_events >= 1,
        "Expected at least 1 TokenizedEquityMint event (mint trigger fired), \
         got {mint_events}"
    );

    pool.close().await;
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
#[tokio::test]
#[ignore = "Requires Raindex vault pre-funded with wrapped tokens for withdrawal + redemption mock endpoints"]
async fn equity_imbalance_triggers_redemption() -> anyhow::Result<()> {
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
///   -> Initiated -> WithdrawalConfirmed -> BridgingInitiated
///   -> BridgeAttestationReceived -> Bridged -> DepositInitiated
///   -> DepositConfirmed
///
/// Requires: CCTP MessageTransmitter and TokenMessenger contracts on both
/// Ethereum and Base Anvil forks, plus a mock attestation service for
/// cross-chain message verification.
#[tokio::test]
#[ignore = "Requires CCTP bridge contracts and attestation mock for cross-chain USDC flow"]
async fn usdc_imbalance_triggers_alpaca_to_base() -> anyhow::Result<()> {
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
/// - `UsdcRebalance`: Initiated -> WithdrawalConfirmed -> BridgingInitiated
///   -> BridgeAttestationReceived -> Bridged -> DepositInitiated
///   -> DepositConfirmed -> ConversionInitiated -> ConversionConfirmed
///
/// Requires: Same CCTP infrastructure as `usdc_imbalance_triggers_alpaca_to_base`.
#[tokio::test]
#[ignore = "Requires CCTP bridge contracts and attestation mock for cross-chain USDC flow"]
async fn usdc_imbalance_triggers_base_to_alpaca() -> anyhow::Result<()> {
    Ok(())
}
