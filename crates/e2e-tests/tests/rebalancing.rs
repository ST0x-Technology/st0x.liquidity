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

use std::time::Duration;

use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{B256, U256, utils::parse_units};
use alloy::providers::Provider;
use alloy::providers::ext::AnvilApi as _;
use alloy::signers::local::PrivateKeySigner;
use rust_decimal_macros::dec;
use tokio::task::JoinHandle;

use e2e_tests::common::{
    ExpectedPosition, assert_equity_rebalancing_flow, build_rebalancing_ctx,
    build_usdc_rebalancing_ctx, spawn_bot, wait_for_processing,
};
use e2e_tests::services::TestInfra;
use e2e_tests::services::alpaca_tokenization::REDEMPTION_WALLET;
use e2e_tests::services::base_chain::{TakeDirection, USDC_BASE};
use e2e_tests::services::cctp_contracts;
use e2e_tests::services::ethereum_chain::USDC_ETHEREUM;
use st0x_execution::Symbol;
use st0x_hedge::UsdcRebalancing;
use st0x_hedge::bindings::TestMintBurnToken;

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

    assert_equity_rebalancing_flow(
        &expected_positions,
        &take_results,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path,
        e2e_tests::common::EquityRebalanceType::Mint {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
        },
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

    let redemption_wallet_balance =
        e2e_tests::services::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;

    assert_equity_rebalancing_flow(
        &expected_positions,
        &[_take_result],
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.db_path,
        e2e_tests::common::EquityRebalanceType::Redeem {
            symbol: "AAPL",
            tokenization: &infra.tokenization_service,
            redemption_wallet_balance,
        },
    )
    .await?;

    bot.abort();
    Ok(())
}

// ── USDC CCTP rebalancing test infrastructure ────────────────────────

/// CCTP layer that sits on top of [`TestInfra`].
///
/// Deploys CCTP V2 contracts on a second (Ethereum) Anvil chain and on
/// the existing Base chain from `TestInfra`, links them, replaces Base
/// USDC with a mint/burn token, mints USDC on both chains, registers
/// broker wallet endpoints, and starts the attestation watcher.
struct CctpInfra {
    ethereum_endpoint: String,
    attestation_base_url: String,
    token_messenger: alloy::primitives::Address,
    message_transmitter: alloy::primitives::Address,

    // Kept alive so resources aren't dropped while the test runs.
    _ethereum_anvil: AnvilInstance,
    _attestation_watcher: JoinHandle<()>,
}

impl CctpInfra {
    /// Deploys CCTP infrastructure on top of an existing [`TestInfra`].
    async fn start<BP: Provider + Clone>(infra: &TestInfra<BP>) -> anyhow::Result<Self> {
        let ethereum_anvil = Anvil::new().chain_id(1u64).spawn();
        let ethereum_endpoint = ethereum_anvil.endpoint();

        // Use Anvil account #2 as CCTP deployer to avoid nonce collisions
        // with account #0 (owner), whose provider is already used by
        // BaseChain for contract deployments and vault operations. Each
        // cctp_contracts function creates a fresh provider from key+endpoint,
        // so sharing the owner key causes "nonce too low" when Base's
        // block_time(1) hasn't yet mined the owner's pending transactions.
        let cctp_deployer_key = B256::from_slice(&ethereum_anvil.keys()[2].to_bytes());
        let cctp_deployer = PrivateKeySigner::from_bytes(&cctp_deployer_key)?.address();

        // Fund the CCTP deployer on Base (Ethereum Anvil pre-funds all
        // default accounts, but Base's Anvil instance is separate).
        let hundred_eth: U256 = parse_units("100", 18)?.into();
        infra
            .base_chain
            .provider
            .anvil_set_balance(cctp_deployer, hundred_eth)
            .await?;

        let attester_key = B256::random();
        let attester_address = PrivateKeySigner::from_bytes(&attester_key)?.address();

        let mut ethereum_cctp = cctp_contracts::deploy_cctp_on_chain(
            &ethereum_endpoint,
            &cctp_deployer_key,
            0, // Ethereum domain
            attester_address,
        )
        .await?;

        let mut base_cctp = cctp_contracts::deploy_cctp_on_chain(
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            6, // Base domain
            attester_address,
        )
        .await?;

        let eth_provider = alloy::providers::ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await?;

        // The bot uses USDC at canonical addresses, not the freshly deployed
        // MockMintBurnToken. Replace bytecode at those addresses with
        // TestMintBurnToken so CCTP can mint/burn there.

        // Ethereum: place TestMintBurnToken at USDC_ETHEREUM
        eth_provider
            .anvil_set_code(USDC_ETHEREUM, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await?;

        cctp_contracts::set_max_burn_amount(
            &ethereum_endpoint,
            &cctp_deployer_key,
            ethereum_cctp.token_minter,
            USDC_ETHEREUM,
            U256::from(1_000_000_000_000u64),
        )
        .await?;

        ethereum_cctp.usdc = USDC_ETHEREUM;

        // Base: place TestMintBurnToken at USDC_BASE. Storage slots
        // (name, symbol, decimals, balances) were already set by
        // deploy_usdc_at_base in TestInfra::start and persist across
        // anvil_set_code calls.
        infra
            .base_chain
            .provider
            .anvil_set_code(USDC_BASE, TestMintBurnToken::DEPLOYED_BYTECODE.clone())
            .await?;

        cctp_contracts::set_max_burn_amount(
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            base_cctp.token_minter,
            USDC_BASE,
            U256::from(1_000_000_000_000u64),
        )
        .await?;

        base_cctp.usdc = USDC_BASE;

        cctp_contracts::link_chains(
            &ethereum_endpoint,
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            &ethereum_cctp,
            &base_cctp,
        )
        .await?;

        // MockMintBurnToken.mint() has no access control, so any key works.
        let mint_amount = U256::from(1_000_000_000_000u64); // 1M USDC

        cctp_contracts::mint_usdc(
            &infra.base_chain.endpoint(),
            &cctp_deployer_key,
            USDC_BASE,
            infra.base_chain.owner,
            mint_amount,
        )
        .await?;

        cctp_contracts::mint_usdc(
            &ethereum_endpoint,
            &cctp_deployer_key,
            USDC_ETHEREUM,
            infra.base_chain.owner,
            mint_amount,
        )
        .await?;

        // Register broker wallet endpoints for USDC pipeline
        infra
            .broker_service
            .register_wallet_endpoints(infra.base_chain.owner);

        // USDC/USD conversion is a 1:1 stablecoin pair on Alpaca
        infra
            .broker_service
            .set_symbol_fill_price(Symbol::force_new("USDCUSD".to_string()), dec!(1.0));

        // Start attestation watcher connecting both chain providers
        let eth_watcher_provider = alloy::providers::ProviderBuilder::new()
            .connect(&ethereum_endpoint)
            .await?;
        let base_watcher_provider = alloy::providers::ProviderBuilder::new()
            .connect(&infra.base_chain.endpoint())
            .await?;
        let attestation_watcher = infra.attestation_service.start_watcher(
            eth_watcher_provider,
            base_watcher_provider,
            attester_key,
        );

        Ok(Self {
            ethereum_endpoint,
            attestation_base_url: infra.attestation_service.base_url(),
            token_messenger: base_cctp.token_messenger,
            message_transmitter: base_cctp.message_transmitter,
            _ethereum_anvil: ethereum_anvil,
            _attestation_watcher: attestation_watcher,
        })
    }

    fn cctp_overrides(&self) -> e2e_tests::common::CctpOverrides {
        e2e_tests::common::CctpOverrides {
            attestation_base_url: self.attestation_base_url.clone(),
            token_messenger: self.token_messenger,
            message_transmitter: self.message_transmitter,
        }
    }
}

// ── USDC CCTP rebalancing tests ──────────────────────────────────────

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
/// Infrastructure: Deploys CCTP contracts fresh on two plain Anvil chains
/// (no mainnet fork required). A background watcher auto-signs attestations
/// for `MessageSent` events using a test attester key.
#[test_log::test(tokio::test)]
async fn usdc_imbalance_triggers_alpaca_to_base() -> anyhow::Result<()> {
    let infra = TestInfra::start(vec![("AAPL", dec!(150.00))], vec![]).await?;
    let cctp = CctpInfra::start(&infra).await?;

    // Small USDC vault — deposit destination for bridged USDC
    let usdc_amount: U256 = parse_units("10000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

    // Set up BuyEquity orders BEFORE bot starts
    let mut prepared_orders = Vec::new();
    for _ in 0..3 {
        prepared_orders.push(
            infra
                .base_chain
                .setup_order()
                .symbol("AAPL")
                .amount(dec!(7.5))
                .price(dec!(150.00))
                .direction(TakeDirection::BuyEquity)
                .call()
                .await?,
        );
    }

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_usdc_rebalancing_ctx(
        &infra.base_chain,
        &cctp.ethereum_endpoint,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
        usdc_vault_id,
        cctp.cctp_overrides(),
    )?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

    // AFTER bot starts: take orders from taker account
    let mut take_results = Vec::new();
    for prepared in &prepared_orders {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    wait_for_processing(&mut bot, 45).await;

    // ── Layer 1: Full hedging pipeline ───────────────────────────────
    let expected_positions = [ExpectedPosition {
        symbol: "AAPL",
        amount: dec!(22.5),
        direction: TakeDirection::BuyEquity,
        onchain_price: dec!(150.00),
        broker_fill_price: dec!(150.00),
        expected_accumulated_long: dec!(22.5),
        expected_accumulated_short: dec!(0),
        expected_net: dec!(0),
    }];

    e2e_tests::common::assert_usdc_rebalancing_flow(
        &expected_positions,
        &take_results,
        &infra.base_chain.provider,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.attestation_service,
        &infra.db_path,
        usdc_vault_id,
        e2e_tests::common::UsdcRebalanceType::AlpacaToBase,
    )
    .await?;

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
/// - `UsdcRebalance`: Initiated -> WithdrawalConfirmed -> BridgingInitiated
///   -> BridgeAttestationReceived -> Bridged -> DepositInitiated
///   -> DepositConfirmed -> ConversionInitiated -> ConversionConfirmed
///
/// Infrastructure: Same as `usdc_imbalance_triggers_alpaca_to_base` but the
/// Raindex USDC vault is pre-funded so the bot can withdraw from it.
#[test_log::test(tokio::test)]
async fn usdc_imbalance_triggers_base_to_alpaca() -> anyhow::Result<()> {
    let infra = TestInfra::start(vec![("AAPL", dec!(150.00))], vec![]).await?;
    let cctp = CctpInfra::start(&infra).await?;

    // Pre-fund the USDC vault with enough to create a TooMuchOnchain
    // imbalance. The broker mock starts with $100k offchain cash.
    // After hedging 3 SellEquity trades (7.5 * $150 = $1,125 each),
    // offchain drops to ~$96.6k. Onchain needs to exceed
    // target+deviation (60%) of total to trigger BaseToAlpaca.
    // $200k onchain / ($200k + $96.6k) = ~0.67 > 0.6 threshold.
    let usdc_amount: U256 = parse_units("200000", 6)?.into();
    let usdc_vault_id = infra.base_chain.create_usdc_vault(usdc_amount).await?;

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
                .amount(dec!(7.5))
                .price(dec!(150.00))
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
    let _deposit_watcher = infra.broker_service.start_deposit_watcher(
        eth_deposit_provider,
        USDC_ETHEREUM,
        infra.base_chain.owner,
    );

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let ctx = build_usdc_rebalancing_ctx(
        &infra.base_chain,
        &cctp.ethereum_endpoint,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
        usdc_vault_id,
        cctp.cctp_overrides(),
    )?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

    // AFTER bot starts: take orders from taker account
    let mut take_results = Vec::new();
    for prepared in &prepared_orders {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    wait_for_processing(&mut bot, 80).await;

    // ── Layer 1: Full hedging pipeline ───────────────────────────────
    let expected_positions = [ExpectedPosition {
        symbol: "AAPL",
        amount: dec!(22.5),
        direction: TakeDirection::SellEquity,
        onchain_price: dec!(150.00),
        broker_fill_price: dec!(150.00),
        expected_accumulated_long: dec!(0),
        expected_accumulated_short: dec!(22.5),
        expected_net: dec!(0),
    }];

    e2e_tests::common::assert_usdc_rebalancing_flow(
        &expected_positions,
        &take_results,
        &infra.base_chain.provider,
        infra.base_chain.orderbook_addr,
        infra.base_chain.owner,
        &infra.broker_service,
        &infra.attestation_service,
        &infra.db_path,
        usdc_vault_id,
        e2e_tests::common::UsdcRebalanceType::BaseToAlpaca,
    )
    .await?;

    bot.abort();
    Ok(())
}
