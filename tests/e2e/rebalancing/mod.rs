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

mod assertions;

use std::collections::HashMap;
use std::sync::Arc;

use alloy::providers::RootProvider;
use alloy::providers::ext::AnvilApi as _;

use st0x_evm::test_chain::evm_mapping_slot;
use st0x_hedge::OperationMode;

use self::assertions::*;

/// Control test: direct high-precision sell-side Raindex prices should not
/// break equity rebalancing. This avoids buy-side reciprocal math and verifies
/// that direct decimal price literals do not block the mint pipeline.
#[test_log::test(tokio::test)]
async fn equity_mint_handles_direct_high_precision_sell_price() -> anyhow::Result<()> {
    let onchain_price = Decimal::from_str("112.50000000000000000000000002")?;
    let broker_fill_price = dec!(110);
    let amount_per_trade = dec!(7.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

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
        .redemption_wallet(REDEMPTION_WALLET)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

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
        .amount(dec!(22.5))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(0))
        .expected_accumulated_short(dec!(22.5))
        .expected_net(dec!(0))
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
    let onchain_price = dec!(150.00);
    let broker_fill_price = dec!(148.00);
    let amount_per_trade = dec!(7.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

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
        .redemption_wallet(REDEMPTION_WALLET)
        .call()?;
    let mut bot = spawn_bot(ctx);

    // Let bot finish coordination before submitting takes.
    tokio::time::sleep(Duration::from_secs(8)).await;

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
        .amount(dec!(22.5))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(0))
        .expected_accumulated_short(dec!(22.5))
        .expected_net(dec!(0))
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
    let onchain_price = dec!(112.50);
    let broker_fill_price = dec!(113.60);
    let trade_amount = dec!(12.5);

    let infra =
        TestInfra::start(vec![("AAPL", broker_fill_price)], vec![("AAPL", dec!(20))]).await?;

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
        .redemption_wallet(REDEMPTION_WALLET)
        .call()?;

    let mut bot = spawn_bot(ctx);

    // Wait for bot's coordination phase before submitting takes.
    tokio::time::sleep(Duration::from_secs(8)).await;

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

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(trade_amount)
        .direction(TakeDirection::BuyEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(12.5))
        .expected_accumulated_short(dec!(0))
        .expected_net(dec!(0))
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
///
/// Run explicitly when validating the decimal-normalization fix:
/// `cargo test --test e2e -- --ignored --nocapture \
///   equity_redemption_buy_inv_repeating_reciprocal_regression`
#[ignore = "Diagnostic repro: run with --nocapture and inspect PrecisionLoss logs"]
#[test_log::test(tokio::test)]
async fn equity_redemption_buy_inv_repeating_reciprocal_regression() -> anyhow::Result<()> {
    let onchain_price = dec!(115);
    let broker_fill_price = dec!(113.57);
    let trade_amount = dec!(12.5);

    let infra =
        TestInfra::start(vec![("AAPL", broker_fill_price)], vec![("AAPL", dec!(20))]).await?;

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
    let extra_equity: U256 = parse_units("100", 18)?.into();
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
        .redemption_wallet(REDEMPTION_WALLET)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

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
        .expected_accumulated_short(dec!(0))
        .expected_net(dec!(0))
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
/// buy-side reciprocal in Rust `Decimal` and inject it as a Rainlang literal.
///
/// This uses the same rebalancing redemption setup as the `inv(price)` repro
/// but replaces the generated Rainlang expression with a direct reciprocal
/// string, matching the old harness path.
#[ignore = "Diagnostic repro: run with --nocapture and inspect PrecisionLoss logs"]
#[test_log::test(tokio::test)]
async fn equity_redemption_buy_literal_reciprocal_regression() -> anyhow::Result<()> {
    let onchain_price = dec!(112);
    let broker_fill_price = dec!(113.57);
    let trade_amount = dec!(12.5);

    let reciprocal_literal = (dec!(1.0) / onchain_price).to_string();
    let usdc_total = trade_amount * onchain_price;
    let max_amount_base: U256 = parse_units(&format!("{usdc_total:.6}"), 6)?.into();
    let rain_expression_override = format!("_ _: {max_amount_base} {reciprocal_literal};:;");

    let infra =
        TestInfra::start(vec![("AAPL", broker_fill_price)], vec![("AAPL", dec!(20))]).await?;

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
    let equity_vault_id = prepared.input_vault_id;
    let extra_equity: U256 = parse_units("100", 18)?.into();
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
        .redemption_wallet(REDEMPTION_WALLET)
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

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
        .expected_accumulated_short(dec!(0))
        .expected_net(dec!(0))
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
///   -> Initiated -> WithdrawalConfirmed -> BridgingInitiated
///   -> BridgeAttestationReceived -> Bridged -> DepositInitiated
///   -> DepositConfirmed
///
/// Infrastructure: Deploys CCTP contracts fresh on two plain Anvil chains
/// (no mainnet fork required). A background watcher auto-signs attestations
/// for `MessageSent` events using a test attester key.
#[test_log::test(tokio::test)]
async fn usdc_imbalance_triggers_alpaca_to_base() -> anyhow::Result<()> {
    let onchain_price = dec!(158.39);
    let broker_fill_price = dec!(155.00);
    let amount_per_trade = dec!(7.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;
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
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

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
        st0x_hedge::bindings::IOrderBookV6::IOrderBookV6Instance::new(
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

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(amount_per_trade * dec!(3))
        .direction(TakeDirection::BuyEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(amount_per_trade * dec!(3))
        .expected_accumulated_short(dec!(0))
        .expected_net(dec!(0))
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
    let onchain_price = dec!(158.39);
    let broker_fill_price = dec!(155.00);
    let amount_per_trade = dec!(7.5);

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;
    let cctp = CctpInfra::start(&infra).await?;

    // Pre-fund the USDC vault so the ratio stays below 0.6 before
    // takes (145k / 245k = 0.59) but exceeds after SellEquity fills
    // add ~3.6k USDC (148.6k / 245k = 0.61 > 0.6 threshold).
    let usdc_amount: U256 = parse_units("145000", 6)?.into();
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
        .call()?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

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

    let ethereum_usdc_balance_before_rebalance =
        crate::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
            .balanceOf(infra.base_chain.owner)
            .call()
            .await?;

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

    let expected_positions = [ExpectedPosition::builder()
        .symbol("AAPL")
        .amount(amount_per_trade * dec!(3))
        .direction(TakeDirection::SellEquity)
        .onchain_price(onchain_price)
        .broker_fill_price(broker_fill_price)
        .expected_accumulated_long(dec!(0))
        .expected_accumulated_short(amount_per_trade * dec!(3))
        .expected_net(dec!(0))
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

    bot.abort();
    Ok(())
}

/// Base wallet USDC balance detected by inventory poller.
///
/// When rebalancing is enabled, the conductor wires the Base wallet
/// into the inventory polling service. Each polling cycle reads the
/// wallet's USDC balance via `balanceOf` at the canonical Base USDC
/// address and emits a `BaseWalletCash` snapshot event.
///
/// Expected CQRS event:
/// - `InventorySnapshot`: BaseWalletCash (usdc_balance = funded amount)
///
/// Infrastructure: Uses the existing Base Anvil from TestInfra with
/// `DeployableERC20` bytecode already deployed at `USDC_BASE`. The bot
/// owner's balance is set via `anvil_set_storage_at`. No CCTP contracts
/// or trade activity required -- pure polling verification.
#[test_log::test(tokio::test)]
async fn base_wallet_usdc_balance_detected_by_inventory_poller() -> anyhow::Result<()> {
    let infra = TestInfra::start(vec![("AAPL", dec!(150))], vec![]).await?;

    let base_wallet: Arc<dyn st0x_evm::Wallet<Provider = RootProvider>> =
        Arc::new(TestWallet::new(
            &infra.base_chain.owner_key,
            infra.base_chain.endpoint().parse()?,
            1,
        )?);

    let alpaca_auth = st0x_execution::AlpacaBrokerApiCtx {
        api_key: st0x_execution::alpaca_broker_api::TEST_API_KEY.to_owned(),
        api_secret: st0x_execution::alpaca_broker_api::TEST_API_SECRET.to_owned(),
        account_id: st0x_execution::AlpacaAccountId::new(uuid::uuid!(
            "904837e3-3b76-47ec-b432-046db621571b"
        )),
        mode: Some(st0x_execution::AlpacaBrokerApiMode::Mock(
            infra.broker_service.base_url(),
        )),
        asset_cache_ttl: Duration::from_secs(3600),
        time_in_force: st0x_execution::TimeInForce::Day,
    };

    let rebalancing_ctx = st0x_hedge::RebalancingCtx::with_wallets()
        .equity(st0x_hedge::ImbalanceThreshold::new(dec!(0.5), dec!(0.1))?)
        .usdc(UsdcRebalancing::Disabled)
        .redemption_wallet(alloy::primitives::Address::random())
        .alpaca_broker_auth(alpaca_auth.clone())
        .base_wallet(base_wallet.clone())
        .ethereum_wallet(base_wallet)
        .call();

    let broker_ctx = st0x_hedge::config::BrokerCtx::AlpacaBrokerApi(alpaca_auth);

    let usdc_vault_id = infra
        .base_chain
        .create_usdc_vault(parse_units("1000", 6)?.into())
        .await?;

    let funded_amount: U256 = parse_units("10000", 6)?.into();
    let balance_slot = evm_mapping_slot(infra.base_chain.owner, 0);
    infra
        .base_chain
        .provider
        .anvil_set_storage_at(
            crate::base_chain::USDC_BASE,
            balance_slot,
            funded_amount.into(),
        )
        .await?;

    let mut assets = infra.assets_config();
    assets.cash = Some(st0x_hedge::config::CashAssetConfig {
        vault_id: Some(usdc_vault_id),
        rebalancing: OperationMode::Disabled,
        operational_limit: None,
    });

    let current_block = infra.base_chain.provider.get_block_number().await?;

    let ctx = st0x_hedge::config::Ctx::for_test()
        .database_url(infra.db_path.display().to_string())
        .ws_rpc_url(infra.base_chain.ws_endpoint()?)
        .orderbook(infra.base_chain.orderbook)
        .deployment_block(current_block)
        .broker(broker_ctx)
        .trading_mode(st0x_hedge::TradingMode::Rebalancing(Box::new(
            rebalancing_ctx,
        )))
        .assets(assets)
        .inventory_poll_interval(2)
        .call()?;

    let mut bot = spawn_bot(ctx);

    crate::poll::poll_for_events(
        &mut bot,
        &infra.db_path,
        "InventorySnapshotEvent::BaseWalletCash",
        1,
    )
    .await;

    let pool = crate::poll::connect_db(&infra.db_path).await?;
    let events = crate::poll::fetch_events_by_type(&pool, "InventorySnapshot").await?;
    let base_wallet_cash_event = events
        .iter()
        .find(|event| event.event_type == "InventorySnapshotEvent::BaseWalletCash")
        .expect("Expected BaseWalletCash event");

    let usdc_balance_str = base_wallet_cash_event
        .payload
        .get("BaseWalletCash")
        .and_then(|val| val.get("usdc_balance"))
        .and_then(|val| val.as_str())
        .expect("BaseWalletCash payload missing usdc_balance");
    let usdc_balance: Decimal = usdc_balance_str.parse()?;

    assert_eq!(
        usdc_balance,
        dec!(10000),
        "Base wallet USDC balance should match funded amount"
    );

    pool.close().await;
    bot.abort();
    Ok(())
}
