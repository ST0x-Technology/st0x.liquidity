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

mod utils;

use self::utils::*;

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
    let ctx = build_rebalancing_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
        UsdcRebalancing::Disabled,
        REDEMPTION_WALLET,
    )?;
    let mut bot = spawn_bot(ctx);

    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut take_results = Vec::new();
    for prepared in &prepared_orders {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
        tokio::time::sleep(Duration::from_secs(3)).await;
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
        .orderbook_addr(infra.base_chain.orderbook_addr)
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

    let ctx = build_rebalancing_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
        UsdcRebalancing::Disabled,
        REDEMPTION_WALLET,
    )?;
    let mut bot = spawn_bot(ctx);

    // Let bot finish coordination before submitting takes.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Take orders from taker account (separate nonces).
    let mut take_results = Vec::new();
    for prepared in &prepared_orders {
        take_results.push(infra.base_chain.take_prepared_order(prepared).await?);
        tokio::time::sleep(Duration::from_secs(3)).await;
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
        .orderbook_addr(infra.base_chain.orderbook_addr)
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

    // Extra equity tips the onchain ratio above 0.6 threshold -> Redemption.
    let vault_addr = infra.equity_addresses[0].1;
    let underlying_addr = infra.equity_addresses[0].2;
    let redemption_wallet_balance_before =
        e2e_tests::services::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;
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
        REDEMPTION_WALLET,
    )?;
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
        e2e_tests::services::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&[take_result])
        .provider(&infra.base_chain.provider)
        .orderbook_addr(infra.base_chain.orderbook_addr)
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
/// `cargo test -p e2e-tests --test rebalancing -- --ignored --nocapture \
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
        e2e_tests::services::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
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

    let ctx = build_rebalancing_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
        UsdcRebalancing::Disabled,
        REDEMPTION_WALLET,
    )?;
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
        e2e_tests::services::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&[take_result])
        .provider(&infra.base_chain.provider)
        .orderbook_addr(infra.base_chain.orderbook_addr)
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
        e2e_tests::services::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
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

    let ctx = build_rebalancing_ctx(
        &infra.base_chain,
        &infra.broker_service,
        &infra.db_path,
        current_block,
        &infra.equity_addresses,
        UsdcRebalancing::Disabled,
        REDEMPTION_WALLET,
    )?;
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
        e2e_tests::services::base_chain::IERC20::new(underlying_addr, &infra.base_chain.provider)
            .balanceOf(REDEMPTION_WALLET)
            .call()
            .await?;

    assert_equity_rebalancing_flow()
        .expected_positions(&expected_positions)
        .take_results(&[take_result])
        .provider(&infra.base_chain.provider)
        .orderbook_addr(infra.base_chain.orderbook_addr)
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

    let usdc_vault_balance_before_rebalance =
        st0x_hedge::bindings::IOrderBookV6::IOrderBookV6Instance::new(
            infra.base_chain.orderbook_addr,
            &infra.base_chain.provider,
        )
        .vaultBalance2(
            infra.base_chain.owner,
            e2e_tests::services::base_chain::USDC_BASE,
            usdc_vault_id,
        )
        .call()
        .await?;
    let ethereum_usdc_balance_before_rebalance =
        e2e_tests::services::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
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
        e2e_tests::services::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
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
        .orderbook_addr(infra.base_chain.orderbook_addr)
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
    let _deposit_watcher = infra.broker_service.start_deposit_watcher(
        eth_deposit_provider,
        USDC_ETHEREUM,
        infra.base_chain.owner,
    );

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

    let usdc_vault_balance_before_rebalance =
        st0x_hedge::bindings::IOrderBookV6::IOrderBookV6Instance::new(
            infra.base_chain.orderbook_addr,
            &infra.base_chain.provider,
        )
        .vaultBalance2(
            infra.base_chain.owner,
            e2e_tests::services::base_chain::USDC_BASE,
            usdc_vault_id,
        )
        .call()
        .await?;
    let ethereum_usdc_balance_before_rebalance =
        e2e_tests::services::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
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
        e2e_tests::services::base_chain::IERC20::new(USDC_ETHEREUM, &eth_balance_provider)
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
        .orderbook_addr(infra.base_chain.orderbook_addr)
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
