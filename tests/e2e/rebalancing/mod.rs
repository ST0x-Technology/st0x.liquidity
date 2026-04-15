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

use std::collections::HashMap;

use st0x_execution::SharesBlockchain;
use st0x_hedge::OperationMode;

use self::assertions::*;

use st0x_float_macro::float;

/// Control test: direct high-precision sell-side Raindex prices should not
/// break equity rebalancing. This avoids buy-side reciprocal math and verifies
/// that direct decimal price literals do not block the mint pipeline.

#[test_log::test(tokio::test)]
async fn equity_mint_handles_direct_high_precision_sell_price() -> anyhow::Result<()> {
    let onchain_price = float!(&"112.50000000000000000000000002".to_string());
    let broker_fill_price = float!(110);
    let amount_per_trade = float!(7.5);

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
        .redemption_wallet(REDEMPTION_WALLET)
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
        .redemption_wallet(REDEMPTION_WALLET)
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
        .redemption_wallet(REDEMPTION_WALLET)
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
        .redemption_wallet(REDEMPTION_WALLET)
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
///   -> Initiated -> WithdrawalConfirmed -> BridgingInitiated
///   -> BridgeAttestationReceived -> Bridged -> DepositInitiated
///   -> DepositConfirmed
///
/// Infrastructure: Deploys CCTP contracts fresh on two plain Anvil chains
/// (no mainnet fork required). A background watcher auto-signs attestations
/// for `MessageSent` events using a test attester key.
#[test_log::test(tokio::test)]
async fn usdc_imbalance_triggers_alpaca_to_base() -> anyhow::Result<()> {
    let onchain_price = float!(158.39);
    let broker_fill_price = float!(155.00);
    let amount_per_trade = float!(7.5);
    let expected_alpaca_wallet_balance = float!(1250.75);

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

    let current_block = infra.base_chain.provider.get_block_number().await?;
    infra
        .broker_service
        .set_wallet_usdc_balance(expected_alpaca_wallet_balance);

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

    let total_amount = (amount_per_trade * float!(3)).unwrap();

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
    assert_alpaca_wallet_usdc_event(&infra.db_path, expected_alpaca_wallet_balance).await?;

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
    let onchain_price = float!(158.39);
    let broker_fill_price = float!(155.00);
    let amount_per_trade = float!(7.5);

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

    let total_amount = (amount_per_trade * float!(3)).unwrap();

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
        .redemption_wallet(REDEMPTION_WALLET)
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

    // Wait for additional poll cycles after rejection to verify no second
    // redemption is triggered. The bot polls inventory every ~15s; two
    // full cycles ensures the InflightEquity snapshot has run post-rejection
    // and the sticky mechanism has been exercised.
    tokio::time::sleep(Duration::from_secs(35)).await;

    let pool = connect_db(&infra.db_path).await?;

    // Verify the redemption event sequence includes RedemptionRejected.
    let redeem_events = fetch_events_by_type(&pool, "EquityRedemption").await?;
    assert_event_subsequence(
        &redeem_events,
        &[
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            "EquityRedemptionEvent::TokensUnwrapped",
            "EquityRedemptionEvent::TokensSent",
            "EquityRedemptionEvent::Detected",
            "EquityRedemptionEvent::RedemptionRejected",
        ],
    );

    // Verify that InflightEquity snapshot events were emitted, proving the
    // poll cycle ran and the bot processed the pending redemption request.
    // After rejection, the request disappears from pending -- the sticky
    // marker must prevent the InflightEquity snapshot from zeroing inflight.
    let snapshot_events = fetch_events_by_type(&pool, "InventorySnapshot").await?;
    let inflight_poll_count = snapshot_events
        .iter()
        .filter(|event| event.event_type == "InventorySnapshotEvent::InflightEquity")
        .count();
    assert!(
        inflight_poll_count >= 1,
        "Expected at least 1 InflightEquity snapshot event (proving the poll ran and \
         the sticky mechanism was exercised), got {inflight_poll_count}",
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

    let infra = TestInfra::start(vec![("AAPL", broker_fill_price)], vec![]).await?;

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
        .redemption_wallet(REDEMPTION_WALLET)
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
    let foreign_wallet = alloy::primitives::Address::random();
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

    // Verify exactly one mint aggregate completed (the foreign request
    // didn't cause additional mint operations).
    let mint_events = fetch_events_by_type(&pool, "TokenizedEquityMint").await?;
    assert_event_subsequence(
        &mint_events,
        &[
            "TokenizedEquityMintEvent::MintRequested",
            "TokenizedEquityMintEvent::MintAccepted",
            "TokenizedEquityMintEvent::TokensReceived",
            "TokenizedEquityMintEvent::TokensWrapped",
            "TokenizedEquityMintEvent::DepositedIntoRaindex",
        ],
    );

    // Verify only one mint aggregate exists (no spurious second mint
    // triggered by inflated inflight from the foreign request).
    let mint_aggregate_ids: std::collections::HashSet<&str> = mint_events
        .iter()
        .map(|event| event.aggregate_id.as_str())
        .collect();
    assert_eq!(
        mint_aggregate_ids.len(),
        1,
        "Expected exactly 1 mint aggregate (foreign wallet request should be filtered), \
         got {}: {mint_aggregate_ids:?}",
        mint_aggregate_ids.len(),
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

/// Inflight polling picks up pending tokenization requests end-to-end.
///
/// The conductor's inventory poller calls `list_pending_requests()` on
/// every poll cycle. When pending requests exist (matching the conductor's
/// wallet), the poller aggregates them into an `InflightEquity` snapshot
/// command, which the CQRS aggregate turns into an `InflightEquity` event.
///
/// This test injects a pending mint request into the tokenization mock,
/// starts the bot, and verifies that `InflightEquity` events are emitted
/// with non-empty mint data -- proving the full pipeline from HTTP poll
/// through CQRS event emission works end-to-end.
#[test_log::test(tokio::test)]
async fn inflight_polling_emits_events_for_pending_requests() -> anyhow::Result<()> {
    let broker_fill_price = float!("150.00");
    let onchain_price = float!("150.00");

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        // No offchain positions — avoids rebalancing triggering a real
        // mint that could satisfy the assertion instead of the injected
        // pending request.
        vec![],
    )
    .await?;

    // We need at least one order set up so the vault registry gets seeded
    // and the inventory poller has valid vault IDs to query.
    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!("5"))
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // Inject a pending mint request with the bot's wallet address BEFORE
    // starting the bot. The first inventory poll should pick this up.
    infra.tokenization_service.inject_pending_request(
        "AAPL",
        float!("25"),
        infra.base_chain.owner,
        TokenizationRequestType::Mint,
    );

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
        .redemption_wallet(REDEMPTION_WALLET)
        .call()?;

    let mut bot = spawn_bot(ctx);

    // Wait for at least one InflightEquity event to be emitted.
    poll_for_events_with_timeout(
        &mut bot,
        &infra.db_path,
        "InventorySnapshotEvent::InflightEquity",
        1,
        Duration::from_secs(30),
    )
    .await;

    let pool = connect_db(&infra.db_path).await?;

    // Verify InflightEquity events were emitted.
    let snapshot_events = fetch_events_by_type(&pool, "InventorySnapshot").await?;
    let inflight_events: Vec<_> = snapshot_events
        .iter()
        .filter(|event| event.event_type == "InventorySnapshotEvent::InflightEquity")
        .collect();
    assert!(
        !inflight_events.is_empty(),
        "Expected at least 1 InflightEquity event from polling"
    );

    // Verify the first InflightEquity event payload contains the injected
    // pending mint for AAPL with the exact quantity we injected (25 shares).
    let payload = &inflight_events[0].payload;
    let inner = payload
        .get("InflightEquity")
        .expect("Event payload should be wrapped in InflightEquity variant");
    let mints = inner
        .get("mints")
        .expect("InflightEquity should contain mints field");
    let aapl_quantity = mints
        .get("AAPL")
        .expect("InflightEquity mints should contain AAPL from the injected pending request");
    assert_eq!(
        aapl_quantity.as_str().unwrap(),
        "25",
        "InflightEquity mint quantity for AAPL should match the injected \
         pending request (25 shares), got: {aapl_quantity}"
    );

    pool.close().await;
    bot.abort();
    Ok(())
}

/// Inflight state survives bot restart via CQRS event replay.
///
/// When the bot restarts against the same database, the CQRS aggregate
/// replays all previous events including `InflightEquity`. The trigger's
/// `on_snapshot` handler processes the replayed events and restores the
/// inflight state in the `InventoryView`.
///
/// This test:
/// 1. Starts a bot, lets it poll and emit `InflightEquity` with a pending
///    mint
/// 2. Stops the bot
/// 3. Starts a new bot on the same database
/// 4. Verifies the second bot starts successfully (CQRS replay doesn't
///    crash) and continues emitting snapshot events (is functional)
/// 5. Verifies dedup: the aggregate suppresses duplicate InflightEquity
///    events when the pending request state hasn't changed
#[test_log::test(tokio::test)]
async fn inflight_state_survives_restart() -> anyhow::Result<()> {
    let broker_fill_price = float!("150.00");
    let onchain_price = float!("150.00");

    let infra = TestInfra::start(
        vec![("AAPL", broker_fill_price)],
        // No offchain positions: the test focuses on inflight polling
        // and restart behavior, not rebalancing decisions.
        vec![],
    )
    .await?;

    let prepared = infra
        .base_chain
        .setup_order()
        .symbol("AAPL")
        .amount(float!("5"))
        .price(onchain_price)
        .direction(TakeDirection::SellEquity)
        .call()
        .await?;

    // Inject a pending mint request that will persist across restarts
    // (the mock server stays alive).
    infra.tokenization_service.inject_pending_request(
        "AAPL",
        float!("25"),
        infra.base_chain.owner,
        TokenizationRequestType::Mint,
    );

    let current_block = infra.base_chain.provider.get_block_number().await?;
    let cash_vault_id = prepared.input_vault_id;
    let equity_vault_ids = HashMap::from([("AAPL".to_owned(), prepared.output_vault_id)]);

    // --- First bot: let it poll and emit InflightEquity events ---
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
        .redemption_wallet(REDEMPTION_WALLET)
        .call()?;

    let mut bot1 = spawn_bot(ctx1);

    // Wait for at least one InflightEquity event from the first bot.
    poll_for_events_with_timeout(
        &mut bot1,
        &infra.db_path,
        "InventorySnapshotEvent::InflightEquity",
        1,
        Duration::from_secs(30),
    )
    .await;

    // Record the total event count before stopping the first bot.
    let pool = connect_db(&infra.db_path).await?;
    let events_before_restart = fetch_events_by_type(&pool, "InventorySnapshot").await?;
    let total_count_before = events_before_restart.len();
    let inflight_count_before = events_before_restart
        .iter()
        .filter(|event| event.event_type == "InventorySnapshotEvent::InflightEquity")
        .count();
    pool.close().await;

    assert!(
        inflight_count_before >= 1,
        "First bot should have emitted at least 1 InflightEquity event"
    );

    // Stop the first bot.
    bot1.abort();
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
        .redemption_wallet(REDEMPTION_WALLET)
        .call()?;

    let bot2 = spawn_bot(ctx2);

    // Wait for the second bot to start polling and emit new snapshot
    // events. This proves the CQRS replay succeeded and the bot is
    // functional after restart.
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Verify the second bot is still alive (didn't crash on replay).
    assert!(
        !bot2.is_finished(),
        "Second bot should still be running after replaying events"
    );

    let pool = connect_db(&infra.db_path).await?;

    // The second bot should have emitted new snapshot events after
    // replaying the first bot's events, proving it's functional.
    let events_after_restart = fetch_events_by_type(&pool, "InventorySnapshot").await?;
    let total_count_after = events_after_restart.len();
    assert!(
        total_count_after > total_count_before,
        "Second bot should emit new snapshot events after restart \
         (before: {total_count_before}, after: {total_count_after})"
    );

    // Verify aggregate dedup: the InflightEquity event count should not
    // explode after restart. The aggregate replays previous events and
    // sets its state. When the poller emits the same inflight data, the
    // aggregate's `handle` method suppresses the duplicate. We allow a
    // modest increase for natural state transitions (e.g., the pending
    // request completing after polls_until_complete).
    let inflight_count_after = events_after_restart
        .iter()
        .filter(|event| event.event_type == "InventorySnapshotEvent::InflightEquity")
        .count();
    let new_inflight_events = inflight_count_after - inflight_count_before;
    assert!(
        new_inflight_events <= 3,
        "Expected at most 3 new InflightEquity events after restart \
         (dedup should suppress duplicates), got {new_inflight_events} \
         (before: {inflight_count_before}, after: {inflight_count_after})"
    );

    pool.close().await;
    bot2.abort();
    Ok(())
}
