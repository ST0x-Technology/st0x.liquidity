//! Alpaca crypto wallet CLI commands (deposit, withdraw, whitelist,
//! transfers, convert).

use alloy::primitives::Address;
use rust_decimal::Decimal;
use std::io::Write;

use st0x_evm::{Evm, IntoErrorRegistry, Wallet};
use st0x_execution::{AlpacaBrokerApi, ConversionDirection, Executor, Positive};

use super::ConvertDirection;
use crate::alpaca_wallet::{
    AlpacaWalletService, Network, TokenSymbol, TransferStatus, WhitelistStatus,
};
use crate::bindings::IERC20;
use crate::config::{BrokerCtx, Ctx};
use crate::onchain::{USDC_ETHEREUM, USDC_ETHEREUM_SEPOLIA};
use crate::threshold::Usdc;

pub(super) async fn alpaca_deposit_command<Registry: IntoErrorRegistry, W: Write>(
    stdout: &mut W,
    amount: Usdc,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "Depositing USDC directly to Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-deposit requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let sender_address = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "   Sender wallet: {sender_address}")?;

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    writeln!(stdout, "   Fetching Alpaca deposit address...")?;
    let usdc_symbol = TokenSymbol::new("USDC");
    let ethereum = Network::new("ethereum");
    let deposit_address = alpaca_wallet
        .get_wallet_address(&usdc_symbol, &ethereum)
        .await?;
    writeln!(stdout, "   Alpaca deposit address: {deposit_address}")?;

    let amount_u256 = amount.to_u256_6_decimals()?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    let (usdc_address, network) = if alpaca_auth.is_sandbox() {
        (USDC_ETHEREUM_SEPOLIA, "Ethereum Sepolia")
    } else {
        (USDC_ETHEREUM, "Ethereum Mainnet")
    };
    writeln!(stdout, "   Network: {network}")?;
    writeln!(stdout, "   USDC contract: {usdc_address}")?;
    let balance = rebalancing_ctx
        .ethereum_wallet()
        .call::<Registry, _>(
            usdc_address,
            IERC20::balanceOfCall {
                account: sender_address,
            },
        )
        .await?;
    writeln!(stdout, "   Current USDC balance: {balance}")?;

    if balance < amount_u256 {
        anyhow::bail!("Insufficient USDC balance: have {balance}, need {amount_u256}");
    }

    writeln!(stdout, "   Sending USDC transfer via Fireblocks...")?;
    let ethereum_wallet = rebalancing_ctx.ethereum_wallet().clone();
    let tx_receipt = ethereum_wallet
        .submit::<Registry, _>(
            usdc_address,
            IERC20::transferCall {
                to: deposit_address,
                amount: amount_u256,
            },
            "USDC transfer to Alpaca",
        )
        .await?;

    let tx_hash = tx_receipt.transaction_hash;
    writeln!(stdout, "   Transaction hash: {tx_hash}")?;
    writeln!(stdout, "   Waiting for Alpaca to detect deposit...")?;

    let transfer = alpaca_wallet.poll_deposit_by_tx_hash(&tx_hash).await?;

    match transfer.status {
        TransferStatus::Complete => {
            writeln!(stdout, "Alpaca deposit completed successfully!")?;
            writeln!(stdout, "   Transfer ID: {}", transfer.id)?;
            writeln!(stdout, "   Amount: {} USDC", transfer.amount)?;
        }
        TransferStatus::Failed => {
            writeln!(stdout, "Alpaca deposit failed!")?;
            writeln!(stdout, "   Transfer ID: {}", transfer.id)?;
            anyhow::bail!("Deposit was detected but marked as failed by Alpaca");
        }
        status => {
            writeln!(
                stdout,
                "Unexpected transfer status after polling: {status:?}"
            )?;
            anyhow::bail!("Deposit ended with unexpected status: {status:?}");
        }
    }

    Ok(())
}

pub(super) async fn alpaca_withdraw_command<Registry: IntoErrorRegistry, W: Write>(
    stdout: &mut W,
    amount: Usdc,
    to_address: Option<Address>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "Withdrawing USDC from Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-withdraw requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let sender_address = rebalancing_ctx.base_wallet().address();

    let destination = to_address.unwrap_or(sender_address);
    writeln!(stdout, "   Destination address: {destination}")?;

    let (usdc_address, network) = if alpaca_auth.is_sandbox() {
        (USDC_ETHEREUM_SEPOLIA, "Ethereum Sepolia")
    } else {
        (USDC_ETHEREUM, "Ethereum Mainnet")
    };

    writeln!(stdout, "   Network: {network}")?;
    writeln!(stdout, "   USDC contract: {usdc_address}")?;

    let balance_before = rebalancing_ctx
        .ethereum_wallet()
        .call::<Registry, _>(
            usdc_address,
            IERC20::balanceOfCall {
                account: destination,
            },
        )
        .await?;

    writeln!(stdout, "   Balance before: {balance_before}")?;
    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    let usdc_asset = TokenSymbol::new("USDC");
    let positive_amount = Positive::new(amount)?;

    writeln!(stdout, "   Initiating withdrawal...")?;
    let transfer = alpaca_wallet
        .initiate_withdrawal(positive_amount, &usdc_asset, &destination)
        .await?;

    writeln!(stdout, "   Withdrawal initiated: {}", transfer.id)?;
    writeln!(stdout, "   Status: {:?}", transfer.status)?;
    writeln!(stdout, "   Waiting for withdrawal to complete...")?;

    let final_transfer = alpaca_wallet
        .poll_transfer_until_complete(&transfer.id)
        .await?;

    match final_transfer.status {
        TransferStatus::Complete => {
            writeln!(stdout, "Alpaca withdrawal completed successfully!")?;
            writeln!(stdout, "   Transfer ID: {}", final_transfer.id)?;
            writeln!(stdout, "   Amount: {} USDC", final_transfer.amount)?;

            if let Some(tx_hash) = final_transfer.tx {
                writeln!(stdout, "   Transaction hash: {tx_hash}")?;
            }

            let balance_after = rebalancing_ctx
                .ethereum_wallet()
                .call::<Registry, _>(
                    usdc_address,
                    IERC20::balanceOfCall {
                        account: destination,
                    },
                )
                .await?;

            writeln!(stdout, "   Balance after: {balance_after}")?;
            let expected_increase = amount.to_u256_6_decimals()?;
            if balance_after >= balance_before + expected_increase {
                writeln!(stdout, "   Balance increased as expected!")?;
            } else {
                writeln!(
                    stdout,
                    "   Warning: Balance increase less than expected (may need to wait for confirmation)"
                )?;
            }
        }
        TransferStatus::Failed => {
            writeln!(stdout, "Alpaca withdrawal failed!")?;
            writeln!(stdout, "   Transfer ID: {}", final_transfer.id)?;
            anyhow::bail!("Withdrawal failed");
        }
        status => {
            writeln!(
                stdout,
                "Unexpected transfer status after polling: {status:?}"
            )?;
            anyhow::bail!("Withdrawal ended with unexpected status: {status:?}");
        }
    }

    Ok(())
}

pub(super) async fn alpaca_whitelist_command<W: Write>(
    stdout: &mut W,
    address: Option<Address>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-whitelist requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let target_address = address.unwrap_or_else(|| rebalancing_ctx.base_wallet().address());

    writeln!(stdout, "Whitelisting address for Alpaca withdrawals")?;
    writeln!(stdout, "   Address: {target_address}")?;
    writeln!(stdout, "   Asset: USDC")?;
    writeln!(stdout, "   Network: Ethereum")?;

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    writeln!(stdout, "   Checking existing whitelist entries...")?;
    let existing = alpaca_wallet.get_whitelisted_addresses().await?;

    for entry in &existing {
        if entry.address == target_address && entry.asset.as_ref() == "USDC" {
            writeln!(stdout, "   Address already whitelisted!")?;
            writeln!(stdout, "   Status: {:?}", entry.status)?;
            writeln!(stdout, "   Created: {}", entry.created_at)?;
            return Ok(());
        }
    }

    writeln!(stdout, "   Creating whitelist entry...")?;
    let entry = alpaca_wallet
        .create_whitelist_entry(
            &target_address,
            &TokenSymbol::new("USDC"),
            &Network::new("ethereum"),
        )
        .await?;

    writeln!(stdout, "Whitelist entry created!")?;
    writeln!(stdout, "   ID: {}", entry.id)?;
    writeln!(stdout, "   Status: {:?}", entry.status)?;
    writeln!(stdout, "   Created: {}", entry.created_at)?;

    if entry.status == WhitelistStatus::Pending {
        writeln!(
            stdout,
            "\nNote: Address is PENDING approval. In production this takes ~24 hours."
        )?;
        writeln!(
            stdout,
            "In sandbox, approval may be instant - try withdrawing."
        )?;
    }

    Ok(())
}

pub(super) async fn alpaca_whitelist_list_command<W: Write>(
    stdout: &mut W,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-whitelist-list requires Alpaca Broker API configuration");
    };

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    writeln!(stdout, "Fetching whitelisted addresses...")?;

    let entries = alpaca_wallet.get_whitelisted_addresses().await?;

    if entries.is_empty() {
        writeln!(stdout, "\nNo whitelisted addresses found.")?;
        return Ok(());
    }

    writeln!(stdout, "\nFound {} whitelist entries:\n", entries.len())?;

    for entry in &entries {
        writeln!(stdout, "Entry {}", entry.id)?;
        writeln!(stdout, "   Address: {}", entry.address)?;
        writeln!(stdout, "   Asset: {}", entry.asset.as_ref())?;
        writeln!(stdout, "   Chain: {}", entry.chain.as_ref())?;
        writeln!(stdout, "   Status: {:?}", entry.status)?;
        writeln!(stdout, "   Created: {}", entry.created_at)?;
        writeln!(stdout)?;
    }

    Ok(())
}

pub(super) async fn alpaca_unwhitelist_command<W: Write>(
    stdout: &mut W,
    address: Address,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-unwhitelist requires Alpaca Broker API configuration");
    };

    writeln!(stdout, "Removing address from Alpaca whitelist")?;
    writeln!(stdout, "   Address: {address}")?;

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    let removed = alpaca_wallet.remove_whitelist_entries(&address).await?;

    writeln!(stdout, "Removed {} whitelist entry/entries:", removed.len())?;
    for entry in &removed {
        writeln!(stdout, "   ID: {}", entry.id)?;
        writeln!(stdout, "   Asset: {}", entry.asset.as_ref())?;
        writeln!(stdout, "   Status was: {:?}", entry.status)?;
        writeln!(stdout, "   Created: {}", entry.created_at)?;
    }

    Ok(())
}

pub(super) async fn alpaca_transfers_command<W: Write>(
    stdout: &mut W,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-transfers requires Alpaca Broker API configuration");
    };

    let alpaca_wallet = AlpacaWalletService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    );

    writeln!(stdout, "Fetching Alpaca crypto wallet transfers...")?;
    writeln!(stdout, "   Account: {}", alpaca_auth.account_id)?;

    let transfers = alpaca_wallet.list_all_transfers().await?;

    if transfers.is_empty() {
        writeln!(stdout, "\nNo transfers found.")?;
        return Ok(());
    }

    writeln!(stdout, "\nFound {} transfers:\n", transfers.len())?;

    for transfer in transfers {
        writeln!(stdout, "Transfer {}", transfer.id)?;
        writeln!(stdout, "   Direction: {:?}", transfer.direction)?;
        writeln!(stdout, "   Amount: {} {}", transfer.amount, transfer.asset)?;
        writeln!(stdout, "   Status: {:?}", transfer.status)?;
        writeln!(stdout, "   Chain: {}", transfer.chain)?;
        writeln!(stdout, "   From: {}", transfer.from)?;
        writeln!(stdout, "   To: {}", transfer.to)?;
        if let Some(tx) = &transfer.tx {
            writeln!(stdout, "   Tx Hash: {tx}")?;
        }
        writeln!(stdout, "   Created: {}", transfer.created_at)?;
        writeln!(stdout)?;
    }

    Ok(())
}

pub(super) async fn alpaca_convert_command<W: Write>(
    stdout: &mut W,
    direction: ConvertDirection,
    amount: Usdc,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let direction_str = match direction {
        ConvertDirection::ToUsd => "USDC → USD",
        ConvertDirection::ToUsdc => "USD → USDC",
    };

    writeln!(stdout, "Converting {direction_str} on Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let rebalancing_ctx = ctx.rebalancing_ctx()?;

    let executor =
        AlpacaBrokerApi::try_from_ctx(rebalancing_ctx.alpaca_broker_auth.clone()).await?;

    let conversion_direction = match direction {
        ConvertDirection::ToUsd => ConversionDirection::UsdcToUsd,
        ConvertDirection::ToUsdc => ConversionDirection::UsdToUsdc,
    };

    let amount_decimal: Decimal = amount.into();

    writeln!(stdout, "   Placing market order...")?;

    let order = executor
        .convert_usdc_usd(amount_decimal, conversion_direction)
        .await?;

    writeln!(stdout, "Conversion completed successfully!")?;
    writeln!(stdout, "   Order ID: {}", order.id)?;
    writeln!(stdout, "   Symbol: {}", order.symbol)?;
    writeln!(stdout, "   Quantity: {}", order.quantity)?;
    writeln!(stdout, "   Status: {}", order.status_display())?;
    if let Some(price) = order.filled_average_price {
        writeln!(stdout, "   Filled Price: ${price:.4}")?;
    }
    if let Some(filled_qty) = order.filled_quantity {
        writeln!(stdout, "   Filled Quantity: {filled_qty}")?;
    }
    if let (Some(price), Some(quantity)) = (order.filled_average_price, order.filled_quantity) {
        let usd_amount = price * quantity;
        writeln!(stdout, "   USD Amount: ${usd_amount}")?;
    }
    writeln!(stdout, "   Created: {}", order.created_at)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, address};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use url::Url;
    use uuid::uuid;

    use st0x_evm::NoOpErrorRegistry;
    use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};

    use super::*;
    use crate::cli::ConvertDirection;
    use crate::config::{LogLevel, OperationalLimits, TradingMode};
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::EvmCtx;
    use crate::rebalancing::RebalancingCtx;
    use crate::rebalancing::trigger::UsdcRebalancing;
    use crate::threshold::ExecutionThreshold;

    fn create_ctx_without_alpaca() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            operational_limits: OperationalLimits::Disabled,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: Address::ZERO,
            },
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn create_alpaca_ctx_without_rebalancing() -> Ctx {
        let mut ctx = create_ctx_without_alpaca();
        ctx.broker = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        });
        ctx
    }

    fn create_full_alpaca_ctx() -> Ctx {
        let alpaca_account_id = AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            operational_limits: OperationalLimits::Disabled,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
                api_key: "test-key".to_string(),
                api_secret: "test-secret".to_string(),
                account_id: alpaca_account_id,
                mode: Some(AlpacaBrokerApiMode::Sandbox),
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: TimeInForce::default(),
            }),
            telemetry: None,
            trading_mode: TradingMode::Rebalancing(Box::new(RebalancingCtx::stub(
                ImbalanceThreshold {
                    target: dec!(0.5),
                    deviation: dec!(0.1),
                },
                UsdcRebalancing::Disabled,
                Address::ZERO,
                B256::ZERO,
                AlpacaBrokerApiCtx {
                    api_key: "test-key".to_string(),
                    api_secret: "test-secret".to_string(),
                    account_id: alpaca_account_id,
                    mode: Some(AlpacaBrokerApiMode::Sandbox),
                    asset_cache_ttl: std::time::Duration::from_secs(3600),
                    time_in_force: TimeInForce::default(),
                },
                HashMap::new(),
            ))),
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    #[tokio::test]
    async fn test_alpaca_deposit_requires_rebalancing_ctx() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let amount = Usdc(dec!(100));

        let mut stdout = Vec::new();
        let err_msg = alpaca_deposit_command::<NoOpErrorRegistry, _>(&mut stdout, amount, &ctx)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err_msg.contains("requires rebalancing mode"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_deposit_writes_amount_to_stdout() {
        let ctx = create_full_alpaca_ctx();
        let amount = Usdc(dec!(500.50));

        let mut stdout = Vec::new();
        alpaca_deposit_command::<NoOpErrorRegistry, _>(&mut stdout, amount, &ctx)
            .await
            .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("500.50 USDC"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_withdraw_requires_rebalancing_ctx() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let amount = Usdc(dec!(100));

        let mut stdout = Vec::new();
        let err_msg =
            alpaca_withdraw_command::<NoOpErrorRegistry, _>(&mut stdout, amount, None, &ctx)
                .await
                .unwrap_err()
                .to_string();
        assert!(
            err_msg.contains("requires rebalancing mode"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_whitelist_requires_rebalancing_ctx() {
        let ctx = create_alpaca_ctx_without_rebalancing();

        let mut stdout = Vec::new();
        let err_msg = alpaca_whitelist_command(&mut stdout, None, &ctx)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err_msg.contains("requires rebalancing mode"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_convert_writes_direction_to_stdout() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let amount = Usdc(dec!(500.50));

        let mut stdout = Vec::new();
        alpaca_convert_command(&mut stdout, ConvertDirection::ToUsd, amount, &ctx)
            .await
            .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("USDC → USD"),
            "Expected USDC to USD in output, got: {output}"
        );
        assert!(
            output.contains("500.50 USDC"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_convert_to_usdc_writes_direction_to_stdout() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let amount = Usdc(dec!(250));

        let mut stdout = Vec::new();
        alpaca_convert_command(&mut stdout, ConvertDirection::ToUsdc, amount, &ctx)
            .await
            .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("USD → USDC"),
            "Expected USD to USDC in output, got: {output}"
        );
    }
}
