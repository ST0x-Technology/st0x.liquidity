//! Alpaca crypto wallet CLI commands (deposit, withdraw, whitelist, transfers, convert).

use alloy::primitives::Address;
use alloy::providers::ProviderBuilder;
use rust_decimal::Decimal;
use st0x_execution::Executor;
use st0x_execution::alpaca_broker_api::ConversionDirection;
use std::io::Write;

use super::ConvertDirection;
use crate::alpaca_wallet::{
    AlpacaWalletService, Network, TokenSymbol, TransferStatus, WhitelistStatus,
};
use crate::bindings::IERC20;
use crate::cctp::{USDC_ETHEREUM, USDC_ETHEREUM_SEPOLIA};
use crate::env::{BrokerConfig, Config};
use crate::threshold::Usdc;

pub(super) async fn alpaca_deposit_command<W: Write>(
    stdout: &mut W,
    amount: Usdc,
    config: &Config,
) -> anyhow::Result<()> {
    writeln!(stdout, "Depositing USDC directly to Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let BrokerConfig::AlpacaBrokerApi(alpaca_auth) = &config.broker else {
        anyhow::bail!("alpaca-deposit requires Alpaca Broker API configuration");
    };

    let rebalancing_config = config.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "alpaca-deposit requires rebalancing configuration (set REBALANCING_ENABLED=true)"
        )
    })?;

    let resolved = rebalancing_config.signer.resolve().await?;

    writeln!(stdout, "   Sender wallet: {}", resolved.address())?;

    // CLI deposit uses a wallet-enabled provider for direct ERC20 transfers.
    // Fireblocks CLI support for this command will be added in a future phase.
    let wallet = resolved
        .wallet()
        .ok_or_else(|| anyhow::anyhow!("alpaca-deposit currently requires a local signer"))?
        .clone();

    let ethereum_provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rebalancing_config.ethereum_rpc_url.clone());

    let broker_api_base_url = if alpaca_auth.is_sandbox() {
        "https://broker-api.sandbox.alpaca.markets"
    } else {
        "https://broker-api.alpaca.markets"
    };

    let alpaca_wallet = AlpacaWalletService::new(
        broker_api_base_url.to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.alpaca_broker_api_key.clone(),
        alpaca_auth.alpaca_broker_api_secret.clone(),
    );

    writeln!(stdout, "   Fetching Alpaca deposit address...")?;
    let usdc = TokenSymbol::new("USDC");
    let ethereum = Network::new("ethereum");
    let deposit_address = alpaca_wallet.get_wallet_address(&usdc, &ethereum).await?;
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
    let usdc = IERC20::IERC20Instance::new(usdc_address, &ethereum_provider);

    let balance = usdc.balanceOf(resolved.address()).call().await?;
    writeln!(stdout, "   Current USDC balance: {balance}")?;

    if balance < amount_u256 {
        anyhow::bail!("Insufficient USDC balance: have {balance}, need {amount_u256}");
    }

    writeln!(stdout, "   Sending USDC transfer transaction...")?;
    let tx_receipt = usdc
        .transfer(deposit_address, amount_u256)
        .send()
        .await?
        .get_receipt()
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

pub(super) async fn alpaca_withdraw_command<W: Write>(
    stdout: &mut W,
    amount: Usdc,
    to_address: Option<Address>,
    config: &Config,
) -> anyhow::Result<()> {
    writeln!(stdout, "Withdrawing USDC from Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let BrokerConfig::AlpacaBrokerApi(alpaca_auth) = &config.broker else {
        anyhow::bail!("alpaca-withdraw requires Alpaca Broker API configuration");
    };

    let rebalancing_config = config.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "alpaca-withdraw requires rebalancing configuration (set REBALANCING_ENABLED=true)"
        )
    })?;

    let resolved = rebalancing_config.signer.resolve().await?;

    let destination = to_address.unwrap_or_else(|| resolved.address());
    writeln!(stdout, "   Destination address: {destination}")?;

    let wallet = resolved
        .wallet()
        .ok_or_else(|| anyhow::anyhow!("alpaca-withdraw currently requires a local signer"))?
        .clone();

    let ethereum_provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rebalancing_config.ethereum_rpc_url.clone());

    let (usdc_address, network) = if alpaca_auth.is_sandbox() {
        (USDC_ETHEREUM_SEPOLIA, "Ethereum Sepolia")
    } else {
        (USDC_ETHEREUM, "Ethereum Mainnet")
    };
    writeln!(stdout, "   Network: {network}")?;
    writeln!(stdout, "   USDC contract: {usdc_address}")?;
    let usdc = IERC20::IERC20Instance::new(usdc_address, &ethereum_provider);

    let balance_before = usdc.balanceOf(destination).call().await?;
    writeln!(stdout, "   Balance before: {balance_before}")?;

    let broker_api_base_url = if alpaca_auth.is_sandbox() {
        "https://broker-api.sandbox.alpaca.markets"
    } else {
        "https://broker-api.alpaca.markets"
    };

    let alpaca_wallet = AlpacaWalletService::new(
        broker_api_base_url.to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.alpaca_broker_api_key.clone(),
        alpaca_auth.alpaca_broker_api_secret.clone(),
    );

    let usdc_asset = TokenSymbol::new("USDC");
    let amount_decimal: rust_decimal::Decimal = amount.into();

    writeln!(stdout, "   Initiating withdrawal...")?;
    let transfer = alpaca_wallet
        .initiate_withdrawal(amount_decimal, &usdc_asset, &destination)
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

            let balance_after = usdc.balanceOf(destination).call().await?;
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
    config: &Config,
) -> anyhow::Result<()> {
    let BrokerConfig::AlpacaBrokerApi(alpaca_auth) = &config.broker else {
        anyhow::bail!("alpaca-whitelist requires Alpaca Broker API configuration");
    };

    let rebalancing_config = config.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "alpaca-whitelist requires rebalancing configuration (set REBALANCING_ENABLED=true)"
        )
    })?;

    let signer_address = rebalancing_config.signer.address().await?;
    let target_address = address.unwrap_or(signer_address);

    writeln!(stdout, "Whitelisting address for Alpaca withdrawals")?;
    writeln!(stdout, "   Address: {target_address}")?;
    writeln!(stdout, "   Asset: USDC")?;
    writeln!(stdout, "   Network: Ethereum")?;

    let broker_api_base_url = if alpaca_auth.is_sandbox() {
        "https://broker-api.sandbox.alpaca.markets"
    } else {
        "https://broker-api.alpaca.markets"
    };

    let alpaca_wallet = AlpacaWalletService::new(
        broker_api_base_url.to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.alpaca_broker_api_key.clone(),
        alpaca_auth.alpaca_broker_api_secret.clone(),
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

pub(super) async fn alpaca_transfers_command<W: Write>(
    stdout: &mut W,
    config: &Config,
) -> anyhow::Result<()> {
    let BrokerConfig::AlpacaBrokerApi(alpaca_auth) = &config.broker else {
        anyhow::bail!("alpaca-transfers requires Alpaca Broker API configuration");
    };

    let rebalancing_config = config.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "alpaca-transfers requires rebalancing configuration (set REBALANCING_ENABLED=true)"
        )
    })?;

    let broker_api_base_url = if alpaca_auth.is_sandbox() {
        "https://broker-api.sandbox.alpaca.markets"
    } else {
        "https://broker-api.alpaca.markets"
    };

    let alpaca_wallet = AlpacaWalletService::new(
        broker_api_base_url.to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.alpaca_broker_api_key.clone(),
        alpaca_auth.alpaca_broker_api_secret.clone(),
    );

    writeln!(stdout, "Fetching Alpaca crypto wallet transfers...")?;
    writeln!(
        stdout,
        "   Account: {}",
        rebalancing_config.alpaca_account_id
    )?;

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
    config: &Config,
) -> anyhow::Result<()> {
    let direction_str = match direction {
        ConvertDirection::ToUsd => "USDC → USD",
        ConvertDirection::ToUsdc => "USD → USDC",
    };

    writeln!(stdout, "Converting {direction_str} on Alpaca")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let BrokerConfig::AlpacaBrokerApi(alpaca_auth) = &config.broker else {
        anyhow::bail!("alpaca-convert requires Alpaca Broker API configuration");
    };

    let executor =
        st0x_execution::alpaca_broker_api::AlpacaBrokerApi::try_from_config(alpaca_auth.clone())
            .await?;

    let conversion_direction = match direction {
        ConvertDirection::ToUsd => ConversionDirection::UsdcToUsd,
        ConvertDirection::ToUsdc => ConversionDirection::UsdToUsdc,
    };

    let amount_decimal: rust_decimal::Decimal = amount.into();

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
    if let (Some(price), Some(qty)) = (order.filled_average_price, order.filled_quantity) {
        match Decimal::try_from(price) {
            Ok(price_decimal) => {
                let usd_amount = price_decimal * qty;
                writeln!(stdout, "   USD Amount: ${usd_amount}")?;
            }
            Err(e) => {
                writeln!(stdout, "   USD Amount: (conversion error: {e})")?;
            }
        }
    }
    writeln!(stdout, "   Created: {}", order.created_at)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, address};
    use rust_decimal_macros::dec;
    use st0x_execution::alpaca_broker_api::{AlpacaBrokerApiAuthEnv, AlpacaBrokerApiMode};
    use uuid::uuid;

    use super::*;
    use crate::alpaca_wallet::AlpacaAccountId;
    use crate::cli::ConvertDirection;
    use crate::env::LogLevel;
    use crate::fireblocks::SignerConfig;
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::EvmEnv;
    use crate::rebalancing::RebalancingConfig;
    use crate::threshold::ExecutionThreshold;

    fn create_config_without_alpaca() -> Config {
        Config {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmEnv {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Some(Address::ZERO),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::DryRun,
            hyperdx: None,
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn create_alpaca_config_without_rebalancing() -> Config {
        let mut config = create_config_without_alpaca();
        config.broker = BrokerConfig::AlpacaBrokerApi(AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test-key".to_string(),
            alpaca_broker_api_secret: "test-secret".to_string(),
            alpaca_account_id: "test-account-id".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
        });
        config
    }

    fn create_full_alpaca_config() -> Config {
        let alpaca_account_id = AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));
        Config {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmEnv {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Some(Address::ZERO),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::AlpacaBrokerApi(AlpacaBrokerApiAuthEnv {
                alpaca_broker_api_key: "test-key".to_string(),
                alpaca_broker_api_secret: "test-secret".to_string(),
                alpaca_account_id: alpaca_account_id.to_string(),
                alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
            }),
            hyperdx: None,
            rebalancing: Some(RebalancingConfig {
                signer: SignerConfig::Local(B256::ZERO),
                ethereum_rpc_url: url::Url::parse("http://localhost:8545").unwrap(),
                usdc_vault_id: B256::ZERO,
                redemption_wallet: Address::ZERO,
                alpaca_account_id,
                equity_threshold: ImbalanceThreshold {
                    target: dec!(0.5),
                    deviation: dec!(0.1),
                },
                usdc_threshold: ImbalanceThreshold {
                    target: dec!(0.5),
                    deviation: dec!(0.1),
                },
                alpaca_broker_auth: AlpacaBrokerApiAuthEnv {
                    alpaca_broker_api_key: "test-key".to_string(),
                    alpaca_broker_api_secret: "test-secret".to_string(),
                    alpaca_account_id: alpaca_account_id.to_string(),
                    alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
                },
            }),
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    #[tokio::test]
    async fn test_alpaca_deposit_requires_alpaca_broker() {
        let config = create_config_without_alpaca();
        let amount = Usdc(dec!(100));

        let mut stdout = Vec::new();
        let result = alpaca_deposit_command(&mut stdout, amount, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_deposit_requires_rebalancing_config() {
        let config = create_alpaca_config_without_rebalancing();
        let amount = Usdc(dec!(100));

        let mut stdout = Vec::new();
        let result = alpaca_deposit_command(&mut stdout, amount, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_deposit_writes_amount_to_stdout() {
        let config = create_full_alpaca_config();
        let amount = Usdc(dec!(500.50));

        let mut stdout = Vec::new();
        let _ = alpaca_deposit_command(&mut stdout, amount, &config).await;

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("500.50 USDC"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_withdraw_requires_alpaca_broker() {
        let config = create_config_without_alpaca();
        let amount = Usdc(dec!(100));

        let mut stdout = Vec::new();
        let result = alpaca_withdraw_command(&mut stdout, amount, None, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_withdraw_requires_rebalancing_config() {
        let config = create_alpaca_config_without_rebalancing();
        let amount = Usdc(dec!(100));

        let mut stdout = Vec::new();
        let result = alpaca_withdraw_command(&mut stdout, amount, None, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_whitelist_requires_alpaca_broker() {
        let config = create_config_without_alpaca();

        let mut stdout = Vec::new();
        let result = alpaca_whitelist_command(&mut stdout, None, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_whitelist_requires_rebalancing_config() {
        let config = create_alpaca_config_without_rebalancing();

        let mut stdout = Vec::new();
        let result = alpaca_whitelist_command(&mut stdout, None, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_transfers_requires_alpaca_broker() {
        let config = create_config_without_alpaca();

        let mut stdout = Vec::new();
        let result = alpaca_transfers_command(&mut stdout, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_transfers_requires_rebalancing_config() {
        let config = create_alpaca_config_without_rebalancing();

        let mut stdout = Vec::new();
        let result = alpaca_transfers_command(&mut stdout, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_convert_requires_alpaca_broker() {
        let config = create_config_without_alpaca();
        let amount = Usdc(dec!(100));

        let mut stdout = Vec::new();
        let result =
            alpaca_convert_command(&mut stdout, ConvertDirection::ToUsd, amount, &config).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_alpaca_convert_writes_direction_to_stdout() {
        let config = create_alpaca_config_without_rebalancing();
        let amount = Usdc(dec!(500.50));

        let mut stdout = Vec::new();
        let _ = alpaca_convert_command(&mut stdout, ConvertDirection::ToUsd, amount, &config).await;

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
        let config = create_alpaca_config_without_rebalancing();
        let amount = Usdc(dec!(250));

        let mut stdout = Vec::new();
        let _ =
            alpaca_convert_command(&mut stdout, ConvertDirection::ToUsdc, amount, &config).await;

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("USD → USDC"),
            "Expected USD to USDC in output, got: {output}"
        );
    }
}
