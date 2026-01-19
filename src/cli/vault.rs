//! Raindex vault deposit and withdrawal CLI commands.

use alloy::network::EthereumWallet;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use std::io::Write;

use crate::bindings::IERC20;
use crate::cctp::USDC_BASE;
use crate::env::Config;
use crate::onchain::vault::{VaultId, VaultService};
use crate::threshold::Usdc;

pub(super) async fn vault_deposit_command<
    W: Write,
    BP: Provider + Clone + Send + Sync + 'static,
>(
    stdout: &mut W,
    amount: Usdc,
    config: &Config,
    base_provider: BP,
) -> anyhow::Result<()> {
    writeln!(stdout, "Depositing USDC to Raindex vault")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let rebalancing_config = config.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "vault-deposit requires rebalancing configuration (set REBALANCING_ENABLED=true)"
        )
    })?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.ethereum_private_key)?;
    let base_wallet = EthereumWallet::from(signer.clone());
    let sender_address = signer.address();

    writeln!(stdout, "   Sender wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", config.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {}", rebalancing_config.usdc_vault_id)?;

    let base_provider_with_wallet = ProviderBuilder::new()
        .wallet(base_wallet)
        .connect_provider(base_provider);

    let usdc = IERC20::IERC20Instance::new(USDC_BASE, &base_provider_with_wallet);

    let balance = usdc.balanceOf(sender_address).call().await?;
    writeln!(stdout, "   Current USDC balance: {balance}")?;

    let amount_u256 = amount.to_u256_6_decimals()?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    if balance < amount_u256 {
        anyhow::bail!("Insufficient USDC balance: have {balance}, need {amount_u256}");
    }

    writeln!(stdout, "   Approving orderbook to spend USDC...")?;
    let approve_receipt = usdc
        .approve(config.evm.orderbook, amount_u256)
        .send()
        .await?
        .get_receipt()
        .await?;
    writeln!(
        stdout,
        "   Approval tx: {}",
        approve_receipt.transaction_hash
    )?;

    let vault_service = VaultService::new(base_provider_with_wallet, config.evm.orderbook);
    let vault_id = VaultId(rebalancing_config.usdc_vault_id);

    writeln!(stdout, "   USDC address: {USDC_BASE}")?;
    writeln!(stdout, "   Depositing to vault...")?;
    let deposit_tx = vault_service.deposit_usdc(vault_id, amount_u256).await?;
    writeln!(stdout, "   Deposit tx: {deposit_tx}")?;

    writeln!(stdout, "Vault deposit completed successfully!")?;

    Ok(())
}

pub(super) async fn vault_withdraw_command<
    W: Write,
    BP: Provider + Clone + Send + Sync + 'static,
>(
    stdout: &mut W,
    amount: Usdc,
    config: &Config,
    base_provider: BP,
) -> anyhow::Result<()> {
    writeln!(stdout, "Withdrawing USDC from Raindex vault")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let rebalancing_config = config.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "vault-withdraw requires rebalancing configuration (set REBALANCING_ENABLED=true)"
        )
    })?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.ethereum_private_key)?;
    let base_wallet = EthereumWallet::from(signer.clone());
    let sender_address = signer.address();

    writeln!(stdout, "   Recipient wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", config.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {}", rebalancing_config.usdc_vault_id)?;

    let base_provider_with_wallet = ProviderBuilder::new()
        .wallet(base_wallet)
        .connect_provider(base_provider);

    let vault_service = VaultService::new(base_provider_with_wallet, config.evm.orderbook);
    let vault_id = VaultId(rebalancing_config.usdc_vault_id);

    let amount_u256 = amount.to_u256_6_decimals()?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    writeln!(stdout, "   Withdrawing from vault...")?;
    let withdraw_tx = vault_service.withdraw_usdc(vault_id, amount_u256).await?;
    writeln!(stdout, "   Withdraw tx: {withdraw_tx}")?;

    writeln!(stdout, "Vault withdrawal completed successfully!")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use alloy::providers::mock::Asserter;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    use super::*;
    use crate::env::{BrokerConfig, LogLevel};
    use crate::onchain::EvmEnv;

    fn create_config_without_rebalancing() -> Config {
        Config {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmEnv {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Address::ZERO,
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::DryRun,
            hyperdx: None,
            rebalancing: None,
        }
    }

    fn create_mock_provider() -> impl Provider + Clone + 'static {
        let asserter = Asserter::new();
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    #[tokio::test]
    async fn test_vault_deposit_requires_rebalancing_config() {
        let config = create_config_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = vault_deposit_command(&mut stdout, amount, &config, provider).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_withdraw_requires_rebalancing_config() {
        let config = create_config_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = vault_withdraw_command(&mut stdout, amount, &config, provider).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_deposit_writes_amount_to_stdout() {
        let config = create_config_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("500.50").unwrap());

        let mut stdout = Vec::new();
        let _ = vault_deposit_command(&mut stdout, amount, &config, provider).await;

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("500.50 USDC"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_vault_withdraw_writes_amount_to_stdout() {
        let config = create_config_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("250.25").unwrap());

        let mut stdout = Vec::new();
        let _ = vault_withdraw_command(&mut stdout, amount, &config, provider).await;

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("250.25 USDC"),
            "Expected amount in output, got: {output}"
        );
    }
}
