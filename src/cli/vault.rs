//! Raindex vault deposit and withdrawal CLI commands.

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use cqrs_es::persist::GenericQuery;
use rust_decimal::Decimal;
use sqlite_es::SqliteViewRepository;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use thiserror::Error;

use crate::bindings::IERC20;
use crate::config::Ctx;
use crate::onchain::REQUIRED_CONFIRMATIONS;
use crate::onchain::raindex::{RaindexService, VaultId};
use crate::threshold::Usdc;
use crate::vault_registry::VaultRegistryAggregate;

pub(super) struct Deposit {
    pub(super) amount: Decimal,
    pub(super) token: Address,
    pub(super) vault_id: B256,
    pub(super) decimals: u8,
}

#[derive(Debug, Error)]
pub enum VaultCliError {
    #[error("negative amount: {0}")]
    NegativeAmount(Decimal),

    #[error("amount overflow when scaling to {decimals} decimals")]
    AmountOverflow { decimals: u8 },

    #[error("failed to parse scaled amount as U256")]
    ParseError(#[from] alloy::primitives::ruint::ParseError),
}

fn decimal_to_u256(amount: Decimal, decimals: u8) -> Result<U256, VaultCliError> {
    if amount.is_sign_negative() {
        return Err(VaultCliError::NegativeAmount(amount));
    }

    let scale = Decimal::from(10u64.pow(u32::from(decimals)));
    let scaled = amount
        .checked_mul(scale)
        .ok_or(VaultCliError::AmountOverflow { decimals })?;

    Ok(U256::from_str_radix(&scaled.trunc().to_string(), 10)?)
}

pub(super) async fn vault_deposit_command<
    W: Write,
    BP: Provider + Clone + Send + Sync + 'static,
>(
    stdout: &mut W,
    deposit: Deposit,
    ctx: &Ctx,
    pool: &SqlitePool,
    base_provider: BP,
) -> anyhow::Result<()> {
    let Deposit {
        amount,
        token,
        vault_id,
        decimals,
    } = deposit;
    writeln!(stdout, "Depositing tokens to Raindex vault")?;
    writeln!(stdout, "   Amount: {amount}")?;
    writeln!(stdout, "   Token: {token}")?;
    writeln!(stdout, "   Decimals: {decimals}")?;

    let rebalancing_config = ctx
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("vault-deposit requires rebalancing configuration"))?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.evm_private_key)?;
    let base_wallet = EthereumWallet::from(signer.clone());
    let sender_address = signer.address();

    writeln!(stdout, "   Sender wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {vault_id}")?;

    let base_provider_with_wallet = ProviderBuilder::new()
        .wallet(base_wallet)
        .connect_provider(base_provider);

    let token_contract = IERC20::IERC20Instance::new(token, &base_provider_with_wallet);

    let balance = token_contract.balanceOf(sender_address).call().await?;
    writeln!(stdout, "   Current token balance: {balance}")?;

    let amount_u256 = decimal_to_u256(amount, decimals)?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    if balance < amount_u256 {
        anyhow::bail!("Insufficient token balance: have {balance}, need {amount_u256}");
    }

    writeln!(stdout, "   Approving orderbook to spend tokens...")?;
    let approve_receipt = token_contract
        .approve(ctx.evm.orderbook, amount_u256)
        .send()
        .await?
        .with_required_confirmations(REQUIRED_CONFIRMATIONS)
        .get_receipt()
        .await?;
    writeln!(
        stdout,
        "   Approval tx: {}",
        approve_receipt.transaction_hash
    )?;

    let vault_registry_view_repo = Arc::new(SqliteViewRepository::<
        VaultRegistryAggregate,
        VaultRegistryAggregate,
    >::new(
        pool.clone(), "vault_registry_view".to_string()
    ));
    let vault_registry_query = Arc::new(GenericQuery::new(vault_registry_view_repo));

    let raindex_service = RaindexService::new(
        base_provider_with_wallet,
        ctx.evm.orderbook,
        vault_registry_query,
        sender_address,
    );

    writeln!(stdout, "   Depositing to vault...")?;
    let deposit_tx = raindex_service
        .deposit(token, VaultId(vault_id), amount_u256, decimals)
        .await?;
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
    ctx: &Ctx,
    pool: &SqlitePool,
    base_provider: BP,
) -> anyhow::Result<()> {
    writeln!(stdout, "Withdrawing USDC from Raindex vault")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let rebalancing_config = ctx
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("vault-withdraw requires rebalancing configuration"))?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.evm_private_key)?;
    let base_wallet = EthereumWallet::from(signer.clone());
    let sender_address = signer.address();

    writeln!(stdout, "   Recipient wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {}", rebalancing_config.usdc_vault_id)?;

    let base_provider_with_wallet = ProviderBuilder::new()
        .wallet(base_wallet)
        .connect_provider(base_provider);

    let vault_registry_view_repo = Arc::new(SqliteViewRepository::<
        VaultRegistryAggregate,
        VaultRegistryAggregate,
    >::new(
        pool.clone(), "vault_registry_view".to_string()
    ));
    let vault_registry_query = Arc::new(GenericQuery::new(vault_registry_view_repo));

    let raindex_service = RaindexService::new(
        base_provider_with_wallet,
        ctx.evm.orderbook,
        vault_registry_query,
        sender_address,
    );
    let vault_id = VaultId(rebalancing_config.usdc_vault_id);

    let amount_u256 = amount.to_u256_6_decimals()?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    writeln!(stdout, "   Withdrawing from vault...")?;
    let withdraw_tx = raindex_service.withdraw_usdc(vault_id, amount_u256).await?;
    writeln!(stdout, "   Withdraw tx: {withdraw_tx}")?;

    writeln!(stdout, "Vault withdrawal completed successfully!")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};
    use alloy::providers::mock::Asserter;
    use std::str::FromStr;
    use url::Url;

    use super::*;
    use crate::config::{BrokerCtx, LogLevel};
    use crate::onchain::EvmCtx;
    use crate::threshold::ExecutionThreshold;

    fn create_ctx_without_rebalancing() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Some(Address::ZERO),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn create_mock_provider() -> impl Provider + Clone + 'static {
        let asserter = Asserter::new();
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    const TEST_TOKEN: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    const TEST_VAULT_ID: B256 =
        b256!("0000000000000000000000000000000000000000000000000000000000000001");

    #[tokio::test]
    async fn test_vault_deposit_requires_rebalancing_config() {
        let ctx = create_ctx_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Decimal::from_str("100").unwrap();

        let mut stdout = Vec::new();
        let deposit = Deposit {
            amount,
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            decimals: 6,
        };
        let result = vault_deposit_command(
            &mut stdout,
            deposit,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
            provider,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_withdraw_requires_rebalancing_config() {
        let ctx = create_ctx_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = vault_withdraw_command(
            &mut stdout,
            amount,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
            provider,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_deposit_writes_amount_to_stdout() {
        let ctx = create_ctx_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Decimal::from_str("500.50").unwrap();

        let mut stdout = Vec::new();
        let deposit = Deposit {
            amount,
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            decimals: 6,
        };
        vault_deposit_command(
            &mut stdout,
            deposit,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
            provider,
        )
        .await
        .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("500.50"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_vault_withdraw_writes_amount_to_stdout() {
        let ctx = create_ctx_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("250.25").unwrap());

        let mut stdout = Vec::new();
        vault_withdraw_command(
            &mut stdout,
            amount,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
            provider,
        )
        .await
        .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("250.25 USDC"),
            "Expected amount in output, got: {output}"
        );
    }

    #[test]
    fn test_decimal_to_u256_valid_6_decimals() {
        let amount = Decimal::from_str("100.5").unwrap();
        let result = decimal_to_u256(amount, 6).unwrap();
        assert_eq!(result, U256::from(100_500_000u64));
    }

    #[test]
    fn test_decimal_to_u256_valid_18_decimals() {
        let amount = Decimal::from_str("3").unwrap();
        let result = decimal_to_u256(amount, 18).unwrap();
        assert_eq!(result, U256::from_str("3000000000000000000").unwrap());
    }

    #[test]
    fn test_decimal_to_u256_negative_amount_fails() {
        let amount = Decimal::from_str("-100").unwrap();
        let result = decimal_to_u256(amount, 6);
        assert!(matches!(result, Err(VaultCliError::NegativeAmount(_))));
    }

    #[test]
    fn test_decimal_to_u256_zero() {
        let amount = Decimal::ZERO;
        let result = decimal_to_u256(amount, 6).unwrap();
        assert_eq!(result, U256::ZERO);
    }
}
