//! Raindex vault deposit and withdrawal CLI commands.

use alloy::primitives::{Address, B256, U256};
use rain_math_float::{Float, FloatError};
use sqlx::SqlitePool;
use std::io::Write;
use thiserror::Error;

use st0x_event_sorcery::StoreBuilder;
use st0x_evm::OpenChainErrorRegistry;
use st0x_float_serde::format_float;

use crate::config::Ctx;
use crate::onchain::raindex::{RaindexService, RaindexVaultId};
use crate::threshold::Usdc;
use crate::vault_registry::VaultRegistry;

pub(super) struct Deposit {
    pub(super) amount: Float,
    pub(super) token: Address,
    pub(super) vault_id: B256,
    pub(super) decimals: u8,
}

#[derive(Debug, Error)]
pub(crate) enum VaultCliError {
    #[error("negative amount: {}", format_float(.0))]
    NegativeAmount(Float),

    #[error("float operation failed: {0}")]
    Float(#[from] FloatError),

    #[error("failed to parse scaled amount as U256")]
    ParseError(#[from] alloy::primitives::ruint::ParseError),

    #[error(
        "assets.cash.vault_id is required for USDC withdrawal \
         but not configured"
    )]
    MissingCashVaultId,
}

fn float_to_u256(amount: Float, decimals: u8) -> Result<U256, VaultCliError> {
    if amount.lt(Float::zero()?)? {
        return Err(VaultCliError::NegativeAmount(amount));
    }

    Ok(amount.to_fixed_decimal(decimals)?)
}

pub(super) async fn vault_deposit_command<Writer: Write>(
    stdout: &mut Writer,
    deposit: Deposit,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let Deposit {
        amount,
        token,
        vault_id,
        decimals,
    } = deposit;
    writeln!(stdout, "Depositing tokens to Raindex vault")?;
    writeln!(stdout, "   Amount: {}", format_float(&amount))?;
    writeln!(stdout, "   Token: {token}")?;
    writeln!(stdout, "   Decimals: {decimals}")?;

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let sender_address = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "   Sender wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {vault_id}")?;

    let amount_u256 = float_to_u256(amount, decimals)?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    let (_vault_store, vault_registry_projection) =
        StoreBuilder::<VaultRegistry>::new(pool.clone())
            .build(())
            .await?;

    let raindex_service = RaindexService::new(
        rebalancing_ctx.base_wallet().clone(),
        ctx.evm.orderbook,
        vault_registry_projection,
        sender_address,
    );

    writeln!(stdout, "   Depositing to vault (approve + deposit)...")?;
    let deposit_tx = raindex_service
        .deposit::<OpenChainErrorRegistry>(token, RaindexVaultId(vault_id), amount_u256, decimals)
        .await?;
    writeln!(stdout, "   Deposit tx: {deposit_tx}")?;

    writeln!(stdout, "Vault deposit completed successfully!")?;

    Ok(())
}

pub(super) async fn vault_withdraw_command<Writer: Write>(
    stdout: &mut Writer,
    amount: Usdc,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    writeln!(stdout, "Withdrawing USDC from Raindex vault")?;
    writeln!(stdout, "   Amount: {amount} USDC")?;

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let sender_address = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "   Recipient wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    let usdc_vault_id = ctx
        .assets
        .cash
        .as_ref()
        .and_then(|cash| cash.vault_id)
        .ok_or(VaultCliError::MissingCashVaultId)?;

    writeln!(stdout, "   Vault ID: {usdc_vault_id}")?;

    let (_vault_store, vault_registry_projection) =
        StoreBuilder::<VaultRegistry>::new(pool.clone())
            .build(())
            .await?;

    let raindex_service = RaindexService::new(
        rebalancing_ctx.base_wallet().clone(),
        ctx.evm.orderbook,
        vault_registry_projection,
        sender_address,
    );

    let vault_id = RaindexVaultId(usdc_vault_id);

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
    use alloy::primitives::{Address, address, b256};
    use url::Url;

    use super::*;
    use crate::config::{AssetsConfig, BrokerCtx, EquitiesConfig, LogLevel, TradingMode};
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
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: Address::ZERO,
            },
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
        }
    }

    const TEST_TOKEN: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    const TEST_VAULT_ID: B256 =
        b256!("0000000000000000000000000000000000000000000000000000000000000001");

    #[tokio::test]
    async fn test_vault_deposit_requires_rebalancing_ctx() {
        let ctx = create_ctx_without_rebalancing();
        let amount = float!("100");

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
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing mode"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_withdraw_requires_rebalancing_ctx() {
        let ctx = create_ctx_without_rebalancing();
        let amount = Usdc(float!("100"));

        let mut stdout = Vec::new();
        let result = vault_withdraw_command(
            &mut stdout,
            amount,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing mode"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_deposit_writes_amount_to_stdout() {
        let ctx = create_ctx_without_rebalancing();

        let mut stdout = Vec::new();
        let deposit = Deposit {
            amount: float!("500.5"),
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            decimals: 6,
        };
        vault_deposit_command(
            &mut stdout,
            deposit,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
        )
        .await
        .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("500.5"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_vault_withdraw_writes_amount_to_stdout() {
        let ctx = create_ctx_without_rebalancing();
        let amount = Usdc(float!("250.25"));

        let mut stdout = Vec::new();
        vault_withdraw_command(
            &mut stdout,
            amount,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
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
    fn test_float_to_u256_valid_6_decimals() {
        let amount = float!("100.5");
        let result = float_to_u256(amount, 6).unwrap();
        assert_eq!(result, U256::from(100_500_000u64));
    }

    #[test]
    fn test_float_to_u256_valid_18_decimals() {
        let amount = float!("3");
        let result = float_to_u256(amount, 18).unwrap();
        assert_eq!(
            result,
            U256::from_str_radix("3000000000000000000", 10).unwrap()
        );
    }

    #[test]
    fn test_float_to_u256_negative_amount_fails() {
        let amount = float!("-100");
        let result = float_to_u256(amount, 6);
        assert!(matches!(result, Err(VaultCliError::NegativeAmount(_))));
    }

    #[test]
    fn test_float_to_u256_zero() {
        let amount = Float::zero().unwrap();
        let result = float_to_u256(amount, 6).unwrap();
        assert_eq!(result, U256::ZERO);
    }
}
