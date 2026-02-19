//! Raindex vault deposit and withdrawal CLI commands.

use alloy::primitives::{Address, B256, U256};
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;
use thiserror::Error;

use st0x_event_sorcery::Projection;
use st0x_evm::OpenChainErrorRegistry;

use crate::config::Ctx;
use crate::onchain::raindex::{RaindexService, RaindexVaultId};
use crate::threshold::Usdc;
use crate::vault_registry::VaultRegistry;

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

pub(super) async fn vault_deposit_command<W: Write>(
    stdout: &mut W,
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
    writeln!(stdout, "   Amount: {amount}")?;
    writeln!(stdout, "   Token: {token}")?;
    writeln!(stdout, "   Decimals: {decimals}")?;

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let sender_address = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "   Sender wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {vault_id}")?;

    let amount_u256 = decimal_to_u256(amount, decimals)?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    let vault_registry_projection = Arc::new(Projection::<VaultRegistry>::sqlite(pool.clone())?);

    let raindex_service = RaindexService::new(
        rebalancing_ctx.base_wallet().clone(),
        ctx.evm.orderbook,
        vault_registry_projection,
        sender_address,
    );

    writeln!(
        stdout,
        "   Depositing to vault (approve + deposit via Fireblocks)..."
    )?;
    let deposit_tx = raindex_service
        .deposit::<OpenChainErrorRegistry>(token, RaindexVaultId(vault_id), amount_u256, decimals)
        .await?;
    writeln!(stdout, "   Deposit tx: {deposit_tx}")?;

    writeln!(stdout, "Vault deposit completed successfully!")?;

    Ok(())
}

pub(super) async fn vault_withdraw_command<W: Write>(
    stdout: &mut W,
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
    writeln!(stdout, "   Vault ID: {}", rebalancing_ctx.usdc_vault_id)?;

    let vault_registry_projection = Arc::new(Projection::<VaultRegistry>::sqlite(pool.clone())?);

    let raindex_service = RaindexService::new(
        rebalancing_ctx.base_wallet().clone(),
        ctx.evm.orderbook,
        vault_registry_projection,
        sender_address,
    );

    let vault_id = RaindexVaultId(rebalancing_ctx.usdc_vault_id);

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
    use rust_decimal_macros::dec;
    use std::str::FromStr;
    use url::Url;

    use super::*;
    use crate::config::{BrokerCtx, LogLevel, TradingMode};
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
            broker: BrokerCtx::DryRun,
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: Address::ZERO,
            },
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    const TEST_TOKEN: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    const TEST_VAULT_ID: B256 =
        b256!("0000000000000000000000000000000000000000000000000000000000000001");

    #[tokio::test]
    async fn test_vault_deposit_requires_rebalancing_ctx() {
        let ctx = create_ctx_without_rebalancing();
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
        let amount = Usdc(Decimal::from_str("100").unwrap());

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
            amount: dec!(500.50),
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
            output.contains("500.50"),
            "Expected amount in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_vault_withdraw_writes_amount_to_stdout() {
        let ctx = create_ctx_without_rebalancing();
        let amount = Usdc(Decimal::from_str("250.25").unwrap());

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
