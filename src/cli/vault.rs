//! Raindex vault deposit and withdrawal CLI commands.

use alloy::primitives::{Address, B256, U256};
use anyhow::Context;
use rain_math_float::{Float, FloatError};
use std::io::Write;
use thiserror::Error;

use st0x_config::Ctx;
use st0x_evm::{Evm, IERC20, OpenChainErrorRegistry, USDC_BASE};
use st0x_float_macro::float;
use st0x_float_serde::format_float_with_fallback;
use st0x_raindex::{Raindex, RaindexService, RaindexVaultId};

pub(super) struct Deposit {
    pub(super) amount: Float,
    pub(super) token: Address,
    pub(super) vault_id: B256,
}

pub(super) struct Withdraw {
    pub(super) amount: Float,
    pub(super) token: Address,
    pub(super) vault_id: B256,
}

#[derive(Debug, Error)]
pub(crate) enum VaultCliError {
    #[error("negative amount: {}", format_float_with_fallback(.0))]
    NegativeAmount(Float),

    #[error("float operation failed: {0}")]
    Float(#[from] FloatError),

    #[error("failed to parse scaled amount as U256")]
    ParseError(#[from] alloy::primitives::ruint::ParseError),
}

fn float_to_u256(amount: Float, decimals: u8) -> Result<U256, VaultCliError> {
    if amount.lt(Float::zero()?)? {
        return Err(VaultCliError::NegativeAmount(amount));
    }

    Ok(amount.to_fixed_decimal(decimals)?)
}

async fn get_token_decimals<E: Evm>(evm: &E, token: Address) -> anyhow::Result<u8> {
    evm.call::<OpenChainErrorRegistry, _>(token, IERC20::decimalsCall {})
        .await
        .with_context(|| format!("failed to read decimals() for token {token}"))
}

pub(super) async fn vault_deposit_command<Writer: Write>(
    stdout: &mut Writer,
    deposit: Deposit,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let Deposit {
        amount,
        token,
        vault_id,
    } = deposit;
    writeln!(stdout, "Depositing tokens to Raindex vault")?;
    writeln!(stdout, "   Amount: {}", format_float_with_fallback(&amount))?;
    writeln!(stdout, "   Token: {token}")?;

    if amount.lt(float!(0))? {
        return Err(VaultCliError::NegativeAmount(amount).into());
    }

    let wallet_ctx = ctx.wallet()?;
    let sender_address = wallet_ctx.base_wallet().address();

    writeln!(stdout, "   Sender wallet: {sender_address}")?;
    writeln!(stdout, "   Inventory: {}", ctx.evm.inventory_address())?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {vault_id}")?;

    let raindex_service = RaindexService::new(
        wallet_ctx.base_wallet().clone(),
        crate::onchain::raindex_contracts(&ctx.evm),
        sender_address,
    );

    let token_decimals = get_token_decimals(wallet_ctx.base_wallet(), token).await?;
    writeln!(stdout, "   Decimals: {token_decimals}")?;
    let amount_u256 = float_to_u256(amount, token_decimals)?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    writeln!(stdout, "   Depositing to vault (approve + deposit)...")?;
    let deposit_tx = raindex_service
        .deposit::<OpenChainErrorRegistry>(
            token,
            RaindexVaultId(vault_id),
            amount_u256,
            token_decimals,
        )
        .await?;
    writeln!(stdout, "   Deposit tx: {deposit_tx}")?;

    writeln!(stdout, "Vault deposit completed successfully!")?;

    Ok(())
}

pub(super) async fn vault_withdraw_command<Writer: Write>(
    stdout: &mut Writer,
    withdraw: Withdraw,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let Withdraw {
        amount,
        token,
        vault_id,
    } = withdraw;

    writeln!(stdout, "Withdrawing tokens from Raindex vault")?;
    writeln!(stdout, "   Amount: {}", format_float_with_fallback(&amount))?;
    writeln!(stdout, "   Token: {token}")?;

    if amount.lt(float!(0))? {
        return Err(VaultCliError::NegativeAmount(amount).into());
    }

    let wallet_ctx = ctx.wallet()?;
    let sender_address = wallet_ctx.base_wallet().address();

    writeln!(stdout, "   Recipient wallet: {sender_address}")?;
    writeln!(stdout, "   Inventory: {}", ctx.evm.inventory_address())?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {vault_id}")?;

    let raindex_service = RaindexService::new(
        wallet_ctx.base_wallet().clone(),
        crate::onchain::raindex_contracts(&ctx.evm),
        sender_address,
    );

    let token_decimals = get_token_decimals(wallet_ctx.base_wallet(), token).await?;
    writeln!(stdout, "   Decimals: {token_decimals}")?;
    let amount_u256 = float_to_u256(amount, token_decimals)?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    writeln!(stdout, "   Withdrawing from vault...")?;
    let withdraw_tx = raindex_service
        .withdraw(token, RaindexVaultId(vault_id), amount_u256, token_decimals)
        .await?;
    writeln!(stdout, "   Withdraw tx: {withdraw_tx}")?;

    writeln!(stdout, "Vault withdrawal completed successfully!")?;

    Ok(())
}

pub(super) async fn vault_withdraw_usdc_command<Writer: Write>(
    stdout: &mut Writer,
    amount: st0x_finance::Usdc,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    ctx.wallet()?;

    let cash =
        ctx.assets.cash.as_ref().ok_or_else(|| {
            anyhow::anyhow!("assets.cash.vault_ids is required but not configured")
        })?;

    let vault_id =
        cash.vault_ids.first().copied().ok_or_else(|| {
            anyhow::anyhow!("assets.cash.vault_ids is required but not configured")
        })?;

    if cash.vault_ids.len() > 1 {
        writeln!(
            stdout,
            "Warning: {} USDC vaults configured, using the first one: {vault_id}",
            cash.vault_ids.len()
        )?;
    }

    let withdraw = Withdraw {
        amount: amount.into(),
        token: USDC_BASE,
        vault_id,
    };

    vault_withdraw_command(stdout, withdraw, ctx).await
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::sol_types::SolCall;
    use url::Url;

    use st0x_config::ExecutionThreshold;
    use st0x_config::RebalancingCtx;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        AssetsConfig, BrokerCtx, CashAssetConfig, EquitiesConfig, LogLevel, OperationMode,
        TradingMode,
    };
    use st0x_config::{EvmCtx, IngestionCutoff, InventoryMode};
    use st0x_evm::IERC20::decimalsCall;
    use st0x_evm::ReadOnlyEvm;
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::ImbalanceThreshold;

    fn create_ctx_without_rebalancing() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            board_port: 8081,
            evm: EvmCtx {
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                inventory: InventoryMode::Managed {
                    inventory: address!("0x2345678901234567890123456789012345678901"),
                },
                vault_owner: Address::ZERO,
                deployment_block: 1,
                required_confirmations: 0,
                ingestion_cutoff: IngestionCutoff::Safe,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            order_fill_poll_interval: 5,
            extended_hours_reprice_timeout_secs: 300,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            alerts: None,
            trading_mode: TradingMode::Standalone,
            order_owner: Address::ZERO,
            wallet: None,
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            travel_rule: None,
            rest_api: None,
            issuance: create_test_issuance_ctx(),
            redemption_wallet: None,
        }
    }

    fn create_ctx_with_rebalancing(cash: Option<CashAssetConfig>) -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            board_port: 8081,
            evm: EvmCtx {
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                inventory: InventoryMode::Managed {
                    inventory: address!("0x2345678901234567890123456789012345678901"),
                },
                vault_owner: Address::ZERO,
                deployment_block: 1,
                required_confirmations: 0,
                ingestion_cutoff: IngestionCutoff::Safe,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            order_fill_poll_interval: 5,
            extended_hours_reprice_timeout_secs: 300,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            alerts: None,
            trading_mode: TradingMode::Rebalancing(Box::new(
                RebalancingCtx::stub()
                    .equity(ImbalanceThreshold {
                        target: float!(0.5),
                        deviation: float!(0.1),
                    })
                    .usdc(ImbalanceThreshold {
                        target: float!(0.5),
                        deviation: float!(0.1),
                    })
                    .call(),
            )),
            order_owner: Address::ZERO,
            wallet: Some(st0x_config::OnchainWalletCtx::stub()),
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash,
            },
            travel_rule: None,
            rest_api: None,
            issuance: create_test_issuance_ctx(),
            redemption_wallet: Some(Address::ZERO),
        }
    }

    const TEST_TOKEN: Address = address!("9876543210987654321098765432109876543210");
    const TEST_VAULT_ID: B256 =
        b256!("0000000000000000000000000000000000000000000000000000000000000001");

    #[tokio::test]
    async fn test_vault_deposit_requires_wallet_config() {
        let ctx = create_ctx_without_rebalancing();
        let amount = float!(100);

        let mut stdout = Vec::new();
        let deposit = Deposit {
            amount,
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
        };
        let result = vault_deposit_command(&mut stdout, deposit, &ctx).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("configured [wallet] section"),
            "Expected wallet config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_withdraw_requires_wallet_config() {
        let ctx = create_ctx_without_rebalancing();
        let withdraw = Withdraw {
            amount: float!(100),
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
        };

        let mut stdout = Vec::new();
        let result = vault_withdraw_command(&mut stdout, withdraw, &ctx).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("configured [wallet] section"),
            "Expected wallet config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_deposit_writes_amount_to_stdout() {
        let ctx = create_ctx_without_rebalancing();

        let mut stdout = Vec::new();
        let deposit = Deposit {
            amount: float!(500.5),
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
        };
        vault_deposit_command(&mut stdout, deposit, &ctx)
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
        let withdraw = Withdraw {
            amount: float!(250.25),
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
        };

        let mut stdout = Vec::new();
        vault_withdraw_command(&mut stdout, withdraw, &ctx)
            .await
            .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("250.25"),
            "Expected amount in output, got: {output}"
        );
        assert!(
            output.contains(&TEST_TOKEN.to_string()),
            "Expected token in output, got: {output}"
        );
    }

    #[test]
    fn test_float_to_u256_valid_6_decimals() {
        let amount = float!(100.5);
        let result = float_to_u256(amount, 6).unwrap();
        assert_eq!(result, U256::from(100_500_000u64));
    }

    #[test]
    fn test_float_to_u256_valid_18_decimals() {
        let amount = float!(3);
        let result = float_to_u256(amount, 18).unwrap();
        assert_eq!(
            result,
            U256::from_str_radix("3000000000000000000", 10).unwrap()
        );
    }

    #[test]
    fn test_float_to_u256_negative_amount_fails() {
        let amount = float!(-100);
        let result = float_to_u256(amount, 6);
        assert!(matches!(result, Err(VaultCliError::NegativeAmount(_))));
    }

    #[test]
    fn test_float_to_u256_zero() {
        let amount = Float::zero().unwrap();
        let result = float_to_u256(amount, 6).unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[tokio::test]
    async fn get_token_decimals_reads_metadata() {
        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let decimals = get_token_decimals(&evm, TEST_TOKEN).await.unwrap();

        assert_eq!(decimals, 18);
    }

    #[tokio::test]
    async fn get_token_decimals_reads_usdc_metadata() {
        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let decimals = get_token_decimals(&evm, USDC_BASE).await.unwrap();

        assert_eq!(decimals, 6);
    }

    #[tokio::test]
    async fn get_token_decimals_adds_token_context_on_failure() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("boom");
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let err = get_token_decimals(&evm, TEST_TOKEN).await.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("failed to read decimals() for token"),
            "Expected token decimals context, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&TEST_TOKEN.to_string()),
            "Expected token address in error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_vault_deposit_negative_amount_fails_before_metadata_lookup() {
        let ctx = create_ctx_with_rebalancing(None);
        let deposit = Deposit {
            amount: float!(-1),
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
        };

        let mut stdout = Vec::new();
        let result = vault_deposit_command(&mut stdout, deposit, &ctx).await;

        assert!(matches!(
            result,
            Err(ref error) if error.to_string().contains("negative amount")
        ));
    }

    #[tokio::test]
    async fn test_vault_withdraw_negative_amount_fails_before_metadata_lookup() {
        let ctx = create_ctx_with_rebalancing(None);
        let withdraw = Withdraw {
            amount: float!(-1),
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
        };

        let mut stdout = Vec::new();
        let result = vault_withdraw_command(&mut stdout, withdraw, &ctx).await;

        assert!(matches!(
            result,
            Err(ref error) if error.to_string().contains("negative amount")
        ));
    }

    #[tokio::test]
    async fn withdraw_usdc_fails_when_cash_vault_id_missing() {
        let ctx = create_ctx_with_rebalancing(Some(CashAssetConfig {
            vault_ids: Vec::new(),
            rebalancing: OperationMode::Enabled,
            operational_limit: None,
            reserved: None,
        }));
        let amount = Usdc::new(float!(100));

        let mut stdout = Vec::new();
        let err_msg = vault_withdraw_usdc_command(&mut stdout, amount, &ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err_msg.contains("assets.cash.vault_ids is required but not configured"),
            "Expected vault_id missing error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn withdraw_usdc_requires_wallet_config_before_cash_vault_lookup() {
        let amount = Usdc::new(float!(100));

        let mut stdout = Vec::new();
        let err_msg =
            vault_withdraw_usdc_command(&mut stdout, amount, &create_ctx_without_rebalancing())
                .await
                .unwrap_err()
                .to_string();

        assert!(
            err_msg.contains("configured [wallet] section"),
            "Expected wallet config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn withdraw_usdc_fails_when_cash_config_missing() {
        let ctx = create_ctx_with_rebalancing(None);
        let amount = Usdc::new(float!(100));

        let mut stdout = Vec::new();
        let err_msg = vault_withdraw_usdc_command(&mut stdout, amount, &ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err_msg.contains("assets.cash.vault_ids is required but not configured"),
            "Expected vault_id missing error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn withdraw_usdc_passes_cash_vault_lookup_when_vault_id_configured() {
        let ctx = create_ctx_with_rebalancing(Some(CashAssetConfig {
            vault_ids: vec![TEST_VAULT_ID],
            rebalancing: OperationMode::Enabled,
            operational_limit: None,
            reserved: None,
        }));
        let amount = Usdc::new(float!(100));

        let mut stdout = Vec::new();
        let result = vault_withdraw_usdc_command(&mut stdout, amount, &ctx).await;

        // The vault lookup succeeds but the raindex service call will fail
        // because we're using stub wallets. The important thing is we got
        // past the vault_id lookup.
        result.unwrap_err();
        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains(&TEST_VAULT_ID.to_string()),
            "Expected vault ID in output (proving lookup succeeded), got: {output}"
        );
        assert!(
            output.contains(&USDC_BASE.to_string()),
            "Expected USDC token in output, got: {output}"
        );
    }

    #[test]
    fn test_float_to_u256_valid_18_decimals_fractional() {
        let amount = float!(1.25);
        let result = float_to_u256(amount, 18).unwrap();
        assert_eq!(
            result,
            U256::from_str_radix("1250000000000000000", 10).unwrap()
        );
    }
}
