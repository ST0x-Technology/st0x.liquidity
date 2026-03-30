//! Raindex vault deposit and withdrawal CLI commands.

use alloy::primitives::{Address, B256, U256};
use rain_math_float::{Float, FloatError};
use sqlx::SqlitePool;
use std::io::Write;
use thiserror::Error;

use st0x_event_sorcery::StoreBuilder;
use st0x_evm::{Evm, OpenChainErrorRegistry};
use st0x_float_serde::format_float_with_fallback;

use crate::bindings::IERC20;
use crate::config::Ctx;
use crate::onchain::USDC_BASE;
use crate::onchain::raindex::{Raindex, RaindexService, RaindexVaultId};
use crate::vault_registry::VaultRegistry;

pub(super) struct Deposit {
    pub(super) amount: Float,
    pub(super) token: Address,
    pub(super) vault_id: B256,
    pub(super) decimals: u8,
}

pub(super) struct Withdraw {
    pub(super) amount: Float,
    pub(super) token: Address,
    pub(super) vault_id: B256,
    pub(super) decimals: u8,
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

async fn validate_token_decimals<E: Evm>(
    evm: &E,
    token: Address,
    provided_decimals: u8,
) -> anyhow::Result<u8> {
    let onchain_decimals: u8 = evm
        .call::<OpenChainErrorRegistry, _>(token, IERC20::decimalsCall {})
        .await?;

    if onchain_decimals != provided_decimals {
        anyhow::bail!(
            "token {token} decimals mismatch: provided {provided_decimals}, onchain \
             metadata reports {onchain_decimals}"
        );
    }

    Ok(onchain_decimals)
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
    writeln!(stdout, "   Amount: {}", format_float_with_fallback(&amount))?;
    writeln!(stdout, "   Token: {token}")?;
    writeln!(stdout, "   Decimals: {decimals}")?;

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let sender_address = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "   Sender wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {vault_id}")?;

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

    let verified_decimals =
        validate_token_decimals(rebalancing_ctx.base_wallet(), token, decimals).await?;
    let amount_u256 = float_to_u256(amount, verified_decimals)?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    writeln!(stdout, "   Depositing to vault (approve + deposit)...")?;
    let deposit_tx = raindex_service
        .deposit::<OpenChainErrorRegistry>(
            token,
            RaindexVaultId(vault_id),
            amount_u256,
            verified_decimals,
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
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let Withdraw {
        amount,
        token,
        vault_id,
        decimals,
    } = withdraw;

    writeln!(stdout, "Withdrawing tokens from Raindex vault")?;
    writeln!(stdout, "   Amount: {}", format_float_with_fallback(&amount))?;
    writeln!(stdout, "   Token: {token}")?;
    writeln!(stdout, "   Decimals: {decimals}")?;

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let sender_address = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "   Recipient wallet: {sender_address}")?;
    writeln!(stdout, "   Orderbook: {}", ctx.evm.orderbook)?;
    writeln!(stdout, "   Vault ID: {vault_id}")?;

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

    let verified_decimals =
        validate_token_decimals(rebalancing_ctx.base_wallet(), token, decimals).await?;
    let amount_u256 = float_to_u256(amount, verified_decimals)?;
    writeln!(stdout, "   Amount (smallest unit): {amount_u256}")?;

    writeln!(stdout, "   Withdrawing from vault...")?;
    let withdraw_tx = raindex_service
        .withdraw(
            token,
            RaindexVaultId(vault_id),
            amount_u256,
            verified_decimals,
        )
        .await?;
    writeln!(stdout, "   Withdraw tx: {withdraw_tx}")?;

    writeln!(stdout, "Vault withdrawal completed successfully!")?;

    Ok(())
}

pub(super) async fn vault_withdraw_usdc_command<Writer: Write>(
    stdout: &mut Writer,
    amount: st0x_finance::Usdc,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let vault_id = ctx
        .assets
        .cash
        .as_ref()
        .and_then(|cash| cash.vault_id)
        .ok_or_else(|| anyhow::anyhow!("assets.cash.vault_id is required but not configured"))?;

    let withdraw = Withdraw {
        amount: amount.into(),
        token: USDC_BASE,
        vault_id,
        decimals: 6,
    };

    vault_withdraw_command(stdout, withdraw, ctx, pool).await
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::sol_types::SolCall;
    use url::Url;

    use st0x_evm::ReadOnlyEvm;
    use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};
    use st0x_finance::Usdc;

    use super::*;
    use crate::bindings::IERC20::decimalsCall;
    use crate::config::{
        AssetsConfig, BrokerCtx, CashAssetConfig, EquitiesConfig, LogLevel, OperationMode,
        TradingMode,
    };
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::EvmCtx;
    use crate::rebalancing::RebalancingCtx;
    use crate::threshold::ExecutionThreshold;
    use st0x_float_macro::float;

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

    fn create_ctx_with_rebalancing(cash: Option<CashAssetConfig>) -> Ctx {
        let alpaca_broker_auth = AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };

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
                    .redemption_wallet(Address::ZERO)
                    .alpaca_broker_auth(alpaca_broker_auth)
                    .call(),
            )),
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash,
            },
        }
    }

    const TEST_TOKEN: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    const TEST_VAULT_ID: B256 =
        b256!("0000000000000000000000000000000000000000000000000000000000000001");

    #[tokio::test]
    async fn test_vault_deposit_requires_rebalancing_ctx() {
        let ctx = create_ctx_without_rebalancing();
        let amount = float!(100);

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
        let withdraw = Withdraw {
            amount: float!(100),
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            decimals: 18,
        };

        let mut stdout = Vec::new();
        let result = vault_withdraw_command(
            &mut stdout,
            withdraw,
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
            amount: float!(500.5),
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
        let withdraw = Withdraw {
            amount: float!(250.25),
            token: TEST_TOKEN,
            vault_id: TEST_VAULT_ID,
            decimals: 18,
        };

        let mut stdout = Vec::new();
        vault_withdraw_command(
            &mut stdout,
            withdraw,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
        )
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
        assert!(
            output.contains("18"),
            "Expected decimals in output, got: {output}"
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
    async fn validate_token_decimals_accepts_matching_metadata() {
        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let decimals = validate_token_decimals(&evm, TEST_TOKEN, 18).await.unwrap();

        assert_eq!(decimals, 18);
    }

    #[tokio::test]
    async fn validate_token_decimals_rejects_metadata_mismatch() {
        let asserter = Asserter::new();
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let err = validate_token_decimals(&evm, TEST_TOKEN, 6)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains(&TEST_TOKEN.to_string()),
            "Expected token address in mismatch error, got: {err}"
        );
        assert!(
            err.contains("provided 6"),
            "Expected provided decimals in mismatch error, got: {err}"
        );
        assert!(
            err.contains("metadata reports 18"),
            "Expected onchain decimals in mismatch error, got: {err}"
        );
    }

    #[tokio::test]
    async fn withdraw_usdc_fails_when_cash_vault_id_missing() {
        let ctx = create_ctx_with_rebalancing(Some(CashAssetConfig {
            vault_id: None,
            rebalancing: OperationMode::Enabled,
            operational_limit: None,
        }));
        let amount = Usdc::new(float!(100));

        let mut stdout = Vec::new();
        let err_msg = vault_withdraw_usdc_command(
            &mut stdout,
            amount,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            err_msg.contains("assets.cash.vault_id is required but not configured"),
            "Expected vault_id missing error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn withdraw_usdc_fails_when_cash_config_missing() {
        let ctx = create_ctx_with_rebalancing(None);
        let amount = Usdc::new(float!(100));

        let mut stdout = Vec::new();
        let err_msg = vault_withdraw_usdc_command(
            &mut stdout,
            amount,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            err_msg.contains("assets.cash.vault_id is required but not configured"),
            "Expected vault_id missing error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn withdraw_usdc_passes_cash_vault_lookup_when_vault_id_configured() {
        let ctx = create_ctx_with_rebalancing(Some(CashAssetConfig {
            vault_id: Some(TEST_VAULT_ID),
            rebalancing: OperationMode::Enabled,
            operational_limit: None,
        }));
        let amount = Usdc::new(float!(100));

        let mut stdout = Vec::new();
        let result = vault_withdraw_usdc_command(
            &mut stdout,
            amount,
            &ctx,
            &SqlitePool::connect(":memory:").await.unwrap(),
        )
        .await;

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
            output.contains(&crate::onchain::USDC_BASE.to_string()),
            "Expected USDC token in output, got: {output}"
        );
        assert!(
            output.contains("Decimals: 6"),
            "Expected USDC decimals in output, got: {output}"
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
