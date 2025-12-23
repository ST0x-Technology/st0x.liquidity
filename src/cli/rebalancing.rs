//! Transfer equity and USDC rebalancing CLI commands.

use alloy::network::EthereumWallet;
use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::signers::local::PrivateKeySigner;
use cqrs_es::CqrsFramework;
use cqrs_es::persist::PersistedEventStore;
use sqlite_es::SqliteEventRepository;
use sqlx::SqlitePool;
use std::io::Write;
use std::sync::Arc;

use st0x_broker::Symbol;

use crate::alpaca_tokenization::AlpacaTokenizationService;
use crate::alpaca_wallet::AlpacaWalletService;
use crate::cctp::{
    CctpBridge, Evm, MESSAGE_TRANSMITTER_V2, TOKEN_MESSENGER_V2, USDC_BASE, USDC_ETHEREUM,
};
use crate::env::{BrokerConfig, Config};
use crate::equity_redemption::RedemptionAggregateId;
use crate::onchain::vault::{VaultId, VaultService};
use crate::rebalancing::mint::Mint;
use crate::rebalancing::redemption::Redeem;
use crate::rebalancing::usdc::UsdcRebalanceManager;
use crate::rebalancing::{MintManager, RedemptionManager};
use crate::shares::FractionalShares;
use crate::threshold::Usdc;
use crate::tokenized_equity_mint::IssuerRequestId;
use crate::usdc_rebalance::UsdcRebalanceId;

use super::TransferDirection;

pub(super) async fn transfer_equity_command<W: Write>(
    stdout: &mut W,
    direction: TransferDirection,
    symbol: &Symbol,
    quantity: FractionalShares,
    token_address: Option<Address>,
    config: &Config,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let direction_str = match direction {
        TransferDirection::ToRaindex => "Alpaca → Raindex (mint)",
        TransferDirection::ToAlpaca => "Raindex → Alpaca (redeem)",
    };

    writeln!(stdout, "🔄 Transferring equity: {direction_str}")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;

    let BrokerConfig::Alpaca(alpaca_auth) = &config.broker else {
        anyhow::bail!("transfer-equity requires Alpaca broker configuration");
    };

    let rebalancing_config = config.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "transfer-equity requires rebalancing configuration (set REBALANCING_ENABLED=true)"
        )
    })?;

    let ws = WsConnect::new(config.evm.ws_rpc_url.as_str());
    let base_provider = ProviderBuilder::new().connect_ws(ws).await?;

    let tokenization_service = Arc::new(AlpacaTokenizationService::new(
        alpaca_auth.base_url(),
        alpaca_auth.alpaca_api_key.clone(),
        alpaca_auth.alpaca_api_secret.clone(),
        base_provider.clone(),
        rebalancing_config.redemption_wallet,
    ));

    match direction {
        TransferDirection::ToRaindex => {
            writeln!(stdout, "   Creating mint request...")?;

            let mint_store =
                PersistedEventStore::new_event_store(SqliteEventRepository::new(pool.clone()));
            let mint_cqrs = Arc::new(CqrsFramework::new(mint_store, vec![], ()));
            let mint_manager = MintManager::new(tokenization_service, mint_cqrs);

            let issuer_request_id =
                IssuerRequestId::new(format!("cli-mint-{}", uuid::Uuid::new_v4()));

            let wallet = rebalancing_config.market_maker_wallet;

            writeln!(stdout, "   Issuer Request ID: {}", issuer_request_id.0)?;
            writeln!(stdout, "   Receiving Wallet: {wallet}")?;

            mint_manager
                .execute_mint(&issuer_request_id, symbol.clone(), quantity, wallet)
                .await?;

            writeln!(stdout, "✅ Mint completed successfully")?;
        }

        TransferDirection::ToAlpaca => {
            let token = token_address.ok_or_else(|| {
                anyhow::anyhow!("--token-address is required for to-alpaca direction (redemption)")
            })?;

            writeln!(stdout, "   Token Address: {token}")?;
            writeln!(stdout, "   Sending tokens for redemption...")?;

            let redemption_store =
                PersistedEventStore::new_event_store(SqliteEventRepository::new(pool.clone()));
            let redemption_cqrs = Arc::new(CqrsFramework::new(redemption_store, vec![], ()));
            let redemption_manager = RedemptionManager::new(tokenization_service, redemption_cqrs);

            let aggregate_id =
                RedemptionAggregateId::new(format!("cli-redeem-{}", uuid::Uuid::new_v4()));

            let amount = quantity.to_u256_18_decimals()?;

            writeln!(stdout, "   Aggregate ID: {}", aggregate_id.0)?;
            writeln!(stdout, "   Amount (wei): {amount}")?;

            redemption_manager
                .execute_redemption(&aggregate_id, symbol.clone(), quantity, token, amount)
                .await?;

            writeln!(stdout, "✅ Redemption completed successfully")?;
        }
    }

    Ok(())
}

pub(super) async fn transfer_usdc_command<W: Write, BP>(
    stdout: &mut W,
    direction: TransferDirection,
    amount: Usdc,
    config: &Config,
    pool: &SqlitePool,
    base_provider: BP,
) -> anyhow::Result<()>
where
    BP: Provider + Clone + Send + Sync + 'static,
{
    let dir = match direction {
        TransferDirection::ToRaindex => "Alpaca -> Raindex",
        TransferDirection::ToAlpaca => "Raindex -> Alpaca",
    };
    writeln!(stdout, "Transferring USDC: {dir}, Amount: {amount} USDC")?;

    let BrokerConfig::Alpaca(alpaca_auth) = &config.broker else {
        anyhow::bail!("transfer-usdc requires Alpaca broker configuration");
    };

    let rebalancing_config = config.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!("transfer-usdc requires rebalancing config (REBALANCING_ENABLED=true)")
    })?;

    writeln!(stdout, "   Vault ID: {}", rebalancing_config.usdc_vault_id)?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.ethereum_private_key)?;

    let ethereum_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(rebalancing_config.ethereum_rpc_url.clone());

    let base_provider_with_wallet = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_provider(base_provider);

    let broker_url = if alpaca_auth.is_paper_trading() {
        "https://broker-api.sandbox.alpaca.markets"
    } else {
        "https://broker-api.alpaca.markets"
    };

    let alpaca_wallet = Arc::new(AlpacaWalletService::new(
        broker_url.into(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.alpaca_api_key.clone(),
        alpaca_auth.alpaca_api_secret.clone(),
    ));

    let ethereum_evm = Evm::new(
        ethereum_provider,
        signer.clone(),
        USDC_ETHEREUM,
        TOKEN_MESSENGER_V2,
        MESSAGE_TRANSMITTER_V2,
    );

    let base_cctp = Evm::new(
        base_provider_with_wallet.clone(),
        signer.clone(),
        USDC_BASE,
        TOKEN_MESSENGER_V2,
        MESSAGE_TRANSMITTER_V2,
    );

    let base_vault = Evm::new(
        base_provider_with_wallet,
        signer,
        USDC_BASE,
        TOKEN_MESSENGER_V2,
        MESSAGE_TRANSMITTER_V2,
    );

    let bridge = Arc::new(CctpBridge::new(ethereum_evm, base_cctp));
    let vault_service =
        Arc::new(VaultService::new(base_vault, config.evm.orderbook, USDC_BASE).await?);
    let event_store =
        PersistedEventStore::new_event_store(SqliteEventRepository::new(pool.clone()));
    let cqrs = Arc::new(CqrsFramework::new(event_store, vec![], ()));

    let rebalance_manager = UsdcRebalanceManager::new(
        alpaca_wallet,
        bridge,
        vault_service,
        cqrs,
        rebalancing_config.market_maker_wallet,
        VaultId(rebalancing_config.usdc_vault_id),
    );

    let rebalance_id = UsdcRebalanceId::new(format!("cli-usdc-{}", uuid::Uuid::new_v4()));
    writeln!(
        stdout,
        "   Rebalance ID: {} (may take several minutes)",
        rebalance_id.0
    )?;

    match direction {
        TransferDirection::ToRaindex => {
            rebalance_manager
                .execute_alpaca_to_base(&rebalance_id, amount)
                .await?;
            writeln!(stdout, "USDC transfer to Raindex completed successfully")?;
        }
        TransferDirection::ToAlpaca => {
            rebalance_manager
                .execute_base_to_alpaca(&rebalance_id, amount)
                .await?;
            writeln!(stdout, "USDC transfer to Alpaca completed successfully")?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use rust_decimal::Decimal;
    use st0x_broker::alpaca::{AlpacaAuthEnv, AlpacaTradingMode};
    use std::str::FromStr;

    use super::*;
    use crate::env::LogLevel;
    use crate::onchain::EvmEnv;
    use crate::test_utils::setup_test_db;

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

    fn create_alpaca_config_without_rebalancing() -> Config {
        let mut config = create_config_without_rebalancing();
        config.broker = BrokerConfig::Alpaca(AlpacaAuthEnv {
            alpaca_api_key: "test-key".to_string(),
            alpaca_api_secret: "test-secret".to_string(),
            alpaca_trading_mode: AlpacaTradingMode::Paper,
        });
        config
    }

    fn create_mock_provider() -> impl Provider + Clone + 'static {
        let asserter = Asserter::new();
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    #[tokio::test]
    async fn test_transfer_equity_requires_alpaca_broker() {
        let config = create_config_without_rebalancing();
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares(Decimal::from_str("10.5").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_equity_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            &symbol,
            quantity,
            None,
            &config,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca broker configuration"),
            "Expected Alpaca broker error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_equity_requires_rebalancing_config() {
        let config = create_alpaca_config_without_rebalancing();
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares(Decimal::from_str("10.5").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_equity_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            &symbol,
            quantity,
            None,
            &config,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_requires_alpaca_broker() {
        let config = create_config_without_rebalancing();
        let pool = setup_test_db().await;
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &config,
            &pool,
            provider,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca broker configuration"),
            "Expected Alpaca broker error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_requires_rebalancing_config() {
        let config = create_alpaca_config_without_rebalancing();
        let pool = setup_test_db().await;
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &config,
            &pool,
            provider,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing config"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_writes_direction_to_stdout() {
        let config = create_alpaca_config_without_rebalancing();
        let pool = setup_test_db().await;
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let _ = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &config,
            &pool,
            provider,
        )
        .await;

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Alpaca -> Raindex"),
            "Expected direction in output, got: {output}"
        );
    }
}
