//! Transfer equity and USDC rebalancing CLI commands.

use alloy::network::EthereumWallet;
use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::signers::local::PrivateKeySigner;
use sqlx::SqlitePool;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use st0x_bridge::cctp::{CctpBridge, CctpCtx};
use st0x_event_sorcery::StoreBuilder;
use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Executor, FractionalShares, Symbol,
    TimeInForce,
};

use super::TransferDirection;
use crate::alpaca_tokenization::{
    AlpacaTokenizationService, TokenizationRequest, TokenizationRequestStatus,
};
use crate::alpaca_wallet::AlpacaWalletService;
use crate::bindings::IERC20;
use crate::config::{BrokerCtx, Ctx};
use crate::equity_redemption::RedemptionAggregateId;
use crate::onchain::vault::{VaultId, VaultService};
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::rebalancing::mint::Mint;
use crate::rebalancing::redemption::Redeem;
use crate::rebalancing::usdc::UsdcRebalanceManager;
use crate::rebalancing::{MintManager, RedemptionManager};
use crate::threshold::Usdc;
use crate::tokenized_equity_mint::IssuerRequestId;
use crate::usdc_rebalance::UsdcRebalanceId;

pub(super) async fn transfer_equity_command<W: Write>(
    stdout: &mut W,
    direction: TransferDirection,
    symbol: &Symbol,
    quantity: FractionalShares,
    token_address: Option<Address>,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let direction_str = match direction {
        TransferDirection::ToRaindex => "Alpaca → Raindex (mint)",
        TransferDirection::ToAlpaca => "Raindex → Alpaca (redeem)",
    };

    writeln!(stdout, "🔄 Transferring equity: {direction_str}")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("transfer-equity requires Alpaca Broker API configuration");
    };

    let rebalancing_config = ctx
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("transfer-equity requires rebalancing configuration"))?;

    let ws = WsConnect::new(ctx.evm.ws_rpc_url.as_str());
    let base_provider = ProviderBuilder::new().connect_ws(ws).await?;

    let tokenization_service = Arc::new(AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        base_provider.clone(),
        rebalancing_config.redemption_wallet,
    ));

    match direction {
        TransferDirection::ToRaindex => {
            writeln!(stdout, "   Creating mint request...")?;

            let mint_store = Arc::new(StoreBuilder::new(pool.clone()).build(()).await?);
            let mint_manager = MintManager::new(tokenization_service, mint_store);

            let issuer_request_id =
                IssuerRequestId::new(format!("cli-mint-{}", uuid::Uuid::new_v4()));

            let signer = PrivateKeySigner::from_bytes(&rebalancing_config.evm_private_key)?;
            let wallet = signer.address();

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

            let redemption_store = Arc::new(StoreBuilder::new(pool.clone()).build(()).await?);
            let redemption_manager = RedemptionManager::new(tokenization_service, redemption_store);

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
    ctx: &Ctx,
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

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("transfer-usdc requires Alpaca Broker API configuration");
    };

    let rebalancing_config = ctx
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("transfer-usdc requires rebalancing configuration"))?;

    writeln!(stdout, "   Vault ID: {}", rebalancing_config.usdc_vault_id)?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.evm_private_key)?;

    let ethereum_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(rebalancing_config.ethereum_rpc_url.clone());

    let base_provider_with_wallet = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_provider(base_provider);

    let broker_mode = if alpaca_auth.is_sandbox() {
        AlpacaBrokerApiMode::Sandbox
    } else {
        AlpacaBrokerApiMode::Production
    };

    let broker_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_auth.api_key.clone(),
        api_secret: alpaca_auth.api_secret.clone(),
        account_id: rebalancing_config.alpaca_account_id.to_string(),
        mode: Some(broker_mode),
        asset_cache_ttl: std::time::Duration::from_secs(3600),
        time_in_force: TimeInForce::default(),
    };

    let alpaca_broker = Arc::new(AlpacaBrokerApi::try_from_ctx(broker_auth.clone()).await?);

    let alpaca_wallet = Arc::new(AlpacaWalletService::new(
        broker_auth.base_url().to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    ));

    let owner = signer.address();

    let bridge = Arc::new(CctpBridge::try_from_ctx(CctpCtx {
        ethereum_provider,
        base_provider: base_provider_with_wallet.clone(),
        owner,
        usdc_ethereum: USDC_ETHEREUM,
        usdc_base: USDC_BASE,
    })?);
    let vault_service = Arc::new(VaultService::new(
        base_provider_with_wallet,
        ctx.evm.orderbook,
    ));
    let usdc_store = Arc::new(StoreBuilder::new(pool.clone()).build(()).await?);

    let rebalance_manager = UsdcRebalanceManager::new(
        alpaca_broker,
        alpaca_wallet,
        bridge,
        vault_service,
        usdc_store,
        owner,
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

/// Isolated tokenization command - calls Alpaca tokenization API directly.
pub(super) async fn alpaca_tokenize_command<W: Write, P: Provider + Clone>(
    stdout: &mut W,
    symbol: Symbol,
    quantity: FractionalShares,
    token: Address,
    ctx: &Ctx,
    provider: P,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔄 Requesting tokenization via Alpaca API")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;
    writeln!(stdout, "   Token: {token}")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-tokenize requires Alpaca Broker API configuration");
    };

    let rebalancing_config = ctx.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!("alpaca-tokenize requires rebalancing configuration for wallet addresses")
    })?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.evm_private_key)?;
    let receiving_wallet = signer.address();
    writeln!(stdout, "   Receiving wallet: {receiving_wallet}")?;

    let erc20 = IERC20::new(token, provider.clone());
    let initial_balance = erc20.balanceOf(receiving_wallet).call().await?;
    writeln!(stdout, "   Initial balance: {initial_balance}")?;

    let expected_amount = quantity.to_u256_18_decimals()?;
    let expected_final = initial_balance + expected_amount;
    writeln!(stdout, "   Expected final balance: {expected_final}")?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        provider.clone(),
        rebalancing_config.redemption_wallet,
    );

    writeln!(stdout, "   Sending mint request to Alpaca...")?;

    let request = tokenization_service
        .request_mint(symbol.clone(), quantity, receiving_wallet)
        .await?;

    writeln!(stdout, "   Request ID: {}", request.id.0)?;
    writeln!(stdout, "   Status: {:?}", request.status)?;

    if request.status == TokenizationRequestStatus::Pending {
        writeln!(stdout, "   Polling Alpaca until completion...")?;

        let completed = tokenization_service
            .poll_mint_until_complete(&request.id)
            .await?;

        writeln!(stdout, "   Alpaca status: {:?}", completed.status)?;

        if let Some(tx_hash) = completed.tx_hash {
            writeln!(stdout, "   Alpaca tx hash: {tx_hash}")?;
        }

        if completed.status == TokenizationRequestStatus::Rejected {
            writeln!(stdout, "❌ Tokenization was rejected by Alpaca")?;
            return Ok(());
        }
    }

    writeln!(stdout, "   Polling for tokens to arrive on Base...")?;

    let poll_interval = Duration::from_secs(5);
    let max_attempts = 60; // 5 minutes max

    for attempt in 1..=max_attempts {
        let current_balance = erc20.balanceOf(receiving_wallet).call().await?;

        if current_balance >= expected_final {
            writeln!(stdout, "   Final balance: {current_balance}")?;
            writeln!(stdout, "✅ Tokenization completed - tokens received!")?;
            return Ok(());
        }

        if attempt % 6 == 0 {
            writeln!(
                stdout,
                "   Still waiting... (attempt {attempt}/{max_attempts}, balance: {current_balance})",
            )?;
        }

        tokio::time::sleep(poll_interval).await;
    }

    let final_balance = erc20.balanceOf(receiving_wallet).call().await?;
    writeln!(stdout, "   Final balance: {final_balance}")?;
    writeln!(stdout, "⏳ Timed out waiting for tokens (may still arrive)")?;

    Ok(())
}

/// Isolated redemption command - calls Alpaca tokenization API directly.
pub(super) async fn alpaca_redeem_command<W: Write, P: Provider + Clone>(
    stdout: &mut W,
    symbol: Symbol,
    quantity: FractionalShares,
    token: Address,
    ctx: &Ctx,
    provider: P,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔄 Requesting redemption via Alpaca API")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;
    writeln!(stdout, "   Token: {token}")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-redeem requires Alpaca Broker API configuration");
    };

    let rebalancing_config = ctx.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!("alpaca-redeem requires rebalancing configuration for wallet addresses")
    })?;

    let redemption_wallet = rebalancing_config.redemption_wallet;
    writeln!(stdout, "   Redemption wallet: {redemption_wallet}")?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.evm_private_key)?;
    let provider_with_wallet = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_provider(provider);

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        provider_with_wallet,
        redemption_wallet,
    );

    let amount = quantity.to_u256_18_decimals()?;
    writeln!(stdout, "   Amount (wei): {amount}")?;

    writeln!(stdout, "   Sending tokens to redemption wallet...")?;

    let tx_hash = tokenization_service
        .send_for_redemption(token, amount)
        .await?;

    writeln!(stdout, "   Transfer tx: {tx_hash}")?;
    writeln!(stdout, "   Waiting for Alpaca to detect transfer...")?;

    let request = tokenization_service.poll_for_redemption(&tx_hash).await?;

    writeln!(stdout, "   Request ID: {}", request.id.0)?;
    writeln!(stdout, "   Status: {:?}", request.status)?;

    if request.status == TokenizationRequestStatus::Pending {
        writeln!(stdout, "   Polling until completion...")?;

        let completed = tokenization_service
            .poll_redemption_until_complete(&request.id)
            .await?;

        writeln!(stdout, "   Final status: {:?}", completed.status)?;

        match completed.status {
            TokenizationRequestStatus::Completed => {
                writeln!(stdout, "✅ Redemption completed successfully")?;
            }
            TokenizationRequestStatus::Rejected => {
                writeln!(stdout, "❌ Redemption was rejected")?;
            }
            TokenizationRequestStatus::Pending => {
                writeln!(stdout, "⏳ Redemption still pending (polling timed out)")?;
            }
        }
    }

    Ok(())
}

/// List all Alpaca tokenization requests.
pub(super) async fn alpaca_tokenization_requests_command<W: Write, P: Provider + Clone>(
    stdout: &mut W,
    ctx: &Ctx,
    provider: P,
) -> anyhow::Result<()> {
    writeln!(stdout, "📋 Listing Alpaca tokenization requests")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-tokenization-requests requires Alpaca Broker API configuration");
    };

    let rebalancing_config = ctx.rebalancing.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "alpaca-tokenization-requests requires rebalancing configuration for account ID"
        )
    })?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_config.alpaca_account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        provider,
        rebalancing_config.redemption_wallet,
    );

    let requests = tokenization_service.list_requests().await?;

    if requests.is_empty() {
        writeln!(stdout, "   No tokenization requests found")?;
        return Ok(());
    }

    writeln!(stdout, "   Found {} request(s):", requests.len())?;
    writeln!(stdout)?;

    for request in requests {
        format_tokenization_request(stdout, &request)?;
    }

    Ok(())
}

fn format_tokenization_request<W: Write>(
    stdout: &mut W,
    request: &TokenizationRequest,
) -> io::Result<()> {
    let type_str = request.r#type.map_or_else(
        || "unknown".to_string(),
        |transfer_type| transfer_type.to_string(),
    );

    let status_str = match request.status {
        TokenizationRequestStatus::Pending => "⏳ pending",
        TokenizationRequestStatus::Completed => "✅ completed",
        TokenizationRequestStatus::Rejected => "❌ rejected",
    };

    writeln!(stdout, "   ─────────────────────────────────────")?;
    writeln!(stdout, "   ID:       {}", request.id.0)?;
    writeln!(stdout, "   Type:     {type_str}")?;
    writeln!(stdout, "   Status:   {status_str}")?;
    writeln!(stdout, "   Symbol:   {}", request.underlying_symbol)?;
    writeln!(stdout, "   Quantity: {}", request.quantity)?;

    if let Some(ref wallet) = request.wallet {
        writeln!(stdout, "   Wallet:   {wallet}")?;
    }

    writeln!(stdout, "   Created:  {}", request.created_at)?;

    if let Some(ref tx_hash) = request.tx_hash {
        writeln!(stdout, "   Tx Hash:  {tx_hash}")?;
    }

    if let Some(ref issuer_id) = request.issuer_request_id {
        writeln!(stdout, "   Issuer ID: {}", issuer_id.0)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use rust_decimal::Decimal;
    use st0x_execution::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};
    use std::str::FromStr;
    use url::Url;

    use super::*;
    use crate::config::LogLevel;
    use crate::onchain::EvmCtx;
    use crate::test_utils::setup_test_db;
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

    fn create_alpaca_ctx_without_rebalancing() -> Ctx {
        let mut ctx = create_ctx_without_rebalancing();
        ctx.broker = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: "test-account-id".to_string(),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        });
        ctx
    }

    fn create_mock_provider() -> impl Provider + Clone + 'static {
        let asserter = Asserter::new();
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    #[tokio::test]
    async fn test_transfer_equity_requires_alpaca_broker() {
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(Decimal::from_str("10.5").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_equity_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            &symbol,
            quantity,
            None,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_equity_requires_rebalancing_config() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(Decimal::from_str("10.5").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_equity_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            &symbol,
            quantity,
            None,
            &ctx,
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
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
            provider,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_requires_rebalancing_config() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
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
        let ctx = create_alpaca_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let provider = create_mock_provider();
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
            provider,
        )
        .await
        .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Alpaca -> Raindex"),
            "Expected direction in output, got: {output}"
        );
    }

    #[test]
    fn cli_broker_mode_sandbox_when_sandbox_auth() {
        let alpaca_auth = AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: "test-account-id".to_string(),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };

        let broker_mode = if alpaca_auth.is_sandbox() {
            AlpacaBrokerApiMode::Sandbox
        } else {
            AlpacaBrokerApiMode::Production
        };

        assert_eq!(
            broker_mode,
            AlpacaBrokerApiMode::Sandbox,
            "Sandbox auth should yield Sandbox broker mode"
        );
    }

    #[test]
    fn cli_broker_mode_production_when_production_auth() {
        let alpaca_auth = AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: "test-account-id".to_string(),
            mode: Some(AlpacaBrokerApiMode::Production),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };

        let broker_mode = if alpaca_auth.is_sandbox() {
            AlpacaBrokerApiMode::Sandbox
        } else {
            AlpacaBrokerApiMode::Production
        };

        assert_eq!(
            broker_mode,
            AlpacaBrokerApiMode::Production,
            "Production auth should yield Production broker mode"
        );
    }
}
