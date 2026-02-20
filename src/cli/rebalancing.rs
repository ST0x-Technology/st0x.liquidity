//! Transfer equity and USDC rebalancing CLI commands.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use sqlx::SqlitePool;
use std::io::{self, Write};
use std::sync::Arc;

use st0x_bridge::cctp::{CctpBridge, CctpCtx};
use st0x_event_sorcery::{Projection, StoreBuilder};
use st0x_evm::{Evm, OpenChainErrorRegistry, ReadOnlyEvm};
use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Executor, FractionalShares, Symbol,
    TimeInForce,
};

use super::TransferDirection;
use crate::alpaca_wallet::AlpacaWalletService;
use crate::bindings::IERC20;
use crate::config::{BrokerCtx, Ctx};
use crate::onchain::raindex::{RaindexService, RaindexVaultId};
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::rebalancing::equity::{CrossVenueEquityTransfer, Equity, EquityTransferServices};
use crate::rebalancing::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use crate::rebalancing::usdc::CrossVenueCashTransfer;
use crate::threshold::Usdc;
use crate::tokenization::{
    AlpacaTokenizationService, TokenizationRequest, TokenizationRequestStatus, Tokenizer,
};
use crate::tokenized_equity_mint::IssuerRequestId;
use crate::vault_registry::VaultRegistry;
use crate::wrapper::{Wrapper, WrapperService};

pub(super) async fn transfer_equity_command<Writer: Write>(
    stdout: &mut Writer,
    direction: TransferDirection,
    symbol: &Symbol,
    quantity: FractionalShares,
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

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let wallet = rebalancing_ctx.base_wallet().address();
    let base_caller = rebalancing_ctx.base_wallet().clone();

    let tokenization_service = Arc::new(AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        base_caller.clone(),
        rebalancing_ctx.redemption_wallet,
    ));

    match direction {
        TransferDirection::ToRaindex => {
            writeln!(stdout, "   Creating mint request...")?;

            let vault_registry_projection =
                Arc::new(Projection::<VaultRegistry>::sqlite(pool.clone())?);
            let wrapper: Arc<dyn Wrapper> = Arc::new(WrapperService::new(
                base_caller.clone(),
                rebalancing_ctx.equities.clone(),
            ));

            let raindex = Arc::new(RaindexService::new(
                base_caller.clone(),
                ctx.evm.orderbook,
                vault_registry_projection.clone(),
                wallet,
            ));

            let services = EquityTransferServices {
                raindex: raindex.clone(),
                tokenizer: tokenization_service.clone(),
                wrapper: wrapper.clone(),
            };

            let mint_store = Arc::new(
                StoreBuilder::new(pool.clone())
                    .build(services.clone())
                    .await?,
            );
            let redemption_store = Arc::new(StoreBuilder::new(pool.clone()).build(services).await?);

            let equity_transfer = CrossVenueEquityTransfer::new(
                raindex,
                tokenization_service,
                wrapper,
                wallet,
                mint_store,
                redemption_store,
            );

            writeln!(stdout, "   Receiving Wallet: {wallet}")?;

            CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
                &equity_transfer,
                Equity {
                    symbol: symbol.clone(),
                    quantity,
                },
            )
            .await?;

            writeln!(stdout, "✅ Mint completed successfully")?;
        }

        TransferDirection::ToAlpaca => {
            writeln!(stdout, "   Sending tokens for redemption...")?;

            let vault_registry_projection =
                Arc::new(Projection::<VaultRegistry>::sqlite(pool.clone())?);
            let wrapper: Arc<dyn Wrapper> = Arc::new(WrapperService::new(
                base_caller.clone(),
                rebalancing_ctx.equities.clone(),
            ));

            let raindex = Arc::new(RaindexService::new(
                base_caller.clone(),
                ctx.evm.orderbook,
                vault_registry_projection,
                wallet,
            ));

            let services = EquityTransferServices {
                raindex: raindex.clone(),
                tokenizer: tokenization_service.clone(),
                wrapper: wrapper.clone(),
            };

            let mint_store = Arc::new(
                StoreBuilder::new(pool.clone())
                    .build(services.clone())
                    .await?,
            );
            let redemption_store = Arc::new(StoreBuilder::new(pool.clone()).build(services).await?);

            let equity_transfer = CrossVenueEquityTransfer::new(
                raindex,
                tokenization_service,
                wrapper,
                wallet,
                mint_store,
                redemption_store,
            );

            CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
                &equity_transfer,
                Equity {
                    symbol: symbol.clone(),
                    quantity,
                },
            )
            .await?;

            writeln!(stdout, "✅ Redemption completed successfully")?;
        }
    }

    Ok(())
}

pub(super) async fn transfer_usdc_command<Writer: Write>(
    stdout: &mut Writer,
    direction: TransferDirection,
    amount: Usdc,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let dir = match direction {
        TransferDirection::ToRaindex => "Alpaca -> Raindex",
        TransferDirection::ToAlpaca => "Raindex -> Alpaca",
    };
    writeln!(stdout, "Transferring USDC: {dir}, Amount: {amount} USDC")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("transfer-usdc requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let owner = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "   Vault ID: {}", rebalancing_ctx.usdc_vault_id)?;

    let broker_mode = if alpaca_auth.is_sandbox() {
        AlpacaBrokerApiMode::Sandbox
    } else {
        AlpacaBrokerApiMode::Production
    };

    let broker_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_auth.api_key.clone(),
        api_secret: alpaca_auth.api_secret.clone(),
        account_id: rebalancing_ctx.alpaca_broker_auth.account_id,
        mode: Some(broker_mode),
        asset_cache_ttl: std::time::Duration::from_secs(3600),
        time_in_force: TimeInForce::default(),
    };

    let alpaca_broker = Arc::new(AlpacaBrokerApi::try_from_ctx(broker_auth.clone()).await?);

    let alpaca_wallet = Arc::new(AlpacaWalletService::new(
        broker_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    ));

    let bridge = Arc::new(CctpBridge::try_from_ctx(CctpCtx {
        usdc_ethereum: USDC_ETHEREUM,
        usdc_base: USDC_BASE,
        ethereum_wallet: rebalancing_ctx.ethereum_wallet().clone(),
        base_wallet: rebalancing_ctx.base_wallet().clone(),
    })?);

    let vault_registry_projection = Arc::new(Projection::<VaultRegistry>::sqlite(pool.clone())?);
    let vault_service = Arc::new(RaindexService::new(
        rebalancing_ctx.base_wallet().clone(),
        ctx.evm.orderbook,
        vault_registry_projection,
        owner,
    ));
    let usdc_store = Arc::new(StoreBuilder::new(pool.clone()).build(()).await?);

    let rebalance_manager = CrossVenueCashTransfer::new(
        alpaca_broker,
        alpaca_wallet,
        bridge,
        vault_service,
        usdc_store,
        owner,
        RaindexVaultId(rebalancing_ctx.usdc_vault_id),
    );

    writeln!(stdout, "   Transfer may take several minutes...")?;

    match direction {
        TransferDirection::ToRaindex => {
            CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
                &rebalance_manager,
                amount,
            )
            .await?;
            writeln!(stdout, "USDC transfer to Raindex completed successfully")?;
        }
        TransferDirection::ToAlpaca => {
            CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
                &rebalance_manager,
                amount,
            )
            .await?;
            writeln!(stdout, "USDC transfer to Alpaca completed successfully")?;
        }
    }

    Ok(())
}

/// Isolated tokenization command - calls Alpaca tokenization API directly.
pub(super) async fn alpaca_tokenize_command<Writer: Write, Prov: Provider + Clone + 'static>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: FractionalShares,
    token: Address,
    recipient: Option<Address>,
    ctx: &Ctx,
    provider: Prov,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔄 Requesting tokenization via Alpaca API")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;
    writeln!(stdout, "   Token: {token}")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-tokenize requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;

    let receiving_wallet = recipient.unwrap_or_else(|| rebalancing_ctx.base_wallet().address());
    writeln!(stdout, "   Receiving wallet: {receiving_wallet}")?;

    let read_evm = ReadOnlyEvm::new(provider.clone());
    let initial_balance: U256 = read_evm
        .call::<OpenChainErrorRegistry, _>(
            token,
            IERC20::balanceOfCall {
                account: receiving_wallet,
            },
        )
        .await?;

    writeln!(stdout, "   Initial balance: {initial_balance}")?;
    let expected_amount = quantity.to_u256_18_decimals()?;
    let expected_final = initial_balance
        .checked_add(expected_amount)
        .ok_or_else(|| {
            anyhow::anyhow!("balance overflow: {initial_balance} + {expected_amount}")
        })?;
    writeln!(stdout, "   Expected final balance: {expected_final}")?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        rebalancing_ctx.base_wallet().clone(),
        rebalancing_ctx.redemption_wallet,
    );

    writeln!(stdout, "   Sending mint request to Alpaca...")?;

    let issuer_request_id = IssuerRequestId::new(uuid::Uuid::new_v4().to_string());
    let request = tokenization_service
        .request_mint(
            symbol.clone(),
            quantity,
            receiving_wallet,
            issuer_request_id,
        )
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

    let poll_interval = std::time::Duration::from_secs(5);
    let max_attempts = 60; // 5 minutes max

    for attempt in 1..=max_attempts {
        let current_balance: U256 = read_evm
            .call::<OpenChainErrorRegistry, _>(
                token,
                IERC20::balanceOfCall {
                    account: receiving_wallet,
                },
            )
            .await?;

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

    let final_balance: U256 = read_evm
        .call::<OpenChainErrorRegistry, _>(
            token,
            IERC20::balanceOfCall {
                account: receiving_wallet,
            },
        )
        .await?;

    writeln!(stdout, "   Final balance: {final_balance}")?;
    writeln!(stdout, "⏳ Timed out waiting for tokens (may still arrive)")?;

    Ok(())
}

/// Isolated redemption command - calls Alpaca tokenization API directly.
pub(super) async fn alpaca_redeem_command<Writer: Write>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: FractionalShares,
    token: Address,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔄 Requesting redemption via Alpaca API")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;
    writeln!(stdout, "   Token: {token}")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-redeem requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;

    let redemption_wallet = rebalancing_ctx.redemption_wallet;
    writeln!(stdout, "   Redemption wallet: {redemption_wallet}")?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        rebalancing_ctx.base_wallet().clone(),
        redemption_wallet,
    );

    let amount = quantity.to_u256_18_decimals()?;
    writeln!(stdout, "   Amount (wei): {amount}")?;

    writeln!(stdout, "   Sending tokens to redemption wallet...")?;

    let tx_hash = Tokenizer::send_for_redemption(&tokenization_service, token, amount).await?;

    writeln!(stdout, "   Transfer tx: {tx_hash}")?;
    writeln!(stdout, "   Waiting for Alpaca to detect transfer...")?;

    let request = Tokenizer::poll_for_redemption(&tokenization_service, &tx_hash).await?;

    writeln!(stdout, "   Request ID: {}", request.id.0)?;
    writeln!(stdout, "   Status: {:?}", request.status)?;

    if request.status == TokenizationRequestStatus::Pending {
        writeln!(stdout, "   Polling until completion...")?;

        let completed =
            Tokenizer::poll_redemption_until_complete(&tokenization_service, &request.id).await?;

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
pub(super) async fn alpaca_tokenization_requests_command<Writer: Write>(
    stdout: &mut Writer,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "📋 Listing Alpaca tokenization requests")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-tokenization-requests requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        rebalancing_ctx.base_wallet().clone(),
        rebalancing_ctx.redemption_wallet,
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

fn format_tokenization_request<Writer: Write>(
    stdout: &mut Writer,
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
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use url::Url;
    use uuid::uuid;

    use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce};

    use super::*;
    use crate::config::{LogLevel, TradingMode};
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
        let mut ctx = create_ctx_without_rebalancing();
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
    async fn test_transfer_equity_requires_rebalancing_ctx() {
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
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing mode"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_requires_alpaca_broker() {
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
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
    async fn test_transfer_usdc_requires_rebalancing_ctx() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing mode"),
            "Expected rebalancing mode error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_writes_direction_to_stdout() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let amount = Usdc(Decimal::from_str("100").unwrap());

        let mut stdout = Vec::new();
        transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
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
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
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
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
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
