//! Spawns the rebalancing infrastructure.

use alloy::primitives::Address;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use st0x_bridge::cctp::{CctpBridge, CctpCtx, CctpError};
use st0x_event_sorcery::Store;
use st0x_evm::Wallet;
use st0x_execution::{AlpacaBrokerApi, AlpacaBrokerApiError, EmptySymbolError, Executor};

use super::equity::CrossVenueEquityTransfer;
use super::usdc::CrossVenueCashTransfer;
use super::{Rebalancer, RebalancingCtx, TriggeredOperation};
use crate::alpaca_wallet::{AlpacaWalletError, AlpacaWalletService};
use crate::equity_redemption::EquityRedemption;
use crate::onchain::raindex::{RaindexService, RaindexVaultId};
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::tokenization::Tokenizer;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;
use crate::wrapper::WrapperService;

/// Errors that can occur when spawning the rebalancer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SpawnRebalancerError {
    #[error("failed to create Alpaca wallet service: {0}")]
    AlpacaWallet(#[from] AlpacaWalletError),
    #[error("failed to create Alpaca broker API: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("failed to create CCTP bridge: {0}")]
    Cctp(#[from] CctpError),
    #[error("failed to create wrapper service: {0}")]
    Wrapper(#[from] EmptySymbolError),
}

pub(crate) struct RebalancingCqrsFrameworks {
    pub(crate) mint: Arc<Store<TokenizedEquityMint>>,
    pub(crate) redemption: Arc<Store<EquityRedemption>>,
    pub(crate) usdc: Arc<Store<UsdcRebalance>>,
}

/// Spawns the rebalancing infrastructure.
///
/// All CQRS frameworks are created in the conductor and passed here to ensure
/// single-instance initialization with all required query processors.
pub(crate) async fn spawn_rebalancer<Chain: Wallet + Clone>(
    ctx: &RebalancingCtx,
    ethereum_wallet: Chain,
    base_wallet: Chain,
    market_maker_wallet: Address,
    operation_receiver: mpsc::Receiver<TriggeredOperation>,
    frameworks: RebalancingCqrsFrameworks,
    raindex_service: Arc<RaindexService<Chain>>,
    tokenizer: Arc<dyn Tokenizer>,
) -> Result<JoinHandle<()>, SpawnRebalancerError> {
    let services = Services::new(
        ctx,
        ethereum_wallet,
        base_wallet,
        market_maker_wallet,
        raindex_service,
        tokenizer,
    )
    .await?;

    let rebalancer =
        services.into_rebalancer(ctx, market_maker_wallet, operation_receiver, frameworks);

    let handle = tokio::spawn(async move {
        rebalancer.run().await;
    });

    info!("Rebalancing infrastructure initialized");
    Ok(handle)
}

/// External service clients for rebalancing operations.
///
/// Holds connections to Alpaca APIs, CCTP bridge, and vault services.
/// Providers for both chains are obtained from the wallets on `RebalancingCtx`.
struct Services<Chain: Wallet> {
    broker: Arc<AlpacaBrokerApi>,
    wallet: Arc<AlpacaWalletService>,
    cctp: Arc<CctpBridge<Chain, Chain>>,
    raindex: Arc<RaindexService<Chain>>,
    tokenizer: Arc<dyn Tokenizer>,
    wrapper: Arc<WrapperService<Chain>>,
}

impl<Chain: Wallet> Services<Chain> {
    /// Converts Services into a configured Rebalancer.
    fn into_rebalancer(
        self,
        ctx: &RebalancingCtx,
        market_maker_wallet: Address,
        operation_receiver: mpsc::Receiver<TriggeredOperation>,
        frameworks: RebalancingCqrsFrameworks,
    ) -> Rebalancer {
        let equity = Arc::new(CrossVenueEquityTransfer::new(
            self.raindex.clone(),
            self.tokenizer,
            self.wrapper,
            market_maker_wallet,
            frameworks.mint,
            frameworks.redemption,
        ));

        let usdc = Arc::new(CrossVenueCashTransfer::new(
            self.broker,
            self.wallet,
            self.cctp,
            self.raindex,
            frameworks.usdc,
            market_maker_wallet,
            RaindexVaultId(ctx.usdc_vault_id),
        ));

        Rebalancer::new(
            Arc::clone(&equity) as _,
            equity as _,
            Arc::clone(&usdc) as _,
            usdc as _,
            operation_receiver,
        )
    }
}

impl<Chain: Wallet + Clone> Services<Chain> {
    /// Creates the services needed for rebalancing.
    ///
    /// RaindexService is passed in rather than created here because it is
    /// needed for CQRS framework initialization in the conductor, which
    /// must happen before spawn_rebalancer is called.
    async fn new(
        ctx: &RebalancingCtx,
        ethereum_wallet: Chain,
        base_wallet: Chain,
        market_maker_wallet: Address,
        raindex: Arc<RaindexService<Chain>>,
        tokenizer: Arc<dyn Tokenizer>,
    ) -> Result<Self, SpawnRebalancerError> {
        let broker_auth = &ctx.alpaca_broker_auth;

        let broker = Arc::new(AlpacaBrokerApi::try_from_ctx(broker_auth.clone()).await?);

        let wallet = Arc::new(AlpacaWalletService::new(
            broker_auth.base_url().to_string(),
            ctx.alpaca_broker_auth.account_id,
            broker_auth.api_key.clone(),
            broker_auth.api_secret.clone(),
        ));

        let cctp = Arc::new(CctpBridge::try_from_ctx(CctpCtx {
            usdc_ethereum: USDC_ETHEREUM,
            usdc_base: USDC_BASE,
            ethereum_wallet,
            base_wallet: base_wallet.clone(),
        })?);

        let wrapper = Arc::new(WrapperService::new(base_wallet, ctx.equities.clone()));

        Ok(Self {
            broker,
            wallet,
            cctp,
            raindex,
            tokenizer,
            wrapper,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::Ethereum;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{address, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, FractionalShares, Symbol,
        TimeInForce,
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    use st0x_event_sorcery::{Projection, test_store};
    use st0x_evm::fireblocks::{
        FireblocksApiUserId, FireblocksEnvironment, FireblocksVaultAccountId,
    };
    use st0x_evm::local::RawPrivateKeyWallet;

    use super::*;
    use crate::alpaca_wallet::{AlpacaTransferId, AlpacaWalletService};
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::RebalancingTriggerConfig;
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::rebalancing::trigger::UsdcRebalancing;
    use crate::tokenization::alpaca::AlpacaTokenizationService;
    use crate::tokenization::mock::MockTokenizer;
    use crate::vault_registry::VaultRegistry;
    use crate::wrapper::mock::MockWrapper;

    type BaseProvider = FillProvider<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        RootProvider<Ethereum>,
        Ethereum,
    >;

    const TEST_ORDERBOOK: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

    fn make_ctx() -> RebalancingCtx {
        RebalancingCtx {
            equity: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc: UsdcRebalancing::Enabled {
                target: dec!(0.6),
                deviation: dec!(0.15),
            },
            redemption_wallet: address!("0x1234567890123456789012345678901234567890"),
            usdc_vault_id: b256!(
                "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            ),
            alpaca_broker_auth: AlpacaBrokerApiCtx {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
                account_id: AlpacaAccountId::new(Uuid::nil()),
                mode: Some(AlpacaBrokerApiMode::Sandbox),
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: TimeInForce::default(),
            },
            equities: HashMap::new(),
            fireblocks_api_user_id: FireblocksApiUserId::new("test-user"),
            fireblocks_secret: vec![0u8; 32],
            fireblocks_vault_account_id: FireblocksVaultAccountId::new("0"),
            fireblocks_environment: FireblocksEnvironment::Sandbox,
            fireblocks_chain_asset_ids: serde_json::from_value(
                serde_json::json!({"1": "ETH", "8453": "BASECHAIN_ETH"}),
            )
            .unwrap(),
        }
    }

    #[test]
    fn spawn_rebalancer_error_display_alpaca_wallet() {
        let err = SpawnRebalancerError::AlpacaWallet(AlpacaWalletError::TransferNotFound {
            transfer_id: AlpacaTransferId::from(Uuid::nil()),
        });

        let display = format!("{err}");

        assert!(
            display.contains("failed to create Alpaca wallet service"),
            "Expected error message to contain 'failed to create Alpaca wallet service', got: {display}"
        );
    }

    #[test]
    fn trigger_config_uses_equity_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingTriggerConfig {
            equity: ctx.equity,
            usdc: ctx.usdc,
        };

        assert_eq!(trigger_config.equity.target, dec!(0.5));
        assert_eq!(trigger_config.equity.deviation, dec!(0.2));
    }

    #[test]
    fn trigger_config_uses_usdc_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingTriggerConfig {
            equity: ctx.equity,
            usdc: ctx.usdc,
        };

        let UsdcRebalancing::Enabled { target, deviation } = trigger_config.usdc else {
            panic!("expected enabled");
        };
        assert_eq!(target, dec!(0.6));
        assert_eq!(deviation, dec!(0.15));
    }

    async fn make_services_with_mock_wallet(
        server: &httpmock::MockServer,
    ) -> (Services<RawPrivateKeyWallet<BaseProvider>>, RebalancingCtx) {
        let anvil = Anvil::new().spawn();
        let base_provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let rebalancing_ctx = make_ctx();

        let evm_private_key =
            b256!("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");

        let base_wallet =
            RawPrivateKeyWallet::new(&evm_private_key, base_provider.clone(), 1).unwrap();
        let ethereum_wallet =
            RawPrivateKeyWallet::new(&evm_private_key, base_provider.clone(), 1).unwrap();

        let tokenization = Arc::new(AlpacaTokenizationService::new(
            server.base_url(),
            rebalancing_ctx.alpaca_broker_auth.account_id,
            "test_key".into(),
            "test_secret".into(),
            base_wallet.clone(),
            rebalancing_ctx.redemption_wallet,
        ));

        let _account_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{}/account",
                rebalancing_ctx.alpaca_broker_auth.account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": rebalancing_ctx.alpaca_broker_auth.account_id.to_string(),
                    "status": "ACTIVE"
                }));
        });

        let broker_auth = AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: rebalancing_ctx.alpaca_broker_auth.account_id,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };
        let broker = Arc::new(
            AlpacaBrokerApi::try_from_ctx(broker_auth)
                .await
                .expect("Failed to create test broker API"),
        );

        let wallet = Arc::new(AlpacaWalletService::new(
            server.base_url(),
            rebalancing_ctx.alpaca_broker_auth.account_id,
            "test_key".into(),
            "test_secret".into(),
        ));

        let cctp = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                usdc_ethereum: USDC_ETHEREUM,
                usdc_base: USDC_BASE,
                ethereum_wallet,
                base_wallet: base_wallet.clone(),
            })
            .unwrap(),
        );

        let wrapper = Arc::new(WrapperService::new(
            base_wallet.clone(),
            rebalancing_ctx.equities.clone(),
        ));

        let vault_registry_projection = Arc::new(
            Projection::<VaultRegistry>::sqlite(crate::test_utils::setup_test_db().await).unwrap(),
        );
        let owner = base_wallet.address();
        let raindex = Arc::new(RaindexService::new(
            base_wallet,
            TEST_ORDERBOOK,
            vault_registry_projection,
            owner,
        ));

        let services = Services {
            broker,
            wallet,
            cctp,
            raindex,
            tokenizer: tokenization,
            wrapper,
        };

        (services, rebalancing_ctx)
    }

    #[tokio::test]
    async fn broker_auth_failure_returns_spawn_error() {
        let server = MockServer::start();

        let _account_mock = server.mock(|when, then| {
            when.method(GET).path_contains("/trading/accounts/");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({"message": "Invalid API credentials"}));
        });

        let rebalancing_ctx = make_ctx();
        let broker_auth = AlpacaBrokerApiCtx {
            api_key: "invalid_key".to_string(),
            api_secret: "invalid_secret".to_string(),
            account_id: rebalancing_ctx.alpaca_broker_auth.account_id,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };

        let spawn_error: SpawnRebalancerError = AlpacaBrokerApi::try_from_ctx(broker_auth)
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(spawn_error, SpawnRebalancerError::AlpacaBrokerApi(_)),
            "Expected AlpacaBrokerApi error variant, got: {spawn_error:?}"
        );
    }

    #[tokio::test]
    async fn into_rebalancer_dispatches_mint_operation() {
        let server = MockServer::start();
        let (services, config) = make_services_with_mock_wallet(&server).await;

        let pool = crate::test_utils::setup_test_db().await;
        let mock_services = EquityTransferServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            raindex: Arc::new(MockRaindex::new()),
            wrapper: Arc::new(MockWrapper::new()),
        };
        let mint_cqrs = Arc::new(test_store(pool.clone(), mock_services.clone()));
        let usdc_cqrs = Arc::new(test_store(pool.clone(), ()));

        let redemption_cqrs = Arc::new(test_store(pool.clone(), mock_services));

        let frameworks = RebalancingCqrsFrameworks {
            mint: mint_cqrs,
            redemption: redemption_cqrs,
            usdc: usdc_cqrs,
        };

        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let rebalancer = services.into_rebalancer(&config, Address::random(), rx, frameworks);

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(10)),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;
    }
}
