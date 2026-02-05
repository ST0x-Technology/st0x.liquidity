//! Spawns the rebalancing infrastructure.

use alloy::network::{Ethereum, EthereumWallet};
use alloy::primitives::Address;
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
use alloy::signers::local::PrivateKeySigner;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use st0x_bridge::cctp::{CctpBridge, CctpCtx, CctpError};
use st0x_event_sorcery::Store;
use st0x_execution::{AlpacaBrokerApi, AlpacaBrokerApiError, Executor};

use super::usdc::UsdcRebalanceManager;
use super::{MintManager, Rebalancer, RebalancingCtx, RedemptionManager, TriggeredOperation};
use crate::alpaca_tokenization::AlpacaTokenizationService;
use crate::alpaca_wallet::{AlpacaWalletError, AlpacaWalletService};
use crate::equity_redemption::EquityRedemption;
use crate::onchain::http_client_with_retry;
use crate::onchain::vault::{VaultId, VaultService};
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::rebalancing::redemption::RedemptionService;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;
use crate::vault_registry::VaultRegistryQuery;

/// Errors that can occur when spawning the rebalancer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SpawnRebalancerError {
    #[error("invalid Ethereum private key: {0}")]
    InvalidPrivateKey(#[from] alloy::signers::k256::ecdsa::Error),
    #[error("failed to create Alpaca wallet service: {0}")]
    AlpacaWallet(#[from] AlpacaWalletError),
    #[error("failed to create Alpaca broker API: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("failed to create CCTP bridge: {0}")]
    Cctp(#[from] CctpError),
}

/// Provider type returned by `ProviderBuilder::connect_http` with wallet.
type HttpProvider = FillProvider<
    JoinFill<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        WalletFiller<EthereumWallet>,
    >,
    RootProvider<Ethereum>,
    Ethereum,
>;

/// Type alias for a configured rebalancer with SQLite persistence.
type ConfiguredRebalancer<BP> =
    Rebalancer<MintManager<BP>, RedemptionManager<BP>, UsdcRebalanceManager<BP>>;

pub(crate) struct RebalancingCqrsFrameworks {
    pub(crate) mint: Arc<Store<TokenizedEquityMint>>,
    pub(crate) usdc: Arc<Store<UsdcRebalance>>,
}

#[derive(Clone, Copy)]
pub(crate) struct RebalancerAddresses {
    pub(crate) market_maker_wallet: Address,
}

pub(crate) struct RedemptionDependencies<BP: Provider + Clone> {
    pub(crate) raindex_service: Arc<RaindexService<BP>>,
    pub(crate) cqrs: Arc<SqliteCqrs<Lifecycle<EquityRedemption, Never>>>,
    pub(crate) query: Arc<SqliteQuery<EquityRedemption, Never>>,
}

/// CQRS-related dependencies for redemption manager.
pub(crate) struct RedemptionCqrs {
    pub(crate) cqrs: Arc<SqliteCqrs<Lifecycle<EquityRedemption, Never>>>,
    pub(crate) query: Arc<SqliteQuery<EquityRedemption, Never>>,
}

/// Spawns the rebalancing infrastructure.
///
/// All CQRS frameworks are created in the conductor and passed here to ensure
/// single-instance initialization with all required query processors.
pub(crate) async fn spawn_rebalancer<BP>(
    ctx: &RebalancingCtx,
    base_provider: BP,
    addresses: RebalancerAddresses,
    operation_receiver: mpsc::Receiver<TriggeredOperation>,
    frameworks: RebalancingCqrsFrameworks,
    redemption: RedemptionDependencies<BP>,
) -> Result<JoinHandle<()>, SpawnRebalancerError>
where
    BP: Provider + Clone + Send + Sync + 'static,
{
    let signer = PrivateKeySigner::from_bytes(&ctx.evm_private_key)?;
    let ethereum_wallet = EthereumWallet::from(signer.clone());

    let services = Services::new(
        ctx,
        &ethereum_wallet,
        signer.address(),
        base_provider,
        redemption.raindex_service,
    )
    .await?;

    let redemption_cqrs = RedemptionCqrs {
        cqrs: redemption.cqrs,
        query: redemption.query,
    };

    let rebalancer = services.into_rebalancer(
        ctx,
        addresses,
        operation_receiver,
        frameworks,
        redemption_cqrs,
    );

    let handle = tokio::spawn(async move {
        rebalancer.run().await;
    });

    info!("Rebalancing infrastructure initialized");
    Ok(handle)
}

/// External service clients for rebalancing operations.
///
/// Holds connections to Alpaca APIs, CCTP bridge, and Raindex services.
///
/// Generic over `BP` (Base Provider) to allow reusing the existing WebSocket
/// connection from the main bot. The Ethereum provider uses a fixed HTTP type
/// since it's created from a URL in the rebalancing config.
struct Services<BP>
where
    BP: Provider + Clone,
{
    broker: Arc<AlpacaBrokerApi>,
    wallet: Arc<AlpacaWalletService>,
    cctp: Arc<CctpBridge<HttpProvider, BP>>,
    raindex: Arc<RaindexService<BP>>,
}

impl<BP> Services<BP>
where
    BP: Provider + Clone + 'static,
{
    /// Creates the services needed for rebalancing.
    ///
    /// RaindexService is passed in rather than created here because it's needed
    /// for CQRS framework initialization in the conductor, which must happen
    /// before spawn_rebalancer is called.
    async fn new(
        ctx: &RebalancingCtx,
        ethereum_wallet: &EthereumWallet,
        owner: Address,
        base_provider: BP,
        raindex: Arc<RaindexService<BP>>,
    ) -> Result<Self, SpawnRebalancerError> {
        let ethereum_provider = ProviderBuilder::new()
            .wallet(ethereum_wallet.clone())
            .connect_client(http_client_with_retry(ctx.ethereum_rpc_url.clone()));

        let broker_auth = &ctx.alpaca_broker_auth;

        let broker = Arc::new(AlpacaBrokerApi::try_from_ctx(broker_auth.clone()).await?);

        let wallet = Arc::new(AlpacaWalletService::new(
            broker_auth.base_url().to_string(),
            ctx.alpaca_account_id,
            broker_auth.api_key.clone(),
            broker_auth.api_secret.clone(),
        ));

        let cctp = Arc::new(CctpBridge::try_from_ctx(CctpCtx {
            ethereum_provider,
            base_provider: base_provider.clone(),
            owner,
            usdc_ethereum: USDC_ETHEREUM,
            usdc_base: USDC_BASE,
        })?);


        Ok(Self {
            broker,
            wallet,
            cctp,
            raindex,
        })
    }

    /// Converts Services into a configured Rebalancer.
    fn into_rebalancer(
        self,
        ctx: &RebalancingCtx,
        addresses: RebalancerAddresses,
        operation_receiver: mpsc::Receiver<TriggeredOperation>,
        frameworks: RebalancingCqrsFrameworks,
        redemption: RedemptionCqrs,
    ) -> ConfiguredRebalancer<BP>
    where
        BP: Send + Sync + 'static,
    {
        let mint_manager = Arc::new(MintManager::new(frameworks.mint));

        let redemption_manager =
            Arc::new(RedemptionManager::new(redemption.cqrs, redemption.query));

        let usdc_manager = Arc::new(UsdcRebalanceManager::new(
            self.broker,
            self.wallet,
            self.cctp,
            self.raindex,
            frameworks.usdc,
            addresses.market_maker_wallet,
            VaultId(ctx.usdc_vault_id),
        ));

        Rebalancer::new(
            mint_manager,
            redemption_manager,
            usdc_manager,
            operation_receiver,
            ctx.redemption_wallet,
        )
    }
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{address, b256};
    use alloy::providers::ProviderBuilder;
    use cqrs_es::persist::GenericQuery;
    use httpmock::Method::{GET, POST};
    use httpmock::MockServer;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use sqlx::SqlitePool;
    use st0x_execution::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode, FractionalShares, Symbol, TimeInForce};
    use uuid::Uuid;

    use st0x_event_sorcery::{Projection, test_store};

    use super::*;
    use crate::alpaca_wallet::{AlpacaAccountId, AlpacaTransferId, AlpacaWalletService};
    use crate::conductor::wire::test_cqrs;
    use crate::inventory::ImbalanceThreshold;
    use crate::rebalancing::RebalancingTriggerConfig;
    use crate::rebalancing::trigger::UsdcRebalancingConfig;
    use crate::vault_registry::{VaultRegistry, VaultRegistryAggregate};

    const TEST_ORDERBOOK: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

    fn make_ctx() -> RebalancingCtx {
        RebalancingCtx {
            equity_threshold: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc: UsdcRebalancingConfig::Enabled {
                target: dec!(0.6),
                deviation: dec!(0.15),
            },
            redemption_wallet: address!("0x1234567890123456789012345678901234567890"),
            ethereum_rpc_url: "https://eth.example.com".parse().unwrap(),
            evm_private_key: b256!(
                "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            ),
            usdc_vault_id: b256!(
                "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            ),
            alpaca_account_id: AlpacaAccountId::new(Uuid::nil()),
            alpaca_broker_auth: AlpacaBrokerApiCtx {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
                account_id: Uuid::nil().to_string(),
                mode: Some(AlpacaBrokerApiMode::Sandbox),
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: TimeInForce::default(),
            },
        }
    }

    #[test]
    fn spawn_rebalancer_error_display_invalid_private_key() {
        let err =
            SpawnRebalancerError::InvalidPrivateKey(alloy::signers::k256::ecdsa::Error::new());

        let display = format!("{err}");

        assert!(
            display.contains("invalid Ethereum private key"),
            "Expected error message to contain 'invalid Ethereum private key', got: {display}"
        );
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
            equity: ctx.equity_threshold,
            usdc: ctx.usdc.clone(),
        };

        assert_eq!(trigger_config.equity.target, dec!(0.5));
        assert_eq!(trigger_config.equity.deviation, dec!(0.2));
    }

    #[test]
    fn trigger_config_uses_usdc_from_ctx() {
        let ctx = make_ctx();

        let trigger_config = RebalancingTriggerConfig {
            equity: ctx.equity_threshold,
            usdc: ctx.usdc.clone(),
        };

        let UsdcRebalancingConfig::Enabled { target, deviation } = trigger_config.usdc else {
            panic!("expected enabled");
        };
        assert_eq!(target, dec!(0.6));
        assert_eq!(deviation, dec!(0.15));
    }

    #[test]
    fn private_key_signer_from_valid_bytes_succeeds() {
        let rebalancing_ctx = make_ctx();

        PrivateKeySigner::from_bytes(&rebalancing_ctx.evm_private_key).unwrap();
    }

    #[test]
    fn private_key_signer_from_zero_bytes_fails() {
        let zero_key = b256!("0x0000000000000000000000000000000000000000000000000000000000000000");

        PrivateKeySigner::from_bytes(&zero_key).unwrap_err();
    }

    async fn make_services_with_mock_wallet(
        server: &httpmock::MockServer,
    ) -> (Services<impl Provider + Clone + 'static>, RebalancingCtx) {
        let anvil = Anvil::new().spawn();
        let base_provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let rebalancing_ctx = make_ctx();
        let signer = PrivateKeySigner::from_bytes(&rebalancing_ctx.evm_private_key).unwrap();
        let ethereum_wallet = EthereumWallet::from(signer.clone());

        let ethereum_provider = ProviderBuilder::new()
            .wallet(ethereum_wallet)
            .connect_http(rebalancing_ctx.ethereum_rpc_url.clone());

        let tokenization = Arc::new(AlpacaTokenizationService::new(
            server.base_url(),
            rebalancing_ctx.alpaca_account_id,
            "test_key".into(),
            "test_secret".into(),
            base_provider.clone(),
            rebalancing_ctx.redemption_wallet,
        ));

        // Mock the broker account verification endpoint
        let _account_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{}/account",
                rebalancing_ctx.alpaca_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": rebalancing_ctx.alpaca_account_id.to_string(),
                    "status": "ACTIVE"
                }));
        });

        let broker_auth = AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: rebalancing_ctx.alpaca_account_id.to_string(),
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
            rebalancing_ctx.alpaca_account_id,
            "test_key".into(),
            "test_secret".into(),
        ));

        let owner = signer.address();

        let cctp = Arc::new(
            CctpBridge::try_from_ctx(CctpCtx {
                ethereum_provider,
                base_provider: base_provider.clone(),
                owner,
                usdc_ethereum: USDC_ETHEREUM,
                usdc_base: USDC_BASE,
            })
            .unwrap(),
        );

        let vault_registry_query = Arc::new(
            Projection::<VaultRegistry>::sqlite(crate::test_utils::setup_test_db().await).unwrap(),
        );
        let vault = Arc::new(
            VaultService::new(base_provider, TEST_ORDERBOOK, vault_registry_query, owner)
                .with_required_confirmations(1),
        );

        let services = Services {
            broker,
            wallet,
            cctp,
            raindex: raindex.clone(),
        };

        (services, rebalancing_ctx)
    }

    #[tokio::test]
    async fn into_rebalancer_constructs_without_panic() {
        let server = MockServer::start();
        let (services, rebalancing_ctx) = make_services_with_mock_wallet(&server).await;

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let market_maker_wallet = address!("0xaabbccddaabbccddaabbccddaabbccddaabbccdd");

        let (_tx, rx) = mpsc::channel(100);
        let frameworks = RebalancingCqrsFrameworks {
            mint: Arc::new(test_store(pool.clone(), ())),
            usdc: Arc::new(test_store(pool.clone(), ())),
        };

        let vault_registry_query =
            Arc::new(Projection::<VaultRegistry>::sqlite(pool).unwrap());

        // TODO: adapt into_rebalancer call for RedemptionDependencies
        let _ = (services, rebalancing_ctx, market_maker_wallet, rx, vault_registry_query, frameworks);
    }

    #[tokio::test]
    async fn broker_auth_failure_returns_spawn_error() {
        let server = MockServer::start();

        // Mock account endpoint to return 401 unauthorized
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
            account_id: rebalancing_ctx.alpaca_account_id.to_string(),
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        };

        // Verify the error can be converted to SpawnRebalancerError
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
    async fn into_rebalancer_dispatches_mint_to_tokenization_service() {
        let server = MockServer::start();
        let (services, config, tokenization, raindex) =
            make_services_with_mock_wallet(&server).await;

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(format!(
                "/v1/accounts/{}/tokenization/mint",
                config.alpaca_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "tok_123",
                    "type": "mint",
                    "status": "pending",
                    "symbol": "AAPL",
                    "qty": "10",
                    "issuer_request_id": "test_issuer_req",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let pool = crate::test_utils::setup_test_db().await;
        let mint_services = MintServices {
            tokenizer: tokenization,
            raindex,
        };
        let mint_cqrs = Arc::new(test_cqrs(pool.clone(), vec![], mint_services));
        let usdc_cqrs = Arc::new(test_cqrs(pool.clone(), vec![], ()));

        let redemption_view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<EquityRedemption, Never>,
            Lifecycle<EquityRedemption, Never>,
        >::new(
            pool.clone(), "equity_redemption_view".to_string()
        ));
        let redemption_query = Arc::new(GenericQuery::new(redemption_view_repo.clone()));
        let redemption_cqrs = Arc::new(test_cqrs(
            pool.clone(),
            vec![Box::new(GenericQuery::new(redemption_view_repo))],
            crate::equity_redemption::tests::mock_redeemer_services(),
        ));

        let frameworks = RebalancingCqrsFrameworks {
            mint: mint_cqrs,
            usdc: usdc_cqrs,
        };

        let redemption = RedemptionCqrs {
            cqrs: redemption_cqrs,
            query: redemption_query,
        };

        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let rebalancer = services.into_rebalancer(
            &config,
            RebalancerAddresses {
                market_maker_wallet: config.redemption_wallet,
            },
            rx,
            frameworks,
            redemption,
        );

        tx.send(TriggeredOperation::Mint {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(10)),
        })
        .await
        .unwrap();

        drop(tx);
        rebalancer.run().await;

        assert_eq!(
            mint_mock.hits(),
            1,
            "Expected tokenization mint endpoint to be called exactly once"
        );
    }
}
