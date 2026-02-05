//! Spawns the rebalancing infrastructure.

use alloy::network::{Ethereum, EthereumWallet};
use alloy::primitives::Address;
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
use alloy::signers::local::PrivateKeySigner;
use sqlite_es::SqliteCqrs;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use st0x_execution::alpaca_broker_api::AlpacaBrokerApiError;
use st0x_execution::{AlpacaBrokerApi, EmptySymbolError, Executor};

use super::usdc::UsdcRebalanceManager;
use super::{MintManager, Rebalancer, RebalancingConfig, RedemptionManager, TriggeredOperation};
use crate::alpaca_wallet::{AlpacaWalletError, AlpacaWalletService};
use crate::cctp::{
    CctpBridge, Evm, MESSAGE_TRANSMITTER_V2, TOKEN_MESSENGER_V2, USDC_BASE, USDC_ETHEREUM,
};
use crate::equity_redemption::{EquityRedemption, RedemptionEventStore};
use crate::lifecycle::{Lifecycle, Never, SqliteQuery};
use crate::onchain::http_client_with_retry;
use crate::onchain::raindex::{Raindex, RaindexService, VaultId};
use crate::tokenization::{AlpacaTokenizationService, Tokenizer};
use crate::tokenized_equity_mint::{MintEventStore, TokenizedEquityMint};
use crate::usdc_rebalance::{UsdcEventStore, UsdcRebalance};
use crate::vault_registry::VaultRegistryQuery;
use crate::wrapper::{Wrapper, WrapperService};

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
    Cctp(#[from] crate::cctp::CctpError),
    #[error("failed to create wrapper service: {0}")]
    Wrapper(#[from] EmptySymbolError),
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
type ConfiguredRebalancer<BP> = Rebalancer<
    MintManager<MintEventStore>,
    RedemptionManager<RedemptionEventStore>,
    UsdcRebalanceManager<BP, UsdcEventStore>,
>;

pub(crate) struct RebalancingCqrsFrameworks {
    pub(crate) mint: Arc<SqliteCqrs<Lifecycle<TokenizedEquityMint, Never>>>,
    pub(crate) usdc: Arc<SqliteCqrs<Lifecycle<UsdcRebalance, Never>>>,
}

#[derive(Clone, Copy)]
pub(crate) struct RebalancerAddresses {
    pub(crate) market_maker_wallet: Address,
    pub(crate) orderbook: Address,
}

pub(crate) struct RedemptionDependencies<BP: Provider + Clone> {
    pub(crate) raindex_service: Arc<RaindexService<BP>>,
    pub(crate) tokenization_service: Arc<AlpacaTokenizationService<BP>>,
    pub(crate) vault_registry_query: Arc<VaultRegistryQuery>,
    pub(crate) cqrs: Arc<SqliteCqrs<Lifecycle<EquityRedemption, Never>>>,
    pub(crate) query: Arc<SqliteQuery<EquityRedemption, Never>>,
}

/// CQRS-related dependencies for redemption manager.
pub(crate) struct RedemptionCqrs {
    pub(crate) tokenizer: Arc<dyn Tokenizer>,
    pub(crate) cqrs: Arc<SqliteCqrs<Lifecycle<EquityRedemption, Never>>>,
    pub(crate) query: Arc<SqliteQuery<EquityRedemption, Never>>,
}

/// Spawns the rebalancing infrastructure.
///
/// All CQRS frameworks are created in the conductor and passed here to ensure
/// single-instance initialization with all required query processors.
pub(crate) async fn spawn_rebalancer<BP>(
    config: &RebalancingConfig,
    base_provider: BP,
    addresses: RebalancerAddresses,
    operation_receiver: mpsc::Receiver<TriggeredOperation>,
    frameworks: RebalancingCqrsFrameworks,
    redemption: RedemptionDependencies<BP>,
) -> Result<JoinHandle<()>, SpawnRebalancerError>
where
    BP: Provider + Clone + Send + Sync + 'static,
{
    let signer = PrivateKeySigner::from_bytes(&config.evm_private_key)?;
    let ethereum_wallet = EthereumWallet::from(signer.clone());

    let tokenizer: Arc<dyn Tokenizer> = redemption.tokenization_service.clone();

    let services = Services::new(
        config,
        &ethereum_wallet,
        signer.address(),
        base_provider,
        redemption.raindex_service,
        redemption.tokenization_service,
    )
    .await?;

    let redemption_cqrs = RedemptionCqrs {
        tokenizer,
        cqrs: redemption.cqrs,
        query: redemption.query,
    };

    let rebalancer = services.into_rebalancer(
        config,
        addresses,
        operation_receiver,
        frameworks,
        redemption_cqrs,
        redemption.vault_registry_query,
    );

    let handle = tokio::spawn(async move {
        rebalancer.run().await;
    });

    info!("Rebalancing infrastructure initialized");
    Ok(handle)
}

/// External service clients for rebalancing operations.
///
/// Holds connections to Alpaca APIs, CCTP bridge, and vault services.
///
/// Generic over `BP` (Base Provider) to allow reusing the existing WebSocket
/// connection from the main bot. The Ethereum provider uses a fixed HTTP type
/// since it's created from a URL in the rebalancing config.
struct Services<BP>
where
    BP: Provider + Clone,
{
    tokenization: Arc<AlpacaTokenizationService<BP>>,
    broker: Arc<AlpacaBrokerApi>,
    wallet: Arc<AlpacaWalletService>,
    cctp: Arc<CctpBridge<HttpProvider, BP>>,
    raindex: Arc<RaindexService<BP>>,
    wrapper: Arc<WrapperService<BP>>,
}

impl<BP> Services<BP>
where
    BP: Provider + Clone + 'static,
{
    /// Creates the services needed for rebalancing.
    ///
    /// RaindexService and AlpacaTokenizationService are passed in rather than
    /// created here because they are needed for CQRS framework initialization in
    /// the conductor, which must happen before spawn_rebalancer is called.
    async fn new(
        config: &RebalancingConfig,
        ethereum_wallet: &EthereumWallet,
        owner: Address,
        base_provider: BP,
        raindex: Arc<RaindexService<BP>>,
        tokenization: Arc<AlpacaTokenizationService<BP>>,
    ) -> Result<Self, SpawnRebalancerError> {
        let ethereum_provider = ProviderBuilder::new()
            .wallet(ethereum_wallet.clone())
            .connect_client(http_client_with_retry(config.ethereum_rpc_url.clone()));

        let broker_auth = &config.alpaca_broker_auth;

        let broker = Arc::new(AlpacaBrokerApi::try_from_config(broker_auth.clone()).await?);

        let wallet = Arc::new(AlpacaWalletService::new(
            broker_auth.base_url().to_string(),
            config.alpaca_account_id,
            broker_auth.api_key.clone(),
            broker_auth.api_secret.clone(),
        ));

        let ethereum_evm = Evm::new(
            ethereum_provider,
            owner,
            USDC_ETHEREUM,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let wrapper = Arc::new(WrapperService::new(
            base_provider.clone(),
            owner,
            config.equities.clone(),
        ));

        let base_evm_for_cctp = Evm::new(
            base_provider,
            owner,
            USDC_BASE,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        );

        let cctp = Arc::new(CctpBridge::new(ethereum_evm, base_evm_for_cctp)?);

        Ok(Self {
            tokenization,
            broker,
            wallet,
            cctp,
            raindex,
            wrapper,
        })
    }

    /// Converts Services into a configured Rebalancer.
    fn into_rebalancer(
        self,
        config: &RebalancingConfig,
        addresses: RebalancerAddresses,
        operation_receiver: mpsc::Receiver<TriggeredOperation>,
        frameworks: RebalancingCqrsFrameworks,
        redemption: RedemptionCqrs,
        vault_registry_query: Arc<VaultRegistryQuery>,
    ) -> ConfiguredRebalancer<BP>
    where
        BP: Send + Sync + 'static,
    {
        let mint_manager = Arc::new(MintManager::new(
            self.tokenization as Arc<dyn Tokenizer>,
            self.raindex.clone() as Arc<dyn Raindex>,
            self.wrapper as Arc<dyn Wrapper>,
            vault_registry_query,
            addresses.orderbook,
            addresses.market_maker_wallet,
            frameworks.mint,
        ));

        let redemption_manager = Arc::new(RedemptionManager::new(
            redemption.tokenizer,
            redemption.cqrs,
            redemption.query,
        ));

        let usdc_manager = Arc::new(UsdcRebalanceManager::new(
            self.broker,
            self.wallet,
            self.cctp,
            self.raindex,
            frameworks.usdc,
            addresses.market_maker_wallet,
            VaultId(config.usdc_vault_id),
        ));

        Rebalancer::new(
            mint_manager,
            redemption_manager,
            usdc_manager,
            operation_receiver,
            addresses.market_maker_wallet,
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
    use sqlite_es::SqliteViewRepository;
    use st0x_execution::alpaca_broker_api::{
        AlpacaAccountId, AlpacaBrokerApiAuthConfig, AlpacaBrokerApiMode,
    };
    use st0x_execution::{FractionalShares, Symbol};
    use uuid::Uuid;

    use super::*;
    use crate::alpaca_wallet::{AlpacaTransferId, AlpacaWalletService};
    use crate::conductor::wire::test_cqrs;
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::mock::MockVault;
    use crate::rebalancing::RebalancingTriggerConfig;
    use crate::rebalancing::trigger::UsdcRebalancingConfig;
    use crate::tokenization::mock::MockTokenizer;
    use crate::tokenized_equity_mint::MintServices;
    use crate::vault_registry::VaultRegistryAggregate;
    use std::collections::HashMap;

    fn mock_mint_services() -> MintServices {
        MintServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            raindex: Arc::new(MockVault::new()),
        }
    }

    const TEST_ORDERBOOK: Address = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

    fn make_config() -> RebalancingConfig {
        RebalancingConfig {
            equity: ImbalanceThreshold {
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
            alpaca_broker_auth: AlpacaBrokerApiAuthConfig {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
                account_id: AlpacaAccountId::new(Uuid::nil()),
                mode: Some(AlpacaBrokerApiMode::Sandbox),
            },
            equities: HashMap::new(),
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
    fn trigger_config_uses_equity_from_config() {
        let config = make_config();

        let trigger_config = RebalancingTriggerConfig {
            equity: config.equity,
            usdc: config.usdc,
        };

        assert_eq!(trigger_config.equity.target, dec!(0.5));
        assert_eq!(trigger_config.equity.deviation, dec!(0.2));
    }

    #[test]
    fn trigger_config_uses_usdc_from_config() {
        let config = make_config();

        let trigger_config = RebalancingTriggerConfig {
            equity: config.equity,
            usdc: config.usdc,
        };

        let UsdcRebalancingConfig::Enabled { target, deviation } = trigger_config.usdc else {
            panic!("expected enabled");
        };
        assert_eq!(target, dec!(0.6));
        assert_eq!(deviation, dec!(0.15));
    }

    #[test]
    fn private_key_signer_from_valid_bytes_succeeds() {
        let config = make_config();

        let result = PrivateKeySigner::from_bytes(&config.evm_private_key);

        assert!(
            result.is_ok(),
            "Expected valid private key to parse successfully"
        );
    }

    #[test]
    fn private_key_signer_from_zero_bytes_fails() {
        let zero_key = b256!("0x0000000000000000000000000000000000000000000000000000000000000000");

        let result = PrivateKeySigner::from_bytes(&zero_key);

        assert!(result.is_err(), "Expected zero private key to fail parsing");
    }

    async fn make_services_with_mock_wallet(
        server: &httpmock::MockServer,
    ) -> (Services<impl Provider + Clone + 'static>, RebalancingConfig) {
        let anvil = Anvil::new().spawn();
        let base_provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let config = make_config();
        let signer = PrivateKeySigner::from_bytes(&config.evm_private_key).unwrap();
        let ethereum_wallet = EthereumWallet::from(signer.clone());

        let ethereum_provider = ProviderBuilder::new()
            .wallet(ethereum_wallet)
            .connect_http(config.ethereum_rpc_url.clone());

        let tokenization = Arc::new(AlpacaTokenizationService::new(
            server.base_url(),
            config.alpaca_account_id,
            "test_key".into(),
            "test_secret".into(),
            base_provider.clone(),
            config.redemption_wallet,
        ));

        // Mock the broker account verification endpoint
        let _account_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{}/account",
                config.alpaca_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": config.alpaca_account_id.to_string(),
                    "status": "ACTIVE"
                }));
        });

        let broker_auth = AlpacaBrokerApiAuthConfig {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: config.alpaca_account_id,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
        };
        let broker = Arc::new(
            AlpacaBrokerApi::try_from_config(broker_auth)
                .await
                .expect("Failed to create test broker API"),
        );

        let wallet = Arc::new(AlpacaWalletService::new(
            server.base_url(),
            config.alpaca_account_id,
            "test_key".into(),
            "test_secret".into(),
        ));

        let owner = signer.address();

        let ethereum_evm = Evm::new(
            ethereum_provider,
            owner,
            USDC_ETHEREUM,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        )
        .with_required_confirmations(1);

        let base_evm_for_cctp = Evm::new(
            base_provider.clone(),
            owner,
            USDC_BASE,
            TOKEN_MESSENGER_V2,
            MESSAGE_TRANSMITTER_V2,
        )
        .with_required_confirmations(1);

        let cctp = Arc::new(CctpBridge::new(ethereum_evm, base_evm_for_cctp).unwrap());

        let vault_registry_view_repo = Arc::new(SqliteViewRepository::<
            VaultRegistryAggregate,
            VaultRegistryAggregate,
        >::new(
            crate::test_utils::setup_test_db().await,
            "vault_registry_view".to_string(),
        ));
        let vault_registry_query = Arc::new(GenericQuery::new(vault_registry_view_repo));

        let wrapper = Arc::new(WrapperService::new(
            base_provider.clone(),
            owner,
            config.equities.clone(),
        ));

        let raindex = Arc::new(
            RaindexService::new(base_provider, TEST_ORDERBOOK, vault_registry_query, owner)
                .with_required_confirmations(1),
        );

        let services = Services {
            tokenization,
            broker,
            wallet,
            cctp,
            raindex,
            wrapper,
        };

        (services, config)
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

        let config = make_config();
        let broker_auth = AlpacaBrokerApiAuthConfig {
            api_key: "invalid_key".to_string(),
            api_secret: "invalid_secret".to_string(),
            account_id: config.alpaca_account_id,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
        };

        let result = AlpacaBrokerApi::try_from_config(broker_auth).await;

        assert!(
            result.is_err(),
            "Expected auth failure to return error, got: {result:?}"
        );

        // Verify the error can be converted to SpawnRebalancerError
        let spawn_error: SpawnRebalancerError = result.unwrap_err().into();
        assert!(
            matches!(spawn_error, SpawnRebalancerError::AlpacaBrokerApi(_)),
            "Expected AlpacaBrokerApi error variant, got: {spawn_error:?}"
        );
    }

    #[tokio::test]
    async fn into_rebalancer_dispatches_mint_to_tokenization_service() {
        let server = MockServer::start();
        let (services, config) = make_services_with_mock_wallet(&server).await;

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
        let mint_cqrs = Arc::new(test_cqrs(pool.clone(), vec![], mock_mint_services()));
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

        let vault_registry_view_repo = Arc::new(SqliteViewRepository::<
            VaultRegistryAggregate,
            VaultRegistryAggregate,
        >::new(
            pool.clone(), "vault_registry_view".to_string()
        ));
        let vault_registry_query = Arc::new(GenericQuery::new(vault_registry_view_repo));

        let frameworks = RebalancingCqrsFrameworks {
            mint: mint_cqrs,
            usdc: usdc_cqrs,
        };

        let redemption = RedemptionCqrs {
            tokenizer: services.tokenization.clone(),
            cqrs: redemption_cqrs,
            query: redemption_query,
        };

        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let rebalancer = services.into_rebalancer(
            &config,
            RebalancerAddresses {
                market_maker_wallet: config.redemption_wallet,
                orderbook: TEST_ORDERBOOK,
            },
            rx,
            frameworks,
            redemption,
            vault_registry_query,
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
