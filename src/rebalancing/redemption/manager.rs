//! RedemptionManager orchestrates the EquityRedemption workflow.
//!
//! Coordinates the `EquityRedemption` aggregate with polling for detection and completion.
//! The aggregate handles vault withdraw and token send atomically via its Services.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use async_trait::async_trait;
use st0x_execution::{FractionalShares, Symbol};
use std::sync::Arc;
use tracing::{error, info, instrument, warn};

use st0x_event_sorcery::Store;

use super::service::RedemptionService;
use super::{Redeem, RedemptionError};
use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId};
use crate::lifecycle::{Lifecycle, Never, SqliteQuery};
use crate::tokenization::{AlpacaTokenizationService, TokenizationRequestStatus};
use crate::tokenized_equity_mint::TokenizationRequestId;

use crate::onchain::vault::{VaultId, VaultService};
use crate::vault_registry::{VaultRegistry, VaultRegistryQuery};

pub(crate) struct RedemptionManager<P>
where
    P: Provider + Clone,
{
    service: Arc<RedemptionService<P>>,
    cqrs: Arc<Store<EquityRedemption>>,
    vault_registry_query: Arc<VaultRegistryQuery>,
    orderbook: Address,
    owner: Address,
}

impl<P> RedemptionManager<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        service: Arc<RedemptionService<P>>,
        cqrs: Arc<Store<EquityRedemption>>,
        vault_registry_query: Arc<VaultRegistryQuery>,
        orderbook: Address,
        owner: Address,
    ) -> Self {
        Self {
            alpaca,
            cqrs,
            vault_registry_query,
            orderbook,
            owner,
        }
    }

    /// Executes the Redeem command and extracts the redemption tx hash.
    ///
    /// The aggregate atomically:
    /// 1. Withdraws tokens from vault (emits VaultWithdrawn)
    /// 2. Sends tokens to Alpaca (emits TokensSent or SendFailed)
    async fn execute_redeem(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, RedemptionError> {
        self.cqrs
            .send(
                aggregate_id,
                EquityRedemptionCommand::Redeem {
                    symbol,
                    quantity: quantity.inner(),
                    token,
                    amount,
                },
            )
            .await?;

        // Load aggregate to get the redemption tx hash
        let state = self
            .cqrs
            .load(aggregate_id)
            .await?
            .ok_or(RedemptionError::AggregateNotFound)?;

        match state {
            EquityRedemption::TokensSent { redemption_tx, .. } => Ok(redemption_tx),
            EquityRedemption::Failed { reason, .. } => {
                Err(RedemptionError::SendFailed { reason })
            }
            other => {
                error!(?other, "Unexpected aggregate state after Redeem command");
                Err(RedemptionError::UnexpectedState)
            }
        }
    }

    /// Polls for redemption detection and records it.
    async fn poll_detection(
        &self,
        aggregate_id: &RedemptionAggregateId,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequestId, RedemptionError> {
        let detected = match self.alpaca.poll_for_redemption(tx_hash).await {
            Ok(req) => req,
            Err(e) => {
                warn!("Polling for redemption detection failed: {e}");
                self.cqrs
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::FailDetection {
                            reason: format!("Detection polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(RedemptionError::Alpaca(e));
            }
        };

        self.cqrs
            .send(
                aggregate_id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: detected.id.clone(),
                },
            )
            .await?;

        Ok(detected.id)
    }

    /// Polls for completion and finalizes the redemption.
    async fn poll_completion(
        &self,
        aggregate_id: &RedemptionAggregateId,
        request_id: &TokenizationRequestId,
    ) -> Result<(), RedemptionError> {
        let completed = match self.alpaca.poll_redemption_until_complete(request_id).await {
            Ok(req) => req,
            Err(e) => {
                warn!("Polling for completion failed: {e}");
                self.cqrs
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: format!("Completion polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(RedemptionError::Alpaca(e));
            }
        };

        match completed.status {
            TokenizationRequestStatus::Completed => {
                self.cqrs
                    .send(aggregate_id, EquityRedemptionCommand::Complete)
                    .await?;
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.cqrs
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: "Redemption rejected by Alpaca".to_string(),
                        },
                    )
                    .await?;
                Err(RedemptionError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                unreachable!("poll_redemption_until_complete should not return Pending status")
            }
        }
    }

    /// Executes the full redemption workflow.
    #[instrument(skip(self), fields(%symbol, ?quantity, %token, %amount))]
    async fn execute_redemption_impl(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        info!(%symbol, ?quantity, %token, %amount, "Starting redemption workflow");

        let redemption_tx = self
            .execute_redeem(aggregate_id, symbol, quantity, token, amount)
            .await?;

        info!(%redemption_tx, "Tokens sent, polling for detection");
        let request_id = self.poll_detection(aggregate_id, &redemption_tx).await?;

        info!(%request_id, "Redemption detected, polling for completion");
        self.poll_completion(aggregate_id, &request_id).await?;

        info!("Redemption workflow completed successfully");
        Ok(())
    }
}

#[async_trait]
impl<P> Redeem for RedemptionManager<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    async fn execute_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        self.execute_redemption_impl(aggregate_id, symbol, quantity, token, amount)
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, TxHash, address, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use sqlx::SqlitePool;

    use st0x_event_sorcery::test_store;

    use super::*;
    use crate::bindings::{OrderBook, TOFUTokenDecimals, TestERC20};
    use crate::conductor::wire::test_cqrs;
    use crate::equity_redemption::RedemptionServices;
    use crate::equity_redemption::mock::MockRedeemer;
    use crate::onchain::vault::{VaultId, VaultService};
    use crate::rebalancing::redemption::service::TOKENIZED_EQUITY_DECIMALS;
    use crate::vault_registry::{VaultRegistryAggregate, VaultRegistryCommand};

    const TEST_ORDERBOOK: Address = address!("0x1111111111111111111111111111111111111111");
    const TEST_OWNER: Address = address!("0x2222222222222222222222222222222222222222");

    async fn create_test_store_instance(
        services: RedemptionServices,
    ) -> Arc<Store<EquityRedemption>> {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        Arc::new(test_store(pool, services))
    }

    /// Creates a vault registry CQRS framework with a view query processor.
    /// Returns the query for loading data after commands are executed.
    fn create_vault_registry_cqrs(
        pool: &SqlitePool,
    ) -> (SqliteCqrs<VaultRegistryAggregate>, Arc<VaultRegistryQuery>) {
        let view_repo = Arc::new(SqliteViewRepository::<
            VaultRegistryAggregate,
            VaultRegistryAggregate,
        >::new(
            pool.clone(), "vault_registry_view".to_string()
        ));
        let query = Arc::new(GenericQuery::new(view_repo.clone()));
        let cqrs = test_cqrs::<VaultRegistryAggregate>(
            pool.clone(),
            vec![Box::new(GenericQuery::new(view_repo))],
            (),
        );
        (cqrs, query)
    }

    fn create_redemption_cqrs(
        pool: &SqlitePool,
        services: RedemptionServices,
    ) -> (
        SqliteCqrs<Lifecycle<EquityRedemption, Never>>,
        Arc<RedemptionQuery>,
    ) {
        let view_repo = Arc::new(SqliteViewRepository::<
            Lifecycle<EquityRedemption, Never>,
            Lifecycle<EquityRedemption, Never>,
        >::new(
            pool.clone(), "equity_redemption_view".to_string()
        ));
        let query = Arc::new(GenericQuery::new(view_repo.clone()));
        let cqrs = test_cqrs(
            pool.clone(),
            vec![Box::new(GenericQuery::new(view_repo))],
            services,
        );
        (cqrs, query)
    }

    async fn seed_vault_registry(
        pool: &SqlitePool,
        token: Address,
        vault_id: B256,
    ) -> Arc<VaultRegistryQuery> {
        seed_vault_registry_with_params(pool, TEST_ORDERBOOK, TEST_OWNER, token, vault_id).await
    }

    async fn seed_vault_registry_with_params(
        pool: &SqlitePool,
        orderbook: Address,
        owner: Address,
        token: Address,
        vault_id: B256,
    ) -> Arc<VaultRegistryQuery> {
        let (cqrs, query) = create_vault_registry_cqrs(pool);
        let aggregate_id = VaultRegistry::aggregate_id(orderbook, owner);

        cqrs.execute(
            &aggregate_id,
            VaultRegistryCommand::DiscoverEquityVault {
                token,
                vault_id,
                discovered_in: TxHash::ZERO,
                symbol: Symbol::new("TEST").unwrap(),
            },
        )
        .await
        .unwrap();

        query
    }

    #[tokio::test]
    async fn load_vault_id_returns_none_when_registry_empty() {
        let pool = crate::test_utils::setup_test_db().await;
        let (_cqrs, vault_registry_query) = create_vault_registry_cqrs(&pool);

        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let result = vault_registry_query.load(&aggregate_id).await;

        assert!(result.is_none(), "Expected None for empty registry");
    }

    #[tokio::test]
    async fn load_vault_id_returns_vault_id_when_registered() {
        let pool = crate::test_utils::setup_test_db().await;
        let token = address!("0x1234567890abcdef1234567890abcdef12345678");
        let expected_vault_id =
            b256!("0xabcdef0000000000000000000000000000000000000000000000000000000001");

        let vault_registry_query = seed_vault_registry(&pool, token, expected_vault_id).await;

        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let lifecycle = vault_registry_query.load(&aggregate_id).await.unwrap();

        let Lifecycle::Live(registry) = lifecycle else {
            panic!("Expected Live registry");
        };

        assert_eq!(
            registry.vault_id_by_token(token),
            Some(expected_vault_id),
            "Expected vault ID to be registered"
        );
    }

    #[tokio::test]
    async fn load_vault_id_returns_none_for_unknown_token() {
        let pool = crate::test_utils::setup_test_db().await;
        let registered_token = address!("0x1234567890abcdef1234567890abcdef12345678");
        let unknown_token = address!("0xabcdef0123456789abcdef0123456789abcdef01");
        let vault_id = b256!("0xabcdef0000000000000000000000000000000000000000000000000000000001");

        let vault_registry_query = seed_vault_registry(&pool, registered_token, vault_id).await;

        let aggregate_id = VaultRegistry::aggregate_id(TEST_ORDERBOOK, TEST_OWNER);
        let lifecycle = vault_registry_query.load(&aggregate_id).await.unwrap();

        let Lifecycle::Live(registry) = lifecycle else {
            panic!("Expected Live registry");
        };

        assert_eq!(
            registry.vault_id_by_token(unknown_token),
            None,
            "Expected None for unknown token"
        );
    }

    #[tokio::test]
    async fn redemption_cqrs_updates_view_after_command() {
        let pool = crate::test_utils::setup_test_db().await;
        let (cqrs, query) = create_redemption_cqrs(&pool, Arc::new(MockRedeemer::new()));

        let aggregate_id = "test-redemption-1";
        let symbol = Symbol::new("TEST").unwrap();

        cqrs.execute(
            aggregate_id,
            EquityRedemptionCommand::Redeem {
                symbol: symbol.clone(),
                quantity: rust_decimal_macros::dec!(50.0),
                token: Address::random(),
                amount: U256::from(50) * U256::from(10).pow(U256::from(18)),
            },
        )
        .await
        .unwrap();

        let lifecycle = query.load(aggregate_id).await;
        assert!(
            lifecycle.is_some(),
            "Expected view to be updated after Redeem command"
        );

        let Lifecycle::Live(EquityRedemption::TokensSent { symbol: s, .. }) = lifecycle.unwrap()
        else {
            panic!("Expected TokensSent state");
        };
        assert_eq!(s, symbol);
    }

    // --- Test infrastructure for vault integration tests ---

    const TOFU_DECIMALS_ADDRESS: Address = address!("0x4f1C29FAAB7EDdF8D7794695d8259996734Cc665");
    const TEST_VAULT_ID: VaultId = VaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));

    type LocalEvmProvider = FillProvider<
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

    struct LocalEvmWithVault {
        _anvil: AnvilInstance,
        provider: LocalEvmProvider,
        signer: PrivateKeySigner,
        orderbook_address: Address,
        token_address: Address,
    }

    impl LocalEvmWithVault {
        async fn new() -> Self {
            let anvil = Anvil::new().spawn();
            let endpoint = anvil.endpoint();

            let private_key_bytes = anvil.keys()[0].to_bytes();
            let signer =
                PrivateKeySigner::from_bytes(&B256::from_slice(&private_key_bytes)).unwrap();

            let wallet = EthereumWallet::from(signer.clone());
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_http(endpoint.parse().unwrap());

            // Deploy TOFU decimals singleton
            let tofu = TOFUTokenDecimals::deploy(&provider).await.unwrap();
            let deployed_code = provider.get_code_at(*tofu.address()).await.unwrap();
            provider
                .raw_request::<_, ()>(
                    "anvil_setCode".into(),
                    (TOFU_DECIMALS_ADDRESS, deployed_code),
                )
                .await
                .unwrap();

            // Deploy orderbook
            let orderbook = OrderBook::deploy(&provider).await.unwrap();
            let orderbook_address = *orderbook.address();

            // Deploy test token and mint to signer
            let token = TestERC20::deploy(&provider).await.unwrap();
            let token_address = *token.address();
            let initial_supply = U256::from(1_000_000) * U256::from(10).pow(U256::from(18));
            token
                .mint(signer.address(), initial_supply)
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();

            Self {
                _anvil: anvil,
                provider,
                signer,
                orderbook_address,
                token_address,
            }
        }

        async fn deposit_tokens_to_vault(&self, amount: U256) {
            let token = TestERC20::new(self.token_address, &self.provider);
            let vault_service = VaultService::new(self.provider.clone(), self.orderbook_address);

            // Approve orderbook to spend tokens
            token
                .approve(self.orderbook_address, amount)
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();

            // Deposit to vault
            vault_service
                .deposit(
                    self.token_address,
                    TEST_VAULT_ID,
                    amount,
                    TOKENIZED_EQUITY_DECIMALS,
                )
                .await
                .unwrap();
        }
    }

    /// Tests the full redemption workflow with vault withdrawal.
    #[tokio::test]
    async fn redemption_full_workflow_succeeds() {
        let pool = crate::test_utils::setup_test_db().await;
        let local_evm = LocalEvmWithVault::new().await;
        let server = MockServer::start();

        let tokenization_service = Arc::new(create_test_service_with_provider(
            &server,
            local_evm.provider.clone(),
            TEST_REDEMPTION_WALLET,
        ));
        let vault_service = Arc::new(VaultService::new(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
        ));

        let vault_registry_query = seed_vault_registry_with_params(
            &pool,
            local_evm.orderbook_address,
            local_evm.signer.address(),
            local_evm.token_address,
            TEST_VAULT_ID.0,
        )
        .await;

        let deposit_amount = U256::from(100) * U256::from(10).pow(U256::from(18));
        local_evm.deposit_tokens_to_vault(deposit_amount).await;

        let redemption_service = Arc::new(RedemptionService::new(
            vault_service,
            tokenization_service,
            vault_registry_query.clone(),
            local_evm.orderbook_address,
            local_evm.signer.address(),
        ));

        // Mock Alpaca API: token transfer triggers redemption detection
        let _transfer_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path_contains("/transfers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "transfer_123",
                    "status": "COMPLETE"
                }));
        });

        // Use a known tx_hash so we can match it in the mock responses
        let redemption_tx: TxHash =
            b256!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        // Mock: poll_for_redemption returns pending request with matching tx_hash
        let _poll_detection_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path_contains("/tokenization/requests")
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "redeem_123",
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": "TEST",
                    "token_symbol": "tTEST",
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "tx_hash": redemption_tx,
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        // Mock: poll_redemption_until_complete returns completed
        let _poll_complete_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path_contains("/tokenization/requests");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "redeem_123",
                    "type": "redeem",
                    "status": "completed",
                    "underlying_symbol": "TEST",
                    "token_symbol": "tTEST",
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "tx_hash": redemption_tx,
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let mock_redeemer: RedemptionServices = Arc::new(MockRedeemer::with_redemption_tx(redemption_tx));
        let cqrs = create_test_store_instance(mock_redeemer).await;

        let manager = RedemptionManager::new(
            redemption_service,
            cqrs,
            vault_registry_query,
            local_evm.orderbook_address,
            local_evm.signer.address(),
        );

        let aggregate_id = RedemptionAggregateId::new("test-redemption-1");
        let symbol = Symbol::new("TEST").unwrap();
        let quantity = FractionalShares::new(rust_decimal_macros::dec!(50));
        let amount = U256::from(50) * U256::from(10).pow(U256::from(18));

        let result = manager
            .execute_redemption(
                &aggregate_id,
                symbol,
                quantity,
                local_evm.token_address,
                amount,
            )
            .await;

        assert!(
            result.is_ok(),
            "Expected redemption to succeed, got: {result:?}"
        );
    }
}
