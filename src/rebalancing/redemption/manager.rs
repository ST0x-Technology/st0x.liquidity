//! RedemptionManager orchestrates the EquityRedemption workflow.
//!
//! Dispatches commands to the `EquityRedemption` aggregate which handles all I/O
//! via its Services pattern (vault withdraw, token send, polling).

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use st0x_event_sorcery::Store;
use st0x_execution::{FractionalShares, Symbol};
use std::sync::Arc;
use tracing::{error, info, instrument, warn};

use super::{Redeem, RedemptionError};
use crate::equity_redemption::{
    DetectionFailure, EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId,
};
use crate::tokenization::{TokenizationRequestStatus, Tokenizer, TokenizerError};
use crate::tokenized_equity_mint::TokenizationRequestId;

pub(crate) struct RedemptionManager {
    tokenizer: Arc<dyn Tokenizer>,
    cqrs: Arc<Store<EquityRedemption>>,
}

impl RedemptionManager {
    pub(crate) fn new(tokenizer: Arc<dyn Tokenizer>, cqrs: Arc<Store<EquityRedemption>>) -> Self {
        Self { tokenizer, cqrs }
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

        let entity =
            self.cqrs
                .load(aggregate_id)
                .await?
                .ok_or(RedemptionError::EntityNotFound {
                    aggregate_id: aggregate_id.clone(),
                })?;

        match entity {
            EquityRedemption::TokensSent { redemption_tx, .. } => Ok(redemption_tx),
            entity @ EquityRedemption::Failed { .. } => Err(RedemptionError::SendFailed { entity }),
            entity => {
                error!(?entity, "Unexpected entity after Redeem command");
                Err(RedemptionError::UnexpectedEntity { entity })
            }
        }
    }

    /// Polls for redemption detection and records it.
    async fn poll_detection(
        &self,
        aggregate_id: &RedemptionAggregateId,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequestId, RedemptionError> {
        let detected = match self.tokenizer.poll_for_redemption(tx_hash).await {
            Ok(req) => req,
            Err(error) => {
                warn!(%error, "Polling for redemption detection failed");
                let failure = match &error {
                    TokenizerError::Alpaca(AlpacaTokenizationError::PollTimeout { .. }) => {
                        DetectionFailure::Timeout
                    }
                    TokenizerError::Alpaca(other) => DetectionFailure::ApiError {
                        status_code: other.status_code().map(|status| status.as_u16()),
                    },
                };

                self.cqrs
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::FailDetection { failure },
                    )
                    .await?;

                return Err(error.into());
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
        let completed = match self
            .tokenizer
            .poll_redemption_until_complete(request_id)
            .await
        {
            Ok(req) => req,
            Err(error) => {
                warn!(%error, "Polling for completion failed");
                self.cqrs
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: error.to_string(),
                        },
                    )
                    .await?;
                return Err(error.into());
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
                            reason: "Alpaca rejected the redemption request".to_string(),
                        },
                    )
                    .await?;
                Err(RedemptionError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                warn!("poll_redemption_until_complete returned Pending status");
                Err(RedemptionError::UnexpectedPendingStatus)
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

        info!(%request_id, "Redemption detected, awaiting completion");
        self.poll_completion(aggregate_id, &request_id).await?;

        info!("Redemption workflow completed successfully");
        Ok(())
    }
}

#[async_trait]
impl Redeem for RedemptionManager {
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
    use alloy::primitives::{TxHash, address, b256};
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use uuid::Uuid;

    use st0x_event_sorcery::{Projection, Store, StoreBuilder, test_store};

    use super::*;
    use crate::alpaca_wallet::AlpacaAccountId;
    use crate::onchain::mock::MockRaindex;
    use crate::onchain::raindex::RaindexVaultId;
    use crate::rebalancing::transfer::EquityTransferServices;
    use crate::tokenization::alpaca::AlpacaTokenizationService;
    use crate::tokenization::mock::MockTokenizer;
    use crate::vault_registry::{
        VaultRegistry, VaultRegistryCommand, VaultRegistryId, VaultRegistryProjection,
    };

    const TEST_ORDERBOOK: Address = address!("0x1111111111111111111111111111111111111111");
    const TEST_OWNER: Address = address!("0x2222222222222222222222222222222222222222");

    fn mock_services() -> EquityTransferServices {
        EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
        }
    }

    async fn create_test_store_instance(
        services: EquityTransferServices,
    ) -> Arc<Store<EquityRedemption>> {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        Arc::new(test_store(pool, services))
    }

    async fn create_vault_registry_cqrs(
        pool: &SqlitePool,
    ) -> (Store<VaultRegistry>, Arc<VaultRegistryProjection>) {
        let query = Arc::new(VaultRegistryProjection::sqlite(pool.clone()).unwrap());
        let store = StoreBuilder::new(pool.clone())
            .with(query.as_ref().clone())
            .build(())
            .await
            .unwrap();
        (store, query)
    }

    fn create_redemption_cqrs(
        pool: &SqlitePool,
        services: EquityTransferServices,
    ) -> (Store<EquityRedemption>, Arc<Projection<EquityRedemption>>) {
        let query = Arc::new(Projection::<EquityRedemption>::sqlite(pool.clone()).unwrap());
        let store = test_store(pool.clone(), services);
        (store, query)
    }

    async fn seed_vault_registry(
        pool: &SqlitePool,
        token: Address,
        vault_id: B256,
    ) -> Arc<VaultRegistryProjection> {
        seed_vault_registry_with_params(pool, TEST_ORDERBOOK, TEST_OWNER, token, vault_id).await
    }

    async fn seed_vault_registry_with_params(
        pool: &SqlitePool,
        orderbook: Address,
        owner: Address,
        token: Address,
        vault_id: B256,
    ) -> Arc<VaultRegistryProjection> {
        let (store, query) = create_vault_registry_cqrs(pool).await;
        let aggregate_id = VaultRegistryId { orderbook, owner };

        store
            .send(
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
        let (_store, vault_registry_projection) = create_vault_registry_cqrs(&pool).await;

        let aggregate_id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
        };
        let result = vault_registry_projection.load(&aggregate_id).await.unwrap();

        assert!(result.is_none(), "Expected None for empty registry");
    }

    #[tokio::test]
    async fn load_vault_id_returns_vault_id_when_registered() {
        let pool = crate::test_utils::setup_test_db().await;
        let token = address!("0x1234567890abcdef1234567890abcdef12345678");
        let expected_vault_id =
            b256!("0xabcdef0000000000000000000000000000000000000000000000000000000001");

        let vault_registry_projection = seed_vault_registry(&pool, token, expected_vault_id).await;

        let aggregate_id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
        };
        let registry = vault_registry_projection
            .load(&aggregate_id)
            .await
            .unwrap()
            .expect("Expected registry to exist");

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

        let vault_registry_projection =
            seed_vault_registry(&pool, registered_token, vault_id).await;

        let aggregate_id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_OWNER,
        };
        let registry = vault_registry_projection
            .load(&aggregate_id)
            .await
            .unwrap()
            .expect("Expected registry to exist");

        assert_eq!(
            registry.vault_id_by_token(unknown_token),
            None,
            "Expected None for unknown token"
        );
    }

    #[tokio::test]
    async fn redemption_cqrs_updates_view_after_command() {
        let pool = crate::test_utils::setup_test_db().await;
        let services = mock_services();
        let (store, _query) = create_redemption_cqrs(&pool, services);

        let aggregate_id = RedemptionAggregateId::new("test-redemption-1");
        let symbol = Symbol::new("TEST").unwrap();

        store
            .send(
                &aggregate_id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: dec!(50.0),
                    token: Address::random(),
                    amount: U256::from(50) * U256::from(10).pow(U256::from(18)),
                },
            )
            .await
            .unwrap();

        let entity = store
            .load(&aggregate_id)
            .await
            .unwrap()
            .expect("Expected entity to exist after Redeem command");

        let EquityRedemption::TokensSent { symbol: s, .. } = entity else {
            panic!("Expected TokensSent state, got: {entity:?}");
        };
        assert_eq!(s, symbol);
    }

    // --- Test infrastructure for vault integration tests ---

    const TOFU_DECIMALS_ADDRESS: Address = address!("0x4f1C29FAAB7EDdF8D7794695d8259996734Cc665");
    const TEST_VAULT_ID: RaindexVaultId = RaindexVaultId(b256!(
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

        async fn deposit_tokens_to_vault(
            &self,
            amount: U256,
            vault_registry_projection: Arc<VaultRegistryProjection>,
        ) {
            let token = TestERC20::new(self.token_address, &self.provider);
            let owner = self.signer.address();
            let raindex_service = RaindexService::new(
                self.provider.clone(),
                self.orderbook_address,
                vault_registry_projection,
                owner,
            )
            .with_required_confirmations(1);

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
            raindex_service
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

        // Create vault registry query first so we can use it for both deposit and redemption
        let vault_registry_projection = seed_vault_registry_with_params(
            &pool,
            local_evm.orderbook_address,
            local_evm.signer.address(),
            local_evm.token_address,
            TEST_VAULT_ID.0,
        )
        .await;

        let deposit_amount = U256::from(100) * U256::from(10).pow(U256::from(18));
        local_evm
            .deposit_tokens_to_vault(deposit_amount, vault_registry_projection.clone())
            .await;

        // Known redemption tx for matching in mocks
        let redemption_tx = TxHash::random();

        let raindex_service = Arc::new(
            RaindexService::new(
                local_evm.provider.clone(),
                local_evm.orderbook_address,
                vault_registry_projection,
                local_evm.signer.address(),
            )
            .with_required_confirmations(1),
        );

        let alpaca_service = Arc::new(AlpacaTokenizationService::new(
            server.base_url(),
            AlpacaAccountId::new(Uuid::nil()),
            "test-api-key".to_string(),
            "test-api-secret".to_string(),
            local_evm.provider.clone(),
            Address::random(),
        ));

        let redemption_service = Arc::new(RedemptionService::new(raindex_service, alpaca_service));

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

        let mock_redeemer: RedemptionServices =
            Arc::new(MockRedeemer::with_redemption_tx(redemption_tx));
        let cqrs = create_test_store_instance(mock_redeemer).await;

        let manager = RedemptionManager::new(redemption_service, cqrs);

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

        result.unwrap();
    }
}
