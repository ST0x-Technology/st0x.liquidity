//! Orchestrates Alpaca-to-Raindex equity inventory transfer.
//!
//! Coordinates the `TokenizedEquityMint` aggregate which handles tokenization API calls
//! and vault deposits internally via its Services. Implements the `Mint` trait for
//! integration with the rebalancing trigger system.

use alloy::primitives::Address;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info, instrument};

use st0x_event_sorcery::Store;
use st0x_execution::{FractionalShares, Symbol};

use super::{Mint, MintError};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintCommand,
};

pub(crate) struct MintManager {
    cqrs: Arc<Store<TokenizedEquityMint>>,
}

impl MintManager {
    pub(crate) fn new(cqrs: Arc<Store<TokenizedEquityMint>>) -> Self {
        Self { cqrs }
    }

    #[instrument(skip(self), fields(%symbol, ?quantity, %wallet))]
    async fn execute_mint_impl(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError> {
        debug!("Executing mint command");

        self.cqrs
            .send(
                issuer_request_id,
                TokenizedEquityMintCommand::Mint {
                    issuer_request_id: issuer_request_id.clone(),
                    symbol,
                    quantity: quantity.inner(),
                    wallet,
                },
            )
            .await?;

        debug!("Executing deposit command");

        self.cqrs
            .send(issuer_request_id, TokenizedEquityMintCommand::Deposit)
            .await?;

        info!("Mint workflow completed successfully");
        Ok(())
    }
}

#[async_trait]
impl Mint for MintManager {
    async fn execute_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError> {
        self.execute_mint_impl(issuer_request_id, symbol, quantity, wallet)
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use st0x_event_sorcery::test_store;

    use super::*;
    use crate::onchain::mock::MockRaindex;
    use crate::test_utils::setup_test_db;
    use crate::tokenization::mock::MockTokenizer;
    use crate::tokenized_equity_mint::MintServices;

    fn mock_mint_services() -> MintServices {
        MintServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            raindex: Arc::new(MockRaindex::new()),
        }
    }

    async fn create_test_store() -> Arc<Store<TokenizedEquityMint>> {
        let pool = setup_test_db().await;
        Arc::new(test_store(pool, mock_mint_services()))
    }

    #[tokio::test]
    async fn execute_mint_sends_mint_and_deposit_commands() {
        let store = create_test_store().await;
        let manager = MintManager::new(store);

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(&IssuerRequestId::new("mint-001"), symbol, quantity, wallet)
            .await;

        // MockTokenizer returns success, MockRaindex returns success
        result.unwrap();
    }

    #[tokio::test]
    async fn trait_impl_delegates_to_execute_mint_impl() {
        let store = create_test_store().await;
        let manager = MintManager::new(store);

        let mint_trait: &dyn Mint = &manager;

        let result = mint_trait
            .execute_mint(
                &IssuerRequestId::new("trait-001"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(50.0)),
                address!("0x1234567890abcdef1234567890abcdef12345678"),
            )
            .await;

        // MockTokenizer and MockRaindex always succeed
        result.unwrap();
    }

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

            let tofu = TOFUTokenDecimals::deploy(&provider).await.unwrap();
            let deployed_code = provider.get_code_at(*tofu.address()).await.unwrap();
            provider
                .raw_request::<_, ()>(
                    "anvil_setCode".into(),
                    (TOFU_DECIMALS_ADDRESS, deployed_code),
                )
                .await
                .unwrap();

            let orderbook = OrderBook::deploy(&provider).await.unwrap();
            let orderbook_address = *orderbook.address();

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
    }

    fn create_vault_registry_cqrs(
        pool: &sqlx::SqlitePool,
    ) -> (Store<VaultRegistry>, Arc<VaultRegistryProjection>) {
        let query = Arc::new(Projection::<VaultRegistry>::sqlite(pool.clone()).unwrap());
        let store = test_store(pool.clone(), ());
        (store, query)
    }

    async fn seed_vault_registry(
        pool: &sqlx::SqlitePool,
        orderbook: Address,
        owner: Address,
        token: Address,
        vault_id: B256,
        symbol: Symbol,
    ) -> Arc<VaultRegistryProjection> {
        let (cqrs, query) = create_vault_registry_cqrs(pool);
        let aggregate_id = VaultRegistry::aggregate_id(orderbook, owner);

        cqrs.execute(
            &aggregate_id,
            VaultRegistryCommand::DiscoverEquityVault {
                token,
                vault_id,
                discovered_in: TxHash::ZERO,
                symbol,
            },
        )
        .await
        .unwrap();

        query
    }

    #[tokio::test]
    async fn execute_mint_full_workflow_with_vault_deposit() {
        let server = MockServer::start();
        let pool = crate::test_utils::setup_test_db().await;
        let local_evm = LocalEvmWithVault::new().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let vault_registry_projection = seed_vault_registry(
            &pool,
            local_evm.orderbook_address,
            local_evm.signer.address(),
            local_evm.token_address,
            TEST_VAULT_ID.0,
            symbol.clone(),
        )
        .await;

        let tokenizer: Arc<dyn Tokenizer> = Arc::new(create_test_service_with_provider(
            &server,
            local_evm.provider.clone(),
            local_evm.signer.address(),
        ));
        let vault: Arc<dyn Raindex> = Arc::new(
            RaindexService::new(
                local_evm.provider.clone(),
                local_evm.orderbook_address,
                vault_registry_projection.clone(),
                local_evm.signer.address(),
            )
            .with_required_confirmations(1),
        );
        let cqrs = create_test_store_instance().await;
        let manager = MintManager::new(
            tokenizer,
            vault,
            vault_registry_projection,
            local_evm.orderbook_address,
            local_evm.signer.address(),
            cqrs,
        );

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("full_flow_test"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_completed_response("full_flow_test")]));
        });

        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = local_evm.signer.address();

        let result = manager
            .execute_mint_impl(
                &IssuerRequestId::new("full-flow-001"),
                symbol,
                quantity,
                wallet,
            )
            .await;

        result.unwrap();

        mint_mock.assert();
        poll_mock.assert();
    }
}
