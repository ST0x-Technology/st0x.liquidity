//! Orchestrates Alpaca-to-Raindex equity inventory transfer.
//!
//! Coordinates the `TokenizedEquityMint` aggregate which handles tokenization API calls
//! and vault deposits internally via its Services. Implements the `Mint` trait for
//! integration with the rebalancing trigger system.

use alloy::primitives::Address;
use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use st0x_execution::{FractionalShares, Symbol};
use std::sync::Arc;
use tracing::{debug, info, instrument};

use super::{Mint, MintError};
use crate::lifecycle::{Lifecycle, Never};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintCommand,
};

pub(crate) struct MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>,
}

impl<ES> MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    pub(crate) fn new(cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>) -> Self {
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
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::Mint {
                    issuer_request_id: issuer_request_id.clone(),
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    wallet,
                },
            )
            .await?;

        debug!("Executing deposit command");

        self.cqrs
            .execute(&issuer_request_id.0, TokenizedEquityMintCommand::Deposit)
            .await?;

        info!("Mint workflow completed");
        Ok(())
    }
}

#[async_trait]
impl<ES> Mint for MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>> + Send + Sync,
    ES::AC: Send,
{
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
    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, TxHash, U256, address, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
    };
    use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
    use alloy::signers::local::PrivateKeySigner;
    use cqrs_es::CqrsFramework;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::persist::GenericQuery;
    use rust_decimal_macros::dec;
    use sqlite_es::{SqliteCqrs, SqliteViewRepository};

    use super::*;
    use crate::bindings::{OrderBook, TOFUTokenDecimals, TestERC20};
    use crate::conductor::wire::test_cqrs;
    use crate::onchain::mock::MockRaindex;
    use crate::onchain::raindex::{Raindex, RaindexService, VaultId};
    use crate::tokenization::Tokenizer;
    use crate::tokenization::alpaca::tests::{
        create_test_service_with_provider, tokenization_mint_path, tokenization_requests_path,
    };
    use crate::tokenization::mock::MockTokenizer;
    use crate::tokenized_equity_mint::MintServices;
    use crate::vault_registry::{
        VaultRegistry, VaultRegistryAggregate, VaultRegistryCommand, VaultRegistryQuery,
    };
    use httpmock::prelude::*;
    use serde_json::json;

    const TOFU_DECIMALS_ADDRESS: Address = address!("0x4f1C29FAAB7EDdF8D7794695d8259996734Cc665");
    const TEST_VAULT_ID: VaultId = VaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));

    type TestCqrs = CqrsFramework<
        Lifecycle<TokenizedEquityMint, Never>,
        MemStore<Lifecycle<TokenizedEquityMint, Never>>,
    >;

    fn create_test_cqrs_with_mocks() -> Arc<TestCqrs> {
        let store = MemStore::default();
        let services = MintServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            raindex: Arc::new(MockRaindex::new()),
        };
        Arc::new(CqrsFramework::new(store, vec![], services))
    }

    fn create_test_cqrs_with_services(
        tokenizer: Arc<dyn Tokenizer>,
        raindex: Arc<dyn Raindex>,
    ) -> Arc<TestCqrs> {
        let store = MemStore::default();
        let services = MintServices { tokenizer, raindex };
        Arc::new(CqrsFramework::new(store, vec![], services))
    }

    fn sample_pending_response(id: &str) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "mint",
            "status": "pending",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "issuer_request_id": "issuer_123",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    fn sample_completed_response(id: &str) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "mint",
            "status": "completed",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "issuer_request_id": "issuer_123",
            "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[test]
    fn fractional_shares_converts_to_u256_18_decimals() {
        let value = FractionalShares::new(dec!(100.5));
        let result = value.to_u256_18_decimals().unwrap();

        let expected = U256::from(100_500_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn fractional_shares_converts_whole_number() {
        let value = FractionalShares::new(dec!(42));
        let result = value.to_u256_18_decimals().unwrap();

        let expected = U256::from(42_000_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn fractional_shares_converts_zero() {
        let value = FractionalShares::new(dec!(0));
        let result = value.to_u256_18_decimals().unwrap();

        assert_eq!(result, U256::ZERO);
    }

    #[tokio::test]
    async fn mint_manager_executes_mint_and_deposit_commands() {
        let cqrs = create_test_cqrs_with_mocks();
        let manager = MintManager::new(cqrs);

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(&IssuerRequestId::new("mint-001"), symbol, quantity, wallet)
            .await;

        // MockTokenizer always succeeds, MockRaindex always succeeds
        result.unwrap();
    }

    #[tokio::test]
    async fn trait_impl_delegates_to_execute_mint_impl() {
        let cqrs = create_test_cqrs_with_mocks();
        let manager = MintManager::new(cqrs);

        let result = manager
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

    async fn seed_vault_registry(
        pool: &sqlx::SqlitePool,
        orderbook: Address,
        owner: Address,
        token: Address,
        vault_id: B256,
        symbol: Symbol,
    ) -> Arc<VaultRegistryQuery> {
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

        let vault_registry_query = seed_vault_registry(
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
        let raindex: Arc<dyn Raindex> = Arc::new(
            RaindexService::new(
                local_evm.provider.clone(),
                local_evm.orderbook_address,
                vault_registry_query,
                local_evm.signer.address(),
            )
            .with_required_confirmations(1),
        );
        let cqrs = create_test_cqrs_with_services(tokenizer, raindex);
        let manager = MintManager::new(cqrs);

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
