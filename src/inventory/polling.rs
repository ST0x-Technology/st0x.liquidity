//! Inventory polling service for fetching actual balances and emitting
//! snapshot events.
//!
//! This service polls onchain vaults and offchain broker accounts to fetch
//! actual inventory balances, then emits InventorySnapshotCommands to record
//! the fetched values. The InventoryView reacts to these events to reconcile
//! tracked inventory.

use alloy::primitives::Address;
use alloy::providers::RootProvider;
use futures_util::future::try_join_all;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::debug;

use st0x_event_sorcery::{SendError, Store};
use st0x_evm::{Evm, EvmError, OpenChainErrorRegistry, Wallet};
use st0x_execution::{Executor, FractionalShares, InventoryResult, SharesConversionError, Symbol};

use crate::bindings::IERC20;
use crate::inventory::snapshot::{
    InventorySnapshot, InventorySnapshotCommand, InventorySnapshotId,
};
use crate::onchain::raindex::{RaindexError, RaindexService, RaindexVaultId};
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::rebalancing::usdc::{UsdcTransferError, u256_to_usdc};
use crate::vault_registry::{VaultRegistry, VaultRegistryId};

/// Error type for inventory polling operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryPollingError<ExecutorError> {
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    Executor(ExecutorError),
    #[error(transparent)]
    SnapshotAggregate(#[from] SendError<InventorySnapshot>),
    #[error(transparent)]
    VaultRegistry(#[from] SendError<VaultRegistry>),
    #[error(transparent)]
    Evm(#[from] EvmError),
    #[error(transparent)]
    UsdcConversion(#[from] UsdcTransferError),
    #[error("vault balance mismatch: expected {expected:?}, got {actual:?}")]
    VaultBalanceMismatch {
        expected: Vec<Address>,
        actual: Vec<Address>,
    },
    #[error(transparent)]
    SharesConversion(#[from] SharesConversionError),
}

pub(crate) struct WalletPollingCtx {
    pub(crate) ethereum: Option<Arc<dyn Wallet<Provider = RootProvider>>>,
    pub(crate) base: Option<Arc<dyn Wallet<Provider = RootProvider>>>,
    pub(crate) unwrapped_equity_token_addresses: HashMap<Symbol, Address>,
    pub(crate) wrapped_equity_token_addresses: HashMap<Symbol, Address>,
}

/// Service that polls actual inventory from onchain vaults and offchain brokers.
pub(crate) struct InventoryPollingService<Chain, Exe>
where
    Chain: Evm,
{
    raindex_service: Arc<RaindexService<Chain>>,
    executor: Exe,
    vault_registry: Arc<Store<VaultRegistry>>,
    orderbook: Address,
    order_owner: Address,
    snapshot: Arc<Store<InventorySnapshot>>,
    wallet_polling: WalletPollingCtx,
}

impl<Chain, Exe> InventoryPollingService<Chain, Exe>
where
    Chain: Evm,
    Exe: Executor,
{
    pub(crate) fn new(
        raindex_service: Arc<RaindexService<Chain>>,
        executor: Exe,
        vault_registry: Arc<Store<VaultRegistry>>,
        orderbook: Address,
        order_owner: Address,
        snapshot: Arc<Store<InventorySnapshot>>,
        wallet_polling: WalletPollingCtx,
    ) -> Self {
        Self {
            raindex_service,
            executor,
            vault_registry,
            orderbook,
            order_owner,
            snapshot,
            wallet_polling,
        }
    }

    /// Polls actual inventory from all venues and emits snapshot commands.
    ///
    /// 1. Queries onchain equity and USDC balances from Base vaults
    /// 2. Queries Ethereum wallet USDC balance (if configured)
    /// 3. Queries offchain positions and cash from executor
    ///
    /// Registered queries are dispatched when commands are executed.
    pub(crate) async fn poll_and_record(&self) -> Result<(), InventoryPollingError<Exe::Error>> {
        let snapshot_id = InventorySnapshotId {
            orderbook: self.orderbook,
            owner: self.order_owner,
        };

        self.poll_onchain(&snapshot_id).await?;
        self.poll_ethereum_cash(&snapshot_id).await?;
        self.poll_base_wallet_cash(&snapshot_id).await?;
        self.poll_base_wallet_unwrapped_equity(&snapshot_id).await?;
        self.poll_base_wallet_wrapped_equity(&snapshot_id).await?;
        self.poll_offchain(&snapshot_id).await?;

        Ok(())
    }

    async fn poll_onchain(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let vault_registry = self.load_vault_registry().await?;

        let Some(registry) = vault_registry else {
            debug!("Vault registry not initialized, skipping onchain polling");
            return Ok(());
        };

        self.poll_onchain_equity(snapshot_id, &registry).await?;
        self.poll_onchain_cash(snapshot_id, &registry).await?;

        Ok(())
    }

    async fn load_vault_registry(
        &self,
    ) -> Result<Option<VaultRegistry>, InventoryPollingError<Exe::Error>> {
        let vault_registry_id = VaultRegistryId {
            orderbook: self.orderbook,
            owner: self.order_owner,
        };

        Ok(self.vault_registry.load(&vault_registry_id).await?)
    }

    async fn poll_onchain_equity(
        &self,
        snapshot_id: &InventorySnapshotId,
        registry: &VaultRegistry,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        if registry.equity_vaults.is_empty() {
            debug!("No equity vaults discovered, skipping onchain equity polling");
            return Ok(());
        }

        let expected_tokens: Vec<_> = registry.equity_vaults.keys().copied().collect();

        let balance_futures = registry.equity_vaults.values().map(|vault| async {
            self.raindex_service
                .get_equity_balance::<OpenChainErrorRegistry>(
                    self.order_owner,
                    vault.token,
                    RaindexVaultId(vault.vault_id),
                )
                .await
                .map(|balance| (vault.token, vault.symbol.clone(), balance))
        });

        let results = try_join_all(balance_futures).await?;

        let balances: BTreeMap<_, _> = results
            .iter()
            .map(|(_, symbol, balance)| (symbol.clone(), *balance))
            .collect();

        let fetched_tokens: Vec<_> = results.into_iter().map(|(token, _, _)| token).collect();

        if expected_tokens != fetched_tokens {
            return Err(InventoryPollingError::VaultBalanceMismatch {
                expected: expected_tokens,
                actual: fetched_tokens,
            });
        }

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OnchainEquity { balances },
            )
            .await?;

        Ok(())
    }

    async fn poll_onchain_cash(
        &self,
        snapshot_id: &InventorySnapshotId,
        registry: &VaultRegistry,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let Some(usdc_vault) = &registry.usdc_vault else {
            debug!("No USDC vault discovered, skipping onchain cash polling");
            return Ok(());
        };

        let usdc_balance = self
            .raindex_service
            .get_usdc_balance::<OpenChainErrorRegistry>(
                self.order_owner,
                RaindexVaultId(usdc_vault.vault_id),
            )
            .await?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OnchainCash { usdc_balance },
            )
            .await?;

        Ok(())
    }

    async fn poll_ethereum_cash(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let Some(wallet) = &self.wallet_polling.ethereum else {
            debug!("No Ethereum wallet configured, skipping Ethereum cash polling");
            return Ok(());
        };

        let raw_balance = wallet
            .call::<OpenChainErrorRegistry, _>(
                USDC_ETHEREUM,
                IERC20::balanceOfCall {
                    account: wallet.address(),
                },
            )
            .await?;

        let usdc_balance = u256_to_usdc(raw_balance)?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::EthereumCash { usdc_balance },
            )
            .await?;

        Ok(())
    }

    async fn poll_base_wallet_cash(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let Some(wallet) = &self.wallet_polling.base else {
            debug!("No Base wallet configured, skipping Base cash polling");
            return Ok(());
        };

        let raw_balance = wallet
            .call::<OpenChainErrorRegistry, _>(
                USDC_BASE,
                IERC20::balanceOfCall {
                    account: wallet.address(),
                },
            )
            .await?;

        let usdc_balance = u256_to_usdc(raw_balance)?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::BaseWalletCash { usdc_balance },
            )
            .await?;

        Ok(())
    }

    async fn poll_base_wallet_unwrapped_equity(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let Some(wallet) = &self.wallet_polling.base else {
            debug!("No Base wallet configured, skipping Base unwrapped equity polling");
            return Ok(());
        };

        if self
            .wallet_polling
            .unwrapped_equity_token_addresses
            .is_empty()
        {
            debug!(
                "No unwrapped equity token addresses configured, skipping Base unwrapped equity polling"
            );
            return Ok(());
        }

        let balances = self
            .poll_base_wallet_token_balances(
                wallet,
                &self.wallet_polling.unwrapped_equity_token_addresses,
            )
            .await?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::BaseWalletUnwrappedEquity { balances },
            )
            .await?;

        Ok(())
    }

    async fn poll_base_wallet_wrapped_equity(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let Some(wallet) = &self.wallet_polling.base else {
            debug!("No Base wallet configured, skipping Base wrapped equity polling");
            return Ok(());
        };

        if self
            .wallet_polling
            .wrapped_equity_token_addresses
            .is_empty()
        {
            debug!(
                "No wrapped equity token addresses configured, skipping Base wrapped equity polling"
            );
            return Ok(());
        }

        let balances = self
            .poll_base_wallet_token_balances(
                wallet,
                &self.wallet_polling.wrapped_equity_token_addresses,
            )
            .await?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::BaseWalletWrappedEquity { balances },
            )
            .await?;

        Ok(())
    }

    async fn poll_base_wallet_token_balances(
        &self,
        wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
        token_addresses: &HashMap<Symbol, Address>,
    ) -> Result<BTreeMap<Symbol, FractionalShares>, InventoryPollingError<Exe::Error>> {
        let balance_futures = token_addresses.iter().map(|(symbol, token_addr)| async {
            let raw_balance = wallet
                .call::<OpenChainErrorRegistry, _>(
                    *token_addr,
                    IERC20::balanceOfCall {
                        account: wallet.address(),
                    },
                )
                .await?;

            let shares = FractionalShares::from_u256_18_decimals(raw_balance)?;
            Ok::<_, InventoryPollingError<Exe::Error>>((symbol.clone(), shares))
        });

        let results = try_join_all(balance_futures).await?;

        Ok(results.into_iter().collect())
    }

    async fn poll_offchain(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let inventory_result = self
            .executor
            .get_inventory()
            .await
            .map_err(InventoryPollingError::Executor)?;

        let InventoryResult::Fetched(inventory) = inventory_result else {
            debug!("Executor returned non-fetched inventory result, skipping offchain polling");
            return Ok(());
        };

        let positions: BTreeMap<_, _> = inventory
            .positions
            .into_iter()
            .map(|position| (position.symbol, position.quantity))
            .collect();

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OffchainEquity { positions },
            )
            .await?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OffchainCash {
                    cash_balance_cents: inventory.cash_balance_cents,
                },
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, Bytes, TxHash, U256, address, b256};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{Provider, ProviderBuilder, RootProvider};
    use alloy::rpc::client::RpcClient;
    use alloy::rpc::types::TransactionReceipt;
    use alloy::sol_types::SolValue;
    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use sqlx::{Row, SqlitePool};

    use st0x_event_sorcery::{StoreBuilder, test_store};
    use st0x_evm::ReadOnlyEvm;
    use st0x_execution::{EquityPosition, FractionalShares, Inventory, MockExecutor, Symbol};

    use super::*;
    use crate::inventory::snapshot::InventorySnapshotEvent;
    use crate::test_utils::setup_test_db;
    use crate::vault_registry::{VaultRegistry, VaultRegistryCommand};

    struct MockEthereumWallet {
        address: Address,
        provider: RootProvider,
    }

    impl MockEthereumWallet {
        fn with_asserter(asserter: &Asserter) -> Arc<dyn Wallet<Provider = RootProvider>> {
            Arc::new(Self {
                address: address!("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"),
                provider: RootProvider::new(RpcClient::mocked(asserter.clone())),
            })
        }
    }

    struct MockBaseWallet {
        address: Address,
        provider: RootProvider,
    }

    impl MockBaseWallet {
        fn with_asserter(asserter: &Asserter) -> Arc<dyn Wallet<Provider = RootProvider>> {
            Arc::new(Self {
                address: address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                provider: RootProvider::new(RpcClient::mocked(asserter.clone())),
            })
        }
    }

    #[async_trait]
    impl Evm for MockEthereumWallet {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            &self.provider
        }
    }

    #[async_trait]
    impl Wallet for MockEthereumWallet {
        fn address(&self) -> Address {
            self.address
        }

        async fn send(
            &self,
            _contract: Address,
            _calldata: Bytes,
            _note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            panic!("MockEthereumWallet::send should not be called in polling tests")
        }
    }

    #[async_trait]
    impl Evm for MockBaseWallet {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            &self.provider
        }
    }

    #[async_trait]
    impl Wallet for MockBaseWallet {
        fn address(&self) -> Address {
            self.address
        }

        async fn send(
            &self,
            _contract: Address,
            _calldata: Bytes,
            _note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            panic!("MockBaseWallet::send should not be called in polling tests")
        }
    }

    /// A Float (bytes32) representing zero balance, used as mock vaultBalance2 response.
    const ZERO_FLOAT_HEX: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000000";

    /// Creates a mock provider with no queued RPC responses.
    /// Any unexpected RPC call will fail immediately.
    fn mock_provider() -> impl Provider + Clone {
        let asserter = Asserter::new();
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    fn test_addresses() -> (Address, Address) {
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let order_owner = address!("0x2222222222222222222222222222222222222222");
        (orderbook, order_owner)
    }

    fn test_symbol(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn test_shares(n: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(n))
    }

    async fn create_test_raindex_service(
        pool: &SqlitePool,
        provider: impl Provider + Clone + 'static,
    ) -> Arc<RaindexService<ReadOnlyEvm<impl Provider + Clone + 'static>>> {
        let (_vault_store, vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        Arc::new(RaindexService::new(
            ReadOnlyEvm::new(provider),
            Address::ZERO,
            vault_registry_projection,
            Address::ZERO,
        ))
    }

    #[tokio::test]
    async fn poll_and_record_emits_offchain_equity_command_with_executor_positions() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![
                EquityPosition {
                    symbol: test_symbol("AAPL"),
                    quantity: test_shares(100),
                    market_value: Some(dec!(15000)),
                },
                EquityPosition {
                    symbol: test_symbol("MSFT"),
                    quantity: test_shares(50),
                    market_value: Some(dec!(20000)),
                },
            ],
            cash_balance_cents: 10_000_000,
        };
        let executor = MockExecutor::new().with_inventory(inventory.clone());

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("Expected OffchainEquity event to be emitted");

        let InventorySnapshotEvent::OffchainEquity { positions, .. } = offchain_equity_event else {
            panic!("Expected OffchainEquity event, got {offchain_equity_event:?}");
        };
        assert_eq!(positions.len(), 2, "Expected 2 positions");
        assert_eq!(
            positions.get(&test_symbol("AAPL")),
            Some(&test_shares(100)),
            "AAPL position mismatch"
        );
        assert_eq!(
            positions.get(&test_symbol("MSFT")),
            Some(&test_shares(50)),
            "MSFT position mismatch"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_offchain_cash_command_with_executor_cash_balance() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            cash_balance_cents: 25_000_000, // $250,000.00
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_cash_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainCash { .. }))
            .expect("Expected OffchainCash event to be emitted");

        let InventorySnapshotEvent::OffchainCash {
            cash_balance_cents, ..
        } = offchain_cash_event
        else {
            panic!("Expected OffchainCash event, got {offchain_cash_event:?}");
        };
        assert_eq!(
            *cash_balance_cents, 25_000_000,
            "Cash balance mismatch: expected $250,000.00"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_empty_positions_when_executor_has_no_positions() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            cash_balance_cents: 5_000_000,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("Expected OffchainEquity event even with empty positions");

        let InventorySnapshotEvent::OffchainEquity { positions, .. } = offchain_equity_event else {
            panic!("Expected OffchainEquity event, got {offchain_equity_event:?}");
        };
        assert!(positions.is_empty(), "Expected empty positions map");
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_offchain_commands_when_executor_returns_unimplemented() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        // Should succeed without error
        service.poll_and_record().await.unwrap();

        // Verify NO offchain events were emitted
        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_offchain_equity = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }));
        let has_offchain_cash = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OffchainCash { .. }));

        assert!(
            !has_offchain_equity,
            "Should NOT emit OffchainEquity when executor returns Unimplemented"
        );
        assert!(
            !has_offchain_cash,
            "Should NOT emit OffchainCash when executor returns Unimplemented"
        );
        assert!(
            logs_contain(
                "Executor returned non-fetched inventory result, skipping offchain polling"
            ),
            "Should log debug message explaining why offchain polling was skipped"
        );
    }

    #[tokio::test]
    async fn poll_and_record_handles_negative_cash_balance() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        // Margin account with negative cash (borrowed funds)
        let inventory = Inventory {
            positions: vec![EquityPosition {
                symbol: test_symbol("AAPL"),
                quantity: test_shares(1000),
                market_value: Some(dec!(150000)),
            }],
            cash_balance_cents: -5_000_000, // -$50,000 (margin debt)
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_cash_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainCash { .. }))
            .expect("Expected OffchainCash event");

        let InventorySnapshotEvent::OffchainCash {
            cash_balance_cents, ..
        } = offchain_cash_event
        else {
            panic!("Expected OffchainCash event, got {offchain_cash_event:?}");
        };
        assert_eq!(
            *cash_balance_cents, -5_000_000,
            "Should preserve negative cash balance"
        );
    }

    #[tokio::test]
    async fn poll_and_record_handles_fractional_share_positions() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let fractional_qty = FractionalShares::new(dec!(12.345)); // 12.345 shares
        let inventory = Inventory {
            positions: vec![EquityPosition {
                symbol: test_symbol("AAPL"),
                quantity: fractional_qty,
                market_value: Some(dec!(1851.75)),
            }],
            cash_balance_cents: 1_000_000,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("Expected OffchainEquity event");

        let InventorySnapshotEvent::OffchainEquity { positions, .. } = offchain_equity_event else {
            panic!("Expected OffchainEquity event, got {offchain_equity_event:?}");
        };
        assert_eq!(
            positions.get(&test_symbol("AAPL")),
            Some(&fractional_qty),
            "Should preserve fractional share quantity"
        );
    }

    #[tokio::test]
    async fn poll_and_record_uses_correct_aggregate_id() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let orderbook = address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let order_owner = address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");

        let inventory = Inventory {
            positions: vec![],
            cash_balance_cents: 10_000,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let expected_aggregate_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        }
        .to_string();
        let events = load_events_for_aggregate(&pool, &expected_aggregate_id).await;

        assert!(
            !events.is_empty(),
            "Expected events under aggregate ID {expected_aggregate_id}"
        );
    }

    const TEST_TOKEN: Address = address!("0x9876543210987654321098765432109876543210");
    const TEST_VAULT_ID: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
    const TEST_TX_HASH: TxHash =
        b256!("0x1111111111111111111111111111111111111111111111111111111111111111");

    async fn discover_equity_vault(
        pool: &SqlitePool,
        orderbook: Address,
        order_owner: Address,
        token: Address,
        vault_id: B256,
        symbol: Symbol,
    ) {
        let store = test_store::<VaultRegistry>(pool.clone(), ());
        let vault_registry_id = VaultRegistryId {
            orderbook,
            owner: order_owner,
        };

        store
            .send(
                &vault_registry_id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token,
                    vault_id,
                    discovered_in: TEST_TX_HASH,
                    symbol,
                },
            )
            .await
            .unwrap();
    }

    async fn discover_usdc_vault(
        pool: &SqlitePool,
        orderbook: Address,
        order_owner: Address,
        vault_id: B256,
    ) {
        let store = test_store::<VaultRegistry>(pool.clone(), ());
        let vault_registry_id = VaultRegistryId {
            orderbook,
            owner: order_owner,
        };

        store
            .send(
                &vault_registry_id,
                VaultRegistryCommand::DiscoverUsdcVault {
                    vault_id,
                    discovered_in: TEST_TX_HASH,
                },
            )
            .await
            .unwrap();
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_onchain_when_vault_registry_not_initialized() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_onchain_equity = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OnchainEquity { .. }));
        let has_onchain_cash = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OnchainCash { .. }));

        assert!(
            !has_onchain_equity,
            "Should NOT emit OnchainEquity when VaultRegistry not initialized"
        );
        assert!(
            !has_onchain_cash,
            "Should NOT emit OnchainCash when VaultRegistry not initialized"
        );
        assert!(
            logs_contain("Vault registry not initialized, skipping onchain polling"),
            "Should log debug message explaining why onchain polling was skipped"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_onchain_equity_when_no_equity_vaults_discovered() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_usdc_vault(&pool, orderbook, order_owner, TEST_VAULT_ID).await;

        let asserter = Asserter::new();
        asserter.push_success(&ZERO_FLOAT_HEX); // vaultBalance2 for USDC vault
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(&pool, provider).await;

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_onchain_equity = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OnchainEquity { .. }));

        assert!(
            !has_onchain_equity,
            "Should NOT emit OnchainEquity when no equity vaults discovered"
        );
        assert!(
            logs_contain("No equity vaults discovered, skipping onchain equity polling"),
            "Should log debug message explaining why equity polling was skipped"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_onchain_cash_when_no_usdc_vault_discovered() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_equity_vault(
            &pool,
            orderbook,
            order_owner,
            TEST_TOKEN,
            TEST_VAULT_ID,
            test_symbol("AAPL"),
        )
        .await;

        let asserter = Asserter::new();
        asserter.push_success(&ZERO_FLOAT_HEX); // vaultBalance2 for equity vault
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(&pool, provider).await;

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_onchain_cash = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OnchainCash { .. }));

        assert!(
            !has_onchain_cash,
            "Should NOT emit OnchainCash when no USDC vault discovered"
        );
        assert!(
            logs_contain("No USDC vault discovered, skipping onchain cash polling"),
            "Should log debug message explaining why cash polling was skipped"
        );
    }

    #[tokio::test]
    async fn poll_and_record_fails_on_rpc_failure_for_equity_vault() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_equity_vault(
            &pool,
            orderbook,
            order_owner,
            TEST_TOKEN,
            TEST_VAULT_ID,
            test_symbol("AAPL"),
        )
        .await;

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure"); // vaultBalance2 for equity vault
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(&pool, provider).await;

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        let error = service.poll_and_record().await.unwrap_err();

        assert!(
            matches!(error, InventoryPollingError::Raindex(_)),
            "Expected Vault error when RPC fails, got {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_and_record_fails_on_rpc_failure_for_usdc_vault() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_usdc_vault(&pool, orderbook, order_owner, TEST_VAULT_ID).await;

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure"); // vaultBalance2 for USDC vault
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(&pool, provider).await;

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        let error = service.poll_and_record().await.unwrap_err();

        assert!(
            matches!(error, InventoryPollingError::Raindex(_)),
            "Expected Vault error when RPC fails, got {error:?}"
        );
    }

    /// Loads all InventorySnapshotEvents for the given orderbook/owner from the event store.
    async fn load_snapshot_events(
        pool: &SqlitePool,
        orderbook: Address,
        order_owner: Address,
    ) -> Vec<InventorySnapshotEvent> {
        let aggregate_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        }
        .to_string();
        load_events_for_aggregate(pool, &aggregate_id).await
    }

    /// Loads InventorySnapshot events for a specific aggregate ID from the SQLite event store.
    async fn load_events_for_aggregate(
        pool: &SqlitePool,
        aggregate_id: &str,
    ) -> Vec<InventorySnapshotEvent> {
        let rows = sqlx::query(
            r"
            SELECT payload
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'InventorySnapshot'
            ORDER BY sequence ASC
            ",
        )
        .bind(aggregate_id)
        .fetch_all(pool)
        .await
        .unwrap();

        rows.iter()
            .map(|row| {
                let payload: String = row.get("payload");
                serde_json::from_str(&payload).unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn poll_and_record_emits_ethereum_cash_when_wallet_provided() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let raw_usdc = U256::from(5_000_000_000u64); // 5000 USDC (6 decimals)
        let asserter = Asserter::new();
        let encoded = alloy::hex::encode_prefixed(raw_usdc.abi_encode());
        asserter.push_success(&encoded);
        let ethereum_wallet = MockEthereumWallet::with_asserter(&asserter);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: Some(ethereum_wallet),
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let ethereum_cash_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::EthereumCash { .. }))
            .expect("Expected EthereumCash event to be emitted");

        let InventorySnapshotEvent::EthereumCash { usdc_balance, .. } = ethereum_cash_event else {
            panic!("Expected EthereumCash event, got {ethereum_cash_event:?}");
        };
        let expected = u256_to_usdc(raw_usdc).unwrap();
        assert_eq!(*usdc_balance, expected, "USDC balance mismatch");
    }

    #[tokio::test]
    async fn poll_and_record_emits_base_wallet_cash_when_wallet_provided() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let raw_usdc = U256::from(5_000_000_000u64); // 5000 USDC (6 decimals)
        let asserter = Asserter::new();
        let encoded = alloy::hex::encode_prefixed(raw_usdc.abi_encode());
        asserter.push_success(&encoded);
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let base_wallet_cash_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::BaseWalletCash { .. }))
            .expect("Expected BaseWalletCash event to be emitted");

        let InventorySnapshotEvent::BaseWalletCash { usdc_balance, .. } = base_wallet_cash_event
        else {
            panic!("Expected BaseWalletCash event, got {base_wallet_cash_event:?}");
        };
        let expected = u256_to_usdc(raw_usdc).unwrap();
        assert_eq!(*usdc_balance, expected, "USDC balance mismatch");
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_ethereum_cash_when_no_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_ethereum_cash = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::EthereumCash { .. }));

        assert!(
            !has_ethereum_cash,
            "Should NOT emit EthereumCash when no Ethereum wallet configured"
        );
        assert!(
            logs_contain("No Ethereum wallet configured, skipping Ethereum cash polling"),
            "Should log debug message explaining why Ethereum cash polling was skipped"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_base_wallet_cash_when_no_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_base_wallet_cash = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::BaseWalletCash { .. }));

        assert!(
            !has_base_wallet_cash,
            "Should NOT emit BaseWalletCash when no Base wallet configured"
        );
        assert!(
            logs_contain("No Base wallet configured, skipping Base cash polling"),
            "Should log debug message explaining why Base cash polling was skipped"
        );
    }

    #[tokio::test]
    async fn poll_and_record_propagates_ethereum_rpc_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        asserter.push_failure_msg("Ethereum RPC failure");
        let ethereum_wallet = MockEthereumWallet::with_asserter(&asserter);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: Some(ethereum_wallet),
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        let error = service.poll_and_record().await.unwrap_err();
        assert!(matches!(error, InventoryPollingError::Evm(_)));
    }

    #[tokio::test]
    async fn poll_and_record_propagates_base_wallet_rpc_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        asserter.push_failure_msg("Base RPC failure");
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        let error = service.poll_and_record().await.unwrap_err();
        assert!(matches!(error, InventoryPollingError::Evm(_)));
    }

    #[tokio::test]
    async fn poll_and_record_emits_base_wallet_unwrapped_equity_when_wallet_and_tokens_provided() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let token_addr = address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let raw_balance = U256::from(500_000u64) * U256::from(10u64).pow(U256::from(18u64)); // 500,000 tokens (18 decimals)
        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        asserter.push_success(&usdc_encoded); // USDC balanceOf (zero)
        let equity_encoded = alloy::hex::encode_prefixed(raw_balance.abi_encode());
        asserter.push_success(&equity_encoded); // equity token balanceOf
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut equity_tokens = HashMap::new();
        equity_tokens.insert(test_symbol("AAPL"), token_addr);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: equity_tokens,
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let equity_event = events
            .iter()
            .find(|event| {
                matches!(
                    event,
                    InventorySnapshotEvent::BaseWalletUnwrappedEquity { .. }
                )
            })
            .expect("Expected BaseWalletUnwrappedEquity event to be emitted");

        let InventorySnapshotEvent::BaseWalletUnwrappedEquity { balances, .. } = equity_event
        else {
            panic!("Expected BaseWalletUnwrappedEquity event, got {equity_event:?}");
        };
        let expected = FractionalShares::from_u256_18_decimals(raw_balance).unwrap();
        assert_eq!(
            balances.get(&test_symbol("AAPL")),
            Some(&expected),
            "AAPL balance mismatch"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_base_wallet_unwrapped_equity_when_no_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_equity = events.iter().any(|event| {
            matches!(
                event,
                InventorySnapshotEvent::BaseWalletUnwrappedEquity { .. }
            )
        });

        assert!(
            !has_equity,
            "Should NOT emit BaseWalletUnwrappedEquity when no Base wallet configured"
        );
        assert!(logs_contain(
            "No Base wallet configured, skipping Base unwrapped equity polling"
        ));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_base_wallet_unwrapped_equity_when_no_token_addresses() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let raw_usdc = U256::from(1_000_000u64); // 1 USDC
        let asserter = Asserter::new();
        let encoded = alloy::hex::encode_prefixed(raw_usdc.abi_encode());
        asserter.push_success(&encoded);
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_equity = events.iter().any(|event| {
            matches!(
                event,
                InventorySnapshotEvent::BaseWalletUnwrappedEquity { .. }
            )
        });

        assert!(
            !has_equity,
            "Should NOT emit BaseWalletUnwrappedEquity when no token addresses configured"
        );
        assert!(logs_contain(
            "No unwrapped equity token addresses configured, skipping Base unwrapped equity polling"
        ));
    }

    #[tokio::test]
    async fn poll_and_record_propagates_base_wallet_unwrapped_equity_rpc_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        asserter.push_success(&usdc_encoded); // USDC balanceOf succeeds
        asserter.push_failure_msg("Equity RPC failure"); // equity balanceOf fails
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut equity_tokens = HashMap::new();
        equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingCtx {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: equity_tokens,
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        let error = service.poll_and_record().await.unwrap_err();
        assert!(
            matches!(error, InventoryPollingError::Evm(_)),
            "Expected Evm error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_base_wallet_wrapped_equity_when_wallet_and_tokens_provided() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let token_addr = address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        let raw_balance = U256::from(250_000u64) * U256::from(10u64).pow(U256::from(18u64));
        let asserter = Asserter::new();
        let usdc_encoded = format!("0x{}", alloy::hex::encode(U256::ZERO.abi_encode()));
        asserter.push_success(&usdc_encoded); // USDC balanceOf (zero)
        let equity_encoded = format!("0x{}", alloy::hex::encode(raw_balance.abi_encode()));
        asserter.push_success(&equity_encoded); // wrapped equity token balanceOf
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(test_symbol("AAPL"), token_addr);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingConfig {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: wrapped_equity_tokens,
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let wrapped_equity_event = events
            .iter()
            .find(|event| {
                matches!(
                    event,
                    InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
                )
            })
            .expect("Expected BaseWalletWrappedEquity event to be emitted");

        let InventorySnapshotEvent::BaseWalletWrappedEquity { balances, .. } = wrapped_equity_event
        else {
            panic!("Expected BaseWalletWrappedEquity event, got {wrapped_equity_event:?}");
        };
        let expected = FractionalShares::from_u256_18_decimals(raw_balance).unwrap();
        assert_eq!(
            balances.get(&test_symbol("AAPL")),
            Some(&expected),
            "AAPL wrapped balance mismatch"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_base_wallet_wrapped_equity_for_multiple_assets() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let raw_balance = U256::from(42u64) * U256::from(10u64).pow(U256::from(18u64));
        let asserter = Asserter::new();
        let usdc_encoded = format!("0x{}", alloy::hex::encode(U256::ZERO.abi_encode()));
        let wrapped_equity_encoded = format!("0x{}", alloy::hex::encode(raw_balance.abi_encode()));
        asserter.push_success(&usdc_encoded); // USDC balanceOf (zero)
        asserter.push_success(&wrapped_equity_encoded); // first wrapped token balanceOf
        asserter.push_success(&wrapped_equity_encoded); // second wrapped token balanceOf
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        );
        wrapped_equity_tokens.insert(
            test_symbol("TSLA"),
            address!("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingConfig {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: wrapped_equity_tokens,
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let wrapped_equity_event = events
            .iter()
            .find(|event| {
                matches!(
                    event,
                    InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
                )
            })
            .expect("Expected BaseWalletWrappedEquity event to be emitted");

        let InventorySnapshotEvent::BaseWalletWrappedEquity { balances, .. } = wrapped_equity_event
        else {
            panic!("Expected BaseWalletWrappedEquity event, got {wrapped_equity_event:?}");
        };
        let expected = FractionalShares::from_u256_18_decimals(raw_balance).unwrap();
        assert_eq!(balances.get(&test_symbol("AAPL")), Some(&expected));
        assert_eq!(balances.get(&test_symbol("TSLA")), Some(&expected));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_base_wallet_wrapped_equity_when_no_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingConfig {
                ethereum: None,
                base: None,
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: wrapped_equity_tokens,
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_wrapped_equity = events.iter().any(|event| {
            matches!(
                event,
                InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
            )
        });

        assert!(
            !has_wrapped_equity,
            "Should NOT emit BaseWalletWrappedEquity when no Base wallet configured"
        );
        assert!(logs_contain(
            "No Base wallet configured, skipping Base wrapped equity polling"
        ));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_base_wallet_wrapped_equity_when_no_token_addresses() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let raw_usdc = U256::from(1_000_000u64); // 1 USDC
        let asserter = Asserter::new();
        let encoded = format!("0x{}", alloy::hex::encode(raw_usdc.abi_encode()));
        asserter.push_success(&encoded);
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingConfig {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: HashMap::new(),
            },
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_wrapped_equity = events.iter().any(|event| {
            matches!(
                event,
                InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
            )
        });

        assert!(
            !has_wrapped_equity,
            "Should NOT emit BaseWalletWrappedEquity when no token addresses configured"
        );
        assert!(logs_contain(
            "No wrapped equity token addresses configured, skipping Base wrapped equity polling"
        ));
    }

    #[tokio::test]
    async fn poll_and_record_propagates_base_wallet_wrapped_equity_rpc_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        let usdc_encoded = format!("0x{}", alloy::hex::encode(U256::ZERO.abi_encode()));
        asserter.push_success(&usdc_encoded); // USDC balanceOf succeeds
        asserter.push_failure_msg("Wrapped equity RPC failure"); // wrapped equity balanceOf fails
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingConfig {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: wrapped_equity_tokens,
            },
        );

        let error = service.poll_and_record().await.unwrap_err();
        assert!(
            matches!(error, InventoryPollingError::Evm(_)),
            "Expected Evm error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_and_record_skips_unchanged_base_wallet_wrapped_equity_snapshot() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(&pool, provider.clone()).await;
        let (orderbook, order_owner) = test_addresses();

        let raw_balance = U256::from(3u64) * U256::from(10u64).pow(U256::from(18u64));
        let asserter = Asserter::new();
        let usdc_encoded = format!("0x{}", alloy::hex::encode(U256::ZERO.abi_encode()));
        let wrapped_equity_encoded = format!("0x{}", alloy::hex::encode(raw_balance.abi_encode()));
        asserter.push_success(&usdc_encoded); // first poll USDC balanceOf
        asserter.push_success(&wrapped_equity_encoded); // first poll wrapped balanceOf
        asserter.push_success(&usdc_encoded); // second poll USDC balanceOf
        asserter.push_success(&wrapped_equity_encoded); // second poll wrapped balanceOf
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            orderbook,
            order_owner,
            Arc::new(test_store(pool.clone(), ())),
            WalletPollingConfig {
                ethereum: None,
                base: Some(base_wallet),
                unwrapped_equity_token_addresses: HashMap::new(),
                wrapped_equity_token_addresses: wrapped_equity_tokens,
            },
        );

        service.poll_and_record().await.unwrap();
        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let wrapped_equity_event_count = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
                )
            })
            .count();

        assert_eq!(
            wrapped_equity_event_count, 1,
            "Should not emit a second BaseWalletWrappedEquity event when the balance is unchanged"
        );
    }
}
