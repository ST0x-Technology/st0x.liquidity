//! Rain OrderBook V6 (Raindex) vault operations on Base.
//!
//! This module provides a service layer for depositing and withdrawing tokens to/from
//! Raindex vaults using the `deposit4` and `withdraw4` contract functions.
//!
//! The primary use case is USDC vault management for inventory rebalancing in the
//! market making system.
//!
//! ## V6 Float Format
//!
//! OrderBook V6 uses a custom float format (B256) for amounts. All conversions between
//! standard fixed-point amounts (U256) and the float format MUST use rain-math-float.

use alloy::primitives::{Address, B256, TxHash, U256, address};
use async_trait::async_trait;
use rain_math_float::Float;
use std::sync::Arc;
use tracing::{debug, info};

use st0x_event_sorcery::ProjectionError;
use st0x_evm::{Evm, EvmError, IntoErrorRegistry, OpenChainErrorRegistry, Wallet};
use st0x_exact_decimal::ExactDecimal;
use st0x_execution::{FractionalShares, Symbol};

use crate::bindings::{IERC20, IOrderBookV6};
use crate::threshold::Usdc;
use crate::vault_registry::{VaultRegistry, VaultRegistryId, VaultRegistryProjection};

const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
const USDC_DECIMALS: u8 = 6;

/// Vault identifier for Rain OrderBook vaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RaindexVaultId(pub(crate) B256);

#[derive(Debug, thiserror::Error)]
pub(crate) enum RaindexError {
    #[error("EVM error: {0}")]
    Evm(#[from] EvmError),
    #[error("Contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("Float error: {0}")]
    Float(#[from] rain_math_float::FloatError),
    #[error("Amount cannot be zero")]
    ZeroAmount,
    #[error("Vault registry not found for aggregate {0}")]
    RegistryNotFound(VaultRegistryId),
    #[error(transparent)]
    Projection(#[from] ProjectionError<VaultRegistry>),
    #[error("Vault not found for token {0}")]
    VaultNotFound(Address),
    #[error("Token not found for symbol {0}")]
    TokenNotFound(Symbol),
}

/// Service for managing Rain OrderBook vault operations.
///
/// Parameterized by [`Evm`] for read operations (balance queries, vault
/// lookups) and additionally requires [`Wallet`] for write operations
/// (deposits, withdrawals).
///
/// # Example
///
/// ```ignore
/// let service = RaindexService::new(wallet, orderbook_address, projection, owner);
///
/// // Lookup vault ID for a token (read-only, needs Evm)
/// let vault_id = service.lookup_vault_id(token_address).await?;
///
/// // Deposit USDC to vault (needs Wallet)
/// let amount = U256::from(1000) * U256::from(10).pow(U256::from(6)); // 1000 USDC
/// service.deposit_usdc(vault_id, amount).await?;
/// ```
pub(crate) struct RaindexService<E: Evm> {
    orderbook_address: Address,
    vault_registry_projection: Arc<VaultRegistryProjection>,
    evm: E,
    owner: Address,
}

impl<E: Evm> RaindexService<E> {
    pub(crate) fn new(
        evm: E,
        orderbook: Address,
        vault_registry_projection: Arc<VaultRegistryProjection>,
        owner: Address,
    ) -> Self {
        Self {
            orderbook_address: orderbook,
            vault_registry_projection,
            evm,
            owner,
        }
    }

    async fn load_registry(&self) -> Result<VaultRegistry, RaindexError> {
        let aggregate_id = VaultRegistryId {
            orderbook: self.orderbook_address,
            owner: self.owner,
        };

        self.vault_registry_projection
            .load(&aggregate_id)
            .await?
            .ok_or(RaindexError::RegistryNotFound(aggregate_id))
    }
}

/// Write operations that require a [`Wallet`] for transaction signing.
impl<W: Wallet> RaindexService<W> {
    /// Deposits tokens to a Rain OrderBook vault.
    ///
    /// # Parameters
    ///
    /// * `token` - ERC20 token address to deposit
    /// * `vault_id` - Target vault identifier
    /// * `amount` - Amount of tokens to deposit (in token's base units)
    /// * `decimals` - Token decimals for float conversion
    ///
    /// # Errors
    ///
    /// Returns `RaindexError::ZeroAmount` if amount is zero.
    /// Returns `RaindexError::Float` if amount cannot be converted to float format.
    /// Returns `RaindexError::Evm` for transaction errors.
    pub(crate) async fn deposit<Registry: IntoErrorRegistry>(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        if amount.is_zero() {
            return Err(RaindexError::ZeroAmount);
        }

        self.approve_for_orderbook::<Registry>(token, amount)
            .await?;

        self.deposit4_to_vault::<Registry>(token, vault_id, amount, decimals)
            .await
    }

    async fn approve_for_orderbook<Registry: IntoErrorRegistry>(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<(), RaindexError> {
        let current_allowance: U256 = self
            .evm
            .call::<Registry, _>(
                token,
                IERC20::allowanceCall {
                    owner: self.owner,
                    spender: self.orderbook_address,
                },
            )
            .await?;

        if current_allowance >= amount {
            debug!(%token, %amount, %current_allowance, "Sufficient allowance, skipping approve");
            return Ok(());
        }

        debug!(%token, %amount, %current_allowance, spender = %self.orderbook_address, "Sending ERC20 approve");

        let receipt = self
            .evm
            .submit::<Registry, _>(
                token,
                IERC20::approveCall {
                    spender: self.orderbook_address,
                    amount,
                },
                "ERC20 approve for orderbook",
            )
            .await?;

        info!(tx_hash = %receipt.transaction_hash, "Approve confirmed");
        Ok(())
    }

    async fn deposit4_to_vault<Registry: IntoErrorRegistry>(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        let amount_float = Float::from_fixed_decimal(amount, decimals)?;

        debug!(%token, ?vault_id, %amount, "Sending deposit4");

        let calldata = IOrderBookV6::deposit4Call {
            token,
            vaultId: vault_id.0,
            depositAmount: amount_float.get_inner(),
            tasks: Vec::new(),
        };

        let receipt = self
            .evm
            .submit::<Registry, _>(self.orderbook_address, calldata, "deposit4 to vault")
            .await?;

        info!(tx_hash = %receipt.transaction_hash, "deposit4 confirmed");
        Ok(receipt.transaction_hash)
    }

    /// Deposits USDC to a Rain OrderBook vault on Base.
    ///
    /// Convenience method that calls `deposit` with the Base USDC address and decimals.
    ///
    /// # Parameters
    ///
    /// * `vault_id` - Target vault identifier
    /// * `amount` - Amount of USDC to deposit (in USDC's base units, 6 decimals)
    pub(crate) async fn deposit_usdc(
        &self,
        vault_id: RaindexVaultId,
        amount: U256,
    ) -> Result<TxHash, RaindexError> {
        self.deposit::<OpenChainErrorRegistry>(USDC_BASE, vault_id, amount, USDC_DECIMALS)
            .await
    }

    /// Withdraws USDC from a Rain OrderBook vault on Base.
    ///
    /// Convenience method that calls `withdraw` with the Base USDC address and decimals.
    ///
    /// # Parameters
    ///
    /// * `vault_id` - Source vault identifier
    /// * `target_amount` - Target amount of USDC to withdraw (in USDC's base units, 6 decimals)
    pub(crate) async fn withdraw_usdc(
        &self,
        vault_id: RaindexVaultId,
        target_amount: U256,
    ) -> Result<TxHash, RaindexError> {
        self.withdraw(USDC_BASE, vault_id, target_amount, USDC_DECIMALS)
            .await
    }
}

/// Read operations that only need chain access (no signing).
impl<E: Evm> RaindexService<E> {
    pub(crate) async fn get_equity_balance<Registry: IntoErrorRegistry>(
        &self,
        owner: Address,
        token: Address,
        vault_id: RaindexVaultId,
    ) -> Result<FractionalShares, RaindexError> {
        let exact = self
            .get_vault_balance::<Registry>(owner, token, vault_id)
            .await?;
        Ok(FractionalShares::new(exact))
    }

    /// Gets the USDC balance of a vault on Base.
    pub(crate) async fn get_usdc_balance<Registry: IntoErrorRegistry>(
        &self,
        owner: Address,
        vault_id: RaindexVaultId,
    ) -> Result<Usdc, RaindexError> {
        let exact = self
            .get_vault_balance::<Registry>(owner, USDC_BASE, vault_id)
            .await?;
        Ok(Usdc(exact))
    }

    async fn get_vault_balance<Registry: IntoErrorRegistry>(
        &self,
        owner: Address,
        token: Address,
        vault_id: RaindexVaultId,
    ) -> Result<ExactDecimal, RaindexError> {
        let balance_float = self
            .evm
            .call::<Registry, _>(
                self.orderbook_address,
                IOrderBookV6::vaultBalance2Call {
                    owner,
                    token,
                    vaultId: vault_id.0,
                },
            )
            .await?;

        Ok(ExactDecimal::from_raw(balance_float))
    }
}

/// Abstraction for Raindex (Rain OrderBook) operations.
///
/// This trait abstracts deposit, withdraw, and vault lookup operations for Raindex,
/// allowing different implementations (real service, mock) to be used interchangeably.
#[async_trait]
pub(crate) trait Raindex: Send + Sync {
    /// Looks up the vault ID for a given token from the vault registry.
    async fn lookup_vault_id(&self, token: Address) -> Result<RaindexVaultId, RaindexError>;

    /// Looks up the token address and vault ID for a given symbol from the vault registry.
    async fn lookup_vault_info(
        &self,
        symbol: &Symbol,
    ) -> Result<(Address, RaindexVaultId), RaindexError>;

    /// Deposits tokens to a Rain OrderBook vault.
    async fn deposit(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError>;

    /// Withdraws tokens from a Rain OrderBook vault.
    async fn withdraw(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        target_amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError>;
}

#[async_trait]
impl<W: Wallet> Raindex for RaindexService<W> {
    async fn lookup_vault_id(&self, token: Address) -> Result<RaindexVaultId, RaindexError> {
        let registry = self.load_registry().await?;
        registry
            .vault_id_by_token(token)
            .map(RaindexVaultId)
            .ok_or(RaindexError::VaultNotFound(token))
    }

    async fn lookup_vault_info(
        &self,
        symbol: &Symbol,
    ) -> Result<(Address, RaindexVaultId), RaindexError> {
        let registry = self.load_registry().await?;
        let token = registry
            .token_by_symbol(symbol)
            .ok_or_else(|| RaindexError::TokenNotFound(symbol.clone()))?;
        let vault_id = registry
            .vault_id_by_token(token)
            .ok_or(RaindexError::VaultNotFound(token))?;
        Ok((token, RaindexVaultId(vault_id)))
    }

    async fn deposit(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        Self::deposit::<OpenChainErrorRegistry>(self, token, vault_id, amount, decimals).await
    }

    async fn withdraw(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        target_amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        if target_amount.is_zero() {
            return Err(RaindexError::ZeroAmount);
        }

        let amount_float = Float::from_fixed_decimal(target_amount, decimals)?;

        let receipt = self
            .evm
            .submit::<OpenChainErrorRegistry, _>(
                self.orderbook_address,
                IOrderBookV6::withdraw4Call {
                    token,
                    vaultId: vault_id.0,
                    targetAmount: amount_float.get_inner(),
                    tasks: Vec::new(),
                },
                "withdraw4 from vault",
            )
            .await?;

        info!(tx_hash = %receipt.transaction_hash, "withdraw4 confirmed");
        Ok(receipt.transaction_hash)
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::Ethereum;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, b256};
    use alloy::providers::Provider;
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use alloy::transports::{RpcError, TransportErrorKind};
    use proptest::prelude::*;
    use tracing_test::traced_test;

    use st0x_evm::NoOpErrorRegistry;
    use st0x_evm::Wallet;
    use st0x_evm::local::RawPrivateKeyWallet;

    use st0x_event_sorcery::StoreBuilder;

    use super::*;
    use crate::bindings::{IOrderBookV6, OrderBook, TOFUTokenDecimals, TestERC20};
    /// Address where LibTOFUTokenDecimals expects the singleton contract to be deployed.
    const TOFU_DECIMALS_ADDRESS: Address = address!("0xF66761F6b5F58202998D6Cd944C81b22Dc6d4f1E");

    type BaseProvider = FillProvider<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        RootProvider<Ethereum>,
        Ethereum,
    >;

    struct LocalEvm {
        anvil: AnvilInstance,
        wallet: RawPrivateKeyWallet<BaseProvider>,
        private_key: B256,
        orderbook_address: Address,
        token_address: Address,
    }

    impl LocalEvm {
        async fn new() -> Result<Self, LocalEvmError> {
            let anvil = Anvil::new().spawn();
            let endpoint = anvil.endpoint();

            let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());

            let base_provider = ProviderBuilder::new().connect_http(endpoint.parse()?);
            let wallet = RawPrivateKeyWallet::new(&private_key, base_provider, 1)?;

            Self::deploy_tofu_decimals(wallet.provider()).await?;

            let orderbook_address = Self::deploy_orderbook(wallet.provider()).await?;

            let token_address = Self::deploy_token(wallet.provider(), wallet.address()).await?;

            Ok(Self {
                anvil,
                wallet,
                private_key,
                orderbook_address,
                token_address,
            })
        }

        async fn deploy_tofu_decimals(provider: &impl Provider) -> Result<(), LocalEvmError> {
            let tofu = TOFUTokenDecimals::deploy(provider).await?;
            let deployed_code = provider.get_code_at(*tofu.address()).await?;

            provider
                .raw_request::<_, ()>(
                    "anvil_setCode".into(),
                    (TOFU_DECIMALS_ADDRESS, deployed_code),
                )
                .await?;

            Ok(())
        }

        async fn deploy_orderbook(provider: &impl Provider) -> Result<Address, LocalEvmError> {
            let orderbook = OrderBook::deploy(provider).await?;

            Ok(*orderbook.address())
        }

        async fn deploy_token(
            provider: &impl Provider,
            recipient: Address,
        ) -> Result<Address, LocalEvmError> {
            let token = TestERC20::deploy(provider).await?;

            let initial_supply = U256::from(1_000_000) * U256::from(10).pow(U256::from(18));

            token
                .mint(recipient, initial_supply)
                .send()
                .await?
                .get_receipt()
                .await?;

            Ok(*token.address())
        }

        async fn approve_tokens(
            &self,
            token: Address,
            spender: Address,
            amount: U256,
        ) -> Result<(), LocalEvmError> {
            let token_contract = TestERC20::new(token, self.wallet.provider());

            token_contract
                .approve(spender, amount)
                .send()
                .await?
                .get_receipt()
                .await?;

            Ok(())
        }

        async fn get_vault_balance(
            &self,
            token: Address,
            vault_id: B256,
        ) -> Result<B256, LocalEvmError> {
            let balance = self
                .wallet
                .call::<NoOpErrorRegistry, _>(
                    self.orderbook_address,
                    IOrderBookV6::vaultBalance2Call {
                        owner: self.wallet.address(),
                        token,
                        vaultId: vault_id,
                    },
                )
                .await?;

            Ok(balance)
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum LocalEvmError {
        #[error("EVM error")]
        Evm(#[from] EvmError),
        #[error("Contract error")]
        Contract(#[from] alloy::contract::Error),
        #[error("Provider error")]
        Provider(#[from] alloy::providers::PendingTransactionError),
        #[error("RPC error")]
        Rpc(#[from] RpcError<TransportErrorKind>),
        #[error("URL parse error")]
        UrlParse(#[from] url::ParseError),
    }

    const TEST_TOKEN_DECIMALS: u8 = 18;
    const TEST_VAULT_ID: RaindexVaultId = RaindexVaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));

    async fn create_test_raindex_service(
        local_evm: &LocalEvm,
    ) -> RaindexService<RawPrivateKeyWallet<BaseProvider>> {
        let pool = crate::test_utils::setup_test_db().await;
        let (_store, vault_registry_projection) = StoreBuilder::<VaultRegistry>::new(pool)
            .build(())
            .await
            .unwrap();

        let base_provider =
            ProviderBuilder::new().connect_http(local_evm.anvil.endpoint().parse().unwrap());

        let wallet = RawPrivateKeyWallet::new(&local_evm.private_key, base_provider, 1).unwrap();

        let owner = wallet.address();

        RaindexService::new(
            wallet,
            local_evm.orderbook_address,
            vault_registry_projection,
            owner,
        )
    }

    #[tokio::test]
    async fn deposit_rejects_zero_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let service = create_test_raindex_service(&local_evm).await;

        let result = service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                TEST_VAULT_ID,
                U256::ZERO,
                TEST_TOKEN_DECIMALS,
            )
            .await;

        assert!(matches!(result.unwrap_err(), RaindexError::ZeroAmount));
    }

    #[tokio::test]
    #[traced_test]
    async fn deposit_approves_and_transfers_without_prior_allowance() {
        let local_evm = LocalEvm::new().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let vault_id = TEST_VAULT_ID;

        // Explicitly set allowance to zero to demonstrate RaindexService handles approval
        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                U256::ZERO,
            )
            .await
            .unwrap();

        let service = create_test_raindex_service(&local_evm).await;

        let vault_balance_before = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        assert!(vault_balance_before.is_zero());

        let tx_hash = service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                vault_id,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        assert!(!tx_hash.is_zero());

        let vault_balance_after = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        let expected_float = Float::from_fixed_decimal(deposit_amount, TEST_TOKEN_DECIMALS)
            .unwrap()
            .get_inner();
        assert_eq!(vault_balance_after, expected_float);

        assert!(logs_contain("Sending ERC20 approve"));
        assert!(logs_contain("Approve confirmed"));
        assert!(logs_contain("Sending deposit4"));
        assert!(logs_contain("deposit4 confirmed"));
    }

    #[tokio::test]
    #[traced_test]
    async fn deposit_skips_approve_when_allowance_is_sufficient() {
        let local_evm = LocalEvm::new().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let vault_id = TEST_VAULT_ID;

        // Pre-approve with sufficient allowance -- deposit should skip the approve tx
        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();

        let service = create_test_raindex_service(&local_evm).await;

        let vault_balance_before = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        assert!(vault_balance_before.is_zero());

        let tx_hash = service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                vault_id,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        assert!(!tx_hash.is_zero());

        let vault_balance_after = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        let expected_float = Float::from_fixed_decimal(deposit_amount, TEST_TOKEN_DECIMALS)
            .unwrap()
            .get_inner();
        assert_eq!(vault_balance_after, expected_float);

        // Approve should NOT have been sent since allowance was already sufficient
        assert!(!logs_contain("Sending ERC20 approve"));
        assert!(logs_contain("Sufficient allowance"));
    }

    #[tokio::test]
    async fn withdraw_rejects_zero_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let service = create_test_raindex_service(&local_evm).await;

        let result = service
            .withdraw(
                local_evm.token_address,
                TEST_VAULT_ID,
                U256::ZERO,
                TEST_TOKEN_DECIMALS,
            )
            .await;

        assert!(matches!(result.unwrap_err(), RaindexError::ZeroAmount));
    }

    #[tokio::test]
    async fn test_withdraw_succeeds_with_deployed_contract() {
        let local_evm = LocalEvm::new().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let withdraw_amount = U256::from(500) * U256::from(10).pow(U256::from(18));
        let vault_id = TEST_VAULT_ID;

        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();

        let service = create_test_raindex_service(&local_evm).await;

        service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                vault_id,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        let tx_hash = service
            .withdraw(
                local_evm.token_address,
                vault_id,
                withdraw_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        assert!(!tx_hash.is_zero());
    }

    #[test]
    fn usdc_base_address_is_correct() {
        assert_eq!(
            USDC_BASE,
            address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
        );
    }

    #[tokio::test]
    async fn get_equity_balance_returns_zero_for_empty_vault() {
        let local_evm = LocalEvm::new().await.unwrap();
        let service = create_test_raindex_service(&local_evm).await;

        let balance = service
            .get_equity_balance::<NoOpErrorRegistry>(
                local_evm.wallet.address(),
                local_evm.token_address,
                TEST_VAULT_ID,
            )
            .await
            .unwrap();

        assert_eq!(balance, FractionalShares::ZERO);
    }

    #[tokio::test]
    async fn get_equity_balance_returns_deposited_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));

        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();

        let service = create_test_raindex_service(&local_evm).await;

        service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        let balance = service
            .get_equity_balance::<NoOpErrorRegistry>(
                local_evm.wallet.address(),
                local_evm.token_address,
                TEST_VAULT_ID,
            )
            .await
            .unwrap();

        let expected = FractionalShares::new(ExactDecimal::parse("1000").unwrap());
        assert_eq!(
            balance, expected,
            "Expected 1000 shares but got {balance:?}"
        );
    }

    #[tokio::test]
    async fn get_equity_balance_returns_remaining_after_withdrawal() {
        let local_evm = LocalEvm::new().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let withdraw_amount = U256::from(300) * U256::from(10).pow(U256::from(18));

        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();

        let service = create_test_raindex_service(&local_evm).await;

        service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        service
            .withdraw(
                local_evm.token_address,
                TEST_VAULT_ID,
                withdraw_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        let balance = service
            .get_equity_balance::<NoOpErrorRegistry>(
                local_evm.wallet.address(),
                local_evm.token_address,
                TEST_VAULT_ID,
            )
            .await
            .unwrap();

        let expected = FractionalShares::new(ExactDecimal::parse("700").unwrap());
        assert_eq!(balance, expected, "Expected 700 shares but got {balance:?}");
    }

    /// Values with large exponents produce scientific notation from
    /// Float::format(), which Decimal::from_str cannot parse.
    /// ExactDecimal::from_raw handles this correctly.
    #[test]
    fn large_exponent_produces_correct_value() {
        let float = Float::parse("100000000000000000000".to_string())
            .expect("valid Float from decimal string");

        let exact = ExactDecimal::from_raw(float.get_inner());
        assert_eq!(exact, ExactDecimal::parse("100000000000000000000").unwrap());
    }

    proptest! {
        /// Roundtrip: decimal string -> Float::parse -> ExactDecimal::from_raw ->
        /// ExactDecimal matches the original string.
        #[test]
        fn roundtrip_from_decimal_string(
            integer in 0u64..1_000_000_000,
            fraction in 0u32..1_000_000,
        ) {
            let input = format!("{integer}.{fraction:06}");
            let float = Float::parse(input.clone()).map_err(|err| {
                TestCaseError::Reject(format!("Float::parse rejected {input}: {err}").into())
            })?;

            let exact = ExactDecimal::from_raw(float.get_inner());

            // Re-parse original to compare as ExactDecimal
            let expected = ExactDecimal::parse(&input).map_err(|err| {
                TestCaseError::fail(format!("ExactDecimal::parse failed for {input}: {err}"))
            })?;
            prop_assert_eq!(exact, expected);
        }

        /// Roundtrip: random bytes -> ExactDecimal::from_raw -> format ->
        /// Float::parse -> get_inner produces equivalent Float value.
        #[test]
        fn roundtrip_from_raw_bytes(raw in any::<[u8; 32]>()) {
            let bytes = B256::from(raw);
            let float = Float::from_raw(bytes);

            // Skip values that rain-math-float considers invalid
            if float.format_with_scientific(false).is_err() {
                return Ok(());
            }

            let exact = ExactDecimal::from_raw(bytes);

            let Ok(formatted) = exact.format_decimal() else {
                return Ok(());
            };

            let roundtripped = Float::parse(formatted.clone()).map_err(|err| {
                TestCaseError::fail(format!(
                    "Float::parse failed on ExactDecimal output '{formatted}': {err}"
                ))
            })?;

            // Compare via formatted decimal strings (Float internal representation
            // may differ but the decimal value should be equivalent)
            let original_str = float.format_with_scientific(false).unwrap();
            let roundtripped_str = roundtripped.format_with_scientific(false).unwrap();
            prop_assert_eq!(original_str, roundtripped_str);
        }
    }

    #[tokio::test]
    async fn deposit_with_production_amount_succeeds() {
        let local_evm = LocalEvm::new().await.unwrap();

        // Exact amount from production failure: 1.410161147 shares with 18 decimals
        let deposit_amount = U256::from(1_410_161_147_000_000_000_u128);

        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                U256::ZERO,
            )
            .await
            .unwrap();

        let service = create_test_raindex_service(&local_evm).await;

        // This should succeed - the approve inside deposit() should cover the transferFrom amount
        service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();
    }
}
