//! Rain OrderBook V5 vault operations on Base.
//!
//! This module provides a service layer for depositing and withdrawing tokens to/from
//! Rain OrderBook vaults using the `deposit3` and `withdraw3` contract functions.
//!
//! The primary use case is USDC vault management for inventory rebalancing in the
//! market making system.
//!
//! ## V5 Float Format
//!
//! OrderBook V5 uses a custom float format (B256) for amounts. All conversions between
//! standard fixed-point amounts (U256) and the float format MUST use rain-math-float.

use alloy::primitives::{Address, B256, TxHash, U256, address};
use alloy::providers::Provider;
use async_trait::async_trait;
use rain_error_decoding::AbiDecodedErrorType;
use rain_math_float::Float;
use rust_decimal::Decimal;
use st0x_execution::FractionalShares;
use std::sync::Arc;

use crate::bindings::IOrderBookV5;
use crate::error_decoding::handle_contract_error;
use crate::lifecycle::Lifecycle;
use crate::onchain::REQUIRED_CONFIRMATIONS;
use crate::threshold::Usdc;
use crate::vault_registry::{VaultRegistry, VaultRegistryQuery};

const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
const USDC_DECIMALS: u8 = 6;

/// Vault identifier for Rain OrderBook vaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VaultId(pub(crate) B256);

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultError {
    #[error("Transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
    #[error("Contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("Contract reverted: {0}")]
    Revert(#[from] AbiDecodedErrorType),
    #[error("Float error: {0}")]
    Float(#[from] rain_math_float::FloatError),
    #[error("Decimal parse error: {0}")]
    DecimalParse(#[from] rust_decimal::Error),
    #[error("Amount cannot be zero")]
    ZeroAmount,
    #[error("Vault registry not found for aggregate {0}")]
    RegistryNotFound(String),
    #[error("Vault registry not initialized")]
    RegistryNotInitialized,
    #[error("Vault registry in failed state")]
    RegistryFailed,
    #[error("Vault not found for token {0}")]
    VaultNotFound(Address),
}

/// Service for managing Rain OrderBook vault operations.
///
/// # Example
///
/// ```ignore
/// let service = VaultService::new(provider, orderbook_address, vault_registry_query, owner);
///
/// // Lookup vault ID for a token
/// let vault_id = service.lookup_vault_id(token_address).await?;
///
/// // Deposit USDC to vault
/// let amount = U256::from(1000) * U256::from(10).pow(U256::from(6)); // 1000 USDC
/// service.deposit_usdc(vault_id, amount).await?;
///
/// // Withdraw USDC from vault
/// service.withdraw_usdc(vault_id, amount).await?;
/// ```
pub(crate) struct VaultService<P>
where
    P: Provider + Clone,
{
    orderbook: IOrderBookV5::IOrderBookV5Instance<P>,
    orderbook_address: Address,
    vault_registry_query: Arc<VaultRegistryQuery>,
    owner: Address,
    required_confirmations: u64,
}

impl<P> VaultService<P>
where
    P: Provider + Clone,
{
    pub(crate) fn new(
        provider: P,
        orderbook: Address,
        vault_registry_query: Arc<VaultRegistryQuery>,
        owner: Address,
    ) -> Self {
        Self {
            orderbook: IOrderBookV5::new(orderbook, provider),
            orderbook_address: orderbook,
            vault_registry_query,
            owner,
            required_confirmations: REQUIRED_CONFIRMATIONS,
        }
    }

    /// Sets the number of confirmations to wait after transactions.
    /// Use 1 for tests running against anvil (single-node, no sync delays).
    #[cfg(test)]
    pub(crate) fn with_required_confirmations(mut self, confirmations: u64) -> Self {
        self.required_confirmations = confirmations;
        self
    }

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
    /// Returns `VaultError::ZeroAmount` if amount is zero.
    /// Returns `VaultError::Float` if amount cannot be converted to float format.
    /// Returns `VaultError::Transaction` or `VaultError::Contract` for blockchain errors.
    pub(crate) async fn deposit(
        &self,
        token: Address,
        vault_id: VaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, VaultError> {
        if amount.is_zero() {
            return Err(VaultError::ZeroAmount);
        }

        let amount_float = Float::from_fixed_decimal(amount, decimals)?;

        let tasks = Vec::new();

        let pending = match self
            .orderbook
            .deposit3(token, vault_id.0, amount_float.get_inner(), tasks)
            .send()
            .await
        {
            Ok(pending) => pending,
            Err(error) => return Err(handle_contract_error(error).await),
        };

        let receipt = pending.get_receipt().await?;

        Ok(receipt.transaction_hash)
    }

    /// Withdraws tokens from a Rain OrderBook vault.
    ///
    /// # Parameters
    ///
    /// * `token` - ERC20 token address to withdraw
    /// * `vault_id` - Source vault identifier
    /// * `target_amount` - Target amount of tokens to withdraw (in token's base units)
    /// * `decimals` - Token decimals for float conversion
    ///
    /// # Errors
    ///
    /// Returns `VaultError::ZeroAmount` if target_amount is zero.
    /// Returns `VaultError::Float` if amount cannot be converted to float format.
    /// Returns `VaultError::Transaction` or `VaultError::Contract` for blockchain errors.
    pub(crate) async fn withdraw(
        &self,
        token: Address,
        vault_id: VaultId,
        target_amount: U256,
        decimals: u8,
    ) -> Result<TxHash, VaultError> {
        if target_amount.is_zero() {
            return Err(VaultError::ZeroAmount);
        }

        let amount_float = Float::from_fixed_decimal(target_amount, decimals)?;

        let tasks = Vec::new();

        let pending = match self
            .orderbook
            .withdraw3(token, vault_id.0, amount_float.get_inner(), tasks)
            .send()
            .await
        {
            Ok(pending) => pending,
            Err(error) => return Err(handle_contract_error(error).await),
        };

        // Wait for confirmations to ensure state propagates across load-balanced
        // RPC nodes before subsequent operations that depend on the withdrawal
        let receipt = pending
            .with_required_confirmations(self.required_confirmations)
            .get_receipt()
            .await?;

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
        vault_id: VaultId,
        amount: U256,
    ) -> Result<TxHash, VaultError> {
        self.deposit(USDC_BASE, vault_id, amount, USDC_DECIMALS)
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
        vault_id: VaultId,
        target_amount: U256,
    ) -> Result<TxHash, VaultError> {
        self.withdraw(USDC_BASE, vault_id, target_amount, USDC_DECIMALS)
            .await
    }

    /// Gets the current equity balance of a tokenized equity vault.
    pub(crate) async fn get_equity_balance(
        &self,
        owner: Address,
        token: Address,
        vault_id: VaultId,
    ) -> Result<FractionalShares, VaultError> {
        let decimal = self.get_vault_balance(owner, token, vault_id).await?;
        Ok(FractionalShares::new(decimal))
    }

    /// Gets the USDC balance of a vault on Base.
    pub(crate) async fn get_usdc_balance(
        &self,
        owner: Address,
        vault_id: VaultId,
    ) -> Result<Usdc, VaultError> {
        let decimal = self.get_vault_balance(owner, USDC_BASE, vault_id).await?;
        Ok(Usdc(decimal))
    }

    async fn get_vault_balance(
        &self,
        owner: Address,
        token: Address,
        vault_id: VaultId,
    ) -> Result<Decimal, VaultError> {
        let balance_float = self
            .orderbook
            .vaultBalance2(owner, token, vault_id.0)
            .call()
            .await?;

        float_to_decimal(balance_float)
    }
}

/// Converts a Float (bytes32) amount to Decimal.
///
/// Uses format_with_scientific(false) to avoid scientific notation
/// (e.g. "1e20") that Decimal::from_str cannot parse.
fn float_to_decimal(float: B256) -> Result<Decimal, VaultError> {
    let float = Float::from_raw(float);
    let formatted = float.format_with_scientific(false)?;
    Ok(formatted.parse::<Decimal>()?)
}

/// Abstraction for Rain OrderBook vault operations.
///
/// This trait abstracts deposit, withdraw, and vault lookup operations for the Rain OrderBook,
/// allowing different implementations (real service, mock) to be used interchangeably.
#[async_trait]
pub(crate) trait Vault: Send + Sync {
    /// Looks up the vault ID for a given token from the vault registry.
    async fn lookup_vault_id(&self, token: Address) -> Result<VaultId, VaultError>;

    /// Deposits tokens to a Rain OrderBook vault.
    async fn deposit(
        &self,
        token: Address,
        vault_id: VaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, VaultError>;

    /// Withdraws tokens from a Rain OrderBook vault.
    async fn withdraw(
        &self,
        token: Address,
        vault_id: VaultId,
        target_amount: U256,
        decimals: u8,
    ) -> Result<TxHash, VaultError>;
}

#[async_trait]
impl<P> Vault for VaultService<P>
where
    P: Provider + Clone + Send + Sync,
{
    async fn lookup_vault_id(&self, token: Address) -> Result<VaultId, VaultError> {
        let aggregate_id = VaultRegistry::aggregate_id(self.orderbook_address, self.owner);
        let Some(lifecycle) = self.vault_registry_query.load(&aggregate_id).await else {
            return Err(VaultError::RegistryNotFound(aggregate_id));
        };

        match lifecycle {
            Lifecycle::Uninitialized => Err(VaultError::RegistryNotInitialized),
            Lifecycle::Live(registry) => registry
                .vault_id_by_token(token)
                .map(VaultId)
                .ok_or(VaultError::VaultNotFound(token)),
            Lifecycle::Failed { .. } => Err(VaultError::RegistryFailed),
        }
    }

    async fn deposit(
        &self,
        token: Address,
        vault_id: VaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, VaultError> {
        Self::deposit(self, token, vault_id, amount, decimals).await
    }

    async fn withdraw(
        &self,
        token: Address,
        vault_id: VaultId,
        target_amount: U256,
        decimals: u8,
    ) -> Result<TxHash, VaultError> {
        Self::withdraw(self, token, vault_id, target_amount, decimals).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use alloy::signers::local::PrivateKeySigner;
    use alloy::transports::{RpcError, TransportErrorKind};
    use proptest::prelude::*;

    use super::*;
    use crate::bindings::{IOrderBookV5, OrderBook, TOFUTokenDecimals, TestERC20};
    use crate::vault_registry::VaultRegistryAggregate;

    /// Address where LibTOFUTokenDecimals expects the singleton contract to be deployed.
    const TOFU_DECIMALS_ADDRESS: Address = address!("0x4f1C29FAAB7EDdF8D7794695d8259996734Cc665");

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

    struct LocalEvm {
        _anvil: AnvilInstance,
        provider: LocalEvmProvider,
        signer: PrivateKeySigner,
        orderbook_address: Address,
        token_address: Address,
    }

    impl LocalEvm {
        async fn new() -> Result<Self, LocalEvmError> {
            let anvil = Anvil::new().spawn();
            let endpoint = anvil.endpoint();

            let private_key_bytes = anvil.keys()[0].to_bytes();
            let signer = PrivateKeySigner::from_bytes(&B256::from_slice(&private_key_bytes))?;

            let wallet = EthereumWallet::from(signer.clone());
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_http(endpoint.parse()?);

            Self::deploy_tofu_decimals(&provider).await?;

            let orderbook_address = Self::deploy_orderbook(&provider).await?;

            let token_address = Self::deploy_token(&provider, signer.address()).await?;

            Ok(Self {
                _anvil: anvil,
                provider,
                signer,
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
            let token_contract = TestERC20::new(token, &self.provider);

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
            let orderbook = IOrderBookV5::new(self.orderbook_address, &self.provider);

            let balance = orderbook
                .vaultBalance2(self.signer.address(), token, vault_id)
                .call()
                .await?;

            Ok(balance)
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum LocalEvmError {
        #[error("Invalid private key")]
        InvalidPrivateKey(#[from] alloy::signers::k256::ecdsa::Error),
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
    const TEST_VAULT_ID: VaultId = VaultId(b256!(
        "0x0000000000000000000000000000000000000000000000000000000000000001"
    ));

    async fn create_test_vault_service(
        provider: LocalEvmProvider,
        orderbook_address: Address,
        owner: Address,
    ) -> VaultService<LocalEvmProvider> {
        let pool = crate::test_utils::setup_test_db().await;
        let vault_registry_view_repo =
            Arc::new(SqliteViewRepository::<
                VaultRegistryAggregate,
                VaultRegistryAggregate,
            >::new(pool, "vault_registry_view".to_string()));
        let vault_registry_query: Arc<VaultRegistryQuery> =
            Arc::new(GenericQuery::new(vault_registry_view_repo));

        VaultService::new(provider, orderbook_address, vault_registry_query, owner)
    }

    #[tokio::test]
    async fn deposit_rejects_zero_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let service = create_test_vault_service(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
            local_evm.signer.address(),
        )
        .await;

        let result = service
            .deposit(
                local_evm.token_address,
                TEST_VAULT_ID,
                U256::ZERO,
                TEST_TOKEN_DECIMALS,
            )
            .await;

        assert!(matches!(result.unwrap_err(), VaultError::ZeroAmount));
    }

    #[tokio::test]
    async fn test_deposit_succeeds_with_deployed_contract() {
        let local_evm = LocalEvm::new().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let vault_id = TEST_VAULT_ID;

        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();

        let service = create_test_vault_service(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
            local_evm.signer.address(),
        )
        .await;

        let vault_balance_before = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        assert!(vault_balance_before.is_zero());

        let tx_hash = service
            .deposit(
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
    }

    #[tokio::test]
    async fn withdraw_rejects_zero_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let service = create_test_vault_service(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
            local_evm.signer.address(),
        )
        .await;

        let result = service
            .withdraw(
                local_evm.token_address,
                TEST_VAULT_ID,
                U256::ZERO,
                TEST_TOKEN_DECIMALS,
            )
            .await;

        assert!(matches!(result.unwrap_err(), VaultError::ZeroAmount));
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

        let service = create_test_vault_service(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
            local_evm.signer.address(),
        )
        .await
        .with_required_confirmations(1);

        service
            .deposit(
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
        let service = create_test_vault_service(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
            local_evm.signer.address(),
        )
        .await;

        let balance = service
            .get_equity_balance(
                local_evm.signer.address(),
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

        let service = create_test_vault_service(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
            local_evm.signer.address(),
        )
        .await;

        service
            .deposit(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        let balance = service
            .get_equity_balance(
                local_evm.signer.address(),
                local_evm.token_address,
                TEST_VAULT_ID,
            )
            .await
            .unwrap();

        let expected = FractionalShares::new(Decimal::from(1000));
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

        let service = create_test_vault_service(
            local_evm.provider.clone(),
            local_evm.orderbook_address,
            local_evm.signer.address(),
        )
        .await
        .with_required_confirmations(1);

        service
            .deposit(
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
            .get_equity_balance(
                local_evm.signer.address(),
                local_evm.token_address,
                TEST_VAULT_ID,
            )
            .await
            .unwrap();

        let expected = FractionalShares::new(Decimal::from(700));
        assert_eq!(balance, expected, "Expected 700 shares but got {balance:?}");
    }

    /// Values with large exponents produce scientific notation from
    /// Float::format(), which Decimal::from_str cannot parse.
    /// format_with_scientific(false) prevents this.
    #[test]
    fn large_exponent_does_not_produce_scientific_notation() {
        let float = Float::parse("100000000000000000000".to_string())
            .expect("valid Float from decimal string");

        let decimal = float_to_decimal(float.get_inner()).unwrap();
        assert_eq!(decimal, Decimal::from(100_000_000_000_000_000_000_u128));
    }

    proptest! {
        /// Roundtrip: decimal string -> Float::parse -> float_to_decimal ->
        /// Decimal matches the original string.
        #[test]
        fn roundtrip_from_decimal_string(
            integer in 0u64..1_000_000_000,
            fraction in 0u32..1_000_000,
        ) {
            let input = format!("{integer}.{fraction:06}");
            let float = Float::parse(input.clone()).map_err(|err| {
                TestCaseError::Reject(format!("Float::parse rejected {input}: {err}").into())
            })?;

            let decimal = float_to_decimal(float.get_inner()).map_err(|err| {
                TestCaseError::fail(format!(
                    "float_to_decimal failed for {input}: {err}"
                ))
            })?;

            // Re-parse original to compare as Decimal (avoids string format differences)
            let expected: Decimal = input.parse().unwrap();
            prop_assert_eq!(decimal, expected);
        }

        /// Roundtrip: random bytes -> float_to_decimal -> Float::parse ->
        /// get_inner produces equivalent Float value.
        #[test]
        fn roundtrip_from_raw_bytes(raw in any::<[u8; 32]>()) {
            let bytes = B256::from(raw);
            let float = Float::from_raw(bytes);

            // Skip values that rain-math-float considers invalid
            if float.format_with_scientific(false).is_err() {
                return Ok(());
            }

            let Ok(decimal) = float_to_decimal(bytes) else {
                return Ok(());
            };

            let roundtripped = Float::parse(decimal.to_string()).map_err(|err| {
                TestCaseError::fail(format!(
                    "Float::parse failed on float_to_decimal output '{decimal}': {err}"
                ))
            })?;

            // Compare via formatted decimal strings (Float internal representation
            // may differ but the decimal value should be equivalent)
            let original_str = float.format_with_scientific(false).unwrap();
            let roundtripped_str = roundtripped.format_with_scientific(false).unwrap();
            prop_assert_eq!(original_str, roundtripped_str);
        }
    }
}
