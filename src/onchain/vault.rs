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

use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::Signer;
use alloy::sol_types::SolEvent;
use rain_error_decoding::AbiDecodedErrorType;
use rain_math_float::Float;
use tracing::{debug, info};

use crate::bindings::{IERC20, IOrderBookV5};
use crate::cctp::Evm;
use crate::error_decoding::handle_contract_error;

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
    #[error("Float error: {0}")]
    Float(#[from] rain_math_float::FloatError),
    #[error("Amount cannot be zero")]
    ZeroAmount,
}

/// Service for managing Rain OrderBook vault operations.
///
/// # Example
///
/// ```ignore
/// let account = Evm::new(provider, signer);
/// let service = VaultService::new(account, orderbook_address);
///
/// // Deposit USDC to vault
/// let vault_id = VaultId(U256::from(1));
/// let amount = U256::from(1000) * U256::from(10).pow(U256::from(6)); // 1000 USDC
/// service.deposit_usdc(vault_id, amount).await?;
///
/// // Withdraw USDC from vault
/// service.withdraw_usdc(vault_id, amount).await?;
/// ```
pub(crate) struct VaultService<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    account: Evm<P, S>,
    orderbook: Address,
}

impl<P, S> VaultService<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    pub(crate) fn new(account: Evm<P, S>, orderbook: Address) -> Self {
        Self { account, orderbook }
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

        let contract = IOrderBookV5::new(self.orderbook, self.account.provider.clone());

        let tasks = Vec::new();

        let receipt = contract
            .deposit3(token, vault_id.0, amount_float.get_inner(), tasks)
            .send()
            .await?
            .get_receipt()
            .await?;

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

        let contract = IOrderBookV5::new(self.orderbook, self.account.provider.clone());

        let tasks = Vec::new();

        let receipt = contract
            .withdraw3(token, vault_id.0, amount_float.get_inner(), tasks)
            .send()
            .await?
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, b256};
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use alloy::signers::local::PrivateKeySigner;
    use alloy::transports::{RpcError, TransportErrorKind};

    use crate::bindings::{IOrderBookV5, OrderBook, TOFUTokenDecimals, TestERC20};

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
            let signer = PrivateKeySigner::from_bytes(&B256::from_slice(&private_key_bytes))
                .map_err(|e| LocalEvmError::InvalidPrivateKey(e.into()))?;

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

        fn evm(&self) -> Evm<LocalEvmProvider, PrivateKeySigner> {
            let dummy_address = address!("0x0000000000000000000000000000000000000000");
            Evm::new(
                self.provider.clone(),
                self.signer.clone(),
                dummy_address,
                dummy_address,
                dummy_address,
            )
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum LocalEvmError {
        #[error("Invalid private key")]
        InvalidPrivateKey(#[source] alloy::signers::Error),
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

    #[tokio::test]
    async fn deposit_rejects_zero_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let service = VaultService::new(local_evm.evm(), local_evm.orderbook_address);

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

        let service = VaultService::new(local_evm.evm(), local_evm.orderbook_address);

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

        let service = VaultService::new(local_evm.evm(), local_evm.orderbook_address);

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

        let service = VaultService::new(local_evm.evm(), local_evm.orderbook_address);

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
}
