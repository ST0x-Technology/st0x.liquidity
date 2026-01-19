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
    #[error("Contract reverted: {0}")]
    Revert(#[from] AbiDecodedErrorType),
    #[error("Float error: {0}")]
    Float(#[from] rain_math_float::FloatError),
    #[error("Amount cannot be zero")]
    ZeroAmount,
    #[error("Insufficient vault balance: requested {requested}, available {available}")]
    InsufficientBalance { requested: U256, available: U256 },
    #[error("WithdrawV2 event not found in transaction receipt")]
    WithdrawEventNotFound,
    #[error("No contract code at orderbook address {0}")]
    NoContractCode(Address),
    #[error("Address {0} is not a valid OrderBook V5 contract")]
    InvalidOrderbook(Address),
    #[error("RPC error: {0}")]
    Rpc(#[from] alloy::transports::TransportError),
}

/// Service for managing Rain OrderBook vault operations.
///
/// # Example
///
/// ```ignore
/// let account = Evm::new(provider, signer);
/// let service = VaultService::new(account, orderbook_address, usdc_address);
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
    usdc_address: Address,
}

impl<P, S> VaultService<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    /// Creates a new VaultService after verifying the orderbook is a valid V5 contract.
    ///
    /// # Errors
    ///
    /// Returns `VaultError::NoContractCode` if there's no code at the address.
    /// Returns `VaultError::InvalidOrderbook` if the contract doesn't implement V5 interface.
    pub(crate) async fn new(
        account: Evm<P, S>,
        orderbook: Address,
        usdc_address: Address,
    ) -> Result<Self, VaultError> {
        let code = account.provider.get_code_at(orderbook).await?;

        if code.is_empty() {
            return Err(VaultError::NoContractCode(orderbook));
        }

        // Verify it's a V5 orderbook by checking for deposit3 with V5 signature
        // deposit3(address,bytes32,bytes32,TaskV2[]) selector = 0x7921a962
        let deposit3_selector: [u8; 4] = [0x79, 0x21, 0xa9, 0x62];
        if !code.as_ref().windows(4).any(|w| w == deposit3_selector) {
            return Err(VaultError::InvalidOrderbook(orderbook));
        }

        debug!(%orderbook, "Verified OrderBook V5 contract");

        Ok(Self {
            account,
            orderbook,
            usdc_address,
        })
    }

    /// Wraps the service in an Arc for shared ownership.
    pub(crate) fn arc(self) -> std::sync::Arc<Self> {
        std::sync::Arc::new(self)
    }

    /// Creates a VaultService without validation.
    ///
    /// For tests that don't deploy an actual orderbook contract.
    #[cfg(test)]
    pub(crate) fn new_unchecked(
        account: Evm<P, S>,
        orderbook: Address,
        usdc_address: Address,
    ) -> Self {
        Self {
            account,
            orderbook,
            usdc_address,
        }
    }

    async fn ensure_token_approval(&self, token: Address, amount: U256) -> Result<(), VaultError> {
        let owner = self.account.signer.address();
        let token_contract = IERC20::new(token, self.account.provider.clone());

        let allowance = token_contract
            .allowance(owner, self.orderbook)
            .call()
            .await?;

        if allowance < amount {
            debug!(%allowance, %amount, "Approving token spend");
            let approval_receipt = token_contract
                .approve(self.orderbook, amount)
                .send()
                .await?
                .get_receipt()
                .await?;
            info!(tx = %approval_receipt.transaction_hash, "Approval confirmed");
        }

        Ok(())
    }

    /// Deposits tokens to a Rain OrderBook vault.
    ///
    /// Handles ERC20 approval automatically if the current allowance is insufficient.
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

        debug!(%token, ?vault_id, %amount, decimals, "Starting deposit");

        self.ensure_token_approval(token, amount).await?;

        let amount_float = Float::from_fixed_decimal(amount, decimals)?;
        let contract = IOrderBookV5::new(self.orderbook, self.account.provider.clone());

        let pending = match contract
            .deposit3(token, vault_id.0, amount_float.get_inner(), Vec::new())
            .send()
            .await
        {
            Ok(pending) => pending,
            Err(e) => return Err(handle_contract_error(e).await),
        };

        let receipt = pending.get_receipt().await?;
        info!(tx = %receipt.transaction_hash, %amount, "Deposit confirmed");

        Ok(receipt.transaction_hash)
    }

    fn parse_withdraw_event(
        receipt: &alloy::rpc::types::TransactionReceipt,
    ) -> Result<U256, VaultError> {
        let withdraw_event = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| IOrderBookV5::WithdrawV2::decode_log(log.as_ref()).ok())
            .ok_or(VaultError::WithdrawEventNotFound)?;

        Ok(withdraw_event.withdrawAmountUint256)
    }

    /// Withdraws tokens from a Rain OrderBook vault.
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

        debug!(%token, ?vault_id, %target_amount, decimals, "Starting withdraw");

        let amount_float = Float::from_fixed_decimal(target_amount, decimals)?;
        let contract = IOrderBookV5::new(self.orderbook, self.account.provider.clone());

        let pending = match contract
            .withdraw3(token, vault_id.0, amount_float.get_inner(), Vec::new())
            .send()
            .await
        {
            Ok(pending) => pending,
            Err(e) => return Err(handle_contract_error(e).await),
        };

        let receipt = pending.get_receipt().await?;
        let actual_withdrawn = Self::parse_withdraw_event(&receipt)?;

        if actual_withdrawn < target_amount {
            return Err(VaultError::InsufficientBalance {
                requested: target_amount,
                available: actual_withdrawn,
            });
        }

        info!(tx = %receipt.transaction_hash, %actual_withdrawn, "Withdraw confirmed");

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
        self.deposit(self.usdc_address, vault_id, amount, USDC_DECIMALS)
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
        self.withdraw(self.usdc_address, vault_id, target_amount, USDC_DECIMALS)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::network::{Ethereum, EthereumWallet};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, address, b256};
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

        async fn get_allowance(
            &self,
            token: Address,
            spender: Address,
        ) -> Result<U256, LocalEvmError> {
            let token_contract = TestERC20::new(token, &self.provider);
            let allowance = token_contract
                .allowance(self.signer.address(), spender)
                .call()
                .await?;
            Ok(allowance)
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

        let service = VaultService::new(
            local_evm.evm(),
            local_evm.orderbook_address,
            local_evm.token_address,
        )
        .await
        .unwrap();

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

        let service = VaultService::new(
            local_evm.evm(),
            local_evm.orderbook_address,
            local_evm.token_address,
        )
        .await
        .unwrap();

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

        let service = VaultService::new(
            local_evm.evm(),
            local_evm.orderbook_address,
            local_evm.token_address,
        )
        .await
        .unwrap();

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

        let service = VaultService::new(
            local_evm.evm(),
            local_evm.orderbook_address,
            local_evm.token_address,
        )
        .await
        .unwrap();

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

    #[tokio::test]
    async fn deposit_auto_approves_when_allowance_is_zero() {
        let local_evm = LocalEvm::new().await.unwrap();
        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));

        // Verify starting with zero allowance
        let allowance_before = local_evm
            .get_allowance(local_evm.token_address, local_evm.orderbook_address)
            .await
            .unwrap();
        assert!(allowance_before.is_zero());

        let service = VaultService::new(
            local_evm.evm(),
            local_evm.orderbook_address,
            local_evm.token_address,
        )
        .await
        .unwrap();

        // Deposit should auto-approve and succeed
        let tx_hash = service
            .deposit(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        assert!(!tx_hash.is_zero());

        // Allowance should be zero after deposit consumed it
        let allowance_after = local_evm
            .get_allowance(local_evm.token_address, local_evm.orderbook_address)
            .await
            .unwrap();
        assert!(allowance_after.is_zero());
    }

    #[tokio::test]
    async fn deposit_skips_approval_when_allowance_is_sufficient() {
        let local_evm = LocalEvm::new().await.unwrap();
        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let pre_approval = deposit_amount * U256::from(2);

        // Pre-approve more than needed
        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                pre_approval,
            )
            .await
            .unwrap();

        let allowance_before = local_evm
            .get_allowance(local_evm.token_address, local_evm.orderbook_address)
            .await
            .unwrap();
        assert_eq!(allowance_before, pre_approval);

        let service = VaultService::new(
            local_evm.evm(),
            local_evm.orderbook_address,
            local_evm.token_address,
        )
        .await
        .unwrap();

        // Deposit should succeed without additional approval
        service
            .deposit(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        // Allowance should be reduced by deposit amount, not reset
        let allowance_after = local_evm
            .get_allowance(local_evm.token_address, local_evm.orderbook_address)
            .await
            .unwrap();
        assert_eq!(allowance_after, pre_approval - deposit_amount);
    }

    #[tokio::test]
    async fn deposit_auto_approves_when_allowance_is_insufficient() {
        let local_evm = LocalEvm::new().await.unwrap();
        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let insufficient_approval = deposit_amount / U256::from(2);

        // Pre-approve less than needed
        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                insufficient_approval,
            )
            .await
            .unwrap();

        let allowance_before = local_evm
            .get_allowance(local_evm.token_address, local_evm.orderbook_address)
            .await
            .unwrap();
        assert_eq!(allowance_before, insufficient_approval);

        let service = VaultService::new(
            local_evm.evm(),
            local_evm.orderbook_address,
            local_evm.token_address,
        )
        .await
        .unwrap();

        // Deposit should auto-approve the exact amount needed and succeed
        service
            .deposit(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        // Allowance should be zero after deposit consumed the exact approval
        let allowance_after = local_evm
            .get_allowance(local_evm.token_address, local_evm.orderbook_address)
            .await
            .unwrap();
        assert!(allowance_after.is_zero());
    }
}
