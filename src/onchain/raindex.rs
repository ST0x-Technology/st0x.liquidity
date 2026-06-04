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
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, TransactionReceipt};
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use rain_math_float::Float;
use tracing::{debug, info};

use st0x_evm::{Evm, EvmError, IntoErrorRegistry, OpenChainErrorRegistry, Wallet};
use st0x_execution::FractionalShares;
use st0x_finance::Usdc;

use crate::bindings::{IERC20, IOrderBookV6};

pub(crate) const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
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
    #[error("RPC transport error: {0}")]
    RpcTransport(#[from] RpcError<TransportErrorKind>),
    #[error("ABI decode error: {0}")]
    SolType(#[from] alloy::sol_types::Error),
    /// A withdrawal scan could not confirm presence or absence: the queried node
    /// is not confirmations-deep past `from_block`, so an empty result may be RPC
    /// lag rather than a true absence. Retryable -- the caller must NOT re-execute
    /// the irreversible withdraw on this.
    #[error("withdrawal scan inconclusive: node not caught up past block {from_block}")]
    ScanInconclusive { from_block: u64 },
}

impl RaindexError {
    /// `true` if this error reports that a submitted transaction was dropped from
    /// the mempool and will never mine -- a terminal failure, distinct from a
    /// still-pending transaction that simply has not confirmed yet.
    pub(crate) fn is_transaction_dropped(&self) -> bool {
        match self {
            Self::Evm(EvmError::TransactionDropped { .. }) => true,
            Self::Evm(_)
            | Self::Contract(_)
            | Self::Float(_)
            | Self::ZeroAmount
            | Self::RpcTransport(_)
            | Self::SolType(_)
            | Self::ScanInconclusive { .. } => false,
        }
    }
}

/// Number of `eth_getLogs` scans that must agree the effect is absent before a
/// resume re-executes an irreversible withdraw. Defends against a single
/// load-balanced RPC node lagging and returning a false-empty result.
const SCAN_ATTEMPTS: u32 = 5;

/// Backoff between scan retries; different load-balanced nodes may answer each.
const SCAN_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(150);

/// Blocks the chain head must be past `from_block` before an empty scan is
/// trusted as a true absence (the on-chain effect lands at/after `from_block`).
const SCAN_FINALITY_MARGIN: u64 = 2;

/// Service for managing Rain OrderBook vault operations.
///
/// Parameterized by [`Evm`] for read operations (balance queries, withdrawal
/// scans) and additionally requires [`Wallet`] for write operations
/// (deposits, withdrawals).
///
/// # Example
///
/// ```ignore
/// let service = RaindexService::new(wallet, orderbook_address, owner);
///
/// // Submit a USDC deposit to the vault, then confirm it (needs Wallet)
/// let amount = U256::from(1000) * U256::from(10).pow(U256::from(6)); // 1000 USDC
/// let deposit_tx = service.submit_deposit_usdc(vault_id, amount).await?;
/// service.confirm_tx(deposit_tx).await?;
/// ```
pub(crate) struct RaindexService<E: Evm> {
    orderbook_address: Address,
    evm: E,
    owner: Address,
}

impl<E: Evm> RaindexService<E> {
    pub(crate) fn new(evm: E, orderbook: Address, owner: Address) -> Self {
        Self {
            orderbook_address: orderbook,
            evm,
            owner,
        }
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
            debug!(target: "orderbook", %token, %amount, %current_allowance, "Sufficient allowance, skipping approve");
            return Ok(());
        }

        debug!(target: "orderbook", %token, %amount, %current_allowance, spender = %self.orderbook_address, "Sending ERC20 approve");

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

        info!(target: "orderbook", tx_hash = %receipt.transaction_hash, %token, "Approve confirmed");
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

        debug!(target: "orderbook", %token, ?vault_id, %amount, "Sending deposit4");

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

        info!(target: "orderbook", tx_hash = %receipt.transaction_hash, %token, %amount, "deposit4 confirmed");
        Ok(receipt.transaction_hash)
    }

    /// Submit a deposit4 transaction without waiting for confirmation.
    async fn submit_deposit4_to_vault(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        let amount_float = Float::from_fixed_decimal(amount, decimals)?;

        debug!(target: "orderbook", %token, ?vault_id, %amount, "Sending deposit4");

        let calldata = IOrderBookV6::deposit4Call {
            token,
            vaultId: vault_id.0,
            depositAmount: amount_float.get_inner(),
            tasks: Vec::new(),
        };

        let tx_hash = self
            .evm
            .submit_pending(self.orderbook_address, calldata, "deposit4 to vault")
            .await?;

        info!(target: "orderbook", %tx_hash, %token, %amount, "deposit4 submitted");
        Ok(tx_hash)
    }

    /// Submits a USDC deposit to a Rain OrderBook vault on Base WITHOUT waiting
    /// for confirmation, returning the broadcast tx hash.
    ///
    /// Used by the crash-safe deposit path: the hash is persisted as
    /// `InitiateDeposit` before confirmation, so a crash during the confirmation
    /// wait resumes from `DepositInitiated` (re-verifying the recorded tx via
    /// `confirm_tx`) instead of re-submitting a second deposit.
    pub(crate) async fn submit_deposit_usdc(
        &self,
        vault_id: RaindexVaultId,
        amount: U256,
    ) -> Result<TxHash, RaindexError> {
        // `submit_deposit` handles the ERC20 approval before submitting deposit4
        // (unlike the bare `submit_deposit4_to_vault`), then returns without
        // confirming.
        self.submit_deposit(USDC_BASE, vault_id, amount, USDC_DECIMALS)
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
        Ok(Usdc::new(exact))
    }

    /// Scans for a `WithdrawV2` event from this owner's vault strictly after
    /// `from_block`, returning the most recent match's `(tx_hash, withdrawn_amount)`.
    ///
    /// Crash-safe withdrawal recovery: a transfer records the chain head before
    /// the on-chain withdraw, so on resume this detects an already-submitted
    /// withdrawal and the caller adopts it instead of re-issuing (which would
    /// double-spend the vault). The head is captured before submitting, so this
    /// transfer's withdraw lands strictly after `from_block`; the scan excludes the
    /// `from_block` block itself so an earlier withdrawal from the same vault (which
    /// matches on the same `(sender, token, vaultId)`) is never adopted. Matches on
    /// `(sender == self.owner, token, vaultId)` -- never on amount, so a partial
    /// fill is still detected -- and returns the actual on-chain
    /// `withdrawAmountUint256` so the caller can reconcile a partial withdrawal
    /// rather than laundering it into a full-amount burn.
    ///
    /// Returns `Ok(None)` ONLY when the queried node is confirmations-deep past
    /// `from_block` and repeated scans agree the effect is absent. A node that may
    /// be lagging (the dRPC load-balancing hazard AGENTS.md warns about) yields a
    /// retryable [`RaindexError::ScanInconclusive`] instead, so the caller never
    /// re-executes the irreversible withdraw off a single stale empty `eth_getLogs`.
    pub(crate) async fn find_recent_withdrawal(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        from_block: u64,
    ) -> Result<Option<(TxHash, U256)>, RaindexError> {
        let RaindexVaultId(vault_id) = vault_id;
        let filter = Filter::new()
            .from_block(from_block)
            .address(self.orderbook_address)
            .event_signature(IOrderBookV6::WithdrawV2::SIGNATURE_HASH);

        for attempt in 1..=SCAN_ATTEMPTS {
            let logs = self.evm.provider().get_logs(&filter).await?;

            // get_logs returns ascending block order; iterate newest-first so the
            // most recent matching withdrawal wins -- under single-in-flight that
            // is this transfer's withdrawal.
            for log in logs.iter().rev() {
                let decoded = log.log_decode::<IOrderBookV6::WithdrawV2>()?;
                let event = decoded.data();

                if event.sender == self.owner
                    && event.token == token
                    && event.vaultId == vault_id
                    && log.block_number.is_some_and(|block| block > from_block)
                    && let Some(tx_hash) = log.transaction_hash
                {
                    debug!(target: "orderbook", %tx_hash, from_block, "Found existing withdrawal during resume");
                    return Ok(Some((tx_hash, event.withdrawAmountUint256)));
                }
            }

            // No match on this query. A single empty eth_getLogs from a
            // load-balanced node is not authoritative (the dRPC lag hazard, see
            // src/onchain/clear.rs). Only conclude a true absence once the head is
            // confirmations-deep past from_block AND repeated scans agree; else
            // retry, and if still inconclusive return a retryable error so the
            // caller never re-withdraws off a stale empty result.
            let head = self.evm.provider().get_block_number().await?;
            let caught_up = head >= from_block.saturating_add(SCAN_FINALITY_MARGIN);

            if caught_up && attempt == SCAN_ATTEMPTS {
                return Ok(None);
            }

            if attempt < SCAN_ATTEMPTS {
                tokio::time::sleep(SCAN_RETRY_BACKOFF).await;
            }
        }

        Err(RaindexError::ScanInconclusive { from_block })
    }

    /// Returns the current chain head. Captured before submitting an on-chain
    /// action so a later [`find_recent_withdrawal`](Self::find_recent_withdrawal)
    /// scan can bound its search to blocks at or after the action.
    pub(crate) async fn current_block(&self) -> Result<u64, RaindexError> {
        Ok(self.evm.provider().get_block_number().await?)
    }

    async fn get_vault_balance<Registry: IntoErrorRegistry>(
        &self,
        owner: Address,
        token: Address,
        vault_id: RaindexVaultId,
    ) -> Result<Float, RaindexError> {
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

        Ok(Float::from_raw(balance_float))
    }
}

/// Abstraction for Raindex (Rain OrderBook) operations.
///
/// This trait abstracts deposit and withdraw operations for Raindex,
/// allowing different implementations (real service, mock) to be used interchangeably.
#[async_trait]
pub(crate) trait Raindex: Send + Sync {
    /// Withdraws tokens from a Rain OrderBook vault (atomic submit + confirm).
    async fn withdraw(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        target_amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError>;

    /// Submit a vault deposit without waiting for confirmation.
    ///
    /// Handles approval if needed, submits the deposit4 transaction,
    /// and returns the tx hash immediately. Use
    /// [`confirm_tx`](Raindex::confirm_tx) to wait for confirmation.
    async fn submit_deposit(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError>;

    /// Submit a vault withdrawal without waiting for confirmation.
    ///
    /// Returns the tx hash immediately. Use
    /// [`confirm_tx`](Raindex::confirm_tx) to wait for confirmation.
    async fn submit_withdraw(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        target_amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError>;

    /// Wait for a previously submitted transaction to be confirmed.
    async fn confirm_tx(&self, tx_hash: TxHash) -> Result<(), RaindexError> {
        self.confirm_tx_receipt(tx_hash).await.map(|_| ())
    }

    /// Wait for a previously submitted transaction to be confirmed and return the receipt.
    async fn confirm_tx_receipt(&self, tx_hash: TxHash)
    -> Result<TransactionReceipt, RaindexError>;
}

#[async_trait]
impl<W: Wallet> Raindex for RaindexService<W> {
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

        info!(target: "orderbook", tx_hash = %receipt.transaction_hash, %token, %target_amount, "withdraw4 confirmed");
        Ok(receipt.transaction_hash)
    }

    async fn submit_deposit(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        if amount.is_zero() {
            return Err(RaindexError::ZeroAmount);
        }

        self.approve_for_orderbook::<OpenChainErrorRegistry>(token, amount)
            .await?;

        self.submit_deposit4_to_vault(token, vault_id, amount, decimals)
            .await
    }

    async fn submit_withdraw(
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

        let tx_hash = self
            .evm
            .submit_pending(
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

        info!(target: "orderbook", %tx_hash, %token, %target_amount, "withdraw4 submitted");
        Ok(tx_hash)
    }

    async fn confirm_tx_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<TransactionReceipt, RaindexError> {
        let receipt = self.evm.confirm::<OpenChainErrorRegistry>(tx_hash).await?;

        info!(target: "orderbook", tx_hash = %receipt.transaction_hash, "Transaction confirmed");
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::Ethereum;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::Log as PrimitiveLog;
    use alloy::primitives::{B256, b256};
    use alloy::providers::Provider;
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
    };
    use alloy::providers::{Identity, ProviderBuilder, RootProvider};
    use alloy::rpc::types::Log;
    use alloy::transports::{RpcError, TransportErrorKind};
    use proptest::prelude::*;
    use tracing_test::traced_test;

    use alloy::providers::mock::Asserter;
    use serde_json::json;

    use st0x_evm::NoOpErrorRegistry;
    use st0x_evm::ReadOnlyEvm;
    use st0x_evm::Wallet;
    use st0x_evm::local::RawPrivateKeyWallet;

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

            Self::deploy_tofu_decimals(wallet.signing_provider()).await?;

            let orderbook_address = Self::deploy_orderbook(wallet.signing_provider()).await?;

            let token_address =
                Self::deploy_token(wallet.signing_provider(), wallet.address()).await?;

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
            let token_contract = TestERC20::new(token, self.wallet.signing_provider());

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

    fn create_test_raindex_service(
        local_evm: &LocalEvm,
    ) -> RaindexService<RawPrivateKeyWallet<BaseProvider>> {
        let base_provider =
            ProviderBuilder::new().connect_http(local_evm.anvil.endpoint().parse().unwrap());

        let wallet = RawPrivateKeyWallet::new(&local_evm.private_key, base_provider, 1).unwrap();

        let owner = wallet.address();

        RaindexService::new(wallet, local_evm.orderbook_address, owner)
    }

    #[tokio::test]
    async fn find_recent_withdrawal_is_inconclusive_when_node_lags() {
        let asserter = Asserter::new();
        let from_block = 100u64;
        // Every scan attempt sees an empty get_logs and a head BELOW from_block --
        // a lagging load-balanced node. The scan must NOT report absence (which
        // would let the caller re-withdraw and double-spend); it must surface a
        // retryable error so the resume re-runs instead.
        for _ in 0..SCAN_ATTEMPTS {
            asserter.push_success(&json!([]));
            asserter.push_success(&json!("0x32")); // head = 50 < from_block
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let service = RaindexService::new(ReadOnlyEvm::new(provider), Address::ZERO, Address::ZERO);

        let error = service
            .find_recent_withdrawal(USDC_BASE, TEST_VAULT_ID, from_block)
            .await
            .unwrap_err();

        assert!(
            matches!(error, RaindexError::ScanInconclusive { from_block: fb } if fb == from_block),
            "lagging node must yield retryable ScanInconclusive, got: {error:?}",
        );
    }

    #[tokio::test]
    async fn find_recent_withdrawal_is_none_when_caught_up_and_absent() {
        let asserter = Asserter::new();
        let from_block = 100u64;
        // Empty get_logs corroborated across every attempt, with a head well past
        // from_block: the withdrawal is genuinely absent, so re-issuing is safe.
        for _ in 0..SCAN_ATTEMPTS {
            asserter.push_success(&json!([]));
            asserter.push_success(&json!("0x200")); // head = 512 >> from_block
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let service = RaindexService::new(ReadOnlyEvm::new(provider), Address::ZERO, Address::ZERO);

        let result = service
            .find_recent_withdrawal(USDC_BASE, TEST_VAULT_ID, from_block)
            .await
            .unwrap();

        assert_eq!(result, None);
    }

    /// A `WithdrawV2` from this owner's vault, mined at `block_number`, that
    /// `find_recent_withdrawal` will see for `(USDC_BASE, TEST_VAULT_ID)`.
    fn withdraw_log(block_number: u64, withdraw_amount: U256) -> Log {
        let event = IOrderBookV6::WithdrawV2 {
            sender: Address::ZERO,
            token: USDC_BASE,
            vaultId: TEST_VAULT_ID.0,
            targetAmount: B256::ZERO,
            withdrawAmount: B256::ZERO,
            withdrawAmountUint256: withdraw_amount,
        };

        Log {
            inner: PrimitiveLog {
                address: Address::ZERO,
                data: event.encode_log_data(),
            },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(b256!(
                "0x00000000000000000000000000000000000000000000000000000000000000aa"
            )),
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    #[tokio::test]
    async fn find_recent_withdrawal_excludes_match_at_from_block() {
        let asserter = Asserter::new();
        let from_block = 100u64;
        // A matching withdrawal mined AT from_block belongs to an earlier transfer:
        // from_block is the head captured *before* this transfer's withdraw, so this
        // transfer's withdraw can only land strictly after it. Adopting the at-block
        // log would make the resume skip a withdraw it never issued.
        let stale = json!([withdraw_log(from_block, U256::from(7u64))]);
        for _ in 0..SCAN_ATTEMPTS {
            asserter.push_success(&stale);
            asserter.push_success(&json!("0x200")); // head = 512 >> from_block
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let service = RaindexService::new(ReadOnlyEvm::new(provider), Address::ZERO, Address::ZERO);

        let result = service
            .find_recent_withdrawal(USDC_BASE, TEST_VAULT_ID, from_block)
            .await
            .unwrap();

        assert_eq!(
            result, None,
            "a withdrawal mined at from_block must be excluded (strictly-after semantics)",
        );
    }

    #[tokio::test]
    async fn find_recent_withdrawal_adopts_match_after_from_block() {
        let asserter = Asserter::new();
        let from_block = 100u64;
        let amount = U256::from(7u64);
        let log = withdraw_log(from_block + 1, amount);
        let expected_tx = log.transaction_hash.unwrap();
        // The very first scan finds the strictly-after match and adopts it.
        asserter.push_success(&json!([log]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let service = RaindexService::new(ReadOnlyEvm::new(provider), Address::ZERO, Address::ZERO);

        let result = service
            .find_recent_withdrawal(USDC_BASE, TEST_VAULT_ID, from_block)
            .await
            .unwrap();

        assert_eq!(result, Some((expected_tx, amount)));
    }

    #[tokio::test]
    async fn deposit_rejects_zero_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let service = create_test_raindex_service(&local_evm);

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

        let service = create_test_raindex_service(&local_evm);

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

        let service = create_test_raindex_service(&local_evm);

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

        let service = create_test_raindex_service(&local_evm);

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
    async fn find_recent_withdrawal_returns_tx_and_amount_of_real_withdrawal() {
        let local_evm = LocalEvm::new().await.unwrap();
        let service = create_test_raindex_service(&local_evm);

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();
        service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        let from_block = service.current_block().await.unwrap();

        let withdraw_amount = U256::from(400) * U256::from(10).pow(U256::from(18));
        let withdraw_tx = service
            .withdraw(
                local_evm.token_address,
                TEST_VAULT_ID,
                withdraw_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        let found = service
            .find_recent_withdrawal(local_evm.token_address, TEST_VAULT_ID, from_block)
            .await
            .unwrap();

        assert_eq!(
            found,
            Some((withdraw_tx, withdraw_amount)),
            "scan must return the real withdrawal's tx and actual on-chain withdrawn amount",
        );
    }

    #[tokio::test]
    async fn find_recent_withdrawal_returns_none_for_non_matching_vault_or_token() {
        let local_evm = LocalEvm::new().await.unwrap();
        let service = create_test_raindex_service(&local_evm);

        // Capture from_block BEFORE the approve/deposit/withdraw txs so those three
        // blocks advance the head past from_block + SCAN_FINALITY_MARGIN, letting
        // the non-matching scans below conclude a true absence (None) rather than
        // ScanInconclusive.
        let from_block = service.current_block().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();
        service
            .deposit::<NoOpErrorRegistry>(
                local_evm.token_address,
                TEST_VAULT_ID,
                deposit_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        let withdraw_amount = U256::from(400) * U256::from(10).pow(U256::from(18));
        service
            .withdraw(
                local_evm.token_address,
                TEST_VAULT_ID,
                withdraw_amount,
                TEST_TOKEN_DECIMALS,
            )
            .await
            .unwrap();

        let other_vault = RaindexVaultId(b256!(
            "0x0000000000000000000000000000000000000000000000000000000000000002"
        ));
        assert_eq!(
            service
                .find_recent_withdrawal(local_evm.token_address, other_vault, from_block)
                .await
                .unwrap(),
            None,
            "a withdrawal on a different vault must not be adopted",
        );
        assert_eq!(
            service
                .find_recent_withdrawal(USDC_BASE, TEST_VAULT_ID, from_block)
                .await
                .unwrap(),
            None,
            "a withdrawal of a different token must not be adopted",
        );
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

        let service = create_test_raindex_service(&local_evm);

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
        let service = create_test_raindex_service(&local_evm);

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

        let service = create_test_raindex_service(&local_evm);

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

        let expected = FractionalShares::new(Float::parse("1000".to_string()).unwrap());
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

        let service = create_test_raindex_service(&local_evm);

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

        let expected = FractionalShares::new(Float::parse("700".to_string()).unwrap());
        assert_eq!(balance, expected, "Expected 700 shares but got {balance:?}");
    }

    /// Values with large exponents produce scientific notation from
    /// Float::format(), which Decimal::from_str cannot parse.
    /// Float::from_raw handles this correctly.
    #[test]
    fn large_exponent_produces_correct_value() {
        let float = Float::parse("100000000000000000000".to_string())
            .expect("valid Float from decimal string");

        let exact = Float::from_raw(float.get_inner());
        assert!(
            exact
                .eq(Float::parse("100000000000000000000".to_string()).unwrap())
                .unwrap()
        );
    }

    proptest! {
        /// Roundtrip: decimal string -> Float::parse -> Float::from_raw ->
        /// Float matches the original string.
        #[test]
        fn roundtrip_from_decimal_string(
            integer in 0u64..1_000_000_000,
            fraction in 0u32..1_000_000,
        ) {
            let input = format!("{integer}.{fraction:06}");
            let float = Float::parse(input.clone()).map_err(|err| {
                TestCaseError::Reject(format!("Float::parse rejected {input}: {err}").into())
            })?;

            let exact = Float::from_raw(float.get_inner());

            // Re-parse original to compare as Float
            let expected = Float::parse(input.clone()).map_err(|err| {
                TestCaseError::fail(format!("Float::parse failed for {input}: {err}"))
            })?;
            let eq_result = exact.eq(expected).map_err(|float_err| TestCaseError::fail(format!("Float::eq failed: {float_err}")))?;
            prop_assert!(eq_result);
        }

        /// Roundtrip: random bytes -> Float::from_raw -> format ->
        /// Float::parse -> get_inner produces equivalent Float value.
        #[test]
        fn roundtrip_from_raw_bytes(raw in any::<[u8; 32]>()) {
            let bytes = B256::from(raw);
            let float = Float::from_raw(bytes);

            // Skip values that rain-math-float considers invalid
            if float.format_with_scientific(false).is_err() {
                return Ok(());
            }

            let exact = Float::from_raw(bytes);

            let Ok(formatted) = exact.format_with_scientific(false) else {
                return Ok(());
            };

            let roundtripped = Float::parse(formatted.clone()).map_err(|err| {
                TestCaseError::fail(format!(
                    "Float::parse failed on Float output '{formatted}': {err}"
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

        let service = create_test_raindex_service(&local_evm);

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
