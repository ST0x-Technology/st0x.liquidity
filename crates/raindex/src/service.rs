//! Rain OrderBook V6 (Raindex) vault operations on Base.
//!
//! [`RaindexService`] implements the [`Raindex`](crate::Raindex) trait for depositing and
//! withdrawing tokens to/from Raindex vaults using the `deposit4` and `withdraw4`
//! contract functions. The primary use case is USDC vault management for inventory
//! rebalancing in the market making system.
//!
//! This module and its private `IRaindexV6`/`IERC20` bindings are gated behind the
//! `rain` feature; the [`Raindex`](crate::Raindex) trait and its domain types ship in the
//! default build.
//!
//! ## V6 Float Format
//!
//! OrderBook V6 uses a custom float format (B256) for amounts. All conversions between
//! standard fixed-point amounts (U256) and the float format MUST use rain-math-float.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, TransactionReceipt};
use alloy::sol;
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use rain_math_float::Float;
use tracing::{debug, info};

use st0x_evm::{Evm, IntoErrorRegistry, OpenChainErrorRegistry, USDC_BASE, Wallet};
use st0x_execution::FractionalShares;
use st0x_finance::Usdc;

use crate::{Raindex, RaindexError, RaindexVaultId};

sol!(
    #![sol(all_derives = true, rpc)]
    IRaindexV6, env!("ST0X_IORDERBOOK_V6_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    IRaindexInventory, env!("ST0X_RAINDEX_INVENTORY_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    IERC20, env!("ST0X_IERC20_ABI")
);

const USDC_DECIMALS: u8 = 6;

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
/// Writes (deposits, withdrawals) settle through the shared `RaindexInventory`
/// contract (raindex.governance): the inventory owns the underlying Raindex
/// vaults so every venue adapter and this bot's rebalancing path share one
/// capital pool. The inventory's `deposit4`/`withdraw4` have signatures
/// identical to `IRaindexV6` -- calldata is the same shape, only the target
/// contract differs.
///
/// Reads (`vaultBalance2`, historical `WithdrawV2` scans) still go to the
/// orderbook directly since the inventory is just a wrapper on top of it.
///
/// Parameterized by [`Evm`] for read operations (balance queries, withdrawal
/// scans) and additionally requires [`Wallet`] for write operations
/// (deposits, withdrawals).
///
/// # Example
///
/// ```ignore
/// let service = RaindexService::new(wallet, inventory_address, orderbook_address, owner);
///
/// // Submit a USDC deposit to the vault, then confirm it (needs Wallet)
/// let amount = U256::from(1000) * U256::from(10).pow(U256::from(6)); // 1000 USDC
/// let deposit_tx = service.submit_deposit_usdc(vault_id, amount).await?;
/// service.confirm_tx(deposit_tx).await?;
/// ```
pub struct RaindexService<E: Evm> {
    /// Shared `RaindexInventory` — target of every deposit4/withdraw4 write
    /// and the source of `OperatorWithdraw`/`OperatorDeposit` events surfaced
    /// during withdrawal recovery.
    inventory_address: Address,
    /// Underlying Rain OrderBook — target of `vaultBalance2` reads and the
    /// backwards-compat `WithdrawV2` scan for pre-inventory withdrawals still
    /// in flight during the cutover window.
    orderbook_address: Address,
    evm: E,
    owner: Address,
}

impl<E: Evm> RaindexService<E> {
    pub fn new(evm: E, inventory: Address, orderbook: Address, owner: Address) -> Self {
        Self {
            inventory_address: inventory,
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
    pub async fn deposit<Registry: IntoErrorRegistry>(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        if amount.is_zero() {
            return Err(RaindexError::ZeroAmount);
        }

        self.approve_for_inventory::<Registry>(token, amount)
            .await?;

        self.deposit4_to_vault::<Registry>(token, vault_id, amount, decimals)
            .await
    }

    async fn approve_for_inventory<Registry: IntoErrorRegistry>(
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
                    spender: self.inventory_address,
                },
            )
            .await?;

        if current_allowance >= amount {
            debug!(target: "inventory", %token, %amount, %current_allowance, "Sufficient allowance, skipping approve");
            return Ok(());
        }

        debug!(target: "inventory", %token, %amount, %current_allowance, spender = %self.inventory_address, "Sending ERC20 approve");

        let receipt = self
            .evm
            .submit::<Registry, _>(
                token,
                IERC20::approveCall {
                    spender: self.inventory_address,
                    amount,
                },
                "ERC20 approve for inventory",
            )
            .await?;

        info!(target: "inventory", tx_hash = %receipt.transaction_hash, %token, "Approve confirmed");
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

        debug!(target: "inventory", %token, ?vault_id, %amount, "Sending deposit4");

        let calldata = IRaindexInventory::deposit4Call {
            token,
            vaultId: vault_id.0,
            depositAmount: amount_float.get_inner(),
            tasks: Vec::new(),
        };

        let receipt = self
            .evm
            .submit::<Registry, _>(self.inventory_address, calldata, "deposit4 to vault")
            .await?;

        info!(target: "inventory", tx_hash = %receipt.transaction_hash, %token, %amount, "deposit4 confirmed");
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

        debug!(target: "inventory", %token, ?vault_id, %amount, "Sending deposit4");

        let calldata = IRaindexInventory::deposit4Call {
            token,
            vaultId: vault_id.0,
            depositAmount: amount_float.get_inner(),
            tasks: Vec::new(),
        };

        let tx_hash = self
            .evm
            .submit_pending(self.inventory_address, calldata, "deposit4 to vault")
            .await?;

        info!(target: "inventory", %tx_hash, %token, %amount, "deposit4 submitted");
        Ok(tx_hash)
    }

    /// Submits a USDC deposit to a Rain OrderBook vault on Base WITHOUT waiting
    /// for confirmation, returning the broadcast tx hash.
    ///
    /// Used by the crash-safe deposit path: the hash is persisted as
    /// `InitiateDeposit` before confirmation, so a crash during the confirmation
    /// wait resumes from `DepositInitiated` (re-verifying the recorded tx via
    /// `confirm_tx`) instead of re-submitting a second deposit.
    pub async fn submit_deposit_usdc(
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
    pub async fn withdraw_usdc(
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
    pub async fn get_equity_balance<Registry: IntoErrorRegistry>(
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
    pub async fn get_usdc_balance<Registry: IntoErrorRegistry>(
        &self,
        owner: Address,
        vault_id: RaindexVaultId,
    ) -> Result<Usdc, RaindexError> {
        let exact = self
            .get_vault_balance::<Registry>(owner, USDC_BASE, vault_id)
            .await?;
        Ok(Usdc::new(exact))
    }

    /// Scans for a withdrawal by this bot's EOA on the given vault strictly
    /// after `from_block`, returning the most recent match's
    /// `(tx_hash, withdrawn_amount)`.
    ///
    /// Watches both the current and pre-migration event shapes in a single
    /// query, so a resume that straddles the cutover still adopts an in-flight
    /// withdraw instead of re-issuing:
    ///
    /// * **Current path (inventory):** `RaindexInventory.OperatorWithdraw`
    ///   filtered by `operator == self.owner`. Emitted for every rebalance
    ///   withdrawal that routes through the shared inventory. The event's
    ///   `amount` is the actual balance delta forwarded to the operator, so
    ///   partial fills reconcile correctly.
    /// * **Backwards-compat path (orderbook):** `IRaindexV6.WithdrawV2`
    ///   filtered by `sender == self.owner`. Matches only pre-migration
    ///   withdrawals the bot submitted directly against the orderbook -- once
    ///   the last pre-migration withdraw has drained from crash-recovery state
    ///   this branch becomes dead code.
    ///
    /// Crash-safe withdrawal recovery: a transfer records the chain head before
    /// the on-chain withdraw, so on resume this detects an already-submitted
    /// withdrawal and the caller adopts it instead of re-issuing (which would
    /// double-spend the vault). The head is captured before submitting, so this
    /// transfer's withdraw lands strictly after `from_block`; the scan excludes
    /// the `from_block` block itself so an earlier withdrawal from the same
    /// vault is never adopted.
    ///
    /// Returns `Ok(None)` ONLY when the queried node is confirmations-deep past
    /// `from_block` and repeated scans agree the effect is absent. A node that
    /// may be lagging (the dRPC load-balancing hazard AGENTS.md warns about)
    /// yields a retryable [`RaindexError::ScanInconclusive`] instead, so the
    /// caller never re-executes the irreversible withdraw off a single stale
    /// empty `eth_getLogs`.
    pub async fn find_recent_withdrawal(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        from_block: u64,
    ) -> Result<Option<(TxHash, U256)>, RaindexError> {
        let RaindexVaultId(vault_id) = vault_id;
        // Union filter: two contracts, two event signatures. `get_logs`
        // returns matches from either address that match either topic0.
        // Non-matching events on those addresses are filtered by log decode
        // (event type mismatch → decode fails → skip).
        let filter = Filter::new()
            .from_block(from_block)
            .address(vec![self.inventory_address, self.orderbook_address])
            .event_signature(vec![
                IRaindexInventory::OperatorWithdraw::SIGNATURE_HASH,
                IRaindexV6::WithdrawV2::SIGNATURE_HASH,
            ]);

        for attempt in 1..=SCAN_ATTEMPTS {
            let logs = self.evm.provider().get_logs(&filter).await?;

            // get_logs returns ascending block order; iterate newest-first so
            // the most recent matching withdrawal wins -- under single-in-flight
            // that is this transfer's withdrawal.
            for log in logs.iter().rev() {
                let block_after_from = log.block_number.is_some_and(|b| b > from_block);
                if !block_after_from {
                    continue;
                }
                let Some(tx_hash) = log.transaction_hash else {
                    continue;
                };

                if let Some(topic) = log.topics().first() {
                    if *topic == IRaindexInventory::OperatorWithdraw::SIGNATURE_HASH {
                        let decoded = log.log_decode::<IRaindexInventory::OperatorWithdraw>()?;
                        let event = decoded.data();
                        if event.operator == self.owner
                            && event.token == token
                            && event.vaultId == vault_id
                        {
                            debug!(target: "inventory", %tx_hash, from_block, "Found existing OperatorWithdraw during resume");
                            return Ok(Some((tx_hash, event.amount)));
                        }
                    } else if *topic == IRaindexV6::WithdrawV2::SIGNATURE_HASH {
                        let decoded = log.log_decode::<IRaindexV6::WithdrawV2>()?;
                        let event = decoded.data();
                        if event.sender == self.owner
                            && event.token == token
                            && event.vaultId == vault_id
                        {
                            debug!(target: "orderbook", %tx_hash, from_block, "Found existing pre-migration WithdrawV2 during resume");
                            return Ok(Some((tx_hash, event.withdrawAmountUint256)));
                        }
                    }
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
    pub async fn current_block(&self) -> Result<u64, RaindexError> {
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
                IRaindexV6::vaultBalance2Call {
                    owner,
                    token,
                    vaultId: vault_id.0,
                },
            )
            .await?;

        Ok(Float::from_raw(balance_float))
    }
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
                self.inventory_address,
                IRaindexInventory::withdraw4Call {
                    token,
                    vaultId: vault_id.0,
                    targetAmount: amount_float.get_inner(),
                    tasks: Vec::new(),
                },
                "withdraw4 from vault",
            )
            .await?;

        info!(target: "inventory", tx_hash = %receipt.transaction_hash, %token, %target_amount, "withdraw4 confirmed");
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

        self.approve_for_inventory::<OpenChainErrorRegistry>(token, amount)
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
                self.inventory_address,
                IRaindexInventory::withdraw4Call {
                    token,
                    vaultId: vault_id.0,
                    targetAmount: amount_float.get_inner(),
                    tasks: Vec::new(),
                },
                "withdraw4 from vault",
            )
            .await?;

        info!(target: "inventory", %tx_hash, %token, %target_amount, "withdraw4 submitted");
        Ok(tx_hash)
    }

    async fn confirm_tx_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<TransactionReceipt, RaindexError> {
        let receipt = self.evm.confirm::<OpenChainErrorRegistry>(tx_hash).await?;

        info!(target: "inventory", tx_hash = %receipt.transaction_hash, "Transaction confirmed");
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::{Ethereum, TransactionBuilder};
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::Log as PrimitiveLog;
    use alloy::primitives::{B256, address, b256};
    use alloy::providers::ext::AnvilApi as _;
    use alloy::providers::fillers::{
        BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
    };
    use alloy::providers::mock::Asserter;
    use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
    use alloy::rpc::types::{Log, TransactionRequest};
    use alloy::sol;
    use alloy::transports::{RpcError, TransportErrorKind};
    use proptest::prelude::*;
    use serde_json::json;
    use tracing_test::traced_test;

    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_evm::{EvmError, NoOpErrorRegistry, ReadOnlyEvm, Wallet};

    use super::*;

    sol!(
        #![sol(all_derives = true, rpc)]
        RaindexV6, env!("ST0X_ORDERBOOK_ABI")
    );

    sol!(
        #![sol(all_derives = true, rpc)]
        TestERC20, env!("ST0X_TEST_ERC20_ABI")
    );

    /// Deterministic singleton address of the TOFUTokenDecimals contract. The
    /// orderbook's `LibTOFUTokenDecimals.ensureDeployed` hardcodes this address and
    /// checks the codehash, so any test exercising deposits, withdrawals, or order
    /// takes must place the canonical runtime here.
    const TOFU_TOKEN_DECIMALS: Address = address!("0x200e12D10bb0c5E4a17e7018f0F1161919bb9389");

    /// Canonical TOFUTokenDecimals init bytecode, copied from
    /// rain-tofu-erc20-decimals' `LibTOFUTokenDecimals.TOFU_DECIMALS_EXPECTED_CREATION_CODE`.
    /// Deploying this and etching the resulting runtime at `TOFU_TOKEN_DECIMALS` yields the
    /// codehash `ensureDeployed` requires; rain.orderbook's own recompile of TOFUTokenDecimals.sol
    /// does not match that hash, so its artifact bytecode cannot be used directly.
    const TOFU_DECIMALS_CREATION_CODE: &str = "0x6080604052348015600e575f80fd5b5061044b8061001c5f395ff3fe608060405234801561000f575f80fd5b506004361061004a575f3560e01c80630782d7e11461004e57806354636d2b14610078578063b7bad1b11461009d578063f5c36eaf146100b0575b5f80fd5b61006161005c366004610363565b6100c3565b60405161006f929190610403565b60405180910390f35b61008b610086366004610363565b6100d8565b60405160ff909116815260200161006f565b6100616100ab366004610363565b6100e9565b61008b6100be366004610363565b6100f5565b5f806100cf5f84610100565b91509150915091565b5f6100e35f836101f0565b92915050565b5f806100cf5f84610281565b5f6100e35f83610356565b73ffffffffffffffffffffffffffffffffffffffff81165f9081526020838152604080832081518083019092525460ff8082161515835261010090910416818301527f313ce56700000000000000000000000000000000000000000000000000000000808452839283908190816004818a5afa915060203d1015610182575f91505b811561019857505f5160ff811115610198575f91505b816101af57505050602001516003925090506101e9565b83516101c3575f955093506101e992505050565b836020015160ff1681146101d85760026101db565b60015b846020015195509550505050505b9250929050565b5f805f6101fd8585610281565b909250905060018260038111156102165761021661039d565b1415801561023557505f8260038111156102325761023261039d565b14155b156102795783826040517fee07877f000000000000000000000000000000000000000000000000000000008152600401610270929190610421565b60405180910390fd5b949350505050565b5f805f8061028f8686610100565b90925090505f8260038111156102a7576102a761039d565b0361034b576040805180820182526001815260ff838116602080840191825273ffffffffffffffffffffffffffffffffffffffff8a165f908152908b9052939093209151825493517fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00009094169015157fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ff161761010093909116929092029190911790555b909590945092505050565b5f805f6101fd8585610100565b5f60208284031215610373575f80fd5b813573ffffffffffffffffffffffffffffffffffffffff81168114610396575f80fd5b9392505050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52602160045260245ffd5b600481106103ff577f4e487b71000000000000000000000000000000000000000000000000000000005f52602160045260245ffd5b9052565b6040810161041182856103ca565b60ff831660208301529392505050565b73ffffffffffffffffffffffffffffffffffffffff831681526040810161039660208301846103ca56";

    /// Deploys the canonical TOFUTokenDecimals init bytecode and etches the resulting
    /// runtime at [`TOFU_TOKEN_DECIMALS`]. The orderbook checks both the address and
    /// the codehash, so the runtime must come from executing the canonical creation
    /// code rather than from a recompiled artifact.
    async fn deploy_tofu_singleton<P: Provider>(provider: &P) {
        let creation_code = alloy::hex::decode(TOFU_DECIMALS_CREATION_CODE).unwrap();
        let deployed = provider
            .send_transaction(TransactionRequest::default().with_deploy_code(creation_code))
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap()
            .contract_address
            .unwrap();
        let runtime = provider.get_code_at(deployed).await.unwrap();
        provider
            .anvil_set_code(TOFU_TOKEN_DECIMALS, runtime)
            .await
            .unwrap();
    }

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

            deploy_tofu_singleton(wallet.signing_provider()).await;

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

        async fn deploy_orderbook(provider: &impl Provider) -> Result<Address, LocalEvmError> {
            let orderbook = RaindexV6::deploy(provider).await?;

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
                    IRaindexV6::vaultBalance2Call {
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

        // The test doesn't deploy a separate inventory; pointing both at the
        // same orderbook is fine because deposit4/withdraw4 have identical
        // selectors, so IRaindexInventory calldata dispatches to the
        // orderbook's own implementations and the WithdrawV2 event fires. The
        // dual-scan finds this via the backwards-compat branch.
        RaindexService::new(
            wallet,
            local_evm.orderbook_address,
            local_evm.orderbook_address,
            owner,
        )
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
        let service = RaindexService::new(
            ReadOnlyEvm::new(provider),
            Address::ZERO,
            Address::ZERO,
            Address::ZERO,
        );

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
        let service = RaindexService::new(
            ReadOnlyEvm::new(provider),
            Address::ZERO,
            Address::ZERO,
            Address::ZERO,
        );

        let result = service
            .find_recent_withdrawal(USDC_BASE, TEST_VAULT_ID, from_block)
            .await
            .unwrap();

        assert_eq!(result, None);
    }

    /// A `WithdrawV2` from this owner's vault, mined at `block_number`, that
    /// `find_recent_withdrawal` will see for `(USDC_BASE, TEST_VAULT_ID)`.
    fn withdraw_log(block_number: u64, withdraw_amount: U256) -> Log {
        let event = IRaindexV6::WithdrawV2 {
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

    /// An inventory `OperatorWithdraw` for this operator, mined at
    /// `block_number`, that `find_recent_withdrawal` will see for
    /// `(USDC_BASE, TEST_VAULT_ID)`.
    fn operator_withdraw_log(block_number: u64, amount: U256) -> Log {
        let event = IRaindexInventory::OperatorWithdraw {
            operator: Address::ZERO,
            token: USDC_BASE,
            vaultId: TEST_VAULT_ID.0,
            amount,
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
                "0x00000000000000000000000000000000000000000000000000000000000000bb"
            )),
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    #[tokio::test]
    async fn find_recent_withdrawal_adopts_inventory_operator_withdraw() {
        let asserter = Asserter::new();
        let from_block = 100u64;
        let amount = U256::from(11u64);
        let log = operator_withdraw_log(from_block + 1, amount);
        let expected_tx = log.transaction_hash.unwrap();
        // Inventory-path event decoded via the OperatorWithdraw branch of the
        // dual-scan; amount surfaces from the event's `amount` field rather
        // than the orderbook's `withdrawAmountUint256`.
        asserter.push_success(&json!([log]));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let service = RaindexService::new(
            ReadOnlyEvm::new(provider),
            Address::ZERO,
            Address::ZERO,
            Address::ZERO,
        );

        let result = service
            .find_recent_withdrawal(USDC_BASE, TEST_VAULT_ID, from_block)
            .await
            .unwrap();

        assert_eq!(result, Some((expected_tx, amount)));
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
        let service = RaindexService::new(
            ReadOnlyEvm::new(provider),
            Address::ZERO,
            Address::ZERO,
            Address::ZERO,
        );

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
        let service = RaindexService::new(
            ReadOnlyEvm::new(provider),
            Address::ZERO,
            Address::ZERO,
            Address::ZERO,
        );

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
