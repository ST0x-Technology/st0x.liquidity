//! Raindex (Rain OrderBook V6) abstraction.
//!
//! This crate provides the generic [`Raindex`] trait for Rain OrderBook vault
//! operations (deposit, withdraw, confirm) plus the shared domain types
//! ([`RaindexVaultId`], [`RaindexError`]) used across consumers.
//!
//! The concrete Rain implementation lives in the application crate while this
//! crate owns the shared trait, errors, and identifier types used by consumers.

use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::rpc::types::TransactionReceipt;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;

use st0x_evm::EvmError;

#[cfg(feature = "rain")]
mod service;
#[cfg(feature = "rain")]
pub use service::RaindexService;

/// Vault identifier for Rain OrderBook vaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaindexVaultId(pub B256);

/// The two Raindex contract addresses a [`RaindexService`] talks to.
///
/// Named fields make the inventory/orderbook distinction compile-checked at
/// every construction — the two are both `Address` and semantically opposite
/// (writes go to the inventory; the orderbook is only read and
/// back-compat-scanned), so passing them positionally invited a silent swap
/// that no test would catch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaindexContracts {
    /// Shared `RaindexInventory` — target of every `deposit4`/`withdraw4` write
    /// and source of `Operator{Deposit,Withdraw}` events.
    pub inventory: Address,
    /// Underlying Rain OrderBook — target of `vaultBalance2` reads and the
    /// backwards-compat `WithdrawV2` scan.
    pub orderbook: Address,
}

#[derive(Debug, thiserror::Error)]
pub enum RaindexError {
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
    /// The shared `RaindexInventory` reverted a `withdraw4` because the vault
    /// could not cover the requested amount (e.g. a concurrent Raindex clear
    /// drained it first). Unlike the orderbook's `withdraw4`, which returns
    /// `min(target, balance)`, the inventory settles atomically — it reverts
    /// rather than forward a short fill. Retryable once the vault is refunded,
    /// and distinguishable so the caller can reconcile instead of blindly
    /// re-submitting the same reverting withdraw.
    #[error(
        "inventory vault under-funded for {token}: requested {requested}, vault could cover {received}"
    )]
    InsufficientVaultLiquidity {
        token: Address,
        requested: U256,
        received: U256,
    },
    /// The bot's signing wallet does not hold `OPERATOR_ROLE` on the configured
    /// `RaindexInventory`, so every `deposit4`/`withdraw4` would revert
    /// `AccessControlUnauthorizedAccount`. Raised by the startup preflight so the
    /// misconfiguration surfaces before the bot burns gas reverting on the first
    /// rebalance — the role must be granted (RAI-1198) before rebalancing runs.
    #[error(
        "signing wallet {wallet} lacks OPERATOR_ROLE on inventory {inventory}; \
         grant it before enabling rebalancing"
    )]
    MissingOperatorRole { inventory: Address, wallet: Address },
}

impl RaindexError {
    /// `true` if this error reports that a submitted transaction was dropped from
    /// the mempool and will never mine -- a terminal failure, distinct from a
    /// still-pending transaction that simply has not confirmed yet.
    pub fn is_transaction_dropped(&self) -> bool {
        match self {
            Self::Evm(evm_error) => evm_error.is_transaction_dropped(),
            Self::Contract(_)
            | Self::Float(_)
            | Self::ZeroAmount
            | Self::RpcTransport(_)
            | Self::SolType(_)
            | Self::ScanInconclusive { .. }
            | Self::InsufficientVaultLiquidity { .. }
            | Self::MissingOperatorRole { .. } => false,
        }
    }
}

/// Abstraction for Raindex (Rain OrderBook) operations.
///
/// This trait abstracts deposit and withdraw operations for Raindex,
/// allowing different implementations (real service, mock) to be used interchangeably.
#[async_trait]
pub trait Raindex: Send + Sync {
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
