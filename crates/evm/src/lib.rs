//! EVM chain interaction abstraction.
//!
//! This crate provides two traits for interacting with EVM chains:
//!
//! - [`Evm`] -- read-only chain access with error-decoded view calls.
//!   Provides the underlying provider and a `call` method that
//!   automatically decodes Solidity revert data via the OpenChain
//!   selector registry.
//!
//! - [`Wallet`] -- extends `Evm` with a signing identity and
//!   transaction submission. Implementations handle key management
//!   and signing (Turnkey secure enclaves or raw private key in
//!   production, `RawPrivateKeyWallet` in tests).
//!
//! Error decoding is built into both `Evm::call` (view calls) and
//! `Wallet::submit` (write transactions), so consumers get
//! human-readable revert reasons without manual wiring.

use alloy::consensus::Transaction;
use alloy::primitives::{Address, Bytes, TxHash};
use alloy::providers::Provider;
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::sol_types::SolCall;
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
use tokio::time::{interval, timeout};
use tracing::{debug, warn};

pub mod error_decoding;
pub use error_decoding::{IntoErrorRegistry, NoOpErrorRegistry, OpenChainErrorRegistry};
use error_decoding::{decode_reverted_receipt, decode_rpc_revert};
pub use rain_error_decoding::AbiDecodedErrorType;

pub mod nonce;
pub use nonce::ResettableNonceManager;

mod bindings;
pub use bindings::{IERC20, IPyth, PythStructs};

mod tokens;
pub use tokens::{USDC_BASE, USDC_ETHEREUM, USDC_ETHEREUM_SEPOLIA};

#[cfg(any(feature = "turnkey", feature = "local-signer"))]
mod submit;

#[cfg(feature = "local-signer")]
pub mod local;
#[cfg(feature = "turnkey")]
pub mod turnkey;

#[cfg(feature = "mock")]
pub mod test_chain;

#[cfg(feature = "test-support")]
pub mod stub;
#[cfg(feature = "test-support")]
pub use stub::StubWallet;

/// Wallet backend discriminant. Deserialized from a `kind` field in
/// wallet config sections. Variants are feature-gated so unconfigured
/// backends fail at parse time with "unknown variant".
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WalletKind {
    #[cfg(feature = "turnkey")]
    Turnkey,
    #[cfg(feature = "local-signer")]
    PrivateKey,
}

/// Deserializes `self` into any `DeserializeOwned` target.
///
/// Implement this for your config format (e.g. a TOML newtype) so
/// [`WalletKind::try_into_wallet`] and [`TryIntoWallet::try_into_wallet`]
/// can parse backend-specific settings and credentials from it.
pub trait Parser: Send {
    type Error: Into<EvmError>;

    /// # Errors
    ///
    /// Returns `Self::Error` if deserialization fails.
    fn parse<Target: serde::de::DeserializeOwned>(self) -> Result<Target, Self::Error>;
}

impl WalletKind {
    /// Dispatch wallet construction by variant.
    ///
    /// Parses the backend's `Settings` and `Credentials` from the
    /// raw config/secrets via [`Parser::parse`], then delegates to
    /// [`TryIntoWallet::try_from_ctx`].
    ///
    /// # Errors
    ///
    /// Returns [`EvmError`] if config/secrets parsing or wallet
    /// construction fails.
    ///
    /// When no wallet backend features are enabled, `WalletKind` is
    /// uninhabited and this method can never be called.
    #[cfg_attr(
        not(any(feature = "turnkey", feature = "local-signer")),
        allow(clippy::unused_async, clippy::uninhabited_references, unused_variables)
    )]
    pub async fn try_into_wallet<Raw, Node>(
        &self,
        ctx: WalletCtx<Raw, Raw, Node>,
    ) -> Result<Arc<dyn Wallet<Provider = Node>>, EvmError>
    where
        Raw: Parser,
        Node: Provider + Clone + Send + Sync + 'static,
    {
        match *self {
            #[cfg(feature = "turnkey")]
            Self::Turnkey => {
                let WalletCtx {
                    settings,
                    credentials,
                    provider,
                    required_confirmations,
                } = ctx;
                let wallet = turnkey::TurnkeyWallet::try_from_ctx(WalletCtx {
                    settings: settings.parse().map_err(Into::into)?,
                    credentials: credentials.parse().map_err(Into::into)?,
                    provider,
                    required_confirmations,
                })
                .await?;
                Ok(Arc::new(wallet))
            }
            #[cfg(feature = "local-signer")]
            Self::PrivateKey => {
                let WalletCtx {
                    settings: _,
                    credentials,
                    provider,
                    required_confirmations,
                } = ctx;
                let wallet = local::RawPrivateKeyWallet::try_from_ctx(WalletCtx {
                    settings: (),
                    credentials: credentials.parse().map_err(Into::into)?,
                    provider,
                    required_confirmations,
                })
                .await?;
                Ok(Arc::new(wallet))
            }
        }
    }
}

/// Poll interval between `eth_blockNumber` calls in [`wait_for_node_sync`].
pub const NODE_SYNC_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum number of polls before [`wait_for_node_sync`] gives up.
///
/// 30 attempts at 1s each gives a 30s budget — generous for even a
/// temporarily lagging backend node on a fast chain like Base.
pub const NODE_SYNC_MAX_ATTEMPTS: u32 = 30;

/// Errors that can occur during EVM operations.
#[derive(Debug, thiserror::Error)]
pub enum EvmError {
    #[error("transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
    #[error("transport error: {0}")]
    Transport(#[from] alloy::transports::RpcError<alloy::transports::TransportErrorKind>),
    #[error("contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("ABI decode error: {0}")]
    AbiDecode(#[from] alloy::sol_types::Error),
    #[error("decoded contract error: {0}")]
    DecodedRevert(#[from] AbiDecodedErrorType),
    #[error("transaction reverted: {tx_hash}")]
    Reverted { tx_hash: alloy::primitives::TxHash },
    #[error("wallet config parse error: {0}")]
    WalletConfigParse(Box<dyn std::error::Error + Send + Sync>),
    #[error(
        "timed out waiting for transaction receipt after {}s: {tx_hash}",
        timeout_secs
    )]
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    ReceiptTimeout {
        tx_hash: alloy::primitives::TxHash,
        timeout_secs: u64,
    },
    /// Transaction was dropped from the mempool (not found via
    /// `eth_getTransactionByHash` after initial propagation window).
    #[error(
        "transaction {tx_hash} dropped from mempool \
         (not found after {elapsed_secs}s)"
    )]
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    TransactionDropped {
        tx_hash: alloy::primitives::TxHash,
        elapsed_secs: u64,
    },
    /// A stuck under-gassed pending transaction occupies the wallet's
    /// next nonce, and `attempts` fee-bumped resubmits at that nonce all
    /// came back "replacement transaction underpriced". The replacement
    /// fee never exceeded the stuck transaction's fee within the bounded
    /// escalation budget -- surfaced as a hard error rather than bumping
    /// the fee without limit.
    #[error(
        "replacement transaction still underpriced after {attempts} \
         fee-bumped resubmits"
    )]
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    ReplacementUnderpriced { attempts: u32 },
    /// Bumping the EIP-1559 fee for a replacement transaction overflowed
    /// `u128`. Only reachable if the RPC returns an absurd fee estimate;
    /// surfaced as a hard error rather than silently wrapping a financial
    /// value.
    #[error("replacement fee bump overflowed u128 (network fee estimate too large)")]
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    ReplacementFeeOverflow,
    #[cfg(feature = "local-signer")]
    #[error("invalid private key: {0}")]
    InvalidPrivateKey(#[from] alloy::signers::k256::ecdsa::Error),
    #[cfg(feature = "turnkey")]
    #[error("Turnkey error: {0}")]
    Turnkey(#[from] turnkey::TurnkeyError),
    /// The RPC node's tip remained below the required block after exhausting
    /// the poll budget. Raised by [`wait_for_node_sync`] when a load-balanced
    /// backend never catches up within the retry window.
    #[error(
        "RPC node tip {observed_tip} is behind required block {required_block} \
         after {attempts} polls"
    )]
    NodeBehindRequiredBlock {
        observed_tip: u64,
        required_block: u64,
        attempts: u32,
    },
}

impl EvmError {
    /// `true` if this error represents an EVM transaction revert (as opposed
    /// to a transport failure or other non-revert error).
    ///
    /// Handles both post-mining reverts ([`Self::Reverted`],
    /// [`Self::DecodedRevert`], [`Self::Contract`] with actual revert data)
    /// and pre-mining preflight rejections via [`Self::Transport`]:
    ///
    /// - Code 3: the Ethereum JSON-RPC spec execution-revert code; Anvil uses
    ///   this in simulation mode. Unambiguously a revert regardless of message.
    /// - Codes -32000 / -32003: shared by multiple failure types ("nonce too
    ///   low", "underpriced", "execution reverted"). Only classified as revert
    ///   when the message contains `"execution reverted"`.
    ///
    /// All other variants (`Transaction`, `AbiDecode`, `WalletConfigParse`,
    /// `NodeBehindRequiredBlock`, and all feature-gated variants) are
    /// non-revert by definition.
    pub fn is_revert(&self) -> bool {
        match self {
            Self::Reverted { .. } | Self::DecodedRevert(_) => true,
            // Contract wraps alloy::contract::Error, which is also produced for
            // pure transport failures (connection reset, timeout) when revert
            // data cannot be decoded. Only classify as revert when actual EVM
            // revert data is present.
            Self::Contract(contract_err) => contract_err.as_revert_data().is_some(),
            Self::Transport(rpc_error) => rpc_error.as_error_resp().is_some_and(|payload| {
                payload.code == 3 || payload.message.contains("execution reverted")
            }),
            Self::Transaction(_) | Self::AbiDecode(_) | Self::WalletConfigParse(_) => false,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReceiptTimeout { .. } => false,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::TransactionDropped { .. } => false,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReplacementUnderpriced { .. } => false,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReplacementFeeOverflow => false,
            #[cfg(feature = "local-signer")]
            Self::InvalidPrivateKey(_) => false,
            #[cfg(feature = "turnkey")]
            Self::Turnkey(_) => false,
            Self::NodeBehindRequiredBlock { .. } => false,
        }
    }

    /// `true` if this reports a transaction dropped from the mempool (it will
    /// never mine) -- a terminal failure, distinct from a still-pending tx that
    /// merely has not confirmed yet. Only the wallet builds (`turnkey` /
    /// `local-signer`), which run the confirm loop, can produce it.
    pub fn is_transaction_dropped(&self) -> bool {
        match self {
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::TransactionDropped { .. } => true,
            Self::Transaction(_)
            | Self::Transport(_)
            | Self::Contract(_)
            | Self::AbiDecode(_)
            | Self::DecodedRevert(_)
            | Self::Reverted { .. }
            | Self::WalletConfigParse(_) => false,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReceiptTimeout { .. } => false,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReplacementUnderpriced { .. } => false,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReplacementFeeOverflow => false,
            #[cfg(feature = "local-signer")]
            Self::InvalidPrivateKey(_) => false,
            #[cfg(feature = "turnkey")]
            Self::Turnkey(_) => false,
            Self::NodeBehindRequiredBlock { .. } => false,
        }
    }

    /// The raw ABI-encoded revert bytes this error carries, if any.
    ///
    /// A contract revert reaches us in more than one shape: an `alloy`
    /// contract-call error (`Contract`), a raw JSON-RPC error response
    /// (`Transport`, e.g. a revert surfaced during gas estimation before the tx
    /// is sent), or a mined revert the selector registry decoded
    /// (`DecodedRevert`, whether or not it matched a known signature). Callers
    /// use this to re-decode a revert against a specific ABI -- e.g. a custom
    /// error the shared registry does not know -- instead of branching on the
    /// error variant themselves.
    pub fn revert_data(&self) -> Option<alloy::primitives::Bytes> {
        match self {
            Self::Contract(contract_err) => contract_err.as_revert_data(),
            Self::Transport(rpc_error) => rpc_error
                .as_error_resp()
                .and_then(alloy::rpc::json_rpc::ErrorPayload::as_revert_data),
            Self::DecodedRevert(AbiDecodedErrorType::Unknown(bytes)) => {
                Some(alloy::primitives::Bytes::copy_from_slice(bytes))
            }
            Self::DecodedRevert(AbiDecodedErrorType::Known { data, .. }) => {
                Some(alloy::primitives::Bytes::copy_from_slice(data))
            }
            Self::Reverted { .. }
            | Self::Transaction(_)
            | Self::AbiDecode(_)
            | Self::WalletConfigParse(_)
            | Self::NodeBehindRequiredBlock { .. } => None,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReceiptTimeout { .. } => None,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::TransactionDropped { .. } => None,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReplacementUnderpriced { .. } => None,
            #[cfg(any(feature = "turnkey", feature = "local-signer"))]
            Self::ReplacementFeeOverflow => None,
            #[cfg(feature = "local-signer")]
            Self::InvalidPrivateKey(_) => None,
            #[cfg(feature = "turnkey")]
            Self::Turnkey(_) => None,
        }
    }
}

impl EvmError {
    /// Returns `true` if this is a "nonce too low" RPC error, which
    /// occurs when an external process (e.g. CLI) submitted
    /// transactions from the same wallet, advancing the chain nonce
    /// past the locally cached value.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) fn is_nonce_too_low(&self) -> bool {
        match self {
            Self::Transport(rpc_error) => rpc_error
                .as_error_resp()
                .is_some_and(|payload| payload.message.contains("nonce too low")),
            _ => false,
        }
    }

    /// Returns `true` if this is a "replacement transaction underpriced"
    /// RPC error. It occurs when a different transaction already sits in
    /// the mempool at the target nonce and the new submission's fee does
    /// not exceed it by the node's required replacement margin (geth needs
    /// at least 10% higher). Recovering means resubmitting at the same
    /// nonce with a bumped fee.
    ///
    /// Deliberately does NOT match "already known": that error means the
    /// *identical* transaction (same hash) is already pending, which is an
    /// idempotent re-send, not a fee problem. Bumping the fee for it would
    /// needlessly replace an already-accepted transaction.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) fn is_replacement_underpriced(&self) -> bool {
        match self {
            Self::Transport(rpc_error) => rpc_error.as_error_resp().is_some_and(|payload| {
                payload
                    .message
                    .contains("replacement transaction underpriced")
            }),
            _ => false,
        }
    }
}

impl From<std::convert::Infallible> for EvmError {
    fn from(never: std::convert::Infallible) -> Self {
        match never {}
    }
}

/// Read-only EVM chain access with error-decoded view calls.
///
/// Provides the underlying provider for direct chain queries (balance
/// checks, block subscriptions, etc.) and a [`call`](Evm::call) method
/// that executes `eth_call` with automatic Solidity revert decoding.
///
/// Implementations only need to supply the provider -- `call` has a
/// default implementation that handles error decoding.
#[async_trait]
pub trait Evm: Send + Sync + 'static {
    /// The provider type used for chain access.
    type Provider: Provider + Clone + Send + Sync;

    /// Returns the underlying provider for direct chain queries.
    fn provider(&self) -> &Self::Provider;

    /// Execute a typed view call with automatic revert decoding.
    ///
    /// Encodes the call via `SolCall::abi_encode()`, runs `eth_call`,
    /// decodes the return value on success, and decodes the Solidity
    /// error via the OpenChain selector registry on revert.
    async fn call<Registry: IntoErrorRegistry, Call: SolCall + Send>(
        &self,
        contract: Address,
        call: Call,
    ) -> Result<Call::Return, EvmError>
    where
        Self: Sized,
    {
        execute_call::<Registry, Call>(self.provider(), contract, call).await
    }
}

/// Signing wallet on an EVM chain.
///
/// Extends [`Evm`] with a wallet identity (address) and transaction
/// submission. Implementations provide [`send`](Wallet::send)
/// (raw calldata submission), while [`submit`](Wallet::submit) wraps
/// it with typed encoding and revert decoding.
///
/// Key management varies by implementation: `TurnkeyWallet` signs
/// via Turnkey secure enclaves when the `turnkey` feature is enabled,
/// while `RawPrivateKeyWallet` signs locally with a raw private key
/// when the `local-signer` feature is enabled.
///
/// Implementations that support construction from config + secrets
/// should also implement [`TryIntoWallet`].
#[async_trait]
pub trait Wallet: Evm {
    /// Returns the address this wallet signs transactions from.
    fn address(&self) -> Address;

    /// Submit a signed transaction and return the tx hash immediately,
    /// without waiting for confirmation.
    ///
    /// Implementations handle signing and submission to the mempool.
    /// The returned hash can be passed to [`await_receipt`](Wallet::await_receipt)
    /// to wait for confirmation separately.
    async fn send_pending(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TxHash, EvmError>;

    /// Wait for a previously submitted transaction to be confirmed.
    ///
    /// Polls for the receipt and waits for the configured confirmation
    /// depth before returning.
    async fn await_receipt(&self, tx_hash: TxHash) -> Result<TransactionReceipt, EvmError>;

    /// Send raw calldata as a signed transaction.
    ///
    /// Implementations handle signing, submission, and waiting for the
    /// receipt. They should NOT check `receipt.status()` -- the default
    /// [`submit`](Wallet::submit) handles revert detection and decoding.
    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError>;

    /// Submit a typed contract call with automatic revert decoding.
    ///
    /// Encodes the call via `SolCall::abi_encode()`, delegates to
    /// [`send`](Wallet::send), then if the transaction reverted,
    /// replays as `eth_call` at the reverted block to extract and
    /// decode the revert reason.
    async fn submit<Registry: IntoErrorRegistry, Call: SolCall + Send>(
        &self,
        contract: Address,
        call: Call,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError>
    where
        Self: Sized,
    {
        let calldata = Bytes::from(call.abi_encode());
        let receipt = self.send(contract, calldata.clone(), note).await?;
        decode_reverted_receipt::<Registry>(
            self.provider(),
            self.address(),
            contract,
            calldata,
            receipt,
        )
        .await
    }

    /// Submit a typed contract call without waiting for confirmation.
    ///
    /// Like [`submit`](Wallet::submit), but returns the tx hash
    /// immediately instead of waiting for the receipt. Use
    /// [`confirm`](Wallet::confirm) to wait and decode reverts later.
    async fn submit_pending<Call: SolCall + Send>(
        &self,
        contract: Address,
        call: Call,
        note: &str,
    ) -> Result<TxHash, EvmError>
    where
        Self: Sized,
    {
        let calldata = Bytes::from(call.abi_encode());
        self.send_pending(contract, calldata, note).await
    }

    /// Wait for a previously submitted transaction and decode reverts.
    ///
    /// Counterpart to [`submit_pending`](Wallet::submit_pending).
    /// Awaits the receipt, then if the transaction reverted, fetches
    /// the original transaction from chain to recover `from`,
    /// `to`, and `input`, and replays as `eth_call` to decode the
    /// Solidity error via `Registry`.
    async fn confirm<Registry: IntoErrorRegistry>(
        &self,
        tx_hash: TxHash,
    ) -> Result<TransactionReceipt, EvmError>
    where
        Self: Sized,
    {
        let receipt = self.await_receipt(tx_hash).await?;

        if receipt.status() {
            return Ok(receipt);
        }

        // Recover the original calldata from chain so we can replay
        // the failed call and decode the Solidity revert reason.
        let original_tx = self
            .provider()
            .get_transaction_by_hash(tx_hash)
            .await?
            .ok_or(EvmError::Reverted { tx_hash })?;

        let contract = original_tx.to().unwrap_or_default();
        let calldata = original_tx.input().clone();

        decode_reverted_receipt::<Registry>(
            self.provider(),
            self.address(),
            contract,
            calldata,
            receipt,
        )
        .await
    }
}

/// Everything needed to construct a wallet: parsed settings,
/// credentials, an RPC provider, and confirmation depth.
pub struct WalletCtx<Settings, Credentials, Node> {
    pub settings: Settings,
    pub credentials: Credentials,
    pub provider: Node,
    pub required_confirmations: u64,
}

/// Async constructor for wallet implementations.
///
/// Each backend defines what it needs as `Settings` (non-secret
/// config like address, org ID) and `Credentials` (secrets like
/// private keys). The caller parses raw config into these types
/// and passes them via [`WalletCtx`].
#[async_trait]
pub trait TryIntoWallet: Wallet + Sized {
    /// Non-secret configuration (address, organization ID, etc.).
    type Settings: Send;

    /// Secret material (private keys, API keys, etc.).
    type Credentials: Send;

    /// Construct the wallet from parsed settings and credentials.
    async fn try_from_ctx(
        ctx: WalletCtx<Self::Settings, Self::Credentials, Self::Provider>,
    ) -> Result<Self, EvmError>;

    /// Construct the wallet from raw config/secrets, parsing each
    /// via [`Parser::parse`] into the backend's `Settings` and
    /// `Credentials`.
    async fn try_into_wallet<Raw>(
        ctx: WalletCtx<Raw, Raw, Self::Provider>,
    ) -> Result<Self, EvmError>
    where
        Raw: Parser,
        Self::Settings: serde::de::DeserializeOwned,
        Self::Credentials: serde::de::DeserializeOwned,
    {
        let WalletCtx {
            settings,
            credentials,
            provider,
            required_confirmations,
        } = ctx;

        let settings: Self::Settings = settings.parse().map_err(Into::into)?;
        let credentials: Self::Credentials = credentials.parse().map_err(Into::into)?;

        Self::try_from_ctx(WalletCtx {
            settings,
            credentials,
            provider,
            required_confirmations,
        })
        .await
    }
}

#[async_trait]
impl<Inner: Evm + ?Sized> Evm for Arc<Inner> {
    type Provider = Inner::Provider;

    fn provider(&self) -> &Self::Provider {
        (**self).provider()
    }

    async fn call<Registry: IntoErrorRegistry, Call: SolCall + Send>(
        &self,
        contract: Address,
        call: Call,
    ) -> Result<Call::Return, EvmError>
    where
        Self: Sized,
    {
        execute_call::<Registry, Call>(self.provider(), contract, call).await
    }
}

#[async_trait]
impl<Inner: Wallet + ?Sized> Wallet for Arc<Inner> {
    fn address(&self) -> Address {
        (**self).address()
    }

    async fn send_pending(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TxHash, EvmError> {
        (**self).send_pending(contract, calldata, note).await
    }

    async fn await_receipt(&self, tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
        (**self).await_receipt(tx_hash).await
    }

    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        (**self).send(contract, calldata, note).await
    }

    async fn submit<Registry: IntoErrorRegistry, Call: SolCall + Send>(
        &self,
        contract: Address,
        call: Call,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError>
    where
        Self: Sized,
    {
        let calldata = Bytes::from(call.abi_encode());
        let receipt = self.send(contract, calldata.clone(), note).await?;
        decode_reverted_receipt::<Registry>(
            self.provider(),
            self.address(),
            contract,
            calldata,
            receipt,
        )
        .await
    }
}

/// Execute a typed view call with automatic revert decoding.
///
/// Shared logic for `Evm::call` -- encodes via `SolCall`, runs
/// `eth_call`, decodes returns on success, decodes revert via the
/// selector registry on failure.
async fn execute_call<Registry: IntoErrorRegistry, Call: SolCall>(
    provider: &impl Provider,
    contract: Address,
    call: Call,
) -> Result<Call::Return, EvmError> {
    let calldata = call.abi_encode();
    let tx = TransactionRequest::default()
        .to(contract)
        .input(calldata.into());

    match provider.call(tx).await {
        Ok(result) => Ok(Call::abi_decode_returns(result.as_ref())?),
        Err(rpc_err) => Err(decode_rpc_revert::<Registry>(rpc_err).await),
    }
}

/// Spin-polls `eth_blockNumber` until the serving node reports >= `required_block`.
///
/// Load-balanced RPCs route successive calls to different backend nodes. After
/// tx N confirms at block B, the very next call may hit a node still at B-1.
/// This function blocks the caller until one poll observes height >= B, ensuring
/// dependent writes see the prerequisite tx's effects.
///
/// # Errors
///
/// Returns [`EvmError::NodeBehindRequiredBlock`] after `max_attempts` polls if
/// the node never catches up.
///
/// Returns [`EvmError::Transport`] if the budget is exhausted while every poll
/// resulted in a transport error (i.e. no successful `eth_blockNumber` response
/// was ever received).
///
/// Transient transport errors (e.g. a momentary connection failure on a
/// load-balanced RPC) are treated the same as a behind-tip poll: the attempt
/// is counted against the budget and the loop continues. This preserves the
/// full 30-attempt window as tolerance for both sync lag and transient
/// failures, rather than aborting on the first network hiccup.
pub async fn wait_for_node_sync(
    provider: &impl Provider,
    required_block: u64,
    poll_interval: Duration,
    max_attempts: u32,
) -> Result<(), EvmError> {
    // The production caller always passes NODE_SYNC_MAX_ATTEMPTS (>= 1).
    // Passing 0 would cause an empty loop and a misleading NodeBehindRequiredBlock
    // with observed_tip=0 and attempts=0 — a programming error.
    debug_assert!(
        max_attempts >= 1,
        "wait_for_node_sync: max_attempts must be at least 1"
    );

    // Tracks the most recent successful block number response.
    // Only appears in NodeBehindRequiredBlock, which requires at least
    // one successful poll.
    let mut observed_tip = 0u64;
    // True once at least one get_block_number call returned Ok (even if stale).
    // Used to distinguish "all polls failed with transport error" (return Transport)
    // from "some polls succeeded but node never caught up" (return NodeBehindRequiredBlock).
    let mut any_poll_succeeded = false;

    for attempt in 1..=max_attempts {
        if attempt > 1 {
            sleep(poll_interval).await;
        }

        match provider.get_block_number().await {
            Ok(tip) => {
                observed_tip = tip;
                any_poll_succeeded = true;
                if observed_tip >= required_block {
                    return Ok(());
                }
                debug!(
                    target: "evm",
                    observed_tip,
                    required_block,
                    attempt,
                    max_attempts,
                    "RPC node behind required block; retrying"
                );
            }
            Err(transport_error) => {
                warn!(
                    target: "evm",
                    ?transport_error,
                    attempt,
                    max_attempts,
                    "Transient transport error polling block number; retrying"
                );
                // Treat transient transport errors as a behind-tip poll:
                // count the attempt against the budget and retry, rather
                // than aborting all remaining retries on the first hiccup.
                // If attempts are exhausted, return Transport only when no
                // poll ever succeeded; otherwise fall through to
                // NodeBehindRequiredBlock with the last observed tip.
                if attempt == max_attempts && !any_poll_succeeded {
                    return Err(EvmError::Transport(transport_error));
                }
            }
        }
    }

    Err(EvmError::NodeBehindRequiredBlock {
        observed_tip,
        required_block,
        attempts: max_attempts,
    })
}

/// Poll interval for fallback receipt polling.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const RECEIPT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Maximum time to wait for a transaction receipt before giving up.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const RECEIPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Maximum time to wait for confirmation depth after a receipt is found.
/// Generous (30 min) because the tx is already mined — we just need blocks.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const CONFIRMATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Grace period before the dropped-from-mempool check may fire. A
/// freshly-submitted tx is not visible on every node of a load-balanced RPC
/// for several seconds, and on Ethereum mainnet it is not mined for ~1 block
/// (~12s); concluding "dropped" earlier yields false positives that fail a tx
/// which actually lands. Set above one mainnet block time.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const DROPPED_TX_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// Consecutive `get_transaction_by_hash == None` observations (after the grace
/// period) required before concluding the tx was dropped. Any sighting —
/// pending or mined — resets the count. Debounces the load-balanced-RPC race
/// where a single lagging node transiently reports a live tx as absent.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const DROPPED_TX_CONSECUTIVE_MISSES: u32 = 3;

/// Consecutive transport errors (with no successful poll in between) tolerated
/// while polling before giving up. A transient blip on a load-balanced RPC
/// recovers within a poll or two and a successful poll resets the count; this
/// many failures in a row with no success means the endpoint is unreachable, so
/// fail fast with the transport error instead of spinning the full
/// inclusion/confirmation timeout against a dead RPC.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const MAX_CONSECUTIVE_TRANSPORT_ERRORS: u32 = 5;

/// Tuning parameters for [`wait_for_receipt_with_config`]. Bundled into a struct
/// so the wait function keeps a small argument list as knobs are added.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReceiptWaitConfig {
    /// Interval between receipt polls.
    pub(crate) poll_interval: std::time::Duration,
    /// Max time to wait for the tx to be included in a block.
    pub(crate) inclusion_timeout: std::time::Duration,
    /// Max time to wait for confirmation depth once a receipt is found.
    pub(crate) confirmation_timeout: std::time::Duration,
    /// Grace period before the dropped-from-mempool check may fire.
    pub(crate) dropped_grace: std::time::Duration,
    /// Consecutive mempool-absence observations required to conclude a drop.
    pub(crate) dropped_consecutive_misses: u32,
}

#[cfg(any(feature = "turnkey", feature = "local-signer"))]
impl ReceiptWaitConfig {
    /// Defaults tuned for Ethereum mainnet (~12s blocks) behind a
    /// load-balanced RPC.
    const fn mainnet_defaults() -> Self {
        Self {
            poll_interval: RECEIPT_POLL_INTERVAL,
            inclusion_timeout: RECEIPT_TIMEOUT,
            confirmation_timeout: CONFIRMATION_TIMEOUT,
            dropped_grace: DROPPED_TX_GRACE,
            dropped_consecutive_misses: DROPPED_TX_CONSECUTIVE_MISSES,
        }
    }
}

/// Polls for a transaction receipt with confirmation depth, bypassing alloy's
/// heartbeat-based `get_receipt()` which can hang when the WebSocket
/// subscription misses a block.
///
/// The inclusion timeout applies to the **pre-inclusion** wait (receipt not
/// yet found). Once a receipt exists, confirmation polling continues with a
/// generous safety timeout — a mined transaction should not be reported as
/// failed, but a completely stalled RPC must not block the wallet forever.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
pub(crate) async fn wait_for_receipt(
    provider: &impl Provider,
    tx_hash: alloy::primitives::TxHash,
    required_confirmations: u64,
) -> Result<TransactionReceipt, EvmError> {
    wait_for_receipt_with_config(
        provider,
        tx_hash,
        required_confirmations,
        ReceiptWaitConfig::mainnet_defaults(),
    )
    .await
}

/// Parameterized version of [`wait_for_receipt`] for testability.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
pub(crate) async fn wait_for_receipt_with_config(
    provider: &impl Provider,
    tx_hash: alloy::primitives::TxHash,
    required_confirmations: u64,
    config: ReceiptWaitConfig,
) -> Result<TransactionReceipt, EvmError> {
    let mut poll = interval(config.poll_interval);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let start = std::time::Instant::now();

    // Phase 1: wait for inclusion (with timeout).
    let (receipt, receipt_block) = timeout(config.inclusion_timeout, async {
        // Consecutive polls where the tx is absent from BOTH the receipt lookup
        // and the mempool. A single absence is unreliable on a load-balanced
        // RPC (the lookup may hit a lagging node that has not seen a recent tx),
        // so we require several in a row — and only after a grace period —
        // before concluding the tx was dropped.
        let mut consecutive_misses = 0u32;
        let mut consecutive_transport_errors = 0u32;

        loop {
            poll.tick().await;

            // A transient transport error on a load-balanced RPC must not abort
            // the wait: count it as an inconclusive poll and retry within the
            // inclusion timeout, mirroring `wait_for_node_sync`. Aborting on the
            // first error surfaces a live, broadcast tx as failed. But a
            // sustained run of errors means the RPC is down, not blipping, so
            // give up at the cap rather than spinning the whole timeout.
            // The error run resets only when a full poll cycle completes with no
            // transport error -- a receipt comes back, or (post-grace) a clean
            // receipt+mempool pair. Resetting on the receipt poll alone would let
            // a mempool-only failure never accumulate to the cap.
            let maybe_receipt = match provider.get_transaction_receipt(tx_hash).await {
                Ok(maybe_receipt) => maybe_receipt,
                Err(transport_error) => {
                    note_receipt_poll_transport_error(
                        &mut consecutive_transport_errors,
                        transport_error,
                        "polling for receipt",
                    )?;
                    continue;
                }
            };

            let Some(receipt) = maybe_receipt else {
                // Not yet mined: decide whether to keep waiting or conclude the tx
                // was dropped, advancing the miss / transport-error runs. A
                // terminal outcome (dropped, or RPC down past the cap) propagates.
                classify_pending_receipt(
                    provider,
                    tx_hash,
                    &config,
                    start,
                    &mut consecutive_misses,
                    &mut consecutive_transport_errors,
                )
                .await?;
                continue;
            };

            // A receipt came back -- the RPC responded, so the error run is clean.
            consecutive_transport_errors = 0;

            let Some(block) = receipt.block_number else {
                continue;
            };

            return Ok::<_, EvmError>((receipt, block));
        }
    })
    .await
    .map_err(|_| EvmError::ReceiptTimeout {
        tx_hash,
        timeout_secs: config.inclusion_timeout.as_secs(),
    })??;

    // Phase 2: wait for confirmation depth (generous timeout).
    timeout(config.confirmation_timeout, async {
        let mut consecutive_transport_errors = 0u32;

        loop {
            // Same load-balanced-RPC tolerance as the inclusion phase: a
            // transient transport error must not abort confirmation polling of
            // an already-mined tx, but a sustained run (the RPC is down) gives
            // up at the cap instead of spinning the whole confirmation timeout.
            let current_block = match provider.get_block_number().await {
                Ok(current_block) => {
                    consecutive_transport_errors = 0;
                    current_block
                }
                Err(transport_error) => {
                    note_receipt_poll_transport_error(
                        &mut consecutive_transport_errors,
                        transport_error,
                        "polling block number for confirmation",
                    )?;
                    poll.tick().await;
                    continue;
                }
            };
            let confirmations = current_block.saturating_sub(receipt_block) + 1;

            if confirmations >= required_confirmations {
                return Ok::<_, EvmError>(receipt);
            }

            poll.tick().await;
        }
    })
    .await
    .map_err(|_| EvmError::ReceiptTimeout {
        tx_hash,
        timeout_secs: config.confirmation_timeout.as_secs(),
    })?
}

/// Classify an inclusion poll whose receipt is absent. Returns `Ok(())` to keep
/// waiting; returns a terminal `Err` when the tx is confirmed dropped (missing
/// from the receipt lookup and the mempool past the grace period for
/// `dropped_consecutive_misses` polls) or the RPC is down past the transport cap.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
async fn classify_pending_receipt(
    provider: &impl Provider,
    tx_hash: alloy::primitives::TxHash,
    config: &ReceiptWaitConfig,
    start: std::time::Instant,
    consecutive_misses: &mut u32,
    consecutive_transport_errors: &mut u32,
) -> Result<(), EvmError> {
    // Before the grace period a missing tx is almost certainly still propagating
    // or awaiting its first block. The receipt poll itself succeeded, so the
    // transport-error run is clean.
    if start.elapsed() < config.dropped_grace {
        *consecutive_transport_errors = 0;
        return Ok(());
    }

    // A transport error here is not a confirmed absence; retry rather than
    // counting it toward the drop threshold.
    let mempool_tx = match provider.get_transaction_by_hash(tx_hash).await {
        Ok(mempool_tx) => {
            *consecutive_transport_errors = 0;
            mempool_tx
        }
        Err(transport_error) => {
            // A transport error is not a confirmed absence, so it breaks the run
            // of consecutive confirmed absences the drop threshold relies on.
            // Reset the miss count: otherwise an interleaved blip
            // (absent, transport error, absent, absent) could accumulate to the
            // threshold and falsely declare a live-but-lagging tx dropped,
            // prompting a resubmit of a transaction that is still in flight.
            *consecutive_misses = 0;
            note_receipt_poll_transport_error(
                consecutive_transport_errors,
                transport_error,
                "checking mempool",
            )?;
            return Ok(());
        }
    };

    // Still in the mempool: not dropped, so reset the miss run.
    if mempool_tx.is_some() {
        *consecutive_misses = 0;
        return Ok(());
    }

    // Absent from both the receipt lookup and the mempool past the grace period.
    *consecutive_misses += 1;

    if *consecutive_misses >= config.dropped_consecutive_misses {
        return Err(EvmError::TransactionDropped {
            tx_hash,
            elapsed_secs: start.elapsed().as_secs(),
        });
    }

    Ok(())
}

/// Record a transport error during a receipt-wait poll and enforce the
/// consecutive-error cap. Returns `Err(Transport)` once
/// `MAX_CONSECUTIVE_TRANSPORT_ERRORS` polls in a row have failed (the RPC is down,
/// not blipping); otherwise logs and returns `Ok(())` so the caller retries.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
fn note_receipt_poll_transport_error(
    consecutive_transport_errors: &mut u32,
    transport_error: alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
    context: &str,
) -> Result<(), EvmError> {
    *consecutive_transport_errors += 1;

    if *consecutive_transport_errors >= MAX_CONSECUTIVE_TRANSPORT_ERRORS {
        return Err(EvmError::Transport(transport_error));
    }

    warn!(
        target: "evm",
        ?transport_error,
        consecutive_transport_errors = *consecutive_transport_errors,
        context,
        "Transient transport error during receipt wait; retrying"
    );

    Ok(())
}

/// Read-only EVM access wrapping a bare [`Provider`].
///
/// Use this when you need an [`Evm`] implementation but don't have
/// (or need) signing capabilities. Wraps any `Provider` into an `Evm`
/// with the default error-decoded `call` implementation.
pub struct ReadOnlyEvm<P> {
    provider: P,
}

impl<P> ReadOnlyEvm<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<P> Evm for ReadOnlyEvm<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    type Provider = P;

    fn provider(&self) -> &P {
        &self.provider
    }
}

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    use alloy::primitives::Bloom;
    use alloy::primitives::{Address, U256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    use alloy::rpc::types::TransactionReceipt;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol;
    use std::sync::Arc;

    use super::*;

    /// Build a mock provider whose `eth_blockNumber` returns the given sequence
    /// of block numbers in order, one per call.
    fn mock_block_number_provider(block_numbers: &[u64]) -> impl Provider + Clone {
        let asserter = Asserter::new();
        for block in block_numbers {
            asserter.push_success(&serde_json::Value::from(*block));
        }
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    #[tokio::test]
    async fn wait_for_node_sync_succeeds_when_node_is_already_at_required_block() {
        let provider = mock_block_number_provider(&[10]);

        wait_for_node_sync(&provider, 10, Duration::ZERO, 5)
            .await
            .expect("node is at required block — should succeed immediately");
    }

    #[tokio::test]
    async fn wait_for_node_sync_succeeds_when_node_is_ahead_of_required_block() {
        let provider = mock_block_number_provider(&[15]);

        wait_for_node_sync(&provider, 10, Duration::ZERO, 5)
            .await
            .expect("node tip > required block — should succeed immediately");
    }

    #[tokio::test]
    async fn wait_for_node_sync_retries_until_node_catches_up() {
        // Node returns blocks 8, 9 (below required 10), then 10 (at required).
        let provider = mock_block_number_provider(&[8, 9, 10]);

        wait_for_node_sync(&provider, 10, Duration::ZERO, 5)
            .await
            .expect("node catches up on third poll — should succeed");
    }

    #[tokio::test]
    async fn wait_for_node_sync_fails_after_budget_exhausted() {
        // Node always reports block 5, never reaching required block 10.
        let provider = mock_block_number_provider(&[5, 5, 5]);

        let error = wait_for_node_sync(&provider, 10, Duration::ZERO, 3)
            .await
            .expect_err("budget exhausted — should fail");

        assert!(
            matches!(
                error,
                EvmError::NodeBehindRequiredBlock {
                    observed_tip: 5,
                    required_block: 10,
                    attempts: 3,
                }
            ),
            "expected NodeBehindRequiredBlock with correct fields, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn wait_for_node_sync_fails_with_transport_error_when_all_attempts_fail() {
        let asserter = Asserter::new();
        // Push 3 transport error responses; the exact error returned is an
        // implementation detail — the contract is that EvmError::Transport is
        // propagated after the budget is exhausted with all-transport errors.
        for iteration in 1u32..=3 {
            asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: format!("connection refused attempt {iteration}").into(),
                data: None,
            });
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = wait_for_node_sync(&provider, 10, Duration::ZERO, 3).await;

        let error =
            result.expect_err("all transport errors should propagate as EvmError::Transport");
        assert!(
            matches!(error, EvmError::Transport(_)),
            "expected EvmError::Transport after exhausting budget with all-transport errors, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn wait_for_node_sync_succeeds_when_second_attempt_succeeds_after_transport_error() {
        let asserter = Asserter::new();
        // First attempt: transport error; second attempt: block 10 (at required).
        let error_payload = alloy::rpc::json_rpc::ErrorPayload {
            code: -32000,
            message: "connection refused".into(),
            data: None,
        };
        asserter.push_failure(error_payload);
        asserter.push_success(&serde_json::Value::from(10u64));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        wait_for_node_sync(&provider, 10, Duration::ZERO, 5)
            .await
            .expect("first transport error then success at block 10 — should succeed");
    }

    /// Verifies that when early attempts observe a stale tip (poll succeeded)
    /// and the final attempt returns a transport error, the function returns
    /// `NodeBehindRequiredBlock` (not `Transport`) using the last observed tip.
    ///
    /// This distinguishes the mixed case (some polls succeeded, node stale)
    /// from the all-transport-error case (every poll failed before seeing any
    /// block number). Only the all-transport-error case returns `Transport`.
    #[tokio::test]
    async fn wait_for_node_sync_returns_node_behind_when_stale_then_transport_error() {
        let asserter = Asserter::new();
        let stale_tip = 5u64;
        let required_block = 10u64;
        // First two attempts return a stale block number.
        asserter.push_success(&serde_json::Value::from(stale_tip));
        asserter.push_success(&serde_json::Value::from(stale_tip));
        // Third (final) attempt returns a transport error.
        asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
            code: -32000,
            message: "connection refused on final attempt".into(),
            data: None,
        });
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = wait_for_node_sync(&provider, required_block, Duration::ZERO, 3)
            .await
            .expect_err("mixed stale-then-transport must fail");

        assert!(
            matches!(
                error,
                EvmError::NodeBehindRequiredBlock {
                    observed_tip: 5,
                    required_block: 10,
                    attempts: 3,
                }
            ),
            "must return NodeBehindRequiredBlock with last observed tip when at \
             least one poll succeeded, got: {error:?}"
        );
    }

    sol!(
        #![sol(all_derives = true, rpc)]
        TestERC20,
        env!("ST0X_TEST_ERC20_ABI")
    );

    sol!(
        #![sol(all_derives = true, rpc)]
        IERC20,
        env!("ST0X_IERC20_ABI")
    );

    fn anvil_signer(anvil: &AnvilInstance) -> EthereumWallet {
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        EthereumWallet::from(signer)
    }

    #[tokio::test]
    async fn read_only_evm_call_returns_view_result() {
        let anvil = Anvil::new().spawn();
        let url = anvil.endpoint_url();
        let deploy_provider = ProviderBuilder::new()
            .wallet(anvil_signer(&anvil))
            .connect_http(url.clone());

        let token = TestERC20::deploy(&deploy_provider).await.unwrap();
        let token_address = *token.address();

        let read_only = ReadOnlyEvm::new(
            ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(url),
        );

        let total_supply: U256 = read_only
            .call::<NoOpErrorRegistry, _>(token_address, IERC20::totalSupplyCall {})
            .await
            .unwrap();

        assert_eq!(total_supply, U256::ZERO);
    }

    #[tokio::test]
    async fn read_only_evm_call_decodes_revert_on_failure() {
        let anvil = Anvil::new().spawn();
        let read_only = ReadOnlyEvm::new(
            ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(anvil.endpoint_url()),
        );

        let error = read_only
            .call::<NoOpErrorRegistry, _>(
                Address::random(),
                IERC20::balanceOfCall {
                    account: Address::ZERO,
                },
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, EvmError::AbiDecode(_)),
            "expected AbiDecode error for call to non-contract address \
             (empty return data), got: {error:?}"
        );
    }

    #[tokio::test]
    async fn arc_evm_delegates_call() {
        let anvil = Anvil::new().spawn();
        let url = anvil.endpoint_url();
        let deploy_provider = ProviderBuilder::new()
            .wallet(anvil_signer(&anvil))
            .connect_http(url.clone());

        let token = TestERC20::deploy(&deploy_provider).await.unwrap();
        let token_address = *token.address();

        let read_only = Arc::new(ReadOnlyEvm::new(
            ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(url),
        ));

        let total_supply: U256 = read_only
            .call::<NoOpErrorRegistry, _>(token_address, IERC20::totalSupplyCall {})
            .await
            .unwrap();

        assert_eq!(total_supply, U256::ZERO);
    }

    #[tokio::test]
    async fn arc_evm_exposes_provider() {
        let anvil = Anvil::new().spawn();
        let read_only = Arc::new(ReadOnlyEvm::new(
            ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(anvil.endpoint_url()),
        ));

        let block_number = read_only.provider().get_block_number().await.unwrap();

        assert_eq!(block_number, 0);
    }

    /// Parser that succeeds with a JSON value (`Some`) or fails (`None`).
    #[allow(dead_code)]
    struct MaybeParser(Option<serde_json::Value>);

    impl Parser for MaybeParser {
        type Error = EvmError;

        fn parse<Target: serde::de::DeserializeOwned>(self) -> Result<Target, Self::Error> {
            let Self(value) = self;

            value.map_or_else(
                || {
                    Err(EvmError::WalletConfigParse(
                        "intentional test parse failure".into(),
                    ))
                },
                |json| {
                    serde_json::from_value(json)
                        .map_err(|error| EvmError::WalletConfigParse(Box::new(error)))
                },
            )
        }
    }

    #[cfg(feature = "local-signer")]
    #[tokio::test]
    async fn wallet_kind_private_key_dispatch_succeeds() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let private_key = alloy::primitives::B256::random();
        let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
        let expected_address = signer.address();

        let ctx = WalletCtx {
            settings: MaybeParser(Some(serde_json::json!(null))),
            credentials: MaybeParser(Some(serde_json::json!({
                "private_key": format!("{private_key}")
            }))),
            provider,
            required_confirmations: 1,
        };

        let wallet = WalletKind::PrivateKey.try_into_wallet(ctx).await.unwrap();

        assert_eq!(
            wallet.address(),
            expected_address,
            "wallet address should match the private key's address"
        );
    }

    #[cfg(feature = "local-signer")]
    #[tokio::test]
    async fn wallet_kind_private_key_credentials_parse_error() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let ctx = WalletCtx {
            settings: MaybeParser(Some(serde_json::json!(null))),
            credentials: MaybeParser(None),
            provider,
            required_confirmations: 1,
        };

        let result = WalletKind::PrivateKey.try_into_wallet(ctx).await;
        let Err(error) = result else {
            panic!("expected WalletConfigParse error, got Ok");
        };

        assert!(
            matches!(error, EvmError::WalletConfigParse(_)),
            "expected WalletConfigParse error, got: {error:?}"
        );
    }

    #[cfg(feature = "turnkey")]
    #[tokio::test]
    async fn wallet_kind_turnkey_settings_parse_error() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let ctx = WalletCtx {
            settings: MaybeParser(None),
            credentials: MaybeParser(Some(serde_json::json!({}))),
            provider,
            required_confirmations: 1,
        };

        let result = WalletKind::Turnkey.try_into_wallet(ctx).await;
        let Err(error) = result else {
            panic!("expected WalletConfigParse error from settings parse, got Ok");
        };

        assert!(
            matches!(error, EvmError::WalletConfigParse(_)),
            "expected WalletConfigParse error from settings parse, got: {error:?}"
        );
    }

    #[cfg(feature = "turnkey")]
    #[tokio::test]
    async fn wallet_kind_turnkey_credentials_parse_error() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let ctx = WalletCtx {
            settings: MaybeParser(Some(serde_json::json!({
                "address": format!("{}", Address::random()),
                "organization_id": "org-test-123"
            }))),
            credentials: MaybeParser(None),
            provider,
            required_confirmations: 1,
        };

        let result = WalletKind::Turnkey.try_into_wallet(ctx).await;
        let Err(error) = result else {
            panic!("expected WalletConfigParse error from credentials parse, got Ok");
        };

        assert!(
            matches!(error, EvmError::WalletConfigParse(_)),
            "expected WalletConfigParse error from credentials parse, got: {error:?}"
        );
    }

    #[cfg(feature = "turnkey")]
    #[tokio::test]
    async fn wallet_kind_turnkey_rejects_blank_organization_id_during_parse() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let ctx = WalletCtx {
            settings: MaybeParser(Some(serde_json::json!({
                "address": format!("{}", Address::random()),
                "organization_id": "   "
            }))),
            credentials: MaybeParser(Some(serde_json::json!({
                "api_private_key":
                    "0000000000000000000000000000000000000000000000000000000000000001"
            }))),
            provider,
            required_confirmations: 1,
        };

        let result = WalletKind::Turnkey.try_into_wallet(ctx).await;
        let Err(error) = result else {
            panic!("expected blank organization ID to fail during parsing");
        };

        assert!(
            matches!(error, EvmError::WalletConfigParse(_)),
            "expected WalletConfigParse error, got: {error:?}"
        );
    }

    #[cfg(feature = "turnkey")]
    #[tokio::test]
    async fn wallet_kind_turnkey_rejects_invalid_api_key_during_parse() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let ctx = WalletCtx {
            settings: MaybeParser(Some(serde_json::json!({
                "address": format!("{}", Address::random()),
                "organization_id": "org-test-123"
            }))),
            credentials: MaybeParser(Some(serde_json::json!({
                "api_private_key": "not-a-p256-private-key"
            }))),
            provider,
            required_confirmations: 1,
        };

        let result = WalletKind::Turnkey.try_into_wallet(ctx).await;
        let Err(error) = result else {
            panic!("expected malformed API private key to fail during parsing");
        };

        assert!(
            matches!(error, EvmError::WalletConfigParse(_)),
            "expected WalletConfigParse error, got: {error:?}"
        );
    }

    #[cfg(feature = "local-signer")]
    #[tokio::test]
    async fn wait_for_receipt_returns_receipt_with_single_confirmation() {
        let anvil = Anvil::new().spawn();
        let signer = anvil_signer(&anvil);
        let provider = ProviderBuilder::new()
            .wallet(signer)
            .connect_http(anvil.endpoint_url());

        let tx = TransactionRequest::default()
            .to(Address::ZERO)
            .value(U256::ZERO);
        let pending = provider.send_transaction(tx).await.unwrap();
        let tx_hash = *pending.tx_hash();

        let read_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let receipt = wait_for_receipt(&read_provider, tx_hash, 1).await.unwrap();

        assert_eq!(receipt.transaction_hash, tx_hash);
        assert!(receipt.status());
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn wait_for_receipt_gives_up_after_consecutive_transport_errors() {
        // A transient transport error must not abort the wait on the first
        // hiccup (a live, broadcast tx would be reported failed), so errors are
        // retried -- but only up to a cap. A sustained run with no successful
        // poll means the RPC is unreachable, so the wait must fail fast with the
        // transport error at the cap instead of spinning the full inclusion
        // timeout against a dead endpoint.
        let asserter = Asserter::new();
        for iteration in 0..(MAX_CONSECUTIVE_TRANSPORT_ERRORS + 2) {
            asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: format!("connection refused {iteration}").into(),
                data: None,
            });
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = wait_for_receipt_with_config(
            &provider,
            alloy::primitives::B256::random(),
            1,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(1),
                // Generous, so the consecutive-error cap is what trips, not the
                // inclusion timeout.
                inclusion_timeout: std::time::Duration::from_secs(30),
                confirmation_timeout: std::time::Duration::from_secs(1),
                dropped_grace: std::time::Duration::from_secs(60),
                dropped_consecutive_misses: 3,
            },
        )
        .await;

        assert!(
            matches!(result, Err(EvmError::Transport(_))),
            "a dead RPC must fail fast with Transport at the consecutive-error cap, \
             not spin the inclusion timeout, got: {result:?}"
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn wait_for_receipt_gives_up_when_only_mempool_polls_fail() {
        // Receipt polls succeed (tx not yet mined -> Ok(None)) while every mempool
        // poll fails -- plausible on a load-balanced provider routing the two
        // methods to different backends. The transport-error run must still reach
        // the cap: the receipt success must NOT reset it before the mempool poll
        // fails, or this case would spin the full inclusion timeout instead of
        // failing fast.
        let asserter = Asserter::new();
        for iteration in 0..(MAX_CONSECUTIVE_TRANSPORT_ERRORS + 2) {
            // get_transaction_receipt -> Ok(None): tx not yet mined.
            asserter.push_success(&serde_json::Value::Null);
            // get_transaction_by_hash -> transport error.
            asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: format!("mempool backend down {iteration}").into(),
                data: None,
            });
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = wait_for_receipt_with_config(
            &provider,
            alloy::primitives::B256::random(),
            1,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(1),
                inclusion_timeout: std::time::Duration::from_secs(30),
                confirmation_timeout: std::time::Duration::from_secs(1),
                // Check the mempool from the first poll so the interleave runs.
                dropped_grace: std::time::Duration::ZERO,
                // High, so the drop check never fires before the transport cap.
                dropped_consecutive_misses: 100,
            },
        )
        .await;

        assert!(
            matches!(result, Err(EvmError::Transport(_))),
            "mempool-only transport failures must still reach the fail-fast cap, got: {result:?}"
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    fn mined_receipt(tx_hash: alloy::primitives::B256) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs: vec![],
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: Some(alloy::primitives::B256::random()),
            block_number: Some(0),
            gas_used: 21000,
            effective_gas_price: 1,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn wait_for_receipt_gives_up_in_confirmation_phase_after_consecutive_errors() {
        // Phase 1 succeeds (the tx is mined), then every confirmation-depth poll
        // (get_block_number) fails. The Phase 2 cap must fire so a dead RPC during
        // confirmation polling fails fast instead of spinning the confirmation
        // timeout. Phase 2 has its own counter, untested before this.
        let tx_hash = alloy::primitives::B256::random();
        let asserter = Asserter::new();
        // Phase 1: get_transaction_receipt -> Some(mined receipt at block 0).
        asserter.push_success(&mined_receipt(tx_hash));
        // Phase 2: every get_block_number poll errors.
        for iteration in 0..(MAX_CONSECUTIVE_TRANSPORT_ERRORS + 2) {
            asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: format!("node down {iteration}").into(),
                data: None,
            });
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = wait_for_receipt_with_config(
            &provider,
            tx_hash,
            // Require depth > 1 so Phase 2 must poll the tip past the receipt block.
            2,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(1),
                inclusion_timeout: std::time::Duration::from_secs(1),
                // Generous, so the cap trips, not the confirmation timeout.
                confirmation_timeout: std::time::Duration::from_secs(30),
                dropped_grace: std::time::Duration::from_secs(60),
                dropped_consecutive_misses: 3,
            },
        )
        .await;

        assert!(
            matches!(result, Err(EvmError::Transport(_))),
            "a dead RPC during confirmation polling must fail fast at the cap, got: {result:?}"
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn wait_for_receipt_tolerates_transient_errors_separated_by_a_success() {
        // (MAX-1) receipt-poll errors, then ONE successful poll, then (MAX-1) more
        // errors, then the receipt. The total error count exceeds the cap, but it
        // never reaches MAX *in a row* -- the success resets the run. The wait must
        // therefore succeed, proving the counter is consecutive, not cumulative.
        let tx_hash = alloy::primitives::B256::random();
        let asserter = Asserter::new();
        for iteration in 0..(MAX_CONSECUTIVE_TRANSPORT_ERRORS - 1) {
            asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: format!("blip a {iteration}").into(),
                data: None,
            });
        }
        // Receipt poll Ok(None): tx not yet mined. Before the grace window, this is
        // a clean poll that resets the transport-error run.
        asserter.push_success(&serde_json::Value::Null);
        for iteration in 0..(MAX_CONSECUTIVE_TRANSPORT_ERRORS - 1) {
            asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: format!("blip b {iteration}").into(),
                data: None,
            });
        }
        // The receipt finally arrives, ending Phase 1.
        asserter.push_success(&mined_receipt(tx_hash));
        // Phase 2 with required_confirmations = 1: the first block-number poll
        // satisfies the depth immediately.
        asserter.push_success(&serde_json::Value::from(0u64));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = wait_for_receipt_with_config(
            &provider,
            tx_hash,
            1,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(1),
                // Generous, so the wait does not time out before the receipt lands.
                inclusion_timeout: std::time::Duration::from_secs(30),
                confirmation_timeout: std::time::Duration::from_secs(1),
                // Large, so the Ok(None) poll stays in the before-grace reset path.
                dropped_grace: std::time::Duration::from_secs(60),
                dropped_consecutive_misses: 3,
            },
        )
        .await;

        let receipt = result.unwrap();
        assert_eq!(receipt.transaction_hash, tx_hash);
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn wait_for_receipt_confirmation_tolerates_transient_errors_separated_by_a_success() {
        // Phase 2 twin of the Phase 1 reset test above: (MAX-1) block-number
        // errors, one successful block-number poll that does not yet satisfy the
        // depth, then (MAX-1) more errors, then a block number that meets the
        // depth. The total error count exceeds the cap but never reaches MAX in a
        // row -- the mid-sequence success resets Phase 2's own counter -- so the
        // wait must succeed, proving the confirmation-phase counter is
        // consecutive, not cumulative.
        let tx_hash = alloy::primitives::B256::random();
        let asserter = Asserter::new();
        // Phase 1: the tx is mined at block 0.
        asserter.push_success(&mined_receipt(tx_hash));
        // Phase 2 first run of transient block-number errors.
        for iteration in 0..(MAX_CONSECUTIVE_TRANSPORT_ERRORS - 1) {
            asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: format!("blip a {iteration}").into(),
                data: None,
            });
        }
        // A successful block-number poll at the receipt block: confirmations = 1,
        // below the required depth of 2, so polling continues -- but the success
        // resets the transport-error run.
        asserter.push_success(&serde_json::Value::from(0u64));
        // Phase 2 second run of transient block-number errors.
        for iteration in 0..(MAX_CONSECUTIVE_TRANSPORT_ERRORS - 1) {
            asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: format!("blip b {iteration}").into(),
                data: None,
            });
        }
        // The tip advances one block: confirmations = 2, satisfying the depth.
        asserter.push_success(&serde_json::Value::from(1u64));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = wait_for_receipt_with_config(
            &provider,
            tx_hash,
            // Depth > 1 so Phase 2 must poll the tip past the receipt block.
            2,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(1),
                inclusion_timeout: std::time::Duration::from_secs(1),
                // Generous, so the cap logic is exercised, not the timeout.
                confirmation_timeout: std::time::Duration::from_secs(30),
                dropped_grace: std::time::Duration::from_secs(60),
                dropped_consecutive_misses: 3,
            },
        )
        .await;

        let receipt = result.unwrap();
        assert_eq!(receipt.transaction_hash, tx_hash);
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn wait_for_receipt_mempool_transport_error_does_not_trigger_false_drop() {
        // With dropped_consecutive_misses = 3: two confirmed absences straddle a
        // single mempool transport error, then two more absences, then the tx is
        // mined. The transport error must reset the miss run, so the drop
        // threshold is never reached and the wait returns the receipt. Without the
        // reset the run would be [1, (blip), 2, 3] and falsely declare the tx
        // dropped on the fourth poll -- before it is seen mined.
        let tx_hash = alloy::primitives::B256::random();
        let asserter = Asserter::new();
        // Poll 1: receipt None, mempool absent -> miss 1.
        asserter.push_success(&serde_json::Value::Null);
        asserter.push_success(&serde_json::Value::Null);
        // Poll 2: receipt None, mempool transport error -> reset the miss run.
        asserter.push_success(&serde_json::Value::Null);
        asserter.push_failure(alloy::rpc::json_rpc::ErrorPayload {
            code: -32000,
            message: "mempool backend blip".into(),
            data: None,
        });
        // Poll 3: receipt None, mempool absent -> miss 1 (not 2).
        asserter.push_success(&serde_json::Value::Null);
        asserter.push_success(&serde_json::Value::Null);
        // Poll 4: receipt None, mempool absent -> miss 2 (not 3 -> no false drop).
        asserter.push_success(&serde_json::Value::Null);
        asserter.push_success(&serde_json::Value::Null);
        // Poll 5: the tx is finally mined at block 0, ending Phase 1.
        asserter.push_success(&mined_receipt(tx_hash));
        // Phase 2 with required_confirmations = 1: satisfied immediately.
        asserter.push_success(&serde_json::Value::from(0u64));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = wait_for_receipt_with_config(
            &provider,
            tx_hash,
            1,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(1),
                inclusion_timeout: std::time::Duration::from_secs(30),
                confirmation_timeout: std::time::Duration::from_secs(1),
                // Check the mempool from the first poll so the interleave runs.
                dropped_grace: std::time::Duration::ZERO,
                dropped_consecutive_misses: 3,
            },
        )
        .await;

        let receipt = result.unwrap();
        assert_eq!(receipt.transaction_hash, tx_hash);
    }

    #[cfg(feature = "local-signer")]
    #[tokio::test]
    async fn wait_for_receipt_detects_dropped_tx() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let fake_hash = alloy::primitives::B256::random();

        let result = wait_for_receipt_with_config(
            &provider,
            fake_hash,
            1,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(50),
                inclusion_timeout: std::time::Duration::from_secs(30),
                confirmation_timeout: std::time::Duration::from_secs(60),
                dropped_grace: std::time::Duration::ZERO,
                dropped_consecutive_misses: 1,
            },
        )
        .await;

        assert!(
            matches!(result, Err(EvmError::TransactionDropped { tx_hash, .. }) if tx_hash == fake_hash),
            "expected TransactionDropped for nonexistent tx, got: {result:?}"
        );
    }

    /// Regression for RAI-904: a tx that is still visible in the mempool (not
    /// yet mined) must NEVER be reported as dropped — even on a slow chain.
    /// Before the fix, a single `get_transaction_by_hash == None` after a few
    /// polls falsely failed a live tx; now a mempool sighting keeps it alive
    /// until the inclusion timeout, surfacing `ReceiptTimeout`, not
    /// `TransactionDropped`.
    #[cfg(feature = "local-signer")]
    #[tokio::test]
    async fn wait_for_receipt_does_not_drop_tx_visible_in_mempool() {
        // `--no-mining`: the tx is accepted into the mempool but never mined,
        // so `get_transaction_receipt` stays None while `get_transaction_by_hash`
        // returns the pending tx.
        let anvil = Anvil::new().arg("--no-mining").spawn();
        let signer = anvil_signer(&anvil);
        let provider = ProviderBuilder::new()
            .wallet(signer)
            .connect_http(anvil.endpoint_url());

        let tx = TransactionRequest::default()
            .to(Address::ZERO)
            .value(U256::ZERO);
        let pending = provider.send_transaction(tx).await.unwrap();
        let tx_hash = *pending.tx_hash();

        let read_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let result = wait_for_receipt_with_config(
            &read_provider,
            tx_hash,
            1,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(50),
                inclusion_timeout: std::time::Duration::from_millis(600),
                confirmation_timeout: std::time::Duration::from_secs(60),
                // Grace already elapsed and a single miss would trip the old
                // logic — the tx is in the mempool, so it must not be dropped.
                dropped_grace: std::time::Duration::ZERO,
                dropped_consecutive_misses: 1,
            },
        )
        .await;

        assert!(
            matches!(result, Err(EvmError::ReceiptTimeout { tx_hash: ht, .. }) if ht == tx_hash),
            "a live (pending) tx must time out, not be reported dropped, got: {result:?}"
        );
    }

    /// The dropped-from-mempool check must not fire before the grace period,
    /// even for a genuinely absent tx (RAI-904).
    #[cfg(feature = "local-signer")]
    #[tokio::test]
    async fn wait_for_receipt_respects_dropped_grace() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let fake_hash = alloy::primitives::B256::random();
        let grace = std::time::Duration::from_millis(400);
        let start = std::time::Instant::now();

        let result = wait_for_receipt_with_config(
            &provider,
            fake_hash,
            1,
            ReceiptWaitConfig {
                poll_interval: std::time::Duration::from_millis(50),
                inclusion_timeout: std::time::Duration::from_secs(30),
                confirmation_timeout: std::time::Duration::from_secs(60),
                dropped_grace: grace,
                dropped_consecutive_misses: 1,
            },
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            matches!(result, Err(EvmError::TransactionDropped { .. })),
            "expected TransactionDropped once grace elapsed, got: {result:?}"
        );
        assert!(
            elapsed >= grace,
            "drop must not be declared before the grace period: elapsed {elapsed:?} < {grace:?}"
        );
    }

    #[cfg(feature = "local-signer")]
    #[tokio::test]
    async fn wait_for_receipt_waits_for_multiple_confirmations() {
        // Anvil with 1-second block time auto-mines blocks.
        let anvil = Anvil::new().block_time(1).spawn();
        let signer = anvil_signer(&anvil);
        let provider = ProviderBuilder::new()
            .wallet(signer)
            .connect_http(anvil.endpoint_url());

        let tx = TransactionRequest::default()
            .to(Address::ZERO)
            .value(U256::ZERO);
        let pending = provider.send_transaction(tx).await.unwrap();
        let tx_hash = *pending.tx_hash();

        let read_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let receipt = wait_for_receipt(&read_provider, tx_hash, 3).await.unwrap();

        assert_eq!(receipt.transaction_hash, tx_hash);

        let current_block = read_provider.get_block_number().await.unwrap();
        let receipt_block = receipt.block_number.unwrap();
        let confirmations = current_block.saturating_sub(receipt_block) + 1;

        assert!(
            confirmations >= 3,
            "expected at least 3 confirmations, got {confirmations}"
        );
    }
}
