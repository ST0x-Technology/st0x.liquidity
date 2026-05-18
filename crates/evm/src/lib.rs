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
use rain_error_decoding::AbiDecodedErrorType;
use serde::Deserialize;
use std::sync::Arc;
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
use tokio::time::{interval, timeout};

pub mod error_decoding;
pub use error_decoding::{IntoErrorRegistry, NoOpErrorRegistry, OpenChainErrorRegistry};
use error_decoding::{decode_reverted_receipt, decode_rpc_revert};

pub mod nonce;
pub use nonce::ResettableNonceManager;

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
    #[cfg(feature = "local-signer")]
    #[error("invalid private key: {0}")]
    InvalidPrivateKey(#[from] alloy::signers::k256::ecdsa::Error),
    #[cfg(feature = "turnkey")]
    #[error("Turnkey error: {0}")]
    Turnkey(#[from] turnkey::TurnkeyError),
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

    /// Submit raw calldata with automatic revert detection.
    ///
    /// Like [`send`](Wallet::send), but checks the receipt status and
    /// returns [`EvmError::Reverted`] if the transaction reverted.
    /// Uses [`NoOpErrorRegistry`] since the caller doesn't know the
    /// contract's error types.
    async fn submit_raw(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError>
    where
        Self: Sized,
    {
        let receipt = self.send(contract, calldata.clone(), note).await?;
        decode_reverted_receipt::<NoOpErrorRegistry>(
            self.provider(),
            self.address(),
            contract,
            calldata,
            receipt,
        )
        .await
    }

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

    async fn submit_raw(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError>
    where
        Self: Sized,
    {
        let receipt = self.send(contract, calldata.clone(), note).await?;
        decode_reverted_receipt::<NoOpErrorRegistry>(
            self.provider(),
            self.address(),
            contract,
            calldata,
            receipt,
        )
        .await
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

/// Poll interval for fallback receipt polling.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const RECEIPT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Maximum time to wait for a transaction receipt before giving up.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const RECEIPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

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
        RECEIPT_POLL_INTERVAL,
        RECEIPT_TIMEOUT,
        CONFIRMATION_TIMEOUT,
    )
    .await
}

/// Maximum time to wait for confirmation depth after a receipt is found.
/// Generous (30 min) because the tx is already mined — we just need blocks.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const CONFIRMATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Number of receipt polls before checking whether the transaction was
/// dropped from the mempool. Gives the tx time to propagate before
/// concluding it was never broadcast.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
const DROPPED_TX_CHECK_AFTER_POLLS: u32 = 3;

/// Parameterized version of [`wait_for_receipt`] for testability.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
pub(crate) async fn wait_for_receipt_with_config(
    provider: &impl Provider,
    tx_hash: alloy::primitives::TxHash,
    required_confirmations: u64,
    poll_interval: std::time::Duration,
    inclusion_timeout: std::time::Duration,
    confirmation_timeout: std::time::Duration,
) -> Result<TransactionReceipt, EvmError> {
    let mut poll = interval(poll_interval);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let start = std::time::Instant::now();

    // Phase 1: wait for inclusion (with timeout).
    let (receipt, receipt_block) = timeout(inclusion_timeout, async {
        let mut poll_count = 0u32;

        loop {
            poll.tick().await;
            poll_count += 1;

            let Some(receipt) = provider.get_transaction_receipt(tx_hash).await? else {
                // After initial propagation window, check if the tx
                // still exists in the mempool. If not, it was dropped
                // and will never mine.
                if poll_count >= DROPPED_TX_CHECK_AFTER_POLLS
                    && provider.get_transaction_by_hash(tx_hash).await?.is_none()
                {
                    return Err(EvmError::TransactionDropped {
                        tx_hash,
                        elapsed_secs: start.elapsed().as_secs(),
                    });
                }

                continue;
            };

            let Some(block) = receipt.block_number else {
                continue;
            };

            return Ok::<_, EvmError>((receipt, block));
        }
    })
    .await
    .map_err(|_| EvmError::ReceiptTimeout {
        tx_hash,
        timeout_secs: inclusion_timeout.as_secs(),
    })??;

    // Phase 2: wait for confirmation depth (generous timeout).
    timeout(confirmation_timeout, async {
        loop {
            let current_block = provider.get_block_number().await?;
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
        timeout_secs: confirmation_timeout.as_secs(),
    })?
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
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{Address, U256};
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol;
    use std::sync::Arc;

    use super::*;

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
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(60),
        )
        .await;

        assert!(
            matches!(result, Err(EvmError::TransactionDropped { tx_hash, .. }) if tx_hash == fake_hash),
            "expected TransactionDropped for nonexistent tx, got: {result:?}"
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
