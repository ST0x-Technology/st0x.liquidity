//! Turnkey secure enclave transaction submission.
//!
//! `TurnkeyWallet` submits transactions via Turnkey's AWS Nitro secure
//! enclaves for low-latency signing (50-100ms). Like
//! [`RawPrivateKeyWallet`](super::local::RawPrivateKeyWallet), it wraps
//! the base provider with a [`WalletFiller`] -- the only difference is
//! the signer: `TurnkeySigner` (remote signing via Turnkey API) instead
//! of `PrivateKeySigner` (local key).

use alloy::consensus::SignableTransaction;
use alloy::network::{Ethereum, EthereumWallet, TxSigner};
use alloy::primitives::{Address, B256, Bytes, ChainId, Signature, TxHash, U256, hex, normalize_v};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder};
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::signers::{Error as SignerError, Result as SignerResult, Signer};
use async_trait::async_trait;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde::Serialize;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use tracing::{error, info, trace, warn};
use turnkey_api_key_stamper::{Stamp, StampHeader, TurnkeyP256ApiKey};
use turnkey_client::generated::{
    Activity, ActivityResponse, ActivityStatus, SignRawPayloadIntentV2, SignRawPayloadRequest,
    immutable::activity::v1::{SignRawPayloadResult, result},
    immutable::common::v1::{HashFunction, PayloadEncoding},
};
use turnkey_client::{RetryConfig, TurnkeyClientError};

use crate::nonce::ResettableNonceManager;
use crate::{Evm, EvmError, TryIntoWallet, Wallet, WalletCtx};

/// Turnkey organization identifier (non-secret, lives in plaintext
/// config).
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct TurnkeyOrganizationId(String);

impl TurnkeyOrganizationId {
    pub fn new(value: String) -> Self {
        Self(value)
    }
}

/// Hex-encoded P-256 API private key for Turnkey authentication
/// (secret, lives in encrypted config).
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct TurnkeyApiPrivateKey(String);

impl TurnkeyApiPrivateKey {
    pub fn new(value: String) -> Self {
        Self(value)
    }
}

impl std::fmt::Debug for TurnkeyApiPrivateKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// Errors specific to the Turnkey signing backend.
#[derive(Debug, thiserror::Error)]
pub enum TurnkeyError {
    #[error("Turnkey signer error: {0}")]
    Signer(#[from] TracingTurnkeySignerError),
}

/// Component of an ECDSA signature returned by Turnkey.
#[derive(Debug, Clone, Copy)]
pub enum SignatureComponent {
    R,
    S,
    V,
}

/// Errors that can occur when using the traced Turnkey signer.
#[derive(Debug, thiserror::Error)]
pub enum TracingTurnkeySignerError {
    #[error(transparent)]
    TurnkeyClient(#[from] TurnkeyClientError),
    #[error("invalid hex string: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("signature component {component:?} has invalid byte length: {len}")]
    BadComponentLength {
        component: SignatureComponent,
        len: usize,
    },
    #[error("signature v value {v} is not a valid recovery id")]
    UnnormalizableV { v: u8 },
    #[error("transaction is missing a chain id")]
    MissingTxChainId,
    #[error("system time is before UNIX epoch: {source}")]
    SystemTime { source: SystemTimeError },
}

/// Non-secret Turnkey configuration: wallet address and organization ID.
#[derive(Debug, Clone, Deserialize)]
pub struct TurnkeySettings {
    pub address: Address,
    pub organization_id: TurnkeyOrganizationId,
}

/// Secret Turnkey credential: the P-256 API private key.
#[derive(Clone, Deserialize)]
pub struct TurnkeyCredentials {
    pub api_private_key: TurnkeyApiPrivateKey,
}

impl std::fmt::Debug for TurnkeyCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnkeyCredentials")
            .field("api_private_key", &"[REDACTED]")
            .finish()
    }
}

/// Provider type produced by wrapping a base provider with fillers
/// and a [`WalletFiller`] backed by a Turnkey signer.
///
/// Uses [`ResettableNonceManager`] instead of alloy's default
/// `CachedNonceManager` so the nonce cache can be invalidated when
/// external processes (e.g. CLI commands) advance the chain nonce.
type SignerProvider<P> = FillProvider<
    JoinFill<
        JoinFill<
            JoinFill<
                JoinFill<JoinFill<Identity, GasFiller>, BlobGasFiller>,
                NonceFiller<ResettableNonceManager>,
            >,
            ChainIdFiller,
        >,
        WalletFiller<EthereumWallet>,
    >,
    P,
    Ethereum,
>;

/// Wallet that signs transactions via Turnkey's secure enclaves.
///
/// Stores both the base provider (exposed via [`Evm::provider()`] for
/// read-only chain access) and a signing provider (used internally by
/// [`Wallet::send()`]). This separation ensures `type Provider = P`,
/// matching the existing `dyn Wallet<Provider = RootProvider>` trait
/// objects used throughout the codebase.
pub struct TurnkeyWallet<P: Provider> {
    /// Base provider for read-only chain access (view calls, balance
    /// checks, block subscriptions).
    provider: P,
    /// Provider wrapped with gas/nonce/chain-id/wallet fillers for
    /// transaction signing and submission.
    signing_provider: SignerProvider<P>,
    /// Shared nonce manager — cloned from the one inside
    /// `signing_provider` so we can call [`invalidate()`] on "nonce
    /// too low" errors without needing to traverse the filler chain.
    nonce_manager: ResettableNonceManager,
    address: Address,
    required_confirmations: u64,
}

impl<P: Provider> std::fmt::Debug for TurnkeyWallet<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnkeyWallet")
            .field("address", &self.address)
            .field("required_confirmations", &self.required_confirmations)
            .finish_non_exhaustive()
    }
}

impl<P: Provider + Clone + Send + Sync + 'static> TurnkeyWallet<P> {
    /// Creates a new `TurnkeyWallet` from a context containing API
    /// credentials, wallet address, and base provider.
    ///
    /// Constructs a `TurnkeySigner` using the P-256 API private key,
    /// wraps it in an `EthereumWallet`, and builds the signing
    /// provider with standard fillers. The base provider is cloned
    /// and stored separately for read-only access.
    pub async fn new(
        ctx: WalletCtx<TurnkeySettings, TurnkeyCredentials, P>,
    ) -> Result<Self, EvmError> {
        let TurnkeySettings {
            address,
            organization_id,
        } = ctx.settings;
        let TurnkeyCredentials { api_private_key } = ctx.credentials;

        let chain_id = ctx.provider.get_chain_id().await?;

        let signer = TracingTurnkeySigner::from_api_key(
            &api_private_key,
            organization_id,
            address,
            Some(chain_id),
        )
        .map_err(TurnkeyError::from)?;

        let eth_wallet = EthereumWallet::from(signer);

        let base_provider = ctx.provider.clone();
        let nonce_manager = ResettableNonceManager::default();

        let signing_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .filler(GasFiller)
            .filler(BlobGasFiller::default())
            .with_nonce_management(nonce_manager.clone())
            .filler(ChainIdFiller::default())
            .wallet(eth_wallet)
            .connect_provider(ctx.provider);

        let required_confirmations = ctx.required_confirmations;

        info!(target: "wallet", %address, required_confirmations, "Turnkey wallet initialized");

        Ok(Self {
            provider: base_provider,
            signing_provider,
            nonce_manager,
            address,
            required_confirmations,
        })
    }

    /// Creates a `TurnkeyWallet` from a pre-built `TurnkeyClient`.
    ///
    /// Used in tests to inject a client configured with a mock server
    /// base URL. Production code should use [`new`](Self::new).
    #[cfg(test)]
    async fn from_client(
        client: TracingTurnkeyClient,
        organization_id: TurnkeyOrganizationId,
        address: Address,
        provider: P,
        required_confirmations: u64,
    ) -> Result<Self, EvmError> {
        let chain_id = provider.get_chain_id().await?;

        let signer = TracingTurnkeySigner::new(client, organization_id, address, Some(chain_id));

        let eth_wallet = EthereumWallet::from(signer);

        let base_provider = provider.clone();
        let nonce_manager = ResettableNonceManager::default();

        let signing_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .filler(GasFiller)
            .filler(BlobGasFiller::default())
            .with_nonce_management(nonce_manager.clone())
            .filler(ChainIdFiller::default())
            .wallet(eth_wallet)
            .connect_provider(provider);

        Ok(Self {
            provider: base_provider,
            signing_provider,
            nonce_manager,
            address,
            required_confirmations,
        })
    }
}

struct TracingTurnkeyClient {
    http: reqwest::Client,
    base_url: String,
    api_key: TurnkeyP256ApiKey,
    retry_config: RetryConfig,
}

impl std::fmt::Debug for TracingTurnkeyClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TracingTurnkeyClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl TracingTurnkeyClient {
    fn from_api_key(api_private_key: &TurnkeyApiPrivateKey) -> Result<Self, TurnkeyClientError> {
        let TurnkeyApiPrivateKey(api_key_hex) = api_private_key;
        let api_key = TurnkeyP256ApiKey::from_strings(api_key_hex, None)?;
        Ok(Self::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("st0x-turnkey-client")
                .build()
                .map_err(TurnkeyClientError::ReqwestBuilder)?,
            "https://api.turnkey.com".to_string(),
            api_key,
            RetryConfig::default(),
        ))
    }

    #[cfg(test)]
    fn for_base_url(
        base_url: String,
        api_key: TurnkeyP256ApiKey,
    ) -> Result<Self, TurnkeyClientError> {
        Ok(Self::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("st0x-turnkey-client")
                .build()
                .map_err(TurnkeyClientError::ReqwestBuilder)?,
            base_url,
            api_key,
            RetryConfig::default(),
        ))
    }

    fn new(
        http: reqwest::Client,
        base_url: String,
        api_key: TurnkeyP256ApiKey,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            http,
            base_url,
            api_key,
            retry_config,
        }
    }

    fn current_timestamp() -> Result<u128, TracingTurnkeySignerError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|source| TracingTurnkeySignerError::SystemTime { source })
    }

    async fn sign_raw_payload(
        &self,
        organization_id: TurnkeyOrganizationId,
        timestamp_ms: u128,
        params: SignRawPayloadIntentV2,
    ) -> Result<SignRawPayloadResult, TurnkeyClientError> {
        let TurnkeyOrganizationId(organization_id) = organization_id;
        let request = SignRawPayloadRequest {
            r#type: "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2".to_string(),
            timestamp_ms: timestamp_ms.to_string(),
            parameters: Some(params),
            organization_id,
        };
        let activity = self
            .process_activity(&request, "/public/v1/submit/sign_raw_payload")
            .await?;
        let inner = activity
            .result
            .ok_or(TurnkeyClientError::MissingResult)?
            .inner
            .ok_or(TurnkeyClientError::MissingInnerResult)?;

        match inner {
            result::Inner::SignRawPayloadResult(result) => Ok(result),
            other => Err(TurnkeyClientError::UnexpectedInnerActivityResult(
                serde_json::to_string(&other)?,
            )),
        }
    }

    async fn process_activity<Request: Serialize + Sync>(
        &self,
        request: &Request,
        path: &str,
    ) -> Result<Activity, TurnkeyClientError> {
        let mut retry_count = 0;

        loop {
            let response: ActivityResponse = self.process_request(request, path).await?;
            let activity = response
                .activity
                .ok_or(TurnkeyClientError::MissingActivity)?;

            match activity.status {
                ActivityStatus::Completed => return Ok(activity),
                ActivityStatus::Pending => {
                    if retry_count >= self.retry_config.max_retries {
                        return Err(TurnkeyClientError::ExceededRetries(retry_count));
                    }

                    retry_count += 1;
                    tokio::time::sleep(self.retry_config.compute_delay(retry_count)).await;
                }
                ActivityStatus::Failed => {
                    return Err(TurnkeyClientError::ActivityFailed(activity.failure));
                }
                ActivityStatus::ConsensusNeeded => {
                    return Err(TurnkeyClientError::ActivityRequiresApproval(activity.id));
                }
                ActivityStatus::Unspecified
                | ActivityStatus::Created
                | ActivityStatus::Rejected => {
                    return Err(TurnkeyClientError::UnexpectedActivityStatus(
                        activity.status.as_str_name().to_string(),
                    ));
                }
            }
        }
    }

    async fn process_request<Request, Response>(
        &self,
        request: &Request,
        path: &str,
    ) -> Result<Response, TurnkeyClientError>
    where
        Request: Serialize + Sync,
        Response: serde::de::DeserializeOwned,
    {
        let url = format!("{}{}", self.base_url, path);
        let post_body = serde_json::to_string(request)?;
        let StampHeader { name, value } = self.api_key.stamp(post_body.as_bytes())?;
        let response = self
            .http
            .post(&url)
            .header(name, value)
            .body(post_body)
            .send()
            .await?;
        let status = response.status();
        let content_type = response.headers().get(CONTENT_TYPE).cloned();
        // Read raw bytes and parse the success body with `serde_json::from_slice`
        // so invalid UTF-8 fails fast, matching the fail-fast convention the
        // Alpaca clients follow. Lossy decoding is used only for the trace line
        // and the error-body display.
        let bytes = response.bytes().await?;

        // The successful `sign_raw_payload` body contains the ECDSA signature
        // components (r/s/v). These are signature material, not key material:
        // they do not expose the private key, and for transaction signing they
        // become public on-chain once the tx is broadcast. This signer is only
        // used for transaction signing (no off-chain raw-hash signing path), so
        // logging the body does not leak a secret.
        trace!(
            target: "wallet",
            method = "POST",
            status = %status,
            url = %url,
            body = %String::from_utf8_lossy(&bytes),
            "Turnkey API response body received"
        );

        // Error mapping below reproduces upstream `turnkey_client`'s handling
        // (including its String-valued error variants); it is not a new
        // stringly-typed-error introduction. This client is a deliberate fork
        // of `turnkey_client` to insert the response-body trace above.
        let content_type = content_type
            .ok_or(TurnkeyClientError::MissingContentTypeHeader)?
            .to_str()
            .map_err(|error| TurnkeyClientError::HeaderToStrError(error.to_string()))?
            .parse::<mime::Mime>()
            .map_err(|error| TurnkeyClientError::HeaderFromStrError(error.to_string()))?;

        if !status.is_success() {
            return Err(TurnkeyClientError::UnexpectedHttpStatus(
                status.as_u16(),
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
        }

        // Compare only the MIME essence (type/subtype) so responses that carry
        // parameters such as `application/json; charset=utf-8` are accepted;
        // strict equality against `mime::APPLICATION_JSON` rejects them.
        if content_type.essence_str() != mime::APPLICATION_JSON.essence_str() {
            return Err(TurnkeyClientError::UnexpectedMimeType(
                content_type.to_string(),
            ));
        }

        serde_json::from_slice(&bytes).map_err(|error| {
            TurnkeyClientError::Decode(String::from_utf8_lossy(&bytes).into_owned(), error)
        })
    }
}

struct TracingTurnkeySigner {
    client: TracingTurnkeyClient,
    organization_id: TurnkeyOrganizationId,
    address: Address,
    chain_id: Option<ChainId>,
}

impl std::fmt::Debug for TracingTurnkeySigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TracingTurnkeySigner")
            .field("organization_id", &self.organization_id)
            .field("address", &self.address)
            .field("chain_id", &self.chain_id)
            .finish_non_exhaustive()
    }
}

impl TracingTurnkeySigner {
    fn new(
        client: TracingTurnkeyClient,
        organization_id: TurnkeyOrganizationId,
        address: Address,
        chain_id: Option<ChainId>,
    ) -> Self {
        Self {
            client,
            organization_id,
            address,
            chain_id,
        }
    }

    fn from_api_key(
        api_private_key: &TurnkeyApiPrivateKey,
        organization_id: TurnkeyOrganizationId,
        address: Address,
        chain_id: Option<ChainId>,
    ) -> Result<Self, TracingTurnkeySignerError> {
        let client = TracingTurnkeyClient::from_api_key(api_private_key)?;
        Ok(Self::new(client, organization_id, address, chain_id))
    }

    fn parse_signature(
        response: &SignRawPayloadResult,
    ) -> Result<Signature, TracingTurnkeySignerError> {
        let r_bytes = hex::decode(&response.r)?;
        let s_bytes = hex::decode(&response.s)?;
        let v_bytes = hex::decode(&response.v)?;

        let r_arr: [u8; 32] = r_bytes.try_into().map_err(|bytes: Vec<u8>| {
            TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::R,
                len: bytes.len(),
            }
        })?;
        let r = U256::from_be_bytes(r_arr);

        let s_arr: [u8; 32] = s_bytes.try_into().map_err(|bytes: Vec<u8>| {
            TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::S,
                len: bytes.len(),
            }
        })?;
        let s = U256::from_be_bytes(s_arr);

        let [v_byte]: [u8; 1] = v_bytes.try_into().map_err(|bytes: Vec<u8>| {
            TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::V,
                len: bytes.len(),
            }
        })?;

        let parity = normalize_v(u64::from(v_byte))
            .ok_or(TracingTurnkeySignerError::UnnormalizableV { v: v_byte })?;

        Ok(Signature::new(r, s, parity))
    }
}

#[async_trait]
impl TxSigner<Signature> for TracingTurnkeySigner {
    fn address(&self) -> Address {
        self.address
    }

    async fn sign_transaction(
        &self,
        tx: &mut dyn SignableTransaction<Signature>,
    ) -> SignerResult<Signature> {
        if let Some(chain_id) = self.chain_id()
            && !tx.set_chain_id_checked(chain_id)
        {
            // `set_chain_id_checked` only returns false when the tx already
            // carries a (mismatching) chain id, so `tx.chain_id()` is always
            // Some here. The `MissingTxChainId` fallback is defensive (avoids
            // a panic the no-unwrap rule forbids) and is not expected to fire.
            let tx_chain_id = tx
                .chain_id()
                .ok_or_else(|| SignerError::other(TracingTurnkeySignerError::MissingTxChainId))?;
            return Err(SignerError::TransactionChainIdMismatch {
                signer: chain_id,
                tx: tx_chain_id,
            });
        }

        self.sign_hash(&tx.signature_hash()).await
    }
}

#[async_trait]
impl Signer for TracingTurnkeySigner {
    async fn sign_hash(&self, hash: &B256) -> SignerResult<Signature> {
        let response = self
            .client
            .sign_raw_payload(
                self.organization_id.clone(),
                TracingTurnkeyClient::current_timestamp().map_err(SignerError::other)?,
                SignRawPayloadIntentV2 {
                    sign_with: self.address.to_string(),
                    payload: hex::encode(hash),
                    encoding: PayloadEncoding::Hexadecimal,
                    hash_function: HashFunction::NoOp,
                },
            )
            .await
            .map_err(|error| SignerError::other(TracingTurnkeySignerError::TurnkeyClient(error)))?;

        Self::parse_signature(&response).map_err(SignerError::other)
    }

    fn address(&self) -> Address {
        self.address
    }

    fn chain_id(&self) -> Option<ChainId> {
        self.chain_id
    }

    fn set_chain_id(&mut self, chain_id: Option<ChainId>) {
        self.chain_id = chain_id;
    }
}

#[async_trait]
impl<P> Evm for TurnkeyWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    type Provider = P;

    fn provider(&self) -> &P {
        &self.provider
    }
}

#[async_trait]
impl<P> Wallet for TurnkeyWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    fn address(&self) -> Address {
        self.address
    }

    async fn send_pending(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TxHash, EvmError> {
        info!(target: "wallet", %contract, note, "Submitting Turnkey contract call");

        let tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.into());

        let pending = match self.signing_provider.send_transaction(tx.clone()).await {
            Ok(pending) => pending,
            Err(error) => {
                let error = EvmError::from(error);

                if !error.is_nonce_too_low() {
                    warn!(target: "wallet", %contract, note, %error, "Transaction \
                        send failed -- invalidating nonce cache to prevent nonce gap");
                    self.nonce_manager.invalidate();
                    return Err(error);
                }

                warn!(target: "wallet", %contract, note, "Nonce too low — \
                    invalidating cache and retrying (external nonce change detected)");
                self.nonce_manager.invalidate();

                self.signing_provider
                    .send_transaction(tx)
                    .await
                    .map_err(|error| {
                        let error = EvmError::from(error);
                        error!(target: "wallet", %error, "Nonce-too-low retry also failed \
                            -- invalidating nonce cache");
                        self.nonce_manager.invalidate();
                        error
                    })?
            }
        };

        let tx_hash = *pending.tx_hash();

        info!(target: "wallet", %tx_hash, note, "Transaction submitted");

        Ok(tx_hash)
    }

    async fn await_receipt(&self, tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
        let receipt =
            crate::wait_for_receipt(&self.provider, tx_hash, self.required_confirmations).await?;

        info!(target: "wallet", tx_hash = %receipt.transaction_hash, "Transaction confirmed");

        Ok(receipt)
    }

    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        let tx_hash = self.send_pending(contract, calldata, note).await?;
        self.await_receipt(tx_hash).await
    }
}

#[async_trait]
impl<P> TryIntoWallet for TurnkeyWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    type Settings = TurnkeySettings;
    type Credentials = TurnkeyCredentials;

    async fn try_from_ctx(
        ctx: WalletCtx<TurnkeySettings, TurnkeyCredentials, P>,
    ) -> Result<Self, EvmError> {
        Self::new(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::U256;
    use alloy::providers::ext::AnvilApi;
    use httpmock::MockServer;

    use super::*;

    /// Generate a fresh P-256 API key for testing.
    fn test_api_key() -> TurnkeyP256ApiKey {
        TurnkeyP256ApiKey::generate()
    }

    /// Build a Turnkey client that sends requests to the mock server.
    fn mock_client(server: &MockServer) -> TracingTurnkeyClient {
        TracingTurnkeyClient::for_base_url(server.base_url(), test_api_key()).unwrap()
    }

    /// Build a Turnkey client with a custom retry config so the retry
    /// loop can be exercised without real backoff delays.
    fn mock_client_with_retry(
        server: &MockServer,
        retry_config: RetryConfig,
    ) -> TracingTurnkeyClient {
        TracingTurnkeyClient::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap(),
            server.base_url(),
            test_api_key(),
            retry_config,
        )
    }

    /// 32-byte big-endian hex for the U256 value 1.
    const VALID_R_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    /// 32-byte big-endian hex for the U256 value 2.
    const VALID_S_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000002";

    fn signature_result(r: &str, s: &str, v: &str) -> SignRawPayloadResult {
        SignRawPayloadResult {
            r: r.to_string(),
            s: s.to_string(),
            v: v.to_string(),
        }
    }

    /// JSON body for a COMPLETED `sign_raw_payload` activity carrying the
    /// given signature components, shaped like a real Turnkey response.
    fn completed_sign_raw_payload_body(r: &str, s: &str, v: &str) -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_COMPLETED",
                "type": "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2",
                "fingerprint": "fingerprint",
                "result": {
                    "signRawPayloadResult": { "r": r, "s": s, "v": v }
                }
            }
        })
    }

    /// JSON body for a PENDING activity (no result yet).
    fn pending_activity_body() -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_PENDING",
                "type": "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2",
                "fingerprint": "fingerprint"
            }
        })
    }

    #[test]
    fn parse_signature_decodes_valid_components() {
        let result = signature_result(VALID_R_HEX, VALID_S_HEX, "00");

        let signature = TracingTurnkeySigner::parse_signature(&result).unwrap();

        assert_eq!(signature.r(), U256::from(1));
        assert_eq!(signature.s(), U256::from(2));
        assert!(!signature.v());
    }

    #[test]
    fn parse_signature_rejects_short_r_component() {
        let result = signature_result("0011", VALID_S_HEX, "00");

        let error = TracingTurnkeySigner::parse_signature(&result).unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::R,
                len: 2
            }
        ));
    }

    #[test]
    fn parse_signature_rejects_oversized_v_component() {
        let result = signature_result(VALID_R_HEX, VALID_S_HEX, "0000");

        let error = TracingTurnkeySigner::parse_signature(&result).unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::BadComponentLength {
                component: SignatureComponent::V,
                len: 2
            }
        ));
    }

    #[test]
    fn parse_signature_rejects_unnormalizable_v() {
        // v = 2 is not a valid recovery id (not 0/1, 27/28, or >= 35).
        let result = signature_result(VALID_R_HEX, VALID_S_HEX, "02");

        let error = TracingTurnkeySigner::parse_signature(&result).unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::UnnormalizableV { v: 2 }
        ));
    }

    #[tokio::test]
    async fn sign_hash_returns_signature_from_completed_activity() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_raw_payload");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(completed_sign_raw_payload_body(
                    VALID_R_HEX,
                    VALID_S_HEX,
                    "00",
                ));
        });

        let signer = TracingTurnkeySigner::new(
            mock_client(&server),
            TurnkeyOrganizationId::new("org-test".to_string()),
            Address::random(),
            Some(1),
        );

        let signature = signer.sign_hash(&B256::ZERO).await.unwrap();

        assert_eq!(signature.r(), U256::from(1));
        assert_eq!(signature.s(), U256::from(2));
        assert!(!signature.v());
    }

    #[tokio::test]
    async fn sign_raw_payload_exhausts_retries_while_activity_stays_pending() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_raw_payload");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(pending_activity_body());
        });

        // `RetryConfig::none()` caps retries at zero, so the first PENDING
        // response immediately exhausts retries with no backoff sleep.
        let client = mock_client_with_retry(&server, RetryConfig::none());

        let error = client
            .sign_raw_payload(
                TurnkeyOrganizationId::new("org-test".to_string()),
                0,
                SignRawPayloadIntentV2 {
                    sign_with: Address::random().to_string(),
                    payload: hex::encode(B256::ZERO),
                    encoding: PayloadEncoding::Hexadecimal,
                    hash_function: HashFunction::NoOp,
                },
            )
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(error, TurnkeyClientError::ExceededRetries(0)));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn send_signing_failure() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_raw_payload");
            then.status(500)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "error": "internal server error"
                }));
        });

        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());
        let client = mock_client(&server);

        let wallet = TurnkeyWallet::from_client(
            client,
            TurnkeyOrganizationId::new("org-test".to_string()),
            Address::random(),
            provider,
            1,
        )
        .await
        .unwrap();

        let error = wallet
            .send(Address::ZERO, Bytes::new(), "test signing failure")
            .await
            .unwrap_err();

        // Turnkey signer errors surface through alloy's transport
        // layer as Transport(LocalUsageError(Signer(Other(...)))) --
        // the signer failure happens inside `send_transaction`.
        assert!(
            matches!(error, EvmError::Transport(_)),
            "expected Transport error wrapping signer failure, got: {error:?}"
        );
        let error_str = format!("{error:?}");
        assert!(
            error_str.contains("TurnkeyClient"),
            "expected TurnkeyClient error in chain, got: {error_str}"
        );
        assert!(logs_contain("Turnkey API response body received"));
        assert!(logs_contain("internal server error"));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn process_request_logs_success_response_body() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "turnkey_marker": "success-body"
                }));
        });

        let client = mock_client(&server);
        let response: serde_json::Value = client
            .process_request(&serde_json::json!({"request": "body"}), "/public/v1/test")
            .await
            .unwrap();

        assert_eq!(response["turnkey_marker"], "success-body");
        assert!(logs_contain("Turnkey API response body received"));
        assert!(logs_contain("turnkey_marker"));
        assert!(logs_contain("success-body"));
    }

    #[tokio::test]
    async fn process_request_accepts_json_content_type_with_charset_parameter() {
        let server = MockServer::start();

        // A `Content-Type` carrying a `charset` parameter must still be accepted:
        // the essence (`application/json`) is what matters, not exact equality.
        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "application/json; charset=utf-8")
                .json_body(serde_json::json!({ "turnkey_marker": "ok" }));
        });

        let client = mock_client(&server);
        let response: serde_json::Value = client
            .process_request(&serde_json::json!({"request": "body"}), "/public/v1/test")
            .await
            .unwrap();

        assert_eq!(response["turnkey_marker"], "ok");
    }

    #[tokio::test]
    async fn process_request_rejects_non_json_content_type() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "text/html")
                .body("<html></html>");
        });

        let client = mock_client(&server);
        let error = client
            .process_request::<_, serde_json::Value>(
                &serde_json::json!({"request": "body"}),
                "/public/v1/test",
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TurnkeyClientError::UnexpectedMimeType(mime) if mime == "text/html"
        ));
    }

    #[tokio::test]
    async fn new_constructs_wallet_with_correct_address() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());
        let server = MockServer::start();
        let client = mock_client(&server);
        let expected_address = Address::random();

        let wallet = TurnkeyWallet::from_client(
            client,
            TurnkeyOrganizationId::new("org-test".to_string()),
            expected_address,
            provider,
            1,
        )
        .await
        .unwrap();

        assert_eq!(wallet.address(), expected_address);
    }

    // --- Integration tests (real Turnkey API + local Anvil) ---------
    //
    // These tests sign via the real Turnkey API but submit to a local
    // Anvil instance (no testnet ETH needed -- Anvil funds the address
    // via `anvil_setBalance`).
    //
    // Required env vars (set in `.env`, loaded by direnv):
    //   TURNKEY_API_PRIVATE_KEY  -- hex-encoded P-256 API private key
    //   TURNKEY_ORG_ID           -- Turnkey organization ID
    //   TURNKEY_ADDRESS          -- Ethereum address managed by Turnkey
    //
    // See `.env.example` for the template.

    /// Returns `None` when `TURNKEY_*` env vars are absent, allowing
    /// tests to skip gracefully in environments without credentials.
    fn turnkey_env() -> Option<(String, String, Address)> {
        let api_key = std::env::var("TURNKEY_API_PRIVATE_KEY").ok()?;
        let org_id = std::env::var("TURNKEY_ORG_ID").ok()?;
        let address: Address = std::env::var("TURNKEY_ADDRESS")
            .ok()?
            .parse()
            .expect("TURNKEY_ADDRESS must be valid hex address");

        Some((api_key, org_id, address))
    }

    /// Spin up Anvil, fund the Turnkey address, and return a
    /// `TurnkeyWallet` connected to the local node.
    async fn integration_wallet(
        api_key: String,
        org_id: String,
        address: Address,
    ) -> (TurnkeyWallet<impl Provider + Clone>, AnvilInstance) {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        // Fund the Turnkey-managed address on the local Anvil node.
        provider
            .anvil_set_balance(address, U256::from(10) * U256::from(10).pow(U256::from(18)))
            .await
            .expect("anvil_setBalance should succeed");

        let wallet = TurnkeyWallet::new(WalletCtx {
            settings: TurnkeySettings {
                address,
                organization_id: TurnkeyOrganizationId::new(org_id),
            },
            credentials: TurnkeyCredentials {
                api_private_key: TurnkeyApiPrivateKey::new(api_key),
            },
            provider,
            required_confirmations: 1,
        })
        .await
        .expect("failed to construct TurnkeyWallet from env vars");

        (wallet, anvil)
    }

    #[ignore = "requires TURNKEY_* env vars -- run with `cargo test -- --ignored`"]
    #[tokio::test]
    async fn turnkey_integration() {
        let (api_key, org_id, address) = turnkey_env()
            .expect("TURNKEY_API_PRIVATE_KEY, TURNKEY_ORG_ID, and TURNKEY_ADDRESS must be set");

        let (wallet, _anvil) = integration_wallet(api_key, org_id, address).await;
        let self_address = wallet.address();

        // Wallet address matches configured address.
        assert_eq!(wallet.address(), address);

        // Sequential 0-value self-transfer: exercises the full Turnkey
        // signing round-trip.
        let receipt = wallet
            .send(self_address, Bytes::new(), "integration test self-transfer")
            .await
            .expect("Turnkey signing and submission should succeed");

        assert!(
            receipt.status(),
            "self-transfer should succeed, tx: {}",
            receipt.transaction_hash
        );
        assert_eq!(
            receipt.from, self_address,
            "transaction should be from the configured wallet"
        );

        // Two parallel 0-value self-transfers to verify nonce
        // management doesn't collide under concurrent signing.
        let (receipt_a, receipt_b) = tokio::join!(
            wallet.send(self_address, Bytes::new(), "concurrent-a"),
            wallet.send(self_address, Bytes::new(), "concurrent-b"),
        );

        let receipt_a = receipt_a.expect("concurrent send A should succeed");
        let receipt_b = receipt_b.expect("concurrent send B should succeed");

        assert!(receipt_a.status(), "tx A should succeed");
        assert!(receipt_b.status(), "tx B should succeed");
        assert_ne!(
            receipt_a.transaction_hash, receipt_b.transaction_hash,
            "transactions must have different hashes"
        );
    }
}
