//! Alpaca tokenization API client for mint and redemption
//! operations.
//!
//! This module provides a client for interacting with Alpaca's
//! tokenization API, which enables converting offchain shares to
//! onchain tokens (minting) and converting onchain tokens back to
//! offchain shares (redemption).
//!
//! # API Endpoints
//!
//! - `POST /v1/accounts/{ap_account_id}/tokenization/mint` - Request mint (shares to tokens)
//! - `GET /v1/accounts/{ap_account_id}/tokenization/requests` - List/poll tokenization requests
//!
//! # Workflows
//!
//! **Mint** (TokenizedEquityMint aggregate):
//! 1. Call `request_mint` with symbol, qty, wallet
//! 2. Receive `TokenizationRequest` with `tokenization_request_id`
//! 3. Poll until status is `Completed` or `Rejected`
//!
//! **Redemption** (EquityRedemption aggregate):
//! 1. Send tokens to redemption wallet (onchain tx)
//! 2. Poll `list_requests` for Alpaca's detection
//! 3. Poll until status is `Completed` or `Rejected`

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, error, trace, warn};

use st0x_evm::{EvmError, IntoErrorRegistry, OpenChainErrorRegistry, Wallet};
use st0x_execution::{AlpacaAccountId, FractionalShares, Symbol};

use super::{Tokenizer, TokenizerError};
use crate::alpaca_wallet::{Network, PollingConfig};
use crate::bindings::IERC20;
use crate::onchain::io::{OneToOneTokenizedShares, TokenizedSymbol};
use crate::threshold::Usd;
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizationRequestId};

/// High-level service for Alpaca tokenization operations.
///
/// Wraps `AlpacaTokenizationClient` with default polling configuration.
pub(crate) struct AlpacaTokenizationService<W: Wallet> {
    client: AlpacaTokenizationClient<W>,
    polling_config: PollingConfig,
}

impl<W: Wallet> AlpacaTokenizationService<W> {
    /// Create a new tokenization service.
    pub(crate) fn new(
        base_url: String,
        account_id: AlpacaAccountId,
        api_key: String,
        api_secret: String,
        wallet: W,
        redemption_wallet: Address,
    ) -> Self {
        let client = AlpacaTokenizationClient::new(
            base_url,
            account_id,
            api_key,
            api_secret,
            wallet,
            redemption_wallet,
        );

        Self {
            client,
            polling_config: PollingConfig::default(),
        }
    }

    /// Request a mint operation to convert offchain shares to onchain tokens.
    pub(crate) async fn request_mint(
        &self,
        underlying_symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let request = MintRequest {
            underlying_symbol,
            quantity,
            issuer: Issuer::new("st0x"),
            network: Network::new("base"),
            wallet,
            issuer_request_id,
        };
        self.client.request_mint(request).await
    }

    /// Poll a mint request until it reaches a terminal state.
    pub(crate) async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        self.client
            .poll_until_terminal(id, &self.polling_config)
            .await
    }

    /// Returns the redemption wallet address.
    pub(crate) fn redemption_wallet(&self) -> Address {
        self.client.redemption_wallet
    }

    /// Send tokens to the redemption wallet to initiate a redemption.
    pub(crate) async fn send_for_redemption<Registry: IntoErrorRegistry>(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, AlpacaTokenizationError> {
        self.client
            .send_tokens_for_redemption::<Registry>(token, amount)
            .await
    }

    /// Poll until Alpaca detects a redemption transfer.
    pub(crate) async fn poll_for_redemption(
        &self,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        self.client
            .poll_for_redemption_detection(tx_hash, &self.polling_config)
            .await
    }

    /// Poll a redemption request until it reaches a terminal state.
    pub(crate) async fn poll_redemption_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        self.client
            .poll_until_terminal(id, &self.polling_config)
            .await
    }

    /// List all tokenization requests.
    pub(crate) async fn list_requests(
        &self,
    ) -> Result<Vec<TokenizationRequest>, AlpacaTokenizationError> {
        self.client
            .list_requests(ListRequestsParams::default())
            .await
    }

    #[cfg(test)]
    fn new_with_client(client: AlpacaTokenizationClient<W>, polling_config: PollingConfig) -> Self {
        Self {
            client,
            polling_config,
        }
    }
}

/// Type of tokenization request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TokenizationRequestType {
    Mint,
    Redeem,
}

impl std::fmt::Display for TokenizationRequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mint => write!(f, "mint"),
            Self::Redeem => write!(f, "redeem"),
        }
    }
}

/// Status of a tokenization request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TokenizationRequestStatus {
    Pending,
    Completed,
    Rejected,
}

impl std::fmt::Display for TokenizationRequestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Completed => write!(f, "completed"),
            Self::Rejected => write!(f, "rejected"),
        }
    }
}

/// Token issuer identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Issuer(String);

impl Issuer {
    fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

/// A tokenization request returned by the Alpaca API.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(crate) struct TokenizationRequest {
    #[serde(rename = "tokenization_request_id")]
    pub(crate) id: TokenizationRequestId,
    #[serde(default)]
    pub(crate) r#type: Option<TokenizationRequestType>,
    pub(crate) status: TokenizationRequestStatus,
    pub(crate) underlying_symbol: Symbol,
    #[serde(deserialize_with = "deserialize_tokenized_symbol")]
    pub(crate) token_symbol: Option<TokenizedSymbol<OneToOneTokenizedShares>>,
    #[serde(rename = "qty")]
    pub(crate) quantity: FractionalShares,
    issuer: Issuer,
    network: Network,
    #[serde(rename = "wallet_address")]
    pub(crate) wallet: Option<Address>,
    pub(crate) issuer_request_id: Option<IssuerRequestId>,
    #[serde(default, deserialize_with = "deserialize_tx_hash")]
    pub(crate) tx_hash: Option<TxHash>,
    pub(crate) fees: Option<Usd>,
    pub(crate) created_at: DateTime<Utc>,
    updated_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
impl TokenizationRequest {
    /// Create a mock TokenizationRequest for testing.
    pub(crate) fn mock(status: TokenizationRequestStatus) -> Self {
        Self {
            id: TokenizationRequestId("MOCK_REQ_ID".to_string()),
            r#type: None,
            status,
            underlying_symbol: Symbol::new("AAPL").unwrap(),
            token_symbol: None,
            quantity: FractionalShares::ZERO,
            issuer: Issuer::new("alpaca"),
            network: Network::new("base"),
            wallet: None,
            issuer_request_id: None,
            tx_hash: None,
            fees: None,
            created_at: Utc::now(),
            updated_at: None,
        }
    }

    /// Create a mock completed TokenizationRequest with tx_hash for testing.
    pub(crate) fn mock_completed() -> Self {
        Self {
            id: TokenizationRequestId("MOCK_REQ_ID".to_string()),
            r#type: None,
            status: TokenizationRequestStatus::Completed,
            underlying_symbol: Symbol::new("AAPL").unwrap(),
            token_symbol: None,
            quantity: FractionalShares::ZERO,
            issuer: Issuer::new("alpaca"),
            network: Network::new("base"),
            wallet: None,
            issuer_request_id: None,
            tx_hash: Some(TxHash::ZERO),
            fees: None,
            created_at: Utc::now(),
            updated_at: None,
        }
    }
}

fn deserialize_tokenized_symbol<'de, D>(
    deserializer: D,
) -> Result<Option<TokenizedSymbol<OneToOneTokenizedShares>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.filter(|symbol_str| !symbol_str.is_empty())
        .map(|symbol_str| symbol_str.parse().map_err(serde::de::Error::custom))
        .transpose()
}

/// Deserialize tx_hash that may be an empty string (Alpaca quirk).
fn deserialize_tx_hash<'de, D>(deserializer: D) -> Result<Option<TxHash>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;

    match opt {
        None => Ok(None),
        Some(value) if value.is_empty() => Ok(None),
        Some(value) => value.parse().map(Some).map_err(serde::de::Error::custom),
    }
}

/// Request body for initiating a mint operation.
#[derive(Debug, Clone, Serialize)]
struct MintRequest {
    underlying_symbol: Symbol,
    #[serde(rename = "qty")]
    quantity: FractionalShares,
    issuer: Issuer,
    network: Network,
    #[serde(rename = "wallet_address")]
    wallet: Address,
    issuer_request_id: IssuerRequestId,
}

/// Parameters for filtering tokenization requests.
#[derive(Debug, Clone, Default)]
struct ListRequestsParams {
    request_type: Option<TokenizationRequestType>,
    status: Option<TokenizationRequestStatus>,
    underlying_symbol: Option<Symbol>,
}

/// Errors that can occur when interacting with the Alpaca tokenization API.
#[derive(Debug, Error)]
pub(crate) enum AlpacaTokenizationError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("Failed to parse API response: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("API error (status {status}): {message}")]
    ApiError { status: StatusCode, message: String },

    #[error("Insufficient position for symbol: {symbol}")]
    InsufficientPosition { symbol: Symbol },

    #[error("Account not supported for tokenization")]
    UnsupportedAccount,

    #[error("Invalid parameters: {details}")]
    InvalidParameters { details: String },

    #[error("Request not found: {id}")]
    RequestNotFound { id: TokenizationRequestId },

    #[error("EVM error: {0}")]
    Evm(#[from] EvmError),

    #[error("Poll timeout after {elapsed:?}")]
    PollTimeout { elapsed: Duration },
}

impl AlpacaTokenizationError {
    /// Returns the HTTP status code if this error was caused by an API response.
    pub(crate) fn status_code(&self) -> Option<StatusCode> {
        match self {
            Self::ApiError { status, .. } => Some(*status),
            Self::Reqwest(error) => error.status(),
            _ => None,
        }
    }
}

fn map_mint_error(status: StatusCode, message: String, symbol: Symbol) -> AlpacaTokenizationError {
    match status {
        StatusCode::FORBIDDEN => {
            if message.contains("insufficient") || message.contains("position") {
                AlpacaTokenizationError::InsufficientPosition { symbol }
            } else {
                AlpacaTokenizationError::UnsupportedAccount
            }
        }
        StatusCode::UNPROCESSABLE_ENTITY => {
            AlpacaTokenizationError::InvalidParameters { details: message }
        }
        _ => AlpacaTokenizationError::ApiError { status, message },
    }
}

/// Client for Alpaca's tokenization API and redemption transfers.
struct AlpacaTokenizationClient<W: Wallet> {
    http_client: Client,
    base_url: String,
    account_id: AlpacaAccountId,
    api_key: String,
    api_secret: String,
    wallet: W,
    redemption_wallet: Address,
}

impl<W: Wallet> AlpacaTokenizationClient<W> {
    fn new(
        base_url: String,
        account_id: AlpacaAccountId,
        api_key: String,
        api_secret: String,
        wallet: W,
        redemption_wallet: Address,
    ) -> Self {
        Self {
            http_client: Client::new(),
            base_url,
            account_id,
            api_key,
            api_secret,
            wallet,
            redemption_wallet,
        }
    }

    /// Request a mint operation to convert offchain shares to onchain tokens.
    ///
    /// # Errors
    ///
    /// - `InsufficientPosition` if the account lacks the required shares (403)
    /// - `UnsupportedAccount` if the account is not enabled for tokenization (403)
    /// - `InvalidParameters` if the request parameters are invalid (422)
    /// - `ApiError` for other API errors
    /// - `Reqwest` for network errors
    async fn request_mint(
        &self,
        request: MintRequest,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let url = format!(
            "{}/v1/accounts/{}/tokenization/mint",
            self.base_url, self.account_id
        );

        debug!(
            url = %url,
            symbol = %request.underlying_symbol,
            quantity = %request.quantity,
            wallet = %request.wallet,
            "Sending tokenization mint request"
        );

        let response = self
            .http_client
            .post(&url)
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .json(&request)
            .send()
            .await?;

        let status = response.status();

        if status.is_success() {
            let body = response.text().await?;
            let tokenization_request: TokenizationRequest = serde_json::from_str(&body)
                .inspect_err(|error| {
                    error!(
                        body = %body,
                        error = %error,
                        "Failed to deserialize tokenization response"
                    );
                })?;

            debug!(request_id = %tokenization_request.id.0, "Mint request created");
            return Ok(tokenization_request);
        }

        let message = response.text().await?;
        warn!(status = %status, message = %message, "Tokenization request failed");
        Err(map_mint_error(status, message, request.underlying_symbol))
    }

    /// List tokenization requests with optional filtering.
    ///
    /// # Errors
    ///
    /// - `ApiError` for API errors
    /// - `Reqwest` for network errors
    async fn list_requests(
        &self,
        params: ListRequestsParams,
    ) -> Result<Vec<TokenizationRequest>, AlpacaTokenizationError> {
        let mut url = format!(
            "{}/v1/accounts/{}/tokenization/requests",
            self.base_url, self.account_id
        );
        let mut query_params = Vec::new();

        if let Some(ref request_type) = params.request_type {
            let type_str = match request_type {
                TokenizationRequestType::Mint => "mint",
                TokenizationRequestType::Redeem => "redeem",
            };
            query_params.push(format!("type={type_str}"));
        }

        if let Some(ref status) = params.status {
            let status_str = match status {
                TokenizationRequestStatus::Pending => "pending",
                TokenizationRequestStatus::Completed => "completed",
                TokenizationRequestStatus::Rejected => "rejected",
            };
            query_params.push(format!("status={status_str}"));
        }

        if let Some(ref symbol) = params.underlying_symbol {
            query_params.push(format!("underlying_symbol={symbol}"));
        }

        if !query_params.is_empty() {
            url.push('?');
            url.push_str(&query_params.join("&"));
        }

        let response = self
            .http_client
            .get(&url)
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .send()
            .await?;

        let status = response.status();

        if status.is_success() {
            let body = response.text().await?;
            trace!(body = %body, "List requests response body");

            let requests: Vec<TokenizationRequest> =
                serde_json::from_str(&body).inspect_err(|error| {
                    error!(
                        body = %body,
                        error = %error,
                        "Failed to deserialize list requests response"
                    );
                })?;

            debug!(count = requests.len(), "Listed tokenization requests");

            return Ok(requests);
        }

        let message = response.text().await?;
        Err(AlpacaTokenizationError::ApiError { status, message })
    }

    /// Get a single tokenization request by ID.
    ///
    /// # Errors
    ///
    /// - `RequestNotFound` if the request doesn't exist
    /// - `ApiError` for API errors
    /// - `Reqwest` for network errors
    async fn get_request(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let requests = self.list_requests(ListRequestsParams::default()).await?;

        requests
            .into_iter()
            .find(|request| request.id == *id)
            .ok_or_else(|| AlpacaTokenizationError::RequestNotFound { id: id.clone() })
    }

    /// Send tokens to the redemption wallet to initiate a redemption.
    ///
    /// This transfers ERC20 tokens from the signer's address to the configured
    /// redemption wallet. Once Alpaca detects this transfer, a redemption request
    /// will appear in `list_requests`.
    ///
    /// # Errors
    ///
    /// - `Evm(EvmError)` if the ERC20 transfer transaction fails
    async fn send_tokens_for_redemption<Registry: IntoErrorRegistry>(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, AlpacaTokenizationError> {
        let receipt = self
            .wallet
            .submit::<Registry, _>(
                token,
                IERC20::transferCall {
                    to: self.redemption_wallet,
                    amount,
                },
                "ERC20 transfer for redemption",
            )
            .await?;

        Ok(receipt.transaction_hash)
    }

    /// Find a redemption request by its onchain transaction hash.
    ///
    /// This polls the Alpaca API to check if they have detected a token transfer
    /// that initiates a redemption. Returns `None` if Alpaca hasn't detected
    /// the transfer yet.
    ///
    /// # Errors
    ///
    /// - `ApiError` for API errors
    /// - `Reqwest` for network errors
    async fn find_redemption_by_tx(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<TokenizationRequest>, AlpacaTokenizationError> {
        let params = ListRequestsParams {
            request_type: Some(TokenizationRequestType::Redeem),
            ..Default::default()
        };

        let requests = self.list_requests(params).await?;

        Ok(requests
            .into_iter()
            .find(|request| request.tx_hash.as_ref() == Some(tx_hash)))
    }

    /// Poll until a tokenization request reaches a terminal state (Completed or Rejected).
    ///
    /// # Errors
    ///
    /// - `PollTimeout` if the timeout is exceeded
    /// - `RequestNotFound` if the request doesn't exist
    /// - `ApiError` for API errors
    /// - `Reqwest` for network errors
    async fn poll_until_terminal(
        &self,
        id: &TokenizationRequestId,
        config: &PollingConfig,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let start = Instant::now();
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if start.elapsed() >= config.timeout {
                return Err(AlpacaTokenizationError::PollTimeout {
                    elapsed: start.elapsed(),
                });
            }

            let request = self.get_request(id).await?;

            match request.status {
                TokenizationRequestStatus::Completed | TokenizationRequestStatus::Rejected => {
                    return Ok(request);
                }
                TokenizationRequestStatus::Pending => {}
            }
        }
    }

    /// Poll until Alpaca detects a redemption transfer.
    ///
    /// # Errors
    ///
    /// - `PollTimeout` if the timeout is exceeded before detection
    /// - `ApiError` for API errors
    /// - `Reqwest` for network errors
    async fn poll_for_redemption_detection(
        &self,
        tx_hash: &TxHash,
        config: &PollingConfig,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let start = Instant::now();
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if start.elapsed() >= config.timeout {
                return Err(AlpacaTokenizationError::PollTimeout {
                    elapsed: start.elapsed(),
                });
            }

            if let Some(request) = self.find_redemption_by_tx(tx_hash).await? {
                return Ok(request);
            }
        }
    }
}

#[async_trait]
impl<W: Wallet> Tokenizer for AlpacaTokenizationService<W> {
    async fn request_mint(
        &self,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(Self::request_mint(self, symbol, quantity, wallet, issuer_request_id).await?)
    }

    async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(Self::poll_mint_until_complete(self, id).await?)
    }

    fn redemption_wallet(&self) -> Address {
        Self::redemption_wallet(self)
    }

    async fn send_for_redemption(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, TokenizerError> {
        Ok(Self::send_for_redemption::<OpenChainErrorRegistry>(self, token, amount).await?)
    }

    async fn poll_for_redemption(
        &self,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(Self::poll_for_redemption(self, tx_hash).await?)
    }

    async fn poll_redemption_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(Self::poll_redemption_until_complete(self, id).await?)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{Address, B256, address, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use httpmock::MockServer;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use std::time::Duration;
    use uuid::uuid;

    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_evm::{Evm, OpenChainErrorRegistry};

    use super::*;
    use crate::bindings::TestERC20;

    pub(crate) const TEST_REDEMPTION_WALLET: Address =
        address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

    pub(crate) const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    pub(crate) fn tokenization_mint_path() -> String {
        format!("/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/mint")
    }

    pub(crate) fn tokenization_requests_path() -> String {
        format!("/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests")
    }

    pub(crate) fn setup_anvil() -> (AnvilInstance, String, B256) {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.endpoint();
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        (anvil, endpoint, private_key)
    }

    async fn create_test_client(
        server: &MockServer,
        anvil_endpoint: &str,
        private_key: &B256,
        redemption_wallet: Address,
    ) -> AlpacaTokenizationClient<impl Wallet> {
        let provider = ProviderBuilder::new()
            .connect(anvil_endpoint)
            .await
            .unwrap();

        let wallet = RawPrivateKeyWallet::new(private_key, provider, 1).unwrap();

        AlpacaTokenizationClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
            wallet,
            redemption_wallet,
        )
    }

    fn create_test_service(
        client: AlpacaTokenizationClient<impl Wallet>,
    ) -> AlpacaTokenizationService<impl Wallet> {
        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };
        AlpacaTokenizationService::new_with_client(client, config)
    }

    pub(crate) async fn create_test_service_from_mock(
        server: &MockServer,
        anvil_endpoint: &str,
        private_key: &B256,
        redemption_wallet: Address,
    ) -> AlpacaTokenizationService<impl Wallet> {
        let client =
            create_test_client(server, anvil_endpoint, private_key, redemption_wallet).await;
        create_test_service(client)
    }

    fn create_mint_request() -> MintRequest {
        MintRequest {
            underlying_symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(dec!(100.5)),
            issuer: Issuer::new("st0x"),
            network: Network::new("base"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            issuer_request_id: IssuerRequestId::new("test-issuer-request-id"),
        }
    }

    #[tokio::test]
    async fn test_request_mint_success() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let mint_mock = server.mock(|when, then| {
            when.method(POST)
                .path(tokenization_mint_path())
                .header("APCA-API-KEY-ID", "test_api_key")
                .header("APCA-API-SECRET-KEY", "test_api_secret")
                .json_body(json!({
                    "underlying_symbol": "AAPL",
                    "qty": "100.5",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "issuer_request_id": "test-issuer-request-id"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "tokenization_request_id": "tok_req_123",
                    "type": "mint",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "100.5",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "issuer_request_id": "iss_req_456",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let request = create_mint_request();
        let result = client.request_mint(request).await.unwrap();

        assert_eq!(result.id, TokenizationRequestId("tok_req_123".to_string()));
        assert_eq!(result.r#type, Some(TokenizationRequestType::Mint));
        assert_eq!(result.status, TokenizationRequestStatus::Pending);
        assert_eq!(result.underlying_symbol.to_string(), "AAPL");
        assert_eq!(
            result.token_symbol.as_ref().map(ToString::to_string),
            Some("tAAPL".to_string())
        );
        assert_eq!(result.quantity, FractionalShares::new(dec!(100.5)));
        assert_eq!(result.issuer, Issuer::new("st0x"));
        assert_eq!(result.network, Network::new("base"));
        assert_eq!(
            result.issuer_request_id,
            Some(IssuerRequestId("iss_req_456".to_string()))
        );

        mint_mock.assert();
    }

    #[tokio::test]
    async fn test_request_mint_insufficient_position() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(403)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40_310_000,
                    "message": "insufficient position for AAPL"
                }));
        });

        let request = create_mint_request();

        let err = client.request_mint(request).await.unwrap_err();
        assert!(
            matches!(&err, AlpacaTokenizationError::InsufficientPosition { symbol } if symbol.to_string() == "AAPL"),
            "expected InsufficientPosition for AAPL, got: {err:?}"
        );

        mint_mock.assert();
    }

    #[tokio::test]
    async fn test_request_mint_invalid_parameters() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(422)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 42_210_000,
                    "message": "invalid wallet address format"
                }));
        });

        let request = create_mint_request();

        let err = client.request_mint(request).await.unwrap_err();
        assert!(
            matches!(&err, AlpacaTokenizationError::InvalidParameters { details } if details.contains("invalid wallet address")),
            "expected InvalidParameters with 'invalid wallet address', got: {err:?}"
        );

        mint_mock.assert();
    }

    fn sample_tokenization_request_json(
        id: &str,
        request_type: &str,
        symbol: &str,
    ) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": request_type,
            "status": "pending",
            "underlying_symbol": symbol,
            "token_symbol": format!("t{symbol}"),
            "qty": "50.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[tokio::test]
    async fn test_list_requests_all() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path(tokenization_requests_path())
                .header("APCA-API-KEY-ID", "test_api_key")
                .header("APCA-API-SECRET-KEY", "test_api_secret");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    sample_tokenization_request_json("req_1", "mint", "AAPL"),
                    sample_tokenization_request_json("req_2", "redeem", "TSLA")
                ]));
        });

        let result = client
            .list_requests(ListRequestsParams::default())
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, TokenizationRequestId("req_1".to_string()));
        assert_eq!(result[0].r#type, Some(TokenizationRequestType::Mint));
        assert_eq!(result[1].id, TokenizationRequestId("req_2".to_string()));
        assert_eq!(result[1].r#type, Some(TokenizationRequestType::Redeem));

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_list_requests_filter_by_type_mint() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path(tokenization_requests_path())
                .query_param("type", "mint");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_tokenization_request_json(
                    "req_1", "mint", "AAPL"
                )]));
        });

        let params = ListRequestsParams {
            request_type: Some(TokenizationRequestType::Mint),
            ..Default::default()
        };
        let result = client.list_requests(params).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].r#type, Some(TokenizationRequestType::Mint));

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_list_requests_filter_by_type_redeem() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path(tokenization_requests_path())
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_tokenization_request_json(
                    "req_2", "redeem", "TSLA"
                )]));
        });

        let params = ListRequestsParams {
            request_type: Some(TokenizationRequestType::Redeem),
            ..Default::default()
        };
        let result = client.list_requests(params).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].r#type, Some(TokenizationRequestType::Redeem));

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_get_request_found() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    sample_tokenization_request_json("req_1", "mint", "AAPL"),
                    sample_tokenization_request_json("req_2", "redeem", "TSLA")
                ]));
        });

        let id = TokenizationRequestId("req_2".to_string());
        let result = client.get_request(&id).await.unwrap();

        assert_eq!(result.id, id);
        assert_eq!(result.r#type, Some(TokenizationRequestType::Redeem));
        assert_eq!(result.underlying_symbol.to_string(), "TSLA");

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_get_request_not_found() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_tokenization_request_json(
                    "req_1", "mint", "AAPL"
                )]));
        });

        let id = TokenizationRequestId("nonexistent".to_string());

        let err = client.get_request(&id).await.unwrap_err();
        assert!(
            matches!(&err, AlpacaTokenizationError::RequestNotFound { id: found_id } if found_id.0 == "nonexistent"),
            "expected RequestNotFound, got: {err:?}"
        );

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_send_tokens_for_redemption_success() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let mint_amount = U256::from(1_000_000_000u64);
        token
            .mint(wallet.address(), mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let client = AlpacaTokenizationClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
            wallet,
            TEST_REDEMPTION_WALLET,
        );

        let transfer_amount = U256::from(100_000u64);

        client
            .send_tokens_for_redemption::<OpenChainErrorRegistry>(token_address, transfer_amount)
            .await
            .unwrap();

        let balance = token
            .balanceOf(TEST_REDEMPTION_WALLET)
            .call()
            .await
            .unwrap();
        assert_eq!(
            balance, transfer_amount,
            "redemption wallet should have received tokens"
        );
    }

    #[tokio::test]
    async fn test_send_tokens_for_redemption_insufficient_balance() {
        let (_anvil, endpoint, key) = setup_anvil();
        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let client = AlpacaTokenizationClient::new(
            "http://unused".to_string(),
            TEST_ACCOUNT_ID,
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
            wallet,
            TEST_REDEMPTION_WALLET,
        );

        let transfer_amount = U256::from(100_000u64);
        let err = client
            .send_tokens_for_redemption::<OpenChainErrorRegistry>(token_address, transfer_amount)
            .await
            .unwrap_err();

        assert!(
            matches!(err, AlpacaTokenizationError::Evm(_)),
            "expected Evm error variant, got: {err:?}"
        );
    }

    fn sample_redemption_request_json_with_tx(
        id: &str,
        symbol: &str,
        tx_hash: TxHash,
    ) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "redeem",
            "status": "pending",
            "underlying_symbol": symbol,
            "token_symbol": format!("t{symbol}"),
            "qty": "50.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "tx_hash": tx_hash,
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[tokio::test]
    async fn test_find_redemption_by_tx_found() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path(tokenization_requests_path())
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    sample_redemption_request_json_with_tx("redeem_1", "AAPL", hash),
                    sample_redemption_request_json_with_tx(
                        "redeem_2",
                        "TSLA",
                        fixed_bytes!(
                            "0x1111111111111111111111111111111111111111111111111111111111111111"
                        )
                    )
                ]));
        });

        let result = client.find_redemption_by_tx(&hash).await.unwrap();

        assert!(result.is_some(), "expected to find redemption request");
        let request = result.unwrap();
        assert_eq!(request.id, TokenizationRequestId("redeem_1".to_string()));
        assert_eq!(request.tx_hash, Some(hash));

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_find_redemption_by_tx_not_detected() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path(tokenization_requests_path())
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let hash: TxHash =
            fixed_bytes!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        assert!(
            client.find_redemption_by_tx(&hash).await.unwrap().is_none(),
            "expected None when redemption not yet detected"
        );

        list_mock.assert();
    }

    fn sample_request_with_status(id: &str, status: &str) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "mint",
            "status": status,
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[tokio::test]
    async fn test_poll_until_terminal_completed() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_request_with_status("req_1", "completed")]));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let id = TokenizationRequestId("req_1".to_string());
        let result = client.poll_until_terminal(&id, &config).await.unwrap();

        assert_eq!(result.status, TokenizationRequestStatus::Completed);
        list_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_until_terminal_rejected() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_request_with_status("req_1", "rejected")]));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let id = TokenizationRequestId("req_1".to_string());
        let result = client.poll_until_terminal(&id, &config).await.unwrap();

        assert_eq!(result.status, TokenizationRequestStatus::Rejected);
        list_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_for_redemption_detection_success() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path(tokenization_requests_path())
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_redemption_request_json_with_tx(
                    "redeem_1", "AAPL", hash
                )]));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let result = client
            .poll_for_redemption_detection(&hash, &config)
            .await
            .unwrap();

        assert_eq!(result.id, TokenizationRequestId("redeem_1".to_string()));
        assert_eq!(result.tx_hash, Some(hash));
        list_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_timeout() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let _list_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_request_with_status("req_1", "pending")]));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(50),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let id = TokenizationRequestId("req_1".to_string());
        let result = client.poll_until_terminal(&id, &config).await;

        assert!(
            matches!(result, Err(AlpacaTokenizationError::PollTimeout { .. })),
            "expected PollTimeout, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_service_mint_poll_completed() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;
        let service = create_test_service(client);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_request_with_status("mint_123", "pending"));
        });

        let list_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_request_with_status("mint_123", "completed")]));
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let mint_result = service
            .request_mint(symbol, quantity, wallet, IssuerRequestId::new("test-id"))
            .await
            .unwrap();

        assert_eq!(
            mint_result.id,
            TokenizationRequestId("mint_123".to_string())
        );
        assert_eq!(mint_result.status, TokenizationRequestStatus::Pending);

        let poll_result = service
            .poll_mint_until_complete(&mint_result.id)
            .await
            .unwrap();

        assert_eq!(poll_result.status, TokenizationRequestStatus::Completed);

        mint_mock.assert();
        list_mock.assert();
    }

    #[tokio::test]
    async fn test_service_redemption_detected_poll_completed() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;
        let service = create_test_service(client);

        let hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let detection_mock = server.mock(|when, then| {
            when.method(GET)
                .path(tokenization_requests_path())
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "redeem_456",
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "tx_hash": hash,
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let complete_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "redeem_456",
                    "type": "redeem",
                    "status": "completed",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "tx_hash": hash,
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let detected = service.poll_for_redemption(&hash).await.unwrap();

        assert_eq!(detected.id, TokenizationRequestId("redeem_456".to_string()));
        assert_eq!(detected.status, TokenizationRequestStatus::Pending);

        let completed = service
            .poll_redemption_until_complete(&detected.id)
            .await
            .unwrap();

        assert_eq!(completed.status, TokenizationRequestStatus::Completed);

        detection_mock.assert();
        complete_mock.assert();
    }

    #[tokio::test]
    async fn test_request_mint_includes_issuer_request_id_in_body() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let issuer_request_id = IssuerRequestId::new("our-tracking-id-123");

        let mint_mock = server.mock(|when, then| {
            when.method(POST)
                .path(tokenization_mint_path())
                .json_body(json!({
                    "underlying_symbol": "AAPL",
                    "qty": "100.5",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "issuer_request_id": "our-tracking-id-123"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "tokenization_request_id": "tok_req_456",
                    "type": "mint",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "100.5",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "issuer_request_id": "our-tracking-id-123",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.5));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = service
            .request_mint(symbol, quantity, wallet, issuer_request_id.clone())
            .await
            .unwrap();

        assert_eq!(
            result.issuer_request_id,
            Some(issuer_request_id),
            "Alpaca should return the same issuer_request_id we sent"
        );

        mint_mock.assert();
    }

    #[test]
    fn test_deserialize_tokenization_request_with_empty_token_symbol() {
        let json = json!({
            "tokenization_request_id": "tok_req_empty",
            "status": "pending",
            "underlying_symbol": "AAPL",
            "token_symbol": "",
            "qty": "10",
            "issuer": "alpaca",
            "network": "base",
            "created_at": "2024-01-15T10:30:00Z"
        });

        let request: TokenizationRequest = serde_json::from_value(json).unwrap();
        assert_eq!(
            request.token_symbol, None,
            "Empty token_symbol string should deserialize as None"
        );
    }

    #[test]
    fn fees_absent_deserializes_as_none() {
        let json = json!({
            "tokenization_request_id": "req_no_fees",
            "status": "completed",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "5",
            "issuer": "alpaca",
            "network": "base",
            "created_at": "2024-01-15T10:30:00Z"
        });

        let request: TokenizationRequest = serde_json::from_value(json).unwrap();
        assert_eq!(request.fees, None);
    }

    #[test]
    fn fees_null_deserializes_as_none() {
        let json = json!({
            "tokenization_request_id": "req_null_fees",
            "status": "completed",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "5",
            "issuer": "alpaca",
            "network": "base",
            "fees": null,
            "created_at": "2024-01-15T10:30:00Z"
        });

        let request: TokenizationRequest = serde_json::from_value(json).unwrap();
        assert_eq!(request.fees, None);
    }

    #[test]
    fn fees_with_value_deserializes_as_some() {
        let json = json!({
            "tokenization_request_id": "req_with_fees",
            "status": "completed",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "5",
            "issuer": "alpaca",
            "network": "base",
            "fees": "1.50",
            "created_at": "2024-01-15T10:30:00Z"
        });

        let request: TokenizationRequest = serde_json::from_value(json).unwrap();
        assert_eq!(request.fees, Some(Usd(dec!(1.50))));
    }
}
