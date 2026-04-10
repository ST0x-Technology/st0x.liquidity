use apca::api::v2::account::{self, GetError};
use apca::{Client, RequestError};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;

/// Trading mode for Alpaca Trading API
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlpacaTradingApiMode {
    Paper,
    Live,
    #[cfg(test)]
    #[serde(skip_deserializing)]
    Mock(String),
}

impl AlpacaTradingApiMode {
    /// Returns the base URL for Alpaca Trading API.
    fn base_url(&self) -> String {
        match self {
            Self::Paper => "https://paper-api.alpaca.markets".to_string(),
            Self::Live => "https://api.alpaca.markets".to_string(),
            #[cfg(test)]
            Self::Mock(url) => url.clone(),
        }
    }

    fn market_data_base_url(&self) -> String {
        match self {
            Self::Paper | Self::Live => "https://data.alpaca.markets".to_string(),
            #[cfg(test)]
            Self::Mock(url) => url.clone(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct AlpacaTradingApiCtx {
    pub api_key: String,
    pub api_secret: String,
    pub trading_mode: Option<AlpacaTradingApiMode>,
    pub counter_trade_slippage_bps: u16,
}

impl AlpacaTradingApiCtx {
    pub fn trading_mode(&self) -> AlpacaTradingApiMode {
        self.trading_mode
            .clone()
            .unwrap_or(AlpacaTradingApiMode::Paper)
    }

    pub fn base_url(&self) -> String {
        self.trading_mode().base_url()
    }

    pub fn is_paper_trading(&self) -> bool {
        matches!(self.trading_mode(), AlpacaTradingApiMode::Paper)
    }
}

impl std::fmt::Debug for AlpacaTradingApiCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaTradingApiCtx")
            .field("api_key", &"[REDACTED]")
            .field("api_secret", &"[REDACTED]")
            .field("trading_mode", &self.trading_mode())
            .field(
                "counter_trade_slippage_bps",
                &self.counter_trade_slippage_bps,
            )
            .finish()
    }
}

/// Alpaca Trading API client wrapper with authentication and context
pub struct AlpacaTradingApiClient {
    client: Client,
    http_client: reqwest::Client,
    base_url: String,
    market_data_base_url: String,
    trading_mode: AlpacaTradingApiMode,
}

impl std::fmt::Debug for AlpacaTradingApiClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaTradingApiClient")
            .field("client", &"<Client>")
            .field("trading_mode", &self.trading_mode)
            .finish_non_exhaustive()
    }
}

impl AlpacaTradingApiClient {
    pub(crate) fn new(ctx: &AlpacaTradingApiCtx) -> Result<Self, super::AlpacaTradingApiError> {
        let base_url = ctx.base_url();
        let api_info = apca::ApiInfo::from_parts(&base_url, &ctx.api_key, &ctx.api_secret)?;
        let market_data_base_url = ctx.trading_mode().market_data_base_url();
        let api_key_header = HeaderName::from_static("apca-api-key-id");
        let api_secret_header = HeaderName::from_static("apca-api-secret-key");

        let client = Client::new(api_info);
        let headers = HeaderMap::from_iter([
            (api_key_header, HeaderValue::from_str(&ctx.api_key)?),
            (api_secret_header, HeaderValue::from_str(&ctx.api_secret)?),
            (CONTENT_TYPE, HeaderValue::from_static("application/json")),
        ]);
        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(Self {
            client,
            http_client,
            base_url,
            market_data_base_url,
            trading_mode: ctx.trading_mode(),
        })
    }

    pub(crate) async fn verify_account(&self) -> Result<(), RequestError<GetError>> {
        let _account = self.client.issue::<account::Get>(&()).await?;
        Ok(())
    }

    pub(crate) fn is_paper_trading(&self) -> bool {
        matches!(self.trading_mode, AlpacaTradingApiMode::Paper)
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn market_data_base_url(&self) -> &str {
        &self.market_data_base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_paper_ctx() -> AlpacaTradingApiCtx {
        AlpacaTradingApiCtx {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            trading_mode: Some(AlpacaTradingApiMode::Paper),
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    fn create_test_live_ctx() -> AlpacaTradingApiCtx {
        AlpacaTradingApiCtx {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            trading_mode: Some(AlpacaTradingApiMode::Live),
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    #[test]
    fn test_alpaca_trading_api_mode_urls() {
        assert_eq!(
            AlpacaTradingApiMode::Paper.base_url(),
            "https://paper-api.alpaca.markets".to_string()
        );
        assert_eq!(
            AlpacaTradingApiMode::Live.base_url(),
            "https://api.alpaca.markets".to_string()
        );
    }

    #[test]
    fn test_alpaca_trading_api_client_new_valid_ctx() {
        let ctx = create_test_paper_ctx();
        let result = AlpacaTradingApiClient::new(&ctx);

        let client = result.unwrap();
        assert!(client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_trading_api_client_new_live_ctx() {
        let ctx = create_test_live_ctx();
        let result = AlpacaTradingApiClient::new(&ctx);

        let client = result.unwrap();
        assert!(!client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_trading_api_client_new_empty_credentials() {
        let empty_ctx = AlpacaTradingApiCtx {
            api_key: String::new(),
            api_secret: String::new(),
            trading_mode: None,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        let result = AlpacaTradingApiClient::new(&empty_ctx);

        let client = result.unwrap();
        assert!(client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_trading_api_client_paper_vs_live_state_consistency() {
        let paper_ctx = create_test_paper_ctx();
        let live_ctx = create_test_live_ctx();

        let paper_client = AlpacaTradingApiClient::new(&paper_ctx).unwrap();
        let live_client = AlpacaTradingApiClient::new(&live_ctx).unwrap();

        assert!(paper_client.is_paper_trading());
        assert!(!live_client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_trading_api_auth_env_debug_redacts_secrets() {
        let ctx = AlpacaTradingApiCtx {
            api_key: "secret_key_id_123".to_string(),
            api_secret: "super_secret_key_456".to_string(),
            trading_mode: Some(AlpacaTradingApiMode::Paper),
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        let debug_output = format!("{ctx:?}");

        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("secret_key_id_123"));
        assert!(!debug_output.contains("super_secret_key_456"));
        assert!(debug_output.contains("Paper"));
    }

    #[test]
    fn test_alpaca_trading_api_client_debug_does_not_leak_credentials() {
        let ctx = create_test_paper_ctx();
        let client = AlpacaTradingApiClient::new(&ctx).unwrap();

        let debug_output = format!("{client:?}");

        assert!(!debug_output.contains("test_key_id"));
        assert!(!debug_output.contains("test_secret_key"));
        assert!(debug_output.contains("Paper"));
        assert!(debug_output.contains("<Client>"));
    }
}
