use apca::api::v2::account::{self, GetError};
use apca::{Client, RequestError};
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
}

#[derive(Clone, Deserialize)]
pub struct AlpacaTradingApiAuthConfig {
    pub api_key: String,
    pub api_secret: String,
    pub trading_mode: Option<AlpacaTradingApiMode>,
}

impl AlpacaTradingApiAuthConfig {
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

impl std::fmt::Debug for AlpacaTradingApiAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaTradingApiAuthConfig")
            .field("api_key", &"[REDACTED]")
            .field("api_secret", &"[REDACTED]")
            .field("trading_mode", &self.trading_mode())
            .finish()
    }
}

/// Alpaca Trading API client wrapper with authentication and configuration
pub struct AlpacaTradingApiClient {
    client: Client,
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
    pub(crate) fn new(env: &AlpacaTradingApiAuthConfig) -> Result<Self, super::AlpacaTradingApiError> {
        let base_url = env.base_url();
        let api_info =
            apca::ApiInfo::from_parts(&base_url, &env.api_key, &env.api_secret)?;

        let client = Client::new(api_info);

        Ok(Self {
            client,
            trading_mode: env.trading_mode(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_paper_config() -> AlpacaTradingApiAuthConfig {
        AlpacaTradingApiAuthConfig {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            trading_mode: Some(AlpacaTradingApiMode::Paper),
        }
    }

    fn create_test_live_config() -> AlpacaTradingApiAuthConfig {
        AlpacaTradingApiAuthConfig {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            trading_mode: Some(AlpacaTradingApiMode::Live),
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
    fn test_alpaca_trading_api_client_new_valid_config() {
        let config = create_test_paper_config();
        let result = AlpacaTradingApiClient::new(&config);

        let client = result.unwrap();
        assert!(client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_trading_api_client_new_live_config() {
        let config = create_test_live_config();
        let result = AlpacaTradingApiClient::new(&config);

        let client = result.unwrap();
        assert!(!client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_trading_api_client_new_empty_credentials() {
        let empty_config = AlpacaTradingApiAuthConfig {
            api_key: String::new(),
            api_secret: String::new(),
            trading_mode: None,
        };

        let result = AlpacaTradingApiClient::new(&empty_config);

        let client = result.unwrap();
        assert!(client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_trading_api_client_paper_vs_live_state_consistency() {
        let paper_config = create_test_paper_config();
        let live_config = create_test_live_config();

        let paper_client = AlpacaTradingApiClient::new(&paper_config).unwrap();
        let live_client = AlpacaTradingApiClient::new(&live_config).unwrap();

        assert!(paper_client.is_paper_trading());
        assert!(!live_client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_trading_api_auth_env_debug_redacts_secrets() {
        let config = AlpacaTradingApiAuthConfig {
            api_key: "secret_key_id_123".to_string(),
            api_secret: "super_secret_key_456".to_string(),
            trading_mode: Some(AlpacaTradingApiMode::Paper),
        };

        let debug_output = format!("{config:?}");

        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("secret_key_id_123"));
        assert!(!debug_output.contains("super_secret_key_456"));
        assert!(debug_output.contains("Paper"));
    }

    #[test]
    fn test_alpaca_trading_api_client_debug_does_not_leak_credentials() {
        let config = create_test_paper_config();
        let client = AlpacaTradingApiClient::new(&config).unwrap();

        let debug_output = format!("{client:?}");

        assert!(!debug_output.contains("test_key_id"));
        assert!(!debug_output.contains("test_secret_key"));
        assert!(debug_output.contains("Paper"));
        assert!(debug_output.contains("<Client>"));
    }
}
