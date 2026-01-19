use apca::api::v2::account::{self, GetError};
use apca::{Client, RequestError};
use clap::{Parser, ValueEnum};

/// Trading mode for Alpaca Trading API
#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum AlpacaTradingApiMode {
    /// Paper trading mode (simulated trading with fake money)
    Paper,
    /// Live trading mode (real money)
    Live,
    /// Mock mode for testing (test-only)
    #[cfg(test)]
    #[clap(skip)]
    Mock(String),
}

impl AlpacaTradingApiMode {
    fn base_url(&self) -> String {
        match self {
            Self::Paper => "https://paper-api.alpaca.markets".to_string(),
            Self::Live => "https://api.alpaca.markets".to_string(),
            #[cfg(test)]
            Self::Mock(url) => url.clone(),
        }
    }
}

/// Alpaca Trading API authentication environment configuration
#[derive(Parser, Clone)]
pub struct AlpacaTradingApiAuthEnv {
    /// Alpaca API key
    #[clap(long, env)]
    pub alpaca_api_key: String,

    /// Alpaca API secret
    #[clap(long, env)]
    pub alpaca_api_secret: String,

    /// Trading mode: paper or live (defaults to paper for safety)
    #[clap(long, env, default_value = "paper")]
    pub alpaca_trading_mode: AlpacaTradingApiMode,
}

impl std::fmt::Debug for AlpacaTradingApiAuthEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaTradingApiAuthEnv")
            .field("alpaca_api_key", &"[REDACTED]")
            .field("alpaca_api_secret", &"[REDACTED]")
            .field("alpaca_trading_mode", &self.alpaca_trading_mode)
            .finish()
    }
}

impl AlpacaTradingApiAuthEnv {
    pub fn base_url(&self) -> String {
        self.alpaca_trading_mode.base_url()
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
    pub(crate) fn new(env: &AlpacaTradingApiAuthEnv) -> Result<Self, super::AlpacaTradingApiError> {
        let base_url = env.base_url();
        let api_info =
            apca::ApiInfo::from_parts(&base_url, &env.alpaca_api_key, &env.alpaca_api_secret)?;

        let client = Client::new(api_info);

        Ok(Self {
            client,
            trading_mode: env.alpaca_trading_mode.clone(),
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

    fn create_test_paper_config() -> AlpacaTradingApiAuthEnv {
        AlpacaTradingApiAuthEnv {
            alpaca_api_key: "test_key_id".to_string(),
            alpaca_api_secret: "test_secret_key".to_string(),
            alpaca_trading_mode: AlpacaTradingApiMode::Paper,
        }
    }

    fn create_test_live_config() -> AlpacaTradingApiAuthEnv {
        AlpacaTradingApiAuthEnv {
            alpaca_api_key: "test_key_id".to_string(),
            alpaca_api_secret: "test_secret_key".to_string(),
            alpaca_trading_mode: AlpacaTradingApiMode::Live,
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
        let empty_config = AlpacaTradingApiAuthEnv {
            alpaca_api_key: String::new(),
            alpaca_api_secret: String::new(),
            alpaca_trading_mode: AlpacaTradingApiMode::Paper,
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
        let config = AlpacaTradingApiAuthEnv {
            alpaca_api_key: "secret_key_id_123".to_string(),
            alpaca_api_secret: "super_secret_key_456".to_string(),
            alpaca_trading_mode: AlpacaTradingApiMode::Paper,
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
