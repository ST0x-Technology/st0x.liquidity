use apca::api::v2::account::{self, GetError};
use apca::{Client, RequestError};
use clap::{Parser, ValueEnum};
use tokio::sync::OnceCell;

/// Trading mode for Alpaca API
#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum AlpacaTradingMode {
    /// Paper trading mode (simulated trading with fake money)
    Paper,
    /// Live trading mode (real money)
    Live,
    /// Mock mode for testing (test-only)
    #[cfg(test)]
    #[clap(skip)]
    Mock(String),
}

impl AlpacaTradingMode {
    fn base_url(&self) -> String {
        match self {
            Self::Paper => "https://paper-api.alpaca.markets".to_string(),
            Self::Live => "https://api.alpaca.markets".to_string(),
            #[cfg(test)]
            Self::Mock(url) => url.clone(),
        }
    }
}

/// Alpaca API authentication environment configuration
#[derive(Parser, Clone)]
pub struct AlpacaAuthEnv {
    /// Alpaca API key
    #[clap(long, env)]
    pub alpaca_api_key: String,

    /// Alpaca API secret
    #[clap(long, env)]
    pub alpaca_api_secret: String,

    /// Trading mode: paper or live (defaults to paper for safety)
    #[clap(long, env, default_value = "paper")]
    pub alpaca_trading_mode: AlpacaTradingMode,
}

impl std::fmt::Debug for AlpacaAuthEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaAuthEnv")
            .field("alpaca_api_key", &"[REDACTED]")
            .field("alpaca_api_secret", &"[REDACTED]")
            .field("alpaca_trading_mode", &self.alpaca_trading_mode)
            .finish()
    }
}

impl AlpacaAuthEnv {
    pub fn base_url(&self) -> String {
        self.alpaca_trading_mode.base_url()
    }
}

/// Alpaca API client wrapper with authentication and configuration
pub struct AlpacaClient {
    client: Client,
    trading_mode: AlpacaTradingMode,
    account_id: OnceCell<String>,
}

impl std::fmt::Debug for AlpacaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaClient")
            .field("client", &"<Client>")
            .field("trading_mode", &self.trading_mode)
            .finish_non_exhaustive()
    }
}

impl AlpacaClient {
    pub(crate) fn new(env: &AlpacaAuthEnv) -> Result<Self, crate::BrokerError> {
        let base_url = env.base_url();
        let api_info =
            apca::ApiInfo::from_parts(&base_url, &env.alpaca_api_key, &env.alpaca_api_secret)?;

        let client = Client::new(api_info);

        Ok(Self {
            client,
            trading_mode: env.alpaca_trading_mode.clone(),
            account_id: OnceCell::new(),
        })
    }

    pub(crate) async fn verify_account(&self) -> Result<(), RequestError<GetError>> {
        let _account = self.client.issue::<account::Get>(&()).await?;
        Ok(())
    }

    /// Get the account ID, fetching and caching it if not already cached.
    ///
    /// This method caches the account ID after the first retrieval to prevent
    /// repeated API calls. Subsequent calls return the cached value.
    pub(crate) async fn get_account_id(&self) -> Result<String, RequestError<GetError>> {
        self.account_id
            .get_or_try_init(|| async {
                let account = self.client.issue::<account::Get>(&()).await?;
                Ok(account.id.to_string())
            })
            .await
            .cloned()
    }

    pub(crate) fn is_paper_trading(&self) -> bool {
        matches!(self.trading_mode, AlpacaTradingMode::Paper)
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_test_paper_config() -> AlpacaAuthEnv {
        AlpacaAuthEnv {
            alpaca_api_key: "test_key_id".to_string(),
            alpaca_api_secret: "test_secret_key".to_string(),
            alpaca_trading_mode: AlpacaTradingMode::Paper,
        }
    }

    fn create_test_live_config() -> AlpacaAuthEnv {
        AlpacaAuthEnv {
            alpaca_api_key: "test_key_id".to_string(),
            alpaca_api_secret: "test_secret_key".to_string(),
            alpaca_trading_mode: AlpacaTradingMode::Live,
        }
    }

    #[test]
    fn test_alpaca_trading_mode_urls() {
        assert_eq!(
            AlpacaTradingMode::Paper.base_url(),
            "https://paper-api.alpaca.markets".to_string()
        );
        assert_eq!(
            AlpacaTradingMode::Live.base_url(),
            "https://api.alpaca.markets".to_string()
        );
    }

    #[test]
    fn test_alpaca_client_new_valid_config() {
        let config = create_test_paper_config();
        let result = AlpacaClient::new(&config);

        let client = result.unwrap();
        assert!(client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_client_new_live_config() {
        let config = create_test_live_config();
        let result = AlpacaClient::new(&config);

        let client = result.unwrap();
        assert!(!client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_client_new_empty_credentials() {
        let empty_config = AlpacaAuthEnv {
            alpaca_api_key: String::new(),
            alpaca_api_secret: String::new(),
            alpaca_trading_mode: AlpacaTradingMode::Paper,
        };

        let result = AlpacaClient::new(&empty_config);

        let client = result.unwrap();
        assert!(client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_client_paper_vs_live_state_consistency() {
        let paper_config = create_test_paper_config();
        let live_config = create_test_live_config();

        let paper_client = AlpacaClient::new(&paper_config).unwrap();
        let live_client = AlpacaClient::new(&live_config).unwrap();

        assert!(paper_client.is_paper_trading());
        assert!(!live_client.is_paper_trading());
    }

    #[test]
    fn test_alpaca_auth_env_debug_redacts_secrets() {
        let config = AlpacaAuthEnv {
            alpaca_api_key: "secret_key_id_123".to_string(),
            alpaca_api_secret: "super_secret_key_456".to_string(),
            alpaca_trading_mode: AlpacaTradingMode::Paper,
        };

        let debug_output = format!("{config:?}");

        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("secret_key_id_123"));
        assert!(!debug_output.contains("super_secret_key_456"));
        assert!(debug_output.contains("Paper"));
    }

    #[test]
    fn test_alpaca_client_debug_does_not_leak_credentials() {
        let config = create_test_paper_config();
        let client = AlpacaClient::new(&config).unwrap();

        let debug_output = format!("{client:?}");

        assert!(!debug_output.contains("test_key_id"));
        assert!(!debug_output.contains("test_secret_key"));
        assert!(debug_output.contains("Paper"));
        assert!(debug_output.contains("<Client>"));
    }

    fn create_mock_auth_env(base_url: &str) -> AlpacaAuthEnv {
        AlpacaAuthEnv {
            alpaca_api_key: "test_key_id".to_string(),
            alpaca_api_secret: "test_secret_key".to_string(),
            alpaca_trading_mode: AlpacaTradingMode::Mock(base_url.to_string()),
        }
    }

    fn create_account_mock<'a>(server: &'a MockServer, account_id: &str) -> httpmock::Mock<'a> {
        server.mock(|when, then| {
            when.method(GET).path("/v2/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": account_id,
                    "account_number": "PA1234567890",
                    "status": "ACTIVE",
                    "currency": "USD",
                    "buying_power": "100000.00",
                    "regt_buying_power": "100000.00",
                    "daytrading_buying_power": "400000.00",
                    "non_marginable_buying_power": "100000.00",
                    "cash": "100000.00",
                    "accrued_fees": "0",
                    "pending_transfer_out": "0",
                    "pending_transfer_in": "0",
                    "portfolio_value": "100000.00",
                    "pattern_day_trader": false,
                    "trading_blocked": false,
                    "transfers_blocked": false,
                    "account_blocked": false,
                    "created_at": "2020-01-01T00:00:00Z",
                    "trade_suspended_by_user": false,
                    "multiplier": "4",
                    "shorting_enabled": true,
                    "equity": "100000.00",
                    "last_equity": "100000.00",
                    "long_market_value": "0",
                    "short_market_value": "0",
                    "initial_margin": "0",
                    "maintenance_margin": "0",
                    "last_maintenance_margin": "0",
                    "sma": "0",
                    "daytrade_count": 0
                }));
        })
    }

    #[tokio::test]
    async fn test_get_account_id_successful_retrieval() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let mock = create_account_mock(&server, expected_account_id);

        let env = create_mock_auth_env(&server.base_url());
        let client = AlpacaClient::new(&env).unwrap();

        let account_id = client.get_account_id().await.unwrap();

        assert_eq!(account_id, expected_account_id);
        mock.assert();
    }

    #[tokio::test]
    async fn test_get_account_id_caching_behavior() {
        let server = MockServer::start();
        let expected_account_id = "12345678-1234-1234-1234-123456789abc";
        let mock = create_account_mock(&server, expected_account_id);

        let env = create_mock_auth_env(&server.base_url());
        let client = AlpacaClient::new(&env).unwrap();

        let first_call = client.get_account_id().await.unwrap();
        let second_call = client.get_account_id().await.unwrap();
        let third_call = client.get_account_id().await.unwrap();

        assert_eq!(first_call, expected_account_id);
        assert_eq!(second_call, expected_account_id);
        assert_eq!(third_call, expected_account_id);

        mock.assert_hits(1);
    }

    #[tokio::test]
    async fn test_get_account_id_api_error() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/v2/account");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Invalid API credentials"
                }));
        });

        let env = create_mock_auth_env(&server.base_url());
        let client = AlpacaClient::new(&env).unwrap();

        let result = client.get_account_id().await;

        assert!(result.is_err());
        mock.assert();
    }

    #[tokio::test]
    async fn test_get_account_id_malformed_response() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/v2/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "account_number": "PA1234567890",
                    "status": "ACTIVE"
                }));
        });

        let env = create_mock_auth_env(&server.base_url());
        let client = AlpacaClient::new(&env).unwrap();

        let result = client.get_account_id().await;

        assert!(result.is_err());
        mock.assert();
    }
}
