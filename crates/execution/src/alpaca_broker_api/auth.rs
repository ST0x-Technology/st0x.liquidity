use clap::{Parser, ValueEnum};
use serde::Deserialize;
use uuid::Uuid;

/// Mode for Alpaca Broker API
#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum AlpacaBrokerApiMode {
    /// Sandbox environment (paper trading)
    Sandbox,
    /// Production environment (real money)
    Production,
    /// Mock mode for testing (test-only)
    #[cfg(test)]
    #[clap(skip)]
    Mock(String),
}

impl AlpacaBrokerApiMode {
    pub(super) fn base_url(&self) -> &str {
        match self {
            Self::Sandbox => "https://broker-api.sandbox.alpaca.markets",
            Self::Production => "https://broker-api.alpaca.markets",
            #[cfg(test)]
            Self::Mock(url) => url,
        }
    }
}

/// Alpaca Broker API authentication environment configuration
#[derive(Parser, Clone)]
pub struct AlpacaBrokerApiAuthEnv {
    /// Alpaca Broker API key
    #[clap(long, env)]
    pub alpaca_broker_api_key: String,

    /// Alpaca Broker API secret
    #[clap(long, env)]
    pub alpaca_broker_api_secret: String,

    /// Alpaca account ID for trading operations
    #[clap(long, env)]
    pub alpaca_account_id: String,

    /// Broker API mode: sandbox or production (defaults to sandbox for safety)
    #[clap(long, env, default_value = "sandbox")]
    pub alpaca_broker_api_mode: AlpacaBrokerApiMode,
}

impl std::fmt::Debug for AlpacaBrokerApiAuthEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaBrokerApiAuthEnv")
            .field("alpaca_broker_api_key", &"[REDACTED]")
            .field("alpaca_broker_api_secret", &"[REDACTED]")
            .field("alpaca_account_id", &self.alpaca_account_id)
            .field("alpaca_broker_api_mode", &self.alpaca_broker_api_mode)
            .finish()
    }
}

impl AlpacaBrokerApiAuthEnv {
    /// Returns the base URL for Alpaca Broker API.
    pub fn base_url(&self) -> &str {
        self.alpaca_broker_api_mode.base_url()
    }

    /// Returns true if using sandbox mode (paper trading).
    pub fn is_sandbox(&self) -> bool {
        !matches!(self.alpaca_broker_api_mode, AlpacaBrokerApiMode::Production)
    }
}

/// Account status from Alpaca Broker API
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AccountStatus {
    Onboarding,
    SubmissionFailed,
    Submitted,
    AccountUpdated,
    ApprovalPending,
    Active,
    Rejected,
    Disabled,
    DisableRequested,
    AccountClosed,
}

/// Response from the account verification endpoint
#[derive(Debug, Deserialize)]
pub(super) struct AccountResponse {
    pub id: Uuid,
    pub status: AccountStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_sandbox_config() -> AlpacaBrokerApiAuthEnv {
        AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key_id".to_string(),
            alpaca_broker_api_secret: "test_secret_key".to_string(),
            alpaca_account_id: "test_account_123".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
        }
    }

    fn create_test_production_config() -> AlpacaBrokerApiAuthEnv {
        AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key_id".to_string(),
            alpaca_broker_api_secret: "test_secret_key".to_string(),
            alpaca_account_id: "test_account_123".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Production,
        }
    }

    #[test]
    fn test_alpaca_broker_api_mode_urls() {
        assert_eq!(
            AlpacaBrokerApiMode::Sandbox.base_url(),
            "https://broker-api.sandbox.alpaca.markets"
        );
        assert_eq!(
            AlpacaBrokerApiMode::Production.base_url(),
            "https://broker-api.alpaca.markets"
        );
    }

    #[test]
    fn test_alpaca_broker_api_auth_env_base_url() {
        let sandbox_config = create_test_sandbox_config();
        assert_eq!(
            sandbox_config.base_url(),
            "https://broker-api.sandbox.alpaca.markets"
        );

        let production_config = create_test_production_config();
        assert_eq!(
            production_config.base_url(),
            "https://broker-api.alpaca.markets"
        );
    }

    #[test]
    fn test_alpaca_broker_api_auth_env_debug_redacts_secrets() {
        let config = AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "super_secret_key_123".to_string(),
            alpaca_broker_api_secret: "ultra_secret_secret_456".to_string(),
            alpaca_account_id: "account_789".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
        };

        let debug_output = format!("{config:?}");

        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("super_secret_key_123"));
        assert!(!debug_output.contains("ultra_secret_secret_456"));
        assert!(debug_output.contains("account_789"));
        assert!(debug_output.contains("Sandbox"));
    }
}
