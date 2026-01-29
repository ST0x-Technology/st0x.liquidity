use serde::Deserialize;
use uuid::Uuid;

/// Mode for Alpaca Broker API
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlpacaBrokerApiMode {
    /// Sandbox environment (paper trading)
    Sandbox,
    /// Production environment (real money)
    Production,
    /// Mock mode for testing (available via `mock` feature or in tests)
    #[cfg(any(test, feature = "mock"))]
    #[serde(skip_deserializing)]
    Mock(String),
}

impl AlpacaBrokerApiMode {
    pub(super) fn base_url(&self) -> &str {
        match self {
            Self::Sandbox => "https://broker-api.sandbox.alpaca.markets",
            Self::Production => "https://broker-api.alpaca.markets",
            #[cfg(any(test, feature = "mock"))]
            Self::Mock(url) => url,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct AlpacaBrokerApiAuthConfig {
    pub api_key: String,
    pub api_secret: String,
    pub account_id: String,
    pub mode: Option<AlpacaBrokerApiMode>,
}

impl AlpacaBrokerApiAuthConfig {
    pub fn mode(&self) -> AlpacaBrokerApiMode {
        self.mode.clone().unwrap_or(AlpacaBrokerApiMode::Sandbox)
    }

    pub fn base_url(&self) -> &str {
        match &self.mode {
            Some(mode) => mode.base_url(),
            None => AlpacaBrokerApiMode::Sandbox.base_url(),
        }
    }

    pub fn is_sandbox(&self) -> bool {
        match &self.mode {
            Some(mode) => !matches!(mode, AlpacaBrokerApiMode::Production),
            None => true,
        }
    }
}

impl std::fmt::Debug for AlpacaBrokerApiAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaBrokerApiAuthConfig")
            .field("api_key", &"[REDACTED]")
            .field("api_secret", &"[REDACTED]")
            .field("account_id", &self.account_id)
            .field("mode", &self.mode())
            .finish()
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

    fn create_test_sandbox_config() -> AlpacaBrokerApiAuthConfig {
        AlpacaBrokerApiAuthConfig {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            account_id: "test_account_123".to_string(),
            mode: None,
        }
    }

    fn create_test_production_config() -> AlpacaBrokerApiAuthConfig {
        AlpacaBrokerApiAuthConfig {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            account_id: "test_account_123".to_string(),
            mode: Some(AlpacaBrokerApiMode::Production),
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
        let config = AlpacaBrokerApiAuthConfig {
            api_key: "super_secret_key_123".to_string(),
            api_secret: "ultra_secret_secret_456".to_string(),
            account_id: "account_789".to_string(),
            mode: None,
        };

        let debug_output = format!("{config:?}");

        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("super_secret_key_123"));
        assert!(!debug_output.contains("ultra_secret_secret_456"));
        assert!(debug_output.contains("account_789"));
        assert!(debug_output.contains("Sandbox"));
    }
}
