use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Strongly typed Alpaca account identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaAccountId(Uuid);

impl AlpacaAccountId {
    pub const fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for AlpacaAccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

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
    pub account_id: AlpacaAccountId,
    pub mode: Option<AlpacaBrokerApiMode>,
}

impl AlpacaBrokerApiAuthConfig {
    pub fn mode(&self) -> AlpacaBrokerApiMode {
        self.mode.clone().unwrap_or(AlpacaBrokerApiMode::Sandbox)
    }

    pub fn base_url(&self) -> &str {
        self.mode.as_ref().map_or_else(
            || AlpacaBrokerApiMode::Sandbox.base_url(),
            |mode| mode.base_url(),
        )
    }

    pub fn is_sandbox(&self) -> bool {
        self.mode
            .as_ref()
            .is_none_or(|mode| !matches!(mode, AlpacaBrokerApiMode::Production))
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
    use uuid::uuid;

    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_test_sandbox_config() -> AlpacaBrokerApiAuthConfig {
        AlpacaBrokerApiAuthConfig {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: None,
        }
    }

    fn create_test_production_config() -> AlpacaBrokerApiAuthConfig {
        AlpacaBrokerApiAuthConfig {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            account_id: TEST_ACCOUNT_ID,
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
            account_id: TEST_ACCOUNT_ID,
            mode: None,
        };

        let debug_output = format!("{config:?}");

        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("super_secret_key_123"));
        assert!(!debug_output.contains("ultra_secret_secret_456"));
        assert!(debug_output.contains("904837e3-3b76-47ec-b432-046db621571b"));
        assert!(debug_output.contains("Sandbox"));
    }
}
