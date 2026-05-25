use std::str::FromStr;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::TimeInForce;
/// Strongly typed Alpaca account identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaAccountId(Uuid);

impl AlpacaAccountId {
    pub const fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for AlpacaAccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for AlpacaAccountId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(value).map(Self)
    }
}

/// Mode for Alpaca Broker API
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlpacaBrokerApiMode {
    /// Sandbox environment (paper trading)
    Sandbox,
    /// Production environment (real money)
    Production,
    /// Mock mode for testing (available via `mock` feature or in tests)
    #[cfg(any(test, feature = "mock"))]
    Mock(String),
}

impl<'de> Deserialize<'de> for AlpacaBrokerApiMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(AlpacaBrokerApiModeVisitor)
    }
}

struct AlpacaBrokerApiModeVisitor;

impl<'de> Visitor<'de> for AlpacaBrokerApiModeVisitor {
    type Value = AlpacaBrokerApiMode;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .write_str("\"sandbox\", \"production\", or { type = \"mock\", base_url = \"...\" }")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match value {
            "sandbox" => Ok(AlpacaBrokerApiMode::Sandbox),
            "production" => Ok(AlpacaBrokerApiMode::Production),
            #[cfg(any(test, feature = "mock"))]
            "mock" => Err(E::custom(
                "mock mode requires { type = \"mock\", base_url = \"...\" }",
            )),
            #[cfg(not(any(test, feature = "mock")))]
            "mock" => Err(E::custom("mock mode requires the mock feature")),
            _ => Err(E::unknown_variant(value, &["sandbox", "production"])),
        }
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut mode_type = None;
        // `base_url` is only meaningful for the mock variant, which is
        // feature-gated. Without the mock feature the value is consumed and
        // discarded so parsing still succeeds (and yields the clearer
        // "mock mode requires the mock feature" error below).
        #[cfg(any(test, feature = "mock"))]
        let mut base_url = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => mode_type = Some(map.next_value::<String>()?),
                #[cfg(any(test, feature = "mock"))]
                "base_url" => base_url = Some(map.next_value::<String>()?),
                #[cfg(not(any(test, feature = "mock")))]
                "base_url" => {
                    map.next_value::<de::IgnoredAny>()?;
                }
                _ => return Err(de::Error::unknown_field(&key, &["type", "base_url"])),
            }
        }

        match mode_type.as_deref() {
            #[cfg(any(test, feature = "mock"))]
            Some("mock") => {
                let base_url = base_url.ok_or_else(|| de::Error::missing_field("base_url"))?;
                Ok(AlpacaBrokerApiMode::Mock(base_url))
            }
            #[cfg(not(any(test, feature = "mock")))]
            Some("mock") => Err(de::Error::custom("mock mode requires the mock feature")),
            Some(other) => Err(de::Error::unknown_variant(
                other,
                &["sandbox", "production", "mock"],
            )),
            None => Err(de::Error::missing_field("type")),
        }
    }
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

    pub(super) fn market_data_base_url(&self) -> &str {
        match self {
            Self::Sandbox | Self::Production => "https://data.alpaca.markets",
            #[cfg(any(test, feature = "mock"))]
            Self::Mock(url) => url,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct AlpacaBrokerApiCtx {
    pub api_key: String,
    pub api_secret: String,
    pub account_id: AlpacaAccountId,
    pub mode: Option<AlpacaBrokerApiMode>,
    #[serde(
        default = "default_asset_cache_ttl_secs",
        deserialize_with = "deserialize_duration_secs"
    )]
    pub asset_cache_ttl: std::time::Duration,
    #[serde(default)]
    pub time_in_force: TimeInForce,
    pub counter_trade_slippage_bps: u16,
}

fn default_asset_cache_ttl_secs() -> std::time::Duration {
    std::time::Duration::from_secs(3600)
}

fn deserialize_duration_secs<'de, D>(deserializer: D) -> Result<std::time::Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let secs = u64::deserialize(deserializer)?;
    Ok(std::time::Duration::from_secs(secs))
}

impl AlpacaBrokerApiCtx {
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

impl std::fmt::Debug for AlpacaBrokerApiCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaBrokerApiCtx")
            .field("api_key", &"[REDACTED]")
            .field("api_secret", &"[REDACTED]")
            .field("account_id", &self.account_id)
            .field("mode", &self.mode())
            .field("asset_cache_ttl", &self.asset_cache_ttl)
            .field("time_in_force", &self.time_in_force)
            .field(
                "counter_trade_slippage_bps",
                &self.counter_trade_slippage_bps,
            )
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

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
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
        let sandbox_ctx = create_test_ctx(AlpacaBrokerApiMode::Sandbox);
        assert_eq!(
            sandbox_ctx.base_url(),
            "https://broker-api.sandbox.alpaca.markets"
        );

        let production_ctx = create_test_ctx(AlpacaBrokerApiMode::Production);
        assert_eq!(
            production_ctx.base_url(),
            "https://broker-api.alpaca.markets"
        );
    }

    #[test]
    fn test_alpaca_broker_api_mode_deserializes_mock_table() {
        let mode: AlpacaBrokerApiMode =
            serde_json::from_str(r#"{"type":"mock","base_url":"http://127.0.0.1:1234"}"#).unwrap();

        assert_eq!(
            mode,
            AlpacaBrokerApiMode::Mock("http://127.0.0.1:1234".to_string())
        );
    }

    #[test]
    fn test_alpaca_broker_api_mode_deserializes_string_forms() {
        let sandbox: AlpacaBrokerApiMode = serde_json::from_str(r#""sandbox""#).unwrap();
        assert_eq!(sandbox, AlpacaBrokerApiMode::Sandbox);

        let production: AlpacaBrokerApiMode = serde_json::from_str(r#""production""#).unwrap();
        assert_eq!(production, AlpacaBrokerApiMode::Production);
    }

    #[test]
    fn test_alpaca_broker_api_mode_unknown_string_errors() {
        let error = serde_json::from_str::<AlpacaBrokerApiMode>(r#""fidelity""#).unwrap_err();
        assert!(
            error.to_string().contains("fidelity"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_alpaca_broker_api_mode_mock_table_missing_base_url_errors() {
        let error = serde_json::from_str::<AlpacaBrokerApiMode>(r#"{"type":"mock"}"#).unwrap_err();
        assert!(
            error.to_string().contains("base_url"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_alpaca_broker_api_mode_unknown_type_errors() {
        let error =
            serde_json::from_str::<AlpacaBrokerApiMode>(r#"{"type":"fidelity"}"#).unwrap_err();
        assert!(
            error.to_string().contains("fidelity"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_alpaca_broker_api_mode_unknown_field_errors() {
        let error = serde_json::from_str::<AlpacaBrokerApiMode>(
            r#"{"type":"mock","base_url":"http://x","extra":"y"}"#,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("extra"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_alpaca_account_id_from_str() {
        let account_id: AlpacaAccountId = "904837e3-3b76-47ec-b432-046db621571b".parse().unwrap();

        assert_eq!(account_id, TEST_ACCOUNT_ID);
    }

    #[test]
    fn test_alpaca_account_id_from_str_invalid() {
        let result = "not-a-uuid".parse::<AlpacaAccountId>();
        assert!(result.is_err());
    }

    #[test]
    fn test_alpaca_broker_api_auth_env_debug_redacts_secrets() {
        let ctx = AlpacaBrokerApiCtx {
            api_key: "super_secret_key_123".to_string(),
            api_secret: "ultra_secret_secret_456".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: None,
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        let debug_output = format!("{ctx:?}");

        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("super_secret_key_123"));
        assert!(!debug_output.contains("ultra_secret_secret_456"));
        assert!(debug_output.contains("904837e3-3b76-47ec-b432-046db621571b"));
        assert!(debug_output.contains("Sandbox"));
    }
}
