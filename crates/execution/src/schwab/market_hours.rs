use backon::{ExponentialBuilder, Retryable};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::{Tz, US::Eastern};
use reqwest::header::{self, HeaderMap, HeaderValue};
use serde::Deserialize;
use sqlx::SqlitePool;
use tracing::debug;

use super::{SchwabAuthEnv, SchwabError, SchwabTokens};

/// Market session types for trading hours.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketSession {
    PreMarket,
    Regular,
    AfterHours,
}

impl std::str::FromStr for MarketSession {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PRE_MARKET" => Ok(Self::PreMarket),
            "REGULAR" => Ok(Self::Regular),
            "AFTER_HOURS" => Ok(Self::AfterHours),
            _ => Err(format!("Invalid market session: {s}")),
        }
    }
}

/// Market status representing whether the market is currently open or closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketStatus {
    Open,
    Closed,
}

/// Market hours information for a specific date and session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketHours {
    pub date: NaiveDate,
    pub session_type: MarketSession,
    /// Start time in Eastern timezone (None for closed days)
    pub start: Option<DateTime<Tz>>,
    /// End time in Eastern timezone (None for closed days)
    pub end: Option<DateTime<Tz>>,
    pub is_open: bool,
}

impl MarketHours {
    /// Get current market status based on current time.
    pub fn current_status(&self) -> MarketStatus {
        if !self.is_open {
            return MarketStatus::Closed;
        }

        let Some(start) = self.start else {
            return MarketStatus::Closed;
        };

        let Some(end) = self.end else {
            return MarketStatus::Closed;
        };

        let now = Utc::now().with_timezone(&Eastern);

        if now >= start && now < end {
            MarketStatus::Open
        } else {
            MarketStatus::Closed
        }
    }
}

/// Raw API response structure for market hours endpoint.
/// Uses serde to deserialize the Schwab API response format.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketHoursResponse {
    equity: Option<EquityMarketHours>,
}

#[derive(Debug, Deserialize)]
struct EquityMarketHours {
    #[serde(rename = "EQ")]
    eq: Option<SessionHours>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_field_names)]
struct SessionHours {
    date: String,
    #[allow(dead_code)]
    market_type: String,
    #[allow(dead_code)]
    exchange: Option<String>,
    #[allow(dead_code)]
    category: Option<String>,
    #[allow(dead_code)]
    product: String,
    #[allow(dead_code)]
    product_name: Option<String>,
    is_open: bool,
    session_hours: Option<SessionHoursDetail>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_field_names)]
struct SessionHoursDetail {
    #[allow(dead_code)]
    pre_market: Option<Vec<TimeRange>>,
    regular_market: Option<Vec<TimeRange>>,
    #[allow(dead_code)]
    post_market: Option<Vec<TimeRange>>,
}

#[derive(Debug, Deserialize)]
struct TimeRange {
    start: String,
    end: String,
}

/// Fetch market hours for a specific date from the Schwab Market Data API.
///
/// Uses the `/marketdata/v1/markets/{marketId}` endpoint with "equity" as the market ID.
/// Returns market hours in Eastern timezone per the API specification.
pub async fn fetch_market_hours(
    env: &SchwabAuthEnv,
    pool: &SqlitePool,
    date: Option<&str>,
) -> Result<MarketHours, SchwabError> {
    let access_token = SchwabTokens::get_valid_access_token(pool, env).await?;

    let headers = [
        (
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {access_token}"))?,
        ),
        (header::ACCEPT, HeaderValue::from_str("application/json")?),
    ]
    .into_iter()
    .collect::<HeaderMap>();

    let mut url = format!("{}/marketdata/v1/markets/equity", env.schwab_base_url);

    if let Some(date_param) = date {
        use std::fmt::Write;
        write!(url, "?date={date_param}").map_err(|e| SchwabError::RequestFailed {
            action: "format URL".to_string(),
            status: reqwest::StatusCode::OK,
            body: format!("Failed to format date parameter: {e}"),
        })?;
    }

    debug!("Fetching market hours from: {url}");

    let client = reqwest::Client::new();
    let response = (|| async { client.get(&url).headers(headers.clone()).send().await })
        .retry(ExponentialBuilder::default())
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "Failed to read response body".to_string());
        return Err(SchwabError::RequestFailed {
            action: "fetch market hours".to_string(),
            status,
            body,
        });
    }

    let market_hours_response: MarketHoursResponse = response.json().await?;

    parse_market_hours_response(market_hours_response, date)
}

fn parse_market_hours_response(
    response: MarketHoursResponse,
    _requested_date: Option<&str>,
) -> Result<MarketHours, SchwabError> {
    let Some(equity) = response.equity else {
        return Err(SchwabError::RequestFailed {
            action: "parse market hours".to_string(),
            status: reqwest::StatusCode::OK,
            body: "No equity market hours in response".to_string(),
        });
    };

    let Some(eq) = equity.eq else {
        return Err(SchwabError::RequestFailed {
            action: "parse market hours".to_string(),
            status: reqwest::StatusCode::OK,
            body: "No EQ section in equity market hours".to_string(),
        });
    };

    let date = parse_date(&eq.date).map_err(|e| SchwabError::RequestFailed {
        action: "parse market hours date".to_string(),
        status: reqwest::StatusCode::OK,
        body: format!("Invalid date format '{}': {e}", eq.date),
    })?;

    // If market is closed (weekends/holidays), return closed market hours
    if !eq.is_open {
        return Ok(MarketHours {
            date,
            session_type: MarketSession::Regular,
            start: None,
            end: None,
            is_open: false,
        });
    }

    let Some(session_hours) = eq.session_hours else {
        return Ok(MarketHours {
            date,
            session_type: MarketSession::Regular,
            start: None,
            end: None,
            is_open: false,
        });
    };

    // Extract regular market hours (9:30 AM - 4:00 PM ET)
    let Some(regular_market) = session_hours.regular_market else {
        return Ok(MarketHours {
            date,
            session_type: MarketSession::Regular,
            start: None,
            end: None,
            is_open: false,
        });
    };

    let Some(time_range) = regular_market.first() else {
        return Ok(MarketHours {
            date,
            session_type: MarketSession::Regular,
            start: None,
            end: None,
            is_open: false,
        });
    };

    let start = parse_datetime(&time_range.start, date)?;
    let end = parse_datetime(&time_range.end, date)?;

    Ok(MarketHours {
        date,
        session_type: MarketSession::Regular,
        start: Some(start.with_timezone(&Eastern)),
        end: Some(end.with_timezone(&Eastern)),
        is_open: true,
    })
}

fn parse_date(date_str: &str) -> Result<NaiveDate, chrono::ParseError> {
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
}

fn parse_datetime(datetime_str: &str, date: NaiveDate) -> Result<DateTime<Utc>, SchwabError> {
    // Schwab API returns datetime strings like "2025-01-03T09:30:00-05:00"
    if let Ok(dt) = DateTime::parse_from_rfc3339(datetime_str) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Fallback: try parsing just time and combine with date
    if let Ok(time) = chrono::NaiveTime::parse_from_str(datetime_str, "%H:%M:%S") {
        let naive_dt = date.and_time(time);
        if let Some(eastern_dt) = Eastern.from_local_datetime(&naive_dt).single() {
            return Ok(eastern_dt.with_timezone(&Utc));
        }
    }

    // Return proper error for unsupported formats
    Err(SchwabError::RequestFailed {
        action: "parse datetime".to_string(),
        status: reqwest::StatusCode::OK,
        body: format!("Unsupported datetime format: '{datetime_str}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{TEST_ENCRYPTION_KEY, setup_test_db, setup_test_tokens};
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_test_env_with_mock_server(mock_server: &MockServer) -> SchwabAuthEnv {
        SchwabAuthEnv {
            schwab_app_key: "test_app_key".to_string(),
            schwab_app_secret: "test_app_secret".to_string(),
            schwab_redirect_uri: "https://127.0.0.1".to_string(),
            schwab_base_url: mock_server.base_url(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    #[tokio::test]
    async fn test_fetch_market_hours_open_market() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let mock_response = json!({
            "equity": {
                "EQ": {
                    "date": "2025-01-03",
                    "marketType": "EQUITY",
                    "exchange": "NYSE",
                    "category": "EQUITY",
                    "product": "EQ",
                    "productName": "Equity",
                    "isOpen": true,
                    "sessionHours": {
                        "preMarket": [{
                            "start": "2025-01-03T04:00:00-05:00",
                            "end": "2025-01-03T09:30:00-05:00"
                        }],
                        "regularMarket": [{
                            "start": "2025-01-03T09:30:00-05:00",
                            "end": "2025-01-03T16:00:00-05:00"
                        }],
                        "postMarket": [{
                            "start": "2025-01-03T16:00:00-05:00",
                            "end": "2025-01-03T20:00:00-05:00"
                        }]
                    }
                }
            }
        });

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/marketdata/v1/markets/equity")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "application/json");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = fetch_market_hours(&env, &pool, None).await;

        mock.assert();
        let market_hours = result.unwrap();
        assert_eq!(
            market_hours.date,
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap()
        );
        assert_eq!(market_hours.session_type, MarketSession::Regular);
        assert!(market_hours.is_open);
        assert!(market_hours.start.is_some());
        assert!(market_hours.end.is_some());
    }

    #[tokio::test]
    async fn test_fetch_market_hours_closed_market() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let mock_response = json!({
            "equity": {
                "EQ": {
                    "date": "2025-01-04",
                    "marketType": "EQUITY",
                    "exchange": "NYSE",
                    "category": "EQUITY",
                    "product": "EQ",
                    "productName": "Equity",
                    "isOpen": false
                }
            }
        });

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/marketdata/v1/markets/equity")
                .query_param("date", "2025-01-04")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "application/json");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let result = fetch_market_hours(&env, &pool, Some("2025-01-04")).await;

        mock.assert();
        let market_hours = result.unwrap();
        assert_eq!(
            market_hours.date,
            NaiveDate::from_ymd_opt(2025, 1, 4).unwrap()
        );
        assert_eq!(market_hours.session_type, MarketSession::Regular);
        assert!(!market_hours.is_open);
        assert!(market_hours.start.is_none());
        assert!(market_hours.end.is_none());
        assert_eq!(market_hours.current_status(), MarketStatus::Closed);
    }

    #[tokio::test]
    async fn test_fetch_market_hours_api_error() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let mock = server.mock(|when, then| {
            when.method(GET).path("/marketdata/v1/markets/equity");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({"error": "Internal server error"}));
        });

        let result = fetch_market_hours(&env, &pool, None).await;

        mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RequestFailed { action, status, .. }
            if action == "fetch market hours" && status.as_u16() == 500
        ));
    }

    #[tokio::test]
    async fn test_fetch_market_hours_invalid_response() {
        let server = MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let mock = server.mock(|when, then| {
            when.method(GET).path("/marketdata/v1/markets/equity");
            then.status(200)
                .header("content-type", "application/json")
                .body("invalid json");
        });

        let result = fetch_market_hours(&env, &pool, None).await;

        mock.assert();
        assert!(matches!(result.unwrap_err(), SchwabError::Reqwest(_)));
    }

    #[test]
    fn test_market_session_from_str() {
        assert_eq!(
            "PRE_MARKET".parse::<MarketSession>().unwrap(),
            MarketSession::PreMarket
        );
        assert_eq!(
            "REGULAR".parse::<MarketSession>().unwrap(),
            MarketSession::Regular
        );
        assert_eq!(
            "AFTER_HOURS".parse::<MarketSession>().unwrap(),
            MarketSession::AfterHours
        );

        let result = "INVALID".parse::<MarketSession>();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid market session: INVALID");
    }

    #[test]
    fn test_parse_date_valid() {
        let result = parse_date("2025-01-03");
        assert_eq!(
            result.unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap()
        );
    }

    #[test]
    fn test_parse_date_invalid() {
        let result = parse_date("invalid-date");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_datetime_rfc3339() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 3).unwrap();
        let result = parse_datetime("2025-01-03T09:30:00-05:00", date);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_datetime_time_only() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 3).unwrap();
        let result = parse_datetime("09:30:00", date);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_datetime_invalid() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 3).unwrap();
        let result = parse_datetime("invalid-time", date);
        assert!(matches!(
            result.unwrap_err(),
            SchwabError::RequestFailed { action, .. }
            if action == "parse datetime"
        ));
    }
}
