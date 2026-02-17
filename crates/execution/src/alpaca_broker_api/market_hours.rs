use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;
use tracing::debug;

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;

/// Response from the calendar endpoint for regular market hours.
/// We use regular hours (open/close) because Alpaca only allows extended_hours
/// with limit orders, not market orders.
#[derive(Debug, Clone, Deserialize)]
struct CalendarDay {
    #[serde(deserialize_with = "deserialize_time")]
    open: NaiveTime,
    #[serde(deserialize_with = "deserialize_time")]
    close: NaiveTime,
}

fn deserialize_time<'de, D>(deserializer: D) -> Result<NaiveTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let time_str = String::deserialize(deserializer)?;
    NaiveTime::parse_from_str(&time_str, "%H:%M").map_err(serde::de::Error::custom)
}

/// Returns true if the market is currently open for trading.
pub(super) async fn is_market_open(
    client: &AlpacaBrokerApiClient,
) -> Result<bool, AlpacaBrokerApiError> {
    is_market_open_at(client, Utc::now()).await
}

/// Returns true if the market is open at the given time.
async fn is_market_open_at(
    client: &AlpacaBrokerApiClient,
    now: DateTime<Utc>,
) -> Result<bool, AlpacaBrokerApiError> {
    let now_et = now.with_timezone(&New_York);
    let today = now_et.date_naive();

    let calendar = get_calendar(client, today, today).await?;

    let Some(today_calendar) = calendar.into_iter().next() else {
        debug!("Today is not a trading day");
        return Ok(false);
    };

    let now_time = now_et.time();
    let is_open = now_time >= today_calendar.open && now_time < today_calendar.close;

    debug!(
        open = %today_calendar.open,
        close = %today_calendar.close,
        now = %now_time,
        is_open,
        "Checked market hours"
    );

    Ok(is_open)
}

async fn get_calendar(
    client: &AlpacaBrokerApiClient,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<CalendarDay>, AlpacaBrokerApiError> {
    let url = format!(
        "{}/v1/calendar?start={}&end={}",
        client.base_url(),
        start.format("%Y-%m-%d"),
        end.format("%Y-%m-%d")
    );

    debug!("Fetching market calendar from {}", url);

    client.get(&url).await
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::TimeInForce;
    use crate::alpaca_broker_api::auth::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode};

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: "test_account_123".to_string(),
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
        }
    }

    #[tokio::test]
    async fn test_get_calendar_returns_market_hours() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", "2025-01-06")
                .query_param("end", "2025-01-06");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": "2025-01-06",
                        "open": "09:30",
                        "close": "16:00"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let date = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap();
        let calendar = get_calendar(&client, date, date).await.unwrap();

        mock.assert();
        assert_eq!(calendar.len(), 1);
        assert_eq!(calendar[0].open, NaiveTime::from_hms_opt(9, 30, 0).unwrap());
        assert_eq!(
            calendar[0].close,
            NaiveTime::from_hms_opt(16, 0, 0).unwrap()
        );
    }

    #[test]
    fn test_calendar_day_deserializes_real_api_format() {
        let json = r#"{
            "date": "2025-01-06",
            "open": "09:30",
            "close": "16:00"
        }"#;

        let day: CalendarDay = serde_json::from_str(json).unwrap();

        assert_eq!(day.open, NaiveTime::from_hms_opt(9, 30, 0).unwrap());
        assert_eq!(day.close, NaiveTime::from_hms_opt(16, 0, 0).unwrap());
    }

    #[test]
    fn test_calendar_day_rejects_hhmm_format_without_colon() {
        let json = r#"{
            "date": "2025-01-06",
            "open": "0930",
            "close": "1600"
        }"#;

        let err = serde_json::from_str::<CalendarDay>(json).unwrap_err();
        assert!(
            err.to_string().contains("invalid"),
            "expected parse error for missing colon, got: {err}"
        );
    }

    #[test]
    fn test_calendar_day_rejects_invalid_hour() {
        let json = r#"{
            "date": "2025-01-06",
            "open": "25:30",
            "close": "16:00"
        }"#;

        let err = serde_json::from_str::<CalendarDay>(json).unwrap_err();
        assert!(
            err.to_string().contains("out of range"),
            "expected out of range error for hour 25, got: {err}"
        );
    }

    #[test]
    fn test_calendar_day_rejects_invalid_minute() {
        let json = r#"{
            "date": "2025-01-06",
            "open": "09:60",
            "close": "16:00"
        }"#;

        let err = serde_json::from_str::<CalendarDay>(json).unwrap_err();
        assert!(
            err.to_string().contains("out of range"),
            "expected out of range error for minute 60, got: {err}"
        );
    }

    fn mock_trading_day(server: &MockServer, date: &str) {
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", date)
                .query_param("end", date);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": date,
                        "open": "09:30",
                        "close": "16:00"
                    }
                ]));
        });
    }

    fn mock_non_trading_day(server: &MockServer, date: &str) {
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", date)
                .query_param("end", date);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });
    }

    /// Constructs a UTC timestamp corresponding to a specific ET time on a given date.
    fn et_time_as_utc(date: &str, hour: u32, min: u32) -> DateTime<Utc> {
        let naive_date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        let naive_time = NaiveTime::from_hms_opt(hour, min, 0).unwrap();
        let naive_dt = naive_date.and_time(naive_time);
        naive_dt
            .and_local_timezone(New_York)
            .single()
            .unwrap()
            .with_timezone(&Utc)
    }

    #[tokio::test]
    async fn is_market_open_during_trading_hours() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let midday = et_time_as_utc("2025-01-06", 12, 0);

        assert!(is_market_open_at(&client, midday).await.unwrap());
    }

    #[tokio::test]
    async fn is_market_closed_before_open() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let before_open = et_time_as_utc("2025-01-06", 9, 0);

        assert!(!is_market_open_at(&client, before_open).await.unwrap());
    }

    #[tokio::test]
    async fn is_market_closed_at_close_time() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let at_close = et_time_as_utc("2025-01-06", 16, 0);

        assert!(!is_market_open_at(&client, at_close).await.unwrap());
    }

    #[tokio::test]
    async fn is_market_open_at_open_time() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let at_open = et_time_as_utc("2025-01-06", 9, 30);

        assert!(is_market_open_at(&client, at_open).await.unwrap());
    }

    #[tokio::test]
    async fn is_market_closed_on_non_trading_day() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day(&server, "2025-01-04");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let saturday = et_time_as_utc("2025-01-04", 12, 0);

        assert!(!is_market_open_at(&client, saturday).await.unwrap());
    }
}
