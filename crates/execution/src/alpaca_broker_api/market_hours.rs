use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;
use tracing::{debug, info};

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;

/// Response from the calendar endpoint for regular market hours.
/// We use regular hours (open/close) because Alpaca only allows extended_hours
/// with limit orders, not market orders.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct CalendarDay {
    #[serde(deserialize_with = "deserialize_date")]
    pub date: NaiveDate,
    #[serde(deserialize_with = "deserialize_hhmm_time")]
    pub open: NaiveTime,
    #[serde(deserialize_with = "deserialize_hhmm_time")]
    pub close: NaiveTime,
}

fn deserialize_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(serde::de::Error::custom)
}

fn deserialize_hhmm_time<'de, D>(deserializer: D) -> Result<NaiveTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.len() != 4 {
        return Err(serde::de::Error::custom(format!(
            "expected HHMM format, got: {s}"
        )));
    }

    let hour: u32 = s[0..2]
        .parse()
        .map_err(|_| serde::de::Error::custom(format!("invalid hour in: {s}")))?;
    let min: u32 = s[2..4]
        .parse()
        .map_err(|_| serde::de::Error::custom(format!("invalid minute in: {s}")))?;

    NaiveTime::from_hms_opt(hour, min, 0)
        .ok_or_else(|| serde::de::Error::custom(format!("invalid time: {s}")))
}

enum MarketStatus {
    NotTradingDay { next_day: CalendarDay },
    BeforeOpen { today: CalendarDay },
    AfterClose { next_day: CalendarDay },
    Open { today: CalendarDay },
}

async fn check_market_status(
    client: &AlpacaBrokerApiClient,
    now_et: chrono::DateTime<chrono_tz::Tz>,
) -> Result<MarketStatus, AlpacaBrokerApiError> {
    let today = now_et.date_naive();
    let calendar = get_calendar(client, today, today).await?;

    if calendar.is_empty() {
        let next_day = find_next_trading_day(client, today).await?;
        return Ok(MarketStatus::NotTradingDay { next_day });
    }

    let today_calendar = calendar.into_iter().next().expect("checked non-empty");
    let now_et_time = now_et.time();

    if now_et_time < today_calendar.open {
        return Ok(MarketStatus::BeforeOpen {
            today: today_calendar,
        });
    }

    if now_et_time >= today_calendar.close {
        let tomorrow = today + chrono::Duration::days(1);
        let next_day = find_next_trading_day(client, tomorrow).await?;
        return Ok(MarketStatus::AfterClose { next_day });
    }

    Ok(MarketStatus::Open {
        today: today_calendar,
    })
}

async fn wait_for_next_session(
    day: &CalendarDay,
    now: chrono::DateTime<Utc>,
    reason: &str,
) -> Result<(), AlpacaBrokerApiError> {
    let wait_duration = duration_until_session_start(day, now)?;
    info!(
        next_trading_day = %day.date,
        open = %day.open,
        wait_secs = wait_duration.as_secs(),
        "{reason}"
    );
    tokio::time::sleep(wait_duration).await;
    Ok(())
}

/// Waits until regular market hours are open and returns the duration until
/// the market closes.
///
/// Regular market hours are 9:30am-4pm ET on trading days. This respects
/// market holidays and early closures.
pub(super) async fn wait_until_market_open(
    client: &AlpacaBrokerApiClient,
) -> Result<std::time::Duration, AlpacaBrokerApiError> {
    loop {
        let now = Utc::now();
        let now_et = now.with_timezone(&New_York);

        match check_market_status(client, now_et).await? {
            MarketStatus::NotTradingDay { next_day } => {
                wait_for_next_session(
                    &next_day,
                    now,
                    "Today is not a trading day, waiting for next session",
                )
                .await?;
            }
            MarketStatus::BeforeOpen { today } => {
                wait_for_next_session(&today, now, "Before market open, waiting").await?;
            }
            MarketStatus::AfterClose { next_day } => {
                wait_for_next_session(&next_day, now, "After market close, waiting for next day")
                    .await?;
            }
            MarketStatus::Open { today } => {
                let duration = duration_until_session_end(&today, now)?;
                debug!(
                    close = %today.close,
                    duration_secs = duration.as_secs(),
                    "Market is open"
                );
                return Ok(duration);
            }
        }
    }
}

async fn find_next_trading_day(
    client: &AlpacaBrokerApiClient,
    from_date: NaiveDate,
) -> Result<CalendarDay, AlpacaBrokerApiError> {
    let to_date = from_date + chrono::Duration::days(10);

    let calendar = get_calendar(client, from_date, to_date).await?;

    calendar
        .into_iter()
        .next()
        .ok_or_else(|| AlpacaBrokerApiError::NoTradingDaysFound {
            from: from_date.format("%Y-%m-%d").to_string(),
            to: to_date.format("%Y-%m-%d").to_string(),
        })
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

fn duration_until_session_start(
    day: &CalendarDay,
    now: chrono::DateTime<Utc>,
) -> Result<std::time::Duration, AlpacaBrokerApiError> {
    let session_start_naive = day.date.and_time(day.open);
    let session_start_et = New_York
        .from_local_datetime(&session_start_naive)
        .single()
        .ok_or_else(|| {
            AlpacaBrokerApiError::CalendarParse(format!(
                "Ambiguous datetime: {} {}",
                day.date, day.open
            ))
        })?;
    let session_start_utc = session_start_et.with_timezone(&Utc);

    (session_start_utc - now)
        .to_std()
        .map_err(|_| AlpacaBrokerApiError::DurationConversion)
}

fn duration_until_session_end(
    day: &CalendarDay,
    now: chrono::DateTime<Utc>,
) -> Result<std::time::Duration, AlpacaBrokerApiError> {
    let now_et = now.with_timezone(&New_York);
    let date = now_et.date_naive();
    let session_end_naive = date.and_time(day.close);
    let session_end_et = New_York
        .from_local_datetime(&session_end_naive)
        .single()
        .ok_or_else(|| {
            AlpacaBrokerApiError::CalendarParse(format!(
                "Ambiguous datetime: {} {}",
                day.date, day.close
            ))
        })?;
    let session_end_utc = session_end_et.with_timezone(&Utc);

    (session_end_utc - now)
        .to_std()
        .map_err(|_| AlpacaBrokerApiError::DurationConversion)
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::alpaca_broker_api::auth::{AlpacaBrokerApiAuthEnv, AlpacaBrokerApiMode};

    fn create_test_config(base_url: &str) -> AlpacaBrokerApiAuthEnv {
        AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key".to_string(),
            alpaca_broker_api_secret: "test_secret".to_string(),
            alpaca_account_id: "test_account_123".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Mock(base_url.to_string()),
        }
    }

    fn calendar_day(date: &str, open: (u32, u32), close: (u32, u32)) -> CalendarDay {
        CalendarDay {
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            open: NaiveTime::from_hms_opt(open.0, open.1, 0).unwrap(),
            close: NaiveTime::from_hms_opt(close.0, close.1, 0).unwrap(),
        }
    }

    #[tokio::test]
    async fn test_find_next_trading_day() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", "2025-01-04")
                .query_param("end", "2025-01-14");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": "2025-01-06",
                        "open": "0930",
                        "close": "1600"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let from_date = NaiveDate::from_ymd_opt(2025, 1, 4).unwrap();
        let next_day = find_next_trading_day(&client, from_date).await.unwrap();

        mock.assert();
        assert_eq!(next_day.date, NaiveDate::from_ymd_opt(2025, 1, 6).unwrap());
    }

    #[tokio::test]
    async fn test_find_next_trading_day_no_days_found() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET).path("/v1/calendar");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let from_date = NaiveDate::from_ymd_opt(2025, 1, 4).unwrap();
        let result = find_next_trading_day(&client, from_date).await;

        mock.assert();
        assert!(matches!(
            result.unwrap_err(),
            AlpacaBrokerApiError::NoTradingDaysFound { .. }
        ));
    }

    #[test]
    fn test_duration_until_session_end() {
        let day = calendar_day("2025-01-06", (9, 30), (16, 0));

        // 2pm ET = 7pm UTC on Jan 6, 2025
        // Market closes at 4pm ET = 9pm UTC on Jan 6, 2025
        // Duration should be 2 hours
        let now = Utc.with_ymd_and_hms(2025, 1, 6, 19, 0, 0).unwrap();
        let duration = duration_until_session_end(&day, now).unwrap();
        assert_eq!(duration.as_secs(), 2 * 3600);
    }

    #[test]
    fn test_duration_until_session_start() {
        let day = calendar_day("2025-01-06", (9, 30), (16, 0));

        // 9am ET = 2pm UTC on Jan 6, 2025
        // Market opens at 9:30am ET = 2:30pm UTC on Jan 6, 2025
        // Duration should be 30 minutes
        let now = Utc.with_ymd_and_hms(2025, 1, 6, 14, 0, 0).unwrap();
        let duration = duration_until_session_start(&day, now).unwrap();
        assert_eq!(duration.as_secs(), 30 * 60);
    }

    #[tokio::test]
    async fn test_get_calendar_with_naivedate() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

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
                        "open": "0930",
                        "close": "1600"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let date = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap();
        let calendar = get_calendar(&client, date, date).await.unwrap();

        mock.assert();
        assert_eq!(calendar.len(), 1);
        assert_eq!(calendar[0].date, date);
        assert_eq!(calendar[0].open, NaiveTime::from_hms_opt(9, 30, 0).unwrap());
        assert_eq!(
            calendar[0].close,
            NaiveTime::from_hms_opt(16, 0, 0).unwrap()
        );
    }
}
