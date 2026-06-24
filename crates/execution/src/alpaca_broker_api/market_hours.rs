use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;
use tracing::{debug, warn};

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;

use crate::MarketSession;

/// Response from the Alpaca calendar endpoint
/// (https://docs.alpaca.markets/reference/getcalendar-1).
///
/// `date` identifies the trading day the entry describes, so callers can
/// verify the broker answered for the day they actually queried.
/// `open`/`close` are the regular trading hours (typically 09:30-16:00 ET).
/// `session_open`/`session_close` span the full extended session including
/// pre-market and after-hours (typically 04:00-20:00 ET). Alpaca only allows
/// `extended_hours: true` on limit orders, not market orders.
///
/// CONTRACT RISK: Alpaca's reference does not define `session_open`/
/// `session_close` semantics, and their observed values have changed over
/// time (community reports show 07:00/19:00 historically, 04:00/20:00
/// currently -- forum.alpaca.markets/t/2400). This module assumes they span
/// exactly the window in which Alpaca accepts `extended_hours: true` limit
/// orders, i.e. the 4:00-9:30/16:00-20:00 windows described in
/// https://docs.alpaca.markets/docs/orders-at-alpaca#extended-hours-trading.
/// If Alpaca redefines the session bounds (e.g. for 24/5 overnight trading),
/// `Extended` classification may cover times where extended-hours limit
/// orders are rejected; the failure mode is broker rejections of the hedge
/// order, retried by the hedge job, not silent misclassification of money
/// amounts.
#[derive(Debug, Clone, Deserialize)]
struct CalendarDay {
    date: NaiveDate,
    #[serde(deserialize_with = "deserialize_time")]
    open: NaiveTime,
    #[serde(deserialize_with = "deserialize_time")]
    close: NaiveTime,
    #[serde(deserialize_with = "deserialize_time")]
    session_open: NaiveTime,
    #[serde(deserialize_with = "deserialize_time")]
    session_close: NaiveTime,
}

fn deserialize_time<'de, D>(deserializer: D) -> Result<NaiveTime, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveTime::parse_from_str(&s, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(&s, "%H%M"))
        .map_err(serde::de::Error::custom)
}

/// Returns true if the market is currently open for trading.
pub(super) async fn is_market_open(
    client: &AlpacaBrokerApiClient,
) -> Result<bool, AlpacaBrokerApiError> {
    is_market_open_at(client, Utc::now()).await
}

/// Returns the current market session (regular, extended, or closed).
pub(super) async fn market_session(
    client: &AlpacaBrokerApiClient,
) -> Result<MarketSession, AlpacaBrokerApiError> {
    market_session_at(client, Utc::now()).await
}

/// Returns the market session at the given time.
async fn market_session_at(
    client: &AlpacaBrokerApiClient,
    now: DateTime<Utc>,
) -> Result<MarketSession, AlpacaBrokerApiError> {
    let now_et = now.with_timezone(&New_York);
    let today = now_et.date_naive();

    let calendar = get_calendar(client, today, today).await?;

    let Some(today_calendar) = calendar.into_iter().next() else {
        debug!("Today is not a trading day");
        return Ok(MarketSession::Closed);
    };

    // The broker may answer a non-trading-day query with the NEAREST trading
    // day instead of an empty list. A LATER date is positive evidence the
    // queried day has no trading session, so classify it Closed -- erroring
    // here would turn every weekend/holiday tick into a multi-day error storm
    // (failed scans, burned hedge-job retries) instead of the spec'd
    // "Closed: leave the exposure for the next scan". An EARLIER date proves
    // nothing about today and indicates a broken response, so fail fast
    // rather than classify against another day's session windows.
    if today_calendar.date > today {
        debug!(
            queried = %today,
            returned = %today_calendar.date,
            "Calendar returned a later trading day; queried day is not a trading day"
        );
        return Ok(MarketSession::Closed);
    }
    if today_calendar.date < today {
        return Err(AlpacaBrokerApiError::CalendarDateMismatch {
            queried: today,
            returned: today_calendar.date,
        });
    }

    // Detect a silent redefinition of the undocumented session bounds (see
    // the CONTRACT RISK note on `CalendarDay`). A NARROWED window is the
    // dangerous direction -- fills landing inside the assumed 04:00-20:00
    // extended window but outside Alpaca's would classify Closed and sit
    // unhedged with no broker rejection to surface it -- so warn loudly when
    // the broker's bounds differ from the documented extended-hours window.
    let expected_session_open = NaiveTime::from_hms_opt(4, 0, 0);
    let expected_session_close = NaiveTime::from_hms_opt(20, 0, 0);
    if Some(today_calendar.session_open) != expected_session_open {
        warn!(
            session_open = %today_calendar.session_open,
            "Alpaca calendar session_open differs from the assumed 04:00 ET \
             extended-hours open; session classification may not match \
             extended-hours order eligibility"
        );
    }
    // session_close legitimately narrows on early-close trading days
    // (half days end the post-market session early), so a mismatch there is
    // expected several days a year -- log it for visibility without paging
    // anyone. A redefinition narrowing REGULAR days would also surface in
    // hedge behavior (orders deferred to the next scan).
    if Some(today_calendar.session_close) != expected_session_close {
        debug!(
            session_close = %today_calendar.session_close,
            "Alpaca calendar session_close differs from the typical 20:00 ET \
             extended-hours close (expected on early-close days)"
        );
    }

    let now_time = now_et.time();

    let session = if now_time >= today_calendar.open && now_time < today_calendar.close {
        MarketSession::Regular
    } else if now_time >= today_calendar.session_open && now_time < today_calendar.session_close {
        MarketSession::Extended
    } else {
        MarketSession::Closed
    };

    debug!(
        regular_open = %today_calendar.open,
        regular_close = %today_calendar.close,
        session_open = %today_calendar.session_open,
        session_close = %today_calendar.session_close,
        now = %now_time,
        ?session,
        "Checked market session"
    );

    Ok(session)
}

/// Returns true if the market is open for regular trading at the given time.
///
/// Derived from [`market_session_at`] so the regular-hours predicate cannot
/// drift from the session classification: `is_market_open` is true exactly
/// when the session is [`MarketSession::Regular`].
async fn is_market_open_at(
    client: &AlpacaBrokerApiClient,
    now: DateTime<Utc>,
) -> Result<bool, AlpacaBrokerApiError> {
    Ok(market_session_at(client, now).await? == MarketSession::Regular)
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
    use uuid::uuid;

    use super::*;
    use crate::alpaca_broker_api::TimeInForce;
    use crate::alpaca_broker_api::auth::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    };

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
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
                        "close": "16:00",
                        "session_open": "0400",
                        "session_close": "2000"
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
            "close": "16:00",
            "session_open": "0400",
            "session_close": "2000"
        }"#;

        let day: CalendarDay = serde_json::from_str(json).unwrap();

        assert_eq!(day.open, NaiveTime::from_hms_opt(9, 30, 0).unwrap());
        assert_eq!(day.close, NaiveTime::from_hms_opt(16, 0, 0).unwrap());
        assert_eq!(day.session_open, NaiveTime::from_hms_opt(4, 0, 0).unwrap());
        assert_eq!(
            day.session_close,
            NaiveTime::from_hms_opt(20, 0, 0).unwrap()
        );
    }

    #[test]
    fn test_calendar_day_accepts_hhmm_format_without_colon() {
        let json = r#"{
            "date": "2025-01-06",
            "open": "0930",
            "close": "1600",
            "session_open": "0400",
            "session_close": "2000"
        }"#;

        let day: CalendarDay = serde_json::from_str(json).unwrap();

        assert_eq!(day.open, NaiveTime::from_hms_opt(9, 30, 0).unwrap());
        assert_eq!(day.close, NaiveTime::from_hms_opt(16, 0, 0).unwrap());
        assert_eq!(day.session_open, NaiveTime::from_hms_opt(4, 0, 0).unwrap());
        assert_eq!(
            day.session_close,
            NaiveTime::from_hms_opt(20, 0, 0).unwrap()
        );
    }

    #[test]
    fn test_calendar_day_rejects_invalid_hour() {
        let json = r#"{
            "date": "2025-01-06",
            "open": "25:30",
            "close": "16:00",
            "session_open": "0400",
            "session_close": "2000"
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
            "close": "16:00",
            "session_open": "0400",
            "session_close": "2000"
        }"#;

        let err = serde_json::from_str::<CalendarDay>(json).unwrap_err();
        assert!(
            err.to_string().contains("out of range")
                || err.to_string().contains("invalid characters"),
            "expected parse error for minute 60, got: {err}"
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
                        "close": "16:00",
                        "session_open": "0400",
                        "session_close": "2000"
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
    async fn is_market_open_false_during_extended_hours() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let pre_market = et_time_as_utc("2025-01-06", 7, 0);

        assert!(
            !is_market_open_at(&client, pre_market).await.unwrap(),
            "is_market_open must be true only during the Regular session, not pre-market"
        );
    }

    #[tokio::test]
    async fn market_session_is_closed_when_calendar_returns_a_later_trading_day() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        // Saturday query answered with Monday's entry (the nearest trading
        // day). A later date is positive evidence Saturday has no trading
        // session, so the session is Closed -- NOT an error, which would
        // storm every weekend tick, and NOT a classification against
        // Monday's session windows.
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", "2025-01-04")
                .query_param("end", "2025-01-04");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": "2025-01-06",
                        "open": "09:30",
                        "close": "16:00",
                        "session_open": "0400",
                        "session_close": "2000"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        // 18:00 ET Saturday would classify Extended against Monday's
        // session windows if the date guard were missing.
        let saturday_evening = et_time_as_utc("2025-01-04", 18, 0);

        let session = market_session_at(&client, saturday_evening).await.unwrap();

        assert_eq!(session, MarketSession::Closed);
    }

    #[tokio::test]
    async fn market_session_errors_when_calendar_returns_an_earlier_date() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        // An EARLIER date proves nothing about the queried day -- the
        // response is broken, so classification must fail fast rather than
        // trust another day's session windows.
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", "2025-01-07")
                .query_param("end", "2025-01-07");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": "2025-01-06",
                        "open": "09:30",
                        "close": "16:00",
                        "session_open": "0400",
                        "session_close": "2000"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let tuesday_midday = et_time_as_utc("2025-01-07", 12, 0);

        let error = market_session_at(&client, tuesday_midday)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::CalendarDateMismatch { queried, returned }
                    if queried == NaiveDate::from_ymd_opt(2025, 1, 7).unwrap()
                        && returned == NaiveDate::from_ymd_opt(2025, 1, 6).unwrap()
            ),
            "expected CalendarDateMismatch, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn is_market_open_is_false_when_calendar_returns_a_later_trading_day() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        // Saturday answered with Monday's entry: a later trading day means
        // the queried day is closed, so the regular-hours predicate is
        // false -- 12:00 ET would be inside Monday's regular hours if the
        // date guard were missing.
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", "2025-01-04")
                .query_param("end", "2025-01-04");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": "2025-01-06",
                        "open": "09:30",
                        "close": "16:00",
                        "session_open": "0400",
                        "session_close": "2000"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let saturday_noon = et_time_as_utc("2025-01-04", 12, 0);

        let open = is_market_open_at(&client, saturday_noon).await.unwrap();

        assert!(!open, "a non-trading day must report the market as closed");
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

    #[tokio::test]
    async fn market_session_regular_during_trading_hours() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let midday = et_time_as_utc("2025-01-06", 12, 0);

        assert_eq!(
            market_session_at(&client, midday).await.unwrap(),
            MarketSession::Regular
        );
    }

    #[tokio::test]
    async fn market_session_extended_pre_market() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let pre_market = et_time_as_utc("2025-01-06", 7, 0);

        assert_eq!(
            market_session_at(&client, pre_market).await.unwrap(),
            MarketSession::Extended,
            "7:00 AM ET is pre-market (between session_open 4:00 and open 9:30)"
        );
    }

    #[tokio::test]
    async fn market_session_extended_after_hours() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let after_hours = et_time_as_utc("2025-01-06", 18, 0);

        assert_eq!(
            market_session_at(&client, after_hours).await.unwrap(),
            MarketSession::Extended,
            "6:00 PM ET is after-hours (between close 16:00 and session_close 20:00)"
        );
    }

    #[tokio::test]
    async fn market_session_closed_before_extended_session() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let overnight = et_time_as_utc("2025-01-06", 3, 0);

        assert_eq!(
            market_session_at(&client, overnight).await.unwrap(),
            MarketSession::Closed,
            "3:00 AM ET is before session_open (4:00), should be Closed"
        );
    }

    #[tokio::test]
    async fn market_session_closed_after_extended_session() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let late_night = et_time_as_utc("2025-01-06", 21, 0);

        assert_eq!(
            market_session_at(&client, late_night).await.unwrap(),
            MarketSession::Closed,
            "9:00 PM ET is after session_close (20:00), should be Closed"
        );
    }

    #[tokio::test]
    async fn market_session_closed_on_non_trading_day() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day(&server, "2025-01-04");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let saturday = et_time_as_utc("2025-01-04", 12, 0);

        assert_eq!(
            market_session_at(&client, saturday).await.unwrap(),
            MarketSession::Closed
        );
    }

    #[tokio::test]
    async fn market_session_extended_at_session_open_boundary() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let at_session_open = et_time_as_utc("2025-01-06", 4, 0);

        assert_eq!(
            market_session_at(&client, at_session_open).await.unwrap(),
            MarketSession::Extended,
            "Exactly at session_open should be Extended"
        );
    }

    #[tokio::test]
    async fn market_session_regular_at_regular_open_boundary() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let at_open = et_time_as_utc("2025-01-06", 9, 30);

        assert_eq!(
            market_session_at(&client, at_open).await.unwrap(),
            MarketSession::Regular,
            "Exactly at regular open should be Regular"
        );
    }

    #[tokio::test]
    async fn market_session_extended_at_regular_close_boundary() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let at_close = et_time_as_utc("2025-01-06", 16, 0);

        assert_eq!(
            market_session_at(&client, at_close).await.unwrap(),
            MarketSession::Extended,
            "Exactly at regular close transitions to Extended (after-hours)"
        );
    }
}
