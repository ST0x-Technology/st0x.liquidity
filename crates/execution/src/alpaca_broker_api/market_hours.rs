use std::cmp::Ordering;

use chrono::{DateTime, Days, NaiveDate, NaiveTime, Timelike, Utc};
use chrono_tz::America::New_York;
use serde::Deserialize;
use tracing::{debug, warn};

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;
use crate::{MarketSession, MarketSessionStatus, PostCloseGap};

const NEXT_SESSION_LOOKAHEAD_DAYS: u64 = 14;

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
/// If Alpaca redefines the session bounds, `Extended` classification may
/// cover times where extended-hours limit orders are rejected; the failure
/// mode is broker rejections of the hedge order, retried by the hedge job,
/// not silent misclassification of money amounts.
///
/// The calendar does NOT model the 24/5 overnight session. Its fixed
/// 20:00-04:00 ET window is derived from adjacent trading days instead: the
/// evening leg (20:00-24:00 on day D) exists iff D+1 is a trading day, the
/// morning leg (00:00-04:00 on day D) exists iff D is a trading day. See
/// SPEC "External contract (Alpaca 24/5 overnight)".
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

/// Returns the current market session (regular, extended, overnight, or
/// closed).
pub(super) async fn market_session(
    client: &AlpacaBrokerApiClient,
) -> Result<MarketSession, AlpacaBrokerApiError> {
    market_session_at(client, Utc::now()).await
}

/// Returns the current market session with extended-session close metadata.
pub(super) async fn market_session_status(
    client: &AlpacaBrokerApiClient,
) -> Result<MarketSessionStatus, AlpacaBrokerApiError> {
    market_session_status_at(client, Utc::now()).await
}

/// Returns the market session at the given time.
///
/// Deliberately does NOT route through `market_session_status_at`: that
/// function conditionally performs a second calendar HTTP round trip (the
/// post-close-gap lookahead) whenever the session is Extended. Plain session
/// callers -- `is_market_open`, and the per-symbol readiness/cancellation
/// checks that only need the session value -- never consume that
/// metadata, so routing them through the status path would pay for an
/// avoidable broker call on every extended-hours tick. Only
/// `market_session_status_at` (consumed by the close-flatten path) needs it.
pub(super) async fn market_session_at(
    client: &AlpacaBrokerApiClient,
    now: DateTime<Utc>,
) -> Result<MarketSession, AlpacaBrokerApiError> {
    Ok(session_and_close_at(client, now).await?.session)
}

/// Returns the market session and extended-session close at the given time.
pub(super) async fn market_session_status_at(
    client: &AlpacaBrokerApiClient,
    now: DateTime<Utc>,
) -> Result<MarketSessionStatus, AlpacaBrokerApiError> {
    let SessionAndClose {
        session,
        extended_session_closes_at,
        today,
    } = session_and_close_at(client, now).await?;

    // `post_close_gap` is only meaningful once the session is Extended (see
    // `CloseFlattenPolicy::active_window`, which discards it otherwise), and
    // computing it issues a full calendar HTTP round trip. Skip the network
    // call entirely for the far more common Regular/Closed cases.
    let post_close_gap = if session == MarketSession::Extended {
        classify_post_close_gap(client, today).await
    } else {
        PostCloseGap::Unknown
    };

    Ok(MarketSessionStatus {
        session,
        extended_session_closes_at,
        post_close_gap,
    })
}

/// Session classification plus the extended-session close time, without the
/// post-close-gap lookahead. Shared by the lightweight `market_session_at`
/// path and `market_session_status_at`, which layers the lookahead on top
/// only when the session is Extended.
struct SessionAndClose {
    session: MarketSession,
    extended_session_closes_at: Option<DateTime<Utc>>,
    /// The queried trading day, in Alpaca's calendar timezone. Threaded back
    /// out so `market_session_status_at` can feed it to
    /// `classify_post_close_gap` without recomputing the timezone
    /// conversion.
    today: NaiveDate,
}

async fn session_and_close_at(
    client: &AlpacaBrokerApiClient,
    now: DateTime<Utc>,
) -> Result<SessionAndClose, AlpacaBrokerApiError> {
    let now_et = now.with_timezone(&New_York);
    let today = now_et.date_naive();

    let Some(tomorrow) = today.checked_add_days(Days::new(1)) else {
        warn!(%today, "Could not compute next calendar day; classifying the session as closed");
        return Ok(SessionAndClose {
            session: MarketSession::Closed,
            extended_session_closes_at: None,
            today,
        });
    };

    // One request answers both overnight legs: today's entry classifies the
    // regular/extended windows and the morning leg, tomorrow's presence
    // decides the evening leg.
    let calendar = get_calendar(client, today, tomorrow).await?;

    // The broker may answer a non-trading-day query with the NEAREST trading
    // day instead of only the days in range. A date PAST the queried window
    // is positive evidence that neither queried day trades, so ignore it --
    // erroring would turn every weekend/holiday tick into a multi-day error
    // storm (failed scans, burned hedge-job retries) instead of the spec'd
    // "Closed: leave the exposure for the next scan". An EARLIER date proves
    // nothing about the queried days and indicates a broken response, so
    // fail fast rather than classify against another day's session windows.
    let mut today_calendar = None;
    let mut tomorrow_is_trading_day = false;
    for day in calendar {
        match day.date.cmp(&today) {
            Ordering::Less => {
                return Err(AlpacaBrokerApiError::CalendarDateMismatch {
                    queried: today,
                    returned: day.date,
                });
            }
            Ordering::Equal => today_calendar = Some(day),
            Ordering::Greater if day.date == tomorrow => tomorrow_is_trading_day = true,
            Ordering::Greater => {
                debug!(
                    queried = %today,
                    returned = %day.date,
                    "Calendar returned a trading day past the queried window"
                );
            }
        }
    }

    let now_time = now_et.time();
    // The overnight session spans fixed clock times -- 20:00-04:00 ET -- by
    // Alpaca's 24/5 contract; it is not described by any calendar field (see
    // the CalendarDay doc). An early close therefore leaves a Closed gap
    // between `session_close` and 20:00 rather than starting overnight early.
    let in_overnight_evening_leg = now_time.hour() >= 20;
    let in_overnight_morning_leg = now_time.hour() < 4;

    let Some(today_calendar) = today_calendar else {
        // The evening leg needs only tomorrow to trade: Sunday 20:00 and a
        // holiday's own 20:00 both start the next trading day's overnight
        // session even though today itself never traded.
        if in_overnight_evening_leg && tomorrow_is_trading_day {
            return Ok(SessionAndClose {
                session: MarketSession::Overnight,
                extended_session_closes_at: None,
                today,
            });
        }
        debug!("Today is not a trading day");
        return Ok(SessionAndClose {
            session: MarketSession::Closed,
            extended_session_closes_at: None,
            today,
        });
    };

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

    let extended_session_closes_at = local_market_time_to_utc(today, today_calendar.session_close)?;

    let session = if now_time >= today_calendar.open && now_time < today_calendar.close {
        MarketSession::Regular
    } else if now_time >= today_calendar.session_open && now_time < today_calendar.session_close {
        MarketSession::Extended
    } else if in_overnight_evening_leg && tomorrow_is_trading_day {
        MarketSession::Overnight
    } else if in_overnight_morning_leg {
        // Today's calendar entry exists, so today is a trading day and its
        // overnight morning leg (00:00-04:00) is open.
        MarketSession::Overnight
    } else {
        MarketSession::Closed
    };

    debug!(
        regular_open = %today_calendar.open,
        regular_close = %today_calendar.close,
        session_open = %today_calendar.session_open,
        session_close = %today_calendar.session_close,
        now = %now_time,
        tomorrow_is_trading_day,
        ?session,
        "Checked market session"
    );

    // During Overnight, today's extended close is already in the past, and
    // the calendar-less Overnight legs (Sunday, a holiday's own evening)
    // report None -- keep the field consistent per session rather than
    // dependent on whether today happened to trade.
    let extended_session_closes_at = match session {
        MarketSession::Overnight => None,
        MarketSession::Regular | MarketSession::Extended | MarketSession::Closed => {
            Some(extended_session_closes_at)
        }
    };

    Ok(SessionAndClose {
        session,
        extended_session_closes_at,
        today,
    })
}

async fn classify_post_close_gap(
    client: &AlpacaBrokerApiClient,
    current_trading_day: NaiveDate,
) -> PostCloseGap {
    let Some(start) = current_trading_day.checked_add_days(Days::new(1)) else {
        warn!(%current_trading_day, "Could not compute next calendar day; treating post-close gap as unknown");
        return PostCloseGap::Unknown;
    };
    let Some(end) = current_trading_day.checked_add_days(Days::new(NEXT_SESSION_LOOKAHEAD_DAYS))
    else {
        warn!(%current_trading_day, "Could not compute calendar lookahead; treating post-close gap as unknown");
        return PostCloseGap::Unknown;
    };

    let calendar = match get_calendar(client, start, end).await {
        Ok(calendar) => calendar,
        Err(error) => {
            warn!(
                %error,
                %current_trading_day,
                "Failed to fetch next trading session; treating post-close gap as unknown"
            );
            return PostCloseGap::Unknown;
        }
    };

    let Some(next_trading_day) = calendar
        .into_iter()
        .map(|day| day.date)
        .filter(|date| *date > current_trading_day)
        .min()
    else {
        warn!(
            %current_trading_day,
            lookahead_days = NEXT_SESSION_LOOKAHEAD_DAYS,
            "Calendar did not identify the next trading session; treating post-close gap as unknown"
        );
        return PostCloseGap::Unknown;
    };

    if next_trading_day == start {
        PostCloseGap::OrdinaryOvernight
    } else {
        PostCloseGap::MultiDayClosure
    }
}

fn local_market_time_to_utc(
    date: NaiveDate,
    time: NaiveTime,
) -> Result<DateTime<Utc>, AlpacaBrokerApiError> {
    date.and_time(time)
        .and_local_timezone(New_York)
        .single()
        .map(|date_time| date_time.with_timezone(&Utc))
        .ok_or(AlpacaBrokerApiError::CalendarLocalTimeUnresolvable { date, time })
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
    use chrono::TimeZone;
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
            auth: crate::AlpacaBrokerAuth::Basic {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
            },
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

    fn calendar_entry(
        date: &str,
        open: &str,
        close: &str,
        session_open: &str,
        session_close: &str,
    ) -> serde_json::Value {
        json!({
            "date": date,
            "open": open,
            "close": close,
            "session_open": session_open,
            "session_close": session_close
        })
    }

    fn trading_day_entry(date: &str) -> serde_json::Value {
        calendar_entry(date, "09:30", "16:00", "0400", "2000")
    }

    fn next_date(date: &str) -> String {
        NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .unwrap()
            .checked_add_days(Days::new(1))
            .unwrap()
            .format("%Y-%m-%d")
            .to_string()
    }

    /// Mocks the classification window `[date, date+1]` with the given
    /// calendar entries.
    fn mock_calendar_window(server: &MockServer, date: &str, entries: serde_json::Value) {
        let end = next_date(date);
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", date)
                .query_param("end", &end);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(entries);
        });
    }

    /// An ordinary trading day followed by another trading day (a weekday
    /// pair like Monday/Tuesday).
    fn mock_trading_day(server: &MockServer, date: &str) {
        let tomorrow = next_date(date);
        mock_calendar_window(
            server,
            date,
            json!([trading_day_entry(date), trading_day_entry(&tomorrow)]),
        );
    }

    /// A trading day whose NEXT day does not trade (a Friday, or the eve of
    /// a full holiday).
    fn mock_last_trading_day_before_gap(server: &MockServer, date: &str) {
        mock_calendar_window(server, date, json!([trading_day_entry(date)]));
    }

    /// A non-trading day whose next day trades (a Sunday, or a holiday whose
    /// following day is a trading day).
    fn mock_non_trading_day_before_trading_day(server: &MockServer, date: &str) {
        let tomorrow = next_date(date);
        mock_calendar_window(server, date, json!([trading_day_entry(&tomorrow)]));
    }

    /// A non-trading day whose next day does not trade either (a Saturday,
    /// or a holiday followed by a weekend).
    fn mock_non_trading_day(server: &MockServer, date: &str) {
        mock_calendar_window(server, date, json!([]));
    }

    fn mock_next_trading_day(
        server: &MockServer,
        range_start: &str,
        range_end: &str,
        next_trading_day: Option<&str>,
    ) {
        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", range_start)
                .query_param("end", range_end);
            let body = next_trading_day.map_or_else(
                || json!([]),
                |date| {
                    json!([{
                        "date": date,
                        "open": "09:30",
                        "close": "16:00",
                        "session_open": "0400",
                        "session_close": "2000"
                    }])
                },
            );
            then.status(200)
                .header("content-type", "application/json")
                .json_body(body);
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
        // day). A date past the queried [Saturday, Sunday] window is positive
        // evidence neither day trades, so the session is Closed -- NOT an
        // error, which would storm every weekend tick, and NOT a
        // classification against Monday's session windows.
        mock_calendar_window(
            &server,
            "2025-01-04",
            json!([trading_day_entry("2025-01-06")]),
        );

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
        mock_calendar_window(
            &server,
            "2025-01-07",
            json!([trading_day_entry("2025-01-06")]),
        );

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

        // Saturday answered with Monday's entry: a trading day past the
        // queried window means the queried day is closed, so the
        // regular-hours predicate is false -- 12:00 ET would be inside
        // Monday's regular hours if the date guard were missing.
        mock_calendar_window(
            &server,
            "2025-01-04",
            json!([trading_day_entry("2025-01-06")]),
        );

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
    async fn market_session_status_exposes_extended_session_close() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let after_hours = et_time_as_utc("2025-01-06", 18, 0);

        let status = market_session_status_at(&client, after_hours)
            .await
            .unwrap();

        assert_eq!(status.session, MarketSession::Extended);
        assert_eq!(
            status.extended_session_closes_at,
            Some(et_time_as_utc("2025-01-06", 20, 0))
        );
    }

    #[tokio::test]
    async fn market_session_at_never_triggers_post_close_gap_lookahead_even_when_extended() {
        // `market_session_at`/`market_session` only ever consume `.session`,
        // never the close-gap metadata -- so unlike `market_session_status_at`,
        // they must skip the lookahead call even while the session IS
        // Extended (readiness/cancellation checks poll this every tick during
        // extended hours). Asserting zero hits pins that the network call
        // never fires on this path, not merely that a failure would be
        // masked by `classify_post_close_gap`'s Unknown fallback.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");
        let lookahead_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", "2025-01-07")
                .query_param("end", "2025-01-20");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": "2025-01-07",
                        "open": "09:30",
                        "close": "16:00",
                        "session_open": "0400",
                        "session_close": "2000"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let after_hours = et_time_as_utc("2025-01-06", 18, 0);

        let session = market_session_at(&client, after_hours).await.unwrap();

        assert_eq!(session, MarketSession::Extended);
        lookahead_mock.assert_calls(0);
    }

    #[tokio::test]
    async fn market_session_status_skips_post_close_gap_lookahead_when_not_extended() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");
        // The post-close-gap lookahead call must not fire outside the
        // Extended session -- it's only meaningful for close-flatten, which
        // only activates during Extended. Asserting zero hits pins that the
        // network call is skipped, not merely that its (swallowed) failure
        // is masked by `classify_post_close_gap`'s Unknown fallback.
        let lookahead_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", "2025-01-07")
                .query_param("end", "2025-01-20");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "date": "2025-01-07",
                        "open": "09:30",
                        "close": "16:00",
                        "session_open": "0400",
                        "session_close": "2000"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let midday = et_time_as_utc("2025-01-06", 12, 0);

        let status = market_session_status_at(&client, midday).await.unwrap();

        assert_eq!(status.session, MarketSession::Regular);
        assert_eq!(status.post_close_gap, PostCloseGap::Unknown);
        lookahead_mock.assert_calls(0);
    }

    #[tokio::test]
    async fn market_session_status_classifies_ordinary_weekday_overnight() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");
        mock_next_trading_day(&server, "2025-01-07", "2025-01-20", Some("2025-01-07"));

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let status = market_session_status_at(&client, et_time_as_utc("2025-01-06", 19, 50))
            .await
            .unwrap();

        assert_eq!(status.post_close_gap, PostCloseGap::OrdinaryOvernight);
    }

    #[tokio::test]
    async fn market_session_status_classifies_friday_weekend_gap() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-10");
        mock_next_trading_day(&server, "2025-01-11", "2025-01-24", Some("2025-01-13"));

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let status = market_session_status_at(&client, et_time_as_utc("2025-01-10", 19, 50))
            .await
            .unwrap();

        assert_eq!(status.post_close_gap, PostCloseGap::MultiDayClosure);
    }

    #[tokio::test]
    async fn market_session_status_classifies_weekday_holiday_gap() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-07-03");
        mock_next_trading_day(&server, "2025-07-04", "2025-07-17", Some("2025-07-07"));

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let status = market_session_status_at(&client, et_time_as_utc("2025-07-03", 16, 50))
            .await
            .unwrap();

        assert_eq!(status.post_close_gap, PostCloseGap::MultiDayClosure);
    }

    #[tokio::test]
    async fn market_session_status_treats_missing_next_session_as_unknown() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");
        mock_next_trading_day(&server, "2025-01-07", "2025-01-20", None);

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let status = market_session_status_at(&client, et_time_as_utc("2025-01-06", 19, 50))
            .await
            .unwrap();

        assert_eq!(status.post_close_gap, PostCloseGap::Unknown);
    }

    #[tokio::test]
    async fn market_session_status_uses_early_close_session_close() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        // July 3 half-day; July 4 is a full holiday, so no tomorrow entry.
        mock_calendar_window(
            &server,
            "2025-07-03",
            json!([calendar_entry(
                "2025-07-03",
                "09:30",
                "13:00",
                "0400",
                "1700"
            )]),
        );
        mock_next_trading_day(&server, "2025-07-04", "2025-07-17", Some("2025-07-07"));

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let after_regular_close = et_time_as_utc("2025-07-03", 13, 30);

        let status = market_session_status_at(&client, after_regular_close)
            .await
            .unwrap();

        assert_eq!(status.session, MarketSession::Extended);
        assert_eq!(
            status.extended_session_closes_at,
            Some(et_time_as_utc("2025-07-03", 17, 0))
        );
        assert_eq!(status.post_close_gap, PostCloseGap::MultiDayClosure);
    }

    #[tokio::test]
    async fn market_session_overnight_before_extended_session() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let overnight = et_time_as_utc("2025-01-06", 3, 0);

        assert_eq!(
            market_session_at(&client, overnight).await.unwrap(),
            MarketSession::Overnight,
            "3:00 AM ET on a trading day is the overnight morning leg"
        );
    }

    #[tokio::test]
    async fn market_session_overnight_after_extended_session() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let late_night = et_time_as_utc("2025-01-06", 21, 0);

        assert_eq!(
            market_session_at(&client, late_night).await.unwrap(),
            MarketSession::Overnight,
            "9:00 PM ET on a weeknight before a trading day is the overnight evening leg"
        );
    }

    #[tokio::test]
    async fn weeknight_overnight_reports_no_extended_close_like_the_sunday_leg() {
        // A weeknight Overnight evening takes the today-calendar branch,
        // where today's 20:00 close is already in the past; the Sunday
        // and holiday Overnight legs report None. The field must be
        // consistent per session, not depend on whether today traded.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");
        mock_next_trading_day(&server, "2025-01-07", "2025-01-20", Some("2025-01-07"));

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let status = market_session_status_at(&client, et_time_as_utc("2025-01-06", 21, 0))
            .await
            .unwrap();

        assert_eq!(status.session, MarketSession::Overnight);
        assert_eq!(status.extended_session_closes_at, None);
    }

    #[tokio::test]
    async fn market_session_uses_early_close_calendar_boundaries() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        // July 3 half-day; July 4 is a full holiday, so no tomorrow entry.
        mock_calendar_window(
            &server,
            "2025-07-03",
            json!([calendar_entry(
                "2025-07-03",
                "09:30",
                "13:00",
                "0400",
                "1700"
            )]),
        );

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let after_regular_close = et_time_as_utc("2025-07-03", 13, 1);
        let session_close = et_time_as_utc("2025-07-03", 17, 0);

        assert_eq!(
            market_session_at(&client, after_regular_close)
                .await
                .unwrap(),
            MarketSession::Extended,
            "After an early regular close should be Extended until session_close"
        );
        assert_eq!(
            market_session_at(&client, session_close).await.unwrap(),
            MarketSession::Closed,
            "Exactly at early session_close should be Closed"
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

    #[tokio::test]
    async fn market_session_overnight_at_session_close_boundary() {
        // The extended window is half-open: `now < session_close`, so 20:00 ET
        // exactly (the documented after-hours close) is no longer Extended --
        // it is the first instant of the overnight evening leg when the next
        // day trades. Pins the 20:00 hand-over edge.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let at_session_close = et_time_as_utc("2025-01-06", 20, 0);

        assert_eq!(
            market_session_at(&client, at_session_close).await.unwrap(),
            MarketSession::Overnight,
            "Exactly at session_close (20:00 ET) the overnight session begins"
        );
    }

    #[tokio::test]
    async fn market_session_overnight_at_morning_leg_end_boundary() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let just_before_session_open = et_time_as_utc("2025-01-06", 3, 59);

        assert_eq!(
            market_session_at(&client, just_before_session_open)
                .await
                .unwrap(),
            MarketSession::Overnight,
            "3:59 AM ET is still the overnight morning leg; 4:00 hands over to Extended"
        );
    }

    #[tokio::test]
    async fn market_session_sunday_evening_is_overnight() {
        // Sunday is not a trading day, but Monday is: the overnight evening
        // leg opens at 20:00 because the NEXT day trades. 2025-01-05 is a
        // Sunday, 2025-01-06 a Monday.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day_before_trading_day(&server, "2025-01-05");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-01-05", 20, 0))
                .await
                .unwrap(),
            MarketSession::Overnight,
            "Sunday 20:00 ET starts the trading week's first overnight session"
        );
        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-01-05", 19, 59))
                .await
                .unwrap(),
            MarketSession::Closed,
            "Sunday 19:59 ET is still the weekend"
        );
    }

    #[tokio::test]
    async fn market_session_friday_evening_is_closed() {
        // 2025-01-10 is a Friday; Saturday does not trade, so no overnight
        // session follows the Friday extended close.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_last_trading_day_before_gap(&server, "2025-01-10");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-01-10", 20, 0))
                .await
                .unwrap(),
            MarketSession::Closed,
            "Friday 20:00 ET has no overnight session (Saturday does not trade)"
        );
        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-01-10", 21, 0))
                .await
                .unwrap(),
            MarketSession::Closed
        );
    }

    #[tokio::test]
    async fn market_session_saturday_overnight_hours_are_closed() {
        // Saturday trades on neither leg: Friday evening never opened an
        // overnight session (morning leg needs Saturday to trade) and
        // Saturday evening does not lead into a trading Sunday.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day(&server, "2025-01-04");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-01-04", 3, 0))
                .await
                .unwrap(),
            MarketSession::Closed
        );
        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-01-04", 21, 0))
                .await
                .unwrap(),
            MarketSession::Closed
        );
    }

    #[tokio::test]
    async fn market_session_holiday_evening_is_overnight() {
        // Thanksgiving 2025-11-27 (Thursday) is a full holiday, but Friday
        // 2025-11-28 trades: the holiday's own 20:00 starts the next trading
        // day's overnight session, exactly like a Sunday evening.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day_before_trading_day(&server, "2025-11-27");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-11-27", 21, 0))
                .await
                .unwrap(),
            MarketSession::Overnight,
            "a holiday evening trades overnight when the next day is a trading day"
        );
    }

    #[tokio::test]
    async fn market_session_holiday_eve_evening_is_closed() {
        // 2025-11-26 (Wednesday) trades, but Thanksgiving follows: the
        // overnight session immediately preceding a full holiday does not
        // run.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_last_trading_day_before_gap(&server, "2025-11-26");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-11-26", 20, 30))
                .await
                .unwrap(),
            MarketSession::Closed,
            "no overnight session on the eve of a full holiday"
        );
    }

    #[tokio::test]
    async fn market_session_holiday_morning_is_closed() {
        // The holiday's own morning leg (00:00-04:00) is closed: the morning
        // leg belongs to the current day's trade date, and a holiday has
        // none. (The preceding eve at 20:00 never opened it either.)
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day_before_trading_day(&server, "2025-11-27");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-11-27", 3, 0))
                .await
                .unwrap(),
            MarketSession::Closed
        );
    }

    #[tokio::test]
    async fn market_session_early_close_gap_is_closed_then_overnight() {
        // An early close narrows session_close (here 17:00) but the overnight
        // session still opens at the fixed 20:00 ET: the 17:00-20:00 gap is
        // Closed, then Overnight when the next day trades.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_calendar_window(
            &server,
            "2025-01-06",
            json!([
                calendar_entry("2025-01-06", "09:30", "13:00", "0400", "1700"),
                trading_day_entry("2025-01-07"),
            ]),
        );

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-01-06", 17, 30))
                .await
                .unwrap(),
            MarketSession::Closed,
            "the gap between an early session_close and 20:00 ET is Closed"
        );
        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-01-06", 20, 0))
                .await
                .unwrap(),
            MarketSession::Overnight,
            "the overnight session opens at the fixed 20:00 ET even after an early close"
        );
    }

    #[tokio::test]
    async fn market_session_status_skips_post_close_gap_lookahead_when_overnight() {
        // Overnight ticks must stay as cheap as Regular/Closed ones: the
        // post-close-gap lookahead is close-flatten metadata and close
        // flattening only activates during Extended.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-01-06");
        let lookahead_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/calendar")
                .query_param("start", "2025-01-07")
                .query_param("end", "2025-01-20");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([trading_day_entry("2025-01-07")]));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let status = market_session_status_at(&client, et_time_as_utc("2025-01-06", 21, 0))
            .await
            .unwrap();

        assert_eq!(status.session, MarketSession::Overnight);
        assert_eq!(status.post_close_gap, PostCloseGap::Unknown);
        lookahead_mock.assert_calls(0);
    }

    #[tokio::test]
    async fn market_session_overnight_boundaries_hold_on_dst_fall_back_sunday() {
        // 2025-11-02 is the DST fall-back Sunday in America/New_York: clocks
        // leave EDT (UTC-4) during the night and 20:00 ET that evening is
        // already EST (UTC-5). The 19:59/20:00 boundary must land on the
        // post-transition offset -- a classifier doing fixed-offset math
        // would place 20:00 EST an hour off.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day_before_trading_day(&server, "2025-11-02");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-11-02", 19, 59))
                .await
                .unwrap(),
            MarketSession::Closed,
            "19:59 EST on the fall-back Sunday is still the weekend"
        );
        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-11-02", 20, 0))
                .await
                .unwrap(),
            MarketSession::Overnight,
            "20:00 EST on the fall-back Sunday starts the week's overnight session"
        );
    }

    #[tokio::test]
    async fn market_session_morning_leg_boundaries_hold_after_dst_fall_back() {
        // The Monday after the 2025-11-02 fall-back trades on EST. The
        // overnight morning leg must still hand over to Extended exactly at
        // 04:00 local, now one UTC hour later than the preceding Friday.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2025-11-03");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-11-03", 3, 59))
                .await
                .unwrap(),
            MarketSession::Overnight
        );
        assert_eq!(
            market_session_at(&client, et_time_as_utc("2025-11-03", 4, 0))
                .await
                .unwrap(),
            MarketSession::Extended
        );
    }

    #[tokio::test]
    async fn market_session_overnight_boundaries_hold_on_dst_spring_forward_sunday() {
        // 2026-03-08 is the spring-forward Sunday: 02:00-03:00 EST never
        // occurs and the evening runs on EDT (UTC-4). The Sunday-open rule
        // must hold at the new offset.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day_before_trading_day(&server, "2026-03-08");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2026-03-08", 19, 59))
                .await
                .unwrap(),
            MarketSession::Closed
        );
        assert_eq!(
            market_session_at(&client, et_time_as_utc("2026-03-08", 20, 0))
                .await
                .unwrap(),
            MarketSession::Overnight,
            "20:00 EDT on the spring-forward Sunday starts the week's overnight session"
        );
    }

    #[tokio::test]
    async fn market_session_morning_leg_boundaries_hold_after_dst_spring_forward() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_trading_day(&server, "2026-03-09");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert_eq!(
            market_session_at(&client, et_time_as_utc("2026-03-09", 3, 59))
                .await
                .unwrap(),
            MarketSession::Overnight
        );
        assert_eq!(
            market_session_at(&client, et_time_as_utc("2026-03-09", 4, 0))
                .await
                .unwrap(),
            MarketSession::Extended
        );
    }

    #[tokio::test]
    async fn market_session_classifies_both_passes_of_the_repeated_fall_back_hour() {
        // During the 2025-11-02 fall-back, the wall-clock times 01:00-02:00
        // ET occur twice: once as EDT (05:00-06:00 UTC) and once as EST
        // (06:00-07:00 UTC). Classification consumes a UTC instant, so both
        // passes are distinct, unambiguous inputs -- `et_time_as_utc` cannot
        // even construct them, hence the raw UTC timestamps. Both land on a
        // non-trading Sunday morning and classify Closed.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        mock_non_trading_day_before_trading_day(&server, "2025-11-02");

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let first_pass_edt = Utc.with_ymd_and_hms(2025, 11, 2, 5, 30, 0).unwrap();
        let second_pass_est = Utc.with_ymd_and_hms(2025, 11, 2, 6, 30, 0).unwrap();

        assert_eq!(
            market_session_at(&client, first_pass_edt).await.unwrap(),
            MarketSession::Closed
        );
        assert_eq!(
            market_session_at(&client, second_pass_est).await.unwrap(),
            MarketSession::Closed
        );
    }

    #[tokio::test]
    async fn market_session_propagates_a_calendar_http_failure() {
        // A failed calendar response must fail closed AND be observable: the
        // classifier returns the error to its caller (which defers the
        // hedge and logs) rather than silently classifying against unknown
        // session bounds.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        server.mock(|when, then| {
            when.method(GET).path("/v1/calendar");
            then.status(503)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "temporarily unavailable" }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let error = market_session_at(&client, et_time_as_utc("2025-01-06", 12, 0))
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                AlpacaBrokerApiError::ApiError { status, .. }
                    if *status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            ),
            "expected the 503 to propagate as ApiError, got: {error:?}"
        );
    }

    #[test]
    fn local_market_time_to_utc_rejects_ambiguous_dst_fallback_time() {
        // 2025-11-02 is the DST fall-back date in America/New_York: clocks
        // move from 02:00 EDT back to 01:00 EST, so every local time in
        // [01:00, 02:00) occurs twice and cannot be resolved to a single
        // UTC instant by `and_local_timezone(..).single()`.
        let date = NaiveDate::from_ymd_opt(2025, 11, 2).unwrap();
        let time = NaiveTime::from_hms_opt(1, 30, 0).unwrap();

        let error = local_market_time_to_utc(date, time).unwrap_err();

        assert!(matches!(
            error,
            AlpacaBrokerApiError::CalendarLocalTimeUnresolvable {
                date: error_date,
                time: error_time,
            } if error_date == date && error_time == time
        ));
    }

    #[test]
    fn local_market_time_to_utc_rejects_nonexistent_dst_spring_forward_time() {
        // 2026-03-08 is the DST spring-forward date in America/New_York:
        // clocks jump from 02:00 EST directly to 03:00 EDT, so every local
        // time in [02:00, 03:00) never occurs and `and_local_timezone(..)`
        // returns `LocalResult::None` -- the other `.single() == None` case
        // besides the fall-back ambiguity covered above.
        let date = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        let time = NaiveTime::from_hms_opt(2, 30, 0).unwrap();

        let error = local_market_time_to_utc(date, time).unwrap_err();

        assert!(matches!(
            error,
            AlpacaBrokerApiError::CalendarLocalTimeUnresolvable {
                date: error_date,
                time: error_time,
            } if error_date == date && error_time == time
        ));
    }

    #[test]
    fn local_market_time_to_utc_resolves_ordinary_session_close_time() {
        // Companion to the ambiguous-time test above: an ordinary session
        // close (20:00 ET, never ambiguous) must resolve to a single UTC
        // instant rather than hitting the `CalendarLocalTimeUnresolvable`
        // fallback.
        let date = NaiveDate::from_ymd_opt(2025, 1, 6).unwrap();
        let time = NaiveTime::from_hms_opt(20, 0, 0).unwrap();

        let resolved = local_market_time_to_utc(date, time).unwrap();

        assert_eq!(resolved, et_time_as_utc("2025-01-06", 20, 0));
    }
}
