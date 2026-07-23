//! Shared `Retry-After` header parsing for Alpaca's broker, wallet, and
//! market-data HTTP clients (RAI-1494). A single parser so every client
//! captures the header the same way instead of duplicating the delay-seconds
//! vs. HTTP-date branching per call site.

use std::str::FromStr;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use reqwest::header::{HeaderMap, RETRY_AFTER};

/// Parses an HTTP `Retry-After` header value into a [`Duration`] relative to
/// `now`.
///
/// Tries the delay-seconds form (`"120"`) first, then falls back to the
/// HTTP-date form (RFC 7231's preferred IMF-fixdate, e.g.
/// `"Sun, 06 Nov 1994 08:49:37 GMT"`).
///
/// **Not pinned to a confirmed real Alpaca 429 response** (RAI-1494 review
/// finding): every 429 test in this codebase synthesizes the header via
/// `httpmock` rather than replaying a captured live response, and Alpaca's
/// own official SDKs are, if anything, evidence AGAINST relying on it --
/// neither `alpacahq/alpaca-py`'s `RetryHTTPAdapter`
/// (`alpaca/common/rest.py`, `DEFAULT_RETRY_WAIT_SECONDS`) nor
/// `alpacahq/alpaca-trade-api-go`'s `ClientOpts.RetryDelay`
/// (`marketdata/rest_test.go`'s `TestDefaultDo_TooMany429s`) read ANY
/// response header on a 429 -- both just sleep a fixed configured delay and
/// retry, blind to whatever the server sent. This does not prove Alpaca
/// never sends `Retry-After`, only that neither official SDK trusts one.
/// This is exactly why the caller (`decide_backpressure`) must degrade
/// gracefully when this parser returns `None`: an absent or unparseable
/// header falls back to a tested escalating backoff, so a wrong assumption
/// here only makes the backoff sub-optimal, never breaks correctness.
///
/// `"0"` parses to `Some(Duration::ZERO)` -- a legitimate broker value.
/// Flooring a near-zero delay to a minimum sleep is the caller's concern
/// (`decide_backpressure`), not this parser's.
///
/// Returns `None` for a malformed value, a date at or before `now`, or a date
/// `SystemTime` cannot represent the difference for. A parse failure here is
/// not a financial value -- falling back to the caller's escalating backoff
/// is the correct, intentionally soft behavior, unlike this codebase's
/// fail-fast rule for financial data.
///
/// `pub` (re-exported from the crate root, module itself stays private) so
/// `st0x-tokenization`'s Alpaca client can reuse the same parser for its own
/// `Retry-After` capture rather than duplicating the delay-seconds/HTTP-date
/// branching a second time.
///
/// Alpaca's documented 429 error body carries a numeric `code` and `message`,
/// no `Retry-After` header -- the header-absence case is already pinned by
/// `retry_after_from_response_headers_returns_none_when_absent` below (this
/// parser only ever reads headers, so a documented-body fixture would assert
/// nothing that case doesn't already cover), and
/// `decide_backpressure_escalates_the_fallback_when_retry_after_is_absent`
/// (`st0x-hedge`'s `conductor::job` tests) asserts the caller's escalating
/// fallback is what actually runs in that case. Alpaca not sending the
/// header is the expected case, not an edge case this code merely tolerates.
pub fn parse_retry_after(header_value: &str, now: SystemTime) -> Option<Duration> {
    let trimmed = header_value.trim();

    if let Ok(delay_seconds) = u64::from_str(trimmed) {
        return Some(Duration::from_secs(delay_seconds));
    }

    let parsed_date = DateTime::parse_from_rfc2822(trimmed).ok()?;
    let parsed_system_time: SystemTime = parsed_date.with_timezone(&Utc).into();

    parsed_system_time.duration_since(now).ok()
}

/// Reads and parses the `Retry-After` header from a response's headers, if
/// present, using [`parse_retry_after`].
///
/// Returns `None` for a missing header, a header value that is not valid
/// ASCII/visible-text, or a value the parser cannot interpret.
///
/// `pub` (re-exported from the crate root) so every Alpaca client
/// (broker, wallet, market-data, and `st0x-tokenization`'s client) captures
/// the header the same way instead of duplicating this
/// `get -> to_str -> parse_retry_after` chain per call site.
pub fn retry_after_from_response_headers(headers: &HeaderMap) -> Option<Duration> {
    let header_value = headers.get(RETRY_AFTER)?.to_str().ok()?;

    parse_retry_after(header_value, SystemTime::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_retry_after_reads_delay_seconds() {
        let now = SystemTime::now();

        assert_eq!(
            parse_retry_after("120", now),
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn parse_retry_after_reads_zero_delay_seconds() {
        let now = SystemTime::now();

        assert_eq!(parse_retry_after("0", now), Some(Duration::ZERO));
    }

    #[test]
    fn parse_retry_after_reads_http_date() {
        // "Sun, 06 Nov 1994 08:49:37 GMT" is exactly unix second 784111777.
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(784_111_777 - 60);

        assert_eq!(
            parse_retry_after("Sun, 06 Nov 1994 08:49:37 GMT", now),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn parse_retry_after_trims_surrounding_whitespace() {
        let now = SystemTime::now();

        assert_eq!(
            parse_retry_after("  45  ", now),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn parse_retry_after_returns_none_for_a_past_http_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(784_111_777 + 60);

        assert_eq!(
            parse_retry_after("Sun, 06 Nov 1994 08:49:37 GMT", now),
            None
        );
    }

    #[test]
    fn parse_retry_after_returns_none_for_garbage_input() {
        let now = SystemTime::now();

        assert_eq!(parse_retry_after("not-a-retry-after-value", now), None);
    }

    #[test]
    fn parse_retry_after_returns_none_for_empty_string() {
        let now = SystemTime::now();

        assert_eq!(parse_retry_after("", now), None);
    }

    #[test]
    fn retry_after_from_response_headers_reads_a_present_header() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "30".parse().unwrap());

        assert_eq!(
            retry_after_from_response_headers(&headers),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn retry_after_from_response_headers_returns_none_when_absent() {
        let headers = HeaderMap::new();

        assert_eq!(retry_after_from_response_headers(&headers), None);
    }

    #[test]
    fn retry_after_from_response_headers_returns_none_for_unparseable_value() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "not-a-value".parse().unwrap());

        assert_eq!(retry_after_from_response_headers(&headers), None);
    }
}
