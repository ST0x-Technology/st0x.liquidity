//! Operational alerting: out-of-band notifications for conditions an operator
//! must react to (low native-gas balance, stuck rebalancing transfers,
//! dead-lettered hedges, supervised-worker terminal failures).
//!
//! The [`Notifier`] trait abstracts the delivery channel; [`LogNotifier`] is
//! the only production implementation: it emits each alert as a structured
//! ERROR log with target `operational_alert`. Delivery to humans happens
//! downstream, in the log pipeline (Cloud Logging -> Grafana alert rules,
//! matching on the target string in the gcplogs stream), so the bot itself
//! holds no delivery credentials and delivery cannot fail in-process.
//!
//! Monitors that raise alerts (see `crate::conductor::monitor::gas`) depend on
//! the trait so they stay testable against a capturing mock.

use async_trait::async_trait;
use tracing::error;

/// Sends an operational alert over some channel.
///
/// Kept as a trait so monitors depend on the capability, not the concrete
/// log transport, which keeps them unit-testable with a capturing mock.
#[async_trait]
pub(crate) trait Notifier: Send + Sync {
    async fn notify(&self, message: &str) -> Result<(), NotifierError>;
}

/// Error type of [`Notifier::notify`].
///
/// Uninhabited outside test builds: the production [`LogNotifier`] emits a
/// log line and cannot fail, so `notify` is infallible in practice. The
/// `Result` stays in the trait so alert-failure handling at the call sites
/// (retry/backoff paths, bounded-timeout sends) remains exercisable in tests.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NotifierError {
    /// Simulated delivery failure, constructible only from tests, for
    /// exercising the call sites' alert-failure handling.
    #[cfg(test)]
    #[error("simulated notifier delivery failure")]
    Simulated,
}

/// A [`Notifier`] that emits each alert as a structured ERROR log.
///
/// The target string `operational_alert` is the delivery contract: the
/// downstream metric filter matches it as a substring of the gcplogs
/// stream. The `alert = true` field is a secondary marker for structured
/// queries, and the human-readable alert text is the event message (the
/// `message` field in JSON log output).
pub(crate) struct LogNotifier;

/// Separator that replaces newlines in an alert message. Chosen because the
/// downstream `kind` label extractor captures `[A-Za-z ]+` after the target,
/// so a non-alphabetic separator terminates the class exactly where the first
/// line used to end.
const LINE_SEPARATOR: &str = " · ";

/// Flatten an alert message onto a single line.
///
/// Delivery runs through Docker's `gcplogs` driver, which emits **one Cloud
/// Logging entry per line**. A multi-line alert therefore arrived as one entry
/// carrying the headline and several orphan entries carrying the detail, none
/// of them matching the `operational_alert` target. The logs-based metric and
/// the Grafana rule built on it only ever saw the headline, so a page read
/// "Portfolio snapshot mark missing" with no symbol, no balance and no repair
/// command — the operator had to go back to the raw logs to learn anything.
///
/// Keeping the message on one line keeps the whole alert in the entry that the
/// pipeline actually reads. Trailing whitespace on a line is trimmed so the
/// separator does not double up, and empty lines are dropped.
fn flatten(message: &str) -> String {
    message
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(LINE_SEPARATOR)
}

#[async_trait]
impl Notifier for LogNotifier {
    async fn notify(&self, message: &str) -> Result<(), NotifierError> {
        let message = flatten(message);
        error!(target: "operational_alert", alert = true, "{message}");
        Ok(())
    }
}

#[cfg(test)]
pub(crate) use test_support::CapturingNotifier;

/// Test-only notifier helpers. Lives in a `#[cfg(test)]` module (rather than
/// bare `#[cfg(test)]` items) so clippy's `allow-unwrap-in-tests` applies to the
/// `Mutex`-lock unwraps below, matching the crate's `test_utils` pattern.
#[cfg(test)]
mod test_support {
    use async_trait::async_trait;

    use super::{Notifier, NotifierError};

    /// A [`Notifier`] that captures every message passed to `notify()`, for tests
    /// that assert operator alerts fire at the right moments without asserting
    /// on log output. Shared across the crate's test modules.
    #[derive(Default)]
    pub(crate) struct CapturingNotifier {
        captured: std::sync::Mutex<Vec<String>>,
    }

    impl CapturingNotifier {
        pub(crate) fn messages(&self) -> Vec<String> {
            self.captured.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Notifier for CapturingNotifier {
        async fn notify(&self, message: &str) -> Result<(), NotifierError> {
            self.captured.lock().unwrap().push(message.to_string());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The target string is the delivery contract (the downstream metric
    /// filter substring-matches `operational_alert` on the gcplogs stream);
    /// the `alert = true` marker and the message text ride along for
    /// structured queries. All three must survive refactors verbatim.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn notify_emits_a_structured_operational_alert_event() {
        LogNotifier.notify("gas balance low on base").await.unwrap();

        assert!(
            logs_contain("operational_alert"),
            "the alert must be emitted under the operational_alert target"
        );
        assert!(
            logs_contain("alert=true"),
            "the alert marker field must be present for log-based routing"
        );
        assert!(
            logs_contain("gas balance low on base"),
            "the alert text must be the event message"
        );
    }

    /// Docker's gcplogs driver emits one Cloud Logging entry per line, so an
    /// embedded newline used to split an alert into a headline entry plus
    /// orphan detail entries that carried no `operational_alert` target. The
    /// page then named the alert class and nothing else. The whole message
    /// must ride in one entry.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn notify_keeps_a_multi_line_alert_in_one_event() {
        LogNotifier
            .notify(
                "🚨 Portfolio snapshot mark missing\nET day: 2026-09-11\nSymbol: FTF\n\nRepair: st0x-cli portfolio-snapshot set --day 2026-09-11 --symbol FTF",
            )
            .await
            .unwrap();

        assert!(
            logs_contain(
                "Portfolio snapshot mark missing · ET day: 2026-09-11 · Symbol: FTF · Repair: st0x-cli portfolio-snapshot set --day 2026-09-11 --symbol FTF"
            ),
            "every line of the alert must survive on one line, blank lines dropped"
        );
    }

    /// The `kind` label extractor downstream captures `[A-Za-z ]+` after the
    /// target, so the separator has to be non-alphabetic or the class would
    /// swallow the detail that follows the headline.
    #[test]
    fn flatten_separates_lines_with_a_non_alphabetic_marker() {
        assert_eq!(flatten("headline\ndetail"), "headline · detail");
        assert!(
            !LINE_SEPARATOR.chars().any(char::is_alphabetic),
            "an alphabetic separator would extend the extracted alert kind"
        );
    }

    /// A single-line alert, which is most of them, must be untouched.
    #[test]
    fn flatten_leaves_a_single_line_alert_alone() {
        assert_eq!(
            flatten("gas balance low on base"),
            "gas balance low on base"
        );
    }
}
