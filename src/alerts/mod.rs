//! Operational alerting: out-of-band notifications for conditions an operator
//! must react to (low native-gas balance, stuck rebalancing transfers,
//! dead-lettered hedges, supervised-worker terminal failures).
//!
//! The [`Notifier`] trait abstracts the delivery channel; [`LogNotifier`] is
//! the only production implementation: it emits each alert as a structured
//! ERROR log with target `operational_alert`. Delivery to humans happens
//! downstream, in the log pipeline (Cloud Logging -> Grafana alert rules), so
//! the bot itself holds no delivery credentials and delivery cannot fail
//! in-process.
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
/// The `operational_alert` target and `alert = true` field are the contract
/// the downstream log-based alert rules match on; the human-readable alert
/// text is the event message (the `message` field in JSON log output).
pub(crate) struct LogNotifier;

#[async_trait]
impl Notifier for LogNotifier {
    async fn notify(&self, message: &str) -> Result<(), NotifierError> {
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

    /// The structured shape is the delivery contract: downstream alert rules
    /// match the `operational_alert` target and the `alert = true` marker and
    /// read the message text, so all three must survive refactors verbatim.
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
}
