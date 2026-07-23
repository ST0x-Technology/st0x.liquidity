//! Operational alerting: out-of-band notifications for conditions and completed
//! lifecycle operations an operator needs to see.
//!
//! The [`Notifier`] trait abstracts the delivery channel; [`TelegramNotifier`]
//! is the only implementation today. Monitors that raise alerts (see
//! `crate::conductor::monitor::gas`) depend on the trait so they stay testable
//! against a capturing mock.
//!
//! [`NoopNotifier`] is the explicit absence implementation: used when the
//! `[alerts]` config section is omitted. Its presence in the type system makes
//! the absence of alerting intentional and visible rather than silently skipped
//! via `Option`.

pub(crate) mod telegram;

pub(crate) use telegram::TelegramNotifier;

use async_trait::async_trait;
use reqwest::StatusCode;
use std::sync::Arc;
use tracing::{debug, info};

use st0x_config::AlertsCtx;

/// Sends an operational alert over some channel.
///
/// Kept as a trait so monitors depend on the capability, not the concrete
/// Telegram transport, which keeps them unit-testable with a capturing mock.
#[async_trait]
pub(crate) trait Notifier: Send + Sync {
    async fn notify(&self, message: &str) -> Result<(), NotifierError>;
}

/// Builds the configured operational notification channel.
///
/// An absent `[alerts]` section is represented explicitly by [`NoopNotifier`].
/// If the section is present, construction failures propagate so callers never
/// silently discard notifications an operator configured.
pub(crate) fn build_notifier(
    alerts: Option<&AlertsCtx>,
) -> Result<Arc<dyn Notifier>, NotifierError> {
    let Some(alerts) = alerts else {
        debug!("Operational alerting is not configured; using NoopNotifier");
        return Ok(Arc::new(NoopNotifier));
    };
    let notifier =
        TelegramNotifier::new(&alerts.bot_token, alerts.chat_id, alerts.message_thread_id)?;
    info!("Telegram operational notifier configured");
    Ok(Arc::new(notifier))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NotifierError {
    #[error("failed to build Telegram HTTP client")]
    ClientBuild(#[source] reqwest::Error),
    #[error("Telegram sendMessage request failed")]
    Request(#[source] reqwest::Error),
    #[error("Telegram API reported failed delivery with HTTP status {status}")]
    ApiError { status: StatusCode },
}

/// A [`Notifier`] that discards every message without error.
///
/// Used when the `[alerts]` config section is absent: the caller receives an
/// `Arc<dyn Notifier>` pointing at this type, making the absence explicit
/// (no `Option` branch, no silent skip). The no-op path is visible in the
/// type system and in startup logs.
pub(crate) struct NoopNotifier;

#[async_trait]
impl Notifier for NoopNotifier {
    async fn notify(&self, _message: &str) -> Result<(), NotifierError> {
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
    /// that assert operator alerts fire at the right moments without a real
    /// delivery channel. Shared across the crate's test modules.
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
