//! Operational alerting: out-of-band notifications for conditions an operator
//! must react to (currently, a low native-gas balance on the bot wallet).
//!
//! The [`Notifier`] trait abstracts the delivery channel; [`TelegramNotifier`]
//! is the only implementation today. Monitors that raise alerts (see
//! `crate::conductor::monitor::gas`) depend on the trait so they stay testable
//! against a capturing mock.

pub(crate) mod telegram;

pub(crate) use telegram::{Notifier, TelegramNotifier};
