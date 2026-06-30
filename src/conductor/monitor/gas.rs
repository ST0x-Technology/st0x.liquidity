//! Supervised native-gas balance monitor.
//!
//! [`GasMonitor`] is a long-running [`SupervisedTask`] that polls the bot
//! wallet's native-ETH balance on a fixed interval and raises an alert when it
//! drops below the configured threshold. Alerts go to two places: a structured
//! `warn!`/`error!` log (target `"gas"`) and a [`Notifier`] (Telegram in
//! production).
//!
//! ## Alert de-duplication
//!
//! Polling is frequent, but operators must not be paged on every tick while a
//! balance stays low. The monitor therefore tracks an [`AlertState`]:
//!
//! - It alerts once on the transition into the low state.
//! - While low, it re-alerts at most once per `realert_interval`.
//! - It emits a single recovery notice on the transition back above threshold.
//!
//! ## Failure handling
//!
//! Balance reads go through the [`BalanceReader`] trait. A read failure is a
//! transient RPC blip: it is logged and swallowed so a hiccup never halts the
//! monitor (mirroring `InventoryMonitor`). The supervisor restarts the task
//! only if the tick loop itself panics.

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::utils::format_ether;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use async_trait::async_trait;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::alerts::Notifier;

/// Reads the native balance of an address, abstracted so the monitor's
/// threshold logic can be driven with canned values in tests instead of a
/// live RPC provider.
#[async_trait]
pub(crate) trait BalanceReader: Send + Sync {
    async fn native_balance(&self, address: Address) -> Result<U256, BalanceReadError>;
}

/// Opaque wrapper around the underlying provider's balance-read error. Kept as
/// a boxed error (rather than a `String`) so the source chain is preserved.
#[derive(Debug, thiserror::Error)]
#[error("failed to read native balance")]
pub(crate) struct BalanceReadError(#[source] Box<dyn std::error::Error + Send + Sync>);

/// [`BalanceReader`] backed by an alloy [`Provider`] (the Base/orderbook-chain
/// provider the conductor already owns).
pub(crate) struct ProviderBalanceReader<Prov> {
    provider: Prov,
}

impl<Prov> ProviderBalanceReader<Prov> {
    pub(crate) fn new(provider: Prov) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<Prov: Provider + Send + Sync> BalanceReader for ProviderBalanceReader<Prov> {
    async fn native_balance(&self, address: Address) -> Result<U256, BalanceReadError> {
        self.provider
            .get_balance(address)
            .await
            .map_err(|error| BalanceReadError(Box::new(error)))
    }
}

/// Tracks whether the balance is currently in the low-alert state, and when the
/// last alert fired, so repeated low polls don't spam notifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AlertState {
    /// Balance is at or above threshold (or never observed low).
    Normal,
    /// Balance is below threshold; carries the time the last alert was sent.
    Low { last_alerted: Instant },
}

/// Outcome of a single balance evaluation. Separated from I/O so the dedup
/// state machine is unit-testable without real time or RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    /// Balance is healthy and was already healthy: stay quiet.
    StillHealthy,
    /// Balance crossed below threshold (or first low observation): alert.
    DroppedBelow,
    /// Balance remains below threshold but the re-alert interval has not
    /// elapsed: suppress.
    StillLowSuppressed,
    /// Balance remains below threshold and the re-alert interval elapsed:
    /// alert again.
    StillLowRealert,
    /// Balance recovered to at or above threshold: send recovery notice.
    Recovered,
}

/// Pure de-dup transition. Given the current state, the latest balance, the
/// threshold, the re-alert interval and "now", returns the next state and the
/// outcome the caller should act on. No I/O, no logging — those happen in the
/// caller so this stays trivially testable.
fn evaluate(
    state: AlertState,
    balance: U256,
    threshold: U256,
    realert_interval: Duration,
    now: Instant,
) -> (AlertState, PollOutcome) {
    let is_low = balance < threshold;

    match (state, is_low) {
        (AlertState::Normal, false) => (AlertState::Normal, PollOutcome::StillHealthy),
        (AlertState::Normal, true) => (
            AlertState::Low { last_alerted: now },
            PollOutcome::DroppedBelow,
        ),
        (AlertState::Low { .. }, false) => (AlertState::Normal, PollOutcome::Recovered),
        (AlertState::Low { last_alerted }, true) => {
            if now.duration_since(last_alerted) >= realert_interval {
                (
                    AlertState::Low { last_alerted: now },
                    PollOutcome::StillLowRealert,
                )
            } else {
                (
                    AlertState::Low { last_alerted },
                    PollOutcome::StillLowSuppressed,
                )
            }
        }
    }
}

/// Long-running supervised task that polls a wallet's native balance and raises
/// low-gas alerts. Cheaply cloneable: all heavy state lives behind `Arc`.
#[derive(Clone)]
pub(crate) struct GasMonitor {
    pub(crate) balance_reader: Arc<dyn BalanceReader>,
    pub(crate) notifier: Arc<dyn Notifier>,
    pub(crate) wallet: Address,
    /// Human-readable chain label included in logs and alert text.
    pub(crate) chain: &'static str,
    pub(crate) threshold_wei: U256,
    pub(crate) poll_interval: Duration,
    pub(crate) realert_interval: Duration,
}

impl GasMonitor {
    /// Evaluates one poll: reads the balance, advances the dedup state, and
    /// fires logs/notifications for the resulting outcome. Returns the next
    /// state. A read failure is logged and leaves the state unchanged.
    async fn poll_once(&self, state: AlertState, now: Instant) -> AlertState {
        let balance = match self.balance_reader.native_balance(self.wallet).await {
            Ok(balance) => balance,
            Err(error) => {
                warn!(
                    target: "gas",
                    ?error,
                    wallet = %self.wallet,
                    chain = self.chain,
                    "Failed to read native balance; will retry next tick"
                );
                return state;
            }
        };

        let (next_state, outcome) = evaluate(
            state,
            balance,
            self.threshold_wei,
            self.realert_interval,
            now,
        );

        self.act_on_outcome(outcome, balance).await;

        next_state
    }

    /// Emits the log line and notification appropriate for `outcome`. Quiet
    /// outcomes (`StillHealthy`, `StillLowSuppressed`) produce no output.
    async fn act_on_outcome(&self, outcome: PollOutcome, balance: U256) {
        let balance_eth = format_ether(balance);
        let threshold_eth = format_ether(self.threshold_wei);

        match outcome {
            PollOutcome::StillHealthy | PollOutcome::StillLowSuppressed => {}
            PollOutcome::DroppedBelow | PollOutcome::StillLowRealert => {
                error!(
                    target: "gas",
                    wallet = %self.wallet,
                    chain = self.chain,
                    balance_eth = %balance_eth,
                    threshold_eth = %threshold_eth,
                    "Wallet native-gas balance is below threshold"
                );

                let message = format!(
                    "\u{26a0}\u{fe0f} Low gas: wallet {} on {} has {} ETH (threshold {} ETH)",
                    self.wallet, self.chain, balance_eth, threshold_eth
                );
                self.send(&message).await;
            }
            PollOutcome::Recovered => {
                info!(
                    target: "gas",
                    wallet = %self.wallet,
                    chain = self.chain,
                    balance_eth = %balance_eth,
                    threshold_eth = %threshold_eth,
                    "Wallet native-gas balance recovered above threshold"
                );

                let message = format!(
                    "\u{2705} Gas recovered: wallet {} on {} has {} ETH (threshold {} ETH)",
                    self.wallet, self.chain, balance_eth, threshold_eth
                );
                self.send(&message).await;
            }
        }
    }

    /// Sends a notification, logging (but not propagating) a delivery failure
    /// so a flaky notifier never crashes the supervised task.
    async fn send(&self, message: &str) {
        if let Err(error) = self.notifier.notify(message).await {
            warn!(
                target: "gas",
                ?error,
                "Failed to deliver gas alert notification"
            );
        }
    }
}

impl SupervisedTask for GasMonitor {
    async fn run(&mut self) -> TaskResult {
        info!(
            target: "gas",
            wallet = %self.wallet,
            chain = self.chain,
            threshold_eth = %format_ether(self.threshold_wei),
            "Gas monitor started"
        );

        let mut interval = tokio::time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut state = AlertState::Normal;

        loop {
            interval.tick().await;
            state = self.poll_once(state, Instant::now()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Notifier that records every message it is asked to deliver, so tests can
    /// assert exactly which alerts fired.
    struct CapturingNotifier {
        messages: Mutex<Vec<String>>,
    }

    impl CapturingNotifier {
        fn new() -> Self {
            Self {
                messages: Mutex::new(Vec::new()),
            }
        }

        fn messages(&self) -> Vec<String> {
            self.messages.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Notifier for CapturingNotifier {
        async fn notify(&self, message: &str) -> Result<(), crate::alerts::NotifierError> {
            self.messages.lock().unwrap().push(message.to_owned());
            Ok(())
        }
    }

    /// Balance reader returning a fixed value, or an error when `fail` is set.
    struct StaticBalanceReader {
        balance: U256,
        fail: bool,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("forced balance read failure")]
    struct ForcedFailure;

    #[async_trait]
    impl BalanceReader for StaticBalanceReader {
        async fn native_balance(&self, _address: Address) -> Result<U256, BalanceReadError> {
            if self.fail {
                Err(BalanceReadError(Box::new(ForcedFailure)))
            } else {
                Ok(self.balance)
            }
        }
    }

    fn monitor_with(balance: U256, fail: bool, notifier: Arc<CapturingNotifier>) -> GasMonitor {
        GasMonitor {
            balance_reader: Arc::new(StaticBalanceReader { balance, fail }),
            notifier,
            wallet: Address::ZERO,
            chain: "base",
            threshold_wei: U256::from(100u64),
            poll_interval: Duration::from_secs(60),
            realert_interval: Duration::from_secs(3600),
        }
    }

    #[tokio::test]
    async fn below_threshold_alerts_and_logs_drop() {
        let notifier = Arc::new(CapturingNotifier::new());
        let monitor = monitor_with(U256::from(50u64), false, notifier.clone());

        let now = Instant::now();
        let next = monitor.poll_once(AlertState::Normal, now).await;

        assert_eq!(next, AlertState::Low { last_alerted: now });
        let messages = notifier.messages();
        assert_eq!(messages.len(), 1, "drop below threshold must alert once");
        assert!(
            messages[0].contains("Low gas"),
            "alert text should mark a low-gas condition, got: {}",
            messages[0]
        );
    }

    #[tokio::test]
    async fn above_threshold_stays_quiet() {
        let notifier = Arc::new(CapturingNotifier::new());
        let monitor = monitor_with(U256::from(150u64), false, notifier.clone());

        let next = monitor.poll_once(AlertState::Normal, Instant::now()).await;

        assert_eq!(next, AlertState::Normal);
        assert!(
            notifier.messages().is_empty(),
            "a healthy balance must not notify"
        );
    }

    #[tokio::test]
    async fn second_low_poll_within_realert_interval_is_suppressed() {
        let notifier = Arc::new(CapturingNotifier::new());
        let monitor = monitor_with(U256::from(50u64), false, notifier.clone());

        let start = Instant::now();
        let state = monitor.poll_once(AlertState::Normal, start).await;

        // Re-poll only 10 minutes later: well inside the 1h re-alert window.
        let later = start + Duration::from_secs(600);
        let next = monitor.poll_once(state, later).await;

        assert_eq!(
            next, state,
            "a suppressed re-alert must not advance last_alerted"
        );
        assert_eq!(
            notifier.messages().len(),
            1,
            "a low poll inside the re-alert window must not re-notify"
        );
    }

    #[tokio::test]
    async fn low_poll_after_realert_interval_realerts() {
        let notifier = Arc::new(CapturingNotifier::new());
        let monitor = monitor_with(U256::from(50u64), false, notifier.clone());

        let start = Instant::now();
        let state = monitor.poll_once(AlertState::Normal, start).await;

        // Re-poll just past the 1h re-alert window.
        let later = start + Duration::from_secs(3601);
        monitor.poll_once(state, later).await;

        assert_eq!(
            notifier.messages().len(),
            2,
            "a low poll past the re-alert window must re-notify"
        );
    }

    #[tokio::test]
    async fn recovery_from_low_notifies_once() {
        let low_notifier = Arc::new(CapturingNotifier::new());
        let low_monitor = monitor_with(U256::from(50u64), false, low_notifier.clone());
        let low_state = low_monitor
            .poll_once(AlertState::Normal, Instant::now())
            .await;

        // Same dedup state, but now the balance is healthy again.
        let recover_notifier = Arc::new(CapturingNotifier::new());
        let recover_monitor = monitor_with(U256::from(150u64), false, recover_notifier.clone());
        let next = recover_monitor.poll_once(low_state, Instant::now()).await;

        assert_eq!(next, AlertState::Normal, "recovery returns to Normal");
        let messages = recover_notifier.messages();
        assert_eq!(messages.len(), 1, "recovery must notify exactly once");
        assert!(
            messages[0].contains("recovered"),
            "recovery text should mention recovery, got: {}",
            messages[0]
        );
    }

    #[tokio::test]
    async fn read_failure_preserves_state_and_does_not_notify() {
        let notifier = Arc::new(CapturingNotifier::new());
        let monitor = monitor_with(U256::from(50u64), true, notifier.clone());

        let state = AlertState::Low {
            last_alerted: Instant::now(),
        };
        let next = monitor.poll_once(state, Instant::now()).await;

        assert_eq!(
            next, state,
            "a read failure must leave dedup state unchanged"
        );
        assert!(
            notifier.messages().is_empty(),
            "a read failure must not produce an alert"
        );
    }
}
