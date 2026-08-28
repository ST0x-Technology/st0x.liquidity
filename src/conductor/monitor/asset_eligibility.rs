//! Supervised overnight asset-eligibility sync.
//!
//! [`AssetEligibilityMonitor`] is a long-running `SupervisedTask` that
//! refreshes every configured equity's Alpaca asset attributes into the
//! shared [`EligibilitySnapshots`] store: once at startup (authorizing a
//! mid-session start, per the spec's window rule) and then daily at the
//! 19:55 ET slot five minutes before the overnight session opens.
//!
//! Daily on purpose, a superset of the spec's sync days (Sunday through
//! Thursday plus holiday eves): a Friday or Saturday sync refreshes a
//! store nothing reads, which costs a few requests but spares the
//! scheduler a trading-calendar dependency. The session-scoped staleness
//! check in `validate_overnight_eligibility` is what actually gates
//! placements.
//!
//! A failed sync alerts through the [`Notifier`] path instead of
//! silently serving stale eligibility: the session-scoped staleness
//! check in `validate_overnight_eligibility` already fails closed, so
//! the alert is the operator's signal that overnight placements will
//! defer until a sync succeeds.

use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskResult};
use tracing::{error, info};

use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, EligibilitySnapshots, Symbol, TryIntoExecutor,
    next_eligibility_sync_at, sync_eligibility,
};

use crate::alerts::Notifier;

#[derive(Clone)]
pub(crate) struct AssetEligibilityMonitor {
    pub(crate) broker_ctx: AlpacaBrokerApiCtx,
    pub(crate) symbols: Vec<Symbol>,
    pub(crate) store: EligibilitySnapshots,
    pub(crate) notifier: Arc<dyn Notifier>,
}

impl SupervisedTask for AssetEligibilityMonitor {
    async fn run(&mut self) -> TaskResult {
        // Built here rather than injected: construction verifies the
        // account over HTTP, so a failure lands in the supervisor's
        // restart-with-backoff path instead of aborting conductor
        // startup.
        let broker = self.broker_ctx.clone().try_into_executor().await?;

        info!(
            symbols = self.symbols.len(),
            "Asset eligibility monitor started"
        );

        // Startup sync: a bot starting mid-session gets an in-window
        // snapshot immediately instead of deferring until 19:55.
        self.sync_and_alert(&broker).await;

        loop {
            let now = Utc::now();
            let next = next_eligibility_sync_at(now);
            // `next` is strictly in the future; the zero clamp only
            // guards the instant between computing and converting.
            let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
            info!(%next, "Next asset eligibility sync scheduled");
            tokio::time::sleep(wait).await;

            self.sync_and_alert(&broker).await;
        }
    }
}

impl AssetEligibilityMonitor {
    async fn sync_and_alert(&self, broker: &AlpacaBrokerApi) {
        match sync_eligibility(broker, &self.symbols, &self.store).await {
            Ok(()) => info!(
                symbols = self.symbols.len(),
                "Asset eligibility sync completed"
            ),
            Err(sync_error) => {
                error!(
                    ?sync_error,
                    "Asset eligibility sync failed; overnight placements fail closed \
                     until the next successful sync"
                );
                let message = format!("Overnight asset-eligibility sync failed: {sync_error}");
                if let Err(notify_error) = self.notifier.notify(&message).await {
                    error!(
                        ?notify_error,
                        "Failed to deliver the eligibility sync alert"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use httpmock::MockServer;
    use serde_json::json;
    use std::sync::Mutex;
    use uuid::uuid;

    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, TimeInForce, TryIntoExecutor,
    };

    use crate::alerts::NotifierError;

    use super::*;

    /// Captures every alert message so tests assert delivery and content.
    #[derive(Default)]
    struct RecordingNotifier {
        messages: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Notifier for RecordingNotifier {
        async fn notify(&self, message: &str) -> Result<(), NotifierError> {
            self.messages.lock().unwrap().push(message.to_string());
            Ok(())
        }
    }

    fn mock_account(server: &MockServer) {
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE"
                }));
        });
    }

    fn mock_broker_ctx(server: &MockServer) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            auth: st0x_execution::AlpacaBrokerAuth::Basic {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
            },
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    async fn monitor_and_broker(
        server: &MockServer,
        notifier: Arc<RecordingNotifier>,
    ) -> (
        AssetEligibilityMonitor,
        AlpacaBrokerApi,
        EligibilitySnapshots,
    ) {
        let broker_ctx = mock_broker_ctx(server);
        let broker = broker_ctx.clone().try_into_executor().await.unwrap();
        let store = EligibilitySnapshots::default();
        (
            AssetEligibilityMonitor {
                broker_ctx,
                symbols: vec![Symbol::new("AAPL").unwrap()],
                store: store.clone(),
                notifier,
            },
            broker,
            store,
        )
    }

    #[tokio::test]
    async fn successful_sync_records_snapshots_and_sends_no_alert() {
        let server = MockServer::start_async().await;
        mock_account(&server);
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/assets/AAPL");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "status": "active",
                    "tradable": true,
                    "attributes": ["overnight_tradable"]
                }));
        });
        let notifier = Arc::new(RecordingNotifier::default());
        let (task, broker, store) = monitor_and_broker(&server, notifier.clone()).await;

        task.sync_and_alert(&broker).await;

        assert_eq!(
            store
                .get(&Symbol::new("AAPL").unwrap())
                .unwrap()
                .details
                .overnight_tradable,
            Some(true)
        );
        assert_eq!(*notifier.messages.lock().unwrap(), Vec::<String>::new());
    }

    #[tokio::test]
    async fn failed_sync_alerts_through_the_notifier() {
        let server = MockServer::start_async().await;
        mock_account(&server);
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/v1/assets/AAPL");
            then.status(500).body("broker exploded");
        });
        let notifier = Arc::new(RecordingNotifier::default());
        let (task, broker, store) = monitor_and_broker(&server, notifier.clone()).await;

        task.sync_and_alert(&broker).await;

        assert_eq!(store.get(&Symbol::new("AAPL").unwrap()), None);
        assert_eq!(
            *notifier.messages.lock().unwrap(),
            vec![
                "Overnight asset-eligibility sync failed: eligibility sync failed for 1 symbol(s)"
                    .to_string()
            ]
        );
    }
}
