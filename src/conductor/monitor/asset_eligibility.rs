//! Supervised overnight asset-eligibility sync.
//!
//! [`AssetEligibilityMonitor`] is a long-running `SupervisedTask` that
//! refreshes every configured equity's Alpaca asset attributes into the
//! shared [`EligibilitySnapshots`] store: once at startup (authorizing a
//! mid-session start, per the spec's window rule) and then daily at the
//! 19:55 ET slot five minutes before the overnight session opens.
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
    AlpacaBrokerApi, EligibilitySnapshots, Symbol, next_eligibility_sync_at, sync_eligibility,
};

use crate::alerts::Notifier;

#[derive(Clone)]
pub(crate) struct AssetEligibilityMonitor {
    pub(crate) broker: Arc<AlpacaBrokerApi>,
    pub(crate) symbols: Vec<Symbol>,
    pub(crate) store: EligibilitySnapshots,
    pub(crate) notifier: Arc<dyn Notifier>,
}

impl SupervisedTask for AssetEligibilityMonitor {
    async fn run(&mut self) -> TaskResult {
        info!(
            symbols = self.symbols.len(),
            "Asset eligibility monitor started"
        );

        // Startup sync: a bot starting mid-session gets an in-window
        // snapshot immediately instead of deferring until 19:55.
        self.sync_and_alert().await;

        loop {
            let now = Utc::now();
            let next = next_eligibility_sync_at(now);
            // `next` is strictly in the future; the zero clamp only
            // guards the instant between computing and converting.
            let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
            info!(%next, "Next asset eligibility sync scheduled");
            tokio::time::sleep(wait).await;

            self.sync_and_alert().await;
        }
    }
}

impl AssetEligibilityMonitor {
    async fn sync_and_alert(&self) {
        match sync_eligibility(&self.broker, &self.symbols, &self.store).await {
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

    async fn mock_broker(server: &MockServer) -> Arc<AlpacaBrokerApi> {
        let ctx = AlpacaBrokerApiCtx {
            auth: st0x_execution::AlpacaBrokerAuth::Basic {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
            },
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };
        Arc::new(ctx.try_into_executor().await.unwrap())
    }

    fn monitor(
        broker: Arc<AlpacaBrokerApi>,
        notifier: Arc<RecordingNotifier>,
    ) -> (AssetEligibilityMonitor, EligibilitySnapshots) {
        let store = EligibilitySnapshots::default();
        (
            AssetEligibilityMonitor {
                broker,
                symbols: vec![Symbol::new("AAPL").unwrap()],
                store: store.clone(),
                notifier,
            },
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
        let (task, store) = monitor(mock_broker(&server).await, notifier.clone());

        task.sync_and_alert().await;

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
        let (task, store) = monitor(mock_broker(&server).await, notifier.clone());

        task.sync_and_alert().await;

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
