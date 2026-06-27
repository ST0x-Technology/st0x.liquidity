//! Latency/error capture decorator over Alpaca's conversion-only broker methods.
//!
//! The rebalancer calls three methods (`convert_usdc_usd`, `find_conversion_order`,
//! `poll_conversion_to_terminal`) that are specific to Alpaca and not on the generic
//! `Executor` trait, so `InstrumentedExecutor` does not cover them. This decorator
//! wraps an `AlpacaBrokerApi` once at rebalancer startup, in
//! `spawn_rebalancing_infrastructure`, and emits `dependency='broker'` samples for
//! each call. Lives in the main crate: the execution crate stays telemetry-free.
//!
//! # Duration semantics
//!
//! The `duration` field in recorded samples has different meanings depending on
//! the operation:
//!
//! - `find_conversion_order` -- a single HTTP request; duration reflects
//!   point-in-time network latency (milliseconds).
//! - `convert_usdc_usd` and `poll_conversion_to_terminal` -- these methods
//!   poll with 500 ms sleeps until the crypto order reaches a terminal state,
//!   so `duration` captures end-to-end settlement wait time (seconds to
//!   minutes). Dashboard latency queries must treat these two operations
//!   separately from `find_conversion_order` to avoid misleading averages.

use std::time::Instant;

use chrono::Utc;
use rain_math_float::Float;
use uuid::Uuid;

use st0x_execution::alpaca_broker_api::{CryptoOrderOutcome, CryptoOrderResponse};
use st0x_execution::{AlpacaBrokerApi, AlpacaBrokerApiError, ClientOrderId, ConversionDirection};

use super::{Dependency, DependencyCallSample, TelemetrySender, scrub_secrets};

/// An [`AlpacaBrokerApi`] whose USDC conversion calls are timed and recorded.
///
/// Only the three conversion methods the rebalancer uses are exposed and timed;
/// they are the calls that produce `dependency='broker'` gaps in the dashboard.
/// Other `AlpacaBrokerApi` methods are intentionally not surfaced here.
#[derive(Debug, Clone)]
pub(crate) struct InstrumentedAlpacaBroker {
    inner: AlpacaBrokerApi,
    telemetry: TelemetrySender,
}

impl InstrumentedAlpacaBroker {
    pub(crate) fn new(inner: AlpacaBrokerApi, telemetry: TelemetrySender) -> Self {
        Self { inner, telemetry }
    }

    fn record<Value>(
        &self,
        operation: &'static str,
        started: Instant,
        result: &Result<Value, AlpacaBrokerApiError>,
    ) {
        self.telemetry.record(DependencyCallSample {
            recorded_at: Utc::now(),
            dependency: Dependency::Broker,
            operation: operation.into(),
            duration: started.elapsed(),
            error: result
                .as_ref()
                .err()
                .map(|error| scrub_secrets(&error.to_string())),
        });
    }

    pub(crate) async fn convert_usdc_usd(
        &self,
        amount: Float,
        direction: ConversionDirection,
        client_order_id: &ClientOrderId,
    ) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
        let started = Instant::now();
        let result = self
            .inner
            .convert_usdc_usd(amount, direction, client_order_id)
            .await;
        self.record("convert_usdc_usd", started, &result);
        result
    }

    pub(crate) async fn find_conversion_order(
        &self,
        client_order_id: &ClientOrderId,
    ) -> Result<Option<CryptoOrderResponse>, AlpacaBrokerApiError> {
        let started = Instant::now();
        let result = self.inner.find_conversion_order(client_order_id).await;
        self.record("find_conversion_order", started, &result);
        result
    }

    pub(crate) async fn poll_conversion_to_terminal(
        &self,
        order_id: Uuid,
    ) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
        let started = Instant::now();
        let result = self.inner.poll_conversion_to_terminal(order_id).await;

        // The inner method returns `Ok(order)` for BOTH filled and terminally-failed
        // orders (canceled/expired/rejected); classify to surface failures as errors.
        // Its poll loop only returns once the order is terminal, so a returned `Ok` is
        // `Filled` or `Failed` -- `Pending` is not reachable here, but a non-failed
        // order is not a broker error either way, so it shares the `Filled` arm.
        let error = match &result {
            Err(error) => Some(scrub_secrets(&error.to_string())),
            Ok(order) => match order.classify() {
                CryptoOrderOutcome::Filled | CryptoOrderOutcome::Pending => None,
                CryptoOrderOutcome::Failed(reason) => Some(scrub_secrets(&format!(
                    "conversion order terminally failed: {reason}"
                ))),
            },
        };

        self.telemetry.record(DependencyCallSample {
            recorded_at: Utc::now(),
            dependency: Dependency::Broker,
            operation: "poll_conversion_to_terminal".into(),
            duration: started.elapsed(),
            error,
        });

        result
    }
}

#[cfg(test)]
mod tests {
    use httpmock::Method::{GET, POST};
    use httpmock::MockServer;
    use serde_json::json;
    use uuid::{Uuid, uuid};

    use st0x_execution::alpaca_broker_api::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode};
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApi, ClientOrderId, ConversionDirection, Executor,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::telemetry::spawn_dependency_call_writer;
    use crate::test_utils::setup_test_db;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));
    const TEST_ORDER_ID: Uuid = uuid!("61e7b016-9c91-4a97-b912-615c9d365c9d");

    async fn make_broker(server: &MockServer) -> AlpacaBrokerApi {
        let _account_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/account"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": TEST_ACCOUNT_ID.to_string(),
                    "status": "ACTIVE"
                }));
        });

        let auth = AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: st0x_execution::TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        AlpacaBrokerApi::try_from_ctx(auth)
            .await
            .expect("Failed to create test broker API")
    }

    fn filled_order_json(order_id: &str, amount: &str) -> serde_json::Value {
        json!({
            "id": order_id,
            "symbol": "USDCUSD",
            "qty": amount,
            "status": "filled",
            "filled_avg_price": "1.0001",
            "filled_qty": amount,
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    /// Named projection of a `dependency_call_samples` row for readable assertions.
    #[derive(sqlx::FromRow)]
    struct SampleRow {
        dependency: String,
        operation: String,
        outcome: String,
        error: Option<String>,
    }

    async fn drain_samples(
        pool: &sqlx::SqlitePool,
        instrumented: InstrumentedAlpacaBroker,
        sender: TelemetrySender,
        receiver: tokio::sync::mpsc::Receiver<DependencyCallSample>,
    ) -> Vec<SampleRow> {
        let writer = spawn_dependency_call_writer(pool.clone(), receiver);
        // Both sender clones must drop for the writer's channel to close: the
        // decorator holds one internally, the test holds the other.
        drop(instrumented);
        drop(sender);
        writer.await.unwrap();

        sqlx::query_as(
            "SELECT dependency, operation, outcome, error \
             FROM dependency_call_samples ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn records_convert_usdc_usd_as_broker_operation() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        let _place_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(filled_order_json(&TEST_ORDER_ID.to_string(), "100"));
        });

        let _get_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders/{TEST_ORDER_ID}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(filled_order_json(&TEST_ORDER_ID.to_string(), "100"));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());
        let client_order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        instrumented
            .convert_usdc_usd(
                float!(100),
                ConversionDirection::UsdcToUsd,
                &client_order_id,
            )
            .await
            .unwrap();

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "convert_usdc_usd");
        assert_eq!(rows[0].outcome, "ok");
        assert_eq!(rows[0].error, None);
    }

    #[tokio::test]
    async fn records_convert_usd_to_usdc_as_broker_operation() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        let _place_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(filled_order_json(&TEST_ORDER_ID.to_string(), "100"));
        });

        let _get_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders/{TEST_ORDER_ID}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(filled_order_json(&TEST_ORDER_ID.to_string(), "100"));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());
        let client_order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        instrumented
            .convert_usdc_usd(
                float!(100),
                ConversionDirection::UsdToUsdc,
                &client_order_id,
            )
            .await
            .unwrap();

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "convert_usdc_usd");
        assert_eq!(rows[0].outcome, "ok");
        assert_eq!(rows[0].error, None);
    }

    #[tokio::test]
    async fn records_find_conversion_order_as_broker_operation() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        // 404 -> find_conversion_order returns Ok(None)
        let _get_mock = server.mock(|when, then| {
            when.method(GET).path_includes("/orders:by_client_order_id");
            then.status(404)
                .header("content-type", "application/json")
                .json_body(json!({"message": "order not found"}));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());
        let client_order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let result = instrumented
            .find_conversion_order(&client_order_id)
            .await
            .unwrap();
        let None = result else {
            panic!("expected find_conversion_order to return None, got {result:?}")
        };

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "find_conversion_order");
        assert_eq!(rows[0].outcome, "ok");
        assert_eq!(rows[0].error, None);
    }

    #[tokio::test]
    async fn records_poll_conversion_to_terminal_as_broker_operation() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        let _get_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders/{TEST_ORDER_ID}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(filled_order_json(&TEST_ORDER_ID.to_string(), "50"));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());

        instrumented
            .poll_conversion_to_terminal(TEST_ORDER_ID)
            .await
            .unwrap();

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "poll_conversion_to_terminal");
        assert_eq!(rows[0].outcome, "ok");
        assert_eq!(rows[0].error, None);
    }

    #[tokio::test]
    async fn records_convert_usdc_usd_error_as_broker_operation() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        // POST places the order and returns an id; GET polls for status and
        // returns "canceled", which poll_crypto_order_until_filled maps to
        // CryptoOrderFailed immediately without sleeping.
        let _place_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": TEST_ORDER_ID.to_string(),
                    "symbol": "USDCUSD",
                    "qty": "100",
                    "status": "canceled",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let _poll_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders/{TEST_ORDER_ID}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": TEST_ORDER_ID.to_string(),
                    "symbol": "USDCUSD",
                    "qty": "100",
                    "status": "canceled",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());
        let client_order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        instrumented
            .convert_usdc_usd(
                float!(100),
                ConversionDirection::UsdcToUsd,
                &client_order_id,
            )
            .await
            .unwrap_err();

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "convert_usdc_usd");
        assert_eq!(rows[0].outcome, "error");
        assert_eq!(
            rows[0].error.as_deref(),
            Some("Crypto order 61e7b016-9c91-4a97-b912-615c9d365c9d failed: Canceled")
        );
    }

    #[tokio::test]
    async fn records_poll_conversion_to_terminal_error_as_broker_operation() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        // HTTP 500 produces a transport-level error in the poll GET.
        let _poll_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders/{TEST_ORDER_ID}"
            ));
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({"message": "internal server error"}));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());

        instrumented
            .poll_conversion_to_terminal(TEST_ORDER_ID)
            .await
            .unwrap_err();

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "poll_conversion_to_terminal");
        assert_eq!(rows[0].outcome, "error");
        assert_eq!(
            rows[0].error.as_deref(),
            Some("Alpaca API error (500 Internal Server Error): internal server error")
        );
    }

    #[tokio::test]
    async fn records_broker_api_error_with_scrubbed_message() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        // Return 401 with a message that contains a URL-bearing "secret" -- scrub_secrets
        // must strip the path component before the error is persisted.
        let _get_mock = server.mock(|when, then| {
            when.method(GET).path_includes("/orders:by_client_order_id");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "https://broker-api.alpaca.markets/v2/SECRET_KEY: unauthorized"
                }));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());
        let client_order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        instrumented
            .find_conversion_order(&client_order_id)
            .await
            .unwrap_err();

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "find_conversion_order");
        assert_eq!(rows[0].outcome, "error");
        let error_msg = rows[0].error.as_deref().unwrap_or("");
        // scrub_secrets strips the path after the host, replacing it with /<redacted>.
        // The colon after SECRET_KEY and the trailing word are preserved as separate tokens.
        assert_eq!(
            error_msg,
            "Alpaca API error (401 Unauthorized): \
             https://broker-api.alpaca.markets/<redacted> unauthorized",
            "scrubbed error must redact the URL path and preserve the host"
        );
        assert!(
            !error_msg.contains("SECRET_KEY"),
            "error message must not contain the secret key; got: {error_msg}"
        );
    }

    #[tokio::test]
    async fn records_find_conversion_order_error_as_broker_operation() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        let _get_mock = server.mock(|when, then| {
            when.method(GET).path_includes("/orders:by_client_order_id");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({"message": "internal server error"}));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());
        let client_order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        instrumented
            .find_conversion_order(&client_order_id)
            .await
            .unwrap_err();

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "find_conversion_order");
        assert_eq!(rows[0].outcome, "error");
        assert_eq!(
            rows[0].error.as_deref(),
            Some("Alpaca API error (500 Internal Server Error): internal server error")
        );
    }

    #[tokio::test]
    async fn records_poll_conversion_to_terminal_terminal_failure_records_error() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let broker = make_broker(&server).await;

        // Canceled order: inner returns Ok(order) but classify() => Failed(Canceled).
        let _get_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders/{TEST_ORDER_ID}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": TEST_ORDER_ID.to_string(),
                    "symbol": "USDCUSD",
                    "qty": "100",
                    "status": "canceled",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let (sender, receiver) = TelemetrySender::channel();
        let instrumented = InstrumentedAlpacaBroker::new(broker, sender.clone());

        // Returns Ok even for terminal failures -- the caller classifies.
        instrumented
            .poll_conversion_to_terminal(TEST_ORDER_ID)
            .await
            .unwrap();

        let rows = drain_samples(&pool, instrumented, sender, receiver).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dependency, "broker");
        assert_eq!(rows[0].operation, "poll_conversion_to_terminal");
        assert_eq!(rows[0].outcome, "error");
        assert_eq!(
            rows[0].error.as_deref(),
            Some("conversion order terminally failed: Canceled")
        );
    }
}
