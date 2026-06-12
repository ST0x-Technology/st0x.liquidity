//! Tower layer recording latency/outcome for every RPC transport call.
//!
//! Stacked onto the alloy `ClientBuilder`, so it observes every JSON-RPC
//! request the process makes -- monitor polls, contract reads, transaction
//! submission -- regardless of which provider handle issued it. Transport
//! errors (node down, timeout) are recorded; JSON-RPC error payloads inside
//! a successful HTTP exchange are not, since those are method-level
//! semantics rather than dependency health.

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use alloy::transports::TransportError;
use chrono::Utc;
use tower::{Layer, Service};

use super::{Dependency, DependencyCallSample, TelemetrySender, scrub_secrets};

/// Layer wrapping the RPC transport in a [`RpcTelemetryService`].
#[derive(Debug, Clone)]
pub(crate) struct RpcTelemetryLayer {
    telemetry: TelemetrySender,
}

impl RpcTelemetryLayer {
    pub(crate) fn new(telemetry: TelemetrySender) -> Self {
        Self { telemetry }
    }
}

impl<Inner> Layer<Inner> for RpcTelemetryLayer {
    type Service = RpcTelemetryService<Inner>;

    fn layer(&self, inner: Inner) -> Self::Service {
        RpcTelemetryService {
            inner,
            telemetry: self.telemetry.clone(),
        }
    }
}

/// Transport middleware timing each request packet.
#[derive(Debug, Clone)]
pub(crate) struct RpcTelemetryService<Inner> {
    inner: Inner,
    telemetry: TelemetrySender,
}

impl<Inner> Service<RequestPacket> for RpcTelemetryService<Inner>
where
    Inner: Service<RequestPacket, Response = ResponsePacket, Error = TransportError>,
    Inner::Future: Send + 'static,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future =
        Pin<Box<dyn Future<Output = Result<ResponsePacket, TransportError>> + Send + 'static>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        let operation = operation_label(&request);
        let telemetry = self.telemetry.clone();
        let started = Instant::now();
        let call = self.inner.call(request);

        Box::pin(async move {
            let result = call.await;
            telemetry.record(DependencyCallSample {
                recorded_at: Utc::now(),
                dependency: Dependency::Rpc,
                operation,
                duration: started.elapsed(),
                // Transport errors embed the request URL, which carries the
                // provider API key -- scrub before it reaches the database.
                error: result
                    .as_ref()
                    .err()
                    .map(|error| scrub_secrets(&error.to_string())),
            });
            result
        })
    }
}

/// The JSON-RPC method for single requests; batches are labeled as one
/// `batch` operation rather than exploded per method.
fn operation_label(request: &RequestPacket) -> Cow<'static, str> {
    match request {
        RequestPacket::Single(single) => single.method_clone(),
        RequestPacket::Batch(_) => Cow::Borrowed("batch"),
    }
}

#[cfg(test)]
mod tests {
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::rpc::client::ClientBuilder;
    use httpmock::MockServer;

    use crate::telemetry::spawn_dependency_call_writer;
    use crate::test_utils::setup_test_db;

    use super::*;

    #[tokio::test]
    async fn layered_provider_records_method_latency_and_outcome() {
        let server = MockServer::start_async().await;
        let success = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": "0x10",
                }));
            })
            .await;

        let pool = setup_test_db().await;
        let (sender, receiver) = TelemetrySender::channel();
        let writer = spawn_dependency_call_writer(pool.clone(), receiver);

        let client = ClientBuilder::default()
            .layer(RpcTelemetryLayer::new(sender.clone()))
            .http(server.url("/").parse().unwrap());
        let provider = ProviderBuilder::new().connect_client(client);

        let block = provider.get_block_number().await.unwrap();
        assert_eq!(block, 16);
        success.assert_async().await;

        drop(provider);
        drop(sender);
        writer.await.unwrap();

        let rows: Vec<(String, String, String)> =
            sqlx::query_as("SELECT dependency, operation, outcome FROM dependency_call_samples")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "rpc");
        assert_eq!(rows[0].1, "eth_blockNumber");
        assert_eq!(rows[0].2, "ok");
    }

    #[tokio::test]
    async fn transport_failure_is_recorded_as_error() {
        let pool = setup_test_db().await;
        let (sender, receiver) = TelemetrySender::channel();
        let writer = spawn_dependency_call_writer(pool.clone(), receiver);

        // Nothing listens on this port: the transport call must fail.
        let client = ClientBuilder::default()
            .layer(RpcTelemetryLayer::new(sender.clone()))
            .http("http://127.0.0.1:1".parse().unwrap());
        let provider = ProviderBuilder::new().connect_client(client);

        provider.get_block_number().await.unwrap_err();

        drop(provider);
        drop(sender);
        writer.await.unwrap();

        let (operation, outcome, error): (String, String, Option<String>) =
            sqlx::query_as("SELECT operation, outcome, error FROM dependency_call_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(operation, "eth_blockNumber");
        assert_eq!(outcome, "error");
        assert!(
            !error.unwrap().is_empty(),
            "transport error message must be recorded"
        );
    }
}
