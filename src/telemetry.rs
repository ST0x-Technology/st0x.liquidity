//! HyperDX observability integration for trace export.
//!
//! This module provides optional OpenTelemetry trace export to HyperDX for real-time
//! monitoring and debugging of bot operations. When enabled via the `HYPERDX_API_KEY`
//! environment variable, traces are batched and exported to HyperDX. When disabled,
//! the bot runs normally with console-only logging.
//!
//! # Architecture
//!
//! Uses OpenTelemetry's [`BatchSpanProcessor`] for efficient trace export:
//! - **Batch size**: 512 spans per batch
//! - **Queue size**: 2048 spans maximum
//! - **Export interval**: 3 seconds
//! - **Protocol**: gRPC via OTLP
//!
//! ## Blocking HTTP Client Requirement
//!
//! **CRITICAL**: The [`BatchSpanProcessor`] spawns background threads that run outside
//! the tokio runtime. These threads require a blocking HTTP client (from `reqwest::blocking`)
//! rather than an async client. Using an async client would panic with "no reactor running"
//! because the background threads have no tokio runtime available.
//!
//! The blocking client is created in a separate thread to avoid blocking the main tokio
//! runtime during initialization.
//!
//! # Usage
//!
//! ```ignore
//! // Optional telemetry setup through config
//! let config = env::Env::parse().into_config();
//!
//! let telemetry_guard = if let Some(ref hyperdx) = config.hyperdx {
//!     match hyperdx.setup_telemetry() {
//!         Ok(guard) => Some(guard),
//!         Err(e) => {
//!             eprintln!("Failed to setup telemetry: {e}");
//!             None
//!         }
//!     }
//! } else {
//!     None
//! };
//!
//! // Bot runs normally regardless of telemetry availability
//! // ...
//!
//! // Guard flushes remaining traces on drop
//! drop(telemetry_guard);
//! ```
//!
//! # Trace Filtering
//!
//! The module sets up per-layer filtering to control what gets logged to console vs
//! exported to HyperDX:
//! - Console layer: Respects `RUST_LOG` environment variable, defaults to bot crates only
//! - Telemetry layer: Independent filter, defaults to bot crates only
//!
//! This prevents external crate spam (e.g., from `alloy`, `rocket`) from cluttering
//! traces while still allowing those logs in console if needed.

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::{Layer, SubscriberExt};

#[derive(Debug, Clone)]
pub struct HyperDxConfig {
    pub(crate) api_key: String,
    pub(crate) service_name: String,
    pub(crate) log_level: tracing::Level,
}

impl HyperDxConfig {
    pub fn setup_telemetry(&self) -> Result<TelemetryGuard, TelemetryError> {
        let headers = HashMap::from([("authorization".to_string(), self.api_key.clone())]);

        let http_client = std::thread::spawn(|| {
            reqwest::blocking::Client::builder()
                .gzip(true)
                .build()
                .map_err(|e| format!("Failed to build HTTP client: {e}"))
        })
        .join()
        .map_err(|_| TelemetryError::ThreadSpawn)?
        .map_err(TelemetryError::HttpClient)?;

        let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_http_client(http_client)
            .with_endpoint("https://in-otel.hyperdx.io/v1/traces")
            .with_headers(headers)
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .build()?;

        let batch_exporter = BatchSpanProcessor::builder(otlp_exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_export_batch_size(512)
                    .with_max_queue_size(2048)
                    .with_scheduled_delay(Duration::from_secs(3))
                    .build(),
            )
            .build();

        let tracer_provider = SdkTracerProvider::builder()
            .with_span_processor(batch_exporter)
            .with_resource(
                Resource::builder()
                    .with_service_name(self.service_name.clone())
                    .with_attributes(vec![KeyValue::new("deployment.environment", "production")])
                    .build(),
            )
            .build();

        let tracer = tracer_provider.tracer(TRACER_NAME);

        let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        let default_filter = format!(
            "st0x_hedge={},st0x_broker={}",
            self.log_level, self.log_level
        );

        let fmt_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| default_filter.clone().into());

        let telemetry_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| default_filter.into());

        let fmt_layer = tracing_subscriber::fmt::layer().with_filter(fmt_filter);
        let telemetry_layer = telemetry_layer.with_filter(telemetry_filter);

        let subscriber = Registry::default().with(fmt_layer).with(telemetry_layer);

        tracing::subscriber::set_global_default(subscriber)?;

        Ok(TelemetryGuard { tracer_provider })
    }
}

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("Failed to build OTLP exporter")]
    OtlpExporter(#[from] ExporterBuildError),

    #[error("Failed to build HTTP client")]
    HttpClient(String),

    #[error("Failed to spawn HTTP client thread")]
    ThreadSpawn,

    #[error("Failed to set global subscriber")]
    Subscriber(#[from] tracing::subscriber::SetGlobalDefaultError),
}

pub struct TelemetryGuard {
    tracer_provider: SdkTracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Flush any pending spans to HyperDX before shutdown.
        // This blocks until all pending exports complete or timeout.
        // The BatchSpanProcessor uses scheduled_delay (3s) as its export interval,
        // and force_flush waits for pending exports with its own internal timeout.
        if let Err(e) = self.tracer_provider.force_flush() {
            eprintln!("Failed to flush telemetry spans: {e:?}");
        }

        // Shutdown the tracer provider to clean up background threads and resources.
        // This ensures the BatchSpanProcessor's background thread terminates cleanly.
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("Failed to shutdown telemetry provider: {e:?}");
        }
    }
}

/// Instrumentation library name used to identify the source of traces in the
/// OpenTelemetry system. This appears in telemetry backends as the library
/// that generated the spans.
///
/// This is distinct from the service name:
/// - Service name (e.g., "st0x-hedge"): Identifies which service the traces
///   come from in a distributed system. Shows as `service.name` resource
///   attribute.
/// - Tracer name (this constant): Identifies which instrumentation library
///   within the service created the spans. Used to distinguish between
///   application code ("st0x_tracer") and auto-instrumented libraries
///   (e.g., "reqwest", "sqlx").
///
/// Since we use a single tracer for all application code without library
/// auto-instrumentation, this distinction is somewhat artificial but
/// maintained for semantic clarity.
const TRACER_NAME: &str = "st0x-tracer";
