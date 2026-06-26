//! OpenTelemetry observability integration for the self-hosted st0x stack.
//!
//! Exports traces to VictoriaTraces and logs to VictoriaLogs using OTLP/HTTP.
//! When `[telemetry]` is absent from config, the bot runs with console-only
//! logging. No external SaaS dependency or API key required.
//!
//! # Architecture
//!
//! Uses OpenTelemetry's [`BatchSpanProcessor`] and [`BatchLogProcessor`] for
//! efficient export:
//! - **Batch size**: 512 spans/records per batch
//! - **Queue size**: 2048 maximum
//! - **Export interval**: 3 seconds
//! - **Protocol**: OTLP/HTTP binary
//!
//! ## Blocking HTTP Client Requirement
//!
//! **CRITICAL**: Both batch processors spawn background threads that run
//! outside the tokio runtime. These threads require a blocking HTTP client
//! (from `reqwest::blocking`) rather than an async client. Using an async
//! client would panic with "no reactor running" because the background threads
//! have no tokio runtime available.
//!
//! The blocking client is created in a separate thread to avoid blocking the
//! main tokio runtime during initialization.
//!
//! # Usage
//!
//! ```ignore
//! let ctx = Ctx::load_files(&config_path, &secrets_path)?;
//! let log_level: tracing::Level = (&ctx.log_level).into();
//!
//! let telemetry_guard = if let Some(ref telemetry) = ctx.telemetry {
//!     match telemetry.setup(log_level, ctx.log_dir.as_deref()) {
//!         Ok((_file_guard, guard)) => Some(guard),
//!         Err(e) => {
//!             eprintln!("Failed to setup telemetry: {e}");
//!             None
//!         }
//!     }
//! } else {
//!     None
//! };
//! // Guard flushes remaining traces and logs on drop
//! drop(telemetry_guard);
//! ```

use itertools::Itertools;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{BatchLogProcessor, SdkLoggerProvider};
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};
use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::{EnvFilter, Registry};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TelemetryConfig {
    pub(crate) service_name: String,
    pub(crate) traces_endpoint: String,
    pub(crate) logs_endpoint: String,
}

#[derive(Clone)]
pub struct TelemetryCtx {
    pub(crate) service_name: String,
    pub(crate) traces_endpoint: String,
    pub(crate) logs_endpoint: String,
}

impl std::fmt::Debug for TelemetryCtx {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelemetryCtx")
            .field("service_name", &self.service_name)
            .field("traces_endpoint", &self.traces_endpoint)
            .field("logs_endpoint", &self.logs_endpoint)
            .finish()
    }
}

impl TelemetryCtx {
    pub(crate) fn new(config: TelemetryConfig) -> Self {
        Self {
            service_name: config.service_name,
            traces_endpoint: config.traces_endpoint,
            logs_endpoint: config.logs_endpoint,
        }
    }

    pub fn setup(
        &self,
        log_level: tracing::Level,
        log_dir: Option<&str>,
    ) -> Result<(Option<FileLogGuard>, TelemetryGuard), TelemetryError> {
        let http_client =
            std::thread::spawn(|| reqwest::blocking::Client::builder().gzip(true).build())
                .join()??;

        let resource = Resource::builder()
            .with_service_name(self.service_name.clone())
            .with_attributes(vec![KeyValue::new("deployment.environment", "production")])
            .build();

        let tracer_provider = {
            let span_exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_http_client(http_client.clone())
                .with_endpoint(format!(
                    "{}/opentelemetry/v1/traces",
                    self.traces_endpoint
                ))
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .build()?;

            let batch_processor = BatchSpanProcessor::builder(span_exporter)
                .with_batch_config(
                    BatchConfigBuilder::default()
                        .with_max_export_batch_size(512)
                        .with_max_queue_size(2048)
                        .with_scheduled_delay(Duration::from_secs(3))
                        .build(),
                )
                .build();

            SdkTracerProvider::builder()
                .with_span_processor(batch_processor)
                .with_resource(resource.clone())
                .build()
        };

        let logger_provider = {
            let log_exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .with_http_client(http_client)
                .with_endpoint(format!(
                    "{}/insert/opentelemetry/v1/logs",
                    self.logs_endpoint
                ))
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .build()?;

            let batch_processor = BatchLogProcessor::builder(log_exporter).build();

            SdkLoggerProvider::builder()
                .with_log_processor(batch_processor)
                .with_resource(resource)
                .build()
        };

        let tracer = tracer_provider.tracer(TRACER_NAME);

        let telemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_level(true);

        let otel_log_layer = OpenTelemetryTracingBridge::new(&logger_provider);

        let fmt_layer = tracing_subscriber::fmt::layer().with_filter(mk_env_filter(log_level));
        let telemetry_layer = telemetry_layer.with_filter(mk_telemetry_filter(log_level));
        let otel_log_layer = otel_log_layer.with_filter(mk_telemetry_filter(log_level));

        let file_guard = if let Some(dir) = log_dir {
            let file_appender = tracing_appender::rolling::daily(dir, "st0x-hedge.log");
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

            let file_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_writer(non_blocking)
                .with_filter(mk_crate_filter(log_level));

            let subscriber = Registry::default()
                .with(fmt_layer)
                .with(telemetry_layer)
                .with(otel_log_layer)
                .with(file_layer);

            tracing::subscriber::set_global_default(subscriber)?;

            Some(FileLogGuard { _guard: guard })
        } else {
            let subscriber = Registry::default()
                .with(fmt_layer)
                .with(telemetry_layer)
                .with(otel_log_layer);

            tracing::subscriber::set_global_default(subscriber)?;

            None
        };

        Ok((
            file_guard,
            TelemetryGuard {
                tracer_provider,
                logger_provider,
            },
        ))
    }
}

pub struct TelemetryGuard {
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Err(error) = self.tracer_provider.force_flush() {
            eprintln!("Failed to flush telemetry spans: {error:?}");
        }

        if let Err(error) = self.tracer_provider.shutdown() {
            eprintln!("Failed to shutdown trace provider: {error:?}");
        }

        if let Err(errors) = self.logger_provider.force_flush() {
            eprintln!("Failed to flush log records: {errors:?}");
        }

        if let Err(errors) = self.logger_provider.shutdown() {
            eprintln!("Failed to shutdown log provider: {errors:?}");
        }
    }
}

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("HTTP client builder thread panicked")]
    ThreadJoin,
    #[error("Failed to build HTTP client")]
    HttpClient(#[from] reqwest::Error),
    #[error("Failed to build OTLP exporter")]
    OtlpExporter(#[from] ExporterBuildError),
    #[error("Failed to set global subscriber")]
    Subscriber(#[from] tracing::subscriber::SetGlobalDefaultError),
}

impl From<Box<dyn std::any::Any + Send>> for TelemetryError {
    fn from(_: Box<dyn std::any::Any + Send>) -> Self {
        Self::ThreadJoin
    }
}

/// Instrumentation library name used to identify the source of spans in the
/// OpenTelemetry system. Distinguishes application code from auto-instrumented
/// libraries (e.g. reqwest, sqlx).
const TRACER_NAME: &str = "st0x-tracer";

/// Guard returned when file logging is enabled. Dropping flushes buffered writes.
pub struct FileLogGuard {
    _guard: tracing_appender::non_blocking::WorkerGuard,
}

pub fn setup_tracing(
    log_level: &crate::config::LogLevel,
    log_dir: Option<&str>,
) -> Option<FileLogGuard> {
    let level: tracing::Level = log_level.into();
    let env_filter = mk_env_filter(level);

    if let Some(dir) = log_dir {
        let file_appender = tracing_appender::rolling::daily(dir, "st0x-hedge.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let file_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(non_blocking)
            .with_filter(mk_crate_filter(level));

        let fmt_layer = tracing_subscriber::fmt::layer()
            .compact()
            .with_filter(env_filter);

        let subscriber = Registry::default().with(fmt_layer).with(file_layer);

        if tracing::subscriber::set_global_default(subscriber).is_err() {
            eprintln!("Failed to set global subscriber (already set)");
            return None;
        }

        Some(FileLogGuard { _guard: guard })
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .init();

        None
    }
}

pub fn mk_env_filter(level: tracing::Level) -> EnvFilter {
    let fallback_filter = mk_crate_filter(level);

    EnvFilter::try_from_default_env().unwrap_or(fallback_filter)
}

fn mk_telemetry_filter(level: tracing::Level) -> EnvFilter {
    mk_crate_filter(level)
}

fn mk_crate_filter(level: tracing::Level) -> EnvFilter {
    // TODO: parse from the manifest or something
    const CRATES: [&str; 9] = [
        "hedge",
        "bridge",
        "dto",
        "event-sorcery",
        "evm",
        "execution",
        "finance",
        "float-macro",
        "float-serde",
    ];

    /// Domain-based log targets used via `target: "..."` in tracing macros.
    /// These must be listed here so the `EnvFilter` captures them alongside
    /// module-path-based crate targets.
    const DOMAIN_TARGETS: [&str; 11] = [
        "bridge",
        "broker",
        "cqrs",
        "dashboard",
        "hedge",
        "inventory",
        "orderbook",
        "rebalance",
        "startup",
        "tokenization",
        "wallet",
    ];

    let our_crates = CRATES
        .iter()
        .map(|pkg| pkg.replace('-', "_"))
        .map(|pkg| format!("st0x_{pkg}={level}"))
        .join(",");

    let domain_targets = DOMAIN_TARGETS
        .iter()
        .map(|target| format!("{target}={level}"))
        .join(",");

    EnvFilter::from(format!("warn,{our_crates},{domain_targets}"))
}
