//! HyperDX observability integration for trace export.
//!
//! This module provides optional OpenTelemetry trace export to HyperDX for real-time
//! monitoring and debugging of bot operations. When enabled via the `[hyperdx]` section
//! in the secrets TOML file, traces are batched and exported to HyperDX. When disabled,
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
//! // Optional telemetry setup through ctx
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

use itertools::Itertools;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tracing_appender::rolling::{InitError, RollingFileAppender, Rotation};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::{EnvFilter, Registry};

use crate::LogLevel;

/// Number of daily log files the rolling appender retains. Older files are
/// pruned automatically as new ones roll, bounding on-disk log growth so a
/// long-running deployment cannot fill the disk (root cause of the 2026-06-08
/// SQLITE_FULL incident).
const LOG_RETENTION_DAYS: usize = 14;

/// Build the daily-rolling file appender used by every file-logging path.
///
/// Unlike `tracing_appender::rolling::daily`, the builder form bounds history
/// via [`LOG_RETENTION_DAYS`] and surfaces directory/file creation failures as
/// [`InitError`] rather than panicking.
fn build_log_file_appender(dir: &str) -> Result<RollingFileAppender, InitError> {
    RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("st0x-hedge.log")
        .max_log_files(LOG_RETENTION_DAYS)
        .build(dir)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    pub service_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetrySecrets {
    pub api_key: String,
}

#[derive(Clone)]
pub struct TelemetryCtx {
    pub api_key: String,
    pub service_name: String,
}

impl std::fmt::Debug for TelemetryCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryCtx")
            .field("api_key", &"[REDACTED]")
            .field("service_name", &self.service_name)
            .finish()
    }
}

impl TelemetryCtx {
    pub fn new(
        config: Option<TelemetryConfig>,
        secrets: Option<TelemetrySecrets>,
    ) -> Result<Option<Self>, TelemetryAssemblyError> {
        match (config, secrets) {
            (Some(config), Some(secrets)) => Ok(Some(Self {
                api_key: secrets.api_key,
                service_name: config.service_name,
            })),
            (None, None) => Ok(None),
            (Some(_), None) => Err(TelemetryAssemblyError::SecretsMissing),
            (None, Some(_)) => Err(TelemetryAssemblyError::ConfigMissing),
        }
    }

    pub fn setup(
        &self,
        log_level: tracing::Level,
        log_dir: Option<&str>,
        extra_layer: Option<ExtraLayer>,
    ) -> Result<(Option<FileLogGuard>, TelemetryGuard), TelemetryError> {
        let headers = HashMap::from([("authorization".to_string(), self.api_key.clone())]);

        let http_client =
            std::thread::spawn(|| reqwest::blocking::Client::builder().gzip(true).build())
                .join()??;

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

        let telemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_level(true);

        let fmt_layer = tracing_subscriber::fmt::layer().with_filter(mk_env_filter(log_level));
        let telemetry_layer = telemetry_layer.with_filter(mk_telemetry_filter(log_level));

        let file_appender = log_dir.and_then(|dir| match build_log_file_appender(dir) {
            Ok(appender) => Some(appender),
            Err(error) => {
                // A misconfigured log directory must not take HyperDX export
                // down with it: keep the OTLP layer alive and continue without
                // file logging rather than failing the whole telemetry setup.
                eprintln!("Failed to build rolling file appender, continuing without file logging: {error}");
                None
            }
        });

        let file_guard = if let Some(file_appender) = file_appender {
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

            let file_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_writer(non_blocking)
                .with_filter(mk_crate_filter(log_level));

            let subscriber = Registry::default()
                .with(extra_layer)
                .with(fmt_layer)
                .with(telemetry_layer)
                .with(file_layer);

            tracing::subscriber::set_global_default(subscriber)?;

            Some(FileLogGuard { _guard: guard })
        } else {
            let subscriber = Registry::default()
                .with(extra_layer)
                .with(fmt_layer)
                .with(telemetry_layer);

            tracing::subscriber::set_global_default(subscriber)?;

            None
        };

        Ok((file_guard, TelemetryGuard { tracer_provider }))
    }
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
        if let Err(error) = self.tracer_provider.force_flush() {
            eprintln!("Failed to flush telemetry spans: {error:?}");
        }

        // Shutdown the tracer provider to clean up background threads and resources.
        // This ensures the BatchSpanProcessor's background thread terminates cleanly.
        if let Err(error) = self.tracer_provider.shutdown() {
            eprintln!("Failed to shutdown telemetry provider: {error:?}");
        }
    }
}

#[derive(Debug, Error)]
pub enum TelemetryAssemblyError {
    #[error("telemetry config present but telemetry secrets missing")]
    SecretsMissing,
    #[error("telemetry secrets present but telemetry config missing")]
    ConfigMissing,
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

/// Guard returned when file logging is enabled. Dropping flushes buffered writes.
pub struct FileLogGuard {
    _guard: tracing_appender::non_blocking::WorkerGuard,
}

/// Boxed Layer trait object used to plug in caller-owned extra layers
/// (e.g. an apalis-board SSE broadcaster) without making `setup_tracing`
/// generic over them.
pub type ExtraLayer =
    Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static>;

pub fn setup_tracing(
    log_level: &LogLevel,
    log_dir: Option<&str>,
    extra_layer: Option<ExtraLayer>,
) -> Option<FileLogGuard> {
    let level: tracing::Level = log_level.into();
    let env_filter = mk_env_filter(level);

    let Some(dir) = log_dir else {
        install_console_only_subscriber(extra_layer, env_filter);
        return None;
    };

    let file_appender = match build_log_file_appender(dir) {
        Ok(appender) => appender,
        Err(error) => {
            // A misconfigured log directory must not silently disable all
            // logging: degrade to console-only so the operator still sees
            // output (and this error) instead of a silent process.
            eprintln!("Failed to build rolling file appender, using console only: {error}");
            install_console_only_subscriber(extra_layer, env_filter);
            return None;
        }
    };

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(non_blocking)
        .with_filter(mk_crate_filter(level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(env_filter);

    let subscriber = Registry::default()
        .with(extra_layer)
        .with(fmt_layer)
        .with(file_layer);

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        eprintln!("Failed to set global subscriber (already set)");
        return None;
    }

    Some(FileLogGuard { _guard: guard })
}

/// Install a console-only tracing subscriber as the global default.
///
/// Used both when no log directory is configured and as a fallback when the
/// rolling file appender fails to build, so a missing/unwritable log directory
/// degrades to console logging rather than disabling logging entirely.
fn install_console_only_subscriber(extra_layer: Option<ExtraLayer>, env_filter: EnvFilter) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(env_filter);

    let subscriber = Registry::default().with(extra_layer).with(fmt_layer);

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        eprintln!("Failed to set global subscriber (already set)");
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
    const CRATES: [&str; 10] = [
        "config",
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

#[cfg(test)]
mod tests {
    use std::io::Write;
    use tempfile::{NamedTempFile, tempdir};

    use super::*;

    #[test]
    fn build_log_file_appender_writes_to_prefixed_file() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        let mut appender = build_log_file_appender(dir_path).unwrap();
        appender.write_all(b"retention test line\n").unwrap();
        appender.flush().unwrap();

        let log_files: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("st0x-hedge.log"))
            .collect();

        assert_eq!(
            log_files.len(),
            1,
            "expected exactly one log file with the configured prefix, found: {log_files:?}"
        );

        let contents = std::fs::read_to_string(dir.path().join(&log_files[0])).unwrap();
        assert!(
            contents.contains("retention test line"),
            "log file should contain the written line, got: {contents:?}"
        );
    }

    #[test]
    fn build_log_file_appender_surfaces_directory_creation_error() {
        // A regular file cannot contain a subdirectory, so directory creation
        // fails: the fallible builder must surface the error rather than panic.
        let file = NamedTempFile::new().unwrap();
        let uncreatable_dir = file.path().join("nested");
        let dir_path = uncreatable_dir.to_str().unwrap();

        let Err(_) = build_log_file_appender(dir_path) else {
            panic!("expected appender build to fail when the log directory cannot be created");
        };
    }

    #[test]
    fn setup_tracing_degrades_to_console_only_when_log_dir_is_invalid() {
        let file = NamedTempFile::new().unwrap();
        let uncreatable_dir = file.path().join("nested");

        let file_guard = setup_tracing(&LogLevel::Info, uncreatable_dir.to_str(), None);

        assert!(
            file_guard.is_none(),
            "an invalid log dir must degrade to console-only logging, not return a file guard"
        );
    }

    #[test]
    fn telemetry_setup_continues_without_file_logging_when_log_dir_is_invalid() {
        let file = NamedTempFile::new().unwrap();
        let uncreatable_dir = file.path().join("nested");

        let ctx = TelemetryCtx {
            api_key: "test-key".to_string(),
            service_name: "test-service".to_string(),
        };

        let (file_guard, _telemetry_guard) = ctx
            .setup(tracing::Level::INFO, uncreatable_dir.to_str(), None)
            .unwrap();

        assert!(
            file_guard.is_none(),
            "an invalid log dir must not produce a file guard nor fail telemetry setup"
        );
    }

    #[test]
    fn build_log_file_appender_prunes_files_beyond_retention_limit() {
        let dir = tempdir().unwrap();

        // Seed more dated log files than the retention window. tracing-appender
        // prunes at construction time, so building the appender must delete the
        // oldest files down to the LOG_RETENTION_DAYS bound. Days are kept in a
        // valid 01..=N range so the filename date parses on platforms where the
        // pruner falls back to parsing the date from the filename.
        let seeded = LOG_RETENTION_DAYS + 6;
        for day in 1..=seeded {
            let name = format!("st0x-hedge.log.2026-05-{day:02}");
            std::fs::File::create(dir.path().join(name)).unwrap();
        }

        build_log_file_appender(dir.path().to_str().unwrap()).unwrap();

        let remaining = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("st0x-hedge.log")
            })
            .count();

        // The pruner deletes seeded files down to max_files - 1 and the
        // appender then creates the current day's file (dated after the seeded
        // names, so it never collides), leaving exactly the retention bound.
        assert_eq!(
            remaining, LOG_RETENTION_DAYS,
            "retention should leave exactly {LOG_RETENTION_DAYS} files \
             (max_files - 1 pruned survivors + today's file), found {remaining}"
        );
    }
}
