//! OpenTelemetry observability integration for the self-hosted st0x stack.
//!
//! Exports traces to VictoriaTraces and logs to VictoriaLogs using OTLP/HTTP.
//! When `[telemetry]` is absent from config, the bot runs with console-only
//! logging. No external SaaS dependency or API key required.
//!
//! ## Blocking HTTP Client Requirement
//!
//! **CRITICAL**: Both batch processors spawn background threads that run outside
//! the tokio runtime and require `reqwest::blocking`. Using an async client panics
//! with "no reactor running". The client is created in a separate thread to avoid
//! blocking the main tokio runtime during initialization.

use itertools::Itertools;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{
    BatchConfigBuilder as LogBatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider,
};
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing_appender::rolling::{InitError, RollingFileAppender, Rotation};
use tracing_subscriber::fmt::format::{JsonFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::{EnvFilter, Registry};
use url::Url;

use crate::{LogFormat, LogLevel};

/// Retain one week of daily log files. Older files are pruned automatically as
/// new ones roll. This caps file count, not bytes; sink-level filtering reduces
/// within-day growth separately.
const LOG_RETENTION_DAYS: usize = 7;

/// Validated configuration for the local rotating log sink.
///
/// Keeping the directory and threshold together prevents runtime callers from
/// enabling one without the other. The stdout/export threshold remains the
/// top-level [`LogLevel`].
#[derive(Clone, Debug)]
pub struct FileLogging {
    directory: String,
    level: LogLevel,
}

impl FileLogging {
    #[must_use]
    pub fn new(directory: String, level: LogLevel) -> Self {
        Self { directory, level }
    }

    #[must_use]
    pub fn directory(&self) -> &str {
        &self.directory
    }

    #[must_use]
    pub fn level(&self) -> &LogLevel {
        &self.level
    }
}

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

/// Console text rendering for [`console_fmt_layer`]. The telemetry-enabled
/// subscriber renders full text; the file and console-only subscribers render
/// compact text. Carried as a type so the deliberate difference is visible at
/// the call sites instead of inferred from duplicated `match` blocks.
#[derive(Clone, Copy)]
enum ConsoleTextStyle {
    Compact,
    Full,
}

/// Build the console fmt layer for `log_format`, writing to `writer`.
///
/// The JSON arm flattens each event: the message and the event fields are
/// top-level keys next to `timestamp`, `level` and `target`, so a log shipper
/// that parses the line finds the text at `message` (Cloud Logging:
/// `jsonPayload.message`) and each field under its own name. Span context
/// stays under `span` and `spans`. The rolling file layer keeps the nested
/// `fields` shape on purpose (see [`file_fmt_layer`]).
fn console_fmt_layer<S, W>(
    log_format: LogFormat,
    env_filter: EnvFilter,
    style: ConsoleTextStyle,
    writer: W,
) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    match log_format {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .fmt_fields(JsonFields::new())
            .event_format(JsonWithOtelLines(
                tracing_subscriber::fmt::format().json().flatten_event(true),
                OtelLineShape::Flat,
            ))
            .with_ansi(false)
            .with_writer(writer)
            .with_filter(env_filter)
            .boxed(),
        LogFormat::Text => match style {
            ConsoleTextStyle::Compact => tracing_subscriber::fmt::layer()
                .compact()
                .with_writer(writer)
                .with_filter(env_filter)
                .boxed(),
            ConsoleTextStyle::Full => tracing_subscriber::fmt::layer()
                .with_writer(writer)
                .with_filter(env_filter)
                .boxed(),
        },
    }
}

/// Wraps either JSON shape, the flattened console or the nested rolling file,
/// and writes OpenTelemetry's internal logs with one `message`.
///
/// Before opentelemetry 0.32, `opentelemetry_sdk` and its sibling crates log
/// through `otel_warn!` and friends, which record a `message` field and then
/// an empty format string, itself recorded as `message`. Flattened, such a
/// line has two `message` keys, and a reader keeps one of them: maybe the
/// empty one. These are the lines that say the OTLP pipeline drops logs or
/// spans, so they are written by [`write_otel_internal_line`] instead, with
/// one `message`. The rolling file uses it too, in its nested shape, because
/// its readers take `fields.message`.
///
/// The duplicate-`message` handling is a workaround for those versions:
/// opentelemetry 0.32 (open-telemetry/opentelemetry-rust#3317) stops adding
/// the empty format string. After that bump, only the `<name>: <error>`
/// fallback for export errors, which record no `message`, is still needed.
struct JsonWithOtelLines<F>(F, OtelLineShape);

/// Where an OpenTelemetry internal line puts its fields: at the top level, as
/// the flattened console does, or under `fields`, as the rolling file does.
#[derive(Clone, Copy)]
enum OtelLineShape {
    Flat,
    Nested,
}

impl<S, N, F> FormatEvent<S, N> for JsonWithOtelLines<F>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        if is_otel_internal(event.metadata().target()) {
            write_otel_internal_line(writer, event, self.1)
        } else {
            self.0.format_event(ctx, writer, event)
        }
    }
}

/// Whether `target` is one of OpenTelemetry's crates, which log under their
/// package name (`opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`,
/// ...).
fn is_otel_internal(target: &str) -> bool {
    target.starts_with("opentelemetry")
}

/// Writes an OpenTelemetry internal log as one JSON line: `timestamp`,
/// `level`, `target` and the event fields, with a single `message`. That is
/// the non-empty one, or for an event with none (an export error records
/// only `name` and `error`) the event's name, then its error. Span context is
/// left out on purpose: these lines are about the SDK, not the code that
/// happened to run.
fn write_otel_internal_line(
    mut writer: Writer<'_>,
    event: &tracing::Event<'_>,
    shape: OtelLineShape,
) -> std::fmt::Result {
    let mut recorded = OtelInternalFields::default();
    event.record(&mut recorded);

    let metadata = event.metadata();
    let mut fields = recorded.0;
    let has_message = fields
        .get("message")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|message| !message.is_empty());
    if !has_message {
        let name = fields
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| metadata.name());
        let message = fields
            .get("error")
            .and_then(serde_json::Value::as_str)
            .map_or_else(|| name.to_string(), |error| format!("{name}: {error}"));
        fields.insert("message".to_string(), serde_json::Value::from(message));
    }

    let mut line = match shape {
        OtelLineShape::Flat => fields,
        OtelLineShape::Nested => {
            serde_json::Map::from_iter([("fields".to_string(), serde_json::Value::Object(fields))])
        }
    };
    line.insert(
        "timestamp".to_string(),
        serde_json::Value::from(
            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        ),
    );
    line.insert(
        "level".to_string(),
        serde_json::Value::from(metadata.level().as_str()),
    );
    line.insert(
        "target".to_string(),
        serde_json::Value::from(metadata.target()),
    );

    writeln!(writer, "{}", serde_json::Value::Object(line))
}

/// The fields of an OpenTelemetry internal log. A second `message` replaces
/// the first only when the first is empty, so the empty format string never
/// hides the real text.
#[derive(Default)]
struct OtelInternalFields(serde_json::Map<String, serde_json::Value>);

impl OtelInternalFields {
    fn insert(&mut self, field: &tracing::field::Field, value: serde_json::Value) {
        let name = field.name();
        let keeps_existing = name == "message"
            && value.as_str() == Some("")
            && self.0.get(name).is_some_and(|existing| existing != "");
        if !keeps_existing {
            self.0.insert(name.to_string(), value);
        }
    }
}

impl tracing::field::Visit for OtelInternalFields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.insert(field, serde_json::Value::from(format!("{value:?}")));
    }
}

/// Build the OTel [`Resource`] shared by the trace and log providers. Carries
/// `service.name` and the `deployment.environment` attribute so signals from
/// different environments are distinguishable in VictoriaTraces/VictoriaLogs.
fn build_resource(service_name: &str, environment: &str) -> Resource {
    Resource::builder()
        .with_service_name(service_name.to_string())
        .with_attributes(vec![KeyValue::new(
            "deployment.environment",
            environment.to_string(),
        )])
        .build()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    pub service_name: String,
    /// Deployment environment (e.g. `production`, `staging`) exported as the
    /// `deployment.environment` resource attribute. Without it, staging and
    /// prod traces/logs are indistinguishable in VictoriaTraces/VictoriaLogs.
    pub environment: String,
    pub traces_endpoint: Url,
    pub logs_endpoint: Url,
}

#[derive(Clone)]
pub struct TelemetryCtx {
    pub service_name: String,
    pub environment: String,
    pub traces_endpoint: Url,
    pub logs_endpoint: Url,
}

impl std::fmt::Debug for TelemetryCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryCtx")
            .field("service_name", &self.service_name)
            .field("environment", &self.environment)
            .field("traces_endpoint", &self.traces_endpoint)
            .field("logs_endpoint", &self.logs_endpoint)
            .finish()
    }
}

impl From<TelemetryConfig> for TelemetryCtx {
    fn from(config: TelemetryConfig) -> Self {
        Self {
            service_name: config.service_name,
            environment: config.environment,
            traces_endpoint: config.traces_endpoint,
            logs_endpoint: config.logs_endpoint,
        }
    }
}

impl TelemetryCtx {
    pub fn setup(
        &self,
        log_level: tracing::Level,
        log_format: LogFormat,
        file_logging: Option<&FileLogging>,
        extra_layer: Option<ExtraLayer>,
        log_sink: Option<Arc<dyn LogEventSink>>,
    ) -> Result<(Option<FileLogGuard>, TelemetryGuard), TelemetryError> {
        let http_client =
            std::thread::spawn(|| reqwest::blocking::Client::builder().gzip(true).build())
                .join()??;

        let resource = build_resource(&self.service_name, &self.environment);

        let tracer_provider = {
            let span_exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_http_client(http_client.clone())
                .with_endpoint(
                    self.traces_endpoint
                        .join("insert/opentelemetry/v1/traces")?
                        .as_str(),
                )
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
                .with_endpoint(
                    self.logs_endpoint
                        .join("insert/opentelemetry/v1/logs")?
                        .as_str(),
                )
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .build()?;

            SdkLoggerProvider::builder()
                .with_log_processor(
                    BatchLogProcessor::builder(log_exporter)
                        .with_batch_config(
                            LogBatchConfigBuilder::default()
                                .with_max_export_batch_size(512)
                                .with_max_queue_size(2048)
                                .with_scheduled_delay(Duration::from_secs(3))
                                .build(),
                        )
                        .build(),
                )
                .with_resource(resource)
                .build()
        };

        let tracer = tracer_provider.tracer(TRACER_NAME);
        let telemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_level(true)
            .with_filter(mk_crate_filter(log_level));

        let otel_log_layer = OpenTelemetryTracingBridge::new(&logger_provider)
            .with_filter(mk_crate_filter(log_level));

        let fmt_layer = console_fmt_layer(
            log_format,
            mk_env_filter(log_level),
            ConsoleTextStyle::Full,
            std::io::stdout,
        );

        let file_appender = file_logging.and_then(|file_logging| {
            match build_log_file_appender(file_logging.directory()) {
                Ok(appender) => Some((appender, file_logging.level().into())),
                Err(error) => {
                    eprintln!("Failed to build rolling file appender, continuing without file logging: {error}");
                    None
                }
            }
        });

        let file_guard = if let Some((file_appender, file_level)) = file_appender {
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            let file_layer = file_fmt_layer(non_blocking, file_level);
            let count_layer = log_sink.map(|sink| log_count_layer(sink, file_level));

            let subscriber = Registry::default()
                .with(extra_layer)
                .with(fmt_layer)
                .with(telemetry_layer)
                .with(otel_log_layer)
                .with(file_layer)
                .with(count_layer);

            tracing::subscriber::set_global_default(subscriber)?;

            Some(FileLogGuard { _guard: guard })
        } else {
            let subscriber = Registry::default()
                .with(extra_layer)
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
            eprintln!("Failed to shutdown tracer provider: {error:?}");
        }
        if let Err(error) = self.logger_provider.force_flush() {
            eprintln!("Failed to flush log records: {error:?}");
        }
        if let Err(error) = self.logger_provider.shutdown() {
            eprintln!("Failed to shutdown logger provider: {error:?}");
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
    #[error("Invalid telemetry endpoint URL")]
    EndpointUrl(#[from] url::ParseError),
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

/// Installs the global subscriber.
///
/// `log_sink` receives every event the file log writes and is attached only when a file layer is built, so it counts
/// exactly what `/logs` and `/performance/reliability` can read back.
pub fn setup_tracing(
    log_level: &LogLevel,
    log_format: LogFormat,
    file_logging: Option<&FileLogging>,
    extra_layer: Option<ExtraLayer>,
    log_sink: Option<Arc<dyn LogEventSink>>,
) -> Option<FileLogGuard> {
    let level: tracing::Level = log_level.into();
    let env_filter = mk_env_filter(level);

    let Some(file_logging) = file_logging else {
        install_console_only_subscriber(log_format, extra_layer, env_filter);
        return None;
    };

    let file_appender = match build_log_file_appender(file_logging.directory()) {
        Ok(appender) => appender,
        Err(error) => {
            // A misconfigured log directory must not silently disable all
            // logging: degrade to console-only so the operator still sees
            // output (and this error) instead of a silent process.
            eprintln!("Failed to build rolling file appender, using console only: {error}");
            install_console_only_subscriber(log_format, extra_layer, env_filter);
            return None;
        }
    };

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let file_level = file_logging.level().into();
    let file_layer = file_fmt_layer(non_blocking, file_level);
    let count_layer = log_sink.map(|sink| log_count_layer(sink, file_level));

    let fmt_layer = console_fmt_layer(
        log_format,
        env_filter,
        ConsoleTextStyle::Compact,
        std::io::stdout,
    );

    let subscriber = Registry::default()
        .with(extra_layer)
        .with(fmt_layer)
        .with(file_layer)
        .with(count_layer);

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        eprintln!("Failed to set global subscriber (already set)");
        return None;
    }

    Some(FileLogGuard { _guard: guard })
}

/// Builds the local JSON layer with only its configured threshold. Deliberately
/// bypasses [`mk_env_filter`] so `RUST_LOG` can refine stdout diagnostics
/// without increasing local disk volume.
///
/// The file shape is not flattened: the event text stays at
/// `fields.message`. The dashboard's log panel and the t0.devops liquidity
/// exporter (`ship_botlogs`, which feeds the `liquidity-botlogs` log) read
/// `fields` and `fields.message` from `/logs`, which serves these files, so
/// this shape is a contract of its own, separate from the console JSON.
fn file_fmt_layer<S, W>(writer: W, level: tracing::Level) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .fmt_fields(JsonFields::new())
        .event_format(JsonWithOtelLines(
            tracing_subscriber::fmt::format().json(),
            OtelLineShape::Nested,
        ))
        .with_ansi(false)
        .with_writer(writer)
        .with_filter(mk_crate_filter(level))
        .boxed()
}

/// Receives the level and target of every event the file log writes. The
/// caller owns what it does with them; it must not log, because it runs
/// inside the subscriber.
pub trait LogEventSink: Send + Sync {
    fn record(&self, level: tracing::Level, target: &str);
}

/// Forwards each event it sees to a [`LogEventSink`].
struct LogCountLayer {
    sink: Arc<dyn LogEventSink>,
}

impl<S: tracing::Subscriber> Layer<S> for LogCountLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
        let metadata = event.metadata();
        self.sink.record(*metadata.level(), metadata.target());
    }
}

/// The counting layer behind the file layer's own filter, so the sink sees
/// the events the file log writes and no others: in particular no dependency
/// TRACE callsite is enabled for it.
fn log_count_layer<S>(
    sink: Arc<dyn LogEventSink>,
    file_level: tracing::Level,
) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    LogCountLayer { sink }
        .with_filter(mk_crate_filter(file_level))
        .boxed()
}

/// Install a console-only tracing subscriber as the global default.
///
/// Used both when no log directory is configured and as a fallback when the
/// rolling file appender fails to build, so a missing/unwritable log directory
/// degrades to console logging rather than disabling logging entirely.
fn install_console_only_subscriber(
    log_format: LogFormat,
    extra_layer: Option<ExtraLayer>,
    env_filter: EnvFilter,
) {
    let fmt_layer = console_fmt_layer(
        log_format,
        env_filter,
        ConsoleTextStyle::Compact,
        std::io::stdout,
    );

    let subscriber = Registry::default().with(extra_layer).with(fmt_layer);

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        eprintln!("Failed to set global subscriber (already set)");
    }
}

pub fn mk_env_filter(level: tracing::Level) -> EnvFilter {
    let fallback_filter = mk_crate_filter(level);

    EnvFilter::try_from_default_env().unwrap_or(fallback_filter)
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
    /// module-path-based crate targets -- a `target:` overrides the module
    /// path, so an unlisted target's events are silently dropped at every
    /// level. Keep in sync with
    /// `grep -rhoE 'target: "[a-z_]+"' src/ crates/` (plus `cqrs` from the
    /// external st0x-event-sorcery crate).
    const DOMAIN_TARGETS: [&str; 22] = [
        "backfill",
        "bridge",
        "broker",
        "cqrs",
        "dashboard",
        "equity",
        "evm",
        "gas",
        "hedge",
        "inventory",
        "liq_event",
        "liq_trade",
        "liq_transfer",
        "market_data",
        "operational_alert",
        "orderbook",
        "rebalance",
        "reliability",
        "shutdown",
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
    fn crate_filter_enables_every_domain_target_in_use() {
        // A `target: "..."` overrides the module path, so a target absent
        // from DOMAIN_TARGETS is silently dropped at every level. This
        // list mirrors `grep -rhoE 'target: "[a-z_]+"' src/ crates/` in
        // the workspace; extend both when introducing a new target.
        let filter = mk_crate_filter(tracing::Level::TRACE).to_string();

        for target in [
            "backfill",
            "bridge",
            "broker",
            // From the external st0x-event-sorcery crate, so the grep in
            // the DOMAIN_TARGETS doc cannot find it -- pinned here so
            // removing it from the filter fails this test.
            "cqrs",
            "dashboard",
            "equity",
            "evm",
            "gas",
            "hedge",
            "inventory",
            "liq_event",
            "liq_trade",
            "liq_transfer",
            "market_data",
            "orderbook",
            "rebalance",
            "reliability",
            "shutdown",
            "startup",
            "tokenization",
            "wallet",
        ] {
            assert!(
                filter.contains(&format!("{target}=trace")),
                "domain target {target} is missing from the env filter: {filter}"
            );
        }
    }

    /// Captures everything written through a subscriber layer so tests can
    /// assert on the emitted bytes.
    #[derive(Clone, Default)]
    struct SharedWriter(std::sync::Arc<parking_lot::Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Emits one ERROR alert line inside a span through `layer` and returns
    /// the parsed line with its (wall-clock) timestamp removed.
    fn emit_alert_line(
        layer: Box<dyn Layer<Registry> + Send + Sync>,
        writer: &SharedWriter,
    ) -> serde_json::Value {
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("recheck", aggregate_id = "abc-123");
            let _entered = span.enter();
            tracing::error!(
                target: "operational_alert",
                alert = true,
                kind = "Low gas",
                "Low gas: json shape pin"
            );
        });

        let bytes = writer.0.lock().clone();
        let output = std::str::from_utf8(&bytes).unwrap();
        let mut lines = output.lines();
        let line = lines.next().expect("one log line was emitted");
        assert_eq!(lines.next(), None, "one event is one line: {output}");

        let mut entry: serde_json::Value = serde_json::from_str(line).unwrap();
        let timestamp = entry
            .as_object_mut()
            .unwrap()
            .remove("timestamp")
            .expect("every line carries a timestamp");
        assert!(timestamp.is_string(), "timestamp is a string: {timestamp}");
        entry
    }

    /// Pins the console JSON a log shipper parses: the message and the event
    /// fields are top-level keys, so the text is at `jsonPayload.message` and
    /// an alert's `kind` at `jsonPayload.kind`. Span context stays nested. A
    /// tracing-subscriber upgrade that changes this shape must fail here, not
    /// in the shipper.
    ///
    /// Both styles are pinned: `TelemetryCtx::setup` passes `Full` and the
    /// other subscribers `Compact`.
    #[test]
    fn json_console_layer_flattens_the_event_onto_the_line() {
        for style in [ConsoleTextStyle::Compact, ConsoleTextStyle::Full] {
            let writer = SharedWriter::default();
            let layer = console_fmt_layer(
                LogFormat::Json,
                mk_crate_filter(tracing::Level::TRACE),
                style,
                writer.clone(),
            );

            let entry = emit_alert_line(layer, &writer);

            assert_eq!(
                entry,
                serde_json::json!({
                    "level": "ERROR",
                    "target": "operational_alert",
                    "message": "Low gas: json shape pin",
                    "alert": true,
                    "kind": "Low gas",
                    "span": {"aggregate_id": "abc-123", "name": "recheck"},
                    "spans": [{"aggregate_id": "abc-123", "name": "recheck"}],
                })
            );
        }
    }

    /// An OpenTelemetry export error records only `name` and `error`, so its
    /// line's `message` is built from them instead of left empty.
    #[test]
    fn an_opentelemetry_error_without_a_message_gets_one_from_its_name() {
        let writer = SharedWriter::default();
        let layer = console_fmt_layer(
            LogFormat::Json,
            mk_crate_filter(tracing::Level::TRACE),
            ConsoleTextStyle::Full,
            writer.clone(),
        );
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            // The shape `otel_error!(name: ..., error = ...)` expands to.
            tracing::error!(
                name: "BatchLogProcessor.ExportError",
                target: "opentelemetry_sdk",
                name = "BatchLogProcessor.ExportError",
                error = "connection refused",
                ""
            );
        });

        let bytes = writer.0.lock().clone();
        let entry: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap().trim_end()).unwrap();
        assert_eq!(
            entry["message"],
            "BatchLogProcessor.ExportError: connection refused"
        );
    }

    /// Emits the event OpenTelemetry's `otel_warn!(name: ..., message = ...)`
    /// expands to for `BatchLogProcessor.LogsDropped`: the caller passes the
    /// fields, `message` among them, and the empty format string. Like
    /// `otel_warn!`, the caller's `message` field is invisible to the
    /// workspace scan, which reads the tracing call here and finds no
    /// reserved name.
    macro_rules! otel_internal_warn {
        ($($fields:tt)+) => {
            tracing::warn!(
                name: "BatchLogProcessor.LogsDropped",
                target: "opentelemetry_sdk",
                name = "BatchLogProcessor.LogsDropped",
                $($fields)+
            )
        };
    }

    /// The rolling file writes an OpenTelemetry internal log in its nested
    /// shape, with one `message` under `fields`, which its readers take.
    #[test]
    fn the_file_layer_writes_an_opentelemetry_log_with_one_nested_message() {
        let writer = SharedWriter::default();
        let layer = file_fmt_layer(writer.clone(), tracing::Level::TRACE);
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            otel_internal_warn!(message = "Logs were dropped", "");
        });

        let bytes = writer.0.lock().clone();
        let line = std::str::from_utf8(&bytes).unwrap().trim_end();
        assert_eq!(line.matches("\"message\"").count(), 1, "{line}");
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(entry["fields"]["message"], "Logs were dropped");
        assert_eq!(entry["level"], "WARN");
        assert_eq!(entry["target"], "opentelemetry_sdk");
    }

    /// OpenTelemetry's internal logs record `message` as a field and then an
    /// empty format string. The console line has one `message` key, with the
    /// real text, and no span context.
    #[test]
    fn an_opentelemetry_internal_log_has_one_message() {
        let writer = SharedWriter::default();
        let layer = console_fmt_layer(
            LogFormat::Json,
            mk_crate_filter(tracing::Level::TRACE),
            ConsoleTextStyle::Full,
            writer.clone(),
        );
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("recheck", aggregate_id = "abc-123");
            let _entered = span.enter();
            otel_internal_warn!(
                dropped_logs_count = 3_u64,
                message = "Logs were dropped",
                ""
            );
        });

        let bytes = writer.0.lock().clone();
        let line = std::str::from_utf8(&bytes).unwrap().trim_end();
        assert_eq!(line.matches("\"message\"").count(), 1, "{line}");
        let mut entry: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(
            entry
                .as_object_mut()
                .unwrap()
                .remove("timestamp")
                .is_some_and(|timestamp| timestamp.is_string())
        );
        assert_eq!(
            entry,
            serde_json::json!({
                "level": "WARN",
                "target": "opentelemetry_sdk",
                "name": "BatchLogProcessor.LogsDropped",
                "dropped_logs_count": 3,
                "message": "Logs were dropped",
            })
        );
    }

    /// The rolling file keeps the nested shape: the dashboard's log panel and
    /// the exporter's `ship_botlogs` read the message under `fields` from
    /// `/logs`, and
    /// `/performance/reliability` reads the raw `"level":"ERROR"` substring.
    #[test]
    fn file_layer_keeps_the_nested_fields_shape() {
        let writer = SharedWriter::default();
        let layer = file_fmt_layer(writer.clone(), tracing::Level::TRACE);

        let entry = emit_alert_line(layer, &writer);

        assert_eq!(
            entry,
            serde_json::json!({
                "level": "ERROR",
                "target": "operational_alert",
                "fields": {
                    "message": "Low gas: json shape pin",
                    "alert": true,
                    "kind": "Low gas",
                },
                "span": {"aggregate_id": "abc-123", "name": "recheck"},
                "spans": [{"aggregate_id": "abc-123", "name": "recheck"}],
            })
        );
        let raw = String::from_utf8(writer.0.lock().clone()).unwrap();
        assert!(raw.contains(r#""level":"ERROR""#), "{raw}");
    }

    /// Text output is unchanged by the JSON flattening: the alert text and
    /// its fields stay on one human-readable line.
    #[test]
    fn text_console_layer_renders_the_message_and_fields() {
        let writer = SharedWriter::default();
        let layer = console_fmt_layer(
            LogFormat::Text,
            mk_crate_filter(tracing::Level::TRACE),
            ConsoleTextStyle::Compact,
            writer.clone(),
        );
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(
                target: "operational_alert",
                alert = true,
                kind = "Low gas",
                "Low gas: text shape pin"
            );
        });

        let output = String::from_utf8(writer.0.lock().clone()).unwrap();
        assert_eq!(output.lines().count(), 1, "{output}");
        assert!(output.contains("Low gas: text shape pin"), "{output}");
        assert!(output.contains("kind"), "{output}");
        assert!(!output.trim_start().starts_with('{'), "{output}");
    }

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
        let file_logging =
            FileLogging::new(uncreatable_dir.to_str().unwrap().to_owned(), LogLevel::Info);

        let file_guard = setup_tracing(
            &LogLevel::Info,
            LogFormat::Text,
            Some(&file_logging),
            None,
            None,
        );

        assert!(
            file_guard.is_none(),
            "an invalid log dir must degrade to console-only logging, not return a file guard"
        );
    }

    /// A log directory that cannot be created leaves the process console
    /// only, so nothing is counted: the endpoint has no files to read either.
    #[test]
    fn setup_tracing_attaches_no_counter_without_a_file_layer() {
        let file = NamedTempFile::new().unwrap();
        let uncreatable_dir = file.path().join("nested");
        let file_logging =
            FileLogging::new(uncreatable_dir.to_str().unwrap().to_owned(), LogLevel::Info);
        let sink = Arc::new(RecordingSink::default());

        let file_guard = setup_tracing(
            &LogLevel::Info,
            LogFormat::Text,
            Some(&file_logging),
            None,
            Some(sink.clone()),
        );
        tracing::error!(target: "hedge", "not written to a file");

        assert!(file_guard.is_none());
        assert_eq!(*sink.0.lock(), []);
    }

    #[test]
    fn build_log_file_appender_prunes_files_beyond_retention_limit() {
        assert_eq!(LOG_RETENTION_DAYS, 7, "production keeps seven daily logs");

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

    #[test]
    fn build_resource_carries_service_name_and_environment() {
        use opentelemetry::Key;

        // The whole point of the environment field (Juan's review): the values
        // must actually land on the OTel resource, or staging and prod telemetry
        // are indistinguishable downstream.
        let resource = build_resource("st0x-liquidity", "staging");

        let service_name = resource
            .get(&Key::new("service.name"))
            .expect("service.name attribute must be set");
        assert_eq!(&*service_name.as_str(), "st0x-liquidity");

        let environment = resource
            .get(&Key::new("deployment.environment"))
            .expect("deployment.environment attribute must be set");
        assert_eq!(&*environment.as_str(), "staging");
    }

    #[derive(Default)]
    struct RecordingSink(parking_lot::Mutex<Vec<(tracing::Level, String)>>);

    impl LogEventSink for RecordingSink {
        fn record(&self, level: tracing::Level, target: &str) {
            self.0.lock().push((level, target.to_string()));
        }
    }

    /// The sink sees what the file layer writes at its level and nothing a
    /// dependency logs below WARN.
    #[test]
    fn the_log_count_layer_sees_only_what_the_file_layer_writes() {
        let sink = Arc::new(RecordingSink::default());
        let file = SharedWriter::default();
        let subscriber = Registry::default()
            .with(file_fmt_layer(file.clone(), tracing::Level::INFO))
            .with(log_count_layer(sink.clone(), tracing::Level::INFO));

        tracing::subscriber::with_default(subscriber, || {
            tracing::trace!(target: "hyper", "dependency detail");
            tracing::info!(target: "hyper", "dependency info");
            tracing::warn!(target: "hyper", "dependency warning");
            tracing::debug!(target: "hedge", "below the file level");
            tracing::info!(target: "hedge", "hedge info");
            tracing::error!(target: "hedge", "hedge error");
        });

        assert_eq!(
            *sink.0.lock(),
            [
                (tracing::Level::WARN, "hyper".to_string()),
                (tracing::Level::INFO, "hedge".to_string()),
                (tracing::Level::ERROR, "hedge".to_string()),
            ]
        );
        let file = String::from_utf8(file.0.lock().clone()).unwrap();
        assert_eq!(file.lines().count(), 3, "{file}");
    }

    #[test]
    fn stdout_trace_and_file_info_filters_are_independent() {
        let stdout = SharedWriter::default();
        let file = SharedWriter::default();
        let stdout_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(stdout.clone())
            .with_filter(mk_crate_filter(tracing::Level::TRACE));
        let file_layer = file_fmt_layer(file.clone(), tracing::Level::INFO);
        let subscriber = Registry::default().with(stdout_layer).with(file_layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::trace!(target: "rebalance", "remote-only detail");
            tracing::info!(target: "rebalance", "retained locally");
        });

        let stdout = String::from_utf8(stdout.0.lock().clone()).unwrap();
        let file = String::from_utf8(file.0.lock().clone()).unwrap();

        assert!(stdout.contains("remote-only detail"));
        assert!(stdout.contains("retained locally"));
        assert!(!file.contains("remote-only detail"));
        assert!(file.contains("retained locally"));
    }

    #[test]
    fn telemetry_setup_continues_without_file_logging_when_log_dir_is_invalid() {
        // A regular file cannot contain a subdirectory, so the log directory
        // cannot be created. setup() must keep the OTLP trace/log pipeline live
        // and degrade only the file-logging half, returning a None file guard
        // rather than failing the whole telemetry stack.
        let file = NamedTempFile::new().unwrap();
        let uncreatable_dir = file.path().join("nested");
        let file_logging =
            FileLogging::new(uncreatable_dir.to_str().unwrap().to_owned(), LogLevel::Info);

        let ctx = TelemetryCtx {
            service_name: "test-service".to_string(),
            environment: "test".to_string(),
            traces_endpoint: Url::parse("http://localhost:10428").unwrap(),
            logs_endpoint: Url::parse("http://localhost:9428").unwrap(),
        };

        let (file_guard, _telemetry_guard) = ctx
            .setup(
                tracing::Level::INFO,
                LogFormat::Text,
                Some(&file_logging),
                None,
                None,
            )
            .unwrap();

        assert!(
            file_guard.is_none(),
            "an invalid log dir must degrade to console-only logging (None file \
             guard) while the OTLP exporters stay live"
        );
    }

    /// The top-level keys the flattened console JSON writes itself. An event
    /// field with one of these names writes a second key of that name on the
    /// line, and a JSON reader keeps only one of them.
    const RESERVED_CONSOLE_KEYS: [&str; 6] =
        ["message", "timestamp", "level", "target", "span", "spans"];

    /// The arguments of the tracing macro call that starts at `open` (just
    /// after its `(`), split at top-level commas, up to the first argument
    /// that is a string literal: that literal is the message, and the fields
    /// come before it. String literals inside an argument are skipped whole,
    /// so a `)` or `,` in a value does not end or split it, and so are `//`
    /// comments, up to the end of their line.
    fn macro_fields(source: &str, open: usize) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current = String::new();
        let mut depth = 0_u32;
        let mut characters = source[open..].chars().peekable();
        while let Some(character) = characters.next() {
            match character {
                '/' if characters.peek() == Some(&'/') => {
                    characters.by_ref().find(|&inner| inner == '\n');
                    continue;
                }
                '"' if depth == 0 && current.trim().is_empty() => break,
                '"' => {
                    current.push(character);
                    let mut escaped = false;
                    for inner in characters.by_ref() {
                        current.push(inner);
                        match inner {
                            '\\' if !escaped => escaped = true,
                            '"' if !escaped => break,
                            _ => escaped = false,
                        }
                    }
                    continue;
                }
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' if depth == 0 => break,
                ')' | ']' | '}' => depth -= 1,
                ',' if depth == 0 => {
                    fields.push(std::mem::take(&mut current));
                    continue;
                }
                _ => {}
            }
            current.push(character);
        }
        fields.push(current);
        fields
    }

    /// The field name a macro argument records, if it is a field: `name = x`,
    /// `name` or `%name`/`?name` shorthand. `target: "x"` is the macro's
    /// target, not a field.
    fn field_name(argument: &str) -> Option<&str> {
        let argument = argument.trim();
        let name = argument
            .split_once('=')
            .map_or(argument, |(name, _)| name)
            .trim()
            .trim_start_matches(['%', '?']);
        let is_identifier = !name.is_empty()
            && name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_');
        is_identifier.then_some(name)
    }

    /// The tracing event macros the workspace scan reads.
    const EVENT_MACROS: [&str; 6] = ["trace", "debug", "info", "warn", "error", "event"];

    /// Where the arguments of each tracing event macro call in `source`
    /// start: just after the `(`, `[` or `{` that follows `warn!` and the
    /// others, with any whitespace between them. A name preceded by an
    /// identifier character (`otel_warn!`) is another macro and is skipped.
    fn event_macro_calls(source: &str) -> Vec<usize> {
        let mut opens = Vec::new();
        for name in EVENT_MACROS {
            let bang = format!("{name}!");
            for (offset, _) in source.match_indices(&bang) {
                let preceded_by_identifier = source[..offset]
                    .chars()
                    .next_back()
                    .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_');
                if preceded_by_identifier {
                    continue;
                }
                let after_bang = offset + bang.len();
                let rest = &source[after_bang..];
                let delimited = rest.trim_start();
                if delimited.starts_with(['(', '[', '{']) {
                    opens.push(after_bang + rest.len() - delimited.len() + 1);
                }
            }
        }
        opens.sort_unstable();
        opens
    }

    /// No tracing event in the workspace records a field with a reserved
    /// console key's name (docs/observability.md, "Console format").
    #[test]
    fn no_tracing_event_records_a_reserved_console_key() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut pending = vec![root.join("src"), root.join("crates")];
        let mut violations = Vec::new();

        while let Some(path) = pending.pop() {
            if path.is_dir() {
                pending.extend(
                    std::fs::read_dir(&path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
                continue;
            }
            if path.extension().is_none_or(|extension| extension != "rs") {
                continue;
            }

            let source = std::fs::read_to_string(&path).unwrap();
            for open in event_macro_calls(&source) {
                for argument in macro_fields(&source, open) {
                    if let Some(name) = field_name(&argument)
                        && RESERVED_CONSOLE_KEYS.contains(&name)
                    {
                        let line = source[..open].matches('\n').count() + 1;
                        violations.push(format!("{}:{line}: {name}", path.display()));
                    }
                }
            }
        }

        assert_eq!(violations, Vec::<String>::new());
    }

    #[test]
    fn the_call_scan_reads_every_delimiter_form_and_skips_other_macros() {
        // Split so the workspace scan does not read this fixture as calls.
        let source = concat!(
            "warn",
            "!(a)\n",
            "tracing::info",
            "! { b }\n",
            "error",
            "![c]\n",
            "debug",
            "!{d}\n",
            "otel_warn",
            "!(e)\n",
            "info_span",
            "!(f)\n",
            "warn",
            "! is not a call\n",
        );
        let found: Vec<_> = event_macro_calls(source)
            .into_iter()
            .map(|open| source[open..].trim_start().chars().next().unwrap())
            .collect();

        assert_eq!(found, ['a', 'b', 'c', 'd']);
    }

    #[test]
    fn the_field_scan_reads_fields_and_skips_the_target_and_format_arguments() {
        // Split so the workspace scan does not read this fixture as a call.
        let source = concat!(
            "warn",
            r#"!(target: "wallet", %contract, note = "a, b)", target, level = std::u32::MAX, "#,
            "// a comment ends here: ), spans\n",
            r#"message = %format!("a: {x}"), "text {}", span)"#
        );
        let fields: Vec<_> = macro_fields(source, "warn!(".len())
            .iter()
            .filter_map(|argument| field_name(argument).map(str::to_string))
            .collect();

        assert_eq!(fields, ["contract", "note", "target", "level", "message"]);
    }
}
