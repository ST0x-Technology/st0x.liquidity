//! Shared structured audit records for mutation-capable operator routes.

use std::error::Error as _;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use axum::Json;
use axum::body::{Body, to_bytes};
use axum::extract::{MatchedPath, Request};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use chrono::{SecondsFormat, Utc};
use futures_util::FutureExt;
use http_body_util::LengthLimitError;
use parking_lot::Mutex;
use percent_encoding::percent_decode_str;
use serde_json::Value;
use tracing::{Level, error, info, warn};
use uuid::Uuid;

const AUDIT_SCHEMA: &str = "st0x.operations.audit.v1";
const SERVICE: &str = "liquidity";
const REQUEST_ID_HEADER: &str = "x-request-id";
const BODY_LIMIT: usize = 2 * 1024 * 1024;
const TARGET_VALUE_LIMIT: usize = 256;
const REASON_LIMIT: usize = 1024;
const UNAUTHENTICATED: &str = "unauthenticated";
const NOT_IDENTIFIED: &str = "not_identified";
const NOT_PROVIDED: &str = "not_provided";
const UNMATCHED_ROUTE: &str = "unmatched";

#[derive(Clone, Copy, Debug)]
struct AuditRouteTemplate(&'static str);

#[cfg(test)]
#[derive(Clone, Debug)]
struct AuditCompletionSignal(Arc<tokio::sync::Notify>);

#[derive(Clone, Debug)]
struct OperationsAuditContext {
    inner: Arc<AuditContextInner>,
}

#[derive(Debug)]
struct AuditContextInner {
    request_id: Uuid,
    role: &'static str,
    route: String,
    path: String,
    principal: Mutex<String>,
    targets: Mutex<Vec<String>>,
    reason: Mutex<String>,
    #[cfg(test)]
    completion: Option<AuditCompletionSignal>,
}

impl OperationsAuditContext {
    fn new(request: &Request, role: &'static str, principal: &'static str) -> Self {
        let route = request
            .extensions()
            .get::<AuditRouteTemplate>()
            .map(|route| route.0.to_string())
            .or_else(|| {
                request
                    .extensions()
                    .get::<MatchedPath>()
                    .map(|path| path.as_str().to_string())
            })
            .unwrap_or_else(|| UNMATCHED_ROUTE.to_string());
        Self {
            inner: Arc::new(AuditContextInner {
                request_id: request_id(request),
                role,
                route,
                path: request.uri().path().to_string(),
                principal: Mutex::new(principal.to_string()),
                targets: Mutex::new(Vec::new()),
                reason: Mutex::new(NOT_PROVIDED.to_string()),
                #[cfg(test)]
                completion: request.extensions().get::<AuditCompletionSignal>().cloned(),
            }),
        }
    }

    fn set_principal(&self, principal: impl Into<String>) {
        *self.inner.principal.lock() = principal.into();
    }

    fn capture_path_targets(&self) -> Result<(), AuditValueError> {
        let targets = dynamic_path_values(&self.inner.route, &self.inner.path)?;
        for target in &targets {
            validate_audit_value("path parameter", target, TARGET_VALUE_LIMIT)?;
        }
        *self.inner.targets.lock() = targets;
        Ok(())
    }

    fn capture_body(&self, body: &Value) -> Result<(), AuditValueError> {
        let metadata = audit_body_metadata(&self.inner.route, body)?;
        let mut targets = self.inner.targets.lock();
        for target in metadata.targets {
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
        drop(targets);

        if let Some(reason) = metadata.reason {
            *self.inner.reason.lock() = reason;
        }
        Ok(())
    }

    fn event(&self, status: StatusCode) -> OperationsAuditEvent {
        let targets = self.inner.targets.lock();
        OperationsAuditEvent {
            principal: self.inner.principal.lock().clone(),
            role: self.inner.role,
            route: self.inner.route.clone(),
            request_id: self.inner.request_id,
            target_id: if targets.is_empty() {
                NOT_IDENTIFIED.to_string()
            } else {
                targets.join(":")
            },
            reason: self.inner.reason.lock().clone(),
            outcome: AuditOutcome::from_status(status),
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        }
    }
}

#[derive(Debug)]
struct OperationsAuditEvent {
    principal: String,
    role: &'static str,
    route: String,
    request_id: Uuid,
    target_id: String,
    reason: String,
    outcome: AuditOutcome,
    timestamp: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuditOutcome {
    Success,
    Denied,
    ValidationFailure,
    CommandFailure,
}

impl AuditOutcome {
    const fn from_status(status: StatusCode) -> Self {
        match status.as_u16() {
            200..=299 => Self::Success,
            401 | 403 => Self::Denied,
            400 | 404 | 405 | 413 | 415 | 422 => Self::ValidationFailure,
            _ => Self::CommandFailure,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Denied => "denied",
            Self::ValidationFailure => "validation_failure",
            Self::CommandFailure => "command_failure",
        }
    }

    const fn level(self) -> Level {
        match self {
            Self::Success => Level::INFO,
            Self::Denied | Self::ValidationFailure | Self::CommandFailure => Level::WARN,
        }
    }
}

trait OperationsAuditRecorder {
    fn record(&self, event: &OperationsAuditEvent) -> Result<(), AuditRecordError>;
}

#[derive(Debug, thiserror::Error)]
enum AuditRecordError {
    #[error("operations_audit target is disabled at {level}")]
    TargetDisabled { level: Level },
    #[cfg(test)]
    #[error("simulated audit recorder failure")]
    Simulated,
}

struct LogOperationsAuditRecorder;

impl OperationsAuditRecorder for LogOperationsAuditRecorder {
    fn record(&self, event: &OperationsAuditEvent) -> Result<(), AuditRecordError> {
        let level = event.outcome.level();
        let enabled = match event.outcome {
            AuditOutcome::Success => tracing::enabled!(target: "operations_audit", Level::INFO),
            AuditOutcome::Denied
            | AuditOutcome::ValidationFailure
            | AuditOutcome::CommandFailure => {
                tracing::enabled!(target: "operations_audit", Level::WARN)
            }
        };
        if !enabled {
            return Err(AuditRecordError::TargetDisabled { level });
        }

        match event.outcome {
            AuditOutcome::Success => info!(
                target: "operations_audit",
                audit_schema = AUDIT_SCHEMA,
                service = SERVICE,
                principal = %event.principal,
                role = event.role,
                route = %event.route,
                request_id = %event.request_id,
                target_id = %event.target_id,
                reason = %event.reason,
                outcome = event.outcome.as_str(),
                timestamp = %event.timestamp,
                "Operations audit event"
            ),
            AuditOutcome::Denied
            | AuditOutcome::ValidationFailure
            | AuditOutcome::CommandFailure => warn!(
                target: "operations_audit",
                audit_schema = AUDIT_SCHEMA,
                service = SERVICE,
                principal = %event.principal,
                role = event.role,
                route = %event.route,
                request_id = %event.request_id,
                target_id = %event.target_id,
                reason = %event.reason,
                outcome = event.outcome.as_str(),
                timestamp = %event.timestamp,
                "Operations audit event"
            ),
        }

        Ok(())
    }
}

fn record_with(recorder: &dyn OperationsAuditRecorder, event: &OperationsAuditEvent) {
    if let Err(record_error) = recorder.record(event) {
        error!(
            target: "operational_alert",
            alert = true,
            audit_schema = AUDIT_SCHEMA,
            service = SERVICE,
            principal = %event.principal,
            role = event.role,
            route = %event.route,
            request_id = %event.request_id,
            target_id = %event.target_id,
            reason = %event.reason,
            outcome = event.outcome.as_str(),
            timestamp = %event.timestamp,
            %record_error,
            "Operations audit recording failed"
        );
    }
}

fn finalize_response(context: &OperationsAuditContext, mut response: Response) -> Response {
    let event = context.event(response.status());
    if let Ok(request_id) = HeaderValue::from_str(&event.request_id.to_string()) {
        response.headers_mut().insert(REQUEST_ID_HEADER, request_id);
    }
    record_with(&LogOperationsAuditRecorder, &event);
    #[cfg(test)]
    if let Some(completion) = &context.inner.completion {
        completion.0.notify_one();
    }
    response
}

/// Wraps authentication, request extraction, and the handler so every final
/// response produces one audit record. The owned task outlives a disconnected
/// HTTP waiter, so client cancellation cannot cancel command completion or its
/// final audit event.
pub(crate) async fn audit_request(
    role: &'static str,
    mut request: Request,
    next: Next,
) -> Response {
    let context = OperationsAuditContext::new(&request, role, UNAUTHENTICATED);
    request.extensions_mut().insert(context.clone());

    let task_context = context.clone();
    let completion = tokio::spawn(async move {
        let response =
            if let Ok(response) = AssertUnwindSafe(next.run(request)).catch_unwind().await {
                response
            } else {
                error!(
                    target: "operational_alert",
                    alert = true,
                    audit_schema = AUDIT_SCHEMA,
                    service = SERVICE,
                    role,
                    request_id = %task_context.inner.request_id,
                    "Audited operator command panicked"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "operator command failed" })),
                )
                    .into_response()
            };
        finalize_response(&task_context, response)
    });

    match completion.await {
        Ok(response) => response,
        Err(join_error) => {
            error!(
                target: "operational_alert",
                alert = true,
                audit_schema = AUDIT_SCHEMA,
                service = SERVICE,
                role,
                request_id = %context.inner.request_id,
                task_cancelled = join_error.is_cancelled(),
                "Audited operator command task failed"
            );
            let response = (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "operator command failed" })),
            )
                .into_response();
            finalize_response(&context, response)
        }
    }
}

/// Captures only route-specific canonical audit identifiers from the JSON body
/// after the caller has passed its authentication gate, then reconstructs the
/// body unchanged for the route extractor.
pub(crate) async fn capture_request_body(request: Request, next: Next) -> Response {
    let context = request
        .extensions()
        .get::<OperationsAuditContext>()
        .cloned();
    if let Some(context) = &context
        && let Err(error) = context.capture_path_targets()
    {
        return audit_value_error_response(&error);
    }

    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, BODY_LIMIT).await {
        Ok(bytes) => bytes,
        Err(error) => {
            let length_limited = error
                .source()
                .is_some_and(<dyn std::error::Error>::is::<LengthLimitError>);
            if length_limited {
                warn!(
                    target: "api",
                    %error,
                    "Operations audit request body exceeded limit"
                );
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    Json(serde_json::json!({ "error": "request body exceeds limit" })),
                )
                    .into_response();
            }

            warn!(
                target: "api",
                %error,
                "Operations audit request body stream failed"
            );
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "failed to read request body" })),
            )
                .into_response();
        }
    };

    if let Some(context) = context
        && let Ok(body) = serde_json::from_slice::<Value>(&bytes)
        && let Err(error) = context.capture_body(&body)
    {
        return audit_value_error_response(&error);
    }

    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

pub(crate) fn set_audit_route_template(request: &mut Request, route: &'static str) {
    request.extensions_mut().insert(AuditRouteTemplate(route));
}

pub(crate) fn record_principal(request: &Request, principal: impl Into<String>) {
    if let Some(context) = request.extensions().get::<OperationsAuditContext>() {
        context.set_principal(principal);
    }
}

fn request_id(request: &Request) -> Uuid {
    request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(Uuid::new_v4)
}

#[derive(Clone, Copy)]
struct AuditBodyFields {
    targets: &'static [&'static str],
    reason: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct AuditBodyMetadata {
    targets: Vec<String>,
    reason: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum AuditValueError {
    TooLong {
        field: &'static str,
        max_bytes: usize,
    },
    InvalidPathUtf8,
}

fn audit_body_fields(route: &str) -> AuditBodyFields {
    let no_fields = AuditBodyFields {
        targets: &[],
        reason: false,
    };
    match route {
        "/liquidity-write/transfers/fail/{kind}/{id}"
        | "/transfers/fail/{kind}/{id}"
        | "/liquidity-write/transfers/usdc/{id}/clear-pending-burn"
        | "/liquidity-write/transfers/usdc/{id}/fail"
        | "/liquidity-write/positions/{symbol}/set" => AuditBodyFields {
            targets: &[],
            reason: true,
        },
        "/liquidity-write/transfers/usdc/{id}/reconcile"
        | "/liquidity-write/transfers/{kind}/{id}/reconcile" => AuditBodyFields {
            targets: &["supersedingTx"],
            reason: true,
        },
        "/liquidity-write/transfers/equity_redemption/{id}/adopt-withdrawal" => AuditBodyFields {
            targets: &["replacementTx"],
            reason: true,
        },
        "/liquidity-write/positions/{symbol}/release-hedge" => AuditBodyFields {
            targets: &["order_id"],
            reason: true,
        },
        "/liquidity-write/portfolio-snapshot/marks" => AuditBodyFields {
            targets: &["day", "symbol"],
            reason: true,
        },
        "/liquidity-write/views/{view}/rebuild" => AuditBodyFields {
            targets: &["id"],
            reason: false,
        },
        "/liquidity-write/cctp/complete-mint" => AuditBodyFields {
            targets: &["burnTx", "sourceChain"],
            reason: false,
        },
        "/liquidity-write/capital/transfer-usdc" => AuditBodyFields {
            targets: &["direction", "chain"],
            reason: false,
        },
        "/liquidity-write/capital/vault-deposit" | "/liquidity-write/capital/vault-withdraw" => {
            AuditBodyFields {
                targets: &["chain", "token", "vaultId"],
                reason: false,
            }
        }
        "/liquidity-write/capital/vault-withdraw-usdc"
        | "/liquidity-write/capital/reset-allowance" => AuditBodyFields {
            targets: &["chain"],
            reason: false,
        },
        "/liquidity-write/capital/cctp-bridge" => AuditBodyFields {
            targets: &["operationId", "from"],
            reason: false,
        },
        "/liquidity-write/capital/cctp-burn-supersede" => AuditBodyFields {
            targets: &["operationId", "supersedingTx"],
            reason: false,
        },
        _ => no_fields,
    }
}

fn audit_body_metadata(route: &str, body: &Value) -> Result<AuditBodyMetadata, AuditValueError> {
    let fields = audit_body_fields(route);
    let Some(object) = body.as_object() else {
        return Ok(AuditBodyMetadata {
            targets: Vec::new(),
            reason: None,
        });
    };

    let mut targets = Vec::with_capacity(fields.targets.len());
    for &field in fields.targets {
        if let Some(value) = object.get(field).and_then(Value::as_str) {
            validate_audit_value(field, value, TARGET_VALUE_LIMIT)?;
            if !targets.iter().any(|target| target == value) {
                targets.push(value.to_string());
            }
        }
    }

    let reason = if fields.reason {
        object
            .get("reason")
            .and_then(Value::as_str)
            .map(|reason| {
                validate_audit_value("reason", reason, REASON_LIMIT)?;
                Ok(reason.to_string())
            })
            .transpose()?
    } else {
        None
    };

    Ok(AuditBodyMetadata { targets, reason })
}

fn validate_audit_value(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), AuditValueError> {
    if value.len() > max_bytes {
        Err(AuditValueError::TooLong { field, max_bytes })
    } else {
        Ok(())
    }
}

fn audit_value_error_response(error: &AuditValueError) -> Response {
    let message = match error {
        AuditValueError::TooLong { field, max_bytes } => {
            warn!(
                target: "api",
                field,
                max_bytes,
                "Operations audit field exceeded its size limit"
            );
            format!("audit field `{field}` exceeds {max_bytes} bytes")
        }
        AuditValueError::InvalidPathUtf8 => {
            warn!(
                target: "api",
                "Operations audit path parameter is not valid UTF-8"
            );
            "audit path parameter is not valid UTF-8".to_string()
        }
    };
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": message })),
    )
        .into_response()
}

fn dynamic_path_values(route: &str, path: &str) -> Result<Vec<String>, AuditValueError> {
    route
        .split('?')
        .next()
        .unwrap_or(route)
        .trim_matches('/')
        .split('/')
        .zip(path.trim_matches('/').split('/'))
        .filter(|(template, _)| {
            ((template.starts_with('{') && template.ends_with('}'))
                || (template.starts_with('<') && template.ends_with('>')))
                && !template.starts_with("{*")
        })
        .map(|(_, actual)| {
            percent_decode_str(actual)
                .decode_utf8()
                .map(std::borrow::Cow::into_owned)
                .map_err(|_| AuditValueError::InvalidPathUtf8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use axum::body::{Body, Bytes, to_bytes};
    use axum::extract::{Path, Request};
    use axum::http::{Request as HttpRequest, StatusCode};
    use axum::middleware::Next;
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::Value;
    use tokio::sync::Notify;
    use tower::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{EnvFilter, Registry};
    use tracing_test::traced_test;
    use uuid::uuid;

    use super::{
        AuditBodyMetadata, AuditCompletionSignal, AuditOutcome, AuditRecordError, AuditValueError,
        BODY_LIMIT, LogOperationsAuditRecorder, OperationsAuditEvent, OperationsAuditRecorder,
        REASON_LIMIT, TARGET_VALUE_LIMIT, audit_body_metadata, audit_request, capture_request_body,
        dynamic_path_values, record_principal, record_with,
    };

    struct FailingRecorder;

    impl OperationsAuditRecorder for FailingRecorder {
        fn record(&self, _event: &OperationsAuditEvent) -> Result<(), AuditRecordError> {
            Err(AuditRecordError::Simulated)
        }
    }

    fn event(outcome: AuditOutcome) -> OperationsAuditEvent {
        OperationsAuditEvent {
            principal: "accounts.google.com:1234".to_string(),
            role: "write",
            route: "/liquidity-write/transfers/{kind}/{id}/reconcile".to_string(),
            request_id: uuid!("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            target_id: "mint:mint-9".to_string(),
            reason: "incident 42".to_string(),
            outcome,
            timestamp: "2026-01-01T00:00:00.000Z".to_string(),
        }
    }

    #[test]
    fn response_statuses_have_stable_outcomes() {
        assert_eq!(
            AuditOutcome::from_status(StatusCode::OK),
            AuditOutcome::Success
        );
        assert_eq!(
            AuditOutcome::from_status(StatusCode::UNAUTHORIZED),
            AuditOutcome::Denied
        );
        assert_eq!(
            AuditOutcome::from_status(StatusCode::UNPROCESSABLE_ENTITY),
            AuditOutcome::ValidationFailure
        );
        assert_eq!(
            AuditOutcome::from_status(StatusCode::INTERNAL_SERVER_ERROR),
            AuditOutcome::CommandFailure
        );
    }

    #[test]
    fn target_is_composed_from_path_and_canonical_body_identifiers() {
        let path_targets = dynamic_path_values(
            "/liquidity-write/transfers/{kind}/{id}/reconcile",
            "/liquidity-write/transfers/mint/mint-9/reconcile",
        )
        .unwrap();
        let body = audit_body_metadata(
            "/liquidity-write/transfers/{kind}/{id}/reconcile",
            &serde_json::json!({
                "supersedingTx": "0x1234",
                "reason": "incident 42"
            }),
        )
        .unwrap();

        assert_eq!(path_targets, ["mint", "mint-9"]);
        assert_eq!(body.targets, ["0x1234"]);
        assert_eq!(body.reason.as_deref(), Some("incident 42"));
    }

    #[test]
    fn nested_and_alias_fields_cannot_forge_audit_metadata() {
        let body = audit_body_metadata(
            "/liquidity-write/transfers/{kind}/{id}/reconcile",
            &serde_json::json!({
                "audit_reason": "forged snake alias",
                "auditReason": "forged alias",
                "superseding_tx": "forged alias",
                "nested": {
                    "reason": "forged nested",
                    "supersedingTx": "forged nested"
                }
            }),
        )
        .unwrap();

        assert_eq!(
            body,
            AuditBodyMetadata {
                targets: Vec::new(),
                reason: None,
            }
        );
    }

    #[test]
    fn body_only_routes_identify_their_canonical_targets() {
        let cctp = audit_body_metadata(
            "/liquidity-write/cctp/complete-mint",
            &serde_json::json!({
                "burnTx": "0xburn",
                "sourceChain": "base",
                "burn_tx": "forged"
            }),
        )
        .unwrap();
        assert_eq!(cctp.targets, ["0xburn", "base"]);

        let capital = audit_body_metadata(
            "/liquidity-write/capital/vault-deposit",
            &serde_json::json!({
                "chain": "base",
                "token": "0xtoken",
                "vaultId": "0xvault"
            }),
        )
        .unwrap();
        assert_eq!(capital.targets, ["base", "0xtoken", "0xvault"]);

        let position = audit_body_metadata(
            "/liquidity-write/positions/{symbol}/release-hedge",
            &serde_json::json!({
                "order_id": "order-1",
                "orderId": "forged alias",
                "reason": "incident 42"
            }),
        )
        .unwrap();
        assert_eq!(position.targets, ["order-1"]);

        let mark = audit_body_metadata(
            "/liquidity-write/portfolio-snapshot/marks",
            &serde_json::json!({
                "day": "2026-10-09",
                "symbol": "AAPL",
                "reason": "correct close"
            }),
        )
        .unwrap();
        assert_eq!(mark.targets, ["2026-10-09", "AAPL"]);
    }

    #[test]
    fn wildcard_paths_do_not_create_raw_uri_cardinality() {
        assert_eq!(
            dynamic_path_values(
                "/liquidity-write/{*path}",
                "/liquidity-write/arbitrary/unmatched/path"
            )
            .unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn path_targets_use_axum_compatible_percent_decoding() {
        assert_eq!(
            dynamic_path_values(
                "/liquidity-write/positions/{symbol}/set",
                "/liquidity-write/positions/%41APL/set"
            )
            .unwrap(),
            ["AAPL"]
        );
        assert_eq!(
            dynamic_path_values(
                "/liquidity-write/positions/{symbol}/set",
                "/liquidity-write/positions/%FF/set"
            ),
            Err(AuditValueError::InvalidPathUtf8)
        );
    }

    #[test]
    fn audit_body_values_have_explicit_size_limits() {
        let oversized_reason = "r".repeat(REASON_LIMIT + 1);
        let reason_error = audit_body_metadata(
            "/liquidity-write/transfers/fail/{kind}/{id}",
            &serde_json::json!({ "reason": oversized_reason }),
        )
        .unwrap_err();
        assert_eq!(
            reason_error,
            AuditValueError::TooLong {
                field: "reason",
                max_bytes: REASON_LIMIT,
            }
        );

        let oversized_target = "t".repeat(TARGET_VALUE_LIMIT + 1);
        let target_error = audit_body_metadata(
            "/liquidity-write/cctp/complete-mint",
            &serde_json::json!({
                "burnTx": oversized_target,
                "sourceChain": "base"
            }),
        )
        .unwrap_err();
        assert_eq!(
            target_error,
            AuditValueError::TooLong {
                field: "burnTx",
                max_bytes: TARGET_VALUE_LIMIT,
            }
        );
    }

    #[traced_test]
    #[test]
    fn shared_query_fields_are_logged_for_every_outcome() {
        for outcome in [
            AuditOutcome::Success,
            AuditOutcome::Denied,
            AuditOutcome::ValidationFailure,
            AuditOutcome::CommandFailure,
        ] {
            record_with(&LogOperationsAuditRecorder, &event(outcome));
        }

        logs_assert(|lines| {
            for outcome in ["success", "denied", "validation_failure", "command_failure"] {
                let matched = lines.iter().any(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains("audit_schema=\"st0x.operations.audit.v1\"")
                        && line.contains("service=\"liquidity\"")
                        && line.contains("principal=accounts.google.com:1234")
                        && line.contains("role=\"write\"")
                        && line.contains("request_id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                        && line.contains("target_id=mint:mint-9")
                        && line.contains("reason=incident 42")
                        && line.contains(&format!("outcome=\"{outcome}\""))
                });
                if !matched {
                    return Err(format!("missing audit event for {outcome}: {lines:?}"));
                }
            }
            Ok(())
        });
    }

    async fn test_authentication(request: Request, next: Next) -> Response {
        let Some(principal) = request
            .headers()
            .get("x-test-principal")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
        else {
            return StatusCode::UNAUTHORIZED.into_response();
        };

        record_principal(&request, principal);
        next.run(request).await
    }

    async fn command(
        Path((kind, _id)): Path<(String, String)>,
        Json(_body): Json<Value>,
    ) -> StatusCode {
        if kind == "failure" {
            StatusCode::INTERNAL_SERVER_ERROR
        } else {
            StatusCode::OK
        }
    }

    async fn panicking_command() -> StatusCode {
        panic!("simulated handler panic");
    }

    async fn wait_then_panic(started: Arc<Notify>, release: Arc<Notify>) -> StatusCode {
        started.notify_one();
        release.notified().await;
        panic!("simulated handler panic");
    }

    async fn wait_for_signal(signal: &Notify) {
        tokio::time::timeout(Duration::from_secs(1), signal.notified())
            .await
            .expect("audit synchronization timed out");
    }

    fn with_audit_layers(router: Router) -> Router {
        router
            .layer(axum::middleware::from_fn(capture_request_body))
            .layer(axum::middleware::from_fn(test_authentication))
            .layer(axum::middleware::from_fn(|request, next| async move {
                audit_request("write", request, next).await
            }))
    }

    fn audited_router() -> Router {
        with_audit_layers(Router::new().route(
            "/liquidity-write/transfers/{kind}/{id}/reconcile",
            post(command),
        ))
    }

    fn request(
        path: &str,
        request_id: &str,
        principal: Option<&str>,
        body: &str,
    ) -> HttpRequest<Body> {
        let mut request = HttpRequest::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .header("x-request-id", request_id);
        if let Some(principal) = principal {
            request = request.header("x-test-principal", principal);
        }
        request.body(Body::from(body.to_string())).unwrap()
    }

    #[traced_test]
    #[tokio::test]
    async fn middleware_audits_success_denial_validation_and_command_failure() {
        let app = audited_router();
        let cases = [
            (
                request(
                    "/liquidity-write/transfers/success/widget-1/reconcile",
                    "11111111-1111-1111-1111-111111111111",
                    Some("accounts.google.com:1234"),
                    r#"{"supersedingTx":"op-1","reason":"incident 42"}"#,
                ),
                StatusCode::OK,
            ),
            (
                request(
                    "/liquidity-write/transfers/success/widget-2/reconcile",
                    "22222222-2222-2222-2222-222222222222",
                    None,
                    r#"{"supersedingTx":"op-2","reason":"incident 42"}"#,
                ),
                StatusCode::UNAUTHORIZED,
            ),
            (
                request(
                    "/liquidity-write/transfers/success/widget-3/reconcile",
                    "33333333-3333-3333-3333-333333333333",
                    Some("accounts.google.com:1234"),
                    "{",
                ),
                StatusCode::BAD_REQUEST,
            ),
            (
                request(
                    "/liquidity-write/transfers/failure/widget-4/reconcile",
                    "44444444-4444-4444-4444-444444444444",
                    Some("accounts.google.com:1234"),
                    r#"{"supersedingTx":"op-4","reason":"incident 42"}"#,
                ),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];

        for (request, expected_status) in cases {
            let expected_request_id = request.headers()["x-request-id"].clone();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected_status);
            assert_eq!(
                response.headers().get("x-request-id"),
                Some(&expected_request_id)
            );
        }

        logs_assert(|lines| {
            for (request_id, principal, target_id, reason, outcome) in [
                (
                    "11111111-1111-1111-1111-111111111111",
                    "accounts.google.com:1234",
                    "success:widget-1:op-1",
                    "incident 42",
                    "success",
                ),
                (
                    "22222222-2222-2222-2222-222222222222",
                    "unauthenticated",
                    "not_identified",
                    "not_provided",
                    "denied",
                ),
                (
                    "33333333-3333-3333-3333-333333333333",
                    "accounts.google.com:1234",
                    "success:widget-3",
                    "not_provided",
                    "validation_failure",
                ),
                (
                    "44444444-4444-4444-4444-444444444444",
                    "accounts.google.com:1234",
                    "failure:widget-4:op-4",
                    "incident 42",
                    "command_failure",
                ),
            ] {
                let matched = lines.iter().any(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(&format!("principal={principal}"))
                        && line.contains(&format!("request_id={request_id}"))
                        && line.contains(&format!("target_id={target_id}"))
                        && line.contains(&format!("reason={reason}"))
                        && line.contains(&format!("outcome=\"{outcome}\""))
                });
                if !matched {
                    return Err(format!(
                        "missing {outcome} audit for request {request_id}: {lines:?}"
                    ));
                }
            }
            Ok(())
        });
    }

    #[tokio::test]
    async fn oversized_audit_value_is_rejected_before_the_handler() {
        let called = Arc::new(AtomicBool::new(false));
        let handler_called = Arc::clone(&called);
        let app = with_audit_layers(Router::new().route(
            "/liquidity-write/positions/{symbol}/release-hedge",
            post(move |Json(_body): Json<Value>| {
                let called = Arc::clone(&handler_called);
                async move {
                    called.store(true, Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        ));
        let body = serde_json::json!({
            "order_id": "x".repeat(TARGET_VALUE_LIMIT + 1),
            "reason": "incident 42"
        })
        .to_string();

        let response = app
            .clone()
            .oneshot(request(
                "/liquidity-write/positions/AAPL/release-hedge",
                "55555555-5555-5555-5555-555555555555",
                Some("accounts.google.com:1234"),
                &body,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!called.load(Ordering::SeqCst));
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "error": format!(
                    "audit field `order_id` exceeds {TARGET_VALUE_LIMIT} bytes"
                )
            })
        );

        let oversized_reason = serde_json::json!({
            "order_id": "order-1",
            "reason": "r".repeat(REASON_LIMIT + 1)
        })
        .to_string();
        let response = app
            .oneshot(request(
                "/liquidity-write/positions/AAPL/release-hedge",
                "56565656-5656-5656-5656-565656565656",
                Some("accounts.google.com:1234"),
                &oversized_reason,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!called.load(Ordering::SeqCst));
    }

    #[traced_test]
    #[tokio::test]
    async fn validation_diagnostics_do_not_pollute_the_schema_audit_target() {
        let app = audited_router();
        let request_id = "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a";
        let body = serde_json::json!({
            "supersedingTx": "x".repeat(TARGET_VALUE_LIMIT + 1)
        })
        .to_string();

        let response = app
            .oneshot(request(
                "/liquidity-write/transfers/success/widget/reconcile",
                request_id,
                Some("accounts.google.com:1234"),
                &body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        logs_assert(|lines| {
            let audit_records: Vec<_> = lines
                .iter()
                .filter(|line| line.contains("operations_audit:"))
                .collect();
            if audit_records.len() != 1
                || !audit_records[0].contains("Operations audit event")
                || !audit_records[0].contains("audit_schema=\"st0x.operations.audit.v1\"")
                || !audit_records[0].contains(request_id)
                || !audit_records[0].contains("outcome=\"validation_failure\"")
            {
                return Err(format!(
                    "expected exactly one schema event on operations_audit: {lines:?}"
                ));
            }
            if !lines
                .iter()
                .any(|line| line.contains("api: Operations audit field exceeded its size limit"))
            {
                return Err(format!("missing ordinary validation diagnostic: {lines:?}"));
            }
            Ok(())
        });
    }

    #[tokio::test]
    async fn invalid_utf8_path_is_rejected_only_after_authentication() {
        let app = audited_router();
        let denied = app
            .clone()
            .oneshot(request(
                "/liquidity-write/transfers/success/%FF/reconcile",
                "57575757-5757-5757-5757-575757575757",
                None,
                "{}",
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

        let rejected = app
            .oneshot(request(
                "/liquidity-write/transfers/success/%FF/reconcile",
                "58585858-5858-5858-5858-585858585858",
                Some("accounts.google.com:1234"),
                "{}",
            ))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(rejected.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({ "error": "audit path parameter is not valid UTF-8" })
        );
    }

    #[tokio::test]
    async fn length_limit_and_body_stream_failures_have_distinct_statuses() {
        let app = audited_router();
        let too_large = request(
            "/liquidity-write/transfers/success/widget/reconcile",
            "66666666-6666-6666-6666-666666666666",
            Some("accounts.google.com:1234"),
            &"x".repeat(BODY_LIMIT + 1),
        );
        let response = app.clone().oneshot(too_large).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let stream = futures_util::stream::once(async {
            Err::<Bytes, std::io::Error>(std::io::Error::other("stream failed"))
        });
        let failed_stream = HttpRequest::builder()
            .method("POST")
            .uri("/liquidity-write/transfers/success/widget/reconcile")
            .header("content-type", "application/json")
            .header("x-request-id", "77777777-7777-7777-7777-777777777777")
            .header("x-test-principal", "accounts.google.com:1234")
            .body(Body::from_stream(stream))
            .unwrap();
        let response = app.oneshot(failed_stream).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({ "error": "failed to read request body" })
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn handler_task_failure_returns_and_audits_one_command_failure() {
        let app = with_audit_layers(
            Router::new().route("/liquidity-write/panic", post(panicking_command)),
        );
        let request_id = "88888888-8888-8888-8888-888888888888";
        let response = app
            .oneshot(request(
                "/liquidity-write/panic",
                request_id,
                Some("accounts.google.com:1234"),
                "{}",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response.headers().get("x-request-id").unwrap(), request_id);
        logs_assert(|lines| {
            let count = lines
                .iter()
                .filter(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(request_id)
                        && line.contains("outcome=\"command_failure\"")
                })
                .count();
            if count == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected one command-failure audit, found {count}: {lines:?}"
                ))
            }
        });
    }

    #[traced_test]
    #[tokio::test]
    async fn dropping_http_waiter_does_not_cancel_audit_completion() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler_started = Arc::clone(&started);
        let handler_release = Arc::clone(&release);
        let app = with_audit_layers(Router::new().route(
            "/liquidity-write/transfers/resume",
            post(move || {
                let started = Arc::clone(&handler_started);
                let release = Arc::clone(&handler_release);
                async move {
                    started.notify_one();
                    release.notified().await;
                    StatusCode::OK
                }
            }),
        ));
        let request_id = "99999999-9999-9999-9999-999999999999";
        let completion = Arc::new(Notify::new());
        let mut audited_request = request(
            "/liquidity-write/transfers/resume",
            request_id,
            Some("accounts.google.com:1234"),
            "{}",
        );
        audited_request
            .extensions_mut()
            .insert(AuditCompletionSignal(Arc::clone(&completion)));
        let waiter = tokio::spawn(app.oneshot(audited_request));

        wait_for_signal(&started).await;
        waiter.abort();
        let _cancelled = waiter.await;
        release.notify_one();
        wait_for_signal(&completion).await;

        logs_assert(|lines| {
            let count = lines
                .iter()
                .filter(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(request_id)
                        && line.contains("outcome=\"success\"")
                })
                .count();
            if count == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected one cancellation-safe audit, found {count}: {lines:?}"
                ))
            }
        });
    }

    #[traced_test]
    #[tokio::test]
    async fn cancelled_http_waiter_still_audits_a_panicking_handler_once() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let completion = Arc::new(Notify::new());
        let handler_started = Arc::clone(&started);
        let handler_release = Arc::clone(&release);
        let app = with_audit_layers(Router::new().route(
            "/liquidity-write/panic-after-cancel",
            post(move || {
                wait_then_panic(Arc::clone(&handler_started), Arc::clone(&handler_release))
            }),
        ));
        let request_id = "abababab-abab-abab-abab-abababababab";
        let mut audited_request = request(
            "/liquidity-write/panic-after-cancel",
            request_id,
            Some("accounts.google.com:1234"),
            "{}",
        );
        audited_request
            .extensions_mut()
            .insert(AuditCompletionSignal(Arc::clone(&completion)));
        let waiter = tokio::spawn(app.oneshot(audited_request));

        wait_for_signal(&started).await;
        waiter.abort();
        let _cancelled = waiter.await;
        release.notify_one();
        wait_for_signal(&completion).await;

        logs_assert(|lines| {
            let matching: Vec<_> = lines
                .iter()
                .filter(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(request_id)
                })
                .collect();
            if matching.len() != 1 || !matching[0].contains("outcome=\"command_failure\"") {
                Err(format!(
                    "expected one command-failure audit after cancellation: {lines:?}"
                ))
            } else {
                Ok(())
            }
        });
    }

    #[test]
    fn recorder_checks_the_events_actual_level() {
        let subscriber = Registry::default().with(EnvFilter::new(
            "operations_audit=warn,operational_alert=off",
        ));
        tracing::subscriber::with_default(subscriber, || {
            let Err(AuditRecordError::TargetDisabled { level }) =
                LogOperationsAuditRecorder.record(&event(AuditOutcome::Success))
            else {
                panic!("INFO success must be rejected by a WARN-only audit target");
            };
            assert_eq!(level, tracing::Level::INFO);

            LogOperationsAuditRecorder
                .record(&event(AuditOutcome::Denied))
                .unwrap();
        });
    }

    #[traced_test]
    #[test]
    fn recorder_failure_is_visible_and_cannot_replace_the_command_result() {
        record_with(&FailingRecorder, &event(AuditOutcome::Success));

        logs_assert(|lines| {
            lines
                .iter()
                .any(|line| {
                    line.contains("operational_alert: Operations audit recording failed")
                        && line.contains("principal=accounts.google.com:1234")
                        && line.contains("target_id=mint:mint-9")
                        && line.contains("request_id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                        && line.contains("outcome=\"success\"")
                })
                .then_some(())
                .ok_or_else(|| format!("missing audit recording failure: {lines:?}"))
        });
    }
}
