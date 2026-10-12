//! Shared structured audit records for mutation-capable operator routes.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use axum::Json;
use axum::extract::{FromRequest, FromRequestParts, MatchedPath, Query, Request};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures_util::FutureExt;
use parking_lot::Mutex;
use percent_encoding::percent_decode_str;
use serde::de::DeserializeOwned;
use tokio_util::task::TaskTracker;
use tracing::{Level, error, info, warn};
use uuid::Uuid;

use crate::alerts::AlertKind;
use crate::api::spawn_detached;

const AUDIT_SCHEMA: &str = "st0x.operations.audit.v1";
const SERVICE: &str = "liquidity";
const REQUEST_ID_HEADER: &str = "x-request-id";
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
    mutable: Mutex<AuditMutable>,
    #[cfg(test)]
    completion: Option<AuditCompletionSignal>,
}

/// What authentication and extraction learn after the context is created,
/// behind one lock so no accessor depends on a lock order.
#[derive(Debug)]
struct AuditMutable {
    principal: String,
    targets: Vec<String>,
    reason: String,
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
                mutable: Mutex::new(AuditMutable {
                    principal: principal.to_string(),
                    targets: Vec::new(),
                    reason: NOT_PROVIDED.to_string(),
                }),
                #[cfg(test)]
                completion: request.extensions().get::<AuditCompletionSignal>().cloned(),
            }),
        }
    }

    fn set_principal(&self, principal: impl Into<String>) {
        self.inner.mutable.lock().principal = principal.into();
    }

    fn capture_path_targets(&self) -> Result<(), AuditValueError> {
        let targets = dynamic_path_values(&self.inner.route, &self.inner.path)?;
        for target in &targets {
            validate_audit_value("path parameter", target, TARGET_VALUE_LIMIT)?;
        }
        self.inner.mutable.lock().targets = targets;
        Ok(())
    }

    /// Records the identity a parsed request declares, after the path
    /// targets. Every value is checked before any is recorded, so a refused
    /// request leaves only its path targets.
    fn record_request_identity(
        &self,
        request: &impl AuditedRequest,
    ) -> Result<(), AuditValueError> {
        let targets = request.audit_targets();
        for target in &targets {
            validate_audit_value("target", target, TARGET_VALUE_LIMIT)?;
        }
        let reason = request.audit_reason();
        if let Some(reason) = reason {
            validate_audit_value("reason", reason, REASON_LIMIT)?;
        }

        let mut mutable = self.inner.mutable.lock();
        for target in targets {
            if !mutable.targets.contains(&target) {
                mutable.targets.push(target);
            }
        }
        if let Some(reason) = reason {
            mutable.reason = reason.to_string();
        }
        drop(mutable);
        Ok(())
    }

    fn event(&self, status: StatusCode) -> OperationsAuditEvent {
        let mutable = self.inner.mutable.lock();
        OperationsAuditEvent {
            principal: mutable.principal.clone(),
            role: self.inner.role,
            route: self.inner.route.clone(),
            request_id: self.inner.request_id,
            target_id: if mutable.targets.is_empty() {
                NOT_IDENTIFIED.to_string()
            } else {
                mutable.targets.join(":")
            },
            reason: mutable.reason.clone(),
            outcome: AuditOutcome::from_status(status),
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

        // Every caller-influenced value is recorded as a string value, so the
        // text formatter quotes and escapes it and a newline in a decoded path
        // segment or reason cannot start a forged record line.
        match event.outcome {
            AuditOutcome::Success => info!(
                target: "operations_audit",
                audit_schema = AUDIT_SCHEMA,
                service = SERVICE,
                principal = event.principal.as_str(),
                role = event.role,
                route = event.route.as_str(),
                request_id = %event.request_id,
                target_id = event.target_id.as_str(),
                reason = event.reason.as_str(),
                outcome = event.outcome.as_str(),
                "Operations audit event"
            ),
            AuditOutcome::Denied
            | AuditOutcome::ValidationFailure
            | AuditOutcome::CommandFailure => warn!(
                target: "operations_audit",
                audit_schema = AUDIT_SCHEMA,
                service = SERVICE,
                principal = event.principal.as_str(),
                role = event.role,
                route = event.route.as_str(),
                request_id = %event.request_id,
                target_id = event.target_id.as_str(),
                reason = event.reason.as_str(),
                outcome = event.outcome.as_str(),
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
            kind = AlertKind::OperationsAuditRecordingFailed.as_str(),
            audit_schema = AUDIT_SCHEMA,
            service = SERVICE,
            principal = event.principal.as_str(),
            role = event.role,
            route = event.route.as_str(),
            request_id = %event.request_id,
            target_id = event.target_id.as_str(),
            reason = event.reason.as_str(),
            outcome = event.outcome.as_str(),
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
/// response produces one audit record. The command and its final audit event
/// run as one task on `detached_tasks`: a disconnected HTTP waiter cannot cancel
/// it, and the shutdown drain waits for it. A request that arrives after the
/// drain closed the tracker is refused with 503 and audited as a command
/// failure.
pub(crate) async fn audit_request(
    detached_tasks: &TaskTracker,
    role: &'static str,
    mut request: Request,
    next: Next,
) -> Response {
    let context = OperationsAuditContext::new(&request, role, UNAUTHENTICATED);
    request.extensions_mut().insert(context.clone());

    let task_context = context.clone();
    let spawned = spawn_detached(
        detached_tasks,
        "audited operator command",
        context.inner.request_id,
        async move {
            let response =
                if let Ok(response) = AssertUnwindSafe(next.run(request)).catch_unwind().await {
                    response
                } else {
                    error!(
                        target: "operational_alert",
                        alert = true,
                        kind = AlertKind::AuditedOperatorCommandPanicked.as_str(),
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
        },
    );
    let completion = match spawned {
        Ok(completion) => completion,
        Err(refusal) => return finalize_response(&context, refusal.into_response()),
    };

    // The join error's text can carry a panic payload, so the caller gets the
    // same generic body as a handler panic.
    if let Ok(response) = completion.await {
        response
    } else {
        error!(
            target: "operational_alert",
            alert = true,
            kind = AlertKind::AuditedOperatorCommandTaskFailed.as_str(),
            audit_schema = AUDIT_SCHEMA,
            service = SERVICE,
            role,
            request_id = %context.inner.request_id,
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

/// Captures the route's path values as audit targets once the caller has
/// passed its authentication gate. Identifiers in the body or query are
/// recorded by [`Audited`] and [`AuditedQuery`] after they parse.
pub(crate) async fn capture_audit_path_targets(request: Request, next: Next) -> Response {
    if let Some(context) = request.extensions().get::<OperationsAuditContext>()
        && let Err(error) = context.capture_path_targets()
    {
        return audit_value_error_response(&error);
    }

    next.run(request).await
}

/// Entity identifiers and reason a mutation's typed request carries. Read
/// only after the request parsed, so a field the command does not declare
/// never reaches the audit record.
pub(crate) trait AuditedRequest {
    /// Identifiers the request names beyond the route's path parameters.
    fn audit_targets(&self) -> Vec<String> {
        Vec::new()
    }

    /// The operator's stated reason, when the command takes one.
    fn audit_reason(&self) -> Option<&str> {
        None
    }
}

/// JSON extractor for audited mutation routes. Parses exactly like [`Json`]
/// (same limit, same rejections), then records the body's
/// [`AuditedRequest`] identity for the request's audit event. An identifier
/// or reason over its size limit is refused with 400 before the handler runs.
pub(crate) struct Audited<T>(pub(crate) T);

impl<T, State> FromRequest<State> for Audited<T>
where
    T: DeserializeOwned + AuditedRequest,
    State: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &State) -> Result<Self, Self::Rejection> {
        let context = request
            .extensions()
            .get::<OperationsAuditContext>()
            .cloned();
        let Json(body) = Json::<T>::from_request(request, state)
            .await
            .map_err(IntoResponse::into_response)?;
        if let Some(context) = context {
            context
                .record_request_identity(&body)
                .map_err(|error| audit_value_error_response(&error))?;
        }
        Ok(Self(body))
    }
}

/// [`Audited`] for a query string: parses exactly like [`Query`], then
/// records the query's [`AuditedRequest`] identity.
pub(crate) struct AuditedQuery<T>(pub(crate) T);

impl<T, State> FromRequestParts<State> for AuditedQuery<T>
where
    T: DeserializeOwned + AuditedRequest,
    State: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &State) -> Result<Self, Self::Rejection> {
        let Query(query) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        if let Some(context) = parts.extensions.get::<OperationsAuditContext>() {
            context
                .record_request_identity(&query)
                .map_err(|error| audit_value_error_response(&error))?;
        }
        Ok(Self(query))
    }
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

#[derive(Debug, PartialEq, Eq)]
enum AuditValueError {
    TooLong {
        field: &'static str,
        max_bytes: usize,
    },
    InvalidPathUtf8,
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

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::extract::{Path, Request};
    use axum::http::{Request as HttpRequest, StatusCode};
    use axum::middleware::Next;
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use serde::Deserialize;
    use serde_json::Value;
    use tokio::sync::Notify;
    use tokio_util::task::TaskTracker;
    use tower::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{EnvFilter, Registry};
    use tracing_test::traced_test;
    use uuid::uuid;

    use super::{
        AuditCompletionSignal, AuditOutcome, AuditRecordError, AuditValueError, Audited,
        AuditedQuery, AuditedRequest, LogOperationsAuditRecorder, OperationsAuditEvent,
        OperationsAuditRecorder, REASON_LIMIT, TARGET_VALUE_LIMIT, audit_request,
        capture_audit_path_targets, dynamic_path_values, record_principal, record_with,
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
                        && line.contains("principal=\"accounts.google.com:1234\"")
                        && line.contains("role=\"write\"")
                        && line.contains("request_id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                        && line.contains("target_id=\"mint:mint-9\"")
                        && line.contains("reason=\"incident 42\"")
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

    /// A typed reconcile body: `supersedingTx` is its identifier and `reason`
    /// its audit reason. Serde ignores every other field.
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ReconcileCommand {
        superseding_tx: Option<String>,
        reason: Option<String>,
    }

    impl AuditedRequest for ReconcileCommand {
        fn audit_targets(&self) -> Vec<String> {
            self.superseding_tx.iter().cloned().collect()
        }

        fn audit_reason(&self) -> Option<&str> {
            self.reason.as_deref()
        }
    }

    /// A typed recheck query: the operator asserts `deposit_tx`.
    #[derive(Deserialize)]
    struct RecheckProbe {
        deposit_tx: Option<String>,
    }

    impl AuditedRequest for RecheckProbe {
        fn audit_targets(&self) -> Vec<String> {
            self.deposit_tx.iter().cloned().collect()
        }
    }

    async fn command(
        Path((kind, _id)): Path<(String, String)>,
        Audited(_body): Audited<ReconcileCommand>,
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

    /// Dropping this panic payload panics again, outside the command's unwind
    /// guard, so the audited task itself fails to join.
    struct PanicsWhenDropped;

    impl Drop for PanicsWhenDropped {
        fn drop(&mut self) {
            panic!("secret panic payload");
        }
    }

    async fn panic_escaping_the_unwind_guard() -> StatusCode {
        std::panic::panic_any(PanicsWhenDropped);
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
        with_tracked_audit_layers(router, TaskTracker::new())
    }

    fn with_tracked_audit_layers(router: Router, detached_tasks: TaskTracker) -> Router {
        router
            .layer(axum::middleware::from_fn(capture_audit_path_targets))
            .layer(axum::middleware::from_fn(test_authentication))
            .layer(axum::middleware::from_fn(move |request, next| {
                let detached_tasks = detached_tasks.clone();
                async move { audit_request(&detached_tasks, "write", request, next).await }
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
                        && line.contains(&format!("principal=\"{principal}\""))
                        && line.contains(&format!("request_id={request_id}"))
                        && line.contains(&format!("target_id=\"{target_id}\""))
                        && line.contains(&format!("reason=\"{reason}\""))
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

    /// Path targets are percent-decoded and reasons are free text, so an
    /// encoded or literal newline must stay inside its quoted value rather
    /// than start a forged record line in text output.
    #[traced_test]
    #[tokio::test]
    async fn newlines_in_path_targets_and_reasons_cannot_forge_a_record() {
        let response = audited_router()
            .oneshot(request(
                "/liquidity-write/transfers/success/x%0Aoperations_audit:%20forged/reconcile",
                "55555555-5555-5555-5555-555555555555",
                Some("accounts.google.com:1234"),
                r#"{"reason":"ok\noperations_audit: forged"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        logs_assert(|lines| {
            if let Some(forged) = lines
                .iter()
                .find(|line| line.starts_with("operations_audit:"))
            {
                return Err(format!("forged record line: {forged}"));
            }
            lines
                .iter()
                .any(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains("55555555-5555-5555-5555-555555555555")
                        && line.contains(r#"target_id="success:x\noperations_audit: forged""#)
                        && line.contains(r#"reason="ok\noperations_audit: forged""#)
                })
                .then_some(())
                .ok_or_else(|| format!("missing escaped audit event: {lines:?}"))
        });
    }

    #[traced_test]
    #[tokio::test]
    async fn oversized_identifier_or_reason_is_refused_before_the_handler() {
        let called = Arc::new(AtomicBool::new(false));
        let handler_called = Arc::clone(&called);
        let app = with_audit_layers(Router::new().route(
            "/liquidity-write/transfers/{kind}/{id}/reconcile",
            post(move |Audited(_body): Audited<ReconcileCommand>| {
                let called = Arc::clone(&handler_called);
                async move {
                    called.store(true, Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        ));
        let cases = [
            (
                "55555555-5555-5555-5555-555555555555",
                serde_json::json!({
                    "supersedingTx": "x".repeat(TARGET_VALUE_LIMIT + 1),
                    "reason": "incident 42"
                }),
                format!("audit field `target` exceeds {TARGET_VALUE_LIMIT} bytes"),
            ),
            (
                "56565656-5656-5656-5656-565656565656",
                serde_json::json!({
                    "supersedingTx": "op-1",
                    "reason": "r".repeat(REASON_LIMIT + 1)
                }),
                format!("audit field `reason` exceeds {REASON_LIMIT} bytes"),
            ),
        ];

        for (request_id, body, expected_error) in &cases {
            let response = app
                .clone()
                .oneshot(request(
                    "/liquidity-write/transfers/mint/mint-9/reconcile",
                    request_id,
                    Some("accounts.google.com:1234"),
                    &body.to_string(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{request_id}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body, serde_json::json!({ "error": expected_error }));
        }
        assert!(!called.load(Ordering::SeqCst), "the handler must not run");

        logs_assert(|lines| {
            for (request_id, _, _) in &cases {
                let recorded = lines.iter().any(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(request_id)
                        && line.contains("target_id=\"mint:mint-9\"")
                        && line.contains("reason=\"not_provided\"")
                        && line.contains("outcome=\"validation_failure\"")
                });
                if !recorded {
                    return Err(format!(
                        "refused request {request_id} must record only its path: {lines:?}"
                    ));
                }
            }
            Ok(())
        });
    }

    /// Only the identity the typed request declares reaches the record: a
    /// snake-case alias, an undeclared field, and a nested copy of a declared
    /// field are parsed past and never recorded.
    #[traced_test]
    #[tokio::test]
    async fn undeclared_aliased_and_nested_fields_never_reach_the_record() {
        let request_id = "13131313-1313-1313-1313-131313131313";
        let body = serde_json::json!({
            "reason": "incident 42",
            "superseding_tx": "forged-alias",
            "auditReason": "forged-alias",
            "orderId": "forged-undeclared",
            "nested": {
                "reason": "forged-nested",
                "supersedingTx": "forged-nested"
            }
        })
        .to_string();

        let response = audited_router()
            .oneshot(request(
                "/liquidity-write/transfers/success/widget-1/reconcile",
                request_id,
                Some("accounts.google.com:1234"),
                &body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        logs_assert(|lines| {
            let records: Vec<_> = lines
                .iter()
                .filter(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(request_id)
                })
                .collect();
            match records.as_slice() {
                [record]
                    if record.contains("target_id=\"success:widget-1\"")
                        && record.contains("reason=\"incident 42\"")
                        && !record.contains("forged") =>
                {
                    Ok(())
                }
                _ => Err(format!(
                    "expected one record of the declared identity: {lines:?}"
                )),
            }
        });
    }

    #[traced_test]
    #[tokio::test]
    async fn query_identifier_is_recorded_as_a_target() {
        let called = Arc::new(AtomicBool::new(false));
        let handler_called = Arc::clone(&called);
        let app = with_audit_layers(Router::new().route(
            "/liquidity-write/transfers/recheck/{kind}/{id}",
            post(move |AuditedQuery(_query): AuditedQuery<RecheckProbe>| {
                let called = Arc::clone(&handler_called);
                async move {
                    called.store(true, Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        ));

        let oversized = app
            .clone()
            .oneshot(request(
                &format!(
                    "/liquidity-write/transfers/recheck/usdc_deposit/transfer-1?deposit_tx={}",
                    "d".repeat(TARGET_VALUE_LIMIT + 1)
                ),
                "14141414-1414-1414-1414-141414141414",
                Some("accounts.google.com:1234"),
                "",
            ))
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::BAD_REQUEST);
        assert!(!called.load(Ordering::SeqCst), "the handler must not run");

        let request_id = "15151515-1515-1515-1515-151515151515";
        let response = app
            .oneshot(request(
                "/liquidity-write/transfers/recheck/usdc_deposit/transfer-1?deposit_tx=0xabc",
                request_id,
                Some("accounts.google.com:1234"),
                "",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        logs_assert(|lines| {
            lines
                .iter()
                .any(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(request_id)
                        && line.contains("target_id=\"usdc_deposit:transfer-1:0xabc\"")
                        && line.contains("outcome=\"success\"")
                })
                .then_some(())
                .ok_or_else(|| format!("missing query target in the audit: {lines:?}"))
        });
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

    /// A join error's text carries the task's panic payload, so a failed join
    /// answers with the same generic body as a handler panic.
    #[traced_test]
    #[tokio::test]
    async fn task_join_failure_answers_with_the_generic_body() {
        let app = with_audit_layers(Router::new().route(
            "/liquidity-write/escaping-panic",
            post(panic_escaping_the_unwind_guard),
        ));
        let request_id = "16161616-1616-1616-1616-161616161616";
        let response = app
            .oneshot(request(
                "/liquidity-write/escaping-panic",
                request_id,
                Some("accounts.google.com:1234"),
                "{}",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response.headers().get("x-request-id").unwrap(), request_id);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({ "error": "operator command failed" })
        );

        logs_assert(|lines| {
            let alerted = lines.iter().any(|line| {
                line.contains("operational_alert: Audited operator command task failed")
                    && line.contains(request_id)
            });
            let audits = lines
                .iter()
                .filter(|line| {
                    line.contains("operations_audit: Operations audit event")
                        && line.contains(request_id)
                        && line.contains("outcome=\"command_failure\"")
                })
                .count();
            if alerted && audits == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected the task-failure alert and one command-failure audit: {lines:?}"
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

    /// The shutdown drain waits for an audited command that is in flight, so
    /// the runtime cannot exit between the command finishing and its audit
    /// event. A request that arrives after the drain closed the tracker is
    /// refused with 503 and still audited.
    #[traced_test]
    #[tokio::test]
    async fn shutdown_drain_waits_for_the_audit_and_refuses_later_requests() {
        let detached_tasks = TaskTracker::new();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler_started = Arc::clone(&started);
        let handler_release = Arc::clone(&release);
        let app = with_tracked_audit_layers(
            Router::new().route(
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
            ),
            detached_tasks.clone(),
        );
        let in_flight_id = "cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd";
        let in_flight = tokio::spawn(app.clone().oneshot(request(
            "/liquidity-write/transfers/resume",
            in_flight_id,
            Some("accounts.google.com:1234"),
            "{}",
        )));
        wait_for_signal(&started).await;

        detached_tasks.close();
        let drain = tokio::spawn({
            let detached_tasks = detached_tasks.clone();
            async move { detached_tasks.wait().await }
        });
        tokio::task::yield_now().await;
        assert!(
            !drain.is_finished(),
            "the drain must wait for the audited command"
        );

        let refused_id = "efefefef-efef-efef-efef-efefefefefef";
        let refused = app
            .oneshot(request(
                "/liquidity-write/transfers/resume",
                refused_id,
                Some("accounts.google.com:1234"),
                "{}",
            ))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(refused.headers().get("x-request-id").unwrap(), refused_id);

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .expect("the drain must finish once the command and its audit are done")
            .unwrap();
        assert!(logs_contain(&format!("request_id={in_flight_id}")));
        assert_eq!(in_flight.await.unwrap().unwrap().status(), StatusCode::OK);

        logs_assert(|lines| {
            for (request_id, outcome) in
                [(in_flight_id, "success"), (refused_id, "command_failure")]
            {
                let count = lines
                    .iter()
                    .filter(|line| {
                        line.contains("operations_audit: Operations audit event")
                            && line.contains(request_id)
                            && line.contains(&format!("outcome=\"{outcome}\""))
                    })
                    .count();
                if count != 1 {
                    return Err(format!(
                        "expected one {outcome} audit for {request_id}, found {count}: {lines:?}"
                    ));
                }
            }
            Ok(())
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
                        && line.contains("principal=\"accounts.google.com:1234\"")
                        && line.contains("target_id=\"mint:mint-9\"")
                        && line.contains("request_id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                        && line.contains("outcome=\"success\"")
                })
                .then_some(())
                .ok_or_else(|| format!("missing audit recording failure: {lines:?}"))
        });
    }
}
