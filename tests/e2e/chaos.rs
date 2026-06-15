//! HTTP-level chaos proxies for the e2e harness.
//!
//! [`ChaosProxy`] sits between the bot's `rpc_url` and the real Anvil
//! node. Forwards JSON-RPC traffic by default; when configured, perturbs
//! matched methods: empty `eth_getLogs` results model load-balancer
//! inconsistency where one node in the upstream pool is behind on
//! indexing, and delayed responses model upstream latency.
//!
//! The bot ingests fills over a single HTTP transport (continuous
//! `eth_getLogs` polling), so each JSON-RPC request and its response are
//! one HTTP round-trip -- the request already carries its `method`, so
//! perturbation is decided per request without tracking ids across
//! frames.
//!
//! [`LatencyProxy`] is a plain HTTP pass-through for non-RPC upstreams
//! (the Alpaca broker mock). It forwards every request immediately --
//! so the upstream processes it -- and, when armed, holds the response
//! past the caller's timeout. This models "the broker executed the
//! request but the acknowledgement was slow", which the caller cannot
//! distinguish from the request never arriving. The delay lives in a
//! proxy rather than the broker mock because `httpmock` dynamic
//! responders are synchronous: sleeping inside one blocks the mock
//! server's runtime and stalls unrelated endpoints.
//!
//! Used by the chaos test suite to validate the bot's recovery paths
//! against adversarial external behaviour without modifying the
//! production code paths. Additional behaviours (drop, error, stall)
//! land alongside the chaos sub-issues that need them.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::warn;
use url::Url;

/// Active behaviour the chaos proxy applies to JSON-RPC requests
/// matching [`ChaosConfig::method`].
#[derive(Debug, Clone)]
pub(crate) enum ChaosBehaviour {
    /// Reply with `{"result": []}` regardless of what the upstream would
    /// have returned. Models load-balancer inconsistency where one node
    /// in the pool is behind and answers `eth_getLogs` with no events.
    EmptyResult,
    /// Forward the request upstream immediately, then hold the response
    /// for `duration` before relaying it. Models a slow upstream node:
    /// the request is processed on time, only the answer is late.
    Delay { duration: Duration },
}

/// Knob describing which method to perturb and for how many calls.
///
/// For `EmptyResult` chaos on `eth_getLogs`, each perturbed `getLogs`
/// response is paired with a stale `eth_blockNumber` response so the
/// bot's read-after-write tip check actually triggers. The pairing is
/// tracked via `pending_stale_tips`: incremented on each perturbed
/// `getLogs`, decremented on each consumed stale `blockNumber`.
#[derive(Debug, Clone)]
pub(crate) struct ChaosConfig {
    pub method: String,
    pub behaviour: ChaosBehaviour,
    pub remaining: usize,
    pub pending_stale_tips: usize,
}

type SharedConfig = Arc<Mutex<Option<ChaosConfig>>>;

/// Axum handler state: the mutable chaos config plus what's needed to
/// forward un-perturbed requests to the real Anvil node.
#[derive(Clone)]
struct ProxyState {
    config: SharedConfig,
    upstream: Url,
    client: Client,
}

/// Spawned proxy. Holds the listening endpoint and a handle the test can
/// use to mutate the active [`ChaosConfig`] mid-run.
#[derive(Debug)]
pub(crate) struct ChaosProxy {
    /// `http://127.0.0.1:<port>` the bot should connect to instead of the
    /// real Anvil endpoint.
    pub endpoint: Url,
    /// Shared mutable config; `None` means transparent forwarding.
    config: SharedConfig,
    /// Serve loop's handle. Aborted on drop so the proxy dies with its
    /// parent test.
    server_task: tokio::task::AbortHandle,
}

impl ChaosProxy {
    /// Spawn a chaos proxy in front of `upstream` (the real Anvil HTTP
    /// endpoint) and start listening on a free local port. The proxy
    /// keeps running until the returned handle is dropped.
    pub(crate) async fn start(upstream: Url) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let endpoint: Url = format!("http://127.0.0.1:{port}").parse()?;

        let config: SharedConfig = Arc::new(Mutex::new(None));
        let state = ProxyState {
            config: config.clone(),
            upstream,
            client: Client::new(),
        };

        let app = Router::new().fallback(handle_rpc).with_state(state);
        let handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                warn!(?error, "chaos proxy server stopped");
            }
        });

        Ok(Self {
            endpoint,
            config,
            server_task: handle.abort_handle(),
        })
    }

    /// Convenience: reply to the next `count` `eth_getLogs` calls with
    /// an empty `result`, modelling a load-balancer node that is
    /// behind on indexing. Each perturbed response is paired with a
    /// stale `eth_blockNumber` reply so the bot's read-after-write tip
    /// check triggers and retries the request.
    pub(crate) async fn empty_get_logs(&self, count: usize) {
        *self.config.lock().await = Some(ChaosConfig {
            method: "eth_getLogs".to_owned(),
            behaviour: ChaosBehaviour::EmptyResult,
            remaining: count,
            pending_stale_tips: 0,
        });
    }

    /// Convenience: hold the responses of the next `count` `eth_getLogs`
    /// calls for `duration` each, modelling a slow upstream node. The
    /// requests still reach the real node on time -- only the answers
    /// are late.
    pub(crate) async fn delay_get_logs(&self, duration: Duration, count: usize) {
        *self.config.lock().await = Some(ChaosConfig {
            method: "eth_getLogs".to_owned(),
            behaviour: ChaosBehaviour::Delay { duration },
            remaining: count,
            pending_stale_tips: 0,
        });
    }

    /// Convenience: hold the responses of the next `count`
    /// `eth_getTransactionReceipt` calls for `duration` each. The receipt
    /// fetch is the first RPC call the trade-accounting job makes, so a
    /// held receipt pins that job mid-`perform` -- used by crash tests to
    /// widen the in-flight window they kill the bot inside.
    pub(crate) async fn delay_transaction_receipts(&self, duration: Duration, count: usize) {
        *self.config.lock().await = Some(ChaosConfig {
            method: "eth_getTransactionReceipt".to_owned(),
            behaviour: ChaosBehaviour::Delay { duration },
            remaining: count,
            pending_stale_tips: 0,
        });
    }
}

impl Drop for ChaosProxy {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

/// Knob describing which plain-HTTP requests to delay and for how many.
#[derive(Debug, Clone)]
struct LatencyConfig {
    method: reqwest::Method,
    path_suffix: String,
    delay: Duration,
    remaining: usize,
}

type SharedLatencyConfig = Arc<Mutex<Option<LatencyConfig>>>;

/// Axum handler state for [`LatencyProxy`]: the mutable latency config
/// plus what's needed to forward requests to the real upstream.
#[derive(Clone)]
struct LatencyProxyState {
    config: SharedLatencyConfig,
    upstream: Url,
    client: Client,
}

/// Plain-HTTP latency-injecting pass-through proxy.
///
/// Forwards every request to the upstream immediately -- so the upstream
/// processes it on time -- and, when armed, holds the matched responses
/// for the configured duration before relaying them. From the caller's
/// side a sufficiently delayed response is a timeout on a request the
/// upstream actually executed.
#[derive(Debug)]
pub(crate) struct LatencyProxy {
    /// `http://127.0.0.1:<port>` the bot should connect to instead of
    /// the real upstream endpoint.
    pub endpoint: Url,
    /// Shared mutable config; `None` means transparent forwarding.
    config: SharedLatencyConfig,
    /// Serve loop's handle. Aborted on drop so the proxy dies with its
    /// parent test.
    server_task: tokio::task::AbortHandle,
}

impl LatencyProxy {
    /// Spawn a latency proxy in front of `upstream` and start listening
    /// on a free local port. The proxy keeps running until the returned
    /// handle is dropped.
    pub(crate) async fn start(upstream: Url) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let endpoint: Url = format!("http://127.0.0.1:{port}").parse()?;

        let config: SharedLatencyConfig = Arc::new(Mutex::new(None));
        let state = LatencyProxyState {
            config: config.clone(),
            upstream,
            // Bound the forward leg so an unresponsive upstream cannot hang the
            // proxy task indefinitely; the injected latency is a separate sleep,
            // not this request, so 10s is ample headroom for the real forward.
            client: Client::builder().timeout(Duration::from_secs(10)).build()?,
        };

        let app = Router::new().fallback(handle_http).with_state(state);
        let handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                warn!(?error, "latency proxy server stopped");
            }
        });

        Ok(Self {
            endpoint,
            config,
            server_task: handle.abort_handle(),
        })
    }

    /// Convenience: hold the responses of the next `count` broker order
    /// placement calls (`POST .../orders`) for `duration` each. The
    /// placements still reach the broker on time -- only the
    /// acknowledgements are late.
    pub(crate) async fn delay_order_placements(&self, duration: Duration, count: usize) {
        *self.config.lock().await = Some(LatencyConfig {
            method: reqwest::Method::POST,
            path_suffix: "/orders".to_owned(),
            delay: duration,
            remaining: count,
        });
    }
}

impl Drop for LatencyProxy {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

/// Catch-all pass-through handler: forwards the request verbatim to the
/// upstream, then relays the response -- after the armed delay when the
/// request matches the active [`LatencyConfig`].
async fn handle_http(
    State(state): State<LatencyProxyState>,
    request: axum::extract::Request,
) -> Response {
    let (parts, body) = request.into_parts();

    let Ok(body_bytes) = axum::body::to_bytes(body, usize::MAX).await else {
        warn!("latency proxy failed to read request body");
        return (
            axum::http::StatusCode::BAD_GATEWAY,
            "latency proxy request body error",
        )
            .into_response();
    };

    let mut upstream_url = state.upstream.clone();
    upstream_url.set_path(parts.uri.path());
    upstream_url.set_query(parts.uri.query());

    let mut upstream_request = state
        .client
        .request(parts.method.clone(), upstream_url)
        .body(body_bytes);
    for (name, value) in &parts.headers {
        if name == reqwest::header::HOST {
            continue;
        }
        upstream_request = upstream_request.header(name, value);
    }

    let upstream_response = match upstream_request.send().await {
        Ok(response) => response,
        Err(error) => {
            warn!(?error, "latency proxy failed to forward request");
            return (
                axum::http::StatusCode::BAD_GATEWAY,
                "latency proxy upstream error",
            )
                .into_response();
        }
    };

    let status = upstream_response.status();
    let headers = upstream_response.headers().clone();
    let bytes = match upstream_response.bytes().await {
        Ok(response_bytes) => response_bytes,
        Err(error) => {
            warn!(?error, "latency proxy failed to read response body");
            return (
                axum::http::StatusCode::BAD_GATEWAY,
                "latency proxy response body error",
            )
                .into_response();
        }
    };

    let delay = matched_delay(&state, &parts.method, parts.uri.path()).await;

    if let Some(duration) = delay {
        tokio::time::sleep(duration).await;
    }

    let mut response = (status, bytes).into_response();
    for (name, value) in &headers {
        // reqwest already decoded the body, so framing and content-encoding
        // headers no longer describe the bytes being relayed -- forwarding
        // Content-Encoding would make the downstream client re-decode plain
        // bytes. Drop them; axum sets the correct framing for the new body.
        if name == reqwest::header::TRANSFER_ENCODING
            || name == reqwest::header::CONTENT_LENGTH
            || name == reqwest::header::CONTENT_ENCODING
        {
            continue;
        }
        response.headers_mut().insert(name.clone(), value.clone());
    }

    response
}

/// Consumes one unit of the armed latency budget if this request matches
/// the active [`LatencyConfig`], returning the delay to apply. Call only
/// after the upstream response has been received successfully.
async fn matched_delay(
    state: &LatencyProxyState,
    method: &reqwest::Method,
    path: &str,
) -> Option<Duration> {
    let mut guard = state.config.lock().await;

    let result = match guard.as_mut() {
        Some(cfg)
            if cfg.remaining > 0 && cfg.method == *method && path.ends_with(&cfg.path_suffix) =>
        {
            cfg.remaining -= 1;
            Some(cfg.delay)
        }
        Some(_) | None => None,
    };

    drop(guard);
    result
}

/// Single JSON-RPC POST handler. Anvil requests are single objects (one
/// method per HTTP call); batched arrays are handled element-wise so each
/// sub-request is perturbed or forwarded independently.
async fn handle_rpc(State(state): State<ProxyState>, body: Bytes) -> Response {
    let Ok(parsed) = serde_json::from_slice::<Value>(&body) else {
        // Not JSON we understand -- forward the raw bytes verbatim.
        return forward_raw(&state, body).await;
    };

    let response = match parsed {
        Value::Array(requests) => {
            let mut responses = Vec::with_capacity(requests.len());
            for request in requests {
                responses.push(process_one(&state, request).await);
            }
            Value::Array(responses)
        }
        single => process_one(&state, single).await,
    };

    json_response(&response)
}

/// Resolved perturbation for one JSON-RPC request.
enum Perturbation {
    /// Reply with this synthesized response instead of forwarding.
    Synthesize(Value),
    /// Forward upstream, then hold the response for this long.
    DelayResponse(Duration),
}

/// Decide a single JSON-RPC request: synthesize or delay a perturbed
/// response when the active chaos config matches, otherwise forward it
/// upstream untouched.
async fn process_one(state: &ProxyState, request: Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let perturbation = match method {
        Some(method) => perturb(state, &method, &id).await,
        None => None,
    };

    match perturbation {
        Some(Perturbation::Synthesize(response)) => response,
        Some(Perturbation::DelayResponse(duration)) => {
            let response = forward_or_rpc_error(state, &request, id).await;
            tokio::time::sleep(duration).await;
            response
        }
        None => forward_or_rpc_error(state, &request, id).await,
    }
}

/// Forwards one request upstream, shaping any transport failure into a
/// JSON-RPC error response carrying the request's `id`.
async fn forward_or_rpc_error(state: &ProxyState, request: &Value, id: Value) -> Value {
    forward_one(state, request).await.unwrap_or_else(|error| {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32603, "message": error.to_string() },
        })
    })
}

/// Returns the perturbation the active chaos config applies to this
/// method, or `None` to forward untouched.
async fn perturb(state: &ProxyState, method: &str, id: &Value) -> Option<Perturbation> {
    let mut guard = state.config.lock().await;

    let result = match guard.as_mut() {
        None => None,
        Some(cfg) => {
            // Drain the paired stale-tip queue first: each perturbed
            // `getLogs` owes one stale `eth_blockNumber` so the bot's
            // read-after-write tip check fires. Draining before the budget
            // check keeps the two counters from desyncing when `getLogs`
            // budget exhausts mid-batch while a paired tip is still owed.
            let is_paired_tip = method == "eth_blockNumber"
                && cfg.method == "eth_getLogs"
                && matches!(cfg.behaviour, ChaosBehaviour::EmptyResult)
                && cfg.pending_stale_tips > 0;

            if is_paired_tip {
                cfg.pending_stale_tips -= 1;
                Some(Perturbation::Synthesize(
                    json!({ "jsonrpc": "2.0", "id": id, "result": "0x0" }),
                ))
            } else if cfg.method == method && cfg.remaining > 0 {
                match cfg.behaviour {
                    ChaosBehaviour::EmptyResult => {
                        cfg.remaining -= 1;
                        // Queue a stale tip for the next `eth_blockNumber`.
                        cfg.pending_stale_tips += 1;
                        Some(Perturbation::Synthesize(
                            json!({ "jsonrpc": "2.0", "id": id, "result": [] }),
                        ))
                    }
                    ChaosBehaviour::Delay { duration } => {
                        cfg.remaining -= 1;
                        Some(Perturbation::DelayResponse(duration))
                    }
                }
            } else {
                None
            }
        }
    };

    drop(guard);
    result
}

/// Forward a single parsed JSON-RPC request to the upstream node and
/// return its response body.
async fn forward_one(state: &ProxyState, request: &Value) -> anyhow::Result<Value> {
    let response = state
        .client
        .post(state.upstream.clone())
        .json(request)
        .send()
        .await?;
    Ok(response.json::<Value>().await?)
}

/// Forward an opaque (non-JSON) body verbatim and relay the raw response.
async fn forward_raw(state: &ProxyState, body: Bytes) -> Response {
    match state
        .client
        .post(state.upstream.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(response) => {
            let bytes = response.bytes().await.unwrap_or_default();
            ([(reqwest::header::CONTENT_TYPE, "application/json")], bytes).into_response()
        }
        Err(error) => {
            warn!(?error, "chaos proxy failed to forward raw request");
            (
                axum::http::StatusCode::BAD_GATEWAY,
                "chaos proxy upstream error",
            )
                .into_response()
        }
    }
}

fn json_response(value: &Value) -> Response {
    match serde_json::to_vec(value) {
        Ok(bytes) => ([(reqwest::header::CONTENT_TYPE, "application/json")], bytes).into_response(),
        Err(error) => {
            warn!(?error, "chaos proxy failed to serialize response");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
