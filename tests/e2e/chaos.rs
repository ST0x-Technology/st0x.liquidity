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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use reqwest::Client;
use serde_json::{Value, json};
use sqlx::ConnectOptions;
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
    /// Merge the last non-empty result previously served for the same
    /// event signature into the response. Models a load-balanced or
    /// caching RPC node serving stale results that ignore the requested
    /// block range -- the same logs are delivered again in a later poll
    /// round, after the consumer's checkpoint has already advanced.
    ReplayLogs,
    /// Like [`ChaosBehaviour::ReplayLogs`], but rewrites each re-served
    /// log's `blockHash` to [`FORK_BLOCK_HASH`]. Models a load-balanced
    /// RPC node on a forked branch re-serving an already-ingested fill's
    /// log under the fork's block -- the reorg the consumer detects via
    /// block-hash mismatch against the hash it persisted at ingestion.
    ForkReplay,
}

/// Block hash the [`ChaosBehaviour::ForkReplay`] perturbation stamps onto
/// re-served logs. All-`0xff` cannot collide with a real Anvil block hash,
/// so the consumer's persisted-vs-observed comparison always sees a
/// mismatch and treats the re-served fill as reorged off the canonical
/// chain.
const FORK_BLOCK_HASH: &str = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

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

/// Last non-empty `eth_getLogs` result served per event signature
/// (`topics[0]` of the request filter), used by
/// [`ChaosBehaviour::ReplayLogs`] to re-deliver stale logs.
type LogsCache = Arc<Mutex<HashMap<String, Vec<Value>>>>;

/// Axum handler state: the mutable chaos config plus what's needed to
/// forward un-perturbed requests to the real Anvil node.
#[derive(Clone)]
struct ProxyState {
    config: SharedConfig,
    logs_cache: LogsCache,
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
    /// Shared logs cache, retained so a severed-and-restored proxy keeps
    /// its replay state.
    logs_cache: LogsCache,
    /// Upstream endpoint, retained so [`ChaosProxy::restore`] can respawn
    /// the serve loop after a severance.
    upstream: Url,
    /// Listening port, retained so [`ChaosProxy::restore`] rebinds the
    /// same address the bot is configured with.
    port: u16,
    /// Serve loop's join handle. Aborted on sever/drop; awaited by
    /// `restore` before rebinding so the listener is fully released first.
    server_task: tokio::task::JoinHandle<()>,
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
        let logs_cache: LogsCache = Arc::new(Mutex::new(HashMap::new()));

        let state = ProxyState {
            config: config.clone(),
            logs_cache: logs_cache.clone(),
            upstream: upstream.clone(),
            client: Client::new(),
        };
        let server_task = serve_proxy(
            listener,
            Router::new().fallback(handle_rpc).with_state(state),
        );

        Ok(Self {
            endpoint,
            config,
            logs_cache,
            upstream,
            port,
            server_task,
        })
    }

    fn state(&self) -> ProxyState {
        ProxyState {
            config: self.config.clone(),
            logs_cache: self.logs_cache.clone(),
            upstream: self.upstream.clone(),
            client: Client::new(),
        }
    }

    /// Severs connectivity: the serve loop is aborted and the listener
    /// dropped, so every connection to the proxy's endpoint is refused --
    /// the component behind it is down while the rest of the world stays
    /// up.
    pub(crate) fn sever(&self) {
        self.server_task.abort();
    }

    /// Restores connectivity after [`ChaosProxy::sever`]: rebinds the
    /// same port and respawns the serve loop with the retained upstream
    /// and chaos state.
    pub(crate) async fn restore(&mut self) -> anyhow::Result<()> {
        // `abort()` only schedules cancellation, so await the severed task's
        // completion before rebinding -- otherwise the old listener may still
        // hold the port and race the rebind (SO_REUSEADDR mitigates but does
        // not eliminate that).
        self.server_task.abort();
        let _ = (&mut self.server_task).await;
        let listener = rebind(self.port)?;
        self.server_task = serve_proxy(
            listener,
            Router::new().fallback(handle_rpc).with_state(self.state()),
        );
        Ok(())
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

    /// Convenience: merge the last non-empty `eth_getLogs` result for
    /// the same event signature into the next `count` `eth_getLogs`
    /// responses, modelling a stale node re-serving logs the consumer
    /// already ingested in an earlier poll round.
    pub(crate) async fn replay_get_logs(&self, count: usize) {
        *self.config.lock().await = Some(ChaosConfig {
            method: "eth_getLogs".to_owned(),
            behaviour: ChaosBehaviour::ReplayLogs,
            remaining: count,
            pending_stale_tips: 0,
        });
    }

    /// Convenience: re-serve the last non-empty `eth_getLogs` result for
    /// the same event signature into the next `count` `eth_getLogs`
    /// responses with each re-served log's `blockHash` rewritten to
    /// [`FORK_BLOCK_HASH`], modelling a forked RPC node re-serving an
    /// already-ingested fill's log under a competing block. A generous
    /// `count` covers both event-signature polls across several rounds;
    /// the consumer's reorg reversal is exactly-once, so re-detecting the
    /// same fork on later rounds is an idempotent no-op.
    pub(crate) async fn fork_replay_get_logs(&self, count: usize) {
        *self.config.lock().await = Some(ChaosConfig {
            method: "eth_getLogs".to_owned(),
            behaviour: ChaosBehaviour::ForkReplay,
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

/// Spawns a proxy serve loop on the given listener, returning its join
/// handle so callers can both abort it (sever) and await its shutdown
/// (before rebinding the same port).
fn serve_proxy(listener: tokio::net::TcpListener, app: Router) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            warn!(?error, "chaos proxy server stopped");
        }
    })
}

/// Rebinds a previously-used local port for a restored proxy.
/// `SO_REUSEADDR` is required: the severed proxy's connections linger in
/// TIME_WAIT and would otherwise fail the rebind.
fn rebind(port: u16) -> anyhow::Result<tokio::net::TcpListener> {
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(format!("127.0.0.1:{port}").parse()?)?;
    Ok(socket.listen(1024)?)
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
    /// Upstream endpoint, retained so [`LatencyProxy::restore`] can
    /// respawn the serve loop after a severance.
    upstream: Url,
    /// Listening port, retained so [`LatencyProxy::restore`] rebinds the
    /// same address the bot is configured with.
    port: u16,
    /// Serve loop's join handle. Aborted on sever/drop; awaited by
    /// `restore` before rebinding so the listener is fully released first.
    server_task: tokio::task::JoinHandle<()>,
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
            upstream: upstream.clone(),
            client: Self::forward_client()?,
        };
        let server_task = serve_proxy(
            listener,
            Router::new().fallback(handle_http).with_state(state),
        );

        Ok(Self {
            endpoint,
            config,
            upstream,
            port,
            server_task,
        })
    }

    /// Forward client for the proxy's upstream leg, bounded by a 10s timeout so
    /// an unresponsive upstream cannot hang the proxy task indefinitely; the
    /// injected latency is a separate sleep, not this request.
    fn forward_client() -> anyhow::Result<Client> {
        Ok(Client::builder().timeout(Duration::from_secs(10)).build()?)
    }

    /// Builds a fresh handler state with the timeout-bounded forward client, so
    /// a restored proxy keeps the same hang-protection as the original.
    fn state(&self) -> anyhow::Result<LatencyProxyState> {
        Ok(LatencyProxyState {
            config: self.config.clone(),
            upstream: self.upstream.clone(),
            client: Self::forward_client()?,
        })
    }

    /// Severs connectivity: the serve loop is aborted and the listener
    /// dropped, so every connection to the proxy's endpoint is refused --
    /// the broker is down while the chain stays up.
    pub(crate) fn sever(&self) {
        self.server_task.abort();
    }

    /// Restores connectivity after [`LatencyProxy::sever`]: rebinds the
    /// same port and respawns the serve loop with the retained upstream
    /// and latency state.
    pub(crate) async fn restore(&mut self) -> anyhow::Result<()> {
        // `abort()` only schedules cancellation, so await the severed task's
        // completion before rebinding -- otherwise the old listener may still
        // hold the port and race the rebind (SO_REUSEADDR mitigates but does
        // not eliminate that).
        self.server_task.abort();
        let _ = (&mut self.server_task).await;
        let listener = rebind(self.port)?;
        self.server_task = serve_proxy(
            listener,
            Router::new()
                .fallback(handle_http)
                .with_state(self.state()?),
        );
        Ok(())
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

/// Holds SQLite's write lock on the bot's database file from a second
/// connection, modelling a co-located process (e.g. the reporter)
/// contending for the WAL write lock. Readers are unaffected under WAL;
/// every bot write blocks until release or the bot pool's busy timeout
/// expires.
///
/// Prefer [`DbLock::release`] for a deterministic, explicit unlock. There is
/// deliberately no `Drop` impl: dropping the raw `SqliteConnection` -- including
/// on a test panic between acquire and release -- closes it, which rolls back
/// the open `BEGIN IMMEDIATE` and releases the WAL lock. A blocking `ROLLBACK`
/// inside `Drop` would instead risk panicking on the tokio runtime.
pub(crate) struct DbLock {
    connection: sqlx::SqliteConnection,
}

impl DbLock {
    /// Opens a raw connection to the database file and takes the write
    /// lock via `BEGIN IMMEDIATE`.
    pub(crate) async fn acquire(db_path: &std::path::Path) -> anyhow::Result<Self> {
        // Wait for the bot pool to yield the WAL write lock instead of
        // failing immediately. SQLite's default busy timeout is 0ms, so
        // without this `BEGIN IMMEDIATE` races the running pipeline and the
        // acquire can spuriously return SQLITE_BUSY -- surfacing as a test
        // failure rather than the contention the test means to exercise.
        let mut connection = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(db_path)
            .busy_timeout(Duration::from_secs(5))
            .connect()
            .await?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut connection)
            .await?;

        Ok(Self { connection })
    }

    /// Releases the write lock.
    pub(crate) async fn release(mut self) -> anyhow::Result<()> {
        sqlx::query("ROLLBACK")
            .execute(&mut self.connection)
            .await?;
        Ok(())
    }
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
    /// Forward upstream, then merge previously-cached logs for the same
    /// event signature into the response, in the given mode.
    ReplayResponse(ReplayMode),
}

/// How [`replay_cached_logs`] re-serves cached logs.
#[derive(Debug, Clone, Copy)]
enum ReplayMode {
    /// Re-serve cached logs verbatim, modelling a stale node on the same
    /// chain ([`ChaosBehaviour::ReplayLogs`]).
    SameChain,
    /// Re-serve cached logs with their `blockHash` rewritten to
    /// [`FORK_BLOCK_HASH`], modelling a node on a competing fork
    /// ([`ChaosBehaviour::ForkReplay`]).
    Forked,
}

/// Decide a single JSON-RPC request: synthesize, delay, or replay a
/// perturbed response when the active chaos config matches, otherwise
/// forward it upstream untouched.
///
/// Every forwarded `eth_getLogs` response refreshes the per-signature
/// logs cache so a later [`ChaosBehaviour::ReplayLogs`] arm has stale
/// logs to re-serve.
async fn process_one(state: &ProxyState, request: Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let is_get_logs = method.as_deref() == Some("eth_getLogs");

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
        Some(Perturbation::ReplayResponse(mode)) => {
            let response = forward_or_rpc_error(state, &request, id).await;
            replay_cached_logs(state, &request, response, mode).await
        }
        None => {
            let response = forward_or_rpc_error(state, &request, id).await;
            if is_get_logs {
                cache_get_logs_result(state, &request, &response).await;
            }
            response
        }
    }
}

/// Extracts the request filter's event signature (`topics[0]`), the key
/// under which `eth_getLogs` results are cached and replayed.
fn filter_event_signature(request: &Value) -> Option<String> {
    request["params"][0]["topics"][0]
        .as_str()
        .map(str::to_owned)
}

/// Refreshes the logs cache with this response's result when non-empty,
/// keyed by the request filter's event signature.
async fn cache_get_logs_result(state: &ProxyState, request: &Value, response: &Value) {
    let Some(signature) = filter_event_signature(request) else {
        return;
    };

    let Some(logs) = response["result"].as_array() else {
        return;
    };

    if logs.is_empty() {
        return;
    }

    state
        .logs_cache
        .lock()
        .await
        .insert(signature, logs.clone());
}

/// Merges the cached logs for the request's event signature into the
/// response, skipping logs already present (matched by transaction hash
/// and log index), then refreshes the cache with the original result.
/// The merged response models a stale node re-serving logs from outside
/// the requested block range. Under [`ReplayMode::Forked`] each re-served
/// log's `blockHash` is rewritten to [`FORK_BLOCK_HASH`] so the merge
/// models a competing fork rather than the same chain.
async fn replay_cached_logs(
    state: &ProxyState,
    request: &Value,
    mut response: Value,
    mode: ReplayMode,
) -> Value {
    let Some(signature) = filter_event_signature(request) else {
        return response;
    };

    // Read the previously cached logs and refresh the cache with this
    // response's result under a single lock, so the read-then-refresh is one
    // atomic critical section rather than two racy acquisitions.
    let cached_logs = {
        let mut cache = state.logs_cache.lock().await;
        let previous = cache.get(&signature).cloned();
        if let Some(logs) = response["result"]
            .as_array()
            .filter(|logs| !logs.is_empty())
        {
            cache.insert(signature.clone(), logs.clone());
        }
        previous
    };

    let Some(cached_logs) = cached_logs else {
        warn!(
            signature,
            "ReplayLogs armed but no cached logs for this event signature; \
             forwarding unmodified"
        );
        return response;
    };

    let Some(result) = response["result"].as_array_mut() else {
        return response;
    };

    let log_key = |log: &Value| {
        (
            log["transactionHash"].as_str().map(str::to_owned),
            log["logIndex"].as_str().map(str::to_owned),
        )
    };
    let present: Vec<_> = result.iter().map(log_key).collect();

    let mut replayed: Vec<Value> = cached_logs
        .into_iter()
        .filter(|log| !present.contains(&log_key(log)))
        .collect();

    match mode {
        ReplayMode::Forked => {
            for log in &mut replayed {
                log["blockHash"] = json!(FORK_BLOCK_HASH);
            }
        }
        ReplayMode::SameChain => {}
    }

    warn!(
        signature,
        replayed = replayed.len(),
        ?mode,
        "replay: re-serving stale logs alongside the live response"
    );
    result.extend(replayed);

    response
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
                    ChaosBehaviour::ReplayLogs => {
                        cfg.remaining -= 1;
                        Some(Perturbation::ReplayResponse(ReplayMode::SameChain))
                    }
                    ChaosBehaviour::ForkReplay => {
                        cfg.remaining -= 1;
                        Some(Perturbation::ReplayResponse(ReplayMode::Forked))
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

#[cfg(test)]
mod tests {
    use reqwest::Client;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use url::Url;

    use super::{ProxyState, ReplayMode, replay_cached_logs};

    /// Pins the [`ChaosBehaviour::ForkReplay`] harness contract the reorg e2e
    /// depends on: a fill log cached from an earlier witnessing round is re-served
    /// on a later `eth_getLogs` round as the *same* log (transaction hash and log
    /// index preserved) but stamped with the competing fork's block hash. The
    /// bot's reorg detection keys entirely off that block-hash mismatch, so if the
    /// merge ever stopped re-serving the cached log or stopped rewriting its
    /// `blockHash`, the e2e would silently stop exercising a reorg.
    ///
    /// This exercises [`replay_cached_logs`] directly -- the proxy's response
    /// transform -- without the upstream node or the full bot: the only collaborator
    /// it needs is the pre-seeded logs cache.
    #[tokio::test]
    async fn fork_replay_reserves_cached_fill_under_competing_block_hash() {
        let signature =
            "0x1111111111111111111111111111111111111111111111111111111111111111".to_owned();
        let canonical_block_hash =
            "0x00000000000000000000000000000000000000000000000000000000000000aa";
        let transaction_hash = "0x00000000000000000000000000000000000000000000000000000000000000bb";
        let log_index = "0x0";

        // The fill the bot already ingested, cached by the proxy under its event
        // signature during the witnessing round, on its original canonical block.
        let cached_log = json!({
            "transactionHash": transaction_hash,
            "logIndex": log_index,
            "blockHash": canonical_block_hash,
        });

        let state = ProxyState {
            config: Arc::new(Mutex::new(None)),
            logs_cache: Arc::new(Mutex::new(HashMap::from([(
                signature.clone(),
                vec![cached_log],
            )]))),
            // Never used by `replay_cached_logs` (it transforms an already-received
            // response), but required to build the handler state.
            upstream: Url::parse("http://127.0.0.1:1").unwrap(),
            client: Client::new(),
        };

        // A later poll round whose live result is empty: the forked node serves no
        // new logs in the requested range but replays the stale cached fill.
        let request = json!({ "params": [{ "topics": [signature] }] });
        let live_response = json!({ "jsonrpc": "2.0", "id": 1, "result": [] });

        let replayed =
            replay_cached_logs(&state, &request, live_response, ReplayMode::Forked).await;

        let result = replayed["result"]
            .as_array()
            .expect("the replayed response must carry a result array");
        assert_eq!(
            result.len(),
            1,
            "ForkReplay must re-serve the single cached fill into the empty live result",
        );

        let reserved = &result[0];
        assert_eq!(
            reserved["transactionHash"],
            json!(transaction_hash),
            "the re-served log must be the same fill (transaction hash preserved)",
        );
        assert_eq!(
            reserved["logIndex"],
            json!(log_index),
            "the re-served log must be the same fill (log index preserved)",
        );

        // 32 bytes of 0xff, derived independently of the production constant so the
        // assertion pins the value rather than comparing the code against itself.
        let competing_fork_hash = format!("0x{}", "f".repeat(64));
        assert_eq!(
            reserved["blockHash"],
            json!(competing_fork_hash),
            "ForkReplay must stamp the re-served fill with the competing fork block hash",
        );
        assert_ne!(
            reserved["blockHash"],
            json!(canonical_block_hash),
            "the re-served block hash must differ from the canonical hash the bot \
             persisted, so the bot detects the block-hash mismatch",
        );
    }
}
