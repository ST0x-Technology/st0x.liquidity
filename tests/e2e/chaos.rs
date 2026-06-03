//! HTTP-level chaos proxy for the e2e Anvil provider.
//!
//! Sits between the bot's `rpc_url` and the real Anvil node. Forwards
//! JSON-RPC traffic by default; when configured, returns empty
//! `eth_getLogs` results that model load-balancer inconsistency where
//! one node in the upstream pool is behind on indexing.
//!
//! The bot ingests fills over a single HTTP transport (continuous
//! `eth_getLogs` polling), so each JSON-RPC request and its response are
//! one HTTP round-trip -- the request already carries its `method`, so
//! perturbation is decided per request without tracking ids across
//! frames.
//!
//! Used by the chaos test suite to validate the bot's recovery paths
//! against adversarial RPC behaviour without modifying the production
//! code paths. Additional behaviours (drop, error, stall) land
//! alongside the chaos sub-issues that need them.

use std::sync::Arc;

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
}

impl Drop for ChaosProxy {
    fn drop(&mut self) {
        self.server_task.abort();
    }
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

/// Decide a single JSON-RPC request: synthesize a perturbed response when
/// the active chaos config matches, otherwise forward it upstream.
async fn process_one(state: &ProxyState, request: Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned);

    if let Some(method) = method
        && let Some(perturbed) = perturb(state, &method, &id).await
    {
        return perturbed;
    }

    forward_one(state, &request).await.unwrap_or_else(|error| {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32603, "message": error.to_string() },
        })
    })
}

/// Returns a synthesized response if the active chaos config perturbs this
/// method, or `None` to forward.
async fn perturb(state: &ProxyState, method: &str, id: &Value) -> Option<Value> {
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
                Some(json!({ "jsonrpc": "2.0", "id": id, "result": "0x0" }))
            } else if cfg.method == method && cfg.remaining > 0 {
                match cfg.behaviour {
                    ChaosBehaviour::EmptyResult => {
                        cfg.remaining -= 1;
                        // Queue a stale tip for the next `eth_blockNumber`.
                        cfg.pending_stale_tips += 1;
                        Some(json!({ "jsonrpc": "2.0", "id": id, "result": [] }))
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
