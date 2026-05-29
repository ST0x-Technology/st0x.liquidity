//! WebSocket-level chaos proxy for the e2e Anvil provider.
//!
//! Sits between the bot's `ws_rpc_url` and the real Anvil node. Forwards
//! JSON-RPC traffic by default; when configured, returns empty
//! `eth_getLogs` results that model load-balancer inconsistency where
//! one node in the upstream pool is behind on indexing.
//!
//! Used by the chaos test suite to validate the bot's recovery paths
//! against adversarial RPC behaviour without modifying the production
//! code paths. Additional behaviours (drop, error, stall) land
//! alongside the chaos sub-issues that need them.

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_async, connect_async, tungstenite::protocol::Message,
};
use tracing::{debug, warn};
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

/// Per-connection map of in-flight JSON-RPC request id to the method that
/// produced it, so the response interceptor knows which method's chaos
/// config to consult when a response arrives.
type PendingMethods = Arc<Mutex<HashMap<String, String>>>;

/// Spawned proxy. Holds the listening port and a handle the test can use
/// to mutate the active [`ChaosConfig`] mid-run.
#[derive(Debug)]
pub(crate) struct ChaosProxy {
    /// `ws://127.0.0.1:<port>` the bot should connect to instead of the
    /// real Anvil endpoint.
    pub endpoint: Url,
    /// Shared mutable config; `None` means transparent forwarding.
    config: SharedConfig,
    /// Accept loop's handle. Aborted on drop so the proxy dies with its
    /// parent test.
    listener_task: tokio::task::AbortHandle,
}

impl ChaosProxy {
    /// Spawn a chaos proxy in front of `upstream_ws` and start listening
    /// on a free local port. The proxy keeps running until the returned
    /// handle is dropped.
    pub(crate) async fn start(upstream_ws: Url) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let endpoint: Url = format!("ws://127.0.0.1:{port}").parse()?;

        let config: SharedConfig = Arc::new(Mutex::new(None));

        let upstream = upstream_ws.clone();
        let task_config = config.clone();
        let handle = tokio::spawn(async move {
            accept_loop(listener, upstream, task_config).await;
        });

        Ok(Self {
            endpoint,
            config,
            listener_task: handle.abort_handle(),
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
        self.listener_task.abort();
    }
}

async fn accept_loop(listener: TcpListener, upstream_ws: Url, config: SharedConfig) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(error) => {
                warn!(?error, "chaos proxy accept failed");
                continue;
            }
        };

        let upstream = upstream_ws.clone();
        let conn_config = config.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, upstream, conn_config).await {
                warn!(?error, "chaos proxy connection ended with error");
            }
        });
    }
}

async fn handle_connection(
    client_stream: TcpStream,
    upstream_ws: Url,
    config: SharedConfig,
) -> anyhow::Result<()> {
    let client_ws = accept_async(client_stream).await?;
    let (upstream_ws_conn, _response) = connect_async(upstream_ws.as_str()).await?;

    let (client_sink, client_stream) = client_ws.split();
    let (upstream_sink, upstream_stream) = upstream_ws_conn.split();
    let pending: PendingMethods = Arc::new(Mutex::new(HashMap::new()));

    let client_to_upstream =
        forward_client_to_upstream(client_stream, upstream_sink, pending.clone());
    let upstream_to_client =
        forward_upstream_to_client(upstream_stream, client_sink, pending, config);

    tokio::select! {
        () = client_to_upstream => {}
        () = upstream_to_client => {}
    }

    Ok(())
}

async fn forward_client_to_upstream(
    mut client_stream: futures_util::stream::SplitStream<WebSocketStream<TcpStream>>,
    mut upstream_sink: futures_util::stream::SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        Message,
    >,
    pending: PendingMethods,
) {
    while let Some(frame) = client_stream.next().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                debug!(?error, "client->upstream stream ended");
                break;
            }
        };

        if let Message::Text(ref text) = frame {
            record_pending_method(text, &pending).await;
        }

        if upstream_sink.send(frame).await.is_err() {
            break;
        }
    }
}

async fn forward_upstream_to_client(
    mut upstream_stream: futures_util::stream::SplitStream<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
    >,
    mut client_sink: futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>,
    pending: PendingMethods,
    config: SharedConfig,
) {
    while let Some(frame) = upstream_stream.next().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                debug!(?error, "upstream->client stream ended");
                break;
            }
        };

        let outgoing = match frame {
            Message::Text(text) => Message::Text(
                perturb_if_matching(text.to_string(), &pending, &config)
                    .await
                    .into(),
            ),
            other => other,
        };

        if client_sink.send(outgoing).await.is_err() {
            break;
        }
    }
}

/// On the client->upstream path: parse a JSON-RPC request frame and
/// remember its id->method mapping so the response path can check the
/// chaos config against the method.
async fn record_pending_method(text: &str, pending: &PendingMethods) {
    let Ok(json) = serde_json::from_str::<Value>(text) else {
        return;
    };
    let (Some(id), Some(method)) = (json.get("id"), json.get("method").and_then(Value::as_str))
    else {
        return;
    };
    let id_key = id.to_string();
    pending.lock().await.insert(id_key, method.to_owned());
}

/// On the upstream->client path: if this response corresponds to a
/// method targeted by the active chaos config, apply the configured
/// behaviour to the response body before forwarding.
async fn perturb_if_matching(
    text: String,
    pending: &PendingMethods,
    config: &SharedConfig,
) -> String {
    let Ok(mut json) = serde_json::from_str::<Value>(&text) else {
        return text;
    };
    let Some(id) = json.get("id").cloned() else {
        return text;
    };
    let id_key = id.to_string();

    let method = pending.lock().await.remove(&id_key);
    let Some(method) = method else {
        return text;
    };

    let mut guard = config.lock().await;
    let Some(cfg) = guard.as_mut() else {
        return text;
    };

    // For `EmptyResult` chaos on `eth_getLogs`, each perturbed `getLogs`
    // queues a paired stale `eth_blockNumber` (so the bot's
    // read-after-write tip check actually fires). Drain that queue
    // before checking whether new perturbations are still in budget --
    // otherwise the counters can desync when `getLogs` budget exhausts
    // mid-batch while a paired tip is still owed.
    let is_paired_tip = method == "eth_blockNumber"
        && cfg.method == "eth_getLogs"
        && matches!(cfg.behaviour, ChaosBehaviour::EmptyResult)
        && cfg.pending_stale_tips > 0;
    if is_paired_tip {
        if let Some(obj) = json.as_object_mut() {
            obj.insert("result".to_owned(), Value::String("0x0".to_owned()));
            obj.remove("error");
        }
        cfg.pending_stale_tips -= 1;
        drop(guard);
        return serde_json::to_string(&json).unwrap_or(text);
    }

    if cfg.method != method || cfg.remaining == 0 {
        return text;
    }

    match cfg.behaviour {
        ChaosBehaviour::EmptyResult => {
            if let Some(obj) = json.as_object_mut() {
                obj.insert("result".to_owned(), Value::Array(Vec::new()));
                obj.remove("error");
            }
        }
    }
    cfg.remaining -= 1;
    // Queue a stale tip for the next `eth_blockNumber` so the bot's
    // read-after-write check fires for this perturbed `getLogs`.
    cfg.pending_stale_tips += 1;
    drop(guard);

    serde_json::to_string(&json).unwrap_or(text)
}
