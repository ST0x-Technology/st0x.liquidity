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

use std::sync::Arc;
use tokio::sync::Mutex;
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
/// Fields are wired by [`ChaosProxy::empty_get_logs`] but not yet read --
/// the proxy implementation that consumes them lands in the next upstack
/// PR per TTDD types-first sequencing.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ChaosConfig {
    pub method: String,
    pub behaviour: ChaosBehaviour,
    pub remaining: usize,
}

/// Spawned proxy. Holds the listening port and a handle the test can use
/// to mutate the active [`ChaosConfig`] mid-run.
#[derive(Debug)]
pub(crate) struct ChaosProxy {
    /// `ws://127.0.0.1:<port>` the bot should connect to instead of the
    /// real Anvil endpoint.
    pub endpoint: Url,
    /// Shared mutable config; `None` means transparent forwarding.
    config: Arc<Mutex<Option<ChaosConfig>>>,
}

impl ChaosProxy {
    /// Spawn a chaos proxy in front of `upstream_ws` and start listening
    /// on a free local port. The proxy keeps running until the returned
    /// handle is dropped (the listener task ends with its parent).
    pub(crate) async fn start(_upstream_ws: Url) -> anyhow::Result<Self> {
        // No-op await so the stub still satisfies `clippy::unused_async`
        // until the implementation PR lands real async work here.
        tokio::task::yield_now().await;
        todo!(
            "chaos proxy: bind a tokio TcpListener on 127.0.0.1:0, accept connections, upgrade to WebSocket via tokio-tungstenite, open a parallel WS connection to upstream_ws, and forward frames in both directions while applying the active ChaosConfig to matching JSON-RPC requests"
        )
    }

    /// Convenience: reply to the next `count` `eth_getLogs` calls with
    /// an empty `result`, modelling a load-balancer node that is
    /// behind on indexing.
    pub(crate) async fn empty_get_logs(&self, count: usize) {
        *self.config.lock().await = Some(ChaosConfig {
            method: "eth_getLogs".to_owned(),
            behaviour: ChaosBehaviour::EmptyResult,
            remaining: count,
        });
    }
}
