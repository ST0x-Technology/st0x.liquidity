//! EVM configuration types: chain config, RPC secrets, and runtime context.

use alloy::primitives::Address;
use serde::Deserialize;
use url::Url;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvmConfig {
    pub orderbook: Address,
    pub deployment_block: u64,
    pub required_confirmations: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvmSecrets {
    /// HTTP RPC URL for the orderbook chain. Drives continuous `eth_getLogs`
    /// fill polling and all read-only contract calls (single transport, no
    /// WebSocket).
    #[serde(rename = "rpc_url")]
    pub rpc: Url,
    /// Base chain RPC URL for wallet operations. Required when `[wallet]`
    /// is configured.
    #[serde(rename = "base_rpc_url")]
    pub base: Option<Url>,
    /// Ethereum mainnet RPC URL for wallet operations. Required when
    /// `[wallet]` is configured.
    #[serde(rename = "ethereum_rpc_url")]
    pub ethereum: Option<Url>,
}

#[derive(Clone)]
pub struct EvmCtx {
    pub rpc_url: Url,
    pub orderbook: Address,
    pub deployment_block: u64,
    pub required_confirmations: u64,
}

impl std::fmt::Debug for EvmCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvmCtx")
            .field("rpc_url", &"[REDACTED]")
            .field("orderbook", &self.orderbook)
            .field("deployment_block", &self.deployment_block)
            .field("required_confirmations", &self.required_confirmations)
            .finish()
    }
}

impl EvmCtx {
    pub fn new(config: &EvmConfig, secrets: EvmSecrets) -> Self {
        Self {
            rpc_url: secrets.rpc,
            orderbook: config.orderbook,
            deployment_block: config.deployment_block,
            required_confirmations: config.required_confirmations,
        }
    }
}
