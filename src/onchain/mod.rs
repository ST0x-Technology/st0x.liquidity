use alloy::primitives::Address;
use alloy::rpc::client::RpcClient;
use alloy::transports::layers::RetryBackoffLayer;
use serde::Deserialize;
use url::Url;

pub(crate) mod accumulator;
pub(crate) mod backfill;
mod clear;
pub(crate) mod io;
pub(crate) mod position_calculator;
pub(crate) mod pyth;
mod take_order;
pub(crate) mod trade;
pub(crate) mod vault;

pub(crate) use trade::OnchainTrade;

#[derive(Deserialize)]
pub(crate) struct EvmConfig {
    pub(crate) orderbook: Address,
    pub(crate) order_owner: Option<Address>,
    pub(crate) deployment_block: u64,
}

#[derive(Deserialize)]
pub(crate) struct EvmSecrets {
    pub(crate) ws_rpc_url: Url,
}

#[derive(Debug, Clone)]
pub(crate) struct EvmCtx {
    pub(crate) ws_rpc_url: Url,
    pub(crate) orderbook: Address,
    pub(crate) order_owner: Option<Address>,
    pub(crate) deployment_block: u64,
}

/// Number of block confirmations to wait after transactions before subsequent
/// operations that depend on the state change. This ensures state propagates
/// across load-balanced RPC providers (like dRPC) that may route requests to
/// different backend nodes.
pub(crate) const REQUIRED_CONFIRMATIONS: u64 = 3;

/// Maximum retries for transient RPC errors (rate limits, null responses, etc.)
const RPC_MAX_RETRIES: u32 = 10;

/// Initial backoff duration in milliseconds before retrying
const RPC_INITIAL_BACKOFF_MS: u64 = 1000;

/// Compute units per second budget for rate limiting
const RPC_COMPUTE_UNITS_PER_SECOND: u64 = 100;

/// Creates an HTTP RPC client with retry layer for transient errors.
///
/// Use with `ProviderBuilder::new().connect_client(client)` for read-only calls,
/// or `ProviderBuilder::new().wallet(w).connect_client(client)` for signing.
pub(crate) fn http_client_with_retry(url: Url) -> RpcClient {
    let retry_layer = RetryBackoffLayer::new(
        RPC_MAX_RETRIES,
        RPC_INITIAL_BACKOFF_MS,
        RPC_COMPUTE_UNITS_PER_SECOND,
    );
    RpcClient::builder().layer(retry_layer).http(url)
}
