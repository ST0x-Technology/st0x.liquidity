use alloy::primitives::{Address, address};
use alloy::rpc::client::RpcClient;
use alloy::transports::layers::RetryBackoffLayer;
use clap::Parser;
use url::Url;

pub(crate) const USDC_ETHEREUM: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
pub(crate) const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
pub(crate) const USDC_ETHEREUM_SEPOLIA: Address =
    address!("0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238");

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

pub(crate) mod accumulator;
pub(crate) mod backfill;
mod clear;
pub(crate) mod io;
pub(crate) mod position_calculator;
pub(crate) mod pyth;
mod take_order;
pub(crate) mod trade;
pub(crate) mod vault;

pub use trade::OnchainTrade;

#[derive(Parser, Debug, Clone)]
pub struct EvmEnv {
    #[clap(short, long, env)]
    pub ws_rpc_url: url::Url,
    #[clap(short = 'b', long, env)]
    pub orderbook: Address,
    #[clap(short, long, env)]
    pub order_owner: Option<Address>,
    #[clap(short = 'd', long, env)]
    pub deployment_block: u64,
}
