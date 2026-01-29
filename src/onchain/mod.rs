use alloy::primitives::Address;
use serde::Deserialize;

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

#[derive(Debug, Clone, Deserialize)]
pub struct EvmConfig {
    pub ws_rpc_url: url::Url,
    pub orderbook: Address,
    pub order_owner: Option<Address>,
    pub deployment_block: u64,
}
