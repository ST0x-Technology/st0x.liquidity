//! Token address configuration and USDC constants for onchain operations.

use alloy::primitives::{Address, address};
use serde::Deserialize;

/// Wrapped and unwrapped token addresses for an equity.
#[derive(Debug, Clone, Deserialize)]
pub struct EquityTokenAddresses {
    pub wrapped: Address,
    pub unwrapped: Address,
}

pub const USDC_ETHEREUM: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
pub const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
pub const USDC_ETHEREUM_SEPOLIA: Address = address!("0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238");
