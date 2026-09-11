//! Canonical USDC token contract addresses across supported chains.

use alloy::primitives::{Address, address};

/// USDC on Ethereum mainnet.
pub const USDC_ETHEREUM: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

/// USDC on Base mainnet.
pub const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");

/// USDC on Ethereum Sepolia testnet.
pub const USDC_ETHEREUM_SEPOLIA: Address = address!("0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238");

/// USDC on HyperEVM mainnet.
pub const USDC_HYPEREVM: Address = address!("0xb88339CB7199b77E23DB6E890353E22632Ba630f");

/// USDC on Robinhood Chain (chain 4663), the standard Arbitrum-bridged token
/// (`L2GatewayRouter.calculateL2TokenAddress` of L1 USDC). Verified on-chain
/// to answer `symbol() == "USDC"` with 6 decimals.
pub const USDC_ROBINHOOD: Address = address!("0x80e0e24718dbFcad49ECAA6F1e6C89A190586cA8");
