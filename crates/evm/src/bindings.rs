//! Shared Solidity contract ABI bindings.
//!
//! `IERC20` is a pure EVM primitive consumed by chain-touching crates in the
//! workspace, so it lives here in `st0x-evm` rather than being redeclared per
//! consumer. Bindings are generated from the `ST0X_*_ABI` environment
//! variables provided by the Nix dev shell at compile time.

use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, env!("ST0X_IERC20_ABI")
);
