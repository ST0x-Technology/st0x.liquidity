//! Shared Solidity contract ABI bindings.
//!
//! `IERC20` and `IPyth` are pure EVM primitives consumed by every chain-touching
//! crate in the workspace, so they live here in `st0x-evm` rather than being
//! redeclared per consumer. Bindings are generated from the `ST0X_*_ABI`
//! environment variables provided by the Nix dev shell at compile time.

use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, env!("ST0X_IERC20_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IPyth, env!("ST0X_IPYTH_ABI")
);
