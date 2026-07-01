//! Solidity ABI bindings used by the tokenization crate's test and mock code.
//!
//! `TestERC20` (ArbTest Token, no-arg constructor) backs the integration tests;
//! `DeployableERC20` (configurable name/symbol/decimals) backs the mock
//! tokenization API. Both are loaded from the workspace `ST0X_*_ABI` env vars.

#[cfg(any(test, feature = "test-support"))]
alloy::sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TestERC20, env!("ST0X_TEST_ERC20_ABI")
);

#[cfg(any(test, feature = "mock", feature = "test-support"))]
alloy::sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    DeployableERC20, env!("ST0X_DEPLOYABLE_ERC20_ABI")
);
