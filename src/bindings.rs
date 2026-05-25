//! Solidity contract ABI bindings for raindex orderbook, ERC20,
//! and Pyth oracle contracts.

use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IOrderBookV6, env!("ST0X_IORDERBOOK_V6_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, env!("ST0X_IERC20_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC4626, env!("ST0X_IERC4626_ABI")
);

#[cfg(test)]
sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TestERC20, env!("ST0X_TEST_ERC20_ABI")
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    OrderBook, env!("ST0X_ORDERBOOK_ABI")
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TOFUTokenDecimals, env!("ST0X_TOFU_TOKEN_DECIMALS_ABI")
);

// ERC20 with configurable name, symbol, and decimals via constructor args.
// Distinct from `TestERC20` (ArbTest Token) which has a no-arg constructor.
#[cfg(any(test, feature = "mock", feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    DeployableERC20, env!("ST0X_DEPLOYABLE_ERC20_ABI")
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    Interpreter, env!("ST0X_INTERPRETER_ABI")
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    Store, env!("ST0X_STORE_ABI")
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    Parser, env!("ST0X_PARSER_ABI")
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    Deployer, env!("ST0X_DEPLOYER_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IPyth, env!("ST0X_IPYTH_ABI")
);

sol!(
    #![sol(all_derives = true)]
    LibDecimalFloat, env!("ST0X_LIB_DECIMAL_FLOAT_ABI")
);
