//! Solidity contract ABI bindings for raindex orderbook, ERC20,
//! and Pyth oracle contracts.

use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IOrderBookV6, "../../lib/rain.orderbook/out/IOrderBookV6.sol/IOrderBookV6.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, "../../lib/forge-std/out/IERC20.sol/IERC20.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC4626, "../../lib/forge-std/out/IERC4626.sol/IERC4626.json"
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TestERC20, "../../lib/rain.orderbook/out/ArbTest.sol/Token.json"
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    OrderBook, "../../lib/rain.orderbook/out/OrderBookV6.sol/OrderBookV6.json"
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TOFUTokenDecimals, "../../lib/rain.orderbook/out/TOFUTokenDecimals.sol/TOFUTokenDecimals.json"
);

// ERC20 with configurable name, symbol, and decimals via constructor args.
// Distinct from `TestERC20` (ArbTest Token) which has a no-arg constructor.
#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    DeployableERC20, "../../lib/rain.orderbook/lib/rain.interpreter/out/TestERC20.sol/TestERC20.json"
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    Interpreter, "../../lib/rain.orderbook/out/Rainterpreter.sol/Rainterpreter.json"
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    Store, "../../lib/rain.orderbook/out/RainterpreterStore.sol/RainterpreterStore.json"
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    Parser, "../../lib/rain.orderbook/out/RainterpreterParser.sol/RainterpreterParser.json"
);

#[cfg(any(test, feature = "test-support"))]
sol!(
    #![sol(all_derives = true, rpc)]
    Deployer, "../../lib/rain.orderbook/out/RainterpreterExpressionDeployer.sol/RainterpreterExpressionDeployer.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IPyth, "../../lib/pyth-crosschain/target_chains/ethereum/sdk/solidity/abis/IPyth.json"
);

sol!(
    #![sol(all_derives = true)]
    LibDecimalFloat, "../../lib/rain.orderbook/out/LibDecimalFloat.sol/LibDecimalFloat.json"
);

// Minimal binding for the MetaV1_2 event emitted by the OrderBook contract.
// Emitted in the same transaction as AddOrderV3, with `subject` = order hash
// and `meta` = CBOR-encoded RainMetaDocumentV1 containing the Rainlang source.
// We define this inline rather than importing the full OrderBook ABI to avoid
// pulling in deployment logic and keeping it available in production code
// (the full OrderBook binding is behind test-support).
sol! {
    #![sol(all_derives = true)]
    #[derive(serde::Serialize, serde::Deserialize)]
    event MetaV1_2(address sender, bytes32 subject, bytes meta);
}
