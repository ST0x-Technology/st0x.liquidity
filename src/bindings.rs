use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IOrderBookV4, "lib/rain.orderbook.interface/out/IOrderBookV4.sol/IOrderBookV4.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IOrderBookV5, "lib/rain.orderbook.interface/out/IOrderBookV5.sol/IOrderBookV5.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, "lib/forge-std/out/IERC20.sol/IERC20.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IPyth, "node_modules/@pythnetwork/pyth-sdk-solidity/abis/IPyth.json"
);
