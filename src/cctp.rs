use alloy::sol;

sol!(
    #[sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TokenMessengerV2,
    "lib/evm-cctp-contracts/out/TokenMessengerV2.sol/TokenMessengerV2.json"
);

sol!(
    #[sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    MessageTransmitterV2,
    "lib/evm-cctp-contracts/out/MessageTransmitterV2.sol/MessageTransmitterV2.json"
);
