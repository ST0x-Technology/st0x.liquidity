//! Relay stand-ins for Anvil: a depository that emits `RelayErc20Deposit` and
//! a stable that can withhold a transfer fee. Sources and artifacts are in
//! `relay-test-contracts/`; `crates/bridge/AGENTS.md` has the command that
//! rebuilds the artifacts.

use alloy::providers::Provider;
use alloy::sol;

use super::RelayEndContracts;

sol!(
    #![sol(rpc)]
    MockDepository,
    "relay-test-contracts/MockDepository.json"
);

sol!(
    #![sol(rpc)]
    MockStable,
    "relay-test-contracts/MockStable.json"
);

/// Deploys a stand-in stable and depository through `provider`, whose wallet
/// signs both deploys.
pub async fn deploy_relay_end<P: Provider>(
    provider: &P,
) -> Result<RelayEndContracts, alloy::contract::Error> {
    let stable = *MockStable::deploy(provider).await?.address();
    let depository = *MockDepository::deploy(provider).await?.address();

    Ok(RelayEndContracts { stable, depository })
}
