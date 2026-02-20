//! CCTP attester setup helper for e2e tests on forked chains.
//!
//! On forked mainnet, the `MessageTransmitterV2` contract already has
//! Circle's production attesters configured with a multi-sig threshold.
//! This module uses Anvil account impersonation to add a test attester
//! and lower the signature threshold to 1, so our test key can sign
//! attestations locally.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::providers::ext::AnvilApi as _;
use alloy::sol;

/// CCTP `MessageTransmitterV2` contract address (same on all chains).
const MESSAGE_TRANSMITTER_V2: Address =
    alloy::primitives::address!("0x81D40F21F12A8F0E3252Bccb954D722d4c464B64");

// Minimal ABI for attester management functions.
sol! {
    #[sol(rpc)]
    #[allow(clippy::too_many_arguments)]
    interface IMessageTransmitterAttester {
        function attesterManager() external view returns (address);
        function enableAttester(address newAttester) external;
        function setSignatureThreshold(uint256 newSignatureThreshold) external;
    }
}

/// Adds a test attester to `MessageTransmitterV2` on a forked chain and
/// sets the signature threshold to 1.
///
/// This uses Anvil's `anvil_impersonateAccount` to execute calls from the
/// `attesterManager` address without needing its private key.
pub async fn setup_test_attester<P: Provider>(
    provider: &P,
    test_attester_address: Address,
) -> anyhow::Result<()> {
    let contract = IMessageTransmitterAttester::new(MESSAGE_TRANSMITTER_V2, provider);

    // Query the current attester manager
    let manager = contract.attesterManager().call().await?;

    // Impersonate the attester manager
    provider.anvil_impersonate_account(manager).await?;

    // Enable our test attester
    contract
        .enableAttester(test_attester_address)
        .from(manager)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Lower the signature threshold to 1 so our single test attester suffices
    contract
        .setSignatureThreshold(U256::from(1))
        .from(manager)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Stop impersonating
    provider.anvil_stop_impersonating_account(manager).await?;

    Ok(())
}
