//! Shared Anvil fixtures for integration tests.

use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::B256;

/// Spawns a local Anvil node and returns it with its endpoint and first
/// private key.
pub(crate) fn setup_anvil() -> (AnvilInstance, String, B256) {
    let anvil = Anvil::new().spawn();
    let endpoint = anvil.endpoint();
    let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
    (anvil, endpoint, private_key)
}
