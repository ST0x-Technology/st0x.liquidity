//! Shared Anvil fixtures for integration tests.

use alloy::node_bindings::Anvil;
use alloy::primitives::B256;

use crate::test_utils::{TestAnvilInstance, spawn_anvil};

/// Spawns a local Anvil node and returns it with its endpoint and first
/// private key.
pub(crate) fn setup_anvil() -> (TestAnvilInstance, String, B256) {
    let anvil = spawn_anvil(Anvil::new());
    let endpoint = anvil.endpoint();
    let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
    (anvil, endpoint, private_key)
}
