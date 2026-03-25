//! E2E integration tests for the st0x-hedge bot.
//!
//! Tests the full bot lifecycle from onchain event detection through CQRS
//! processing to offchain order execution and rebalancing operations.

// Panics and unwraps are acceptable in test code. The `allow-unwrap-in-tests`
// and `allow-expect-in-tests` clippy config options do not apply to files under
// `tests/` (only to `#[test]` fns and `#[cfg(test)]` blocks), so we set the
// blanket allows here at the crate root instead.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod assert;
mod base_chain;
mod cctp;
mod dev_harness;
mod full_system;
mod hedging;
mod poll;
mod rebalancing;
mod test_infra;
