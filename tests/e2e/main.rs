//! E2E integration tests for the st0x-hedge bot.
//!
//! Tests the full bot lifecycle from onchain event detection through CQRS
//! processing to offchain order execution and rebalancing operations.

mod assert;
mod base_chain;
mod cctp;
mod hedging;
mod poll;
mod rebalancing;
mod test_infra;
