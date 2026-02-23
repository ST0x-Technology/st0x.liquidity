//! CCTP (Cross-Chain Transfer Protocol) infrastructure for e2e tests.
//!
//! Groups all CCTP-related test infrastructure: contract deployment,
//! attestation mocking, and the full infrastructure orchestrator that
//! wires everything together for USDC rebalancing tests.

pub mod attestation;
pub mod contracts;
mod infra;

pub use infra::{CctpInfra, CctpOverrides, USDC_ETHEREUM};
