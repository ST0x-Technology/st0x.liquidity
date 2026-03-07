//! E2E test infrastructure for st0x-hedge.
//!
//! Provides mock services (broker, tokenization, CCTP attestation),
//! local Anvil chain setup, and shared assertion helpers used by all
//! end-to-end test binaries.

pub mod common;
pub mod services;
