//! E2E integration tests for the st0x-taker bot.
//!
//! Tests the full bot lifecycle: order discovery via Anvil events,
//! classification, and profitability evaluation with mocked Alpaca
//! market data.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod discovery;
mod poll;
mod profitability;
mod test_infra;
