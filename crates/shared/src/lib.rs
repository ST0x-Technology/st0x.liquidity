//! Shared types for tokenized equity operations.
//!
//! This crate provides types shared between the hedging bot (`st0x-hedge`)
//! and the order taker bot. It includes contract bindings, token wrapping
//! abstractions, and tokenized symbol types.

pub mod bindings;

pub mod equity_token;
pub mod tokenized_symbol;
pub mod wrapper;

pub use equity_token::{EquityTokenAddresses, USDC_BASE, USDC_ETHEREUM, USDC_ETHEREUM_SEPOLIA};
pub use tokenized_symbol::{
    OneToOneTokenizedShares, TokenizationForm, TokenizedSymbol, TokenizedSymbolError,
    WrappedTokenizedShares,
};
pub use wrapper::{RatioError, UnderlyingPerWrapped, Wrapper, WrapperError, WrapperService};
