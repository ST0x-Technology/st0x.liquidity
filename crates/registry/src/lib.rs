//! Shared reference-data registry for st0x.liquidity.
//!
//! Houses reference-data lookups consumed by both domain and application
//! crates. Today that is token-symbol resolution: the [`SymbolCache`]
//! (token address -> ERC20 symbol) and the per-symbol [`get_symbol_lock`]
//! that serializes concurrent trade processing.

mod symbol;

pub use symbol::{SymbolCache, get_symbol_lock};
