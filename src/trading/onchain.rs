//! On-chain trade processing: [`inclusion`] validates blockchain event
//! metadata, [`trade_accountant`] accounts for DEX fills via CQRS commands, and
//! [`skipped_fill`] durably records fills the accountant chose to skip, and
//! [`exclusion`] decides which fills are kept out of hedging.

pub(crate) mod exclusion;
pub(crate) mod inclusion;
pub(crate) mod skipped_fill;
pub(crate) mod trade_accountant;
