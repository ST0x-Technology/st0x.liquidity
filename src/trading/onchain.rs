//! On-chain trade processing: [`inclusion`] validates blockchain event
//! metadata, [`trade_accountant`] accounts for DEX fills via CQRS commands, and
//! [`skipped_fill`] durably records fills the accountant chose to skip.

pub(crate) mod inclusion;
pub(crate) mod skipped_fill;
pub(crate) mod trade_accountant;
