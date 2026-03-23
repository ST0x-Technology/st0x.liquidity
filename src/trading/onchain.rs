//! On-chain trade processing: [`inclusion`] validates blockchain event
//! metadata, [`trade_accountant`] accounts for DEX fills via CQRS commands.

pub(crate) mod inclusion;
pub(crate) mod trade_accountant;
