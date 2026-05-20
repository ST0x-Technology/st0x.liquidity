//! Inventory DTOs for per-symbol and USDC balance snapshots.
//!
//! Represents the current distribution of assets across onchain and
//! offchain venues, split by availability (available vs in-flight).

use chrono::{DateTime, Utc};
use serde::Serialize;
use ts_rs::TS;

use st0x_finance::{FractionalShares, HasZero, Symbol, Usdc};

/// Per-symbol equity balances split by venue and availability.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInventory {
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub onchain_available: FractionalShares,
    #[ts(type = "string")]
    pub onchain_inflight: FractionalShares,
    #[ts(type = "string")]
    pub offchain_available: FractionalShares,
    #[ts(type = "string")]
    pub offchain_inflight: FractionalShares,
    /// Equity tokens observed in the Base wallet between venues.
    pub inflight_equity: InFlightEquity,
}

/// Equity tokens sitting in wallets between venues, observed by polling.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InFlightEquity {
    /// Unwrapped tokenized equity (`tSTOCK`) parked on the Base wallet.
    #[ts(type = "string")]
    pub base_wallet_unwrapped: FractionalShares,
    /// Wrapped equity vault shares (`wtSTOCK`) parked on the Base wallet.
    #[ts(type = "string")]
    pub base_wallet_wrapped: FractionalShares,
}

/// Onchain and offchain USDC balances split by availability.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UsdcInventory {
    #[ts(type = "string")]
    pub onchain_available: Usdc,
    #[ts(type = "string")]
    pub onchain_inflight: Usdc,
    #[ts(type = "string")]
    pub offchain_available: Usdc,
    #[ts(type = "string")]
    pub offchain_inflight: Usdc,
    /// Gross offchain USD balance before cash reserve subtraction.
    #[ts(type = "string | null")]
    pub offchain_gross: Option<Usdc>,
    /// Settled cash that can be withdrawn/transferred out of the offchain
    /// broker (Alpaca's `cash_withdrawable` field -- excludes T+1 unsettled
    /// equity-sale proceeds). This is what can actually be rebalanced to
    /// Raindex.
    #[ts(type = "string | null")]
    pub withdrawable_cash: Option<Usdc>,
    /// USDC observed at intermediate wallet locations between venues.
    pub inflight_cash: InFlightCash,
}

/// USDC sitting in wallets between venues, observed by polling.
///
/// `None` means the wallet has not been observed yet; `Some(ZERO)` means
/// it was observed empty. The values are tracked separately from venue
/// inventory and never feed into imbalance math.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InFlightCash {
    /// USDC parked on the Ethereum wallet between Alpaca and CCTP.
    #[ts(type = "string | null")]
    pub ethereum_wallet: Option<Usdc>,
    /// USDC parked on the Base wallet between CCTP and the Raindex vaults.
    #[ts(type = "string | null")]
    pub base_wallet: Option<Usdc>,
}

impl InFlightCash {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            ethereum_wallet: None,
            base_wallet: None,
        }
    }
}

/// Full inventory snapshot across all symbols and USDC.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Inventory {
    pub per_symbol: Vec<SymbolInventory>,
    pub usdc: UsdcInventory,
}

impl Inventory {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            per_symbol: Vec::new(),
            usdc: UsdcInventory {
                onchain_available: Usdc::ZERO,
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::ZERO,
                offchain_inflight: Usdc::ZERO,
                offchain_gross: None,
                withdrawable_cash: None,
                inflight_cash: InFlightCash::empty(),
            },
        }
    }
}

/// Point-in-time snapshot of the current inventory state broadcast to clients.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InventorySnapshot {
    pub inventory: Inventory,
    pub fetched_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn inventory_with_inflight_serializes_correctly() {
        let inventory = Inventory {
            per_symbol: vec![SymbolInventory {
                symbol: Symbol::new("TSLA").unwrap(),
                onchain_available: FractionalShares::new(float!(50)),
                onchain_inflight: FractionalShares::new(float!(5)),
                offchain_available: FractionalShares::new(float!(45)),
                offchain_inflight: FractionalShares::ZERO,
                inflight_equity: InFlightEquity {
                    base_wallet_unwrapped: FractionalShares::new(float!(3)),
                    base_wallet_wrapped: FractionalShares::new(float!(2)),
                },
            }],
            usdc: UsdcInventory {
                onchain_available: Usdc::new(float!(10000)),
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::new(float!(5000)),
                offchain_inflight: Usdc::new(float!(500)),
                offchain_gross: Some(Usdc::new(float!(6000))),
                withdrawable_cash: Some(Usdc::new(float!(4500))),
                inflight_cash: InFlightCash {
                    ethereum_wallet: Some(Usdc::new(float!(250))),
                    base_wallet: Some(Usdc::ZERO),
                },
            },
        };

        let json = serde_json::to_value(&inventory).expect("serialization should succeed");

        let symbol = &json["perSymbol"][0];
        assert_eq!(symbol["symbol"], json!("TSLA"));
        assert_eq!(symbol["onchainAvailable"], json!("50"));
        assert_eq!(symbol["onchainInflight"], json!("5"));
        assert_eq!(symbol["offchainAvailable"], json!("45"));
        assert_eq!(symbol["offchainInflight"], json!("0"));
        assert_eq!(symbol["inflightEquity"]["baseWalletUnwrapped"], json!("3"));
        assert_eq!(symbol["inflightEquity"]["baseWalletWrapped"], json!("2"));

        let usdc = &json["usdc"];
        assert_eq!(usdc["onchainAvailable"], json!("10000"));
        assert_eq!(usdc["onchainInflight"], json!("0"));
        assert_eq!(usdc["offchainAvailable"], json!("5000"));
        assert_eq!(usdc["offchainInflight"], json!("500"));
        assert_eq!(usdc["offchainGross"], json!("6000"));
        assert_eq!(usdc["withdrawableCash"], json!("4500"));
        assert_eq!(usdc["inflightCash"]["ethereumWallet"], json!("250"));
        assert_eq!(usdc["inflightCash"]["baseWallet"], json!("0"));
    }
}
