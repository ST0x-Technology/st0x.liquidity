//! Inventory DTOs for per-symbol and USDC balance snapshots.
//!
//! Represents the current distribution of assets across onchain and
//! offchain venues, split by availability (available vs in-flight).

use chrono::{DateTime, Utc};
use serde::Serialize;
use ts_rs::TS;

use st0x_finance::{FractionalShares, HasZero, Symbol, Usdc};

use crate::ChainName;

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
    /// Every chain's vault balance for this symbol, in chain order. The
    /// `onchain*` fields above are the primary chain's entry alone: wrapped
    /// shares on different chains cannot be added together. A chain whose
    /// vault the bot has not read yet has no entry.
    pub onchain_by_chain: Vec<OnchainEquityBalance>,
    /// Equity tokens observed in the Base wallet between venues.
    pub inflight_equity: InFlightEquity,
}

/// One chain's vault balance of a symbol's wrapped equity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OnchainEquityBalance {
    pub chain: ChainName,
    #[ts(type = "string")]
    pub available: FractionalShares,
    #[ts(type = "string")]
    pub inflight: FractionalShares,
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
    /// The settlement stable the onchain balances are held in; the dashboard
    /// labels the cash row with it.
    pub symbol: String,
    #[ts(type = "string")]
    pub onchain_available: Usdc,
    #[ts(type = "string")]
    pub onchain_inflight: Usdc,
    #[ts(type = "string")]
    pub offchain_available: Usdc,
    #[ts(type = "string")]
    pub offchain_inflight: Usdc,
    /// Every chain's cash vault balance, in chain order. The `onchain*`
    /// fields above are the primary chain's entry alone. A chain whose vault
    /// the bot has not read yet has no entry.
    pub onchain_by_chain: Vec<OnchainUsdcBalance>,
    /// Gross offchain USD balance before cash reserve subtraction.
    #[ts(type = "string | null")]
    pub offchain_gross: Option<Usdc>,
    /// Settled cash that can be withdrawn/transferred out of the offchain
    /// broker (Alpaca's `cash_withdrawable` field -- excludes T+1 unsettled
    /// equity-sale proceeds). This is what can actually be rebalanced to
    /// Raindex.
    #[ts(type = "string | null")]
    pub withdrawable_cash: Option<Usdc>,
    /// USDC held as a token asset in the Alpaca account.
    #[ts(type = "string | null")]
    pub alpaca_usdc: Option<Usdc>,
    /// USDC observed at intermediate wallet locations between venues.
    pub inflight_cash: InFlightCash,
}

/// One chain's cash vault balance, in that chain's settlement stable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OnchainUsdcBalance {
    pub chain: ChainName,
    /// The chain's settlement stable (USDC, or USDG on Robinhood): the
    /// `usdc.symbol` above names the primary chain's only.
    pub symbol: String,
    #[ts(type = "string")]
    pub available: Usdc,
    #[ts(type = "string")]
    pub inflight: Usdc,
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
    /// No balances at all, labelled with `symbol` as the settlement stable.
    #[must_use]
    pub fn empty(symbol: &str) -> Self {
        Self {
            per_symbol: Vec::new(),
            usdc: UsdcInventory {
                symbol: symbol.to_string(),
                onchain_available: Usdc::ZERO,
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::ZERO,
                offchain_inflight: Usdc::ZERO,
                onchain_by_chain: Vec::new(),
                offchain_gross: None,
                withdrawable_cash: None,
                alpaca_usdc: None,
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
                onchain_by_chain: Vec::new(),
                inflight_equity: InFlightEquity {
                    base_wallet_unwrapped: FractionalShares::new(float!(3)),
                    base_wallet_wrapped: FractionalShares::new(float!(2)),
                },
            }],
            usdc: UsdcInventory {
                symbol: "USDC".to_string(),
                onchain_available: Usdc::new(float!(10000)),
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::new(float!(5000)),
                offchain_inflight: Usdc::new(float!(500)),
                onchain_by_chain: Vec::new(),
                offchain_gross: Some(Usdc::new(float!(6000))),
                withdrawable_cash: Some(Usdc::new(float!(4500))),
                alpaca_usdc: Some(Usdc::new(float!(125))),
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
        assert_eq!(usdc["symbol"], json!("USDC"));
        assert_eq!(usdc["onchainAvailable"], json!("10000"));
        assert_eq!(usdc["onchainInflight"], json!("0"));
        assert_eq!(usdc["offchainAvailable"], json!("5000"));
        assert_eq!(usdc["offchainInflight"], json!("500"));
        assert_eq!(usdc["offchainGross"], json!("6000"));
        assert_eq!(usdc["withdrawableCash"], json!("4500"));
        assert_eq!(usdc["alpacaUsdc"], json!("125"));
        assert_eq!(usdc["inflightCash"]["ethereumWallet"], json!("250"));
        assert_eq!(usdc["inflightCash"]["baseWallet"], json!("0"));
    }

    #[test]
    fn onchain_balances_by_chain_serialize_with_wire_chain_names() {
        let symbol_inventory = SymbolInventory {
            symbol: Symbol::new("TSLA").unwrap(),
            onchain_available: FractionalShares::new(float!(50)),
            onchain_inflight: FractionalShares::new(float!(5)),
            offchain_available: FractionalShares::ZERO,
            offchain_inflight: FractionalShares::ZERO,
            onchain_by_chain: vec![
                OnchainEquityBalance {
                    chain: ChainName::Base,
                    available: FractionalShares::new(float!(50)),
                    inflight: FractionalShares::new(float!(5)),
                },
                OnchainEquityBalance {
                    chain: ChainName::HyperEvm,
                    available: FractionalShares::new(float!(1.25)),
                    inflight: FractionalShares::ZERO,
                },
            ],
            inflight_equity: InFlightEquity {
                base_wallet_unwrapped: FractionalShares::ZERO,
                base_wallet_wrapped: FractionalShares::ZERO,
            },
        };
        let usdc_by_chain = vec![
            OnchainUsdcBalance {
                chain: ChainName::Base,
                symbol: "USDC".to_string(),
                available: Usdc::new(float!(10000)),
                inflight: Usdc::new(float!(250.5)),
            },
            OnchainUsdcBalance {
                chain: ChainName::Robinhood,
                symbol: "USDG".to_string(),
                available: Usdc::new(float!(300)),
                inflight: Usdc::ZERO,
            },
        ];

        let symbol_json = serde_json::to_value(&symbol_inventory).unwrap();
        let usdc_json = serde_json::to_value(&usdc_by_chain).unwrap();

        assert_eq!(
            symbol_json["onchainByChain"],
            json!([
                { "chain": "base", "available": "50", "inflight": "5" },
                { "chain": "hyperevm", "available": "1.25", "inflight": "0" },
            ])
        );
        assert_eq!(
            usdc_json,
            json!([
                { "chain": "base", "symbol": "USDC", "available": "10000", "inflight": "250.5" },
                { "chain": "robinhood", "symbol": "USDG", "available": "300", "inflight": "0" },
            ])
        );
    }

    #[test]
    fn empty_inventory_lists_no_onchain_usdc_by_chain() {
        let json = serde_json::to_value(Inventory::empty("USDC")).unwrap();

        assert_eq!(json["usdc"]["onchainByChain"], json!([]));
    }
}
