//! Transfer operation DTOs for cross-venue asset movements.
//!
//! Covers equity minting, equity redemption, and USDC bridging -- each
//! with its own operation struct, status enum, and ID tag type.

use chrono::{DateTime, Utc};
use serde::Serialize;
use ts_rs::TS;

use st0x_finance::{FractionalShares, Id, Symbol, Usdc};

/// Tag type for equity mint operation IDs.
pub enum EquityMintTag {}

/// Tag type for equity redemption operation IDs.
pub enum EquityRedemptionTag {}

/// Tag type for USDC bridge operation IDs.
pub enum UsdcBridgeTag {}

/// Transfer operation: a tagged union per operation type.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransferOperation {
    EquityMint(EquityMintOperation),
    EquityRedemption(EquityRedemptionOperation),
    UsdcBridge(UsdcBridgeOperation),
}

/// Minting tokenized equity from real shares.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EquityMintOperation {
    #[ts(type = "string")]
    pub id: Id<EquityMintTag>,
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub quantity: FractionalShares,
    pub status: EquityMintStatus,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle stages for an equity mint.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum EquityMintStatus {
    Minting,
    Wrapping,
    Depositing,
    Completed { completed_at: DateTime<Utc> },
    Failed { failed_at: DateTime<Utc> },
}

/// Redeeming tokenized equity back to real shares.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EquityRedemptionOperation {
    #[ts(type = "string")]
    pub id: Id<EquityRedemptionTag>,
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub quantity: FractionalShares,
    pub status: EquityRedemptionStatus,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle stages for an equity redemption.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum EquityRedemptionStatus {
    Withdrawing,
    Unwrapping,
    Sending,
    PendingConfirmation,
    Completed { completed_at: DateTime<Utc> },
    Failed { failed_at: DateTime<Utc> },
}

/// Direction for USDC bridge transfers.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum UsdcBridgeDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

/// Bridging USDC between venues (onchain <-> offchain).
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UsdcBridgeOperation {
    #[ts(type = "string")]
    pub id: Id<UsdcBridgeTag>,
    pub direction: UsdcBridgeDirection,
    #[ts(type = "string")]
    pub amount: Usdc,
    pub status: UsdcBridgeStatus,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle stages for a USDC bridge transfer.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum UsdcBridgeStatus {
    Converting,
    Withdrawing,
    Bridging,
    Depositing,
    Completed { completed_at: DateTime<Utc> },
    Failed { failed_at: DateTime<Utc> },
}

impl TransferOperation {
    /// Whether this transfer is in a terminal state (completed or failed).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::EquityMint(op) => {
                use EquityMintStatus::*;

                match &op.status {
                    Completed { .. } | Failed { .. } => true,
                    Minting | Wrapping | Depositing => false,
                }
            }
            Self::EquityRedemption(op) => {
                use EquityRedemptionStatus::*;

                match &op.status {
                    Completed { .. } | Failed { .. } => true,
                    Withdrawing | Unwrapping | Sending | PendingConfirmation => false,
                }
            }
            Self::UsdcBridge(op) => {
                use UsdcBridgeStatus::*;

                match &op.status {
                    Completed { .. } | Failed { .. } => true,
                    Converting | Withdrawing | Bridging | Depositing => false,
                }
            }
        }
    }

    /// The last time this transfer was updated.
    #[must_use]
    pub fn updated_at(&self) -> DateTime<Utc> {
        match self {
            Self::EquityMint(op) => op.updated_at,
            Self::EquityRedemption(op) => op.updated_at,
            Self::UsdcBridge(op) => op.updated_at,
        }
    }
}

/// Warning emitted when transfer data is partially loaded.
///
/// Variant names encode the category — no separate `TransferCategory`
/// enum needed. Replay failures carry the dto `Id<Tag>` — the internal
/// aggregate ID types convert to these at the boundary via `to_string()`.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum TransferWarning {
    MintCategoryUnavailable,
    RedemptionCategoryUnavailable,
    BridgeCategoryUnavailable,
    MintReplayFailed {
        #[ts(type = "string")]
        id: Id<EquityMintTag>,
    },
    RedemptionReplayFailed {
        #[ts(type = "string")]
        id: Id<EquityRedemptionTag>,
    },
    BridgeReplayFailed {
        #[ts(type = "string")]
        id: Id<UsdcBridgeTag>,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn transfer_operation_equity_mint_serializes_with_kind_tag() {
        let operation = TransferOperation::EquityMint(EquityMintOperation {
            id: Id::new("mint-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            status: EquityMintStatus::Minting,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let serialized = serde_json::to_value(&operation).expect("serialization should succeed");
        assert_eq!(serialized["kind"], json!("equity_mint"));
        assert_eq!(serialized["status"], json!({"status": "minting"}));
        assert_eq!(serialized["symbol"], json!("AAPL"));
    }

    #[test]
    fn transfer_operation_usdc_bridge_serializes_with_kind_tag() {
        let operation = TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new("bridge-001"),
            direction: UsdcBridgeDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            status: UsdcBridgeStatus::Completed {
                completed_at: Utc::now(),
            },
            started_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let serialized = serde_json::to_value(&operation).expect("serialization should succeed");
        assert_eq!(serialized["kind"], json!("usdc_bridge"));
        assert_eq!(serialized["status"]["status"], json!("completed"));
        assert!(serialized["status"]["completedAt"].is_string());
        assert_eq!(serialized["direction"], json!("alpaca_to_base"));
    }

    fn mint_operation(status: EquityMintStatus, updated_at: DateTime<Utc>) -> TransferOperation {
        TransferOperation::EquityMint(EquityMintOperation {
            id: Id::new("mint-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            status,
            started_at: Utc::now(),
            updated_at,
        })
    }

    fn redemption_operation(
        status: EquityRedemptionStatus,
        updated_at: DateTime<Utc>,
    ) -> TransferOperation {
        TransferOperation::EquityRedemption(EquityRedemptionOperation {
            id: Id::new("redeem-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            status,
            started_at: Utc::now(),
            updated_at,
        })
    }

    fn bridge_operation(status: UsdcBridgeStatus, updated_at: DateTime<Utc>) -> TransferOperation {
        TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new("bridge-001"),
            direction: UsdcBridgeDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            status,
            started_at: Utc::now(),
            updated_at,
        })
    }

    #[test]
    fn is_terminal_equity_mint() {
        let now = Utc::now();

        assert!(
            mint_operation(EquityMintStatus::Completed { completed_at: now }, now).is_terminal()
        );
        assert!(mint_operation(EquityMintStatus::Failed { failed_at: now }, now).is_terminal());

        assert!(!mint_operation(EquityMintStatus::Minting, now).is_terminal());
        assert!(!mint_operation(EquityMintStatus::Wrapping, now).is_terminal());
        assert!(!mint_operation(EquityMintStatus::Depositing, now).is_terminal());
    }

    #[test]
    fn is_terminal_equity_redemption() {
        let now = Utc::now();

        assert!(
            redemption_operation(EquityRedemptionStatus::Completed { completed_at: now }, now)
                .is_terminal()
        );
        assert!(
            redemption_operation(EquityRedemptionStatus::Failed { failed_at: now }, now)
                .is_terminal()
        );

        assert!(!redemption_operation(EquityRedemptionStatus::Withdrawing, now).is_terminal());
        assert!(!redemption_operation(EquityRedemptionStatus::Unwrapping, now).is_terminal());
        assert!(!redemption_operation(EquityRedemptionStatus::Sending, now).is_terminal());
        assert!(
            !redemption_operation(EquityRedemptionStatus::PendingConfirmation, now).is_terminal()
        );
    }

    #[test]
    fn is_terminal_usdc_bridge() {
        let now = Utc::now();

        assert!(
            bridge_operation(UsdcBridgeStatus::Completed { completed_at: now }, now).is_terminal()
        );
        assert!(bridge_operation(UsdcBridgeStatus::Failed { failed_at: now }, now).is_terminal());

        assert!(!bridge_operation(UsdcBridgeStatus::Converting, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Withdrawing, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Bridging, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Depositing, now).is_terminal());
    }

    #[test]
    fn updated_at_returns_inner_value() {
        let timestamp = Utc::now();

        assert_eq!(
            mint_operation(EquityMintStatus::Minting, timestamp).updated_at(),
            timestamp
        );
        assert_eq!(
            redemption_operation(EquityRedemptionStatus::Withdrawing, timestamp).updated_at(),
            timestamp
        );
        assert_eq!(
            bridge_operation(UsdcBridgeStatus::Converting, timestamp).updated_at(),
            timestamp
        );
    }
}
