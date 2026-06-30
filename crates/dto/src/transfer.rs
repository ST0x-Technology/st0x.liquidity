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
    Completed {
        completed_at: DateTime<Utc>,
    },
    Failed {
        failed_at: DateTime<Utc>,
    },
    // Plain `//` comment, not `///` -- ts-rs propagates doc comments into the
    // generated TS binding and a field-level doc trips the dashboard's
    // type-aware eslint. Keep rationale here in Rust.
    Reconciled {
        reconciled_at: DateTime<Utc>,
        // The mint aggregate's Failed.reason is always a non-optional String, so
        // the DTO field is String (not Option<String>). Redemption and USDC
        // reconciled variants use Option<String> because their failure paths can
        // legitimately produce None (e.g. Timeout detection failure for
        // redemption, pre-existing-field replays for USDC).
        failure_reason: String,
        reconcile_reason: String,
    },
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
    Completed {
        completed_at: DateTime<Utc>,
    },
    Failed {
        failed_at: DateTime<Utc>,
    },
    // Plain `//` comment, not `///` -- ts-rs propagates doc comments into the
    // generated TS binding and a field-level doc trips the dashboard's
    // type-aware eslint. Keep rationale here in Rust.
    Reconciled {
        reconciled_at: DateTime<Utc>,
        failure_reason: Option<String>,
        reconcile_reason: String,
    },
}

/// Direction for USDC bridge transfers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
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
    Completed {
        completed_at: DateTime<Utc>,
    },
    Failed {
        failed_at: DateTime<Utc>,
        // True when the failure happened post-burn (`DepositFailed`, a post-burn
        // `BridgingFailed`, or a `BaseToAlpaca ConversionFailed`) -- the only USDC
        // failures `transfer reconcile --kind usdc` accepts. False for pre-burn
        // failures, which strand nothing and the CLI rejects, so the dashboard can
        // offer reconcile per-object only when this is true. NOTE: a plain `//`
        // comment, not `///` -- ts-rs emits doc comments into the generated TS
        // binding, and a field-level doc lands awkwardly inside the object type
        // literal (`{ failedAt: string, /** ... */ postBurn: boolean }`), which
        // trips the dashboard's type-aware eslint. Keep the rationale here in Rust.
        post_burn: bool,
    },
    // Plain `//` comment, not `///` -- ts-rs propagates doc comments into the
    // generated TS binding and a field-level doc trips the dashboard's
    // type-aware eslint. Keep rationale here in Rust.
    Reconciled {
        reconciled_at: DateTime<Utc>,
        failure_reason: Option<String>,
        reconcile_reason: String,
    },
}

impl TransferOperation {
    /// Whether this transfer is in a terminal state (completed, failed, or reconciled).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::EquityMint(op) => {
                use EquityMintStatus::*;

                match &op.status {
                    Completed { .. } | Failed { .. } | Reconciled { .. } => true,
                    Minting | Wrapping | Depositing => false,
                }
            }
            Self::EquityRedemption(op) => {
                use EquityRedemptionStatus::*;

                match &op.status {
                    Completed { .. } | Failed { .. } | Reconciled { .. } => true,
                    Withdrawing | Unwrapping | Sending | PendingConfirmation => false,
                }
            }
            Self::UsdcBridge(op) => {
                use UsdcBridgeStatus::*;

                match &op.status {
                    Completed { .. } | Failed { .. } | Reconciled { .. } => true,
                    Converting | Withdrawing | Bridging | Depositing => false,
                }
            }
        }
    }

    /// When this transfer was initiated.
    #[must_use]
    pub fn started_at(&self) -> DateTime<Utc> {
        match self {
            Self::EquityMint(op) => op.started_at,
            Self::EquityRedemption(op) => op.started_at,
            Self::UsdcBridge(op) => op.started_at,
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

    #[test]
    fn usdc_bridge_failed_serializes_post_burn_flag() {
        let post_burn = TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new("bridge-001"),
            direction: UsdcBridgeDirection::BaseToAlpaca,
            amount: Usdc::new(float!(1000)),
            status: UsdcBridgeStatus::Failed {
                failed_at: Utc::now(),
                post_burn: true,
            },
            started_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let serialized = serde_json::to_value(&post_burn).expect("serialization should succeed");
        assert_eq!(serialized["status"]["status"], json!("failed"));
        assert_eq!(serialized["status"]["postBurn"], json!(true));
        assert!(serialized["status"]["failedAt"].is_string());

        let pre_burn = TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new("bridge-002"),
            direction: UsdcBridgeDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            status: UsdcBridgeStatus::Failed {
                failed_at: Utc::now(),
                post_burn: false,
            },
            started_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let serialized = serde_json::to_value(&pre_burn).expect("serialization should succeed");
        assert_eq!(serialized["status"]["status"], json!("failed"));
        assert_eq!(serialized["status"]["postBurn"], json!(false));
        assert!(serialized["status"]["failedAt"].is_string());
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
        assert!(
            mint_operation(
                EquityMintStatus::Reconciled {
                    reconciled_at: now,
                    failure_reason: "timed out".to_string(),
                    reconcile_reason: "wrapped manually".to_string(),
                },
                now,
            )
            .is_terminal()
        );

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
        assert!(
            redemption_operation(
                EquityRedemptionStatus::Reconciled {
                    reconciled_at: now,
                    failure_reason: None,
                    reconcile_reason: "deposited manually".to_string(),
                },
                now,
            )
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
        assert!(
            bridge_operation(
                UsdcBridgeStatus::Failed {
                    failed_at: now,
                    post_burn: true
                },
                now
            )
            .is_terminal()
        );

        // `post_burn` gates whether reconcile is offered, not terminality: a
        // pre-burn failure is just as terminal as a post-burn one.
        assert!(
            bridge_operation(
                UsdcBridgeStatus::Failed {
                    failed_at: now,
                    post_burn: false
                },
                now
            )
            .is_terminal()
        );

        assert!(
            bridge_operation(
                UsdcBridgeStatus::Reconciled {
                    reconciled_at: now,
                    failure_reason: None,
                    reconcile_reason: "funds moved manually".to_string(),
                },
                now,
            )
            .is_terminal()
        );

        assert!(!bridge_operation(UsdcBridgeStatus::Converting, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Withdrawing, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Bridging, now).is_terminal());
        assert!(!bridge_operation(UsdcBridgeStatus::Depositing, now).is_terminal());
    }

    #[test]
    fn equity_mint_reconciled_serializes_correctly() {
        let reconciled_at = "2026-01-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let operation = TransferOperation::EquityMint(EquityMintOperation {
            id: Id::new("mint-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            status: EquityMintStatus::Reconciled {
                reconciled_at,
                failure_reason: "timed out".to_string(),
                reconcile_reason: "wrapped manually".to_string(),
            },
            started_at: reconciled_at,
            updated_at: reconciled_at,
        });

        let serialized = serde_json::to_value(&operation).expect("serialization should succeed");
        assert_eq!(serialized["status"]["status"], json!("reconciled"));
        assert_eq!(
            serialized["status"]["reconciledAt"],
            json!("2026-01-02T00:00:00Z")
        );
        assert_eq!(serialized["status"]["failureReason"], json!("timed out"));
        assert_eq!(
            serialized["status"]["reconcileReason"],
            json!("wrapped manually")
        );
    }

    #[test]
    fn equity_redemption_reconciled_serializes_correctly() {
        let reconciled_at = "2026-01-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let operation = TransferOperation::EquityRedemption(EquityRedemptionOperation {
            id: Id::new("redeem-001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(50)),
            status: EquityRedemptionStatus::Reconciled {
                reconciled_at,
                failure_reason: None,
                reconcile_reason: "deposited manually".to_string(),
            },
            started_at: reconciled_at,
            updated_at: reconciled_at,
        });

        let serialized = serde_json::to_value(&operation).expect("serialization should succeed");
        assert_eq!(serialized["status"]["status"], json!("reconciled"));
        assert_eq!(
            serialized["status"]["reconciledAt"],
            json!("2026-01-02T00:00:00Z")
        );
        assert_eq!(serialized["status"]["failureReason"], json!(null));
        assert_eq!(
            serialized["status"]["reconcileReason"],
            json!("deposited manually")
        );
    }

    #[test]
    fn usdc_bridge_reconciled_serializes_correctly() {
        let reconciled_at = "2026-01-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let operation = TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new("bridge-001"),
            direction: UsdcBridgeDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            status: UsdcBridgeStatus::Reconciled {
                reconciled_at,
                failure_reason: None,
                reconcile_reason: "funds moved manually".to_string(),
            },
            started_at: reconciled_at,
            updated_at: reconciled_at,
        });

        let serialized = serde_json::to_value(&operation).expect("serialization should succeed");
        assert_eq!(serialized["status"]["status"], json!("reconciled"));
        assert_eq!(
            serialized["status"]["reconciledAt"],
            json!("2026-01-02T00:00:00Z")
        );
        assert_eq!(serialized["status"]["failureReason"], json!(null));
        assert_eq!(
            serialized["status"]["reconcileReason"],
            json!("funds moved manually")
        );
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
