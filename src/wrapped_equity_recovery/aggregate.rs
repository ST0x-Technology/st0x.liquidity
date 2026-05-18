//! Aggregate recording automated recovery of wrapped equity tokens
//! (wtSTOCK) found on the Base wallet outside the Raindex vault.
//!
//! See SPEC.md, section "WrappedEquityRecovery Aggregate" for the
//! full specification and rationale.
//!
//! # State Flow
//!
//! ```text
//!                  Detect
//!                     v
//!                Detected ----+
//!                /  |  \      |
//!  DispatchToMint  | DispatchToRedemption
//!                  |
//!         SubmitOrphanDeposit
//!                  v
//!     OrphanDepositSubmitted
//!                  v
//!      ConfirmOrphanDeposit
//!                  v
//!          OrphanDeposited
//!                  v
//!          CompleteRecovery
//!                  v
//!              Completed
//! ```
//!
//! Any non-terminal state can also receive `FailRecovery`, transitioning
//! the aggregate to `Failed`. The dispatched-to-{mint,redemption} states
//! complete once the underlying aggregate (TokenizedEquityMint /
//! EquityRedemption) has driven its own state machine forward; this
//! aggregate only records the dispatch decision, not the downstream work.

use std::str::FromStr;

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::{FractionalShares, Symbol};

use crate::equity_redemption::RedemptionAggregateId;
use crate::tokenized_equity_mint::IssuerRequestId;

/// Aggregate identifier. Each detection creates a fresh UUID; multiple
/// recoveries for the same symbol are independent aggregates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct WrappedEquityRecoveryId(pub(crate) Uuid);

impl std::fmt::Display for WrappedEquityRecoveryId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for WrappedEquityRecoveryId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Outcome carried in the terminal `RecoveryCompleted` event so consumers
/// can tell which recovery path the aggregate took.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RecoveryOutcome {
    MintResumed {
        mint_id: IssuerRequestId,
    },
    RedemptionResumed {
        redemption_id: RedemptionAggregateId,
    },
    OrphanDeposited {
        vault_deposit_tx_hash: TxHash,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Error, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecoveryError {
    #[error("recovery already initialized")]
    AlreadyInitialized,

    #[error("command not valid from current state {state}")]
    InvalidTransition { state: String },

    #[error("recovery is already in terminal state")]
    Terminal,

    #[error("symbol/shares mismatch between command and state")]
    Mismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecoveryCommand {
    /// Initial command. Records the detection trigger.
    Detect {
        symbol: Symbol,
        shares: FractionalShares,
    },
    /// Recovery has an active mint to resume. Records the dispatch decision;
    /// the actual deposit is driven by the existing `TokenizedEquityMint`
    /// aggregate via `CrossVenueEquityTransfer::resume_mint`.
    DispatchToMint { mint_id: IssuerRequestId },
    /// Recovery has an active redemption to resume. Records the dispatch
    /// decision; the actual unwrap is driven by the existing
    /// `EquityRedemption` aggregate via
    /// `CrossVenueEquityTransfer::resume_redemption`.
    DispatchToRedemption {
        redemption_id: RedemptionAggregateId,
    },
    /// Orphan path. Submits the Raindex deposit via `Services::raindex`.
    /// Side effect runs inside the handler so the event is emitted iff
    /// the deposit was actually submitted onchain.
    SubmitOrphanDeposit,
    /// Orphan path. Waits for the deposit transaction to confirm.
    ConfirmOrphanDeposit { vault_deposit_tx_hash: TxHash },
    /// Closes the recovery with the dispatched-or-deposited outcome.
    CompleteRecovery { outcome: RecoveryOutcome },
    /// Marks the recovery as failed with the supplied reason.
    FailRecovery { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecoveryEvent {
    Detected {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
    },
    DispatchedToMint {
        mint_id: IssuerRequestId,
        dispatched_at: DateTime<Utc>,
    },
    DispatchedToRedemption {
        redemption_id: RedemptionAggregateId,
        dispatched_at: DateTime<Utc>,
    },
    OrphanDepositSubmitted {
        vault_deposit_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },
    OrphanDeposited {
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },
    RecoveryCompleted {
        outcome: RecoveryOutcome,
        completed_at: DateTime<Utc>,
    },
    RecoveryFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for WrappedEquityRecoveryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Detected { .. } => "WrappedEquityRecoveryEvent::Detected",
            Self::DispatchedToMint { .. } => "WrappedEquityRecoveryEvent::DispatchedToMint",
            Self::DispatchedToRedemption { .. } => {
                "WrappedEquityRecoveryEvent::DispatchedToRedemption"
            }
            Self::OrphanDepositSubmitted { .. } => {
                "WrappedEquityRecoveryEvent::OrphanDepositSubmitted"
            }
            Self::OrphanDeposited { .. } => "WrappedEquityRecoveryEvent::OrphanDeposited",
            Self::RecoveryCompleted { .. } => "WrappedEquityRecoveryEvent::RecoveryCompleted",
            Self::RecoveryFailed { .. } => "WrappedEquityRecoveryEvent::RecoveryFailed",
        }
        .to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecovery {
    Detected {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
    },
    DispatchedToMint {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        mint_id: IssuerRequestId,
        dispatched_at: DateTime<Utc>,
    },
    DispatchedToRedemption {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        redemption_id: RedemptionAggregateId,
        dispatched_at: DateTime<Utc>,
    },
    OrphanDepositSubmitted {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        vault_deposit_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },
    OrphanDeposited {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        vault_deposit_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
        deposited_at: DateTime<Utc>,
    },
    Completed {
        symbol: Symbol,
        shares: FractionalShares,
        outcome: RecoveryOutcome,
        completed_at: DateTime<Utc>,
    },
    Failed {
        symbol: Symbol,
        shares: FractionalShares,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

#[async_trait]
impl EventSourced for WrappedEquityRecovery {
    type Id = WrappedEquityRecoveryId;
    type Event = WrappedEquityRecoveryEvent;
    type Command = WrappedEquityRecoveryCommand;
    type Error = WrappedEquityRecoveryError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "WrappedEquityRecovery";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(_event: &Self::Event) -> Option<Self> {
        todo!("originate WrappedEquityRecovery from Detected event")
    }

    fn evolve(_entity: &Self, _event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        todo!("evolve WrappedEquityRecovery across state transitions")
    }

    async fn initialize(
        _command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        todo!("initialize: only `Detect` is valid here")
    }

    async fn transition(
        &self,
        _command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        todo!("transition: route command based on (state, command)")
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, fixed_bytes};
    use chrono::Utc;
    use rain_math_float::Float;

    use st0x_event_sorcery::EventSourced;
    use st0x_execution::{FractionalShares, Symbol};

    use super::*;

    fn services() -> () {}

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn one_share() -> FractionalShares {
        FractionalShares::new(Float::parse("1".to_string()).unwrap())
    }

    fn detected_state() -> WrappedEquityRecovery {
        WrappedEquityRecovery::Detected {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        }
    }

    fn fake_tx_hash() -> TxHash {
        TxHash::from(fixed_bytes!(
            "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        ))
    }

    #[tokio::test]
    async fn detect_initializes_aggregate_into_detected_state() {
        let events = WrappedEquityRecovery::initialize(
            WrappedEquityRecoveryCommand::Detect {
                symbol: aapl(),
                shares: one_share(),
            },
            &services(),
        )
        .await
        .expect("Detect should initialize");

        assert!(
            matches!(events.as_slice(), [WrappedEquityRecoveryEvent::Detected { symbol, .. }] if *symbol == aapl()),
            "Expected single Detected event, got {events:?}",
        );

        let originated = WrappedEquityRecovery::originate(&events[0])
            .expect("Detected event should originate aggregate");
        assert!(
            matches!(originated, WrappedEquityRecovery::Detected { .. }),
            "Expected Detected state, got {originated:?}",
        );
    }

    #[tokio::test]
    async fn dispatch_to_mint_records_decision_from_detected_state() {
        let detected = detected_state();
        let mint_id = IssuerRequestId::new("issuer-123");

        let events = detected
            .transition(
                WrappedEquityRecoveryCommand::DispatchToMint {
                    mint_id: mint_id.clone(),
                },
                &services(),
            )
            .await
            .expect("Dispatch should succeed from Detected");

        assert!(
            matches!(
                events.as_slice(),
                [WrappedEquityRecoveryEvent::DispatchedToMint { mint_id: dispatched_mint, .. }]
                    if *dispatched_mint == mint_id,
            ),
            "Expected single DispatchedToMint event, got {events:?}",
        );
    }

    #[tokio::test]
    async fn dispatch_to_redemption_records_decision_from_detected_state() {
        let detected = detected_state();
        let redemption_id = RedemptionAggregateId("redeem-1".to_string());

        let events = detected
            .transition(
                WrappedEquityRecoveryCommand::DispatchToRedemption {
                    redemption_id: redemption_id.clone(),
                },
                &services(),
            )
            .await
            .expect("Dispatch should succeed from Detected");

        assert!(
            matches!(
                events.as_slice(),
                [WrappedEquityRecoveryEvent::DispatchedToRedemption {
                    redemption_id: dispatched_redemption,
                    ..
                }] if *dispatched_redemption == redemption_id,
            ),
            "Expected single DispatchedToRedemption event, got {events:?}",
        );
    }

    #[tokio::test]
    async fn submit_orphan_deposit_emits_event_with_returned_tx_hash() {
        let detected = detected_state();
        let services = services();

        let events = detected
            .transition(WrappedEquityRecoveryCommand::SubmitOrphanDeposit, &services)
            .await
            .expect("SubmitOrphanDeposit should succeed from Detected");

        assert!(
            matches!(
                events.as_slice(),
                [WrappedEquityRecoveryEvent::OrphanDepositSubmitted { .. }],
            ),
            "Expected single OrphanDepositSubmitted event, got {events:?}",
        );
    }

    #[tokio::test]
    async fn confirm_orphan_deposit_completes_orphan_branch() {
        let submitted = WrappedEquityRecovery::OrphanDepositSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            vault_deposit_tx_hash: fake_tx_hash(),
            submitted_at: Utc::now(),
        };

        let events = submitted
            .transition(
                WrappedEquityRecoveryCommand::ConfirmOrphanDeposit {
                    vault_deposit_tx_hash: fake_tx_hash(),
                },
                &services(),
            )
            .await
            .expect("ConfirmOrphanDeposit should succeed from OrphanDepositSubmitted");

        assert!(
            matches!(
                events.as_slice(),
                [WrappedEquityRecoveryEvent::OrphanDeposited { .. }],
            ),
            "Expected single OrphanDeposited event, got {events:?}",
        );
    }

    #[tokio::test]
    async fn complete_recovery_marks_terminal_success() {
        let deposited = WrappedEquityRecovery::OrphanDeposited {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            vault_deposit_tx_hash: fake_tx_hash(),
            submitted_at: Utc::now(),
            deposited_at: Utc::now(),
        };

        let events = deposited
            .transition(
                WrappedEquityRecoveryCommand::CompleteRecovery {
                    outcome: RecoveryOutcome::OrphanDeposited {
                        vault_deposit_tx_hash: fake_tx_hash(),
                    },
                },
                &services(),
            )
            .await
            .expect("CompleteRecovery should succeed from OrphanDeposited");

        assert!(
            matches!(
                events.as_slice(),
                [WrappedEquityRecoveryEvent::RecoveryCompleted { .. }],
            ),
            "Expected single RecoveryCompleted event, got {events:?}",
        );
    }

    #[tokio::test]
    async fn fail_recovery_rejected_from_terminal_state() {
        let completed = WrappedEquityRecovery::Completed {
            symbol: aapl(),
            shares: one_share(),
            outcome: RecoveryOutcome::OrphanDeposited {
                vault_deposit_tx_hash: fake_tx_hash(),
            },
            completed_at: Utc::now(),
        };

        let error = completed
            .transition(
                WrappedEquityRecoveryCommand::FailRecovery {
                    reason: "should be rejected".to_string(),
                },
                &services(),
            )
            .await
            .expect_err("FailRecovery on Completed should error");

        assert!(
            matches!(error, WrappedEquityRecoveryError::Terminal),
            "Expected Terminal error, got {error:?}",
        );
    }

    #[test]
    fn id_roundtrips_through_string() {
        let id = WrappedEquityRecoveryId(uuid::Uuid::new_v4());
        let parsed = id.to_string().parse::<WrappedEquityRecoveryId>().unwrap();
        assert_eq!(id, parsed);
    }
}
