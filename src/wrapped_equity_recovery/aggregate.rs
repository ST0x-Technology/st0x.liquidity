//! Aggregate recording automated recovery of wrapped equity tokens
//! (wtSTOCK) found on the Base wallet outside the Raindex vault.
//!
//! See SPEC.md, section "WrappedEquityRecovery Aggregate" for the full
//! specification and rationale.
//!
//! # State Flow
//!
//! ```text
//!                Detect
//!                  v
//!               Detected ----+
//!               /  |   \     |
//!  DispatchToMint  |   DispatchToRedemption
//!                  |
//!         SubmitOrphanDeposit
//!                  v
//!     OrphanDepositSubmitted
//!                  v
//!      ConfirmOrphanDeposit
//!                  v
//!          OrphanDeposited
//! ```
//!
//! The dispatch-success states (`DispatchedToMint`, `DispatchedToRedemption`,
//! `OrphanDeposited`) are themselves terminal -- no separate `Completed`
//! state. Any non-terminal state can receive `FailRecovery`, transitioning
//! to `Failed`.
//!
//! The handlers are pure event recorders (`Jobs = Nil`): the recovery job
//! ([`WrappedEquityRecoveryJob`](super::job::WrappedEquityRecoveryJob)) performs
//! the raindex/wrapper/transfer I/O and sends the matching command carrying the
//! outcome. A terminal service failure is recorded by sending `FailRecovery`; a
//! retryable failure surfaces as a job error so apalis retries from the same
//! persisted state. The orphan path persists each step independently: once
//! `OrphanDepositSubmitted` lands, a crash resumes from the persisted tx hash
//! and only re-confirms, never re-depositing. The submit step itself is NOT
//! double-deposit-safe -- a crash between `submit_deposit` returning and
//! `OrphanDepositSubmitted` landing re-runs `submit_deposit` on resume. That
//! dual-write window is architecturally unavoidable (no XA across the chain and
//! SQLite) and is the same gap the prior atomic-handler design had.

use alloy::primitives::TxHash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, JobQueue, Nil};
use st0x_execution::{FractionalShares, Symbol};
use st0x_raindex::Raindex;
use st0x_tokenization::IssuerRequestId;
use st0x_wrapper::Wrapper;

use crate::equity_redemption::RedemptionAggregateId;
use crate::rebalancing::equity::CrossVenueEquityTransfer;
use crate::vault_lookup::VaultLookup;

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

/// Onchain/transfer dependencies the recovery job uses to perform the
/// recovery's side effects before recording the outcome through the aggregate's
/// pure commands. (The aggregate itself takes `Jobs = Nil`.)
#[derive(Clone)]
pub(crate) struct WrappedEquityRecoveryServices {
    pub(crate) raindex: Arc<dyn Raindex>,
    pub(crate) vault_lookup: Arc<dyn VaultLookup>,
    pub(crate) wrapper: Arc<dyn Wrapper>,
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
}

/// Domain errors returned from the aggregate's pure `initialize`/`transition`
/// handlers -- state-machine guards only. The handlers perform no I/O; the
/// recovery job records terminal service failures as `RecoveryFailed` events
/// and surfaces retryable service failures as its own job errors.
#[derive(Debug, Clone, Serialize, Deserialize, Error, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecoveryError {
    #[error("recovery already initialized")]
    AlreadyInitialized,
    #[error("recovery not yet initialized; only Detect is valid")]
    Uninitialized,
    #[error("command not valid from state {state:?}")]
    InvalidTransition { state: Box<WrappedEquityRecovery> },
    #[error("recovery is already in terminal state")]
    Terminal,
}

/// Commands are pure event recorders: the recovery job
/// ([`WrappedEquityRecoveryJob`](super::job::WrappedEquityRecoveryJob)) performs
/// the onchain/transfer I/O and sends the matching command carrying the outcome.
/// A terminal service failure is recorded by sending `FailRecovery`; a retryable
/// failure surfaces as a job error (no command) so apalis retries from the same
/// persisted state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecoveryCommand {
    /// Initial command. Records the detection trigger.
    Detect {
        symbol: Symbol,
        shares: FractionalShares,
    },

    /// The job resumed an active mint successfully; record the dispatch.
    DispatchToMint { mint_id: IssuerRequestId },

    /// The job resumed an active redemption successfully; record the dispatch.
    DispatchToRedemption {
        redemption_id: RedemptionAggregateId,
    },

    /// Orphan path step 1. Records the submitted Raindex deposit tx hash the job
    /// got from `raindex.submit_deposit` (after resolving the wrapped token and
    /// vault).
    SubmitOrphanDeposit { vault_deposit_tx_hash: TxHash },

    /// Orphan path step 2. Records the confirmed deposit (the tx hash is read
    /// from state); the job has already confirmed it via `raindex.confirm_tx`.
    ConfirmOrphanDeposit,

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

    Failed {
        symbol: Symbol,
        shares: FractionalShares,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl WrappedEquityRecovery {
    pub(super) fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::DispatchedToMint { .. }
                | Self::DispatchedToRedemption { .. }
                | Self::OrphanDeposited { .. }
                | Self::Failed { .. }
        )
    }
}

impl EventSourced for WrappedEquityRecovery {
    type Id = WrappedEquityRecoveryId;
    type Event = WrappedEquityRecoveryEvent;
    type Command = WrappedEquityRecoveryCommand;
    type Error = WrappedEquityRecoveryError;
    type Jobs = Nil;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "WrappedEquityRecovery";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            WrappedEquityRecoveryEvent::Detected {
                symbol,
                shares,
                detected_at,
            } => Some(Self::Detected {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use WrappedEquityRecoveryEvent::*;

        Ok(match (entity, event) {
            (
                Self::Detected {
                    symbol,
                    shares,
                    detected_at,
                },
                DispatchedToMint {
                    mint_id,
                    dispatched_at,
                },
            ) => Some(Self::DispatchedToMint {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                mint_id: mint_id.clone(),
                dispatched_at: *dispatched_at,
            }),

            (
                Self::Detected {
                    symbol,
                    shares,
                    detected_at,
                },
                DispatchedToRedemption {
                    redemption_id,
                    dispatched_at,
                },
            ) => Some(Self::DispatchedToRedemption {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                redemption_id: redemption_id.clone(),
                dispatched_at: *dispatched_at,
            }),

            (
                Self::Detected {
                    symbol,
                    shares,
                    detected_at,
                },
                OrphanDepositSubmitted {
                    vault_deposit_tx_hash,
                    submitted_at,
                },
            ) => Some(Self::OrphanDepositSubmitted {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                submitted_at: *submitted_at,
            }),

            (
                Self::OrphanDepositSubmitted {
                    symbol,
                    shares,
                    detected_at,
                    submitted_at,
                    ..
                },
                OrphanDeposited {
                    vault_deposit_tx_hash,
                    deposited_at,
                },
            ) => Some(Self::OrphanDeposited {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                submitted_at: *submitted_at,
                deposited_at: *deposited_at,
            }),

            (
                Self::Detected { symbol, shares, .. }
                | Self::OrphanDepositSubmitted { symbol, shares, .. },
                RecoveryFailed { reason, failed_at },
            ) => Some(Self::Failed {
                symbol: symbol.clone(),
                shares: *shares,
                reason: reason.clone(),
                failed_at: *failed_at,
            }),

            _ => None,
        })
    }

    fn initialize(
        command: Self::Command,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            WrappedEquityRecoveryCommand::Detect { symbol, shares } => {
                Ok(vec![WrappedEquityRecoveryEvent::Detected {
                    symbol,
                    shares,
                    detected_at: Utc::now(),
                }])
            }
            _ => Err(WrappedEquityRecoveryError::Uninitialized),
        }
    }

    fn transition(
        &self,
        command: Self::Command,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use WrappedEquityRecoveryCommand::*;

        if self.is_terminal() {
            return Err(WrappedEquityRecoveryError::Terminal);
        }

        match (self, command) {
            (_, Detect { .. }) => Err(WrappedEquityRecoveryError::AlreadyInitialized),

            (Self::Detected { .. }, DispatchToMint { mint_id }) => {
                Ok(vec![WrappedEquityRecoveryEvent::DispatchedToMint {
                    mint_id,
                    dispatched_at: Utc::now(),
                }])
            }

            (Self::Detected { .. }, DispatchToRedemption { redemption_id }) => {
                Ok(vec![WrappedEquityRecoveryEvent::DispatchedToRedemption {
                    redemption_id,
                    dispatched_at: Utc::now(),
                }])
            }

            (
                Self::Detected { .. },
                SubmitOrphanDeposit {
                    vault_deposit_tx_hash,
                },
            ) => Ok(vec![WrappedEquityRecoveryEvent::OrphanDepositSubmitted {
                vault_deposit_tx_hash,
                submitted_at: Utc::now(),
            }]),

            (
                Self::OrphanDepositSubmitted {
                    vault_deposit_tx_hash,
                    ..
                },
                ConfirmOrphanDeposit,
            ) => Ok(vec![WrappedEquityRecoveryEvent::OrphanDeposited {
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                deposited_at: Utc::now(),
            }]),

            (
                Self::Detected { .. } | Self::OrphanDepositSubmitted { .. },
                FailRecovery { reason },
            ) => Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason,
                failed_at: Utc::now(),
            }]),

            (state, _) => Err(WrappedEquityRecoveryError::InvalidTransition {
                state: Box::new(state.clone()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{TxHash, fixed_bytes};
    use chrono::Utc;
    use rain_math_float::Float;
    use uuid::Uuid;

    use st0x_event_sorcery::EventSourced;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_tokenization::issuer_request_id;

    use crate::equity_redemption::redemption_aggregate_id;

    use super::*;

    const FAKE_TX_HASH: TxHash = TxHash::new(
        fixed_bytes!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef").0,
    );

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

    #[test]
    fn detect_initializes_aggregate_into_detected_state() {
        let events = WrappedEquityRecovery::initialize(
            WrappedEquityRecoveryCommand::Detect {
                symbol: aapl(),
                shares: one_share(),
            },
            &mut JobQueue::default(),
        )
        .expect("Detect should initialize");

        let [WrappedEquityRecoveryEvent::Detected { symbol, .. }] = events.as_slice() else {
            panic!("Expected single Detected event, got {events:?}");
        };
        assert_eq!(*symbol, aapl());

        let originated = WrappedEquityRecovery::originate(&events[0])
            .expect("Detected event should originate aggregate");
        assert!(
            matches!(originated, WrappedEquityRecovery::Detected { .. }),
            "Expected Detected state, got {originated:?}",
        );
    }

    #[test]
    fn detect_rejected_once_initialized() {
        let error = detected_state()
            .transition(
                WrappedEquityRecoveryCommand::Detect {
                    symbol: aapl(),
                    shares: one_share(),
                },
                &mut JobQueue::default(),
            )
            .expect_err("Detect on an initialized aggregate should error");

        assert!(
            matches!(error, WrappedEquityRecoveryError::AlreadyInitialized),
            "Expected AlreadyInitialized, got {error:?}",
        );
    }

    #[test]
    fn dispatch_to_mint_records_dispatch_with_mint_id() {
        let mint_id = issuer_request_id("ISS-1");

        let events = detected_state()
            .transition(
                WrappedEquityRecoveryCommand::DispatchToMint {
                    mint_id: mint_id.clone(),
                },
                &mut JobQueue::default(),
            )
            .expect("DispatchToMint should succeed from Detected");

        let [
            WrappedEquityRecoveryEvent::DispatchedToMint {
                mint_id: recorded, ..
            },
        ] = events.as_slice()
        else {
            panic!("Expected single DispatchedToMint event, got {events:?}");
        };
        assert_eq!(*recorded, mint_id);
    }

    #[test]
    fn dispatch_to_redemption_records_dispatch_with_redemption_id() {
        let redemption_id = redemption_aggregate_id("redemption-1");

        let events = detected_state()
            .transition(
                WrappedEquityRecoveryCommand::DispatchToRedemption {
                    redemption_id: redemption_id.clone(),
                },
                &mut JobQueue::default(),
            )
            .expect("DispatchToRedemption should succeed from Detected");

        let [
            WrappedEquityRecoveryEvent::DispatchedToRedemption {
                redemption_id: recorded,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("Expected single DispatchedToRedemption event, got {events:?}");
        };
        assert_eq!(*recorded, redemption_id);
    }

    #[test]
    fn submit_orphan_deposit_records_submitted_tx_hash() {
        let events = detected_state()
            .transition(
                WrappedEquityRecoveryCommand::SubmitOrphanDeposit {
                    vault_deposit_tx_hash: FAKE_TX_HASH,
                },
                &mut JobQueue::default(),
            )
            .expect("SubmitOrphanDeposit should succeed from Detected");

        let [
            WrappedEquityRecoveryEvent::OrphanDepositSubmitted {
                vault_deposit_tx_hash,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("Expected single OrphanDepositSubmitted event, got {events:?}");
        };
        assert_eq!(*vault_deposit_tx_hash, FAKE_TX_HASH);
    }

    #[test]
    fn confirm_orphan_deposit_records_deposit_from_state_tx_hash() {
        let submitted = WrappedEquityRecovery::OrphanDepositSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            vault_deposit_tx_hash: FAKE_TX_HASH,
            submitted_at: Utc::now(),
        };

        let events = submitted
            .transition(
                WrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
                &mut JobQueue::default(),
            )
            .expect("ConfirmOrphanDeposit should succeed from OrphanDepositSubmitted");

        let [
            WrappedEquityRecoveryEvent::OrphanDeposited {
                vault_deposit_tx_hash,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("Expected single OrphanDeposited event, got {events:?}");
        };
        assert_eq!(*vault_deposit_tx_hash, FAKE_TX_HASH);
    }

    #[test]
    fn fail_recovery_from_detected_records_failure() {
        let events = detected_state()
            .transition(
                WrappedEquityRecoveryCommand::FailRecovery {
                    reason: "boom".to_string(),
                },
                &mut JobQueue::default(),
            )
            .expect("FailRecovery should succeed from Detected");

        let [WrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice() else {
            panic!("Expected single RecoveryFailed event, got {events:?}");
        };
        assert_eq!(reason, "boom");
    }

    #[test]
    fn fail_recovery_from_orphan_deposit_submitted_records_failure() {
        let submitted = WrappedEquityRecovery::OrphanDepositSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            vault_deposit_tx_hash: FAKE_TX_HASH,
            submitted_at: Utc::now(),
        };

        let events = submitted
            .transition(
                WrappedEquityRecoveryCommand::FailRecovery {
                    reason: "deposit confirmation dropped".to_string(),
                },
                &mut JobQueue::default(),
            )
            .expect("FailRecovery should succeed from OrphanDepositSubmitted");

        let [WrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice() else {
            panic!("Expected single RecoveryFailed event, got {events:?}");
        };
        assert_eq!(
            reason, "deposit confirmation dropped",
            "FailRecovery must carry the operator-supplied reason through unchanged",
        );
    }

    #[test]
    fn confirm_orphan_deposit_rejected_from_detected() {
        let error = detected_state()
            .transition(
                WrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
                &mut JobQueue::default(),
            )
            .expect_err("ConfirmOrphanDeposit from Detected should be an invalid transition");

        assert!(
            matches!(error, WrappedEquityRecoveryError::InvalidTransition { .. }),
            "Expected InvalidTransition, got {error:?}",
        );
    }

    #[test]
    fn fail_recovery_rejected_from_terminal_state() {
        let deposited = WrappedEquityRecovery::OrphanDeposited {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            vault_deposit_tx_hash: FAKE_TX_HASH,
            submitted_at: Utc::now(),
            deposited_at: Utc::now(),
        };

        let error = deposited
            .transition(
                WrappedEquityRecoveryCommand::FailRecovery {
                    reason: "should be rejected".to_string(),
                },
                &mut JobQueue::default(),
            )
            .expect_err("FailRecovery on a terminal state should error");

        assert!(
            matches!(error, WrappedEquityRecoveryError::Terminal),
            "Expected Terminal error, got {error:?}",
        );
    }

    #[test]
    fn id_roundtrips_through_string() {
        let id = WrappedEquityRecoveryId(Uuid::new_v4());
        let parsed = id.to_string().parse::<WrappedEquityRecoveryId>().unwrap();
        assert_eq!(id, parsed);
    }
}
