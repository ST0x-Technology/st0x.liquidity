//! Aggregate recording automated recovery of unwrapped equity tokens
//! (tSTOCK) found on the Base wallet.
//!
//! See SPEC.md, section "Unwrapped Equity Recovery" for the full
//! specification and rationale.
//!
//! # State Flow
//!
//! ```text
//!                  Detect
//!                    v
//!                Detected ----+
//!                /   |  \     |
//!  DispatchToMint    |   DispatchToRedemption
//!                    |
//!           SubmitOrphanWrap
//!                    v
//!         OrphanWrapSubmitted
//!                    v
//!           ConfirmOrphanWrap
//!                    v
//!              OrphanWrapped
//!                    v
//!         SubmitOrphanDeposit
//!                    v
//!       OrphanDepositSubmitted
//!                    v
//!         ConfirmOrphanDeposit
//!                    v
//!             OrphanDeposited
//! ```
//!
//! The orphan path persists each step independently so a crash between
//! steps (e.g. between `submit_wrap` returning and the event landing)
//! can resume from the persisted tx hash without double-wrapping.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::{FractionalShares, Symbol};
use st0x_raindex::Raindex;
use st0x_wrapper::Wrapper;

use crate::equity_redemption::RedemptionAggregateId;
use crate::rebalancing::equity::CrossVenueEquityTransfer;
use crate::tokenized_equity_mint::IssuerRequestId;
use crate::vault_lookup::VaultLookup;

/// Aggregate identifier. Each detection creates a fresh UUID; multiple
/// recoveries for the same symbol are independent aggregates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct UnwrappedEquityRecoveryId(pub(crate) Uuid);

impl std::fmt::Display for UnwrappedEquityRecoveryId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for UnwrappedEquityRecoveryId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Onchain/transfer dependencies the recovery job uses to perform the
/// recovery's side effects before recording the outcome through the aggregate's
/// pure commands. (The aggregate itself takes `Services = ()`.)
#[derive(Clone)]
pub(crate) struct UnwrappedEquityRecoveryServices {
    pub(crate) raindex: Arc<dyn Raindex>,
    pub(crate) vault_lookup: Arc<dyn VaultLookup>,
    pub(crate) wrapper: Arc<dyn Wrapper>,
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
    /// Bot wallet on Base; the wrap receiver and the address the deposit
    /// pulls wrapped tokens from.
    pub(crate) wallet: Address,
}

/// Domain errors returned from the aggregate's pure `initialize`/`transition`
/// handlers -- state-machine guards only. The handlers perform no I/O; the
/// recovery job records terminal service failures as `RecoveryFailed` events
/// and surfaces retryable service failures as its own job errors.
#[derive(Debug, Clone, Serialize, Deserialize, Error, PartialEq, Eq)]
pub(crate) enum UnwrappedEquityRecoveryError {
    #[error("recovery already initialized")]
    AlreadyInitialized,

    #[error("recovery not yet initialized; only Detect is valid")]
    Uninitialized,

    #[error("command not valid from state {state:?}")]
    InvalidTransition { state: Box<UnwrappedEquityRecovery> },

    #[error("recovery is already in terminal state")]
    Terminal,
}

/// Commands are pure event recorders: the recovery job
/// ([`UnwrappedEquityRecoveryJob`](super::job::UnwrappedEquityRecoveryJob))
/// performs the onchain/transfer I/O and sends the matching command carrying
/// the outcome. A terminal service failure is recorded by sending
/// `FailRecovery`; a retryable failure surfaces as a job error (no command) so
/// apalis retries from the same persisted state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UnwrappedEquityRecoveryCommand {
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

    /// Orphan path step 1. Records the submitted wrap tx hash the job got from
    /// `wrapper.submit_wrap`.
    SubmitOrphanWrap { wrap_tx_hash: TxHash },

    /// Orphan path step 2. Records the confirmed wrap: the actual minted
    /// wrapped amount and the confirmation block, from `wrapper.confirm_wrap`.
    ConfirmOrphanWrap {
        wrapped_amount: U256,
        wrap_block: u64,
    },

    /// Orphan path step 3. Records the submitted Raindex deposit tx hash the
    /// job got from `raindex.submit_deposit`.
    SubmitOrphanDeposit { vault_deposit_tx_hash: TxHash },

    /// Orphan path step 4. Records the confirmed deposit (the tx hash is read
    /// from state); the job has already confirmed it via `raindex.confirm_tx`.
    ConfirmOrphanDeposit,

    /// Marks the recovery as failed with the supplied reason.
    FailRecovery { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UnwrappedEquityRecoveryEvent {
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

    OrphanWrapSubmitted {
        wrap_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },

    OrphanWrapped {
        wrap_tx_hash: TxHash,
        wrapped_amount: U256,
        confirmed_at: DateTime<Utc>,
        /// Block where the wrap tx confirmed; `None` for events emitted before this field was
        /// added (schema backward-compatibility).
        #[serde(default)]
        wrap_block: Option<u64>,
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

impl DomainEvent for UnwrappedEquityRecoveryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Detected { .. } => "UnwrappedEquityRecoveryEvent::Detected",
            Self::DispatchedToMint { .. } => "UnwrappedEquityRecoveryEvent::DispatchedToMint",
            Self::DispatchedToRedemption { .. } => {
                "UnwrappedEquityRecoveryEvent::DispatchedToRedemption"
            }
            Self::OrphanWrapSubmitted { .. } => "UnwrappedEquityRecoveryEvent::OrphanWrapSubmitted",
            Self::OrphanWrapped { .. } => "UnwrappedEquityRecoveryEvent::OrphanWrapped",
            Self::OrphanDepositSubmitted { .. } => {
                "UnwrappedEquityRecoveryEvent::OrphanDepositSubmitted"
            }
            Self::OrphanDeposited { .. } => "UnwrappedEquityRecoveryEvent::OrphanDeposited",
            Self::RecoveryFailed { .. } => "UnwrappedEquityRecoveryEvent::RecoveryFailed",
        }
        .to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UnwrappedEquityRecovery {
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

    OrphanWrapSubmitted {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        wrap_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },

    OrphanWrapped {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        wrap_tx_hash: TxHash,
        wrap_submitted_at: DateTime<Utc>,
        wrapped_amount: U256,
        wrap_confirmed_at: DateTime<Utc>,
        /// Block where the wrap tx confirmed; `None` for aggregates persisted before this field.
        #[serde(default)]
        wrap_block: Option<u64>,
    },

    OrphanDepositSubmitted {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        wrap_tx_hash: TxHash,
        wrapped_amount: U256,
        vault_deposit_tx_hash: TxHash,
        deposit_submitted_at: DateTime<Utc>,
    },

    OrphanDeposited {
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        wrap_tx_hash: TxHash,
        wrapped_amount: U256,
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },

    Failed {
        symbol: Symbol,
        shares: FractionalShares,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl UnwrappedEquityRecovery {
    pub(super) fn is_terminal(&self) -> bool {
        match self {
            Self::DispatchedToMint { .. }
            | Self::DispatchedToRedemption { .. }
            | Self::OrphanDeposited { .. }
            | Self::Failed { .. } => true,
            Self::Detected { .. }
            | Self::OrphanWrapSubmitted { .. }
            | Self::OrphanWrapped { .. }
            | Self::OrphanDepositSubmitted { .. } => false,
        }
    }
}

#[async_trait]
impl EventSourced for UnwrappedEquityRecovery {
    type Id = UnwrappedEquityRecoveryId;
    type Event = UnwrappedEquityRecoveryEvent;
    type Command = UnwrappedEquityRecoveryCommand;
    type Error = UnwrappedEquityRecoveryError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "UnwrappedEquityRecovery";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            UnwrappedEquityRecoveryEvent::Detected {
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
        use UnwrappedEquityRecoveryEvent::*;

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
                OrphanWrapSubmitted {
                    wrap_tx_hash,
                    submitted_at,
                },
            ) => Some(Self::OrphanWrapSubmitted {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                wrap_tx_hash: *wrap_tx_hash,
                submitted_at: *submitted_at,
            }),

            (
                Self::OrphanWrapSubmitted {
                    symbol,
                    shares,
                    detected_at,
                    wrap_tx_hash,
                    submitted_at: wrap_submitted_at,
                },
                OrphanWrapped {
                    wrap_tx_hash: confirm_wrap_tx_hash,
                    wrapped_amount,
                    confirmed_at,
                    wrap_block,
                },
            ) if wrap_tx_hash == confirm_wrap_tx_hash => Some(Self::OrphanWrapped {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                wrap_tx_hash: *wrap_tx_hash,
                wrap_submitted_at: *wrap_submitted_at,
                wrapped_amount: *wrapped_amount,
                wrap_confirmed_at: *confirmed_at,
                wrap_block: *wrap_block,
            }),

            (
                Self::OrphanWrapped {
                    symbol,
                    shares,
                    detected_at,
                    wrap_tx_hash,
                    wrapped_amount,
                    ..
                },
                OrphanDepositSubmitted {
                    vault_deposit_tx_hash,
                    submitted_at,
                },
            ) => Some(Self::OrphanDepositSubmitted {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                wrap_tx_hash: *wrap_tx_hash,
                wrapped_amount: *wrapped_amount,
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                deposit_submitted_at: *submitted_at,
            }),

            (
                Self::OrphanDepositSubmitted {
                    symbol,
                    shares,
                    detected_at,
                    wrap_tx_hash,
                    wrapped_amount,
                    vault_deposit_tx_hash,
                    ..
                },
                OrphanDeposited {
                    vault_deposit_tx_hash: confirm_deposit_tx_hash,
                    deposited_at,
                },
            ) if vault_deposit_tx_hash == confirm_deposit_tx_hash => Some(Self::OrphanDeposited {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                wrap_tx_hash: *wrap_tx_hash,
                wrapped_amount: *wrapped_amount,
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                deposited_at: *deposited_at,
            }),

            (
                Self::Detected { symbol, shares, .. }
                | Self::OrphanWrapSubmitted { symbol, shares, .. }
                | Self::OrphanWrapped { symbol, shares, .. }
                | Self::OrphanDepositSubmitted { symbol, shares, .. },
                RecoveryFailed { reason, failed_at },
            ) => Some(Self::Failed {
                symbol: symbol.clone(),
                shares: *shares,
                reason: reason.clone(),
                failed_at: *failed_at,
            }),

            (state, _) => Err(UnwrappedEquityRecoveryError::InvalidTransition {
                state: Box::new(state.clone()),
            })?,
        })
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            UnwrappedEquityRecoveryCommand::Detect { symbol, shares } => {
                Ok(vec![UnwrappedEquityRecoveryEvent::Detected {
                    symbol,
                    shares,
                    detected_at: Utc::now(),
                }])
            }
            _ => Err(UnwrappedEquityRecoveryError::Uninitialized),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use UnwrappedEquityRecoveryCommand::*;

        if self.is_terminal() {
            return Err(UnwrappedEquityRecoveryError::Terminal);
        }

        match (self, command) {
            (_, Detect { .. }) => Err(UnwrappedEquityRecoveryError::AlreadyInitialized),

            (Self::Detected { .. }, DispatchToMint { mint_id }) => {
                Ok(vec![UnwrappedEquityRecoveryEvent::DispatchedToMint {
                    mint_id,
                    dispatched_at: Utc::now(),
                }])
            }

            (Self::Detected { .. }, DispatchToRedemption { redemption_id }) => {
                Ok(vec![UnwrappedEquityRecoveryEvent::DispatchedToRedemption {
                    redemption_id,
                    dispatched_at: Utc::now(),
                }])
            }

            (Self::Detected { .. }, SubmitOrphanWrap { wrap_tx_hash }) => {
                Ok(vec![UnwrappedEquityRecoveryEvent::OrphanWrapSubmitted {
                    wrap_tx_hash,
                    submitted_at: Utc::now(),
                }])
            }

            (
                Self::OrphanWrapSubmitted { wrap_tx_hash, .. },
                ConfirmOrphanWrap {
                    wrapped_amount,
                    wrap_block,
                },
            ) => Ok(vec![UnwrappedEquityRecoveryEvent::OrphanWrapped {
                wrap_tx_hash: *wrap_tx_hash,
                wrapped_amount,
                confirmed_at: Utc::now(),
                wrap_block: Some(wrap_block),
            }]),

            (
                Self::OrphanWrapped { .. },
                SubmitOrphanDeposit {
                    vault_deposit_tx_hash,
                },
            ) => Ok(vec![UnwrappedEquityRecoveryEvent::OrphanDepositSubmitted {
                vault_deposit_tx_hash,
                submitted_at: Utc::now(),
            }]),

            (
                Self::OrphanDepositSubmitted {
                    vault_deposit_tx_hash,
                    ..
                },
                ConfirmOrphanDeposit,
            ) => Ok(vec![UnwrappedEquityRecoveryEvent::OrphanDeposited {
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                deposited_at: Utc::now(),
            }]),

            (
                Self::Detected { .. }
                | Self::OrphanWrapSubmitted { .. }
                | Self::OrphanWrapped { .. }
                | Self::OrphanDepositSubmitted { .. },
                FailRecovery { reason },
            ) => Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason,
                failed_at: Utc::now(),
            }]),

            (state, _) => Err(UnwrappedEquityRecoveryError::InvalidTransition {
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

    use st0x_event_sorcery::EventSourced;
    use st0x_execution::{FractionalShares, Symbol};

    use super::*;

    const FAKE_WRAP_TX: TxHash = TxHash::new(
        fixed_bytes!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef").0,
    );

    const OTHER_TX: TxHash = TxHash::new(
        fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111").0,
    );

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn one_share() -> FractionalShares {
        FractionalShares::new(Float::parse("1".to_string()).unwrap())
    }

    fn detected() -> UnwrappedEquityRecovery {
        UnwrappedEquityRecovery::Detected {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        }
    }

    fn orphan_wrapped() -> UnwrappedEquityRecovery {
        UnwrappedEquityRecovery::OrphanWrapped {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            wrap_submitted_at: Utc::now(),
            wrapped_amount: U256::from(123u64),
            wrap_confirmed_at: Utc::now(),
            wrap_block: None,
        }
    }

    #[tokio::test]
    async fn detect_initializes_aggregate_into_detected_state() {
        let events = UnwrappedEquityRecovery::initialize(
            UnwrappedEquityRecoveryCommand::Detect {
                symbol: aapl(),
                shares: one_share(),
            },
            &(),
        )
        .await
        .unwrap();
        let [UnwrappedEquityRecoveryEvent::Detected { symbol, shares, .. }] = events.as_slice()
        else {
            panic!("expected single Detected event, got {events:?}");
        };
        assert_eq!(*symbol, aapl(), "Detected must carry the detected symbol");
        assert_eq!(
            *shares,
            one_share(),
            "Detected must carry the detected share quantity",
        );
    }

    #[tokio::test]
    async fn dispatch_to_mint_emits_dispatched_to_mint_with_command_id() {
        let mint_id = crate::tokenized_equity_mint::issuer_request_id("dispatch-mint");
        let events = detected()
            .transition(
                UnwrappedEquityRecoveryCommand::DispatchToMint {
                    mint_id: mint_id.clone(),
                },
                &(),
            )
            .await
            .expect("DispatchToMint should succeed from Detected");
        let [
            UnwrappedEquityRecoveryEvent::DispatchedToMint {
                mint_id: emitted, ..
            },
        ] = events.as_slice()
        else {
            panic!("expected single DispatchedToMint event, got {events:?}");
        };
        assert_eq!(
            *emitted, mint_id,
            "DispatchedToMint must carry the command's mint_id",
        );
    }

    #[tokio::test]
    async fn dispatch_to_redemption_emits_dispatched_to_redemption_with_command_id() {
        let redemption_id =
            crate::equity_redemption::redemption_aggregate_id("dispatch-redemption");
        let events = detected()
            .transition(
                UnwrappedEquityRecoveryCommand::DispatchToRedemption {
                    redemption_id: redemption_id.clone(),
                },
                &(),
            )
            .await
            .expect("DispatchToRedemption should succeed from Detected");
        let [
            UnwrappedEquityRecoveryEvent::DispatchedToRedemption {
                redemption_id: emitted,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("expected single DispatchedToRedemption event, got {events:?}");
        };
        assert_eq!(
            *emitted, redemption_id,
            "DispatchedToRedemption must carry the command's redemption_id",
        );
    }

    #[test]
    fn evolve_replays_full_orphan_path() {
        let detected_at = Utc::now();
        let submitted_at = Utc::now();
        let wrapped_at = Utc::now();
        let deposit_submitted_at = Utc::now();
        let deposited_at = Utc::now();
        let wrapped_amount = U256::from(123u64);
        let deposit_tx = OTHER_TX;

        let mut state =
            UnwrappedEquityRecovery::originate(&UnwrappedEquityRecoveryEvent::Detected {
                symbol: aapl(),
                shares: one_share(),
                detected_at,
            })
            .expect("Detected should originate aggregate");

        state = UnwrappedEquityRecovery::evolve(
            &state,
            &UnwrappedEquityRecoveryEvent::OrphanWrapSubmitted {
                wrap_tx_hash: FAKE_WRAP_TX,
                submitted_at,
            },
        )
        .unwrap()
        .expect("OrphanWrapSubmitted should advance state");

        state = UnwrappedEquityRecovery::evolve(
            &state,
            &UnwrappedEquityRecoveryEvent::OrphanWrapped {
                wrap_tx_hash: FAKE_WRAP_TX,
                wrapped_amount,
                confirmed_at: wrapped_at,
                wrap_block: None,
            },
        )
        .unwrap()
        .expect("OrphanWrapped should advance state");

        state = UnwrappedEquityRecovery::evolve(
            &state,
            &UnwrappedEquityRecoveryEvent::OrphanDepositSubmitted {
                vault_deposit_tx_hash: deposit_tx,
                submitted_at: deposit_submitted_at,
            },
        )
        .unwrap()
        .expect("OrphanDepositSubmitted should advance state");

        state = UnwrappedEquityRecovery::evolve(
            &state,
            &UnwrappedEquityRecoveryEvent::OrphanDeposited {
                vault_deposit_tx_hash: deposit_tx,
                deposited_at,
            },
        )
        .unwrap()
        .expect("OrphanDeposited should advance state");

        assert_eq!(
            state,
            UnwrappedEquityRecovery::OrphanDeposited {
                symbol: aapl(),
                shares: one_share(),
                detected_at,
                wrap_tx_hash: FAKE_WRAP_TX,
                wrapped_amount,
                vault_deposit_tx_hash: deposit_tx,
                deposited_at,
            },
        );
    }

    #[tokio::test]
    async fn detect_on_live_aggregate_is_rejected() {
        let state = UnwrappedEquityRecovery::Detected {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        };
        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::Detect {
                    symbol: aapl(),
                    shares: one_share(),
                },
                &(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            UnwrappedEquityRecoveryError::AlreadyInitialized
        ));
    }

    #[tokio::test]
    async fn confirm_orphan_wrap_only_valid_after_submit_wrap() {
        let state = UnwrappedEquityRecovery::Detected {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        };
        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap {
                    wrapped_amount: U256::from(1u64),
                    wrap_block: 1,
                },
                &(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            UnwrappedEquityRecoveryError::InvalidTransition { .. }
        ));
    }

    #[tokio::test]
    async fn submit_orphan_deposit_only_valid_after_confirm_wrap() {
        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit {
                    vault_deposit_tx_hash: FAKE_WRAP_TX,
                },
                &(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            UnwrappedEquityRecoveryError::InvalidTransition { .. }
        ));
    }

    #[tokio::test]
    async fn terminal_state_rejects_further_commands() {
        let state = UnwrappedEquityRecovery::Failed {
            symbol: aapl(),
            shares: one_share(),
            reason: "test".to_string(),
            failed_at: Utc::now(),
        };
        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanWrap {
                    wrap_tx_hash: FAKE_WRAP_TX,
                },
                &(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, UnwrappedEquityRecoveryError::Terminal));
    }

    #[tokio::test]
    async fn submit_orphan_wrap_emits_submitted_event() {
        let events = detected()
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanWrap {
                    wrap_tx_hash: FAKE_WRAP_TX,
                },
                &(),
            )
            .await
            .expect("SubmitOrphanWrap should succeed from Detected");
        let [UnwrappedEquityRecoveryEvent::OrphanWrapSubmitted { wrap_tx_hash, .. }] =
            events.as_slice()
        else {
            panic!("expected single OrphanWrapSubmitted event, got {events:?}");
        };
        assert_eq!(
            *wrap_tx_hash, FAKE_WRAP_TX,
            "OrphanWrapSubmitted must carry the command-supplied submitted tx hash -- it is \
             the crash-recovery anchor ConfirmOrphanWrap confirms against",
        );
    }

    #[tokio::test]
    async fn confirm_orphan_wrap_emits_confirmed_wrapped_amount() {
        let wrapped_amount = U256::from(7u64);
        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        let events = state
            .transition(
                UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap {
                    wrapped_amount,
                    wrap_block: 5,
                },
                &(),
            )
            .await
            .expect("ConfirmOrphanWrap should succeed from OrphanWrapSubmitted");
        let [
            UnwrappedEquityRecoveryEvent::OrphanWrapped {
                wrap_tx_hash,
                wrapped_amount: confirmed,
                wrap_block,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("expected single OrphanWrapped event, got {events:?}");
        };
        assert_eq!(
            *confirmed, wrapped_amount,
            "OrphanWrapped should carry the command-supplied minted wrapped amount",
        );
        assert_eq!(
            *wrap_tx_hash, FAKE_WRAP_TX,
            "OrphanWrapped should carry the submitted wrap tx hash from state",
        );
        assert_eq!(
            *wrap_block,
            Some(5),
            "OrphanWrapped should carry the command-supplied confirmation block",
        );
    }

    #[tokio::test]
    async fn submit_orphan_deposit_emits_submitted_event() {
        let vault_deposit_tx = OTHER_TX;
        let events = orphan_wrapped()
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit {
                    vault_deposit_tx_hash: vault_deposit_tx,
                },
                &(),
            )
            .await
            .expect("SubmitOrphanDeposit should succeed from OrphanWrapped");
        let [
            UnwrappedEquityRecoveryEvent::OrphanDepositSubmitted {
                vault_deposit_tx_hash,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("expected single OrphanDepositSubmitted event, got {events:?}");
        };
        assert_eq!(
            *vault_deposit_tx_hash, vault_deposit_tx,
            "OrphanDepositSubmitted must carry the command-supplied deposit tx hash -- it \
             is the crash-recovery anchor ConfirmOrphanDeposit confirms against",
        );
    }

    #[tokio::test]
    async fn confirm_orphan_deposit_completes_orphan_branch() {
        let state = UnwrappedEquityRecovery::OrphanDepositSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            wrapped_amount: U256::from(123u64),
            vault_deposit_tx_hash: FAKE_WRAP_TX,
            deposit_submitted_at: Utc::now(),
        };
        let events = state
            .transition(UnwrappedEquityRecoveryCommand::ConfirmOrphanDeposit, &())
            .await
            .expect("ConfirmOrphanDeposit should succeed from OrphanDepositSubmitted");
        let [
            UnwrappedEquityRecoveryEvent::OrphanDeposited {
                vault_deposit_tx_hash,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("expected single OrphanDeposited event, got {events:?}");
        };
        assert_eq!(
            *vault_deposit_tx_hash, FAKE_WRAP_TX,
            "OrphanDeposited must carry the submitted deposit tx hash from state -- it is \
             the idempotency anchor confirm_tx ran against",
        );
    }

    #[tokio::test]
    async fn fail_recovery_from_detected_emits_recovery_failed() {
        let events = detected()
            .transition(
                UnwrappedEquityRecoveryCommand::FailRecovery {
                    reason: "operator abort".to_string(),
                },
                &(),
            )
            .await
            .expect("FailRecovery should succeed from a non-terminal state");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert_eq!(reason, "operator abort");
    }

    /// The `OrphanWrapped` evolution is gated on the confirm tx hash matching
    /// the submitted one, so a confirmation for a different wrap can never bind
    /// to this aggregate.
    #[test]
    fn evolve_rejects_orphan_wrapped_with_mismatched_wrap_tx() {
        let submitted = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        let mismatched = UnwrappedEquityRecoveryEvent::OrphanWrapped {
            wrap_tx_hash: OTHER_TX,
            wrapped_amount: U256::from(1u64),
            confirmed_at: Utc::now(),
            wrap_block: None,
        };
        let error = UnwrappedEquityRecovery::evolve(&submitted, &mismatched).unwrap_err();
        assert!(matches!(
            error,
            UnwrappedEquityRecoveryError::InvalidTransition { .. }
        ));
    }

    #[test]
    fn evolve_rejects_orphan_deposited_with_mismatched_deposit_tx() {
        let submitted = UnwrappedEquityRecovery::OrphanDepositSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            wrapped_amount: U256::from(1u64),
            vault_deposit_tx_hash: FAKE_WRAP_TX,
            deposit_submitted_at: Utc::now(),
        };
        let mismatched = UnwrappedEquityRecoveryEvent::OrphanDeposited {
            vault_deposit_tx_hash: OTHER_TX,
            deposited_at: Utc::now(),
        };
        let error = UnwrappedEquityRecovery::evolve(&submitted, &mismatched).unwrap_err();
        assert!(matches!(
            error,
            UnwrappedEquityRecoveryError::InvalidTransition { .. }
        ));
    }

    #[test]
    fn id_roundtrips_through_string() {
        let id = UnwrappedEquityRecoveryId(Uuid::new_v4());
        let parsed = id.to_string().parse::<UnwrappedEquityRecoveryId>().unwrap();
        assert_eq!(id, parsed);
    }
}
