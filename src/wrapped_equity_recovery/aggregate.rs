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
//! to `Failed`. Service-call failures inside the dispatch handlers also
//! emit `RecoveryFailed` (consistent with `TokenizedEquityMint`'s
//! `MintAcceptanceFailed` pattern) so failures remain first-class events.

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::{FractionalShares, Symbol};
use st0x_raindex::Raindex;
use st0x_wrapper::Wrapper;

use crate::equity_redemption::RedemptionAggregateId;
use crate::rebalancing::equity::CrossVenueEquityTransfer;
use crate::tokenized_equity_mint::{IssuerRequestId, TOKENIZED_EQUITY_DECIMALS};
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

/// Services the aggregate calls inside its command handlers. Each dispatch
/// path needs at least one of these; the handler runs the side effect and
/// emits the success event iff it actually completed.
#[derive(Clone)]
pub(crate) struct WrappedEquityRecoveryServices {
    pub(crate) raindex: Arc<dyn Raindex>,
    pub(crate) vault_lookup: Arc<dyn VaultLookup>,
    pub(crate) wrapper: Arc<dyn Wrapper>,
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
}

/// Domain errors returned from the aggregate's `initialize`/`transition`
/// handlers. Service failures (raindex/wrapper/transfer) do NOT flow through
/// this enum -- they are recorded as `RecoveryFailed` events instead, so
/// failures remain first-class entries in the audit trail.
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecoveryCommand {
    /// Initial command. Records the detection trigger; invokes no services.
    Detect {
        symbol: Symbol,
        shares: FractionalShares,
    },

    /// Recovery has an active mint to resume. The handler calls
    /// `services.transfer.resume_mint(mint_id)` and emits `DispatchedToMint`
    /// iff `resume_mint` returns Ok.
    DispatchToMint { mint_id: IssuerRequestId },

    /// Recovery has an active redemption to resume. The handler calls
    /// `services.transfer.resume_redemption(redemption_id)` and emits
    /// `DispatchedToRedemption` iff `resume_redemption` returns Ok.
    DispatchToRedemption {
        redemption_id: RedemptionAggregateId,
    },

    /// Orphan path. The handler resolves the wrapped-token address via
    /// `services.wrapper.lookup_derivative(symbol)`, looks up the Raindex
    /// vault, calls `services.raindex.submit_deposit(...)`, and emits
    /// `OrphanDepositSubmitted` with the returned tx hash.
    SubmitOrphanDeposit,

    /// Orphan path. The handler reads `vault_deposit_tx_hash` from the
    /// current state and calls `services.raindex.confirm_tx(tx_hash)`,
    /// emitting `OrphanDeposited` iff confirmation succeeds.
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

#[async_trait]
impl EventSourced for WrappedEquityRecovery {
    type Id = WrappedEquityRecoveryId;
    type Event = WrappedEquityRecoveryEvent;
    type Command = WrappedEquityRecoveryCommand;
    type Error = WrappedEquityRecoveryError;
    type Services = WrappedEquityRecoveryServices;
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

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
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

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use WrappedEquityRecoveryCommand::*;

        if self.is_terminal() {
            return Err(WrappedEquityRecoveryError::Terminal);
        }

        let now = Utc::now();
        match (self, command) {
            (_, Detect { .. }) => Err(WrappedEquityRecoveryError::AlreadyInitialized),

            (Self::Detected { .. }, DispatchToMint { mint_id }) => {
                resume_mint_or_fail(&services.transfer, &mint_id, now).await
            }

            (Self::Detected { .. }, DispatchToRedemption { redemption_id }) => {
                resume_redemption_or_fail(&services.transfer, &redemption_id, now).await
            }

            (Self::Detected { symbol, shares, .. }, SubmitOrphanDeposit) => {
                submit_orphan_deposit_or_fail(services, symbol, *shares, now).await
            }

            (
                Self::OrphanDepositSubmitted {
                    vault_deposit_tx_hash,
                    ..
                },
                ConfirmOrphanDeposit,
            ) => {
                confirm_orphan_deposit_or_fail(&services.raindex, *vault_deposit_tx_hash, now).await
            }

            (
                Self::Detected { .. } | Self::OrphanDepositSubmitted { .. },
                FailRecovery { reason },
            ) => Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason,
                failed_at: now,
            }]),

            (state, _) => Err(WrappedEquityRecoveryError::InvalidTransition {
                state: Box::new(state.clone()),
            }),
        }
    }
}

async fn resume_mint_or_fail(
    transfer: &CrossVenueEquityTransfer,
    mint_id: &IssuerRequestId,
    now: DateTime<Utc>,
) -> Result<Vec<WrappedEquityRecoveryEvent>, WrappedEquityRecoveryError> {
    match transfer.resume_mint(mint_id).await {
        Ok(()) => {
            info!(target: "rebalance", %mint_id, "Wrapped equity recovery: resume_mint succeeded");
            Ok(vec![WrappedEquityRecoveryEvent::DispatchedToMint {
                mint_id: mint_id.clone(),
                dispatched_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %mint_id, ?error, "Wrapped equity recovery: resume_mint failed");
            Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("resume_mint failed: {error}"),
                failed_at: now,
            }])
        }
    }
}

async fn resume_redemption_or_fail(
    transfer: &CrossVenueEquityTransfer,
    redemption_id: &RedemptionAggregateId,
    now: DateTime<Utc>,
) -> Result<Vec<WrappedEquityRecoveryEvent>, WrappedEquityRecoveryError> {
    match transfer.resume_redemption(redemption_id).await {
        Ok(()) => {
            info!(target: "rebalance", %redemption_id, "Wrapped equity recovery: resume_redemption succeeded");
            Ok(vec![WrappedEquityRecoveryEvent::DispatchedToRedemption {
                redemption_id: redemption_id.clone(),
                dispatched_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %redemption_id, ?error, "Wrapped equity recovery: resume_redemption failed");
            Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("resume_redemption failed: {error}"),
                failed_at: now,
            }])
        }
    }
}

async fn submit_orphan_deposit_or_fail(
    services: &WrappedEquityRecoveryServices,
    symbol: &Symbol,
    shares: FractionalShares,
    now: DateTime<Utc>,
) -> Result<Vec<WrappedEquityRecoveryEvent>, WrappedEquityRecoveryError> {
    let wrapped_token = match services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Wrapped equity recovery: lookup_derivative failed");
            return Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    let vault_id = match services
        .vault_lookup
        .vault_id_for_token(wrapped_token)
        .await
    {
        Ok(id) => id,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Wrapped equity recovery: vault_id_for_token failed");
            return Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("vault_lookup.vault_id_for_token failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    let raw = match shares.to_u256_18_decimals() {
        Ok(raw) => raw,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Wrapped equity recovery: shares conversion failed");
            return Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("shares conversion failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    match services
        .raindex
        .submit_deposit(wrapped_token, vault_id, raw, TOKENIZED_EQUITY_DECIMALS)
        .await
    {
        Ok(vault_deposit_tx_hash) => {
            info!(target: "rebalance", %symbol, %vault_deposit_tx_hash, "Wrapped equity recovery: submit_deposit succeeded");
            Ok(vec![WrappedEquityRecoveryEvent::OrphanDepositSubmitted {
                vault_deposit_tx_hash,
                submitted_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Wrapped equity recovery: submit_deposit failed");
            Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("raindex.submit_deposit failed: {error}"),
                failed_at: now,
            }])
        }
    }
}

async fn confirm_orphan_deposit_or_fail(
    raindex: &Arc<dyn Raindex>,
    vault_deposit_tx_hash: TxHash,
    now: DateTime<Utc>,
) -> Result<Vec<WrappedEquityRecoveryEvent>, WrappedEquityRecoveryError> {
    match raindex.confirm_tx(vault_deposit_tx_hash).await {
        Ok(()) => {
            info!(target: "rebalance", %vault_deposit_tx_hash, "Wrapped equity recovery: confirm_tx succeeded");
            Ok(vec![WrappedEquityRecoveryEvent::OrphanDeposited {
                vault_deposit_tx_hash,
                deposited_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %vault_deposit_tx_hash, ?error, "Wrapped equity recovery: confirm_tx failed");
            Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("raindex.confirm_tx failed: {error}"),
                failed_at: now,
            }])
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, TxHash, fixed_bytes};
    use chrono::Utc;
    use rain_math_float::Float;
    use uuid::Uuid;

    use st0x_event_sorcery::EventSourced;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_raindex::RaindexVaultId;
    use st0x_wrapper::MockWrapper;

    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::tokenization::mock::MockTokenizer;
    use crate::vault_lookup::MockVaultLookup;

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

    fn mock_vault_lookup() -> MockVaultLookup {
        MockVaultLookup::new()
            .with_vault(Address::ZERO, RaindexVaultId(B256::ZERO))
            .with_default_vault(RaindexVaultId(B256::ZERO))
    }

    fn detected_state() -> WrappedEquityRecovery {
        WrappedEquityRecovery::Detected {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        }
    }

    async fn test_services() -> WrappedEquityRecoveryServices {
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let services = EquityTransferServices {
            raindex: raindex.clone(),
            vault_lookup: Arc::new(mock_vault_lookup()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: wrapper.clone(),
        };
        let mint_store = Arc::new(st0x_event_sorcery::test_store(
            pool.clone(),
            services.clone(),
        ));
        let redemption_store = Arc::new(st0x_event_sorcery::test_store(pool, services));
        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            raindex.clone(),
            Arc::new(mock_vault_lookup()),
            Arc::new(MockTokenizer::new()),
            wrapper.clone(),
            Address::random(),
            mint_store,
            redemption_store,
        ));
        WrappedEquityRecoveryServices {
            raindex,
            vault_lookup: Arc::new(mock_vault_lookup()),
            wrapper,
            transfer,
        }
    }

    #[tokio::test]
    async fn detect_initializes_aggregate_into_detected_state() {
        let services = test_services().await;
        let events = WrappedEquityRecovery::initialize(
            WrappedEquityRecoveryCommand::Detect {
                symbol: aapl(),
                shares: one_share(),
            },
            &services,
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
    async fn submit_orphan_deposit_emits_event_with_returned_tx_hash() {
        let services = test_services().await;
        let detected = detected_state();

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
        let services = test_services().await;
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
                &services,
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
    async fn fail_recovery_rejected_from_terminal_state() {
        let services = test_services().await;
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
                &services,
            )
            .await
            .expect_err("FailRecovery on a terminal state should error");

        assert!(
            matches!(error, WrappedEquityRecoveryError::Terminal),
            "Expected Terminal error, got {error:?}",
        );
    }

    /// `resume_mint` fails because no mint aggregate exists in the store ->
    /// the handler records the failure as `RecoveryFailed`. Proves service
    /// failures flow through events, not aggregate errors.
    #[tokio::test]
    async fn dispatch_to_mint_records_failure_when_resume_mint_fails() {
        let services = test_services().await;
        let detected = detected_state();
        let mint_id = IssuerRequestId::new("ISS-NONEXISTENT");

        let events = detected
            .transition(
                WrappedEquityRecoveryCommand::DispatchToMint {
                    mint_id: mint_id.clone(),
                },
                &services,
            )
            .await
            .expect(
                "DispatchToMint should return Ok with a RecoveryFailed event on service failure",
            );

        let [WrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice() else {
            panic!("Expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("resume_mint failed"),
            "RecoveryFailed reason should mention resume_mint; got {reason:?}",
        );
    }

    /// `resume_redemption` fails because no redemption aggregate exists ->
    /// the handler records the failure as `RecoveryFailed`.
    #[tokio::test]
    async fn dispatch_to_redemption_records_failure_when_resume_redemption_fails() {
        let services = test_services().await;
        let detected = detected_state();
        let redemption_id = RedemptionAggregateId("nonexistent".to_string());

        let events = detected
            .transition(
                WrappedEquityRecoveryCommand::DispatchToRedemption {
                    redemption_id: redemption_id.clone(),
                },
                &services,
            )
            .await
            .expect("DispatchToRedemption should return Ok with RecoveryFailed on service failure");

        let [WrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice() else {
            panic!("Expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("resume_redemption failed"),
            "RecoveryFailed reason should mention resume_redemption; got {reason:?}",
        );
    }

    /// `submit_deposit` reverts -> the handler records the failure as
    /// `RecoveryFailed` instead of advancing into `OrphanDepositSubmitted`.
    #[tokio::test]
    async fn submit_orphan_deposit_records_failure_when_raindex_reports_revert() {
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::reverting_deposit());
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let inner_services = EquityTransferServices {
            raindex: raindex.clone(),
            vault_lookup: Arc::new(mock_vault_lookup()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: wrapper.clone(),
        };
        let mint_store = Arc::new(st0x_event_sorcery::test_store(
            pool.clone(),
            inner_services.clone(),
        ));
        let redemption_store = Arc::new(st0x_event_sorcery::test_store(pool, inner_services));
        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            raindex.clone(),
            Arc::new(mock_vault_lookup()),
            Arc::new(MockTokenizer::new()),
            wrapper.clone(),
            Address::random(),
            mint_store,
            redemption_store,
        ));
        let services = WrappedEquityRecoveryServices {
            raindex,
            vault_lookup: Arc::new(mock_vault_lookup()),
            wrapper,
            transfer,
        };

        let events = detected_state()
            .transition(WrappedEquityRecoveryCommand::SubmitOrphanDeposit, &services)
            .await
            .expect("SubmitOrphanDeposit should return Ok with RecoveryFailed on revert");

        let [WrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice() else {
            panic!("Expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("submit_deposit failed"),
            "RecoveryFailed reason should mention submit_deposit; got {reason:?}",
        );
    }

    #[test]
    fn id_roundtrips_through_string() {
        let id = WrappedEquityRecoveryId(Uuid::new_v4());
        let parsed = id.to_string().parse::<WrappedEquityRecoveryId>().unwrap();
        assert_eq!(id, parsed);
    }
}
