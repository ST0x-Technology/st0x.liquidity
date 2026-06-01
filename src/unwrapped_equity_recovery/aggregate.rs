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
use tracing::{info, warn};
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::{FractionalShares, SharesBlockchain, Symbol};

use crate::equity_redemption::RedemptionAggregateId;
use crate::onchain::raindex::Raindex;
use crate::rebalancing::equity::CrossVenueEquityTransfer;
use crate::tokenized_equity_mint::{IssuerRequestId, TOKENIZED_EQUITY_DECIMALS};
use crate::wrapper::Wrapper;

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

/// Services the aggregate calls inside its command handlers.
#[derive(Clone)]
pub(crate) struct UnwrappedEquityRecoveryServices {
    pub(crate) raindex: Arc<dyn Raindex>,
    pub(crate) wrapper: Arc<dyn Wrapper>,
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
    /// Bot wallet on Base; the wrap receiver and the address the deposit
    /// pulls wrapped tokens from.
    pub(crate) wallet: Address,
}

/// Domain errors returned from the aggregate's `initialize`/`transition`
/// handlers. Service failures (raindex/wrapper/transfer) do NOT flow through
/// this enum -- they are recorded as `RecoveryFailed` events instead, so
/// failures remain first-class entries in the audit trail.
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UnwrappedEquityRecoveryCommand {
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

    /// Orphan path step 1. The handler resolves the wrapped-token
    /// address via `services.wrapper.lookup_derivative(symbol)`, calls
    /// `services.wrapper.submit_wrap(...)`, and emits
    /// `OrphanWrapSubmitted` with the returned tx hash.
    SubmitOrphanWrap,

    /// Orphan path step 2. The handler reads `wrap_tx_hash` from the
    /// current state and calls `services.wrapper.confirm_wrap(...)`,
    /// emitting `OrphanWrapped` with the actual minted wrapped amount
    /// iff confirmation succeeds.
    ConfirmOrphanWrap,

    /// Orphan path step 3. The handler reads `wrapped_amount` from the
    /// current state, looks up the Raindex vault, calls
    /// `services.raindex.submit_deposit(...)`, and emits
    /// `OrphanDepositSubmitted` with the returned tx hash.
    SubmitOrphanDeposit,

    /// Orphan path step 4. The handler reads `vault_deposit_tx_hash`
    /// from the current state and calls
    /// `services.raindex.confirm_tx(tx_hash)`, emitting
    /// `OrphanDeposited` iff confirmation succeeds.
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
impl EventSourced for UnwrappedEquityRecovery {
    type Id = UnwrappedEquityRecoveryId;
    type Event = UnwrappedEquityRecoveryEvent;
    type Command = UnwrappedEquityRecoveryCommand;
    type Error = UnwrappedEquityRecoveryError;
    type Services = UnwrappedEquityRecoveryServices;
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
                },
            ) if wrap_tx_hash == confirm_wrap_tx_hash => Some(Self::OrphanWrapped {
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                wrap_tx_hash: *wrap_tx_hash,
                wrap_submitted_at: *wrap_submitted_at,
                wrapped_amount: *wrapped_amount,
                wrap_confirmed_at: *confirmed_at,
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
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use UnwrappedEquityRecoveryCommand::*;

        if self.is_terminal() {
            return Err(UnwrappedEquityRecoveryError::Terminal);
        }

        let now = Utc::now();
        match (self, command) {
            (_, Detect { .. }) => Err(UnwrappedEquityRecoveryError::AlreadyInitialized),

            (Self::Detected { .. }, DispatchToMint { mint_id }) => {
                resume_mint_or_fail(&services.transfer, &mint_id, now).await
            }

            (Self::Detected { .. }, DispatchToRedemption { redemption_id }) => {
                resume_redemption_or_fail(&services.transfer, &redemption_id, now).await
            }

            (Self::Detected { symbol, shares, .. }, SubmitOrphanWrap) => {
                submit_orphan_wrap_or_fail(services, symbol, *shares, now).await
            }

            (
                Self::OrphanWrapSubmitted {
                    symbol,
                    wrap_tx_hash,
                    ..
                },
                ConfirmOrphanWrap,
            ) => confirm_orphan_wrap_or_fail(services, symbol, *wrap_tx_hash, now).await,

            (
                Self::OrphanWrapped {
                    symbol,
                    wrapped_amount,
                    ..
                },
                SubmitOrphanDeposit,
            ) => submit_orphan_deposit_or_fail(services, symbol, *wrapped_amount, now).await,

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
                Self::Detected { .. }
                | Self::OrphanWrapSubmitted { .. }
                | Self::OrphanWrapped { .. }
                | Self::OrphanDepositSubmitted { .. },
                FailRecovery { reason },
            ) => Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason,
                failed_at: now,
            }]),

            (state, _) => Err(UnwrappedEquityRecoveryError::InvalidTransition {
                state: Box::new(state.clone()),
            }),
        }
    }
}

async fn resume_mint_or_fail(
    transfer: &CrossVenueEquityTransfer,
    mint_id: &IssuerRequestId,
    now: DateTime<Utc>,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    match transfer.resume_mint(mint_id).await {
        Ok(()) => {
            info!(target: "rebalance", %mint_id, "Unwrapped equity recovery: resume_mint succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::DispatchedToMint {
                mint_id: mint_id.clone(),
                dispatched_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %mint_id, ?error, "Unwrapped equity recovery: resume_mint failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
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
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    match transfer.resume_redemption(redemption_id).await {
        Ok(()) => {
            info!(target: "rebalance", %redemption_id, "Unwrapped equity recovery: resume_redemption succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::DispatchedToRedemption {
                redemption_id: redemption_id.clone(),
                dispatched_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %redemption_id, ?error, "Unwrapped equity recovery: resume_redemption failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("resume_redemption failed: {error}"),
                failed_at: now,
            }])
        }
    }
}

async fn submit_orphan_wrap_or_fail(
    services: &UnwrappedEquityRecoveryServices,
    symbol: &Symbol,
    shares: FractionalShares,
    now: DateTime<Utc>,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_derivative failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    let underlying_amount = match shares.to_u256_18_decimals() {
        Ok(raw) => raw,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: shares conversion failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("shares conversion failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    match services
        .wrapper
        .submit_wrap(wrapped_token, underlying_amount, services.wallet)
        .await
    {
        Ok(wrap_tx_hash) => {
            info!(target: "rebalance", %symbol, %wrap_tx_hash, "Unwrapped equity recovery: submit_wrap succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::OrphanWrapSubmitted {
                wrap_tx_hash,
                submitted_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: submit_wrap failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.submit_wrap failed: {error}"),
                failed_at: now,
            }])
        }
    }
}

async fn confirm_orphan_wrap_or_fail(
    services: &UnwrappedEquityRecoveryServices,
    symbol: &Symbol,
    wrap_tx_hash: TxHash,
    now: DateTime<Utc>,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_derivative failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    match services
        .wrapper
        .confirm_wrap(wrapped_token, wrap_tx_hash)
        .await
    {
        Ok(wrapped_amount) => {
            info!(target: "rebalance", %symbol, %wrap_tx_hash, %wrapped_amount, "Unwrapped equity recovery: confirm_wrap succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::OrphanWrapped {
                wrap_tx_hash,
                wrapped_amount,
                confirmed_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %symbol, %wrap_tx_hash, ?error, "Unwrapped equity recovery: confirm_wrap failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.confirm_wrap failed: {error}"),
                failed_at: now,
            }])
        }
    }
}

async fn submit_orphan_deposit_or_fail(
    services: &UnwrappedEquityRecoveryServices,
    symbol: &Symbol,
    wrapped_amount: U256,
    now: DateTime<Utc>,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_derivative failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    let vault_id = match services.raindex.lookup_vault_id(wrapped_token).await {
        Ok(id) => id,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_vault_id failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("raindex.lookup_vault_id failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    match services
        .raindex
        .submit_deposit(
            wrapped_token,
            vault_id,
            wrapped_amount,
            TOKENIZED_EQUITY_DECIMALS,
        )
        .await
    {
        Ok(vault_deposit_tx_hash) => {
            info!(target: "rebalance", %symbol, %vault_deposit_tx_hash, "Unwrapped equity recovery: submit_deposit succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::OrphanDepositSubmitted {
                vault_deposit_tx_hash,
                submitted_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: submit_deposit failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
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
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    match raindex.confirm_tx(vault_deposit_tx_hash).await {
        Ok(()) => {
            info!(target: "rebalance", %vault_deposit_tx_hash, "Unwrapped equity recovery: confirm_tx succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::OrphanDeposited {
                vault_deposit_tx_hash,
                deposited_at: now,
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %vault_deposit_tx_hash, ?error, "Unwrapped equity recovery: confirm_tx failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("raindex.confirm_tx failed: {error}"),
                failed_at: now,
            }])
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

    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::tokenization::mock::MockTokenizer;
    use crate::wrapper::mock::MockWrapper;

    use super::*;

    const FAKE_WRAP_TX: TxHash = TxHash::new(
        fixed_bytes!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef").0,
    );

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn one_share() -> FractionalShares {
        FractionalShares::new(Float::parse("1".to_string()).unwrap())
    }

    async fn test_services() -> UnwrappedEquityRecoveryServices {
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let services = EquityTransferServices {
            raindex: raindex.clone(),
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
            Arc::new(MockTokenizer::new()),
            wrapper.clone(),
            Address::random(),
            mint_store,
            redemption_store,
        ));
        UnwrappedEquityRecoveryServices {
            raindex,
            wrapper,
            transfer,
            wallet: Address::random(),
        }
    }

    #[tokio::test]
    async fn detect_initializes_aggregate_into_detected_state() {
        let services = test_services().await;
        let events = UnwrappedEquityRecovery::initialize(
            UnwrappedEquityRecoveryCommand::Detect {
                symbol: aapl(),
                shares: one_share(),
            },
            &services,
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UnwrappedEquityRecoveryEvent::Detected { .. }
        ));
    }

    #[tokio::test]
    async fn detect_on_live_aggregate_is_rejected() {
        let services = test_services().await;
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
                &services,
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
        let services = test_services().await;
        let state = UnwrappedEquityRecovery::Detected {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        };
        let error = state
            .transition(UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap, &services)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            UnwrappedEquityRecoveryError::InvalidTransition { .. }
        ));
    }

    #[tokio::test]
    async fn submit_orphan_deposit_only_valid_after_confirm_wrap() {
        let services = test_services().await;
        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit,
                &services,
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
        let services = test_services().await;
        let state = UnwrappedEquityRecovery::Failed {
            symbol: aapl(),
            shares: one_share(),
            reason: "test".to_string(),
            failed_at: Utc::now(),
        };
        let error = state
            .transition(UnwrappedEquityRecoveryCommand::SubmitOrphanWrap, &services)
            .await
            .unwrap_err();
        assert!(matches!(error, UnwrappedEquityRecoveryError::Terminal));
    }
}
