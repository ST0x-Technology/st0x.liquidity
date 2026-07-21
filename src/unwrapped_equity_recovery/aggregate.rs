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
use st0x_execution::{FractionalShares, Symbol};
use st0x_raindex::Raindex;
use st0x_tokenization::IssuerRequestId;
use st0x_wrapper::{WrapConfirmation, Wrapper, WrapperError, node_sync_attempts};

use crate::equity_redemption::RedemptionAggregateId;
use crate::rebalancing::equity::CrossVenueEquityTransfer;
use crate::tokenized_equity_mint::TOKENIZED_EQUITY_DECIMALS;
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

/// Services the aggregate calls inside its command handlers.
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

/// Domain errors returned from the aggregate's `initialize`/`transition`
/// handlers. Terminal service failures (raindex/wrapper/transfer) are recorded
/// as `RecoveryFailed` events so failures remain first-class entries in the
/// audit trail. Retryable service failures that should leave the aggregate
/// in-place instead surface as errors through this enum (e.g. `NodeSyncFailed`
/// and the `Retryable*Confirmation` variants).
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

    #[error("wrap confirmation for tx {wrap_tx_hash} is retryable")]
    RetryableWrapConfirmation { wrap_tx_hash: TxHash },

    #[error("deposit confirmation for tx {vault_deposit_tx_hash} is retryable")]
    RetryableDepositConfirmation { vault_deposit_tx_hash: TxHash },
    #[error("RPC node did not catch up to wrap block {required_block} after {attempts} polls")]
    NodeSyncFailed { required_block: u64, attempts: u32 },
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
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use UnwrappedEquityRecoveryCommand::*;

        if self.is_terminal() {
            return Err(UnwrappedEquityRecoveryError::Terminal);
        }

        match (self, command) {
            (_, Detect { .. }) => Err(UnwrappedEquityRecoveryError::AlreadyInitialized),

            (Self::Detected { .. }, DispatchToMint { mint_id }) => {
                resume_mint_or_fail(&services.transfer, &mint_id).await
            }

            (Self::Detected { .. }, DispatchToRedemption { redemption_id }) => {
                resume_redemption_or_fail(&services.transfer, &redemption_id).await
            }

            (Self::Detected { symbol, shares, .. }, SubmitOrphanWrap) => {
                submit_orphan_wrap_or_fail(services, symbol, *shares).await
            }

            (
                Self::OrphanWrapSubmitted {
                    symbol,
                    wrap_tx_hash,
                    ..
                },
                ConfirmOrphanWrap,
            ) => confirm_orphan_wrap_or_fail(services, symbol, *wrap_tx_hash).await,

            (
                Self::OrphanWrapped {
                    symbol,
                    wrapped_amount,
                    wrap_block,
                    ..
                },
                SubmitOrphanDeposit,
            ) => {
                // Skip the wait for legacy aggregates persisted before wrap_block was added.
                if let Some(block) = wrap_block {
                    services
                        .wrapper
                        .wait_for_block(*block)
                        .await
                        .inspect_err(|error| {
                            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: wait_for_block failed");
                        })
                        .map_err(|error| UnwrappedEquityRecoveryError::NodeSyncFailed {
                            required_block: *block,
                            attempts: node_sync_attempts(&error),
                        })?;
                }
                submit_orphan_deposit_or_fail(services, symbol, *wrapped_amount).await
            }

            (
                Self::OrphanDepositSubmitted {
                    vault_deposit_tx_hash,
                    ..
                },
                ConfirmOrphanDeposit,
            ) => confirm_orphan_deposit_or_fail(&services.raindex, *vault_deposit_tx_hash).await,

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

async fn resume_mint_or_fail(
    transfer: &CrossVenueEquityTransfer,
    mint_id: &IssuerRequestId,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    match transfer.resume_mint(mint_id).await {
        Ok(()) => {
            info!(target: "rebalance", %mint_id, "Unwrapped equity recovery: resume_mint succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::DispatchedToMint {
                mint_id: mint_id.clone(),
                dispatched_at: Utc::now(),
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %mint_id, ?error, "Unwrapped equity recovery: resume_mint failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("resume_mint failed: {error}"),
                failed_at: Utc::now(),
            }])
        }
    }
}

async fn resume_redemption_or_fail(
    transfer: &CrossVenueEquityTransfer,
    redemption_id: &RedemptionAggregateId,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    match transfer.resume_redemption(redemption_id).await {
        Ok(()) => {
            info!(target: "rebalance", %redemption_id, "Unwrapped equity recovery: resume_redemption succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::DispatchedToRedemption {
                redemption_id: redemption_id.clone(),
                dispatched_at: Utc::now(),
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %redemption_id, ?error, "Unwrapped equity recovery: resume_redemption failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("resume_redemption failed: {error}"),
                failed_at: Utc::now(),
            }])
        }
    }
}

async fn submit_orphan_wrap_or_fail(
    services: &UnwrappedEquityRecoveryServices,
    symbol: &Symbol,
    shares: FractionalShares,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_derivative failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: Utc::now(),
            }]);
        }
    };

    let underlying_amount = match shares.to_u256_18_decimals() {
        Ok(raw) => raw,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: shares conversion failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("shares conversion failed: {error}"),
                failed_at: Utc::now(),
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
                submitted_at: Utc::now(),
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: submit_wrap failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.submit_wrap failed: {error}"),
                failed_at: Utc::now(),
            }])
        }
    }
}

async fn confirm_orphan_wrap_or_fail(
    services: &UnwrappedEquityRecoveryServices,
    symbol: &Symbol,
    wrap_tx_hash: TxHash,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_derivative failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: Utc::now(),
            }]);
        }
    };

    match services
        .wrapper
        .confirm_wrap(wrapped_token, wrap_tx_hash)
        .await
    {
        Ok(WrapConfirmation {
            shares: wrapped_amount,
            block: wrap_block,
        }) => {
            info!(target: "rebalance", %symbol, %wrap_tx_hash, %wrapped_amount, "Unwrapped equity recovery: confirm_wrap succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::OrphanWrapped {
                wrap_tx_hash,
                wrapped_amount,
                confirmed_at: Utc::now(),
                wrap_block: Some(wrap_block),
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %symbol, %wrap_tx_hash, ?error, "Unwrapped equity recovery: confirm_wrap failed");
            match error {
                WrapperError::MissingDepositEvent
                | WrapperError::Evm(st0x_evm::EvmError::TransactionDropped { .. }) => {
                    Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                        reason: format!("wrapper.confirm_wrap failed: {error}"),
                        failed_at: Utc::now(),
                    }])
                }
                _ => Err(UnwrappedEquityRecoveryError::RetryableWrapConfirmation { wrap_tx_hash }),
            }
        }
    }
}

async fn submit_orphan_deposit_or_fail(
    services: &UnwrappedEquityRecoveryServices,
    symbol: &Symbol,
    wrapped_amount: U256,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_derivative failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: Utc::now(),
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
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: vault_id_for_token failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("vault_lookup.vault_id_for_token failed: {error}"),
                failed_at: Utc::now(),
            }]);
        }
    };

    // `wrapped_amount` is the raw ERC-4626 share amount from the wrap's Deposit
    // event. Wrapped equity tokens (wtSTOCK) are minted at the system-wide
    // tokenized-equity precision -- `TOKENIZED_EQUITY_DECIMALS` (18), the same
    // standard the tSTOCK underlying and `FractionalShares` use -- so the raw
    // amount is already in 18-decimal units and is interpreted as such. The
    // wrapped recovery path leans on the identical invariant via
    // `FractionalShares::to_u256_18_decimals`; a wtSTOCK minted at a different
    // precision would mis-scale this deposit.
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
                submitted_at: Utc::now(),
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: submit_deposit failed");
            Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("raindex.submit_deposit failed: {error}"),
                failed_at: Utc::now(),
            }])
        }
    }
}

async fn confirm_orphan_deposit_or_fail(
    raindex: &Arc<dyn Raindex>,
    vault_deposit_tx_hash: TxHash,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    match raindex.confirm_tx(vault_deposit_tx_hash).await {
        Ok(()) => {
            info!(target: "rebalance", %vault_deposit_tx_hash, "Unwrapped equity recovery: confirm_tx succeeded");
            Ok(vec![UnwrappedEquityRecoveryEvent::OrphanDeposited {
                vault_deposit_tx_hash,
                deposited_at: Utc::now(),
            }])
        }
        Err(error) => {
            warn!(target: "rebalance", %vault_deposit_tx_hash, ?error, "Unwrapped equity recovery: confirm_tx failed");
            if error.is_transaction_dropped() {
                return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                    reason: format!("raindex.confirm_tx failed: {error}"),
                    failed_at: Utc::now(),
                }]);
            }

            Err(UnwrappedEquityRecoveryError::RetryableDepositConfirmation {
                vault_deposit_tx_hash,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, TxHash, fixed_bytes};
    use chrono::Utc;
    use rain_math_float::Float;

    use st0x_event_sorcery::EventSourced;
    use st0x_evm::NODE_SYNC_MAX_ATTEMPTS;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_raindex::RaindexVaultId;
    use st0x_tokenization::issuer_request_id;
    use st0x_tokenization::mock::MockTokenizer;
    use st0x_wrapper::MockWrapper;

    use crate::equity_redemption::redemption_aggregate_id;
    use crate::onchain::mock::{ConfirmTxBehavior, DepositBehavior, DepositCall, MockRaindex};
    use crate::vault_lookup::{MockVaultLookup, VaultLookup};

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

    fn mock_vault_lookup() -> MockVaultLookup {
        MockVaultLookup::new()
            .with_vault(Address::ZERO, RaindexVaultId(B256::ZERO))
            .with_default_vault(RaindexVaultId(B256::ZERO))
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

    async fn test_services() -> UnwrappedEquityRecoveryServices {
        services_with(Arc::new(MockRaindex::new()), Arc::new(MockWrapper::new())).await
    }

    /// Builds the aggregate's `Services` around caller-supplied raindex/wrapper
    /// mocks so failure-path tests can inject reverting/failing variants while
    /// the transfer service (mint/redemption resume) stays wired to fresh
    /// in-memory stores.
    async fn services_with(
        raindex: Arc<dyn Raindex>,
        wrapper: Arc<dyn Wrapper>,
    ) -> UnwrappedEquityRecoveryServices {
        services_with_vault_lookup(raindex, wrapper, Arc::new(mock_vault_lookup())).await
    }

    async fn services_with_vault_lookup(
        raindex: Arc<dyn Raindex>,
        wrapper: Arc<dyn Wrapper>,
        vault_lookup: Arc<dyn VaultLookup>,
    ) -> UnwrappedEquityRecoveryServices {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let mint_store = Arc::new(st0x_event_sorcery::test_store(pool.clone(), ()));
        let redemption_store = Arc::new(st0x_event_sorcery::test_store(pool, ()));
        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            raindex.clone(),
            vault_lookup.clone(),
            Arc::new(MockTokenizer::new()),
            wrapper.clone(),
            Address::random(),
            mint_store,
            redemption_store,
        ));
        UnwrappedEquityRecoveryServices {
            raindex,
            vault_lookup,
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

    #[tokio::test]
    async fn submit_orphan_wrap_emits_submitted_event() {
        let services = test_services().await;
        let events = detected()
            .transition(UnwrappedEquityRecoveryCommand::SubmitOrphanWrap, &services)
            .await
            .expect("SubmitOrphanWrap should succeed from Detected");
        let [UnwrappedEquityRecoveryEvent::OrphanWrapSubmitted { wrap_tx_hash, .. }] =
            events.as_slice()
        else {
            panic!("expected single OrphanWrapSubmitted event, got {events:?}");
        };
        assert_ne!(
            *wrap_tx_hash,
            TxHash::ZERO,
            "OrphanWrapSubmitted must carry the real submitted tx hash -- it is \
             the crash-recovery anchor ConfirmOrphanWrap confirms against",
        );
    }

    /// `submit_wrap` reverting is recorded as `RecoveryFailed`, not surfaced as
    /// an aggregate error -- service failures stay first-class in the audit trail.
    #[tokio::test]
    async fn submit_orphan_wrap_records_failure_when_wrap_fails() {
        let services = services_with(
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing()),
        )
        .await;
        let events = detected()
            .transition(UnwrappedEquityRecoveryCommand::SubmitOrphanWrap, &services)
            .await
            .expect("SubmitOrphanWrap should return Ok with RecoveryFailed on wrap failure");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("submit_wrap failed"),
            "reason should mention submit_wrap; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn submit_orphan_wrap_records_failure_when_derivative_lookup_fails() {
        let services = services_with(
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_derivative_lookup()),
        )
        .await;
        let events = detected()
            .transition(UnwrappedEquityRecoveryCommand::SubmitOrphanWrap, &services)
            .await
            .expect("SubmitOrphanWrap should return Ok with RecoveryFailed on lookup failure");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("lookup_derivative failed"),
            "reason should mention lookup_derivative; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn confirm_orphan_wrap_emits_confirmed_wrapped_amount() {
        let wrapper_mock = Arc::new(MockWrapper::new());
        let wrapped_amount = U256::from(7u64);
        wrapper_mock.seed_submitted_amount(FAKE_WRAP_TX, wrapped_amount);
        let services = services_with(Arc::new(MockRaindex::new()), wrapper_mock).await;
        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        let events = state
            .transition(UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap, &services)
            .await
            .expect("ConfirmOrphanWrap should succeed from OrphanWrapSubmitted");
        let [
            UnwrappedEquityRecoveryEvent::OrphanWrapped {
                wrap_tx_hash,
                wrapped_amount: confirmed,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("expected single OrphanWrapped event, got {events:?}");
        };
        assert_eq!(
            *confirmed, wrapped_amount,
            "OrphanWrapped should carry the actual minted wrapped amount",
        );
        assert_eq!(
            *wrap_tx_hash, FAKE_WRAP_TX,
            "OrphanWrapped should carry the submitted wrap tx hash",
        );
    }

    /// `confirm_wrap` failing on a submitted wrap is recorded as
    /// `RecoveryFailed`, not surfaced as an aggregate error.
    #[tokio::test]
    async fn confirm_orphan_wrap_records_failure_when_confirm_wrap_fails() {
        let services = services_with(
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_confirm_wrap()),
        )
        .await;
        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        let events = state
            .transition(UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap, &services)
            .await
            .expect("ConfirmOrphanWrap should return Ok with RecoveryFailed on confirm failure");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("confirm_wrap failed"),
            "reason should mention confirm_wrap; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn confirm_orphan_wrap_retryable_error_keeps_submitted_state_live() {
        let services = services_with(
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::retryable_confirm_wrap()),
        )
        .await;
        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        let error = state
            .transition(UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap, &services)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            UnwrappedEquityRecoveryError::RetryableWrapConfirmation {
                wrap_tx_hash: FAKE_WRAP_TX,
            }
        ));
    }

    #[tokio::test]
    async fn confirm_orphan_wrap_records_failure_when_derivative_lookup_fails_at_confirm() {
        let services = services_with(
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_derivative_lookup()),
        )
        .await;
        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        let events = state
            .transition(UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap, &services)
            .await
            .expect("ConfirmOrphanWrap should return Ok with RecoveryFailed on lookup failure");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("lookup_derivative failed"),
            "reason should mention lookup_derivative; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn submit_orphan_deposit_emits_submitted_event() {
        let raindex = Arc::new(MockRaindex::new());
        let wrapped_token = Address::random();
        let services = services_with(
            raindex.clone(),
            Arc::new(MockWrapper::new().with_wrapped_token(wrapped_token)),
        )
        .await;
        let events = orphan_wrapped()
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit,
                &services,
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
        assert_ne!(
            *vault_deposit_tx_hash,
            TxHash::ZERO,
            "OrphanDepositSubmitted must carry the real deposit tx hash -- it is \
             the crash-recovery anchor ConfirmOrphanDeposit confirms against",
        );
        assert_eq!(
            raindex.last_deposit_call(),
            Some(DepositCall {
                token: wrapped_token,
                vault_id: RaindexVaultId(B256::ZERO),
                amount: U256::from(123u64),
                decimals: TOKENIZED_EQUITY_DECIMALS,
            }),
            "SubmitOrphanDeposit must deposit the confirmed wrapped amount into the \
             wrapped token vault at tokenized-equity precision",
        );
    }

    #[tokio::test]
    async fn submit_orphan_deposit_records_failure_when_raindex_reverts() {
        let services = services_with(
            Arc::new(
                MockRaindex::new().with_deposit_behavior(DepositBehavior::FailExecutionReverted),
            ),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let events = orphan_wrapped()
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit,
                &services,
            )
            .await
            .expect("SubmitOrphanDeposit should return Ok with RecoveryFailed on revert");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("submit_deposit failed"),
            "reason should mention submit_deposit; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn submit_orphan_deposit_records_failure_when_vault_lookup_fails() {
        let wrapped_token = Address::random();
        let services = services_with_vault_lookup(
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::new().with_wrapped_token(wrapped_token)),
            Arc::new(MockVaultLookup::new()),
        )
        .await;
        let events = orphan_wrapped()
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit,
                &services,
            )
            .await
            .expect("SubmitOrphanDeposit should return Ok with RecoveryFailed on lookup failure");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("vault_lookup.vault_id_for_token failed"),
            "reason should mention vault lookup failure; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn confirm_orphan_deposit_completes_orphan_branch() {
        let raindex = Arc::new(MockRaindex::new());
        let services = services_with(raindex.clone(), Arc::new(MockWrapper::new())).await;
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
            .transition(
                UnwrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
                &services,
            )
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
            "OrphanDeposited must carry the submitted deposit tx hash -- it is \
             the idempotency anchor confirm_tx ran against",
        );
        assert_eq!(
            raindex.last_confirmed_tx(),
            Some(FAKE_WRAP_TX),
            "ConfirmOrphanDeposit must confirm the persisted deposit tx hash",
        );
    }

    /// `confirm_tx` failing on the final deposit confirmation is recorded as
    /// `RecoveryFailed`, not surfaced as an aggregate error.
    #[tokio::test]
    async fn confirm_orphan_deposit_records_failure_when_confirm_tx_fails() {
        let services = services_with(
            Arc::new(MockRaindex::new().with_confirm_behavior(ConfirmTxBehavior::Fail)),
            Arc::new(MockWrapper::new()),
        )
        .await;
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
            .transition(
                UnwrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
                &services,
            )
            .await
            .expect("ConfirmOrphanDeposit should return Ok with RecoveryFailed on confirm failure");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("confirm_tx failed"),
            "reason should mention confirm_tx; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn confirm_orphan_deposit_retryable_error_keeps_submitted_state_live() {
        let services = services_with(
            Arc::new(MockRaindex::new().with_confirm_behavior(ConfirmTxBehavior::Retryable)),
            Arc::new(MockWrapper::new()),
        )
        .await;
        let state = UnwrappedEquityRecovery::OrphanDepositSubmitted {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            wrapped_amount: U256::from(123u64),
            vault_deposit_tx_hash: FAKE_WRAP_TX,
            deposit_submitted_at: Utc::now(),
        };
        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
                &services,
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            UnwrappedEquityRecoveryError::RetryableDepositConfirmation {
                vault_deposit_tx_hash: FAKE_WRAP_TX,
            }
        ));
    }

    /// `resume_mint` fails because no mint aggregate exists -> the handler
    /// records the failure as `RecoveryFailed` rather than erroring.
    #[tokio::test]
    async fn dispatch_to_mint_records_failure_when_resume_mint_fails() {
        let services = test_services().await;
        let events = detected()
            .transition(
                UnwrappedEquityRecoveryCommand::DispatchToMint {
                    mint_id: issuer_request_id("ISS-NONEXISTENT"),
                },
                &services,
            )
            .await
            .expect("DispatchToMint should return Ok with RecoveryFailed on service failure");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("resume_mint failed"),
            "reason should mention resume_mint; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn dispatch_to_redemption_records_failure_when_resume_redemption_fails() {
        let services = test_services().await;
        let events = detected()
            .transition(
                UnwrappedEquityRecoveryCommand::DispatchToRedemption {
                    redemption_id: redemption_aggregate_id("nonexistent"),
                },
                &services,
            )
            .await
            .expect("DispatchToRedemption should return Ok with RecoveryFailed on service failure");
        let [UnwrappedEquityRecoveryEvent::RecoveryFailed { reason, .. }] = events.as_slice()
        else {
            panic!("expected single RecoveryFailed event, got {events:?}");
        };
        assert!(
            reason.contains("resume_redemption failed"),
            "reason should mention resume_redemption; got {reason:?}",
        );
    }

    #[tokio::test]
    async fn fail_recovery_from_detected_emits_recovery_failed() {
        let services = test_services().await;
        let events = detected()
            .transition(
                UnwrappedEquityRecoveryCommand::FailRecovery {
                    reason: "operator abort".to_string(),
                },
                &services,
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

    #[tokio::test]
    async fn submit_orphan_deposit_with_wrap_block_calls_wait_for_block() {
        let raindex = Arc::new(MockRaindex::new());
        let mock_wrapper = Arc::new(MockWrapper::new().with_wrapped_token(Address::random()));
        let services = services_with(
            raindex.clone(),
            Arc::clone(&mock_wrapper) as Arc<dyn Wrapper>,
        )
        .await;

        let state = UnwrappedEquityRecovery::OrphanWrapped {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            wrap_submitted_at: Utc::now(),
            wrapped_amount: U256::from(123u64),
            wrap_confirmed_at: Utc::now(),
            wrap_block: Some(9999),
        };

        let events = state
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit,
                &services,
            )
            .await
            .expect("SubmitOrphanDeposit with wrap_block=Some(9999) should succeed");

        let [UnwrappedEquityRecoveryEvent::OrphanDepositSubmitted { .. }] = events.as_slice()
        else {
            panic!("expected OrphanDepositSubmitted, got {events:?}");
        };

        assert_eq!(
            mock_wrapper.wait_for_block_calls(),
            vec![9999u64],
            "wait_for_block must be called exactly once with wrap_block=9999"
        );
    }

    #[tokio::test]
    async fn submit_orphan_deposit_propagates_err_when_wait_for_block_fails() {
        let services = services_with(
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_wait_for_block()),
        )
        .await;

        let state = UnwrappedEquityRecovery::OrphanWrapped {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            wrap_submitted_at: Utc::now(),
            wrapped_amount: U256::from(123u64),
            wrap_confirmed_at: Utc::now(),
            wrap_block: Some(9999),
        };

        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit,
                &services,
            )
            .await
            .expect_err("wait_for_block failure must propagate as retryable Err");

        assert!(
            matches!(
                error,
                UnwrappedEquityRecoveryError::NodeSyncFailed {
                    required_block: 9999,
                    ..
                }
            ),
            "wait_for_block failure must surface as NodeSyncFailed, got: {error:?}"
        );
    }

    /// Verifies that the `_ => NODE_SYNC_MAX_ATTEMPTS` fallback arm in the
    /// `SubmitOrphanDeposit` error-mapping closure is exercised when
    /// `wait_for_block` fails with a transport error (as opposed to the
    /// `NodeBehindRequiredBlock` arm covered by
    /// `submit_orphan_deposit_propagates_err_when_wait_for_block_fails`).
    ///
    /// A transport error means every poll failed before any block number was
    /// observed, so the budget was fully consumed; the fallback must still
    /// produce `NodeSyncFailed` with `attempts == NODE_SYNC_MAX_ATTEMPTS`.
    #[tokio::test]
    async fn submit_orphan_deposit_propagates_err_when_wait_for_block_fails_with_transport_error() {
        let services = services_with(
            Arc::new(MockRaindex::new()),
            Arc::new(MockWrapper::failing_wait_for_block_transport_error()),
        )
        .await;

        let state = UnwrappedEquityRecovery::OrphanWrapped {
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            wrap_submitted_at: Utc::now(),
            wrapped_amount: U256::from(123u64),
            wrap_confirmed_at: Utc::now(),
            wrap_block: Some(9999),
        };

        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit,
                &services,
            )
            .await
            .expect_err("transport error from wait_for_block must propagate as retryable Err");

        assert!(
            matches!(
                error,
                UnwrappedEquityRecoveryError::NodeSyncFailed {
                    required_block: 9999,
                    attempts: NODE_SYNC_MAX_ATTEMPTS,
                }
            ),
            "transport error must map to NodeSyncFailed with attempts=NODE_SYNC_MAX_ATTEMPTS, \
             got: {error:?}"
        );
    }
}
