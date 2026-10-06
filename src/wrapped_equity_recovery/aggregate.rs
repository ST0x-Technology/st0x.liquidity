//! Aggregate recording automated recovery of wrapped equity tokens
//! (wtSTOCK) found on a chain's bot wallet outside the Raindex vault.
//!
//! Every recovery records the chain it runs on at `Detect`, and each
//! handler resolves that chain's services from the record, never from the
//! primary chain.
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
use sqlx::SqlitePool;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_evm::Chain;
use st0x_execution::{FractionalShares, Symbol};
use st0x_raindex::Raindex;
use st0x_tokenization::IssuerRequestId;

use crate::bot_gas::{
    BotGasEnqueueFailure, BotGasOperationCategory, BotGasReceiptCostEnqueuer, enqueue_equity_cost,
};
use crate::equity_redemption::RedemptionAggregateId;
use crate::rebalancing::equity::{
    ChainEquityServices, ChainServicesMissing, CrossVenueEquityTransfer, EquityTransferServices,
};
use crate::tokenized_equity_mint::TOKENIZED_EQUITY_DECIMALS;

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
    /// Every chain's entry. A recovery drives the entry of the chain it
    /// records, so it reads the same registry, orderbook and wrapper the
    /// saga uses there.
    pub(crate) equity: EquityTransferServices,
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
    /// Enqueues bot-gas cost recording for the orphan-deposit path (which
    /// calls `raindex` directly rather than through `transfer`). See ADR 0017.
    pub(crate) bot_gas_enqueuer: BotGasReceiptCostEnqueuer,
}

/// Domain errors returned from the aggregate's `initialize`/`transition`
/// handlers. Most service failures (raindex/wrapper/transfer) do NOT flow
/// through this enum -- they are recorded as `RecoveryFailed` events instead,
/// so failures remain first-class entries in the audit trail. The
/// exceptions are `ChainServicesMissing` (see its doc) and
/// `BotGasEnqueueFailed`: a bot-gas cost-recording bookkeeping
/// failure inside `confirm_orphan_deposit_or_fail` is deliberately propagated
/// as `Err` rather than folded into `RecoveryFailed`, so the job's shared
/// `redrive_on_bot_gas_failure` mechanism can redrive it instead of
/// permanently failing the recovery over a best-effort accounting write.
///
/// NOTE: `resume_mint_or_fail`/`resume_redemption_or_fail` (the
/// `DispatchToMint`/`DispatchToRedemption` handlers) deliberately do NOT
/// propagate a bot-gas enqueue failure this way -- see the "Known gaps"
/// entry in SPEC.md's bot-gas section. The job now resumes from `Detected`,
/// so a redrive would no longer strand the record, but propagating the
/// failure from these handlers is not wired yet.
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
    /// The recovery's chain has no equity services in this configuration.
    /// Not a terminal failure: no event is recorded, so the recovery stays
    /// open and the job retries once the chain is wired again.
    #[error(transparent)]
    ChainServicesMissing(#[from] ChainServicesMissing),
    /// Enqueueing the bot-gas receipt cost recording job failed after the
    /// preceding on-chain confirm step already succeeded (a local SQLite
    /// write, safe to retry since the aggregate has not advanced past the
    /// confirmed state yet). See [`BotGasEnqueueFailure`] for why the
    /// payload is a rendered `String` rather than a typed source.
    #[error(transparent)]
    BotGasEnqueueFailed(#[from] BotGasEnqueueFailure),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecoveryCommand {
    /// Initial command. Records the detection trigger and the chain whose
    /// wallet holds the shares. Refused with `ChainServicesMissing` when that
    /// chain has no services, so no recovery opens that could not proceed.
    Detect {
        chain: Chain,
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

    /// Orphan path. On the recovery's chain, the handler resolves the
    /// wrapped-token address via `wrapper.lookup_derivative(symbol)`, looks up
    /// the Raindex vault, calls `raindex.submit_deposit(...)`, and emits
    /// `OrphanDepositSubmitted` with the returned tx hash.
    SubmitOrphanDeposit,

    /// Orphan path. The handler reads `vault_deposit_tx_hash` from the
    /// current state and calls the recovery chain's `raindex.confirm_tx`,
    /// emitting `OrphanDeposited` iff confirmation succeeds.
    ConfirmOrphanDeposit,

    /// Marks the recovery as failed with the supplied reason.
    FailRecovery { reason: String },

    /// Fails a changed detection and durably records that it needs a successor.
    FailStaleDetection { reason: String },

    /// Closes an invalid detection while retaining any existing recovery hold.
    FailInvalidDetection { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecoveryEvent {
    Detected {
        /// Events recorded before recoveries named their chain ran on Base.
        #[serde(default = "crate::onchain::legacy_chain")]
        chain: Chain,
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

    InvalidDetectionFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },

    StaleDetectionFailed {
        reason: String,
        failed_at: DateTime<Utc>,
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
            Self::InvalidDetectionFailed { .. } => {
                "WrappedEquityRecoveryEvent::InvalidDetectionFailed"
            }
            Self::StaleDetectionFailed { .. } => "WrappedEquityRecoveryEvent::StaleDetectionFailed",
        }
        .to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DetectionFailureDisposition {
    RestartDetection,
    PreserveHold,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum WrappedEquityRecovery {
    Detected {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
    },

    DispatchedToMint {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        mint_id: IssuerRequestId,
        dispatched_at: DateTime<Utc>,
    },

    DispatchedToRedemption {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        redemption_id: RedemptionAggregateId,
        dispatched_at: DateTime<Utc>,
    },

    OrphanDepositSubmitted {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        vault_deposit_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },

    OrphanDeposited {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        vault_deposit_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
        deposited_at: DateTime<Utc>,
    },

    Failed {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        reason: String,
        failed_at: DateTime<Utc>,
        #[serde(default)]
        detection_failure: Option<DetectionFailureDisposition>,
    },
}

impl WrappedEquityRecovery {
    /// The chain whose wallet holds the recovered shares.
    pub(crate) fn chain(&self) -> Chain {
        match self {
            Self::Detected { chain, .. }
            | Self::DispatchedToMint { chain, .. }
            | Self::DispatchedToRedemption { chain, .. }
            | Self::OrphanDepositSubmitted { chain, .. }
            | Self::OrphanDeposited { chain, .. }
            | Self::Failed { chain, .. } => *chain,
        }
    }

    pub(crate) fn symbol(&self) -> &Symbol {
        match self {
            Self::Detected { symbol, .. }
            | Self::DispatchedToMint { symbol, .. }
            | Self::DispatchedToRedemption { symbol, .. }
            | Self::OrphanDepositSubmitted { symbol, .. }
            | Self::OrphanDeposited { symbol, .. }
            | Self::Failed { symbol, .. } => symbol,
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::DispatchedToMint { .. }
                | Self::DispatchedToRedemption { .. }
                | Self::OrphanDeposited { .. }
                | Self::Failed { .. }
        )
    }
}

/// Every recovery whose latest event leaves it open. Startup reads them to
/// refuse a configuration that builds no services for a chain an open
/// recovery still has to act on.
pub(crate) async fn open_recovery_ids(
    pool: &SqlitePool,
) -> Result<Vec<WrappedEquityRecoveryId>, sqlx::Error> {
    let rows: Vec<String> = sqlx::query_scalar(
        "WITH latest AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'WrappedEquityRecovery' \
             GROUP BY aggregate_id \
         ) \
         SELECT latest.aggregate_id \
         FROM events last_ev \
         INNER JOIN latest \
             ON last_ev.aggregate_id = latest.aggregate_id \
            AND last_ev.sequence = latest.max_seq \
         WHERE last_ev.aggregate_type = 'WrappedEquityRecovery' \
           AND last_ev.event_type IN ( \
               'WrappedEquityRecoveryEvent::Detected', \
               'WrappedEquityRecoveryEvent::OrphanDepositSubmitted' \
           ) \
         ORDER BY latest.aggregate_id",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            row.parse()
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))
        })
        .collect()
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
    const SCHEMA_VERSION: u64 = 2;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            WrappedEquityRecoveryEvent::Detected {
                chain,
                symbol,
                shares,
                detected_at,
            } => Some(Self::Detected {
                chain: *chain,
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
                    chain,
                    symbol,
                    shares,
                    detected_at,
                },
                DispatchedToMint {
                    mint_id,
                    dispatched_at,
                },
            ) => Some(Self::DispatchedToMint {
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                mint_id: mint_id.clone(),
                dispatched_at: *dispatched_at,
            }),

            (
                Self::Detected {
                    chain,
                    symbol,
                    shares,
                    detected_at,
                },
                DispatchedToRedemption {
                    redemption_id,
                    dispatched_at,
                },
            ) => Some(Self::DispatchedToRedemption {
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                redemption_id: redemption_id.clone(),
                dispatched_at: *dispatched_at,
            }),

            (
                Self::Detected {
                    chain,
                    symbol,
                    shares,
                    detected_at,
                },
                OrphanDepositSubmitted {
                    vault_deposit_tx_hash,
                    submitted_at,
                },
            ) => Some(Self::OrphanDepositSubmitted {
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                submitted_at: *submitted_at,
            }),

            (
                Self::OrphanDepositSubmitted {
                    chain,
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
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                submitted_at: *submitted_at,
                deposited_at: *deposited_at,
            }),

            (
                Self::Detected {
                    chain,
                    symbol,
                    shares,
                    ..
                }
                | Self::OrphanDepositSubmitted {
                    chain,
                    symbol,
                    shares,
                    ..
                },
                RecoveryFailed { reason, failed_at },
            ) => Some(Self::Failed {
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                reason: reason.clone(),
                failed_at: *failed_at,
                detection_failure: None,
            }),

            (
                Self::Detected {
                    chain,
                    symbol,
                    shares,
                    ..
                },
                StaleDetectionFailed { reason, failed_at },
            ) => Some(Self::Failed {
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                reason: reason.clone(),
                failed_at: *failed_at,
                detection_failure: Some(DetectionFailureDisposition::RestartDetection),
            }),

            (
                Self::Detected {
                    chain,
                    symbol,
                    shares,
                    ..
                },
                InvalidDetectionFailed { reason, failed_at },
            ) => Some(Self::Failed {
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                reason: reason.clone(),
                failed_at: *failed_at,
                detection_failure: Some(DetectionFailureDisposition::PreserveHold),
            }),

            _ => None,
        })
    }

    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            WrappedEquityRecoveryCommand::Detect {
                chain,
                symbol,
                shares,
            } => {
                services.equity.for_chain(chain)?;

                Ok(vec![WrappedEquityRecoveryEvent::Detected {
                    chain,
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

            // The transfer resolves its own chain's entry; checking the
            // recovery's chain first keeps an unwired chain from recording a
            // terminal failure for a recovery that can still finish. The job
            // dispatches only to a transfer on the recovery's chain
            // (`validate_active_aggregate_quantity`), so the two match here.
            (Self::Detected { chain, .. }, DispatchToMint { mint_id }) => {
                services.equity.for_chain(*chain)?;
                resume_mint_or_fail(&services.transfer, &mint_id, now).await
            }

            (Self::Detected { chain, .. }, DispatchToRedemption { redemption_id }) => {
                services.equity.for_chain(*chain)?;
                resume_redemption_or_fail(&services.transfer, &redemption_id, now).await
            }

            (
                Self::Detected {
                    chain,
                    symbol,
                    shares,
                    ..
                },
                SubmitOrphanDeposit,
            ) => {
                let chain_services = services.equity.for_chain(*chain)?;
                submit_orphan_deposit_or_fail(chain_services, symbol, *shares, now).await
            }

            (
                Self::OrphanDepositSubmitted {
                    chain,
                    symbol,
                    vault_deposit_tx_hash,
                    ..
                },
                ConfirmOrphanDeposit,
            ) => {
                let chain_services = services.equity.for_chain(*chain)?;
                confirm_orphan_deposit_or_fail(
                    *chain,
                    &chain_services.raindex,
                    &services.bot_gas_enqueuer,
                    symbol,
                    *vault_deposit_tx_hash,
                    now,
                )
                .await
            }

            (
                Self::Detected { .. } | Self::OrphanDepositSubmitted { .. },
                FailRecovery { reason },
            ) => Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason,
                failed_at: now,
            }]),

            (Self::Detected { .. }, FailStaleDetection { reason }) => {
                Ok(vec![WrappedEquityRecoveryEvent::StaleDetectionFailed {
                    reason,
                    failed_at: now,
                }])
            }

            (Self::Detected { .. }, FailInvalidDetection { reason }) => {
                Ok(vec![WrappedEquityRecoveryEvent::InvalidDetectionFailed {
                    reason,
                    failed_at: now,
                }])
            }

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
        // A redemption still resolving onchain (a pending withdrawal or issuer
        // send, or a legacy send an operator must verify) has its own jobs
        // driving it, so the recovery has handed it over rather than failed.
        Err(error) if error.is_still_in_progress() => {
            info!(target: "rebalance", %redemption_id, %error, "Wrapped equity recovery: the redemption is still resolving and drives itself");
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
    chain_services: &ChainEquityServices,
    symbol: &Symbol,
    shares: FractionalShares,
    now: DateTime<Utc>,
) -> Result<Vec<WrappedEquityRecoveryEvent>, WrappedEquityRecoveryError> {
    let wrapped_token = match chain_services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Wrapped equity recovery: lookup_derivative failed");
            return Ok(vec![WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: now,
            }]);
        }
    };

    let vault_id = match chain_services
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

    match chain_services
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
    chain: Chain,
    raindex: &Arc<dyn Raindex>,
    bot_gas_enqueuer: &BotGasReceiptCostEnqueuer,
    symbol: &Symbol,
    vault_deposit_tx_hash: TxHash,
    now: DateTime<Utc>,
) -> Result<Vec<WrappedEquityRecoveryEvent>, WrappedEquityRecoveryError> {
    match raindex.confirm_tx(vault_deposit_tx_hash).await {
        Ok(()) => {
            info!(target: "rebalance", %chain, %vault_deposit_tx_hash, "Wrapped equity recovery: confirm_tx succeeded");

            enqueue_equity_cost(
                bot_gas_enqueuer,
                chain,
                vault_deposit_tx_hash,
                BotGasOperationCategory::VaultDeposit,
                symbol,
            )
            .await?;

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
    use std::collections::BTreeMap;
    use uuid::Uuid;

    use st0x_config::ChainEquities;
    use st0x_event_sorcery::{AggregateError, EventSourced, LifecycleError};
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_raindex::RaindexVaultId;
    use st0x_tokenization::issuer_request_id;
    use st0x_tokenization::mock::MockTokenizer;
    use st0x_wrapper::MockWrapper;

    use super::*;
    use crate::bot_gas::pending_bot_gas_jobs;
    use crate::equity_redemption::redemption_aggregate_id;
    use crate::mint_authorization::ConfiguredMintAuthorizer;
    use crate::native_gas::ConfiguredGasReadiness;
    use crate::onchain::mock::{DepositBehavior, MockRaindex};
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::vault_lookup::MockVaultLookup;

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
            chain: Chain::Base,
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        }
    }

    fn chain_services_with(raindex: Arc<dyn Raindex>) -> ChainEquityServices {
        ChainEquityServices {
            wallet: Address::ZERO,
            raindex,
            vault_lookup: Arc::new(mock_vault_lookup()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
            gas_readiness: ConfiguredGasReadiness::Unwired,
            equities: ChainEquities::default(),
        }
    }

    /// Recovery services carrying exactly `chains`, with the transfer wired
    /// to fresh in-memory mint and redemption stores over the same entries.
    async fn services_over(
        chains: BTreeMap<Chain, ChainEquityServices>,
    ) -> WrappedEquityRecoveryServices {
        services_and_pool_over(chains).await.0
    }

    /// [`services_over`], also returning the pool the stores write to.
    async fn services_and_pool_over(
        chains: BTreeMap<Chain, ChainEquityServices>,
    ) -> (WrappedEquityRecoveryServices, sqlx::SqlitePool) {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let equity = EquityTransferServices {
            chains,
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };
        let mint_store = Arc::new(st0x_event_sorcery::test_store(pool.clone(), equity.clone()));
        let redemption_store =
            Arc::new(st0x_event_sorcery::test_store(pool.clone(), equity.clone()));
        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            equity.clone(),
            mint_store,
            redemption_store,
        ));
        (
            WrappedEquityRecoveryServices {
                equity,
                transfer,
                bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
            },
            pool,
        )
    }

    async fn test_services() -> WrappedEquityRecoveryServices {
        services_over(BTreeMap::from([(
            Chain::Base,
            chain_services_with(Arc::new(MockRaindex::new())),
        )]))
        .await
    }

    #[tokio::test]
    async fn detect_initializes_aggregate_into_detected_state() {
        let services = test_services().await;
        let events = WrappedEquityRecovery::initialize(
            WrappedEquityRecoveryCommand::Detect {
                chain: Chain::Base,
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
            chain: Chain::Base,
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

    /// Acceptance criterion: confirming an orphan deposit
    /// enqueues exactly one `VaultDeposit` bot-gas job on Base with the
    /// recovery's symbol.
    #[tokio::test]
    async fn confirm_orphan_deposit_enqueues_vault_deposit_bot_gas_job() {
        let (_pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = crate::bot_gas::RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let mut services = test_services().await;
        services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(queue);

        let submitted = WrappedEquityRecovery::OrphanDepositSubmitted {
            chain: Chain::Base,
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            vault_deposit_tx_hash: FAKE_TX_HASH,
            submitted_at: Utc::now(),
        };

        submitted
            .transition(
                WrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
                &services,
            )
            .await
            .expect("ConfirmOrphanDeposit should succeed from OrphanDepositSubmitted");

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(jobs.len(), 1, "expected exactly one bot-gas job");
        assert_eq!(jobs[0].category, BotGasOperationCategory::VaultDeposit);
        assert_eq!(jobs[0].chain, Chain::Base);
        assert_eq!(jobs[0].tx_hash, FAKE_TX_HASH);
        assert_eq!(jobs[0].symbol, Some(aapl()));
    }

    /// A recovery on Ethereum runs its orphan deposit end to end on
    /// Ethereum's orderbook and charges the gas to Ethereum. Base's orderbook
    /// fails every deposit, so reaching `OrphanDeposited` also shows Base was
    /// never used.
    #[tokio::test]
    async fn orphan_deposit_on_a_secondary_chain_runs_on_that_chains_services() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let base_raindex = Arc::new(
            MockRaindex::new().with_deposit_behavior(DepositBehavior::FailExecutionReverted),
        );
        let ethereum_raindex = Arc::new(MockRaindex::new());
        let mut services = services_over(BTreeMap::from([
            (Chain::Base, chain_services_with(base_raindex.clone())),
            (
                Chain::Ethereum,
                chain_services_with(ethereum_raindex.clone()),
            ),
        ]))
        .await;
        services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(
            crate::bot_gas::RecordBotGasReceiptCostJobQueue::new(&apalis_pool),
        );
        let store = st0x_event_sorcery::test_store::<WrappedEquityRecovery>(pool, services);
        let id = WrappedEquityRecoveryId(Uuid::new_v4());

        for command in [
            WrappedEquityRecoveryCommand::Detect {
                chain: Chain::Ethereum,
                symbol: aapl(),
                shares: one_share(),
            },
            WrappedEquityRecoveryCommand::SubmitOrphanDeposit,
            WrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
        ] {
            store.send(&id, command).await.unwrap();
        }

        let Some(WrappedEquityRecovery::OrphanDeposited {
            chain,
            vault_deposit_tx_hash,
            ..
        }) = store.load(&id).await.unwrap()
        else {
            panic!("the Ethereum recovery must finish its orphan deposit");
        };
        assert_eq!(chain, Chain::Ethereum);
        assert_eq!(
            ethereum_raindex.last_confirmed_tx(),
            Some(vault_deposit_tx_hash)
        );
        assert_eq!(base_raindex.last_deposit_call(), None);
        assert_eq!(base_raindex.last_confirmed_tx(), None);

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(jobs.len(), 1, "expected exactly one bot-gas job");
        assert_eq!(jobs[0].chain, Chain::Ethereum);
        assert_eq!(jobs[0].tx_hash, vault_deposit_tx_hash);
    }

    /// A chain with no services cannot open a recovery, and cannot move one
    /// already open on it: each command is refused with no event recorded,
    /// so the recovery stays open for when the chain is wired again.
    #[tokio::test]
    async fn a_chain_without_services_refuses_commands_without_recording_an_event() {
        let (pool, _apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wired = st0x_event_sorcery::test_store::<WrappedEquityRecovery>(
            pool.clone(),
            services_over(BTreeMap::from([(
                Chain::Ethereum,
                chain_services_with(Arc::new(MockRaindex::new())),
            )]))
            .await,
        );
        let unwired =
            st0x_event_sorcery::test_store::<WrappedEquityRecovery>(pool, test_services().await);

        let refused_id = WrappedEquityRecoveryId(Uuid::new_v4());
        let error = unwired
            .send(
                &refused_id,
                WrappedEquityRecoveryCommand::Detect {
                    chain: Chain::Ethereum,
                    symbol: aapl(),
                    shares: one_share(),
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    WrappedEquityRecoveryError::ChainServicesMissing(ChainServicesMissing {
                        chain: Chain::Ethereum
                    })
                ))
            ),
            "got {error:?}"
        );
        assert_eq!(unwired.load(&refused_id).await.unwrap(), None);

        let open_id = WrappedEquityRecoveryId(Uuid::new_v4());
        wired
            .send(
                &open_id,
                WrappedEquityRecoveryCommand::Detect {
                    chain: Chain::Ethereum,
                    symbol: aapl(),
                    shares: one_share(),
                },
            )
            .await
            .unwrap();

        let error = unwired
            .send(&open_id, WrappedEquityRecoveryCommand::SubmitOrphanDeposit)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    WrappedEquityRecoveryError::ChainServicesMissing(ChainServicesMissing {
                        chain: Chain::Ethereum
                    })
                ))
            ),
            "got {error:?}"
        );
        let state = unwired.load(&open_id).await.unwrap().unwrap();
        assert!(
            matches!(
                state,
                WrappedEquityRecovery::Detected {
                    chain: Chain::Ethereum,
                    ..
                }
            ),
            "the refused command must leave the recovery open, got {state:?}"
        );
    }

    #[test]
    fn legacy_detected_event_without_a_chain_loads_as_base() {
        let legacy = serde_json::json!({
            "Detected": {
                "symbol": "AAPL",
                "shares": "1",
                "detected_at": "2026-01-01T00:00:00Z"
            }
        });

        let event: WrappedEquityRecoveryEvent = serde_json::from_value(legacy).unwrap();
        let state = WrappedEquityRecovery::originate(&event).unwrap();

        assert_eq!(state.chain(), Chain::Base);
        assert_eq!(state.symbol(), &aapl());
    }

    /// A recovery left open by a binary whose events and snapshots carried no
    /// chain: the schema bump discards the old snapshot, replay reads the
    /// legacy `Detected` as Base, and the recovery finishes on Base.
    #[tokio::test]
    async fn open_legacy_recovery_resumes_on_base_after_upgrade() {
        let pool = crate::test_utils::setup_test_db().await;
        let id = WrappedEquityRecoveryId(Uuid::new_v4());
        let deposit_tx = FAKE_TX_HASH;
        let detected =
            r#"{"Detected":{"symbol":"AAPL","shares":"1","detected_at":"2026-01-01T00:00:00Z"}}"#;
        let submitted = format!(
            r#"{{"OrphanDepositSubmitted":{{"vault_deposit_tx_hash":"{deposit_tx}","submitted_at":"2026-01-01T00:00:01Z"}}}}"#
        );
        let legacy_snapshot = format!(
            r#"{{"Live":{{"OrphanDepositSubmitted":{{"symbol":"AAPL","shares":"1","detected_at":"2026-01-01T00:00:00Z","vault_deposit_tx_hash":"{deposit_tx}","submitted_at":"2026-01-01T00:00:01Z"}}}}}}"#
        );
        for (sequence, event_type, payload) in [
            (
                1,
                "WrappedEquityRecoveryEvent::Detected",
                detected.to_string(),
            ),
            (
                2,
                "WrappedEquityRecoveryEvent::OrphanDepositSubmitted",
                submitted,
            ),
        ] {
            sqlx::query(
                "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
                 event_version, payload, metadata) \
                 VALUES ('WrappedEquityRecovery', ?1, ?2, ?3, '1.0', ?4, '{}')",
            )
            .bind(id.to_string())
            .bind(sequence)
            .bind(event_type)
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
             event_version, payload, metadata) VALUES ('SchemaRegistry', 'schema', 1, \
             'SchemaRegistryEvent::VersionUpdated', '1.0', \
             '{\"VersionUpdated\":{\"name\":\"WrappedEquityRecovery\",\"version\":1}}', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence, payload, \
             timestamp) VALUES ('WrappedEquityRecovery', ?1, 2, ?2, '2026-01-01T00:00:01Z')",
        )
        .bind(id.to_string())
        .bind(legacy_snapshot)
        .execute(&pool)
        .await
        .unwrap();

        let base_raindex = Arc::new(MockRaindex::new());
        let store = st0x_event_sorcery::StoreBuilder::<WrappedEquityRecovery>::new(pool.clone())
            .build(
                services_over(BTreeMap::from([(
                    Chain::Base,
                    chain_services_with(base_raindex.clone()),
                )]))
                .await,
            )
            .await
            .unwrap();

        assert_eq!(open_recovery_ids(&pool).await.unwrap(), vec![id.clone()]);
        let resumed = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                resumed,
                WrappedEquityRecovery::OrphanDepositSubmitted {
                    chain: Chain::Base,
                    ..
                }
            ),
            "the legacy recovery must replay as an open Base recovery, got {resumed:?}"
        );

        store
            .send(&id, WrappedEquityRecoveryCommand::ConfirmOrphanDeposit)
            .await
            .unwrap();

        assert!(matches!(
            store.load(&id).await.unwrap(),
            Some(WrappedEquityRecovery::OrphanDeposited {
                chain: Chain::Base,
                ..
            })
        ));
        assert_eq!(base_raindex.last_confirmed_tx(), Some(deposit_tx));
        assert_eq!(open_recovery_ids(&pool).await.unwrap(), vec![]);
    }

    /// Acceptance criterion: an enqueue failure after a confirmed
    /// orphan deposit propagates as a hard error rather than being folded
    /// into `RecoveryFailed`.
    #[tokio::test]
    async fn confirm_orphan_deposit_enqueue_failure_propagates() {
        let (_pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = crate::bot_gas::RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        apalis_pool.close().await;
        let mut services = test_services().await;
        services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(queue);

        let submitted = WrappedEquityRecovery::OrphanDepositSubmitted {
            chain: Chain::Base,
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            vault_deposit_tx_hash: FAKE_TX_HASH,
            submitted_at: Utc::now(),
        };

        let error = submitted
            .transition(
                WrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
                &services,
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            WrappedEquityRecoveryError::BotGasEnqueueFailed(BotGasEnqueueFailure { tx_hash, .. })
                if tx_hash == FAKE_TX_HASH
        ));
    }

    #[tokio::test]
    async fn fail_recovery_rejected_from_terminal_state() {
        let services = test_services().await;
        let deposited = WrappedEquityRecovery::OrphanDeposited {
            chain: Chain::Base,
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
        let mint_id = issuer_request_id("ISS-NONEXISTENT");

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
        let redemption_id = redemption_aggregate_id("nonexistent");

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

    /// A redemption still resolving onchain drives itself through its own
    /// jobs, so dispatching to it hands the recovery over instead of recording
    /// a failure on every detection cycle. A legacy pending send (no recorded
    /// transaction) is one: only an operator closes it.
    #[tokio::test]
    async fn dispatch_to_a_redemption_still_resolving_hands_it_over() {
        let (services, pool) = services_and_pool_over(BTreeMap::from([(
            Chain::Base,
            chain_services_with(Arc::new(MockRaindex::new())),
        )]))
        .await;
        let redemption_id = redemption_aggregate_id("legacy-send-pending");
        let history: [crate::equity_redemption::EquityRedemptionEvent; 3] = [
            crate::equity_redemption::EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: aapl(),
                quantity: st0x_float_macro::float!(1),
                token: Address::repeat_byte(0x11),
                wrapped_amount: alloy::primitives::U256::from(1_000_000_000_000_000_000_u128),
                actual_wrapped_amount: None,
                raindex_withdraw_tx: TxHash::repeat_byte(0x12),
                raindex_withdraw_block: None,
                withdrawn_at: Utc::now(),
            },
            crate::equity_redemption::EquityRedemptionEvent::TokensUnwrapped {
                quantity: Some(st0x_float_macro::float!(1)),
                underlying_token: crate::equity_redemption::UnwrappedProvenance::Legacy(
                    Address::repeat_byte(0x21),
                ),
                unwrap_tx_hash: TxHash::repeat_byte(0x13),
                unwrapped_amount: alloy::primitives::U256::from(1_000_000_000_000_000_000_u128),
                unwrap_block: None,
                unwrapped_at: Utc::now(),
            },
            crate::equity_redemption::EquityRedemptionEvent::SendPending {
                pending_at: Utc::now(),
            },
        ];
        for (sequence, event) in (1_i64..).zip(history) {
            sqlx::query(
                "INSERT INTO events \
                 (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, \
                  metadata) \
                 VALUES ('EquityRedemption', ?1, ?2, ?3, '1', ?4, '{}')",
            )
            .bind(redemption_id.to_string())
            .bind(sequence)
            .bind(st0x_event_sorcery::DomainEvent::event_type(&event))
            .bind(serde_json::to_string(&event).unwrap())
            .execute(&pool)
            .await
            .unwrap();
        }

        let events = detected_state()
            .transition(
                WrappedEquityRecoveryCommand::DispatchToRedemption {
                    redemption_id: redemption_id.clone(),
                },
                &services,
            )
            .await
            .unwrap();

        assert!(
            matches!(
                events.as_slice(),
                [WrappedEquityRecoveryEvent::DispatchedToRedemption { redemption_id: dispatched, .. }]
                    if *dispatched == redemption_id
            ),
            "Expected the recovery to hand over, got {events:?}"
        );
    }

    /// `submit_deposit` reverts -> the handler records the failure as
    /// `RecoveryFailed` instead of advancing into `OrphanDepositSubmitted`.
    #[tokio::test]
    async fn submit_orphan_deposit_records_failure_when_raindex_reports_revert() {
        let services = services_over(BTreeMap::from([(
            Chain::Base,
            chain_services_with(Arc::new(
                MockRaindex::new().with_deposit_behavior(DepositBehavior::FailExecutionReverted),
            )),
        )]))
        .await;

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
