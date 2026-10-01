//! Aggregate recording automated recovery of unwrapped equity tokens
//! (tSTOCK) found on a chain's bot wallet.
//!
//! Every recovery records the chain it runs on at `Detect`, and each
//! handler resolves that chain's services from the record, never from the
//! primary chain.
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

use alloy::primitives::{TxHash, U256};
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
use st0x_wrapper::{WrapConfirmation, WrapperError, node_sync_attempts};

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
    /// Every chain's entry. A recovery drives the entry of the chain it
    /// records, which also carries the wrap receiver -- the wallet the
    /// deposit pulls the wrapped tokens from.
    pub(crate) equity: EquityTransferServices,
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
    /// Enqueues bot-gas cost recording for the orphan wrap/deposit path
    /// (which calls `wrapper`/`raindex` directly rather than through
    /// `transfer`). ADR 0017.
    pub(crate) bot_gas_enqueuer: BotGasReceiptCostEnqueuer,
}

/// Domain errors returned from the aggregate's `initialize`/`transition`
/// handlers. Terminal service failures (raindex/wrapper/transfer) are recorded
/// as `RecoveryFailed` events so failures remain first-class entries in the
/// audit trail. Retryable service failures that should leave the aggregate
/// in-place instead surface as errors through this enum (e.g. `NodeSyncFailed`,
/// `ChainServicesMissing` and the `Retryable*Confirmation` variants).
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

    /// The recovery's chain has no equity services in this configuration.
    /// Not a terminal failure: no event is recorded, so the recovery stays
    /// open and the job retries once the chain is wired again.
    #[error(transparent)]
    ChainServicesMissing(#[from] ChainServicesMissing),

    #[error("wrap confirmation for tx {wrap_tx_hash} is retryable")]
    RetryableWrapConfirmation { wrap_tx_hash: TxHash },

    #[error("deposit confirmation for tx {vault_deposit_tx_hash} is retryable")]
    RetryableDepositConfirmation { vault_deposit_tx_hash: TxHash },
    #[error("RPC node did not catch up to wrap block {required_block} after {attempts} polls")]
    NodeSyncFailed { required_block: u64, attempts: u32 },

    /// Enqueueing the bot-gas receipt cost recording job failed after an
    /// orphan wrap/deposit confirmation succeeded (a local SQLite write, safe
    /// to retry since the aggregate has not advanced past the confirmed state
    /// yet). See [`BotGasEnqueueFailure`] for why the payload is a rendered
    /// `String` rather than a typed source.
    ///
    /// NOTE: `resume_mint_or_fail`/`resume_redemption_or_fail` (the
    /// `DispatchToMint`/`DispatchToRedemption` handlers) deliberately do NOT
    /// propagate a bot-gas enqueue failure this way, mirroring
    /// `WrappedEquityRecoveryError` -- see the "Known gaps" entry in
    /// SPEC.md's bot-gas section.
    #[error(transparent)]
    BotGasEnqueueFailed(#[from] BotGasEnqueueFailure),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UnwrappedEquityRecoveryCommand {
    /// Initial command. Records the detection trigger and the chain whose
    /// wallet holds the tokens. Refused with `ChainServicesMissing` when that
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

    /// Orphan path step 1. On the recovery's chain, the handler resolves
    /// the wrapped-token address via `wrapper.lookup_derivative(symbol)`,
    /// calls `wrapper.submit_wrap(...)`, and emits `OrphanWrapSubmitted` with
    /// the returned tx hash.
    SubmitOrphanWrap,

    /// Orphan path step 2. The handler reads `wrap_tx_hash` from the
    /// current state and calls the recovery chain's `wrapper.confirm_wrap`,
    /// emitting `OrphanWrapped` with the actual minted wrapped amount
    /// iff confirmation succeeds.
    ConfirmOrphanWrap,

    /// Orphan path step 3. The handler reads `wrapped_amount` from the
    /// current state, looks up the Raindex vault, calls the recovery chain's
    /// `raindex.submit_deposit(...)`, and emits `OrphanDepositSubmitted` with
    /// the returned tx hash.
    SubmitOrphanDeposit,

    /// Orphan path step 4. The handler reads `vault_deposit_tx_hash`
    /// from the current state and calls the recovery chain's
    /// `raindex.confirm_tx(tx_hash)`, emitting `OrphanDeposited` iff
    /// confirmation succeeds.
    ConfirmOrphanDeposit,

    /// Marks the recovery as failed with the supplied reason.
    FailRecovery { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UnwrappedEquityRecoveryEvent {
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

    OrphanWrapSubmitted {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        wrap_tx_hash: TxHash,
        submitted_at: DateTime<Utc>,
    },

    OrphanWrapped {
        chain: Chain,
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
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        wrap_tx_hash: TxHash,
        wrapped_amount: U256,
        vault_deposit_tx_hash: TxHash,
        deposit_submitted_at: DateTime<Utc>,
    },

    OrphanDeposited {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        detected_at: DateTime<Utc>,
        wrap_tx_hash: TxHash,
        wrapped_amount: U256,
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },

    Failed {
        chain: Chain,
        symbol: Symbol,
        shares: FractionalShares,
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl UnwrappedEquityRecovery {
    /// The chain whose wallet holds the recovered tokens.
    pub(crate) fn chain(&self) -> Chain {
        match self {
            Self::Detected { chain, .. }
            | Self::DispatchedToMint { chain, .. }
            | Self::DispatchedToRedemption { chain, .. }
            | Self::OrphanWrapSubmitted { chain, .. }
            | Self::OrphanWrapped { chain, .. }
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
            | Self::OrphanWrapSubmitted { symbol, .. }
            | Self::OrphanWrapped { symbol, .. }
            | Self::OrphanDepositSubmitted { symbol, .. }
            | Self::OrphanDeposited { symbol, .. }
            | Self::Failed { symbol, .. } => symbol,
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
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

/// Every recovery whose latest event leaves it open. Startup reads them to
/// refuse a configuration that builds no services for a chain an open
/// recovery still has to act on.
pub(crate) async fn open_recovery_ids(
    pool: &SqlitePool,
) -> Result<Vec<UnwrappedEquityRecoveryId>, sqlx::Error> {
    let rows: Vec<String> = sqlx::query_scalar(
        "WITH latest AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'UnwrappedEquityRecovery' \
             GROUP BY aggregate_id \
         ) \
         SELECT latest.aggregate_id \
         FROM events last_ev \
         INNER JOIN latest \
             ON last_ev.aggregate_id = latest.aggregate_id \
            AND last_ev.sequence = latest.max_seq \
         WHERE last_ev.aggregate_type = 'UnwrappedEquityRecovery' \
           AND last_ev.event_type IN ( \
               'UnwrappedEquityRecoveryEvent::Detected', \
               'UnwrappedEquityRecoveryEvent::OrphanWrapSubmitted', \
               'UnwrappedEquityRecoveryEvent::OrphanWrapped', \
               'UnwrappedEquityRecoveryEvent::OrphanDepositSubmitted' \
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
impl EventSourced for UnwrappedEquityRecovery {
    type Id = UnwrappedEquityRecoveryId;
    type Event = UnwrappedEquityRecoveryEvent;
    type Command = UnwrappedEquityRecoveryCommand;
    type Error = UnwrappedEquityRecoveryError;
    type Services = UnwrappedEquityRecoveryServices;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "UnwrappedEquityRecovery";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 2;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            UnwrappedEquityRecoveryEvent::Detected {
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
        use UnwrappedEquityRecoveryEvent::*;

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
                OrphanWrapSubmitted {
                    wrap_tx_hash,
                    submitted_at,
                },
            ) => Some(Self::OrphanWrapSubmitted {
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                wrap_tx_hash: *wrap_tx_hash,
                submitted_at: *submitted_at,
            }),

            (
                Self::OrphanWrapSubmitted {
                    chain,
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
                chain: *chain,
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
                    chain,
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
                chain: *chain,
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
                    chain,
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
                chain: *chain,
                symbol: symbol.clone(),
                shares: *shares,
                detected_at: *detected_at,
                wrap_tx_hash: *wrap_tx_hash,
                wrapped_amount: *wrapped_amount,
                vault_deposit_tx_hash: *vault_deposit_tx_hash,
                deposited_at: *deposited_at,
            }),

            (
                Self::Detected {
                    chain,
                    symbol,
                    shares,
                    ..
                }
                | Self::OrphanWrapSubmitted {
                    chain,
                    symbol,
                    shares,
                    ..
                }
                | Self::OrphanWrapped {
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
            }),

            (state, _) => Err(UnwrappedEquityRecoveryError::InvalidTransition {
                state: Box::new(state.clone()),
            })?,
        })
    }

    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            UnwrappedEquityRecoveryCommand::Detect {
                chain,
                symbol,
                shares,
            } => {
                services.equity.for_chain(chain)?;

                Ok(vec![UnwrappedEquityRecoveryEvent::Detected {
                    chain,
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

            // The transfer resolves its own chain's entry; checking the
            // recovery's chain first keeps an unwired chain from recording a
            // terminal failure for a recovery that can still finish. The job
            // dispatches only to a transfer on the recovery's chain
            // (`validate_active_aggregate_quantity`), so the two match here.
            (Self::Detected { chain, .. }, DispatchToMint { mint_id }) => {
                services.equity.for_chain(*chain)?;
                resume_mint_or_fail(&services.transfer, &mint_id).await
            }

            (Self::Detected { chain, .. }, DispatchToRedemption { redemption_id }) => {
                services.equity.for_chain(*chain)?;
                resume_redemption_or_fail(&services.transfer, &redemption_id).await
            }

            (
                Self::Detected {
                    chain,
                    symbol,
                    shares,
                    ..
                },
                SubmitOrphanWrap,
            ) => {
                let chain_services = services.equity.for_chain(*chain)?;
                submit_orphan_wrap_or_fail(chain_services, symbol, *shares).await
            }

            (
                Self::OrphanWrapSubmitted {
                    chain,
                    symbol,
                    wrap_tx_hash,
                    ..
                },
                ConfirmOrphanWrap,
            ) => {
                let chain_services = services.equity.for_chain(*chain)?;
                confirm_orphan_wrap_or_fail(
                    *chain,
                    chain_services,
                    &services.bot_gas_enqueuer,
                    symbol,
                    *wrap_tx_hash,
                )
                .await
            }

            (
                Self::OrphanWrapped {
                    chain,
                    symbol,
                    wrapped_amount,
                    wrap_block,
                    ..
                },
                SubmitOrphanDeposit,
            ) => {
                let chain_services = services.equity.for_chain(*chain)?;
                // Skip the wait for legacy aggregates persisted before wrap_block was added.
                if let Some(block) = wrap_block {
                    chain_services
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
                submit_orphan_deposit_or_fail(chain_services, symbol, *wrapped_amount).await
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
                )
                .await
            }

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
        // A redemption still resolving onchain (a pending withdrawal or issuer
        // send, or a legacy send an operator must verify) has its own jobs
        // driving it, so the recovery has handed it over rather than failed.
        Err(error) if error.is_still_in_progress() => {
            info!(target: "rebalance", %redemption_id, %error, "Unwrapped equity recovery: the redemption is still resolving and drives itself");
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
    chain_services: &ChainEquityServices,
    symbol: &Symbol,
    shares: FractionalShares,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match chain_services.wrapper.lookup_derivative(symbol) {
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

    match chain_services
        .wrapper
        .submit_wrap(wrapped_token, underlying_amount, chain_services.wallet)
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
    chain: Chain,
    chain_services: &ChainEquityServices,
    bot_gas_enqueuer: &BotGasReceiptCostEnqueuer,
    symbol: &Symbol,
    wrap_tx_hash: TxHash,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match chain_services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_derivative failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: Utc::now(),
            }]);
        }
    };

    match chain_services
        .wrapper
        .confirm_wrap(wrapped_token, wrap_tx_hash)
        .await
    {
        Ok(WrapConfirmation {
            shares: wrapped_amount,
            block: wrap_block,
        }) => {
            info!(target: "rebalance", %chain, %symbol, %wrap_tx_hash, %wrapped_amount, "Unwrapped equity recovery: confirm_wrap succeeded");

            enqueue_equity_cost(
                bot_gas_enqueuer,
                chain,
                wrap_tx_hash,
                BotGasOperationCategory::Wrap,
                symbol,
            )
            .await?;

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
    chain_services: &ChainEquityServices,
    symbol: &Symbol,
    wrapped_amount: U256,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    let wrapped_token = match chain_services.wrapper.lookup_derivative(symbol) {
        Ok(token) => token,
        Err(error) => {
            warn!(target: "rebalance", %symbol, ?error, "Unwrapped equity recovery: lookup_derivative failed");
            return Ok(vec![UnwrappedEquityRecoveryEvent::RecoveryFailed {
                reason: format!("wrapper.lookup_derivative failed: {error}"),
                failed_at: Utc::now(),
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
    match chain_services
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
    chain: Chain,
    raindex: &Arc<dyn Raindex>,
    bot_gas_enqueuer: &BotGasReceiptCostEnqueuer,
    symbol: &Symbol,
    vault_deposit_tx_hash: TxHash,
) -> Result<Vec<UnwrappedEquityRecoveryEvent>, UnwrappedEquityRecoveryError> {
    match raindex.confirm_tx(vault_deposit_tx_hash).await {
        Ok(()) => {
            info!(target: "rebalance", %chain, %vault_deposit_tx_hash, "Unwrapped equity recovery: confirm_tx succeeded");

            enqueue_equity_cost(
                bot_gas_enqueuer,
                chain,
                vault_deposit_tx_hash,
                BotGasOperationCategory::VaultDeposit,
                symbol,
            )
            .await?;

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
    use alloy::primitives::{Address, B256, TxHash, fixed_bytes};
    use chrono::Utc;
    use rain_math_float::Float;
    use std::collections::BTreeMap;

    use st0x_config::ChainEquities;
    use st0x_event_sorcery::{AggregateError, EventSourced, LifecycleError};
    use st0x_evm::NODE_SYNC_MAX_ATTEMPTS;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_raindex::RaindexVaultId;
    use st0x_tokenization::issuer_request_id;
    use st0x_tokenization::mock::MockTokenizer;
    use st0x_wrapper::{MockWrapper, Wrapper};

    use super::*;
    use crate::bot_gas::{RecordBotGasReceiptCostJobQueue, pending_bot_gas_jobs};
    use crate::equity_redemption::redemption_aggregate_id;
    use crate::mint_authorization::ConfiguredMintAuthorizer;
    use crate::native_gas::ConfiguredGasReadiness;
    use crate::onchain::mock::{ConfirmTxBehavior, DepositBehavior, DepositCall, MockRaindex};
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::vault_lookup::{MockVaultLookup, VaultLookup};

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
            chain: Chain::Base,
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        }
    }

    fn orphan_wrapped() -> UnwrappedEquityRecovery {
        UnwrappedEquityRecovery::OrphanWrapped {
            chain: Chain::Base,
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
        services_over(BTreeMap::from([(
            Chain::Base,
            chain_services_with(raindex, wrapper, vault_lookup),
        )]))
        .await
    }

    fn chain_services_with(
        raindex: Arc<dyn Raindex>,
        wrapper: Arc<dyn Wrapper>,
        vault_lookup: Arc<dyn VaultLookup>,
    ) -> ChainEquityServices {
        ChainEquityServices {
            wallet: Address::random(),
            raindex,
            vault_lookup,
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper,
            mint_authorizer: ConfiguredMintAuthorizer::Disabled,
            gas_readiness: ConfiguredGasReadiness::Unwired,
            equities: ChainEquities::default(),
        }
    }

    /// Recovery services carrying exactly `chains`, with the transfer wired
    /// to fresh in-memory mint and redemption stores over the same entries.
    async fn services_over(
        chains: BTreeMap<Chain, ChainEquityServices>,
    ) -> UnwrappedEquityRecoveryServices {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let equity = EquityTransferServices {
            chains,
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };
        let mint_store = Arc::new(st0x_event_sorcery::test_store(pool.clone(), equity.clone()));
        let redemption_store = Arc::new(st0x_event_sorcery::test_store(pool, equity.clone()));
        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            equity.clone(),
            mint_store,
            redemption_store,
        ));
        UnwrappedEquityRecoveryServices {
            equity,
            transfer,
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        }
    }

    #[tokio::test]
    async fn detect_initializes_aggregate_into_detected_state() {
        let services = test_services().await;
        let events = UnwrappedEquityRecovery::initialize(
            UnwrappedEquityRecoveryCommand::Detect {
                chain: Chain::Base,
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
                chain: Chain::Base,
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
                chain: Chain::Base,
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
            chain: Chain::Base,
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
        };
        let error = state
            .transition(
                UnwrappedEquityRecoveryCommand::Detect {
                    chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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

    /// Acceptance criterion: confirming an orphan wrap enqueues
    /// exactly one `Wrap` bot-gas job on Base with the recovery's symbol.
    #[tokio::test]
    async fn confirm_orphan_wrap_enqueues_wrap_bot_gas_job() {
        let (_pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let wrapper_mock = Arc::new(MockWrapper::new());
        wrapper_mock.seed_submitted_amount(FAKE_WRAP_TX, U256::from(7u64));
        let mut services = services_with(Arc::new(MockRaindex::new()), wrapper_mock).await;
        services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(queue);

        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            chain: Chain::Base,
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            submitted_at: Utc::now(),
        };
        state
            .transition(UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap, &services)
            .await
            .expect("ConfirmOrphanWrap should succeed from OrphanWrapSubmitted");

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(jobs.len(), 1, "expected exactly one bot-gas job");
        assert_eq!(jobs[0].category, BotGasOperationCategory::Wrap);
        assert_eq!(jobs[0].chain, Chain::Base);
        assert_eq!(jobs[0].tx_hash, FAKE_WRAP_TX);
        assert_eq!(jobs[0].symbol, Some(aapl()));
    }

    /// Acceptance criterion: an enqueue failure after a confirmed
    /// orphan wrap propagates as a hard error rather than being folded into
    /// `RecoveryFailed`.
    #[tokio::test]
    async fn confirm_orphan_wrap_enqueue_failure_propagates() {
        let (_pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        apalis_pool.close().await;
        let wrapper_mock = Arc::new(MockWrapper::new());
        wrapper_mock.seed_submitted_amount(FAKE_WRAP_TX, U256::from(7u64));
        let mut services = services_with(Arc::new(MockRaindex::new()), wrapper_mock).await;
        services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(queue);

        let state = UnwrappedEquityRecovery::OrphanWrapSubmitted {
            chain: Chain::Base,
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
            UnwrappedEquityRecoveryError::BotGasEnqueueFailed(BotGasEnqueueFailure { tx_hash, .. })
                if tx_hash == FAKE_WRAP_TX
        ));
    }

    /// Acceptance criterion: confirming an orphan deposit
    /// enqueues exactly one `VaultDeposit` bot-gas job on Base with the
    /// recovery's symbol.
    #[tokio::test]
    async fn confirm_orphan_deposit_enqueues_vault_deposit_bot_gas_job() {
        let (_pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let mut services =
            services_with(Arc::new(MockRaindex::new()), Arc::new(MockWrapper::new())).await;
        services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(queue);

        let state = UnwrappedEquityRecovery::OrphanDepositSubmitted {
            chain: Chain::Base,
            symbol: aapl(),
            shares: one_share(),
            detected_at: Utc::now(),
            wrap_tx_hash: FAKE_WRAP_TX,
            wrapped_amount: U256::from(123u64),
            vault_deposit_tx_hash: FAKE_WRAP_TX,
            deposit_submitted_at: Utc::now(),
        };
        state
            .transition(
                UnwrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
                &services,
            )
            .await
            .expect("ConfirmOrphanDeposit should succeed from OrphanDepositSubmitted");

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        assert_eq!(jobs.len(), 1, "expected exactly one bot-gas job");
        assert_eq!(jobs[0].category, BotGasOperationCategory::VaultDeposit);
        assert_eq!(jobs[0].chain, Chain::Base);
        assert_eq!(jobs[0].tx_hash, FAKE_WRAP_TX);
        assert_eq!(jobs[0].symbol, Some(aapl()));
    }

    /// Acceptance criterion: an enqueue failure after a confirmed
    /// orphan deposit propagates as a hard error rather than being folded
    /// into `RecoveryFailed`.
    #[tokio::test]
    async fn confirm_orphan_deposit_enqueue_failure_propagates() {
        let (_pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        apalis_pool.close().await;
        let mut services =
            services_with(Arc::new(MockRaindex::new()), Arc::new(MockWrapper::new())).await;
        services.bot_gas_enqueuer = BotGasReceiptCostEnqueuer::Enabled(queue);

        let state = UnwrappedEquityRecovery::OrphanDepositSubmitted {
            chain: Chain::Base,
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
            UnwrappedEquityRecoveryError::BotGasEnqueueFailed(BotGasEnqueueFailure { tx_hash, .. })
                if tx_hash == FAKE_WRAP_TX
        ));
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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

    /// A recovery on Ethereum runs its whole orphan path -- wrap, confirm,
    /// deposit, confirm -- on Ethereum's wrapper and orderbook and charges
    /// both confirmed txs to Ethereum. Base's wrapper and orderbook fail every
    /// call, so reaching `OrphanDeposited` also shows Base was never used.
    #[tokio::test]
    async fn orphan_path_on_a_secondary_chain_runs_on_that_chains_services() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let base_raindex = Arc::new(
            MockRaindex::new().with_deposit_behavior(DepositBehavior::FailExecutionReverted),
        );
        let ethereum_raindex = Arc::new(MockRaindex::new());
        let mut services = services_over(BTreeMap::from([
            (
                Chain::Base,
                chain_services_with(
                    base_raindex.clone(),
                    Arc::new(MockWrapper::failing()),
                    Arc::new(mock_vault_lookup()),
                ),
            ),
            (
                Chain::Ethereum,
                chain_services_with(
                    ethereum_raindex.clone(),
                    Arc::new(MockWrapper::new()),
                    Arc::new(mock_vault_lookup()),
                ),
            ),
        ]))
        .await;
        services.bot_gas_enqueuer =
            BotGasReceiptCostEnqueuer::Enabled(RecordBotGasReceiptCostJobQueue::new(&apalis_pool));
        let store = st0x_event_sorcery::test_store::<UnwrappedEquityRecovery>(pool, services);
        let id = UnwrappedEquityRecoveryId(Uuid::new_v4());

        for command in [
            UnwrappedEquityRecoveryCommand::Detect {
                chain: Chain::Ethereum,
                symbol: aapl(),
                shares: one_share(),
            },
            UnwrappedEquityRecoveryCommand::SubmitOrphanWrap,
            UnwrappedEquityRecoveryCommand::ConfirmOrphanWrap,
            UnwrappedEquityRecoveryCommand::SubmitOrphanDeposit,
            UnwrappedEquityRecoveryCommand::ConfirmOrphanDeposit,
        ] {
            store.send(&id, command).await.unwrap();
        }

        let Some(UnwrappedEquityRecovery::OrphanDeposited {
            chain,
            wrap_tx_hash,
            vault_deposit_tx_hash,
            ..
        }) = store.load(&id).await.unwrap()
        else {
            panic!("the Ethereum recovery must finish its orphan path");
        };
        assert_eq!(chain, Chain::Ethereum);
        assert_eq!(
            ethereum_raindex.last_confirmed_tx(),
            Some(vault_deposit_tx_hash)
        );
        assert_eq!(base_raindex.last_deposit_call(), None);
        assert_eq!(base_raindex.last_confirmed_tx(), None);

        let jobs = pending_bot_gas_jobs(&apalis_pool).await;
        let charged: Vec<_> = jobs
            .iter()
            .map(|job| (job.chain, job.category, job.tx_hash))
            .collect();
        assert_eq!(
            charged,
            vec![
                (Chain::Ethereum, BotGasOperationCategory::Wrap, wrap_tx_hash),
                (
                    Chain::Ethereum,
                    BotGasOperationCategory::VaultDeposit,
                    vault_deposit_tx_hash
                ),
            ]
        );
    }

    /// A chain with no services cannot open a recovery, and cannot move one
    /// already open on it: each command is refused with no event recorded,
    /// so the recovery stays open for when the chain is wired again.
    #[tokio::test]
    async fn a_chain_without_services_refuses_commands_without_recording_an_event() {
        let (pool, _apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wired = st0x_event_sorcery::test_store::<UnwrappedEquityRecovery>(
            pool.clone(),
            services_over(BTreeMap::from([(
                Chain::Ethereum,
                chain_services_with(
                    Arc::new(MockRaindex::new()),
                    Arc::new(MockWrapper::new()),
                    Arc::new(mock_vault_lookup()),
                ),
            )]))
            .await,
        );
        let unwired =
            st0x_event_sorcery::test_store::<UnwrappedEquityRecovery>(pool, test_services().await);

        let refused_id = UnwrappedEquityRecoveryId(Uuid::new_v4());
        let error = unwired
            .send(
                &refused_id,
                UnwrappedEquityRecoveryCommand::Detect {
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
                    UnwrappedEquityRecoveryError::ChainServicesMissing(ChainServicesMissing {
                        chain: Chain::Ethereum
                    })
                ))
            ),
            "got {error:?}"
        );
        assert_eq!(unwired.load(&refused_id).await.unwrap(), None);

        let open_id = UnwrappedEquityRecoveryId(Uuid::new_v4());
        wired
            .send(
                &open_id,
                UnwrappedEquityRecoveryCommand::Detect {
                    chain: Chain::Ethereum,
                    symbol: aapl(),
                    shares: one_share(),
                },
            )
            .await
            .unwrap();

        let error = unwired
            .send(&open_id, UnwrappedEquityRecoveryCommand::SubmitOrphanWrap)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    UnwrappedEquityRecoveryError::ChainServicesMissing(ChainServicesMissing {
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
                UnwrappedEquityRecovery::Detected {
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

        let event: UnwrappedEquityRecoveryEvent = serde_json::from_value(legacy).unwrap();
        let state = UnwrappedEquityRecovery::originate(&event).unwrap();

        assert_eq!(state.chain(), Chain::Base);
        assert_eq!(state.symbol(), &aapl());
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
            chain: Chain::Base,
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
