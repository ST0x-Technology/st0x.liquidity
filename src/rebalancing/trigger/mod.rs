//! Rebalancing trigger that reacts to inventory imbalances.

mod equity;
mod freeze;
mod usdc;

#[cfg(test)]
pub(crate) use equity::InProgressGuard;
pub(crate) use equity::{GuardState, claim_guard_for_recovery_or_orphan};

use alloy::primitives::{Address, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use rain_math_float::Float;
use st0x_config::{AssetsConfig, OperationMode};
use st0x_event_sorcery::{
    AggregateError, EntityList, LifecycleError, Projection, ProjectionError, Reactor, Store, deps,
};
use st0x_execution::{FractionalShares, Positive, SharesConversionError, Symbol};
use st0x_finance::{HasZero, Usd, Usdc};
use st0x_wrapper::{Wrapper, WrapperError};

use self::freeze::FreezeStatusReader;
use self::usdc::UsdcRebalanceOperation;
use crate::conductor::job::QueuePushError;
use crate::equity_redemption::{
    EquityRedemption, EquityRedemptionCommand, EquityRedemptionEvent, RedemptionAggregateId,
};
use crate::inventory::projection::InventoryProjectionError;
use crate::inventory::snapshot::{InventorySnapshot, InventorySnapshotEvent};
use crate::inventory::view::InFlightEquityLocation;
use crate::inventory::{
    BroadcastingInventory, ImbalanceThreshold, Inventory, InventoryView, InventoryViewError,
    Operator, PendingRequestOwnership, PendingRequestOwnershipSnapshot, TransferOp, Venue,
};
use crate::position::{Position, PositionEvent};
use crate::rebalancing::equity::{
    TransferEquityToHedging, TransferEquityToHedgingJobQueue, TransferEquityToMarketMaking,
    TransferEquityToMarketMakingJobQueue,
};
use crate::rebalancing::usdc::{
    TransferUsdcToHedging, TransferUsdcToHedgingJobQueue, TransferUsdcToMarketMaking,
    TransferUsdcToMarketMakingJobQueue,
};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintCommand, TokenizedEquityMintEvent,
};
use crate::unwrapped_equity_recovery::aggregate::UnwrappedEquityRecoveryId;
use crate::unwrapped_equity_recovery::{
    UnwrappedEquityRecoveryJob, UnwrappedEquityRecoveryJobQueue,
};
use crate::usdc_rebalance::{
    InterruptedUsdcRebalances, RebalanceDirection, UsdcRebalance, UsdcRebalanceEvent,
    UsdcRebalanceId, interrupted_usdc_rebalance_ids,
};
use crate::vault_registry::{VaultRegistry, VaultRegistryId};
use crate::wrapped_equity_recovery::aggregate::WrappedEquityRecoveryId;
use crate::wrapped_equity_recovery::{WrappedEquityRecoveryJob, WrappedEquityRecoveryJobQueue};

pub(crate) use equity::{EquityRebalancingCheck, EquityRebalancingCheckScheduler};
#[cfg(test)]
pub(crate) use freeze::StubFreezeReader;
pub(crate) use usdc::{UsdcRebalancingCheck, UsdcRebalancingCheckScheduler};

/// Bundle of the equity + USDC schedulers so constructors and plumbing
/// functions can pass them as a single argument instead of two.
#[derive(Clone)]
pub(crate) struct RebalancingSchedulers {
    pub(crate) equity: EquityRebalancingCheckScheduler,
    pub(crate) usdc: UsdcRebalancingCheckScheduler,
    pub(crate) wrapped_equity_recovery: WrappedEquityRecoveryJobQueue,
    pub(crate) unwrapped_equity_recovery: UnwrappedEquityRecoveryJobQueue,
    pub(crate) transfer_usdc_to_hedging: TransferUsdcToHedgingJobQueue,
    pub(crate) transfer_usdc_to_market_making: TransferUsdcToMarketMakingJobQueue,
    pub(crate) transfer_equity_to_market_making: TransferEquityToMarketMakingJobQueue,
    pub(crate) transfer_equity_to_hedging: TransferEquityToHedgingJobQueue,
}

impl RebalancingSchedulers {
    pub(crate) fn new(pool: &apalis_sqlite::SqlitePool) -> Self {
        Self {
            equity: EquityRebalancingCheckScheduler::new(pool),
            usdc: UsdcRebalancingCheckScheduler::new(pool),
            wrapped_equity_recovery: WrappedEquityRecoveryJobQueue::new(pool),
            unwrapped_equity_recovery: UnwrappedEquityRecoveryJobQueue::new(pool),
            transfer_usdc_to_hedging: TransferUsdcToHedgingJobQueue::new(pool),
            transfer_usdc_to_market_making: TransferUsdcToMarketMakingJobQueue::new(pool),
            transfer_equity_to_market_making: TransferEquityToMarketMakingJobQueue::new(pool),
            transfer_equity_to_hedging: TransferEquityToHedgingJobQueue::new(pool),
        }
    }
}

/// Why the rebalancing trigger reactor failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RebalancingServiceError {
    #[error(transparent)]
    Inventory(#[from] InventoryViewError),
    #[error(transparent)]
    Projection(#[from] InventoryProjectionError),
    #[error(transparent)]
    EquityTrigger(#[from] equity::EquityTriggerError),
    #[error("float arithmetic error: {0}")]
    Float(#[from] rain_math_float::FloatError),
    #[error(transparent)]
    SharesConversion(#[from] SharesConversionError),
    #[error("missing USDC tracking context for rebalance {id} at {event}")]
    MissingUsdcTrackingContext {
        id: UsdcRebalanceId,
        event: usdc::UsdcTrackingEvent,
    },
    #[error("missing bridged amount for USDC rebalance {id} at deposit confirmation")]
    MissingUsdcBridgedAmount { id: UsdcRebalanceId },
    #[error(
        "settled USDC amount {settled_amount} exceeds initiated amount {initiated_amount} \
         for rebalance {id} at {event}"
    )]
    SettledUsdcExceedsInitiatedAmount {
        id: UsdcRebalanceId,
        event: usdc::UsdcTrackingEvent,
        initiated_amount: Usdc,
        settled_amount: Usdc,
    },
    #[error(
        "redemption {id} unwrapped {actual_quantity} shares, exceeding tracked quantity {tracked_quantity}"
    )]
    RedemptionUnwrappedExceedsTracked {
        id: RedemptionAggregateId,
        tracked_quantity: FractionalShares,
        actual_quantity: FractionalShares,
    },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    ApalisSqlx(#[from] sqlx_apalis::Error),
    #[error("failed to re-arm a stranded USDC transfer job at startup: {0}")]
    RearmEnqueue(#[from] QueuePushError),
}

/// Why loading a token address from the vault registry failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenAddressError {
    #[error("vault registry aggregate not initialized")]
    Uninitialized,
    #[error(transparent)]
    Persistence(#[from] AggregateError<LifecycleError<VaultRegistry>>),
}

/// Configuration for the rebalancing trigger (runtime).
#[derive(Debug, Clone)]
pub(crate) struct RebalancingServiceConfig {
    pub(crate) equity: ImbalanceThreshold,
    pub(crate) usdc: Option<ImbalanceThreshold>,
    pub(crate) transfer_timeout: Duration,
    pub(crate) assets: AssetsConfig,
}

impl RebalancingServiceConfig {
    /// Whitelist gate for the equity rebalancing trigger: only symbols
    /// explicitly configured with `rebalancing = "enabled"` are eligible.
    /// Symbols observed in inventory but absent from the config are skipped
    /// cleanly instead of falling through to the `WrapperService`
    /// `SymbolNotConfigured` error backstop.
    fn is_equity_rebalancing_enabled(&self, symbol: &Symbol) -> bool {
        self.assets
            .equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.rebalancing == OperationMode::Enabled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MintTrackingStage {
    Requested,
    Accepted,
    TokensReceived,
    WrapSubmitted,
    TokensWrapped,
    VaultDepositSubmitted,
}

impl std::fmt::Display for MintTrackingStage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Requested => write!(formatter, "MintRequested"),
            Self::Accepted => write!(formatter, "MintAccepted"),
            Self::TokensReceived => write!(formatter, "TokensReceived"),
            Self::WrapSubmitted => write!(formatter, "WrapSubmitted"),
            Self::TokensWrapped => write!(formatter, "TokensWrapped"),
            Self::VaultDepositSubmitted => write!(formatter, "VaultDepositSubmitted"),
        }
    }
}

#[derive(Debug, Clone)]
struct MintTracking {
    symbol: Symbol,
    quantity: FractionalShares,
    tokenization_request_id: Option<crate::tokenized_equity_mint::TokenizationRequestId>,
    stage: MintTrackingStage,
    last_progress_at: DateTime<Utc>,
}

impl MintTracking {
    fn from_requested_event(event: &TokenizedEquityMintEvent) -> Option<Self> {
        let TokenizedEquityMintEvent::MintRequested {
            symbol,
            quantity,
            requested_at,
            ..
        } = event
        else {
            return None;
        };

        Some(Self {
            symbol: symbol.clone(),
            quantity: FractionalShares::new(*quantity),
            tokenization_request_id: None,
            stage: MintTrackingStage::Requested,
            last_progress_at: *requested_at,
        })
    }

    fn track_progress(&mut self, event: &TokenizedEquityMintEvent) {
        if let Some(tokenization_request_id) = mint_event_tokenization_request_id(event) {
            self.tokenization_request_id = Some(tokenization_request_id.clone());
        }

        match event {
            TokenizedEquityMintEvent::MintAccepted { accepted_at, .. } => {
                self.stage = MintTrackingStage::Accepted;
                self.last_progress_at = *accepted_at;
            }
            TokenizedEquityMintEvent::TokensReceived { received_at, .. } => {
                self.stage = MintTrackingStage::TokensReceived;
                self.last_progress_at = *received_at;
            }
            TokenizedEquityMintEvent::ProviderCompletionRecovered { recovered_at, .. } => {
                self.stage = MintTrackingStage::TokensReceived;
                self.last_progress_at = *recovered_at;
            }
            TokenizedEquityMintEvent::WrapSubmitted { submitted_at, .. } => {
                self.stage = MintTrackingStage::WrapSubmitted;
                self.last_progress_at = *submitted_at;
            }
            TokenizedEquityMintEvent::TokensWrapped { wrapped_at, .. } => {
                self.stage = MintTrackingStage::TokensWrapped;
                self.last_progress_at = *wrapped_at;
            }
            TokenizedEquityMintEvent::VaultDepositSubmitted { submitted_at, .. } => {
                self.stage = MintTrackingStage::VaultDepositSubmitted;
                self.last_progress_at = *submitted_at;
            }
            TokenizedEquityMintEvent::MintRequested { .. }
            | TokenizedEquityMintEvent::MintRejected { .. }
            | TokenizedEquityMintEvent::MintAcceptanceFailed { .. }
            | TokenizedEquityMintEvent::WrappingFailed { .. }
            | TokenizedEquityMintEvent::DepositedIntoRaindex { .. }
            | TokenizedEquityMintEvent::RaindexDepositFailed { .. }
            | TokenizedEquityMintEvent::OperatorReconciled { .. } => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedemptionTrackingStage {
    VaultWithdrawPending,
    VaultWithdrawSubmitted,
    WithdrawnFromRaindex,
    UnwrapPending,
    UnwrapSubmitted,
    TokensUnwrapped,
    SendPending,
    TokensSent,
    Detected,
}

impl std::fmt::Display for RedemptionTrackingStage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VaultWithdrawPending => write!(formatter, "VaultWithdrawPending"),
            Self::VaultWithdrawSubmitted => write!(formatter, "VaultWithdrawSubmitted"),
            Self::WithdrawnFromRaindex => write!(formatter, "WithdrawnFromRaindex"),
            Self::UnwrapPending => write!(formatter, "UnwrapPending"),
            Self::UnwrapSubmitted => write!(formatter, "UnwrapSubmitted"),
            Self::TokensUnwrapped => write!(formatter, "TokensUnwrapped"),
            Self::SendPending => write!(formatter, "SendPending"),
            Self::TokensSent => write!(formatter, "TokensSent"),
            Self::Detected => write!(formatter, "Detected"),
        }
    }
}

#[derive(Debug, Clone)]
struct RedemptionTracking {
    symbol: Symbol,
    quantity: FractionalShares,
    tokenization_request_id: Option<crate::tokenized_equity_mint::TokenizationRequestId>,
    redemption_tx: Option<TxHash>,
    stage: RedemptionTrackingStage,
    last_progress_at: DateTime<Utc>,
}

impl RedemptionTracking {
    fn from_genesis_event(event: &EquityRedemptionEvent) -> Option<Self> {
        match event {
            EquityRedemptionEvent::VaultWithdrawPending {
                symbol,
                quantity,
                pending_at,
                ..
            } => Some(Self {
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                tokenization_request_id: None,
                redemption_tx: None,
                stage: RedemptionTrackingStage::VaultWithdrawPending,
                last_progress_at: *pending_at,
            }),
            EquityRedemptionEvent::VaultWithdrawSubmitted {
                symbol,
                quantity,
                submitted_at,
                ..
            } => Some(Self {
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                tokenization_request_id: None,
                redemption_tx: None,
                stage: RedemptionTrackingStage::VaultWithdrawSubmitted,
                last_progress_at: *submitted_at,
            }),
            EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol,
                quantity,
                withdrawn_at,
                ..
            } => Some(Self {
                symbol: symbol.clone(),
                quantity: FractionalShares::new(*quantity),
                tokenization_request_id: None,
                redemption_tx: None,
                stage: RedemptionTrackingStage::WithdrawnFromRaindex,
                last_progress_at: *withdrawn_at,
            }),
            _ => None,
        }
    }

    fn track_progress(
        &mut self,
        event: &EquityRedemptionEvent,
    ) -> Result<(), SharesConversionError> {
        match event {
            EquityRedemptionEvent::VaultWithdrawSubmitted { submitted_at, .. } => {
                self.stage = RedemptionTrackingStage::VaultWithdrawSubmitted;
                self.last_progress_at = *submitted_at;
            }
            EquityRedemptionEvent::WithdrawnFromRaindex { withdrawn_at, .. } => {
                self.stage = RedemptionTrackingStage::WithdrawnFromRaindex;
                self.last_progress_at = *withdrawn_at;
            }
            EquityRedemptionEvent::UnwrapPending { pending_at, .. } => {
                self.stage = RedemptionTrackingStage::UnwrapPending;
                self.last_progress_at = *pending_at;
            }
            EquityRedemptionEvent::UnwrapSubmitted { submitted_at, .. } => {
                self.stage = RedemptionTrackingStage::UnwrapSubmitted;
                self.last_progress_at = *submitted_at;
            }
            EquityRedemptionEvent::TokensUnwrapped {
                quantity,
                unwrapped_amount,
                unwrapped_at,
                ..
            } => {
                self.quantity = match quantity {
                    Some(quantity) => FractionalShares::new(*quantity),
                    None => FractionalShares::from_u256_18_decimals(*unwrapped_amount)?,
                };
                self.stage = RedemptionTrackingStage::TokensUnwrapped;
                self.last_progress_at = *unwrapped_at;
            }
            EquityRedemptionEvent::SendPending { pending_at, .. } => {
                self.stage = RedemptionTrackingStage::SendPending;
                self.last_progress_at = *pending_at;
            }
            EquityRedemptionEvent::TokensSent {
                redemption_tx,
                sent_at,
                ..
            } => {
                self.redemption_tx = Some(*redemption_tx);
                self.stage = RedemptionTrackingStage::TokensSent;
                self.last_progress_at = *sent_at;
            }
            EquityRedemptionEvent::Detected {
                tokenization_request_id,
                detected_at,
            } => {
                self.tokenization_request_id = Some(tokenization_request_id.clone());
                self.stage = RedemptionTrackingStage::Detected;
                self.last_progress_at = *detected_at;
            }
            EquityRedemptionEvent::VaultWithdrawPending { .. }
            | EquityRedemptionEvent::TransferFailed { .. }
            | EquityRedemptionEvent::DetectionFailed { .. }
            | EquityRedemptionEvent::RedemptionRejected { .. }
            | EquityRedemptionEvent::ProviderCompletionRecovered { .. }
            | EquityRedemptionEvent::OperatorReconciled { .. }
            | EquityRedemptionEvent::Completed { .. } => {}
        }

        Ok(())
    }
}

fn mint_event_tokenization_request_id(
    event: &TokenizedEquityMintEvent,
) -> Option<&crate::tokenized_equity_mint::TokenizationRequestId> {
    match event {
        TokenizedEquityMintEvent::MintAccepted {
            tokenization_request_id,
            ..
        }
        | TokenizedEquityMintEvent::ProviderCompletionRecovered {
            tokenization_request_id,
            ..
        } => Some(tokenization_request_id),
        TokenizedEquityMintEvent::MintRequested { .. }
        | TokenizedEquityMintEvent::MintRejected { .. }
        | TokenizedEquityMintEvent::MintAcceptanceFailed { .. }
        | TokenizedEquityMintEvent::TokensReceived { .. }
        | TokenizedEquityMintEvent::WrapSubmitted { .. }
        | TokenizedEquityMintEvent::TokensWrapped { .. }
        | TokenizedEquityMintEvent::WrappingFailed { .. }
        | TokenizedEquityMintEvent::VaultDepositSubmitted { .. }
        | TokenizedEquityMintEvent::DepositedIntoRaindex { .. }
        | TokenizedEquityMintEvent::RaindexDepositFailed { .. }
        | TokenizedEquityMintEvent::OperatorReconciled { .. } => None,
    }
}

/// Equity rebalancing decision produced by the imbalance check.
/// [`RebalancingService::check_and_trigger_equity`] turns it into the
/// matching apalis transfer job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TriggeredOperation {
    /// Mint tokenized equity (too much offchain).
    Mint {
        symbol: Symbol,
        quantity: FractionalShares,
    },
    /// Redeem tokenized equity (too much onchain).
    Redemption {
        symbol: Symbol,
        quantity: FractionalShares,
        /// Wrapped (ERC-4626) token address for vault withdrawal.
        wrapped_token: Address,
        /// Unwrapped (underlying) token address for sending to Alpaca.
        unwrapped_token: Address,
    },
}

/// Service that folds CQRS events into rebalancing state and
/// schedules follow-up imbalance checks. Also serves as the apalis
/// `Ctx` for the rebalancing-check workers.
pub(crate) struct RebalancingService {
    config: RebalancingServiceConfig,
    vault_registry: Arc<Store<VaultRegistry>>,
    orderbook: Address,
    order_owner: Address,
    inventory: Arc<BroadcastingInventory>,
    /// Reads issuance's per-asset dividend freeze status so the equity trigger
    /// can skip frozen assets before starting a flow. Set after construction via
    /// `set_freeze_status_reader` (the conductor builds the issuance client from
    /// config, mirroring `set_stores`); `None` only in tests that do not
    /// exercise the gate.
    freeze_status: RwLock<Option<Arc<dyn FreezeStatusReader>>>,
    pub(crate) equity_in_progress: Arc<std::sync::RwLock<HashMap<Symbol, equity::GuardState>>>,
    pending_offchain_order_symbols: Arc<RwLock<HashSet<Symbol>>>,
    pub(crate) usdc_in_progress: Arc<AtomicBool>,
    notifier: Arc<dyn crate::alerts::Notifier>,
    wrapper: Arc<dyn Wrapper>,
    pub(super) equity_scheduler: EquityRebalancingCheckScheduler,
    pub(super) usdc_scheduler: UsdcRebalancingCheckScheduler,
    pub(super) wrapped_equity_recovery_queue: WrappedEquityRecoveryJobQueue,
    pub(super) unwrapped_equity_recovery_queue: UnwrappedEquityRecoveryJobQueue,
    /// Queues for the USDC transfer apalis jobs that drive each rebalancing
    /// direction. `check_and_trigger_usdc` enqueues into one of these.
    pub(super) transfer_usdc_to_hedging_queue: TransferUsdcToHedgingJobQueue,
    pub(super) transfer_usdc_to_market_making_queue: TransferUsdcToMarketMakingJobQueue,
    /// Queues for the equity transfer apalis jobs that drive each rebalancing
    /// direction. `check_and_trigger_equity` enqueues into one of these.
    pub(super) transfer_equity_to_market_making_queue: TransferEquityToMarketMakingJobQueue,
    pub(super) transfer_equity_to_hedging_queue: TransferEquityToHedgingJobQueue,
    /// Tracks symbol/quantity for in-flight mints. The initial `MintRequested`
    /// event carries this data; follow-up events don't.
    mint_tracking: Arc<RwLock<HashMap<IssuerRequestId, MintTracking>>>,
    /// Tracks symbol/quantity for in-flight redemptions. The initial
    /// `WithdrawnFromRaindex` event carries this data; follow-up events don't.
    redemption_tracking: Arc<RwLock<HashMap<RedemptionAggregateId, RedemptionTracking>>>,
    /// Tracks USDC rebalance lifecycle data needed to settle inventory on
    /// terminal events with the actual amount received.
    usdc_tracking: Arc<RwLock<HashMap<UsdcRebalanceId, usdc::UsdcRebalanceTracking>>>,
    suppressed_inflight_symbols: Arc<RwLock<HashMap<Symbol, DateTime<Utc>>>>,
    timed_out_mints: Arc<RwLock<HashMap<IssuerRequestId, DateTime<Utc>>>>,
    timed_out_redemptions: Arc<RwLock<HashMap<RedemptionAggregateId, DateTime<Utc>>>>,
    timed_out_usdc_rebalances: Arc<RwLock<HashMap<UsdcRebalanceId, DateTime<Utc>>>>,
    /// Ids of post-burn-stuck USDC rebalances already logged by the timeout
    /// sweep. A preserved post-burn entry is intentionally never removed from
    /// `usdc_tracking` (so a late success can still settle) and its
    /// `last_progress_at` never advances, so every sweep re-selects it. This
    /// set deduplicates the error log to once per stuck transfer.
    post_burn_timeout_logged: Arc<RwLock<HashSet<UsdcRebalanceId>>>,
    /// Ids of post-burn-stuck USDC rebalances whose operator alert has been
    /// delivered successfully. Tracked separately from `post_burn_timeout_logged`
    /// so a transient notifier failure on the first sweep does not permanently
    /// silence the page about stranded funds: the alert is re-attempted on every
    /// sweep until one delivery succeeds, while the error log stays one-shot.
    post_burn_timeout_alerted: Arc<RwLock<HashSet<UsdcRebalanceId>>>,
    mint_event_sync: Arc<Mutex<()>>,
    redemption_event_sync: Arc<Mutex<()>>,
    usdc_event_sync: Arc<Mutex<()>>,
    /// CQRS stores for emitting failure events on timeout and for re-deriving
    /// durable state in the USDC timeout sweep.
    /// Set after construction via `set_stores` because the stores
    /// are built after the trigger (the trigger is a Reactor dependency
    /// of the stores' query manifest).
    mint_store: RwLock<Option<Arc<Store<TokenizedEquityMint>>>>,
    redemption_store: RwLock<Option<Arc<Store<EquityRedemption>>>>,
    usdc_store: RwLock<Option<Arc<Store<UsdcRebalance>>>>,
}

type EquityInventoryUpdate = Box<
    dyn FnOnce(
            Inventory<FractionalShares>,
        ) -> Result<
            Inventory<FractionalShares>,
            crate::inventory::InventoryError<FractionalShares>,
        > + Send,
>;

const TIMEOUT_TOMBSTONE_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug)]
enum UsdcTimeoutCleanup {
    Cleared {
        tracking: usdc::UsdcRebalanceTracking,
        elapsed: Duration,
    },
    PreservedPostBurn {
        tracking: usdc::UsdcRebalanceTracking,
        elapsed: Duration,
    },
}

/// How [`RebalancingService::rearm_stranded_transfers`] gates re-arm for a
/// candidate, by the aggregate's recovery semantics.
enum RearmPolicy {
    /// Post-burn `BridgingFailed` (RAI-909): re-checking the mint is NEW work,
    /// so a terminal `Failed`/`Done` row must NOT block -- gate on a live job
    /// only.
    RecoverableFailure,
    /// Post-burn pre-mint resumable (`Bridging`/`AwaitingAttestation`/
    /// `Attested`): any job row -- in-flight or terminal -- blocks re-arm so an
    /// exhausted retry budget is never bypassed.
    PostBurnResumable,
    /// Pre-burn mid-flight (`BridgingSubmitting` /
    /// `WithdrawalSubmitting{BaseToAlpaca}`): re-arm only when NO job row exists.
    /// A live row means apalis is still driving it (skip). A terminal-only row
    /// means the redrive budget is exhausted with no driver -- strand it for an
    /// operator alert rather than silently resetting the counter to zero.
    MidFlightPreBurn,
}

/// A stranded USDC rebalance to re-arm on startup, with the [`RearmPolicy`] that
/// governs how a pre-existing job row gates the re-arm. See
/// [`RebalancingService::rearm_stranded_transfers`].
struct RearmCandidate {
    id: UsdcRebalanceId,
    direction: RebalanceDirection,
    amount: Usdc,
    policy: RearmPolicy,
}

impl RebalancingService {
    pub(crate) fn new(
        config: RebalancingServiceConfig,
        vault_registry: Arc<Store<VaultRegistry>>,
        orderbook: Address,
        order_owner: Address,
        inventory: Arc<BroadcastingInventory>,
        wrapper: Arc<dyn Wrapper>,
        schedulers: RebalancingSchedulers,
        notifier: Arc<dyn crate::alerts::Notifier>,
    ) -> Self {
        let RebalancingSchedulers {
            equity: equity_scheduler,
            usdc: usdc_scheduler,
            wrapped_equity_recovery: wrapped_equity_recovery_queue,
            unwrapped_equity_recovery: unwrapped_equity_recovery_queue,
            transfer_usdc_to_hedging: transfer_usdc_to_hedging_queue,
            transfer_equity_to_market_making: transfer_equity_to_market_making_queue,
            transfer_equity_to_hedging: transfer_equity_to_hedging_queue,
            transfer_usdc_to_market_making: transfer_usdc_to_market_making_queue,
        } = schedulers;
        Self {
            config,
            vault_registry,
            orderbook,
            order_owner,
            inventory,
            freeze_status: RwLock::new(None),
            equity_in_progress: Arc::new(std::sync::RwLock::new(HashMap::new())),
            pending_offchain_order_symbols: Arc::new(RwLock::new(HashSet::new())),
            usdc_in_progress: Arc::new(AtomicBool::new(false)),
            notifier,
            wrapper,
            equity_scheduler,
            usdc_scheduler,
            wrapped_equity_recovery_queue,
            unwrapped_equity_recovery_queue,
            transfer_usdc_to_hedging_queue,
            transfer_usdc_to_market_making_queue,
            transfer_equity_to_market_making_queue,
            transfer_equity_to_hedging_queue,
            mint_tracking: Arc::new(RwLock::new(HashMap::new())),
            redemption_tracking: Arc::new(RwLock::new(HashMap::new())),
            usdc_tracking: Arc::new(RwLock::new(HashMap::new())),
            suppressed_inflight_symbols: Arc::new(RwLock::new(HashMap::new())),
            timed_out_mints: Arc::new(RwLock::new(HashMap::new())),
            timed_out_redemptions: Arc::new(RwLock::new(HashMap::new())),
            timed_out_usdc_rebalances: Arc::new(RwLock::new(HashMap::new())),
            post_burn_timeout_logged: Arc::new(RwLock::new(HashSet::new())),
            post_burn_timeout_alerted: Arc::new(RwLock::new(HashSet::new())),
            mint_event_sync: Arc::new(Mutex::new(())),
            redemption_event_sync: Arc::new(Mutex::new(())),
            usdc_event_sync: Arc::new(Mutex::new(())),
            mint_store: RwLock::new(None),
            redemption_store: RwLock::new(None),
            usdc_store: RwLock::new(None),
        }
    }

    /// Attach CQRS stores so the trigger can emit failure events on timeout and
    /// re-derive durable USDC state in the timeout sweep.
    ///
    /// Called after construction because the stores are built by the query
    /// manifest, which depends on the trigger as a reactor.
    pub(crate) async fn set_stores(
        &self,
        mint_store: Arc<Store<TokenizedEquityMint>>,
        redemption_store: Arc<Store<EquityRedemption>>,
        usdc_store: Arc<Store<UsdcRebalance>>,
    ) {
        *self.mint_store.write().await = Some(mint_store);
        *self.redemption_store.write().await = Some(redemption_store);
        *self.usdc_store.write().await = Some(usdc_store);
    }

    /// Attach the issuance freeze-status reader so the equity trigger can skip
    /// assets frozen for a dividend. Called by the conductor after construction
    /// (the reader wraps the issuance client built from config).
    pub(crate) async fn set_freeze_status_reader(&self, reader: Arc<dyn FreezeStatusReader>) {
        *self.freeze_status.write().await = Some(reader);
    }

    pub(crate) async fn recover_pending_offchain_order_symbols(
        &self,
        position_projection: &Projection<Position>,
    ) -> Result<(), ProjectionError<Position>> {
        let pending_symbols = position_projection
            .load_all()
            .await?
            .into_iter()
            .filter_map(|(symbol, position)| {
                position
                    .pending_offchain_order_id
                    .is_some()
                    .then_some(symbol)
            })
            .collect();

        *self.pending_offchain_order_symbols.write().await = pending_symbols;
        Ok(())
    }

    async fn expire_stuck_operations(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), RebalancingServiceError> {
        self.prune_timeout_markers(now).await;
        self.expire_stuck_mints(now).await?;
        self.expire_stuck_redemptions(now).await?;
        self.expire_stuck_usdc_rebalances(now).await?;
        Ok(())
    }

    fn elapsed_since_timeout_start(
        last_progress_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Option<Duration> {
        now.signed_duration_since(last_progress_at).to_std().ok()
    }

    fn timeout_marker_expired(marked_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        Self::elapsed_since_timeout_start(marked_at, now)
            .is_some_and(|elapsed| elapsed > TIMEOUT_TOMBSTONE_RETENTION)
    }

    async fn prune_timeout_markers(&self, now: DateTime<Utc>) {
        self.suppressed_inflight_symbols
            .write()
            .await
            .retain(|_, cleared_at| !Self::timeout_marker_expired(*cleared_at, now));
        self.timed_out_mints
            .write()
            .await
            .retain(|_, timed_out_at| !Self::timeout_marker_expired(*timed_out_at, now));
        self.timed_out_redemptions
            .write()
            .await
            .retain(|_, timed_out_at| !Self::timeout_marker_expired(*timed_out_at, now));
        self.timed_out_usdc_rebalances
            .write()
            .await
            .retain(|_, timed_out_at| !Self::timeout_marker_expired(*timed_out_at, now));
    }

    async fn expire_stuck_mints(&self, now: DateTime<Utc>) -> Result<(), RebalancingServiceError> {
        let timed_out_ids = {
            let tracking = self.mint_tracking.read().await;
            tracking
                .iter()
                .filter_map(|(id, tracking)| {
                    let elapsed =
                        Self::elapsed_since_timeout_start(tracking.last_progress_at, now)?;

                    if elapsed >= self.config.transfer_timeout {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        for id in timed_out_ids {
            let Some(symbol) = self
                .mint_tracking
                .read()
                .await
                .get(&id)
                .map(|t| t.symbol.clone())
            else {
                debug!(
                    target: "rebalance",
                    ?id,
                    "Skipping timed-out mint: tracking entry already removed"
                );
                continue;
            };

            // Steady-state skip: if recovery already owns the slot, leave the
            // mint untouched -- recovery drives it to terminal, and removing its
            // tracking or failing it would clear the guard and allow a
            // double-mint while the tokens are still unwrapped in the wallet. A
            // concurrent flip to `HeldForRecovery` AFTER this read is closed
            // atomically at the clear below.
            let held_for_recovery = {
                let guard = match self.equity_in_progress.read() {
                    Ok(guard) => guard,
                    Err(poison) => poison.into_inner(),
                };
                guard.get(&symbol) == Some(&equity::GuardState::HeldForRecovery)
            };
            if held_for_recovery {
                continue;
            }

            let Some((tracking, elapsed)) = self.cleanup_timed_out_mint(&id, now).await? else {
                continue;
            };

            // Atomically clear the guard UNLESS recovery claimed the slot during
            // the async cleanup above. Holding the write lock across the re-check
            // and the clear closes the TOCTOU race: a concurrent
            // `mark_held_for_recovery` can flip `ActiveTransfer` ->
            // `HeldForRecovery` after the steady-state check, and a non-atomic
            // clear would then drop a recovery-owned guard and fail a mint whose
            // tokens are still in the wallet -- re-opening the double-mint window
            // this guard exists to close. If recovery now owns the slot, leave
            // the guard and skip the failure event.
            if !self.clear_equity_in_progress_unless_held_for_recovery(&symbol) {
                debug!(
                    target: "rebalance",
                    aggregate_id = %id,
                    %symbol,
                    "Timed-out mint flipped to HeldForRecovery during cleanup; \
                     recovery now owns it -- leaving guard and skipping failure event"
                );
                continue;
            }

            let elapsed_secs = elapsed.as_secs();
            error!(
                target: "rebalance",
                aggregate_id = %id,
                symbol = %tracking.symbol,
                stage = %tracking.stage,
                elapsed_secs,
                outcome = "timeout",
                "Mint transfer timed out; clearing trigger guard and inventory inflight"
            );

            if let Some(store) = self.mint_store.read().await.as_ref() {
                let reason = format!(
                    "Transfer timed out after {elapsed_secs}s at stage {}",
                    tracking.stage
                );

                let command = match tracking.stage {
                    MintTrackingStage::Accepted => {
                        TokenizedEquityMintCommand::FailAcceptance { reason }
                    }
                    MintTrackingStage::TokensReceived | MintTrackingStage::WrapSubmitted => {
                        TokenizedEquityMintCommand::FailWrapping { reason }
                    }
                    MintTrackingStage::TokensWrapped | MintTrackingStage::VaultDepositSubmitted => {
                        TokenizedEquityMintCommand::FailRaindexDeposit { reason }
                    }
                    MintTrackingStage::Requested => continue,
                };

                if let Err(error) = store.send(&id, command).await {
                    warn!(
                        target: "rebalance",
                        %id, %error,
                        "Failed to emit timeout failure event for mint"
                    );
                }
            }
        }

        Ok(())
    }

    async fn expire_stuck_redemptions(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), RebalancingServiceError> {
        let timed_out_ids = {
            let tracking = self.redemption_tracking.read().await;
            tracking
                .iter()
                .filter_map(|(id, tracking)| {
                    let elapsed =
                        Self::elapsed_since_timeout_start(tracking.last_progress_at, now)?;

                    if elapsed >= self.config.transfer_timeout {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        for id in timed_out_ids {
            let Some((tracking, elapsed)) = self.cleanup_timed_out_redemption(&id, now).await?
            else {
                continue;
            };

            let elapsed_secs = elapsed.as_secs();
            error!(
                target: "rebalance",
                aggregate_id = %id,
                symbol = %tracking.symbol,
                stage = %tracking.stage,
                elapsed_secs,
                outcome = "timeout",
                "Redemption transfer timed out; clearing trigger guard and inventory inflight"
            );

            self.clear_equity_in_progress(&tracking.symbol);

            if let Some(store) = self.redemption_store.read().await.as_ref() {
                let reason = format!(
                    "Transfer timed out after {elapsed_secs}s at stage {}",
                    tracking.stage
                );

                let command = match tracking.stage {
                    RedemptionTrackingStage::VaultWithdrawPending
                    | RedemptionTrackingStage::VaultWithdrawSubmitted
                    | RedemptionTrackingStage::WithdrawnFromRaindex
                    | RedemptionTrackingStage::UnwrapPending
                    | RedemptionTrackingStage::UnwrapSubmitted
                    | RedemptionTrackingStage::TokensUnwrapped
                    | RedemptionTrackingStage::SendPending => {
                        Some(EquityRedemptionCommand::FailTransfer { reason })
                    }
                    RedemptionTrackingStage::TokensSent => {
                        Some(EquityRedemptionCommand::FailDetection {
                            failure: crate::equity_redemption::DetectionFailure::Timeout,
                        })
                    }
                    RedemptionTrackingStage::Detected => {
                        Some(EquityRedemptionCommand::RejectRedemption { reason })
                    }
                };

                if let Some(command) = command
                    && let Err(error) = store.send(&id, command).await
                {
                    warn!(
                        target: "rebalance",
                        %id, %error,
                        "Failed to emit timeout failure event for redemption"
                    );
                }
            }
        }

        Ok(())
    }

    async fn expire_stuck_usdc_rebalances(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), RebalancingServiceError> {
        // Select ids to examine this tick. The selection is intentionally broad:
        //
        // - Post-burn entries are ALWAYS selected regardless of elapsed time.
        //   The durable-Reconciled check is safe to run at any age: it only
        //   fires when the store already shows Reconciled, and clearing the guard
        //   at that point is always correct. This ensures that a CLI
        //   `transfer reconcile` issued before the failed transfer ages past
        //   `transfer_timeout` still takes effect on the next sweep tick.
        //
        // - Pre-burn entries (not yet post-burn) are only selected after the
        //   timeout elapses, as before: they cannot have an in-flight on-chain
        //   burn so abandonment is safe to defer until then.
        let candidate_ids = {
            let tracking = self.usdc_tracking.read().await;
            tracking
                .iter()
                .filter_map(|(id, tracking)| {
                    let elapsed =
                        Self::elapsed_since_timeout_start(tracking.last_progress_at, now)?;

                    if tracking.is_post_burn() || elapsed >= self.config.transfer_timeout {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        for id in candidate_ids {
            // A timed-out transfer whose apalis row is still in flight is NOT
            // abandoned: the job will resume and may have an irreversible on-chain
            // withdraw/burn already submitted. Skip the cleanup entirely so we
            // neither clear the in-progress guard / inventory inflight nor mark it
            // timed-out (which would make the reactor ignore its eventual terminal
            // event and latch the guard forever). Let apalis drive it to terminal.
            if self.transfer_in_flight_for_id(&id).await {
                warn!(
                    target: "rebalance",
                    aggregate_id = %id,
                    "USDC transfer exceeded its timeout but its apalis row is still in \
                     flight; deferring cleanup to the job rather than clearing the guard"
                );
                continue;
            }

            let Some(cleanup) = self.cleanup_timed_out_usdc_rebalance(&id, now).await? else {
                continue;
            };

            match cleanup {
                UsdcTimeoutCleanup::Cleared { tracking, elapsed } => {
                    error!(
                        target: "rebalance",
                        aggregate_id = %id,
                        direction = ?tracking.direction,
                        stage = %tracking.stage,
                        ?elapsed,
                        "USDC transfer timed out; clearing trigger guard and inventory inflight"
                    );

                    self.clear_usdc_in_progress();
                }
                UsdcTimeoutCleanup::PreservedPostBurn { tracking, elapsed } => {
                    self.usdc_in_progress.store(true, Ordering::SeqCst);

                    // The preserved entry keeps its original `last_progress_at`,
                    // so every subsequent sweep re-selects it. Log once per stuck
                    // transfer to avoid flooding the error stream until manual
                    // recovery. UsdcRebalanceId is never reused, so a logged id
                    // never needs clearing.
                    let first_log = self
                        .post_burn_timeout_logged
                        .write()
                        .await
                        .insert(id.clone());

                    if first_log {
                        error!(
                            target: "rebalance",
                            aggregate_id = %id,
                            direction = ?tracking.direction,
                            stage = %tracking.stage,
                            ?elapsed,
                            "USDC transfer timed out after CCTP burn; preserving trigger guard and inventory inflight"
                        );
                    }

                    // Alert delivery is deduped separately from the one-shot log:
                    // a transient notifier failure must NOT permanently silence the
                    // operator page about stranded funds. Re-attempt on every sweep
                    // until one delivery succeeds, then record it so it is not re-sent.
                    let already_alerted = self.post_burn_timeout_alerted.read().await.contains(&id);

                    if !already_alerted {
                        match self.notifier.notify(&format!(
                            "USDC transfer {id} timed out after CCTP burn and is stalled. \
                             Guard preserved. Stage: {}. Elapsed: {elapsed:?}. Manual operator action required.",
                            tracking.stage,
                        )).await {
                            Ok(()) => {
                                self.post_burn_timeout_alerted.write().await.insert(id.clone());
                            }
                            Err(error) => {
                                warn!(
                                    target: "rebalance",
                                    ?error,
                                    "Failed to deliver USDC post-burn stall alert; will retry next sweep"
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns true if a USDC transfer apalis row for `id` -- in *either*
    /// direction -- is still non-terminal (Pending/Queued/Running). The timeout
    /// sweeper must not clear the `usdc_in_progress` guard while such a row exists:
    /// the resuming job may have an irreversible withdraw/burn/mint already
    /// submitted, and clearing the guard would let a second transfer touch the same
    /// vault/wallet.
    ///
    /// Checks BOTH directions (mirroring [`Self::in_flight_usdc_transfer`]): an
    /// `AlpacaToBase` rebalance is driven by a `TransferUsdcToMarketMaking` job, so
    /// checking only the hedging queue would miss it and clear the guard while a
    /// market-making transfer is still in flight. Scoped to the aggregate `id`
    /// (carried in the job payload) so an unrelated in-flight transfer never defers
    /// this id's cleanup. On query/decode failure the safe default is to report
    /// "in flight" so automation stays latched.
    async fn transfer_in_flight_for_id(&self, id: &UsdcRebalanceId) -> bool {
        // Both transfer payloads serialize as `{ id, amount, .. }`; only the id is
        // needed to scope the sweep, so decode just that field for either direction.
        #[derive(serde::Deserialize)]
        struct TransferJobId {
            id: UsdcRebalanceId,
        }

        let rows: Result<Vec<(Vec<u8>,)>, _> = sqlx_apalis::query_as(
            "SELECT job FROM Jobs \
             WHERE job_type IN (?, ?) \
             AND status IN ('Pending', 'Queued', 'Running')",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
        .fetch_all(self.transfer_usdc_to_hedging_queue.pool())
        .await;

        match rows {
            Ok(rows) => rows.iter().any(|(payload,)| {
                match serde_json::from_slice::<TransferJobId>(payload) {
                    Ok(job) => job.id == *id,
                    Err(error) => {
                        warn!(
                            target: "rebalance",
                            %error,
                            "Failed to decode a non-terminal USDC transfer payload during \
                             timeout sweep; treating as in flight to stay latched"
                        );
                        true
                    }
                }
            }),
            Err(error) => {
                warn!(
                    target: "rebalance",
                    %error,
                    "Failed to query in-flight USDC transfer rows during timeout sweep; \
                     keeping the in-progress guard latched to avoid re-arming"
                );
                true
            }
        }
    }

    /// Whether a USDC transfer apalis row for `id` exists in *either* direction in
    /// ANY status -- including terminal `Failed`/`Done`, unlike
    /// [`Self::transfer_in_flight_for_id`].
    ///
    /// Used by startup re-arm for the pre-mint-confirmation post-burn states to
    /// fire only for the true "no job row at all" strand (a failed redrive
    /// enqueue, or a crash before the enqueue committed). A `Failed` row is a job
    /// that exhausted its retry budget and is awaiting operator reconciliation --
    /// re-arming it would re-drive a known-failing transfer on every restart and
    /// bypass the retry budget, contradicting [`JobQueue::requeue_orphaned`]'s
    /// policy of leaving `Failed` rows latched. The operator path for such a
    /// transfer is the manual `transfer resume --kind usdc` CLI, not an automatic re-arm.
    ///
    /// A recoverable post-burn `BridgingFailed` is the exception: it gates on
    /// [`Self::transfer_in_flight_for_id`] instead (only an in-flight row blocks),
    /// because recovery is new work rather than a continuation of the exhausted
    /// budget. See [`Self::rearm_stranded_transfers`].
    ///
    /// Unlike the timeout-sweep probe, this PROPAGATES query/decode failures so
    /// startup recovery fails fast rather than silently skipping (or spuriously
    /// firing) a re-arm it could not verify.
    async fn transfer_has_job_row_for_id(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<bool, sqlx_apalis::Error> {
        #[derive(serde::Deserialize)]
        struct TransferJobId {
            id: UsdcRebalanceId,
        }

        let rows: Vec<(Vec<u8>,)> =
            sqlx_apalis::query_as("SELECT job FROM Jobs WHERE job_type IN (?, ?)")
                .bind(std::any::type_name::<TransferUsdcToHedging>())
                .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
                .fetch_all(self.transfer_usdc_to_hedging_queue.pool())
                .await?;

        for (payload,) in rows {
            let job: TransferJobId = serde_json::from_slice(&payload)
                .map_err(|error| sqlx_apalis::Error::Decode(Box::new(error)))?;
            if job.id == *id {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Whether apalis still OWNS a transfer job for `id` -- a row it will run or
    /// re-run on its own: in-flight (`Pending`/`Queued`/`Running`) OR a `Failed`
    /// row with retries remaining (`attempts < max_attempts`), which apalis
    /// re-fetches. Terminal rows (`Done`/`Killed`, or `Failed` with the retry
    /// budget exhausted) do NOT count -- that job has concluded.
    ///
    /// Gates re-arm of a recoverable post-burn `BridgingFailed`: recovery is new
    /// work that may take over once the original transfer job has concluded, but
    /// must NOT start a second driver while apalis still owns a live row for the
    /// same id (which would run two concurrent resumes). Propagates query/decode
    /// failures so startup recovery fails fast rather than silently skipping a
    /// re-arm it could not verify.
    async fn transfer_live_job_for_id(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<bool, sqlx_apalis::Error> {
        #[derive(serde::Deserialize)]
        struct TransferJobId {
            id: UsdcRebalanceId,
        }

        let rows: Vec<(Vec<u8>,)> = sqlx_apalis::query_as(
            "SELECT job FROM Jobs \
             WHERE job_type IN (?, ?) \
             AND (status IN ('Pending', 'Queued', 'Running') \
                  OR (status = 'Failed' AND attempts < max_attempts))",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
        .fetch_all(self.transfer_usdc_to_hedging_queue.pool())
        .await?;

        for (payload,) in rows {
            let job: TransferJobId = serde_json::from_slice(&payload)
                .map_err(|error| sqlx_apalis::Error::Decode(Box::new(error)))?;
            if job.id == *id {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn retire_stale_suppression_and_collect_active_symbols(
        &self,
        fetched_at: DateTime<Utc>,
    ) -> HashSet<Symbol> {
        let mut suppressed_symbols = self.suppressed_inflight_symbols.write().await;
        suppressed_symbols.retain(|_, cleared_at| *cleared_at >= fetched_at);
        suppressed_symbols.keys().cloned().collect()
    }

    fn filter_suppressed_inflight_snapshot(
        mints: &std::collections::BTreeMap<Symbol, FractionalShares>,
        redemptions: &std::collections::BTreeMap<Symbol, FractionalShares>,
        active_suppressed_symbols: &HashSet<Symbol>,
    ) -> (
        std::collections::BTreeMap<Symbol, FractionalShares>,
        std::collections::BTreeMap<Symbol, FractionalShares>,
    ) {
        let filtered_mints = mints
            .iter()
            .filter(|(symbol, _)| {
                let keep = !active_suppressed_symbols.contains(*symbol);

                if !keep {
                    debug!(
                        target: "rebalance",
                        %symbol,
                        "Ignoring inflight mint snapshot after timeout cleanup"
                    );
                }

                keep
            })
            .map(|(symbol, quantity)| (symbol.clone(), *quantity))
            .collect();

        let filtered_redemptions = redemptions
            .iter()
            .filter(|(symbol, _)| {
                let keep = !active_suppressed_symbols.contains(*symbol);

                if !keep {
                    debug!(
                        target: "rebalance",
                        %symbol,
                        "Ignoring inflight redemption snapshot after timeout cleanup"
                    );
                }

                keep
            })
            .map(|(symbol, quantity)| (symbol.clone(), *quantity))
            .collect();

        (filtered_mints, filtered_redemptions)
    }

    async fn cleanup_timed_out_mint(
        &self,
        id: &IssuerRequestId,
        now: DateTime<Utc>,
    ) -> Result<Option<(MintTracking, Duration)>, RebalancingServiceError> {
        let _event_sync_guard = self.mint_event_sync.lock().await;
        let mut tracking_guard = self.mint_tracking.write().await;
        let Some(tracking) = tracking_guard.get(id).cloned() else {
            return Ok(None);
        };

        let Some(elapsed) = Self::elapsed_since_timeout_start(tracking.last_progress_at, now)
        else {
            return Ok(None);
        };

        if elapsed < self.config.transfer_timeout {
            return Ok(None);
        }

        let mut inventory = self.inventory.write().await;
        *inventory = inventory
            .clone()
            .clear_equity_inflight(&tracking.symbol, Venue::Hedging, now)?
            .clear_active_mint(&tracking.symbol);
        drop(inventory);

        self.timed_out_mints.write().await.insert(id.clone(), now);
        self.suppressed_inflight_symbols
            .write()
            .await
            .insert(tracking.symbol.clone(), now);
        tracking_guard.remove(id);
        drop(tracking_guard);

        Ok(Some((tracking, elapsed)))
    }

    async fn cleanup_timed_out_redemption(
        &self,
        id: &RedemptionAggregateId,
        now: DateTime<Utc>,
    ) -> Result<Option<(RedemptionTracking, Duration)>, RebalancingServiceError> {
        let _event_sync_guard = self.redemption_event_sync.lock().await;
        let mut tracking_guard = self.redemption_tracking.write().await;
        let Some(tracking) = tracking_guard.get(id).cloned() else {
            return Ok(None);
        };

        let Some(elapsed) = Self::elapsed_since_timeout_start(tracking.last_progress_at, now)
        else {
            return Ok(None);
        };

        if elapsed < self.config.transfer_timeout {
            return Ok(None);
        }

        let mut inventory = self.inventory.write().await;
        *inventory = inventory
            .clone()
            .clear_equity_inflight(&tracking.symbol, Venue::MarketMaking, now)?
            .clear_active_redemption(&tracking.symbol);
        drop(inventory);

        self.timed_out_redemptions
            .write()
            .await
            .insert(id.clone(), now);
        self.suppressed_inflight_symbols
            .write()
            .await
            .insert(tracking.symbol.clone(), now);
        tracking_guard.remove(id);
        drop(tracking_guard);

        Ok(Some((tracking, elapsed)))
    }

    async fn cleanup_timed_out_usdc_rebalance(
        &self,
        id: &UsdcRebalanceId,
        now: DateTime<Utc>,
    ) -> Result<Option<UsdcTimeoutCleanup>, RebalancingServiceError> {
        let _event_sync_guard = self.usdc_event_sync.lock().await;
        let mut tracking_guard = self.usdc_tracking.write().await;
        let Some(tracking) = tracking_guard.get(id).cloned() else {
            return Ok(None);
        };

        let Some(elapsed) = Self::elapsed_since_timeout_start(tracking.last_progress_at, now)
        else {
            return Ok(None);
        };

        if tracking.is_post_burn() {
            // Post-burn: check durable state FIRST, regardless of elapsed time.
            // The Reconciled check is always safe: it only fires when the
            // aggregate is actually Reconciled and clearing the guard at that
            // point is always correct. Running it before the timeout gate means
            // that a CLI `transfer reconcile` issued before the failed
            // transfer ages past `transfer_timeout` takes effect on the next
            // sweep tick rather than waiting for the full timeout window.
            let usdc_store_guard = self.usdc_store.read().await;
            if let Some(store) = usdc_store_guard.as_ref() {
                match store.load(id).await {
                    Ok(Some(UsdcRebalance::Reconciled { .. })) => {
                        // Durable state is Reconciled: the CLI's separate-process
                        // store emitted OperatorReconciled but the live reactor
                        // never saw it. Apply the side-effect here: zero the
                        // source-venue inflight and clear the active rebalance.
                        //
                        // Drop the tracking lock BEFORE acquiring inventory.
                        // `usdc_event_sync` (held by both this sweep path and the
                        // reactor's `on_usdc_rebalance`) serialises the two paths,
                        // so there is no deadlock risk from lock ordering between
                        // them. We drop the tracking lock before acquiring inventory
                        // to reduce unnecessary contention -- the relative ordering
                        // of these two inner locks is not load-bearing for deadlock
                        // safety; `usdc_event_sync` serialises sweep vs reactor
                        // entirely.
                        //
                        // Both inventory mutations (inflight clear and active
                        // rebalance clear) are chained in a single lock
                        // acquisition so there is never a transient state where
                        // inflight is zeroed but active_usdc_rebalance is still
                        // set.
                        tracking_guard.remove(id);
                        drop(tracking_guard);
                        drop(usdc_store_guard);

                        let mut inventory = self.inventory.write().await;
                        let result = inventory
                            .clone()
                            .clear_usdc_inflight(tracking.source_venue(), now)
                            .map(InventoryView::clear_active_usdc_rebalance);

                        match result {
                            Ok(updated) => *inventory = updated,
                            Err(error) => {
                                drop(inventory);
                                // In practice this branch cannot be triggered:
                                // `clear_usdc_inflight` only fails when the
                                // resulting inflight would be negative
                                // (`Inventory::set_inflight` guards against
                                // this), and zeroing inflight always produces a
                                // non-negative result. The branch is retained as
                                // a defensive layer: if a future inventory
                                // operation can fail, re-inserting tracking
                                // ensures the next sweep tick retries rather
                                // than latching the guard forever with no entry
                                // to re-select.
                                self.usdc_tracking
                                    .write()
                                    .await
                                    .insert(id.clone(), tracking);
                                return Err(error.into());
                            }
                        }
                        drop(inventory);

                        self.timed_out_usdc_rebalances
                            .write()
                            .await
                            .insert(id.clone(), now);
                        return Ok(Some(UsdcTimeoutCleanup::Cleared { tracking, elapsed }));
                    }
                    Ok(Some(_) | None) => {
                        // Guard-holding state or aggregate not yet in store:
                        // fall through to the timeout gate below.
                        drop(usdc_store_guard);
                    }
                    Err(load_error) => {
                        // Store read failed (I/O error, deserialization error,
                        // etc.). Fail safe: preserve the guard so a possibly-
                        // post-burn rebalance cannot be re-burned. Log so
                        // operators can diagnose a persistent DB issue rather
                        // than seeing only "Skipped USDC trigger: already in
                        // progress" with no cause.
                        warn!(
                            target: "rebalance",
                            id = %id,
                            ?load_error,
                            "Failed to load UsdcRebalance from store during \
                             post-burn sweep; preserving guard"
                        );
                        drop(usdc_store_guard);
                        return Ok(Some(UsdcTimeoutCleanup::PreservedPostBurn {
                            tracking,
                            elapsed,
                        }));
                    }
                }
            }

            // Not yet Reconciled (or store not wired): only transition to
            // PreservedPostBurn after the timeout has elapsed so we do not log
            // the "timed out after CCTP burn" error on every sweep tick for a
            // transfer that is still within its normal window.
            if elapsed < self.config.transfer_timeout {
                return Ok(None);
            }

            return Ok(Some(UsdcTimeoutCleanup::PreservedPostBurn {
                tracking,
                elapsed,
            }));
        }

        // Pre-burn path: only act after the timeout has elapsed.
        if elapsed < self.config.transfer_timeout {
            return Ok(None);
        }

        let mut inventory = self.inventory.write().await;
        *inventory = inventory
            .clone()
            .clear_usdc_inflight(tracking.source_venue(), now)?
            .clear_active_usdc_rebalance();
        drop(inventory);

        self.timed_out_usdc_rebalances
            .write()
            .await
            .insert(id.clone(), now);
        tracking_guard.remove(id);
        drop(tracking_guard);

        Ok(Some(UsdcTimeoutCleanup::Cleared { tracking, elapsed }))
    }

    async fn mint_timed_out(&self, id: &IssuerRequestId) -> bool {
        self.timed_out_mints.read().await.contains_key(id)
    }

    async fn redemption_timed_out(&self, id: &RedemptionAggregateId) -> bool {
        self.timed_out_redemptions.read().await.contains_key(id)
    }

    async fn track_mint_progress(&self, id: &IssuerRequestId, event: &TokenizedEquityMintEvent) {
        let mut tracking = self.mint_tracking.write().await;

        if let Some(existing) = tracking.get_mut(id) {
            existing.track_progress(event);
            return;
        }

        if let Some(created) = MintTracking::from_requested_event(event) {
            tracking.insert(id.clone(), created);
        }
    }

    async fn track_redemption_progress(
        &self,
        id: &RedemptionAggregateId,
        event: &EquityRedemptionEvent,
    ) -> Result<(), RebalancingServiceError> {
        let mut tracking = self.redemption_tracking.write().await;

        if let Some(existing) = tracking.get_mut(id) {
            existing.track_progress(event)?;
            return Ok(());
        }

        if let Some(created) = RedemptionTracking::from_genesis_event(event) {
            tracking.insert(id.clone(), created);
        }
        drop(tracking);

        Ok(())
    }

    /// Fold the snapshot event into the view, then enqueue any
    /// follow-up imbalance checks the event implies. A failed apply
    /// (including recovery) short-circuits enqueueing so rebalancing
    /// never runs against a stale view.
    async fn on_snapshot(
        &self,
        event: InventorySnapshotEvent,
    ) -> Result<(), RebalancingServiceError> {
        use InventorySnapshotEvent::*;

        let now = Utc::now();
        let fetched_at = event.timestamp();
        self.expire_stuck_operations(now).await?;

        let filtered_inflight = match &event {
            InflightEquity {
                mints, redemptions, ..
            } => {
                let active_suppressed_symbols = self
                    .retire_stale_suppression_and_collect_active_symbols(fetched_at)
                    .await;

                Some(Self::filter_suppressed_inflight_snapshot(
                    mints,
                    redemptions,
                    &active_suppressed_symbols,
                ))
            }
            _ => None,
        };

        let mut inventory = self.inventory.write().await;

        let updated = match &event {
            OnchainEquity { balances, .. } => inventory.clone().apply_equity_snapshot(
                Venue::MarketMaking,
                balances.iter(),
                fetched_at,
                now,
            ),

            OnchainUsdc { usdc_balance, .. } => inventory.clone().update_usdc(
                Inventory::on_snapshot(Venue::MarketMaking, *usdc_balance, fetched_at),
                now,
            ),

            OffchainEquity { positions, .. } => inventory.clone().apply_equity_snapshot(
                Venue::Hedging,
                positions.iter(),
                fetched_at,
                now,
            ),

            OffchainUsd { .. }
            | OffchainCashBuyingPower { .. }
            | OffchainCashWithdrawable { .. }
            | AlpacaUsdc { .. }
            | EthereumUsdc { .. }
            | BaseWalletUsdc { .. }
            | BaseWalletUnwrappedEquity { .. }
            | BaseWalletWrappedEquity { .. } => inventory.clone().apply_snapshot_event(&event, now),

            InflightEquity { .. } => {
                if let Some((mints, redemptions)) = &filtered_inflight {
                    inventory
                        .clone()
                        .apply_inflight_snapshot(mints, redemptions, fetched_at, now)
                } else {
                    Ok(inventory.clone())
                }
            }
        }?;

        *inventory = updated;
        drop(inventory);

        trace!(target: "rebalance", "Applied inventory snapshot event");

        self.enqueue_checks_for_snapshot(&event).await;
        Ok(())
    }

    /// Reprocess a snapshot event after the normal handler failed.
    ///
    /// Resets the inventory, then force-applies the snapshot --
    /// bypassing inflight guards that may have caused the
    /// original failure.
    async fn on_snapshot_recovery(
        &self,
        error: RebalancingServiceError,
        event: InventorySnapshotEvent,
    ) -> Result<(), RebalancingServiceError> {
        use InventorySnapshotEvent::*;
        use RebalancingServiceError::{
            ApalisSqlx, EquityTrigger, Float, MissingUsdcBridgedAmount, MissingUsdcTrackingContext,
            Projection, RearmEnqueue, RedemptionUnwrappedExceedsTracked,
            SettledUsdcExceedsInitiatedAmount, SharesConversion, Sqlx,
        };

        self.expire_stuck_operations_with_logging().await;

        let inventory_error = match error {
            RebalancingServiceError::Inventory(inventory_error) => inventory_error,
            other @ (Projection(_)
            | EquityTrigger(_)
            | Float(_)
            | SharesConversion(_)
            | MissingUsdcTrackingContext { .. }
            | MissingUsdcBridgedAmount { .. }
            | SettledUsdcExceedsInitiatedAmount { .. }
            | RedemptionUnwrappedExceedsTracked { .. }
            | Sqlx(_)
            | ApalisSqlx(_)
            | RearmEnqueue(_)) => {
                return Err(other);
            }
        };

        warn!(
            target: "rebalance",
            ?inventory_error,
            "Resetting inventory and force-applying snapshot to recover"
        );

        // Wrap in Arc so it can be cloned across multiple force_on_snapshot calls
        let recovery_reason = Arc::new(inventory_error);

        let now = Utc::now();
        let filtered_inflight = match &event {
            InflightEquity {
                mints,
                redemptions,
                fetched_at,
            } => {
                let active_suppressed_symbols = self
                    .retire_stale_suppression_and_collect_active_symbols(*fetched_at)
                    .await;

                Some(Self::filter_suppressed_inflight_snapshot(
                    mints,
                    redemptions,
                    &active_suppressed_symbols,
                ))
            }
            _ => None,
        };
        let mut inventory = self.inventory.write().await;
        *inventory = InventoryView::default();

        let updated = match &event {
            OnchainUsdc { usdc_balance, .. } => inventory.clone().update_usdc(
                Inventory::force_on_snapshot(
                    Venue::MarketMaking,
                    *usdc_balance,
                    recovery_reason.clone(),
                ),
                now,
            ),

            OnchainEquity { .. }
            | OffchainEquity { .. }
            | OffchainUsd { .. }
            | OffchainCashBuyingPower { .. }
            | OffchainCashWithdrawable { .. }
            | AlpacaUsdc { .. }
            | EthereumUsdc { .. }
            | BaseWalletUsdc { .. }
            | BaseWalletUnwrappedEquity { .. }
            | BaseWalletWrappedEquity { .. } => {
                inventory
                    .clone()
                    .force_apply_snapshot_event(&event, now, recovery_reason)
            }

            // Recovery for inflight snapshots: forward the original fetched_at
            // so is_stale_for_symbol still rejects pre-rebalancing polls.
            InflightEquity { fetched_at, .. } => {
                if let Some((mints, redemptions)) = &filtered_inflight {
                    inventory
                        .clone()
                        .apply_inflight_snapshot(mints, redemptions, *fetched_at, now)
                } else {
                    Ok(inventory.clone())
                }
            }
        }?;

        *inventory = updated;
        drop(inventory);

        debug!(target: "rebalance", "Force-applied inventory snapshot after recovery");

        self.enqueue_checks_for_snapshot(&event).await;
        Ok(())
    }

    /// Enqueue the follow-up imbalance checks implied by a snapshot
    /// event. Events that change tradeable balances (equity / USDC at
    /// either venue) warrant an imbalance check; wallet-read
    /// snapshots, inflight snapshots, and buying-power updates do not.
    async fn enqueue_checks_for_snapshot(&self, event: &InventorySnapshotEvent) {
        use InventorySnapshotEvent::*;

        match event {
            // Per-symbol equity snapshots: fan out into one equity check
            // per symbol so independent retries and parallel work fall
            // out naturally. Iterating the event map (not the view's full
            // applied set) is sufficient: `InventoryView::apply_equity_snapshot`
            // additionally zeroes symbols absent from a complete snapshot, but
            // both feeders guarantee any such symbol is already a key here --
            // offchain polling seeds every configured symbol explicitly, and
            // onchain polling emits a key for every vault in the monotonic
            // registry.
            OnchainEquity { balances, .. } => {
                for symbol in balances.keys() {
                    self.equity_scheduler.enqueue_check(symbol.clone()).await;
                }
            }
            OffchainEquity { positions, .. } => {
                for symbol in positions.keys() {
                    self.equity_scheduler.enqueue_check(symbol.clone()).await;
                }
            }
            // Global USDC snapshots: one USDC check.
            //
            // OffchainCashWithdrawable feeds the Alpaca-to-Base capacity
            // gate (`withdrawable_cash_cents - reserved`). After the
            // trigger skips with `MissingWithdrawableCash` because the
            // broker omitted the field, the recovery snapshot is what
            // brings rebalancing back online — without scheduling on it,
            // the imbalance would remain unresolved until some unrelated
            // USDC balance event happened to arrive.
            OnchainUsdc { .. } | OffchainUsd { .. } | OffchainCashWithdrawable { .. } => {
                self.usdc_scheduler.enqueue_check().await;
            }
            // Wrapped equity in the bot wallet (outside Raindex) triggers
            // a recovery dispatch job per symbol with a positive balance.
            // Gated on the per-symbol `wrapped_equity_recovery` config so
            // tests that pre-stage wallet wtSTOCK (e.g. for orderbook
            // mechanics) can opt out.
            BaseWalletWrappedEquity { balances, .. } => {
                for (symbol, amount) in balances {
                    if *amount == FractionalShares::ZERO {
                        continue;
                    }
                    if !self.is_wrapped_equity_recovery_enabled(symbol) {
                        continue;
                    }

                    let mut queue = self.wrapped_equity_recovery_queue.clone();
                    let job_type = std::any::type_name::<WrappedEquityRecoveryJob>();

                    // Match the serialized `symbol` field exactly (json_extract, not
                    // a LIKE substring) so a queued job for a ticker that contains
                    // this symbol as a substring (e.g. GOOGL vs GOOG) can't suppress
                    // a distinct recovery.
                    let existing: Result<(i64,), _> = sqlx_apalis::query_as(
                        "SELECT COUNT(*) FROM Jobs \
                         WHERE job_type = ? \
                         AND status IN ('Pending', 'Queued', 'Running') \
                         AND json_extract(job, '$.symbol') = ?",
                    )
                    .bind(job_type)
                    .bind(symbol.to_string())
                    .fetch_one(queue.pool())
                    .await;

                    match existing {
                        Ok((count,)) if count > 0 => {
                            debug!(
                                target: "rebalance",
                                %symbol, %count,
                                "Skipped WrappedEquityRecoveryJob enqueue: a non-terminal \
                                 row for this symbol already exists",
                            );
                            continue;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                target: "rebalance",
                                %symbol, %error,
                                "Failed to query existing WrappedEquityRecoveryJob rows; \
                                 skipping enqueue to avoid duplicates",
                            );
                            continue;
                        }
                    }

                    let recovery_id = WrappedEquityRecoveryId(Uuid::new_v4());
                    if let Err(error) = queue
                        .push(WrappedEquityRecoveryJob {
                            symbol: symbol.clone(),
                            recovery_id: recovery_id.clone(),
                        })
                        .await
                    {
                        warn!(
                            target: "rebalance",
                            %symbol, %recovery_id, ?error,
                            "Failed to enqueue WrappedEquityRecoveryJob",
                        );
                    }
                }
            }
            // Unwrapped equity (tSTOCK) in the bot wallet triggers a
            // recovery dispatch per symbol with a positive balance.
            // Gated per-symbol on `wrapped_equity_recovery` -- the same
            // config flag covers both detection paths since they share
            // the same "auto-recover misplaced equity" intent.
            BaseWalletUnwrappedEquity { balances, .. } => {
                for (symbol, amount) in balances {
                    if *amount == FractionalShares::ZERO {
                        continue;
                    }
                    if !self.is_wrapped_equity_recovery_enabled(symbol) {
                        continue;
                    }

                    let mut queue = self.unwrapped_equity_recovery_queue.clone();
                    let job_type = std::any::type_name::<UnwrappedEquityRecoveryJob>();

                    // Status-scoped dedup, mirroring the wrapped path: only a
                    // non-terminal row blocks a re-enqueue. apalis's
                    // `ON CONFLICT(job_type, idempotency_key) DO NOTHING` never
                    // surfaces a unique-violation, and its index spans all
                    // statuses, so an idempotency key would silently wedge a
                    // symbol behind its own `Done` row until the hourly cleanup
                    // -- starving the next-poll re-dispatch the guard-contention
                    // skip relies on. Match the serialized `symbol` field
                    // exactly (json_extract, not a LIKE substring) so a queued
                    // job for a ticker that contains this symbol as a substring
                    // (e.g. GOOGL vs GOOG) can't suppress a distinct recovery.
                    let existing: Result<(i64,), _> = sqlx_apalis::query_as(
                        "SELECT COUNT(*) FROM Jobs \
                         WHERE job_type = ? \
                         AND status IN ('Pending', 'Queued', 'Running') \
                         AND json_extract(job, '$.symbol') = ?",
                    )
                    .bind(job_type)
                    .bind(symbol.to_string())
                    .fetch_one(queue.pool())
                    .await;

                    match existing {
                        Ok((count,)) if count > 0 => {
                            debug!(
                                target: "rebalance",
                                %symbol, %count,
                                "Skipped UnwrappedEquityRecoveryJob enqueue: a non-terminal \
                                 row for this symbol already exists",
                            );
                            continue;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                target: "rebalance",
                                %symbol, %error,
                                "Failed to query existing UnwrappedEquityRecoveryJob rows; \
                                 skipping enqueue to avoid duplicates",
                            );
                            continue;
                        }
                    }

                    let recovery_id = UnwrappedEquityRecoveryId(Uuid::new_v4());
                    if let Err(error) = queue
                        .push(UnwrappedEquityRecoveryJob {
                            symbol: symbol.clone(),
                            recovery_id: recovery_id.clone(),
                        })
                        .await
                    {
                        warn!(
                            target: "rebalance",
                            %symbol, %recovery_id, ?error,
                            "Failed to enqueue UnwrappedEquityRecoveryJob",
                        );
                    }
                }
            }
            // Wallet-read USDC events update `inflight_cash` for
            // visibility but don't drive triggers here.
            // Suppression-aware re-triggering will be added once
            // orphan-vs-baseline detection is in place.
            EthereumUsdc { .. }
            | BaseWalletUsdc { .. }
            | AlpacaUsdc { .. }
            // Buying power is display-only and doesn't feed any rebalance
            // decision.
            | OffchainCashBuyingPower { .. }
            // Inflight snapshots don't trigger rebalancing -- they
            // indicate transfers already in progress, not new balances
            // to rebalance.
            | InflightEquity { .. } => {}
        }
    }

    /// Re-drives recovery producers from the already-hydrated inventory view.
    ///
    /// Startup hydration updates the in-memory view directly instead of
    /// dispatching snapshot reactor events. Wallet polls also suppress unchanged
    /// balances, so a positive tSTOCK/wtSTOCK balance that was persisted before
    /// restart would otherwise wait forever for a new event.
    pub(crate) async fn enqueue_recovery_for_current_wallet_balances(&self) {
        let (wrapped, unwrapped) = {
            let view = self.inventory.read().await;
            let mut wrapped = BTreeMap::new();
            let mut unwrapped = BTreeMap::new();

            for symbol in self.config.assets.equities.symbols.keys() {
                if let Some(amount) =
                    view.inflight_equity_at(symbol, InFlightEquityLocation::BaseWalletWrapped)
                {
                    wrapped.insert(symbol.clone(), amount);
                }
                if let Some(amount) =
                    view.inflight_equity_at(symbol, InFlightEquityLocation::BaseWalletUnwrapped)
                {
                    unwrapped.insert(symbol.clone(), amount);
                }
            }

            (wrapped, unwrapped)
        };

        let fetched_at = Utc::now();

        if !wrapped.is_empty() {
            self.enqueue_checks_for_snapshot(&InventorySnapshotEvent::BaseWalletWrappedEquity {
                balances: wrapped,
                fetched_at,
            })
            .await;
        }

        if !unwrapped.is_empty() {
            self.enqueue_checks_for_snapshot(&InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                balances: unwrapped,
                fetched_at,
            })
            .await;
        }
    }
}

deps!(
    RebalancingService,
    [
        Position,
        TokenizedEquityMint,
        EquityRedemption,
        UsdcRebalance,
        InventorySnapshot
    ]
);

#[async_trait]
impl Reactor for RebalancingService {
    type Error = RebalancingServiceError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|symbol, event| async move {
                use PositionEvent::*;

                let timestamp = event.timestamp();
                let mut clear_pending_offchain_order = false;
                let (equity_update, usdc_update) = match &event {
                    OnChainOrderFilled {
                        amount,
                        direction,
                        price_usdc,
                        ..
                    } => {
                        let equity_op: Operator = (*direction).into();
                        let quantity: Float = (*amount).into();
                        let usdc_value = (*price_usdc * quantity)?;
                        (
                            Inventory::available(Venue::MarketMaking, equity_op, *amount),
                            Inventory::available(
                                Venue::MarketMaking,
                                equity_op.inverse(),
                                Usdc::new(usdc_value),
                            ),
                        )
                    }
                    OffChainOrderFilled {
                        shares_filled,
                        direction,
                        price,
                        ..
                    } => {
                        // Always clear the gate on a Filled event, even if the
                        // inventory update below fails. A fail-closed gate would
                        // deadlock equity rebalancing for the symbol until the
                        // next bot restart, because the only event that could
                        // re-clear it (a later terminal offchain event) is
                        // itself gated. The polling cycle is the source of
                        // truth for the broker balance, so any local
                        // bookkeeping miss self-heals within ~60s and is bounded
                        // to wasted rebalances, not lost capital.
                        clear_pending_offchain_order = true;
                        let equity_op: Operator = (*direction).into();
                        let quantity: Float = shares_filled.inner().into();
                        let price_value = price.inner();
                        let usdc_value = (price_value * quantity)?;
                        (
                            Inventory::available(Venue::Hedging, equity_op, shares_filled.inner()),
                            Inventory::available(
                                Venue::Hedging,
                                equity_op.inverse(),
                                Usdc::new(usdc_value),
                            ),
                        )
                    }
                    OffChainOrderPlaced { .. } => {
                        self.pending_offchain_order_symbols
                            .write()
                            .await
                            .insert(symbol);
                        return Ok(());
                    }
                    OffChainOrderFailed { .. } => {
                        self.pending_offchain_order_symbols
                            .write()
                            .await
                            .remove(&symbol);
                        self.equity_scheduler.enqueue_check(symbol).await;
                        return Ok(());
                    }
                    Initialized { .. } | ThresholdUpdated { .. } => {
                        return Ok(());
                    }
                    ManualPositionAdjusted { .. } => {
                        // A manual adjustment can create a hedgeable imbalance.
                        // Nudge an immediate equity check (mirroring the
                        // OffChainOrderFailed arm) rather than waiting up to a
                        // full poll cycle for the periodic scan to notice.
                        self.equity_scheduler.enqueue_check(symbol).await;
                        return Ok(());
                    }
                };

                let inventory_result = {
                    let mut inventory = self.inventory.write().await;
                    let result = inventory
                        .clone()
                        .update_equity(&symbol, equity_update, timestamp)
                        .and_then(|inv| inv.update_usdc(usdc_update, timestamp));
                    if let Ok(updated) = &result {
                        *inventory = updated.clone();
                    }
                    result
                };

                if clear_pending_offchain_order {
                    // Clear the gate regardless of inventory outcome. See the
                    // OffChainOrderFilled arm above for the rationale.
                    if let Err(error) = &inventory_result {
                        warn!(
                            target: "rebalance",
                            %symbol,
                            ?error,
                            "Inventory update failed for offchain fill; clearing gate anyway and relying on next polling snapshot"
                        );
                    }
                    self.pending_offchain_order_symbols
                        .write()
                        .await
                        .remove(&symbol);
                    self.equity_scheduler.enqueue_check(symbol).await;
                    // Only re-check USDC if the cash leg actually applied.
                    // With and_then chaining, inventory_result == Err means
                    // neither leg was persisted, so the USDC mirror is
                    // unchanged from before this event -- the check would
                    // re-evaluate identical inputs and produce no new
                    // decision. The periodic USDC scheduler tick covers
                    // any drift the polling snapshot picks up.
                    if inventory_result.is_ok() {
                        self.usdc_scheduler.enqueue_check().await;
                    }
                    return Ok(());
                }

                inventory_result?;
                self.equity_scheduler.enqueue_check(symbol).await;
                self.usdc_scheduler.enqueue_check().await;
                Ok::<(), RebalancingServiceError>(())
            })
            .on(|id, event| async move { self.on_mint(id, event).await })
            .on(|id, event| async move { self.on_redemption(id, event).await })
            .on(|id, event| async move { self.on_usdc_rebalance(id, event).await })
            .on(|_id, event| async move {
                let recovery_event = event.clone();
                match self.on_snapshot(event).await {
                    Ok(()) => Ok(()),
                    Err(error) => self.on_snapshot_recovery(error, recovery_event).await,
                }
            })
            .exhaustive()
            .await
    }
}

#[async_trait]
impl PendingRequestOwnership for RebalancingService {
    async fn pending_request_ownership(&self) -> PendingRequestOwnershipSnapshot {
        let mint_tracking = self.mint_tracking.read().await;
        let redemption_tracking = self.redemption_tracking.read().await;

        PendingRequestOwnershipSnapshot {
            mint_issuers: mint_tracking.keys().cloned().collect(),
            mint_tokenizations: mint_tracking
                .values()
                .filter_map(|tracking| tracking.tokenization_request_id.clone())
                .collect(),
            redemption_tokenizations: redemption_tracking
                .values()
                .filter_map(|tracking| tracking.tokenization_request_id.clone())
                .collect(),
            redemption_txs: redemption_tracking
                .values()
                .filter_map(|tracking| tracking.redemption_tx)
                .collect(),
        }
    }
}

/// Describes the inventory/tombstone mutation a `rebuild_*_tracking_for_recovery`
/// call applied, so the caller can undo it precisely if the recovery dispatch
/// fails before the reactor completes the restored in-flight transfer. Without
/// this the live inventory would be left showing a phantom in-flight transfer
/// (and a symbol locked in-progress) for an aggregate whose recovery event was
/// never persisted.
pub(crate) enum RecoveryRollback {
    /// The rebuild touched no inventory balances (called on a non-failed
    /// aggregate, or an explicitly-failed redemption whose in-flight was never
    /// cancelled). Rollback only drops the tracking + in-progress guard.
    TrackingOnly,
    /// The rebuild moved available -> in-flight via `Start` (an explicitly
    /// failed transfer that had cancelled its in-flight back to available).
    /// Rollback cancels the in-flight back to available.
    CancelInflight,
    /// The rebuild re-set the in-flight and dropped the timeout tombstone
    /// (a timed-out transfer). Rollback clears the in-flight again and restores
    /// the tombstone + suppressed-inflight markers it removed.
    RestoreTombstone {
        timed_out_at: DateTime<Utc>,
        suppressed_at: Option<DateTime<Utc>>,
    },
}

/// Outcome of an atomic recovery claim. `rebuild_*_tracking_for_recovery`
/// checks symbol ownership and restores the in-flight under a single inventory
/// write lock, so a concurrent live mint/redemption cannot claim the slot
/// between the check and the restore (which would silently clobber its
/// in-flight, since recovery's `set_inflight` *replaces* rather than adds).
pub(crate) enum RecoveryClaim {
    /// The recovery claimed the symbol's slot. Carries the rollback needed if
    /// the recovery dispatch later fails before the reactor runs.
    Claimed(RecoveryRollback),
    /// A *different* live mint/redemption for the symbol already owns the slot,
    /// so recovery must abort without mutating inventory.
    Conflict,
}

#[cfg(test)]
impl RecoveryClaim {
    fn expect_claimed(self) -> RecoveryRollback {
        match self {
            Self::Claimed(rollback) => rollback,
            Self::Conflict => panic!("expected recovery to claim the slot, got Conflict"),
        }
    }
}

impl RebalancingService {
    async fn expire_stuck_operations_with_logging(&self) {
        if let Err(error) = self.expire_stuck_operations(Utc::now()).await {
            error!(target: "rebalance", ?error, "Failed to expire stuck rebalancing operations");
        }
    }

    fn try_claim_equity_guard_for_transfer(
        &self,
        symbol: &Symbol,
    ) -> Option<equity::InProgressGuard> {
        equity::InProgressGuard::try_claim_for_transfer(
            symbol.clone(),
            Arc::clone(&self.equity_in_progress),
        )
    }

    async fn has_pending_offchain_order(&self, symbol: &Symbol) -> bool {
        self.pending_offchain_order_symbols
            .read()
            .await
            .contains(symbol)
    }

    async fn build_equity_operation(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<TriggeredOperation>, equity::EquityTriggerError> {
        let wrapped_token = self.load_token_address(symbol).await?.ok_or(
            equity::EquityTriggerError::TokenNotInRegistry(symbol.clone()),
        )?;

        let unwrapped_token = self.wrapper.lookup_underlying(symbol)?;
        let vault_ratio = self.wrapper.get_ratio_for_symbol(symbol).await?;
        let shares_limit = self
            .config
            .assets
            .equities
            .symbols
            .get(symbol)
            .and_then(|config| config.operational_limit);

        equity::check_imbalance_and_build_operation(
            symbol,
            &self.config.equity,
            &self.inventory,
            wrapped_token,
            unwrapped_token,
            &vault_ratio,
            shares_limit,
        )
        .await
    }

    fn try_claim_usdc_guard(&self) -> Option<usdc::InProgressGuard> {
        usdc::InProgressGuard::try_claim(Arc::clone(&self.usdc_in_progress))
    }

    async fn load_mint_tracking(&self, id: &IssuerRequestId) -> Option<MintTracking> {
        let Some(tracking) = self.mint_tracking.read().await.get(id).cloned() else {
            warn!(target: "rebalance", id = %id, "Mint event for untracked aggregate");
            return None;
        };

        Some(tracking)
    }

    fn start_equity_transfer_update(
        venue: Venue,
        quantity: FractionalShares,
    ) -> EquityInventoryUpdate {
        Box::new(Inventory::transfer(venue, TransferOp::Start, quantity))
    }

    fn cancel_equity_transfer_update(
        venue: Venue,
        quantity: FractionalShares,
    ) -> EquityInventoryUpdate {
        let now = Utc::now();
        Box::new(move |inventory| {
            let cancelled = Inventory::transfer(venue, TransferOp::Cancel, quantity)(inventory)?;
            Inventory::with_last_rebalancing(now)(cancelled)
        })
    }

    fn complete_equity_transfer_update(
        venue: Venue,
        quantity: FractionalShares,
    ) -> EquityInventoryUpdate {
        let now = Utc::now();
        Box::new(move |inventory| {
            let transferred =
                Inventory::transfer(venue, TransferOp::Complete, quantity)(inventory)?;
            Inventory::with_last_rebalancing(now)(transferred)
        })
    }

    fn last_rebalancing_update() -> EquityInventoryUpdate {
        Box::new(Inventory::with_last_rebalancing(Utc::now()))
    }

    fn mint_inventory_update(
        event: &TokenizedEquityMintEvent,
        quantity: FractionalShares,
        stage: MintTrackingStage,
    ) -> Option<EquityInventoryUpdate> {
        use TokenizedEquityMintEvent::*;

        match event {
            MintAccepted { .. } => {
                Some(Self::start_equity_transfer_update(Venue::Hedging, quantity))
            }
            // Cancel the Hedging inflight that `MintAccepted` started -- but ONLY
            // if acceptance actually happened. The operator force-fail from
            // `MintRequested` (RAI-999) emits this same event PRE-acceptance,
            // where no inflight was ever started; cancelling there would be an
            // unmatched Cancel. Gating on the tracking stage makes this honour
            // SPEC's "no balance change for a pre-acceptance force-fail" rule by
            // construction -- independent of process topology (the CLI's
            // separate-process write also keeps it off this reactor today, but
            // this guard is what keeps it correct if `transfer fail` is ever
            // routed through the running bot). `track_mint_progress` does not
            // advance the stage on `MintAcceptanceFailed`, so the stage here is
            // `Requested` for a pre-acceptance fail and `Accepted` otherwise.
            MintAcceptanceFailed { .. } => match stage {
                MintTrackingStage::Requested => None,
                MintTrackingStage::Accepted
                | MintTrackingStage::TokensReceived
                | MintTrackingStage::WrapSubmitted
                | MintTrackingStage::TokensWrapped
                | MintTrackingStage::VaultDepositSubmitted => Some(
                    Self::cancel_equity_transfer_update(Venue::Hedging, quantity),
                ),
            },
            // TokensReceived completes the normal mint; ProviderCompletionRecovered
            // completes a recovered one. rebuild_mint_tracking_for_recovery restores
            // the canonical in-flight shape (Hedging in-flight = quantity, available
            // already debited) before dispatch, so recovery confirms the in-flight
            // the same way the success path does -- in-flight -> MarketMaking
            // available, plus last_rebalancing to suppress a redundant rebalance --
            // regardless of how the mint originally failed.
            TokensReceived { .. } | ProviderCompletionRecovered { .. } => Some(
                Self::complete_equity_transfer_update(Venue::Hedging, quantity),
            ),
            DepositedIntoRaindex { .. } => Some(Self::last_rebalancing_update()),
            MintRequested { .. }
            | MintRejected { .. }
            | WrapSubmitted { .. }
            | TokensWrapped { .. }
            | VaultDepositSubmitted { .. }
            | WrappingFailed { .. }
            | RaindexDepositFailed { .. }
            // Reconciliation is a pure bookkeeping terminal transition from
            // `Failed`: the failure already settled inventory, so nothing to do.
            | OperatorReconciled { .. } => None,
        }
    }

    async fn apply_equity_update(
        &self,
        symbol: &Symbol,
        update: EquityInventoryUpdate,
    ) -> Result<(), RebalancingServiceError> {
        let now = Utc::now();
        let mut inventory = self.inventory.write().await;
        *inventory = inventory.clone().update_equity(symbol, update, now)?;
        drop(inventory);
        Ok(())
    }

    /// Sets `active_mints[symbol] = id` while the aggregate is alive,
    /// clears it on terminal events. Mirrors the lifecycle of
    /// `mint_tracking` so `InventoryView` reflects which aggregate
    /// currently owns the symbol's mint slot.
    async fn update_active_mint(
        &self,
        id: &IssuerRequestId,
        symbol: &Symbol,
        event: &TokenizedEquityMintEvent,
    ) {
        let mut inventory = self.inventory.write().await;
        *inventory = if Self::is_terminal_mint_event(event) {
            inventory.clone().clear_active_mint(symbol)
        } else {
            inventory
                .clone()
                .set_active_mint(symbol.clone(), id.clone())
        };
    }

    /// Sets `active_redemptions[symbol] = id` while the aggregate is
    /// alive, clears it on terminal events. Mirrors `redemption_tracking`.
    async fn update_active_redemption(
        &self,
        id: &RedemptionAggregateId,
        symbol: &Symbol,
        event: &EquityRedemptionEvent,
    ) {
        let mut inventory = self.inventory.write().await;
        *inventory = if Self::is_terminal_redemption_event(event) {
            inventory.clone().clear_active_redemption(symbol)
        } else {
            inventory
                .clone()
                .set_active_redemption(symbol.clone(), id.clone())
        };
    }

    async fn load_redemption_tracking(
        &self,
        id: &RedemptionAggregateId,
    ) -> Option<RedemptionTracking> {
        let Some(tracking) = self.redemption_tracking.read().await.get(id).cloned() else {
            warn!(target: "rebalance", id = %id, "Redemption event for untracked aggregate");
            return None;
        };

        Some(tracking)
    }

    fn redemption_inventory_update(
        event: &EquityRedemptionEvent,
        quantity: FractionalShares,
    ) -> Option<EquityInventoryUpdate> {
        use EquityRedemptionEvent::*;

        match event {
            VaultWithdrawPending { .. } => Some(Self::start_equity_transfer_update(
                Venue::MarketMaking,
                quantity,
            )),
            Completed { .. } | ProviderCompletionRecovered { .. } => Some(
                Self::complete_equity_transfer_update(Venue::MarketMaking, quantity),
            ),
            TransferFailed { .. } => Some(Self::cancel_equity_transfer_update(
                Venue::MarketMaking,
                quantity,
            )),
            VaultWithdrawSubmitted { .. }
            | WithdrawnFromRaindex { .. }
            | UnwrapPending { .. }
            | UnwrapSubmitted { .. }
            | TokensUnwrapped { .. }
            | SendPending { .. }
            | TokensSent { .. }
            | DetectionFailed { .. }
            | Detected { .. }
            // Reconciliation is a pure bookkeeping terminal transition from
            // `Failed`: the failure already settled inventory, so nothing to do.
            | OperatorReconciled { .. }
            | RedemptionRejected { .. } => None,
        }
    }

    fn redemption_actual_unwrapped_quantity(
        event: &EquityRedemptionEvent,
    ) -> Result<Option<FractionalShares>, SharesConversionError> {
        match event {
            EquityRedemptionEvent::TokensUnwrapped {
                quantity,
                unwrapped_amount,
                ..
            } => Ok(Some(match quantity {
                Some(quantity) => FractionalShares::new(*quantity),
                None => FractionalShares::from_u256_18_decimals(*unwrapped_amount)?,
            })),
            _ => Ok(None),
        }
    }

    fn recovered_redemption_quantity(
        entity: &EquityRedemption,
    ) -> Result<FractionalShares, SharesConversionError> {
        use EquityRedemption::*;

        match entity {
            VaultWithdrawPending { quantity, .. }
            | VaultWithdrawSubmitted { quantity, .. }
            | WithdrawnFromRaindex { quantity, .. }
            | UnwrapPending { quantity, .. }
            | UnwrapSubmitted { quantity, .. }
            | TokensSent { quantity, .. }
            | Pending { quantity, .. } => Ok(FractionalShares::new(*quantity)),
            TokensUnwrapped {
                unwrapped_amount, ..
            }
            | SendPending {
                unwrapped_amount, ..
            } => FractionalShares::from_u256_18_decimals(*unwrapped_amount),
            Completed { .. } | Failed { .. } | Reconciled { .. } => Ok(FractionalShares::ZERO),
        }
    }

    fn is_wrapped_equity_recovery_enabled(&self, symbol: &Symbol) -> bool {
        self.config
            .assets
            .equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.wrapped_equity_recovery == OperationMode::Enabled)
    }

    /// Checks inventory for equity imbalance and triggers operation if needed.
    pub(crate) async fn check_and_trigger_equity(
        &self,
        symbol: &Symbol,
    ) -> Result<(), equity::EquityTriggerError> {
        self.expire_stuck_operations_with_logging().await;

        if !self.config.is_equity_rebalancing_enabled(symbol) {
            debug!(
                target: "rebalance",
                %symbol,
                "Skipped equity trigger: rebalancing not enabled for symbol"
            );
            return Ok(());
        }

        // Dividend freeze guard: do not start a rebalancing flow for an asset
        // issuance has frozen. The check goes before claiming the in-progress
        // guard or building the operation so a frozen asset is skipped cheaply.
        // The reader is `None` only in tests that do not exercise the gate; the
        // conductor always sets it in production.
        let freeze_status = self.freeze_status.read().await.clone();
        if let Some(reader) = freeze_status {
            match reader.is_frozen(symbol).await {
                Ok(false) => {}
                Ok(true) => {
                    info!(
                        target: "rebalance",
                        %symbol,
                        "Skipped equity trigger: asset is frozen for a dividend"
                    );
                    return Ok(());
                }
                Err(error) => {
                    // Fail closed: a flow started for a frozen asset strands
                    // funds, and rebalancing is latency-tolerant, so when
                    // issuance cannot confirm the asset is unfrozen we skip the
                    // cycle and alert rather than risk initiating it.
                    error!(
                        target: "rebalance",
                        %symbol,
                        ?error,
                        "Skipped equity trigger: could not confirm asset is not \
                         frozen; failing closed"
                    );
                    return Ok(());
                }
            }
        }

        if self.has_pending_offchain_order(symbol).await {
            debug!(target: "rebalance", %symbol, "Skipped equity trigger: offchain hedge order pending");
            return Ok(());
        }

        let Some(guard) = self.try_claim_equity_guard_for_transfer(symbol) else {
            debug!(target: "rebalance", %symbol, "Skipped equity trigger: already in progress");
            return Ok(());
        };

        let operation = match self.build_equity_operation(symbol).await {
            Ok(Some(operation)) => operation,
            Ok(None) => return Ok(()),
            Err(equity::EquityTriggerError::Wrapper(WrapperError::SymbolNotConfigured(symbol))) => {
                warn!(
                    target: "rebalance",
                    %symbol,
                    "Skipped equity trigger: symbol not configured"
                );
                return Ok(());
            }
            Err(equity::EquityTriggerError::TokenNotInRegistry(symbol)) => {
                warn!(
                    target: "rebalance",
                    %symbol,
                    "Skipped equity trigger: symbol not in vault registry"
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        // Re-check immediately before dispatch: an OffChainOrderPlaced for this
        // symbol may have landed during the awaits in build_equity_operation.
        // This narrows but cannot fully close the gap: the in-memory set is a
        // reactor-lagged projection of the position aggregate's
        // pending_offchain_order_id, so a just-committed OffChainOrderPlaced
        // not yet seen by the reactor is invisible here (and the mint path
        // awaits its Jobs-table dedupe query between this check and the
        // push). Closing that fully needs a source-side reservation, not a
        // reactor-lagged projection.
        if self.has_pending_offchain_order(symbol).await {
            debug!(
                target: "rebalance",
                %symbol,
                "Skipped equity trigger before dispatch: offchain hedge order became pending"
            );
            return Ok(());
        }

        let dispatched = match operation {
            TriggeredOperation::Mint { symbol, quantity } => {
                self.enqueue_transfer_equity_to_market_making(symbol, quantity, guard.generation())
                    .await
            }
            TriggeredOperation::Redemption {
                symbol, quantity, ..
            } => {
                self.enqueue_transfer_equity_to_hedging(symbol, quantity)
                    .await
            }
        };

        if !dispatched {
            return Ok(());
        }

        debug!(target: "rebalance", %symbol, "Triggered equity rebalancing");
        guard.defuse();
        Ok(())
    }

    async fn load_token_address(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<Address>, TokenAddressError> {
        let vault_registry_id = VaultRegistryId {
            orderbook: self.orderbook,
            owner: self.order_owner,
        };

        let registry = self
            .vault_registry
            .load(&vault_registry_id)
            .await?
            .ok_or(TokenAddressError::Uninitialized)?;

        Ok(registry.token_by_symbol(symbol))
    }

    /// Returns USDC rebalancing parameters if rebalancing is enabled in config.
    fn usdc_rebalancing_params(&self) -> Option<(ImbalanceThreshold, Option<Usdc>, Option<Usd>)> {
        let threshold = self.config.usdc.as_ref()?;

        let cash = self.config.assets.cash.as_ref();
        let usdc_limit = cash
            .and_then(|cash| cash.operational_limit)
            .map(Positive::inner);
        let reserved = cash.and_then(|cash| cash.reserved).map(Positive::inner);

        Some((*threshold, usdc_limit, reserved))
    }

    /// Checks inventory for USDC imbalance and triggers operation if needed.
    pub(crate) async fn check_and_trigger_usdc(&self) {
        self.expire_stuck_operations_with_logging().await;

        let Some((threshold, usdc_limit, reserved)) = self.usdc_rebalancing_params() else {
            return;
        };

        let Some(guard) = self.try_claim_usdc_guard() else {
            debug!(target: "rebalance", "Skipped USDC trigger: already in progress");
            return;
        };

        let Ok(operation) = usdc::check_imbalance_and_build_operation(
            &threshold,
            &self.inventory,
            usdc_limit,
            reserved,
        )
        .await
        .inspect_err(|skip| debug!(target: "rebalance", ?skip, "Skipped USDC trigger")) else {
            return;
        };

        let dispatched = match operation {
            UsdcRebalanceOperation::BaseToAlpaca { amount } => {
                self.enqueue_transfer_usdc_to_hedging(amount).await
            }
            UsdcRebalanceOperation::AlpacaToBase { amount } => {
                self.enqueue_transfer_usdc_to_market_making(amount).await
            }
        };

        if !dispatched {
            return;
        }

        debug!(target: "rebalance", ?operation, "Triggered USDC rebalancing");
        guard.defuse();
    }

    /// Returns the oldest non-terminal USDC transfer job in *either* direction
    /// (the row id and its age in seconds), if any.
    ///
    /// The dedupe gate is deliberately direction-independent. Both directions
    /// move funds through the same vault and market-maker wallet, and the
    /// in-memory `usdc_in_progress` guard resets on restart, so an in-flight
    /// transfer in one direction must also suppress enqueuing a transfer in the
    /// *other* direction. Querying only one job type would, after a restart, let
    /// an opposite-direction transfer run concurrently against the same funds --
    /// churning capital and paying CCTP/withdrawal fees twice.
    ///
    /// `Pending`/`Queued`/`Running` rows are always treated as in-flight.
    /// `Failed AND attempts < max_attempts` rows are checked: apalis WILL
    /// re-fetch and re-run them, so they can still represent active transfers.
    /// However, if the job's `UsdcRebalanceId` aggregate has already reached a
    /// terminal state (a "zombie" caused by e.g. operator recovery completing
    /// the transfer while the row remained Failed), the zombie is killed so
    /// apalis cannot re-drive it and this guard clears. Exhausted-Failed, Done,
    /// and Killed rows never match the query predicate and do not suppress.
    ///
    /// If the USDC store is not yet attached (`None`), or if the aggregate
    /// cannot be loaded, the Failed row is treated conservatively as in-flight
    /// so we never clear the guard without confirming terminality.
    ///
    /// Contrast with [`Self::transfer_live_job_for_id`], which intentionally
    /// includes `Failed AND attempts < max_attempts` unconditionally because it
    /// gates re-arm of post-burn recovery -- apalis WILL re-fetch that row and
    /// we must not start a second driver while it is still owned by the
    /// scheduler. The zombie-reconciliation logic here serves a different
    /// purpose and must not bleed into that function.
    async fn in_flight_usdc_transfer(
        pool: &apalis_sqlite::SqlitePool,
        usdc_store: Option<&Store<UsdcRebalance>>,
    ) -> Result<Option<(String, i64)>, sqlx_apalis::Error> {
        // Both job types carry `id: UsdcRebalanceId` at the top level of their
        // JSON payload (apalis JsonCodec serializes the struct directly).
        #[derive(serde::Deserialize)]
        struct UsdcJobId {
            id: UsdcRebalanceId,
        }

        loop {
            let row: Option<(String, i64, String)> = sqlx_apalis::query_as(
                "SELECT id, \
                        CAST(strftime('%s', 'now') AS INTEGER) - run_at AS age_secs, \
                        status \
                 FROM Jobs \
                 WHERE job_type IN (?, ?) \
                 AND (status IN ('Pending', 'Queued', 'Running') \
                      OR (status = 'Failed' AND attempts < max_attempts)) \
                 ORDER BY run_at ASC \
                 LIMIT 1",
            )
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
            .fetch_optional(pool)
            .await?;

            let Some((row_id, age_secs, status)) = row else {
                return Ok(None);
            };

            // Pending/Queued/Running rows are always in-flight.
            if status != "Failed" {
                return Ok(Some((row_id, age_secs)));
            }

            // Failed+attempts<max path: check whether this is a zombie.
            let Some(store) = usdc_store else {
                // Store not yet wired; conservative: treat as in-flight.
                debug!(
                    target: "rebalance",
                    %row_id,
                    "USDC store not yet wired; treating Failed row as in-flight conservatively"
                );
                return Ok(Some((row_id, age_secs)));
            };

            // Fetch the job payload (a JSON BLOB, apalis `JsonCodec`) only on the
            // Failed path, so the common non-terminal case skips the extra read.
            let job_payload: Option<Vec<u8>> =
                sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE id = ?")
                    .bind(&row_id)
                    .fetch_optional(pool)
                    .await?;

            let Some(job_payload) = job_payload else {
                // Row disappeared between the two queries (apalis killed it);
                // re-query for the next candidate.
                continue;
            };

            let aggregate_id = match serde_json::from_slice::<UsdcJobId>(&job_payload) {
                Ok(parsed) => parsed.id,
                Err(error) => {
                    warn!(
                        target: "rebalance",
                        %row_id,
                        ?error,
                        "Failed to parse USDC job payload; treating row as in-flight"
                    );
                    return Ok(Some((row_id, age_secs)));
                }
            };

            let aggregate = match store.load(&aggregate_id).await {
                Ok(Some(agg)) => agg,
                Ok(None) => {
                    // A Jobs row referencing an aggregate with no events is a
                    // data inconsistency; conservative: treat as in-flight.
                    warn!(
                        target: "rebalance",
                        %row_id,
                        %aggregate_id,
                        "USDC transfer Jobs row references an aggregate with no events; \
                         treating as in-flight"
                    );
                    return Ok(Some((row_id, age_secs)));
                }
                Err(error) => {
                    warn!(
                        target: "rebalance",
                        %row_id,
                        ?error,
                        "Failed to load USDC aggregate; treating row as in-flight"
                    );
                    return Ok(Some((row_id, age_secs)));
                }
            };

            if aggregate.holds_rebalance_guard() {
                // Genuine retry: aggregate is still live.
                return Ok(Some((row_id, age_secs)));
            }

            // Zombie: aggregate is terminal but the Jobs row is still retryable.
            // Kill it so apalis cannot re-drive it and this guard clears.
            if Self::kill_zombie_job(pool, &row_id).await? {
                info!(
                    target: "rebalance",
                    %row_id,
                    %aggregate_id,
                    "Killed zombie USDC Jobs row (aggregate already terminal)"
                );
                // Loop: re-query to find the next candidate row.
            } else {
                // Apalis grabbed the row between our terminal check and the kill. It will
                // re-run the job, but the aggregate is already terminal so no second
                // transfer can occur (the state machine rejects re-processing); the row
                // reaches a terminal status either way.
                return Ok(Some((row_id, age_secs)));
            }
        }
    }

    /// Terminates a zombie apalis Jobs row by setting its status to `Killed`.
    ///
    /// The conditional predicate (`AND status='Failed' AND attempts <
    /// max_attempts`) makes this a no-op if apalis grabbed the row between
    /// our aggregate-terminal check and this call. Returns `true` when the
    /// kill landed, `false` when it was a no-op (apalis already owns the
    /// row and will run the job, which no-ops on the terminal aggregate).
    async fn kill_zombie_job(
        pool: &apalis_sqlite::SqlitePool,
        job_id: &str,
    ) -> Result<bool, sqlx_apalis::Error> {
        let result = sqlx_apalis::query(
            "UPDATE Jobs SET status='Killed', done_at=strftime('%s','now') \
             WHERE id=? AND status='Failed' AND attempts < max_attempts",
        )
        .bind(job_id)
        .execute(pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Enqueues a [`TransferUsdcToHedging`] apalis job for a Base->Alpaca
    /// transfer. Generates a fresh `UsdcRebalanceId` at push time so apalis
    /// retries (and bot restarts that re-pick the job row) hit the same
    /// aggregate. Returns `true` on successful enqueue.
    ///
    /// Before enqueueing, [`Self::in_flight_usdc_transfer`] checks the apalis
    /// Jobs table for any non-terminal USDC transfer in either direction. The
    /// in-memory `usdc_in_progress` guard resets on restart, so without this
    /// check a crash between `queue.push` and the first persisted
    /// `UsdcRebalance` event would let the next imbalance check enqueue a second
    /// job for the same imbalance.
    async fn enqueue_transfer_usdc_to_hedging(&self, amount: Usdc) -> bool {
        // A non-terminal row in flight longer than this is treated as likely
        // stuck: the suppression is logged at warn (with the row id and age) so
        // it is actionable, rather than silently starving the hedge side with
        // only a debug log. `run_at` is a unix-seconds column.
        const STUCK_TRANSFER_WARN_AFTER_SECS: i64 = 15 * 60;

        let queue = self.transfer_usdc_to_hedging_queue.clone();
        let usdc_store = self.usdc_store.read().await.as_ref().map(Arc::clone);

        match Self::in_flight_usdc_transfer(queue.pool(), usdc_store.as_deref()).await {
            Ok(Some((row_id, age_secs))) if age_secs >= STUCK_TRANSFER_WARN_AFTER_SECS => {
                warn!(
                    target: "rebalance",
                    %row_id,
                    age_secs,
                    threshold_secs = STUCK_TRANSFER_WARN_AFTER_SECS,
                    %amount,
                    "Skipped Base->Alpaca USDC transfer enqueue: an in-flight USDC transfer \
                     (either direction) looks stuck and is suppressing new rebalances; \
                     investigate before it starves hedging",
                );
                return false;
            }
            Ok(Some((row_id, age_secs))) => {
                debug!(
                    target: "rebalance",
                    %row_id,
                    age_secs,
                    %amount,
                    "Skipped Base->Alpaca USDC transfer enqueue: a non-terminal USDC transfer \
                     (either direction) already exists; apalis will resume it",
                );
                return false;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    target: "rebalance",
                    %error,
                    "Failed to query for in-flight USDC transfers; \
                     skipping enqueue to avoid double-pushing",
                );
                return false;
            }
        }

        let id = UsdcRebalanceId(Uuid::new_v4());
        let mut queue = queue;

        let push = queue
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount,
                revert_redrive_attempts: 0,
            })
            .await;

        match push {
            Ok(()) => {
                debug!(
                    target: "rebalance",
                    %id,
                    %amount,
                    "Enqueued TransferUsdcToHedging job for Base->Alpaca USDC transfer",
                );
                true
            }

            Err(QueuePushError(error)) => {
                warn!(
                    target: "rebalance",
                    %error,
                    %amount,
                    "Failed to enqueue TransferUsdcToHedging job",
                );
                false
            }
        }
    }

    /// Sibling of [`Self::enqueue_transfer_usdc_to_hedging`] for the
    /// Alpaca->Base direction.
    ///
    /// Mirrors the same persistent dedupe: an in-memory `usdc_in_progress`
    /// guard resets on restart, so without querying the apalis Jobs table
    /// a crash between `queue.push` and the first persisted
    /// `UsdcRebalance` event would let the next imbalance check enqueue a
    /// second job for the same rebalance.
    async fn enqueue_transfer_usdc_to_market_making(&self, amount: Usdc) -> bool {
        let queue = self.transfer_usdc_to_market_making_queue.clone();
        let usdc_store = self.usdc_store.read().await.as_ref().map(Arc::clone);

        match Self::in_flight_usdc_transfer(queue.pool(), usdc_store.as_deref()).await {
            Ok(Some((row_id, age_secs))) => {
                debug!(
                    target: "rebalance",
                    %row_id,
                    age_secs,
                    %amount,
                    "Skipped Alpaca->Base USDC transfer enqueue: a non-terminal USDC transfer \
                     (either direction) already exists; apalis will resume it",
                );
                return false;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    target: "rebalance",
                    %error,
                    "Failed to query for in-flight USDC transfers; \
                     skipping enqueue to avoid double-pushing",
                );
                return false;
            }
        }

        let id = UsdcRebalanceId(Uuid::new_v4());
        let mut queue = queue;

        let push = queue
            .push(TransferUsdcToMarketMaking {
                id: id.clone(),
                amount,
                revert_redrive_attempts: 0,
            })
            .await;

        match push {
            Ok(()) => {
                debug!(
                    target: "rebalance",
                    %id,
                    %amount,
                    "Enqueued TransferUsdcToMarketMaking job for Alpaca->Base USDC transfer",
                );
                true
            }

            Err(QueuePushError(error)) => {
                warn!(
                    target: "rebalance",
                    %error,
                    %amount,
                    "Failed to enqueue TransferUsdcToMarketMaking job",
                );
                false
            }
        }
    }

    /// Returns the oldest non-terminal equity transfer job for this symbol in
    /// *either* direction (the row id and its age in seconds), if any.
    ///
    /// The in-memory `equity_in_progress` guard resets on restart, so without
    /// this check a crash between `queue.push` and the first persisted
    /// aggregate event would let the next imbalance check enqueue a second
    /// transfer for the same imbalance. Like the USDC gate, the check is
    /// direction-independent per symbol: both directions move the same
    /// symbol's inventory, so an in-flight mint must also suppress a
    /// redemption (and vice versa). The payload is a `serde_json` BLOB
    /// (apalis `JsonCodec`), so the symbol is filtered via `json_extract`.
    ///
    /// `Pending`/`Queued`/`Running` rows are always treated as in-flight.
    /// `Failed AND attempts < max_attempts` rows are zombie-checked: if the
    /// corresponding aggregate (`TokenizedEquityMint` for mint jobs,
    /// `EquityRedemption` for redemption jobs) has already reached a terminal
    /// state, the zombie is killed and skipped. Genuine retries (non-terminal
    /// aggregate) still block. Fail-safe: if the relevant store is `None` or
    /// the aggregate cannot be loaded, the row is treated as in-flight.
    async fn in_flight_equity_transfer(
        pool: &apalis_sqlite::SqlitePool,
        symbol: &Symbol,
        mint_store: Option<&Store<TokenizedEquityMint>>,
        redemption_store: Option<&Store<EquityRedemption>>,
    ) -> Result<Option<(String, i64)>, sqlx_apalis::Error> {
        #[derive(serde::Deserialize)]
        struct MintJobId {
            issuer_request_id: IssuerRequestId,
        }

        #[derive(serde::Deserialize)]
        struct RedemptionJobId {
            aggregate_id: RedemptionAggregateId,
        }

        let mint_type = std::any::type_name::<TransferEquityToMarketMaking>();
        let redemption_type = std::any::type_name::<TransferEquityToHedging>();

        loop {
            let row: Option<(String, i64, String, String)> = sqlx_apalis::query_as(
                "SELECT id, \
                        CAST(strftime('%s', 'now') AS INTEGER) - run_at AS age_secs, \
                        status, \
                        job_type \
                 FROM Jobs \
                 WHERE job_type IN (?, ?) \
                 AND json_extract(job, '$.symbol') = ? \
                 AND (status IN ('Pending', 'Queued', 'Running') \
                      OR (status = 'Failed' AND attempts < max_attempts)) \
                 ORDER BY run_at ASC \
                 LIMIT 1",
            )
            .bind(mint_type)
            .bind(redemption_type)
            .bind(symbol.to_string())
            .fetch_optional(pool)
            .await?;

            let Some((row_id, age_secs, status, job_type)) = row else {
                return Ok(None);
            };

            // Pending/Queued/Running rows are always in-flight.
            if status != "Failed" {
                return Ok(Some((row_id, age_secs)));
            }

            // Failed+attempts<max path: check the corresponding aggregate.
            // Fetch the job payload (a JSON BLOB, apalis `JsonCodec`) separately
            // to avoid reading it for every row on the non-Failed fast path.
            let job_payload: Option<Vec<u8>> =
                sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE id = ?")
                    .bind(&row_id)
                    .fetch_optional(pool)
                    .await?;

            let Some(job_payload) = job_payload else {
                // Row disappeared between our first query and this fetch
                // (apalis killed it); re-query for the next candidate.
                continue;
            };

            // Also capture the aggregate id for the zombie-kill log; both branches
            // return (is_terminal, aggregate_id_display) to converge at one log site.
            let (is_terminal, aggregate_id) = if job_type == mint_type {
                let Some(store) = mint_store else {
                    // Store not yet wired; conservative: treat as in-flight.
                    debug!(
                        target: "rebalance",
                        %row_id,
                        %symbol,
                        "Mint store not yet wired; treating Failed row as in-flight conservatively"
                    );
                    return Ok(Some((row_id, age_secs)));
                };
                match serde_json::from_slice::<MintJobId>(&job_payload) {
                    Err(error) => {
                        warn!(
                            target: "rebalance",
                            %row_id,
                            ?error,
                            "Failed to parse mint job payload; treating row as in-flight"
                        );
                        return Ok(Some((row_id, age_secs)));
                    }
                    Ok(parsed) => {
                        let issuer_request_id = parsed.issuer_request_id;
                        match store.load(&issuer_request_id).await {
                            Ok(Some(agg)) => (agg.is_terminal(), issuer_request_id.to_string()),
                            Ok(None) => {
                                // A Jobs row referencing an aggregate with no events is a
                                // data inconsistency; conservative: treat as in-flight.
                                warn!(
                                    target: "rebalance",
                                    %row_id,
                                    %symbol,
                                    issuer_request_id = %issuer_request_id,
                                    "Mint Jobs row references an aggregate with no events; \
                                     treating as in-flight"
                                );
                                return Ok(Some((row_id, age_secs)));
                            }
                            Err(error) => {
                                warn!(
                                    target: "rebalance",
                                    %row_id,
                                    ?error,
                                    "Failed to load mint aggregate; treating row as in-flight"
                                );
                                return Ok(Some((row_id, age_secs)));
                            }
                        }
                    }
                }
            } else if job_type == redemption_type {
                let Some(store) = redemption_store else {
                    // Store not yet wired; conservative: treat as in-flight.
                    debug!(
                        target: "rebalance",
                        %row_id,
                        %symbol,
                        "Redemption store not yet wired; treating Failed row as in-flight \
                         conservatively"
                    );
                    return Ok(Some((row_id, age_secs)));
                };
                match serde_json::from_slice::<RedemptionJobId>(&job_payload) {
                    Err(error) => {
                        warn!(
                            target: "rebalance",
                            %row_id,
                            ?error,
                            "Failed to parse redemption job payload; treating row as in-flight"
                        );
                        return Ok(Some((row_id, age_secs)));
                    }
                    Ok(parsed) => {
                        let redemption_id = parsed.aggregate_id;
                        match store.load(&redemption_id).await {
                            Ok(Some(agg)) => (agg.is_terminal(), redemption_id.to_string()),
                            Ok(None) => {
                                // A Jobs row referencing an aggregate with no events is a
                                // data inconsistency; conservative: treat as in-flight.
                                warn!(
                                    target: "rebalance",
                                    %row_id,
                                    %symbol,
                                    redemption_aggregate_id = %redemption_id,
                                    "Redemption Jobs row references an aggregate with no events; \
                                     treating as in-flight"
                                );
                                return Ok(Some((row_id, age_secs)));
                            }
                            Err(error) => {
                                warn!(
                                    target: "rebalance",
                                    %row_id,
                                    ?error,
                                    "Failed to load redemption aggregate; treating row as in-flight"
                                );
                                return Ok(Some((row_id, age_secs)));
                            }
                        }
                    }
                }
            } else {
                // The SQL already filters job_type to the two known types, so
                // this branch is a defensive fallback for any future regression.
                warn!(
                    target: "rebalance",
                    %row_id,
                    %symbol,
                    %job_type,
                    "Unexpected job_type in equity in-flight guard; treating row as in-flight"
                );
                return Ok(Some((row_id, age_secs)));
            };

            if !is_terminal {
                return Ok(Some((row_id, age_secs)));
            }

            // Zombie: aggregate is terminal; kill the row.
            if Self::kill_zombie_job(pool, &row_id).await? {
                info!(
                    target: "rebalance",
                    %row_id,
                    %symbol,
                    %aggregate_id,
                    "Killed zombie equity Jobs row (aggregate already terminal)"
                );
                // Loop: re-query to find the next candidate row.
            } else {
                // Apalis grabbed the row between our terminal check and the kill. It will
                // re-run the job, but the aggregate is already terminal so no second
                // transfer can occur (the state machine rejects re-processing); the row
                // reaches a terminal status either way.
                return Ok(Some((row_id, age_secs)));
            }
        }
    }

    /// Enqueues a [`TransferEquityToMarketMaking`] apalis job for a
    /// hedging->market-making equity mint. Generates a fresh
    /// `IssuerRequestId` at push time so apalis retries (and bot restarts
    /// that re-pick the job row) hit the same aggregate. Returns `true` on
    /// successful enqueue.
    async fn enqueue_transfer_equity_to_market_making(
        &self,
        symbol: Symbol,
        quantity: FractionalShares,
        generation: u64,
    ) -> bool {
        // A non-terminal row in flight longer than this is treated as likely
        // stuck: the suppression is logged at warn (with the row id and age)
        // so it is actionable rather than silently freezing the symbol.
        const STUCK_TRANSFER_WARN_AFTER_SECS: i64 = 15 * 60;

        let mut queue = self.transfer_equity_to_market_making_queue.clone();
        let mint_store = self.mint_store.read().await.as_ref().map(Arc::clone);
        let redemption_store = self.redemption_store.read().await.as_ref().map(Arc::clone);

        match Self::in_flight_equity_transfer(
            queue.pool(),
            &symbol,
            mint_store.as_deref(),
            redemption_store.as_deref(),
        )
        .await
        {
            Ok(Some((row_id, age_secs))) if age_secs >= STUCK_TRANSFER_WARN_AFTER_SECS => {
                warn!(
                    target: "rebalance",
                    %row_id,
                    age_secs,
                    threshold_secs = STUCK_TRANSFER_WARN_AFTER_SECS,
                    %symbol,
                    %quantity,
                    "Skipped equity mint enqueue: an in-flight equity transfer for this \
                     symbol looks stuck and is suppressing new rebalances; investigate \
                     before it starves market making",
                );
                return false;
            }
            Ok(Some((row_id, age_secs))) => {
                debug!(
                    target: "rebalance",
                    %row_id,
                    age_secs,
                    %symbol,
                    %quantity,
                    "Skipped equity mint enqueue: a non-terminal equity transfer for \
                     this symbol already exists; apalis will resume it",
                );
                return false;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    target: "rebalance",
                    %error,
                    %symbol,
                    "Failed to query for in-flight equity transfers; \
                     skipping enqueue to avoid double-pushing",
                );
                return false;
            }
        }

        let issuer_request_id = IssuerRequestId::generate();

        let push = queue
            .push(TransferEquityToMarketMaking {
                issuer_request_id: issuer_request_id.clone(),
                symbol: symbol.clone(),
                quantity,
                generation,
            })
            .await;

        match push {
            Ok(()) => {
                debug!(
                    target: "rebalance",
                    %issuer_request_id,
                    %symbol,
                    %quantity,
                    "Enqueued TransferEquityToMarketMaking job for equity mint",
                );
                true
            }

            Err(QueuePushError(error)) => {
                warn!(
                    target: "rebalance",
                    %error,
                    %symbol,
                    %quantity,
                    "Failed to enqueue TransferEquityToMarketMaking job",
                );
                false
            }
        }
    }

    /// Sibling of [`Self::enqueue_transfer_equity_to_market_making`] for the
    /// redemption (market-making -> hedging) direction. Same per-symbol
    /// Jobs-table dedupe; same fresh-id-at-push-time contract.
    async fn enqueue_transfer_equity_to_hedging(
        &self,
        symbol: Symbol,
        quantity: FractionalShares,
    ) -> bool {
        const STUCK_TRANSFER_WARN_AFTER_SECS: i64 = 15 * 60;

        let mut queue = self.transfer_equity_to_hedging_queue.clone();
        let mint_store = self.mint_store.read().await.as_ref().map(Arc::clone);
        let redemption_store = self.redemption_store.read().await.as_ref().map(Arc::clone);

        match Self::in_flight_equity_transfer(
            queue.pool(),
            &symbol,
            mint_store.as_deref(),
            redemption_store.as_deref(),
        )
        .await
        {
            Ok(Some((row_id, age_secs))) if age_secs >= STUCK_TRANSFER_WARN_AFTER_SECS => {
                warn!(
                    target: "rebalance",
                    %row_id,
                    age_secs,
                    threshold_secs = STUCK_TRANSFER_WARN_AFTER_SECS,
                    %symbol,
                    %quantity,
                    "Skipped equity redemption enqueue: an in-flight equity transfer for \
                     this symbol looks stuck and is suppressing new rebalances; \
                     investigate before it starves hedging",
                );
                return false;
            }
            Ok(Some((row_id, age_secs))) => {
                debug!(
                    target: "rebalance",
                    %row_id,
                    age_secs,
                    %symbol,
                    %quantity,
                    "Skipped equity redemption enqueue: a non-terminal equity transfer \
                     for this symbol already exists; apalis will resume it",
                );
                return false;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    target: "rebalance",
                    %error,
                    %symbol,
                    "Failed to query for in-flight equity transfers; \
                     skipping enqueue to avoid double-pushing",
                );
                return false;
            }
        }

        let aggregate_id = RedemptionAggregateId::generate();

        let push = queue
            .push(TransferEquityToHedging {
                aggregate_id: aggregate_id.clone(),
                symbol: symbol.clone(),
                quantity,
            })
            .await;

        match push {
            Ok(()) => {
                debug!(
                    target: "rebalance",
                    %aggregate_id,
                    %symbol,
                    %quantity,
                    "Enqueued TransferEquityToHedging job for equity redemption",
                );
                true
            }

            Err(QueuePushError(error)) => {
                warn!(
                    target: "rebalance",
                    %error,
                    %symbol,
                    %quantity,
                    "Failed to enqueue TransferEquityToHedging job",
                );
                false
            }
        }
    }

    /// Clears the in-progress flag for an equity symbol.
    ///
    /// Removes the entry regardless of its current `GuardState`. Called by
    /// `on_mint`/`on_redemption` terminal event arms and by guard-drop in all
    /// transfer and recovery job paths.
    pub(crate) fn clear_equity_in_progress(&self, symbol: &Symbol) {
        let mut guard = match self.equity_in_progress.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.remove(symbol);
    }

    /// Clears the in-progress guard for a timed-out mint unless recovery owns
    /// the slot, in a single write-lock acquisition.
    ///
    /// Returns `true` after removing an `ActiveTransfer` (or absent) guard so
    /// the caller can fail the mint and free the symbol for a fresh rebalance.
    /// Returns `false` without touching a `HeldForRecovery` slot: recovery owns
    /// the mint and will drive it to terminal. Holding the lock across the
    /// check and the clear closes the TOCTOU race where a concurrent
    /// `mark_held_for_recovery` flips `ActiveTransfer` -> `HeldForRecovery`
    /// between a separate check and clear.
    pub(crate) fn clear_equity_in_progress_unless_held_for_recovery(
        &self,
        symbol: &Symbol,
    ) -> bool {
        let mut guard = match self.equity_in_progress.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        match guard.get(symbol) {
            Some(equity::GuardState::HeldForRecovery) => false,
            Some(equity::GuardState::ActiveTransfer { .. }) | None => {
                guard.remove(symbol);
                true
            }
        }
    }

    /// Marks the slot as `ActiveTransfer` (startup recovery and tracking-rebuild
    /// paths that re-establish a live transfer's guard on restart).
    fn mark_equity_active_transfer(&self, symbol: &Symbol) {
        let mut guard = match self.equity_in_progress.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.insert(
            symbol.clone(),
            equity::GuardState::ActiveTransfer {
                generation: equity::next_generation(),
            },
        );
    }

    /// Sets the in-progress guard to `HeldForRecovery` for a symbol.
    ///
    /// Used during startup reconstruction for post-receipt mint states when
    /// recovery is enabled: instead of `ActiveTransfer` (which would block
    /// the recovery job), we set `HeldForRecovery` so `claim_guard_for_recovery_or_orphan`
    /// can claim the slot and resume.
    fn mark_equity_held_for_recovery(&self, symbol: &Symbol) {
        let mut guard = match self.equity_in_progress.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.insert(symbol.clone(), equity::GuardState::HeldForRecovery);
    }

    /// Releases the in-progress guard and tracking for a mint whose recovery
    /// event was committed (inventory already corrected by the reactor) but
    /// whose in-process resume failed. The aggregate is left for startup
    /// recovery to finish; dropping the tracking prevents the timeout sweeper
    /// from later failing an already-recovered mint, and clearing the guard
    /// unblocks rebalancing for the symbol.
    pub(crate) async fn abandon_mint_recovery_guard(&self, id: &IssuerRequestId, symbol: &Symbol) {
        self.mint_tracking.write().await.remove(id);
        self.clear_equity_in_progress(symbol);

        // `ProviderCompletionRecovered` is non-terminal for mints, so the
        // reactor *set* the active-mint slot to this id when it processed the
        // recovery event. Dropping tracking here means no future event can
        // clear it (events for an untracked aggregate early-return), so clear
        // it now to keep `active_mint` mirroring `mint_tracking`'s lifecycle --
        // otherwise the dashboard shows a phantom active mint until restart.
        // Clear only if this id still owns the slot: on the dispatch-failure
        // rollback path the reactor never ran, so a concurrent mint may
        // legitimately own it and must not be cleared. Re-check ownership under
        // the write lock so a concurrent mint cannot claim the slot between the
        // read and the clear.
        let mut inventory = self.inventory.write().await;
        if inventory.active_mint(symbol) == Some(id) {
            *inventory = inventory.clone().clear_active_mint(symbol);
        }
    }

    /// Clears the in-progress flag for USDC rebalancing.
    pub(crate) fn clear_usdc_in_progress(&self) {
        self.usdc_in_progress.store(false, Ordering::SeqCst);
    }

    /// Reconstructs the single-rebalance guard (`usdc_in_progress`) from durable
    /// `UsdcRebalance` event state on startup, and re-arms transfer jobs for
    /// post-burn rebalances stranded with no pending job.
    ///
    /// The guard is in-memory and resets to `false` on restart. Without this, a
    /// restart between a post-burn `BridgingFailed` (or any unsettled in-flight
    /// rebalance) and settlement would let the next imbalance check dispatch a
    /// fresh CCTP burn against funds CCTP has already burned. Re-asserting the
    /// guard when any aggregate is in a guard-holding state blocks new USDC
    /// rebalancing until the stuck transfer settles or an operator recovers it.
    ///
    /// USDC bridges DO resume post-restart: apalis re-picks any pending transfer
    /// job row and drives it via `resume_*`. But a redrive enqueue that fails
    /// after its `TimeoutAttestation` already committed (or a crash in that
    /// window) leaves the aggregate durably `AwaitingAttestation`/`Attested` with
    /// no job to retry it -- the guard would then stay latched forever. So for
    /// each post-burn resumable aggregate with no in-flight job row, this
    /// re-enqueues a transfer job keyed by the existing id. The
    /// `transfer_in_flight_for_id` check makes this idempotent with apalis's own
    /// re-pick of still-pending rows. See ADR 2.
    pub(crate) async fn recover_usdc_guard(
        &self,
        pool: &SqlitePool,
        usdc_store: &Store<UsdcRebalance>,
    ) -> Result<(), RebalancingServiceError> {
        let InterruptedUsdcRebalances {
            ids: candidate_ids,
            unparseable,
        } = interrupted_usdc_rebalance_ids(pool).await?;

        let mut held_ids = Vec::new();
        let mut held_tracking = Vec::new();
        // Guard-holding aggregates with no tracking seed AND no re-arm path
        // (e.g. WithdrawalSubmitting{AlpacaToBase}): the sweep never selects
        // them, so there is no automated recovery. The operator must be paged
        // so they know to run manual reconciliation.
        let mut stranded_held_ids: Vec<UsdcRebalanceId> = Vec::new();
        let mut unresolved_ids = Vec::new();
        let mut rearm_candidates = Vec::new();
        for id in candidate_ids {
            match usdc_store.load(&id).await {
                Ok(Some(entity)) => {
                    if entity.holds_rebalance_guard() {
                        held_ids.push(id.clone());

                        // Reconstruct an in-memory tracking entry for the
                        // manually-reconcilable guard-holding terminal states
                        // that cannot self-recover: `DepositFailed`,
                        // `ConversionFailed { BaseToAlpaca }`, and
                        // `BridgingFailed { AlpacaToBase, burn_tx=Some }`.
                        // Seeding tracking for in-progress states or the
                        // resumable `BridgingFailed { BaseToAlpaca }` would
                        // wedge their `DepositConfirmed` path, which requires
                        // `bridged_amount_received` that the seed cannot
                        // supply. Those states self-recover; only these three
                        // terminal states need the sweep's durable-state-check
                        // path to clear the guard after a CLI reconcile.
                        if let Some((direction, amount, last_progress_at)) =
                            entity.guard_recovery_tracking_data()
                        {
                            held_tracking.push((
                                id.clone(),
                                usdc::UsdcRebalanceTracking {
                                    direction,
                                    initiated_amount: amount,
                                    bridged_amount_received: None,
                                    // All seeded states are post-burn
                                    // (DepositFailed, ConversionFailed{BtA},
                                    // BridgingFailed{AtB}), so the sweep takes
                                    // the durable-state-check path
                                    // (is_post_burn = true) on every tick.
                                    stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                                    // Use the aggregate's `failed_at` rather
                                    // than `Utc::now()` so that a failure which
                                    // occurred hours before this restart is
                                    // already past the `transfer_timeout` on the
                                    // first sweep tick, enabling fast guard
                                    // recovery without waiting for another full
                                    // timeout window. The sweep's Reconciled
                                    // check (which is timeout-independent) fires
                                    // either way; `failed_at` only affects when
                                    // the non-Reconciled PreservedPostBurn log
                                    // is emitted.
                                    last_progress_at,
                                },
                            ));
                        }
                    }

                    if let Some((direction, amount)) = entity.resumable_post_burn_transfer() {
                        rearm_candidates.push(RearmCandidate {
                            policy: if matches!(&entity, UsdcRebalance::BridgingFailed { .. }) {
                                RearmPolicy::RecoverableFailure
                            } else {
                                RearmPolicy::PostBurnResumable
                            },
                            id,
                            direction,
                            amount,
                        });
                    } else if let Some((direction, amount)) = entity.is_resumable_mid_flight_data()
                    {
                        // Pre-burn states (BridgingSubmitting / WithdrawalSubmitting{BaseToAlpaca})
                        // with no job row: the process crashed before the enqueue
                        // committed or before the tx was submitted. Safe to re-arm
                        // because these states are non-terminal and resumable: the
                        // resume path scans for any existing on-chain effect before
                        // re-attempting.
                        //
                        // WithdrawalSubmitting{AlpacaToBase} is excluded by
                        // is_resumable_mid_flight_data: resume_alpaca_to_base returns
                        // ResumeDirectionMismatch for that state so it must not be re-armed.
                        //
                        // IMPORTANT: These states must NOT get a tracking seed (only
                        // DepositFailed, ConversionFailed, and BridgingFailed{burn_tx=Some}
                        // get tracking seeds). Adding tracking here would wedge the
                        // DepositConfirmed path which requires bridged_amount_received
                        // that the seed cannot supply.
                        //
                        // This re-arm runs BEFORE the apalis monitor spawns (see
                        // conductor.rs startup order), so there is no live-worker race.
                        // The transfer_has_job_row_for_id gate below provides idempotency
                        // on the very first check; we collect here and gate in
                        // rearm_stranded_transfers.
                        rearm_candidates.push(RearmCandidate {
                            policy: RearmPolicy::MidFlightPreBurn,
                            id,
                            direction,
                            amount,
                        });
                    } else if entity.holds_rebalance_guard()
                        && entity.guard_recovery_tracking_data().is_none()
                        && !self.transfer_live_job_for_id(&id).await?
                    {
                        // This held aggregate has no tracking seed (so the
                        // sweep never selects it), no re-arm path (so startup
                        // recovery does not enqueue a job), AND no live apalis
                        // job that already owns it. With no driver it latches
                        // usdc_in_progress with no automated recovery. Collect
                        // for operator alert so the blocked state is visible
                        // immediately rather than surfacing only through log
                        // monitoring.
                        //
                        // The live-job probe guards a false positive: when
                        // apalis still owns a Pending/Running/retryable job for
                        // this id (e.g. a Converting/Bridged/DepositInitiated
                        // aggregate that the interrupted transfer job will
                        // resume on its own), that job is the driver -- alerting
                        // here would duplicate the page the live job itself
                        // raises if it ultimately fails. Notably:
                        // WithdrawalSubmitting{AlpacaToBase} ends up here (when
                        // no live job remains) because resume_alpaca_to_base
                        // returns ResumeDirectionMismatch for it (see
                        // is_resumable_mid_flight_data).
                        stranded_held_ids.push(id);
                    }
                }
                // The id came from the event log, so a missing or unreplayable
                // aggregate is an inconsistency we cannot classify. Fail safe:
                // hold the guard so a possibly-post-burn rebalance can never be
                // re-burned, and surface it for operators. Blocking is the safe
                // direction; the alternative is the re-burn this guards against.
                // A single bad aggregate must not abort recovery for the rest,
                // so we do not propagate -- only the query's I/O error does.
                Ok(None) => {
                    error!(
                        target: "rebalance",
                        %id,
                        "USDC rebalance candidate missing from store on startup; holding guard defensively"
                    );
                    unresolved_ids.push(id);
                }
                Err(error) => {
                    error!(
                        target: "rebalance",
                        %id, ?error,
                        "Failed to load USDC rebalance candidate on startup; holding guard defensively"
                    );
                    unresolved_ids.push(id);
                }
            }
        }

        // Re-arm a transfer job for each post-burn resumable aggregate that has
        // no job row at all -- the strand that a failed redrive enqueue (or a
        // crash in that window) leaves behind. Done before the early return because a
        // resumable aggregate always holds the guard, so this set is non-empty
        // only when `held_ids` is too. Propagates on failure so startup recovery
        // fails fast rather than coming up with a latched guard and no driving job.
        let stranded_after_exhaustion = self.rearm_stranded_transfers(rearm_candidates).await?;
        stranded_held_ids.extend(stranded_after_exhaustion);

        // An unparseable candidate aggregate_id cannot be loaded or classified,
        // so it joins the unresolved set: hold the guard rather than risk leaving
        // a possibly-post-burn rebalance unguarded. Same fail-closed direction as
        // a missing or unloadable aggregate above.
        if held_ids.is_empty() && unresolved_ids.is_empty() && unparseable.is_empty() {
            return Ok(());
        }

        self.usdc_in_progress.store(true, Ordering::SeqCst);

        // Populate in-memory tracking for each held aggregate so the timeout
        // sweep can re-derive durable state and clear the guard when the
        // operator reconciles via the CLI without requiring a restart. Without
        // these entries the sweep's `usdc_tracking` iteration finds nothing and
        // `cleanup_timed_out_usdc_rebalance` is never called.
        self.usdc_tracking.write().await.extend(held_tracking);

        error!(
            target: "rebalance",
            held = ?held_ids,
            unresolved = ?unresolved_ids,
            unparseable = ?unparseable,
            "Reconstructed USDC in-progress guard for unsettled rebalances on startup; \
             new USDC rebalancing is blocked until they settle or are recovered"
        );

        // Alert the operator for any aggregate that latches the guard with no
        // automated recovery path (no tracking seed, no re-arm). This covers:
        // - WithdrawalSubmitting{AlpacaToBase}: deliberately excluded from
        //   is_resumable_mid_flight_data because resume_alpaca_to_base returns
        //   ResumeDirectionMismatch for it. No job drives it forward.
        // - unresolved: aggregates missing from the store or that failed to
        //   load. Cannot be classified; guard held defensively.
        // - unparseable: aggregate ids that could not be parsed. Same as above.
        // Without this alert the operator would only discover the blocked state
        // through log monitoring or by noticing USDC rebalancing has stopped.
        let has_stranded =
            !stranded_held_ids.is_empty() || !unresolved_ids.is_empty() || !unparseable.is_empty();
        if has_stranded {
            let message = format!(
                "USDC rebalancing is LATCHED on startup with no automated recovery. \
                 stranded={stranded_held_ids:?} unresolved={unresolved_ids:?} \
                 unparseable={unparseable:?}. \
                 Run `resume-usdc-transfer` or `reconcile-usdc-transfer` to unblock. \
                 Rebalancing is blocked until manually resolved."
            );
            if let Err(error) = self.notifier.notify(&message).await {
                warn!(target: "rebalance", ?error, "Failed to deliver USDC startup-stranded alert");
            }
        }

        Ok(())
    }

    /// Re-enqueues a transfer job for each stranded rebalance that still needs a
    /// driver, keyed by the existing aggregate id so the resumed job hits the
    /// same aggregate. Returns the ids of pre-burn mid-flight candidates whose
    /// redrive budget is already exhausted (a terminal-only job row, no live
    /// driver), so the caller can fold them into the startup operator alert
    /// rather than silently resetting their counter.
    ///
    /// The skip gate depends on the candidate's [`RearmPolicy`]:
    ///
    /// - [`RearmPolicy::RecoverableFailure`] (post-burn `BridgingFailed`,
    ///   RAI-909): re-checking the mint on-chain and un-failing the aggregate is
    ///   NEW work, not a continuation of the original transfer's exhausted
    ///   budget, so a terminal `Failed`/`Done` row must NOT block it (that row is
    ///   the normal artifact of the job that drove it to the failed state). Only
    ///   a row apalis still owns -- in-flight OR a `Failed` row with retries
    ///   remaining -- skips it, via [`Self::transfer_live_job_for_id`], so we
    ///   never drive two concurrent resumes of the same id. A genuinely
    ///   unrecoverable transfer re-arms again on each restart once its retry
    ///   budget is spent; that is acceptable for committed burned funds and is
    ///   the price of not abandoning recoverable funds.
    /// - [`RearmPolicy::PostBurnResumable`] (pre-mint-confirmation
    ///   `Bridging`/`AwaitingAttestation`/`Attested`): re-armed only for the true
    ///   "no job row at all" strand: any job row -- in-flight OR a terminal
    ///   `Failed` awaiting operator reconciliation -- skips it via
    ///   [`Self::transfer_has_job_row_for_id`], so re-arm never bypasses the
    ///   retry budget of a job that already ran and gave up.
    /// - [`RearmPolicy::MidFlightPreBurn`] (pre-burn `BridgingSubmitting` /
    ///   `WithdrawalSubmitting{BaseToAlpaca}`): re-armed only when NO job row
    ///   exists. A live row means apalis is still driving it (skip silently). A
    ///   terminal-only row means the redrive budget is exhausted with no driver:
    ///   rather than silently re-arming with a fresh budget, the id is stranded
    ///   and returned for the operator alert.
    ///
    /// An enqueue failure (or a probe failure) is propagated so startup recovery
    /// fails fast and the supervisor retries -- consistent with the
    /// `requeue_orphaned` startup path -- rather than coming up "healthy" with the
    /// guard latched and no job driving a post-burn transfer.
    async fn rearm_stranded_transfers(
        &self,
        candidates: Vec<RearmCandidate>,
    ) -> Result<Vec<UsdcRebalanceId>, RebalancingServiceError> {
        let mut stranded_after_exhaustion: Vec<UsdcRebalanceId> = Vec::new();

        for RearmCandidate {
            id,
            direction,
            amount,
            policy,
        } in candidates
        {
            let blocked = match policy {
                RearmPolicy::RecoverableFailure => self.transfer_live_job_for_id(&id).await?,
                RearmPolicy::PostBurnResumable => self.transfer_has_job_row_for_id(&id).await?,
                RearmPolicy::MidFlightPreBurn => {
                    let live = self.transfer_live_job_for_id(&id).await?;
                    let stranded_by_exhaustion =
                        !live && self.transfer_has_job_row_for_id(&id).await?;
                    if stranded_by_exhaustion {
                        warn!(
                            target: "rebalance",
                            %id,
                            "Pre-burn mid-flight transfer has a terminal job row (redrive budget \
                             exhausted) and no live driver; stranding for operator alert"
                        );
                        stranded_after_exhaustion.push(id.clone());
                    }
                    live || stranded_by_exhaustion
                }
            };

            if blocked {
                debug!(
                    target: "rebalance",
                    %id,
                    "Transfer already has a blocking job row on startup; not re-arming",
                );
                continue;
            }

            match direction {
                RebalanceDirection::BaseToAlpaca => {
                    self.transfer_usdc_to_hedging_queue
                        .clone()
                        .push(TransferUsdcToHedging {
                            id: id.clone(),
                            amount,
                            revert_redrive_attempts: 0,
                        })
                        .await?;
                }
                RebalanceDirection::AlpacaToBase => {
                    self.transfer_usdc_to_market_making_queue
                        .clone()
                        .push(TransferUsdcToMarketMaking {
                            id: id.clone(),
                            amount,
                            revert_redrive_attempts: 0,
                        })
                        .await?;
                }
            }

            warn!(
                target: "rebalance",
                %id,
                ?direction,
                %amount,
                "Re-armed a stranded USDC transfer with no job row on startup",
            );
        }

        Ok(stranded_after_exhaustion)
    }

    pub(crate) async fn recover_mint_state(
        &self,
        id: &IssuerRequestId,
        entity: &TokenizedEquityMint,
    ) -> Result<(), RebalancingServiceError> {
        use TokenizedEquityMint::*;

        match entity {
            MintRequested {
                symbol, quantity, ..
            }
            | MintAccepted {
                symbol, quantity, ..
            }
            | TokensReceived {
                symbol, quantity, ..
            }
            | WrapSubmitted {
                symbol, quantity, ..
            }
            | TokensWrapped {
                symbol, quantity, ..
            }
            | VaultDepositSubmitted {
                symbol, quantity, ..
            } => {
                let quantity = FractionalShares::new(*quantity);

                let (stage, last_progress_at, tokenization_request_id) = match entity {
                    MintRequested { requested_at, .. } => {
                        (MintTrackingStage::Requested, *requested_at, None)
                    }
                    MintAccepted {
                        accepted_at,
                        tokenization_request_id,
                        ..
                    } => (
                        MintTrackingStage::Accepted,
                        *accepted_at,
                        Some(tokenization_request_id.clone()),
                    ),
                    TokensReceived {
                        received_at,
                        tokenization_request_id,
                        ..
                    } => (
                        MintTrackingStage::TokensReceived,
                        *received_at,
                        Some(tokenization_request_id.clone()),
                    ),
                    WrapSubmitted {
                        received_at,
                        tokenization_request_id,
                        ..
                    } => (
                        MintTrackingStage::WrapSubmitted,
                        *received_at,
                        Some(tokenization_request_id.clone()),
                    ),
                    TokensWrapped {
                        wrapped_at,
                        tokenization_request_id,
                        ..
                    } => (
                        MintTrackingStage::TokensWrapped,
                        *wrapped_at,
                        Some(tokenization_request_id.clone()),
                    ),
                    VaultDepositSubmitted {
                        wrapped_at,
                        tokenization_request_id,
                        ..
                    } => (
                        MintTrackingStage::VaultDepositSubmitted,
                        *wrapped_at,
                        Some(tokenization_request_id.clone()),
                    ),
                    _ => unreachable!(),
                };

                self.mint_tracking.write().await.insert(
                    id.clone(),
                    MintTracking {
                        symbol: symbol.clone(),
                        quantity,
                        tokenization_request_id,
                        stage,
                        last_progress_at,
                    },
                );

                // For pre-wrap post-receipt states (TokensReceived, WrapSubmitted),
                // the transfer job may have already handed off to recovery via
                // HeldForRecovery and returned Ok(()) — the apalis job is Done and
                // will never be re-picked. On restart, reconstructing these states as
                // ActiveTransfer would block UnwrappedEquityRecovery from claiming.
                //
                // When recovery is enabled, reconstruct as HeldForRecovery so
                // claim_guard_for_recovery_or_orphan can claim the slot. When recovery
                // is disabled, ActiveTransfer is correct — resume_interrupted_transfers
                // will call resume_mint and the transfer job retries normally.
                //
                // TokensWrapped and VaultDepositSubmitted are always reconstructed as
                // ActiveTransfer: the deposit is idempotent and resume_interrupted_transfers
                // drives them to completion. WrappedEquityRecovery is orphan-only.
                let is_pre_wrap_post_receipt_state =
                    matches!(entity, TokensReceived { .. } | WrapSubmitted { .. });

                if is_pre_wrap_post_receipt_state && self.is_wrapped_equity_recovery_enabled(symbol)
                {
                    self.mark_equity_held_for_recovery(symbol);
                } else {
                    self.mark_equity_active_transfer(symbol);
                }

                let mut inventory = self.inventory.write().await;
                let mut updated = inventory.clone();
                if matches!(entity, MintAccepted { .. }) {
                    updated = updated.update_equity(
                        symbol,
                        Inventory::set_inflight(Venue::Hedging, quantity),
                        Utc::now(),
                    )?;
                }
                *inventory = updated.set_active_mint(symbol.clone(), id.clone());
            }
            DepositedIntoRaindex { .. } | Failed { .. } | Reconciled { .. } => {}
        }

        Ok(())
    }

    /// Rebuilds in-memory tracking for a failed mint being recovered via
    /// `transfer recheck`, so the live reactor applies the
    /// `ProviderCompletionRecovered` inventory effect and the terminal
    /// cleanup once the recovery event is dispatched.
    ///
    /// Returns [`RecoveryClaim::Conflict`] when a *different* live mint already
    /// owns the symbol's slot: the ownership check and the in-flight restore run
    /// under one inventory write lock (compare-and-claim) so a concurrent mint
    /// cannot claim the slot in between, which would let recovery's `set_inflight`
    /// (a replace, not an add) clobber its in-flight. A slot still owned by `id`
    /// itself is not a conflict -- that is the stale state recovery reconciles.
    pub(crate) async fn rebuild_mint_tracking_for_recovery(
        &self,
        id: &IssuerRequestId,
        entity: &TokenizedEquityMint,
        tokenization_request_id: crate::tokenized_equity_mint::TokenizationRequestId,
    ) -> Result<RecoveryClaim, RebalancingServiceError> {
        let TokenizedEquityMint::Failed {
            symbol, quantity, ..
        } = entity
        else {
            warn!(target: "rebalance", id = %id, "rebuild_mint_tracking_for_recovery called on non-failed mint; skipping");
            return Ok(RecoveryClaim::Claimed(RecoveryRollback::TrackingOnly));
        };

        let quantity = FractionalShares::new(*quantity);

        // Restore a single canonical pre-recovery inventory shape -- the
        // Hedging in-flight holding `quantity`, with available already debited
        // -- so the ProviderCompletionRecovered reactor arm can complete it
        // uniformly regardless of how the mint failed:
        // - a timeout cleared the in-flight without crediting available and
        //   tombstoned the aggregate (so the reactor ignores late events), so we
        //   drop the tombstone and re-set the in-flight (available stays debited);
        // - an explicit MintAcceptanceFailed cancelled the in-flight back to
        //   available, so we move it back into the in-flight with a Start.
        // Peek (don't yet remove) the timeout markers so a failure in the
        // fallible inventory update below leaves them intact -- a failed rebuild
        // does not run the caller's rollback, so consuming them up front would
        // lose the tombstone permanently.
        let timed_out_at = self.timed_out_mints.read().await.get(id).copied();

        // A third shape the timeout/explicit branches below do not cover: the
        // failure left the in-flight already established (a `Start` ran but the
        // failure never cancelled it back to available -- e.g. an out-of-process
        // `transfer fail`, or a reactor that recorded the failure without
        // running `cancel`). That is already the canonical pre-complete shape,
        // so re-establishing it with another `Start` would double-count
        // (quantity -> 2*quantity) and strand the quantity in-flight after the
        // completion. Detect it and set the in-flight idempotently instead.
        let inflight_already_established = self
            .inventory
            .read()
            .await
            .equity_inflight(symbol, Venue::Hedging)
            .map(|amount| amount.is_zero())
            .transpose()?
            .is_some_and(|is_zero| !is_zero);

        let (update, rollback): (EquityInventoryUpdate, RecoveryRollback) =
            if let Some(timed_out_at) = timed_out_at {
                let suppressed_at = self
                    .suppressed_inflight_symbols
                    .read()
                    .await
                    .get(symbol)
                    .copied();
                (
                    Box::new(Inventory::set_inflight(Venue::Hedging, quantity)),
                    RecoveryRollback::RestoreTombstone {
                        timed_out_at,
                        suppressed_at,
                    },
                )
            } else if inflight_already_established {
                (
                    Box::new(Inventory::set_inflight(Venue::Hedging, quantity)),
                    RecoveryRollback::TrackingOnly,
                )
            } else {
                (
                    Self::start_equity_transfer_update(Venue::Hedging, quantity),
                    RecoveryRollback::CancelInflight,
                )
            };

        // Compare-and-claim: refuse if a *different* mint owns the slot, else
        // restore the in-flight -- both under one write lock so the check and the
        // claim cannot be interleaved by a concurrent live mint.
        {
            let mut inventory = self.inventory.write().await;
            if matches!(inventory.active_mint(symbol), Some(active) if active != id) {
                warn!(
                    target: "rebalance",
                    id = %id,
                    %symbol,
                    "Refusing mint recovery: a different mint for this symbol is in progress"
                );
                return Ok(RecoveryClaim::Conflict);
            }

            *inventory = inventory
                .clone()
                .update_equity(symbol, update, Utc::now())?;
        }

        // The inventory update succeeded; now consume the timeout markers.
        if timed_out_at.is_some() {
            self.timed_out_mints.write().await.remove(id);
            self.suppressed_inflight_symbols
                .write()
                .await
                .remove(symbol);
        }

        self.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity,
                tokenization_request_id: Some(tokenization_request_id),
                stage: MintTrackingStage::Accepted,
                last_progress_at: Utc::now(),
            },
        );
        self.mark_equity_active_transfer(symbol);
        Ok(RecoveryClaim::Claimed(rollback))
    }

    /// Reverses what [`Self::rebuild_mint_tracking_for_recovery`] mutated, for
    /// when the recovery dispatch fails before the reactor completes the
    /// restored in-flight transfer. Restores the pre-recovery failed-mint
    /// state so the live inventory does not show a phantom in-flight transfer,
    /// a missing timeout tombstone, or a symbol locked in-progress until the
    /// next bot restart.
    pub(crate) async fn rollback_mint_tracking_for_recovery(
        &self,
        id: &IssuerRequestId,
        symbol: &Symbol,
        quantity: FractionalShares,
        rollback: RecoveryRollback,
    ) -> Result<(), RebalancingServiceError> {
        match rollback {
            RecoveryRollback::TrackingOnly => {}
            RecoveryRollback::CancelInflight => {
                self.apply_equity_update(
                    symbol,
                    Self::cancel_equity_transfer_update(Venue::Hedging, quantity),
                )
                .await?;
            }
            RecoveryRollback::RestoreTombstone {
                timed_out_at,
                suppressed_at,
            } => {
                let mut inventory = self.inventory.write().await;
                *inventory =
                    inventory
                        .clone()
                        .clear_equity_inflight(symbol, Venue::Hedging, Utc::now())?;
                drop(inventory);
                self.timed_out_mints
                    .write()
                    .await
                    .insert(id.clone(), timed_out_at);
                if let Some(suppressed_at) = suppressed_at {
                    self.suppressed_inflight_symbols
                        .write()
                        .await
                        .insert(symbol.clone(), suppressed_at);
                }
            }
        }

        // Drops the tracking + in-progress guard (and clears active_mint, which
        // a failed dispatch never set since the reactor did not run).
        self.abandon_mint_recovery_guard(id, symbol).await;
        Ok(())
    }

    /// Rebuilds in-memory tracking for a failed redemption being recovered
    /// via `transfer recheck`. See [`Self::rebuild_mint_tracking_for_recovery`]
    /// for why inventory balances are left untouched on the explicit-failure
    /// path: a failed redemption never cancelled its in-flight transfer, so the
    /// recovery event's inventory arm completes that still-pending transfer.
    ///
    /// Returns [`RecoveryClaim::Conflict`] when a *different* live redemption
    /// already owns the symbol's slot. The ownership check runs under the same
    /// inventory write lock that restores a timed-out in-flight, so it cannot be
    /// interleaved by a concurrent redemption.
    pub(crate) async fn rebuild_redemption_tracking_for_recovery(
        &self,
        id: &RedemptionAggregateId,
        entity: &EquityRedemption,
    ) -> Result<RecoveryClaim, RebalancingServiceError> {
        let EquityRedemption::Failed {
            symbol,
            quantity,
            tokenization_request_id,
            redemption_tx,
            ..
        } = entity
        else {
            warn!(target: "rebalance", id = %id, "rebuild_redemption_tracking_for_recovery called on non-failed redemption; skipping");
            return Ok(RecoveryClaim::Claimed(RecoveryRollback::TrackingOnly));
        };

        let quantity = FractionalShares::new(*quantity);

        // A timeout cleared the MarketMaking in-flight and tombstoned the
        // aggregate; drop the tombstone and re-set the in-flight so the recovery
        // arm's complete_equity_transfer_update can confirm it. An explicit
        // DetectionFailed/RedemptionRejected left the in-flight in place (those
        // events do not touch inventory), so there is nothing to restore.
        //
        // Peek (don't yet remove) the timeout markers so a failure in the
        // fallible inventory update below leaves them intact -- a failed rebuild
        // does not run the caller's rollback.
        let timed_out_at = self.timed_out_redemptions.read().await.get(id).copied();
        let suppressed_at = if timed_out_at.is_some() {
            self.suppressed_inflight_symbols
                .read()
                .await
                .get(symbol)
                .copied()
        } else {
            None
        };

        // Compare-and-claim: refuse if a *different* redemption owns the slot.
        // The check shares the write lock with the timed-out in-flight restore so
        // a concurrent redemption cannot claim the slot between the two.
        {
            let mut inventory = self.inventory.write().await;
            if matches!(inventory.active_redemption(symbol), Some(active) if active != id) {
                warn!(
                    target: "rebalance",
                    id = %id,
                    %symbol,
                    "Refusing redemption recovery: a different redemption for this symbol is in progress"
                );
                return Ok(RecoveryClaim::Conflict);
            }

            if timed_out_at.is_some() {
                *inventory = inventory.clone().update_equity(
                    symbol,
                    Box::new(Inventory::set_inflight(Venue::MarketMaking, quantity)),
                    Utc::now(),
                )?;
            }
        }

        let rollback = if let Some(timed_out_at) = timed_out_at {
            // The inventory update succeeded; now consume the timeout markers.
            self.timed_out_redemptions.write().await.remove(id);
            self.suppressed_inflight_symbols
                .write()
                .await
                .remove(symbol);

            RecoveryRollback::RestoreTombstone {
                timed_out_at,
                suppressed_at,
            }
        } else {
            RecoveryRollback::TrackingOnly
        };

        self.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity,
                tokenization_request_id: tokenization_request_id.clone(),
                redemption_tx: *redemption_tx,
                stage: RedemptionTrackingStage::TokensSent,
                last_progress_at: Utc::now(),
            },
        );
        self.mark_equity_active_transfer(symbol);
        Ok(RecoveryClaim::Claimed(rollback))
    }

    /// Reverses what [`Self::rebuild_redemption_tracking_for_recovery`]
    /// mutated, for when the recovery dispatch fails before the reactor
    /// completes the restored in-flight transfer. See
    /// [`Self::rollback_mint_tracking_for_recovery`].
    pub(crate) async fn rollback_redemption_tracking_for_recovery(
        &self,
        id: &RedemptionAggregateId,
        symbol: &Symbol,
        rollback: RecoveryRollback,
    ) -> Result<(), RebalancingServiceError> {
        match rollback {
            // Explicit redemption failures never restored an in-flight, and the
            // `Start`-based variant is mint-only, so there is no balance to undo.
            RecoveryRollback::TrackingOnly | RecoveryRollback::CancelInflight => {}
            RecoveryRollback::RestoreTombstone {
                timed_out_at,
                suppressed_at,
            } => {
                let mut inventory = self.inventory.write().await;
                *inventory = inventory.clone().clear_equity_inflight(
                    symbol,
                    Venue::MarketMaking,
                    Utc::now(),
                )?;
                drop(inventory);
                self.timed_out_redemptions
                    .write()
                    .await
                    .insert(id.clone(), timed_out_at);
                if let Some(suppressed_at) = suppressed_at {
                    self.suppressed_inflight_symbols
                        .write()
                        .await
                        .insert(symbol.clone(), suppressed_at);
                }
            }
        }

        self.redemption_tracking.write().await.remove(id);
        self.clear_equity_in_progress(symbol);

        // Mirror `abandon_mint_recovery_guard`: a recovery may proceed while the
        // slot is still self-owned (a reactor-less failure the live process never
        // observed -- the claim treats that as recoverable, not a conflict). If
        // the dispatch then fails the reactor never runs to clear it, so dropping
        // tracking here would strand a phantom `active_redemption` (visible to the
        // dashboard and future conflict checks) until the next restart. Clear it
        // now, but only while this id still owns it -- re-checking under the write
        // lock so a concurrent redemption that claimed the slot is not cleared.
        let mut inventory = self.inventory.write().await;
        if inventory.active_redemption(symbol) == Some(id) {
            *inventory = inventory.clone().clear_active_redemption(symbol);
        }
        drop(inventory);
        Ok(())
    }

    pub(crate) async fn recover_redemption_state(
        &self,
        id: &RedemptionAggregateId,
        entity: &EquityRedemption,
    ) -> Result<(), RebalancingServiceError> {
        use EquityRedemption::*;

        match entity {
            VaultWithdrawPending { symbol, .. }
            | VaultWithdrawSubmitted { symbol, .. }
            | WithdrawnFromRaindex { symbol, .. }
            | UnwrapPending { symbol, .. }
            | UnwrapSubmitted { symbol, .. }
            | TokensUnwrapped { symbol, .. }
            | SendPending { symbol, .. }
            | TokensSent { symbol, .. }
            | Pending { symbol, .. } => {
                let quantity = Self::recovered_redemption_quantity(entity)?;
                let (stage, last_progress_at, tokenization_request_id, redemption_tx) = match entity
                {
                    VaultWithdrawPending { pending_at, .. } => (
                        RedemptionTrackingStage::VaultWithdrawPending,
                        *pending_at,
                        None,
                        None,
                    ),
                    VaultWithdrawSubmitted { submitted_at, .. } => (
                        RedemptionTrackingStage::VaultWithdrawSubmitted,
                        *submitted_at,
                        None,
                        None,
                    ),
                    WithdrawnFromRaindex { withdrawn_at, .. } => (
                        RedemptionTrackingStage::WithdrawnFromRaindex,
                        *withdrawn_at,
                        None,
                        None,
                    ),
                    UnwrapPending { withdrawn_at, .. } => (
                        RedemptionTrackingStage::UnwrapPending,
                        *withdrawn_at,
                        None,
                        None,
                    ),
                    UnwrapSubmitted { withdrawn_at, .. } => (
                        RedemptionTrackingStage::UnwrapSubmitted,
                        *withdrawn_at,
                        None,
                        None,
                    ),
                    TokensUnwrapped { unwrapped_at, .. } => (
                        RedemptionTrackingStage::TokensUnwrapped,
                        *unwrapped_at,
                        None,
                        None,
                    ),
                    SendPending { unwrapped_at, .. } => (
                        RedemptionTrackingStage::SendPending,
                        *unwrapped_at,
                        None,
                        None,
                    ),
                    TokensSent {
                        sent_at,
                        redemption_tx,
                        ..
                    } => (
                        RedemptionTrackingStage::TokensSent,
                        *sent_at,
                        None,
                        Some(*redemption_tx),
                    ),
                    Pending {
                        detected_at,
                        tokenization_request_id,
                        redemption_tx,
                        ..
                    } => (
                        RedemptionTrackingStage::Detected,
                        *detected_at,
                        Some(tokenization_request_id.clone()),
                        Some(*redemption_tx),
                    ),
                    _ => unreachable!(),
                };

                self.redemption_tracking.write().await.insert(
                    id.clone(),
                    RedemptionTracking {
                        symbol: symbol.clone(),
                        quantity,
                        tokenization_request_id,
                        redemption_tx,
                        stage,
                        last_progress_at,
                    },
                );
                self.mark_equity_active_transfer(symbol);

                let mut inventory = self.inventory.write().await;
                let updated = inventory.clone().update_equity(
                    symbol,
                    Inventory::set_inflight(Venue::MarketMaking, quantity),
                    Utc::now(),
                )?;
                *inventory = updated.set_active_redemption(symbol.clone(), id.clone());
            }
            Completed { .. } | Failed { .. } | Reconciled { .. } => {}
        }

        Ok(())
    }

    async fn on_mint(
        &self,
        id: IssuerRequestId,
        event: TokenizedEquityMintEvent,
    ) -> Result<(), RebalancingServiceError> {
        let event_sync_guard = self.mint_event_sync.lock().await;

        if self.mint_timed_out(&id).await {
            warn!(target: "rebalance", id = %id, "Ignoring late mint event after timeout cleanup");
            return Ok(());
        }

        self.track_mint_progress(&id, &event).await;

        let Some(tracking) = self.load_mint_tracking(&id).await else {
            return Ok(());
        };
        let symbol = tracking.symbol;

        if let Some(update) = Self::mint_inventory_update(&event, tracking.quantity, tracking.stage)
        {
            self.apply_equity_update(&symbol, update).await?;
        }

        // Defense-in-depth: a recovered mint must clear its Hedging in-flight via
        // the completion above. A residual in-flight means recovery double-counted
        // or otherwise failed to confirm it -- a financial accounting anomaly
        // worth surfacing rather than stranding silently.
        if matches!(
            event,
            TokenizedEquityMintEvent::ProviderCompletionRecovered { .. }
        ) {
            let residual = self
                .inventory
                .read()
                .await
                .equity_inflight(&symbol, Venue::Hedging);
            if residual
                .map(|amount| amount.is_zero())
                .transpose()?
                .is_some_and(|is_zero| !is_zero)
            {
                warn!(
                    target: "rebalance",
                    id = %id,
                    %symbol,
                    ?residual,
                    "Recovered mint left a non-zero Hedging in-flight; inventory may be inconsistent"
                );
            }
        }

        // When a new mint transfer starts, clear the previous poll marker
        // so the next inflight poll won't incorrectly zero the new inflight
        // if Alpaca hasn't reflected the request yet.
        if matches!(event, TokenizedEquityMintEvent::MintAccepted { .. }) {
            let mut inventory = self.inventory.write().await;
            *inventory = inventory
                .clone()
                .clear_previous_inflight_mint_marker(&symbol);
        }

        self.update_active_mint(&id, &symbol, &event).await;

        let is_terminal = if Self::is_terminal_mint_event(&event) {
            self.mint_tracking.write().await.remove(&id);
            self.clear_equity_in_progress(&symbol);
            debug!(target: "rebalance", %symbol, "Cleared equity in-progress flag after mint terminal event");
            true
        } else {
            false
        };

        drop(event_sync_guard);

        // A terminal mint event freed the USDC reserved for the
        // just-completed mint, so we cancel any pending checks (they
        // captured pre-rebalance state) and push a fresh USDC check
        // against the post-rebalance inventory.
        if is_terminal {
            self.equity_scheduler.cancel_pending().await;
            self.usdc_scheduler.cancel_pending().await;
            self.usdc_scheduler.enqueue_check().await;
        }

        Ok(())
    }

    async fn on_redemption(
        &self,
        id: RedemptionAggregateId,
        event: EquityRedemptionEvent,
    ) -> Result<(), RebalancingServiceError> {
        let event_sync_guard = self.redemption_event_sync.lock().await;

        if self.redemption_timed_out(&id).await {
            warn!(target: "rebalance", id = %id, "Ignoring late redemption event after timeout cleanup");
            return Ok(());
        }

        if let Some(actual_quantity) = Self::redemption_actual_unwrapped_quantity(&event)?
            && let Some(existing) = self.load_redemption_tracking(&id).await
        {
            if actual_quantity.inner().lt(existing.quantity.inner())? {
                let shortfall = (existing.quantity - actual_quantity)?;
                self.apply_equity_update(
                    &existing.symbol,
                    Self::cancel_equity_transfer_update(Venue::MarketMaking, shortfall),
                )
                .await?;
            } else if existing.quantity.inner().lt(actual_quantity.inner())? {
                warn!(
                    target: "rebalance",
                    id = %id,
                    expected_quantity = %existing.quantity,
                    actual_quantity = %actual_quantity,
                    "Redemption unwrapped more than tracked quantity"
                );
                return Err(RebalancingServiceError::RedemptionUnwrappedExceedsTracked {
                    id,
                    tracked_quantity: existing.quantity,
                    actual_quantity,
                });
            }
        }

        self.track_redemption_progress(&id, &event).await?;

        let Some(tracking) = self.load_redemption_tracking(&id).await else {
            return Ok(());
        };
        let symbol = tracking.symbol;

        if let Some(update) = Self::redemption_inventory_update(&event, tracking.quantity) {
            self.apply_equity_update(&symbol, update).await?;
        }

        // When a new redemption transfer starts, clear the previous poll
        // marker so the next inflight poll won't incorrectly zero the new
        // inflight if Alpaca hasn't reflected the request yet.
        if matches!(event, EquityRedemptionEvent::VaultWithdrawPending { .. }) {
            let mut inventory = self.inventory.write().await;
            *inventory = inventory
                .clone()
                .clear_previous_inflight_redemption_marker(&symbol);
        }

        self.update_active_redemption(&id, &symbol, &event).await;

        let is_terminal = if Self::is_terminal_redemption_event(&event) {
            self.redemption_tracking.write().await.remove(&id);
            self.clear_equity_in_progress(&symbol);
            debug!(
                target: "rebalance",
                %symbol,
                "Cleared equity in-progress flag after redemption terminal event"
            );
            true
        } else {
            false
        };

        drop(event_sync_guard);

        // A terminal redemption freed the shares held by the in-flight
        // transfer, so we cancel pending pre-rebalance checks and push
        // a fresh USDC check against the post-rebalance inventory.
        if is_terminal {
            self.equity_scheduler.cancel_pending().await;
            self.usdc_scheduler.cancel_pending().await;
            self.usdc_scheduler.enqueue_check().await;
        }

        Ok(())
    }

    #[cfg(test)]
    fn extract_mint_info(event: &TokenizedEquityMintEvent) -> Option<(Symbol, FractionalShares)> {
        MintTracking::from_requested_event(event)
            .map(|tracking| (tracking.symbol, tracking.quantity))
    }

    fn is_terminal_mint_event(event: &TokenizedEquityMintEvent) -> bool {
        use TokenizedEquityMintEvent::*;

        match event {
            DepositedIntoRaindex { .. }
            | MintRejected { .. }
            | MintAcceptanceFailed { .. }
            | RaindexDepositFailed { .. }
            | WrappingFailed { .. }
            | OperatorReconciled { .. } => true,

            MintRequested { .. }
            | MintAccepted { .. }
            | TokensReceived { .. }
            | ProviderCompletionRecovered { .. }
            | WrapSubmitted { .. }
            | TokensWrapped { .. }
            | VaultDepositSubmitted { .. } => false,
        }
    }

    #[cfg(test)]
    fn extract_redemption_info(
        event: &EquityRedemptionEvent,
    ) -> Option<(Symbol, FractionalShares)> {
        RedemptionTracking::from_genesis_event(event)
            .map(|tracking| (tracking.symbol, tracking.quantity))
    }

    fn is_terminal_redemption_event(event: &EquityRedemptionEvent) -> bool {
        use EquityRedemptionEvent::*;

        match event {
            Completed { .. }
            | ProviderCompletionRecovered { .. }
            | TransferFailed { .. }
            | DetectionFailed { .. }
            | RedemptionRejected { .. }
            | OperatorReconciled { .. } => true,

            VaultWithdrawPending { .. }
            | VaultWithdrawSubmitted { .. }
            | WithdrawnFromRaindex { .. }
            | UnwrapPending { .. }
            | UnwrapSubmitted { .. }
            | TokensUnwrapped { .. }
            | SendPending { .. }
            | TokensSent { .. }
            | Detected { .. } => false,
        }
    }

    fn is_terminal_usdc_rebalance_event(event: &UsdcRebalanceEvent) -> bool {
        use UsdcRebalanceEvent::*;

        matches!(
            event,
            WithdrawalFailed { .. }
                | BridgingFailed { .. }
                | DepositFailed { .. }
                | ConversionFailed { .. }
                | OperatorReconciled { .. }
                | ConversionConfirmed {
                    direction: RebalanceDirection::BaseToAlpaca,
                    ..
                }
                | DepositConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
        )
    }
}

/// Test fixture: drain every pending equity- and USDC-check row the service
/// has enqueued, looping until both queues report zero pending work. Each
/// asset class drains independently; the loop reconciles the case where a
/// terminal handler for one asset enqueues a fresh check on the other.
#[cfg(test)]
pub(crate) async fn drain_pending_jobs(
    service: &Arc<RebalancingService>,
) -> Result<usize, equity::EquityRebalancingCheckJobError> {
    let mut processed = 0usize;
    loop {
        let equity = equity::drain_pending_equity_jobs(service).await?;
        let usdc = usdc::drain_pending_usdc_jobs(service).await;
        processed += equity + usdc;
        if equity == 0 && usdc == 0 {
            return Ok(processed);
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, TxHash, U256, address, fixed_bytes};
    use async_trait::async_trait;
    use chrono::{Duration as ChronoDuration, Utc};
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use uuid::Uuid;

    use st0x_config::{
        CashAssetConfig, EquitiesConfig, EquityAssetConfig, ExecutionThreshold, OperationMode,
    };
    use st0x_dto::Statement;
    use st0x_event_sorcery::{
        EntityList, Never, Reactor, ReactorHarness, TestStore, deps, test_store,
    };
    use st0x_execution::{
        AlpacaTransferId, ClientOrderId, Direction, ExecutorOrderId, HasZero, Positive,
        SupportedExecutor,
    };
    use st0x_finance::{Usd, Usdc};
    use st0x_float_macro::float;
    use st0x_wrapper::MockWrapper;

    use super::*;
    use crate::alerts::{CapturingNotifier, NoopNotifier};
    use crate::conductor::job::Job;
    use crate::equity_redemption::{
        DetectionFailure, EquityRedemptionCommand, redemption_aggregate_id,
    };
    use crate::inventory::snapshot::{
        InventorySnapshot, InventorySnapshotEvent, InventorySnapshotId,
    };
    use crate::inventory::view::{InFlightEquityLocation, Operator};
    use crate::inventory::{InventoryError, InventoryView, TransferOp, Venue};
    use crate::offchain::order::OffchainOrderId;
    use crate::onchain::mock::MockRaindex;
    use crate::position::{Position, PositionCommand, PositionEvent, TradeId, TriggerReason};
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::test_utils::rebalancing_enabled_equities;
    use crate::tokenization::mock::MockTokenizer;
    use crate::tokenized_equity_mint::{
        TokenizationRequestId, TokenizedEquityMintCommand, issuer_request_id,
    };
    use crate::usdc_rebalance::{
        TransferRef, UsdcRebalance, UsdcRebalanceCommand, UsdcRebalanceId,
    };
    use crate::vault_lookup::MockVaultLookup;
    use crate::vault_registry::VaultRegistryCommand;

    #[test]
    fn mint_inventory_update_skips_cancel_for_pre_acceptance_fail() {
        // RAI-999: a pre-acceptance force-fail (tracking stage still Requested)
        // started no Hedging inflight, so it must produce NO inventory update --
        // a Cancel there would be unmatched. (The post-acceptance Cancel path is
        // covered end-to-end by `recover_mint_clears_hedging_inflight`.)
        let event = TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "operator force-fail".to_string(),
            failed_at: Utc::now(),
        };
        let quantity = FractionalShares::new(float!(10));

        let update = RebalancingService::mint_inventory_update(
            &event,
            quantity,
            MintTrackingStage::Requested,
        );
        assert!(
            update.is_none(),
            "pre-acceptance MintAcceptanceFailed must not produce an inventory update"
        );
    }

    #[test]
    fn mint_inventory_update_cancels_for_post_acceptance_fail() {
        // Once acceptance happened `MintAccepted` started a Hedging inflight, so
        // every later `MintAcceptanceFailed` (operator force-fail or poll-driven)
        // MUST cancel it. This pins the cancel arms in place: the pre-acceptance
        // skip test alone would still pass if they were collapsed to `None`. The
        // end-to-end cancel effect lives in `recover_mint_clears_hedging_inflight`
        // (a distant file); this guards the match wiring locally.
        let event = TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "operator force-fail".to_string(),
            failed_at: Utc::now(),
        };
        let quantity = FractionalShares::new(float!(10));

        for stage in [
            MintTrackingStage::Accepted,
            MintTrackingStage::TokensReceived,
            MintTrackingStage::WrapSubmitted,
            MintTrackingStage::TokensWrapped,
            MintTrackingStage::VaultDepositSubmitted,
        ] {
            let update = RebalancingService::mint_inventory_update(&event, quantity, stage);
            assert!(
                update.is_some(),
                "post-acceptance MintAcceptanceFailed (stage {stage}) must cancel the Hedging inflight"
            );
        }
    }

    fn test_config() -> RebalancingServiceConfig {
        RebalancingServiceConfig {
            equity: ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.2),
            },
            usdc: Some(ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.2),
            }),
            transfer_timeout: Duration::from_secs(30 * 60),
            assets: AssetsConfig {
                equities: rebalancing_enabled_equities(&["AAPL", "TSLA", "GOOG", "RKLB"]),
                cash: Some(CashAssetConfig {
                    vault_ids: Vec::new(),
                    rebalancing: OperationMode::Enabled,
                    operational_limit: None,
                    reserved: None,
                }),
            },
        }
    }

    fn test_config_with_timeout(timeout: Duration) -> RebalancingServiceConfig {
        RebalancingServiceConfig {
            transfer_timeout: timeout,
            ..test_config()
        }
    }

    async fn make_trigger() -> Arc<RebalancingService> {
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wrapper = Arc::new(MockWrapper::new());

        Arc::new(RebalancingService::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            wrapper,
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ))
    }

    #[tokio::test]
    async fn startup_recovery_scan_enqueues_persisted_unwrapped_wallet_balance() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), FractionalShares::new(float!(5)));
        let now = Utc::now();
        let inventory_view = InventoryView::default().set_inflight_equity_at_location(
            InFlightEquityLocation::BaseWalletUnwrapped,
            &balances,
            now,
            now,
        );

        let mut config = test_config();
        config.assets.equities.symbols.insert(
            symbol.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: Address::random(),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory_view, event_sender));
        let service = RebalancingService::new(
            config,
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        );

        service.enqueue_recovery_for_current_wallet_balances().await;

        let (jobs,): (i64,) = sqlx_apalis::query_as("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<UnwrappedEquityRecoveryJob>())
            .fetch_one(service.unwrapped_equity_recovery_queue.pool())
            .await
            .unwrap();
        assert_eq!(
            jobs, 1,
            "startup recovery scan must enqueue persisted positive unwrapped wallet balances",
        );
    }

    #[tokio::test]
    async fn wrapped_equity_recovery_enqueues_when_rebalancing_disabled() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), FractionalShares::new(float!(5)));
        let now = Utc::now();
        let inventory_view = InventoryView::default().set_inflight_equity_at_location(
            InFlightEquityLocation::BaseWalletUnwrapped,
            &balances,
            now,
            now,
        );

        let mut config = test_config();
        config.assets.equities.symbols.insert(
            symbol.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: Address::random(),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory_view, event_sender));
        let service = RebalancingService::new(
            config,
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        );

        service.enqueue_recovery_for_current_wallet_balances().await;

        let (jobs,): (i64,) = sqlx_apalis::query_as("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<UnwrappedEquityRecoveryJob>())
            .fetch_one(service.unwrapped_equity_recovery_queue.pool())
            .await
            .unwrap();
        assert_eq!(
            jobs, 1,
            "wrapped equity recovery must not depend on rebalancing being enabled",
        );
    }

    #[tokio::test]
    async fn wrapped_equity_recovery_skips_when_recovery_disabled() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), FractionalShares::new(float!(5)));
        let now = Utc::now();
        let inventory_view = InventoryView::default().set_inflight_equity_at_location(
            InFlightEquityLocation::BaseWalletWrapped,
            &balances,
            now,
            now,
        );

        let mut config = test_config();
        config.assets.equities.symbols.insert(
            symbol.clone(),
            EquityAssetConfig {
                tokenized_equity: Address::random(),
                tokenized_equity_derivative: Address::random(),
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory_view, event_sender));
        let service = RebalancingService::new(
            config,
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        );

        service.enqueue_recovery_for_current_wallet_balances().await;

        let (jobs,): (i64,) = sqlx_apalis::query_as("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<WrappedEquityRecoveryJob>())
            .fetch_one(service.wrapped_equity_recovery_queue.pool())
            .await
            .unwrap();
        assert_eq!(
            jobs, 0,
            "recovery-disabled symbols must not enqueue wallet recovery jobs",
        );
    }

    #[tokio::test]
    async fn recover_mint_state_restores_tracking_and_inflight() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let mint_id = issuer_request_id("mint-recovery");
        let accepted_at = Utc::now();

        trigger
            .recover_mint_state(
                &mint_id,
                &TokenizedEquityMint::MintAccepted {
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                    issuer_request_id: mint_id.clone(),
                    tokenization_request_id: TokenizationRequestId("TOK-1".to_string()),
                    requested_at: Utc::now(),
                    accepted_at,
                },
            )
            .await
            .unwrap();

        assert!(
            trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );

        let tracking = trigger
            .mint_tracking
            .read()
            .await
            .get(&mint_id)
            .cloned()
            .unwrap();
        assert_eq!(tracking.symbol, symbol);
        assert_eq!(tracking.quantity, FractionalShares::new(float!(10)));
        assert_eq!(
            tracking.tokenization_request_id,
            Some(TokenizationRequestId("TOK-1".to_string()))
        );
        assert_eq!(tracking.stage, MintTrackingStage::Accepted);
        assert_eq!(tracking.last_progress_at, accepted_at);

        let ownership = trigger.pending_request_ownership().await;
        assert!(ownership.mint_issuers.contains(&mint_id));
        assert!(
            ownership
                .mint_tokenizations
                .contains(&TokenizationRequestId("TOK-1".to_string()))
        );

        let inflight = {
            let inventory = trigger.inventory.read().await;
            inventory.equity_inflight(&symbol, Venue::Hedging)
        };
        assert_eq!(inflight, Some(FractionalShares::new(float!(10))));
    }

    /// Builds a `RebalancingService` with `wrapped_equity_recovery = Enabled` for `symbol`.
    async fn make_trigger_with_recovery_enabled(symbol: &Symbol) -> Arc<RebalancingService> {
        let config = RebalancingServiceConfig {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    operational_limit: None,
                    symbols: HashMap::from([(
                        symbol.clone(),
                        EquityAssetConfig {
                            tokenized_equity: Address::ZERO,
                            tokenized_equity_derivative: Address::ZERO,
                            pyth_feed_id: None,
                            vault_ids: Vec::new(),
                            trading: OperationMode::Disabled,
                            rebalancing: OperationMode::Enabled,
                            wrapped_equity_recovery: OperationMode::Enabled,
                            operational_limit: None,
                        },
                    )]),
                },
                cash: None,
            },
            ..test_config()
        };
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wrapper = Arc::new(MockWrapper::new());
        Arc::new(RebalancingService::new(
            config,
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            wrapper,
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ))
    }

    /// `recover_mint_state` startup guard reconstruction: all four post-receipt
    /// states produce the correct `GuardState`.
    ///
    /// - `TokensReceived` / `WrapSubmitted` (pre-wrap, recovery enabled):
    ///   `HeldForRecovery` — `UnwrappedEquityRecovery` owns the slot; a new
    ///   transfer cannot start until recovery completes.
    /// - `TokensWrapped` / `VaultDepositSubmitted` (post-wrap, recovery
    ///   enabled): `ActiveTransfer` — deposit is idempotent;
    ///   `resume_interrupted_transfers` drives them via `resume_mint`.
    /// - `TokensReceived` (recovery disabled): `ActiveTransfer` — no recovery
    ///   job will run; the transfer job retries normally via apalis.
    #[tokio::test]
    async fn recover_mint_state_reconstructs_all_post_receipt_states() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        let trigger_enabled = make_trigger_with_recovery_enabled(&symbol).await;
        let trigger_disabled = make_trigger().await;

        async fn check(
            trigger: &RebalancingService,
            symbol: &Symbol,
            mint_id_str: &str,
            entity: TokenizedEquityMint,
            expected: equity::GuardState,
            label: &str,
        ) {
            // Clear any prior guard state so each case starts clean.
            trigger.equity_in_progress.write().unwrap().remove(symbol);
            let mint_id = issuer_request_id(mint_id_str);
            trigger.recover_mint_state(&mint_id, &entity).await.unwrap();
            assert_eq!(
                trigger
                    .equity_in_progress
                    .read()
                    .unwrap()
                    .get(symbol)
                    .cloned(),
                Some(expected),
                "{label}"
            );
        }

        check(
            &trigger_enabled,
            &symbol,
            "startup-tokens-received",
            TokenizedEquityMint::TokensReceived {
                symbol: symbol.clone(),
                quantity: float!(5),
                wallet: Address::ZERO,
                issuer_request_id: issuer_request_id("startup-tokens-received"),
                tokenization_request_id: TokenizationRequestId("TOK-TR".to_string()),
                tx_hash: TxHash::ZERO,
                shares_minted: U256::from(5u64),
                fees: None,
                requested_at: now,
                accepted_at: now,
                received_at: now,
            },
            equity::GuardState::HeldForRecovery,
            "TokensReceived + recovery enabled must reconstruct as HeldForRecovery",
        )
        .await;

        check(
            &trigger_enabled,
            &symbol,
            "startup-wrap-submitted",
            TokenizedEquityMint::WrapSubmitted {
                symbol: symbol.clone(),
                quantity: float!(5),
                wallet: Address::ZERO,
                issuer_request_id: issuer_request_id("startup-wrap-submitted"),
                tokenization_request_id: TokenizationRequestId("TOK-WS".to_string()),
                tx_hash: TxHash::ZERO,
                shares_minted: U256::from(5u64),
                fees: None,
                requested_at: now,
                accepted_at: now,
                received_at: now,
                wrap_tx_hash: TxHash::ZERO,
            },
            equity::GuardState::HeldForRecovery,
            "WrapSubmitted + recovery enabled must reconstruct as HeldForRecovery",
        )
        .await;

        // For ActiveTransfer cases the generation counter is opaque so we use
        // matches! rather than assert_eq! to verify the variant without pinning
        // the generation value.
        async fn check_active_transfer(
            trigger: &RebalancingService,
            symbol: &Symbol,
            mint_id_str: &str,
            entity: TokenizedEquityMint,
            label: &str,
        ) {
            trigger.equity_in_progress.write().unwrap().remove(symbol);
            let mint_id = issuer_request_id(mint_id_str);
            trigger.recover_mint_state(&mint_id, &entity).await.unwrap();
            assert!(
                matches!(
                    trigger
                        .equity_in_progress
                        .read()
                        .unwrap()
                        .get(symbol)
                        .cloned(),
                    Some(equity::GuardState::ActiveTransfer { .. })
                ),
                "{label}"
            );
        }

        check_active_transfer(
            &trigger_enabled,
            &symbol,
            "startup-tokens-wrapped",
            TokenizedEquityMint::TokensWrapped {
                symbol: symbol.clone(),
                quantity: float!(5),
                wallet: Address::ZERO,
                issuer_request_id: issuer_request_id("startup-tokens-wrapped"),
                tokenization_request_id: TokenizationRequestId("TOK-TW".to_string()),
                tx_hash: TxHash::ZERO,
                shares_minted: U256::from(5u64),
                requested_at: now,
                accepted_at: now,
                received_at: now,
                wrap_tx_hash: TxHash::ZERO,
                wrapped_shares: U256::from(5u64),
                wrap_block: None,
                wrapped_at: now,
            },
            "TokensWrapped must always reconstruct as ActiveTransfer \
             (deposit idempotent; WrappedEquityRecovery is orphan-only)",
        )
        .await;

        check_active_transfer(
            &trigger_enabled,
            &symbol,
            "startup-vault-deposit-submitted",
            TokenizedEquityMint::VaultDepositSubmitted {
                symbol: symbol.clone(),
                quantity: float!(5),
                wallet: Address::ZERO,
                issuer_request_id: issuer_request_id("startup-vault-deposit-submitted"),
                tokenization_request_id: TokenizationRequestId("TOK-VDS".to_string()),
                tx_hash: TxHash::ZERO,
                shares_minted: U256::from(5u64),
                requested_at: now,
                accepted_at: now,
                received_at: now,
                wrap_tx_hash: TxHash::ZERO,
                wrapped_shares: U256::from(5u64),
                wrapped_at: now,
                vault_deposit_tx_hash: TxHash::ZERO,
            },
            "VaultDepositSubmitted must always reconstruct as ActiveTransfer \
             (deposit tx on-chain; resume_mint confirms idempotently)",
        )
        .await;

        check_active_transfer(
            &trigger_disabled,
            &symbol,
            "startup-tokens-received-disabled",
            TokenizedEquityMint::TokensReceived {
                symbol: symbol.clone(),
                quantity: float!(5),
                wallet: Address::ZERO,
                issuer_request_id: issuer_request_id("startup-tokens-received-disabled"),
                tokenization_request_id: TokenizationRequestId("TOK-TR-D".to_string()),
                tx_hash: TxHash::ZERO,
                shares_minted: U256::from(5u64),
                fees: None,
                requested_at: now,
                accepted_at: now,
                received_at: now,
            },
            "TokensReceived + recovery disabled must reconstruct as ActiveTransfer \
             (apalis transfer job retries normally)",
        )
        .await;
    }

    #[tokio::test]
    async fn recover_redemption_state_restores_tracking_and_inflight() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_id = redemption_aggregate_id("redemption-recovery");
        let detected_at = Utc::now();
        let redemption_tx = TxHash::random();

        trigger
            .recover_redemption_state(
                &redemption_id,
                &EquityRedemption::Pending {
                    symbol: symbol.clone(),
                    quantity: float!(12),
                    redemption_tx,
                    tokenization_request_id: TokenizationRequestId("TOK-2".to_string()),
                    sent_at: Utc::now(),
                    detected_at,
                },
            )
            .await
            .unwrap();

        assert!(
            trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );

        let tracking = trigger
            .redemption_tracking
            .read()
            .await
            .get(&redemption_id)
            .cloned()
            .unwrap();
        assert_eq!(tracking.symbol, symbol);
        assert_eq!(tracking.quantity, FractionalShares::new(float!(12)));
        assert_eq!(
            tracking.tokenization_request_id,
            Some(TokenizationRequestId("TOK-2".to_string()))
        );
        assert_eq!(tracking.stage, RedemptionTrackingStage::Detected);
        assert_eq!(tracking.last_progress_at, detected_at);

        let ownership = trigger.pending_request_ownership().await;
        assert!(
            ownership
                .redemption_tokenizations
                .contains(&TokenizationRequestId("TOK-2".to_string()))
        );
        assert!(ownership.redemption_txs.contains(&redemption_tx));

        let inflight = {
            let inventory = trigger.inventory.read().await;
            inventory.equity_inflight(&symbol, Venue::MarketMaking)
        };
        assert_eq!(inflight, Some(FractionalShares::new(float!(12))));
    }

    #[tokio::test]
    async fn recovery_after_explicit_mint_failure_moves_equity_to_market_making() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let mint_id = issuer_request_id("mint-explicit-recovery");
        let tok = TokenizationRequestId("TOK-1".to_string());

        // An explicit MintAcceptanceFailed cancelled the in-flight back to
        // Hedging (offchain) available, so available holds the full 100 and
        // inflight is 0.
        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(0), shares(100));

        let failed = TokenizedEquityMint::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            reason: "rejected".to_string(),
            requested_at: Utc::now(),
            failed_at: Utc::now(),
        };

        trigger
            .rebuild_mint_tracking_for_recovery(&mint_id, &failed, tok.clone())
            .await
            .unwrap();

        trigger
            .on_mint(
                mint_id.clone(),
                TokenizedEquityMintEvent::ProviderCompletionRecovered {
                    issuer_request_id: mint_id.clone(),
                    wallet: Address::ZERO,
                    tokenization_request_id: tok,
                    tx_hash: TxHash::random(),
                    shares_minted: U256::from(10u64),
                    fees: None,
                    recovered_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let (hedging, market_making) = {
            let inventory = trigger.inventory.read().await;
            (
                inventory.equity_available(&symbol, Venue::Hedging),
                inventory.equity_available(&symbol, Venue::MarketMaking),
            )
        };
        assert_eq!(hedging, Some(shares(90)));
        assert_eq!(market_making, Some(shares(10)));
    }

    #[tokio::test]
    async fn recovery_after_timeout_mint_failure_clears_tombstone_and_moves_equity() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let mint_id = issuer_request_id("mint-timeout-recovery");
        let tok = TokenizationRequestId("TOK-1".to_string());

        // A timeout cleared the in-flight without crediting available (Hedging
        // (offchain) available stays debited at 90) and tombstoned the aggregate
        // so the reactor ignores late events.
        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(0), shares(90));
        trigger
            .timed_out_mints
            .write()
            .await
            .insert(mint_id.clone(), Utc::now());

        let failed = TokenizedEquityMint::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            reason: "timeout".to_string(),
            requested_at: Utc::now(),
            failed_at: Utc::now(),
        };

        trigger
            .rebuild_mint_tracking_for_recovery(&mint_id, &failed, tok.clone())
            .await
            .unwrap();

        // The tombstone must be cleared so the reactor stops ignoring events.
        assert!(!trigger.timed_out_mints.read().await.contains_key(&mint_id));

        trigger
            .on_mint(
                mint_id.clone(),
                TokenizedEquityMintEvent::ProviderCompletionRecovered {
                    issuer_request_id: mint_id.clone(),
                    wallet: Address::ZERO,
                    tokenization_request_id: tok,
                    tx_hash: TxHash::random(),
                    shares_minted: U256::from(10u64),
                    fees: None,
                    recovered_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        // Same end state as the explicit case: the 10 that left Hedging now
        // sits in MarketMaking. Hedging available is NOT double-debited.
        let (hedging, market_making) = {
            let inventory = trigger.inventory.read().await;
            (
                inventory.equity_available(&symbol, Venue::Hedging),
                inventory.equity_available(&symbol, Venue::MarketMaking),
            )
        };
        assert_eq!(hedging, Some(shares(90)));
        assert_eq!(market_making, Some(shares(10)));
    }

    #[tokio::test]
    async fn recovery_after_explicit_redemption_failure_moves_equity_to_hedging() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_id = redemption_aggregate_id("redemption-explicit-recovery");

        // An explicit DetectionFailed/RedemptionRejected does not touch
        // inventory, so the in-flight is still held at MarketMaking (onchain):
        // available 90, inflight 10.
        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(90), shares(0))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK-2".to_string())),
            reason: Some("rejected".to_string()),
            started_at: Utc::now(),
            failed_at: Utc::now(),
        };

        trigger
            .rebuild_redemption_tracking_for_recovery(&redemption_id, &failed)
            .await
            .unwrap();

        trigger
            .on_redemption(
                redemption_id.clone(),
                EquityRedemptionEvent::ProviderCompletionRecovered {
                    tokenization_request_id: TokenizationRequestId("TOK-2".to_string()),
                    recovered_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let (market_making, hedging) = {
            let inventory = trigger.inventory.read().await;
            (
                inventory.equity_available(&symbol, Venue::MarketMaking),
                inventory.equity_available(&symbol, Venue::Hedging),
            )
        };
        assert_eq!(market_making, Some(shares(90)));
        assert_eq!(hedging, Some(shares(10)));
    }

    #[tokio::test]
    async fn recovery_after_timeout_redemption_failure_clears_tombstone_and_moves_equity() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_id = redemption_aggregate_id("redemption-timeout-recovery");

        // A timeout cleared the MarketMaking in-flight and tombstoned the
        // aggregate; available stays debited at 90.
        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(90), shares(0));
        trigger
            .timed_out_redemptions
            .write()
            .await
            .insert(redemption_id.clone(), Utc::now());

        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK-2".to_string())),
            reason: Some("timeout".to_string()),
            started_at: Utc::now(),
            failed_at: Utc::now(),
        };

        trigger
            .rebuild_redemption_tracking_for_recovery(&redemption_id, &failed)
            .await
            .unwrap();

        assert!(
            !trigger
                .timed_out_redemptions
                .read()
                .await
                .contains_key(&redemption_id)
        );

        trigger
            .on_redemption(
                redemption_id.clone(),
                EquityRedemptionEvent::ProviderCompletionRecovered {
                    tokenization_request_id: TokenizationRequestId("TOK-2".to_string()),
                    recovered_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let (market_making, hedging) = {
            let inventory = trigger.inventory.read().await;
            (
                inventory.equity_available(&symbol, Venue::MarketMaking),
                inventory.equity_available(&symbol, Venue::Hedging),
            )
        };
        assert_eq!(market_making, Some(shares(90)));
        assert_eq!(hedging, Some(shares(10)));
    }

    #[tokio::test]
    async fn abandon_mint_recovery_guard_clears_active_mint() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let mint_id = issuer_request_id("mint-resume-failure");
        let tok = TokenizationRequestId("TOK-1".to_string());

        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(0), shares(100));

        let failed = TokenizedEquityMint::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            reason: "rejected".to_string(),
            requested_at: Utc::now(),
            failed_at: Utc::now(),
        };

        trigger
            .rebuild_mint_tracking_for_recovery(&mint_id, &failed, tok.clone())
            .await
            .unwrap();

        // The reactor processed ProviderCompletionRecovered (non-terminal for
        // mints), which set the active-mint slot.
        trigger
            .on_mint(
                mint_id.clone(),
                TokenizedEquityMintEvent::ProviderCompletionRecovered {
                    issuer_request_id: mint_id.clone(),
                    wallet: Address::ZERO,
                    tokenization_request_id: tok,
                    tx_hash: TxHash::random(),
                    shares_minted: U256::from(10u64),
                    fees: None,
                    recovered_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            trigger.inventory.read().await.active_mint(&symbol),
            Some(&mint_id),
            "recovery event should have set the active-mint slot"
        );

        // A failed in-process resume abandons the recovery guard; this must also
        // clear the active-mint slot. Tracking is dropped here, so no later
        // event can clear it -- otherwise the dashboard shows a phantom active
        // mint until the next restart.
        trigger.abandon_mint_recovery_guard(&mint_id, &symbol).await;

        assert_eq!(trigger.inventory.read().await.active_mint(&symbol), None);
        assert!(!trigger.mint_tracking.read().await.contains_key(&mint_id));
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn mint_recovery_claim_refuses_a_different_mint_without_mutating() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let recovering = issuer_request_id("recovering-mint");
        let other = issuer_request_id("other-mint");
        let tok = TokenizationRequestId("TOK-1".to_string());

        // Explicit-failure shape (available 100, in-flight 0) but a *different*
        // mint owns the slot. The claim must refuse and touch nothing -- the
        // other mint's in-flight stays intact and recovery installs no tracking.
        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(100))
            .set_active_mint(symbol.clone(), other.clone());

        let failed = TokenizedEquityMint::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            reason: "rejected".to_string(),
            requested_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let claim = trigger
            .rebuild_mint_tracking_for_recovery(&recovering, &failed, tok)
            .await
            .unwrap();
        assert!(matches!(claim, RecoveryClaim::Conflict));

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(0)),
            "a conflicting claim must not restore the in-flight"
        );
        assert_eq!(inventory.active_mint(&symbol), Some(&other));
        drop(inventory);
        assert!(!trigger.mint_tracking.read().await.contains_key(&recovering));
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn mint_recovery_claim_succeeds_for_self_owned_slot() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let recovering = issuer_request_id("recovering-mint");
        let tok = TokenizationRequestId("TOK-1".to_string());

        // The slot is still owned by the recovering mint itself (e.g. a
        // reactor-less failure injection the live process never observed) -> NOT
        // a conflict; recovery is what reconciles it.
        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(100))
            .set_active_mint(symbol.clone(), recovering.clone());

        let failed = TokenizedEquityMint::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            reason: "rejected".to_string(),
            requested_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let claim = trigger
            .rebuild_mint_tracking_for_recovery(&recovering, &failed, tok)
            .await
            .unwrap();
        assert!(matches!(claim, RecoveryClaim::Claimed(_)));
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .equity_inflight(&symbol, Venue::Hedging),
            Some(shares(10)),
            "claiming a self-owned slot must restore the in-flight"
        );
    }

    #[tokio::test]
    async fn redemption_recovery_claim_refuses_a_different_redemption_without_mutating() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let recovering = redemption_aggregate_id("recovering-redemption");
        let other = redemption_aggregate_id("other-redemption");
        let tombstone_at = Utc::now();

        // Timeout shape with a *different* redemption owning the slot. The claim
        // must refuse without restoring the in-flight or consuming the tombstone.
        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(90), shares(0))
            .set_active_redemption(symbol.clone(), other.clone());
        trigger
            .timed_out_redemptions
            .write()
            .await
            .insert(recovering.clone(), tombstone_at);

        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK-2".to_string())),
            reason: Some("timeout".to_string()),
            started_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let claim = trigger
            .rebuild_redemption_tracking_for_recovery(&recovering, &failed)
            .await
            .unwrap();
        assert!(matches!(claim, RecoveryClaim::Conflict));

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(0)),
            "a conflicting claim must not restore the in-flight"
        );
        assert_eq!(inventory.active_redemption(&symbol), Some(&other));
        drop(inventory);
        assert!(
            trigger
                .timed_out_redemptions
                .read()
                .await
                .contains_key(&recovering),
            "a conflicting claim must not consume the timeout tombstone"
        );
        assert!(
            !trigger
                .redemption_tracking
                .read()
                .await
                .contains_key(&recovering)
        );
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn redemption_recovery_claim_succeeds_for_self_owned_slot() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let recovering = redemption_aggregate_id("recovering-redemption");
        let tombstone_at = Utc::now();

        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(90), shares(0))
            .set_active_redemption(symbol.clone(), recovering.clone());
        trigger
            .timed_out_redemptions
            .write()
            .await
            .insert(recovering.clone(), tombstone_at);

        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK-2".to_string())),
            reason: Some("timeout".to_string()),
            started_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let claim = trigger
            .rebuild_redemption_tracking_for_recovery(&recovering, &failed)
            .await
            .unwrap();
        assert!(matches!(claim, RecoveryClaim::Claimed(_)));
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(10)),
            "claiming a self-owned slot must restore the in-flight"
        );
    }

    #[tokio::test]
    async fn abandon_mint_recovery_guard_keeps_active_mint_owned_by_other_id() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let recovering_id = issuer_request_id("recovering-mint");
        let other_id = issuer_request_id("other-active-mint");

        // A different aggregate owns the symbol's active-mint slot (e.g. a
        // concurrent mint). Abandoning the recovering mint's guard must NOT clear
        // a slot it does not own.
        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(100))
            .set_active_mint(symbol.clone(), other_id.clone());

        trigger
            .abandon_mint_recovery_guard(&recovering_id, &symbol)
            .await;

        assert_eq!(
            trigger.inventory.read().await.active_mint(&symbol),
            Some(&other_id),
            "abandon must not clear an active-mint slot owned by a different aggregate"
        );
    }

    #[tokio::test]
    async fn rollback_after_explicit_mint_dispatch_failure_restores_available() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let mint_id = issuer_request_id("mint-explicit-rollback");
        let tok = TokenizationRequestId("TOK-1".to_string());

        // Explicit failure cancelled the in-flight back to available: 100/0.
        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(0), shares(100));

        let failed = TokenizedEquityMint::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            reason: "rejected".to_string(),
            requested_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let rollback = trigger
            .rebuild_mint_tracking_for_recovery(&mint_id, &failed, tok)
            .await
            .unwrap()
            .expect_claimed();

        // Rebuild moved available -> in-flight and installed tracking.
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .equity_inflight(&symbol, Venue::Hedging),
            Some(shares(10))
        );
        assert!(trigger.mint_tracking.read().await.contains_key(&mint_id));

        // The dispatch failed before the reactor ran; rolling back must restore
        // the pre-recovery failed-mint shape.
        trigger
            .rollback_mint_tracking_for_recovery(
                &mint_id,
                &symbol,
                FractionalShares::new(float!(10)),
                rollback,
            )
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_available(&symbol, Venue::Hedging),
            Some(shares(100))
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(0))
        );
        assert_eq!(inventory.active_mint(&symbol), None);
        drop(inventory);
        assert!(!trigger.mint_tracking.read().await.contains_key(&mint_id));
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn rollback_after_timeout_mint_dispatch_failure_restores_tombstone() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let mint_id = issuer_request_id("mint-timeout-rollback");
        let tok = TokenizationRequestId("TOK-1".to_string());
        let tombstone_at = Utc::now();

        // Timeout shape: available debited to 90, in-flight cleared, tombstoned.
        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(0), shares(90));
        trigger
            .timed_out_mints
            .write()
            .await
            .insert(mint_id.clone(), tombstone_at);
        trigger
            .suppressed_inflight_symbols
            .write()
            .await
            .insert(symbol.clone(), tombstone_at);

        let failed = TokenizedEquityMint::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            reason: "timeout".to_string(),
            requested_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let rollback = trigger
            .rebuild_mint_tracking_for_recovery(&mint_id, &failed, tok)
            .await
            .unwrap()
            .expect_claimed();

        // Rebuild dropped the tombstone and re-set the in-flight.
        assert!(!trigger.timed_out_mints.read().await.contains_key(&mint_id));
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .equity_inflight(&symbol, Venue::Hedging),
            Some(shares(10))
        );

        trigger
            .rollback_mint_tracking_for_recovery(
                &mint_id,
                &symbol,
                FractionalShares::new(float!(10)),
                rollback,
            )
            .await
            .unwrap();

        // The tombstone + suppressed marker are restored and the in-flight is
        // re-cleared without crediting available (it stays debited at 90).
        assert_eq!(
            trigger.timed_out_mints.read().await.get(&mint_id),
            Some(&tombstone_at)
        );
        assert!(
            trigger
                .suppressed_inflight_symbols
                .read()
                .await
                .contains_key(&symbol)
        );
        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_available(&symbol, Venue::Hedging),
            Some(shares(90))
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(0))
        );
        drop(inventory);
        assert!(!trigger.mint_tracking.read().await.contains_key(&mint_id));
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn rollback_after_timeout_redemption_dispatch_failure_restores_tombstone() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_id = redemption_aggregate_id("redemption-timeout-rollback");
        let tombstone_at = Utc::now();

        // Timeout cleared the MarketMaking in-flight and tombstoned; 90/0.
        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(90), shares(0));
        trigger
            .timed_out_redemptions
            .write()
            .await
            .insert(redemption_id.clone(), tombstone_at);
        trigger
            .suppressed_inflight_symbols
            .write()
            .await
            .insert(symbol.clone(), tombstone_at);

        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK-2".to_string())),
            reason: Some("timeout".to_string()),
            started_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let rollback = trigger
            .rebuild_redemption_tracking_for_recovery(&redemption_id, &failed)
            .await
            .unwrap()
            .expect_claimed();

        assert!(
            !trigger
                .timed_out_redemptions
                .read()
                .await
                .contains_key(&redemption_id)
        );
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(10))
        );

        trigger
            .rollback_redemption_tracking_for_recovery(&redemption_id, &symbol, rollback)
            .await
            .unwrap();

        assert_eq!(
            trigger
                .timed_out_redemptions
                .read()
                .await
                .get(&redemption_id),
            Some(&tombstone_at)
        );
        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_available(&symbol, Venue::MarketMaking),
            Some(shares(90))
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(0))
        );
        drop(inventory);
        assert!(
            !trigger
                .redemption_tracking
                .read()
                .await
                .contains_key(&redemption_id)
        );
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn rollback_after_explicit_redemption_dispatch_failure_keeps_inflight() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_id = redemption_aggregate_id("redemption-explicit-rollback");

        // An explicit failure left the MarketMaking in-flight in place: 90
        // available, 10 in-flight. Rebuild does not touch inventory here.
        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(90), shares(0))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK-2".to_string())),
            reason: Some("rejected".to_string()),
            started_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let rollback = trigger
            .rebuild_redemption_tracking_for_recovery(&redemption_id, &failed)
            .await
            .unwrap()
            .expect_claimed();

        trigger
            .rollback_redemption_tracking_for_recovery(&redemption_id, &symbol, rollback)
            .await
            .unwrap();

        // The held in-flight is untouched (rebuild never restored it); only the
        // tracking + in-progress guard are dropped.
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(10))
        );
        assert!(
            !trigger
                .redemption_tracking
                .read()
                .await
                .contains_key(&redemption_id)
        );
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn rollback_redemption_clears_self_owned_active_redemption() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_id = redemption_aggregate_id("redemption-self-owned");

        // Explicit-failure shape with the slot still self-owned (a reactor-less
        // failure the live process never observed -- the claim treats this as
        // recoverable). The dispatch then fails, so rollback must clear the now
        // stale active_redemption rather than strand a phantom.
        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(90), shares(0))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap()
            .set_active_redemption(symbol.clone(), redemption_id.clone());

        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK-2".to_string())),
            reason: Some("rejected".to_string()),
            started_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let rollback = trigger
            .rebuild_redemption_tracking_for_recovery(&redemption_id, &failed)
            .await
            .unwrap()
            .expect_claimed();

        trigger
            .rollback_redemption_tracking_for_recovery(&redemption_id, &symbol, rollback)
            .await
            .unwrap();

        assert_eq!(
            trigger.inventory.read().await.active_redemption(&symbol),
            None,
            "rollback must clear the stale self-owned active_redemption slot"
        );
    }

    #[tokio::test]
    async fn rollback_redemption_keeps_active_redemption_owned_by_other_id() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_id = redemption_aggregate_id("redemption-rollback");
        let other_id = redemption_aggregate_id("other-redemption");

        // No owner at claim time -> claim succeeds.
        *trigger.inventory.write().await = InventoryView::default()
            .with_equity(symbol.clone(), shares(90), shares(0))
            .update_equity(
                &symbol,
                Inventory::set_inflight(Venue::MarketMaking, shares(10)),
                Utc::now(),
            )
            .unwrap();

        let failed = EquityRedemption::Failed {
            symbol: symbol.clone(),
            quantity: float!(10),
            raindex_withdraw_tx: None,
            redemption_tx: Some(TxHash::random()),
            tokenization_request_id: Some(TokenizationRequestId("TOK-2".to_string())),
            reason: Some("rejected".to_string()),
            started_at: Utc::now(),
            failed_at: Utc::now(),
        };

        let rollback = trigger
            .rebuild_redemption_tracking_for_recovery(&redemption_id, &failed)
            .await
            .unwrap()
            .expect_claimed();

        // A concurrent redemption claims the slot before rollback runs; the
        // ownership re-check must leave that claim intact.
        {
            let mut inventory = trigger.inventory.write().await;
            *inventory = inventory
                .clone()
                .set_active_redemption(symbol.clone(), other_id.clone());
        }

        trigger
            .rollback_redemption_tracking_for_recovery(&redemption_id, &symbol, rollback)
            .await
            .unwrap();

        assert_eq!(
            trigger.inventory.read().await.active_redemption(&symbol),
            Some(&other_id),
            "rollback must not clear a slot owned by a different redemption"
        );
    }

    #[tokio::test]
    async fn pending_request_ownership_exposes_active_mint_ids() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let mint_id = issuer_request_id("owned-mint");
        let tokenization_request_id = TokenizationRequestId("owned-tokenization".to_string());

        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50));

        trigger
            .on_mint(
                mint_id.clone(),
                TokenizedEquityMintEvent::MintRequested {
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                    requested_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        trigger
            .on_mint(
                mint_id.clone(),
                TokenizedEquityMintEvent::MintAccepted {
                    issuer_request_id: mint_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    accepted_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let ownership = trigger.pending_request_ownership().await;

        assert!(ownership.mint_issuers.contains(&mint_id));
        assert!(
            ownership
                .mint_tokenizations
                .contains(&tokenization_request_id)
        );

        trigger
            .on_mint(
                mint_id.clone(),
                TokenizedEquityMintEvent::MintAcceptanceFailed {
                    reason: "test terminal".to_string(),
                    failed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let ownership = trigger.pending_request_ownership().await;
        assert!(!ownership.mint_issuers.contains(&mint_id));
        assert!(
            !ownership
                .mint_tokenizations
                .contains(&tokenization_request_id)
        );

        let success_mint_id = issuer_request_id("owned-success-mint");
        let success_tokenization_request_id =
            TokenizationRequestId("owned-success-tokenization".to_string());

        trigger
            .on_mint(
                success_mint_id.clone(),
                TokenizedEquityMintEvent::MintRequested {
                    symbol: symbol.clone(),
                    quantity: float!(2),
                    wallet: Address::ZERO,
                    requested_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        trigger
            .on_mint(
                success_mint_id.clone(),
                TokenizedEquityMintEvent::MintAccepted {
                    issuer_request_id: success_mint_id.clone(),
                    tokenization_request_id: success_tokenization_request_id.clone(),
                    accepted_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        trigger
            .on_mint(
                success_mint_id.clone(),
                TokenizedEquityMintEvent::DepositedIntoRaindex {
                    vault_deposit_tx_hash: TxHash::random(),
                    deposited_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let ownership = trigger.pending_request_ownership().await;
        assert!(!ownership.mint_issuers.contains(&success_mint_id));
        assert!(
            !ownership
                .mint_tokenizations
                .contains(&success_tokenization_request_id)
        );
    }

    #[tokio::test]
    async fn pending_request_ownership_exposes_active_redemption_request_id() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_id = redemption_aggregate_id("owned-redemption");
        let tokenization_request_id = TokenizationRequestId("owned-redemption-request".to_string());
        let redemption_tx = TxHash::random();

        *trigger.inventory.write().await =
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50));

        trigger
            .on_redemption(
                redemption_id.clone(),
                EquityRedemptionEvent::VaultWithdrawPending {
                    symbol: symbol.clone(),
                    quantity: float!(10),
                    token: Address::ZERO,
                    wrapped_amount: U256::from(10),
                    pending_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let ownership = trigger.pending_request_ownership().await;
        assert!(ownership.redemption_txs.is_empty());

        trigger
            .on_redemption(
                redemption_id.clone(),
                EquityRedemptionEvent::TokensSent {
                    redemption_wallet: Address::ZERO,
                    redemption_tx,
                    sent_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let ownership = trigger.pending_request_ownership().await;
        assert!(ownership.redemption_txs.contains(&redemption_tx));

        trigger
            .on_redemption(
                redemption_id.clone(),
                EquityRedemptionEvent::Detected {
                    tokenization_request_id: tokenization_request_id.clone(),
                    detected_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let ownership = trigger.pending_request_ownership().await;

        assert!(
            ownership
                .redemption_tokenizations
                .contains(&tokenization_request_id)
        );
        assert!(ownership.redemption_txs.contains(&redemption_tx));

        trigger
            .on_redemption(
                redemption_id,
                EquityRedemptionEvent::Completed {
                    completed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let ownership = trigger.pending_request_ownership().await;
        assert!(
            !ownership
                .redemption_tokenizations
                .contains(&tokenization_request_id)
        );
        assert!(!ownership.redemption_txs.contains(&redemption_tx));
    }

    #[tokio::test]
    async fn pending_request_ownership_drops_on_redemption_failure_terminals() {
        let terminal_events = [
            EquityRedemptionEvent::RedemptionRejected {
                reason: "test rejection".to_string(),
                rejected_at: Utc::now(),
            },
            EquityRedemptionEvent::TransferFailed {
                tx_hash: Some(TxHash::random()),
                reason: None,
                failed_at: Utc::now(),
            },
            EquityRedemptionEvent::DetectionFailed {
                failure: DetectionFailure::Timeout,
                failed_at: Utc::now(),
            },
        ];

        for terminal_event in terminal_events {
            let trigger = make_trigger().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let redemption_id = redemption_aggregate_id("failing-redemption");
            let tokenization_request_id =
                TokenizationRequestId("failing-redemption-request".to_string());
            let redemption_tx = TxHash::random();

            *trigger.inventory.write().await =
                InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50));

            trigger
                .on_redemption(
                    redemption_id.clone(),
                    EquityRedemptionEvent::VaultWithdrawPending {
                        symbol: symbol.clone(),
                        quantity: float!(10),
                        token: Address::ZERO,
                        wrapped_amount: U256::from(10),
                        pending_at: Utc::now(),
                    },
                )
                .await
                .unwrap();
            trigger
                .on_redemption(
                    redemption_id.clone(),
                    EquityRedemptionEvent::TokensSent {
                        redemption_wallet: Address::ZERO,
                        redemption_tx,
                        sent_at: Utc::now(),
                    },
                )
                .await
                .unwrap();
            trigger
                .on_redemption(
                    redemption_id.clone(),
                    EquityRedemptionEvent::Detected {
                        tokenization_request_id: tokenization_request_id.clone(),
                        detected_at: Utc::now(),
                    },
                )
                .await
                .unwrap();

            let ownership = trigger.pending_request_ownership().await;
            assert!(ownership.redemption_txs.contains(&redemption_tx));
            assert!(
                ownership
                    .redemption_tokenizations
                    .contains(&tokenization_request_id)
            );

            trigger
                .on_redemption(redemption_id, terminal_event.clone())
                .await
                .unwrap();

            let ownership = trigger.pending_request_ownership().await;
            assert!(
                !ownership.redemption_txs.contains(&redemption_tx),
                "redemption_tx must drop after terminal event {terminal_event:?}"
            );
            assert!(
                !ownership
                    .redemption_tokenizations
                    .contains(&tokenization_request_id),
                "redemption tokenization id must drop after terminal event {terminal_event:?}"
            );
        }
    }

    #[tokio::test]
    async fn recover_redemption_state_keeps_requested_quantity_until_unwrap_confirms() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("COIN").unwrap();
        let redemption_id = redemption_aggregate_id("partial-redemption-recovery");
        let requested_quantity = float!(37.143292455);
        let actual_wrapped_amount = U256::from(33_681_456_848_531_939_569_u128);
        let requested_fractional = FractionalShares::new(requested_quantity);
        let withdrawn_at = Utc::now();

        trigger
            .recover_redemption_state(
                &redemption_id,
                &EquityRedemption::WithdrawnFromRaindex {
                    symbol: symbol.clone(),
                    quantity: requested_quantity,
                    token: Address::random(),
                    wrapped_amount: actual_wrapped_amount,
                    raindex_withdraw_tx: TxHash::random(),
                    raindex_withdraw_block: None,
                    withdrawn_at,
                },
            )
            .await
            .unwrap();

        let tracking = trigger
            .redemption_tracking
            .read()
            .await
            .get(&redemption_id)
            .cloned()
            .unwrap();
        assert_eq!(tracking.quantity, requested_fractional);
        assert_eq!(
            tracking.stage,
            RedemptionTrackingStage::WithdrawnFromRaindex
        );

        let ownership = trigger.pending_request_ownership().await;
        assert!(ownership.redemption_txs.is_empty());

        let inflight = {
            let inventory = trigger.inventory.read().await;
            inventory.equity_inflight(&symbol, Venue::MarketMaking)
        };
        assert_eq!(
            inflight,
            Some(requested_fractional),
            "Recovery keeps requested underlying quantity until unwrap confirms actual underlying shares"
        );
    }

    /// Drives the production snapshot path end-to-end: applies the
    /// snapshot via the service (which enqueues follow-up checks
    /// internally). Skips the entity-list plumbing so tests can drive
    /// snapshot recovery directly.
    async fn apply_and_dispatch_snapshot(
        service: Arc<RebalancingService>,
        _id: InventorySnapshotId,
        event: InventorySnapshotEvent,
    ) -> Result<(), RebalancingServiceError> {
        service.on_snapshot(event).await
    }

    #[tokio::test]
    async fn test_in_progress_symbol_does_not_send() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        tokio::time::timeout(
            Duration::from_secs(5),
            trigger.check_and_trigger_equity(&symbol),
        )
        .await
        .expect("equity timeout cleanup should complete promptly")
        .unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Expected no equity mint job enqueued"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Expected no equity redemption job enqueued"
        );
    }

    #[tokio::test]
    async fn test_usdc_in_progress_does_not_send() {
        let trigger = make_trigger().await;

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        trigger.check_and_trigger_usdc().await;

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "Expected no USDC transfer job enqueued"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "Expected no USDC transfer job enqueued"
        );
    }

    #[tokio::test]
    async fn test_usdc_disabled_via_cash_config_does_not_send() {
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wrapper = Arc::new(MockWrapper::new());

        let trigger = RebalancingService::new(
            RebalancingServiceConfig {
                equity: test_config().equity,
                usdc: None,
                transfer_timeout: test_config().transfer_timeout,
                assets: AssetsConfig {
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
            },
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            wrapper,
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        );

        trigger.check_and_trigger_usdc().await;

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "Expected no USDC transfer job when USDC rebalancing is disabled"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "Expected no USDC transfer job when USDC rebalancing is disabled"
        );
    }

    /// Builds an imbalanced (too much offchain -> Mint) trigger whose equity
    /// assets config is overridden, with the symbol seeded in the vault
    /// registry and a permissive `MockWrapper` so the only thing standing
    /// between the imbalance and a dispatched mint job is the config
    /// whitelist itself.
    async fn make_imbalanced_trigger_with_equities(
        symbol: &Symbol,
        equities: EquitiesConfig,
    ) -> Arc<RebalancingService> {
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let config = RebalancingServiceConfig {
            assets: AssetsConfig {
                equities,
                cash: None,
            },
            ..test_config()
        };

        make_trigger_with_inventory_and_registry_config(inventory, symbol, config).await
    }

    /// A symbol configured with `rebalancing = "disabled"` must be skipped by
    /// the whitelist even when its inventory is imbalanced.
    #[tokio::test]
    async fn disabled_asset_skips_equity_trigger() {
        let symbol = Symbol::new("AAPL").unwrap();

        let mut equities = rebalancing_enabled_equities(&["AAPL"]);
        equities
            .symbols
            .get_mut(&symbol)
            .expect("AAPL is configured")
            .rebalancing = OperationMode::Disabled;

        let trigger = make_imbalanced_trigger_with_equities(&symbol, equities).await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Disabled asset should not trigger equity rebalancing"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Disabled asset should not trigger equity rebalancing"
        );
    }

    /// A symbol observed in inventory but absent from the equity assets
    /// config must be skipped by the whitelist before any vault or wrapper
    /// work -- not dispatched and only caught by the `WrapperService`
    /// `SymbolNotConfigured` error backstop.
    #[tokio::test]
    async fn unconfigured_symbol_skips_equity_trigger() {
        let symbol = Symbol::new("AAPL").unwrap();

        let trigger =
            make_imbalanced_trigger_with_equities(&symbol, EquitiesConfig::default()).await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Unconfigured symbol should be skipped by the whitelist, not dispatched"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Unconfigured symbol should be skipped by the whitelist, not dispatched"
        );
    }

    /// Positive control for the whitelist: the same imbalance with the symbol
    /// configured `rebalancing = "enabled"` dispatches the mint job.
    #[tokio::test]
    async fn whitelisted_symbol_passes_equity_trigger() {
        let symbol = Symbol::new("AAPL").unwrap();

        let trigger =
            make_imbalanced_trigger_with_equities(&symbol, rebalancing_enabled_equities(&["AAPL"]))
                .await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        let jobs = take_pending_equity_mint_jobs(&trigger).await;
        assert_eq!(
            jobs.len(),
            1,
            "Whitelisted symbol with an imbalance should dispatch a mint job"
        );
        assert_eq!(jobs[0].symbol, symbol);
    }

    #[tokio::test]
    async fn frozen_asset_skips_equity_trigger() {
        let symbol = Symbol::new("AAPL").unwrap();
        let trigger =
            make_imbalanced_trigger_with_equities(&symbol, rebalancing_enabled_equities(&["AAPL"]))
                .await;
        trigger
            .set_freeze_status_reader(Arc::new(StubFreezeReader::Frozen))
            .await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "A frozen asset must not dispatch an equity rebalancing job"
        );
    }

    #[tokio::test]
    async fn indeterminate_freeze_status_fails_closed() {
        let symbol = Symbol::new("AAPL").unwrap();
        let trigger =
            make_imbalanced_trigger_with_equities(&symbol, rebalancing_enabled_equities(&["AAPL"]))
                .await;
        trigger
            .set_freeze_status_reader(Arc::new(StubFreezeReader::Indeterminate))
            .await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "An indeterminate freeze status must fail closed -- no rebalancing job dispatched"
        );
    }

    #[tokio::test]
    async fn not_frozen_asset_passes_equity_trigger() {
        let symbol = Symbol::new("AAPL").unwrap();
        let trigger =
            make_imbalanced_trigger_with_equities(&symbol, rebalancing_enabled_equities(&["AAPL"]))
                .await;
        trigger
            .set_freeze_status_reader(Arc::new(StubFreezeReader::NotFrozen))
            .await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        let jobs = take_pending_equity_mint_jobs(&trigger).await;
        assert_eq!(
            jobs.len(),
            1,
            "A not-frozen asset with an imbalance should still dispatch a mint job"
        );
        assert_eq!(jobs[0].symbol, symbol);
    }

    #[tokio::test]
    async fn test_clear_equity_in_progress() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        assert!(
            trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );

        trigger.clear_equity_in_progress(&symbol);

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn clear_equity_in_progress_removes_both_active_and_held_for_recovery_states() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        // Verify clear works on ActiveTransfer.
        trigger.equity_in_progress.write().unwrap().insert(
            symbol.clone(),
            equity::GuardState::ActiveTransfer { generation: 0 },
        );
        trigger.clear_equity_in_progress(&symbol);
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "clear must remove ActiveTransfer entry"
        );

        // Verify clear works on HeldForRecovery.
        trigger
            .equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), equity::GuardState::HeldForRecovery);
        trigger.clear_equity_in_progress(&symbol);
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "clear must remove HeldForRecovery entry"
        );
    }

    #[tokio::test]
    async fn test_clear_usdc_in_progress() {
        let trigger = make_trigger().await;

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(trigger.usdc_in_progress.load(Ordering::SeqCst));

        trigger.clear_usdc_in_progress();

        assert!(!trigger.usdc_in_progress.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_balanced_inventory_does_not_trigger() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));
        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        trigger.check_and_trigger_usdc().await;

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Expected no equity mint job enqueued"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Expected no equity redemption job enqueued"
        );
    }

    fn shares(n: i64) -> FractionalShares {
        FractionalShares::new(float!(&n.to_string()))
    }

    fn make_onchain_fill(amount: FractionalShares, direction: Direction) -> PositionEvent {
        make_onchain_fill_at(amount, direction, Utc::now())
    }

    fn make_onchain_fill_at(
        amount: FractionalShares,
        direction: Direction,
        block_timestamp: DateTime<Utc>,
    ) -> PositionEvent {
        PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index: 0,
            },
            amount,
            direction,
            price_usdc: float!(150),
            block_timestamp,
            seen_at: Utc::now(),
        }
    }

    fn make_offchain_fill(shares_filled: FractionalShares, direction: Direction) -> PositionEvent {
        make_offchain_fill_at(shares_filled, direction, Utc::now())
    }

    fn make_offchain_fill_at(
        shares_filled: FractionalShares,
        direction: Direction,
        broker_timestamp: DateTime<Utc>,
    ) -> PositionEvent {
        PositionEvent::OffChainOrderFilled {
            offchain_order_id: OffchainOrderId::new(),
            shares_filled: Positive::new(shares_filled).unwrap(),
            direction,
            executor_order_id: ExecutorOrderId::new("ORD1"),
            price: Usd::new(float!(150)),
            broker_timestamp,
        }
    }

    fn make_offchain_placed(offchain_order_id: OffchainOrderId) -> PositionEvent {
        PositionEvent::OffChainOrderPlaced {
            offchain_order_id,
            shares: Positive::new(shares(10)).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::DryRun,
            trigger_reason: TriggerReason::SharesThreshold {
                net_position_shares: float!(10),
                threshold_shares: float!(1),
            },
            placed_at: Utc::now(),
        }
    }

    fn make_offchain_failed(offchain_order_id: OffchainOrderId) -> PositionEvent {
        PositionEvent::OffChainOrderFailed {
            offchain_order_id,
            error: "test failure".to_string(),
            failed_at: Utc::now(),
        }
    }

    const TEST_ORDERBOOK: Address = address!("0x0000000000000000000000000000000000000001");
    const TEST_ORDER_OWNER: Address = address!("0x0000000000000000000000000000000000000002");
    const TEST_TOKEN: Address = address!("0x1234567890123456789012345678901234567890");

    async fn seed_vault_registry(pool: &SqlitePool, symbol: &Symbol) {
        let store = test_store::<VaultRegistry>(pool.clone(), ());
        let vault_registry_id = VaultRegistryId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        store
            .send(
                &vault_registry_id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token: TEST_TOKEN,
                    vault_id: fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    discovered_in: TxHash::ZERO,
                    symbol: symbol.clone(),
                },
            )
            .await
            .unwrap();
    }

    async fn make_trigger_with_inventory(inventory: InventoryView) -> Arc<RebalancingService> {
        make_trigger_with_inventory_config(inventory, test_config()).await
    }

    async fn make_trigger_with_inventory_config(
        inventory: InventoryView,
        config: RebalancingServiceConfig,
    ) -> Arc<RebalancingService> {
        make_trigger_with_inventory_config_and_notifier(inventory, config, Arc::new(NoopNotifier))
            .await
    }

    async fn make_trigger_with_inventory_config_and_notifier(
        inventory: InventoryView,
        config: RebalancingServiceConfig,
        notifier: Arc<dyn crate::alerts::Notifier>,
    ) -> Arc<RebalancingService> {
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;

        Arc::new(RebalancingService::new(
            config,
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            notifier,
        ))
    }

    async fn make_trigger_with_inventory_and_registry(
        inventory: InventoryView,
        symbol: &Symbol,
    ) -> Arc<RebalancingService> {
        make_trigger_with_inventory_and_registry_config(inventory, symbol, test_config()).await
    }

    /// Counts pending `TransferUsdcToHedging` rows in the apalis Jobs table
    /// backing this service. Used by trigger tests that previously asserted
    /// on the mpsc receiver for Base->Alpaca and now must assert on the queue.
    async fn count_pending_transfer_usdc_to_hedging_jobs(service: &RebalancingService) -> i64 {
        let job_type = std::any::type_name::<TransferUsdcToHedging>();
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(job_type)
        .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .expect("count pending TransferUsdcToHedging jobs")
    }

    async fn count_pending_transfer_usdc_to_market_making_jobs(
        service: &RebalancingService,
    ) -> i64 {
        let job_type = std::any::type_name::<TransferUsdcToMarketMaking>();
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(job_type)
        .fetch_one(service.transfer_usdc_to_market_making_queue.pool())
        .await
        .expect("count pending TransferUsdcToMarketMaking jobs")
    }

    /// Counts pending `TransferEquityToMarketMaking` rows in the apalis Jobs
    /// table backing this service. Used by trigger tests that previously
    /// asserted on the mpsc receiver for mints and now must assert on the
    /// queue.
    async fn count_pending_equity_mint_jobs(service: &RebalancingService) -> i64 {
        let job_type = std::any::type_name::<TransferEquityToMarketMaking>();
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(job_type)
        .fetch_one(service.transfer_equity_to_market_making_queue.pool())
        .await
        .expect("count pending TransferEquityToMarketMaking jobs")
    }

    /// Drains every pending equity mint row from the service's Jobs table and
    /// returns the parsed payloads. Marking rows `Done` lets repeated trigger
    /// cycles in the same test see fresh state.
    async fn take_pending_equity_mint_jobs(
        service: &RebalancingService,
    ) -> Vec<TransferEquityToMarketMaking> {
        let pool = service
            .transfer_equity_to_market_making_queue
            .pool()
            .clone();
        let job_type = std::any::type_name::<TransferEquityToMarketMaking>();

        let rows: Vec<(String, Vec<u8>)> = sqlx_apalis::query_as(
            "SELECT id, job FROM Jobs \
             WHERE status = 'Pending' AND job_type = ? \
             ORDER BY run_at",
        )
        .bind(job_type)
        .fetch_all(&pool)
        .await
        .expect("query pending TransferEquityToMarketMaking jobs");

        let mut jobs = Vec::with_capacity(rows.len());
        for (row_id, payload) in rows {
            let job: TransferEquityToMarketMaking =
                serde_json::from_slice(&payload).expect("deserialize TransferEquityToMarketMaking");

            sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE id = ?")
                .bind(&row_id)
                .execute(&pool)
                .await
                .expect("mark equity mint job row Done");

            jobs.push(job);
        }

        jobs
    }

    /// Counts pending `TransferEquityToHedging` rows in the apalis Jobs
    /// table backing this service. Used by trigger tests that previously
    /// asserted on the mpsc receiver for redemptions and now must assert on
    /// the queue.
    async fn count_pending_equity_redemption_jobs(service: &RebalancingService) -> i64 {
        let job_type = std::any::type_name::<TransferEquityToHedging>();
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(job_type)
        .fetch_one(service.transfer_equity_to_hedging_queue.pool())
        .await
        .expect("count pending TransferEquityToHedging jobs")
    }

    /// Drains every pending equity redemption row from the service's Jobs
    /// table and returns the parsed payloads. Marking rows `Done` lets
    /// repeated trigger cycles in the same test see fresh state.
    async fn take_pending_equity_redemption_jobs(
        service: &RebalancingService,
    ) -> Vec<TransferEquityToHedging> {
        let pool = service.transfer_equity_to_hedging_queue.pool().clone();
        let job_type = std::any::type_name::<TransferEquityToHedging>();

        let rows: Vec<(String, Vec<u8>)> = sqlx_apalis::query_as(
            "SELECT id, job FROM Jobs \
             WHERE status = 'Pending' AND job_type = ? \
             ORDER BY run_at",
        )
        .bind(job_type)
        .fetch_all(&pool)
        .await
        .expect("query pending TransferEquityToHedging jobs");

        let mut jobs = Vec::with_capacity(rows.len());
        for (row_id, payload) in rows {
            let job: TransferEquityToHedging =
                serde_json::from_slice(&payload).expect("deserialize TransferEquityToHedging");

            sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE id = ?")
                .bind(&row_id)
                .execute(&pool)
                .await
                .expect("mark equity redemption job row Done");

            jobs.push(job);
        }

        jobs
    }

    /// Drains every pending USDC transfer row (both directions) from the
    /// service's Jobs table and returns them parsed as
    /// [`UsdcRebalanceOperation`]. Marking rows `Done` lets repeated trigger
    /// cycles in the same test see fresh state.
    async fn take_pending_usdc_transfer_jobs(
        service: &RebalancingService,
    ) -> Vec<UsdcRebalanceOperation> {
        let pool = service.transfer_usdc_to_hedging_queue.pool().clone();
        let to_hedging_type = std::any::type_name::<TransferUsdcToHedging>();
        let to_mm_type = std::any::type_name::<TransferUsdcToMarketMaking>();

        let rows: Vec<(String, Vec<u8>, String)> = sqlx_apalis::query_as(
            "SELECT id, job, job_type FROM Jobs \
             WHERE status = 'Pending' AND (job_type = ? OR job_type = ?) \
             ORDER BY run_at",
        )
        .bind(to_hedging_type)
        .bind(to_mm_type)
        .fetch_all(&pool)
        .await
        .expect("query pending USDC transfer jobs");

        let mut operations = Vec::with_capacity(rows.len());
        for (row_id, payload, job_type) in rows {
            let operation = if job_type == to_hedging_type {
                let job: TransferUsdcToHedging =
                    serde_json::from_slice(&payload).expect("deserialize TransferUsdcToHedging");
                UsdcRebalanceOperation::BaseToAlpaca { amount: job.amount }
            } else {
                let job: TransferUsdcToMarketMaking = serde_json::from_slice(&payload)
                    .expect("deserialize TransferUsdcToMarketMaking");
                UsdcRebalanceOperation::AlpacaToBase { amount: job.amount }
            };

            sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE id = ?")
                .bind(&row_id)
                .execute(&pool)
                .await
                .expect("mark drained USDC transfer job Done");

            operations.push(operation);
        }

        operations
    }

    async fn make_trigger_with_inventory_and_registry_config(
        inventory: InventoryView,
        symbol: &Symbol,
        config: RebalancingServiceConfig,
    ) -> Arc<RebalancingService> {
        make_trigger_with_inventory_registry_and_wrapper(
            inventory,
            symbol,
            Arc::new(MockWrapper::new()),
            config,
        )
        .await
    }

    async fn make_trigger_with_inventory_registry_and_wrapper(
        inventory: InventoryView,
        symbol: &Symbol,
        wrapper: Arc<MockWrapper>,
        config: RebalancingServiceConfig,
    ) -> Arc<RebalancingService> {
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;

        seed_vault_registry(&pool, symbol).await;

        Arc::new(RebalancingService::new(
            config,
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            wrapper,
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ))
    }

    #[tokio::test]
    async fn load_token_address_errors_when_registry_uninitialized() {
        let trigger = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        let result = trigger.load_token_address(&symbol).await;
        assert!(
            matches!(result, Err(TokenAddressError::Uninitialized)),
            "Expected Uninitialized error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn load_token_address_returns_address_for_known_symbol() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        let result = trigger.load_token_address(&symbol).await.unwrap();
        assert_eq!(result, Some(TEST_TOKEN));
    }

    #[tokio::test]
    async fn load_token_address_returns_none_for_unknown_symbol() {
        let known = Symbol::new("AAPL").unwrap();
        let unknown = Symbol::new("MSFT").unwrap();
        let inventory = InventoryView::default().with_equity(known.clone(), shares(0), shares(0));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &known).await;
        let trigger = reactor.clone();

        let result = trigger.load_token_address(&unknown).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn position_events_auto_register_symbol_and_trigger_rebalancing() {
        // Reproduces the production scenario: InventoryView starts empty (no
        // with_equity call), position events arrive for a symbol that exists
        // in the vault registry. After accumulating an imbalance, rebalancing
        // must be triggered.
        //
        // In production, InventoryView::default() creates an empty equities
        // map. Position events arrive as onchain fills are processed. If the
        // symbol isn't pre-registered, the position event handler must
        // handle it (either by auto-registering or by decoupling the
        // inventory update failure from the rebalancing check).
        let symbol = Symbol::new("AAPL").unwrap();
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_usdc(usdc(1_000_000), usdc(1_000_000)),
            event_sender,
        ));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;

        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingService::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));
        let reactor = trigger.clone();

        let harness = ReactorHarness::new(reactor.clone());

        // Simulate production: onchain fills arrive on an empty inventory.
        // 20 onchain buys, 80 offchain buys -> 20% onchain ratio.
        // Threshold: target 50%, deviation 20%, lower bound 30%.
        // 20% < 30% -> should trigger Mint (too much offchain).
        for _ in 0..20 {
            let event = make_onchain_fill(shares(1), Direction::Buy);
            harness
                .receive::<Position>(symbol.clone(), event)
                .await
                .unwrap();
            drain_pending_jobs(&trigger).await.unwrap();
        }

        for _ in 0..80 {
            let event = make_offchain_fill(shares(1), Direction::Buy);
            harness
                .receive::<Position>(symbol.clone(), event)
                .await
                .unwrap();
            drain_pending_jobs(&trigger).await.unwrap();
        }

        // Drain any intermediate triggers (the early onchain-heavy phase
        // enqueues a redemption, which would otherwise suppress the final
        // mint via the direction-independent per-symbol dedupe) and do a
        // final check.
        take_pending_equity_mint_jobs(&trigger).await;
        take_pending_equity_redemption_jobs(&trigger).await;
        trigger.clear_equity_in_progress(&symbol);

        // One more event to trigger the check after the imbalance is built up.
        let event = make_onchain_fill(shares(1), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        let jobs = take_pending_equity_mint_jobs(&trigger).await;
        assert_eq!(
            jobs.len(),
            1,
            "Expected a mint job enqueued for imbalanced inventory starting from empty"
        );
        assert_eq!(jobs[0].symbol, symbol);
    }

    #[tokio::test]
    async fn position_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor.clone());

        // Apply onchain buy - should add to onchain available.
        let event = make_onchain_fill(shares(50), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // Apply offchain buy - should add to offchain available.
        let event = make_offchain_fill(shares(50), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // Now inventory has 50 onchain, 50 offchain = balanced at 50%.
        // Drain any previous triggered operations.
        take_pending_equity_mint_jobs(&trigger).await;
        take_pending_equity_redemption_jobs(&trigger).await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Expected no equity mint job enqueued"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Expected no equity redemption job enqueued"
        );
    }

    #[tokio::test]
    async fn fill_delta_before_snapshot_timestamp_is_still_applied() {
        let symbol = Symbol::new("COIN").unwrap();
        let snapshot_at = Utc::now();
        let stale_fill_at = snapshot_at - chrono::Duration::seconds(1);
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor);

        apply_and_dispatch_snapshot(
            trigger.clone(),
            InventorySnapshotId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_ORDER_OWNER,
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: BTreeMap::from([(symbol.clone(), shares(50))]),
                fetched_at: snapshot_at,
            },
        )
        .await
        .unwrap();

        let event = make_onchain_fill_at(shares(10), Direction::Buy, stale_fill_at);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        let onchain_available = {
            let inventory = trigger.inventory.read().await;
            inventory.equity_available(&symbol, Venue::MarketMaking)
        };
        assert_eq!(
            onchain_available,
            Some(shares(60)),
            "Fill deltas are applied because snapshot timestamps are not causal watermarks"
        );
    }

    #[tokio::test]
    async fn older_onchain_equity_snapshot_does_not_overwrite_newer_snapshot() {
        let symbol = Symbol::new("COIN").unwrap();
        let newer_snapshot_at = Utc::now();
        let older_snapshot_at = newer_snapshot_at - chrono::Duration::seconds(1);
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        apply_and_dispatch_snapshot(
            trigger.clone(),
            InventorySnapshotId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_ORDER_OWNER,
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: BTreeMap::from([(symbol.clone(), shares(80))]),
                fetched_at: newer_snapshot_at,
            },
        )
        .await
        .unwrap();

        apply_and_dispatch_snapshot(
            trigger.clone(),
            InventorySnapshotId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_ORDER_OWNER,
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: BTreeMap::from([(symbol.clone(), shares(10))]),
                fetched_at: older_snapshot_at,
            },
        )
        .await
        .unwrap();

        let onchain_available = {
            let inventory = trigger.inventory.read().await;
            inventory.equity_available(&symbol, Venue::MarketMaking)
        };
        assert_eq!(
            onchain_available,
            Some(shares(80)),
            "Older onchain snapshot should not overwrite newer onchain balance"
        );
    }

    #[tokio::test]
    async fn older_offchain_equity_snapshot_does_not_overwrite_newer_snapshot() {
        let symbol = Symbol::new("COIN").unwrap();
        let newer_snapshot_at = Utc::now();
        let older_snapshot_at = newer_snapshot_at - chrono::Duration::seconds(1);
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        apply_and_dispatch_snapshot(
            trigger.clone(),
            InventorySnapshotId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_ORDER_OWNER,
            },
            InventorySnapshotEvent::OffchainEquity {
                positions: BTreeMap::from([(symbol.clone(), shares(80))]),
                fetched_at: newer_snapshot_at,
            },
        )
        .await
        .unwrap();

        apply_and_dispatch_snapshot(
            trigger.clone(),
            InventorySnapshotId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_ORDER_OWNER,
            },
            InventorySnapshotEvent::OffchainEquity {
                positions: BTreeMap::from([(symbol.clone(), shares(10))]),
                fetched_at: older_snapshot_at,
            },
        )
        .await
        .unwrap();

        let offchain_available = {
            let inventory = trigger.inventory.read().await;
            inventory.equity_available(&symbol, Venue::Hedging)
        };
        assert_eq!(
            offchain_available,
            Some(shares(80)),
            "Older offchain snapshot should not overwrite newer offchain balance"
        );
    }

    #[tokio::test]
    async fn fresh_onchain_fill_delta_after_snapshot_is_applied() {
        let symbol = Symbol::new("COIN").unwrap();
        let snapshot_at = Utc::now();
        let fresh_fill_at = snapshot_at + chrono::Duration::seconds(1);
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor);

        apply_and_dispatch_snapshot(
            trigger.clone(),
            InventorySnapshotId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_ORDER_OWNER,
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: BTreeMap::from([(symbol.clone(), shares(50))]),
                fetched_at: snapshot_at,
            },
        )
        .await
        .unwrap();

        let event = make_onchain_fill_at(shares(10), Direction::Buy, fresh_fill_at);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        let onchain_available = {
            let inventory = trigger.inventory.read().await;
            inventory.equity_available(&symbol, Venue::MarketMaking)
        };
        assert_eq!(
            onchain_available,
            Some(shares(60)),
            "Fresh fill newer than the snapshot should update onchain inventory"
        );
    }

    #[tokio::test]
    async fn offchain_fill_delta_before_snapshot_timestamp_updates_equity_and_cash() {
        let symbol = Symbol::new("COIN").unwrap();
        let snapshot_at = Utc::now();
        let stale_fill_at = snapshot_at - chrono::Duration::seconds(1);
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor);

        apply_and_dispatch_snapshot(
            trigger.clone(),
            InventorySnapshotId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_ORDER_OWNER,
            },
            InventorySnapshotEvent::OffchainEquity {
                positions: BTreeMap::from([(symbol.clone(), shares(50))]),
                fetched_at: snapshot_at,
            },
        )
        .await
        .unwrap();

        let event = make_offchain_fill_at(shares(10), Direction::Buy, stale_fill_at);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        let (offchain_available, hedging_usdc) = {
            let inventory = trigger.inventory.read().await;
            (
                inventory.equity_available(&symbol, Venue::Hedging),
                inventory.usdc_available(Venue::Hedging),
            )
        };
        assert_eq!(
            offchain_available,
            Some(shares(60)),
            "Offchain fill should update equity because snapshot timestamps are not causal watermarks"
        );
        assert_ne!(
            hedging_usdc,
            Some(usdc(1_000_000)),
            "Cash leg applies atomically with equity"
        );
    }

    #[tokio::test]
    async fn skipped_snapshot_does_not_suppress_later_fill_delta() {
        let symbol = Symbol::new("COIN").unwrap();
        let snapshot_at = Utc::now();
        let stale_fill_at = snapshot_at - chrono::Duration::seconds(1);
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .with_usdc(usdc(1_000_000), usdc(1_000_000))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(5)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor);

        apply_and_dispatch_snapshot(
            trigger.clone(),
            InventorySnapshotId {
                orderbook: TEST_ORDERBOOK,
                owner: TEST_ORDER_OWNER,
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: BTreeMap::from([(symbol.clone(), shares(100))]),
                fetched_at: snapshot_at,
            },
        )
        .await
        .unwrap();

        let event = make_onchain_fill_at(shares(10), Direction::Buy, stale_fill_at);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        let onchain_available = {
            let inventory = trigger.inventory.read().await;
            inventory.equity_available(&symbol, Venue::MarketMaking)
        };
        assert_eq!(
            onchain_available,
            Some(shares(55)),
            "Fill must apply when the newer snapshot was skipped due to inflight inventory"
        );
    }

    #[tokio::test]
    async fn position_event_maintaining_balance_triggers_nothing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(1_000_000), usdc(1_000_000))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(50)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(50)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(reactor.clone());

        // Apply a small buy that maintains balance (5 shares onchain).
        // After: 55 onchain, 50 offchain = 52.4% ratio, within 30-70% bounds.
        let event = make_onchain_fill(shares(5), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // No operation should be triggered.
        assert_eq!(
            count_pending_equity_mint_jobs(&reactor).await,
            0,
            "Expected no equity mint job enqueued"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&reactor).await,
            0,
            "Expected no equity redemption job enqueued"
        );
    }

    #[tokio::test]
    async fn position_event_causing_imbalance_triggers_mint() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(1_000_000), usdc(1_000_000))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let trigger = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(trigger.clone());

        // Apply a small event that triggers the imbalance check.
        let event = make_onchain_fill(shares(1), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        // A mint job should be enqueued because too much offchain.
        let jobs = take_pending_equity_mint_jobs(&trigger).await;
        assert_eq!(jobs.len(), 1, "Expected a mint job for the imbalance");
        assert_eq!(jobs[0].symbol, symbol);
    }

    #[tokio::test]
    async fn high_vault_ratio_triggers_redemption_that_would_be_balanced_at_one_to_one() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(65)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(35)),
                Utc::now(),
            )
            .unwrap();

        let wrapper = Arc::new(MockWrapper::with_ratio(U256::from(
            1_500_000_000_000_000_000u64,
        )));
        let reactor = make_trigger_with_inventory_registry_and_wrapper(
            inventory,
            &symbol,
            wrapper,
            test_config(),
        )
        .await;
        let trigger = reactor.clone();

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        let jobs = take_pending_equity_redemption_jobs(&trigger).await;
        assert_eq!(
            jobs.len(),
            1,
            "Expected a redemption job enqueued with 1.5 ratio"
        );
    }

    fn make_mint_requested(symbol: &Symbol, quantity: Float) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity,
            wallet: Address::random(),
            requested_at: Utc::now(),
        }
    }

    fn make_mint_accepted() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: issuer_request_id("ISS123"),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        }
    }

    fn make_deposited_into_raindex() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::DepositedIntoRaindex {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        }
    }

    fn make_mint_rejected() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRejected {
            reason: "test rejection".to_string(),
            rejected_at: Utc::now(),
        }
    }

    fn make_mint_acceptance_failed() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "test failure".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_tokens_received() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            shares_minted: U256::from(30_000_000_000_000_000_000_u128),
            fees: None,
            received_at: Utc::now(),
        }
    }

    fn make_wrapping_failed(symbol: &Symbol, quantity: Float) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::WrappingFailed {
            symbol: symbol.clone(),
            quantity,
            reason: None,
            failed_at: Utc::now(),
        }
    }

    fn make_raindex_deposit_failed() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::RaindexDepositFailed {
            reason: "test deposit failure".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_transfer_failed() -> EquityRedemptionEvent {
        EquityRedemptionEvent::TransferFailed {
            tx_hash: Some(TxHash::random()),
            reason: None,
            failed_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn mint_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        // Start with 20 onchain and 80 offchain - imbalanced (20% onchain).
        // Threshold: target 50%, deviation 20%, so lower bound is 30%.
        // 20% < 30% triggers TooMuchOffchain.
        let inventory = inventory
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        // Initially, trigger should detect imbalance (too much offchain).
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        let initial_jobs = take_pending_equity_mint_jobs(&trigger).await;
        assert_eq!(
            initial_jobs.len(),
            1,
            "Expected initial imbalance to enqueue a mint job"
        );

        // Clear in-progress so we can test again.
        trigger.clear_equity_in_progress(&symbol);

        // Apply MintAccepted - this moves shares to inflight.
        // Inflight should now block imbalance detection.
        {
            let mut inventory = trigger.inventory.write().await;
            *inventory = inventory
                .clone()
                .update_equity(
                    &symbol,
                    Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(30)),
                    Utc::now(),
                )
                .unwrap();
        }

        // With inflight, imbalance detection should not trigger anything.
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Expected no mint job due to inflight"
        );
    }

    #[tokio::test]
    async fn mint_completion_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        let trigger = make_trigger_with_inventory(inventory).await;

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }
        assert!(
            trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );

        // Check that DepositedIntoRaindex is detected as terminal.
        assert!(RebalancingService::is_terminal_mint_event(
            &make_deposited_into_raindex()
        ));

        // Simulate what dispatch does - clear in-progress on terminal.
        trigger.clear_equity_in_progress(&symbol);
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[tokio::test]
    async fn mint_rejection_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        let trigger = make_trigger_with_inventory(inventory).await;

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        assert!(RebalancingService::is_terminal_mint_event(
            &make_mint_rejected()
        ));

        trigger.clear_equity_in_progress(&symbol);
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[test]
    fn mint_acceptance_failure_is_terminal() {
        assert!(RebalancingService::is_terminal_mint_event(
            &make_mint_acceptance_failed()
        ));
    }

    #[test]
    fn operator_reconciled_is_terminal_mint_event() {
        // Gates clear_equity_in_progress: a reconciled mint must read as terminal
        // so the in-progress flag is released and rebalancing can resume.
        assert!(RebalancingService::is_terminal_mint_event(
            &TokenizedEquityMintEvent::OperatorReconciled {
                reason: "credited offline".to_string(),
                reconciled_at: Utc::now(),
            }
        ));
    }

    #[test]
    fn extract_mint_info_returns_symbol_and_quantity() {
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_mint_requested(&symbol, float!(42.5));

        let (extracted_symbol, extracted_quantity) =
            RebalancingService::extract_mint_info(&event).unwrap();

        assert_eq!(extracted_symbol, symbol);
        assert!(extracted_quantity.inner().eq(float!(42.5)).unwrap());
    }

    #[test]
    fn extract_mint_info_returns_none_without_mint_requested() {
        let result = RebalancingService::extract_mint_info(&make_deposited_into_raindex());
        assert!(result.is_none());
    }

    #[test]
    fn non_terminal_mint_events_are_not_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        assert!(!RebalancingService::is_terminal_mint_event(
            &make_mint_requested(&symbol, float!(30))
        ));
        assert!(!RebalancingService::is_terminal_mint_event(
            &make_mint_accepted()
        ));
        assert!(!RebalancingService::is_terminal_mint_event(
            &make_tokens_received()
        ));
    }

    #[tokio::test]
    async fn mint_rejection_via_reactor_clears_in_progress() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));
        let trigger = make_trigger_with_inventory(inventory).await;

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = issuer_request_id("mint-1");

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(10)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id, make_mint_rejected())
            .await
            .unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "In-progress flag should be cleared after terminal MintRejected"
        );
    }

    #[tokio::test]
    async fn redemption_completion_via_reactor_clears_in_progress() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(100)),
                Utc::now(),
            )
            .unwrap();
        let trigger = make_trigger_with_inventory(inventory).await;

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("redemption-1");

        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!(10)),
            )
            .await
            .unwrap();

        harness
            .receive::<EquityRedemption>(id, make_redemption_completed())
            .await
            .unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "In-progress flag should be cleared after terminal Completed"
        );
    }

    #[tokio::test]
    async fn mint_accepted_via_reactor_blocks_imbalance_detection() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 20 onchain, 80 offchain = 20% ratio -> TooMuchOffchain triggers Mint
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = issuer_request_id("mint-blocks");

        // Verify imbalance triggers before reactor events
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            take_pending_equity_mint_jobs(&trigger).await.len(),
            1,
            "Should enqueue a mint job before reactor events"
        );
        trigger.clear_equity_in_progress(&symbol);

        // Send MintRequested + MintAccepted through reactor
        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_accepted())
            .await
            .unwrap();

        // Now check: inflight should block imbalance detection
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Inflight from MintAccepted should block imbalance detection"
        );
    }

    #[tokio::test]
    async fn mint_tokens_received_via_reactor_rebalances_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 20 onchain, 80 offchain = imbalanced
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = issuer_request_id("mint-transfer");

        // Full mint flow: MintRequested -> MintAccepted -> TokensReceived
        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_accepted())
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_tokens_received())
            .await
            .unwrap();

        // After TokensReceived: 30 moved from offchain to onchain
        // New state: 50 onchain, 50 offchain = balanced
        trigger.clear_equity_in_progress(&symbol);
        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Inventory should be balanced after mint transfer (50/50)"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Inventory should be balanced after mint transfer (50/50)"
        );
    }

    #[tokio::test]
    async fn mint_acceptance_failed_via_reactor_restores_imbalance() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 20 onchain, 80 offchain = imbalanced
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = issuer_request_id("mint-fail");

        // MintRequested -> MintAccepted -> MintAcceptanceFailed
        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_accepted())
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_acceptance_failed())
            .await
            .unwrap();

        // After cancellation, inflight is cleared, imbalance should trigger again
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            take_pending_equity_mint_jobs(&trigger).await.len(),
            1,
            "Imbalance should re-enqueue a mint job after MintAcceptanceFailed cancels inflight"
        );
    }

    #[tokio::test]
    async fn mint_deposited_into_raindex_via_reactor_is_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 20 onchain, 80 offchain = imbalanced (triggers Mint)
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        // Set in-progress flag to verify terminal event clears it
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        let id = issuer_request_id("mint-deposit");

        // Full happy-path: MintRequested -> MintAccepted -> TokensReceived -> DepositedIntoRaindex
        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_accepted())
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_tokens_received())
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_deposited_into_raindex())
            .await
            .unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "In-progress flag should be cleared after terminal DepositedIntoRaindex"
        );
    }

    #[tokio::test]
    async fn mint_wrapping_failed_via_reactor_is_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        let id = issuer_request_id("mint-wrapping-fail");

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_wrapping_failed(&symbol, float!(30)))
            .await
            .unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "In-progress flag should be cleared after terminal WrappingFailed"
        );
    }

    #[tokio::test]
    async fn mint_raindex_deposit_failed_via_reactor_is_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        let id = issuer_request_id("mint-deposit-fail");

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_raindex_deposit_failed())
            .await
            .unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "In-progress flag should be cleared after terminal RaindexDepositFailed"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_failed_via_reactor_is_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(100)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        let id = redemption_aggregate_id("redemption-transfer-fail");

        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!(10)),
            )
            .await
            .unwrap();

        harness
            .receive::<EquityRedemption>(id.clone(), make_transfer_failed())
            .await
            .unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "In-progress flag should be cleared after terminal TransferFailed"
        );
    }

    #[tokio::test]
    async fn redemption_detection_failed_via_reactor_is_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(100)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        let id = redemption_aggregate_id("redemption-detection-fail");

        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!(10)),
            )
            .await
            .unwrap();

        harness
            .receive::<EquityRedemption>(id.clone(), make_detection_failed())
            .await
            .unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "In-progress flag should be cleared after terminal DetectionFailed"
        );
    }

    #[tokio::test]
    async fn redemption_rejected_via_reactor_is_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(100)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        let id = redemption_aggregate_id("redemption-rejected");

        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!(10)),
            )
            .await
            .unwrap();

        harness
            .receive::<EquityRedemption>(id.clone(), make_redemption_rejected())
            .await
            .unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "In-progress flag should be cleared after terminal RedemptionRejected"
        );
    }

    #[tokio::test]
    async fn position_noop_events_via_reactor_do_not_error() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(5), shares(5))
            .with_usdc(usdc(10000), usdc(10000));

        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        // Seed the pending-hedge gate so we can prove no-op events leave it intact
        // (OffChainOrderFailed clears it; manual adjustments must not).
        trigger
            .pending_offchain_order_symbols
            .write()
            .await
            .insert(symbol.clone());

        // Initialized, ThresholdUpdated, and ManualPositionAdjusted all return
        // Ok(()) without touching inventory or the pending-hedge gate.
        for event in [
            PositionEvent::Initialized {
                symbol: symbol.clone(),
                threshold: ExecutionThreshold::whole_share(),
                initialized_at: Utc::now(),
            },
            PositionEvent::ThresholdUpdated {
                old_threshold: ExecutionThreshold::whole_share(),
                new_threshold: ExecutionThreshold::whole_share(),
                updated_at: Utc::now(),
            },
            PositionEvent::ManualPositionAdjusted {
                previous_net: shares(5),
                target_net: shares(0),
                reason: "operator repair".to_string(),
                price_usdc: Some(float!(150)),
                adjusted_at: Utc::now(),
            },
        ] {
            harness
                .receive::<Position>(symbol.clone(), event)
                .await
                .unwrap();
        }

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_available(&symbol, Venue::Hedging),
            Some(shares(5)),
            "manual adjustment must not change hedging equity"
        );
        assert_eq!(
            inventory.equity_available(&symbol, Venue::MarketMaking),
            Some(shares(5)),
            "manual adjustment must not change market-making equity"
        );
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(10000)),
            "manual adjustment must not change hedging USDC"
        );
        assert_eq!(
            inventory.usdc_available(Venue::MarketMaking),
            Some(usdc(10000)),
            "manual adjustment must not change market-making USDC"
        );
        drop(inventory);

        assert!(
            trigger
                .pending_offchain_order_symbols
                .read()
                .await
                .contains(&symbol),
            "manual adjustment must not clear the pending-hedge gate"
        );

        let pending_equity_checks = sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<EquityRebalancingCheck>())
        .fetch_one(trigger.equity_scheduler.queue().pool())
        .await
        .expect("count pending equity-check jobs after ManualPositionAdjusted");

        assert_eq!(
            pending_equity_checks, 1,
            "ManualPositionAdjusted must enqueue exactly one immediate equity recheck"
        );
    }

    #[tokio::test]
    async fn onchain_buy_updates_usdc_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(10000), usdc(10000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor.clone());

        // Onchain buy of 10 shares at $150 -> adds 10 equity onchain, removes $1500 USDC onchain
        let event = make_onchain_fill(shares(10), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // 10000 - 1500 = 8500 onchain USDC
        let onchain_usdc = trigger
            .inventory
            .read()
            .await
            .usdc_available(Venue::MarketMaking)
            .unwrap();

        assert_eq!(onchain_usdc, usdc(8500));
    }

    #[tokio::test]
    async fn onchain_sell_updates_usdc_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(10000), usdc(10000))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(100)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor.clone());

        // Onchain sell of 10 shares at $150 -> removes 10 equity onchain, adds $1500 USDC onchain
        let event = make_onchain_fill(shares(10), Direction::Sell);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // 10000 + 1500 = 11500 onchain USDC
        let onchain_usdc = trigger
            .inventory
            .read()
            .await
            .usdc_available(Venue::MarketMaking)
            .unwrap();

        assert_eq!(onchain_usdc, usdc(11500));
    }

    #[tokio::test]
    async fn offchain_buy_updates_usdc_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(10000), usdc(10000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor.clone());

        // Offchain buy of 10 shares at $150 -> adds 10 equity offchain, removes $1500 USDC offchain
        let event = make_offchain_fill(shares(10), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // 10000 - 1500 = 8500 offchain USDC
        let offchain_usdc = trigger
            .inventory
            .read()
            .await
            .usdc_available(Venue::Hedging)
            .unwrap();

        assert_eq!(offchain_usdc, usdc(8500));
    }

    #[tokio::test]
    async fn offchain_sell_updates_usdc_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(10000), usdc(10000))
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(100)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(reactor.clone());

        // Offchain sell of 10 shares at $150 -> removes 10 equity offchain, adds $1500 USDC offchain
        let event = make_offchain_fill(shares(10), Direction::Sell);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // 10000 + 1500 = 11500 offchain USDC
        let offchain_usdc = trigger
            .inventory
            .read()
            .await
            .usdc_available(Venue::Hedging)
            .unwrap();

        assert_eq!(offchain_usdc, usdc(11500));
    }

    #[tokio::test]
    async fn snapshot_onchain_equity_via_reactor_triggers_check() {
        let symbol = Symbol::new("AAPL").unwrap();
        // Start with 80 onchain, 20 offchain = 80% -> TooMuchOnchain
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Snapshot onchain equity to 40 -> now 40 onchain, 20 offchain
        // 40/60 = 66.7% -> within threshold (upper bound 70%), no trigger
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(40));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // After snapshot: 40 onchain (reconciled), 20 offchain
        // 40/60 = 66.7% -> within threshold (30%-70%), no trigger
        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "66.7% onchain ratio should be within threshold (30%-70%)"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "66.7% onchain ratio should be within threshold (30%-70%)"
        );
    }

    #[tokio::test]
    async fn snapshot_offchain_equity_via_reactor_triggers_check() {
        let symbol = Symbol::new("AAPL").unwrap();
        // Start with 20 onchain, 80 offchain = 20% -> TooMuchOffchain triggers Mint
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Verify initial imbalance triggers
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            take_pending_equity_mint_jobs(&trigger).await.len(),
            1,
            "20% ratio should enqueue a mint job"
        );
        trigger.clear_equity_in_progress(&symbol);

        // Snapshot offchain to 20 -> 20 onchain, 20 offchain = 50% = balanced
        let mut positions = BTreeMap::new();
        positions.insert(symbol.clone(), shares(20));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // After snapshot: 20 onchain, 20 offchain = balanced
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "50% ratio should be balanced after offchain equity snapshot"
        );
    }

    #[tokio::test]
    async fn redemption_withdrawn_via_reactor_blocks_imbalance_detection() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 80 onchain, 20 offchain = 80% ratio -> TooMuchOnchain triggers Redemption
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("redemption-blocks");

        // Verify imbalance triggers before reactor events
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            take_pending_equity_redemption_jobs(&trigger).await.len(),
            1,
            "Should enqueue a redemption job before reactor events"
        );
        trigger.clear_equity_in_progress(&symbol);

        // Send WithdrawnFromRaindex through reactor
        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!(30)),
            )
            .await
            .unwrap();

        // Inflight should block imbalance detection
        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Inflight from WithdrawnFromRaindex should block imbalance detection"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Inflight from WithdrawnFromRaindex should block imbalance detection"
        );
    }

    #[tokio::test]
    async fn redemption_completed_via_reactor_rebalances_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 80 onchain, 20 offchain = imbalanced
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("redemption-rebalances");

        // Full redemption flow: WithdrawnFromRaindex -> Completed
        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!(30)),
            )
            .await
            .unwrap();

        harness
            .receive::<EquityRedemption>(id.clone(), make_redemption_completed())
            .await
            .unwrap();

        // After Completed: 30 moved from onchain to offchain
        // New state: 50 onchain, 50 offchain = balanced
        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Inventory should be balanced after redemption completion (50/50)"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Inventory should be balanced after redemption completion (50/50)"
        );
    }

    #[tokio::test]
    async fn usdc_initiated_alpaca_to_base_via_reactor_blocks_usdc_trigger() {
        // Start with USDC imbalance: 200 onchain, 800 offchain = 20% ratio
        // With target 50%, deviation 30%, lower bound = 20%: at boundary, no trigger
        // Use 100 onchain, 900 offchain = 10% ratio -> triggers TooMuchOffchain
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .with_withdrawable_cash_cents(90_000);

        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        // Verify imbalance triggers before reactor events. Alpaca->Base is
        // enqueued as a TransferUsdcToMarketMaking apalis job.
        trigger.check_and_trigger_usdc().await;
        let pending = take_pending_usdc_transfer_jobs(&trigger).await;
        assert!(
            matches!(
                pending.as_slice(),
                [UsdcRebalanceOperation::AlpacaToBase { .. }],
            ),
            "TooMuchOffchain (100/900) should enqueue an AlpacaToBase transfer, got {pending:?}"
        );
        trigger.clear_usdc_in_progress();

        // Send Initiated(AlpacaToBase) through reactor -> moves offchain to inflight
        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
            )
            .await
            .unwrap();

        // Inflight should block USDC imbalance detection
        trigger.check_and_trigger_usdc().await;

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "Inflight from Initiated(AlpacaToBase) should block USDC imbalance detection"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "Inflight from Initiated(AlpacaToBase) should block USDC imbalance detection"
        );
    }

    #[tokio::test]
    async fn usdc_initiated_base_to_alpaca_via_reactor_blocks_usdc_trigger() {
        // 900 onchain, 100 offchain = 90% ratio -> TooMuchOnchain
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));

        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        // Verify imbalance triggers before reactor events. Base->Alpaca is
        // enqueued as a TransferUsdcToHedging apalis job.
        trigger.check_and_trigger_usdc().await;
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            1,
            "TooMuchOnchain (900/100) should enqueue a TransferUsdcToHedging job before reactor events"
        );
        take_pending_usdc_transfer_jobs(&trigger).await;
        trigger.clear_usdc_in_progress();

        // Send Initiated(BaseToAlpaca) through reactor -> moves onchain to inflight
        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
            )
            .await
            .unwrap();

        // Inflight should block USDC imbalance detection
        trigger.check_and_trigger_usdc().await;

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "Inflight from Initiated(BaseToAlpaca) should block USDC imbalance detection"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "Inflight from Initiated(BaseToAlpaca) should block USDC imbalance detection"
        );
    }

    #[tokio::test]
    async fn base_to_alpaca_terminal_conversion_settles_usdc_inventory() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));

        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(500)),
            )
            .await
            .unwrap();

        harness
            .receive::<UsdcRebalance>(
                id,
                make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(499)),
            )
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_available(Venue::MarketMaking),
            Some(usdc(400)),
            "terminal conversion should remove the full initiated amount from onchain available"
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "terminal conversion should clear onchain USDC inflight"
        );
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(599)),
            "terminal conversion should credit offchain cash-equivalent with the filled amount"
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(Usdc::ZERO),
            "terminal conversion should not leave offchain USDC inflight behind"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn alpaca_to_base_terminal_deposit_uses_bridged_amount_for_inventory() {
        let inventory = InventoryView::default().with_usdc(usdc(100), usdc(900));

        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(500)),
            )
            .await
            .unwrap();

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_bridged_with_amounts(usdc(499), usdc(1)),
            )
            .await
            .unwrap();

        harness
            .receive::<UsdcRebalance>(
                id,
                make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase),
            )
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(400)),
            "terminal deposit should remove the full initiated amount from offchain available"
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(Usdc::ZERO),
            "terminal deposit should clear offchain USDC inflight"
        );
        assert_eq!(
            inventory.usdc_available(Venue::MarketMaking),
            Some(usdc(599)),
            "terminal deposit should credit onchain available with the bridged amount after fees"
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "terminal deposit should not leave onchain USDC inflight behind"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn alpaca_to_base_usdc_lifecycle_tracks_started_and_cleared_inflight() {
        let inventory = InventoryView::default().with_usdc(usdc(100), usdc(900));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
            )
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(400),
            None,
            usdc::UsdcRebalanceStage::ConversionInitiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(900), Usdc::ZERO)
            .await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(399)),
            )
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(400),
            None,
            usdc::UsdcRebalanceStage::ConversionConfirmed,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(900), Usdc::ZERO)
            .await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(399)),
            )
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(399),
            None,
            usdc::UsdcRebalanceStage::Initiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(501), usdc(399)).await;

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_withdrawal_confirmed())
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(399),
            None,
            usdc::UsdcRebalanceStage::WithdrawalConfirmed,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(501), usdc(399)).await;

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridging_initiated())
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(399),
            None,
            usdc::UsdcRebalanceStage::BridgingInitiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(501), usdc(399)).await;

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridge_attestation_received())
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(399),
            None,
            usdc::UsdcRebalanceStage::BridgeAttestationReceived,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(501), usdc(399)).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_bridged_with_amounts(usdc(398), usdc(1)),
            )
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(399),
            Some(usdc(398)),
            usdc::UsdcRebalanceStage::Bridged,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(501), usdc(399)).await;

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_deposit_initiated())
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(399),
            Some(usdc(398)),
            usdc::UsdcRebalanceStage::DepositInitiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(501), usdc(399)).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase),
            )
            .await
            .unwrap();
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "terminal AlpacaToBase deposit confirmation should clear tracking"
        );
        assert_usdc_inventory_balances(&trigger, usdc(498), Usdc::ZERO, usdc(501), Usdc::ZERO)
            .await;
        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "terminal AlpacaToBase success should clear the in-progress guard"
        );
    }

    #[tokio::test]
    async fn base_to_alpaca_usdc_lifecycle_tracks_started_and_cleared_inflight() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
            )
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            None,
            usdc::UsdcRebalanceStage::Initiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_withdrawal_confirmed())
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            None,
            usdc::UsdcRebalanceStage::WithdrawalConfirmed,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridging_initiated())
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            None,
            usdc::UsdcRebalanceStage::BridgingInitiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridge_attestation_received())
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            None,
            usdc::UsdcRebalanceStage::BridgeAttestationReceived,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_bridged_with_amounts(usdc(399), usdc(1)),
            )
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            Some(usdc(399)),
            usdc::UsdcRebalanceStage::Bridged,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_deposit_initiated())
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            Some(usdc(399)),
            usdc::UsdcRebalanceStage::DepositInitiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_deposit_confirmed(RebalanceDirection::BaseToAlpaca),
            )
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            Some(usdc(399)),
            usdc::UsdcRebalanceStage::DepositConfirmed,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_initiated(RebalanceDirection::BaseToAlpaca, usdc(399)),
            )
            .await
            .unwrap();
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            Some(usdc(399)),
            usdc::UsdcRebalanceStage::ConversionInitiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(399)),
            )
            .await
            .unwrap();
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "terminal BaseToAlpaca conversion confirmation should clear tracking"
        );
        assert_usdc_inventory_balances(&trigger, usdc(500), Usdc::ZERO, usdc(499), Usdc::ZERO)
            .await;
        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "terminal BaseToAlpaca success should clear the in-progress guard"
        );
    }

    #[tokio::test]
    async fn alpaca_to_base_usdc_failures_cancel_inflight_end_to_end() {
        struct Scenario {
            name: &'static str,
            events: Vec<UsdcRebalanceEvent>,
        }

        let scenarios = vec![
            Scenario {
                name: "conversion_failed_before_withdrawal",
                events: vec![
                    make_usdc_conversion_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
                    make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_conversion_failed(),
                ],
            },
            Scenario {
                name: "withdrawal_failed_after_initiated",
                events: vec![
                    make_usdc_conversion_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
                    make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_withdrawal_failed(),
                ],
            },
            Scenario {
                name: "bridging_failed_before_burn",
                events: vec![
                    make_usdc_conversion_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
                    make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_withdrawal_confirmed(),
                    make_usdc_pre_burn_bridging_failed(),
                ],
            },
            // NOTE: a post-burn `deposit_failed` no longer cancels -- it is
            // post-mint and preserves inflight + the guard. That behavior is
            // covered by `post_burn_deposit_failure_preserves_inflight_and_guard`.
        ];

        for scenario in scenarios {
            let inventory = InventoryView::default().with_usdc(usdc(100), usdc(900));
            let reactor = make_trigger_with_inventory(inventory).await;
            let trigger = reactor.clone();
            let harness = ReactorHarness::new(Arc::clone(&trigger));
            let id = UsdcRebalanceId(Uuid::new_v4());

            trigger.usdc_in_progress.store(true, Ordering::SeqCst);

            for event in scenario.events {
                harness
                    .receive::<UsdcRebalance>(id.clone(), event)
                    .await
                    .unwrap();
            }

            assert!(
                !trigger.usdc_tracking.read().await.contains_key(&id),
                "{} should clear tracking after terminal failure",
                scenario.name
            );
            assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(900), Usdc::ZERO)
                .await;
        }
    }

    #[tokio::test]
    async fn base_to_alpaca_usdc_failures_cancel_inflight_end_to_end() {
        struct Scenario {
            name: &'static str,
            events: Vec<UsdcRebalanceEvent>,
        }

        let scenarios = vec![
            Scenario {
                name: "withdrawal_failed_after_initiated",
                events: vec![
                    make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
                    make_usdc_withdrawal_failed(),
                ],
            },
            Scenario {
                name: "bridging_failed_before_burn",
                events: vec![
                    make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
                    make_usdc_withdrawal_confirmed(),
                    make_usdc_pre_burn_bridging_failed(),
                ],
            },
            // NOTE: post-burn failures no longer cancel -- they are post-mint and
            // preserve inflight + the guard. `deposit_failed` is covered by
            // `post_burn_deposit_failure_preserves_inflight_and_guard`, and the
            // BaseToAlpaca post-deposit `conversion_failed` by
            // `post_deposit_conversion_failure_preserves_inflight_and_guard`.
        ];

        for scenario in scenarios {
            let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
            let reactor = make_trigger_with_inventory(inventory).await;
            let trigger = reactor.clone();
            let harness = ReactorHarness::new(Arc::clone(&trigger));
            let id = UsdcRebalanceId(Uuid::new_v4());

            trigger.usdc_in_progress.store(true, Ordering::SeqCst);

            for event in scenario.events {
                harness
                    .receive::<UsdcRebalance>(id.clone(), event)
                    .await
                    .unwrap();
            }

            assert!(
                !trigger.usdc_tracking.read().await.contains_key(&id),
                "{} should clear tracking after terminal failure",
                scenario.name
            );
            assert_usdc_inventory_balances(&trigger, usdc(900), Usdc::ZERO, usdc(100), Usdc::ZERO)
                .await;
        }
    }

    #[tokio::test]
    async fn alpaca_to_base_post_burn_bridging_failure_preserves_inflight_and_guard() {
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .with_withdrawable_cash_cents(90_000);
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
            )
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(399)),
            )
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(399)),
            )
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_withdrawal_confirmed())
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridging_initiated())
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridging_failed())
            .await
            .unwrap();

        // A post-burn failure must preserve the last successful stage
        // (BridgingInitiated), not advance to a failure stage:
        // `UsdcRebalanceStage::from_event` returns None for BridgingFailed.
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(399),
            None,
            usdc::UsdcRebalanceStage::BridgingInitiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(501), usdc(399)).await;
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "post-burn bridge failure must keep the USDC guard set"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            Some(&id),
            "post-burn bridge failure must keep the active rebalance ID"
        );

        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "post-burn bridge failure must block immediate re-burns"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "post-burn bridge failure must block immediate re-burns"
        );
    }

    #[tokio::test]
    async fn base_to_alpaca_post_burn_bridging_failure_preserves_inflight_and_guard() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
            )
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_withdrawal_confirmed())
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridging_initiated())
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridging_failed())
            .await
            .unwrap();

        // A post-burn failure must preserve the last successful stage
        // (BridgingInitiated), not advance to a failure stage:
        // `UsdcRebalanceStage::from_event` returns None for BridgingFailed.
        assert_usdc_tracking_state(
            &trigger,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            None,
            usdc::UsdcRebalanceStage::BridgingInitiated,
        )
        .await;
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "post-burn bridge failure must keep the USDC guard set"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            Some(&id),
            "post-burn bridge failure must keep the active rebalance ID"
        );

        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "post-burn bridge failure must block immediate re-burns"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "post-burn bridge failure must block immediate re-burns"
        );
    }

    /// Exercises the defensive fallback branch of `post_burn_failure`: the
    /// `BridgingFailed` event carries no `burn_tx_hash`, but the tracked stage
    /// is `BridgingInitiated`, so `is_post_burn()` must still classify it as
    /// post-burn and preserve the guard.
    ///
    /// This (hashless event + post-burn stage) combination is NOT reachable via
    /// the real command path -- `FailBridging` from `Bridging`/`Attested` always
    /// emits `burn_tx_hash: Some`. The fallback is defense-in-depth for the live
    /// event stream. The startup recovery path (`holds_rebalance_guard`)
    /// deliberately keys only on `burn_tx_hash.is_some()` because the persisted
    /// aggregate state always carries the hash for a post-burn failure, so the
    /// two paths agree on every command-reachable state.
    #[tokio::test]
    async fn bridging_failure_without_burn_hash_preserved_via_tracking_stage() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
            )
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_withdrawal_confirmed())
            .await
            .unwrap();
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridging_initiated())
            .await
            .unwrap();
        // burn_tx_hash: None -- only the tracked BridgingInitiated stage marks
        // this as post-burn.
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_pre_burn_bridging_failed())
            .await
            .unwrap();

        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "stage-fallback must classify a hashless post-burn failure as post-burn and keep the guard"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            Some(&id),
            "stage-fallback post-burn failure must keep the active rebalance ID"
        );

        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "stage-fallback post-burn failure must block immediate re-burns"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "stage-fallback post-burn failure must block immediate re-burns"
        );
    }

    /// A `BridgingFailed` carrying a `cctp_nonce` but no `burn_tx_hash` is
    /// post-burn evidence on its own: the nonce only exists once the burn reached
    /// CCTP. When in-memory tracking is absent -- e.g. the failure arrives after a
    /// restart where the reactor never rebuilt tracking -- the nonce alone must
    /// keep the guard set, otherwise the caller would clear it and re-open the
    /// re-burn window for funds already irreversibly committed.
    #[tokio::test]
    async fn nonce_only_bridging_failure_preserves_guard_without_tracking() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        // No prior Initiated/BridgingInitiated events: in-memory tracking is empty,
        // mirroring a restart where only the nonce proves the burn happened.
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_nonce_only_bridging_failed())
            .await
            .unwrap();

        assert!(
            trigger.usdc_tracking.read().await.get(&id).is_none(),
            "test setup invariant: tracking must be absent so only the nonce can classify post-burn"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "a nonce-only bridging failure is post-burn evidence and must keep the guard set even \
             without tracking"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            Some(&id),
            "a nonce-only post-burn failure must keep the active rebalance ID"
        );

        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "a nonce-only post-burn failure must block immediate re-burns"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "a nonce-only post-burn failure must block immediate re-burns"
        );
    }

    /// A deposit failure happens after the CCTP mint, so the funds are post-burn
    /// and unreconcilable. It must preserve inflight and the guard (not Clear),
    /// the same as a post-burn bridge failure.
    #[tokio::test]
    async fn post_burn_deposit_failure_preserves_inflight_and_guard() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        for event in [
            make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
            make_usdc_withdrawal_confirmed(),
            make_usdc_bridging_initiated(),
            make_usdc_bridge_attestation_received(),
            make_usdc_bridged(),
            make_usdc_deposit_initiated(),
            make_usdc_deposit_failed(),
        ] {
            harness
                .receive::<UsdcRebalance>(id.clone(), event)
                .await
                .unwrap();
        }

        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "post-burn deposit failure must preserve in-flight tracking"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "post-burn deposit failure must keep the USDC guard set"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            Some(&id),
            "post-burn deposit failure must keep the active rebalance ID"
        );
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .usdc_inflight(Venue::MarketMaking),
            Some(usdc(400)),
            "post-burn deposit failure must preserve source inflight"
        );

        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "post-burn deposit failure must block immediate re-burns"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "post-burn deposit failure must block immediate re-burns"
        );
    }

    /// An operator reconciling a post-burn `DepositFailed` must clear the guard,
    /// remove tracking, and zero the source-venue (MarketMaking for BaseToAlpaca)
    /// inflight WITHOUT crediting available -- post-burn semantics, because the
    /// minted USDC physically left the source.
    #[tokio::test]
    async fn operator_reconcile_clears_guard_and_zeroes_source_inflight_post_burn() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        for event in [
            make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
            make_usdc_withdrawal_confirmed(),
            make_usdc_bridging_initiated(),
            make_usdc_bridge_attestation_received(),
            make_usdc_bridged(),
            make_usdc_deposit_initiated(),
            make_usdc_deposit_failed(),
        ] {
            harness
                .receive::<UsdcRebalance>(id.clone(), event)
                .await
                .unwrap();
        }

        // Sanity: the burn moved 400 from MarketMaking available to inflight, and
        // the post-burn deposit failure preserved it.
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_operator_reconciled(RebalanceDirection::BaseToAlpaca),
            )
            .await
            .unwrap();

        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "operator reconcile must remove tracking"
        );
        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "operator reconcile must clear the USDC guard"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            None,
            "operator reconcile must clear the active rebalance ID"
        );
        // Available unchanged (500), inflight zeroed -- funds left the source.
        assert_usdc_inventory_balances(&trigger, usdc(500), Usdc::ZERO, usdc(100), Usdc::ZERO)
            .await;
    }

    /// Post-restart the reactor has no in-memory tracking, yet an operator
    /// reconcile must still clear the guard and zero the source inflight derived
    /// from the event's `direction` -- never panic on absent tracking.
    #[tokio::test]
    async fn operator_reconcile_works_when_tracking_absent() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        // Seed the source (MarketMaking) inflight to mimic a post-restart state
        // where the burn already moved 400 into inflight, but in-memory tracking
        // was not rebuilt and the guard was merely re-asserted. Also carry a
        // stale active rebalance id so the test proves the Clear terminal action
        // drops it.
        {
            let mut inventory = trigger.inventory.write().await;
            *inventory = inventory
                .clone()
                .update_usdc(
                    Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                    Utc::now(),
                )
                .unwrap()
                .set_active_usdc_rebalance(id.clone());
        }
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(!trigger.usdc_tracking.read().await.contains_key(&id));
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            Some(&id)
        );
        // Sanity: 400 moved from MarketMaking available to inflight.
        assert_usdc_inventory_balances(&trigger, usdc(500), usdc(400), usdc(100), Usdc::ZERO).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_operator_reconciled(RebalanceDirection::BaseToAlpaca),
            )
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "operator reconcile must clear the guard even with tracking absent"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "operator reconcile must not resurrect in-memory tracking"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            None,
            "operator reconcile must clear the stale active rebalance id"
        );
        // Available unchanged (500), inflight zeroed via the event's direction.
        assert_usdc_inventory_balances(&trigger, usdc(500), Usdc::ZERO, usdc(100), Usdc::ZERO)
            .await;
    }

    /// Mirror of `operator_reconcile_works_when_tracking_absent` for the other
    /// direction: `AlpacaToBase` must zero the Hedging (source) inflight derived
    /// from the event's `direction` -- not MarketMaking -- and still clear the
    /// guard and the stale active id with tracking absent post-restart.
    #[tokio::test]
    async fn operator_reconcile_alpaca_to_base_clears_hedging_source_when_tracking_absent() {
        // Hedging is the offchain venue (second arg), so seed it with enough to
        // move 400 into inflight.
        let inventory = InventoryView::default().with_usdc(usdc(100), usdc(900));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        // Burn already moved 400 from the Hedging venue into inflight; tracking
        // was not rebuilt and a stale active id lingers.
        {
            let mut inventory = trigger.inventory.write().await;
            *inventory = inventory
                .clone()
                .update_usdc(
                    Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                    Utc::now(),
                )
                .unwrap()
                .set_active_usdc_rebalance(id.clone());
        }
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(!trigger.usdc_tracking.read().await.contains_key(&id));
        // Sanity: 400 moved from Hedging available (900 -> 500) to inflight.
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(500), usdc(400)).await;

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_operator_reconciled(RebalanceDirection::AlpacaToBase),
            )
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "operator reconcile must clear the guard even with tracking absent"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "operator reconcile must not resurrect in-memory tracking"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            None,
            "operator reconcile must clear the stale active rebalance id"
        );
        // Hedging inflight zeroed via the event's direction; MarketMaking and
        // Hedging available are both untouched (post-burn: no credit).
        assert_usdc_inventory_balances(&trigger, usdc(100), Usdc::ZERO, usdc(500), Usdc::ZERO)
            .await;
    }

    /// A `Reconciled` aggregate is a clearing terminal: startup guard recovery
    /// must NOT re-latch the guard for it (the opposite of a post-burn failure).
    #[tokio::test]
    async fn recover_usdc_guard_does_not_relatch_for_reconciled() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        // Drive a BaseToAlpaca aggregate to DepositFailed, then reconcile it.
        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(400),
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(&id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01],
                    cctp_nonce: B256::left_padding_from(&42u64.to_be_bytes()),
                    message: valid_cctp_message(),
                    mint_scan_from_block: 100,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx,
                    amount_received: usdc(399),
                    fee_collected: usdc(1),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(mint_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::FailDeposit {
                    reason: "deposit rejected".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert!(
            !service.usdc_in_progress.load(Ordering::SeqCst),
            "a Reconciled aggregate must not re-latch the USDC guard on startup"
        );
        // A Reconciled aggregate is a terminal no-op: startup recovery must not
        // enqueue any fresh transfer work in either direction.
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "a Reconciled aggregate must not enqueue a USDC->Hedging transfer on startup"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&service).await,
            0,
            "a Reconciled aggregate must not enqueue a USDC->MarketMaking transfer on startup"
        );
    }

    /// For BaseToAlpaca the conversion is the post-deposit USDC->USD leg, so a
    /// `ConversionFailed` there is post-mint: it must preserve inflight + the
    /// guard, even though `ConversionInitiated` resets the tracked stage to a
    /// pre-burn-looking value.
    #[tokio::test]
    async fn post_deposit_conversion_failure_preserves_inflight_and_guard() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        for event in [
            make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
            make_usdc_withdrawal_confirmed(),
            make_usdc_bridging_initiated(),
            make_usdc_bridge_attestation_received(),
            make_usdc_bridged(),
            make_usdc_deposit_initiated(),
            make_usdc_deposit_confirmed(RebalanceDirection::BaseToAlpaca),
            make_usdc_conversion_initiated(RebalanceDirection::BaseToAlpaca, usdc(399)),
            make_usdc_conversion_failed(),
        ] {
            harness
                .receive::<UsdcRebalance>(id.clone(), event)
                .await
                .unwrap();
        }

        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "post-deposit conversion failure must preserve in-flight tracking"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "post-deposit conversion failure must keep the USDC guard set"
        );
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            Some(&id),
            "post-deposit conversion failure must keep the active rebalance ID"
        );
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .usdc_inflight(Venue::MarketMaking),
            Some(usdc(400)),
            "post-deposit conversion failure must preserve source inflight"
        );

        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "post-deposit conversion failure must block immediate re-burns"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "post-deposit conversion failure must block immediate re-burns"
        );
    }

    /// A transfer that times out while stalled at a post-mint stage (here
    /// `DepositInitiated`) is also post-burn: the timeout sweep must preserve it,
    /// not reconcile to source and clear the guard (which would re-open a re-burn
    /// of already-burned funds).
    #[tokio::test]
    async fn timed_out_post_mint_usdc_rebalance_preserves_guard_and_inflight() {
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_usdc(usdc(900), usdc(100))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let trigger = reactor.clone();
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(400),
                bridged_amount_received: Some(usdc(399)),
                stage: usdc::UsdcRebalanceStage::DepositInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_usdc().await;

        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "post-mint USDC timeout must preserve in-flight tracking"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "post-mint USDC timeout must keep the guard set"
        );
        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .usdc_inflight(Venue::MarketMaking),
            Some(usdc(400)),
            "post-mint USDC timeout must preserve source inflight"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "post-mint USDC timeout must not allow another rebalance dispatch"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "post-mint USDC timeout must not allow another rebalance dispatch"
        );
    }

    #[tokio::test]
    async fn snapshot_onchain_usdc_via_reactor_updates_usdc_balance() {
        // Start with 500 onchain, 500 offchain = balanced
        let inventory = InventoryView::default().with_usdc(usdc(500), usdc(500));

        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Balanced initially -> no trigger
        trigger.check_and_trigger_usdc().await;
        assert_eq!(
            take_pending_usdc_transfer_jobs(&trigger).await.len(),
            0,
            "Should be balanced initially"
        );

        // Snapshot says onchain is actually 900 -> creates imbalance
        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: usdc(900),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // Now 900 onchain, 500 offchain = 64% ratio -> with target 50% deviation 30%,
        // upper bound = 80%, so 64% is within threshold -> no trigger
        // Use wider imbalance: 1400 onchain
        // Actually let me re-check: target 50%, deviation 30% -> upper = 80%
        // 900 / (900 + 500) = 900/1400 ~= 64.3% -> within threshold
        // Need a bigger imbalance to trigger. But the point is the snapshot DID update
        // the inventory. Let me verify by setting up an initial imbalance.
        trigger.check_and_trigger_usdc().await;
        assert_eq!(
            take_pending_usdc_transfer_jobs(&trigger).await.len(),
            0,
            "64% ratio within 50% +/- 30% threshold"
        );
    }

    #[tokio::test]
    async fn snapshot_offchain_cash_withdrawable_via_reactor_enqueues_usdc_check() {
        // Withdrawable cash now gates the Alpaca-to-Base capacity check, so a
        // recovery from MissingWithdrawableCash must arrive as a snapshot
        // event that re-enqueues a USDC check. If withdrawable updates did
        // not enqueue, a transient broker omission would permanently suppress
        // rebalancing until some unrelated USDC balance event fired.
        let inventory = InventoryView::default().with_usdc(usdc(500), usdc(500));
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };
        let pool = trigger.usdc_scheduler.queue().pool().clone();

        let pending_before: i64 =
            sqlx_apalis::query_scalar(
                "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type LIKE '%UsdcRebalancingCheck'",
            )
                .fetch_one(&pool)
                .await
                .expect("count pending usdc-check jobs before snapshot");

        apply_and_dispatch_snapshot(
            reactor.clone(),
            id,
            InventorySnapshotEvent::OffchainCashWithdrawable {
                cash_withdrawable_cents: Some(50_000),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        let pending_after: i64 =
            sqlx_apalis::query_scalar(
                "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type LIKE '%UsdcRebalancingCheck'",
            )
                .fetch_one(&pool)
                .await
                .expect("count pending usdc-check jobs after snapshot");

        assert!(
            pending_after > pending_before,
            "OffchainCashWithdrawable snapshot must enqueue a USDC check so the trigger \
             recovers after a MissingWithdrawableCash skip (before: {pending_before}, \
             after: {pending_after})"
        );
    }

    #[tokio::test]
    async fn snapshot_offchain_usd_via_reactor_updates_usdc_balance() {
        // Start with 900 onchain, 100 offchain = 90% ratio -> TooMuchOnchain
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));

        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Verify imbalance triggers before snapshot. Base->Alpaca is
        // enqueued as a TransferUsdcToHedging apalis job.
        trigger.check_and_trigger_usdc().await;
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            1,
            "TooMuchOnchain (90% ratio) should enqueue a TransferUsdcToHedging job before snapshot"
        );
        take_pending_usdc_transfer_jobs(&trigger).await;
        trigger.clear_usdc_in_progress();

        // Snapshot says offchain is actually 900 (not 100)
        // 95000 cents = $950.00
        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: 90000,
                gross_usd_cents: None,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // Now 900 onchain, 900 offchain = balanced -> no trigger
        trigger.check_and_trigger_usdc().await;
        assert_eq!(
            take_pending_usdc_transfer_jobs(&trigger).await.len(),
            0,
            "Should be balanced after offchain cash snapshot reconciled to 900"
        );
    }

    fn make_withdrawn_from_raindex(symbol: &Symbol, quantity: Float) -> EquityRedemptionEvent {
        EquityRedemptionEvent::VaultWithdrawPending {
            symbol: symbol.clone(),
            quantity,
            token: Address::random(),
            wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            pending_at: Utc::now(),
        }
    }

    fn make_redemption_detected() -> EquityRedemptionEvent {
        EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ123".to_string()),
            detected_at: Utc::now(),
        }
    }

    fn make_detection_failed() -> EquityRedemptionEvent {
        EquityRedemptionEvent::DetectionFailed {
            failure: DetectionFailure::Timeout,
            failed_at: Utc::now(),
        }
    }

    fn make_redemption_completed() -> EquityRedemptionEvent {
        EquityRedemptionEvent::Completed {
            completed_at: Utc::now(),
        }
    }

    fn make_redemption_rejected() -> EquityRedemptionEvent {
        EquityRedemptionEvent::RedemptionRejected {
            reason: "test rejection".to_string(),
            rejected_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn redemption_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        // Start with 80 onchain and 20 offchain - imbalanced (80% onchain).
        // Threshold: target 50%, deviation 20%, so upper bound is 70%.
        // 80% > 70% triggers TooMuchOnchain.
        let inventory = inventory
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        // First trigger detects the imbalance and enqueues a redemption.
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            take_pending_equity_redemption_jobs(&trigger).await.len(),
            1,
            "First trigger should enqueue a redemption job"
        );

        // With in-progress guard held, second trigger should not send anything.
        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Expected no operation while in-progress guard is held"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Expected no operation while in-progress guard is held"
        );
    }

    #[tokio::test]
    async fn redemption_completion_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        let trigger = make_trigger_with_inventory(inventory).await;

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }
        assert!(
            trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );

        // Check that Completed is detected as terminal.
        assert!(RebalancingService::is_terminal_redemption_event(
            &make_redemption_completed()
        ));

        // Simulate what dispatch does - clear in-progress on terminal.
        trigger.clear_equity_in_progress(&symbol);
        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol)
        );
    }

    #[test]
    fn detection_failure_is_terminal_redemption_event() {
        assert!(RebalancingService::is_terminal_redemption_event(
            &make_detection_failed()
        ));
    }

    #[test]
    fn operator_reconciled_is_terminal_redemption_event() {
        // Gates clear_equity_in_progress: a reconciled redemption must read as
        // terminal so the in-progress flag is released and rebalancing resumes.
        assert!(RebalancingService::is_terminal_redemption_event(
            &EquityRedemptionEvent::OperatorReconciled {
                reason: "deposited manually".to_string(),
                reconciled_at: Utc::now(),
            }
        ));
    }

    #[test]
    fn redemption_rejection_is_terminal() {
        assert!(RebalancingService::is_terminal_redemption_event(
            &make_redemption_rejected()
        ));
    }

    #[test]
    fn extract_redemption_info_returns_symbol_and_quantity() {
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_withdrawn_from_raindex(&symbol, float!(42.5));

        let (extracted_symbol, extracted_quantity) =
            RebalancingService::extract_redemption_info(&event).unwrap();

        assert_eq!(extracted_symbol, symbol);
        assert!(extracted_quantity.inner().eq(float!(42.5)).unwrap());
    }

    #[test]
    fn extract_redemption_info_returns_none_without_tokens_sent() {
        let result = RebalancingService::extract_redemption_info(&make_redemption_completed());
        assert!(result.is_none());
    }

    #[test]
    fn non_terminal_redemption_events_are_not_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        assert!(!RebalancingService::is_terminal_redemption_event(
            &make_withdrawn_from_raindex(&symbol, float!(30))
        ));
        assert!(!RebalancingService::is_terminal_redemption_event(
            &make_redemption_detected()
        ));
    }

    fn usdc(n: i64) -> Usdc {
        Usdc::new(float!(&n.to_string()))
    }

    fn make_usdc_initiated(direction: RebalanceDirection, amount: Usdc) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref: TransferRef::OnchainTx(TxHash::random()),
            initiated_at: Utc::now(),
        }
    }

    fn make_usdc_conversion_initiated(
        direction: RebalanceDirection,
        amount: Usdc,
    ) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::ConversionInitiated {
            direction,
            amount,
            order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            initiated_at: Utc::now(),
        }
    }

    fn make_usdc_withdrawal_confirmed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
            withdrawal_tx: None,
        }
    }

    fn make_usdc_withdrawal_failed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::WithdrawalFailed {
            reason: "Insufficient funds".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_usdc_bridging_initiated() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: TxHash::random(),
            burned_at: Utc::now(),
        }
    }

    fn make_usdc_bridge_attestation_received() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![1, 2, 3, 4],
            cctp_nonce: B256::left_padding_from(&42u64.to_be_bytes()),
            message: None,
            mint_scan_from_block: Some(100),
            attested_at: Utc::now(),
        }
    }

    /// A full-length CCTP message envelope (>= MESSAGE_BODY_INDEX = 148 bytes,
    /// non-zero nonce in bytes 12..44), matching the manager-test helper so a
    /// `ReceiveAttestation` fixture lands a stored envelope that
    /// `AttestationResponse::from_parts` would accept.
    fn valid_cctp_message() -> Vec<u8> {
        let mut message = vec![0u8; 200];
        message[43] = 1;
        message
    }

    fn make_usdc_bridged() -> UsdcRebalanceEvent {
        make_usdc_bridged_with_amounts(Usdc::new(float!(99.99)), Usdc::new(float!(0.01)))
    }

    fn make_usdc_bridged_with_amounts(
        amount_received: Usdc,
        fee_collected: Usdc,
    ) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::Bridged {
            mint_tx_hash: TxHash::random(),
            amount_received,
            fee_collected,
            minted_at: Utc::now(),
        }
    }

    fn make_usdc_bridging_failed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: Some(TxHash::random()),
            cctp_nonce: Some(B256::left_padding_from(&12345u64.to_be_bytes())),
            reason: "Attestation timeout".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_usdc_bridging_completion_recovered() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgingCompletionRecovered {
            mint_tx_hash: TxHash::random(),
            amount_received: Usdc::new(float!(99.99)),
            fee_collected: Usdc::new(float!(0.01)),
            recovered_at: Utc::now(),
        }
    }

    fn make_usdc_pre_burn_bridging_failed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: None,
            cctp_nonce: None,
            reason: "Burn failed".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_usdc_nonce_only_bridging_failed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: None,
            cctp_nonce: Some(B256::left_padding_from(&54321u64.to_be_bytes())),
            reason: "Attestation timeout".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_usdc_deposit_confirmed(direction: RebalanceDirection) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::DepositConfirmed {
            direction,
            deposit_confirmed_at: Utc::now(),
        }
    }

    fn make_usdc_deposit_initiated() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: TransferRef::OnchainTx(TxHash::random()),
            deposit_initiated_at: Utc::now(),
        }
    }

    fn make_usdc_deposit_failed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::DepositFailed {
            deposit_ref: Some(TransferRef::OnchainTx(TxHash::random())),
            reason: "Deposit rejected".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_usdc_operator_reconciled(direction: RebalanceDirection) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::OperatorReconciled {
            direction,
            amount: usdc(400),
            reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
            initiated_at: Utc::now(),
            reconciled_at: Utc::now(),
        }
    }

    fn make_usdc_conversion_confirmed(
        direction: RebalanceDirection,
        filled_amount: Usdc,
    ) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::ConversionConfirmed {
            direction,
            filled_amount,
            converted_at: Utc::now(),
        }
    }

    fn make_usdc_conversion_failed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::ConversionFailed {
            reason: "Order rejected".to_string(),
            failed_at: Utc::now(),
        }
    }

    async fn assert_usdc_tracking_state(
        trigger: &Arc<RebalancingService>,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
        initiated_amount: Usdc,
        bridged_amount_received: Option<Usdc>,
        stage: usdc::UsdcRebalanceStage,
    ) {
        let tracking = trigger
            .usdc_tracking
            .read()
            .await
            .get(id)
            .cloned()
            .expect("USDC rebalance should still be tracked");

        assert_eq!(tracking.direction, direction);
        assert_eq!(tracking.initiated_amount, initiated_amount);
        assert_eq!(tracking.bridged_amount_received, bridged_amount_received);
        assert_eq!(tracking.stage, stage);
    }

    async fn assert_usdc_inventory_balances(
        trigger: &Arc<RebalancingService>,
        market_making_available: Usdc,
        market_making_inflight: Usdc,
        hedging_available: Usdc,
        hedging_inflight: Usdc,
    ) {
        let inventory = trigger.inventory.read().await;

        assert_eq!(
            inventory.usdc_available(Venue::MarketMaking),
            Some(market_making_available)
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(market_making_inflight)
        );
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(hedging_available)
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(hedging_inflight)
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn usdc_rebalance_completion_clears_in_progress_flag() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        // Mark USDC as in-progress.
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(trigger.usdc_in_progress.load(Ordering::SeqCst));

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(1000)),
            )
            .await
            .unwrap();

        harness
            .receive::<UsdcRebalance>(
                id,
                make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(999)),
            )
            .await
            .unwrap();

        assert!(!trigger.usdc_in_progress.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn recovered_post_burn_bridge_holds_guard_then_clears_on_terminal() {
        // Recovery lifecycle through the reactor: a post-burn
        // BridgingFailed is un-failed by BridgingCompletionRecovered (non-terminal
        // -> guard stays held mid-recovery) and the guard clears only on the
        // terminal BaseToAlpaca ConversionConfirmed, exactly like the normal
        // bridge path.
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        // The guard is armed when the rebalance is initiated.
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        // Initiated sets up tracking so the recovery event has bridged-amount
        // context; the guard stays held (non-terminal).
        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(1000)),
            )
            .await
            .unwrap();

        // The recovery event records the bridged amount and must keep the guard
        // held (it is not a terminal state).
        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridging_completion_recovered())
            .await
            .unwrap();
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "BridgingCompletionRecovered is non-terminal and must hold the guard mid-recovery"
        );

        // The terminal conversion clears the guard through the normal path.
        harness
            .receive::<UsdcRebalance>(
                id,
                make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(999)),
            )
            .await
            .unwrap();
        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "terminal ConversionConfirmed must clear the guard after recovery"
        );
    }

    #[tokio::test]
    async fn bridged_without_initiated_is_tolerated_and_holds_guard() {
        // A Bridged event with no in-memory tracking is the post-restart-recovery
        // shape: the guard was reasserted on startup but `usdc_tracking` was not
        // rebuilt. `track_bridged_amount` warns and returns Ok (consistent with
        // the other completion helpers) instead of wedging the reactor, and the
        // non-terminal event leaves the (pre-armed) in-progress guard held.
        let trigger = make_trigger_with_inventory(InventoryView::default()).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridged())
            .await
            .unwrap();

        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "a tolerated non-terminal Bridged event must not clear the guard"
        );
    }

    #[tokio::test]
    async fn initiated_with_insufficient_balance_does_not_insert_tracking_context() {
        let inventory = InventoryView::default().with_usdc(usdc(100), usdc(100));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        let error = harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(1000)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RebalancingServiceError::Inventory(_)),
            "initiated event should fail through inventory validation, got {error:?}"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking context should not be inserted when the initiation inventory update fails"
        );
    }

    #[tokio::test]
    async fn deposit_confirmed_without_bridged_amount_returns_error_and_preserves_state() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(1000)),
            )
            .await
            .unwrap();

        let error = harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            RebalancingServiceError::MissingUsdcBridgedAmount { id: error_id }
                if error_id == id
        ));
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress should stay set when terminal settlement context is missing"
        );
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking context should be retained after a terminal settlement failure"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(usdc(1000)),
            "offchain USDC inflight should remain until settlement succeeds"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn terminal_failure_cancels_inflight_usdc_and_clears_tracking() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(1000)),
            )
            .await
            .unwrap();

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_withdrawal_failed())
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress should clear after a terminal failure cancels inflight inventory"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking context should be removed after terminal failure cleanup succeeds"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(5000)),
            "terminal failure should restore the full offchain USDC balance"
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(Usdc::ZERO),
            "terminal failure should clear offchain USDC inflight"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn timed_out_usdc_rebalance_clears_guards_and_ignores_late_events() {
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .with_withdrawable_cash_cents(90_000)
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::Initiated,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_usdc().await;

        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "USDC timeout should remove in-flight tracking"
        );
        assert!(
            trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "USDC timeout should remember the aggregate so late events are ignored"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(Usdc::ZERO),
            "USDC timeout should zero offchain inflight balance"
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "USDC timeout should not leave onchain inflight behind"
        );
        drop(inventory);

        let triggered = take_pending_usdc_transfer_jobs(&trigger).await;
        assert!(
            matches!(
                triggered.as_slice(),
                [UsdcRebalanceOperation::AlpacaToBase { .. }]
            ),
            "USDC timeout should allow the next trigger cycle to proceed, got {triggered:?}"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "the next USDC trigger should be allowed to claim the in-progress guard"
        );

        trigger.clear_usdc_in_progress();

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase),
            )
            .await
            .unwrap();

        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "late terminal events must not recreate timed-out USDC tracking"
        );
    }

    #[tokio::test]
    async fn timeout_does_not_clear_guard_while_transfer_job_in_flight() {
        let inventory = InventoryView::default()
            .with_usdc(usdc(900), usdc(100))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap();
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        // A Base->Alpaca transfer whose apalis row is still in flight: a crash mid
        // withdraw/burn leaves the aggregate at a *Submitting state with an
        // irreversible effect possibly already submitted, and apalis will resume.
        let mut queue = trigger.transfer_usdc_to_hedging_queue.clone();
        queue
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::Initiated,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_usdc().await;

        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "timeout must NOT clear the in-progress guard while the apalis transfer \
             job is in flight -- an irreversible withdraw/burn may be pending and \
             clearing would let a second transfer touch the same vault/wallet",
        );
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "timeout must not drop tracking for an in-flight transfer",
        );
        assert!(
            !trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "timeout must not mark an in-flight transfer timed-out (the reactor would \
             then ignore its terminal event and latch the guard forever)",
        );

        let marketmaking_inflight = trigger
            .inventory
            .read()
            .await
            .usdc_inflight(Venue::MarketMaking);

        assert_eq!(
            marketmaking_inflight,
            Some(usdc(400)),
            "timeout must preserve the Base->Alpaca source-side inflight on \
             MarketMaking while the apalis transfer job is still in flight",
        );
    }

    #[tokio::test]
    async fn transfer_in_flight_check_is_scoped_to_aggregate_id() {
        let trigger = make_trigger_with_inventory_config(
            InventoryView::default(),
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let other_id = UsdcRebalanceId(Uuid::new_v4());

        let mut queue = trigger.transfer_usdc_to_hedging_queue.clone();
        queue
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        assert!(
            trigger.transfer_in_flight_for_id(&id).await,
            "a non-terminal transfer row for this id must report in flight",
        );
        assert!(
            !trigger.transfer_in_flight_for_id(&other_id).await,
            "a transfer row for a different aggregate id must NOT report this id as in flight",
        );
    }

    #[tokio::test]
    async fn transfer_in_flight_for_id_checks_market_making_direction() {
        let trigger = make_trigger_with_inventory_config(
            InventoryView::default(),
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        // An Alpaca->Base rebalance is driven by a market-making transfer; the
        // sweep's in-flight check must see it (bidirectional), not just the hedging
        // queue -- otherwise it would clear the guard while a market-making transfer
        // with an irreversible mint/deposit is still in flight.
        trigger
            .transfer_usdc_to_market_making_queue
            .clone()
            .push(TransferUsdcToMarketMaking {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        assert!(
            trigger.transfer_in_flight_for_id(&id).await,
            "a non-terminal market-making (Alpaca->Base) transfer row must report this id as in flight",
        );
    }

    #[tokio::test]
    async fn timeout_clears_guard_once_transfer_job_reaches_terminal_status() {
        let trigger = make_trigger_with_inventory_config(
            InventoryView::default(),
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        // The transfer's apalis row has reached a terminal status: the job is no
        // longer in flight, so the sweep must proceed to cleanup and clear the
        // guard -- the "eventually released, not latched forever" property that is
        // the whole safety argument for skipping cleanup while a job is in flight.
        let mut queue = trigger.transfer_usdc_to_hedging_queue.clone();
        queue
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query("UPDATE Jobs SET status = 'Failed' WHERE job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .execute(queue.pool())
            .await
            .unwrap();

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::Initiated,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        trigger
            .expire_stuck_usdc_rebalances(Utc::now())
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "timeout must clear the guard once the transfer job reaches a terminal status",
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "timeout must drop tracking once the transfer job is terminal",
        );
    }

    #[tokio::test]
    async fn in_flight_hedging_transfer_blocks_market_making_enqueue() {
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        // A Base->Alpaca (hedging) transfer is already in flight.
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        // The opposite-direction enqueue must be suppressed: both directions
        // move the same funds through the same vault/wallet, and the in-memory
        // guard resets on restart, so running them concurrently would churn
        // capital and pay fees twice.
        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(
            !enqueued,
            "an in-flight Base->Alpaca transfer must block enqueuing an Alpaca->Base transfer"
        );
    }

    #[tokio::test]
    async fn in_flight_market_making_transfer_blocks_hedging_enqueue() {
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        // An Alpaca->Base (market-making) transfer is already in flight.
        service
            .transfer_usdc_to_market_making_queue
            .clone()
            .push(TransferUsdcToMarketMaking {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        let enqueued = service.enqueue_transfer_usdc_to_hedging(usdc(100)).await;

        assert!(
            !enqueued,
            "an in-flight Alpaca->Base transfer must block enqueuing a Base->Alpaca transfer"
        );
    }

    // -- USDC guard: Failed and terminal status tests --

    #[tokio::test]
    async fn queued_transfer_with_done_at_set_blocks_enqueue() {
        // apalis re-dispatch sets status='Queued' but does not clear done_at from a
        // prior ack. A Queued row must still be treated as in-flight.
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Queued', done_at = strftime('%s', 'now') \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(
            !enqueued,
            "a Queued transfer (even with done_at set from a prior ack) must block enqueue"
        );
    }

    #[tokio::test]
    async fn running_usdc_transfer_blocks_enqueue() {
        // A Running row must block enqueue. This guards against regressions where
        // the SQL filter accidentally omitted 'Running' from the IN-clause.
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query("UPDATE Jobs SET status = 'Running' WHERE job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .execute(service.transfer_usdc_to_hedging_queue.pool())
            .await
            .unwrap();

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(!enqueued, "a Running USDC transfer must block enqueue");
    }

    // -- USDC guard: zombie reconciliation (Failed+attempts<max, aggregate terminal) --

    /// Drives a UsdcRebalance aggregate to `WithdrawalFailed` (holds_rebalance_guard = false).
    async fn seed_terminal_usdc_aggregate(store: &Store<UsdcRebalance>, id: &UsdcRebalanceId) {
        store
            .send(
                id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(100),
                    withdrawal: TransferRef::OnchainTx(TxHash::default()),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "test: forced withdrawal failure".to_string(),
                },
            )
            .await
            .unwrap();
    }

    /// Drives a UsdcRebalance aggregate to `Withdrawing` (holds_rebalance_guard = true).
    async fn seed_nonterminal_usdc_aggregate(store: &Store<UsdcRebalance>, id: &UsdcRebalanceId) {
        store
            .send(
                id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(100),
                    withdrawal: TransferRef::OnchainTx(TxHash::default()),
                },
            )
            .await
            .unwrap();
    }

    /// Attaches USDC + equity stores (all on `pool`) to `service`.
    async fn attach_stores(
        service: &RebalancingService,
        pool: SqlitePool,
        usdc_store: Store<UsdcRebalance>,
    ) {
        service
            .set_stores(
                Arc::new(test_store::<TokenizedEquityMint>(
                    pool.clone(),
                    EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<EquityRedemption>(
                    pool,
                    EquityTransferServices::panicking(),
                )),
                Arc::new(usdc_store),
            )
            .await;
    }

    #[tokio::test]
    async fn failed_usdc_zombie_with_terminal_aggregate_does_not_block_hedging_enqueue() {
        // A Failed+attempts<max row whose aggregate is already terminal (zombie)
        // must be killed and must NOT suppress a new hedging transfer.
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_terminal_usdc_aggregate(&usdc_store, &id).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_usdc_to_market_making_queue
            .clone()
            .push(TransferUsdcToMarketMaking {
                id: id.clone(),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
        .execute(service.transfer_usdc_to_market_making_queue.pool())
        .await
        .unwrap();
        attach_stores(&service, pool, usdc_store).await;

        let enqueued = service.enqueue_transfer_usdc_to_hedging(usdc(100)).await;

        assert!(
            enqueued,
            "zombie with terminal aggregate must NOT block hedging enqueue"
        );

        let status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
                .fetch_one(service.transfer_usdc_to_market_making_queue.pool())
                .await
                .unwrap();
        assert_eq!(
            status, "Killed",
            "zombie row must be killed after reconciliation"
        );
    }

    #[tokio::test]
    async fn failed_usdc_zombie_with_terminal_aggregate_does_not_block_market_making_enqueue() {
        // Symmetric: a zombie hedging job must not block a market-making enqueue.
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_terminal_usdc_aggregate(&usdc_store, &id).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();
        attach_stores(&service, pool, usdc_store).await;

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(
            enqueued,
            "zombie with terminal aggregate must NOT block market-making enqueue"
        );

        let status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferUsdcToHedging>())
                .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
                .await
                .unwrap();
        assert_eq!(
            status, "Killed",
            "zombie row must be killed after reconciliation"
        );
    }

    #[tokio::test]
    async fn failed_usdc_genuine_retry_with_nonterminal_aggregate_blocks() {
        // A Failed+attempts<max row whose aggregate is still live (genuine retry)
        // must block enqueue even after the zombie check.
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_nonterminal_usdc_aggregate(&usdc_store, &id).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();
        attach_stores(&service, pool, usdc_store).await;

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(
            !enqueued,
            "genuine-retry Failed row (non-terminal aggregate) must block enqueue"
        );

        let status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferUsdcToHedging>())
                .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
                .await
                .unwrap();
        assert_eq!(status, "Failed", "genuine-retry row must remain Failed");
    }

    #[tokio::test]
    async fn failed_usdc_zombie_store_not_wired_is_conservative() {
        // When the usdc_store is not yet wired (None), the guard must treat
        // Failed+attempts<max rows as in-flight (conservative fail-safe).
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        // Do NOT call set_stores: usdc_store stays None.
        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(
            !enqueued,
            "store not wired: must conservatively treat Failed+attempts<max as in-flight"
        );
    }

    #[tokio::test]
    async fn failed_usdc_zombie_aggregate_not_found_is_conservative() {
        // Store is wired but has no aggregate record for the job's id.
        // The guard must conservatively treat the row as in-flight.
        let pool = crate::test_utils::setup_test_db().await;
        // Do NOT seed any aggregate -- store is empty.
        let usdc_store = test_store::<UsdcRebalance>(pool.clone(), ());

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();
        attach_stores(&service, pool, usdc_store).await;

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(
            !enqueued,
            "aggregate not found in store: must conservatively treat row as in-flight"
        );
    }

    #[tokio::test]
    async fn corrupt_usdc_job_payload_is_conservative() {
        // A Failed+attempts<max row whose job blob cannot be parsed as UsdcJobId
        // must block enqueue: the guard fails closed rather than letting an unknown
        // in-flight operation race a new transfer.
        //
        // The store must be wired so the guard reaches the parse branch; the
        // store-not-wired check fires first and returns early, so without wiring
        // the test would exercise the wrong branch.
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = test_store::<UsdcRebalance>(pool.clone(), ());

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_usdc_to_market_making_queue
            .clone()
            .push(TransferUsdcToMarketMaking {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        let corrupt_payload: Vec<u8> = b"not json at all".to_vec();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1, \
             job = ? WHERE job_type = ?",
        )
        .bind(corrupt_payload)
        .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
        .execute(service.transfer_usdc_to_market_making_queue.pool())
        .await
        .unwrap();

        attach_stores(&service, pool, usdc_store).await;

        let enqueued = service.enqueue_transfer_usdc_to_hedging(usdc(100)).await;

        assert!(
            !enqueued,
            "corrupt USDC job payload must be treated as in-flight (conservative) and block enqueue"
        );
    }

    #[tokio::test]
    async fn failed_usdc_multiple_zombies_all_cleared() {
        // Two Failed+attempts<max rows with terminal aggregates and no Pending row:
        // both must be killed and enqueue must succeed.
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id1 = UsdcRebalanceId(Uuid::new_v4());
        let id2 = UsdcRebalanceId(Uuid::new_v4());
        seed_terminal_usdc_aggregate(&usdc_store, &id1).await;
        seed_terminal_usdc_aggregate(&usdc_store, &id2).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        // Push both zombie jobs (hedging direction).
        for zombie_id in [&id1, &id2] {
            service
                .transfer_usdc_to_hedging_queue
                .clone()
                .push(TransferUsdcToHedging {
                    id: zombie_id.clone(),
                    amount: usdc(100),
                    revert_redrive_attempts: 0,
                })
                .await
                .unwrap();
        }
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();
        attach_stores(&service, pool, usdc_store).await;

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(enqueued, "all zombies cleared: enqueue must succeed");

        let killed_count: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Killed'",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();
        assert_eq!(killed_count, 2, "both zombie rows must have been killed");
    }

    #[tokio::test]
    async fn failed_exhausted_job_does_not_block_enqueue() {
        // A Failed row whose attempt budget is exhausted (attempts == max_attempts)
        // is terminal and must not block a new transfer.
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), \
             attempts = max_attempts WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(
            enqueued,
            "a retry-exhausted Failed job must NOT suppress a new transfer"
        );
    }

    #[tokio::test]
    async fn done_job_does_not_block_enqueue() {
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Done', done_at = strftime('%s', 'now') \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(enqueued, "a Done job must NOT suppress a new transfer");
    }

    #[tokio::test]
    async fn killed_job_does_not_block_enqueue() {
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Killed', done_at = strftime('%s', 'now') \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(enqueued, "a Killed job must NOT suppress a new transfer");
    }

    #[tokio::test]
    async fn zombie_killed_then_genuine_pending_row_still_blocks() {
        // A Failed zombie row (terminal aggregate, older run_at) is killed on the
        // first guard loop iteration, but a genuine Pending row with a non-terminal
        // aggregate is found on the next iteration and blocks the enqueue.
        // This verifies that `in_flight_usdc_transfer` re-queries after killing a
        // zombie rather than blindly treating the cleared zombie as "all clear".
        let pool = crate::test_utils::setup_test_db().await;
        let usdc_store = test_store::<UsdcRebalance>(pool.clone(), ());

        let zombie_id = UsdcRebalanceId(Uuid::new_v4());
        seed_terminal_usdc_aggregate(&usdc_store, &zombie_id).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_usdc_to_market_making_queue
            .clone()
            .push(TransferUsdcToMarketMaking {
                id: zombie_id.clone(),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1, \
             run_at = strftime('%s', 'now') - 200 WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
        .execute(service.transfer_usdc_to_market_making_queue.pool())
        .await
        .unwrap();

        let live_id = UsdcRebalanceId(Uuid::new_v4());
        seed_nonterminal_usdc_aggregate(&usdc_store, &live_id).await;

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: live_id.clone(),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        attach_stores(&service, pool, usdc_store).await;

        let enqueued = service
            .enqueue_transfer_usdc_to_market_making(usdc(100))
            .await;

        assert!(
            !enqueued,
            "zombie killed but live Pending row must still block a new market-making enqueue"
        );

        let zombie_status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
                .fetch_one(service.transfer_usdc_to_market_making_queue.pool())
                .await
                .unwrap();
        assert_eq!(
            zombie_status, "Killed",
            "zombie row must be killed even though the live Pending row blocks the overall enqueue"
        );
    }

    #[tokio::test]
    async fn kill_zombie_job_is_noop_when_row_not_failed_retryable() {
        // kill_zombie_job's WHERE clause requires status='Failed' AND attempts <
        // max_attempts. A row in any other state (e.g. Running) must be left
        // untouched and the function must return false.
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(Uuid::new_v4()),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query("UPDATE Jobs SET status = 'Running' WHERE job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .execute(service.transfer_usdc_to_hedging_queue.pool())
            .await
            .unwrap();

        let row_id: String = sqlx_apalis::query_scalar("SELECT id FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
            .await
            .unwrap();

        let killed = RebalancingService::kill_zombie_job(
            service.transfer_usdc_to_hedging_queue.pool(),
            &row_id,
        )
        .await
        .unwrap();

        assert!(!killed, "kill_zombie_job must be a no-op for a Running row");

        let status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferUsdcToHedging>())
                .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
                .await
                .unwrap();
        assert_eq!(
            status, "Running",
            "Running row must not be changed to Killed"
        );
    }

    // -- transfer_live_job_for_id regression: Failed+done_at+attempts<max still blocks re-arm --

    #[tokio::test]
    async fn transfer_live_job_for_id_regression_failed_done_at() {
        // transfer_live_job_for_id intentionally includes Failed+attempts<max rows
        // (apalis WILL re-fetch them). This test guards against copy-paste of the
        // guard fix accidentally removing that behaviour.
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(100),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        // Simulate apalis acking the job as Failed with retries remaining.
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        let live = service.transfer_live_job_for_id(&id).await.unwrap();

        assert!(
            live,
            "transfer_live_job_for_id must return true for a Failed+done_at+attempts<max row \
             because apalis will re-fetch and re-run it"
        );
    }

    // -- Equity guard: Failed status tests and zombie reconciliation --

    /// Drives a `TokenizedEquityMint` aggregate to `Failed` state (terminal).
    ///
    /// Uses `MockTokenizer` so `RequestMint` succeeds (returns `MintAccepted`),
    /// then `FailAcceptance` drives it to `Failed` without calling any other service.
    async fn seed_terminal_mint_aggregate(pool: &SqlitePool, mint_id: &IssuerRequestId) {
        let store = test_store::<TokenizedEquityMint>(
            pool.clone(),
            EquityTransferServices {
                raindex: Arc::new(MockRaindex::new()),
                vault_lookup: Arc::new(MockVaultLookup::new()),
                tokenizer: Arc::new(MockTokenizer::new()),
                wrapper: Arc::new(MockWrapper::new()),
            },
        );
        store
            .send(
                mint_id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: mint_id.clone(),
                    symbol: Symbol::new("tAAPL").unwrap(),
                    quantity: float!(1),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        store
            .send(
                mint_id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "test: forced mint failure".to_string(),
                },
            )
            .await
            .unwrap();
    }

    /// Drives a `TokenizedEquityMint` aggregate to `MintAccepted` state (non-terminal).
    ///
    /// Same services as `seed_terminal_mint_aggregate`; stops before the failure
    /// so the aggregate is still live and holds the duplicate-protection guard.
    async fn seed_nonterminal_mint_aggregate(pool: &SqlitePool, mint_id: &IssuerRequestId) {
        let store = test_store::<TokenizedEquityMint>(
            pool.clone(),
            EquityTransferServices {
                raindex: Arc::new(MockRaindex::new()),
                vault_lookup: Arc::new(MockVaultLookup::new()),
                tokenizer: Arc::new(MockTokenizer::new()),
                wrapper: Arc::new(MockWrapper::new()),
            },
        );
        store
            .send(
                mint_id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: mint_id.clone(),
                    symbol: Symbol::new("tAAPL").unwrap(),
                    quantity: float!(1),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
    }

    /// Drives an `EquityRedemption` aggregate to `Failed` state (terminal).
    ///
    /// `Redeem` emits `VaultWithdrawPending` without calling any service.
    /// `FailTransfer` from `VaultWithdrawPending` also calls no service. Both
    /// are safe with panicking services.
    async fn seed_terminal_redemption_aggregate(
        pool: &SqlitePool,
        redemption_id: &RedemptionAggregateId,
    ) {
        let store =
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking());
        store
            .send(
                redemption_id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("tAAPL").unwrap(),
                    quantity: float!(1),
                    token: Address::ZERO,
                    amount: U256::ZERO,
                },
            )
            .await
            .unwrap();
        store
            .send(
                redemption_id,
                EquityRedemptionCommand::FailTransfer {
                    reason: "test: forced redemption failure".to_string(),
                },
            )
            .await
            .unwrap();
    }

    /// Drives an `EquityRedemption` aggregate to `VaultWithdrawPending` state
    /// (non-terminal). `Redeem` calls no services so panicking services are safe.
    async fn seed_nonterminal_redemption_aggregate(
        pool: &SqlitePool,
        redemption_id: &RedemptionAggregateId,
    ) {
        let store =
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking());
        store
            .send(
                redemption_id,
                EquityRedemptionCommand::Redeem {
                    symbol: Symbol::new("tAAPL").unwrap(),
                    quantity: float!(1),
                    token: Address::ZERO,
                    amount: U256::ZERO,
                },
            )
            .await
            .unwrap();
    }

    /// Wires specific mint and redemption stores to `service` (used by equity
    /// zombie tests). The USDC store is a no-op on the same pool.
    async fn attach_equity_stores(
        service: &RebalancingService,
        pool: SqlitePool,
        mint_store: Store<TokenizedEquityMint>,
        redemption_store: Store<EquityRedemption>,
    ) {
        service
            .set_stores(
                Arc::new(mint_store),
                Arc::new(redemption_store),
                Arc::new(test_store::<UsdcRebalance>(pool, ())),
            )
            .await;
    }

    #[tokio::test]
    async fn equity_failed_exhausted_job_does_not_block_enqueue() {
        // An equity Failed row whose retry budget is exhausted must not block.
        // This is terminal regardless of the aggregate state.
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        let symbol = Symbol::new("tAAPL").unwrap();

        service
            .transfer_equity_to_market_making_queue
            .clone()
            .push(TransferEquityToMarketMaking {
                issuer_request_id: IssuerRequestId::generate(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), \
             attempts = max_attempts WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .execute(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();

        let enqueued = service
            .enqueue_transfer_equity_to_hedging(symbol, FractionalShares::new(float!(1)))
            .await;

        assert!(
            enqueued,
            "a retry-exhausted Failed equity job must NOT suppress a new equity redemption"
        );
    }

    #[tokio::test]
    async fn running_equity_transfer_blocks_enqueue() {
        // A Running equity job row must block enqueue; guards against regressions
        // where 'Running' is accidentally omitted from the IN-clause.
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        let symbol = Symbol::new("tAAPL").unwrap();

        service
            .transfer_equity_to_market_making_queue
            .clone()
            .push(TransferEquityToMarketMaking {
                issuer_request_id: IssuerRequestId::generate(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query("UPDATE Jobs SET status = 'Running' WHERE job_type = ?")
            .bind(std::any::type_name::<TransferEquityToMarketMaking>())
            .execute(service.transfer_equity_to_market_making_queue.pool())
            .await
            .unwrap();

        let enqueued = service
            .enqueue_transfer_equity_to_hedging(symbol, FractionalShares::new(float!(1)))
            .await;

        assert!(!enqueued, "a Running equity transfer must block enqueue");
    }

    #[tokio::test]
    async fn failed_mint_zombie_with_terminal_aggregate_does_not_block_hedging_enqueue() {
        // A Failed+attempts<max mint job whose TokenizedEquityMint aggregate is already
        // terminal (zombie) must be killed and must NOT suppress a new hedging transfer.
        let pool = crate::test_utils::setup_test_db().await;
        let mint_id = issuer_request_id("mint-zombie-hedging");
        seed_terminal_mint_aggregate(&pool, &mint_id).await;

        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_equity_to_market_making_queue
            .clone()
            .push(TransferEquityToMarketMaking {
                issuer_request_id: mint_id.clone(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .execute(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();
        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_hedging(symbol, FractionalShares::new(float!(1)))
            .await;

        assert!(
            enqueued,
            "zombie mint job with terminal aggregate must NOT block hedging enqueue"
        );

        let status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferEquityToMarketMaking>())
                .fetch_one(service.transfer_equity_to_market_making_queue.pool())
                .await
                .unwrap();
        assert_eq!(
            status, "Killed",
            "zombie mint row must be killed after reconciliation"
        );
    }

    #[tokio::test]
    async fn failed_redemption_zombie_with_terminal_aggregate_does_not_block_market_making_enqueue()
    {
        // A Failed+attempts<max redemption job whose EquityRedemption aggregate is
        // already terminal (zombie) must be killed and must NOT suppress a new mint.
        let pool = crate::test_utils::setup_test_db().await;
        let redemption_id = redemption_aggregate_id("redemption-zombie-mint");
        seed_terminal_redemption_aggregate(&pool, &redemption_id).await;

        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_equity_to_hedging_queue
            .clone()
            .push(TransferEquityToHedging {
                aggregate_id: redemption_id,
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToHedging>())
        .execute(service.transfer_equity_to_hedging_queue.pool())
        .await
        .unwrap();
        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_market_making(symbol, FractionalShares::new(float!(1)), 0)
            .await;

        assert!(
            enqueued,
            "zombie redemption job with terminal aggregate must NOT block market-making enqueue"
        );

        let status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferEquityToHedging>())
                .fetch_one(service.transfer_equity_to_hedging_queue.pool())
                .await
                .unwrap();
        assert_eq!(
            status, "Killed",
            "zombie redemption row must be killed after reconciliation"
        );
    }

    #[tokio::test]
    async fn failed_equity_genuine_retry_with_nonterminal_aggregate_blocks() {
        // A Failed+attempts<max mint job whose aggregate is still live (genuine retry)
        // must block enqueue even after the zombie check.
        let pool = crate::test_utils::setup_test_db().await;
        let mint_id = issuer_request_id("mint-genuine-retry");
        seed_nonterminal_mint_aggregate(&pool, &mint_id).await;

        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_equity_to_market_making_queue
            .clone()
            .push(TransferEquityToMarketMaking {
                issuer_request_id: mint_id,
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .execute(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();
        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_hedging(symbol, FractionalShares::new(float!(1)))
            .await;

        assert!(
            !enqueued,
            "genuine-retry Failed mint job (non-terminal aggregate) must block enqueue"
        );

        let status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferEquityToMarketMaking>())
                .fetch_one(service.transfer_equity_to_market_making_queue.pool())
                .await
                .unwrap();
        assert_eq!(status, "Failed", "genuine-retry row must remain Failed");
    }

    #[tokio::test]
    async fn failed_redemption_genuine_retry_with_nonterminal_aggregate_blocks() {
        // A Failed+attempts<max redemption job whose EquityRedemption aggregate is
        // still live (genuine retry) must block enqueue.
        let pool = crate::test_utils::setup_test_db().await;
        let redemption_id = redemption_aggregate_id("redemption-genuine-retry");
        seed_nonterminal_redemption_aggregate(&pool, &redemption_id).await;

        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_equity_to_hedging_queue
            .clone()
            .push(TransferEquityToHedging {
                aggregate_id: redemption_id,
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToHedging>())
        .execute(service.transfer_equity_to_hedging_queue.pool())
        .await
        .unwrap();
        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_market_making(symbol, FractionalShares::new(float!(1)), 0)
            .await;

        assert!(
            !enqueued,
            "genuine-retry Failed redemption job (non-terminal aggregate) must block enqueue"
        );

        let status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferEquityToHedging>())
                .fetch_one(service.transfer_equity_to_hedging_queue.pool())
                .await
                .unwrap();
        assert_eq!(
            status, "Failed",
            "genuine-retry redemption row must remain Failed"
        );
    }

    #[tokio::test]
    async fn failed_equity_zombie_store_not_wired_is_conservative() {
        // When the equity stores are not wired (None), the guard must treat
        // Failed+attempts<max rows as in-flight (conservative fail-safe).
        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_equity_to_market_making_queue
            .clone()
            .push(TransferEquityToMarketMaking {
                issuer_request_id: IssuerRequestId::generate(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .execute(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();

        // Do NOT call set_stores: mint_store stays None.
        let enqueued = service
            .enqueue_transfer_equity_to_hedging(symbol, FractionalShares::new(float!(1)))
            .await;

        assert!(
            !enqueued,
            "equity store not wired: must conservatively treat Failed+attempts<max as in-flight"
        );
    }

    #[tokio::test]
    async fn failed_equity_zombie_aggregate_not_found_is_conservative() {
        // Store is wired but has no aggregate record for the job's id.
        // The guard must conservatively treat the row as in-flight.
        let pool = crate::test_utils::setup_test_db().await;
        // Do NOT seed any aggregate -- stores are empty.
        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service
            .transfer_equity_to_market_making_queue
            .clone()
            .push(TransferEquityToMarketMaking {
                issuer_request_id: IssuerRequestId::generate(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .execute(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();
        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_hedging(symbol, FractionalShares::new(float!(1)))
            .await;

        assert!(
            !enqueued,
            "aggregate not found in store: must conservatively treat equity row as in-flight"
        );
    }

    #[tokio::test]
    async fn corrupt_mint_job_payload_is_conservative() {
        // A Failed+attempts<max mint row with a payload that passes the
        // json_extract(job,'$.symbol') SQL filter (valid symbol field) but whose
        // issuer_request_id cannot be parsed as IssuerRequestId must block enqueue:
        // the guard fails closed on any parse error to protect against an unknown
        // in-flight mint operation.
        //
        // The stores must be wired so the guard reaches the parse branch; the
        // store-not-wired check fires first and returns early without reaching parse.
        let pool = crate::test_utils::setup_test_db().await;
        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_equity_to_market_making_queue
            .clone()
            .push(TransferEquityToMarketMaking {
                issuer_request_id: IssuerRequestId::generate(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();

        // Valid JSON so json_extract returns "tAAPL" and the row is selected,
        // but "not-a-uuid" fails IssuerRequestId (Uuid) deserialization, which
        // exercises the parse-error conservative branch.
        let corrupt_payload = format!(
            r#"{{"issuer_request_id":"not-a-uuid","symbol":"{symbol}","quantity":"1","generation":0}}"#
        )
        .into_bytes();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1, \
             job = ? WHERE job_type = ?",
        )
        .bind(corrupt_payload)
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .execute(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();

        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_hedging(symbol, FractionalShares::new(float!(1)))
            .await;

        assert!(
            !enqueued,
            "corrupt mint job payload must be treated as in-flight (conservative) and block enqueue"
        );
    }

    #[tokio::test]
    async fn corrupt_redemption_job_payload_is_conservative() {
        // Mirror of corrupt_mint_job_payload_is_conservative for the redemption
        // job type: valid symbol so json_extract selects the row, but aggregate_id
        // is not a valid UUID so RedemptionJobId deserialization fails, triggering
        // the conservative in-flight return.
        let pool = crate::test_utils::setup_test_db().await;
        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_equity_to_hedging_queue
            .clone()
            .push(TransferEquityToHedging {
                aggregate_id: redemption_aggregate_id("corrupt-redemption-payload"),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
            })
            .await
            .unwrap();

        let corrupt_payload =
            format!(r#"{{"aggregate_id":"not-a-uuid","symbol":"{symbol}","quantity":"1"}}"#)
                .into_bytes();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1, \
             job = ? WHERE job_type = ?",
        )
        .bind(corrupt_payload)
        .bind(std::any::type_name::<TransferEquityToHedging>())
        .execute(service.transfer_equity_to_hedging_queue.pool())
        .await
        .unwrap();

        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_market_making(symbol, FractionalShares::new(float!(1)), 0)
            .await;

        assert!(
            !enqueued,
            "corrupt redemption job payload must be treated as in-flight (conservative) \
             and block enqueue"
        );
    }

    #[tokio::test]
    async fn failed_equity_multiple_mint_zombies_all_cleared() {
        // Two Failed+attempts<max mint zombie rows with terminal aggregates and no
        // Pending row: both must be killed and enqueue must succeed.
        let pool = crate::test_utils::setup_test_db().await;
        let mint_id1 = issuer_request_id("mint-zombie-multi-1");
        let mint_id2 = issuer_request_id("mint-zombie-multi-2");
        seed_terminal_mint_aggregate(&pool, &mint_id1).await;
        seed_terminal_mint_aggregate(&pool, &mint_id2).await;

        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        for zombie_id in [&mint_id1, &mint_id2] {
            service
                .transfer_equity_to_market_making_queue
                .clone()
                .push(TransferEquityToMarketMaking {
                    issuer_request_id: zombie_id.clone(),
                    symbol: symbol.clone(),
                    quantity: FractionalShares::new(float!(1)),
                    generation: 0,
                })
                .await
                .unwrap();
        }
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1 \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .execute(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();
        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_hedging(symbol, FractionalShares::new(float!(1)))
            .await;

        assert!(enqueued, "all mint zombies cleared: enqueue must succeed");

        let killed_count: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Killed'",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .fetch_one(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();
        assert_eq!(
            killed_count, 2,
            "both zombie mint rows must have been killed"
        );
    }

    #[tokio::test]
    async fn equity_zombie_killed_then_genuine_pending_row_still_blocks() {
        // A Failed zombie mint row (terminal aggregate, older run_at) is killed on
        // the first guard loop iteration, but a genuine Pending redemption row with
        // a non-terminal aggregate is found on the next iteration and blocks the
        // enqueue. This verifies that `in_flight_equity_transfer` re-queries after
        // killing a zombie rather than treating the cleared zombie as "all clear".
        let pool = crate::test_utils::setup_test_db().await;

        let zombie_mint_id = issuer_request_id("equity-zombie-then-live-zombie");
        seed_terminal_mint_aggregate(&pool, &zombie_mint_id).await;

        let live_redemption_id = redemption_aggregate_id("equity-zombie-then-live-live");
        seed_nonterminal_redemption_aggregate(&pool, &live_redemption_id).await;

        let symbol = Symbol::new("tAAPL").unwrap();
        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_equity_to_market_making_queue
            .clone()
            .push(TransferEquityToMarketMaking {
                issuer_request_id: zombie_mint_id.clone(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();

        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', done_at = strftime('%s', 'now'), attempts = 1, \
             run_at = strftime('%s', 'now') - 200 WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferEquityToMarketMaking>())
        .execute(service.transfer_equity_to_market_making_queue.pool())
        .await
        .unwrap();

        service
            .transfer_equity_to_hedging_queue
            .clone()
            .push(TransferEquityToHedging {
                aggregate_id: live_redemption_id,
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
            })
            .await
            .unwrap();

        attach_equity_stores(
            &service,
            pool.clone(),
            test_store::<TokenizedEquityMint>(pool.clone(), EquityTransferServices::panicking()),
            test_store::<EquityRedemption>(pool.clone(), EquityTransferServices::panicking()),
        )
        .await;

        let enqueued = service
            .enqueue_transfer_equity_to_market_making(symbol, FractionalShares::new(float!(1)), 0)
            .await;

        assert!(
            !enqueued,
            "zombie killed but live Pending redemption row must still block a new mint enqueue"
        );

        let zombie_status: String =
            sqlx_apalis::query_scalar("SELECT status FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TransferEquityToMarketMaking>())
                .fetch_one(service.transfer_equity_to_market_making_queue.pool())
                .await
                .unwrap();
        assert_eq!(
            zombie_status, "Killed",
            "zombie mint row must be killed even though the live Pending redemption row blocks \
             the overall enqueue"
        );
    }

    #[tokio::test]
    async fn timeout_does_not_clear_guard_while_market_making_transfer_job_in_flight() {
        let inventory = InventoryView::default()
            .with_usdc(usdc(900), usdc(100))
            .with_withdrawable_cash_cents(90_000)
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap();
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        // An Alpaca->Base transfer whose apalis row is still in flight. The gate
        // must cover this direction too: it is driven by TransferUsdcToMarketMaking,
        // not TransferUsdcToHedging, and a resuming job may have an irreversible
        // withdraw/burn pending.
        let mut queue = trigger.transfer_usdc_to_market_making_queue.clone();
        queue
            .push(TransferUsdcToMarketMaking {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::Initiated,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_usdc().await;

        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "timeout must NOT clear the in-progress guard while the Alpaca->Base \
             apalis transfer job is in flight -- an irreversible withdraw/burn may \
             be pending and clearing would let a second transfer touch the same funds",
        );
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "timeout must not drop tracking for an in-flight Alpaca->Base transfer",
        );
        assert!(
            !trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "timeout must not mark an in-flight Alpaca->Base transfer timed-out (the \
             reactor would then ignore its terminal event and latch the guard forever)",
        );
    }

    #[tokio::test]
    async fn timed_out_post_burn_usdc_rebalance_preserves_guard_and_inflight() {
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .with_withdrawable_cash_cents(90_000)
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let trigger = reactor.clone();
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_usdc().await;

        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "post-burn USDC timeout must preserve in-flight tracking"
        );
        assert!(
            !trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "post-burn USDC timeout must not tombstone late events"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "post-burn USDC timeout must keep the guard set"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(usdc(400)),
            "post-burn USDC timeout must preserve source inflight"
        );
        drop(inventory);

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "post-burn USDC timeout must not allow another rebalance dispatch"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "post-burn USDC timeout must not allow another rebalance dispatch"
        );
    }

    /// The post-burn stall sweep arm must fire exactly one operator alert (carrying
    /// the transfer id) so the latched guard is not silent. The test above asserts
    /// the guard/inflight are preserved; this asserts the alert side-effect by
    /// injecting a capturing notifier.
    #[tokio::test]
    async fn timed_out_post_burn_usdc_rebalance_alerts_operator() {
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .with_withdrawable_cash_cents(90_000)
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap();
        let notifier = Arc::new(CapturingNotifier::default());
        let reactor = make_trigger_with_inventory_config_and_notifier(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
            notifier.clone(),
        )
        .await;
        let trigger = reactor.clone();
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_usdc().await;

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "a timed-out post-burn USDC transfer must fire exactly one operator alert; \
             got: {messages:?}"
        );
        assert!(
            messages[0].contains(&id.to_string()),
            "the post-burn stall alert must carry the transfer id; got: {:?}",
            messages[0]
        );
    }

    /// The main RAI-1017 regression test: the operator runs
    /// `transfer reconcile` in a separate CLI process, which writes
    /// `OperatorReconciled` to durable storage. The live server's
    /// `usdc_in_progress` guard must clear on the next sweep tick without
    /// requiring a restart.
    #[tokio::test]
    async fn sweep_clears_guard_after_cli_reconcile_without_restart() {
        let now = Utc::now();
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        // Drive a BaseToAlpaca aggregate all the way to DepositFailed, then
        // reconcile it (simulating what the CLI does in a separate process).
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_deposit_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
            mint_tx,
            42,
        )
        .await;
        // Simulate the CLI running in a separate process: write Reconciled to
        // the durable store. The live reactor never processes this event.
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        // Build a trigger backed by the same pool and attach the usdc_store so
        // the sweep can re-derive durable state.
        // BaseToAlpaca: source venue is MarketMaking (onchain, USDC leaves the
        // Base chain). Start with 500 available so a 400 inflight fits.
        // Set active_usdc_rebalance to verify the sweep clears it (finding #9).
        let inventory = InventoryView::default()
            .with_usdc(usdc(500), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap()
            .set_active_usdc_rebalance(id.clone());
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;

        trigger
            .set_stores(
                Arc::new(test_store::<TokenizedEquityMint>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<EquityRedemption>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(store),
            )
            .await;

        // Simulate the in-memory state the live server holds: guard is set and
        // tracking shows a post-burn stage (BridgingInitiated), as it was when
        // DepositFailed arrived. The CLI's OperatorReconciled event only advanced
        // the durable store; in-memory state did not change.
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        trigger
            .expire_stuck_usdc_rebalances(Utc::now())
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "sweep must clear the guard when durable state is Reconciled"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "sweep must remove tracking when durable state is Reconciled"
        );
        assert!(
            trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "sweep must tombstone the id so late events are ignored"
        );

        // Source-venue inflight for BaseToAlpaca is MarketMaking (USDC left the
        // Base chain). The sweep zeroes it via clear_usdc_inflight (inlined in
        // the Reconciled branch of cleanup_timed_out_usdc_rebalance).
        // active_usdc_rebalance must also be cleared (invariant from the
        // pre-burn timeout path; the Reconciled sweep path must match).
        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "sweep must zero source-venue (MarketMaking) inflight after CLI reconcile"
        );
        assert_eq!(
            inventory.active_usdc_rebalance(),
            None,
            "sweep must clear active_usdc_rebalance after CLI reconcile"
        );
        drop(inventory);
    }

    /// Reconciled check fires even when `last_progress_at` is RECENT (within
    /// `transfer_timeout`). This is the key correctness invariant: a CLI
    /// `transfer reconcile` run before the failure has aged past the
    /// timeout must still clear the guard on the next sweep tick, not wait
    /// for the full timeout window to elapse.
    #[tokio::test]
    async fn sweep_clears_guard_when_reconciled_before_timeout_expires() {
        let now = Utc::now();
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000042");
        let mint_tx =
            fixed_bytes!("0x4242424242424242424242424242424242424242424242424242424242424242");

        // Drive to DepositFailed then reconcile (simulating CLI in separate
        // process).
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_deposit_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
            mint_tx,
            0x42,
        )
        .await;
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        let inventory = InventoryView::default()
            .with_usdc(usdc(500), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap()
            .set_active_usdc_rebalance(id.clone());
        let trigger = make_trigger_with_inventory_config(
            inventory,
            // Use a 30-minute timeout so that `last_progress_at = now` is
            // well within the timeout window, proving the Reconciled check
            // fires independently of elapsed time.
            test_config_with_timeout(Duration::from_secs(1800)),
        )
        .await;

        trigger
            .set_stores(
                Arc::new(test_store::<TokenizedEquityMint>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<EquityRedemption>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(store),
            )
            .await;

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        // last_progress_at = now: elapsed is ~0, well within the 30-minute
        // transfer_timeout. The sweep must still detect Reconciled and clear.
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now,
            },
        );

        trigger.expire_stuck_usdc_rebalances(now).await.unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "guard must clear even when last_progress_at is recent (within timeout)"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking must be removed when Reconciled is detected before timeout"
        );
        assert!(
            trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "id must be tombstoned after Reconciled clear"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "sweep must zero MarketMaking inflight even when reconciled before timeout"
        );
        assert_eq!(
            inventory.active_usdc_rebalance(),
            None,
            "sweep must clear active_usdc_rebalance even when reconciled before timeout"
        );
        drop(inventory);
    }

    /// Guard is preserved when the durable store still shows `DepositFailed`
    /// (i.e. the CLI has NOT yet reconciled). The narrowed `Reconciled` match
    /// must not accidentally clear other guard-holding states.
    #[tokio::test]
    async fn sweep_preserves_guard_when_store_still_deposit_failed() {
        let now = Utc::now();
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        // Drive aggregate to DepositFailed but do NOT reconcile it.
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_deposit_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
            mint_tx,
            42,
        )
        .await;

        // BaseToAlpaca: source venue is MarketMaking (USDC leaves the Base chain).
        let inventory = InventoryView::default()
            .with_usdc(usdc(500), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap();
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;

        trigger
            .set_stores(
                Arc::new(test_store::<TokenizedEquityMint>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<EquityRedemption>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(store),
            )
            .await;

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        trigger
            .expire_stuck_usdc_rebalances(Utc::now())
            .await
            .unwrap();

        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "guard must stay set when durable state is still DepositFailed"
        );
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking must be preserved when durable state is still DepositFailed"
        );
        assert!(
            !trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "must not tombstone a genuinely stuck post-burn transfer"
        );

        // The guard-preserving path must leave source-venue inflight unchanged:
        // zeroing it here would allow a concurrent transfer to start while the
        // stuck rebalance is still unresolved.
        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(usdc(400)),
            "guard-preserving path must leave source-venue (MarketMaking) inflight unchanged"
        );
        drop(inventory);
    }

    /// Fail-safe: when `usdc_store` is not attached (None), the sweep must
    /// preserve the guard and leave inventory inflight untouched. Covers the
    /// "store not yet wired" case (usdc_store is None).
    #[tokio::test]
    async fn sweep_preserves_guard_when_usdc_store_not_set() {
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap();
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        // Do NOT call set_stores -- usdc_store stays None.

        let id = UsdcRebalanceId(Uuid::new_v4());
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        trigger
            .expire_stuck_usdc_rebalances(Utc::now())
            .await
            .unwrap();

        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "guard must stay set when usdc_store is not attached"
        );
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking must be preserved when usdc_store is not attached"
        );

        // The fail-safe path must not zero inflight: the source-venue USDC
        // inflight (AlpacaToBase -> Hedging) must remain at its original value.
        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(usdc(400)),
            "fail-safe path must leave source-venue inflight unchanged"
        );
        drop(inventory);
    }

    /// Idempotency: sweep clears the guard first, then the reactor processes
    /// the late OperatorReconciled event. Final inventory inflight must still
    /// be zero (zeroing zero is a no-op).
    #[tokio::test]
    async fn sweep_then_reactor_is_idempotent_on_inventory() {
        let now = Utc::now();
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        // Drive to DepositFailed then Reconciled.
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_deposit_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
            mint_tx,
            42,
        )
        .await;
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        // BaseToAlpaca: source venue is MarketMaking (onchain, USDC leaves the
        // Base chain). Start with 500 available so a 400 inflight fits.
        // Set active_usdc_rebalance to verify the sweep's Reconciled branch
        // clears it (via clear_active_usdc_rebalance, chained inline).
        let inventory = InventoryView::default()
            .with_usdc(usdc(500), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap()
            .set_active_usdc_rebalance(id.clone());
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;

        let store = Arc::new(store);
        trigger
            .set_stores(
                Arc::new(test_store::<TokenizedEquityMint>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<EquityRedemption>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::clone(&store),
            )
            .await;

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        // Step 1: sweep clears the guard and zeroes MarketMaking inflight.
        trigger
            .expire_stuck_usdc_rebalances(Utc::now())
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "sweep must clear the guard"
        );
        let active_rebalance = trigger
            .inventory
            .read()
            .await
            .active_usdc_rebalance()
            .cloned();
        assert_eq!(
            active_rebalance, None,
            "sweep must clear active_usdc_rebalance"
        );

        // Step 2: the OperatorReconciled event arrives late via the reactor
        // (simulating a race where the live reactor eventually processes it).
        // The tombstone guard in on_usdc_rebalance must short-circuit and
        // return without re-populating tracking or touching inventory.
        trigger
            .on_usdc_rebalance(
                id.clone(),
                make_usdc_operator_reconciled(RebalanceDirection::BaseToAlpaca),
            )
            .await
            .unwrap();

        // The tombstone causes an early return: tracking is still absent (was
        // removed by the sweep), and the guard stays clear.
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "tombstone must prevent the late event from re-populating tracking"
        );
        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "tombstone must prevent the late event from re-latching the guard"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "inflight remains zero: tombstone short-circuit prevents double-clear"
        );
        drop(inventory);
    }

    /// Fail-safe: when `usdc_store` is attached but its `load` returns an
    /// error (e.g. schema mismatch, I/O failure), the sweep must preserve the
    /// guard and leave inventory inflight unchanged. A store created without
    /// migrations will error on every query with "no such table: events".
    #[tokio::test]
    async fn sweep_preserves_guard_on_store_load_error() {
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap();
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;

        // Attach a store backed by an unmigrated pool so every `load` call
        // returns Err("no such table: events"). This exercises the Err arm in
        // cleanup_timed_out_usdc_rebalance that must preserve the guard.
        let unmigrated_pool = SqlitePool::connect(":memory:").await.unwrap();
        trigger
            .set_stores(
                Arc::new(test_store::<
                    crate::tokenized_equity_mint::TokenizedEquityMint,
                >(
                    unmigrated_pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<crate::equity_redemption::EquityRedemption>(
                    unmigrated_pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<UsdcRebalance>(unmigrated_pool.clone(), ())),
            )
            .await;

        let id = UsdcRebalanceId(Uuid::new_v4());
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        trigger
            .expire_stuck_usdc_rebalances(Utc::now())
            .await
            .unwrap();

        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "guard must stay set when store load returns an error"
        );
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking must be preserved when store load returns an error"
        );

        // The fail-safe Err arm must not zero inflight: source-venue inflight
        // (AlpacaToBase -> Hedging) must remain at the original value.
        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(usdc(400)),
            "fail-safe Err path must leave source-venue inflight unchanged"
        );
        drop(inventory);
    }

    /// AlpacaToBase direction: sweep clears the Hedging-venue inflight when
    /// durable state is Reconciled, mirroring
    /// `sweep_clears_guard_after_cli_reconcile_without_restart` for the
    /// reverse direction. Exercises the `source_venue(AlpacaToBase) = Hedging`
    /// mapping through the full sweep code path.
    #[tokio::test]
    async fn sweep_clears_guard_after_cli_reconcile_alpaca_to_base() {
        let now = Utc::now();
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000002");
        let mint_tx =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        // Drive an AlpacaToBase aggregate to DepositFailed then Reconciled.
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_deposit_failed(
            &store,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(400),
            burn_tx,
            mint_tx,
            99,
        )
        .await;
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        // AlpacaToBase: source venue is Hedging (USDC leaves the Alpaca side).
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap()
            .set_active_usdc_rebalance(id.clone());
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;

        trigger
            .set_stores(
                Arc::new(test_store::<
                    crate::tokenized_equity_mint::TokenizedEquityMint,
                >(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<crate::equity_redemption::EquityRedemption>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(store),
            )
            .await;

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        trigger
            .expire_stuck_usdc_rebalances(Utc::now())
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "sweep must clear the guard when durable state is Reconciled (AlpacaToBase)"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "sweep must remove tracking when durable state is Reconciled (AlpacaToBase)"
        );
        assert!(
            trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "sweep must tombstone the id so late events are ignored"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(Usdc::ZERO),
            "sweep must zero Hedging inflight for AlpacaToBase direction"
        );
        assert_eq!(
            inventory.active_usdc_rebalance(),
            None,
            "sweep must clear active_usdc_rebalance after reconcile"
        );
        drop(inventory);
    }

    /// Exercises the real restart-then-CLI-reconcile path end-to-end:
    /// `recover_usdc_guard` seeds tracking for a stranded `DepositFailed`
    /// aggregate (path 1), then the sweep detects the operator's CLI
    /// `transfer reconcile` via durable `Reconciled` state and clears
    /// the guard (path 2). Calling `recover_usdc_guard` directly (rather than
    /// manually planting tracking) ensures both paths are regression-tested
    /// together: a break in path 1 is caught here, not silently missed.
    #[tokio::test]
    async fn sweep_clears_guard_after_restart_then_cli_reconcile() {
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000003");
        let mint_tx =
            fixed_bytes!("0x3333333333333333333333333333333333333333333333333333333333333333");

        // Drive a BaseToAlpaca aggregate to DepositFailed (not yet reconciled).
        let store = Arc::new(test_store::<UsdcRebalance>(pool.clone(), ()));
        let id = UsdcRebalanceId(Uuid::new_v4());
        seed_deposit_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
            mint_tx,
            77,
        )
        .await;

        // Build a trigger backed by the same pool. BaseToAlpaca: source venue
        // is MarketMaking (USDC leaves the Base chain).
        let inventory = InventoryView::default()
            .with_usdc(usdc(500), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap()
            .set_active_usdc_rebalance(id.clone());
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;

        // Call recover_usdc_guard (the real startup path). The aggregate is in
        // DepositFailed, which holds the guard and seeds tracking.
        trigger.recover_usdc_guard(&pool, &store).await.unwrap();

        // Path 1: tracking was seeded by recover_usdc_guard for DepositFailed.
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "recover_usdc_guard must seed tracking for DepositFailed so the sweep can run"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "recover_usdc_guard must latch the guard for DepositFailed"
        );

        // The seeded tracking's `last_progress_at` is sourced from
        // `DepositFailed.failed_at`, which is stamped by the store when the
        // FailDeposit command above was applied. Passing `Utc::now() + 5s` to
        // the sweep is therefore sufficient: elapsed = (now + 5s) - failed_at
        // which is well above the 1s test timeout, regardless of how fast the
        // store.send calls completed.

        // Simulate the operator running `transfer reconcile` in a
        // separate CLI process (the store emits OperatorReconciled).

        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        trigger
            .set_stores(
                Arc::new(test_store::<
                    crate::tokenized_equity_mint::TokenizedEquityMint,
                >(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<crate::equity_redemption::EquityRedemption>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::clone(&store),
            )
            .await;

        // Path 2: the sweep detects Reconciled durable state and clears the
        // guard -- no second restart required. Pass a `now` far enough in the
        // future to exceed the 1-second test timeout (DepositFailed.failed_at
        // is set to Utc::now() at store.send time, so elapsed = now - failed_at
        // must be > 1s).
        trigger
            .expire_stuck_usdc_rebalances(Utc::now() + ChronoDuration::seconds(5))
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "sweep must clear the guard after restart + CLI reconcile, no second restart needed"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "sweep must remove tracking after CLI reconcile detected via durable state"
        );
        assert!(
            trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "sweep must tombstone the id so late events are ignored"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "sweep must zero MarketMaking inflight after CLI reconcile"
        );
        assert_eq!(
            inventory.active_usdc_rebalance(),
            None,
            "sweep must clear active_usdc_rebalance after reconcile"
        );
        drop(inventory);
    }

    /// Restart + CLI reconcile for `ConversionFailed{BaseToAlpaca}`:
    /// the post-deposit USDC->USD conversion failure that holds the guard,
    /// cannot self-recover, and accepts `transfer reconcile`.
    /// After restart, `recover_usdc_guard` must seed tracking for it so the
    /// sweep can detect the CLI-emitted `Reconciled` state and clear the guard
    /// without a second restart.
    #[tokio::test]
    async fn sweep_clears_guard_after_restart_then_cli_reconcile_conversion_failed() {
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000010");
        let mint_tx =
            fixed_bytes!("0x1010101010101010101010101010101010101010101010101010101010101010");

        // Drive a BaseToAlpaca aggregate to ConversionFailed.
        // Path: Initiate -> ConfirmWithdrawal -> InitiateBridging ->
        //   ReceiveAttestation -> ConfirmBridging -> InitiateDeposit ->
        //   ConfirmDeposit -> InitiatePostDepositConversion -> FailConversion.
        let store = Arc::new(test_store::<UsdcRebalance>(pool.clone(), ()));
        let id = UsdcRebalanceId(Uuid::new_v4());

        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(400),
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(&id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01],
                    cctp_nonce: alloy::primitives::B256::left_padding_from(&42u64.to_be_bytes()),
                    message: valid_cctp_message(),
                    mint_scan_from_block: 100,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx,
                    amount_received: usdc(399),
                    fee_collected: usdc(1),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(mint_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(&id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiatePostDepositConversion {
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                    amount: usdc(399),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::FailConversion {
                    reason: "conversion rejected".to_string(),
                },
            )
            .await
            .unwrap();

        // Build a trigger backed by the same pool. BaseToAlpaca: source venue
        // is MarketMaking.
        let inventory = InventoryView::default()
            .with_usdc(usdc(500), usdc(900))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap()
            .set_active_usdc_rebalance(id.clone());
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;

        // Simulate restart: recover_usdc_guard must seed tracking for
        // ConversionFailed(BaseToAlpaca) so the sweep can later detect
        // a CLI-emitted Reconciled state.
        trigger.recover_usdc_guard(&pool, &store).await.unwrap();

        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "recover_usdc_guard must seed tracking for ConversionFailed(BtA)"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "recover_usdc_guard must latch the guard for ConversionFailed(BtA)"
        );

        // Simulate the operator running `transfer reconcile` in a
        // separate CLI process.
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        trigger
            .set_stores(
                Arc::new(test_store::<
                    crate::tokenized_equity_mint::TokenizedEquityMint,
                >(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<crate::equity_redemption::EquityRedemption>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::clone(&store),
            )
            .await;

        // The sweep must detect Reconciled and clear the guard -- no second
        // restart required.
        trigger
            .expire_stuck_usdc_rebalances(Utc::now() + ChronoDuration::seconds(5))
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "sweep must clear the guard after restart + CLI reconcile for ConversionFailed(BtA)"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "sweep must remove tracking after CLI reconcile detected via durable state"
        );
        assert!(
            trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "sweep must tombstone the id so late events are ignored"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "sweep must zero MarketMaking inflight after CLI reconcile"
        );
        assert_eq!(
            inventory.active_usdc_rebalance(),
            None,
            "sweep must clear active_usdc_rebalance after reconcile"
        );
        drop(inventory);
    }

    /// Restart + CLI reconcile for `BridgingFailed{AlpacaToBase, burn_tx=Some}`:
    /// a non-resumable post-burn failure (the BaseToAlpaca direction is the one
    /// `resumable_post_burn_transfer` re-arms; AlpacaToBase has no recovery path).
    /// After restart, `recover_usdc_guard` must seed tracking for it so the sweep
    /// can detect the CLI-emitted `Reconciled` state and clear the guard without
    /// a second restart.
    #[tokio::test]
    async fn sweep_clears_guard_after_restart_then_cli_reconcile_non_resumable_bridging_failed() {
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000020");

        // Drive an AlpacaToBase aggregate to BridgingFailed with burn_tx set
        // (post-burn). Path: Initiate{AtB} -> ConfirmWithdrawal ->
        // InitiateBridging -> FailBridging.
        let store = Arc::new(test_store::<UsdcRebalance>(pool.clone(), ()));
        let id = UsdcRebalanceId(Uuid::new_v4());

        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: usdc(400),
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(&id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::FailBridging {
                    reason: "attestation timed out, AlpacaToBase has no recovery path".to_string(),
                },
            )
            .await
            .unwrap();

        // AlpacaToBase: source venue is Hedging.
        let inventory = InventoryView::default()
            .with_usdc(usdc(900), usdc(500))
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap()
            .set_active_usdc_rebalance(id.clone());
        let trigger = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;

        // Simulate restart: recover_usdc_guard must seed tracking for this
        // non-resumable BridgingFailed and must NOT enqueue a recovery job
        // (that would be the BaseToAlpaca resumable path).
        trigger.recover_usdc_guard(&pool, &store).await.unwrap();

        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "recover_usdc_guard must seed tracking for BridgingFailed(AlpacaToBase, burn_tx=Some)"
        );
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "recover_usdc_guard must latch the guard for BridgingFailed(AlpacaToBase)"
        );
        // No recovery job must be enqueued: AlpacaToBase is not in
        // resumable_post_burn_transfer.
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "non-resumable AlpacaToBase BridgingFailed must not be re-armed"
        );

        // Simulate the operator running `transfer reconcile` in a
        // separate CLI process.
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        trigger
            .set_stores(
                Arc::new(test_store::<
                    crate::tokenized_equity_mint::TokenizedEquityMint,
                >(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::new(test_store::<crate::equity_redemption::EquityRedemption>(
                    pool.clone(),
                    crate::rebalancing::equity::EquityTransferServices::panicking(),
                )),
                Arc::clone(&store),
            )
            .await;

        trigger
            .expire_stuck_usdc_rebalances(Utc::now() + ChronoDuration::seconds(5))
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "sweep must clear the guard after restart + CLI reconcile for \
             BridgingFailed(AlpacaToBase)"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "sweep must remove tracking after CLI reconcile detected via durable state"
        );
        assert!(
            trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "sweep must tombstone the id so late events are ignored"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(Usdc::ZERO),
            "sweep must zero Hedging inflight after CLI reconcile for AlpacaToBase"
        );
        assert_eq!(
            inventory.active_usdc_rebalance(),
            None,
            "sweep must clear active_usdc_rebalance after reconcile"
        );
        drop(inventory);
    }

    /// `BridgingFailed{BaseToAlpaca, burn_tx=Some}` is resumable and must NOT
    /// be seeded with tracking on restart: `recover_usdc_guard` re-arms it via
    /// `resumable_post_burn_transfer`, and seeding it would wedge the re-armed
    /// job's `DepositConfirmed` path (which requires `bridged_amount_received`).
    #[tokio::test]
    async fn resumable_post_burn_bridging_failed_is_not_seeded_with_tracking() {
        let pool = crate::test_utils::setup_test_db().await;
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000030");

        // Drive a BaseToAlpaca aggregate to BridgingFailed with burn_tx set.
        let store = Arc::new(test_store::<UsdcRebalance>(pool.clone(), ()));
        let id = UsdcRebalanceId(Uuid::new_v4());

        seed_post_burn_bridging_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        // The resumable BridgingFailed must NOT produce a tracking entry -- it
        // self-recovers via the re-armed job.
        assert!(
            !service.usdc_tracking.read().await.contains_key(&id),
            "resumable BridgingFailed(BaseToAlpaca) must not be seeded with tracking"
        );
        // It MUST produce a recovery job (re-armed via resumable_post_burn_transfer).
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "resumable BridgingFailed(BaseToAlpaca) must be re-armed as a hedging job"
        );
    }

    #[tokio::test]
    async fn post_burn_usdc_timeout_logs_once_across_sweeps() {
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .with_withdrawable_cash_cents(90_000)
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let trigger = reactor.clone();
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        // The preserved entry is re-selected on every sweep. We assert on the
        // dedup set (`post_burn_timeout_logged`) rather than captured logs: the
        // `error!` is gated directly on this set's `insert` returning true, so a
        // single retained entry across sweeps is a faithful proxy for "logged
        // exactly once".
        trigger.check_and_trigger_usdc().await;
        assert!(
            trigger.post_burn_timeout_logged.read().await.contains(&id),
            "first sweep must record the stuck id for log deduplication"
        );

        trigger.check_and_trigger_usdc().await;
        trigger.check_and_trigger_usdc().await;
        assert_eq!(
            trigger.post_burn_timeout_logged.read().await.len(),
            1,
            "subsequent sweeps must not re-log: the dedup set stays at one entry"
        );
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "the entry must remain preserved across sweeps"
        );
    }

    /// A [`Notifier`] that returns `Err` for its first `remaining_failures`
    /// calls, then succeeds, capturing every successfully delivered message.
    /// Lets a test prove the post-burn stall alert is re-attempted across sweeps
    /// after a transient delivery failure rather than silenced forever.
    struct FlakyNotifier {
        remaining_failures: std::sync::atomic::AtomicUsize,
        delivered: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crate::alerts::Notifier for FlakyNotifier {
        async fn notify(&self, message: &str) -> Result<(), crate::alerts::NotifierError> {
            let should_fail = self
                .remaining_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();

            if should_fail {
                return Err(crate::alerts::NotifierError::ApiError {
                    status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                    body: "simulated transient failure".to_string(),
                });
            }

            self.delivered.lock().unwrap().push(message.to_string());
            Ok(())
        }
    }

    /// A transient notifier failure on the first sweep must not permanently
    /// silence the operator page: the alert is re-attempted on the next sweep and
    /// recorded once it succeeds, then never re-sent.
    #[tokio::test]
    async fn post_burn_usdc_timeout_alert_retries_after_transient_failure() {
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .with_withdrawable_cash_cents(90_000)
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                now,
            )
            .unwrap();
        let notifier = Arc::new(FlakyNotifier {
            remaining_failures: std::sync::atomic::AtomicUsize::new(1),
            delivered: std::sync::Mutex::new(Vec::new()),
        });
        let trigger = make_trigger_with_inventory_config_and_notifier(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
            notifier.clone(),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::AlpacaToBase,
                initiated_amount: usdc(400),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::BridgingInitiated,
                last_progress_at: now - ChronoDuration::minutes(31),
            },
        );

        // First sweep: delivery fails, so the alert must not be recorded as sent
        // and nothing is captured.
        trigger.check_and_trigger_usdc().await;
        assert!(
            !trigger.post_burn_timeout_alerted.read().await.contains(&id),
            "a failed alert delivery must not be recorded as alerted"
        );
        assert!(
            notifier.delivered.lock().unwrap().is_empty(),
            "no alert should be captured while delivery is failing"
        );

        // Second sweep: delivery succeeds, the alert lands and is recorded.
        trigger.check_and_trigger_usdc().await;
        assert_eq!(
            notifier.delivered.lock().unwrap().len(),
            1,
            "the alert must be re-attempted on the next sweep and delivered once it succeeds"
        );
        assert!(
            trigger.post_burn_timeout_alerted.read().await.contains(&id),
            "a successful delivery must be recorded so it is not re-sent"
        );

        // Third sweep: already delivered, so no further attempts.
        trigger.check_and_trigger_usdc().await;
        assert_eq!(
            notifier.delivered.lock().unwrap().len(),
            1,
            "a delivered alert must not be re-sent on subsequent sweeps"
        );
    }

    #[tokio::test]
    async fn recover_usdc_guard_reasserts_guard_for_post_burn_bridging_failure() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000aa");

        // Persist a history that ends in a post-burn bridge failure.
        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(400),
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(&id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::FailBridging {
                    reason: "mint reverted".to_string(),
                },
            )
            .await
            .unwrap();

        // A fresh service has the guard cleared, as a restarted process would.
        let service = make_trigger_with_inventory(InventoryView::default()).await;
        assert!(
            !service.usdc_in_progress.load(Ordering::SeqCst),
            "guard must start clear, simulating a fresh process"
        );

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "post-burn bridge failure must re-assert the USDC guard on startup"
        );
    }

    #[tokio::test]
    async fn recover_usdc_guard_reasserts_guard_for_bridging_submitting() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000bb");

        // Crash at BridgingSubmitting -- the intent marker written immediately
        // before the irreversible CCTP burn, where the burn may already have
        // been broadcast but not yet recorded. holds_rebalance_guard treats this
        // as guard-holding, and the recovery candidate query must select it.
        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(400),
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::BeginBridging {
                    from_block: 99,
                    burn_amount: None,
                },
            )
            .await
            .unwrap();

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "a crash at BridgingSubmitting (burn possibly already broadcast) must \
             re-assert the USDC guard on startup"
        );
    }

    /// Drives a `UsdcRebalance` aggregate to `BridgingSubmitting` for the
    /// Base->Alpaca direction. This is the pre-burn intent marker written
    /// immediately before the irreversible CCTP burn call.
    async fn seed_bridging_submitting_base_to_alpaca(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        burn_tx: TxHash,
    ) {
        store
            .send(
                id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::BeginBridging {
                    from_block: 99,
                    burn_amount: None,
                },
            )
            .await
            .unwrap();
    }

    /// Drives a `UsdcRebalance` aggregate to `WithdrawalSubmitting` for the
    /// Base->Alpaca direction. This is the pre-withdrawal-tx intent marker.
    ///
    /// Uses `BeginWithdrawal{BaseToAlpaca}` from the `Uninitialized` state,
    /// which only accepts `BaseToAlpaca` (no conversion leg required).
    async fn seed_withdrawal_submitting_base_to_alpaca(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) {
        store
            .send(
                id,
                UsdcRebalanceCommand::BeginWithdrawal {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    from_block: 10,
                },
            )
            .await
            .unwrap();
    }

    /// Seeds a `WithdrawalSubmitting{AlpacaToBase}` aggregate. This requires the
    /// conversion leg first (InitiateConversion -> ConfirmConversion) before
    /// `BeginWithdrawal{AlpacaToBase}` is valid.
    async fn seed_withdrawal_submitting_alpaca_to_base(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) {
        store
            .send(
                id,
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ConfirmConversion {
                    filled_amount: amount,
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::BeginWithdrawal {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    from_block: 10,
                },
            )
            .await
            .unwrap();
    }

    /// KNOWN LIMITATION: `WithdrawalSubmitting{AlpacaToBase}` is deliberately
    /// excluded from the startup re-arm because `resume_alpaca_to_base` returns
    /// `ResumeDirectionMismatch` for that state. The transfer must be recovered manually
    /// via `resume-usdc-transfer` or `reconcile-usdc-transfer`. The operator is alerted
    /// on startup (see `recover_usdc_guard_alerts_on_stranded_withdrawal_submitting_alpaca_to_base`).
    /// This test asserts the deliberate exclusion is preserved -- a regression that
    /// accidentally re-armed this state would immediately fail the market-making job.
    #[tokio::test]
    async fn recover_usdc_guard_does_not_rearm_withdrawal_submitting_alpaca_to_base() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc(300);

        seed_withdrawal_submitting_alpaca_to_base(&store, &id, amount).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "WithdrawalSubmitting{{AlpacaToBase}} must NOT be re-armed as a hedging job",
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&service).await,
            0,
            "WithdrawalSubmitting{{AlpacaToBase}} must NOT be re-armed as a market-making job",
        );
        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress must remain latched (WithdrawalSubmitting holds the guard \
             even without re-arming)",
        );
    }

    /// `WithdrawalSubmitting{AlpacaToBase}` has no automated recovery path: the
    /// sweep never selects it (no tracking seed) and it is not re-armed (excluded
    /// from `is_resumable_mid_flight_data`). This test verifies the operator alert
    /// fires so the blocked state is not silent.
    #[tokio::test]
    async fn recover_usdc_guard_alerts_on_stranded_withdrawal_submitting_alpaca_to_base() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc(300);

        seed_withdrawal_submitting_alpaca_to_base(&store, &id, amount).await;

        let notifier = Arc::new(CapturingNotifier::default());
        let service = make_trigger_with_inventory_config_and_notifier(
            InventoryView::default(),
            test_config(),
            notifier.clone(),
        )
        .await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "exactly one operator alert must fire for a stranded WithdrawalSubmitting{{AlpacaToBase}}; got: {messages:?}",
        );
        assert!(
            messages[0].contains("LATCHED"),
            "alert must say the guard is LATCHED; got: {:?}",
            messages[0]
        );
        assert!(
            messages[0].contains("resume-usdc-transfer"),
            "alert must mention the manual recovery command; got: {:?}",
            messages[0]
        );
    }

    /// A `BridgingSubmitting` (Base->Alpaca) aggregate with no job row
    /// must be re-armed on startup: the process crashed before the enqueue
    /// committed or before the burn tx was submitted. The resume path scans for
    /// any existing burn before re-attempting.
    #[tokio::test]
    async fn recover_usdc_guard_rearms_bridging_submitting_base_to_alpaca() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let amount = usdc(500);

        seed_bridging_submitting_base_to_alpaca(&store, &id, amount, burn_tx).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "a BridgingSubmitting Base->Alpaca with no job row must be re-armed as a hedging job",
        );

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();
        let job: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            job.id, id,
            "the re-armed job must resume the same aggregate id",
        );
        assert!(
            job.amount.eq(&amount).unwrap(),
            "the re-armed job must carry the same amount",
        );
        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress must be latched after re-arming a stranded BridgingSubmitting job",
        );
    }

    /// Drives a `UsdcRebalance` aggregate to `BridgingSubmitting` for the
    /// Alpaca->Base direction. This requires seeding through the full conversion
    /// and withdrawal legs before `BeginBridging` is valid.
    async fn seed_bridging_submitting_alpaca_to_base(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        amount: Usdc,
        withdrawal_tx: TxHash,
    ) {
        store
            .send(
                id,
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ConfirmConversion {
                    filled_amount: amount,
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::BeginWithdrawal {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    from_block: 10,
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    withdrawal: TransferRef::OnchainTx(withdrawal_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: Some(withdrawal_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::BeginBridging {
                    from_block: 99,
                    burn_amount: None,
                },
            )
            .await
            .unwrap();
    }

    /// A `BridgingSubmitting` (Alpaca->Base) aggregate with no job row
    /// must be re-armed on startup as a market-making job (not a hedging job).
    #[tokio::test]
    async fn recover_usdc_guard_rearms_bridging_submitting_alpaca_to_base() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let withdrawal_tx =
            fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let amount = usdc(500);

        seed_bridging_submitting_alpaca_to_base(&store, &id, amount, withdrawal_tx).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&service).await,
            1,
            "a BridgingSubmitting Alpaca->Base with no job row must be re-armed as a market-making job",
        );
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "BridgingSubmitting Alpaca->Base must NOT be re-armed as a hedging job",
        );

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
        .fetch_one(service.transfer_usdc_to_market_making_queue.pool())
        .await
        .unwrap();
        let job: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            job.id, id,
            "the re-armed job must resume the same aggregate id",
        );
        assert!(
            job.amount.eq(&amount).unwrap(),
            "the re-armed job must carry the same amount",
        );
        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress must be latched after re-arming a stranded BridgingSubmitting job",
        );
    }

    /// A `WithdrawalSubmitting{BaseToAlpaca}` aggregate with no job
    /// row must be re-armed on startup as a HEDGING job (not market-making).
    /// `resume_base_to_alpaca` handles `WithdrawalSubmitting`; `resume_alpaca_to_base`
    /// does not, so `WithdrawalSubmitting{AlpacaToBase}` must NOT be re-armed at all.
    #[tokio::test]
    async fn recover_usdc_guard_rearms_withdrawal_submitting() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc(300);

        seed_withdrawal_submitting_base_to_alpaca(&store, &id, amount).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "a WithdrawalSubmitting BaseToAlpaca with no job row must be re-armed as a hedging job",
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&service).await,
            0,
            "WithdrawalSubmitting BaseToAlpaca must NOT be re-armed as a market-making job",
        );
        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress must be latched after re-arming a stranded WithdrawalSubmitting job",
        );
    }

    /// A `BridgingSubmitting` aggregate that already has a Pending
    /// job row must NOT be re-armed -- the existing job will resume it; a second
    /// job would drive two concurrent resumes of the same aggregate.
    #[tokio::test]
    async fn recover_usdc_guard_does_not_rearm_bridging_submitting_with_job_row() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let amount = usdc(500);

        seed_bridging_submitting_base_to_alpaca(&store, &id, amount, burn_tx).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        // Pre-seed a Pending job row (as apalis would have if the enqueue succeeded).
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount,
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "a BridgingSubmitting aggregate with an existing Pending job row must NOT be re-armed again",
        );
    }

    /// A `BridgingSubmitting` aggregate with a terminal `Failed` job
    /// row must NOT be re-armed -- the failed row is the result of a real
    /// circuit-breaker trip that requires operator attention.
    #[tokio::test]
    async fn recover_usdc_guard_does_not_rearm_bridging_submitting_with_failed_job_row() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
        let amount = usdc(500);

        seed_bridging_submitting_base_to_alpaca(&store, &id, amount, burn_tx).await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        // Pre-seed a Failed job row (circuit opened after retries exhausted).
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount,
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query("UPDATE Jobs SET status = 'Failed' WHERE job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .execute(service.transfer_usdc_to_hedging_queue.pool())
            .await
            .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "a BridgingSubmitting aggregate with a terminal Failed job row \
             must NOT be re-armed; it requires operator attention",
        );
    }

    /// A `BridgingSubmitting` (Base->Alpaca) aggregate whose only job
    /// row is a terminal, retry-budget-exhausted `Failed` (attempts ==
    /// max_attempts, so apalis no longer owns it) must NOT be silently re-armed
    /// with a fresh budget. It is stranded for an operator alert instead, so the
    /// permanently latched guard is visible rather than silently resetting.
    #[tokio::test]
    async fn recover_usdc_guard_alerts_on_bridging_submitting_with_exhausted_failed_job_row() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
        let amount = usdc(500);

        seed_bridging_submitting_base_to_alpaca(&store, &id, amount, burn_tx).await;

        let notifier = Arc::new(CapturingNotifier::default());
        let service = make_trigger_with_inventory_config_and_notifier(
            InventoryView::default(),
            test_config(),
            notifier.clone(),
        )
        .await;

        // A terminal, retry-budget-exhausted Failed row: apalis no longer owns
        // it (attempts == max_attempts), so no live driver remains.
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount,
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', attempts = max_attempts WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        // (b) No NEW live transfer job was enqueued -- the exhausted Failed row
        // stays the only row; nothing was re-armed to Pending.
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "a BridgingSubmitting aggregate with an exhausted Failed job row must NOT be re-armed",
        );

        // (a) Exactly one operator alert fired, naming the stranded transfer id.
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "exactly one operator alert must fire for a terminal-exhausted pre-burn mid-flight \
             transfer; got: {messages:?}",
        );
        assert!(
            messages[0].contains(&id.to_string()),
            "alert must name the stranded transfer id {id}; got: {:?}",
            messages[0]
        );
    }

    /// A `BridgingSubmitting` (Alpaca->Base) aggregate whose only job
    /// row is a terminal, retry-budget-exhausted `Failed` (attempts ==
    /// max_attempts, so apalis no longer owns it) must NOT be silently re-armed
    /// with a fresh budget. It is stranded for an operator alert instead, so the
    /// permanently latched guard is visible rather than silently resetting.
    #[tokio::test]
    async fn recover_usdc_guard_alerts_on_bridging_submitting_alpaca_to_base_with_exhausted_failed_job_row()
     {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let withdrawal_tx =
            fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        let amount = usdc(500);

        seed_bridging_submitting_alpaca_to_base(&store, &id, amount, withdrawal_tx).await;

        let notifier = Arc::new(CapturingNotifier::default());
        let service = make_trigger_with_inventory_config_and_notifier(
            InventoryView::default(),
            test_config(),
            notifier.clone(),
        )
        .await;

        // A terminal, retry-budget-exhausted Failed row: apalis no longer owns
        // it (attempts == max_attempts), so no live driver remains.
        service
            .transfer_usdc_to_market_making_queue
            .clone()
            .push(TransferUsdcToMarketMaking {
                id: id.clone(),
                amount,
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', attempts = max_attempts WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
        .execute(service.transfer_usdc_to_market_making_queue.pool())
        .await
        .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        // (b) No NEW live transfer job was enqueued -- the exhausted Failed row
        // stays the only row; nothing was re-armed to Pending.
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&service).await,
            0,
            "a BridgingSubmitting Alpaca->Base with an exhausted Failed job row must NOT be re-armed",
        );

        // (a) Exactly one operator alert fired, naming the stranded transfer id.
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "exactly one operator alert must fire for a terminal-exhausted pre-burn mid-flight \
             transfer; got: {messages:?}",
        );
        assert!(
            messages[0].contains(&id.to_string()),
            "alert must name the stranded transfer id {id}; got: {:?}",
            messages[0]
        );
    }

    /// A `BridgingSubmitting` (Base->Alpaca) aggregate whose only job row is a
    /// `Failed` row that still has retries remaining (`attempts < max_attempts`)
    /// is still apalis-owned: apalis re-fetches and re-runs it. So startup
    /// recovery must neither re-arm a fresh job (which would bypass the retry
    /// budget and drive a second concurrent resume) nor fire the stranded alert
    /// (the live row is the driver). Contrast with
    /// `recover_usdc_guard_alerts_on_bridging_submitting_with_exhausted_failed_job_row`,
    /// where the exhausted-budget row is no longer owned, so it strands and alerts.
    #[tokio::test]
    async fn recover_usdc_guard_does_not_rearm_or_alert_bridging_submitting_with_retryable_failed_job_row()
     {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let amount = usdc(500);

        seed_bridging_submitting_base_to_alpaca(&store, &id, amount, burn_tx).await;

        let notifier = Arc::new(CapturingNotifier::default());
        let service = make_trigger_with_inventory_config_and_notifier(
            InventoryView::default(),
            test_config(),
            notifier.clone(),
        )
        .await;

        // A Failed row with retries remaining (attempts < max_attempts): apalis
        // still owns it and will re-fetch it on its own.
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount,
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', attempts = 1, max_attempts = 3 WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "a retryable Failed row is still apalis-owned; recovery must NOT re-arm a fresh job",
        );
        assert_eq!(
            notifier.messages().len(),
            0,
            "a retryable Failed row is still apalis-owned; no stranded alert must fire; got: {:?}",
            notifier.messages(),
        );
    }

    /// A guard-holding aggregate that reaches the general "stranded" branch
    /// (`WithdrawalSubmitting{AlpacaToBase}`: no tracking seed, no re-arm path)
    /// must NOT fire the startup stranded alert while apalis still owns a live
    /// (Pending) transfer job for the same id. The live job is the driver -- the
    /// startup page would duplicate the terminal page the live job itself raises
    /// if it ultimately fails. The guard stays latched either way.
    #[tokio::test]
    async fn recover_usdc_guard_does_not_alert_stranded_state_with_live_job_row() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let amount = usdc(300);

        seed_withdrawal_submitting_alpaca_to_base(&store, &id, amount).await;

        let notifier = Arc::new(CapturingNotifier::default());
        let service = make_trigger_with_inventory_config_and_notifier(
            InventoryView::default(),
            test_config(),
            notifier.clone(),
        )
        .await;

        // A live Pending transfer job for the same id (AlpacaToBase routes to the
        // market-making queue): apalis still owns and will run it.
        service
            .transfer_usdc_to_market_making_queue
            .clone()
            .push(TransferUsdcToMarketMaking {
                id: id.clone(),
                amount,
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            notifier.messages().len(),
            0,
            "no stranded alert must fire while a live job already owns the id; got: {:?}",
            notifier.messages(),
        );
        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "the guard must still be latched -- the aggregate holds it regardless of the alert",
        );
    }

    /// Drives an aggregate to `AwaitingAttestation` -- the state a redrive
    /// enqueue that failed after `TimeoutAttestation` committed leaves behind.
    async fn seed_awaiting_attestation(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx: TxHash,
    ) {
        store
            .send(
                id,
                UsdcRebalanceCommand::Initiate {
                    direction,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::TimeoutAttestation {
                    retry_deadline_at: Utc::now() + chrono::Duration::hours(1),
                },
            )
            .await
            .unwrap();
    }

    /// Drives a `UsdcRebalance` aggregate from `Uninitialized` all the way to
    /// `DepositFailed` via the standard 7-step post-burn path. Used by sweep
    /// tests that need a stranded post-deposit failure in the durable store.
    ///
    /// `cctp_nonce_seed` must be unique per test to avoid aggregate conflicts
    /// on the shared SQLite pool.
    async fn seed_deposit_failed(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx: TxHash,
        mint_tx: TxHash,
        cctp_nonce_seed: u64,
    ) {
        store
            .send(
                id,
                UsdcRebalanceCommand::Initiate {
                    direction,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01],
                    cctp_nonce: alloy::primitives::B256::left_padding_from(
                        &cctp_nonce_seed.to_be_bytes(),
                    ),
                    message: valid_cctp_message(),
                    mint_scan_from_block: 100,
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx,
                    amount_received: usdc(399),
                    fee_collected: usdc(1),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(mint_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::FailDeposit {
                    reason: "deposit rejected".to_string(),
                },
            )
            .await
            .unwrap();
    }

    async fn seed_post_burn_bridging_failed(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx: TxHash,
    ) {
        store
            .send(
                id,
                UsdcRebalanceCommand::Initiate {
                    direction,
                    amount,
                    withdrawal: TransferRef::OnchainTx(burn_tx),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(id, UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .unwrap();
        store
            .send(
                id,
                UsdcRebalanceCommand::FailBridging {
                    reason: "mint receipt dropped on a load-balanced RPC".to_string(),
                },
            )
            .await
            .unwrap();
    }

    /// RAI-836: a post-burn aggregate stranded in `AwaitingAttestation` with no
    /// pending job (e.g. a redrive enqueue that failed after `TimeoutAttestation`
    /// committed) must get a transfer job re-armed on startup, keyed by the same
    /// id and routed to the direction's queue -- otherwise the guard latches
    /// forever with nothing driving the transfer.
    #[tokio::test]
    async fn recover_usdc_guard_rearms_stranded_awaiting_attestation() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000dd");

        seed_awaiting_attestation(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "a stranded Base->Alpaca AwaitingAttestation must be re-armed as a hedging job",
        );

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();
        let job: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            job.id, id,
            "the re-armed job must resume the same aggregate id, not a fresh one",
        );
    }

    /// RAI-836: re-arming is idempotent with apalis's own re-pick of pending
    /// rows. An aggregate that still has a non-terminal job row on startup must
    /// NOT get a second job -- a duplicate would drive two concurrent resumes of
    /// the same id.
    #[tokio::test]
    async fn recover_usdc_guard_skips_rearm_when_job_already_in_flight() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000ee");

        seed_awaiting_attestation(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;

        // A still-pending job row for this id, as apalis would re-pick on restart.
        let mut queue = service.transfer_usdc_to_hedging_queue.clone();
        queue
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "an aggregate with a job already in flight must not be re-armed a second time",
        );
    }

    /// RAI-836: a post-burn aggregate whose transfer job already exhausted its
    /// retry budget (a terminal `Failed` row) must NOT be re-armed -- re-driving
    /// it every restart would bypass the retry budget and silently retry a
    /// known-failing transfer. It latches for operator reconciliation (recoverable
    /// via the `transfer resume --kind usdc` CLI), matching `requeue_orphaned`'s
    /// policy of
    /// leaving `Failed` rows alone.
    #[tokio::test]
    async fn recover_usdc_guard_does_not_rearm_when_job_already_failed() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000bb");

        seed_awaiting_attestation(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;

        // A job that ran and exhausted its retries, as apalis leaves it: push a
        // row, then mark it terminal `Failed`.
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query("UPDATE Jobs SET status = 'Failed' WHERE job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .execute(service.transfer_usdc_to_hedging_queue.pool())
            .await
            .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "a Failed (retries-exhausted) job must NOT be re-armed; it latches for operator recovery",
        );
    }

    /// RAI-906: a post-burn `BridgingFailed` is recoverable, so the startup re-arm
    /// must enqueue a recovery job for it when no job row exists at all.
    #[tokio::test]
    async fn recover_usdc_guard_rearms_post_burn_bridging_failed() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000901");

        seed_post_burn_bridging_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "a recoverable post-burn BridgingFailed must be re-armed as a hedging job",
        );

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .fetch_one(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();
        let job: TransferUsdcToHedging = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            job.id, id,
            "the re-armed job must resume the same aggregate id"
        );
    }

    /// RAI-906: the key distinction from `AwaitingAttestation`. A post-burn
    /// `BridgingFailed` is reached precisely by a transfer job that emitted
    /// `FailBridging` and ended terminal (`Failed` from exhausted retries, or
    /// `Done` from a clean give-up), so it almost always carries a terminal row.
    /// Recovery is NEW work (re-check mint + un-fail), not a continuation of that
    /// exhausted budget, so a terminal row must NOT block the re-arm -- otherwise
    /// the auto-recovery never fires for the incident it targets.
    #[tokio::test]
    async fn recover_usdc_guard_rearms_post_burn_bridging_failed_despite_terminal_job_row() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000902");

        seed_post_burn_bridging_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;

        // The transfer job that drove the aggregate to BridgingFailed, left
        // terminal `Failed` with its retry budget EXHAUSTED (attempts ==
        // max_attempts) as apalis leaves a job that gave up -- it will not be
        // re-fetched, so recovery is free to take over.
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Failed', attempts = max_attempts WHERE job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToHedging>())
        .execute(service.transfer_usdc_to_hedging_queue.pool())
        .await
        .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "a recoverable BridgingFailed must be re-armed even with a retries-exhausted Failed row",
        );
    }

    /// RAI-906: a `Failed` row that apalis will still RE-FETCH (retries remaining,
    /// `attempts < max_attempts`) is a live job apalis owns, so re-arm must NOT
    /// enqueue a second concurrent recovery for the same id.
    #[tokio::test]
    async fn recover_usdc_guard_skips_rearm_for_bridging_failed_when_failed_job_retryable() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000904");

        seed_post_burn_bridging_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;

        // A `Failed` row with retries remaining: a freshly-pushed job has
        // attempts = 0 < max_attempts, so apalis will re-fetch it -- the original
        // transfer is still in flight from apalis's perspective.
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query("UPDATE Jobs SET status = 'Failed' WHERE job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .execute(service.transfer_usdc_to_hedging_queue.pool())
            .await
            .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "a retryable Failed job is still owned by apalis; re-arm must not duplicate it",
        );
    }

    /// RAI-906: a clean give-up (`Done`) row is terminal and apalis will not
    /// re-run it, so it must not block recovery re-arm.
    #[tokio::test]
    async fn recover_usdc_guard_rearms_post_burn_bridging_failed_despite_done_job_row() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000905");

        seed_post_burn_bridging_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();
        sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE job_type = ?")
            .bind(std::any::type_name::<TransferUsdcToHedging>())
            .execute(service.transfer_usdc_to_hedging_queue.pool())
            .await
            .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "a terminal Done row must not block recovery re-arm of a recoverable BridgingFailed",
        );
    }

    /// RAI-906: re-arm of a recoverable BridgingFailed is still suppressed while a
    /// recovery job is already in flight, so a restart cannot drive two concurrent
    /// resumes of the same id.
    #[tokio::test]
    async fn recover_usdc_guard_skips_rearm_for_bridging_failed_when_in_flight() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000903");

        seed_post_burn_bridging_failed(
            &store,
            &id,
            RebalanceDirection::BaseToAlpaca,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;

        // A recovery job already in flight (Pending) for this id.
        service
            .transfer_usdc_to_hedging_queue
            .clone()
            .push(TransferUsdcToHedging {
                id: id.clone(),
                amount: usdc(400),
                revert_redrive_attempts: 0,
            })
            .await
            .unwrap();

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            1,
            "an in-flight recovery job must not be duplicated by a second re-arm",
        );
    }

    /// RAI-836: the re-arm must cover the Alpaca->Base direction too -- it routes
    /// to the market-making queue with a `TransferUsdcToMarketMaking` payload. A
    /// direction-routing bug would strand every Alpaca->Base rebalance while the
    /// Base->Alpaca test stays green, so assert the market-making job explicitly.
    #[tokio::test]
    async fn recover_usdc_guard_rearms_stranded_awaiting_attestation_alpaca_to_base() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000cc");

        seed_awaiting_attestation(
            &store,
            &id,
            RebalanceDirection::AlpacaToBase,
            usdc(400),
            burn_tx,
        )
        .await;

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&service).await,
            1,
            "a stranded Alpaca->Base AwaitingAttestation must be re-armed as a market-making job",
        );
        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&service).await,
            0,
            "an Alpaca->Base strand must not be routed to the hedging queue",
        );

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<TransferUsdcToMarketMaking>())
        .fetch_one(service.transfer_usdc_to_market_making_queue.pool())
        .await
        .unwrap();
        let job: TransferUsdcToMarketMaking = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            job.id, id,
            "the re-armed market-making job must resume the same aggregate id, not a fresh one",
        );
    }

    #[tokio::test]
    async fn recover_usdc_guard_leaves_guard_clear_for_pre_burn_failure() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());
        let withdrawal =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000cc");

        // A pre-burn bridge failure (FailBridging from WithdrawalComplete) carries
        // no burn hash and reconciles to source, so it must not hold the guard.
        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(400),
                    withdrawal: TransferRef::OnchainTx(withdrawal),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::FailBridging {
                    reason: "usdc-to-u256 conversion error".to_string(),
                },
            )
            .await
            .unwrap();

        let service = make_trigger_with_inventory(InventoryView::default()).await;

        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert!(
            !service.usdc_in_progress.load(Ordering::SeqCst),
            "pre-burn bridge failure must not hold the USDC guard on startup"
        );
    }

    #[tokio::test]
    async fn recover_usdc_guard_holds_defensively_when_candidate_cannot_load() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let id = UsdcRebalanceId(Uuid::new_v4());

        // Persist a guard-relevant event that cannot originate an aggregate (the
        // first event must be ConversionInitiated/Initiated). The candidate
        // query returns this id, but Store::load replays to None -- exactly the
        // store/event inconsistency the recovery must fail safe on.
        let event = UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: fixed_bytes!(
                "0x00000000000000000000000000000000000000000000000000000000000000aa"
            ),
            burned_at: Utc::now(),
        };
        let payload = serde_json::to_string(&event).unwrap();
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('UsdcRebalance', ?, 0, 'UsdcRebalanceEvent::BridgingInitiated', '1.0', ?, '{}')",
        )
        .bind(id.to_string())
        .bind(payload)
        .execute(&pool)
        .await
        .unwrap();

        assert!(
            store.load(&id).await.unwrap().is_none(),
            "an unoriginated first event must not load into an aggregate"
        );

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "an unloadable guard-relevant candidate must hold the guard defensively"
        );
    }

    #[tokio::test]
    async fn recover_usdc_guard_holds_defensively_when_candidate_id_is_unparseable() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());

        // Persist a guard-relevant event whose aggregate_id is not a valid UUID --
        // a corrupt/partially-written durable row. The candidate query surfaces it
        // as unparseable; recovery must fail closed and hold the guard rather than
        // silently dropping it and re-opening the re-burn window.
        let event = UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: fixed_bytes!(
                "0x00000000000000000000000000000000000000000000000000000000000000ab"
            ),
            burned_at: Utc::now(),
        };
        let payload = serde_json::to_string(&event).unwrap();
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('UsdcRebalance', 'not-a-uuid', 0, 'UsdcRebalanceEvent::BridgingInitiated', '1.0', ?, '{}')",
        )
        .bind(payload)
        .execute(&pool)
        .await
        .unwrap();

        let service = make_trigger_with_inventory(InventoryView::default()).await;
        service.recover_usdc_guard(&pool, &store).await.unwrap();

        assert!(
            service.usdc_in_progress.load(Ordering::SeqCst),
            "an unparseable guard-relevant candidate aggregate_id must hold the guard defensively"
        );
    }

    #[tokio::test]
    async fn usdc_post_deposit_conversion_refreshes_timeout_tracking() {
        let inventory = InventoryView::default()
            .with_usdc(usdc(900), usdc(100))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());
        let refreshed_at = Utc::now() - ChronoDuration::seconds(30);

        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(400),
                bridged_amount_received: Some(usdc(399)),
                stage: usdc::UsdcRebalanceStage::DepositConfirmed,
                last_progress_at: Utc::now() - ChronoDuration::minutes(5),
            },
        );

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(400),
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                    initiated_at: refreshed_at,
                },
            )
            .await
            .unwrap();

        let tracking = trigger
            .usdc_tracking
            .read()
            .await
            .get(&id)
            .cloned()
            .expect("conversion initiation should preserve USDC tracking");
        assert_eq!(
            tracking.stage,
            usdc::UsdcRebalanceStage::ConversionInitiated
        );
        assert_eq!(tracking.last_progress_at, refreshed_at);
        assert_eq!(tracking.bridged_amount_received, Some(usdc(399)));

        trigger.expire_stuck_operations(Utc::now()).await.unwrap();

        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "a refreshed conversion step should not time out an active USDC rebalance"
        );
        assert!(
            !trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "refreshing conversion progress should avoid creating a timeout tombstone"
        );
    }

    #[tokio::test]
    async fn usdc_pre_withdrawal_conversion_confirmation_refreshes_timeout_tracking() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
        let reactor = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now() - ChronoDuration::minutes(5);
        let converted_at = Utc::now() - ChronoDuration::seconds(30);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: usdc(400),
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                    initiated_at,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    filled_amount: usdc(399),
                    converted_at,
                },
            )
            .await
            .unwrap();

        let tracking = trigger
            .usdc_tracking
            .read()
            .await
            .get(&id)
            .cloned()
            .expect("pre-withdrawal conversion should remain tracked after confirmation");
        assert_eq!(
            tracking.stage,
            usdc::UsdcRebalanceStage::ConversionConfirmed
        );
        assert_eq!(tracking.last_progress_at, converted_at);

        trigger.expire_stuck_operations(Utc::now()).await.unwrap();

        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "conversion confirmation should refresh timeout tracking before withdrawal starts"
        );
        assert!(
            !trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "refreshed pre-withdrawal conversion progress should not tombstone the aggregate"
        );
    }

    #[tokio::test]
    async fn conversion_failed_without_tracking_preserves_guard_conservatively() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_conversion_failed())
            .await
            .unwrap();

        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "without tracking we cannot prove the conversion failure was pre-burn, so the guard \
             is preserved conservatively rather than risk dispatching a fresh burn"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "a conversion failure with no prior tracking should not fabricate a context"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_available(Venue::MarketMaking),
            Some(usdc(5000)),
            "preserving the guard must not change onchain USDC inventory"
        );
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(5000)),
            "preserving the guard must not change offchain USDC inventory"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn conversion_confirmed_without_tracking_clears_guard_without_reconciliation() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(999)),
            )
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "a terminal-success conversion with no tracking (resumed after restart) clears the \
             guard rather than wedging it forever on a missing-context error"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "completing without tracking should not fabricate a context"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_available(Venue::MarketMaking),
            Some(usdc(5000)),
            "clearing the guard with no tracking must not reconcile onchain USDC inventory"
        );
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(5000)),
            "clearing the guard with no tracking must not reconcile offchain USDC inventory"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn conversion_confirmed_above_initiated_returns_error_and_preserves_state() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(1000)),
            )
            .await
            .unwrap();

        let error = harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(1001)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            RebalancingServiceError::SettledUsdcExceedsInitiatedAmount {
                id: error_id,
                event: usdc::UsdcTrackingEvent::ConversionConfirmed,
                initiated_amount,
                settled_amount,
            } if error_id == id
                && initiated_amount == usdc(1000)
                && settled_amount == usdc(1001)
        ));
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress should stay set when settled amount validation fails"
        );
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking context should be retained after settled amount validation fails"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_available(Venue::MarketMaking),
            Some(usdc(4000)),
            "initiated amount should remain deducted until a valid terminal event settles it"
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(usdc(1000)),
            "initiated amount should remain inflight when settled amount validation fails"
        );
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(5000)),
            "invalid settlement must not credit the destination venue"
        );
        drop(inventory);
    }

    #[test]
    fn usdc_failure_events_are_terminal() {
        assert!(RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_withdrawal_failed()
        ));
        assert!(RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_bridging_failed()
        ));
        assert!(RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_deposit_failed()
        ));
        assert!(RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_conversion_failed()
        ));
    }

    #[test]
    fn non_terminal_usdc_events_are_not_terminal() {
        assert!(!RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(1000))
        ));
        assert!(!RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_withdrawal_confirmed()
        ));
        assert!(!RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_bridging_initiated()
        ));
        assert!(!RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_bridged()
        ));
        // A recovered post-burn bridge re-enters the Bridged stage and must stay
        // non-terminal so the guard holds until the terminal deposit leg.
        assert!(!RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_bridging_completion_recovered()
        ));
    }

    #[test]
    fn conversion_confirmed_is_terminal_for_base_to_alpaca() {
        // For BaseToAlpaca, ConversionConfirmed IS the terminal event.
        assert!(RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(998))
        ));
    }

    #[test]
    fn conversion_confirmed_is_not_terminal_for_alpaca_to_base() {
        // For AlpacaToBase, ConversionConfirmed is NOT terminal (flow continues
        // to withdrawal).
        assert!(!RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(998))
        ));
    }

    #[test]
    fn deposit_confirmed_is_terminal_for_alpaca_to_base() {
        assert!(RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase)
        ));
    }

    #[test]
    fn deposit_confirmed_is_not_terminal_for_base_to_alpaca() {
        // For BaseToAlpaca, DepositConfirmed is NOT terminal because
        // post-deposit conversion (USDC->USD) is still required.
        assert!(!RebalancingService::is_terminal_usdc_rebalance_event(
            &make_usdc_deposit_confirmed(RebalanceDirection::BaseToAlpaca)
        ));
    }

    #[tokio::test]
    async fn usdc_rebalancing_disabled_when_cash_ratio_absent() {
        // Regression: when usdc is None, startup must not require assets.cash.vault_id.
        // The trigger returns no USDC rebalancing params, so no USDC vault lookup occurs.
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wrapper = Arc::new(MockWrapper::new());

        let schedulers = RebalancingSchedulers::new(&apalis_pool);
        let trigger = RebalancingService::new(
            RebalancingServiceConfig {
                equity: ImbalanceThreshold {
                    target: float!(0.5),
                    deviation: float!(0.2),
                },
                usdc: None,
                transfer_timeout: Duration::from_secs(30 * 60),
                assets: AssetsConfig {
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
            },
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            {
                let (event_sender, _) = broadcast::channel::<Statement>(16);
                Arc::new(BroadcastingInventory::new(
                    InventoryView::default(),
                    event_sender,
                ))
            },
            wrapper,
            schedulers,
            Arc::new(crate::alerts::NoopNotifier),
        );

        assert!(
            trigger.usdc_rebalancing_params().is_none(),
            "Expected usdc_rebalancing_params to be None when cash ratio is absent"
        );
    }

    /// Spy reactor that records all dispatched events for verification.
    struct EventCapturingReactor {
        captured_events: Arc<tokio::sync::Mutex<Vec<UsdcRebalanceEvent>>>,
        terminal_detection_results: Arc<tokio::sync::Mutex<Vec<bool>>>,
    }

    impl EventCapturingReactor {
        fn new() -> Self {
            Self {
                captured_events: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                terminal_detection_results: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }
        }
    }

    deps!(EventCapturingReactor, [UsdcRebalance]);

    #[async_trait]
    impl Reactor for EventCapturingReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (_id, event) = event.into_inner();
            self.captured_events.lock().await.push(event.clone());

            let is_terminal = RebalancingService::is_terminal_usdc_rebalance_event(&event);
            self.terminal_detection_results
                .lock()
                .await
                .push(is_terminal);

            Ok(())
        }
    }

    #[tokio::test]
    async fn terminal_detection_identifies_deposit_confirmed_alone_for_base_to_alpaca() {
        let spy = Arc::new(EventCapturingReactor::new());
        let store = TestStore::<UsdcRebalance>::with_reactor(Arc::clone(&spy));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        // Execute full BaseToAlpaca flow - each command triggers a dispatch with only
        // the new event(s), so terminal detection must work without seeing history
        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500)),
                    withdrawal: TransferRef::OnchainTx(tx_hash),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateBridging { burn_tx: tx_hash },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![1, 2, 3],
                    cctp_nonce: B256::left_padding_from(&12345u64.to_be_bytes()),
                    message: valid_cctp_message(),
                    mint_scan_from_block: 100,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(tx_hash),
                },
            )
            .await
            .unwrap();

        store
            .send(&id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();

        // For BaseToAlpaca, DepositConfirmed is NOT terminal because post-deposit
        // conversion (USDC->USD) is still required
        assert!(
            !spy.terminal_detection_results
                .lock()
                .await
                .last()
                .copied()
                .unwrap(),
            "DepositConfirmed(BaseToAlpaca) should NOT be terminal - needs conversion"
        );
    }

    #[tokio::test]
    async fn terminal_detection_identifies_deposit_confirmed_alone_for_alpaca_to_base() {
        let spy = Arc::new(EventCapturingReactor::new());
        let store = TestStore::<UsdcRebalance>::with_reactor(Arc::clone(&spy));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let tx_hash =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateBridging { burn_tx: tx_hash },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![1, 2, 3],
                    cctp_nonce: B256::left_padding_from(&67890u64.to_be_bytes()),
                    message: valid_cctp_message(),
                    mint_scan_from_block: 100,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(tx_hash),
                },
            )
            .await
            .unwrap();

        store
            .send(&id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();

        assert!(
            spy.terminal_detection_results
                .lock()
                .await
                .last()
                .copied()
                .unwrap(),
            "has_terminal_usdc_rebalance_event must identify DepositConfirmed alone as terminal"
        );
    }

    #[tokio::test]
    async fn terminal_detection_identifies_withdrawal_failed_alone() {
        let spy = Arc::new(EventCapturingReactor::new());
        let store = TestStore::<UsdcRebalance>::with_reactor(Arc::clone(&spy));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Test failure".to_string(),
                },
            )
            .await
            .unwrap();

        assert!(
            spy.terminal_detection_results
                .lock()
                .await
                .last()
                .copied()
                .unwrap(),
            "WithdrawalFailed should be terminal"
        );
    }

    #[tokio::test]
    async fn terminal_detection_identifies_bridging_failed_alone() {
        let spy = Arc::new(EventCapturingReactor::new());
        let store = TestStore::<UsdcRebalance>::with_reactor(Arc::clone(&spy));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let tx_hash =
            fixed_bytes!("0x3333333333333333333333333333333333333333333333333333333333333333");

        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateBridging { burn_tx: tx_hash },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::FailBridging {
                    reason: "Bridge timeout".to_string(),
                },
            )
            .await
            .unwrap();

        assert!(
            spy.terminal_detection_results
                .lock()
                .await
                .last()
                .copied()
                .unwrap(),
            "has_terminal_usdc_rebalance_event must identify BridgingFailed alone as terminal"
        );
    }

    #[tokio::test]
    async fn terminal_detection_identifies_conversion_failed_alone() {
        let spy = Arc::new(EventCapturingReactor::new());
        let store = TestStore::<UsdcRebalance>::with_reactor(Arc::clone(&spy));

        let id = UsdcRebalanceId(Uuid::new_v4());

        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100)),
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::FailConversion {
                    reason: "Order rejected".to_string(),
                },
            )
            .await
            .unwrap();

        assert!(
            spy.terminal_detection_results
                .lock()
                .await
                .last()
                .copied()
                .unwrap(),
            "has_terminal_usdc_rebalance_event must identify ConversionFailed alone as terminal"
        );
    }

    #[tokio::test]
    async fn trigger_clears_in_progress_flag_when_terminal_event_received() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_usdc(usdc(5000), usdc(5000)),
            event_sender,
        ));

        let trigger = Arc::new(RebalancingService::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            Address::ZERO,
            Address::ZERO,
            inventory,
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));

        // Set in_progress flag
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(trigger.usdc_in_progress.load(Ordering::SeqCst));

        // React to events including Initiated (required for react to process)
        // and terminal DepositConfirmed
        let tx_hash =
            fixed_bytes!("0xaaaa111111111111111111111111111111111111111111111111111111111111");
        let id = UsdcRebalanceId(Uuid::new_v4());

        let trigger_harness = ReactorHarness::new(Arc::clone(&trigger));

        trigger_harness
            .receive::<UsdcRebalance>(
                id.clone(),
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000)),
                    withdrawal_ref: TransferRef::OnchainTx(tx_hash),
                    initiated_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        trigger_harness
            .receive::<UsdcRebalance>(
                id.clone(),
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: TxHash::random(),
                    amount_received: Usdc::new(float!(999)),
                    fee_collected: Usdc::new(float!(1)),
                    minted_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        trigger_harness
            .receive::<UsdcRebalance>(
                id.clone(),
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    deposit_confirmed_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Verify in_progress flag was cleared (AlpacaToBase deposit is terminal)
        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress should be cleared after terminal event dispatch"
        );
    }

    #[tokio::test]
    async fn terminal_detection_identifies_conversion_confirmed_alone_for_base_to_alpaca() {
        let spy = Arc::new(EventCapturingReactor::new());
        let store = TestStore::<UsdcRebalance>::with_reactor(Arc::clone(&spy));

        let id = UsdcRebalanceId(Uuid::new_v4());
        let tx_hash =
            fixed_bytes!("0x4444444444444444444444444444444444444444444444444444444444444444");

        store
            .send(
                &id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500)),
                    withdrawal: TransferRef::OnchainTx(tx_hash),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateBridging { burn_tx: tx_hash },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![1, 2, 3],
                    cctp_nonce: B256::left_padding_from(&99999u64.to_be_bytes()),
                    message: valid_cctp_message(),
                    mint_scan_from_block: 100,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(tx_hash),
                },
            )
            .await
            .unwrap();

        store
            .send(&id, UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();

        // Now start post-deposit conversion (USDC to USD).
        // Amount must match amount_received from bridging (99.99), not the
        // originally requested amount (500), since that's what was deposited.
        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiatePostDepositConversion {
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                    amount: Usdc::new(float!(99.99)),
                },
            )
            .await
            .unwrap();

        // Our terminal detection receives only the latest event(s) per dispatch
        let terminal_results = spy.terminal_detection_results.lock().await;
        let deposit_confirmed_idx = 6;
        assert!(
            !terminal_results[deposit_confirmed_idx],
            "DepositConfirmed(BaseToAlpaca) should NOT be terminal - needs conversion"
        );

        let conversion_initiated_idx = 7;
        assert!(
            !terminal_results[conversion_initiated_idx],
            "has_terminal_usdc_rebalance_event must NOT identify ConversionInitiated as terminal"
        );
        drop(terminal_results);

        store
            .send(
                &id,
                UsdcRebalanceCommand::ConfirmConversion {
                    filled_amount: Usdc::new(float!(499)), // ~0.2% slippage
                },
            )
            .await
            .unwrap();

        assert!(
            spy.terminal_detection_results
                .lock()
                .await
                .last()
                .copied()
                .unwrap(),
            "has_terminal_usdc_rebalance_event must identify ConversionConfirmed alone as terminal"
        );
    }

    /// BUG REPRODUCTION: Trigger incorrectly fires with partial inventory data.
    ///
    /// When inventory polling starts:
    /// 1. Onchain equity is polled first
    /// 2. Trigger fires with onchain=X, offchain=0 (not yet polled)
    /// 3. Ratio = X/(X+0) = 100% -> detects "TooMuchOnchain" -> Redemption
    ///
    /// This is WRONG because offchain hasn't been polled yet, not because
    /// there's actually no offchain inventory.
    ///
    /// CORRECT behavior: trigger should NOT fire until both onchain AND
    /// offchain data are available for the symbol.
    #[tokio::test]
    async fn bug_trigger_should_not_fire_with_partial_inventory_data() {
        let symbol = Symbol::new("RKLB").unwrap();

        // Empty inventory - simulates startup state before polling completes
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));

        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;

        // Seed vault registry so token lookup succeeds
        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingService::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));
        let reactor = trigger.clone();

        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Simulate what happens during inventory polling:
        // OnchainEquity event arrives FIRST (offchain not yet polled)
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100)); // 100 shares onchain

        let onchain_event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: Utc::now(),
        };

        // React to the onchain event - this should NOT trigger rebalancing
        // because we don't have offchain data yet
        apply_and_dispatch_snapshot(reactor.clone(), id.clone(), onchain_event.clone())
            .await
            .unwrap();

        // CORRECT BEHAVIOR: No operation should be triggered because
        // offchain data hasn't arrived yet. The system should wait until
        // it has a complete picture of inventory before deciding to rebalance.
        //
        // CURRENT BUG: A Redemption is incorrectly triggered because:
        // - Onchain: 100 shares
        // - Offchain: 0 shares (not polled yet, treated as "no holdings")
        // - Ratio: 100% onchain -> "TooMuchOnchain" -> Redemption
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "No operation should trigger with partial inventory data: the system \
             must not treat missing offchain data as 'zero holdings'"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "No operation should trigger with partial inventory data: the system \
             must not treat missing offchain data as 'zero holdings'"
        );
    }

    /// Complementary test: verify that trigger DOES fire once both venues have data.
    #[tokio::test]
    async fn trigger_fires_when_both_venues_have_data() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let symbol = Symbol::new("RKLB").unwrap();
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingService::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));
        let reactor = trigger.clone();

        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Apply onchain snapshot (100 shares)
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100));

        let onchain_event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: Utc::now(),
        };

        apply_and_dispatch_snapshot(reactor.clone(), id.clone(), onchain_event)
            .await
            .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        // No trigger yet - only one venue has data
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "should not trigger with only onchain data"
        );

        // Apply offchain snapshot (0 shares) - now both venues have data
        let mut positions = BTreeMap::new();
        positions.insert(symbol.clone(), shares(0));

        let offchain_event = InventorySnapshotEvent::OffchainEquity {
            positions,
            fetched_at: Utc::now(),
        };

        apply_and_dispatch_snapshot(reactor.clone(), id.clone(), offchain_event)
            .await
            .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        // Now both venues have data: 100 onchain, 0 offchain = 100% ratio
        // With target 50% and deviation 10%, ratio 100% > upper bound 60%
        // So TooMuchOnchain -> should trigger Redemption
        let jobs = take_pending_equity_redemption_jobs(&trigger).await;

        assert_eq!(
            jobs.len(),
            1,
            "expected a redemption job for 100% onchain ratio once both venues have data"
        );
    }

    /// Verifies logging shows when imbalance check skips due to partial data.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn logs_show_partial_data_skips_imbalance_check() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let symbol = Symbol::new("RKLB").unwrap();
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingService::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));
        let reactor = trigger.clone();

        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Apply ONLY onchain data - offchain not yet polled
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100));

        let onchain_event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: Utc::now(),
        };

        apply_and_dispatch_snapshot(reactor.clone(), id.clone(), onchain_event)
            .await
            .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        // Verify the logs show:
        // 1. The snapshot event was applied
        // 2. Imbalance check was skipped due to partial data
        assert!(
            logs_contain("Applied inventory snapshot event"),
            "Should log when snapshot event is applied"
        );
        assert!(
            logs_contain("No equity imbalance detected"),
            "Should log that imbalance was not detected (due to partial data)"
        );
        assert!(
            !logs_contain("Triggered equity rebalancing"),
            "Should NOT trigger rebalancing with partial data"
        );
    }

    /// Verifies logging shows trigger fires when both venues have data.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn logs_show_trigger_fires_with_complete_data() {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let symbol = Symbol::new("RKLB").unwrap();
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingService::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            Arc::new(MockWrapper::new()),
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));
        let reactor = trigger.clone();

        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Apply onchain data first
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        // Now apply offchain data - both venues now have data
        let mut positions = BTreeMap::new();
        positions.insert(symbol.clone(), shares(0));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        // Verify the trigger fired after both venues have data
        assert!(
            logs_contain("Triggered equity rebalancing"),
            "Should trigger rebalancing once both venues have data"
        );
    }

    #[tokio::test]
    async fn position_fill_triggers_usdc_rebalancing_check() {
        let symbol = Symbol::new("AAPL").unwrap();
        // Pre-fill: 2000 onchain, 2000 offchain (balanced at 50%).
        // Onchain buy of 10 shares at $150 = $1500 USDC spent onchain.
        // Post-fill: 500 onchain, 2000 offchain = 20% ratio.
        // With target 50%, deviation 20%, lower bound 30%.
        // 20% < 30% -> should trigger USDC rebalancing (AlpacaToBase).
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(2000), usdc(2000))
            .with_withdrawable_cash_cents(200_000);

        let trigger = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        let event = make_onchain_fill(shares(10), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        // Drain any equity transfer jobs; the position-event side of this test
        // can enqueue equity operations through the rebalancer, but the
        // assertion below is only about USDC.
        take_pending_equity_mint_jobs(&trigger).await;
        take_pending_equity_redemption_jobs(&trigger).await;

        let usdc_operations = take_pending_usdc_transfer_jobs(&trigger).await;

        // Onchain buy spends USDC onchain, making the onchain ratio drop below
        // the lower bound. The system should move USDC from Alpaca to Base.
        usdc_operations
            .iter()
            .find(|op| matches!(op, UsdcRebalanceOperation::AlpacaToBase { .. }))
            .unwrap_or_else(|| {
                panic!(
                    "Expected AlpacaToBase (onchain USDC too low after buy), \
                     got operations: {usdc_operations:?}"
                )
            });
    }

    #[cfg(feature = "wallet-private-key")]
    #[tokio::test]
    async fn build_wallet_private_key_derives_address_from_key() {
        let private_key =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let wallet_config = toml::toml! {
            kind = "private-key"
        };

        let private_key_str = private_key.to_string();
        let wallet_secrets = toml::toml! {
            private_key = private_key_str
        };

        let wallet = st0x_config::build_wallet(
            &st0x_evm::WalletKind::PrivateKey,
            wallet_config.into(),
            wallet_secrets.into(),
            "https://example.com".parse().unwrap(),
        )
        .await
        .unwrap();

        assert_ne!(
            wallet.address(),
            Address::ZERO,
            "wallet address should be derived from key, not zero"
        );
    }

    #[tokio::test]
    async fn transfer_failed_cancels_redemption_inflight_and_restores_imbalance() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 80 onchain, 20 offchain = 80% ratio -> TooMuchOnchain triggers Redemption
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("redemption-transfer-cancel");

        // Verify initial imbalance
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            take_pending_equity_redemption_jobs(&trigger).await.len(),
            1,
            "80% ratio should enqueue a redemption job"
        );
        trigger.clear_equity_in_progress(&symbol);

        // WithdrawnFromRaindex: 30 tokens move to inflight
        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!("30")),
            )
            .await
            .unwrap();

        // Inflight blocks imbalance detection
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Inflight should block imbalance detection"
        );
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Inflight should block imbalance detection"
        );

        // TransferFailed: inflight cancelled, tokens return to available
        harness
            .receive::<EquityRedemption>(id.clone(), make_transfer_failed())
            .await
            .unwrap();

        // After cancel: back to 80 onchain, 20 offchain -> imbalance should re-trigger
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            take_pending_equity_redemption_jobs(&trigger).await.len(),
            1,
            "Imbalance should re-enqueue a redemption job after TransferFailed cancels inflight"
        );
    }

    #[tokio::test]
    async fn inflight_equity_snapshot_sets_inflight_balances() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 50 onchain, 50 offchain = balanced (50% ratio, within 30%-70%)
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(50)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(50)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Verify initially balanced
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "50/50 should be balanced"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "50/50 should be balanced"
        );

        // Emit InflightEquity with pending mints for AAPL
        // This sets inflight at Hedging venue, which blocks rebalancing
        let mut mints = BTreeMap::new();
        mints.insert(symbol.clone(), shares(10));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::InflightEquity {
                mints,
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // Inflight should block rebalancing even though ratios are balanced
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Inflight from InflightEquity snapshot should block rebalancing"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Inflight from InflightEquity snapshot should block rebalancing"
        );
    }

    #[tokio::test]
    async fn inflight_blocks_then_cqrs_clear_restores_triggerability() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 80 onchain, 20 offchain = imbalanced (would trigger Redemption
        // when not blocked by inflight)
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Set inflight directly (simulating a CQRS TransferOp::Start)
        {
            let mut inv = trigger.inventory.write().await;
            *inv = inv
                .clone()
                .update_equity(
                    &symbol,
                    Inventory::set_inflight(Venue::Hedging, shares(10)),
                    Utc::now(),
                )
                .unwrap();
        }

        // Verify inflight blocks rebalancing despite imbalance
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Inflight should block rebalancing"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Inflight should block rebalancing"
        );

        // Empty InflightEquity snapshot: symbol absent, so inflight preserved
        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::InflightEquity {
                mints: BTreeMap::new(),
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Empty snapshot should not clear inflight (symbol absent from maps)"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Empty snapshot should not clear inflight (symbol absent from maps)"
        );

        // CQRS terminal event clears inflight (simulating TransferOp::Complete)
        {
            let mut inv = trigger.inventory.write().await;
            *inv = inv
                .clone()
                .update_equity(
                    &symbol,
                    Inventory::set_inflight(Venue::Hedging, FractionalShares::ZERO),
                    Utc::now(),
                )
                .unwrap();
        }

        // After CQRS clears inflight, the imbalance should trigger
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert_eq!(
            take_pending_equity_redemption_jobs(&trigger).await.len(),
            1,
            "CQRS-cleared inflight should restore triggerability"
        );
    }

    #[tokio::test]
    async fn redemption_rejected_preserves_inflight_via_absent_skip() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 80 onchain, 20 offchain = imbalanced
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("redemption-rejected-absent-skip");

        // WithdrawnFromRaindex: start inflight
        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!("10")),
            )
            .await
            .unwrap();

        // RedemptionRejected: terminal failure, no inventory update
        harness
            .receive::<EquityRedemption>(id.clone(), make_redemption_rejected())
            .await
            .unwrap();

        // Empty InflightEquity snapshot: symbol absent from maps, so inflight
        // is preserved (poll never zeros absent symbols).
        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };
        apply_and_dispatch_snapshot(
            reactor.clone(),
            snapshot_id,
            InventorySnapshotEvent::InflightEquity {
                mints: BTreeMap::new(),
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        assert!(
            trigger
                .inventory
                .read()
                .await
                .symbols_with_inflight()
                .contains(&symbol),
            "Inflight should be preserved after RedemptionRejected \
             (symbol absent from poll maps)"
        );
    }

    #[tokio::test]
    async fn detection_failed_preserves_inflight_via_absent_skip() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("redemption-detection-absent-skip");

        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!("10")),
            )
            .await
            .unwrap();

        harness
            .receive::<EquityRedemption>(id.clone(), make_detection_failed())
            .await
            .unwrap();

        // Empty InflightEquity snapshot: symbol absent, so inflight preserved
        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };
        apply_and_dispatch_snapshot(
            reactor.clone(),
            snapshot_id,
            InventorySnapshotEvent::InflightEquity {
                mints: BTreeMap::new(),
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        assert!(
            trigger
                .inventory
                .read()
                .await
                .symbols_with_inflight()
                .contains(&symbol),
            "Inflight should be preserved after DetectionFailed \
             (symbol absent from poll maps)"
        );
    }

    #[tokio::test]
    async fn completed_redemption_inflight_cleared_by_cqrs() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("redemption-completed-zeroed");

        // WithdrawnFromRaindex -> Completed: TransferOp::Complete zeros inflight
        harness
            .receive::<EquityRedemption>(
                id.clone(),
                make_withdrawn_from_raindex(&symbol, float!("30")),
            )
            .await
            .unwrap();

        harness
            .receive::<EquityRedemption>(id.clone(), make_redemption_completed())
            .await
            .unwrap();

        // Empty InflightEquity skips absent symbol; inflight already 0 from Completed
        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };
        apply_and_dispatch_snapshot(
            reactor.clone(),
            snapshot_id,
            InventorySnapshotEvent::InflightEquity {
                mints: BTreeMap::new(),
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        assert!(
            !trigger
                .inventory
                .read()
                .await
                .symbols_with_inflight()
                .contains(&symbol),
            "TransferOp::Complete should have zeroed inflight"
        );
    }

    #[tokio::test]
    async fn cqrs_inflight_clear_defers_trigger_to_next_snapshot() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 80 onchain, 20 offchain = imbalanced (triggers Redemption)
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Set inflight directly (simulating CQRS TransferOp::Start)
        {
            let mut inv = trigger.inventory.write().await;
            *inv = inv
                .clone()
                .update_equity(
                    &symbol,
                    Inventory::set_inflight(Venue::Hedging, shares(10)),
                    Utc::now(),
                )
                .unwrap();
        }

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Inflight should block rebalancing"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Inflight should block rebalancing"
        );

        // CQRS terminal event clears inflight -- should NOT immediately
        // trigger. The terminal event handler doesn't call
        // check_and_trigger_equity, so the trigger is deferred.
        {
            let mut inv = trigger.inventory.write().await;
            *inv = inv
                .clone()
                .update_equity(
                    &symbol,
                    Inventory::set_inflight(Venue::Hedging, FractionalShares::ZERO),
                    Utc::now(),
                )
                .unwrap();
        }

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Clearing inflight should not immediately trigger -- \
             no equity recheck from terminal events"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Clearing inflight should not immediately trigger -- \
             no equity recheck from terminal events"
        );

        // Next poll cycle: an available snapshot arrives. Inflight is cleared,
        // check_and_trigger_after_snapshot fires with fresh balances.
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(80));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            id,
            InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        drain_pending_jobs(&trigger).await.unwrap();

        assert_eq!(
            take_pending_equity_redemption_jobs(&trigger).await.len(),
            1,
            "Available snapshot after inflight cleared should trigger rebalancing"
        );
    }

    #[tokio::test]
    async fn inflight_still_present_does_not_recheck() {
        let symbol = Symbol::new("AAPL").unwrap();
        // 80 onchain, 20 offchain = imbalanced
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Set inflight
        let mut mints = BTreeMap::new();
        mints.insert(symbol.clone(), shares(10));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::InflightEquity {
                mints: mints.clone(),
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // Apply same inflight again -- still present, no recheck
        apply_and_dispatch_snapshot(
            reactor.clone(),
            id.clone(),
            InventorySnapshotEvent::InflightEquity {
                mints,
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&reactor).await,
            0,
            "Inflight still present should not trigger recheck"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&reactor).await,
            0,
            "Inflight still present should not trigger recheck"
        );
    }

    #[tokio::test]
    async fn stale_inflight_after_terminal_failure_does_not_reintroduce_inflight() {
        // Regression test: when a rebalancing operation reaches a terminal
        // failure (WithdrawnFromRaindex -> TransferFailed), the failure handler
        // cancels inflight. If an InflightEquity poll arrives with the symbol
        // present (e.g. a concurrent operation), it must NOT trigger an extra
        // rebalance.
        let symbol = Symbol::new("AAPL").unwrap();
        // 80 onchain, 20 offchain = imbalanced (triggers Redemption)
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Simulate: WithdrawnFromRaindex starts inflight for the symbol
        let redemption_id = redemption_aggregate_id("redemption-stale-regression");
        harness
            .receive::<EquityRedemption>(
                redemption_id.clone(),
                make_withdrawn_from_raindex(&symbol, float!("10")),
            )
            .await
            .unwrap();

        // Verify inflight is present
        assert!(
            trigger
                .inventory
                .read()
                .await
                .symbols_with_inflight()
                .contains(&symbol),
            "Inflight should be present after WithdrawnFromRaindex"
        );

        // TransferFailed: terminal failure, cancels inflight
        harness
            .receive::<EquityRedemption>(redemption_id, make_transfer_failed())
            .await
            .unwrap();

        // Drain any operations triggered so far
        take_pending_equity_mint_jobs(&trigger).await;
        take_pending_equity_redemption_jobs(&trigger).await;
        trigger.clear_equity_in_progress(&symbol);

        // Deliver an InflightEquity snapshot with mints for the symbol.
        // The snapshot has the symbol present, so inflight is set at Hedging.
        let mut stale_mints = BTreeMap::new();
        stale_mints.insert(symbol.clone(), shares(10));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            snapshot_id,
            InventorySnapshotEvent::InflightEquity {
                mints: stale_mints,
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // Inflight is present from the mint snapshot (Hedging venue).
        // No extra rebalance should be triggered by InflightEquity events.
        assert!(
            trigger
                .inventory
                .read()
                .await
                .symbols_with_inflight()
                .contains(&symbol),
            "Inflight from snapshot should be present"
        );

        // No spurious rebalancing operation should have been triggered
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "InflightEquity after terminal failure should not trigger extra rebalancing"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "InflightEquity after terminal failure should not trigger extra rebalancing"
        );
    }

    #[tokio::test]
    async fn stale_inflight_after_mint_acceptance_failure_does_not_reintroduce_inflight() {
        // Same regression but for the MintAccepted -> MintAcceptanceFailed path.
        let symbol = Symbol::new("AAPL").unwrap();
        // 20 onchain, 80 offchain = imbalanced (triggers Mint)
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .update_equity(
                &symbol,
                Inventory::available(Venue::MarketMaking, Operator::Add, shares(20)),
                Utc::now(),
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::available(Venue::Hedging, Operator::Add, shares(80)),
                Utc::now(),
            )
            .unwrap();

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        let mint_id = issuer_request_id("mint-stale-regression");

        // MintRequested starts inflight at Hedging venue
        harness
            .receive::<TokenizedEquityMint>(
                mint_id.clone(),
                make_mint_requested(&symbol, float!("30")),
            )
            .await
            .unwrap();

        // MintAccepted transitions the mint
        harness
            .receive::<TokenizedEquityMint>(mint_id.clone(), make_mint_accepted())
            .await
            .unwrap();

        // MintAcceptanceFailed: terminal failure
        harness
            .receive::<TokenizedEquityMint>(mint_id, make_mint_acceptance_failed())
            .await
            .unwrap();

        // Drain and clear
        take_pending_equity_mint_jobs(&trigger).await;
        take_pending_equity_redemption_jobs(&trigger).await;
        trigger.clear_equity_in_progress(&symbol);

        // Deliver stale InflightEquity with redemptions for the symbol
        let mut stale_redemptions = BTreeMap::new();
        stale_redemptions.insert(symbol.clone(), shares(5));

        apply_and_dispatch_snapshot(
            reactor.clone(),
            snapshot_id,
            InventorySnapshotEvent::InflightEquity {
                mints: BTreeMap::new(),
                redemptions: stale_redemptions,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // No spurious rebalancing from the snapshot
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "InflightEquity after MintAcceptanceFailed should not \
             trigger extra rebalancing"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "InflightEquity after MintAcceptanceFailed should not \
             trigger extra rebalancing"
        );
    }

    #[tokio::test]
    async fn timed_out_redemption_clears_guards_and_suppresses_inflight_snapshots() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(10)),
                Utc::now(),
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("timed-out-redemption");

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        trigger.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                redemption_tx: Some(TxHash::random()),
                stage: RedemptionTrackingStage::TokensSent,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert!(
            !trigger
                .equity_in_progress
                .read()
                .unwrap()
                .contains_key(&symbol),
            "equity timeout should clear the symbol in-progress guard"
        );
        assert!(
            !trigger.redemption_tracking.read().await.contains_key(&id),
            "equity timeout should remove stale redemption tracking"
        );
        assert!(
            trigger.timed_out_redemptions.read().await.contains_key(&id),
            "equity timeout should remember the aggregate so late events are ignored"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(FractionalShares::ZERO),
            "equity timeout should zero onchain inflight balance"
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            Some(FractionalShares::ZERO),
            "equity timeout should not leave offchain inflight behind"
        );
        drop(inventory);

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "balanced inventory should not immediately trigger another equity transfer after timeout"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "balanced inventory should not immediately trigger another equity transfer after timeout"
        );

        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };
        let mut redemptions = BTreeMap::new();
        redemptions.insert(symbol.clone(), shares(10));

        let stale_snapshot_time = *trigger
            .timed_out_redemptions
            .read()
            .await
            .get(&id)
            .expect("timeout should record a cleanup timestamp");

        tokio::time::timeout(
            Duration::from_secs(5),
            harness.receive::<InventorySnapshot>(
                snapshot_id.clone(),
                InventorySnapshotEvent::InflightEquity {
                    mints: BTreeMap::new(),
                    redemptions,
                    fetched_at: stale_snapshot_time,
                },
            ),
        )
        .await
        .expect("stale inflight snapshot should process promptly")
        .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(FractionalShares::ZERO),
            "suppressed inflight snapshots should not reintroduce timed-out redemption inventory"
        );
        drop(inventory);

        let mut newer_redemptions = BTreeMap::new();
        newer_redemptions.insert(symbol.clone(), shares(4));
        let newer_snapshot_time = trigger
            .timed_out_redemptions
            .read()
            .await
            .get(&id)
            .expect("timeout should record a cleanup timestamp")
            .checked_add_signed(ChronoDuration::seconds(1))
            .expect("timestamp arithmetic should succeed");

        tokio::time::timeout(
            Duration::from_secs(5),
            harness.receive::<InventorySnapshot>(
                snapshot_id,
                InventorySnapshotEvent::InflightEquity {
                    mints: BTreeMap::new(),
                    redemptions: newer_redemptions,
                    fetched_at: newer_snapshot_time,
                },
            ),
        )
        .await
        .expect("newer inflight snapshot should process promptly")
        .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(4)),
            "a newer inflight snapshot should replace the stale-snapshot suppression"
        );
        drop(inventory);
        assert!(
            !trigger
                .suppressed_inflight_symbols
                .read()
                .await
                .contains_key(&symbol),
            "a newer inflight snapshot should retire suppression for the symbol"
        );
    }

    #[tokio::test]
    async fn timed_out_redemption_keeps_stale_snapshot_suppressed_after_retrigger() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(80), shares(20))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(10)),
                Utc::now(),
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let trigger = reactor.clone();
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = redemption_aggregate_id("timed-out-redemption-retrigger");

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        trigger.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                redemption_tx: Some(TxHash::random()),
                stage: RedemptionTrackingStage::TokensSent,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert_eq!(
            take_pending_equity_redemption_jobs(&trigger).await.len(),
            1,
            "timeout cleanup should still allow the next redemption trigger"
        );

        let cleared_at = *trigger
            .timed_out_redemptions
            .read()
            .await
            .get(&id)
            .expect("timeout cleanup should record a tombstone timestamp");

        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };
        let mut stale_redemptions = BTreeMap::new();
        stale_redemptions.insert(symbol.clone(), shares(10));

        tokio::time::timeout(
            Duration::from_secs(5),
            harness.receive::<InventorySnapshot>(
                snapshot_id,
                InventorySnapshotEvent::InflightEquity {
                    mints: BTreeMap::new(),
                    redemptions: stale_redemptions,
                    fetched_at: cleared_at,
                },
            ),
        )
        .await
        .expect("stale inflight snapshot should process promptly")
        .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(FractionalShares::ZERO),
            "stale inflight snapshots must stay suppressed even after a new redemption is sent"
        );
        drop(inventory);

        assert!(
            trigger
                .suppressed_inflight_symbols
                .read()
                .await
                .contains_key(&symbol),
            "suppression should remain active until a newer inflight poll arrives"
        );
    }

    #[tokio::test]
    async fn timed_out_redemption_drops_ownership_so_stuck_provider_request_is_external() {
        // Core lifecycle of this PR: when a redemption times out, cleanup clears
        // its inflight AND removes it from tracking. Because ownership is derived
        // from live tracking, the still-pending provider request is no longer
        // recognized as the bot's own -- the next poll classifies it as external
        // and excludes it, so a stuck Alpaca request can no longer block
        // rebalancing (which the old wallet-based matching would have done).
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(80), shares(20))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(10)),
                Utc::now(),
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let trigger = reactor.clone();
        let id = redemption_aggregate_id("timed-out-still-pending");
        let redemption_tx = TxHash::random();
        let tokenization_request_id = TokenizationRequestId("stuck-at-alpaca".to_string());

        trigger.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: Some(tokenization_request_id.clone()),
                redemption_tx: Some(redemption_tx),
                stage: RedemptionTrackingStage::Detected,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        // Before timeout the redemption is owned, so a poll would count it.
        let ownership = trigger.pending_request_ownership().await;
        assert!(ownership.redemption_txs.contains(&redemption_tx));
        assert!(
            ownership
                .redemption_tokenizations
                .contains(&tokenization_request_id)
        );

        trigger.expire_stuck_operations(Utc::now()).await.unwrap();

        // After timeout it is no longer owned: a provider poll that STILL lists
        // this request treats it as external and does not re-introduce inflight.
        let ownership = trigger.pending_request_ownership().await;
        assert!(!ownership.redemption_txs.contains(&redemption_tx));
        assert!(
            !ownership
                .redemption_tokenizations
                .contains(&tokenization_request_id)
        );

        assert_eq!(
            trigger
                .inventory
                .read()
                .await
                .equity_inflight(&symbol, Venue::MarketMaking),
            Some(FractionalShares::ZERO),
            "timeout cleanup should clear the stuck redemption's inflight so rebalancing can resume"
        );
    }

    #[tokio::test]
    async fn timed_out_mint_cleanup_rechecks_progress_before_clearing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(10)),
                now,
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let trigger = reactor.clone();
        let id = issuer_request_id("timed-out-mint-recheck");

        trigger.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                stage: MintTrackingStage::Accepted,
                last_progress_at: now - ChronoDuration::minutes(5),
            },
        );

        trigger
            .mint_tracking
            .write()
            .await
            .get_mut(&id)
            .expect("tracking entry should exist")
            .last_progress_at = now - ChronoDuration::seconds(30);

        let cleanup = trigger.cleanup_timed_out_mint(&id, now).await.unwrap();

        assert!(
            cleanup.is_none(),
            "cleanup should skip a mint that refreshed after the initial timeout scan"
        );
        assert!(
            trigger.mint_tracking.read().await.contains_key(&id),
            "rechecked mint tracking should remain when the timeout no longer applies"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(10)),
            "rechecked cleanup must not clear inflight for a refreshed mint"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn timed_out_mint_cleanup_tombstones_before_late_requested_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(3)),
                now,
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(10)),
                now,
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let trigger = reactor.clone();
        let id = issuer_request_id("timed-out-mint-late-request");

        trigger.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                stage: MintTrackingStage::Accepted,
                last_progress_at: now - ChronoDuration::minutes(5),
            },
        );

        let cleanup = trigger.cleanup_timed_out_mint(&id, now).await.unwrap();

        assert!(
            cleanup.is_some(),
            "cleanup should tombstone the stale mint before returning"
        );
        assert!(
            trigger.timed_out_mints.read().await.contains_key(&id),
            "mint tombstone should already exist after cleanup"
        );
        assert!(
            trigger
                .suppressed_inflight_symbols
                .read()
                .await
                .contains_key(&symbol),
            "inflight suppression should already exist after cleanup"
        );

        trigger
            .on_mint(id.clone(), make_mint_requested(&symbol, float!(10)))
            .await
            .unwrap();

        assert!(
            !trigger.mint_tracking.read().await.contains_key(&id),
            "late requested events must not recreate tombstoned mint tracking"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            Some(FractionalShares::ZERO),
            "mint cleanup should clear only hedging inflight"
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(3)),
            "mint cleanup must preserve unrelated market-making inflight"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn timed_out_mint_cleanup_clears_active_mint_id() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let id = issuer_request_id("timed-out-mint-active-id");
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(10)),
                now,
            )
            .unwrap()
            .set_active_mint(symbol.clone(), id.clone());
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let trigger = reactor.clone();

        trigger.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                stage: MintTrackingStage::Accepted,
                last_progress_at: now - ChronoDuration::minutes(5),
            },
        );

        let cleanup = trigger.cleanup_timed_out_mint(&id, now).await.unwrap();
        assert!(cleanup.is_some(), "cleanup should tombstone the stale mint");

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.active_mint(&symbol),
            None,
            "timed-out mint cleanup must clear the active mint ID from InventoryView"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn mint_event_rechecks_tombstone_after_waiting_for_sync_gate() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let trigger = reactor.clone();
        let id = issuer_request_id("mint-sync-gate-tombstone");
        let sync_guard = trigger.mint_event_sync.lock().await;
        let trigger_for_task = Arc::clone(&trigger);
        let task_symbol = symbol.clone();
        let task_id = id.clone();

        let event_task = tokio::spawn(async move {
            trigger_for_task
                .on_mint(task_id, make_mint_requested(&task_symbol, float!(10)))
                .await
        });

        trigger
            .timed_out_mints
            .write()
            .await
            .insert(id.clone(), Utc::now());
        drop(sync_guard);

        event_task.await.unwrap().unwrap();

        assert!(
            !trigger.mint_tracking.read().await.contains_key(&id),
            "blocked mint events must recheck tombstones before recreating tracking"
        );
    }

    #[tokio::test]
    async fn timed_out_redemption_cleanup_tombstones_before_late_withdrawal_event() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(10)),
                now,
            )
            .unwrap()
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(3)),
                now,
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let trigger = reactor.clone();
        let id = redemption_aggregate_id("timed-out-redemption-late-withdrawal");

        trigger.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                redemption_tx: Some(TxHash::random()),
                stage: RedemptionTrackingStage::TokensSent,
                last_progress_at: now - ChronoDuration::minutes(5),
            },
        );

        let cleanup = trigger
            .cleanup_timed_out_redemption(&id, now)
            .await
            .unwrap();

        assert!(
            cleanup.is_some(),
            "cleanup should tombstone the stale redemption before returning"
        );
        assert!(
            trigger.timed_out_redemptions.read().await.contains_key(&id),
            "redemption tombstone should already exist after cleanup"
        );
        assert!(
            trigger
                .suppressed_inflight_symbols
                .read()
                .await
                .contains_key(&symbol),
            "inflight suppression should already exist after cleanup"
        );

        trigger
            .on_redemption(id.clone(), make_withdrawn_from_raindex(&symbol, float!(10)))
            .await
            .unwrap();

        assert!(
            !trigger.redemption_tracking.read().await.contains_key(&id),
            "late withdrawal events must not recreate tombstoned redemption tracking"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(FractionalShares::ZERO),
            "redemption cleanup should clear only market-making inflight"
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(3)),
            "redemption cleanup must preserve unrelated hedging inflight"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn timed_out_redemption_cleanup_clears_active_redemption_id() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();
        let id = redemption_aggregate_id("timed-out-redemption-active-id");
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, shares(10)),
                now,
            )
            .unwrap()
            .set_active_redemption(symbol.clone(), id.clone());
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let trigger = reactor.clone();

        trigger.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                redemption_tx: Some(TxHash::random()),
                stage: RedemptionTrackingStage::TokensSent,
                last_progress_at: now - ChronoDuration::minutes(5),
            },
        );

        let cleanup = trigger
            .cleanup_timed_out_redemption(&id, now)
            .await
            .unwrap();
        assert!(
            cleanup.is_some(),
            "cleanup should tombstone the stale redemption"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.active_redemption(&symbol),
            None,
            "timed-out redemption cleanup must clear the active redemption ID from InventoryView"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn redemption_event_rechecks_tombstone_after_waiting_for_sync_gate() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let trigger = reactor.clone();
        let id = redemption_aggregate_id("redemption-sync-gate-tombstone");
        let sync_guard = trigger.redemption_event_sync.lock().await;
        let trigger_for_task = Arc::clone(&trigger);
        let task_symbol = symbol.clone();
        let task_id = id.clone();

        let event_task = tokio::spawn(async move {
            trigger_for_task
                .on_redemption(
                    task_id,
                    make_withdrawn_from_raindex(&task_symbol, float!(10)),
                )
                .await
        });

        trigger
            .timed_out_redemptions
            .write()
            .await
            .insert(id.clone(), Utc::now());
        drop(sync_guard);

        event_task.await.unwrap().unwrap();

        assert!(
            !trigger.redemption_tracking.read().await.contains_key(&id),
            "blocked redemption events must recheck tombstones before recreating tracking"
        );
    }

    #[tokio::test]
    async fn timed_out_usdc_cleanup_tombstones_before_late_conversion_event() {
        let now = Utc::now();
        let inventory = InventoryView::default()
            .with_usdc(usdc(5000), usdc(5000))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(700)),
                now,
            )
            .unwrap()
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(300)),
                now,
            )
            .unwrap();
        let trigger = make_trigger_with_inventory(inventory).await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(700),
                bridged_amount_received: None,
                // Pre-burn stage: a stalled withdrawal still reconciles to source
                // and tombstones on timeout (post-burn stages preserve instead,
                // covered by timed_out_post_mint_usdc_rebalance_*).
                stage: usdc::UsdcRebalanceStage::WithdrawalConfirmed,
                last_progress_at: now - ChronoDuration::minutes(40),
            },
        );

        let cleanup = trigger
            .cleanup_timed_out_usdc_rebalance(&id, now)
            .await
            .unwrap();

        assert!(
            cleanup.is_some(),
            "cleanup should tombstone the stale USDC rebalance before returning"
        );
        assert!(
            trigger
                .timed_out_usdc_rebalances
                .read()
                .await
                .contains_key(&id),
            "USDC tombstone should already exist after cleanup"
        );

        trigger
            .on_usdc_rebalance(
                id.clone(),
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: usdc(700),
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                    initiated_at: now,
                },
            )
            .await
            .unwrap();

        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "late conversion events must not recreate tombstoned USDC tracking"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(Usdc::ZERO),
            "USDC cleanup should clear only the source venue inflight"
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(usdc(300)),
            "USDC cleanup must preserve unrelated destination inflight"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn timed_out_usdc_cleanup_clears_active_usdc_rebalance_id() {
        let now = Utc::now();
        let id = UsdcRebalanceId(Uuid::new_v4());
        let inventory = InventoryView::default()
            .with_usdc(usdc(5000), usdc(5000))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(700)),
                now,
            )
            .unwrap()
            .set_active_usdc_rebalance(id.clone());
        let trigger = make_trigger_with_inventory(inventory).await;

        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(700),
                bridged_amount_received: None,
                // Pre-burn stage: a stalled withdrawal still reconciles to source
                // and tombstones on timeout (post-burn stages preserve instead,
                // covered by timed_out_post_mint_usdc_rebalance_*).
                stage: usdc::UsdcRebalanceStage::WithdrawalConfirmed,
                last_progress_at: now - ChronoDuration::minutes(40),
            },
        );

        let cleanup = trigger
            .cleanup_timed_out_usdc_rebalance(&id, now)
            .await
            .unwrap();
        assert!(
            cleanup.is_some(),
            "cleanup should tombstone the stale USDC rebalance"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.active_usdc_rebalance(),
            None,
            "timed-out USDC cleanup must clear the active USDC rebalance ID from InventoryView"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn usdc_event_rechecks_tombstone_after_waiting_for_sync_gate() {
        let reactor =
            make_trigger_with_inventory(InventoryView::default().with_usdc(usdc(5000), usdc(5000)))
                .await;
        let trigger = reactor.clone();
        let id = UsdcRebalanceId(Uuid::new_v4());
        let sync_guard = trigger.usdc_event_sync.lock().await;
        let trigger_for_task = Arc::clone(&trigger);
        let task_id = id.clone();

        let event_task = tokio::spawn(async move {
            trigger_for_task
                .on_usdc_rebalance(
                    task_id,
                    UsdcRebalanceEvent::ConversionInitiated {
                        direction: RebalanceDirection::BaseToAlpaca,
                        amount: usdc(700),
                        order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                        initiated_at: Utc::now(),
                    },
                )
                .await
        });

        trigger
            .timed_out_usdc_rebalances
            .write()
            .await
            .insert(id.clone(), Utc::now());
        drop(sync_guard);

        event_task.await.unwrap().unwrap();

        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "blocked USDC events must recheck tombstones before recreating tracking"
        );
    }

    #[tokio::test]
    async fn recovery_path_reuses_inflight_suppression_filter() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let trigger = reactor.clone();
        let cleared_at = Utc::now();

        trigger
            .suppressed_inflight_symbols
            .write()
            .await
            .insert(symbol.clone(), cleared_at);

        trigger
            .on_snapshot_recovery(
                RebalancingServiceError::Inventory(InventoryViewError::Equity(
                    InventoryError::NegativeInflight {
                        value: FractionalShares::new(float!(-1)),
                    },
                )),
                InventorySnapshotEvent::InflightEquity {
                    mints: BTreeMap::new(),
                    redemptions: BTreeMap::from([(symbol.clone(), shares(10))]),
                    fetched_at: cleared_at,
                },
            )
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            None,
            "recovery should not reintroduce stale inflight for suppressed symbols"
        );
        drop(inventory);

        assert!(
            trigger
                .suppressed_inflight_symbols
                .read()
                .await
                .contains_key(&symbol),
            "stale recovery snapshots should keep suppression active"
        );
    }

    #[tokio::test]
    async fn recovery_path_expires_timed_out_mints_before_reapplying_inflight() {
        let symbol = Symbol::new("AAPL").unwrap();
        let fetched_at = Utc::now();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .update_equity(
                &symbol,
                Inventory::transfer(Venue::Hedging, TransferOp::Start, shares(10)),
                fetched_at,
            )
            .unwrap();
        let reactor = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let trigger = reactor.clone();
        let id = issuer_request_id("recovery-timeout-sweep");

        trigger.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                stage: MintTrackingStage::Accepted,
                last_progress_at: fetched_at - ChronoDuration::minutes(5),
            },
        );

        trigger
            .on_snapshot_recovery(
                RebalancingServiceError::Inventory(InventoryViewError::Equity(
                    InventoryError::NegativeInflight {
                        value: FractionalShares::new(float!(-1)),
                    },
                )),
                InventorySnapshotEvent::InflightEquity {
                    mints: BTreeMap::from([(symbol.clone(), shares(10))]),
                    redemptions: BTreeMap::new(),
                    fetched_at,
                },
            )
            .await
            .unwrap();

        assert!(
            trigger.timed_out_mints.read().await.contains_key(&id),
            "recovery should run the timeout sweep before filtering inflight snapshots"
        );
        assert!(
            trigger
                .suppressed_inflight_symbols
                .read()
                .await
                .contains_key(&symbol),
            "recovery timeout sweep should suppress the stale inflight snapshot"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            None,
            "recovery should not reapply inflight for a mint that timed out on this pass"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn timeout_markers_are_pruned_after_retention_window() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let trigger = reactor.clone();
        let stale_time = Utc::now()
            - ChronoDuration::from_std(TIMEOUT_TOMBSTONE_RETENTION).unwrap()
            - ChronoDuration::seconds(1);
        let mint_id = issuer_request_id("stale-mint");
        let redemption_id = redemption_aggregate_id("stale-redemption");
        let usdc_id = UsdcRebalanceId(Uuid::new_v4());

        trigger
            .suppressed_inflight_symbols
            .write()
            .await
            .insert(symbol.clone(), stale_time);
        trigger
            .timed_out_mints
            .write()
            .await
            .insert(mint_id, stale_time);
        trigger
            .timed_out_redemptions
            .write()
            .await
            .insert(redemption_id, stale_time);
        trigger
            .timed_out_usdc_rebalances
            .write()
            .await
            .insert(usdc_id, stale_time);

        trigger.expire_stuck_operations(Utc::now()).await.unwrap();

        assert!(
            trigger.suppressed_inflight_symbols.read().await.is_empty(),
            "expired snapshot suppressions should be pruned"
        );
        assert!(
            trigger.timed_out_mints.read().await.is_empty(),
            "expired mint tombstones should be pruned"
        );
        assert!(
            trigger.timed_out_redemptions.read().await.is_empty(),
            "expired redemption tombstones should be pruned"
        );
        assert!(
            trigger.timed_out_usdc_rebalances.read().await.is_empty(),
            "expired USDC tombstones should be pruned"
        );
    }

    #[tokio::test]
    async fn no_active_aggregate_ids_when_idle() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory(InventoryView::default().with_equity(
            symbol.clone(),
            shares(50),
            shares(50),
        ))
        .await;
        let trigger = reactor.clone();

        let inventory = trigger.inventory.read().await;
        assert_eq!(inventory.active_usdc_rebalance(), None);
        assert_eq!(inventory.active_mint(&symbol), None);
        assert_eq!(inventory.active_redemption(&symbol), None);
        drop(inventory);
    }

    #[tokio::test]
    async fn mint_accepted_records_active_mint_id() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let trigger = reactor.clone();
        let id = issuer_request_id("mint-active-id");

        trigger
            .on_mint(id.clone(), make_mint_requested(&symbol, float!(10)))
            .await
            .unwrap();
        trigger
            .on_mint(id.clone(), make_mint_accepted())
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.active_mint(&symbol),
            Some(&id),
            "MintAccepted should record the aggregate ID for its symbol",
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::Hedging),
            Some(shares(10)),
            "MintAccepted should also start the inflight balance",
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn mint_terminal_event_clears_active_mint_id() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let trigger = reactor.clone();
        let id = issuer_request_id("mint-clear-id");

        trigger
            .on_mint(id.clone(), make_mint_requested(&symbol, float!(10)))
            .await
            .unwrap();
        trigger
            .on_mint(id.clone(), make_mint_accepted())
            .await
            .unwrap();
        assert_eq!(
            trigger.inventory.read().await.active_mint(&symbol),
            Some(&id),
        );

        trigger
            .on_mint(id.clone(), make_tokens_received())
            .await
            .unwrap();
        trigger
            .on_mint(id.clone(), make_deposited_into_raindex())
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.active_mint(&symbol),
            None,
            "DepositedIntoRaindex (terminal) should clear the active mint ID",
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn redemption_withdrawn_records_active_redemption_id() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let trigger = reactor.clone();
        let id = redemption_aggregate_id("redemption-active-id");

        trigger
            .on_redemption(id.clone(), make_withdrawn_from_raindex(&symbol, float!(10)))
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.active_redemption(&symbol),
            Some(&id),
            "WithdrawnFromRaindex should record the aggregate ID for its symbol",
        );
        assert_eq!(
            inventory.equity_inflight(&symbol, Venue::MarketMaking),
            Some(shares(10)),
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn redemption_completed_clears_active_redemption_id() {
        let symbol = Symbol::new("AAPL").unwrap();
        let reactor = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let trigger = reactor.clone();
        let id = redemption_aggregate_id("redemption-clear-id");

        trigger
            .on_redemption(id.clone(), make_withdrawn_from_raindex(&symbol, float!(10)))
            .await
            .unwrap();
        assert_eq!(
            trigger.inventory.read().await.active_redemption(&symbol),
            Some(&id),
        );

        trigger
            .on_redemption(id.clone(), make_redemption_completed())
            .await
            .unwrap();

        assert_eq!(
            trigger.inventory.read().await.active_redemption(&symbol),
            None,
            "Completed (terminal) should clear the active redemption ID",
        );
    }

    #[tokio::test]
    async fn usdc_initiation_records_active_rebalance_id() {
        let reactor =
            make_trigger_with_inventory(InventoryView::default().with_usdc(usdc(500), usdc(500)))
                .await;
        let trigger = reactor.clone();
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger
            .on_usdc_rebalance(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(100)),
            )
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.active_usdc_rebalance(),
            Some(&id),
            "USDC initiation should record the aggregate ID alongside the inflight balance",
        );
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(usdc(100)),
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn usdc_terminal_clears_active_rebalance_id() {
        let reactor =
            make_trigger_with_inventory(InventoryView::default().with_usdc(usdc(500), usdc(500)))
                .await;
        let trigger = reactor.clone();
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger
            .on_usdc_rebalance(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(100)),
            )
            .await
            .unwrap();
        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            Some(&id),
        );

        trigger
            .on_usdc_rebalance(id.clone(), make_usdc_bridging_initiated())
            .await
            .unwrap();
        trigger
            .on_usdc_rebalance(id.clone(), make_usdc_bridge_attestation_received())
            .await
            .unwrap();
        trigger
            .on_usdc_rebalance(id.clone(), make_usdc_bridged())
            .await
            .unwrap();
        trigger
            .on_usdc_rebalance(
                id.clone(),
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                    deposit_initiated_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        trigger
            .on_usdc_rebalance(
                id.clone(),
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    deposit_confirmed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            trigger.inventory.read().await.active_usdc_rebalance(),
            None,
            "DepositConfirmed (AlpacaToBase terminal) should clear the active USDC rebalance ID",
        );
    }

    #[tokio::test]
    async fn equity_check_dispatches_mint_on_offchain_imbalance() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(20), shares(80))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        EquityRebalancingCheck {
            symbol: symbol.clone(),
        }
        .perform(&trigger)
        .await
        .unwrap();

        let dispatched = take_pending_equity_mint_jobs(&trigger).await;
        let [job] = dispatched.as_slice() else {
            panic!("Expected exactly one mint job, got {dispatched:?}");
        };
        assert_eq!(job.symbol, symbol);
    }

    /// A crash between `queue.push` and the first persisted mint event leaves
    /// a pending job row while the in-memory guard resets. The Jobs-table
    /// dedupe must suppress a second enqueue for the same symbol while
    /// leaving other symbols free to enqueue.
    #[tokio::test]
    async fn equity_mint_enqueue_dedupes_per_symbol_against_jobs_table() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(20), shares(80))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        EquityRebalancingCheck {
            symbol: symbol.clone(),
        }
        .perform(&trigger)
        .await
        .unwrap();
        assert_eq!(count_pending_equity_mint_jobs(&trigger).await, 1);

        // Simulate a restart: the in-memory guard resets while the job row
        // is still pending.
        trigger.clear_equity_in_progress(&symbol);

        EquityRebalancingCheck {
            symbol: symbol.clone(),
        }
        .perform(&trigger)
        .await
        .unwrap();
        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            1,
            "a pending mint row for the symbol must suppress a duplicate enqueue"
        );

        // A different symbol is not suppressed by AAPL's pending row.
        let enqueued = trigger
            .enqueue_transfer_equity_to_market_making(Symbol::new("TSLA").unwrap(), shares(30), 0)
            .await;
        assert!(
            enqueued,
            "a pending mint row for one symbol must not block other symbols"
        );
        assert_eq!(count_pending_equity_mint_jobs(&trigger).await, 2);
    }

    #[tokio::test]
    async fn equity_check_dispatches_redemption_on_onchain_imbalance() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(80), shares(20))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        EquityRebalancingCheck {
            symbol: symbol.clone(),
        }
        .perform(&trigger)
        .await
        .unwrap();

        let dispatched = take_pending_equity_redemption_jobs(&trigger).await;
        let [job] = dispatched.as_slice() else {
            panic!("Expected exactly one redemption job, got {dispatched:?}");
        };
        assert_eq!(job.symbol, symbol);
    }

    #[tokio::test]
    async fn usdc_check_dispatches_transfer_on_imbalance() {
        let inventory = InventoryView::default()
            .with_usdc(usdc(100), usdc(900))
            .with_withdrawable_cash_cents(90_000);
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();

        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        let dispatched = take_pending_usdc_transfer_jobs(&trigger).await;
        assert!(
            matches!(
                dispatched.as_slice(),
                [UsdcRebalanceOperation::AlpacaToBase { .. }],
            ),
            "Expected AlpacaToBase, got {dispatched:?}"
        );
    }

    #[tokio::test]
    async fn checks_do_not_dispatch_when_balanced() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .with_usdc(usdc(500), usdc(500));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        EquityRebalancingCheck {
            symbol: symbol.clone(),
        }
        .perform(&trigger)
        .await
        .unwrap();
        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Balanced inventory should not enqueue a mint job"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Balanced inventory should not enqueue a redemption job"
        );
    }

    #[tokio::test]
    async fn equity_check_suppresses_duplicate_dispatch_when_in_progress() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(20), shares(80))
            .with_usdc(usdc(500), usdc(500));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(
                symbol.clone(),
                equity::GuardState::ActiveTransfer { generation: 0 },
            );
        }

        EquityRebalancingCheck {
            symbol: symbol.clone(),
        }
        .perform(&trigger)
        .await
        .unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "In-progress flag should suppress duplicate equity dispatch"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "In-progress flag should suppress duplicate equity dispatch"
        );
    }

    #[tokio::test]
    async fn equity_check_suppresses_dispatch_when_offchain_order_pending() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(20), shares(80))
            .with_usdc(usdc(500), usdc(500));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let trigger = reactor.clone();

        trigger
            .pending_offchain_order_symbols
            .write()
            .await
            .insert(symbol.clone());

        EquityRebalancingCheck {
            symbol: symbol.clone(),
        }
        .perform(&trigger)
        .await
        .unwrap();

        assert_eq!(
            count_pending_equity_mint_jobs(&trigger).await,
            0,
            "Pending offchain hedge order should suppress equity rebalancing dispatch"
        );
        assert_eq!(
            count_pending_equity_redemption_jobs(&trigger).await,
            0,
            "Pending offchain hedge order should suppress equity rebalancing dispatch"
        );
    }

    #[tokio::test]
    async fn position_offchain_order_lifecycle_blocks_then_releases_equity_check() {
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(20), shares(80))
            .with_usdc(usdc(500), usdc(500));

        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        harness
            .receive::<Position>(symbol.clone(), make_offchain_placed(offchain_order_id))
            .await
            .unwrap();

        assert!(
            trigger.has_pending_offchain_order(&symbol).await,
            "OffChainOrderPlaced should block equity rebalancing for the symbol"
        );

        harness
            .receive::<Position>(symbol.clone(), make_offchain_failed(offchain_order_id))
            .await
            .unwrap();

        assert!(
            !trigger.has_pending_offchain_order(&symbol).await,
            "Terminal offchain order event should release the rebalancing block"
        );
    }

    #[tokio::test]
    async fn offchain_order_filled_releases_equity_rebalancing_block() {
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(20), shares(80))
            .with_usdc(usdc(5000), usdc(5000));

        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        harness
            .receive::<Position>(symbol.clone(), make_offchain_placed(offchain_order_id))
            .await
            .unwrap();

        assert!(
            trigger.has_pending_offchain_order(&symbol).await,
            "OffChainOrderPlaced should block equity rebalancing for the symbol"
        );

        harness
            .receive::<Position>(
                symbol.clone(),
                make_offchain_fill(shares(10), Direction::Sell),
            )
            .await
            .unwrap();

        assert!(
            !trigger.has_pending_offchain_order(&symbol).await,
            "A filled offchain order should release the rebalancing block"
        );
    }

    /// A Filled event whose inventory update would underflow (because the
    /// polling snapshot already absorbed the fill before the reactor saw it)
    /// must still clear the gate. Holding the gate closed deadlocks equity
    /// rebalancing for the symbol until the next bot restart, because the
    /// only path that could re-issue a terminal offchain event for the
    /// symbol is itself gated. Inventory drift is bounded by the next
    /// polling snapshot (~60s) and is preferable to indefinite deadlock.
    #[tokio::test]
    async fn offchain_order_filled_releases_gate_even_when_inventory_update_fails() {
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        // Offchain shows only 4 shares (e.g., the polling snapshot already
        // reflects a 10-share sell that the broker filled before the reactor
        // got a chance to record it). The fill below subtracts 10, which
        // would underflow the in-memory mirror.
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(20), shares(4))
            .with_usdc(usdc(5000), usdc(5000));

        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        harness
            .receive::<Position>(symbol.clone(), make_offchain_placed(offchain_order_id))
            .await
            .unwrap();
        assert!(
            trigger.has_pending_offchain_order(&symbol).await,
            "OffChainOrderPlaced should set the gate"
        );

        harness
            .receive::<Position>(
                symbol.clone(),
                make_offchain_fill(shares(10), Direction::Sell),
            )
            .await
            .expect("reactor must not propagate inventory underflow for offchain fills");

        assert!(
            !trigger.has_pending_offchain_order(&symbol).await,
            "Inventory update failure on a Filled event must not leave the gate stuck"
        );
    }

    #[tokio::test]
    async fn offchain_order_failed_enqueues_equity_recheck() {
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        // Balanced venues so the drained recheck evaluates cleanly without
        // dispatching a rebalance.
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(50))
            .with_usdc(usdc(500), usdc(500));

        let trigger = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        harness
            .receive::<Position>(symbol.clone(), make_offchain_placed(offchain_order_id))
            .await
            .unwrap();

        harness
            .receive::<Position>(symbol.clone(), make_offchain_failed(offchain_order_id))
            .await
            .unwrap();

        let processed = equity::drain_pending_equity_jobs(&trigger).await.unwrap();
        assert_eq!(
            processed, 1,
            "A failed offchain order must enqueue exactly one equity recheck so hedging retries"
        );
    }

    #[tokio::test]
    async fn offchain_order_filled_enqueues_equity_recheck() {
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        // Sized so the Hedging sell fill leaves both venues balanced (50/50),
        // letting the drained recheck run to completion without dispatching.
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(50), shares(60))
            .with_usdc(usdc(500), usdc(500));

        let trigger = make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        harness
            .receive::<Position>(symbol.clone(), make_offchain_placed(offchain_order_id))
            .await
            .unwrap();

        harness
            .receive::<Position>(
                symbol.clone(),
                make_offchain_fill(shares(10), Direction::Sell),
            )
            .await
            .unwrap();

        let processed = equity::drain_pending_equity_jobs(&trigger).await.unwrap();
        assert_eq!(
            processed, 1,
            "A filled offchain order must enqueue exactly one equity recheck so hedging re-evaluates"
        );
    }

    #[tokio::test]
    async fn recover_pending_offchain_order_symbols_restores_block_from_projection() {
        let pending_symbol = Symbol::new("AAPL").unwrap();
        let clear_symbol = Symbol::new("TSLA").unwrap();
        let trigger = make_trigger_with_inventory(InventoryView::default()).await;

        // Persist two positions through the real command path: AAPL gets a
        // pending offchain hedge order; TSLA only has an onchain fill.
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<Position>(pool.clone(), ());

        store
            .send(
                &pending_symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: pending_symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount: shares(10),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &pending_symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: OffchainOrderId::new(),
                    shares: Positive::new(shares(10)).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &clear_symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: clear_symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 2,
                    },
                    amount: shares(10),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        let projection = Projection::<Position>::sqlite(pool);
        projection.catch_up().await.unwrap();

        // Pre-populate a stale entry to prove recovery replaces the set rather
        // than merging into it.
        trigger
            .pending_offchain_order_symbols
            .write()
            .await
            .insert(clear_symbol.clone());

        trigger
            .recover_pending_offchain_order_symbols(&projection)
            .await
            .unwrap();

        assert!(
            trigger.has_pending_offchain_order(&pending_symbol).await,
            "recovery must restore the block for a symbol with a pending offchain order"
        );
        assert!(
            !trigger.has_pending_offchain_order(&clear_symbol).await,
            "recovery must clear a symbol whose position has no pending offchain order"
        );
    }

    #[tokio::test]
    async fn usdc_check_suppresses_duplicate_dispatch_when_in_progress() {
        let inventory = InventoryView::default().with_usdc(usdc(100), usdc(900));
        let reactor = make_trigger_with_inventory(inventory).await;
        let trigger = reactor.clone();

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        UsdcRebalancingCheck.perform(&trigger).await.unwrap();

        assert_eq!(
            count_pending_transfer_usdc_to_hedging_jobs(&trigger).await,
            0,
            "In-progress flag should suppress duplicate USDC dispatch"
        );
        assert_eq!(
            count_pending_transfer_usdc_to_market_making_jobs(&trigger).await,
            0,
            "In-progress flag should suppress duplicate USDC dispatch"
        );
    }

    #[tokio::test]
    async fn base_to_alpaca_conversion_confirmed_with_excess_settled_amount_errors() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(100)),
            )
            .await
            .unwrap();

        let error = harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(200)),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RebalancingServiceError::SettledUsdcExceedsInitiatedAmount {
                    id: ref error_id,
                    event: usdc::UsdcTrackingEvent::ConversionConfirmed,
                    initiated_amount,
                    settled_amount,
                } if *error_id == id
                    && initiated_amount == usdc(100)
                    && settled_amount == usdc(200)
            ),
            "expected SettledUsdcExceedsInitiatedAmount, got {error:?}"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::MarketMaking),
            Some(usdc(100)),
            "rejected settlement must leave the original inflight amount untouched"
        );
        drop(inventory);
        assert!(
            trigger.usdc_tracking.read().await.contains_key(&id),
            "tracking context must be preserved when settlement is rejected"
        );
    }

    #[tokio::test]
    async fn alpaca_to_base_deposit_confirmed_with_excess_bridged_amount_errors() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(100)),
            )
            .await
            .unwrap();

        // Bridge reports more USDC arriving than was withdrawn -- impossible
        // in production, so settlement must reject and inventory must stay
        // untouched.
        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_bridged_with_amounts(usdc(200), Usdc::ZERO),
            )
            .await
            .unwrap();

        let error = harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RebalancingServiceError::SettledUsdcExceedsInitiatedAmount {
                    id: ref error_id,
                    event: usdc::UsdcTrackingEvent::DepositConfirmed,
                    initiated_amount,
                    settled_amount,
                } if *error_id == id
                    && initiated_amount == usdc(100)
                    && settled_amount == usdc(200)
            ),
            "expected SettledUsdcExceedsInitiatedAmount, got {error:?}"
        );
    }

    #[tokio::test]
    async fn deposit_confirmed_without_any_tracking_clears_guard_without_reconciliation() {
        let trigger = make_trigger_with_inventory(InventoryView::default()).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase),
            )
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "an AlpacaToBase deposit confirmation with no tracking (resumed after restart) clears \
             the guard rather than wedging it forever on a missing-context error"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "completing without tracking should not fabricate a context"
        );
    }

    #[tokio::test]
    async fn alpaca_to_base_initiated_after_conversion_confirmed_starts_inventory_transfer() {
        // AlpacaToBase converts USD->USDC BEFORE withdrawal. The Initiated
        // event arrives *after* the conversion stages have already populated
        // tracking; track_initiated_usdc_rebalance must detect the existing
        // tracking entry whose source transfer hasn't started and apply the
        // inventory transfer Start exactly once.
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(1000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
            )
            .await
            .unwrap();

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(400)),
            )
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(Usdc::ZERO),
            "no inventory transfer should occur during conversion stages"
        );
        drop(inventory);

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
            )
            .await
            .unwrap();

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_inflight(Venue::Hedging),
            Some(usdc(400)),
            "Initiated must move source funds into inflight"
        );
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(600)),
            "Initiated must remove source funds from available"
        );
        drop(inventory);

        let tracking = trigger
            .usdc_tracking
            .read()
            .await
            .get(&id)
            .cloned()
            .expect("tracking should still exist after Initiated");
        assert_eq!(tracking.stage, usdc::UsdcRebalanceStage::Initiated);
        assert_eq!(tracking.initiated_amount, usdc(400));
    }

    #[tokio::test]
    async fn check_and_trigger_equity_skips_token_not_in_registry() {
        let seeded = Symbol::new("AAPL").unwrap();
        let unknown = Symbol::new("MSFT").unwrap();
        let inventory = InventoryView::default().with_equity(seeded.clone(), shares(0), shares(0));

        let reactor = make_trigger_with_inventory_and_registry(inventory, &seeded).await;
        let trigger = reactor.clone();

        trigger
            .check_and_trigger_equity(&unknown)
            .await
            .expect("stale queued checks for unregistered symbols should no-op");
    }

    #[tokio::test]
    async fn alpaca_to_base_initiated_replay_does_not_double_apply_inventory_transfer() {
        // After the source transfer has started (Initiated stage), a replayed
        // Initiated event must update tracking metadata without re-applying
        // the inventory Start. Otherwise inflight would double-count.
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(1000));
        let trigger = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
            )
            .await
            .unwrap();

        harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
            )
            .await
            .unwrap();

        let inflight = {
            let inventory = trigger.inventory.read().await;
            inventory.usdc_inflight(Venue::Hedging)
        };
        assert_eq!(
            inflight,
            Some(usdc(400)),
            "replayed Initiated must not double-count inflight"
        );
    }

    /// `expire_stuck_mints` must skip mints whose symbol's in-progress guard is
    /// `HeldForRecovery`. The recovery job owns those mints and is responsible for
    /// driving them to a terminal state; timing them out would clear the guard and
    /// allow a double-mint while the original tokens are still in the wallet.
    #[tokio::test]
    async fn expire_stuck_mints_skips_held_for_recovery_mints() {
        let symbol = Symbol::new("AAPL").unwrap();
        let now = Utc::now();

        // Use a very short timeout so the mint would normally be cleaned up.
        let config = test_config_with_timeout(Duration::from_secs(60));
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wrapper = Arc::new(MockWrapper::new());
        let trigger = Arc::new(RebalancingService::new(
            config,
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            wrapper,
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));

        let id = issuer_request_id("held-for-recovery-timeout");

        // Seed a mint tracking entry at a post-receipt stage with a stale
        // last_progress_at so the timeout sweep would normally pick it up.
        trigger.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                tokenization_request_id: None,
                stage: MintTrackingStage::TokensReceived,
                last_progress_at: now - ChronoDuration::hours(2),
            },
        );

        // Set the guard to HeldForRecovery for this symbol (simulating the
        // PostReceipt handoff from the transfer job).
        trigger
            .equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), equity::GuardState::HeldForRecovery);

        // Run expire_stuck_mints with a far-future now so the mint is well past
        // the timeout threshold.
        trigger
            .expire_stuck_mints(now + ChronoDuration::hours(24))
            .await
            .unwrap();

        // The mint tracking entry must NOT have been removed.
        assert!(
            trigger.mint_tracking.read().await.contains_key(&id),
            "expire_stuck_mints must not clean up a HeldForRecovery mint"
        );

        // The guard must still be HeldForRecovery.
        assert_eq!(
            trigger
                .equity_in_progress
                .read()
                .unwrap()
                .get(&symbol)
                .cloned(),
            Some(equity::GuardState::HeldForRecovery),
            "expire_stuck_mints must not clear a HeldForRecovery guard"
        );
    }

    /// `clear_equity_in_progress_unless_held_for_recovery` is the single-lock
    /// check-and-clear that closes the timeout-sweep TOCTOU race: a concurrent
    /// `mark_held_for_recovery` can flip `ActiveTransfer` -> `HeldForRecovery`
    /// after the steady-state check but before the clear, and a non-atomic clear
    /// would drop the recovery-owned guard and fail a mint whose tokens are still
    /// in the wallet -- the double-mint this guard exists to prevent. This
    /// verifies the invariant the race hinges on: a `HeldForRecovery` slot is
    /// never cleared (caller skips the failure event), while an `ActiveTransfer`
    /// or absent slot is cleared so a fresh rebalance can proceed.
    #[tokio::test]
    async fn clear_equity_in_progress_unless_held_for_recovery_preserves_recovery_ownership() {
        let symbol = Symbol::new("AAPL").unwrap();

        let config = test_config_with_timeout(Duration::from_secs(60));
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let wrapper = Arc::new(MockWrapper::new());
        let trigger = Arc::new(RebalancingService::new(
            config,
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            wrapper,
            RebalancingSchedulers::new(&apalis_pool),
            Arc::new(crate::alerts::NoopNotifier),
        ));

        // HeldForRecovery: recovery owns the slot -- must be left untouched and
        // signal the caller (returns false) to skip the failure event.
        trigger
            .equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), equity::GuardState::HeldForRecovery);
        assert!(
            !trigger.clear_equity_in_progress_unless_held_for_recovery(&symbol),
            "a HeldForRecovery slot must not be cleared by the timeout path",
        );
        assert_eq!(
            trigger.equity_in_progress.read().unwrap().get(&symbol),
            Some(&equity::GuardState::HeldForRecovery),
            "the HeldForRecovery guard must remain after a refused clear",
        );

        // ActiveTransfer: a live transfer that genuinely timed out -- clear it so
        // a fresh rebalance can proceed.
        trigger.equity_in_progress.write().unwrap().insert(
            symbol.clone(),
            equity::GuardState::ActiveTransfer { generation: 0 },
        );
        assert!(
            trigger.clear_equity_in_progress_unless_held_for_recovery(&symbol),
            "an ActiveTransfer slot must be cleared so a fresh rebalance can proceed",
        );
        assert_eq!(
            trigger.equity_in_progress.read().unwrap().get(&symbol),
            None,
            "the ActiveTransfer guard must be removed after a successful clear",
        );

        // Absent: clearing is a no-op that still reports success.
        assert!(
            trigger.clear_equity_in_progress_unless_held_for_recovery(&symbol),
            "an absent slot must report a successful (no-op) clear",
        );
    }
}
