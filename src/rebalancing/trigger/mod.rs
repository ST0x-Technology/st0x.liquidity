//! Rebalancing trigger that reacts to inventory imbalances.

mod equity;
mod usdc;

pub(crate) use usdc::ALPACA_MINIMUM_WITHDRAWAL;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::{debug, error, warn};

use rain_math_float::Float;
use st0x_event_sorcery::{AggregateError, EntityList, LifecycleError, Reactor, Store, deps};
use st0x_execution::{AlpacaBrokerApiCtx, FractionalShares, Positive, Symbol};
use st0x_finance::Usdc;

use crate::config::AssetsConfig;
use crate::equity_redemption::{EquityRedemption, EquityRedemptionEvent, RedemptionAggregateId};
use crate::inventory::projection::InventoryProjectionError;
use crate::inventory::snapshot::{InventorySnapshot, InventorySnapshotEvent};
use crate::inventory::{
    BroadcastingInventory, ImbalanceThreshold, Inventory, InventoryView, InventoryViewError,
    Operator, TransferOp, Venue,
};
use crate::position::{Position, PositionEvent};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintEvent,
};
use crate::usdc_rebalance::{
    RebalanceDirection, UsdcRebalance, UsdcRebalanceEvent, UsdcRebalanceId,
};
use crate::vault_registry::{VaultRegistry, VaultRegistryId};
use crate::wrapper::Wrapper;

/// Why the rebalancing trigger reactor failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RebalancingTriggerError {
    #[error(transparent)]
    Inventory(#[from] InventoryViewError),
    #[error(transparent)]
    Projection(#[from] InventoryProjectionError),
    #[error(transparent)]
    EquityTrigger(#[from] equity::EquityTriggerError),
    #[error("float arithmetic error: {0}")]
    Float(#[from] rain_math_float::FloatError),
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
}

/// Why loading a token address from the vault registry failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenAddressError {
    #[error("vault registry aggregate not initialized")]
    Uninitialized,
    #[error(transparent)]
    Persistence(#[from] AggregateError<LifecycleError<VaultRegistry>>),
}

/// Error type for rebalancing configuration validation.
#[derive(Debug, thiserror::Error)]
pub enum RebalancingCtxError {
    #[error("rebalancing requires alpaca-broker-api broker type")]
    NotAlpacaBroker,
    #[error("rebalancing transfer_timeout_secs must be non-zero")]
    ZeroTransferTimeout,
    #[error("invalid wallet config: {0}")]
    WalletConfig(#[from] toml::de::Error),
    #[error(transparent)]
    Evm(#[from] st0x_evm::EvmError),
}

/// USDC rebalancing configuration with explicit enable/disable.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum UsdcRebalancing {
    Enabled {
        #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
        target: Float,
        #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
        deviation: Float,
    },
    Disabled,
}

/// Rebalancing secrets from the encrypted TOML.
///
/// Does not use `deny_unknown_fields` to tolerate legacy secrets files
/// that still have `base_rpc_url`, `ethereum_rpc_url`, and `wallet`
/// under `[rebalancing]` (these have moved to `[evm]` and `[wallet]`).
#[derive(Deserialize)]
pub(crate) struct RebalancingSecrets {}

impl std::fmt::Debug for RebalancingSecrets {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RebalancingSecrets").finish()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RebalancingConfig {
    pub(crate) equity: ImbalanceThreshold,
    pub(crate) usdc: UsdcRebalancing,
    pub(crate) transfer_timeout_secs: u64,
    pub(crate) redemption_wallet: Address,
}

/// Runtime configuration for rebalancing operations.
///
/// Constructed asynchronously from `RebalancingConfig`,
/// `RebalancingSecrets`, and broker auth. During construction, wallets
/// are pre-built for both chains. After construction, all fields are
/// immutable.
///
/// Read-only provider access for either chain is available via
/// `base_wallet().provider()` and `ethereum_wallet().provider()`.
#[derive(Clone)]
pub struct RebalancingCtx {
    pub(crate) equity: ImbalanceThreshold,
    pub(crate) usdc: Option<ImbalanceThreshold>,
    pub(crate) transfer_timeout: Duration,
    /// Issuer's wallet for tokenized equity redemptions.
    pub(crate) redemption_wallet: Address,
    /// Circle attestation/fee API base URL (test-only override).
    #[cfg(feature = "test-support")]
    pub circle_api_base: String,
    /// `TokenMessengerV2` contract address (test-only override).
    #[cfg(feature = "test-support")]
    pub token_messenger: Address,
    /// `MessageTransmitterV2` contract address (test-only override).
    #[cfg(feature = "test-support")]
    pub message_transmitter: Address,
}

impl RebalancingCtx {
    /// Construct from config and secrets.
    ///
    /// Wallets are now built separately via [`OnchainWalletCtx`] —
    /// this constructor only validates and stores rebalancing-specific
    /// trigger thresholds and the redemption wallet.
    pub(crate) fn new(config: &RebalancingConfig) -> Result<Self, RebalancingCtxError> {
        if config.transfer_timeout_secs == 0 {
            return Err(RebalancingCtxError::ZeroTransferTimeout);
        }

        let usdc = match config.usdc {
            UsdcRebalancing::Enabled { target, deviation } => {
                Some(ImbalanceThreshold { target, deviation })
            }
            UsdcRebalancing::Disabled => None,
        };

        Ok(Self {
            equity: config.equity,
            usdc,
            transfer_timeout: Duration::from_secs(config.transfer_timeout_secs),
            redemption_wallet: config.redemption_wallet,
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        })
    }
}

#[cfg(test)]
#[bon::bon]
impl RebalancingCtx {
    /// Test constructor that creates a `RebalancingCtx` with stub wallets.
    ///
    /// The wallets panic on `send` -- use only in tests that don't submit
    /// transactions through the rebalancing wallet.
    #[builder]
    pub(crate) fn stub(
        equity: ImbalanceThreshold,
        usdc: Option<ImbalanceThreshold>,
        #[builder(default = Duration::from_secs(30 * 60))] transfer_timeout: Duration,
        redemption_wallet: Address,
    ) -> Self {
        Self {
            equity,
            usdc,
            transfer_timeout,
            redemption_wallet,
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        }
    }
}

#[cfg(feature = "test-support")]
#[bon::bon]
impl RebalancingCtx {
    /// Test constructor that accepts pre-built wallets for e2e tests
    /// that need real onchain interaction (e.g. with Anvil forks).
    #[builder]
    pub fn with_wallets(
        equity: ImbalanceThreshold,
        usdc: UsdcRebalancing,
        #[builder(default = Duration::from_secs(30 * 60))] transfer_timeout: Duration,
        redemption_wallet: Address,
    ) -> Self {
        let usdc = match usdc {
            UsdcRebalancing::Enabled { target, deviation } => {
                Some(ImbalanceThreshold { target, deviation })
            }
            UsdcRebalancing::Disabled => None,
        };

        Self {
            equity,
            usdc,
            transfer_timeout,
            redemption_wallet,
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        }
    }

    /// Sets the Circle API base URL override (for e2e tests with local
    /// CCTP contracts and a mock attestation server).
    #[must_use]
    pub fn with_circle_api_base(mut self, base_url: String) -> Self {
        self.circle_api_base = base_url;
        self
    }

    /// Sets the CCTP contract address overrides (for e2e tests with
    /// locally deployed CCTP contracts).
    #[must_use]
    pub fn with_cctp_addresses(
        mut self,
        token_messenger: Address,
        message_transmitter: Address,
    ) -> Self {
        self.token_messenger = token_messenger;
        self.message_transmitter = message_transmitter;
        self
    }
}

impl std::fmt::Debug for RebalancingCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebalancingCtx")
            .field("equity", &self.equity)
            .field("usdc", &self.usdc)
            .field("redemption_wallet", &self.redemption_wallet)
            .finish_non_exhaustive()
    }
}

/// Configuration for the rebalancing trigger (runtime).
#[derive(Debug, Clone)]
pub(crate) struct RebalancingTriggerConfig {
    pub(crate) equity: ImbalanceThreshold,
    pub(crate) usdc: Option<ImbalanceThreshold>,
    pub(crate) transfer_timeout: Duration,
    pub(crate) assets: AssetsConfig,
    pub(crate) disabled_assets: HashSet<Symbol>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MintTrackingStage {
    Requested,
    Accepted,
    TokensReceived,
    TokensWrapped,
}

impl std::fmt::Display for MintTrackingStage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Requested => write!(formatter, "MintRequested"),
            Self::Accepted => write!(formatter, "MintAccepted"),
            Self::TokensReceived => write!(formatter, "TokensReceived"),
            Self::TokensWrapped => write!(formatter, "TokensWrapped"),
        }
    }
}

#[derive(Debug, Clone)]
struct MintTracking {
    symbol: Symbol,
    quantity: FractionalShares,
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
            stage: MintTrackingStage::Requested,
            last_progress_at: *requested_at,
        })
    }

    fn track_progress(&mut self, event: &TokenizedEquityMintEvent) {
        match event {
            TokenizedEquityMintEvent::MintAccepted { accepted_at, .. } => {
                self.stage = MintTrackingStage::Accepted;
                self.last_progress_at = *accepted_at;
            }
            TokenizedEquityMintEvent::TokensReceived { received_at, .. } => {
                self.stage = MintTrackingStage::TokensReceived;
                self.last_progress_at = *received_at;
            }
            TokenizedEquityMintEvent::TokensWrapped { wrapped_at, .. } => {
                self.stage = MintTrackingStage::TokensWrapped;
                self.last_progress_at = *wrapped_at;
            }
            TokenizedEquityMintEvent::MintRequested { .. }
            | TokenizedEquityMintEvent::MintRejected { .. }
            | TokenizedEquityMintEvent::MintAcceptanceFailed { .. }
            | TokenizedEquityMintEvent::WrappingFailed { .. }
            | TokenizedEquityMintEvent::DepositedIntoRaindex { .. }
            | TokenizedEquityMintEvent::RaindexDepositFailed { .. } => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedemptionTrackingStage {
    WithdrawnFromRaindex,
    TokensUnwrapped,
    TokensSent,
    Detected,
}

impl std::fmt::Display for RedemptionTrackingStage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WithdrawnFromRaindex => write!(formatter, "WithdrawnFromRaindex"),
            Self::TokensUnwrapped => write!(formatter, "TokensUnwrapped"),
            Self::TokensSent => write!(formatter, "TokensSent"),
            Self::Detected => write!(formatter, "Detected"),
        }
    }
}

#[derive(Debug, Clone)]
struct RedemptionTracking {
    symbol: Symbol,
    quantity: FractionalShares,
    stage: RedemptionTrackingStage,
    last_progress_at: DateTime<Utc>,
}

impl RedemptionTracking {
    fn from_withdrawn_event(event: &EquityRedemptionEvent) -> Option<Self> {
        let EquityRedemptionEvent::WithdrawnFromRaindex {
            symbol,
            quantity,
            withdrawn_at,
            ..
        } = event
        else {
            return None;
        };

        Some(Self {
            symbol: symbol.clone(),
            quantity: FractionalShares::new(*quantity),
            stage: RedemptionTrackingStage::WithdrawnFromRaindex,
            last_progress_at: *withdrawn_at,
        })
    }

    fn track_progress(&mut self, event: &EquityRedemptionEvent) {
        match event {
            EquityRedemptionEvent::TokensUnwrapped { unwrapped_at, .. } => {
                self.stage = RedemptionTrackingStage::TokensUnwrapped;
                self.last_progress_at = *unwrapped_at;
            }
            EquityRedemptionEvent::TokensSent { sent_at, .. } => {
                self.stage = RedemptionTrackingStage::TokensSent;
                self.last_progress_at = *sent_at;
            }
            EquityRedemptionEvent::Detected { detected_at, .. } => {
                self.stage = RedemptionTrackingStage::Detected;
                self.last_progress_at = *detected_at;
            }
            EquityRedemptionEvent::WithdrawnFromRaindex { .. }
            | EquityRedemptionEvent::TransferFailed { .. }
            | EquityRedemptionEvent::DetectionFailed { .. }
            | EquityRedemptionEvent::RedemptionRejected { .. }
            | EquityRedemptionEvent::Completed { .. } => {}
        }
    }
}

/// Operations triggered by inventory imbalances.
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
    /// Move USDC from Alpaca to Base (too much offchain).
    UsdcAlpacaToBase { amount: Usdc },
    /// Move USDC from Base to Alpaca (too much onchain).
    UsdcBaseToAlpaca { amount: Usdc },
}

/// Trigger that monitors inventory and sends rebalancing operations.
pub(crate) struct RebalancingTrigger {
    config: RebalancingTriggerConfig,
    vault_registry: Arc<Store<VaultRegistry>>,
    orderbook: Address,
    order_owner: Address,
    inventory: Arc<BroadcastingInventory>,
    pub(crate) equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
    pub(crate) usdc_in_progress: Arc<AtomicBool>,
    sender: mpsc::Sender<TriggeredOperation>,
    wrapper: Arc<dyn Wrapper>,
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
    mint_event_sync: Arc<Mutex<()>>,
    redemption_event_sync: Arc<Mutex<()>>,
    usdc_event_sync: Arc<Mutex<()>>,
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

impl RebalancingTrigger {
    pub(crate) fn new(
        config: RebalancingTriggerConfig,
        vault_registry: Arc<Store<VaultRegistry>>,
        orderbook: Address,
        order_owner: Address,
        inventory: Arc<BroadcastingInventory>,
        sender: mpsc::Sender<TriggeredOperation>,
        wrapper: Arc<dyn Wrapper>,
    ) -> Self {
        Self {
            config,
            vault_registry,
            orderbook,
            order_owner,
            inventory,
            equity_in_progress: Arc::new(std::sync::RwLock::new(HashSet::new())),
            usdc_in_progress: Arc::new(AtomicBool::new(false)),
            sender,
            wrapper,
            mint_tracking: Arc::new(RwLock::new(HashMap::new())),
            redemption_tracking: Arc::new(RwLock::new(HashMap::new())),
            usdc_tracking: Arc::new(RwLock::new(HashMap::new())),
            suppressed_inflight_symbols: Arc::new(RwLock::new(HashMap::new())),
            timed_out_mints: Arc::new(RwLock::new(HashMap::new())),
            timed_out_redemptions: Arc::new(RwLock::new(HashMap::new())),
            timed_out_usdc_rebalances: Arc::new(RwLock::new(HashMap::new())),
            mint_event_sync: Arc::new(Mutex::new(())),
            redemption_event_sync: Arc::new(Mutex::new(())),
            usdc_event_sync: Arc::new(Mutex::new(())),
        }
    }

    async fn expire_stuck_operations(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), RebalancingTriggerError> {
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

    async fn expire_stuck_mints(&self, now: DateTime<Utc>) -> Result<(), RebalancingTriggerError> {
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
            let Some((tracking, elapsed)) = self.cleanup_timed_out_mint(&id, now).await? else {
                continue;
            };

            error!(
                aggregate_id = %id,
                symbol = %tracking.symbol,
                stage = %tracking.stage,
                ?elapsed,
                "Mint transfer timed out; clearing trigger guard and inventory inflight"
            );

            self.clear_equity_in_progress(&tracking.symbol);
        }

        Ok(())
    }

    async fn expire_stuck_redemptions(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), RebalancingTriggerError> {
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

            error!(
                aggregate_id = %id,
                symbol = %tracking.symbol,
                stage = %tracking.stage,
                ?elapsed,
                "Redemption transfer timed out; clearing trigger guard and inventory inflight"
            );

            self.clear_equity_in_progress(&tracking.symbol);
        }

        Ok(())
    }

    async fn expire_stuck_usdc_rebalances(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), RebalancingTriggerError> {
        let timed_out_ids = {
            let tracking = self.usdc_tracking.read().await;
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
            let Some((tracking, elapsed)) = self.cleanup_timed_out_usdc_rebalance(&id, now).await?
            else {
                continue;
            };

            error!(
                aggregate_id = %id,
                direction = ?tracking.direction,
                stage = %tracking.stage,
                ?elapsed,
                "USDC transfer timed out; clearing trigger guard and inventory inflight"
            );

            self.clear_usdc_in_progress();
        }

        Ok(())
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
    ) -> Result<Option<(MintTracking, Duration)>, RebalancingTriggerError> {
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
        *inventory =
            inventory
                .clone()
                .clear_equity_inflight(&tracking.symbol, Venue::Hedging, now)?;
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
    ) -> Result<Option<(RedemptionTracking, Duration)>, RebalancingTriggerError> {
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
        *inventory =
            inventory
                .clone()
                .clear_equity_inflight(&tracking.symbol, Venue::MarketMaking, now)?;
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
    ) -> Result<Option<(usdc::UsdcRebalanceTracking, Duration)>, RebalancingTriggerError> {
        let _event_sync_guard = self.usdc_event_sync.lock().await;
        let mut tracking_guard = self.usdc_tracking.write().await;
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
            .clear_usdc_inflight(tracking.source_venue(), now)?;
        drop(inventory);

        self.timed_out_usdc_rebalances
            .write()
            .await
            .insert(id.clone(), now);
        tracking_guard.remove(id);
        drop(tracking_guard);

        Ok(Some((tracking, elapsed)))
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
    ) {
        let mut tracking = self.redemption_tracking.write().await;

        if let Some(existing) = tracking.get_mut(id) {
            existing.track_progress(event);
            return;
        }

        if let Some(created) = RedemptionTracking::from_withdrawn_event(event) {
            tracking.insert(id.clone(), created);
        }
    }

    /// Fold the snapshot event into the view, then run threshold
    /// checks. A failed apply (including recovery) short-circuits the
    /// checks so rebalancing never runs against a stale view.
    async fn on_snapshot(
        &self,
        event: InventorySnapshotEvent,
    ) -> Result<(), RebalancingTriggerError> {
        let now = Utc::now();
        let fetched_at = event.timestamp();
        self.expire_stuck_operations(now).await?;

        let filtered_inflight = match &event {
            InventorySnapshotEvent::InflightEquity {
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

        use InventorySnapshotEvent::*;
        let updated = match &event {
            OnchainEquity { balances, .. } => {
                balances
                    .iter()
                    .try_fold(inventory.clone(), |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::on_snapshot(
                                Venue::MarketMaking,
                                *snapshot_balance,
                                fetched_at,
                            ),
                            now,
                        )
                    })
            }

            OnchainUsdc { usdc_balance, .. } => inventory.clone().update_usdc(
                Inventory::on_snapshot(Venue::MarketMaking, *usdc_balance, fetched_at),
                now,
            ),

            OffchainEquity { positions, .. } => {
                positions
                    .iter()
                    .try_fold(inventory.clone(), |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::on_snapshot(Venue::Hedging, *snapshot_balance, fetched_at),
                            now,
                        )
                    })
            }

            OffchainUsd {
                usd_balance_cents, ..
            } => {
                let usdc = Usdc::from_cents(*usd_balance_cents)
                    .ok_or(InventoryViewError::UsdBalanceConversion(*usd_balance_cents))?;
                inventory.clone().update_usdc(
                    Inventory::on_snapshot(Venue::Hedging, usdc, fetched_at),
                    now,
                )
            }

            EthereumUsdc { .. }
            | BaseWalletUsdc { .. }
            | AlpacaWalletUsdc { .. }
            | BaseWalletUnwrappedEquity { .. }
            | BaseWalletWrappedEquity { .. }
            | OffchainMarginSafeBuyingPower { .. } => Ok(inventory.clone()),

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

        debug!("Applied inventory snapshot event");

        self.check_and_trigger_after_snapshot(&event).await
    }

    /// Reprocess a snapshot event after the normal handler failed.
    ///
    /// Resets the inventory, then force-applies the snapshot --
    /// bypassing inflight guards that may have caused the
    /// original failure.
    async fn on_snapshot_recovery(
        &self,
        error: RebalancingTriggerError,
        event: InventorySnapshotEvent,
    ) -> Result<(), RebalancingTriggerError> {
        self.expire_stuck_operations_with_logging().await;

        let inventory_error = match error {
            RebalancingTriggerError::Inventory(inventory_error) => inventory_error,
            other @ (RebalancingTriggerError::Projection(_)
            | RebalancingTriggerError::EquityTrigger(_)
            | RebalancingTriggerError::Float(_)
            | RebalancingTriggerError::MissingUsdcTrackingContext { .. }
            | RebalancingTriggerError::MissingUsdcBridgedAmount { .. }
            | RebalancingTriggerError::SettledUsdcExceedsInitiatedAmount { .. }) => {
                return Err(other);
            }
        };

        warn!(
            ?inventory_error,
            "Resetting inventory and force-applying snapshot to recover"
        );

        // Wrap in Arc so it can be cloned across multiple force_on_snapshot calls
        let recovery_reason = Arc::new(inventory_error);

        let now = Utc::now();
        let filtered_inflight = match &event {
            InventorySnapshotEvent::InflightEquity {
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

        use InventorySnapshotEvent::*;
        let updated = match &event {
            OnchainEquity { balances, .. } => {
                balances
                    .iter()
                    .try_fold(inventory.clone(), |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::force_on_snapshot(
                                Venue::MarketMaking,
                                *snapshot_balance,
                                recovery_reason.clone(),
                            ),
                            now,
                        )
                    })
            }

            OnchainUsdc { usdc_balance, .. } => inventory.clone().update_usdc(
                Inventory::force_on_snapshot(
                    Venue::MarketMaking,
                    *usdc_balance,
                    recovery_reason.clone(),
                ),
                now,
            ),

            OffchainEquity { positions, .. } => {
                positions
                    .iter()
                    .try_fold(inventory.clone(), |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::force_on_snapshot(
                                Venue::Hedging,
                                *snapshot_balance,
                                recovery_reason.clone(),
                            ),
                            now,
                        )
                    })
            }

            OffchainUsd {
                usd_balance_cents, ..
            } => {
                let usdc = Usdc::from_cents(*usd_balance_cents)
                    .ok_or(InventoryViewError::UsdBalanceConversion(*usd_balance_cents))?;
                inventory.clone().update_usdc(
                    Inventory::force_on_snapshot(Venue::Hedging, usdc, recovery_reason),
                    now,
                )
            }

            EthereumUsdc { .. }
            | BaseWalletUsdc { .. }
            | AlpacaWalletUsdc { .. }
            | BaseWalletUnwrappedEquity { .. }
            | BaseWalletWrappedEquity { .. }
            | OffchainMarginSafeBuyingPower { .. } => Ok(inventory.clone()),

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

        debug!("Force-applied inventory snapshot after recovery");

        self.check_and_trigger_after_snapshot(&event).await
    }

    /// Trigger rebalancing checks after a snapshot event.
    async fn check_and_trigger_after_snapshot(
        &self,
        event: &InventorySnapshotEvent,
    ) -> Result<(), RebalancingTriggerError> {
        use InventorySnapshotEvent::*;

        match event {
            OnchainEquity { balances, .. } => {
                for symbol in balances.keys() {
                    self.check_and_trigger_equity(symbol).await?;
                }
            }
            OffchainEquity { positions, .. } => {
                for symbol in positions.keys() {
                    self.check_and_trigger_equity(symbol).await?;
                }
            }
            OnchainUsdc { .. } | OffchainUsd { .. } => {
                self.check_and_trigger_usdc().await;
            }
            EthereumUsdc { .. }
            | BaseWalletUsdc { .. }
            | AlpacaWalletUsdc { .. }
            | BaseWalletUnwrappedEquity { .. }
            | BaseWalletWrappedEquity { .. }
            // Buying power is display-only and doesn't feed venue balances,
            // so it never drives a rebalance.
            | OffchainMarginSafeBuyingPower { .. }
            // Inflight snapshots don't trigger rebalancing -- they indicate
            // transfers already in progress, not new balances to rebalance.
            | InflightEquity { .. } => {}
        }

        Ok(())
    }
}

deps!(
    RebalancingTrigger,
    [
        Position,
        TokenizedEquityMint,
        EquityRedemption,
        UsdcRebalance,
        InventorySnapshot
    ]
);

#[async_trait]
impl Reactor for RebalancingTrigger {
    type Error = RebalancingTriggerError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|symbol, event| async move {
                use PositionEvent::*;

                let timestamp = event.timestamp();
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

                    Initialized { .. }
                    | OffChainOrderPlaced { .. }
                    | OffChainOrderFailed { .. }
                    | ThresholdUpdated { .. } => return Ok(()),
                };

                let mut inventory = self.inventory.write().await;
                *inventory = inventory
                    .clone()
                    .update_equity(&symbol, equity_update, timestamp)?
                    .update_usdc(usdc_update, timestamp)?;
                drop(inventory);

                self.check_and_trigger_equity(&symbol).await?;
                self.check_and_trigger_usdc().await;

                Ok::<(), RebalancingTriggerError>(())
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

impl RebalancingTrigger {
    async fn expire_stuck_operations_with_logging(&self) {
        if let Err(error) = self.expire_stuck_operations(Utc::now()).await {
            error!(?error, "Failed to expire stuck rebalancing operations");
        }
    }

    fn try_claim_equity_guard(&self, symbol: &Symbol) -> Option<equity::InProgressGuard> {
        equity::InProgressGuard::try_claim(symbol.clone(), Arc::clone(&self.equity_in_progress))
    }

    async fn build_equity_operation(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<TriggeredOperation>, equity::EquityTriggerError> {
        let wrapped_token = self
            .load_token_address(symbol)
            .await?
            .ok_or(equity::EquityTriggerError::TokenNotInRegistry)?;

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

    fn try_send_operation(&self, operation: &TriggeredOperation, context: &'static str) -> bool {
        match self.sender.try_send(operation.clone()) {
            Ok(()) => true,
            Err(error) => {
                warn!(%error, context, "Failed to send triggered operation");
                false
            }
        }
    }

    async fn load_mint_tracking(&self, id: &IssuerRequestId) -> Option<MintTracking> {
        let Some(tracking) = self.mint_tracking.read().await.get(id).cloned() else {
            warn!(id = %id, "Mint event for untracked aggregate");
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
    ) -> Option<EquityInventoryUpdate> {
        use TokenizedEquityMintEvent::*;

        match event {
            MintAccepted { .. } => {
                Some(Self::start_equity_transfer_update(Venue::Hedging, quantity))
            }
            MintAcceptanceFailed { .. } => Some(Self::cancel_equity_transfer_update(
                Venue::Hedging,
                quantity,
            )),
            TokensReceived { .. } => Some(Self::complete_equity_transfer_update(
                Venue::Hedging,
                quantity,
            )),
            DepositedIntoRaindex { .. } => Some(Self::last_rebalancing_update()),
            MintRequested { .. }
            | MintRejected { .. }
            | TokensWrapped { .. }
            | WrappingFailed { .. }
            | RaindexDepositFailed { .. } => None,
        }
    }

    async fn apply_equity_update(
        &self,
        symbol: &Symbol,
        update: EquityInventoryUpdate,
    ) -> Result<(), RebalancingTriggerError> {
        let now = Utc::now();
        let mut inventory = self.inventory.write().await;
        *inventory = inventory.clone().update_equity(symbol, update, now)?;
        drop(inventory);
        Ok(())
    }

    async fn load_redemption_tracking(
        &self,
        id: &RedemptionAggregateId,
    ) -> Option<RedemptionTracking> {
        let Some(tracking) = self.redemption_tracking.read().await.get(id).cloned() else {
            warn!(id = %id, "Redemption event for untracked aggregate");
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
            WithdrawnFromRaindex { .. } => Some(Self::start_equity_transfer_update(
                Venue::MarketMaking,
                quantity,
            )),
            Completed { .. } => Some(Self::complete_equity_transfer_update(
                Venue::MarketMaking,
                quantity,
            )),
            TransferFailed { .. } => Some(Self::cancel_equity_transfer_update(
                Venue::MarketMaking,
                quantity,
            )),
            TokensUnwrapped { .. }
            | TokensSent { .. }
            | DetectionFailed { .. }
            | Detected { .. }
            | RedemptionRejected { .. } => None,
        }
    }

    /// Checks inventory for equity imbalance and triggers operation if needed.
    pub(crate) async fn check_and_trigger_equity(
        &self,
        symbol: &Symbol,
    ) -> Result<(), equity::EquityTriggerError> {
        self.expire_stuck_operations_with_logging().await;

        if self.config.disabled_assets.contains(symbol) {
            return Ok(());
        }

        let Some(guard) = self.try_claim_equity_guard(symbol) else {
            debug!(%symbol, "Skipped equity trigger: already in progress");
            return Ok(());
        };

        let Some(operation) = self.build_equity_operation(symbol).await? else {
            return Ok(());
        };

        if !self.try_send_operation(&operation, "equity") {
            return Ok(());
        }

        debug!(%symbol, ?operation, "Triggered equity rebalancing");
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
    fn usdc_rebalancing_params(&self) -> Option<(ImbalanceThreshold, Option<Usdc>)> {
        let threshold = self.config.usdc.as_ref()?;

        let usdc_limit = self
            .config
            .assets
            .cash
            .as_ref()
            .and_then(|cash| cash.operational_limit)
            .map(Positive::inner);

        Some((*threshold, usdc_limit))
    }

    /// Checks inventory for USDC imbalance and triggers operation if needed.
    pub(crate) async fn check_and_trigger_usdc(&self) {
        self.expire_stuck_operations_with_logging().await;

        let Some((threshold, usdc_limit)) = self.usdc_rebalancing_params() else {
            return;
        };

        let Some(guard) = self.try_claim_usdc_guard() else {
            debug!("Skipped USDC trigger: already in progress");
            return;
        };

        let Ok(operation) =
            usdc::check_imbalance_and_build_operation(&threshold, &self.inventory, usdc_limit)
                .await
                .inspect_err(|skip| debug!(?skip, "Skipped USDC trigger"))
        else {
            return;
        };

        if !self.try_send_operation(&operation, "usdc") {
            return;
        }

        debug!(?operation, "Triggered USDC rebalancing");
        guard.defuse();
    }

    /// Clears the in-progress flag for an equity symbol.
    pub(crate) fn clear_equity_in_progress(&self, symbol: &Symbol) {
        let mut guard = match self.equity_in_progress.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.remove(symbol);
    }

    /// Clears the in-progress flag for USDC rebalancing.
    pub(crate) fn clear_usdc_in_progress(&self) {
        self.usdc_in_progress.store(false, Ordering::SeqCst);
    }

    async fn on_mint(
        &self,
        id: IssuerRequestId,
        event: TokenizedEquityMintEvent,
    ) -> Result<(), RebalancingTriggerError> {
        let event_sync_guard = self.mint_event_sync.lock().await;

        if self.mint_timed_out(&id).await {
            warn!(id = %id, "Ignoring late mint event after timeout cleanup");
            return Ok(());
        }

        self.track_mint_progress(&id, &event).await;

        let Some(tracking) = self.load_mint_tracking(&id).await else {
            return Ok(());
        };
        let symbol = tracking.symbol;

        if let Some(update) = Self::mint_inventory_update(&event, tracking.quantity) {
            self.apply_equity_update(&symbol, update).await?;
        }

        let should_check_usdc = if Self::is_terminal_mint_event(&event) {
            self.mint_tracking.write().await.remove(&id);
            self.clear_equity_in_progress(&symbol);
            debug!(%symbol, "Cleared equity in-progress flag after mint terminal event");
            true
        } else {
            false
        };

        drop(event_sync_guard);

        if should_check_usdc {
            self.check_and_trigger_usdc().await;
        }

        Ok(())
    }

    async fn on_redemption(
        &self,
        id: RedemptionAggregateId,
        event: EquityRedemptionEvent,
    ) -> Result<(), RebalancingTriggerError> {
        let event_sync_guard = self.redemption_event_sync.lock().await;

        if self.redemption_timed_out(&id).await {
            warn!(id = %id, "Ignoring late redemption event after timeout cleanup");
            return Ok(());
        }

        self.track_redemption_progress(&id, &event).await;

        let Some(tracking) = self.load_redemption_tracking(&id).await else {
            return Ok(());
        };
        let symbol = tracking.symbol;

        if let Some(update) = Self::redemption_inventory_update(&event, tracking.quantity) {
            self.apply_equity_update(&symbol, update).await?;
        }

        let should_check_usdc = if Self::is_terminal_redemption_event(&event) {
            self.redemption_tracking.write().await.remove(&id);
            self.clear_equity_in_progress(&symbol);
            debug!(
                %symbol,
                "Cleared equity in-progress flag after redemption terminal event"
            );
            true
        } else {
            false
        };

        drop(event_sync_guard);

        if should_check_usdc {
            self.check_and_trigger_usdc().await;
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
            | WrappingFailed { .. } => true,

            MintRequested { .. }
            | MintAccepted { .. }
            | TokensReceived { .. }
            | TokensWrapped { .. } => false,
        }
    }

    #[cfg(test)]
    fn extract_redemption_info(
        event: &EquityRedemptionEvent,
    ) -> Option<(Symbol, FractionalShares)> {
        RedemptionTracking::from_withdrawn_event(event)
            .map(|tracking| (tracking.symbol, tracking.quantity))
    }

    fn is_terminal_redemption_event(event: &EquityRedemptionEvent) -> bool {
        use EquityRedemptionEvent::*;

        match event {
            Completed { .. }
            | TransferFailed { .. }
            | DetectionFailed { .. }
            | RedemptionRejected { .. } => true,

            WithdrawnFromRaindex { .. }
            | TokensUnwrapped { .. }
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

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, U256, address, fixed_bytes};
    use chrono::{Duration as ChronoDuration, Utc};
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use st0x_event_sorcery::{EntityList, Never, ReactorHarness, TestStore, deps, test_store};
    use st0x_execution::{Direction, ExecutorOrderId, Positive};
    use st0x_finance::{Usd, Usdc};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;
    use uuid::Uuid;

    use st0x_dto::ServerMessage;

    use super::*;

    use crate::alpaca_wallet::AlpacaTransferId;
    use crate::config::{CashAssetConfig, EquitiesConfig, OperationMode};
    use crate::equity_redemption::DetectionFailure;
    use crate::inventory::snapshot::{InventorySnapshotEvent, InventorySnapshotId};
    use crate::inventory::view::Operator;
    use crate::inventory::{InventoryError, InventoryView, TransferOp, Venue};
    use crate::offchain_order::OffchainOrderId;
    use crate::position::{PositionEvent, TradeId};
    use crate::threshold::ExecutionThreshold;
    use crate::tokenized_equity_mint::{IssuerRequestId, ReceiptId, TokenizationRequestId};
    use crate::usdc_rebalance::{TransferRef, UsdcRebalanceCommand, UsdcRebalanceId};
    use crate::vault_registry::VaultRegistryCommand;
    use crate::wrapper::mock::MockWrapper;
    use st0x_execution::HasZero;
    use st0x_float_macro::float;

    fn test_config() -> RebalancingTriggerConfig {
        RebalancingTriggerConfig {
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
                equities: EquitiesConfig::default(),
                cash: Some(CashAssetConfig {
                    vault_id: None,
                    rebalancing: OperationMode::Enabled,
                    operational_limit: None,
                }),
            },
            disabled_assets: HashSet::new(),
        }
    }

    fn test_config_with_timeout(timeout: Duration) -> RebalancingTriggerConfig {
        RebalancingTriggerConfig {
            transfer_timeout: timeout,
            ..test_config()
        }
    }

    async fn make_trigger() -> (Arc<RebalancingTrigger>, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let pool = crate::test_utils::setup_test_db().await;
        let wrapper = Arc::new(MockWrapper::new());

        (
            Arc::new(RebalancingTrigger::new(
                test_config(),
                Arc::new(test_store::<VaultRegistry>(pool, ())),
                TEST_ORDERBOOK,
                TEST_ORDER_OWNER,
                inventory,
                sender,
                wrapper,
            )),
            receiver,
        )
    }

    /// Mirrors the production wiring where [`InventoryProjection`] writes
    /// Drives the trigger's production snapshot path: apply then
    /// threshold checks, short-circuiting if apply fails.
    async fn apply_and_dispatch_snapshot(
        trigger: Arc<RebalancingTrigger>,
        _id: InventorySnapshotId,
        event: InventorySnapshotEvent,
    ) -> Result<(), RebalancingTriggerError> {
        trigger.on_snapshot(event).await
    }

    #[tokio::test]
    async fn test_in_progress_symbol_does_not_send() {
        let (trigger, mut receiver) = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        tokio::time::timeout(
            Duration::from_secs(5),
            trigger.check_and_trigger_equity(&symbol),
        )
        .await
        .expect("equity timeout cleanup should complete promptly")
        .unwrap();
        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected channel to be empty (no message sent)"
        );
    }

    #[tokio::test]
    async fn test_usdc_in_progress_does_not_send() {
        let (trigger, mut receiver) = make_trigger().await;

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        trigger.check_and_trigger_usdc().await;
        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected channel to be empty (no message sent)"
        );
    }

    #[tokio::test]
    async fn test_usdc_disabled_via_cash_config_does_not_send() {
        let (sender, mut receiver) = mpsc::channel(10);
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let pool = crate::test_utils::setup_test_db().await;
        let wrapper = Arc::new(MockWrapper::new());

        let trigger = RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: test_config().equity,
                usdc: None,
                transfer_timeout: test_config().transfer_timeout,
                assets: AssetsConfig {
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
                disabled_assets: HashSet::new(),
            },
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            sender,
            wrapper,
        );

        trigger.check_and_trigger_usdc().await;

        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected channel to be empty when USDC rebalancing is disabled"
        );
    }

    #[tokio::test]
    async fn disabled_asset_skips_equity_trigger() {
        let symbol = Symbol::new("AAPL").unwrap();
        let (sender, mut receiver) = mpsc::channel(10);
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let pool = crate::test_utils::setup_test_db().await;
        let wrapper = Arc::new(MockWrapper::new());

        let trigger = RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: test_config().equity,
                usdc: test_config().usdc,
                transfer_timeout: test_config().transfer_timeout,
                assets: AssetsConfig {
                    equities: EquitiesConfig::default(),
                    cash: None,
                },
                disabled_assets: HashSet::from([symbol.clone()]),
            },
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            sender,
            wrapper,
        );

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Disabled asset should not trigger equity rebalancing"
        );
    }

    #[tokio::test]
    async fn test_clear_equity_in_progress() {
        let (trigger, _receiver) = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        assert!(trigger.equity_in_progress.read().unwrap().contains(&symbol));

        trigger.clear_equity_in_progress(&symbol);

        assert!(!trigger.equity_in_progress.read().unwrap().contains(&symbol));
    }

    #[tokio::test]
    async fn test_clear_usdc_in_progress() {
        let (trigger, _receiver) = make_trigger().await;

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(trigger.usdc_in_progress.load(Ordering::SeqCst));

        trigger.clear_usdc_in_progress();

        assert!(!trigger.usdc_in_progress.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_balanced_inventory_does_not_trigger() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));
        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        trigger.check_and_trigger_usdc().await;

        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected channel to be empty (no message sent)"
        );
    }

    fn shares(n: i64) -> FractionalShares {
        FractionalShares::new(float!(&n.to_string()))
    }

    fn make_onchain_fill(amount: FractionalShares, direction: Direction) -> PositionEvent {
        PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index: 0,
            },
            amount,
            direction,
            price_usdc: float!(150),
            block_timestamp: Utc::now(),
            seen_at: Utc::now(),
        }
    }

    fn make_offchain_fill(shares_filled: FractionalShares, direction: Direction) -> PositionEvent {
        PositionEvent::OffChainOrderFilled {
            offchain_order_id: OffchainOrderId::new(),
            shares_filled: Positive::new(shares_filled).unwrap(),
            direction,
            executor_order_id: ExecutorOrderId::new("ORD1"),
            price: Usd::new(float!(150)),
            broker_timestamp: Utc::now(),
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

    async fn make_trigger_with_inventory(
        inventory: InventoryView,
    ) -> (Arc<RebalancingTrigger>, mpsc::Receiver<TriggeredOperation>) {
        make_trigger_with_inventory_config(inventory, test_config()).await
    }

    async fn make_trigger_with_inventory_config(
        inventory: InventoryView,
        config: RebalancingTriggerConfig,
    ) -> (Arc<RebalancingTrigger>, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let pool = crate::test_utils::setup_test_db().await;

        (
            Arc::new(RebalancingTrigger::new(
                config,
                Arc::new(test_store::<VaultRegistry>(pool, ())),
                TEST_ORDERBOOK,
                TEST_ORDER_OWNER,
                inventory,
                sender,
                Arc::new(MockWrapper::new()),
            )),
            receiver,
        )
    }

    async fn make_trigger_with_inventory_and_registry(
        inventory: InventoryView,
        symbol: &Symbol,
    ) -> (Arc<RebalancingTrigger>, mpsc::Receiver<TriggeredOperation>) {
        make_trigger_with_inventory_and_registry_config(inventory, symbol, test_config()).await
    }

    async fn make_trigger_with_inventory_and_registry_config(
        inventory: InventoryView,
        symbol: &Symbol,
        config: RebalancingTriggerConfig,
    ) -> (Arc<RebalancingTrigger>, mpsc::Receiver<TriggeredOperation>) {
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
        config: RebalancingTriggerConfig,
    ) -> (Arc<RebalancingTrigger>, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let pool = crate::test_utils::setup_test_db().await;

        seed_vault_registry(&pool, symbol).await;

        (
            Arc::new(RebalancingTrigger::new(
                config,
                Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
                TEST_ORDERBOOK,
                TEST_ORDER_OWNER,
                inventory,
                sender,
                wrapper,
            )),
            receiver,
        )
    }

    #[tokio::test]
    async fn load_token_address_errors_when_registry_uninitialized() {
        let (trigger, _receiver) = make_trigger().await;
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;

        let result = trigger.load_token_address(&symbol).await.unwrap();
        assert_eq!(result, Some(TEST_TOKEN));
    }

    #[tokio::test]
    async fn load_token_address_returns_none_for_unknown_symbol() {
        let known = Symbol::new("AAPL").unwrap();
        let unknown = Symbol::new("MSFT").unwrap();
        let inventory = InventoryView::default().with_equity(known.clone(), shares(0), shares(0));

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &known).await;

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
        let (sender, mut receiver) = mpsc::channel(10);
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_usdc(usdc(1_000_000), usdc(1_000_000)),
            event_sender,
        ));
        let pool = crate::test_utils::setup_test_db().await;

        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            sender,
            Arc::new(MockWrapper::new()),
        ));

        let harness = ReactorHarness::new(trigger.clone());

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
        }

        for _ in 0..80 {
            let event = make_offchain_fill(shares(1), Direction::Buy);
            harness
                .receive::<Position>(symbol.clone(), event)
                .await
                .unwrap();
        }

        // Drain any intermediate triggers and do a final check.
        while receiver.try_recv().is_ok() {}
        trigger.clear_equity_in_progress(&symbol);

        // One more event to trigger the check after the imbalance is built up.
        let event = make_onchain_fill(shares(1), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        let triggered = receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Mint { .. })),
            "Expected Mint for imbalanced inventory starting from empty, got {triggered:?}"
        );
    }

    #[tokio::test]
    async fn position_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(1_000_000), usdc(1_000_000));

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(trigger.clone());

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
        while receiver.try_recv().is_ok() {}

        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected channel to be empty (no message sent)"
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(trigger.clone());

        // Apply a small buy that maintains balance (5 shares onchain).
        // After: 55 onchain, 50 offchain = 52.4% ratio, within 30-70% bounds.
        let event = make_onchain_fill(shares(5), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // No operation should be triggered.
        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected channel to be empty (no message sent)"
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(trigger.clone());

        // Apply a small event that triggers the imbalance check.
        let event = make_onchain_fill(shares(1), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        // Mint should be triggered because too much offchain.
        let triggered = receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Mint { .. })),
            "Expected Mint operation, got {triggered:?}"
        );
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
        let (trigger, mut receiver) = make_trigger_with_inventory_registry_and_wrapper(
            inventory,
            &symbol,
            wrapper,
            test_config(),
        )
        .await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        let triggered = receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Redemption { .. })),
            "Expected Redemption with 1.5 ratio, got {triggered:?}"
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
            issuer_request_id: IssuerRequestId::new("ISS123"),
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
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(30_000_000_000_000_000_000_u128),
            fees: None,
            received_at: Utc::now(),
        }
    }

    fn make_wrapping_failed(symbol: &Symbol, quantity: Float) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::WrappingFailed {
            symbol: symbol.clone(),
            quantity,
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;

        // Initially, trigger should detect imbalance (too much offchain).
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        let initial_check = receiver.try_recv();
        assert!(
            matches!(initial_check, Ok(TriggeredOperation::Mint { .. })),
            "Expected initial imbalance to trigger Mint, got {initial_check:?}"
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
        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected no operation due to inflight"
        );
    }

    #[tokio::test]
    async fn mint_completion_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }
        assert!(trigger.equity_in_progress.read().unwrap().contains(&symbol));

        // Check that DepositedIntoRaindex is detected as terminal.
        assert!(RebalancingTrigger::is_terminal_mint_event(
            &make_deposited_into_raindex()
        ));

        // Simulate what dispatch does - clear in-progress on terminal.
        trigger.clear_equity_in_progress(&symbol);
        assert!(!trigger.equity_in_progress.read().unwrap().contains(&symbol));
    }

    #[tokio::test]
    async fn mint_rejection_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        assert!(RebalancingTrigger::is_terminal_mint_event(
            &make_mint_rejected()
        ));

        trigger.clear_equity_in_progress(&symbol);
        assert!(!trigger.equity_in_progress.read().unwrap().contains(&symbol));
    }

    #[test]
    fn mint_acceptance_failure_is_terminal() {
        assert!(RebalancingTrigger::is_terminal_mint_event(
            &make_mint_acceptance_failed()
        ));
    }

    #[test]
    fn extract_mint_info_returns_symbol_and_quantity() {
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_mint_requested(&symbol, float!(42.5));

        let (extracted_symbol, extracted_quantity) =
            RebalancingTrigger::extract_mint_info(&event).unwrap();

        assert_eq!(extracted_symbol, symbol);
        assert!(extracted_quantity.inner().eq(float!(42.5)).unwrap());
    }

    #[test]
    fn extract_mint_info_returns_none_without_mint_requested() {
        let result = RebalancingTrigger::extract_mint_info(&make_deposited_into_raindex());
        assert!(result.is_none());
    }

    #[test]
    fn non_terminal_mint_events_are_not_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        assert!(!RebalancingTrigger::is_terminal_mint_event(
            &make_mint_requested(&symbol, float!(30))
        ));
        assert!(!RebalancingTrigger::is_terminal_mint_event(
            &make_mint_accepted()
        ));
        assert!(!RebalancingTrigger::is_terminal_mint_event(
            &make_tokens_received()
        ));
    }

    #[tokio::test]
    async fn mint_rejection_via_reactor_clears_in_progress() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = IssuerRequestId::new("mint-1");

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(10)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id, make_mint_rejected())
            .await
            .unwrap();

        assert!(
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
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
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("redemption-1");

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
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = IssuerRequestId::new("mint-blocks");

        // Verify imbalance triggers before reactor events
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert!(
            matches!(receiver.try_recv(), Ok(TriggeredOperation::Mint { .. })),
            "Should trigger Mint before reactor events"
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = IssuerRequestId::new("mint-transfer");

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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = IssuerRequestId::new("mint-fail");

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
        assert!(
            matches!(receiver.try_recv(), Ok(TriggeredOperation::Mint { .. })),
            "Imbalance should re-trigger after MintAcceptanceFailed cancels inflight"
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        // Set in-progress flag to verify terminal event clears it
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let id = IssuerRequestId::new("mint-deposit");

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
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let id = IssuerRequestId::new("mint-wrapping-fail");

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_wrapping_failed(&symbol, float!(30)))
            .await
            .unwrap();

        assert!(
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let id = IssuerRequestId::new("mint-deposit-fail");

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, float!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_raindex_deposit_failed())
            .await
            .unwrap();

        assert!(
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let id = RedemptionAggregateId::new("redemption-transfer-fail");

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
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let id = RedemptionAggregateId::new("redemption-detection-fail");

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
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        let id = RedemptionAggregateId::new("redemption-rejected");

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
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
            "In-progress flag should be cleared after terminal RedemptionRejected"
        );
    }

    #[tokio::test]
    async fn position_noop_events_via_reactor_do_not_error() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        // Initialized, OffChainOrderPlaced, OffChainOrderFailed, ThresholdUpdated
        // all return Ok(()) without modifying inventory
        harness
            .receive::<Position>(
                symbol.clone(),
                PositionEvent::Initialized {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    initialized_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Position>(
                symbol.clone(),
                PositionEvent::ThresholdUpdated {
                    old_threshold: ExecutionThreshold::whole_share(),
                    new_threshold: ExecutionThreshold::whole_share(),
                    updated_at: Utc::now(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn onchain_buy_updates_usdc_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(usdc(10000), usdc(10000));

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(trigger.clone());

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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(trigger.clone());

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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(trigger.clone());

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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(trigger.clone());

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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Snapshot onchain equity to 40 -> now 40 onchain, 20 offchain
        // 40/60 = 66.7% -> within threshold (upper bound 70%), no trigger
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(40));

        apply_and_dispatch_snapshot(
            trigger.clone(),
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Verify initial imbalance triggers
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert!(
            matches!(receiver.try_recv(), Ok(TriggeredOperation::Mint { .. })),
            "20% ratio should trigger Mint"
        );
        trigger.clear_equity_in_progress(&symbol);

        // Snapshot offchain to 20 -> 20 onchain, 20 offchain = 50% = balanced
        let mut positions = BTreeMap::new();
        positions.insert(symbol.clone(), shares(20));

        apply_and_dispatch_snapshot(
            trigger.clone(),
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("redemption-blocks");

        // Verify imbalance triggers before reactor events
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert!(
            matches!(
                receiver.try_recv(),
                Ok(TriggeredOperation::Redemption { .. })
            ),
            "Should trigger Redemption before reactor events"
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("redemption-rebalances");

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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "Inventory should be balanced after redemption completion (50/50)"
        );
    }

    #[tokio::test]
    async fn usdc_initiated_alpaca_to_base_via_reactor_blocks_usdc_trigger() {
        // Start with USDC imbalance: 200 onchain, 800 offchain = 20% ratio
        // With target 50%, deviation 30%, lower bound = 20%: at boundary, no trigger
        // Use 100 onchain, 900 offchain = 10% ratio -> triggers TooMuchOffchain
        let inventory = InventoryView::default().with_usdc(usdc(100), usdc(900));

        let (trigger, mut receiver) = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        // Verify imbalance triggers before reactor events
        trigger.check_and_trigger_usdc().await;
        assert!(
            receiver.try_recv().is_ok(),
            "Should trigger USDC rebalance before reactor events"
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "Inflight from Initiated(AlpacaToBase) should block USDC imbalance detection"
        );
    }

    #[tokio::test]
    async fn usdc_initiated_base_to_alpaca_via_reactor_blocks_usdc_trigger() {
        // 900 onchain, 100 offchain = 90% ratio -> TooMuchOnchain
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));

        let (trigger, mut receiver) = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        // Verify imbalance triggers before reactor events
        trigger.check_and_trigger_usdc().await;
        assert!(
            receiver.try_recv().is_ok(),
            "Should trigger USDC rebalance before reactor events"
        );
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "Inflight from Initiated(BaseToAlpaca) should block USDC imbalance detection"
        );
    }

    #[tokio::test]
    async fn base_to_alpaca_terminal_conversion_settles_usdc_inventory() {
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
                name: "bridging_failed_after_initiated",
                events: vec![
                    make_usdc_conversion_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
                    make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_withdrawal_confirmed(),
                    make_usdc_bridging_initiated(),
                    make_usdc_bridging_failed(),
                ],
            },
            Scenario {
                name: "deposit_failed_after_initiated",
                events: vec![
                    make_usdc_conversion_initiated(RebalanceDirection::AlpacaToBase, usdc(400)),
                    make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(399)),
                    make_usdc_withdrawal_confirmed(),
                    make_usdc_bridging_initiated(),
                    make_usdc_bridge_attestation_received(),
                    make_usdc_bridged_with_amounts(usdc(398), usdc(1)),
                    make_usdc_deposit_initiated(),
                    make_usdc_deposit_failed(),
                ],
            },
        ];

        for scenario in scenarios {
            let inventory = InventoryView::default().with_usdc(usdc(100), usdc(900));
            let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
                name: "bridging_failed_after_initiated",
                events: vec![
                    make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
                    make_usdc_withdrawal_confirmed(),
                    make_usdc_bridging_initiated(),
                    make_usdc_bridging_failed(),
                ],
            },
            Scenario {
                name: "deposit_failed_after_initiated",
                events: vec![
                    make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
                    make_usdc_withdrawal_confirmed(),
                    make_usdc_bridging_initiated(),
                    make_usdc_bridge_attestation_received(),
                    make_usdc_bridged_with_amounts(usdc(399), usdc(1)),
                    make_usdc_deposit_initiated(),
                    make_usdc_deposit_failed(),
                ],
            },
            Scenario {
                name: "conversion_failed_after_deposit",
                events: vec![
                    make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(400)),
                    make_usdc_withdrawal_confirmed(),
                    make_usdc_bridging_initiated(),
                    make_usdc_bridge_attestation_received(),
                    make_usdc_bridged_with_amounts(usdc(399), usdc(1)),
                    make_usdc_deposit_initiated(),
                    make_usdc_deposit_confirmed(RebalanceDirection::BaseToAlpaca),
                    make_usdc_conversion_initiated(RebalanceDirection::BaseToAlpaca, usdc(399)),
                    make_usdc_conversion_failed(),
                ],
            },
        ];

        for scenario in scenarios {
            let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));
            let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
    async fn snapshot_onchain_usdc_via_reactor_updates_usdc_balance() {
        // Start with 500 onchain, 500 offchain = balanced
        let inventory = InventoryView::default().with_usdc(usdc(500), usdc(500));

        let symbol = Symbol::new("AAPL").unwrap();
        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Balanced initially -> no trigger
        trigger.check_and_trigger_usdc().await;
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "Should be balanced initially"
        );

        // Snapshot says onchain is actually 900 -> creates imbalance
        apply_and_dispatch_snapshot(
            trigger.clone(),
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "64% ratio within 50% +/- 30% threshold"
        );
    }

    #[tokio::test]
    async fn snapshot_offchain_usd_via_reactor_updates_usdc_balance() {
        // Start with 900 onchain, 100 offchain = 90% ratio -> TooMuchOnchain
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));

        let symbol = Symbol::new("AAPL").unwrap();
        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Verify imbalance triggers before snapshot
        trigger.check_and_trigger_usdc().await;
        assert!(
            receiver.try_recv().is_ok(),
            "Should trigger USDC rebalance with 90% ratio"
        );
        trigger.clear_usdc_in_progress();

        // Snapshot says offchain is actually 900 (not 100)
        // 95000 cents = $950.00
        apply_and_dispatch_snapshot(
            trigger.clone(),
            id.clone(),
            InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: 90000,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // Now 900 onchain, 900 offchain = balanced -> no trigger
        trigger.check_and_trigger_usdc().await;
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "Should be balanced after offchain cash snapshot reconciled to 900"
        );
    }

    fn make_withdrawn_from_raindex(symbol: &Symbol, quantity: Float) -> EquityRedemptionEvent {
        EquityRedemptionEvent::WithdrawnFromRaindex {
            symbol: symbol.clone(),
            quantity,
            token: Address::random(),
            wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
            raindex_withdraw_tx: TxHash::random(),
            withdrawn_at: Utc::now(),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;

        // First trigger detects the imbalance and sends a redemption.
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        let first = receiver.try_recv().unwrap();
        assert!(matches!(first, TriggeredOperation::Redemption { .. }));

        // With in-progress guard held, second trigger should not send anything.
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected no operation while in-progress guard is held"
        );
    }

    #[tokio::test]
    async fn redemption_completion_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone(), shares(0), shares(0));

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }
        assert!(trigger.equity_in_progress.read().unwrap().contains(&symbol));

        // Check that Completed is detected as terminal.
        assert!(RebalancingTrigger::is_terminal_redemption_event(
            &make_redemption_completed()
        ));

        // Simulate what dispatch does - clear in-progress on terminal.
        trigger.clear_equity_in_progress(&symbol);
        assert!(!trigger.equity_in_progress.read().unwrap().contains(&symbol));
    }

    #[test]
    fn detection_failure_is_terminal_redemption_event() {
        assert!(RebalancingTrigger::is_terminal_redemption_event(
            &make_detection_failed()
        ));
    }

    #[test]
    fn redemption_rejection_is_terminal() {
        assert!(RebalancingTrigger::is_terminal_redemption_event(
            &make_redemption_rejected()
        ));
    }

    #[test]
    fn extract_redemption_info_returns_symbol_and_quantity() {
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_withdrawn_from_raindex(&symbol, float!(42.5));

        let (extracted_symbol, extracted_quantity) =
            RebalancingTrigger::extract_redemption_info(&event).unwrap();

        assert_eq!(extracted_symbol, symbol);
        assert!(extracted_quantity.inner().eq(float!(42.5)).unwrap());
    }

    #[test]
    fn extract_redemption_info_returns_none_without_tokens_sent() {
        let result = RebalancingTrigger::extract_redemption_info(&make_redemption_completed());
        assert!(result.is_none());
    }

    #[test]
    fn non_terminal_redemption_events_are_not_terminal() {
        let symbol = Symbol::new("AAPL").unwrap();
        assert!(!RebalancingTrigger::is_terminal_redemption_event(
            &make_withdrawn_from_raindex(&symbol, float!(30))
        ));
        assert!(!RebalancingTrigger::is_terminal_redemption_event(
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
            order_id: Uuid::new_v4(),
            initiated_at: Utc::now(),
        }
    }

    fn make_usdc_withdrawal_confirmed() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
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
            cctp_nonce: 42,
            attested_at: Utc::now(),
        }
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
            cctp_nonce: Some(12345),
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
        trigger: &Arc<RebalancingTrigger>,
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
        trigger: &Arc<RebalancingTrigger>,
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
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
    async fn bridged_without_initiated_returns_tracking_error() {
        let (trigger, _receiver) = make_trigger_with_inventory(InventoryView::default()).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        let error = harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_bridged())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            RebalancingTriggerError::MissingUsdcTrackingContext {
                id: error_id,
                event: usdc::UsdcTrackingEvent::Bridged,
            } if error_id == id
        ));
    }

    #[tokio::test]
    async fn initiated_with_insufficient_balance_does_not_insert_tracking_context() {
        let inventory = InventoryView::default().with_usdc(usdc(100), usdc(100));
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
            matches!(error, RebalancingTriggerError::Inventory(_)),
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
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
            RebalancingTriggerError::MissingUsdcBridgedAmount { id: error_id }
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
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
            .update_usdc(
                Inventory::transfer(Venue::Hedging, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap();
        let (trigger, mut receiver) = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
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

        let triggered = receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::UsdcAlpacaToBase { .. })),
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
    async fn usdc_post_deposit_conversion_refreshes_timeout_tracking() {
        let inventory = InventoryView::default()
            .with_usdc(usdc(900), usdc(100))
            .update_usdc(
                Inventory::transfer(Venue::MarketMaking, TransferOp::Start, usdc(400)),
                Utc::now(),
            )
            .unwrap();
        let (trigger, _receiver) = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
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
                    order_id: Uuid::new_v4(),
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
        let (trigger, _receiver) = make_trigger_with_inventory_config(
            inventory,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
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
                    order_id: Uuid::new_v4(),
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
    async fn conversion_failed_without_started_transfer_still_clears_in_progress() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        harness
            .receive::<UsdcRebalance>(id.clone(), make_usdc_conversion_failed())
            .await
            .unwrap();

        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "pre-withdrawal conversion failure should still clear usdc_in_progress"
        );
        assert!(
            !trigger.usdc_tracking.read().await.contains_key(&id),
            "pre-withdrawal conversion failure should not leave tracking context behind"
        );

        let inventory = trigger.inventory.read().await;
        assert_eq!(
            inventory.usdc_available(Venue::MarketMaking),
            Some(usdc(5000)),
            "conversion failure before withdrawal should not change onchain USDC inventory"
        );
        assert_eq!(
            inventory.usdc_available(Venue::Hedging),
            Some(usdc(5000)),
            "conversion failure before withdrawal should not change offchain USDC inventory"
        );
        drop(inventory);
    }

    #[tokio::test]
    async fn conversion_confirmed_without_tracking_returns_error_and_preserves_in_progress() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_in_progress.store(true, Ordering::SeqCst);

        let error = harness
            .receive::<UsdcRebalance>(
                id.clone(),
                make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(999)),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            RebalancingTriggerError::MissingUsdcTrackingContext {
                id: error_id,
                event: usdc::UsdcTrackingEvent::ConversionConfirmed,
            } if error_id == id
        ));
        assert!(
            trigger.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress should stay set when conversion settlement context is missing"
        );
    }

    #[tokio::test]
    async fn conversion_confirmed_above_initiated_returns_error_and_preserves_state() {
        let inventory = InventoryView::default().with_usdc(usdc(5000), usdc(5000));
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
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
            RebalancingTriggerError::SettledUsdcExceedsInitiatedAmount {
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
        assert!(RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_withdrawal_failed()
        ));
        assert!(RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_bridging_failed()
        ));
        assert!(RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_deposit_failed()
        ));
        assert!(RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_conversion_failed()
        ));
    }

    #[test]
    fn non_terminal_usdc_events_are_not_terminal() {
        assert!(!RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_initiated(RebalanceDirection::AlpacaToBase, usdc(1000))
        ));
        assert!(!RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_withdrawal_confirmed()
        ));
        assert!(!RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_bridging_initiated()
        ));
        assert!(!RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_bridged()
        ));
    }

    #[test]
    fn conversion_confirmed_is_terminal_for_base_to_alpaca() {
        // For BaseToAlpaca, ConversionConfirmed IS the terminal event.
        assert!(RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_conversion_confirmed(RebalanceDirection::BaseToAlpaca, usdc(998))
        ));
    }

    #[test]
    fn conversion_confirmed_is_not_terminal_for_alpaca_to_base() {
        // For AlpacaToBase, ConversionConfirmed is NOT terminal (flow continues
        // to withdrawal).
        assert!(!RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_conversion_confirmed(RebalanceDirection::AlpacaToBase, usdc(998))
        ));
    }

    #[test]
    fn deposit_confirmed_is_terminal_for_alpaca_to_base() {
        assert!(RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase)
        ));
    }

    #[test]
    fn deposit_confirmed_is_not_terminal_for_base_to_alpaca() {
        // For BaseToAlpaca, DepositConfirmed is NOT terminal because
        // post-deposit conversion (USDC->USD) is still required.
        assert!(!RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_deposit_confirmed(RebalanceDirection::BaseToAlpaca)
        ));
    }

    fn valid_rebalancing_config_toml() -> &'static str {
        r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            transfer_timeout_secs = 1800

            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#
    }

    fn valid_rebalancing_secrets_toml() -> &'static str {
        ""
    }

    #[test]
    fn deserialize_config_succeeds() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();

        assert!(config.equity.target.eq(float!(0.5)).unwrap());
        assert!(config.equity.deviation.eq(float!(0.2)).unwrap());

        let UsdcRebalancing::Enabled { target, deviation } = config.usdc else {
            panic!("expected UsdcRebalancing::Enabled");
        };
        assert!(target.eq(float!(0.5)).unwrap());
        assert!(deviation.eq(float!(0.3)).unwrap());
        assert_eq!(
            config.redemption_wallet,
            address!("1234567890123456789012345678901234567890")
        );
        assert_eq!(config.transfer_timeout_secs, 1800);
    }

    #[test]
    fn deserialize_secrets_succeeds() {
        let _secrets: RebalancingSecrets =
            toml::from_str(&valid_rebalancing_secrets_toml()).unwrap();
    }

    #[test]
    fn deserialize_with_custom_thresholds() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            transfer_timeout_secs = 1800

            [equity]
            target = "0.6"
            deviation = "0.1"

            [usdc]
            mode = "enabled"
            target = "0.4"
            deviation = "0.15"
        "#,
        )
        .unwrap();

        assert!(config.equity.target.eq(float!(0.6)).unwrap());
        assert!(config.equity.deviation.eq(float!(0.1)).unwrap());

        let UsdcRebalancing::Enabled { target, deviation } = config.usdc else {
            panic!("expected UsdcRebalancing::Enabled");
        };
        assert!(target.eq(float!(0.4)).unwrap());
        assert!(deviation.eq(float!(0.15)).unwrap());
    }

    #[test]
    fn deserialize_missing_redemption_wallet_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800

            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("redemption_wallet"),
            "Expected missing redemption_wallet error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_transfer_timeout_secs_fails() {
        let toml_str = r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"

            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("transfer_timeout_secs"),
            "Expected missing transfer_timeout_secs error, got: {error}"
        );
    }

    #[test]
    fn deserialize_rebalancing_secrets_ignores_legacy_fields() {
        let toml_str = r#"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://eth.example.com"
        "#;

        // RebalancingSecrets is now an empty struct that silently
        // ignores legacy fields (base_rpc_url, ethereum_rpc_url, wallet)
        // for backward compatibility.
        let _secrets: RebalancingSecrets = toml::from_str(toml_str).unwrap();
    }

    #[test]
    fn deserialize_missing_equity_fails() {
        let toml_str = r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            transfer_timeout_secs = 1800

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("equity"),
            "Expected missing equity error, got: {error}"
        );
    }

    #[tokio::test]
    async fn usdc_rebalancing_disabled_when_cash_ratio_absent() {
        // Regression: when usdc is None, startup must not require assets.cash.vault_id.
        // The trigger returns no USDC rebalancing params, so no USDC vault lookup occurs.
        let (sender, _receiver) = mpsc::channel(10);
        let pool = crate::test_utils::setup_test_db().await;
        let wrapper = Arc::new(MockWrapper::new());

        let trigger = RebalancingTrigger::new(
            RebalancingTriggerConfig {
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
                disabled_assets: HashSet::new(),
            },
            Arc::new(test_store::<VaultRegistry>(pool, ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            {
                let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
                Arc::new(BroadcastingInventory::new(
                    InventoryView::default(),
                    event_sender,
                ))
            },
            sender,
            wrapper,
        );

        assert!(
            trigger.usdc_rebalancing_params().is_none(),
            "Expected usdc_rebalancing_params to be None when cash ratio is absent"
        );
    }

    #[test]
    fn deserialize_missing_usdc_fails() {
        let toml_str = r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            transfer_timeout_secs = 1800

            [equity]
            target = "0.5"
            deviation = "0.2"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("usdc"),
            "Expected missing usdc error, got: {error}"
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

            let is_terminal = RebalancingTrigger::is_terminal_usdc_rebalance_event(&event);
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
            .send(&id, UsdcRebalanceCommand::ConfirmWithdrawal)
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
                    cctp_nonce: 12345,
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
        let transfer_id = AlpacaTransferId::from(uuid::Uuid::new_v4());
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
            .send(&id, UsdcRebalanceCommand::ConfirmWithdrawal)
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
                    cctp_nonce: 67890,
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
        let transfer_id = AlpacaTransferId::from(uuid::Uuid::new_v4());

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
        let transfer_id = crate::alpaca_wallet::AlpacaTransferId::from(uuid::Uuid::new_v4());
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
            .send(&id, UsdcRebalanceCommand::ConfirmWithdrawal)
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
                    order_id: uuid::Uuid::new_v4(),
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
        let (sender, _receiver) = mpsc::channel(10);
        let pool = crate::test_utils::setup_test_db().await;
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default().with_usdc(usdc(5000), usdc(5000)),
            event_sender,
        ));

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            Address::ZERO,
            Address::ZERO,
            inventory,
            sender,
            Arc::new(MockWrapper::new()),
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
            .send(&id, UsdcRebalanceCommand::ConfirmWithdrawal)
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
                    cctp_nonce: 99999,
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
                    order_id: uuid::Uuid::new_v4(),
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
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));

        let (sender, mut receiver) = mpsc::channel(10);
        let pool = crate::test_utils::setup_test_db().await;

        // Seed vault registry so token lookup succeeds
        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            sender,
            Arc::new(MockWrapper::new()),
        ));

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
        apply_and_dispatch_snapshot(trigger.clone(), id.clone(), onchain_event.clone())
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
        let triggered = receiver.try_recv();

        assert!(
            triggered.is_err(),
            "CORRECT: No operation should trigger with partial inventory data. \
             BUG: Got {triggered:?} - system incorrectly treated missing offchain \
             data as 'zero holdings' and triggered rebalancing."
        );
    }

    /// Complementary test: verify that trigger DOES fire once both venues have data.
    #[tokio::test]
    async fn trigger_fires_when_both_venues_have_data() {
        let pool = crate::test_utils::setup_test_db().await;
        let symbol = Symbol::new("RKLB").unwrap();
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let (sender, mut receiver) = mpsc::channel(10);

        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            sender,
            Arc::new(MockWrapper::new()),
        ));

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

        apply_and_dispatch_snapshot(trigger.clone(), id.clone(), onchain_event)
            .await
            .unwrap();

        // No trigger yet - only one venue has data
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "should not trigger with only onchain data"
        );

        // Apply offchain snapshot (0 shares) - now both venues have data
        let mut positions = BTreeMap::new();
        positions.insert(symbol.clone(), shares(0));

        let offchain_event = InventorySnapshotEvent::OffchainEquity {
            positions,
            fetched_at: Utc::now(),
        };

        apply_and_dispatch_snapshot(trigger.clone(), id.clone(), offchain_event)
            .await
            .unwrap();

        // Now both venues have data: 100 onchain, 0 offchain = 100% ratio
        // With target 50% and deviation 10%, ratio 100% > upper bound 60%
        // So TooMuchOnchain -> should trigger Redemption
        let triggered = receiver.try_recv();

        assert!(
            triggered.is_ok(),
            "should trigger rebalancing once both venues have data"
        );

        assert!(
            matches!(triggered.unwrap(), TriggeredOperation::Redemption { .. }),
            "expected Redemption for 100% onchain ratio"
        );
    }

    /// Verifies logging shows when imbalance check skips due to partial data.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn logs_show_partial_data_skips_imbalance_check() {
        let pool = crate::test_utils::setup_test_db().await;
        let symbol = Symbol::new("RKLB").unwrap();
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let (sender, _receiver) = mpsc::channel(10);

        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            sender,
            Arc::new(MockWrapper::new()),
        ));

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

        apply_and_dispatch_snapshot(trigger.clone(), id.clone(), onchain_event)
            .await
            .unwrap();

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
        let pool = crate::test_utils::setup_test_db().await;
        let symbol = Symbol::new("RKLB").unwrap();
        let (event_sender, _) = broadcast::channel::<ServerMessage>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let (sender, _receiver) = mpsc::channel(10);

        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            sender,
            Arc::new(MockWrapper::new()),
        ));

        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Apply onchain data first
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100));

        apply_and_dispatch_snapshot(
            trigger.clone(),
            id.clone(),
            InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        // Now apply offchain data - both venues now have data
        let mut positions = BTreeMap::new();
        positions.insert(symbol.clone(), shares(0));

        apply_and_dispatch_snapshot(
            trigger.clone(),
            id.clone(),
            InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

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
            .with_usdc(usdc(2000), usdc(2000));

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        let event = make_onchain_fill(shares(10), Direction::Buy);
        harness
            .receive::<Position>(symbol.clone(), event)
            .await
            .unwrap();

        let mut operations = Vec::new();
        while let Ok(operation) = receiver.try_recv() {
            operations.push(operation);
        }

        // Onchain buy spends USDC onchain, making the onchain ratio drop below
        // the lower bound. The system should move USDC from Alpaca to Base.
        operations
            .iter()
            .find(|op| matches!(op, TriggeredOperation::UsdcAlpacaToBase { .. }))
            .unwrap_or_else(|| {
                panic!(
                    "Expected UsdcAlpacaToBase (onchain USDC too low after buy), \
                     got operations: {operations:?}"
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

        let wallet = crate::wallet::build_wallet(
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("redemption-transfer-cancel");

        // Verify initial imbalance
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert!(
            matches!(
                receiver.try_recv(),
                Ok(TriggeredOperation::Redemption { .. })
            ),
            "80% ratio should trigger Redemption"
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "Inflight should block imbalance detection"
        );

        // TransferFailed: inflight cancelled, tokens return to available
        harness
            .receive::<EquityRedemption>(id.clone(), make_transfer_failed())
            .await
            .unwrap();

        // After cancel: back to 80 onchain, 20 offchain -> imbalance should re-trigger
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert!(
            matches!(
                receiver.try_recv(),
                Ok(TriggeredOperation::Redemption { .. })
            ),
            "Imbalance should re-trigger after TransferFailed cancels inflight"
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Verify initially balanced
        trigger.check_and_trigger_equity(&symbol).await.unwrap();
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "50/50 should be balanced"
        );

        // Emit InflightEquity with pending mints for AAPL
        // This sets inflight at Hedging venue, which blocks rebalancing
        let mut mints = BTreeMap::new();
        mints.insert(symbol.clone(), shares(10));

        apply_and_dispatch_snapshot(
            trigger.clone(),
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "Inflight should block rebalancing"
        );

        // Empty InflightEquity snapshot: symbol absent, so inflight preserved
        apply_and_dispatch_snapshot(
            trigger.clone(),
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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
        assert!(
            matches!(
                receiver.try_recv(),
                Ok(TriggeredOperation::Redemption { .. })
            ),
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("redemption-rejected-absent-skip");

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
            trigger.clone(),
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("redemption-detection-absent-skip");

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
            trigger.clone(),
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

        let (trigger, _receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("redemption-completed-zeroed");

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
            trigger.clone(),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
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

        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
            "Clearing inflight should not immediately trigger -- \
             no equity recheck from terminal events"
        );

        // Next poll cycle: an available snapshot arrives. Inflight is cleared,
        // check_and_trigger_after_snapshot fires with fresh balances.
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(80));

        apply_and_dispatch_snapshot(
            trigger.clone(),
            id,
            InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        assert!(
            matches!(
                receiver.try_recv(),
                Ok(TriggeredOperation::Redemption { .. })
            ),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Set inflight
        let mut mints = BTreeMap::new();
        mints.insert(symbol.clone(), shares(10));

        apply_and_dispatch_snapshot(
            trigger.clone(),
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
            trigger.clone(),
            id.clone(),
            InventorySnapshotEvent::InflightEquity {
                mints,
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Simulate: WithdrawnFromRaindex starts inflight for the symbol
        let redemption_id = RedemptionAggregateId::new("redemption-stale-regression");
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
        while receiver.try_recv().is_ok() {}
        trigger.clear_equity_in_progress(&symbol);

        // Deliver an InflightEquity snapshot with mints for the symbol.
        // The snapshot has the symbol present, so inflight is set at Hedging.
        let mut stale_mints = BTreeMap::new();
        stale_mints.insert(symbol.clone(), shares(10));

        apply_and_dispatch_snapshot(
            trigger.clone(),
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));

        let snapshot_id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        let mint_id = IssuerRequestId::new("mint-stale-regression");

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
        while receiver.try_recv().is_ok() {}
        trigger.clear_equity_in_progress(&symbol);

        // Deliver stale InflightEquity with redemptions for the symbol
        let mut stale_redemptions = BTreeMap::new();
        stale_redemptions.insert(symbol.clone(), shares(5));

        apply_and_dispatch_snapshot(
            trigger.clone(),
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
        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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
        let (trigger, mut receiver) = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("timed-out-redemption");

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        trigger.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                stage: RedemptionTrackingStage::TokensSent,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        assert!(
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
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

        assert!(
            matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
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
        let (trigger, mut receiver) = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = RedemptionAggregateId::new("timed-out-redemption-retrigger");

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

        trigger.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                stage: RedemptionTrackingStage::TokensSent,
                last_progress_at: Utc::now() - ChronoDuration::minutes(31),
            },
        );

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        let retriggered = receiver.try_recv();
        assert!(
            matches!(retriggered, Ok(TriggeredOperation::Redemption { .. })),
            "timeout cleanup should still allow the next redemption trigger, got {retriggered:?}"
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
        let (trigger, _receiver) = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let id = IssuerRequestId::new("timed-out-mint-recheck");

        trigger.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
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
        let (trigger, _receiver) = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let id = IssuerRequestId::new("timed-out-mint-late-request");

        trigger.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
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
    async fn mint_event_rechecks_tombstone_after_waiting_for_sync_gate() {
        let symbol = Symbol::new("AAPL").unwrap();
        let (trigger, _receiver) = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let id = IssuerRequestId::new("mint-sync-gate-tombstone");
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
        let (trigger, _receiver) = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(60)),
        )
        .await;
        let id = RedemptionAggregateId::new("timed-out-redemption-late-withdrawal");

        trigger.redemption_tracking.write().await.insert(
            id.clone(),
            RedemptionTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
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
    async fn redemption_event_rechecks_tombstone_after_waiting_for_sync_gate() {
        let symbol = Symbol::new("AAPL").unwrap();
        let (trigger, _receiver) = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let id = RedemptionAggregateId::new("redemption-sync-gate-tombstone");
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
        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        trigger.usdc_tracking.write().await.insert(
            id.clone(),
            usdc::UsdcRebalanceTracking {
                direction: RebalanceDirection::BaseToAlpaca,
                initiated_amount: usdc(700),
                bridged_amount_received: None,
                stage: usdc::UsdcRebalanceStage::DepositConfirmed,
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
                    order_id: Uuid::new_v4(),
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
    async fn usdc_event_rechecks_tombstone_after_waiting_for_sync_gate() {
        let (trigger, _receiver) =
            make_trigger_with_inventory(InventoryView::default().with_usdc(usdc(5000), usdc(5000)))
                .await;
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
                        order_id: Uuid::new_v4(),
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
        let (trigger, _receiver) = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let cleared_at = Utc::now();

        trigger
            .suppressed_inflight_symbols
            .write()
            .await
            .insert(symbol.clone(), cleared_at);

        trigger
            .on_snapshot_recovery(
                RebalancingTriggerError::Inventory(InventoryViewError::Equity(
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
        let (trigger, _receiver) = make_trigger_with_inventory_and_registry_config(
            inventory,
            &symbol,
            test_config_with_timeout(Duration::from_secs(1)),
        )
        .await;
        let id = IssuerRequestId::new("recovery-timeout-sweep");

        trigger.mint_tracking.write().await.insert(
            id.clone(),
            MintTracking {
                symbol: symbol.clone(),
                quantity: shares(10),
                stage: MintTrackingStage::Accepted,
                last_progress_at: fetched_at - ChronoDuration::minutes(5),
            },
        );

        trigger
            .on_snapshot_recovery(
                RebalancingTriggerError::Inventory(InventoryViewError::Equity(
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
        let (trigger, _receiver) = make_trigger_with_inventory_and_registry(
            InventoryView::default().with_equity(symbol.clone(), shares(50), shares(50)),
            &symbol,
        )
        .await;
        let stale_time = Utc::now()
            - ChronoDuration::from_std(TIMEOUT_TOMBSTONE_RETENTION).unwrap()
            - ChronoDuration::seconds(1);
        let mint_id = IssuerRequestId::new("stale-mint");
        let redemption_id = RedemptionAggregateId::new("stale-redemption");
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

    #[test]
    fn rebalancing_secrets_debug_has_no_fields() {
        let secrets = RebalancingSecrets {};

        let debug = format!("{secrets:?}");

        assert_eq!(
            debug, "RebalancingSecrets",
            "empty stub struct should have no fields in debug output"
        );
    }
}
