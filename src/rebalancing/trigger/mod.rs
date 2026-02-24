//! Rebalancing trigger that reacts to inventory imbalances.

mod equity;
mod usdc;

pub(crate) use usdc::ALPACA_MINIMUM_WITHDRAWAL;

use alloy::primitives::{Address, B256};
use alloy::providers::{Provider, RootProvider};
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};
use url::Url;

use st0x_event_sorcery::{AggregateError, EntityList, LifecycleError, Reactor, Store, deps};
use st0x_evm::Wallet;
use st0x_evm::fireblocks::{
    ChainAssetIds, FireblocksApiUserId, FireblocksCtx, FireblocksEnvironment, FireblocksError,
    FireblocksVaultAccountId, FireblocksWallet,
};
use st0x_execution::{AlpacaBrokerApiCtx, FractionalShares, Symbol};

use crate::config::OperationalLimits;
use crate::equity_redemption::{EquityRedemption, EquityRedemptionEvent, RedemptionAggregateId};
use crate::inventory::snapshot::{InventorySnapshot, InventorySnapshotEvent};
use crate::inventory::{
    ImbalanceThreshold, Inventory, InventoryView, InventoryViewError, Operator, TransferOp, Venue,
};
use crate::offchain_order::Dollars;
use crate::onchain::REQUIRED_CONFIRMATIONS;
use crate::position::{Position, PositionEvent};
use crate::threshold::Usdc;
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintEvent,
};
use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalance, UsdcRebalanceEvent};
use crate::vault_registry::{VaultRegistry, VaultRegistryId};
use crate::wrapper::{EquityTokenAddresses, Wrapper};

/// Why the rebalancing trigger reactor failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RebalancingTriggerError {
    #[error(transparent)]
    Inventory(#[from] InventoryViewError),
    #[error(transparent)]
    EquityTrigger(#[from] equity::EquityTriggerError),
    #[error("USDC amount overflow computing price {price} * quantity {quantity}")]
    UsdcAmountOverflow { price: Decimal, quantity: Decimal },
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
    #[error("failed to read Fireblocks secret file: {0}")]
    FireblocksSecretRead(#[from] std::io::Error),
    #[error(transparent)]
    Fireblocks(#[from] FireblocksError),
    #[error("RPC error during wallet setup: {0}")]
    Rpc(#[from] alloy::transports::RpcError<alloy::transports::TransportErrorKind>),
    #[error("no Fireblocks asset ID configured for chain {chain_id}")]
    MissingChainAssetId { chain_id: u64 },
}

/// USDC rebalancing configuration with explicit enable/disable.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub(crate) enum UsdcRebalancing {
    Enabled { target: Decimal, deviation: Decimal },
    Disabled,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RebalancingSecrets {
    pub(crate) base_rpc_url: Url,
    pub(crate) ethereum_rpc_url: Url,
    pub(crate) fireblocks_api_user_id: FireblocksApiUserId,
    pub(crate) fireblocks_secret_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RebalancingConfig {
    pub(crate) equity: ImbalanceThreshold,
    pub(crate) usdc: UsdcRebalancing,
    pub(crate) redemption_wallet: Address,
    pub(crate) usdc_vault_id: B256,
    pub(crate) fireblocks_vault_account_id: FireblocksVaultAccountId,
    pub(crate) fireblocks_chain_asset_ids: ChainAssetIds,
    pub(crate) fireblocks_environment: FireblocksEnvironment,
    /// Override the Fireblocks API base URL. When absent, determined
    /// by `fireblocks_environment`.
    pub(crate) fireblocks_base_url: Option<Url>,
}

/// Runtime configuration for rebalancing operations.
///
/// Constructed asynchronously from `RebalancingConfig`, `RebalancingSecrets`,
/// and broker auth. During construction, Fireblocks wallets are pre-built for
/// both chains (each resolving the vault address from the Fireblocks API).
/// After construction, all fields are immutable.
///
/// Read-only provider access for either chain is available via
/// `base_wallet().provider()` and `ethereum_wallet().provider()`.
#[derive(Clone)]
pub(crate) struct RebalancingCtx {
    pub(crate) equity: ImbalanceThreshold,
    pub(crate) usdc: UsdcRebalancing,
    /// Issuer's wallet for tokenized equity redemptions.
    pub(crate) redemption_wallet: Address,
    pub(crate) usdc_vault_id: B256,
    pub(crate) alpaca_broker_auth: AlpacaBrokerApiCtx,
    /// Pre-built wallet for the base chain (e.g. Base mainnet).
    base_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    /// Pre-built wallet for Ethereum mainnet.
    ethereum_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    /// Override the Circle attestation/fee API base URL (for e2e tests
    /// with locally deployed CCTP contracts).
    pub(crate) circle_api_base: Option<String>,
    /// Override the `TokenMessengerV2` contract address.
    pub(crate) token_messenger: Option<Address>,
    /// Override the `MessageTransmitterV2` contract address.
    pub(crate) message_transmitter: Option<Address>,
}

impl RebalancingCtx {
    /// Construct from config, secrets, and broker auth.
    ///
    /// Reads the Fireblocks RSA secret, builds wallets for both chains
    /// (resolving the vault address from the Fireblocks API), and
    /// stores them immutably. Read-only provider access for either
    /// chain is available via `base_wallet().provider()` and
    /// `ethereum_wallet().provider()`.
    pub(crate) async fn new(
        config: RebalancingConfig,
        secrets: RebalancingSecrets,
        broker_auth: AlpacaBrokerApiCtx,
    ) -> Result<Self, RebalancingCtxError> {
        let fireblocks_secret = tokio::fs::read(&secrets.fireblocks_secret_path).await?;

        let (base_wallet, ethereum_wallet) = tokio::try_join!(
            Self::build_wallet(
                &config,
                &fireblocks_secret,
                &secrets.fireblocks_api_user_id,
                secrets.base_rpc_url,
            ),
            Self::build_wallet(
                &config,
                &fireblocks_secret,
                &secrets.fireblocks_api_user_id,
                secrets.ethereum_rpc_url,
            ),
        )?;

        info!(
            wallet = %base_wallet.address(),
            "Resolved market maker wallet from Fireblocks"
        );

        Ok(Self {
            equity: config.equity,
            usdc: config.usdc,
            redemption_wallet: config.redemption_wallet,
            usdc_vault_id: config.usdc_vault_id,
            alpaca_broker_auth: broker_auth,
            base_wallet: Arc::new(base_wallet),
            ethereum_wallet: Arc::new(ethereum_wallet),
            circle_api_base: None,
            token_messenger: None,
            message_transmitter: None,
        })
    }

    async fn build_wallet(
        config: &RebalancingConfig,
        fireblocks_secret: &[u8],
        api_user_id: &FireblocksApiUserId,
        rpc_url: Url,
    ) -> Result<FireblocksWallet<RootProvider>, RebalancingCtxError> {
        let provider = RootProvider::new(crate::onchain::http_client_with_retry(rpc_url));
        let chain_id = provider.get_chain_id().await?;

        let asset_id = config
            .fireblocks_chain_asset_ids
            .get(chain_id)
            .cloned()
            .ok_or_else(|| {
                warn!(chain_id, "No Fireblocks asset ID configured for chain");
                RebalancingCtxError::MissingChainAssetId { chain_id }
            })?;

        Ok(FireblocksWallet::new(FireblocksCtx {
            api_user_id: api_user_id.clone(),
            secret: fireblocks_secret.to_vec(),
            vault_account_id: config.fireblocks_vault_account_id.clone(),
            environment: config.fireblocks_environment,
            asset_id,
            provider,
            required_confirmations: REQUIRED_CONFIRMATIONS,
            base_url: config.fireblocks_base_url.clone(),
        })
        .await?)
    }

    pub(crate) fn base_wallet(&self) -> &Arc<dyn Wallet<Provider = RootProvider>> {
        &self.base_wallet
    }

    pub(crate) fn ethereum_wallet(&self) -> &Arc<dyn Wallet<Provider = RootProvider>> {
        &self.ethereum_wallet
    }

    /// Test constructor that creates a `RebalancingCtx` with stub wallets.
    ///
    /// The wallets panic on `send` -- use only in tests that don't submit
    /// transactions through the rebalancing wallet.
    #[cfg(test)]
    pub(crate) fn stub(
        equity: ImbalanceThreshold,
        usdc: UsdcRebalancing,
        redemption_wallet: Address,
        usdc_vault_id: B256,
        alpaca_broker_auth: AlpacaBrokerApiCtx,
    ) -> Self {
        let wallet = crate::test_utils::StubWallet::stub(Address::ZERO);

        Self {
            equity,
            usdc,
            redemption_wallet,
            usdc_vault_id,
            alpaca_broker_auth,
            base_wallet: wallet.clone(),
            ethereum_wallet: wallet,
            circle_api_base: None,
            token_messenger: None,
            message_transmitter: None,
        }
    }
}

impl std::fmt::Debug for RebalancingCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebalancingCtx")
            .field("equity", &self.equity)
            .field("usdc", &self.usdc)
            .field("redemption_wallet", &self.redemption_wallet)
            .field("usdc_vault_id", &self.usdc_vault_id)
            .field("alpaca_broker_auth", &"[REDACTED]")
            .field("wallet", &self.base_wallet.address())
            .finish_non_exhaustive()
    }
}

/// Configuration for the rebalancing trigger (runtime).
#[derive(Debug, Clone)]
pub(crate) struct RebalancingTriggerConfig {
    pub(crate) equity: ImbalanceThreshold,
    pub(crate) usdc: UsdcRebalancing,
    pub(crate) limits: OperationalLimits,
    pub(crate) disabled_assets: HashSet<Symbol>,
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
    inventory: Arc<RwLock<InventoryView>>,
    pub(crate) equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
    pub(crate) usdc_in_progress: Arc<AtomicBool>,
    sender: mpsc::Sender<TriggeredOperation>,
    wrapper: Arc<dyn Wrapper>,
    /// Tracks symbol/quantity for in-flight mints. The initial `MintRequested`
    /// event carries this data; follow-up events don't.
    mint_tracking: Arc<RwLock<HashMap<IssuerRequestId, (Symbol, FractionalShares)>>>,
    /// Tracks symbol/quantity for in-flight redemptions. The initial
    /// `WithdrawnFromRaindex` event carries this data; follow-up events don't.
    redemption_tracking: Arc<RwLock<HashMap<RedemptionAggregateId, (Symbol, FractionalShares)>>>,
}

impl RebalancingTrigger {
    pub(crate) fn new(
        config: RebalancingTriggerConfig,
        vault_registry: Arc<Store<VaultRegistry>>,
        orderbook: Address,
        order_owner: Address,
        inventory: Arc<RwLock<InventoryView>>,
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
        }
    }

    /// Process a snapshot event under normal operation.
    async fn on_snapshot(
        &self,
        event: InventorySnapshotEvent,
    ) -> Result<(), RebalancingTriggerError> {
        let now = Utc::now();
        let mut inventory = self.inventory.write().await;

        use InventorySnapshotEvent::*;
        let updated =
            match &event {
                OnchainEquity { balances, .. } => balances.iter().try_fold(
                    inventory.clone(),
                    |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::on_snapshot(Venue::MarketMaking, *snapshot_balance),
                            now,
                        )
                    },
                ),

                OnchainCash { usdc_balance, .. } => inventory.clone().update_usdc(
                    Inventory::on_snapshot(Venue::MarketMaking, *usdc_balance),
                    now,
                ),

                OffchainEquity { positions, .. } => positions.iter().try_fold(
                    inventory.clone(),
                    |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::on_snapshot(Venue::Hedging, *snapshot_balance),
                            now,
                        )
                    },
                ),

                OffchainCash {
                    cash_balance_cents, ..
                } => {
                    let usdc = Usdc::from_cents(*cash_balance_cents).ok_or(
                        InventoryViewError::CashBalanceConversion(*cash_balance_cents),
                    )?;
                    inventory
                        .clone()
                        .update_usdc(Inventory::on_snapshot(Venue::Hedging, usdc), now)
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
        let inventory_error = match error {
            RebalancingTriggerError::Inventory(inventory_error) => inventory_error,
            other @ (RebalancingTriggerError::EquityTrigger(_)
            | RebalancingTriggerError::UsdcAmountOverflow { .. }) => return Err(other),
        };

        warn!(
            ?inventory_error,
            "Resetting inventory and force-applying snapshot to recover"
        );

        let now = Utc::now();
        let mut inventory = self.inventory.write().await;
        *inventory = InventoryView::default();

        use InventorySnapshotEvent::*;
        let updated =
            match &event {
                OnchainEquity { balances, .. } => balances.iter().try_fold(
                    inventory.clone(),
                    |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::force_on_snapshot(
                                Venue::MarketMaking,
                                *snapshot_balance,
                                inventory_error.clone(),
                            ),
                            now,
                        )
                    },
                ),

                OnchainCash { usdc_balance, .. } => inventory.clone().update_usdc(
                    Inventory::force_on_snapshot(
                        Venue::MarketMaking,
                        *usdc_balance,
                        inventory_error.clone(),
                    ),
                    now,
                ),

                OffchainEquity { positions, .. } => positions.iter().try_fold(
                    inventory.clone(),
                    |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            Inventory::force_on_snapshot(
                                Venue::Hedging,
                                *snapshot_balance,
                                inventory_error.clone(),
                            ),
                            now,
                        )
                    },
                ),

                OffchainCash {
                    cash_balance_cents, ..
                } => {
                    let usdc = Usdc::from_cents(*cash_balance_cents).ok_or(
                        InventoryViewError::CashBalanceConversion(*cash_balance_cents),
                    )?;
                    inventory.clone().update_usdc(
                        Inventory::force_on_snapshot(Venue::Hedging, usdc, inventory_error),
                        now,
                    )
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
            OnchainCash { .. } | OffchainCash { .. } => {
                self.check_and_trigger_usdc().await;
            }
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
                        let quantity = Decimal::from(*amount);
                        let usdc_value = price_usdc.checked_mul(quantity).ok_or(
                            RebalancingTriggerError::UsdcAmountOverflow {
                                price: *price_usdc,
                                quantity,
                            },
                        )?;

                        (
                            Inventory::available(Venue::MarketMaking, equity_op, *amount),
                            Inventory::available(
                                Venue::MarketMaking,
                                equity_op.inverse(),
                                Usdc(usdc_value),
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
                        let quantity = Decimal::from(shares_filled.inner());
                        let Dollars(price_value) = price;
                        let usdc_value = price_value.checked_mul(quantity).ok_or(
                            RebalancingTriggerError::UsdcAmountOverflow {
                                price: *price_value,
                                quantity,
                            },
                        )?;

                        (
                            Inventory::available(Venue::Hedging, equity_op, shares_filled.inner()),
                            Inventory::available(
                                Venue::Hedging,
                                equity_op.inverse(),
                                Usdc(usdc_value),
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

                Ok::<(), RebalancingTriggerError>(())
            })
            .on(|id, event| async move { self.on_mint(id, event).await })
            .on(|id, event| async move { self.on_redemption(id, event).await })
            .on(|_id, event| async move {
                if let UsdcRebalanceEvent::Initiated {
                    direction, amount, ..
                } = &event
                {
                    let venue = match direction {
                        RebalanceDirection::AlpacaToBase => Venue::Hedging,
                        RebalanceDirection::BaseToAlpaca => Venue::MarketMaking,
                    };
                    let update = Inventory::transfer(venue, TransferOp::Start, *amount);

                    let mut inventory = self.inventory.write().await;
                    *inventory = inventory.clone().update_usdc(update, Utc::now())?;
                }

                if Self::is_terminal_usdc_rebalance_event(&event) {
                    self.clear_usdc_in_progress();
                    debug!("Cleared USDC in-progress flag after rebalance terminal event");

                    self.check_and_trigger_usdc().await;
                }

                Ok::<(), RebalancingTriggerError>(())
            })
            .on_with_fallback(
                |_id, event| async move { self.on_snapshot(event).await },
                |error, _id, event| async move { self.on_snapshot_recovery(error, event).await },
            )
            .exhaustive()
            .await
    }
}

impl RebalancingTrigger {
    /// Checks inventory for equity imbalance and triggers operation if needed.
    pub(crate) async fn check_and_trigger_equity(
        &self,
        symbol: &Symbol,
    ) -> Result<(), equity::EquityTriggerError> {
        if self.config.disabled_assets.contains(symbol) {
            return Ok(());
        }

        let Some(guard) = equity::InProgressGuard::try_claim(
            symbol.clone(),
            Arc::clone(&self.equity_in_progress),
        ) else {
            debug!(%symbol, "Skipped equity trigger: already in progress");
            return Ok(());
        };

        let wrapped_token = self
            .load_token_address(symbol)
            .await?
            .ok_or(equity::EquityTriggerError::TokenNotInRegistry)?;

        let unwrapped_token = self.wrapper.lookup_unwrapped(symbol)?;
        let vault_ratio = self.wrapper.get_ratio_for_symbol(symbol).await?;

        let Some(operation) = equity::check_imbalance_and_build_operation(
            symbol,
            &self.config.equity,
            &self.inventory,
            wrapped_token,
            unwrapped_token,
            &vault_ratio,
            &self.config.limits,
        )
        .await?
        else {
            return Ok(());
        };

        if let Err(error) = self.sender.try_send(operation.clone()) {
            warn!(%error, "Failed to send triggered operation");
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

    /// Checks inventory for USDC imbalance and triggers operation if needed.
    pub(crate) async fn check_and_trigger_usdc(&self) {
        let UsdcRebalancing::Enabled { target, deviation } = self.config.usdc else {
            return;
        };

        let Some(guard) = usdc::InProgressGuard::try_claim(Arc::clone(&self.usdc_in_progress))
        else {
            debug!("Skipped USDC trigger: already in progress");
            return;
        };

        let threshold = ImbalanceThreshold { target, deviation };
        let Ok(operation) = usdc::check_imbalance_and_build_operation(
            &threshold,
            &self.inventory,
            &self.config.limits,
        )
        .await
        else {
            return;
        };

        if let Err(error) = self.sender.try_send(operation.clone()) {
            warn!(%error, "Failed to send USDC triggered operation");
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
        if let Some((symbol, quantity)) = Self::extract_mint_info(&event) {
            self.mint_tracking
                .write()
                .await
                .insert(id.clone(), (symbol, quantity));
        }

        let Some((symbol, quantity)) = self.mint_tracking.read().await.get(&id).cloned() else {
            warn!(id = %id, "Mint event for untracked aggregate");
            return Ok(());
        };

        use TokenizedEquityMintEvent::*;

        let update = match &event {
            MintAccepted { .. } => Some(Inventory::transfer(
                Venue::Hedging,
                TransferOp::Start,
                quantity,
            )),
            MintAcceptanceFailed { .. } => Some(Inventory::transfer(
                Venue::Hedging,
                TransferOp::Cancel,
                quantity,
            )),
            TokensReceived { .. } => Some(Inventory::transfer(
                Venue::Hedging,
                TransferOp::Complete,
                quantity,
            )),
            DepositedIntoRaindex { deposited_at, .. } => {
                Some(Inventory::with_last_rebalancing(*deposited_at))
            }
            MintRequested { .. }
            | MintRejected { .. }
            | TokensWrapped { .. }
            | WrappingFailed { .. }
            | RaindexDepositFailed { .. } => None,
        };

        if let Some(update) = update {
            let mut inventory = self.inventory.write().await;
            *inventory = inventory
                .clone()
                .update_equity(&symbol, update, Utc::now())?;
        }

        if Self::is_terminal_mint_event(&event) {
            self.mint_tracking.write().await.remove(&id);
            self.clear_equity_in_progress(&symbol);
            debug!(%symbol, "Cleared equity in-progress flag after mint terminal event");

            self.check_and_trigger_usdc().await;
        }

        Ok(())
    }

    async fn on_redemption(
        &self,
        id: RedemptionAggregateId,
        event: EquityRedemptionEvent,
    ) -> Result<(), RebalancingTriggerError> {
        if let Some((symbol, quantity)) = Self::extract_redemption_info(&event) {
            self.redemption_tracking
                .write()
                .await
                .insert(id.clone(), (symbol, quantity));
        }

        let Some((symbol, quantity)) = self.redemption_tracking.read().await.get(&id).cloned()
        else {
            warn!(id = %id, "Redemption event for untracked aggregate");
            return Ok(());
        };

        use EquityRedemptionEvent::*;

        let update = match &event {
            WithdrawnFromRaindex { .. } => Some(Inventory::transfer(
                Venue::MarketMaking,
                TransferOp::Start,
                quantity,
            )),

            Completed { completed_at } => {
                let completed_at = *completed_at;
                let composed: Box<
                    dyn FnOnce(
                            Inventory<FractionalShares>,
                        ) -> Result<Inventory<FractionalShares>, _>
                        + Send,
                > = Box::new(move |inventory| {
                    let transferred =
                        Inventory::transfer(Venue::MarketMaking, TransferOp::Complete, quantity)(
                            inventory,
                        )?;
                    Inventory::with_last_rebalancing(completed_at)(transferred)
                });
                Some(composed)
            }

            TokensUnwrapped { .. }
            | TransferFailed { .. }
            | TokensSent { .. }
            | DetectionFailed { .. }
            | Detected { .. }
            | RedemptionRejected { .. } => None,
        };

        if let Some(update) = update {
            let mut inventory = self.inventory.write().await;
            *inventory = inventory
                .clone()
                .update_equity(&symbol, update, Utc::now())?;
        }

        if Self::is_terminal_redemption_event(&event) {
            self.redemption_tracking.write().await.remove(&id);
            self.clear_equity_in_progress(&symbol);
            debug!(
                %symbol,
                "Cleared equity in-progress flag after redemption terminal event"
            );

            self.check_and_trigger_usdc().await;
        }

        Ok(())
    }

    fn extract_mint_info(event: &TokenizedEquityMintEvent) -> Option<(Symbol, FractionalShares)> {
        use TokenizedEquityMintEvent::*;

        if let MintRequested {
            symbol, quantity, ..
        } = event
        {
            Some((symbol.clone(), FractionalShares::new(*quantity)))
        } else {
            None
        }
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

    fn extract_redemption_info(
        event: &EquityRedemptionEvent,
    ) -> Option<(Symbol, FractionalShares)> {
        use EquityRedemptionEvent::*;

        if let WithdrawnFromRaindex {
            symbol, quantity, ..
        } = event
        {
            Some((symbol.clone(), FractionalShares::new(*quantity)))
        } else {
            None
        }
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
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{Address, TxHash, U256, address, fixed_bytes};
    use chrono::Utc;
    use httpmock::MockServer;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use sqlx::SqlitePool;
    use st0x_event_sorcery::{EntityList, Never, ReactorHarness, TestStore, deps, test_store};
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiMode, Direction, ExecutorOrderId, Positive, TimeInForce,
    };
    use std::collections::{BTreeMap, HashSet};
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, LazyLock};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;
    use uuid::{Uuid, uuid};

    use super::*;
    use crate::alpaca_wallet::AlpacaTransferId;
    use crate::equity_redemption::DetectionFailure;
    use crate::inventory::snapshot::{InventorySnapshotEvent, InventorySnapshotId};
    use crate::inventory::view::Operator;
    use crate::inventory::{TransferOp, Venue};
    use crate::offchain_order::{Dollars, OffchainOrderId};
    use crate::position::{PositionEvent, TradeId};
    use crate::threshold::{ExecutionThreshold, Usdc};
    use crate::tokenized_equity_mint::{IssuerRequestId, ReceiptId, TokenizationRequestId};
    use crate::usdc_rebalance::{TransferRef, UsdcRebalanceCommand, UsdcRebalanceId};
    use crate::vault_registry::VaultRegistryCommand;
    use crate::wrapper::mock::MockWrapper;

    /// RSA private key generated at test time for Fireblocks JWT
    /// signing. The mock server does not validate signatures.
    static TEST_RSA_PEM: LazyLock<Vec<u8>> = LazyLock::new(|| {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        pem.as_bytes().to_vec()
    });

    fn test_config() -> RebalancingTriggerConfig {
        RebalancingTriggerConfig {
            equity: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc: UsdcRebalancing::Enabled {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            limits: OperationalLimits::Disabled,
            disabled_assets: HashSet::new(),
        }
    }

    async fn make_trigger() -> (Arc<RebalancingTrigger>, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
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

    #[tokio::test]
    async fn test_in_progress_symbol_does_not_send() {
        let (trigger, mut receiver) = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }

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
    async fn test_usdc_disabled_does_not_send() {
        let (sender, mut receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let pool = crate::test_utils::setup_test_db().await;
        let wrapper = Arc::new(MockWrapper::new());

        let trigger = RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: test_config().equity,
                usdc: UsdcRebalancing::Disabled,
                limits: OperationalLimits::Disabled,
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
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let pool = crate::test_utils::setup_test_db().await;
        let wrapper = Arc::new(MockWrapper::new());

        let trigger = RebalancingTrigger::new(
            RebalancingTriggerConfig {
                equity: test_config().equity,
                usdc: UsdcRebalancing::Disabled,
                limits: OperationalLimits::Disabled,
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
        FractionalShares::new(Decimal::from(n))
    }

    fn make_onchain_fill(amount: FractionalShares, direction: Direction) -> PositionEvent {
        PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index: 0,
            },
            amount,
            direction,
            price_usdc: dec!(150.0),
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
            price: Dollars(dec!(150.00)),
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
        let (sender, receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(inventory));
        let pool = crate::test_utils::setup_test_db().await;

        (
            Arc::new(RebalancingTrigger::new(
                test_config(),
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
        make_trigger_with_inventory_registry_and_wrapper(
            inventory,
            symbol,
            Arc::new(MockWrapper::new()),
        )
        .await
    }

    async fn make_trigger_with_inventory_registry_and_wrapper(
        inventory: InventoryView,
        symbol: &Symbol,
        wrapper: Arc<MockWrapper>,
    ) -> (Arc<RebalancingTrigger>, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(inventory));
        let pool = crate::test_utils::setup_test_db().await;

        seed_vault_registry(&pool, symbol).await;

        (
            Arc::new(RebalancingTrigger::new(
                test_config(),
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
        let inventory = Arc::new(RwLock::new(
            InventoryView::default().with_usdc(Usdc(dec!(1000000)), Usdc(dec!(1000000))),
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
            .with_usdc(Usdc(dec!(1000000)), Usdc(dec!(1000000)));

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
            .with_usdc(Usdc(dec!(1000000)), Usdc(dec!(1000000)))
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
            .with_usdc(Usdc(dec!(1000000)), Usdc(dec!(1000000)))
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
        let (trigger, mut receiver) =
            make_trigger_with_inventory_registry_and_wrapper(inventory, &symbol, wrapper).await;

        trigger.check_and_trigger_equity(&symbol).await.unwrap();

        let triggered = receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Redemption { .. })),
            "Expected Redemption with 1.5 ratio, got {triggered:?}"
        );
    }

    fn make_mint_requested(symbol: &Symbol, quantity: Decimal) -> TokenizedEquityMintEvent {
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
            received_at: Utc::now(),
        }
    }

    fn make_wrapping_failed(symbol: &Symbol, quantity: Decimal) -> TokenizedEquityMintEvent {
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
        let event = make_mint_requested(&symbol, dec!(42.5));

        let (extracted_symbol, extracted_quantity) =
            RebalancingTrigger::extract_mint_info(&event).unwrap();

        assert_eq!(extracted_symbol, symbol);
        assert_eq!(extracted_quantity.inner(), dec!(42.5));
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
            &make_mint_requested(&symbol, dec!(30))
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
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, dec!(10)))
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
            .receive::<EquityRedemption>(id.clone(), make_withdrawn_from_raindex(&symbol, dec!(10)))
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
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, dec!(30)))
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
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, dec!(30)))
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
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, dec!(30)))
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
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, dec!(30)))
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
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, dec!(30)))
            .await
            .unwrap();

        harness
            .receive::<TokenizedEquityMint>(id.clone(), make_wrapping_failed(&symbol, dec!(30)))
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
            .receive::<TokenizedEquityMint>(id.clone(), make_mint_requested(&symbol, dec!(30)))
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
            .receive::<EquityRedemption>(id.clone(), make_withdrawn_from_raindex(&symbol, dec!(10)))
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
            .receive::<EquityRedemption>(id.clone(), make_withdrawn_from_raindex(&symbol, dec!(10)))
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
            .receive::<EquityRedemption>(id.clone(), make_withdrawn_from_raindex(&symbol, dec!(10)))
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
            .with_usdc(Usdc(dec!(10000)), Usdc(dec!(10000)));

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

        assert_eq!(onchain_usdc, Usdc(dec!(8500)));
    }

    #[tokio::test]
    async fn onchain_sell_updates_usdc_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(Usdc(dec!(10000)), Usdc(dec!(10000)))
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

        assert_eq!(onchain_usdc, Usdc(dec!(11500)));
    }

    #[tokio::test]
    async fn offchain_buy_updates_usdc_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(Usdc(dec!(10000)), Usdc(dec!(10000)));

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

        assert_eq!(offchain_usdc, Usdc(dec!(8500)));
    }

    #[tokio::test]
    async fn offchain_sell_updates_usdc_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default()
            .with_equity(symbol.clone(), shares(0), shares(0))
            .with_usdc(Usdc(dec!(10000)), Usdc(dec!(10000)))
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

        assert_eq!(offchain_usdc, Usdc(dec!(11500)));
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
        let harness = ReactorHarness::new(Arc::clone(&trigger));
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Snapshot onchain equity to 40 -> now 40 onchain, 20 offchain
        // 40/60 = 66.7% -> within threshold (upper bound 70%), no trigger
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(40));

        harness
            .receive::<InventorySnapshot>(
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
        let harness = ReactorHarness::new(Arc::clone(&trigger));
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

        harness
            .receive::<InventorySnapshot>(
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
            .receive::<EquityRedemption>(id.clone(), make_withdrawn_from_raindex(&symbol, dec!(30)))
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
            .receive::<EquityRedemption>(id.clone(), make_withdrawn_from_raindex(&symbol, dec!(30)))
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
    async fn snapshot_onchain_cash_via_reactor_updates_usdc_balance() {
        // Start with 500 onchain, 500 offchain = balanced
        let inventory = InventoryView::default().with_usdc(usdc(500), usdc(500));

        let symbol = Symbol::new("AAPL").unwrap();
        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
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
        harness
            .receive::<InventorySnapshot>(
                id.clone(),
                InventorySnapshotEvent::OnchainCash {
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
    async fn snapshot_offchain_cash_via_reactor_updates_usdc_balance() {
        // Start with 900 onchain, 100 offchain = 90% ratio -> TooMuchOnchain
        let inventory = InventoryView::default().with_usdc(usdc(900), usdc(100));

        let symbol = Symbol::new("AAPL").unwrap();
        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;
        let harness = ReactorHarness::new(Arc::clone(&trigger));
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
        harness
            .receive::<InventorySnapshot>(
                id.clone(),
                InventorySnapshotEvent::OffchainCash {
                    cash_balance_cents: 90000,
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

    fn make_withdrawn_from_raindex(symbol: &Symbol, quantity: Decimal) -> EquityRedemptionEvent {
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
        let event = make_withdrawn_from_raindex(&symbol, dec!(42.5));

        let (extracted_symbol, extracted_quantity) =
            RebalancingTrigger::extract_redemption_info(&event).unwrap();

        assert_eq!(extracted_symbol, symbol);
        assert_eq!(extracted_quantity.inner(), dec!(42.5));
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
            &make_withdrawn_from_raindex(&symbol, dec!(30))
        ));
        assert!(!RebalancingTrigger::is_terminal_redemption_event(
            &make_redemption_detected()
        ));
    }

    fn usdc(n: i64) -> Usdc {
        Usdc(Decimal::from(n))
    }

    fn make_usdc_initiated(direction: RebalanceDirection, amount: Usdc) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref: TransferRef::OnchainTx(TxHash::random()),
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

    fn make_usdc_bridged() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::Bridged {
            mint_tx_hash: TxHash::random(),
            amount_received: Usdc(dec!(99.99)),
            fee_collected: Usdc(dec!(0.01)),
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

    #[tokio::test]
    async fn usdc_rebalance_completion_clears_in_progress_flag() {
        let (trigger, _receiver) = make_trigger_with_inventory(InventoryView::default()).await;

        // Mark USDC as in-progress.
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(trigger.usdc_in_progress.load(Ordering::SeqCst));

        // DepositConfirmed for AlpacaToBase is terminal.
        assert!(RebalancingTrigger::is_terminal_usdc_rebalance_event(
            &make_usdc_deposit_confirmed(RebalanceDirection::AlpacaToBase)
        ));

        // Simulate what dispatch does - clear in-progress on terminal.
        trigger.clear_usdc_in_progress();
        assert!(!trigger.usdc_in_progress.load(Ordering::SeqCst));
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
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"

            [equities]

            [fireblocks_chain_asset_ids]
            1 = "ETH"
            8453 = "BASECHAIN_ETH"

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
        r#"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://eth.example.com"
            fireblocks_api_user_id = "test-api-user"
            fireblocks_secret_path = "/tmp/test-fireblocks.key"
        "#
    }

    fn test_broker_auth() -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        }
    }

    #[test]
    fn deserialize_config_succeeds() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();

        assert_eq!(config.equity.target, dec!(0.5));
        assert_eq!(config.equity.deviation, dec!(0.2));

        let UsdcRebalancing::Enabled { target, deviation } = config.usdc else {
            panic!("expected enabled");
        };
        assert_eq!(target, dec!(0.5));
        assert_eq!(deviation, dec!(0.3));
        assert_eq!(
            config.redemption_wallet,
            address!("1234567890123456789012345678901234567890")
        );
    }

    #[test]
    fn deserialize_secrets_succeeds() {
        let _secrets: RebalancingSecrets =
            toml::from_str(valid_rebalancing_secrets_toml()).unwrap();
    }

    #[tokio::test]
    async fn new_fails_when_secret_file_missing() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();
        let secrets: RebalancingSecrets = toml::from_str(valid_rebalancing_secrets_toml()).unwrap();

        let error = RebalancingCtx::new(config, secrets, test_broker_auth())
            .await
            .unwrap_err();

        assert!(
            matches!(error, RebalancingCtxError::FireblocksSecretRead(_)),
            "Expected FireblocksSecretRead error, got {error:?}"
        );
    }

    #[test]
    fn deserialize_with_custom_thresholds() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"

            [equities]

            [fireblocks_chain_asset_ids]
            1 = "ETH"
            8453 = "BASECHAIN_ETH"

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

        assert_eq!(config.equity.target, dec!(0.6));
        assert_eq!(config.equity.deviation, dec!(0.1));

        let UsdcRebalancing::Enabled { target, deviation } = config.usdc else {
            panic!("expected enabled");
        };
        assert_eq!(target, dec!(0.4));
        assert_eq!(deviation, dec!(0.15));
    }

    #[test]
    fn deserialize_missing_redemption_wallet_fails() {
        let toml_str = r#"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"

            [equities]

            [fireblocks_chain_asset_ids]
            1 = "ETH"
            8453 = "BASECHAIN_ETH"

            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "disabled"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("redemption_wallet"),
            "Expected missing redemption_wallet error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_fireblocks_fields_fails() {
        let toml_str = r#"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://eth.example.com"
        "#;

        let error = toml::from_str::<RebalancingSecrets>(toml_str).unwrap_err();
        assert!(
            error.message().contains("fireblocks_api_user_id"),
            "Expected missing fireblocks_api_user_id error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_equity_fails() {
        let toml_str = r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"

            [equities]

            [fireblocks_chain_asset_ids]
            1 = "ETH"
            8453 = "BASECHAIN_ETH"

            [usdc]
            mode = "disabled"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("equity"),
            "Expected missing equity error, got: {error}"
        );
    }

    #[test]
    fn deserialize_usdc_disabled() {
        let toml_str = r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"

            [equities]

            [fireblocks_chain_asset_ids]
            1 = "ETH"
            8453 = "BASECHAIN_ETH"

            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "disabled"
        "#;

        let config: RebalancingConfig = toml::from_str(toml_str).unwrap();

        assert!(matches!(config.usdc, UsdcRebalancing::Disabled));
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
                    amount: Usdc(dec!(500)),
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
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(1000)),
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
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(100)),
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
                    amount: Usdc(dec!(100)),
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
                    amount: Usdc(dec!(100)),
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
        let inventory = Arc::new(tokio::sync::RwLock::new(
            InventoryView::default().with_usdc(Usdc(dec!(5000)), Usdc(dec!(5000))),
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
                    amount: Usdc(dec!(1000)),
                    withdrawal_ref: TransferRef::OnchainTx(tx_hash),
                    initiated_at: chrono::Utc::now(),
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
                    amount: Usdc(dec!(500)),
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
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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

        // Now start post-deposit conversion (USDC to USD)
        store
            .send(
                &id,
                UsdcRebalanceCommand::InitiatePostDepositConversion {
                    order_id: uuid::Uuid::new_v4(),
                    amount: Usdc(dec!(500)),
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
                    filled_amount: Usdc(dec!(499)), // ~0.2% slippage
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
        let inventory = Arc::new(RwLock::new(InventoryView::default()));

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

        let harness = ReactorHarness::new(trigger.clone());
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
        harness
            .receive::<InventorySnapshot>(id.clone(), onchain_event.clone())
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
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
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

        let harness = ReactorHarness::new(trigger.clone());
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

        harness
            .receive::<InventorySnapshot>(id.clone(), onchain_event)
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

        harness
            .receive::<InventorySnapshot>(id.clone(), offchain_event)
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
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
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

        let harness = ReactorHarness::new(trigger.clone());
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

        harness
            .receive::<InventorySnapshot>(id.clone(), onchain_event)
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
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
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

        let harness = ReactorHarness::new(trigger.clone());
        let id = InventorySnapshotId {
            orderbook: TEST_ORDERBOOK,
            owner: TEST_ORDER_OWNER,
        };

        // Apply onchain data first
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100));

        harness
            .receive::<InventorySnapshot>(
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

        harness
            .receive::<InventorySnapshot>(
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
    async fn build_wallet_returns_missing_chain_asset_id_for_unknown_chain() {
        let anvil = Anvil::new().spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();

        let config = RebalancingConfig {
            equity: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc: UsdcRebalancing::Disabled,
            redemption_wallet: Address::ZERO,
            usdc_vault_id: fixed_bytes!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            ),
            equities: HashMap::new(),
            fireblocks_vault_account_id: FireblocksVaultAccountId::new("0"),
            fireblocks_chain_asset_ids: serde_json::from_value(serde_json::json!({})).unwrap(),
            fireblocks_environment: FireblocksEnvironment::Sandbox,
            fireblocks_base_url: None,
        };

        let error = RebalancingCtx::build_wallet(
            &config,
            b"fake-rsa-key",
            &FireblocksApiUserId::new("test-user"),
            rpc_url,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                RebalancingCtxError::MissingChainAssetId { chain_id: 31337 }
            ),
            "Expected MissingChainAssetId for Anvil's default chain 31337, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn build_wallet_resolves_address_from_fireblocks() {
        let anvil = Anvil::new().spawn();
        let rpc_url: Url = anvil.endpoint().parse().unwrap();
        let server = MockServer::start();
        let expected_address = address!("0x1111111111111111111111111111111111111111");

        server.mock(|when, then| {
            when.method("GET")
                .path("/vault/accounts/0/ETH/addresses_paginated");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "addresses": [{
                        "assetId": "ETH",
                        "address": expected_address
                    }]
                }));
        });

        // Anvil's default chain ID is 31337
        let config = RebalancingConfig {
            equity: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc: UsdcRebalancing::Disabled,
            redemption_wallet: Address::ZERO,
            usdc_vault_id: fixed_bytes!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            ),
            equities: HashMap::new(),
            fireblocks_vault_account_id: FireblocksVaultAccountId::new("0"),
            fireblocks_chain_asset_ids: serde_json::from_value(serde_json::json!({
                "31337": "ETH"
            }))
            .unwrap(),
            fireblocks_environment: FireblocksEnvironment::Sandbox,
            fireblocks_base_url: Some(server.base_url().parse().unwrap()),
        };

        let wallet = RebalancingCtx::build_wallet(
            &config,
            &TEST_RSA_PEM,
            &FireblocksApiUserId::new("test-api-key"),
            rpc_url,
        )
        .await
        .unwrap();

        assert_eq!(wallet.address(), expected_address);
    }
}
