//! Rebalancing trigger that reacts to inventory imbalances.

mod equity;
mod usdc;

use alloy::primitives::{Address, B256};
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, warn};
use url::Url;

use st0x_execution::{AlpacaBrokerApiCtx, FractionalShares, Symbol};

use st0x_event_sorcery::{AggregateError, LifecycleError, Reactor, load_aggregate};

use crate::alpaca_wallet::AlpacaAccountId;
use crate::equity_redemption::{EquityRedemption, EquityRedemptionEvent, RedemptionAggregateId};
use crate::inventory::{ImbalanceThreshold, InventoryView, InventoryViewError};
use crate::position::{Position, PositionEvent};
use crate::threshold::Usdc;
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintEvent,
};
use crate::usdc_rebalance::{
    RebalanceDirection, UsdcRebalance, UsdcRebalanceEvent, UsdcRebalanceId,
};
use crate::vault_registry::{VaultRegistry, VaultRegistryId};

/// Why loading a token address from the vault registry failed.
#[derive(Debug, thiserror::Error)]
enum TokenAddressError {
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
}

/// USDC rebalancing configuration with explicit enable/disable.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub(crate) enum UsdcRebalancingConfig {
    Enabled { target: Decimal, deviation: Decimal },
    Disabled,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RebalancingSecrets {
    pub(crate) ethereum_rpc_url: Url,
    pub(crate) evm_private_key: B256,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RebalancingConfig {
    pub(crate) equity_threshold: ImbalanceThreshold,
    pub(crate) usdc: UsdcRebalancingConfig,
    pub(crate) redemption_wallet: Address,
    pub(crate) usdc_vault_id: B256,
}

/// Runtime configuration for rebalancing operations.
/// Constructed from `RebalancingConfig` + `RebalancingSecrets` + broker's `AlpacaBrokerApiCtx`.
#[derive(Clone)]
pub(crate) struct RebalancingCtx {
    pub(crate) equity_threshold: ImbalanceThreshold,
    pub(crate) usdc: UsdcRebalancingConfig,
    /// Issuer's wallet for tokenized equity redemptions.
    pub(crate) redemption_wallet: Address,
    pub(crate) ethereum_rpc_url: Url,
    pub(crate) evm_private_key: B256,
    pub(crate) usdc_vault_id: B256,
    /// Parsed from `alpaca_broker_auth.alpaca_account_id` during construction.
    pub(crate) alpaca_account_id: AlpacaAccountId,
    /// Cloned from broker config - ensures consistency.
    pub(crate) alpaca_broker_auth: AlpacaBrokerApiCtx,
}

impl RebalancingCtx {
    /// Construct from config, secrets, and broker's Alpaca auth.
    ///
    /// This is the ONLY way to construct a `RebalancingCtx`, which enforces
    /// the invariant that rebalancing always has valid `AlpacaBrokerApiCtx`.
    pub(crate) fn new(
        config: RebalancingConfig,
        secrets: RebalancingSecrets,
        broker_auth: AlpacaBrokerApiCtx,
    ) -> Result<Self, RebalancingCtxError> {
        let alpaca_account_id = AlpacaAccountId::new(broker_auth.account_id.parse()?);

        Ok(Self {
            equity_threshold: config.equity_threshold,
            usdc: config.usdc,
            redemption_wallet: config.redemption_wallet,
            ethereum_rpc_url: secrets.ethereum_rpc_url,
            evm_private_key: secrets.evm_private_key,
            usdc_vault_id: config.usdc_vault_id,
            alpaca_account_id,
            alpaca_broker_auth: broker_auth,
        })
    }
}

impl std::fmt::Debug for RebalancingCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebalancingCtx")
            .field("equity_threshold", &self.equity_threshold)
            .field("usdc", &self.usdc)
            .field("redemption_wallet", &self.redemption_wallet)
            .field("ethereum_rpc_url", &"[REDACTED]")
            .field("evm_private_key", &"[REDACTED]")
            .field("usdc_vault_id", &self.usdc_vault_id)
            .field("alpaca_account_id", &self.alpaca_account_id)
            .field("alpaca_broker_auth", &"[REDACTED]")
            .finish()
    }
}

/// Configuration for the rebalancing trigger (runtime).
#[derive(Debug, Clone)]
pub(crate) struct RebalancingTriggerConfig {
    pub(crate) equity: ImbalanceThreshold,
    pub(crate) usdc: UsdcRebalancingConfig,
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
        token: Address,
    },
    /// Move USDC from Alpaca to Base (too much offchain).
    UsdcAlpacaToBase { amount: Usdc },
    /// Move USDC from Base to Alpaca (too much onchain).
    UsdcBaseToAlpaca { amount: Usdc },
}

/// Trigger that monitors inventory and sends rebalancing operations.
pub(crate) struct RebalancingTrigger {
    config: RebalancingTriggerConfig,
    pool: SqlitePool,
    orderbook: Address,
    order_owner: Address,
    inventory: Arc<RwLock<InventoryView>>,
    pub(crate) equity_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
    pub(crate) usdc_in_progress: Arc<AtomicBool>,
    sender: mpsc::Sender<TriggeredOperation>,
}

impl RebalancingTrigger {
    pub(crate) fn new(
        config: RebalancingTriggerConfig,
        pool: SqlitePool,
        orderbook: Address,
        order_owner: Address,
        inventory: Arc<RwLock<InventoryView>>,
        sender: mpsc::Sender<TriggeredOperation>,
    ) -> Self {
        Self {
            config,
            pool,
            orderbook,
            order_owner,
            inventory,
            equity_in_progress: Arc::new(std::sync::RwLock::new(HashSet::new())),
            usdc_in_progress: Arc::new(AtomicBool::new(false)),
            sender,
        }
    }
}

#[async_trait]
impl Reactor<Position> for RebalancingTrigger {
    async fn react(&self, symbol: &Symbol, event: &PositionEvent) {
        self.apply_position_event_and_check(symbol, event).await;
    }
}

#[async_trait]
impl Reactor<TokenizedEquityMint> for RebalancingTrigger {
    async fn react(&self, _id: &IssuerRequestId, event: &TokenizedEquityMintEvent) {
        let Some((symbol, quantity)) = Self::extract_mint_info(event) else {
            return;
        };

        self.apply_mint_event_to_inventory(&symbol, event, quantity)
            .await;

        if Self::is_terminal_mint_event(event) {
            self.clear_equity_in_progress(&symbol);
            debug!(symbol = %symbol, "Cleared equity in-progress flag after mint terminal event");

            // After mint completes, USDC balances may have changed
            self.check_and_trigger_usdc().await;
        }
    }
}

#[async_trait]
impl Reactor<EquityRedemption> for RebalancingTrigger {
    async fn react(&self, _id: &RedemptionAggregateId, event: &EquityRedemptionEvent) {
        let Some((symbol, quantity)) = Self::extract_redemption_info(event) else {
            return;
        };

        self.apply_redemption_event_to_inventory(&symbol, event, quantity)
            .await;

        if Self::is_terminal_redemption_event(event) {
            self.clear_equity_in_progress(&symbol);
            debug!(symbol = %symbol, "Cleared equity in-progress flag after redemption terminal event");

            // After redemption completes, USDC balances may have changed
            self.check_and_trigger_usdc().await;
        }
    }
}

#[async_trait]
impl Reactor<UsdcRebalance> for RebalancingTrigger {
    async fn react(&self, _id: &UsdcRebalanceId, event: &UsdcRebalanceEvent) {
        if let Some((direction, amount)) = Self::extract_usdc_rebalance_info(event) {
            self.apply_usdc_rebalance_event_to_inventory(event, &direction, amount)
                .await;
        }

        if Self::is_terminal_usdc_rebalance_event(event) {
            self.clear_usdc_in_progress();
            debug!("Cleared USDC in-progress flag after rebalance terminal event");

            // After USDC rebalance completes, check if more rebalancing is needed
            self.check_and_trigger_usdc().await;
        }
    }
}

impl RebalancingTrigger {
    /// Checks inventory for equity imbalance and triggers operation if needed.
    pub(crate) async fn check_and_trigger_equity(&self, symbol: &Symbol) {
        let Some(guard) = equity::InProgressGuard::try_claim(
            symbol.clone(),
            Arc::clone(&self.equity_in_progress),
        ) else {
            debug!(symbol = %symbol, "Skipped equity trigger: already in progress");
            return;
        };

        let Some(operation) = self.try_build_equity_operation(symbol).await else {
            return;
        };

        if let Err(error) = self.sender.try_send(operation.clone()) {
            warn!(error = %error, "Failed to send triggered operation");
            return;
        }

        debug!(symbol = %symbol, operation = ?operation, "Triggered equity rebalancing");
        guard.defuse();
    }

    async fn try_build_equity_operation(&self, symbol: &Symbol) -> Option<TriggeredOperation> {
        let token_address = match self.load_token_address(symbol).await {
            Ok(Some(addr)) => addr,
            Ok(None) => {
                // Not an error: symbols like USDCUSD (Alpaca's USDC position) aren't
                // tokenized equities and won't be in the vault registry.
                debug!(symbol = %symbol, "Skipped equity trigger: token not in vault registry");
                return None;
            }
            Err(error) => {
                error!(symbol = %symbol, error = %error, "Failed to load vault registry");
                return None;
            }
        };

        equity::check_imbalance_and_build_operation(
            symbol,
            &self.config.equity,
            &self.inventory,
            token_address,
        )
        .await
        .ok()
    }

    async fn load_token_address(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<Address>, TokenAddressError> {
        let vault_registry_id = VaultRegistryId {
            orderbook: self.orderbook,
            owner: self.order_owner,
        };

        let registry = load_aggregate::<VaultRegistry>(self.pool.clone(), &vault_registry_id)
            .await?
            .ok_or(TokenAddressError::Uninitialized)?;

        Ok(registry.token_by_symbol(symbol))
    }

    /// Checks inventory for USDC imbalance and triggers operation if needed.
    pub(crate) async fn check_and_trigger_usdc(&self) {
        let UsdcRebalancingConfig::Enabled { target, deviation } = self.config.usdc else {
            return;
        };

        let Some(guard) = usdc::InProgressGuard::try_claim(Arc::clone(&self.usdc_in_progress))
        else {
            debug!("Skipped USDC trigger: already in progress");
            return;
        };

        let threshold = ImbalanceThreshold { target, deviation };
        let Ok(operation) =
            usdc::check_imbalance_and_build_operation(&threshold, &self.inventory).await
        else {
            return;
        };

        if let Err(error) = self.sender.try_send(operation.clone()) {
            warn!(error = %error, "Failed to send USDC triggered operation");
            return;
        }

        debug!(operation = ?operation, "Triggered USDC rebalancing");
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

    async fn apply_position_event_and_check(&self, symbol: &Symbol, event: &PositionEvent) {
        if let Err(error) = self.apply_position_event_to_inventory(symbol, event).await {
            warn!(symbol = %symbol, error = %error, "Failed to apply position event to inventory");
            return;
        }

        self.check_and_trigger_equity(symbol).await;
    }

    async fn apply_position_event_to_inventory(
        &self,
        symbol: &Symbol,
        event: &PositionEvent,
    ) -> Result<(), InventoryViewError> {
        let mut inventory = self.inventory.write().await;

        let new_inventory = inventory.clone().apply_position_event(symbol, event)?;

        *inventory = new_inventory;
        drop(inventory);

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

        matches!(
            event,
            MintCompleted { .. }
                | MintRejected { .. }
                | MintAcceptanceFailed { .. }
                | VaultDepositFailed { .. }
        )
    }

    async fn apply_mint_event_to_inventory(
        &self,
        symbol: &Symbol,
        event: &TokenizedEquityMintEvent,
        quantity: FractionalShares,
    ) {
        let mut inventory = self.inventory.write().await;

        let result = inventory
            .clone()
            .apply_mint_event(symbol, event, quantity, Utc::now());

        match result {
            Ok(new_inventory) => {
                *inventory = new_inventory;
                drop(inventory);
            }
            Err(error) => {
                warn!(symbol = %symbol, error = %error, "Failed to apply mint event to inventory");
            }
        }
    }

    fn extract_redemption_info(
        event: &EquityRedemptionEvent,
    ) -> Option<(Symbol, FractionalShares)> {
        use EquityRedemptionEvent::*;

        if let VaultWithdrawn {
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

        matches!(
            event,
            Completed { .. }
                | SendFailed { .. }
                | DetectionFailed { .. }
                | RedemptionRejected { .. }
        )
    }

    async fn apply_redemption_event_to_inventory(
        &self,
        symbol: &Symbol,
        event: &EquityRedemptionEvent,
        quantity: FractionalShares,
    ) {
        let mut inventory = self.inventory.write().await;

        let result = inventory
            .clone()
            .apply_redemption_event(symbol, event, quantity, Utc::now());

        match result {
            Ok(new_inventory) => {
                *inventory = new_inventory;
                drop(inventory);
            }
            Err(error) => {
                warn!(symbol = %symbol, error = %error, "Failed to apply redemption event to inventory");
            }
        }
    }

    fn extract_usdc_rebalance_info(
        event: &UsdcRebalanceEvent,
    ) -> Option<(RebalanceDirection, Usdc)> {
        use UsdcRebalanceEvent::*;

        if let Initiated {
            direction, amount, ..
        } = event
        {
            Some((direction.clone(), *amount))
        } else {
            None
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

    async fn apply_usdc_rebalance_event_to_inventory(
        &self,
        event: &UsdcRebalanceEvent,
        direction: &RebalanceDirection,
        amount: Usdc,
    ) {
        let mut inventory = self.inventory.write().await;

        let result =
            inventory
                .clone()
                .apply_usdc_rebalance_event(event, direction, amount, Utc::now());

        match result {
            Ok(new_inventory) => {
                *inventory = new_inventory;
                drop(inventory);
            }
            Err(error) => {
                warn!(error = %error, "Failed to apply USDC rebalance event to inventory");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, U256, address, fixed_bytes};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use st0x_execution::{AlpacaBrokerApiMode, Direction, ExecutorOrderId, Positive, TimeInForce};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use tokio::sync::mpsc;
    use uuid::uuid;

    use st0x_event_sorcery::TestStore;

    use super::*;
    use crate::alpaca_wallet::AlpacaTransferId;
    use crate::offchain_order::{OffchainOrderId, PriceCents};
    use crate::position::TradeId;
    use crate::threshold::Usdc;
    use crate::tokenized_equity_mint::{ReceiptId, TokenizationRequestId};
    use crate::usdc_rebalance::{TransferRef, UsdcRebalanceCommand};
    use crate::vault_registry::VaultRegistryCommand;

    fn test_config() -> RebalancingTriggerConfig {
        RebalancingTriggerConfig {
            equity: ImbalanceThreshold {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
            usdc: UsdcRebalancingConfig::Enabled {
                target: dec!(0.5),
                deviation: dec!(0.2),
            },
        }
    }

    async fn make_trigger() -> (RebalancingTrigger, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let pool = crate::test_utils::setup_test_db().await;

        (
            RebalancingTrigger::new(
                test_config(),
                pool,
                TEST_ORDERBOOK,
                TEST_ORDER_OWNER,
                inventory,
                sender,
            ),
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

        trigger.check_and_trigger_equity(&symbol).await;
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
        let (trigger, mut receiver) = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        trigger.check_and_trigger_equity(&symbol).await;
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
            price_cents: PriceCents(15000),
            broker_timestamp: Utc::now(),
        }
    }

    const TEST_ORDERBOOK: Address = address!("0x0000000000000000000000000000000000000001");
    const TEST_ORDER_OWNER: Address = address!("0x0000000000000000000000000000000000000002");
    const TEST_TOKEN: Address = address!("0x1234567890123456789012345678901234567890");

    async fn seed_vault_registry(pool: &SqlitePool, symbol: &Symbol) {
        let store = st0x_event_sorcery::test_store::<VaultRegistry>(pool.clone(), ());
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
    ) -> (RebalancingTrigger, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(inventory));
        let pool = crate::test_utils::setup_test_db().await;

        (
            RebalancingTrigger::new(
                test_config(),
                pool,
                TEST_ORDERBOOK,
                TEST_ORDER_OWNER,
                inventory,
                sender,
            ),
            receiver,
        )
    }

    async fn make_trigger_with_inventory_and_registry(
        inventory: InventoryView,
        symbol: &Symbol,
    ) -> (RebalancingTrigger, mpsc::Receiver<TriggeredOperation>) {
        let (sender, receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(inventory));
        let pool = crate::test_utils::setup_test_db().await;

        seed_vault_registry(&pool, symbol).await;

        (
            RebalancingTrigger::new(
                test_config(),
                pool,
                TEST_ORDERBOOK,
                TEST_ORDER_OWNER,
                inventory,
                sender,
            ),
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
        let (trigger, _receiver) = make_trigger().await;
        let symbol = Symbol::new("AAPL").unwrap();

        seed_vault_registry(&trigger.pool, &symbol).await;

        let result = trigger.load_token_address(&symbol).await.unwrap();
        assert_eq!(result, Some(TEST_TOKEN));
    }

    #[tokio::test]
    async fn load_token_address_returns_none_for_unknown_symbol() {
        let (trigger, _receiver) = make_trigger().await;
        let known = Symbol::new("AAPL").unwrap();
        let unknown = Symbol::new("MSFT").unwrap();

        seed_vault_registry(&trigger.pool, &known).await;

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
        // symbol isn't pre-registered, apply_position_event_to_inventory must
        // handle it (either by auto-registering or by decoupling the
        // inventory update failure from the rebalancing check).
        let symbol = Symbol::new("AAPL").unwrap();
        let (sender, mut receiver) = mpsc::channel(10);
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let pool = crate::test_utils::setup_test_db().await;

        seed_vault_registry(&pool, &symbol).await;

        let trigger = RebalancingTrigger::new(
            test_config(),
            pool,
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory,
            sender,
        );

        // Simulate production: onchain fills arrive on an empty inventory.
        // 20 onchain buys, 80 offchain buys -> 20% onchain ratio.
        // Threshold: target 50%, deviation 20%, lower bound 30%.
        // 20% < 30% -> should trigger Mint (too much offchain).
        for _ in 0..20 {
            let event = make_onchain_fill(shares(1), Direction::Buy);
            trigger
                .apply_position_event_and_check(&symbol, &event)
                .await;
        }

        for _ in 0..80 {
            let event = make_offchain_fill(shares(1), Direction::Buy);
            trigger
                .apply_position_event_and_check(&symbol, &event)
                .await;
        }

        // Drain any intermediate triggers and do a final check.
        while receiver.try_recv().is_ok() {}
        trigger.clear_equity_in_progress(&symbol);

        // One more event to trigger the check after the imbalance is built up.
        let event = make_onchain_fill(shares(1), Direction::Buy);
        trigger
            .apply_position_event_and_check(&symbol, &event)
            .await;

        let triggered = receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Mint { .. })),
            "Expected Mint for imbalanced inventory starting from empty, got {triggered:?}"
        );
    }

    #[tokio::test]
    async fn position_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        let (trigger, mut receiver) = make_trigger_with_inventory(inventory).await;

        // Apply onchain buy - should add to onchain available.
        let event = make_onchain_fill(shares(50), Direction::Buy);
        trigger
            .apply_position_event_and_check(&symbol, &event)
            .await;

        // Apply offchain buy - should add to offchain available.
        let event = make_offchain_fill(shares(50), Direction::Buy);
        trigger
            .apply_position_event_and_check(&symbol, &event)
            .await;

        // Now inventory has 50 onchain, 50 offchain = balanced at 50%.
        // Drain any previous triggered operations (from apply_position_event_and_check).
        while receiver.try_recv().is_ok() {}

        trigger.check_and_trigger_equity(&symbol).await;
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
        let mut inventory = InventoryView::default().with_equity(symbol.clone());

        // Build balanced initial state: 50 onchain, 50 offchain.
        inventory = inventory
            .apply_position_event(&symbol, &make_onchain_fill(shares(50), Direction::Buy))
            .unwrap();
        inventory = inventory
            .apply_position_event(&symbol, &make_offchain_fill(shares(50), Direction::Buy))
            .unwrap();

        let (trigger, mut receiver) = make_trigger_with_inventory(inventory).await;

        // Apply a small buy that maintains balance (5 shares onchain).
        // After: 55 onchain, 50 offchain = 52.4% ratio, within 30-70% bounds.
        let event = make_onchain_fill(shares(5), Direction::Buy);
        trigger
            .apply_position_event_and_check(&symbol, &event)
            .await;

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
        let mut inventory = InventoryView::default().with_equity(symbol.clone());

        // Build imbalanced state: 20 onchain, 80 offchain = 20% onchain ratio.
        // This is below the 30% lower threshold (50% - 20% deviation).
        inventory = inventory
            .apply_position_event(&symbol, &make_onchain_fill(shares(20), Direction::Buy))
            .unwrap();
        inventory = inventory
            .apply_position_event(&symbol, &make_offchain_fill(shares(80), Direction::Buy))
            .unwrap();

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;

        // Apply a small event that triggers the imbalance check.
        let event = make_onchain_fill(shares(1), Direction::Buy);
        trigger
            .apply_position_event_and_check(&symbol, &event)
            .await;

        // Mint should be triggered because too much offchain.
        let triggered = receiver.try_recv();
        assert!(
            matches!(triggered, Ok(TriggeredOperation::Mint { .. })),
            "Expected Mint operation, got {triggered:?}"
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

    fn make_mint_accepted(symbol: &Symbol, quantity: Decimal) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity,
            issuer_request_id: IssuerRequestId::new("ISS123"),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        }
    }

    fn make_mint_completed(symbol: &Symbol, quantity: Decimal) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintCompleted {
            symbol: symbol.clone(),
            quantity,
            completed_at: Utc::now(),
        }
    }

    fn make_mint_rejected(symbol: &Symbol, quantity: Decimal) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRejected {
            symbol: symbol.clone(),
            quantity,
            reason: "API timeout".to_string(),
            rejected_at: Utc::now(),
        }
    }

    fn make_mint_acceptance_failed(symbol: &Symbol, quantity: Decimal) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintAcceptanceFailed {
            symbol: symbol.clone(),
            quantity,
            reason: "Transaction reverted".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_tokens_received(symbol: &Symbol, quantity: Decimal) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokensReceived {
            symbol: symbol.clone(),
            quantity,
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(30_000_000_000_000_000_000_u128),
            received_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn mint_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        // Start with 20 onchain and 80 offchain - imbalanced (20% onchain).
        // Threshold: target 50%, deviation 20%, so lower bound is 30%.
        // 20% < 30% triggers TooMuchOffchain.
        let inventory = inventory
            .apply_position_event(&symbol, &make_onchain_fill(shares(20), Direction::Buy))
            .unwrap()
            .apply_position_event(&symbol, &make_offchain_fill(shares(80), Direction::Buy))
            .unwrap();

        let (trigger, mut receiver) =
            make_trigger_with_inventory_and_registry(inventory, &symbol).await;

        // Initially, trigger should detect imbalance (too much offchain).
        trigger.check_and_trigger_equity(&symbol).await;
        let initial_check = receiver.try_recv();
        assert!(
            matches!(initial_check, Ok(TriggeredOperation::Mint { .. })),
            "Expected initial imbalance to trigger Mint, got {initial_check:?}"
        );

        // Clear in-progress so we can test again.
        trigger.clear_equity_in_progress(&symbol);

        // Apply MintAccepted - this moves shares to inflight.
        // Inflight should now block imbalance detection.
        trigger
            .apply_mint_event_to_inventory(
                &symbol,
                &make_mint_accepted(&symbol, dec!(30)),
                shares(30),
            )
            .await;

        // With inflight, imbalance detection should not trigger anything.
        trigger.check_and_trigger_equity(&symbol).await;
        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected no operation due to inflight"
        );
    }

    /// Verifies that events arriving without MintRequested (incremental dispatch)
    /// still update inventory correctly. This is the fix for the double-triggering bug
    /// where inventory wasn't updated when events arrived in separate batches.
    #[tokio::test]
    async fn incremental_dispatch_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let threshold = ImbalanceThreshold {
            target: dec!(0.5),
            deviation: dec!(0.1),
        };

        // Start with imbalanced inventory: 30 onchain, 70 offchain.
        let mut inventory = InventoryView::default().with_equity(symbol.clone());
        inventory = inventory
            .apply_position_event(&symbol, &make_onchain_fill(shares(30), Direction::Buy))
            .unwrap();
        inventory = inventory
            .apply_position_event(&symbol, &make_offchain_fill(shares(70), Direction::Buy))
            .unwrap();

        // Verify initial state has no inflight (check_equity_imbalance returns Some).
        assert!(
            inventory
                .check_equity_imbalance(&symbol, &threshold)
                .is_some()
        );

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;

        // Simulate incremental dispatch: MintAccepted arrives WITHOUT MintRequested.
        // Before the fix, this would log a warning and skip inventory update.
        // After the fix, inventory should be updated.
        let events = vec![make_mint_envelope(make_mint_accepted(&symbol, dec!(30)))];
        trigger.dispatch("mint-123", &events).await;

        // Verify inventory was updated: offchain should have inflight now.
        // When inflight exists, check_equity_imbalance returns None.
        let has_imbalance = {
            let inventory = trigger.inventory.read().await;
            inventory.check_equity_imbalance(&symbol, &threshold)
        };
        assert!(
            has_imbalance.is_none(),
            "Expected inflight to block imbalance detection after MintAccepted dispatch"
        );
    }

    #[tokio::test]
    async fn mint_completion_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;

        // Mark symbol as in-progress.
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }
        assert!(trigger.equity_in_progress.read().unwrap().contains(&symbol));

        // Check that MintCompleted is detected as terminal.
        assert!(RebalancingTrigger::is_terminal_mint_event(
            &make_mint_completed()
        ));

        // Simulate what dispatch does - clear in-progress on terminal.
        trigger.clear_equity_in_progress(&symbol);
        assert!(!trigger.equity_in_progress.read().unwrap().contains(&symbol));
    }

    #[tokio::test]
    async fn mint_rejection_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

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

    #[tokio::test]
    async fn terminal_mint_event_in_isolation_clears_in_progress_flag() {
        // This test verifies that when MintCompleted arrives in a separate dispatch
        // (without MintRequested in the same batch), the in_progress flag is still cleared.
        // This simulates what happens when Finalize command is executed.
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        let (trigger, _receiver) = make_trigger_with_inventory(inventory).await;

        // Mark symbol as in-progress (as if a mint was triggered).
        {
            let mut guard = trigger.equity_in_progress.write().unwrap();
            guard.insert(symbol.clone());
        }
        assert!(trigger.equity_in_progress.read().unwrap().contains(&symbol));

        // Dispatch ONLY MintCompleted - no MintRequested in this batch.
        // This is what happens when Finalize command produces MintCompleted.
        let events = vec![make_mint_envelope(make_mint_completed(&symbol, dec!(30)))];

        trigger.dispatch("test-aggregate-id", &events).await;

        // The in_progress flag should be cleared even though MintRequested wasn't in this batch.
        assert!(
            !trigger.equity_in_progress.read().unwrap().contains(&symbol),
            "in_progress flag should be cleared when terminal event has symbol"
        );
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
        let result = RebalancingTrigger::extract_mint_info(&make_mint_completed());
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

    fn make_vault_withdrawn(symbol: &Symbol, quantity: Decimal) -> EquityRedemptionEvent {
        EquityRedemptionEvent::VaultWithdrawn {
            symbol: symbol.clone(),
            quantity,
            token: Address::random(),
            vault_withdraw_tx: TxHash::random(),
            withdrawn_at: Utc::now(),
        }
    }

    fn make_tokens_sent() -> EquityRedemptionEvent {
        EquityRedemptionEvent::TokensSent {
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            sent_at: Utc::now(),
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
            reason: "Alpaca timeout".to_string(),
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
            reason: "Insufficient balance".to_string(),
            rejected_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn redemption_event_updates_inventory() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

        // Start with 80 onchain and 20 offchain - imbalanced (80% onchain).
        // Threshold: target 50%, deviation 20%, so upper bound is 70%.
        // 80% > 70% triggers TooMuchOnchain.
        let inventory = inventory
            .apply_position_event(&symbol, &make_onchain_fill(shares(80), Direction::Buy))
            .unwrap()
            .apply_position_event(&symbol, &make_offchain_fill(shares(20), Direction::Buy))
            .unwrap();

        let (trigger, mut receiver) = make_trigger_with_inventory(inventory).await;

        // Apply TokensSent - this moves shares from onchain available to inflight.
        trigger
            .apply_redemption_event_to_inventory(&symbol, &make_tokens_sent(), shares(30))
            .await;

        // With inflight, imbalance detection should not trigger anything.
        trigger.check_and_trigger_equity(&symbol).await;
        assert!(
            matches!(
                receiver.try_recv().unwrap_err(),
                mpsc::error::TryRecvError::Empty
            ),
            "Expected no operation due to inflight"
        );
    }

    #[tokio::test]
    async fn redemption_completion_clears_in_progress_flag() {
        let symbol = Symbol::new("AAPL").unwrap();
        let inventory = InventoryView::default().with_equity(symbol.clone());

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
        let event = make_vault_withdrawn(&symbol, dec!(42.5));

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
            &make_vault_withdrawn(&symbol, dec!(30))
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
    fn extract_usdc_rebalance_info_returns_direction_and_amount() {
        let event = make_usdc_initiated(RebalanceDirection::BaseToAlpaca, usdc(5000));

        let (extracted_direction, extracted_amount) =
            RebalancingTrigger::extract_usdc_rebalance_info(&event).unwrap();

        assert_eq!(extracted_direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(extracted_amount, usdc(5000));
    }

    #[test]
    fn extract_usdc_rebalance_info_returns_none_without_initiated() {
        let result = RebalancingTrigger::extract_usdc_rebalance_info(&make_usdc_deposit_confirmed(
            RebalanceDirection::AlpacaToBase,
        ));
        assert!(result.is_none());
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
            alpaca_account_id = "904837e3-3b76-47ec-b432-046db621571b"

            [alpaca_broker_auth]
            api_key = "test_key"
            api_secret = "test_secret"
            account_id = "904837e3-3b76-47ec-b432-046db621571b"
            mode = "sandbox"

            [equity_threshold]
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
            ethereum_rpc_url = "https://eth.example.com"
            evm_private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#
    }

    fn test_broker_auth() -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: "904837e3-3b76-47ec-b432-046db621571b".to_string(),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
        }
    }

    #[test]
    fn deserialize_config_succeeds() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();

        assert_eq!(config.equity_threshold.target, dec!(0.5));
        assert_eq!(config.equity_threshold.deviation, dec!(0.2));

        let UsdcRebalancingConfig::Enabled { target, deviation } = config.usdc else {
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

    #[test]
    fn new_constructs_ctx() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();
        let secrets: RebalancingSecrets = toml::from_str(valid_rebalancing_secrets_toml()).unwrap();

        let ctx = RebalancingCtx::new(config, secrets, test_broker_auth()).unwrap();

        assert_eq!(ctx.alpaca_broker_auth.api_key, "test_key");
        assert_eq!(ctx.alpaca_broker_auth.api_secret, "test_secret");
        assert_eq!(
            ctx.alpaca_account_id,
            AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"))
        );
    }

    #[test]
    fn new_fails_with_invalid_account_id() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();
        let secrets: RebalancingSecrets = toml::from_str(valid_rebalancing_secrets_toml()).unwrap();
        let mut broker_auth = test_broker_auth();
        broker_auth.account_id = "not-a-uuid".to_string();

        let result = RebalancingCtx::new(config, secrets, broker_auth);

        assert!(
            matches!(result, Err(RebalancingCtxError::InvalidAccountId(_))),
            "Expected InvalidAccountId error, got {result:?}"
        );
    }

    #[test]
    fn deserialize_with_custom_thresholds() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            alpaca_account_id = "904837e3-3b76-47ec-b432-046db621571b"

            [alpaca_broker_auth]
            api_key = "test_key"
            api_secret = "test_secret"
            account_id = "904837e3-3b76-47ec-b432-046db621571b"
            mode = "sandbox"

            [equity_threshold]
            target = "0.6"
            deviation = "0.1"

            [usdc]
            mode = "enabled"
            target = "0.4"
            deviation = "0.15"
        "#,
        )
        .unwrap();
        let secrets: RebalancingSecrets = toml::from_str(valid_rebalancing_secrets_toml()).unwrap();

        let ctx = RebalancingCtx::new(config, secrets, test_broker_auth()).unwrap();

        assert_eq!(ctx.equity_threshold.target, dec!(0.6));
        assert_eq!(ctx.equity_threshold.deviation, dec!(0.1));

        let UsdcRebalancingConfig::Enabled { target, deviation } = ctx.usdc else {
            panic!("expected enabled");
        };
        assert_eq!(target, dec!(0.4));
        assert_eq!(deviation, dec!(0.15));
    }

    #[test]
    fn deserialize_missing_redemption_wallet_fails() {
        let toml_str = r#"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            alpaca_account_id = "904837e3-3b76-47ec-b432-046db621571b"

            [equity_threshold]
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
    fn deserialize_missing_evm_private_key_fails() {
        let toml_str = r#"
            ethereum_rpc_url = "https://eth.example.com"
        "#;

        let error = toml::from_str::<RebalancingSecrets>(toml_str).unwrap_err();
        assert!(
            error.message().contains("evm_private_key"),
            "Expected missing evm_private_key error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_equity_fails() {
        let toml_str = r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            alpaca_account_id = "904837e3-3b76-47ec-b432-046db621571b"

            [usdc]
            mode = "disabled"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("equity_threshold"),
            "Expected missing equity_threshold error, got: {error}"
        );
    }

    #[test]
    fn deserialize_usdc_disabled() {
        let toml_str = r#"
            redemption_wallet = "0x1234567890123456789012345678901234567890"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

            [equity_threshold]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "disabled"
        "#;

        let config: RebalancingConfig = toml::from_str(toml_str).unwrap();

        assert!(matches!(config.usdc, UsdcRebalancingConfig::Disabled));
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

    #[async_trait]
    impl Reactor<UsdcRebalance> for EventCapturingReactor {
        async fn react(&self, _id: &UsdcRebalanceId, event: &UsdcRebalanceEvent) {
            self.captured_events.lock().await.push(event.clone());

            let is_terminal = RebalancingTrigger::is_terminal_usdc_rebalance_event(event);
            self.terminal_detection_results
                .lock()
                .await
                .push(is_terminal);
        }
    }

    #[tokio::test]
    async fn terminal_detection_identifies_deposit_confirmed_alone_for_base_to_alpaca() {
        let spy = Arc::new(EventCapturingReactor::new());
        let store = TestStore::<UsdcRebalance>::new(vec![Box::new(Arc::clone(&spy))], ());

        let id = UsdcRebalanceId::new("base-to-alpaca-001");
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
        let store = TestStore::<UsdcRebalance>::new(vec![Box::new(Arc::clone(&spy))], ());

        let id = UsdcRebalanceId::new("alpaca-to-base-001");
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
        let store = TestStore::<UsdcRebalance>::new(vec![Box::new(Arc::clone(&spy))], ());

        let id = UsdcRebalanceId::new("withdrawal-failed-test");
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
        let store = TestStore::<UsdcRebalance>::new(vec![Box::new(Arc::clone(&spy))], ());

        let id = UsdcRebalanceId::new("bridging-failed-test");
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
        let store = TestStore::<UsdcRebalance>::new(vec![Box::new(Arc::clone(&spy))], ());

        let id = UsdcRebalanceId::new("conversion-failed-test");

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
            pool,
            Address::ZERO,
            Address::ZERO,
            inventory,
            sender,
        ));

        // Set in_progress flag
        trigger.usdc_in_progress.store(true, Ordering::SeqCst);
        assert!(trigger.usdc_in_progress.load(Ordering::SeqCst));

        // React to events including Initiated (required for react to process)
        // and terminal DepositConfirmed
        let tx_hash =
            fixed_bytes!("0xaaaa111111111111111111111111111111111111111111111111111111111111");
        let id = UsdcRebalanceId::new("test-clear-001".to_string());

        Reactor::<UsdcRebalance>::react(
            trigger.as_ref(),
            &id,
            &UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000)),
                withdrawal_ref: TransferRef::OnchainTx(tx_hash),
                initiated_at: chrono::Utc::now(),
            },
        )
        .await;

        Reactor::<UsdcRebalance>::react(
            trigger.as_ref(),
            &id,
            &UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                deposit_confirmed_at: chrono::Utc::now(),
            },
        )
        .await;

        // Verify in_progress flag was cleared (AlpacaToBase deposit is terminal)
        assert!(
            !trigger.usdc_in_progress.load(Ordering::SeqCst),
            "usdc_in_progress should be cleared after terminal event dispatch"
        );
    }

    #[tokio::test]
    async fn terminal_detection_identifies_conversion_confirmed_alone_for_base_to_alpaca() {
        let spy = Arc::new(EventCapturingReactor::new());
        let store = TestStore::<UsdcRebalance>::new(vec![Box::new(Arc::clone(&spy))], ());

        let id = UsdcRebalanceId::new("conversion-base-to-alpaca-001");
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
    /// 3. Ratio = X/(X+0) = 100% → detects "TooMuchOnchain" → Redemption
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
            pool.clone(),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            sender,
        ));

        // Create InventorySnapshotQuery that will dispatch events to the trigger
        let query = InventorySnapshotQuery::new(inventory.clone(), Some(trigger.clone()));

        // Simulate what happens during inventory polling:
        // OnchainEquity event arrives FIRST (offchain not yet polled)
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100)); // 100 shares onchain

        let onchain_event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: Utc::now(),
        };

        let envelope = EventEnvelope {
            aggregate_id: "test".to_string(),
            sequence: 1,
            payload: onchain_event,
            metadata: HashMap::new(),
        };

        // Dispatch the onchain event - this should NOT trigger rebalancing
        // because we don't have offchain data yet
        query.dispatch("test", &[envelope]).await;

        // CORRECT BEHAVIOR: No operation should be triggered because
        // offchain data hasn't arrived yet. The system should wait until
        // it has a complete picture of inventory before deciding to rebalance.
        //
        // CURRENT BUG: A Redemption is incorrectly triggered because:
        // - Onchain: 100 shares
        // - Offchain: 0 shares (not polled yet, treated as "no holdings")
        // - Ratio: 100% onchain → "TooMuchOnchain" → Redemption
        let triggered = receiver.try_recv();

        assert!(
            triggered.is_err(),
            "CORRECT: No operation should trigger with partial inventory data. \
             BUG: Got {triggered:?} - system incorrectly treated missing offchain \
             data as 'zero holdings' and triggered rebalancing."
        );
    }

    /// Complementary test: verify that trigger DOES fire once both venues have data.
    #[sqlx::test]
    async fn trigger_fires_when_both_venues_have_data(pool: SqlitePool) {
        let symbol = Symbol::new("RKLB").unwrap();
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let (sender, mut receiver) = mpsc::channel(10);

        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            pool.clone(),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            sender,
        ));

        let query = InventorySnapshotQuery::new(inventory.clone(), Some(trigger.clone()));

        // Apply onchain snapshot (100 shares)
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100));

        let onchain_event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: Utc::now(),
        };

        query
            .dispatch(
                "test",
                &[EventEnvelope {
                    aggregate_id: "test".to_string(),
                    sequence: 1,
                    payload: onchain_event,
                    metadata: HashMap::new(),
                }],
            )
            .await;

        // No trigger yet - only one venue has data
        assert!(
            receiver.try_recv().is_err(),
            "should not trigger with only onchain data"
        );

        // Apply offchain snapshot (0 shares) - now both venues have data
        let mut positions = BTreeMap::new();
        positions.insert(symbol.clone(), shares(0));

        let offchain_event = InventorySnapshotEvent::OffchainEquity {
            positions,
            fetched_at: Utc::now(),
        };

        query
            .dispatch(
                "test",
                &[EventEnvelope {
                    aggregate_id: "test".to_string(),
                    sequence: 2,
                    payload: offchain_event,
                    metadata: HashMap::new(),
                }],
            )
            .await;

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
    #[sqlx::test]
    async fn logs_show_partial_data_skips_imbalance_check(pool: SqlitePool) {
        let symbol = Symbol::new("RKLB").unwrap();
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let (sender, _receiver) = mpsc::channel(10);

        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            pool.clone(),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            sender,
        ));

        let query = InventorySnapshotQuery::new(inventory.clone(), Some(trigger.clone()));

        // Apply ONLY onchain data - offchain not yet polled
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100));

        let onchain_event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: Utc::now(),
        };

        query
            .dispatch(
                "test",
                &[EventEnvelope {
                    aggregate_id: "test".to_string(),
                    sequence: 1,
                    payload: onchain_event,
                    metadata: HashMap::new(),
                }],
            )
            .await;

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
    #[sqlx::test]
    async fn logs_show_trigger_fires_with_complete_data(pool: SqlitePool) {
        let symbol = Symbol::new("RKLB").unwrap();
        let inventory = Arc::new(RwLock::new(InventoryView::default()));
        let (sender, _receiver) = mpsc::channel(10);

        seed_vault_registry(&pool, &symbol).await;

        let trigger = Arc::new(RebalancingTrigger::new(
            test_config(),
            pool.clone(),
            TEST_ORDERBOOK,
            TEST_ORDER_OWNER,
            inventory.clone(),
            sender,
        ));

        let query = InventorySnapshotQuery::new(inventory.clone(), Some(trigger.clone()));

        // Apply onchain data first
        let mut balances = BTreeMap::new();
        balances.insert(symbol.clone(), shares(100));

        query
            .dispatch(
                "test",
                &[EventEnvelope {
                    aggregate_id: "test".to_string(),
                    sequence: 1,
                    payload: InventorySnapshotEvent::OnchainEquity {
                        balances,
                        fetched_at: Utc::now(),
                    },
                    metadata: HashMap::new(),
                }],
            )
            .await;

        // Now apply offchain data - both venues now have data
        let mut positions = BTreeMap::new();
        positions.insert(symbol.clone(), shares(0));

        query
            .dispatch(
                "test",
                &[EventEnvelope {
                    aggregate_id: "test".to_string(),
                    sequence: 2,
                    payload: InventorySnapshotEvent::OffchainEquity {
                        positions,
                        fetched_at: Utc::now(),
                    },
                    metadata: HashMap::new(),
                }],
            )
            .await;

        // Verify the trigger fired after both venues have data
        assert!(
            logs_contain("Triggered equity rebalancing"),
            "Should trigger rebalancing once both venues have data"
        );
    }
}
