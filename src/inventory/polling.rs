//! Inventory polling service for fetching actual balances and emitting
//! snapshot events.
//!
//! This service polls onchain vaults and offchain broker accounts to fetch
//! actual inventory balances, then emits InventorySnapshotCommands to record
//! the fetched values. The InventoryView reacts to these events to reconcile
//! tracked inventory.

use alloy::primitives::{Address, B256, TxHash};
use alloy::providers::RootProvider;
use async_trait::async_trait;
use chrono::Utc;
use futures_util::future::try_join_all;
use rain_math_float::FloatError;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::{debug, warn};

use st0x_event_sorcery::{SendError, Store};
use st0x_evm::{Evm, EvmError, OpenChainErrorRegistry, Wallet};
use st0x_execution::{Executor, FractionalShares, InventoryResult, SharesConversionError, Symbol};
use st0x_finance::{HasZero, Usd, UsdToCentsError, Usdc};
use st0x_raindex::{RaindexError, RaindexService, RaindexVaultId};

use crate::bindings::IERC20;
use crate::inventory::snapshot::{
    InventorySnapshot, InventorySnapshotCommand, InventorySnapshotId,
};
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::rebalancing::usdc::{UsdcTransferError, u256_to_usdc};
use crate::tokenization::{TokenizationRequestType, Tokenizer, TokenizerError};
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizationRequestId};
use crate::vault_registry::{VaultRegistry, VaultRegistryId};

/// Pending mints and redemptions aggregated by symbol.
struct PendingRequests {
    mints: BTreeMap<Symbol, FractionalShares>,
    redemptions: BTreeMap<Symbol, FractionalShares>,
}

/// Active bot-owned tokenization provider request identifiers.
#[derive(Debug, Clone, Default)]
pub(crate) struct PendingRequestOwnershipSnapshot {
    pub(crate) mint_issuers: HashSet<IssuerRequestId>,
    pub(crate) mint_tokenizations: HashSet<TokenizationRequestId>,
    pub(crate) redemption_tokenizations: HashSet<TokenizationRequestId>,
    pub(crate) redemption_txs: HashSet<TxHash>,
}

#[async_trait]
pub(crate) trait PendingRequestOwnership: Send + Sync {
    async fn pending_request_ownership(&self) -> PendingRequestOwnershipSnapshot;
}

/// Error type for inventory polling operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryPollingError<ExecutorError> {
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    Executor(ExecutorError),
    #[error(transparent)]
    SnapshotAggregate(#[from] SendError<InventorySnapshot>),
    #[error(transparent)]
    VaultRegistry(#[from] SendError<VaultRegistry>),
    #[error(transparent)]
    Evm(#[from] EvmError),
    #[error(transparent)]
    UsdcConversion(#[from] UsdcTransferError),
    #[error(transparent)]
    Tokenizer(#[from] TokenizerError),
    #[error(transparent)]
    Float(#[from] FloatError),
    #[error("vault balance mismatch: expected {expected:?}, got {actual:?}")]
    VaultBalanceMismatch {
        expected: Vec<Address>,
        actual: Vec<Address>,
    },
    #[error(transparent)]
    SharesConversion(#[from] SharesConversionError),
    #[error(transparent)]
    Reserve(#[from] ReserveError),
}

/// Errors that can occur when computing available cash after subtracting
/// the configured reserve from the gross broker balance.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReserveError {
    #[error("broker USD balance is negative: {cents} cents")]
    NegativeBrokerBalance { cents: i64 },
    #[error(transparent)]
    Arithmetic(#[from] FloatError),
    #[error(transparent)]
    CentsConversion(#[from] UsdToCentsError),
    #[error("failed to convert broker cents {cents} to Usd")]
    BrokerCentsConversion { cents: i64 },
}

pub(crate) struct WalletPollingCtx {
    pub(crate) ethereum: Arc<dyn Wallet<Provider = RootProvider>>,
    pub(crate) base: Arc<dyn Wallet<Provider = RootProvider>>,
    pub(crate) unwrapped_equity_token_addresses: HashMap<Symbol, Address>,
    pub(crate) wrapped_equity_token_addresses: HashMap<Symbol, Address>,
}

/// Service that polls actual inventory from onchain vaults and offchain brokers.
pub(crate) struct InventoryPollingService<Chain, Exe>
where
    Chain: Evm,
{
    raindex_service: Arc<RaindexService<Chain>>,
    executor: Exe,
    vault_registry: Arc<Store<VaultRegistry>>,
    snapshot_id: InventorySnapshotId,
    snapshot: Arc<Store<InventorySnapshot>>,
    wallet_polling: Option<WalletPollingCtx>,
    tokenizer: Option<Arc<dyn Tokenizer>>,
    pending_request_ownership: Option<Arc<dyn PendingRequestOwnership>>,
    external_pending_warnings: Mutex<HashSet<TokenizationRequestId>>,
    unconfigured_symbol_warnings: Mutex<HashSet<Symbol>>,
    retired_equity_vault_warnings: Mutex<HashSet<(Address, B256)>>,
    retired_usdc_vault_warnings: Mutex<HashSet<B256>>,
    configured_equity_symbols: Option<HashSet<Symbol>>,
    configured_equity_vaults: Option<BTreeMap<Address, BTreeSet<B256>>>,
    configured_usdc_vaults: Option<BTreeSet<B256>>,
    reserved_cash: Usd,
}

impl<Chain, Exe> InventoryPollingService<Chain, Exe>
where
    Chain: Evm,
    Exe: Executor,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        raindex_service: Arc<RaindexService<Chain>>,
        executor: Exe,
        vault_registry: Arc<Store<VaultRegistry>>,
        snapshot_id: InventorySnapshotId,
        snapshot: Arc<Store<InventorySnapshot>>,
        wallet_polling: Option<WalletPollingCtx>,
        tokenizer: Option<Arc<dyn Tokenizer>>,
        reserved_cash: Usd,
    ) -> Self {
        Self {
            raindex_service,
            executor,
            vault_registry,
            snapshot_id,
            snapshot,
            wallet_polling,
            tokenizer,
            pending_request_ownership: None,
            external_pending_warnings: Mutex::new(HashSet::new()),
            unconfigured_symbol_warnings: Mutex::new(HashSet::new()),
            retired_equity_vault_warnings: Mutex::new(HashSet::new()),
            retired_usdc_vault_warnings: Mutex::new(HashSet::new()),
            configured_equity_symbols: None,
            configured_equity_vaults: None,
            configured_usdc_vaults: None,
            reserved_cash,
        }
    }

    pub(crate) fn with_configured_equity_symbols(
        mut self,
        configured_equity_symbols: HashSet<Symbol>,
    ) -> Self {
        self.configured_equity_symbols = Some(configured_equity_symbols);
        self
    }

    pub(crate) fn with_configured_vaults(
        mut self,
        configured_equity_vaults: BTreeMap<Address, BTreeSet<B256>>,
        configured_usdc_vaults: Option<BTreeSet<B256>>,
    ) -> Self {
        self.configured_equity_vaults = Some(configured_equity_vaults);
        self.configured_usdc_vaults = configured_usdc_vaults;
        self
    }

    pub(crate) fn with_pending_request_ownership(
        mut self,
        pending_request_ownership: Arc<dyn PendingRequestOwnership>,
    ) -> Self {
        self.pending_request_ownership = Some(pending_request_ownership);
        self
    }

    /// Polls actual inventory from all venues and emits snapshot commands.
    ///
    /// 1. Queries inflight equity from tokenization provider (must be first)
    /// 2. Queries onchain equity and USDC balances from Base vaults
    /// 3. Queries Ethereum wallet USDC balance (if configured)
    /// 4. Queries offchain positions, cash, and Alpaca USDC from executor
    ///
    /// Inflight is a hard precondition, not an independent poll group. Balance
    /// snapshots trigger check_and_trigger_equity, whose dedup guard is
    /// has_inflight(). On startup the first poll tick runs immediately -- if
    /// balance snapshots land before a fresh inflight snapshot, the system can
    /// trigger a duplicate operation for a request Alpaca already has pending.
    /// So an inflight poll failure aborts the whole tick before any balance
    /// snapshot is emitted. The remaining three groups (onchain, wallet,
    /// offchain) are independent: each logs and continues on failure so a
    /// single venue outage does not stall the others.
    pub(crate) async fn poll_and_record(&self) -> Result<(), InventoryPollingError<Exe::Error>> {
        let snapshot_id = &self.snapshot_id;

        if let Err(error) = self.poll_inflight_equity(snapshot_id).await {
            warn!(
                target: "inventory",
                ?error,
                "Inventory inflight equity polling failed; aborting tick before balance snapshots to avoid duplicate operations"
            );
            return Ok(());
        }

        if let Err(error) = self.poll_onchain(snapshot_id).await {
            warn!(target: "inventory", ?error, "Inventory onchain polling failed");
        }

        if let Err(error) = self.poll_wallets(snapshot_id).await {
            warn!(target: "inventory", ?error, "Inventory wallet polling failed");
        }

        if let Err(error) = self.poll_offchain(snapshot_id).await {
            warn!(target: "inventory", ?error, "Inventory offchain polling failed");
        }

        Ok(())
    }

    async fn poll_onchain(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let vault_registry = self.load_vault_registry().await?;

        let Some(registry) = vault_registry else {
            debug!(target: "inventory", "Vault registry not initialized, skipping onchain polling");
            return Ok(());
        };

        self.poll_onchain_equity(snapshot_id, &registry).await?;
        self.poll_onchain_usdc(snapshot_id, &registry).await?;

        Ok(())
    }

    async fn load_vault_registry(
        &self,
    ) -> Result<Option<VaultRegistry>, InventoryPollingError<Exe::Error>> {
        let vault_registry_id = VaultRegistryId {
            orderbook: self.snapshot_id.orderbook,
            owner: self.snapshot_id.owner,
        };

        Ok(self.vault_registry.load(&vault_registry_id).await?)
    }

    async fn poll_onchain_equity(
        &self,
        snapshot_id: &InventorySnapshotId,
        registry: &VaultRegistry,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        if registry.equity_vaults.is_empty() {
            debug!(target: "inventory", "No equity vaults discovered, skipping onchain equity polling");
            return Ok(());
        }

        let expected_tokens: Vec<_> = registry.equity_vaults.keys().copied().collect();

        // Fetch all vault balances in parallel across all tokens and vault IDs,
        // then sum per-token to get the total balance for each asset.
        let per_token_futures = registry
            .equity_vaults
            .iter()
            .filter_map(|(token, vaults_for_token)| {
                let first_vault = vaults_for_token.values().next()?;
                let symbol = first_vault.symbol.clone();
                Some((*token, symbol, vaults_for_token))
            })
            .map(|(token, symbol, vaults_for_token)| async move {
                let vault_futures = vaults_for_token.values().map(|vault| {
                    self.raindex_service
                        .get_equity_balance::<OpenChainErrorRegistry>(
                            self.snapshot_id.owner,
                            vault.token,
                            RaindexVaultId(vault.vault_id),
                        )
                });

                let vault_balances = try_join_all(vault_futures).await?;

                for (vault, balance) in vaults_for_token.values().zip(vault_balances.iter()) {
                    self.warn_if_retired_equity_vault_has_balance(
                        &symbol,
                        token,
                        vault.vault_id,
                        *balance,
                    )?;
                }

                let total = vault_balances[1..]
                    .iter()
                    .copied()
                    .try_fold(vault_balances[0], |acc, balance| acc + balance)?;

                Ok::<_, InventoryPollingError<Exe::Error>>((token, symbol, total))
            });

        let results = try_join_all(per_token_futures).await?;

        let balances: BTreeMap<_, _> = results
            .iter()
            .map(|(_, symbol, balance)| (symbol.clone(), *balance))
            .collect();

        let fetched_tokens: Vec<_> = results.into_iter().map(|(token, _, _)| token).collect();

        if expected_tokens != fetched_tokens {
            return Err(InventoryPollingError::VaultBalanceMismatch {
                expected: expected_tokens,
                actual: fetched_tokens,
            });
        }

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OnchainEquity { balances },
            )
            .await?;

        Ok(())
    }

    async fn poll_onchain_usdc(
        &self,
        snapshot_id: &InventorySnapshotId,
        registry: &VaultRegistry,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        if registry.usdc_vaults.is_empty() {
            debug!(target: "inventory", "No USDC vaults discovered, skipping onchain cash polling");
            return Ok(());
        }

        let balance_futures = registry.usdc_vaults.values().map(|vault| {
            self.raindex_service
                .get_usdc_balance::<OpenChainErrorRegistry>(
                    self.snapshot_id.owner,
                    RaindexVaultId(vault.vault_id),
                )
        });

        let vault_balances = try_join_all(balance_futures).await?;

        for (vault, balance) in registry.usdc_vaults.values().zip(vault_balances.iter()) {
            self.warn_if_retired_usdc_vault_has_balance(vault.vault_id, *balance)?;
        }

        let usdc_balance = vault_balances[1..]
            .iter()
            .copied()
            .try_fold(vault_balances[0], |acc, balance| acc + balance)?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OnchainUsdc { usdc_balance },
            )
            .await?;

        Ok(())
    }

    async fn poll_wallets(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let Some(wallets) = &self.wallet_polling else {
            debug!(target: "inventory", "No wallet polling configured, skipping wallet balance polling");
            return Ok(());
        };

        self.poll_ethereum_usdc(snapshot_id, &wallets.ethereum)
            .await?;

        self.poll_base_wallet_usdc(snapshot_id, &wallets.base)
            .await?;

        let balances = self
            .poll_base_wallet_token_balances(
                &wallets.base,
                &wallets.unwrapped_equity_token_addresses,
            )
            .await?;

        if !balances.is_empty() {
            self.snapshot
                .send(
                    snapshot_id,
                    InventorySnapshotCommand::BaseWalletUnwrappedEquity { balances },
                )
                .await?;
        }

        let balances = self
            .poll_base_wallet_token_balances(&wallets.base, &wallets.wrapped_equity_token_addresses)
            .await?;

        if !balances.is_empty() {
            self.snapshot
                .send(
                    snapshot_id,
                    InventorySnapshotCommand::BaseWalletWrappedEquity { balances },
                )
                .await?;
        }

        Ok(())
    }

    async fn poll_ethereum_usdc(
        &self,
        snapshot_id: &InventorySnapshotId,
        wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let raw_balance = wallet
            .call::<OpenChainErrorRegistry, _>(
                USDC_ETHEREUM,
                IERC20::balanceOfCall {
                    account: wallet.address(),
                },
            )
            .await?;

        let usdc_balance = u256_to_usdc(raw_balance)?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::EthereumUsdc { usdc_balance },
            )
            .await?;

        Ok(())
    }

    async fn poll_base_wallet_usdc(
        &self,
        snapshot_id: &InventorySnapshotId,
        wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let raw_balance = wallet
            .call::<OpenChainErrorRegistry, _>(
                USDC_BASE,
                IERC20::balanceOfCall {
                    account: wallet.address(),
                },
            )
            .await?;

        let usdc_balance = u256_to_usdc(raw_balance)?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::BaseWalletUsdc { usdc_balance },
            )
            .await?;

        Ok(())
    }

    async fn poll_base_wallet_token_balances(
        &self,
        wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
        token_addresses: &HashMap<Symbol, Address>,
    ) -> Result<BTreeMap<Symbol, FractionalShares>, InventoryPollingError<Exe::Error>> {
        let balance_futures = token_addresses.iter().map(|(symbol, token_addr)| async {
            let raw_balance = wallet
                .call::<OpenChainErrorRegistry, _>(
                    *token_addr,
                    IERC20::balanceOfCall {
                        account: wallet.address(),
                    },
                )
                .await?;

            let shares = FractionalShares::from_u256_18_decimals(raw_balance)?;
            Ok::<_, InventoryPollingError<Exe::Error>>((symbol.clone(), shares))
        });

        let results = try_join_all(balance_futures).await?;

        Ok(results.into_iter().collect())
    }

    async fn poll_offchain(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let inventory_result = self
            .executor
            .get_inventory()
            .await
            .map_err(InventoryPollingError::Executor)?;

        let InventoryResult::Fetched(inventory) = inventory_result else {
            debug!(target: "inventory", "Executor returned non-fetched inventory result, skipping offchain polling");
            return Ok(());
        };

        let positions = self.normalize_offchain_positions(inventory.positions);

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OffchainEquity { positions },
            )
            .await?;

        let (gross_usd_cents, available_usd_cents) =
            compute_available_cash(inventory.usd_balance_cents, self.reserved_cash)?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OffchainUsd {
                    usd_balance_cents: available_usd_cents,
                    gross_usd_cents,
                },
            )
            .await?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OffchainCashBuyingPower {
                    cash_buying_power_cents: inventory.cash_buying_power_cents,
                },
            )
            .await?;

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::OffchainCashWithdrawable {
                    cash_withdrawable_cents: inventory.cash_withdrawable_cents,
                },
            )
            .await?;

        if let Some(usdc_balance) = inventory.alpaca_usdc {
            self.snapshot
                .send(
                    snapshot_id,
                    InventorySnapshotCommand::AlpacaUsdc { usdc_balance },
                )
                .await?;
        }

        Ok(())
    }

    fn normalize_offchain_positions(
        &self,
        broker_positions: Vec<st0x_execution::EquityPosition>,
    ) -> BTreeMap<Symbol, FractionalShares> {
        let Some(configured_symbols) = &self.configured_equity_symbols else {
            return broker_positions
                .into_iter()
                .map(|position| (position.symbol, position.quantity))
                .collect();
        };

        let mut positions: BTreeMap<_, _> = configured_symbols
            .iter()
            .map(|symbol| (symbol.clone(), FractionalShares::ZERO))
            .collect();

        let mut unconfigured_this_poll = HashSet::new();

        for position in broker_positions {
            if configured_symbols.contains(&position.symbol) {
                positions.insert(position.symbol, position.quantity);
            } else {
                // Warn once per contiguous appearance, re-warning only if the
                // symbol disappears and later returns (see the bounding retain
                // below). The broker may hold positions outside the configured
                // universe indefinitely, so warning every poll would spam logs.
                if self
                    .lock_unconfigured_symbol_warnings()
                    .insert(position.symbol.clone())
                {
                    warn!(
                        target: "inventory",
                        symbol = %position.symbol,
                        "Skipping unconfigured broker equity position"
                    );
                }

                unconfigured_this_poll.insert(position.symbol);
            }
        }

        // Bound the warn-once set to symbols still seen this poll so it cannot
        // grow without bound as broker positions appear and disappear.
        self.lock_unconfigured_symbol_warnings()
            .retain(|symbol| unconfigured_this_poll.contains(symbol));

        positions
    }

    fn lock_unconfigured_symbol_warnings(&self) -> MutexGuard<'_, HashSet<Symbol>> {
        self.unconfigured_symbol_warnings
            .lock()
            .unwrap_or_else(|poisoned| {
                warn!(
                    target: "inventory",
                    "Unconfigured symbol warning tracker was poisoned; recovering state"
                );
                poisoned.into_inner()
            })
    }

    fn warn_if_retired_equity_vault_has_balance(
        &self,
        symbol: &Symbol,
        token: Address,
        vault_id: B256,
        balance: FractionalShares,
    ) -> Result<(), FloatError> {
        let Some(configured_equity_vaults) = &self.configured_equity_vaults else {
            return Ok(());
        };

        if configured_equity_vaults
            .get(&token)
            .is_some_and(|configured_vaults| configured_vaults.contains(&vault_id))
            || balance.is_zero()?
        {
            return Ok(());
        }

        if self
            .lock_retired_equity_vault_warnings()
            .insert((token, vault_id))
        {
            warn!(
                target: "inventory",
                %symbol,
                %token,
                %vault_id,
                ?balance,
                "Registered equity vault has a positive balance but is no longer configured"
            );
        }

        Ok(())
    }

    fn lock_retired_equity_vault_warnings(&self) -> MutexGuard<'_, HashSet<(Address, B256)>> {
        self.retired_equity_vault_warnings
            .lock()
            .unwrap_or_else(|poisoned| {
                warn!(
                    target: "inventory",
                    "Retired equity vault warning tracker was poisoned; recovering state"
                );
                poisoned.into_inner()
            })
    }

    fn warn_if_retired_usdc_vault_has_balance(
        &self,
        vault_id: B256,
        balance: Usdc,
    ) -> Result<(), FloatError> {
        let Some(configured_usdc_vaults) = &self.configured_usdc_vaults else {
            return Ok(());
        };

        if configured_usdc_vaults.contains(&vault_id) || balance.is_zero()? {
            return Ok(());
        }

        if self.lock_retired_usdc_vault_warnings().insert(vault_id) {
            warn!(
                target: "inventory",
                %vault_id,
                ?balance,
                "Registered USDC vault has a positive balance but is no longer configured"
            );
        }

        Ok(())
    }

    fn lock_retired_usdc_vault_warnings(&self) -> MutexGuard<'_, HashSet<B256>> {
        self.retired_usdc_vault_warnings
            .lock()
            .unwrap_or_else(|poisoned| {
                warn!(
                    target: "inventory",
                    "Retired USDC vault warning tracker was poisoned; recovering state"
                );
                poisoned.into_inner()
            })
    }

    fn aggregate_pending_requests(
        requests: impl Iterator<Item = crate::tokenization::TokenizationRequest>,
    ) -> Result<PendingRequests, FloatError> {
        let mut mints: BTreeMap<Symbol, FractionalShares> = BTreeMap::new();
        let mut redemptions: BTreeMap<Symbol, FractionalShares> = BTreeMap::new();
        let mut seen = HashSet::new();

        for request in requests {
            if !seen.insert(request.id.clone()) {
                warn!(
                    target: "inventory",
                    request_id = %request.id,
                    symbol = %request.underlying_symbol,
                    "Duplicate pending tokenization request from provider, skipping"
                );
                continue;
            }

            let target = match request.r#type {
                Some(TokenizationRequestType::Mint) => &mut mints,
                Some(TokenizationRequestType::Redeem) => &mut redemptions,
                None => {
                    warn!(
                        target: "inventory",
                        request_id = %request.id,
                        symbol = %request.underlying_symbol,
                        "Pending tokenization request has no type, skipping"
                    );
                    continue;
                }
            };

            let entry = target
                .entry(request.underlying_symbol.clone())
                .or_insert(FractionalShares::ZERO);

            *entry = (*entry + request.quantity)?;
        }

        Ok(PendingRequests { mints, redemptions })
    }

    fn lock_external_pending_warnings(&self) -> MutexGuard<'_, HashSet<TokenizationRequestId>> {
        self.external_pending_warnings
            .lock()
            .unwrap_or_else(|poisoned| {
                warn!(
                    target: "inventory",
                    "External pending warning tracker was poisoned; recovering state"
                );
                poisoned.into_inner()
            })
    }

    fn is_own_request(
        &self,
        request: &crate::tokenization::TokenizationRequest,
        ownership: &PendingRequestOwnershipSnapshot,
    ) -> bool {
        let is_owned = match request.r#type {
            Some(TokenizationRequestType::Mint) => {
                request
                    .issuer_request_id
                    .as_ref()
                    .is_some_and(|id| ownership.mint_issuers.contains(id))
                    || ownership.mint_tokenizations.contains(&request.id)
            }
            Some(TokenizationRequestType::Redeem) => {
                ownership.redemption_tokenizations.contains(&request.id)
                    || request
                        .tx_hash
                        .is_some_and(|tx_hash| ownership.redemption_txs.contains(&tx_hash))
            }
            None => {
                // Dedup through the same warn-once set as external requests so a
                // persistently typeless provider row does not warn on every poll.
                if self
                    .lock_external_pending_warnings()
                    .insert(request.id.clone())
                {
                    warn!(
                        target: "inventory",
                        request_id = %request.id,
                        symbol = %request.underlying_symbol,
                        "Pending tokenization request has no type, skipping"
                    );
                }

                return false;
            }
        };

        if is_owned {
            return true;
        }

        if request
            .wallet
            .is_none_or(|wallet| wallet == self.snapshot_id.owner)
        {
            let should_warn = self
                .lock_external_pending_warnings()
                .insert(request.id.clone());

            if should_warn {
                warn!(
                    target: "inventory",
                    request_id = %request.id,
                    issuer_request_id = ?request.issuer_request_id,
                    request_type = ?request.r#type,
                    symbol = %request.underlying_symbol,
                    quantity = %request.quantity,
                    wallet = ?request.wallet,
                    expected_wallet = ?self.snapshot_id.owner,
                    "Ignoring external pending tokenization request"
                );
            }
        }

        false
    }

    async fn poll_inflight_equity(
        &self,
        snapshot_id: &InventorySnapshotId,
    ) -> Result<(), InventoryPollingError<Exe::Error>> {
        let Some(tokenizer) = &self.tokenizer else {
            debug!(target: "inventory", "No tokenizer configured, skipping inflight equity polling");
            return Ok(());
        };

        let fetched_at = Utc::now();
        let pending = tokenizer.list_pending_requests().await?;
        let ownership = if let Some(pending_request_ownership) = &self.pending_request_ownership {
            pending_request_ownership.pending_request_ownership().await
        } else {
            warn!(
                target: "inventory",
                "No pending request ownership source configured; treating provider pending requests as external"
            );
            PendingRequestOwnershipSnapshot::default()
        };

        let current_request_ids: HashSet<TokenizationRequestId> =
            pending.iter().map(|request| request.id.clone()).collect();

        let PendingRequests { mints, redemptions } = Self::aggregate_pending_requests(
            pending
                .into_iter()
                .filter(|request| self.is_own_request(request, &ownership)),
        )?;

        // Bound the warn-once dedup set to requests still present this poll so it
        // cannot grow without bound as external requests appear and complete.
        self.lock_external_pending_warnings()
            .retain(|id| current_request_ids.contains(id));

        debug!(
            target: "inventory",
            mint_symbols = mints.len(),
            redemption_symbols = redemptions.len(),
            "Polled inflight equity from tokenization provider"
        );

        self.snapshot
            .send(
                snapshot_id,
                InventorySnapshotCommand::InflightEquity {
                    mints,
                    redemptions,
                    fetched_at,
                },
            )
            .await?;

        Ok(())
    }
}

/// Computes gross and reserve-adjusted available cash in cents.
///
/// Returns `(gross_usd_cents, available_usd_cents)`. `gross_usd_cents` is
/// `None` when `reserved_cash` is zero (no reserve configured), so the
/// dashboard can hide reserve-specific context for no-reserve deployments.
///
/// When the broker balance is below the configured reserve, available cash is
/// explicitly zero. This is a valid capacity state: all Alpaca cash is protected
/// by the reserve, and downstream rebalancing should see no outbound capacity.
fn compute_available_cash(
    broker_usd_balance_cents: i64,
    reserved_cash: Usd,
) -> Result<(Option<i64>, i64), ReserveError> {
    let gross =
        Usd::from_cents(broker_usd_balance_cents).ok_or(ReserveError::BrokerCentsConversion {
            cents: broker_usd_balance_cents,
        })?;

    if gross < Usd::ZERO {
        return Err(ReserveError::NegativeBrokerBalance {
            cents: broker_usd_balance_cents,
        });
    }

    let available = if gross < reserved_cash {
        Usd::ZERO
    } else {
        (gross - reserved_cash)?
    };

    let gross_usd_cents = if reserved_cash > Usd::ZERO {
        Some(gross.to_cents()?)
    } else {
        None
    };
    let available_usd_cents = available.to_cents()?;

    Ok((gross_usd_cents, available_usd_cents))
}

/// Erases the `Chain` and `Exe` parameters of [`InventoryPollingService`]
/// so the apalis [`Job`] context isn't generic over them.
#[async_trait]
pub(crate) trait Poller: Send + Sync {
    async fn poll(&self) -> Result<(), PollerError>;
}

/// Boxed source error from a [`Poller`] implementation. Polling errors are
/// always logged and swallowed by the supervised inventory monitor; this
/// wrapper only erases the underlying executor-specific error type.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct PollerError(pub(crate) Box<dyn std::error::Error + Send + Sync>);

#[async_trait]
impl<Chain, Exe> Poller for InventoryPollingService<Chain, Exe>
where
    Chain: Evm + Send + Sync + 'static,
    Exe: Executor + Send + Sync + 'static,
    Exe::Error: std::error::Error + Send + Sync + 'static,
{
    async fn poll(&self) -> Result<(), PollerError> {
        Self::poll_and_record(self)
            .await
            .map_err(|error| PollerError(Box::new(error)))
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, Bytes, TxHash, U256, address, b256};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{Provider, ProviderBuilder, RootProvider};
    use alloy::rpc::client::RpcClient;
    use alloy::rpc::types::TransactionReceipt;
    use alloy::sol_types::SolValue;
    use async_trait::async_trait;
    use chrono::Utc;
    use httpmock::prelude::*;
    use sqlx::{Row, SqlitePool};
    use st0x_event_sorcery::test_store;
    use st0x_evm::ReadOnlyEvm;
    use st0x_execution::{EquityPosition, FractionalShares, Inventory, MockExecutor, Symbol};
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::snapshot::InventorySnapshotEvent;
    use crate::test_utils::setup_test_db;
    use crate::vault_registry::{VaultRegistry, VaultRegistryCommand};

    #[derive(Clone, Default)]
    struct TestPendingRequestOwnership(PendingRequestOwnershipSnapshot);

    #[async_trait]
    impl PendingRequestOwnership for TestPendingRequestOwnership {
        async fn pending_request_ownership(&self) -> PendingRequestOwnershipSnapshot {
            self.0.clone()
        }
    }

    fn ownership(
        mint_issuer_request_ids: impl IntoIterator<Item = &'static str>,
        mint_tokenization_request_ids: impl IntoIterator<Item = &'static str>,
        redemption_tokenization_request_ids: impl IntoIterator<Item = &'static str>,
        redemption_txs: impl IntoIterator<Item = TxHash>,
    ) -> Arc<dyn PendingRequestOwnership> {
        Arc::new(TestPendingRequestOwnership(
            PendingRequestOwnershipSnapshot {
                mint_issuers: mint_issuer_request_ids
                    .into_iter()
                    .map(IssuerRequestId::new)
                    .collect(),
                mint_tokenizations: mint_tokenization_request_ids
                    .into_iter()
                    .map(|id| TokenizationRequestId(id.to_string()))
                    .collect(),
                redemption_tokenizations: redemption_tokenization_request_ids
                    .into_iter()
                    .map(|id| TokenizationRequestId(id.to_string()))
                    .collect(),
                redemption_txs: redemption_txs.into_iter().collect(),
            },
        ))
    }

    struct MockEthereumWallet {
        address: Address,
        provider: RootProvider,
    }

    impl MockEthereumWallet {
        fn with_asserter(asserter: &Asserter) -> Arc<dyn Wallet<Provider = RootProvider>> {
            Arc::new(Self {
                address: address!("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"),
                provider: RootProvider::new(RpcClient::mocked(asserter.clone())),
            })
        }
    }

    struct MockBaseWallet {
        address: Address,
        provider: RootProvider,
    }

    impl MockBaseWallet {
        fn with_asserter(asserter: &Asserter) -> Arc<dyn Wallet<Provider = RootProvider>> {
            Arc::new(Self {
                address: address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                provider: RootProvider::new(RpcClient::mocked(asserter.clone())),
            })
        }
    }

    #[async_trait]
    impl Evm for MockEthereumWallet {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            &self.provider
        }
    }

    #[async_trait]
    impl Wallet for MockEthereumWallet {
        fn address(&self) -> Address {
            self.address
        }

        async fn send_pending(
            &self,
            _contract: Address,
            _calldata: Bytes,
            _note: &str,
        ) -> Result<TxHash, EvmError> {
            panic!("MockEthereumWallet::send_pending should not be called in polling tests")
        }

        async fn await_receipt(&self, _tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
            panic!("MockEthereumWallet::await_receipt should not be called in polling tests")
        }

        async fn send(
            &self,
            _contract: Address,
            _calldata: Bytes,
            _note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            panic!("MockEthereumWallet::send should not be called in polling tests")
        }
    }

    #[async_trait]
    impl Evm for MockBaseWallet {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            &self.provider
        }
    }

    #[async_trait]
    impl Wallet for MockBaseWallet {
        fn address(&self) -> Address {
            self.address
        }

        async fn send_pending(
            &self,
            _contract: Address,
            _calldata: Bytes,
            _note: &str,
        ) -> Result<TxHash, EvmError> {
            panic!("MockBaseWallet::send_pending should not be called in polling tests")
        }

        async fn await_receipt(&self, _tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
            panic!("MockBaseWallet::await_receipt should not be called in polling tests")
        }

        async fn send(
            &self,
            _contract: Address,
            _calldata: Bytes,
            _note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            panic!("MockBaseWallet::send should not be called in polling tests")
        }
    }

    fn zero_balance_wallet_asserter(response_count: usize) -> Asserter {
        let asserter = Asserter::new();
        let zero = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        for _ in 0..response_count {
            asserter.push_success(&zero);
        }
        asserter
    }

    /// Creates a fully-mocked `WalletPollingCtx` where all wallets return
    /// zero balances. The `server` must outlive the returned context. Each
    /// wallet gets enough responses for up to 5 consecutive `poll_and_record`
    /// calls.
    fn mock_wallet_polling_ctx(_server: &MockServer) -> WalletPollingCtx {
        let ethereum_asserter = zero_balance_wallet_asserter(5);
        let base_asserter = zero_balance_wallet_asserter(5);

        WalletPollingCtx {
            ethereum: MockEthereumWallet::with_asserter(&ethereum_asserter),
            base: MockBaseWallet::with_asserter(&base_asserter),
            unwrapped_equity_token_addresses: HashMap::new(),
            wrapped_equity_token_addresses: HashMap::new(),
        }
    }

    /// A Float (bytes32) representing zero balance, used as mock vaultBalance2 response.
    const ZERO_FLOAT_HEX: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000000";

    /// Creates a mock provider with no queued RPC responses.
    /// Any unexpected RPC call will fail immediately.
    fn mock_provider() -> impl Provider + Clone {
        let asserter = Asserter::new();
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    fn test_addresses() -> (Address, Address) {
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let order_owner = address!("0x2222222222222222222222222222222222222222");
        (orderbook, order_owner)
    }

    fn test_symbol(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn test_shares(n: i64) -> FractionalShares {
        FractionalShares::new(float!(&n.to_string()))
    }

    fn configured_symbols(symbols: &[&str]) -> HashSet<Symbol> {
        symbols.iter().map(|symbol| test_symbol(symbol)).collect()
    }

    fn configured_equity_vaults(
        token: Address,
        vault_ids: impl IntoIterator<Item = B256>,
    ) -> BTreeMap<Address, BTreeSet<B256>> {
        let mut configured = BTreeMap::new();
        configured.insert(token, vault_ids.into_iter().collect());
        configured
    }

    fn vault_balance_hex(balance: rain_math_float::Float) -> String {
        alloy::hex::encode_prefixed(balance.get_inner())
    }

    fn create_test_raindex_service(
        provider: impl Provider + Clone + 'static,
    ) -> Arc<RaindexService<ReadOnlyEvm<impl Provider + Clone + 'static>>> {
        Arc::new(RaindexService::new(
            ReadOnlyEvm::new(provider),
            Address::ZERO,
            Address::ZERO,
        ))
    }

    #[tokio::test]
    async fn poll_and_record_emits_offchain_equity_command_with_executor_positions() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![
                EquityPosition {
                    symbol: test_symbol("AAPL"),
                    quantity: test_shares(100),
                    market_value: Some(float!(15000)),
                },
                EquityPosition {
                    symbol: test_symbol("MSFT"),
                    quantity: test_shares(50),
                    market_value: Some(float!(20000)),
                },
            ],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory.clone());

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("Expected OffchainEquity event to be emitted");

        let InventorySnapshotEvent::OffchainEquity { positions, .. } = offchain_equity_event else {
            panic!("Expected OffchainEquity event, got {offchain_equity_event:?}");
        };
        assert_eq!(positions.len(), 2, "Expected 2 positions");
        assert_eq!(
            positions.get(&test_symbol("AAPL")),
            Some(&test_shares(100)),
            "AAPL position mismatch"
        );
        assert_eq!(
            positions.get(&test_symbol("MSFT")),
            Some(&test_shares(50)),
            "MSFT position mismatch"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_zero_for_configured_position_absent_from_executor() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![EquityPosition {
                symbol: test_symbol("AAPL"),
                quantity: test_shares(100),
                market_value: Some(float!(15000)),
            }],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        )
        .with_configured_equity_symbols(configured_symbols(&["AAPL", "SPYM"]));

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("Expected OffchainEquity event to be emitted");

        let InventorySnapshotEvent::OffchainEquity { positions, .. } = offchain_equity_event else {
            panic!("Expected OffchainEquity event, got {offchain_equity_event:?}");
        };
        assert_eq!(
            positions.get(&test_symbol("AAPL")),
            Some(&test_shares(100)),
            "present configured broker position must preserve quantity"
        );
        assert_eq!(
            positions.get(&test_symbol("SPYM")),
            Some(&FractionalShares::ZERO),
            "absent configured broker position must be normalized to zero"
        );
    }

    #[tokio::test]
    async fn poll_and_record_ignores_executor_positions_not_in_configured_symbols() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![
                EquityPosition {
                    symbol: test_symbol("AAPL"),
                    quantity: test_shares(100),
                    market_value: Some(float!(15000)),
                },
                EquityPosition {
                    symbol: test_symbol("UNKNOWN"),
                    quantity: test_shares(50),
                    market_value: Some(float!(5000)),
                },
            ],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        )
        .with_configured_equity_symbols(configured_symbols(&["AAPL"]));

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("Expected OffchainEquity event to be emitted");

        let InventorySnapshotEvent::OffchainEquity { positions, .. } = offchain_equity_event else {
            panic!("Expected OffchainEquity event, got {offchain_equity_event:?}");
        };
        assert_eq!(
            positions.len(),
            1,
            "Only configured active symbols should be emitted"
        );
        assert_eq!(
            positions.get(&test_symbol("AAPL")),
            Some(&test_shares(100)),
            "configured position mismatch"
        );
        assert!(
            !positions.contains_key(&test_symbol("UNKNOWN")),
            "unconfigured broker-only position must not be added to offchain equity"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_offchain_usd_command_with_executor_usd_balance() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 25_000_000, // $250,000.00
            cash_buying_power_cents: Some(25_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_usd_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainUsd { .. }))
            .expect("Expected OffchainUsd event to be emitted");

        let InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents, ..
        } = offchain_usd_event
        else {
            panic!("Expected OffchainUsd event, got {offchain_usd_event:?}");
        };
        assert_eq!(
            *usd_balance_cents, 25_000_000,
            "Cash balance mismatch: expected $250,000.00"
        );
    }

    #[tokio::test]
    async fn reserved_cash_subtracted_from_offchain_usd() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 10_000_000, // $100,000
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let reserved = Usd::new(float!(50000)); // $50,000 reserved

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            reserved,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_usd_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainUsd { .. }))
            .expect("Expected OffchainUsd event");

        let InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents,
            gross_usd_cents,
            ..
        } = offchain_usd_event
        else {
            panic!("Expected OffchainUsd event");
        };

        assert_eq!(
            *usd_balance_cents, 5_000_000,
            "Available balance should be $100k - $50k reserved = $50k ($5,000,000 cents)"
        );
        assert_eq!(
            *gross_usd_cents,
            Some(10_000_000),
            "Gross balance should be the full broker balance ($10,000,000 cents)"
        );
    }

    #[tokio::test]
    async fn reserved_cash_larger_than_balance_sets_available_to_zero() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 3_000_000, // $30,000
            cash_buying_power_cents: Some(3_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let reserved = Usd::new(float!(50000)); // $50,000 reserved > balance

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            reserved,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_usd_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainUsd { .. }))
            .expect("Expected OffchainUsd event");

        let InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents,
            gross_usd_cents,
            ..
        } = offchain_usd_event
        else {
            panic!("Expected OffchainUsd event");
        };

        assert_eq!(
            *usd_balance_cents, 0,
            "Reserve-adjusted available cash should be zero when gross is below reserve"
        );
        assert_eq!(
            *gross_usd_cents,
            Some(3_000_000),
            "Gross balance should still be emitted so the dashboard can show cash below reserve"
        );
    }

    #[tokio::test]
    async fn zero_reserved_cash_passes_through_full_balance() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_usd_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainUsd { .. }))
            .expect("Expected OffchainUsd event");

        let InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents,
            gross_usd_cents,
            ..
        } = offchain_usd_event
        else {
            panic!("Expected OffchainUsd event");
        };

        assert_eq!(
            *usd_balance_cents, 10_000_000,
            "With zero reserve, full balance should pass through"
        );
        assert!(
            gross_usd_cents.is_none(),
            "With zero reserve, gross should be None (no reserve configured)"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_empty_positions_when_executor_has_no_positions() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 5_000_000,
            cash_buying_power_cents: Some(5_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("Expected OffchainEquity event even with empty positions");

        let InventorySnapshotEvent::OffchainEquity { positions, .. } = offchain_equity_event else {
            panic!("Expected OffchainEquity event, got {offchain_equity_event:?}");
        };
        assert!(positions.is_empty(), "Expected empty positions map");
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_offchain_commands_when_executor_returns_unimplemented() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        // Should succeed without error
        service.poll_and_record().await.unwrap();

        // Verify NO offchain events were emitted
        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_offchain_equity = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }));
        let has_offchain_usd = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OffchainUsd { .. }));

        assert!(
            !has_offchain_equity,
            "Should NOT emit OffchainEquity when executor returns Unimplemented"
        );
        assert!(
            !has_offchain_usd,
            "Should NOT emit OffchainUsd when executor returns Unimplemented"
        );
        assert!(
            logs_contain(
                "Executor returned non-fetched inventory result, skipping offchain polling"
            ),
            "Should log debug message explaining why offchain polling was skipped"
        );
    }

    #[tokio::test]
    async fn negative_usd_balance_errors() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        // Margin account with negative cash (borrowed funds) — even with
        // zero reserve, negative gross balance is rejected by NonNegative.
        let inventory = Inventory {
            positions: vec![EquityPosition {
                symbol: test_symbol("AAPL"),
                quantity: test_shares(1000),
                market_value: Some(float!(150000)),
            }],
            usd_balance_cents: -5_000_000, // -$50,000 (margin debt)
            cash_buying_power_cents: Some(0),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let error = service.poll_offchain(&snapshot_id).await.unwrap_err();

        assert!(
            matches!(error, InventoryPollingError::Reserve(_)),
            "Negative broker balance should produce Reserve error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_and_record_handles_fractional_share_positions() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let fractional_qty = FractionalShares::new(float!(12.345)); // 12.345 shares
        let inventory = Inventory {
            positions: vec![EquityPosition {
                symbol: test_symbol("AAPL"),
                quantity: fractional_qty,
                market_value: Some(float!(1851.75)),
            }],
            usd_balance_cents: 1_000_000,
            cash_buying_power_cents: Some(1_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let offchain_equity_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("Expected OffchainEquity event");

        let InventorySnapshotEvent::OffchainEquity { positions, .. } = offchain_equity_event else {
            panic!("Expected OffchainEquity event, got {offchain_equity_event:?}");
        };
        assert_eq!(
            positions.get(&test_symbol("AAPL")),
            Some(&fractional_qty),
            "Should preserve fractional share quantity"
        );
    }

    #[tokio::test]
    async fn poll_and_record_uses_correct_aggregate_id() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let orderbook = address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let order_owner = address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");

        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 10_000,
            cash_buying_power_cents: Some(10_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };
        let executor = MockExecutor::new().with_inventory(inventory);

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let expected_aggregate_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        }
        .to_string();
        let events = load_events_for_aggregate(&pool, &expected_aggregate_id).await;

        assert!(
            !events.is_empty(),
            "Expected events under aggregate ID {expected_aggregate_id}"
        );
    }

    const TEST_TOKEN: Address = address!("0x9876543210987654321098765432109876543210");
    const TEST_VAULT_ID: B256 =
        b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
    const TEST_TX_HASH: TxHash =
        b256!("0x1111111111111111111111111111111111111111111111111111111111111111");

    async fn discover_equity_vault(
        pool: &SqlitePool,
        orderbook: Address,
        order_owner: Address,
        token: Address,
        vault_id: B256,
        symbol: Symbol,
    ) {
        let store = test_store::<VaultRegistry>(pool.clone(), ());
        let vault_registry_id = VaultRegistryId {
            orderbook,
            owner: order_owner,
        };

        store
            .send(
                &vault_registry_id,
                VaultRegistryCommand::DiscoverEquityVault {
                    token,
                    vault_id,
                    discovered_in: TEST_TX_HASH,
                    symbol,
                },
            )
            .await
            .unwrap();
    }

    async fn discover_usdc_vault(
        pool: &SqlitePool,
        orderbook: Address,
        order_owner: Address,
        vault_id: B256,
    ) {
        let store = test_store::<VaultRegistry>(pool.clone(), ());
        let vault_registry_id = VaultRegistryId {
            orderbook,
            owner: order_owner,
        };

        store
            .send(
                &vault_registry_id,
                VaultRegistryCommand::DiscoverUsdcVault {
                    vault_id,
                    discovered_in: TEST_TX_HASH,
                },
            )
            .await
            .unwrap();
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_onchain_when_vault_registry_not_initialized() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_onchain_equity = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OnchainEquity { .. }));
        let has_onchain_usdc = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OnchainUsdc { .. }));

        assert!(
            !has_onchain_equity,
            "Should NOT emit OnchainEquity when VaultRegistry not initialized"
        );
        assert!(
            !has_onchain_usdc,
            "Should NOT emit OnchainUsdc when VaultRegistry not initialized"
        );
        assert!(
            logs_contain("Vault registry not initialized, skipping onchain polling"),
            "Should log debug message explaining why onchain polling was skipped"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_onchain_equity_when_no_equity_vaults_discovered() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_usdc_vault(&pool, orderbook, order_owner, TEST_VAULT_ID).await;

        let asserter = Asserter::new();
        asserter.push_success(&ZERO_FLOAT_HEX); // vaultBalance2 for USDC vault
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(provider);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_onchain_equity = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OnchainEquity { .. }));

        assert!(
            !has_onchain_equity,
            "Should NOT emit OnchainEquity when no equity vaults discovered"
        );
        assert!(
            logs_contain("No equity vaults discovered, skipping onchain equity polling"),
            "Should log debug message explaining why equity polling was skipped"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_onchain_usdc_when_no_usdc_vault_discovered() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_equity_vault(
            &pool,
            orderbook,
            order_owner,
            TEST_TOKEN,
            TEST_VAULT_ID,
            test_symbol("AAPL"),
        )
        .await;

        let asserter = Asserter::new();
        asserter.push_success(&ZERO_FLOAT_HEX); // vaultBalance2 for equity vault
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(provider);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_onchain_usdc = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::OnchainUsdc { .. }));

        assert!(
            !has_onchain_usdc,
            "Should NOT emit OnchainUsdc when no USDC vault discovered"
        );
        assert!(
            logs_contain("No USDC vaults discovered, skipping onchain cash polling"),
            "Should log debug message explaining why cash polling was skipped"
        );
    }

    #[tokio::test]
    async fn poll_onchain_fails_on_rpc_failure_for_equity_vault() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_equity_vault(
            &pool,
            orderbook,
            order_owner,
            TEST_TOKEN,
            TEST_VAULT_ID,
            test_symbol("AAPL"),
        )
        .await;

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure"); // vaultBalance2 for equity vault
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(provider);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let error = service.poll_onchain(&snapshot_id).await.unwrap_err();

        assert!(
            matches!(error, InventoryPollingError::Raindex(_)),
            "Expected Vault error when RPC fails, got {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_onchain_fails_on_rpc_failure_for_usdc_vault() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_usdc_vault(&pool, orderbook, order_owner, TEST_VAULT_ID).await;

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure"); // vaultBalance2 for USDC vault
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(provider);

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let error = service.poll_onchain(&snapshot_id).await.unwrap_err();

        assert!(
            matches!(error, InventoryPollingError::Raindex(_)),
            "Expected Vault error when RPC fails, got {error:?}"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_onchain_warns_when_retired_equity_vault_has_positive_balance() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();
        let retired_vault_id = TEST_VAULT_ID;
        let configured_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        discover_equity_vault(
            &pool,
            orderbook,
            order_owner,
            TEST_TOKEN,
            retired_vault_id,
            test_symbol("SGOV"),
        )
        .await;
        discover_equity_vault(
            &pool,
            orderbook,
            order_owner,
            TEST_TOKEN,
            configured_vault_id,
            test_symbol("SGOV"),
        )
        .await;

        let retired_balance = vault_balance_hex(float!(5));
        let asserter = Asserter::new();
        asserter.push_success(&retired_balance);
        asserter.push_success(&ZERO_FLOAT_HEX);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(provider);

        let service = InventoryPollingService::new(
            raindex_service,
            MockExecutor::new(),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        )
        .with_configured_vaults(
            configured_equity_vaults(TEST_TOKEN, [configured_vault_id]),
            None,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        service.poll_onchain(&snapshot_id).await.unwrap();

        assert!(
            logs_contain(
                "Registered equity vault has a positive balance but is no longer configured"
            ),
            "Should warn when a retired equity vault still holds funds"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_onchain_warns_when_retired_usdc_vault_has_positive_balance() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();
        let retired_vault_id = TEST_VAULT_ID;
        let configured_vault_id =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        discover_usdc_vault(&pool, orderbook, order_owner, retired_vault_id).await;
        discover_usdc_vault(&pool, orderbook, order_owner, configured_vault_id).await;

        let retired_balance = vault_balance_hex(float!(125));
        let asserter = Asserter::new();
        asserter.push_success(&retired_balance);
        asserter.push_success(&ZERO_FLOAT_HEX);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(provider);

        let service = InventoryPollingService::new(
            raindex_service,
            MockExecutor::new(),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        )
        .with_configured_vaults(BTreeMap::new(), Some(BTreeSet::from([configured_vault_id])));

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        service.poll_onchain(&snapshot_id).await.unwrap();

        assert!(
            logs_contain(
                "Registered USDC vault has a positive balance but is no longer configured"
            ),
            "Should warn when a retired USDC vault still holds funds"
        );
    }

    #[tokio::test]
    async fn poll_and_record_continues_to_offchain_after_onchain_failure() {
        let pool = setup_test_db().await;
        let (orderbook, order_owner) = test_addresses();

        discover_usdc_vault(&pool, orderbook, order_owner, TEST_VAULT_ID).await;

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let raindex_service = create_test_raindex_service(provider);
        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: Some(Usdc::new(float!(0.788514))),
            cash_withdrawable_cents: None,
        };

        let service = InventoryPollingService::new(
            raindex_service,
            MockExecutor::new().with_inventory(inventory),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, InventorySnapshotEvent::OffchainUsd { .. })),
            "Offchain inventory must still emit when onchain polling fails"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, InventorySnapshotEvent::AlpacaUsdc { .. })),
            "Alpaca USDC must still emit when onchain polling fails"
        );
    }

    #[tokio::test]
    async fn poll_offchain_emits_alpaca_usdc_from_executor_inventory() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();
        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };

        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: Some(Usdc::new(float!(125.75))),
            cash_withdrawable_cents: None,
        };

        let service = InventoryPollingService::new(
            raindex_service,
            MockExecutor::new().with_inventory(inventory),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            snapshot_id.clone(),
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_offchain(&snapshot_id).await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let alpaca_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::AlpacaUsdc { .. }))
            .expect("Expected AlpacaUsdc event to be emitted");

        let InventorySnapshotEvent::AlpacaUsdc { usdc_balance, .. } = alpaca_event else {
            panic!("Expected AlpacaUsdc event, got {alpaca_event:?}");
        };
        assert_eq!(*usdc_balance, Usdc::new(float!(125.75)));
    }

    #[tokio::test]
    async fn poll_offchain_skips_alpaca_usdc_when_executor_omits_it() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();
        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: None,
            cash_withdrawable_cents: None,
        };

        let service = InventoryPollingService::new(
            raindex_service,
            MockExecutor::new().with_inventory(inventory),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            snapshot_id.clone(),
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_offchain(&snapshot_id).await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, InventorySnapshotEvent::AlpacaUsdc { .. })),
            "Must not emit AlpacaUsdc when executor omits Alpaca USDC",
        );
    }

    #[tokio::test]
    async fn poll_and_record_aborts_tick_when_inflight_poll_fails() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        // Inventory is present, so the offchain group would emit balance
        // snapshots if the tick were allowed to continue past the failed
        // inflight poll.
        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: Some(Usdc::new(float!(125.75))),
            cash_withdrawable_cents: None,
        };

        let tokenizer =
            Arc::new(crate::tokenization::mock::MockTokenizer::new().with_list_pending_failure());

        let service = InventoryPollingService::new(
            raindex_service,
            MockExecutor::new().with_inventory(inventory),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            Some(tokenizer),
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        assert!(
            events.is_empty(),
            "Inflight poll failure must abort the tick before any balance \
             snapshot is emitted, but got: {events:?}"
        );
    }

    async fn load_snapshot_events(
        pool: &SqlitePool,
        orderbook: Address,
        order_owner: Address,
    ) -> Vec<InventorySnapshotEvent> {
        let aggregate_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        }
        .to_string();
        load_events_for_aggregate(pool, &aggregate_id).await
    }

    /// Loads InventorySnapshot events for a specific aggregate ID from the SQLite event store.
    async fn load_events_for_aggregate(
        pool: &SqlitePool,
        aggregate_id: &str,
    ) -> Vec<InventorySnapshotEvent> {
        let rows = sqlx::query(
            r"
            SELECT payload
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = 'InventorySnapshot'
            ORDER BY sequence ASC
            ",
        )
        .bind(aggregate_id)
        .fetch_all(pool)
        .await
        .unwrap();

        rows.iter()
            .map(|row| {
                let payload: String = row.get("payload");
                serde_json::from_str(&payload).unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn poll_and_record_emits_ethereum_usdc_when_wallet_provided() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let raw_usdc = U256::from(5_000_000_000u64); // 5000 USDC (6 decimals)
        let asserter = Asserter::new();
        let encoded = alloy::hex::encode_prefixed(raw_usdc.abi_encode());
        asserter.push_success(&encoded);
        let ethereum_wallet = MockEthereumWallet::with_asserter(&asserter);

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                ethereum: ethereum_wallet,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let ethereum_usdc_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::EthereumUsdc { .. }))
            .expect("Expected EthereumUsdc event to be emitted");

        let InventorySnapshotEvent::EthereumUsdc { usdc_balance, .. } = ethereum_usdc_event else {
            panic!("Expected EthereumUsdc event, got {ethereum_usdc_event:?}");
        };
        let expected = u256_to_usdc(raw_usdc).unwrap();
        assert_eq!(*usdc_balance, expected, "USDC balance mismatch");
    }

    #[tokio::test]
    async fn poll_and_record_emits_base_wallet_usdc_when_wallet_provided() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let raw_usdc = U256::from(5_000_000_000u64); // 5000 USDC (6 decimals)
        let asserter = Asserter::new();
        let encoded = alloy::hex::encode_prefixed(raw_usdc.abi_encode());
        asserter.push_success(&encoded);
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let base_wallet_usdc_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::BaseWalletUsdc { .. }))
            .expect("Expected BaseWalletUsdc event to be emitted");

        let InventorySnapshotEvent::BaseWalletUsdc { usdc_balance, .. } = base_wallet_usdc_event
        else {
            panic!("Expected BaseWalletUsdc event, got {base_wallet_usdc_event:?}");
        };
        let expected = u256_to_usdc(raw_usdc).unwrap();
        assert_eq!(*usdc_balance, expected, "USDC balance mismatch");
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_ethereum_usdc_when_no_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_ethereum_usdc = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::EthereumUsdc { .. }));

        assert!(
            !has_ethereum_usdc,
            "Should NOT emit EthereumUsdc when no Ethereum wallet configured"
        );
        assert!(
            logs_contain("No wallet polling configured, skipping wallet balance polling"),
            "Should log debug message explaining why wallet polling was skipped"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_base_wallet_usdc_when_no_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_base_wallet_usdc = events
            .iter()
            .any(|event| matches!(event, InventorySnapshotEvent::BaseWalletUsdc { .. }));

        assert!(
            !has_base_wallet_usdc,
            "Should NOT emit BaseWalletUsdc when no Base wallet configured"
        );
        assert!(
            logs_contain("No wallet polling configured, skipping wallet balance polling"),
            "Should log debug message explaining why wallet polling was skipped"
        );
    }

    #[tokio::test]
    async fn poll_wallets_propagates_ethereum_rpc_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        asserter.push_failure_msg("Ethereum RPC failure");
        let ethereum_wallet = MockEthereumWallet::with_asserter(&asserter);

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                ethereum: ethereum_wallet,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let error = service.poll_wallets(&snapshot_id).await.unwrap_err();
        assert!(matches!(error, InventoryPollingError::Evm(_)));
    }

    #[tokio::test]
    async fn poll_wallets_propagates_base_wallet_rpc_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        asserter.push_failure_msg("Base RPC failure");
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let error = service.poll_wallets(&snapshot_id).await.unwrap_err();
        assert!(matches!(error, InventoryPollingError::Evm(_)));
    }

    #[tokio::test]
    async fn poll_and_record_continues_to_offchain_after_wallet_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        asserter.push_failure_msg("Ethereum RPC failure");
        let ethereum_wallet = MockEthereumWallet::with_asserter(&asserter);

        let server = MockServer::start();
        let inventory = Inventory {
            positions: vec![],
            usd_balance_cents: 10_000_000,
            cash_buying_power_cents: Some(10_000_000),
            alpaca_usdc: Some(Usdc::new(float!(0.788514))),
            cash_withdrawable_cents: None,
        };

        let service = InventoryPollingService::new(
            raindex_service,
            MockExecutor::new().with_inventory(inventory),
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                ethereum: ethereum_wallet,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, InventorySnapshotEvent::OffchainUsd { .. })),
            "Offchain inventory must still emit when wallet polling fails"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, InventorySnapshotEvent::AlpacaUsdc { .. })),
            "Alpaca USDC must still emit when wallet polling fails"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_base_wallet_unwrapped_equity_when_wallet_and_tokens_provided() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let token_addr = address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let raw_balance = U256::from(500_000u64) * U256::from(10u64).pow(U256::from(18u64)); // 500,000 tokens (18 decimals)
        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        asserter.push_success(&usdc_encoded); // USDC balanceOf (zero)
        let equity_encoded = alloy::hex::encode_prefixed(raw_balance.abi_encode());
        asserter.push_success(&equity_encoded); // equity token balanceOf
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut equity_tokens = HashMap::new();
        equity_tokens.insert(test_symbol("AAPL"), token_addr);

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                unwrapped_equity_token_addresses: equity_tokens,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let equity_event = events
            .iter()
            .find(|event| {
                matches!(
                    event,
                    InventorySnapshotEvent::BaseWalletUnwrappedEquity { .. }
                )
            })
            .expect("Expected BaseWalletUnwrappedEquity event to be emitted");

        let InventorySnapshotEvent::BaseWalletUnwrappedEquity { balances, .. } = equity_event
        else {
            panic!("Expected BaseWalletUnwrappedEquity event, got {equity_event:?}");
        };
        let expected = FractionalShares::from_u256_18_decimals(raw_balance).unwrap();
        assert_eq!(
            balances.get(&test_symbol("AAPL")),
            Some(&expected),
            "AAPL balance mismatch"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_base_wallet_unwrapped_equity_when_no_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_equity = events.iter().any(|event| {
            matches!(
                event,
                InventorySnapshotEvent::BaseWalletUnwrappedEquity { .. }
            )
        });

        assert!(
            !has_equity,
            "Should NOT emit BaseWalletUnwrappedEquity when no wallet polling configured"
        );
        assert!(logs_contain(
            "No wallet polling configured, skipping wallet balance polling"
        ));
    }

    #[tokio::test]
    async fn poll_wallets_propagates_base_wallet_unwrapped_equity_rpc_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        asserter.push_success(&usdc_encoded); // USDC balanceOf succeeds
        asserter.push_failure_msg("Equity RPC failure"); // equity balanceOf fails
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut equity_tokens = HashMap::new();
        equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                unwrapped_equity_token_addresses: equity_tokens,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let error = service.poll_wallets(&snapshot_id).await.unwrap_err();
        assert!(
            matches!(error, InventoryPollingError::Evm(_)),
            "Expected Evm error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_wallets_propagates_base_wallet_unwrapped_equity_shares_conversion_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        let equity_encoded = alloy::hex::encode_prefixed(U256::MAX.abi_encode());
        asserter.push_success(&usdc_encoded); // USDC balanceOf succeeds
        asserter.push_success(&equity_encoded); // equity balanceOf overflows conversion
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut equity_tokens = HashMap::new();
        equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                unwrapped_equity_token_addresses: equity_tokens,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let error = service.poll_wallets(&snapshot_id).await.unwrap_err();
        assert!(
            matches!(
                error,
                InventoryPollingError::SharesConversion(SharesConversionError::FloatConversion(_))
            ),
            "Expected shares conversion error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_base_wallet_wrapped_equity_when_wallet_and_tokens_provided() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let token_addr = address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        let raw_balance = U256::from(250_000u64) * U256::from(10u64).pow(U256::from(18u64));
        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        asserter.push_success(&usdc_encoded); // USDC balanceOf (zero)
        let equity_encoded = alloy::hex::encode_prefixed(raw_balance.abi_encode());
        asserter.push_success(&equity_encoded); // wrapped equity token balanceOf
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(test_symbol("AAPL"), token_addr);

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                wrapped_equity_token_addresses: wrapped_equity_tokens,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let wrapped_equity_event = events
            .iter()
            .find(|event| {
                matches!(
                    event,
                    InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
                )
            })
            .expect("Expected BaseWalletWrappedEquity event to be emitted");

        let InventorySnapshotEvent::BaseWalletWrappedEquity { balances, .. } = wrapped_equity_event
        else {
            panic!("Expected BaseWalletWrappedEquity event, got {wrapped_equity_event:?}");
        };
        let expected = FractionalShares::from_u256_18_decimals(raw_balance).unwrap();
        assert_eq!(
            balances.get(&test_symbol("AAPL")),
            Some(&expected),
            "AAPL wrapped balance mismatch"
        );
    }

    #[tokio::test]
    async fn poll_and_record_emits_base_wallet_wrapped_equity_for_multiple_assets() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let raw_balance = U256::from(42u64) * U256::from(10u64).pow(U256::from(18u64));
        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        let wrapped_equity_encoded = alloy::hex::encode_prefixed(raw_balance.abi_encode());
        asserter.push_success(&usdc_encoded); // USDC balanceOf (zero)
        asserter.push_success(&wrapped_equity_encoded); // first wrapped token balanceOf
        asserter.push_success(&wrapped_equity_encoded); // second wrapped token balanceOf
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        );
        wrapped_equity_tokens.insert(
            test_symbol("TSLA"),
            address!("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
        );

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                wrapped_equity_token_addresses: wrapped_equity_tokens,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let wrapped_equity_event = events
            .iter()
            .find(|event| {
                matches!(
                    event,
                    InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
                )
            })
            .expect("Expected BaseWalletWrappedEquity event to be emitted");

        let InventorySnapshotEvent::BaseWalletWrappedEquity { balances, .. } = wrapped_equity_event
        else {
            panic!("Expected BaseWalletWrappedEquity event, got {wrapped_equity_event:?}");
        };
        let expected = FractionalShares::from_u256_18_decimals(raw_balance).unwrap();
        assert_eq!(balances.get(&test_symbol("AAPL")), Some(&expected));
        assert_eq!(balances.get(&test_symbol("TSLA")), Some(&expected));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_and_record_skips_base_wallet_wrapped_equity_when_no_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let has_wrapped_equity = events.iter().any(|event| {
            matches!(
                event,
                InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
            )
        });

        assert!(
            !has_wrapped_equity,
            "Should NOT emit BaseWalletWrappedEquity when no wallet polling configured"
        );
        assert!(logs_contain(
            "No wallet polling configured, skipping wallet balance polling"
        ));
    }

    #[tokio::test]
    async fn poll_wallets_propagates_base_wallet_wrapped_equity_rpc_failure() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        asserter.push_success(&usdc_encoded); // USDC balanceOf succeeds
        asserter.push_failure_msg("Wrapped equity RPC failure"); // wrapped equity balanceOf fails
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        );

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                wrapped_equity_token_addresses: wrapped_equity_tokens,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        let snapshot_id = InventorySnapshotId {
            orderbook,
            owner: order_owner,
        };
        let error = service.poll_wallets(&snapshot_id).await.unwrap_err();
        assert!(
            matches!(error, InventoryPollingError::Evm(_)),
            "Expected Evm error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_and_record_skips_unchanged_base_wallet_wrapped_equity_snapshot() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let raw_balance = U256::from(3u64) * U256::from(10u64).pow(U256::from(18u64));
        let asserter = Asserter::new();
        let usdc_encoded = alloy::hex::encode_prefixed(U256::ZERO.abi_encode());
        let wrapped_equity_encoded = alloy::hex::encode_prefixed(raw_balance.abi_encode());
        asserter.push_success(&usdc_encoded); // first poll USDC balanceOf
        asserter.push_success(&wrapped_equity_encoded); // first poll wrapped balanceOf
        asserter.push_success(&usdc_encoded); // second poll USDC balanceOf
        asserter.push_success(&wrapped_equity_encoded); // second poll wrapped balanceOf
        let base_wallet = MockBaseWallet::with_asserter(&asserter);

        let mut wrapped_equity_tokens = HashMap::new();
        wrapped_equity_tokens.insert(
            test_symbol("AAPL"),
            address!("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        );

        let server = MockServer::start();
        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            Some(WalletPollingCtx {
                base: base_wallet,
                wrapped_equity_token_addresses: wrapped_equity_tokens,
                ..mock_wallet_polling_ctx(&server)
            }),
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();
        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let wrapped_equity_event_count = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
                )
            })
            .count();

        assert_eq!(
            wrapped_equity_event_count, 1,
            "Should not emit a second BaseWalletWrappedEquity event when the balance is unchanged"
        );
    }

    #[tokio::test]
    async fn poll_and_record_skips_token_snapshots_when_token_addresses_empty() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;

        let has_unwrapped = events.iter().any(|event| {
            matches!(
                event,
                InventorySnapshotEvent::BaseWalletUnwrappedEquity { .. }
            )
        });

        let has_wrapped = events.iter().any(|event| {
            matches!(
                event,
                InventorySnapshotEvent::BaseWalletWrappedEquity { .. }
            )
        });

        assert!(
            !has_unwrapped,
            "Should not emit BaseWalletUnwrappedEquity when no token addresses are configured"
        );
        assert!(
            !has_wrapped,
            "Should not emit BaseWalletWrappedEquity when no token addresses are configured"
        );
    }

    fn mock_pending_request(
        request_type: crate::tokenization::TokenizationRequestType,
        symbol: &str,
        quantity: i64,
    ) -> crate::tokenization::TokenizationRequest {
        mock_pending_request_with_wallet(request_type, symbol, quantity, None)
    }

    fn mock_pending_request_no_type(
        symbol: &str,
        quantity: i64,
    ) -> crate::tokenization::TokenizationRequest {
        crate::tokenization::TokenizationRequest {
            id: TokenizationRequestId(format!("REQ_{symbol}_{quantity}_notype")),
            r#type: None,
            status: crate::tokenization::TokenizationRequestStatus::Pending,
            underlying_symbol: test_symbol(symbol),
            quantity: test_shares(quantity),
            wallet: None,
            issuer_request_id: None,
            tx_hash: None,
            token_symbol: None,
            fees: None,
            created_at: Utc::now(),
        }
    }

    fn mock_pending_request_with_wallet(
        request_type: crate::tokenization::TokenizationRequestType,
        symbol: &str,
        quantity: i64,
        wallet: Option<Address>,
    ) -> crate::tokenization::TokenizationRequest {
        crate::tokenization::TokenizationRequest {
            id: TokenizationRequestId(format!("REQ_{symbol}_{quantity}")),
            r#type: Some(request_type),
            status: crate::tokenization::TokenizationRequestStatus::Pending,
            underlying_symbol: test_symbol(symbol),
            quantity: test_shares(quantity),
            wallet,
            issuer_request_id: None,
            tx_hash: None,
            token_symbol: None,
            fees: None,
            created_at: Utc::now(),
        }
    }

    fn mock_pending_request_with_issuer_id(
        request_type: crate::tokenization::TokenizationRequestType,
        symbol: &str,
        quantity: i64,
        issuer_request_id: &str,
        wallet: Option<Address>,
    ) -> crate::tokenization::TokenizationRequest {
        crate::tokenization::TokenizationRequest {
            issuer_request_id: Some(IssuerRequestId::new(issuer_request_id)),
            ..mock_pending_request_with_wallet(request_type, symbol, quantity, wallet)
        }
    }

    fn mock_pending_request_with_tx_hash(
        request_type: crate::tokenization::TokenizationRequestType,
        symbol: &str,
        quantity: i64,
        tx_hash: TxHash,
        wallet: Option<Address>,
    ) -> crate::tokenization::TokenizationRequest {
        crate::tokenization::TokenizationRequest {
            tx_hash: Some(tx_hash),
            ..mock_pending_request_with_wallet(request_type, symbol, quantity, wallet)
        }
    }

    #[tokio::test]
    async fn poll_inflight_equity_emits_mints_and_redemptions() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let tokenizer = Arc::new(
            crate::tokenization::mock::MockTokenizer::new().with_pending_requests(vec![
                mock_pending_request(
                    crate::tokenization::TokenizationRequestType::Mint,
                    "AAPL",
                    10,
                ),
                mock_pending_request(
                    crate::tokenization::TokenizationRequestType::Redeem,
                    "MSFT",
                    5,
                ),
            ]),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            Some(tokenizer),
            Usd::ZERO,
        )
        .with_pending_request_ownership(ownership([], ["REQ_AAPL_10"], ["REQ_MSFT_5"], []));

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let inflight_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }))
            .expect("Expected InflightEquity event");

        let InventorySnapshotEvent::InflightEquity {
            mints, redemptions, ..
        } = inflight_event
        else {
            panic!("Expected InflightEquity event");
        };

        assert_eq!(mints.len(), 1);
        assert_eq!(mints.get(&test_symbol("AAPL")), Some(&test_shares(10)));
        assert_eq!(redemptions.len(), 1);
        assert_eq!(redemptions.get(&test_symbol("MSFT")), Some(&test_shares(5)));
    }

    #[tokio::test]
    async fn poll_inflight_equity_aggregates_same_symbol() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let tokenizer = Arc::new(
            crate::tokenization::mock::MockTokenizer::new().with_pending_requests(vec![
                mock_pending_request(
                    crate::tokenization::TokenizationRequestType::Mint,
                    "AAPL",
                    10,
                ),
                mock_pending_request(
                    crate::tokenization::TokenizationRequestType::Mint,
                    "AAPL",
                    25,
                ),
            ]),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            Some(tokenizer),
            Usd::ZERO,
        )
        .with_pending_request_ownership(ownership(
            [],
            ["REQ_AAPL_10", "REQ_AAPL_25"],
            [],
            [],
        ));

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let inflight_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }))
            .expect("Expected InflightEquity event");

        let InventorySnapshotEvent::InflightEquity { mints, .. } = inflight_event else {
            panic!("Expected InflightEquity event");
        };

        assert_eq!(mints.len(), 1);
        assert_eq!(mints.get(&test_symbol("AAPL")), Some(&test_shares(35)));
    }

    #[tokio::test]
    async fn poll_inflight_equity_skipped_without_tokenizer() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            None,
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let inflight_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }));

        assert!(
            inflight_event.is_none(),
            "No InflightEquity event expected without tokenizer"
        );
    }

    #[tokio::test]
    async fn poll_inflight_equity_emits_empty_snapshot_when_no_requests_pending() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let executor = MockExecutor::new();

        let tokenizer =
            Arc::new(crate::tokenization::mock::MockTokenizer::new().with_pending_requests(vec![]));

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            Some(tokenizer),
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let inflight_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }))
            .expect("Expected InflightEquity event even with empty pending requests");

        let InventorySnapshotEvent::InflightEquity {
            mints, redemptions, ..
        } = inflight_event
        else {
            panic!("Expected InflightEquity event");
        };

        assert!(mints.is_empty(), "No pending mints expected");
        assert!(redemptions.is_empty(), "No pending redemptions expected");
    }

    #[tokio::test]
    async fn poll_inflight_equity_treats_requests_as_external_without_ownership_source() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let tokenizer = Arc::new(
            crate::tokenization::mock::MockTokenizer::new().with_pending_requests(vec![
                mock_pending_request_with_wallet(
                    crate::tokenization::TokenizationRequestType::Mint,
                    "AAPL",
                    10,
                    Some(order_owner),
                ),
                mock_pending_request_with_wallet(
                    crate::tokenization::TokenizationRequestType::Redeem,
                    "TSLA",
                    5,
                    Some(order_owner),
                ),
            ]),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            Some(tokenizer),
            Usd::ZERO,
        );

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let inflight_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }))
            .expect("Expected InflightEquity event");

        let InventorySnapshotEvent::InflightEquity {
            mints, redemptions, ..
        } = inflight_event
        else {
            panic!("Expected InflightEquity event");
        };

        assert!(mints.is_empty(), "No pending mints expected");
        assert!(redemptions.is_empty(), "No pending redemptions expected");
    }

    #[tokio::test]
    async fn poll_inflight_equity_ignores_external_requests_even_for_matching_wallet() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();
        let other_wallet = address!("0x9999999999999999999999999999999999999999");

        let tokenizer = Arc::new(
            crate::tokenization::mock::MockTokenizer::new().with_pending_requests(vec![
                mock_pending_request_with_wallet(
                    crate::tokenization::TokenizationRequestType::Mint,
                    "AAPL",
                    10,
                    Some(order_owner),
                ),
                mock_pending_request_with_wallet(
                    crate::tokenization::TokenizationRequestType::Mint,
                    "MSFT",
                    20,
                    Some(other_wallet),
                ),
                mock_pending_request_with_wallet(
                    crate::tokenization::TokenizationRequestType::Redeem,
                    "TSLA",
                    5,
                    None,
                ),
            ]),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            Some(tokenizer),
            Usd::ZERO,
        )
        .with_pending_request_ownership(ownership([], [], [], []));

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let inflight_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }))
            .expect("Expected InflightEquity event");

        let InventorySnapshotEvent::InflightEquity {
            mints, redemptions, ..
        } = inflight_event
        else {
            panic!("Expected InflightEquity event");
        };

        assert!(
            mints.is_empty(),
            "Matching wallet must not imply bot ownership"
        );
        assert!(
            redemptions.is_empty(),
            "Absent wallet must not imply bot ownership"
        );
    }

    #[tokio::test]
    async fn poll_inflight_equity_includes_owned_mint_by_issuer_request_id() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();

        let tokenizer = Arc::new(
            crate::tokenization::mock::MockTokenizer::new().with_pending_requests(vec![
                mock_pending_request_with_issuer_id(
                    crate::tokenization::TokenizationRequestType::Mint,
                    "AAPL",
                    10,
                    "owned-issuer-request",
                    Some(order_owner),
                ),
                mock_pending_request_with_wallet(
                    crate::tokenization::TokenizationRequestType::Mint,
                    "AAPL",
                    20,
                    Some(order_owner),
                ),
            ]),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            Some(tokenizer),
            Usd::ZERO,
        )
        .with_pending_request_ownership(ownership(["owned-issuer-request"], [], [], []));

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let inflight_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }))
            .expect("Expected InflightEquity event");

        let InventorySnapshotEvent::InflightEquity { mints, .. } = inflight_event else {
            panic!("Expected InflightEquity event");
        };

        assert_eq!(mints.len(), 1);
        assert_eq!(mints.get(&test_symbol("AAPL")), Some(&test_shares(10)));
    }

    #[tokio::test]
    async fn poll_inflight_equity_includes_pending_redemption_intent_before_detection() {
        let pool = setup_test_db().await;
        let provider = mock_provider();
        let raindex_service = create_test_raindex_service(provider.clone());
        let (orderbook, order_owner) = test_addresses();
        let redemption_tx = TxHash::random();
        let external_tx = TxHash::random();

        let tokenizer = Arc::new(
            crate::tokenization::mock::MockTokenizer::new().with_pending_requests(vec![
                mock_pending_request_with_tx_hash(
                    crate::tokenization::TokenizationRequestType::Redeem,
                    "AAPL",
                    5,
                    redemption_tx,
                    Some(order_owner),
                ),
                // Distinct quantity so the external row gets a distinct provider
                // id (REQ_AAPL_7); otherwise dedup-by-id would mask a leaked
                // external request and the assertion could pass even on regression.
                mock_pending_request_with_tx_hash(
                    crate::tokenization::TokenizationRequestType::Redeem,
                    "AAPL",
                    7,
                    external_tx,
                    Some(order_owner),
                ),
            ]),
        );

        let executor = MockExecutor::new();

        let service = InventoryPollingService::new(
            raindex_service,
            executor,
            Arc::new(test_store::<VaultRegistry>(pool.clone(), ())),
            InventorySnapshotId {
                orderbook,
                owner: order_owner,
            },
            Arc::new(test_store(pool.clone(), ())),
            None,
            Some(tokenizer),
            Usd::ZERO,
        )
        .with_pending_request_ownership(ownership([], [], [], [redemption_tx]));

        service.poll_and_record().await.unwrap();

        let events = load_snapshot_events(&pool, orderbook, order_owner).await;
        let inflight_event = events
            .iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }))
            .expect("Expected InflightEquity event");

        let InventorySnapshotEvent::InflightEquity { redemptions, .. } = inflight_event else {
            panic!("Expected InflightEquity event");
        };

        // Only the owned redemption (5 shares) is counted; the external 7-share
        // request must be excluded. If it leaked through, AAPL would total 12.
        assert_eq!(redemptions.get(&test_symbol("AAPL")), Some(&test_shares(5)));
    }

    #[test]
    fn aggregate_pending_requests_skips_none_type_but_keeps_valid_rows() {
        let requests = vec![
            mock_pending_request_no_type("AAPL", 5),
            mock_pending_request(
                crate::tokenization::TokenizationRequestType::Mint,
                "AAPL",
                10,
            ),
            mock_pending_request(
                crate::tokenization::TokenizationRequestType::Redeem,
                "TSLA",
                20,
            ),
            mock_pending_request(
                crate::tokenization::TokenizationRequestType::Mint,
                "AAPL",
                3,
            ),
        ];

        let PendingRequests { mints, redemptions } = InventoryPollingService::<
            ReadOnlyEvm<RootProvider>,
            MockExecutor,
        >::aggregate_pending_requests(
            requests.into_iter()
        )
        .unwrap();

        assert_eq!(
            mints.get(&test_symbol("AAPL")),
            Some(&test_shares(13)),
            "Valid mint rows should aggregate despite a None-type row"
        );

        assert_eq!(
            redemptions.get(&test_symbol("TSLA")),
            Some(&test_shares(20)),
            "Redemption row should be unaffected by the None-type row"
        );

        assert_eq!(mints.len(), 1);
        assert_eq!(redemptions.len(), 1);
    }

    #[test]
    fn aggregate_pending_requests_deduplicates_provider_request_ids() {
        let requests = vec![
            mock_pending_request(
                crate::tokenization::TokenizationRequestType::Mint,
                "AAPL",
                10,
            ),
            mock_pending_request(
                crate::tokenization::TokenizationRequestType::Mint,
                "AAPL",
                10,
            ),
        ];

        let PendingRequests { mints, redemptions } = InventoryPollingService::<
            ReadOnlyEvm<RootProvider>,
            MockExecutor,
        >::aggregate_pending_requests(
            requests.into_iter()
        )
        .unwrap();

        assert_eq!(
            mints.get(&test_symbol("AAPL")),
            Some(&test_shares(10)),
            "Duplicate provider rows should only count once"
        );
        assert!(redemptions.is_empty());
    }
}
