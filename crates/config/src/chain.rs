//! Per-chain configuration: the chain registry, its entries, and the RPC
//! secrets that complete them.
//!
//! A registry entry carries everything the bot needs to act on one chain, so
//! no chain-specific address lives anywhere else. Entries are built once at
//! startup and are immutable for the process lifetime.

use std::collections::{BTreeMap, HashSet};

use alloy::primitives::Address;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

use st0x_evm::Chain;

use crate::assets::ChainAssets;
use crate::enablement::{ChainEnablementError, ChainLifecycle, check_enablement};

/// Which block tag to use as the fill-ingestion cutoff.
///
/// The cutoff caps what the fill monitor treats as safe to ingest. Tags differ
/// in their reorg-safety guarantees and their distance behind the chain tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionCutoff {
    /// OP-Stack `safe` block: the latest L2 block whose sequencer batch has
    /// been posted to L1. Not yet L1-finalized (Casper FFG). Cuts hedging lag
    /// from ~20 min to ~seconds on Base.
    ///
    /// Tradeoff: a sufficiently deep L1 reorg dropping the batch tx could
    /// invalidate a `safe`-ingested fill. In practice this is extremely rare
    /// and far less likely than the latency cost of waiting for L1 finality.
    /// No reversal path exists; full reorg handling is tracked separately.
    Safe,
    /// L1-finalized block (Casper FFG). Full reorg protection but ~20 min
    /// hedging lag on Base. Use when strict reorg protection is required.
    Finalized,
    /// A fixed depth behind the chain tip: the cutoff is the already-fetched
    /// tip minus this many blocks, with no extra RPC round trip. For chains
    /// where consensus tags are unsuitable or unavailable (Ethereum mainnet
    /// watches at 12; HyperEVM has no tags at all).
    Confirmations(u64),
}

/// The config-file discriminant for [`IngestionCutoff`]. `confirmations`
/// additionally requires `ingestion_cutoff_confirmations`; the pairing is
/// validated with named errors when the chain is built (same pattern as
/// [`InventoryModeTag`] + `inventory`).
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionCutoffTag {
    Safe,
    Finalized,
    Confirmations,
}

impl std::fmt::Display for IngestionCutoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Safe => f.write_str("safe"),
            Self::Finalized => f.write_str("finalized"),
            Self::Confirmations(depth) => write!(f, "confirmations({depth})"),
        }
    }
}

/// Whether rebalancing settles through a distinct shared `RaindexInventory` or
/// directly against the orderbook.
///
/// Pre-migration the bot's EOA owns the Raindex vaults and deposits/withdraws
/// settle on the orderbook itself: there is no separate inventory contract and
/// no `OPERATOR_ROLE` to hold. The shared-inventory migration introduces a
/// distinct `RaindexInventory` that owns the vaults, which the bot operates via
/// `OPERATOR_ROLE`. Modelling the two as an explicit enum keeps a production
/// misconfig -- e.g. copying the orderbook address into `inventory` -- from
/// silently bypassing the startup `OPERATOR_ROLE` preflight, which an
/// `inventory == orderbook` equality check could not distinguish from a genuine
/// legacy deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryMode {
    /// No distinct inventory contract: vaults are bot-EOA-owned and settle
    /// directly against the orderbook. The startup `OPERATOR_ROLE` preflight is
    /// skipped and rebalancing `deposit4`/`withdraw4` target the orderbook.
    Legacy,
    /// A distinct shared `RaindexInventory` owns the vaults. The bot must hold
    /// `OPERATOR_ROLE` on it; rebalancing deposits/withdraws settle here.
    Managed { inventory: Address },
}

/// Config-level discriminant for [`InventoryMode`], deserialized from
/// `[chains.<name>].inventory_mode`.
///
/// Kept separate from the resolved enum so the `inventory` address requirement
/// can be validated (required for `managed`, forbidden for `legacy`) at startup
/// rather than trusted from the file.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InventoryModeTag {
    Legacy,
    Managed,
}

/// Venue identified by an inventory adapter's `operator` address.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InventoryAdapterVenue {
    Bebop,
    UniswapV4,
}

/// One deployment-specific shared-inventory adapter.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryAdapter {
    pub venue: InventoryAdapterVenue,
    pub operator: Address,
}

/// Explicit inventory-adapter table configured for one deployment.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(try_from = "Vec<InventoryAdapter>")]
pub struct InventoryAdapters(Vec<InventoryAdapter>);

impl TryFrom<Vec<InventoryAdapter>> for InventoryAdapters {
    type Error = ChainConfigError;

    fn try_from(adapters: Vec<InventoryAdapter>) -> Result<Self, Self::Error> {
        Self::try_new(adapters)
    }
}

impl InventoryAdapters {
    /// Builds and validates an explicit adapter table.
    pub fn try_new(adapters: Vec<InventoryAdapter>) -> Result<Self, ChainConfigError> {
        let adapters = Self(adapters);
        adapters.validate()?;
        Ok(adapters)
    }

    /// Returns the venue configured for `operator`, if any.
    pub fn venue_for(&self, operator: Address) -> Option<InventoryAdapterVenue> {
        self.0
            .iter()
            .find(|adapter| adapter.operator == operator)
            .map(|adapter| adapter.venue)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn validate(&self) -> Result<(), ChainConfigError> {
        let mut operators = HashSet::new();
        for adapter in &self.0 {
            if !operators.insert(adapter.operator) {
                return Err(ChainConfigError::DuplicateInventoryAdapterOperator {
                    operator: adapter.operator,
                });
            }
        }

        Ok(())
    }
}

/// Errors resolving an [`ChainConfig`] into a runtime [`ChainCtx`].
#[derive(Debug, Error)]
pub enum ChainConfigError {
    #[error(
        "[chains.<name>] inventory_mode = \"managed\" requires an `inventory` address, \
         but none was configured"
    )]
    ManagedWithoutInventory,
    #[error(
        "[chains.<name>] inventory_mode = \"legacy\" forbids an `inventory` address, \
         but {inventory} was configured; set inventory_mode = \"managed\" to \
         enable a shared inventory"
    )]
    LegacyWithInventory { inventory: Address },
    #[error(
        "[chains.<name>] inventory_mode = \"legacy\" forbids inventory adapters because \
         no shared inventory exists"
    )]
    LegacyWithInventoryAdapters,
    #[error(
        "[chains.<name>].inventory_adapters configures operator {operator} more than once; \
         one operator cannot identify multiple venues"
    )]
    DuplicateInventoryAdapterOperator { operator: Address },
    #[error(
        "[chains.<name>] ingestion_cutoff = \"confirmations\" requires \
         ingestion_cutoff_confirmations, but none was configured"
    )]
    ConfirmationsWithoutDepth,
    #[error(
        "[chains.<name>] ingestion_cutoff_confirmations is only valid with \
         ingestion_cutoff = \"confirmations\", but the cutoff is \"{tag:?}\""
    )]
    DepthWithoutConfirmations { tag: IngestionCutoffTag },
    #[error("[chains.<name>] ingestion_cutoff_confirmations must be non-zero")]
    ZeroConfirmationDepth,
    #[error("[chains.<name>] order_fill_poll_interval_secs must be non-zero")]
    ZeroOrderFillPollInterval,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    /// How far along this chain is in its bring-up. Required and explicit: a
    /// chain silently defaulting to `active` would start moving funds the
    /// moment it was described.
    pub lifecycle: ChainLifecycle,
    /// Block-confirmation depth required before a transaction this bot submits
    /// to this chain is treated as settled. Per-chain because it encodes one
    /// chain's reorg behaviour; a single global value cannot be right for
    /// every chain. Required on every entry: the bot submits transactions on
    /// transport-only chains too.
    pub required_confirmations: u64,
    /// Present when the bot trades on this chain. Absent for a chain that is
    /// only a cash corridor endpoint, which has no orderbook, no vaults and no
    /// fills to ingest.
    pub trading: Option<TradingConfig>,
}

/// The orderbook side of a chain the bot trades on.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradingConfig {
    pub orderbook: Address,
    /// Rebalancing settlement mode. `legacy` settles directly against the
    /// orderbook with bot-EOA-owned vaults (no inventory contract); `managed`
    /// settles through the shared `RaindexInventory` named by `inventory`, which
    /// the bot must operate via `OPERATOR_ROLE`. See [`InventoryMode`].
    pub inventory_mode: InventoryModeTag,
    /// Shared `RaindexInventory` (raindex.governance) that owns the Raindex
    /// vaults. Required when `inventory_mode = "managed"` and forbidden for
    /// `legacy` (both validated at startup). All venue adapters (Bebop hook,
    /// univ4 hook) and this bot's own rebalancing path settle through it --
    /// `deposit4`/`withdraw4` on this address instead of on the orderbook. Fill
    /// events on the pooled vaults are also surfaced here as
    /// `OperatorDeposit`/`OperatorWithdraw`.
    pub inventory: Option<Address>,
    /// Public, deployment-specific adapter operator addresses used to attribute
    /// shared-inventory settlements to their execution venue.
    pub inventory_adapters: InventoryAdapters,
    /// Address that owns the Raindex orders and vaults on-chain -- the key every
    /// `vaultBalance2` read, vault-registry entry, and order-owner fill match is
    /// scoped by. Required and explicit (no fallback): this parameter determines
    /// fund-routing correctness, so a missing value must fail at startup rather
    /// than silently assume an owner.
    pub vault_owner: Address,
    pub deployment_block: u64,
    pub ingestion_cutoff: IngestionCutoffTag,
    /// Depth for `ingestion_cutoff = "confirmations"`. Required with that
    /// mode and forbidden with the tag-based modes; must be non-zero.
    pub ingestion_cutoff_confirmations: Option<u64>,
    /// Seconds between fill-watch poll cycles on this chain. Required (no
    /// silent default) and non-zero; each watched chain polls independently.
    pub order_fill_poll_interval_secs: u64,
    /// Marks THE primary chain: where trading, rebalancing triggers, and the
    /// cash vaults live. Exactly one watched chain must set it.
    #[serde(default)]
    pub primary: bool,
    /// Alpaca's issuer wallet on this chain -- ERC-20 transfers for redemption
    /// go here. Per chain because a redemption delivers tokens that exist on
    /// one chain, so sending to another chain's issuer address burns them.
    pub redemption_wallet: Option<Address>,
    /// The equities and cash the bot holds on this chain.
    ///
    /// Defaulted rather than required: an absent table and an empty one mean
    /// the same thing -- the bot holds nothing here yet -- which is the normal
    /// state while a chain is being brought up. Requiring the header would add
    /// ceremony without closing a gap, since an empty table stays valid.
    #[serde(default)]
    pub assets: ChainAssets,
}

impl TradingConfig {
    /// Resolves the configured mode + optional `inventory` into an
    /// [`InventoryMode`], failing fast on the two contradictory combinations:
    /// `managed` without an inventory, or `legacy` with one.
    fn resolve_inventory_mode(&self) -> Result<InventoryMode, ChainConfigError> {
        match (self.inventory_mode, self.inventory) {
            (InventoryModeTag::Legacy, None) => Ok(InventoryMode::Legacy),
            (InventoryModeTag::Legacy, Some(inventory)) => {
                Err(ChainConfigError::LegacyWithInventory { inventory })
            }
            (InventoryModeTag::Managed, Some(inventory)) => {
                Ok(InventoryMode::Managed { inventory })
            }
            (InventoryModeTag::Managed, None) => Err(ChainConfigError::ManagedWithoutInventory),
        }
    }

    /// Resolves the cutoff tag + optional depth into an [`IngestionCutoff`],
    /// failing fast on the contradictory combinations.
    fn resolve_ingestion_cutoff(&self) -> Result<IngestionCutoff, ChainConfigError> {
        match (self.ingestion_cutoff, self.ingestion_cutoff_confirmations) {
            (IngestionCutoffTag::Safe, None) => Ok(IngestionCutoff::Safe),
            (IngestionCutoffTag::Finalized, None) => Ok(IngestionCutoff::Finalized),
            (IngestionCutoffTag::Confirmations, Some(0)) => {
                Err(ChainConfigError::ZeroConfirmationDepth)
            }
            (IngestionCutoffTag::Confirmations, Some(depth)) => {
                Ok(IngestionCutoff::Confirmations(depth))
            }
            (IngestionCutoffTag::Confirmations, None) => {
                Err(ChainConfigError::ConfirmationsWithoutDepth)
            }
            (tag, Some(_)) => Err(ChainConfigError::DepthWithoutConfirmations { tag }),
        }
    }

    fn validate_inventory_adapters(&self) -> Result<(), ChainConfigError> {
        if self.inventory_mode == InventoryModeTag::Legacy && !self.inventory_adapters.is_empty() {
            return Err(ChainConfigError::LegacyWithInventoryAdapters);
        }

        self.inventory_adapters.validate()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainSecrets {
    /// HTTP RPC endpoint for this chain. Drives continuous `eth_getLogs` fill
    /// polling, every read-only contract call, and wallet operations (single
    /// transport, no WebSocket).
    ///
    /// Must be an archive node, or otherwise retain state back to the oldest
    /// block a read can pin to: the historic block-pinned bot-gas ETH/USD
    /// valuation call (ADR 0020) fails against a pruned node once the target
    /// receipt block ages out, surfacing as a typed, logged RPC error rather
    /// than silently falling back to another block.
    pub rpc_url: Url,
}

/// A chain the bot acts on but does not trade on: a cash corridor endpoint,
/// with no orderbook, no vaults and no fills to ingest.
#[derive(Clone)]
pub struct ChainCtx {
    pub chain: Chain,
    pub rpc_url: Url,
    pub required_confirmations: u64,
}

impl std::fmt::Debug for ChainCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainCtx")
            .field("chain", &self.chain)
            .field("rpc_url", &"[REDACTED]")
            .field("required_confirmations", &self.required_confirmations)
            .finish()
    }
}

/// The chain the bot trades on: everything a [`ChainCtx`] carries, plus the
/// orderbook side.
///
/// Deliberately flat rather than a `ChainCtx` with a nested orderbook struct.
/// Every consumer of this reads a mix of both halves -- an RPC url and an
/// orderbook address in the same call -- so nesting would buy nothing and add
/// a level of indirection to each of those reads.
#[derive(Clone)]
pub struct TradingChain {
    pub chain: Chain,
    pub rpc_url: Url,
    pub required_confirmations: u64,
    pub orderbook: Address,
    pub inventory: InventoryMode,
    pub inventory_adapters: InventoryAdapters,
    pub vault_owner: Address,
    pub deployment_block: u64,
    pub ingestion_cutoff: IngestionCutoff,
    /// Seconds between fill-watch poll cycles on this chain.
    pub order_fill_poll_interval: std::time::Duration,
    pub redemption_wallet: Option<Address>,
    pub assets: ChainAssets,
}

#[cfg(any(test, feature = "test-support"))]
#[bon::bon]
impl TradingChain {
    /// Test fixture builder: every field defaults to the common test shape
    /// (Base, localhost RPC, 0x11.. addresses, managed inventory, safe
    /// cutoff), so fixtures state only the fields their test depends on and
    /// new required fields sweep through one place.
    #[builder]
    pub fn test(
        #[builder(default = Chain::Base)] chain: Chain,
        #[builder(default = url::Url::parse("http://localhost:8545")
            .unwrap_or_else(|_| unreachable!("hard-coded test RPC URL is valid")))]
        rpc_url: url::Url,
        #[builder(default = 0)] required_confirmations: u64,
        #[builder(default = alloy::primitives::Address::repeat_byte(0x11))] orderbook: Address,
        #[builder(default = InventoryMode::Managed {
            inventory: alloy::primitives::Address::repeat_byte(0x11),
        })]
        inventory: InventoryMode,
        #[builder(default)] inventory_adapters: InventoryAdapters,
        #[builder(default = alloy::primitives::Address::repeat_byte(0x11))] vault_owner: Address,
        #[builder(default = 0)] deployment_block: u64,
        #[builder(default = IngestionCutoff::Safe)] ingestion_cutoff: IngestionCutoff,
        #[builder(default = std::time::Duration::from_secs(1))]
        order_fill_poll_interval: std::time::Duration,
        redemption_wallet: Option<Address>,
        #[builder(default)] assets: ChainAssets,
    ) -> Self {
        Self {
            chain,
            rpc_url,
            required_confirmations,
            orderbook,
            inventory,
            inventory_adapters,
            vault_owner,
            deployment_block,
            ingestion_cutoff,
            order_fill_poll_interval,
            redemption_wallet,
            assets,
        }
    }
}

impl std::fmt::Debug for TradingChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradingChain")
            .field("chain", &self.chain)
            .field("rpc_url", &"[REDACTED]")
            .field("required_confirmations", &self.required_confirmations)
            .field("orderbook", &self.orderbook)
            .field("inventory", &self.inventory)
            .field("inventory_adapters", &self.inventory_adapters)
            .field("vault_owner", &self.vault_owner)
            .field("deployment_block", &self.deployment_block)
            .field("ingestion_cutoff", &self.ingestion_cutoff)
            .field("order_fill_poll_interval", &self.order_fill_poll_interval)
            .field("redemption_wallet", &self.redemption_wallet)
            .field("assets", &self.assets)
            .finish()
    }
}

impl TradingChain {
    fn new(
        chain: Chain,
        config: &ChainConfig,
        trading: &TradingConfig,
        rpc_url: Url,
    ) -> Result<Self, ChainConfigError> {
        trading.validate_inventory_adapters()?;

        if trading.order_fill_poll_interval_secs == 0 {
            return Err(ChainConfigError::ZeroOrderFillPollInterval);
        }

        Ok(Self {
            chain,
            rpc_url,
            required_confirmations: config.required_confirmations,
            orderbook: trading.orderbook,
            inventory: trading.resolve_inventory_mode()?,
            inventory_adapters: trading.inventory_adapters.clone(),
            vault_owner: trading.vault_owner,
            deployment_block: trading.deployment_block,
            ingestion_cutoff: trading.resolve_ingestion_cutoff()?,
            order_fill_poll_interval: std::time::Duration::from_secs(
                trading.order_fill_poll_interval_secs,
            ),
            redemption_wallet: trading.redemption_wallet,
            assets: trading.assets.clone(),
        })
    }

    /// The address rebalancing `deposit4`/`withdraw4` settle against: the shared
    /// inventory in `Managed` mode, or the orderbook itself in `Legacy` mode
    /// (where the bot-owned vaults live on the orderbook).
    pub fn inventory_address(&self) -> Address {
        match self.inventory {
            InventoryMode::Legacy => self.orderbook,
            InventoryMode::Managed { inventory } => inventory,
        }
    }
}

/// Every chain the bot is configured to act on.
///
/// Built once at startup from the `[chains.<name>]` config tables paired with
/// their `[chains.<name>]` secrets entries. A chain described in one file but
/// not the other is a misconfiguration, not a chain to skip: acting on a chain
/// with no RPC, or holding an RPC for a chain with no addresses, are both
/// states where fund routing is undefined.
///
/// The primary chain is its own field rather than an entry in a map, so
/// "exactly one primary" holds by construction instead of being re-checked
/// by every reader; watch-only chains live in `secondary`.
#[derive(Clone, Debug)]
pub struct ChainRegistry {
    primary: TradingChain,
    /// Watched, non-primary chains: fills are ingested and hedged, but
    /// trading, rebalancing triggers, and cash vaults stay on the primary.
    secondary: BTreeMap<Chain, TradingChain>,
    transport: BTreeMap<Chain, ChainCtx>,
}

/// Errors assembling a [`ChainRegistry`] from config and secrets.
#[derive(Debug, Error)]
pub enum ChainRegistryError {
    #[error("no [chains.<name>] table is configured; the bot must act on at least one chain")]
    NoChains,
    #[error(
        "every configured chain has lifecycle = \"disabled\"; the bot would act on \
         no chain at all"
    )]
    NoEnabledChains,
    #[error(
        "no configured chain has a [chains.<name>.trading] table; the bot would have \
         no orderbook to watch and would place no hedges"
    )]
    NoTradingChain,
    #[error(
        "{} chains configure a [trading] table ({}) but none sets primary = true; \
         exactly one watched chain must be the primary (trading, rebalancing, \
         cash vaults)",
        chains.len(),
        chains.iter().map(|chain| chain.as_str()).collect::<Vec<_>>().join(", ")
    )]
    NoPrimaryChain { chains: Vec<Chain> },
    #[error(
        "[chains.{chain}] is configured but the secrets file has no [chains.{chain}] \
         entry supplying its rpc_url"
    )]
    MissingSecrets { chain: Chain },
    #[error(
        "the secrets file has a [chains.{chain}] entry but the config file has no enabled \
         [chains.{chain}] table: the table is missing, or its lifecycle is \"disabled\" and \
         its rpc_url was left behind"
    )]
    UnconfiguredSecrets { chain: Chain },
    #[error(
        "{} chains claim primary = true ({}); exactly one chain can be the primary",
        chains.len(),
        chains.iter().map(|chain| chain.as_str()).collect::<Vec<_>>().join(", ")
    )]
    MultiplePrimaryChains { chains: Vec<Chain> },
    #[error(transparent)]
    Entry(#[from] ChainConfigError),
    #[error(transparent)]
    Enablement(#[from] ChainEnablementError),
}

/// The chains [`enabled_chains`] kept, plus the one that trades.
struct EnabledChains<'config> {
    /// Every chain whose lifecycle is not `disabled`, trading one included.
    enabled: BTreeMap<Chain, &'config ChainConfig>,
    primary_chain: Chain,
    chain_config: &'config ChainConfig,
    trading_table: &'config TradingConfig,
}

/// Drops the disabled chains, picks the primary trading chain, and checks
/// that every surviving chain carries the capabilities its lifecycle needs.
///
/// Exactly one trading chain must claim `primary = true`; the rest are
/// watch-only secondaries. Transport-only entries are unlimited -- nothing
/// watches them by design.
fn enabled_chains(
    configs: &BTreeMap<Chain, ChainConfig>,
) -> Result<EnabledChains<'_>, ChainRegistryError> {
    if configs.is_empty() {
        return Err(ChainRegistryError::NoChains);
    }

    // A disabled chain is not constructed at all, so it needs no secrets
    // entry and contributes nothing to the checks below.
    let enabled: BTreeMap<Chain, &ChainConfig> = configs
        .iter()
        .filter(|(_, config)| config.lifecycle != ChainLifecycle::Disabled)
        .map(|(chain, config)| (*chain, config))
        .collect();

    if enabled.is_empty() {
        return Err(ChainRegistryError::NoEnabledChains);
    }

    let trading_chains: Vec<(Chain, &ChainConfig, &TradingConfig)> = enabled
        .iter()
        .filter_map(|(chain, config)| {
            config
                .trading
                .as_ref()
                .map(|trading| (*chain, *config, trading))
        })
        .collect();

    if trading_chains.is_empty() {
        return Err(ChainRegistryError::NoTradingChain);
    }

    let primaries: Vec<(Chain, &ChainConfig, &TradingConfig)> = trading_chains
        .iter()
        .filter(|(_, _, trading)| trading.primary)
        .copied()
        .collect();
    let (primary_chain, chain_config, trading_table) = match primaries.as_slice() {
        [] => {
            return Err(ChainRegistryError::NoPrimaryChain {
                chains: trading_chains.iter().map(|(chain, _, _)| *chain).collect(),
            });
        }
        [only] => *only,
        many => {
            return Err(ChainRegistryError::MultiplePrimaryChains {
                chains: many.iter().map(|(chain, _, _)| *chain).collect(),
            });
        }
    };

    for (chain, config) in &enabled {
        check_enablement(
            *chain,
            config.lifecycle,
            config.trading.is_some(),
            config.trading.as_ref().map(|trading| &trading.assets),
        )?;
    }

    Ok(EnabledChains {
        enabled,
        primary_chain,
        chain_config,
        trading_table,
    })
}

impl ChainRegistry {
    /// Pairs each configured chain with its secrets entry.
    ///
    /// Refuses more than one trading chain: the config shape admits several,
    /// but the runtime still drives a single fill watcher, so a second one
    /// would be fully described and never read. Failing here is what keeps
    /// that gap from presenting as silently unhedged exposure. Transport-only
    /// entries are unlimited -- nothing watches them by design.
    pub fn new(
        configs: &BTreeMap<Chain, ChainConfig>,
        mut secrets: BTreeMap<Chain, ChainSecrets>,
    ) -> Result<Self, ChainRegistryError> {
        let EnabledChains {
            enabled: configs,
            primary_chain,
            chain_config,
            trading_table,
        } = enabled_chains(configs)?;

        let mut take_rpc_url = |chain: Chain| {
            secrets
                .remove(&chain)
                .ok_or(ChainRegistryError::MissingSecrets { chain })
                .map(|entry| entry.rpc_url)
        };

        let primary = TradingChain::new(
            primary_chain,
            chain_config,
            trading_table,
            take_rpc_url(primary_chain)?,
        )?;

        let mut secondary = BTreeMap::new();
        let mut transport = BTreeMap::new();
        for (chain, config) in &configs {
            if *chain == primary_chain {
                continue;
            }

            match &config.trading {
                Some(trading) => {
                    secondary.insert(
                        *chain,
                        TradingChain::new(*chain, config, trading, take_rpc_url(*chain)?)?,
                    );
                }
                None => {
                    transport.insert(
                        *chain,
                        ChainCtx {
                            chain: *chain,
                            rpc_url: take_rpc_url(*chain)?,
                            required_confirmations: config.required_confirmations,
                        },
                    );
                }
            }
        }

        if let Some(chain) = secrets.keys().next() {
            return Err(ChainRegistryError::UnconfiguredSecrets { chain: *chain });
        }

        Ok(Self {
            primary,
            secondary,
            transport,
        })
    }

    /// Runs every `[chains.<name>]` check that reads the config file alone,
    /// and hands back the primary chain's table so the caller can keep
    /// validating against it.
    ///
    /// [`Self::new`] performs these same checks plus the config/secrets
    /// pairing (each chain's `rpc_url`), which is why the secrets-free
    /// `validate-config` path calls this one instead: a chain table that
    /// fails here fails startup too, whatever the secrets file holds.
    pub fn validate_configs(
        configs: &BTreeMap<Chain, ChainConfig>,
    ) -> Result<&TradingConfig, ChainRegistryError> {
        let EnabledChains { trading_table, .. } = enabled_chains(configs)?;

        trading_table.validate_inventory_adapters()?;
        trading_table.resolve_inventory_mode()?;

        Ok(trading_table)
    }

    /// THE primary chain: where trading, rebalancing triggers, and the cash
    /// vaults live. Watch-only chains are reached via [`Self::watched`].
    pub fn primary(&self) -> &TradingChain {
        &self.primary
    }

    /// Every watched chain (primary first, then secondaries): the chains a
    /// fill watcher runs against.
    pub fn watched(&self) -> impl Iterator<Item = &TradingChain> {
        std::iter::once(&self.primary).chain(self.secondary.values())
    }

    /// The watched chain with this id, if any.
    pub fn watch(&self, chain: Chain) -> Option<&TradingChain> {
        if self.primary.chain == chain {
            return Some(&self.primary);
        }
        self.secondary.get(&chain)
    }

    /// Mutable access to the trading chain, so a fixture can vary one field
    /// (a settlement mode, an unreachable RPC) without rebuilding the registry.
    #[cfg(any(test, feature = "test-support"))]
    pub fn primary_mut(&mut self) -> &mut TradingChain {
        &mut self.primary
    }

    /// A registry holding one primary chain and nothing else.
    ///
    /// Test and fixture construction only: production registries come from
    /// [`Self::new`], which is what enforces the config/secrets pairing.
    #[cfg(any(test, feature = "test-support"))]
    pub fn single_trading_chain(primary: TradingChain) -> Self {
        Self {
            primary,
            secondary: BTreeMap::new(),
            transport: BTreeMap::new(),
        }
    }

    /// The RPC endpoint configured for `chain`, whether it trades or only
    /// carries cash. `None` when the chain has no `[chains.<name>]` entry.
    pub fn rpc_url(&self, chain: Chain) -> Option<&Url> {
        if let Some(watched) = self.watch(chain) {
            return Some(&watched.rpc_url);
        }

        self.transport.get(&chain).map(|entry| &entry.rpc_url)
    }

    /// The confirmation depth configured for `chain`. `None` when the chain
    /// has no `[chains.<name>]` entry. Per chain because the depth encodes one
    /// chain's reorg behaviour; a global value cannot be right for all.
    pub fn required_confirmations(&self, chain: Chain) -> Option<u64> {
        if let Some(watched) = self.watch(chain) {
            return Some(watched.required_confirmations);
        }

        self.transport
            .get(&chain)
            .map(|entry| entry.required_confirmations)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use st0x_execution::Symbol;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct CutoffWrapper {
        ingestion_cutoff: IngestionCutoffTag,
    }

    /// The confirmations mode needs its companion depth and produces the
    /// arithmetic variant; each contradictory pairing fails with its named
    /// error.
    #[test]
    fn confirmations_cutoff_resolution() {
        let with = |tag, depth| TradingConfig {
            ingestion_cutoff: tag,
            ingestion_cutoff_confirmations: depth,
            ..primary_trading_config_toml(true)
        };

        assert_eq!(
            with(IngestionCutoffTag::Confirmations, Some(12))
                .resolve_ingestion_cutoff()
                .unwrap(),
            IngestionCutoff::Confirmations(12)
        );
        assert!(matches!(
            with(IngestionCutoffTag::Confirmations, None)
                .resolve_ingestion_cutoff()
                .unwrap_err(),
            ChainConfigError::ConfirmationsWithoutDepth
        ));
        assert!(matches!(
            with(IngestionCutoffTag::Safe, Some(12))
                .resolve_ingestion_cutoff()
                .unwrap_err(),
            ChainConfigError::DepthWithoutConfirmations {
                tag: IngestionCutoffTag::Safe
            }
        ));
        assert!(matches!(
            with(IngestionCutoffTag::Confirmations, Some(0))
                .resolve_ingestion_cutoff()
                .unwrap_err(),
            ChainConfigError::ZeroConfirmationDepth
        ));
    }

    /// The TOML shape: mode string plus separate depth key.
    #[test]
    fn confirmations_cutoff_toml_round_trip() {
        let config: TradingConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"legacy\"\n\
             inventory_adapters = []\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             order_fill_poll_interval_secs = 12\n\
             primary = false\n\
             ingestion_cutoff = \"confirmations\"\n\
             ingestion_cutoff_confirmations = 12",
        )
        .unwrap();

        assert_eq!(config.ingestion_cutoff, IngestionCutoffTag::Confirmations);
        assert_eq!(config.ingestion_cutoff_confirmations, Some(12));
        assert_eq!(
            config.resolve_ingestion_cutoff().unwrap(),
            IngestionCutoff::Confirmations(12)
        );
    }

    fn dummy_rpc_url() -> Url {
        Url::parse("http://localhost:8545").unwrap()
    }

    /// A chain entry carrying only what lives outside the `[trading]` table,
    /// so a fixture can exercise `TradingConfig` on its own.
    fn base_chain_config() -> ChainConfig {
        ChainConfig {
            lifecycle: ChainLifecycle::Active,
            required_confirmations: 3,
            trading: None,
        }
    }

    #[test]
    fn ingestion_cutoff_deserializes_safe() {
        let wrapper: CutoffWrapper = toml::from_str(
            "ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        )
        .unwrap();

        assert_eq!(wrapper.ingestion_cutoff, IngestionCutoffTag::Safe);
    }

    #[test]
    fn vault_owner_missing_fails_to_parse() {
        // vault_owner determines fund-routing correctness (every vaultBalance2
        // read and fill match is scoped by it), so a config without it must
        // fail at startup rather than silently assume an owner.
        let result: Result<TradingConfig, _> = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory_adapters = []\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        );

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("vault_owner"),
            "expected missing-field error for vault_owner, got: {error}"
        );
    }

    #[test]
    fn inventory_adapters_are_required() {
        let result: Result<TradingConfig, _> = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        );

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("inventory_adapters"),
            "expected missing-field error for inventory_adapters, got: {error}"
        );
    }

    #[test]
    fn inventory_adapters_resolve_configured_operator_venues() {
        let config: TradingConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory_adapters = [\n\
               { venue = \"bebop\", operator = \"0x4444444444444444444444444444444444444444\" },\n\
               { venue = \"uniswap_v4\", operator = \"0x5555555555555555555555555555555555555555\" },\n\
             ]\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        )
        .unwrap();

        TradingChain::new(Chain::Base, &base_chain_config(), &config, dummy_rpc_url()).unwrap();

        assert_eq!(
            config
                .inventory_adapters
                .venue_for(alloy::primitives::address!(
                    "0x4444444444444444444444444444444444444444"
                )),
            Some(InventoryAdapterVenue::Bebop),
        );
        assert_eq!(
            config
                .inventory_adapters
                .venue_for(alloy::primitives::address!(
                    "0x5555555555555555555555555555555555555555"
                )),
            Some(InventoryAdapterVenue::UniswapV4),
        );
        assert_eq!(
            config
                .inventory_adapters
                .venue_for(Address::repeat_byte(0x66)),
            None,
        );
    }

    #[test]
    fn duplicate_inventory_adapter_operator_fails_during_deserialization() {
        let error = toml::from_str::<TradingConfig>(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory_adapters = [\n\
               { venue = \"bebop\", operator = \"0x4444444444444444444444444444444444444444\" },\n\
               { venue = \"uniswap_v4\", operator = \"0x4444444444444444444444444444444444444444\" },\n\
             ]\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        )
        .unwrap_err();

        assert!(
            error.to_string().contains(
                "configures operator 0x4444444444444444444444444444444444444444 more than once"
            ),
            "duplicate operators must fail before a TradingConfig can be constructed: {error}"
        );
    }

    #[test]
    fn inventory_adapters_allow_operator_rotation_for_one_venue() {
        let config: TradingConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory_adapters = [\n\
               { venue = \"bebop\", operator = \"0x4444444444444444444444444444444444444444\" },\n\
               { venue = \"bebop\", operator = \"0x5555555555555555555555555555555555555555\" },\n\
             ]\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        )
        .unwrap();

        TradingChain::new(Chain::Base, &base_chain_config(), &config, dummy_rpc_url()).unwrap();

        assert_eq!(
            config
                .inventory_adapters
                .venue_for(alloy::primitives::address!(
                    "0x5555555555555555555555555555555555555555"
                )),
            Some(InventoryAdapterVenue::Bebop),
        );
    }

    #[test]
    fn legacy_mode_with_inventory_adapters_fails() {
        let config: TradingConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"legacy\"\n\
             inventory_adapters = [\n\
               { venue = \"bebop\", operator = \"0x4444444444444444444444444444444444444444\" },\n\
             ]\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        )
        .unwrap();

        let error = TradingChain::new(Chain::Base, &base_chain_config(), &config, dummy_rpc_url())
            .unwrap_err();

        assert!(matches!(
            error,
            ChainConfigError::LegacyWithInventoryAdapters
        ));
    }

    fn trading_config_toml() -> TradingConfig {
        primary_trading_config_toml(true)
    }

    fn primary_trading_config_toml(primary: bool) -> TradingConfig {
        toml::from_str(&format!(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"legacy\"\n\
             inventory_adapters = []\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             order_fill_poll_interval_secs = 1\n\
             primary = {primary}\n\
             ingestion_cutoff = \"safe\""
        ))
        .unwrap()
    }

    fn chain_config(trading: Option<TradingConfig>) -> ChainConfig {
        ChainConfig {
            lifecycle: ChainLifecycle::Active,
            required_confirmations: 3,
            trading,
        }
    }

    /// A distinct endpoint per chain, so a swap between the trading and
    /// transport lookups cannot satisfy the routing assertions.
    fn rpc_url_for(chain: Chain) -> Url {
        Url::parse(&format!("http://{}.test:8545", chain.as_str())).unwrap()
    }

    fn secrets_for(chains: &[Chain]) -> BTreeMap<Chain, ChainSecrets> {
        chains
            .iter()
            .map(|chain| {
                (
                    *chain,
                    ChainSecrets {
                        rpc_url: rpc_url_for(*chain),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn registry_rejects_an_empty_config() {
        let error = ChainRegistry::new(&BTreeMap::new(), BTreeMap::new()).unwrap_err();

        assert!(
            matches!(error, ChainRegistryError::NoChains),
            "got: {error}"
        );
    }

    /// Disabling a chain without scrubbing its secrets entry fails closed:
    /// the leftover rpc_url raises [`ChainRegistryError::UnconfiguredSecrets`],
    /// whose message names this exact case.
    #[test]
    fn registry_rejects_a_disabled_chain_with_a_leftover_secrets_entry() {
        let mut ethereum = chain_config(None);
        ethereum.lifecycle = ChainLifecycle::Disabled;
        let configs = BTreeMap::from([
            (Chain::Base, chain_config(Some(trading_config_toml()))),
            (Chain::Ethereum, ethereum),
        ]);

        let error =
            ChainRegistry::new(&configs, secrets_for(&[Chain::Base, Chain::Ethereum])).unwrap_err();

        assert!(
            matches!(
                error,
                ChainRegistryError::UnconfiguredSecrets {
                    chain: Chain::Ethereum
                }
            ),
            "got: {error}"
        );
        assert!(
            error.to_string().contains("lifecycle is \"disabled\""),
            "the message must name the disabled case, got: {error}"
        );
    }

    /// Every chain disabled is a misconfiguration, not a valid idle state:
    /// the bot would act on nothing at all.
    #[test]
    fn registry_rejects_a_config_with_every_chain_disabled() {
        let mut config = chain_config(Some(trading_config_toml()));
        config.lifecycle = ChainLifecycle::Disabled;
        let configs = BTreeMap::from([(Chain::Base, config)]);

        let error = ChainRegistry::new(&configs, BTreeMap::new()).unwrap_err();

        assert!(
            matches!(error, ChainRegistryError::NoEnabledChains),
            "got: {error}"
        );
    }

    #[test]
    fn registry_rejects_a_config_with_no_trading_chain() {
        let configs = BTreeMap::from([(Chain::Ethereum, chain_config(None))]);

        let error = ChainRegistry::new(&configs, secrets_for(&[Chain::Ethereum])).unwrap_err();

        assert!(
            matches!(error, ChainRegistryError::NoTradingChain),
            "got: {error}"
        );
    }

    /// The registry structurally admits a second watched chain, but the
    /// capability gate still refuses it until the per-chain watcher code
    /// exists and grants `FillIngestion`/`Hedging` for that chain: the
    /// config layer must never claim a capability the code does not have.
    /// (The acceptance counterpart lands with the capability grant.)
    #[test]
    fn second_watched_chain_is_still_refused_by_the_capability_gate() {
        let configs = BTreeMap::from([
            (Chain::Base, chain_config(Some(trading_config_toml()))),
            (
                Chain::Ethereum,
                chain_config(Some(primary_trading_config_toml(false))),
            ),
        ]);

        let error =
            ChainRegistry::new(&configs, secrets_for(&[Chain::Base, Chain::Ethereum])).unwrap_err();

        assert!(
            matches!(error, ChainRegistryError::Enablement(_)),
            "got: {error}"
        );
    }

    /// `watched()`/`watch()` on a single-primary registry: the primary is the
    /// sole watched chain and lookups by other ids miss.
    #[test]
    fn watched_iterates_the_primary_when_no_secondary_exists() {
        let configs = BTreeMap::from([(Chain::Base, chain_config(Some(trading_config_toml())))]);
        let registry = ChainRegistry::new(&configs, secrets_for(&[Chain::Base])).unwrap();

        let watched: Vec<Chain> = registry.watched().map(|chain| chain.chain).collect();
        assert_eq!(watched, vec![Chain::Base]);
        assert_eq!(
            registry.watch(Chain::Base).map(|chain| chain.chain),
            Some(Chain::Base)
        );
        assert_eq!(
            registry.watch(Chain::Ethereum).map(|chain| chain.chain),
            None
        );
    }

    /// Two primary claimants are a config contradiction, refused with the
    /// full claimant list.
    #[test]
    fn registry_rejects_two_primary_claimants() {
        let configs = BTreeMap::from([
            (Chain::Base, chain_config(Some(trading_config_toml()))),
            (Chain::Ethereum, chain_config(Some(trading_config_toml()))),
        ]);

        let error =
            ChainRegistry::new(&configs, secrets_for(&[Chain::Base, Chain::Ethereum])).unwrap_err();

        assert!(
            matches!(
                error,
                ChainRegistryError::MultiplePrimaryChains { ref chains }
                    if chains == &vec![Chain::Base, Chain::Ethereum]
            ),
            "got: {error}"
        );
    }

    /// Watched chains with no primary claimant fail with a named error: the
    /// bot cannot guess where trading and the cash vaults live.
    #[test]
    fn registry_rejects_watched_chains_with_no_primary() {
        let configs = BTreeMap::from([(
            Chain::Base,
            chain_config(Some(primary_trading_config_toml(false))),
        )]);

        let error = ChainRegistry::new(&configs, secrets_for(&[Chain::Base])).unwrap_err();

        assert!(
            matches!(
                error,
                ChainRegistryError::NoPrimaryChain { ref chains } if chains == &vec![Chain::Base]
            ),
            "got: {error}"
        );
    }

    #[test]
    fn registry_rejects_a_chain_with_no_secrets_entry() {
        let configs = BTreeMap::from([
            (Chain::Base, chain_config(Some(trading_config_toml()))),
            (Chain::Ethereum, chain_config(None)),
        ]);

        let error = ChainRegistry::new(&configs, secrets_for(&[Chain::Base])).unwrap_err();

        assert!(
            matches!(
                error,
                ChainRegistryError::MissingSecrets {
                    chain: Chain::Ethereum
                }
            ),
            "got: {error}"
        );
    }

    #[test]
    fn registry_rejects_a_secrets_entry_with_no_config_table() {
        let configs = BTreeMap::from([(Chain::Base, chain_config(Some(trading_config_toml())))]);

        let error =
            ChainRegistry::new(&configs, secrets_for(&[Chain::Base, Chain::HyperEvm])).unwrap_err();

        assert!(
            matches!(
                error,
                ChainRegistryError::UnconfiguredSecrets {
                    chain: Chain::HyperEvm
                }
            ),
            "got: {error}"
        );
    }

    #[test]
    fn registry_resolves_rpc_urls_for_trading_and_transport_chains() {
        let configs = BTreeMap::from([
            (Chain::Base, chain_config(Some(trading_config_toml()))),
            (Chain::Ethereum, chain_config(None)),
        ]);

        let registry =
            ChainRegistry::new(&configs, secrets_for(&[Chain::Base, Chain::Ethereum])).unwrap();

        assert_eq!(registry.primary().chain, Chain::Base);
        assert_eq!(
            registry.rpc_url(Chain::Base),
            Some(&rpc_url_for(Chain::Base)),
            "the trading chain resolves through its own entry"
        );
        assert_eq!(
            registry.rpc_url(Chain::Ethereum),
            Some(&rpc_url_for(Chain::Ethereum)),
            "a transport chain resolves through the map"
        );
        assert_eq!(
            registry.rpc_url(Chain::HyperEvm),
            None,
            "a chain with no entry has no endpoint"
        );
    }

    #[test]
    fn registry_resolves_confirmation_depths_per_chain() {
        // Depth gates settlement finality and flows into every signer, so a
        // swap between the trading and transport lookups (or a depth taken
        // from the wrong chain's entry) must fail here, not on chain.
        let configs = BTreeMap::from([
            (
                Chain::Base,
                ChainConfig {
                    lifecycle: ChainLifecycle::Active,
                    required_confirmations: 3,
                    trading: Some(trading_config_toml()),
                },
            ),
            (
                Chain::Ethereum,
                ChainConfig {
                    lifecycle: ChainLifecycle::Active,
                    required_confirmations: 12,
                    trading: None,
                },
            ),
        ]);

        let registry =
            ChainRegistry::new(&configs, secrets_for(&[Chain::Base, Chain::Ethereum])).unwrap();

        assert_eq!(
            registry.primary().required_confirmations,
            3,
            "the trading chain's depth comes from its outer chain entry"
        );
        assert_eq!(registry.required_confirmations(Chain::Base), Some(3));
        assert_eq!(
            registry.required_confirmations(Chain::Ethereum),
            Some(12),
            "a transport chain resolves its own depth, not the trading chain's"
        );
        assert_eq!(registry.required_confirmations(Chain::HyperEvm), None);
    }

    #[test]
    fn trading_chain_new_maps_config_fields_to_their_own_slots() {
        // Three same-typed Address fields flow through TradingChain::new. A swap in the
        // mapping would compile silently, so assert each distinct address lands
        // in its own slot (and that a managed inventory resolves to itself).
        let config: TradingConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory_adapters = []\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 7\n\
             primary = true\n\
             redemption_wallet = \"0x4444444444444444444444444444444444444444\"\n\
             [assets.equities.COIN]\n\
             tokenized_equity = \"0x6666666666666666666666666666666666666666\"\n\
             tokenized_equity_derivative = \"0x5555555555555555555555555555555555555555\"\n\
             trading = \"enabled\"\n\
             rebalancing = \"disabled\"\n\
             wrapped_equity_recovery = \"disabled\"",
        )
        .unwrap();

        let ctx =
            TradingChain::new(Chain::Base, &base_chain_config(), &config, dummy_rpc_url()).unwrap();

        assert_eq!(
            ctx.orderbook,
            alloy::primitives::address!("0x1111111111111111111111111111111111111111"),
        );
        assert_eq!(
            ctx.inventory,
            InventoryMode::Managed {
                inventory: alloy::primitives::address!(
                    "0x2222222222222222222222222222222222222222"
                ),
            },
        );
        assert_eq!(
            ctx.inventory_address(),
            alloy::primitives::address!("0x2222222222222222222222222222222222222222"),
        );
        assert_eq!(
            ctx.vault_owner,
            alloy::primitives::address!("0x3333333333333333333333333333333333333333"),
        );
        assert_eq!(
            ctx.redemption_wallet,
            Some(alloy::primitives::address!(
                "0x4444444444444444444444444444444444444444"
            )),
        );
        let coin = ctx
            .assets
            .tokenized_equity(&Symbol::new("COIN").unwrap())
            .expect("the configured assets table must ride along into TradingChain");
        assert_eq!(
            coin,
            alloy::primitives::address!("0x6666666666666666666666666666666666666666"),
        );
    }

    #[test]
    fn managed_mode_without_inventory_fails() {
        let config: TradingConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory_adapters = []\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        )
        .unwrap();

        let error = TradingChain::new(Chain::Base, &base_chain_config(), &config, dummy_rpc_url())
            .unwrap_err();

        assert!(
            matches!(error, ChainConfigError::ManagedWithoutInventory),
            "expected ManagedWithoutInventory, got: {error:?}"
        );
    }

    #[test]
    fn legacy_mode_with_inventory_fails() {
        let config: TradingConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"legacy\"\n\
             inventory_adapters = []\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        )
        .unwrap();

        let error = TradingChain::new(Chain::Base, &base_chain_config(), &config, dummy_rpc_url())
            .unwrap_err();

        assert!(
            matches!(error, ChainConfigError::LegacyWithInventory { .. }),
            "expected LegacyWithInventory, got: {error:?}"
        );
    }

    #[test]
    fn legacy_mode_resolves_inventory_address_to_orderbook() {
        // Legacy: no inventory contract, so rebalancing settles on the orderbook
        // and inventory_address() must return the orderbook address.
        let config: TradingConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"legacy\"\n\
             inventory_adapters = []\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"\n\
             order_fill_poll_interval_secs = 1\n\
             primary = true",
        )
        .unwrap();

        let ctx =
            TradingChain::new(Chain::Base, &base_chain_config(), &config, dummy_rpc_url()).unwrap();

        assert_eq!(ctx.inventory, InventoryMode::Legacy);
        assert_eq!(
            ctx.inventory_address(),
            alloy::primitives::address!("0x1111111111111111111111111111111111111111"),
        );
    }

    #[test]
    fn ingestion_cutoff_deserializes_finalized() {
        let wrapper: CutoffWrapper = toml::from_str("ingestion_cutoff = \"finalized\"").unwrap();

        assert_eq!(wrapper.ingestion_cutoff, IngestionCutoffTag::Finalized);
    }

    #[test]
    fn ingestion_cutoff_rejects_unknown_variant() {
        // An unrecognized variant must fail to deserialize with an error
        // that names the unknown value. Toml 0.9 includes the bad value
        // ("garbage") in the error but not the field name.
        let result: Result<CutoffWrapper, _> = toml::from_str("ingestion_cutoff = \"garbage\"");

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("garbage"),
            "expected unknown-variant error naming the bad value, got: {error}"
        );
    }

    #[test]
    fn ingestion_cutoff_rejects_missing_field() {
        // The field is required; a config without it must fail to deserialize.
        let result: Result<TradingConfig, _> = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory_adapters = []\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1",
        );

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("ingestion_cutoff"),
            "expected missing-field error for ingestion_cutoff, got: {error}"
        );
    }
}
