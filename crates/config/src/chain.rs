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

/// Which block tag to use as the fill-ingestion cutoff.
///
/// The cutoff caps what the fill monitor treats as safe to ingest. Tags differ
/// in their reorg-safety guarantees and their distance behind the chain tip.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
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
}

impl std::fmt::Display for IngestionCutoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Safe => f.write_str("safe"),
            Self::Finalized => f.write_str("finalized"),
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
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
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
    pub ingestion_cutoff: IngestionCutoff,
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
    pub redemption_wallet: Option<Address>,
    pub assets: ChainAssets,
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

        Ok(Self {
            chain,
            rpc_url,
            required_confirmations: config.required_confirmations,
            orderbook: trading.orderbook,
            inventory: trading.resolve_inventory_mode()?,
            inventory_adapters: trading.inventory_adapters.clone(),
            vault_owner: trading.vault_owner,
            deployment_block: trading.deployment_block,
            ingestion_cutoff: trading.ingestion_cutoff,
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
/// The trading chain is its own field rather than an entry in the map, so
/// "exactly one trading chain" holds by construction instead of being
/// re-checked by every reader.
#[derive(Clone, Debug)]
pub struct ChainRegistry {
    trading: TradingChain,
    transport: BTreeMap<Chain, ChainCtx>,
}

/// Errors assembling a [`ChainRegistry`] from config and secrets.
#[derive(Debug, Error)]
pub enum ChainRegistryError {
    #[error("no [chains.<name>] table is configured; the bot must act on at least one chain")]
    NoChains,
    #[error(
        "no configured chain has a [chains.<name>.trading] table; the bot would have \
         no orderbook to watch and would place no hedges"
    )]
    NoTradingChain,
    #[error(
        "[chains.{chain}] is configured but the secrets file has no [chains.{chain}] \
         entry supplying its rpc_url"
    )]
    MissingSecrets { chain: Chain },
    #[error(
        "the secrets file has a [chains.{chain}] entry but the config file has no \
         [chains.{chain}] table describing that chain"
    )]
    UnconfiguredSecrets { chain: Chain },
    #[error(
        "{} chains configure a [trading] table ({}), but this build drives a single \
         fill watcher: the chains beyond the first would be fully described and never \
         watched, so their fills would go unhedged",
        chains.len(),
        chains.iter().map(|chain| chain.as_str()).collect::<Vec<_>>().join(", ")
    )]
    MultipleTradingChains { chains: Vec<Chain> },
    #[error(transparent)]
    Entry(#[from] ChainConfigError),
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
        if configs.is_empty() {
            return Err(ChainRegistryError::NoChains);
        }

        let trading_chains: Vec<(Chain, &ChainConfig, &TradingConfig)> = configs
            .iter()
            .filter_map(|(chain, config)| {
                config
                    .trading
                    .as_ref()
                    .map(|trading| (*chain, config, trading))
            })
            .collect();

        let (trading_chain, chain_config, trading_table) = match trading_chains.as_slice() {
            [] => return Err(ChainRegistryError::NoTradingChain),
            [only] => *only,
            many => {
                return Err(ChainRegistryError::MultipleTradingChains {
                    chains: many.iter().map(|(chain, _, _)| *chain).collect(),
                });
            }
        };

        let mut take_rpc_url = |chain: Chain| {
            secrets
                .remove(&chain)
                .ok_or(ChainRegistryError::MissingSecrets { chain })
                .map(|entry| entry.rpc_url)
        };

        let trading = TradingChain::new(
            trading_chain,
            chain_config,
            trading_table,
            take_rpc_url(trading_chain)?,
        )?;

        let mut transport = BTreeMap::new();
        for (chain, config) in configs {
            if *chain == trading_chain {
                continue;
            }

            transport.insert(
                *chain,
                ChainCtx {
                    chain: *chain,
                    rpc_url: take_rpc_url(*chain)?,
                    required_confirmations: config.required_confirmations,
                },
            );
        }

        if let Some(chain) = secrets.keys().next() {
            return Err(ChainRegistryError::UnconfiguredSecrets { chain: *chain });
        }

        Ok(Self { trading, transport })
    }

    /// The single chain this build trades on.
    ///
    /// Every call site is a place that still assumes one trading chain, so
    /// this is also the list of what per-chain fill watchers have to reach.
    pub fn sole_trading(&self) -> &TradingChain {
        &self.trading
    }

    /// Mutable access to the trading chain, so a fixture can vary one field
    /// (a settlement mode, an unreachable RPC) without rebuilding the registry.
    #[cfg(any(test, feature = "test-support"))]
    pub fn sole_trading_mut(&mut self) -> &mut TradingChain {
        &mut self.trading
    }

    /// A registry holding one trading chain and nothing else.
    ///
    /// Test and fixture construction only: production registries come from
    /// [`Self::new`], which is what enforces the config/secrets pairing.
    #[cfg(any(test, feature = "test-support"))]
    pub fn single_trading_chain(trading: TradingChain) -> Self {
        Self {
            trading,
            transport: BTreeMap::new(),
        }
    }

    /// The RPC endpoint configured for `chain`, whether it trades or only
    /// carries cash. `None` when the chain has no `[chains.<name>]` entry.
    pub fn rpc_url(&self, chain: Chain) -> Option<&Url> {
        if self.trading.chain == chain {
            return Some(&self.trading.rpc_url);
        }

        self.transport.get(&chain).map(|entry| &entry.rpc_url)
    }

    /// The confirmation depth configured for `chain`. `None` when the chain
    /// has no `[chains.<name>]` entry. Per chain because the depth encodes one
    /// chain's reorg behaviour; a global value cannot be right for all.
    pub fn required_confirmations(&self, chain: Chain) -> Option<u64> {
        if self.trading.chain == chain {
            return Some(self.trading.required_confirmations);
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
        ingestion_cutoff: IngestionCutoff,
    }

    fn dummy_rpc_url() -> Url {
        Url::parse("http://localhost:8545").unwrap()
    }

    /// A chain entry carrying only what lives outside the `[trading]` table,
    /// so a fixture can exercise `TradingConfig` on its own.
    fn base_chain_config() -> ChainConfig {
        ChainConfig {
            required_confirmations: 3,
            trading: None,
        }
    }

    #[test]
    fn ingestion_cutoff_deserializes_safe() {
        let wrapper: CutoffWrapper = toml::from_str("ingestion_cutoff = \"safe\"").unwrap();

        assert_eq!(wrapper.ingestion_cutoff, IngestionCutoff::Safe);
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
             ingestion_cutoff = \"safe\"",
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
             ingestion_cutoff = \"safe\"",
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
             ingestion_cutoff = \"safe\"",
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
             ingestion_cutoff = \"safe\"",
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
             ingestion_cutoff = \"safe\"",
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
             ingestion_cutoff = \"safe\"",
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
        toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"legacy\"\n\
             inventory_adapters = []\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             ingestion_cutoff = \"safe\"",
        )
        .unwrap()
    }

    fn chain_config(trading: Option<TradingConfig>) -> ChainConfig {
        ChainConfig {
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

    #[test]
    fn registry_rejects_a_config_with_no_trading_chain() {
        let configs = BTreeMap::from([(Chain::Ethereum, chain_config(None))]);

        let error = ChainRegistry::new(&configs, secrets_for(&[Chain::Ethereum])).unwrap_err();

        assert!(
            matches!(error, ChainRegistryError::NoTradingChain),
            "got: {error}"
        );
    }

    /// The one refusal that keeps unhedged exposure out of production: a second
    /// trading chain would be fully described and never watched.
    #[test]
    fn registry_rejects_a_second_trading_chain() {
        let configs = BTreeMap::from([
            (Chain::Base, chain_config(Some(trading_config_toml()))),
            (Chain::Ethereum, chain_config(Some(trading_config_toml()))),
        ]);

        let error =
            ChainRegistry::new(&configs, secrets_for(&[Chain::Base, Chain::Ethereum])).unwrap_err();

        assert!(
            matches!(
                error,
                ChainRegistryError::MultipleTradingChains { ref chains }
                    if chains == &vec![Chain::Base, Chain::Ethereum]
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

        assert_eq!(registry.sole_trading().chain, Chain::Base);
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
                    required_confirmations: 3,
                    trading: Some(trading_config_toml()),
                },
            ),
            (
                Chain::Ethereum,
                ChainConfig {
                    required_confirmations: 12,
                    trading: None,
                },
            ),
        ]);

        let registry =
            ChainRegistry::new(&configs, secrets_for(&[Chain::Base, Chain::Ethereum])).unwrap();

        assert_eq!(
            registry.sole_trading().required_confirmations,
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
             ingestion_cutoff = \"safe\"",
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
             ingestion_cutoff = \"safe\"",
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
             ingestion_cutoff = \"safe\"",
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

        assert_eq!(wrapper.ingestion_cutoff, IngestionCutoff::Finalized);
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
