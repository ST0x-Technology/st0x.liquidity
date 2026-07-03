//! EVM configuration types: chain config, RPC secrets, and runtime context.

use alloy::primitives::Address;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

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
/// `[raindex].inventory_mode`.
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

/// Errors resolving an [`EvmConfig`] into a runtime [`EvmCtx`].
#[derive(Debug, Error)]
pub enum EvmConfigError {
    #[error(
        "[raindex] inventory_mode = \"managed\" requires an `inventory` address, \
         but none was configured"
    )]
    ManagedWithoutInventory,
    #[error(
        "[raindex] inventory_mode = \"legacy\" forbids an `inventory` address, \
         but {inventory} was configured; set inventory_mode = \"managed\" to \
         enable a shared inventory"
    )]
    LegacyWithInventory { inventory: Address },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvmConfig {
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
    /// Address that owns the Raindex orders and vaults on-chain -- the key every
    /// `vaultBalance2` read, vault-registry entry, and order-owner fill match is
    /// scoped by. Required and explicit (no fallback): this parameter determines
    /// fund-routing correctness, so a missing value must fail at startup rather
    /// than silently assume an owner. Pre-migration (vaults owned by the bot
    /// EOA) set it to the `[wallet]` address. Once the shared-inventory
    /// migration moves the orders/vaults under the inventory contract,
    /// `msg.sender` to Raindex is the inventory, so it becomes the vault owner
    /// and this must be flipped to the `inventory` address in the same change
    /// that grants the bot `OPERATOR_ROLE` and migrates vaults.
    pub vault_owner: Address,
    pub deployment_block: u64,
    pub required_confirmations: u64,
    pub ingestion_cutoff: IngestionCutoff,
}

impl EvmConfig {
    /// Resolves the configured mode + optional `inventory` into an
    /// [`InventoryMode`], failing fast on the two contradictory combinations:
    /// `managed` without an inventory, or `legacy` with one.
    fn resolve_inventory_mode(&self) -> Result<InventoryMode, EvmConfigError> {
        match (self.inventory_mode, self.inventory) {
            (InventoryModeTag::Legacy, None) => Ok(InventoryMode::Legacy),
            (InventoryModeTag::Legacy, Some(inventory)) => {
                Err(EvmConfigError::LegacyWithInventory { inventory })
            }
            (InventoryModeTag::Managed, Some(inventory)) => {
                Ok(InventoryMode::Managed { inventory })
            }
            (InventoryModeTag::Managed, None) => Err(EvmConfigError::ManagedWithoutInventory),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvmSecrets {
    /// HTTP RPC URL for the orderbook chain. Drives continuous `eth_getLogs`
    /// fill polling and all read-only contract calls (single transport, no
    /// WebSocket).
    #[serde(rename = "rpc_url")]
    pub rpc: Url,
    /// Base chain RPC URL for wallet operations. Required when `[wallet]`
    /// is configured.
    #[serde(rename = "base_rpc_url")]
    pub base: Option<Url>,
    /// Ethereum mainnet RPC URL for wallet operations. Required when
    /// `[wallet]` is configured.
    #[serde(rename = "ethereum_rpc_url")]
    pub ethereum: Option<Url>,
}

#[derive(Clone)]
pub struct EvmCtx {
    pub rpc_url: Url,
    pub orderbook: Address,
    pub inventory: InventoryMode,
    pub vault_owner: Address,
    pub deployment_block: u64,
    pub required_confirmations: u64,
    pub ingestion_cutoff: IngestionCutoff,
}

impl std::fmt::Debug for EvmCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvmCtx")
            .field("rpc_url", &"[REDACTED]")
            .field("orderbook", &self.orderbook)
            .field("inventory", &self.inventory)
            .field("vault_owner", &self.vault_owner)
            .field("deployment_block", &self.deployment_block)
            .field("required_confirmations", &self.required_confirmations)
            .field("ingestion_cutoff", &self.ingestion_cutoff)
            .finish()
    }
}

impl EvmCtx {
    pub fn new(config: &EvmConfig, secrets: EvmSecrets) -> Result<Self, EvmConfigError> {
        Ok(Self {
            rpc_url: secrets.rpc,
            orderbook: config.orderbook,
            inventory: config.resolve_inventory_mode()?,
            vault_owner: config.vault_owner,
            deployment_block: config.deployment_block,
            required_confirmations: config.required_confirmations,
            ingestion_cutoff: config.ingestion_cutoff,
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

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct CutoffWrapper {
        ingestion_cutoff: IngestionCutoff,
    }

    fn dummy_secrets() -> EvmSecrets {
        EvmSecrets {
            rpc: Url::parse("http://localhost:8545").unwrap(),
            base: None,
            ethereum: None,
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
        let result: Result<EvmConfig, _> = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             deployment_block = 1\n\
             required_confirmations = 3\n\
             ingestion_cutoff = \"safe\"",
        );

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("vault_owner"),
            "expected missing-field error for vault_owner, got: {error}"
        );
    }

    #[test]
    fn evm_ctx_new_maps_config_fields_to_their_own_slots() {
        // Three same-typed Address fields flow through EvmCtx::new. A swap in the
        // mapping would compile silently, so assert each distinct address lands
        // in its own slot (and that a managed inventory resolves to itself).
        let config: EvmConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             required_confirmations = 3\n\
             ingestion_cutoff = \"safe\"",
        )
        .unwrap();

        let ctx = EvmCtx::new(&config, dummy_secrets()).unwrap();

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
    }

    #[test]
    fn managed_mode_without_inventory_fails() {
        let config: EvmConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             required_confirmations = 3\n\
             ingestion_cutoff = \"safe\"",
        )
        .unwrap();

        let error = EvmCtx::new(&config, dummy_secrets()).unwrap_err();

        assert!(
            matches!(error, EvmConfigError::ManagedWithoutInventory),
            "expected ManagedWithoutInventory, got: {error:?}"
        );
    }

    #[test]
    fn legacy_mode_with_inventory_fails() {
        let config: EvmConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"legacy\"\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             required_confirmations = 3\n\
             ingestion_cutoff = \"safe\"",
        )
        .unwrap();

        let error = EvmCtx::new(&config, dummy_secrets()).unwrap_err();

        assert!(
            matches!(error, EvmConfigError::LegacyWithInventory { .. }),
            "expected LegacyWithInventory, got: {error:?}"
        );
    }

    #[test]
    fn legacy_mode_resolves_inventory_address_to_orderbook() {
        // Legacy: no inventory contract, so rebalancing settles on the orderbook
        // and inventory_address() must return the orderbook address.
        let config: EvmConfig = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"legacy\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             required_confirmations = 3\n\
             ingestion_cutoff = \"safe\"",
        )
        .unwrap();

        let ctx = EvmCtx::new(&config, dummy_secrets()).unwrap();

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
        let result: Result<EvmConfig, _> = toml::from_str(
            "orderbook = \"0x1111111111111111111111111111111111111111\"\n\
             inventory_mode = \"managed\"\n\
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
             vault_owner = \"0x3333333333333333333333333333333333333333\"\n\
             deployment_block = 1\n\
             required_confirmations = 3",
        );

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("ingestion_cutoff"),
            "expected missing-field error for ingestion_cutoff, got: {error}"
        );
    }
}
