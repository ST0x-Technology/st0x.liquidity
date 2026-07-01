//! EVM configuration types: chain config, RPC secrets, and runtime context.

use alloy::primitives::Address;
use serde::Deserialize;
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvmConfig {
    pub orderbook: Address,
    /// Shared `RaindexInventory` (raindex.governance) that owns the Raindex
    /// vaults. All venue adapters (Bebop hook, univ4 hook) and this bot's own
    /// rebalancing path settle through it — `deposit4`/`withdraw4` on this
    /// address instead of on the orderbook. Fill events on the pooled vaults
    /// are also surfaced here as `OperatorDeposit`/`OperatorWithdraw`.
    pub inventory: Address,
    pub deployment_block: u64,
    pub required_confirmations: u64,
    pub ingestion_cutoff: IngestionCutoff,
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
    pub inventory: Address,
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
            .field("deployment_block", &self.deployment_block)
            .field("required_confirmations", &self.required_confirmations)
            .field("ingestion_cutoff", &self.ingestion_cutoff)
            .finish()
    }
}

impl EvmCtx {
    pub fn new(config: &EvmConfig, secrets: EvmSecrets) -> Self {
        Self {
            rpc_url: secrets.rpc,
            orderbook: config.orderbook,
            inventory: config.inventory,
            deployment_block: config.deployment_block,
            required_confirmations: config.required_confirmations,
            ingestion_cutoff: config.ingestion_cutoff,
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

    #[test]
    fn ingestion_cutoff_deserializes_safe() {
        let wrapper: CutoffWrapper = toml::from_str("ingestion_cutoff = \"safe\"").unwrap();

        assert_eq!(wrapper.ingestion_cutoff, IngestionCutoff::Safe);
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
             inventory = \"0x2222222222222222222222222222222222222222\"\n\
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
