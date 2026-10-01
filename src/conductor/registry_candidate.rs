//! Read-only contract checks for a proposed registry copy.
use super::{
    Address, Chain, Ctx, IssuanceClient, OrchestratorEntryMissing, ProviderBuilder, Symbol,
    VaultModeReader, VaultModeTag, bounded_rpc_client, confirm_configured_assets_respond,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum ContractMismatch {
    #[error("{chain}/{symbol} {field} at {token} reports {decimals} decimals; expected 18")]
    Decimals {
        chain: Chain,
        symbol: Symbol,
        token: Address,
        decimals: u8,
        field: &'static str,
    },
    #[error("{chain}/{symbol} vault {token} wraps {actual}, expected {expected}")]
    Underlying {
        chain: Chain,
        symbol: Symbol,
        token: Address,
        expected: Address,
        actual: Address,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CandidateContractError {
    #[error("registry contract identity refused: {0:#}")]
    Refused(anyhow::Error),
    #[error("registry contract checks deferred: {0:#}")]
    Deferred(anyhow::Error),
}

impl CandidateContractError {
    fn probe(error: anyhow::Error) -> Self {
        if error.downcast_ref::<ContractMismatch>().is_some() {
            Self::Refused(error)
        } else {
            Self::Deferred(error)
        }
    }
}

/// Probe every added listing, including disabled ones, using the startup
/// canary. Existing identity is checked before this function is called.
pub(crate) async fn validate_registry_candidate_contracts(
    running: &Ctx,
    candidate: &Ctx,
) -> Result<(), CandidateContractError> {
    let issuance = IssuanceClient::new(
        candidate.issuance.base_url.clone(),
        candidate.issuance.api_key.header_value(),
    )
    .map_err(|error| CandidateContractError::Deferred(error.into()))?;
    for (role, chain) in candidate.chains.hedged_with_roles() {
        let previous = running.chains.hedged_chain(chain.chain);
        let newly_selected = |symbol: &Symbol, row: &st0x_config::ChainEquityAsset| {
            newly_selected_listing(
                role,
                previous.map(|chain| &chain.assets),
                &chain.assets,
                symbol,
                row,
            )
        };
        let mut probes = chain.clone();
        probes.assets.equities.symbols.retain(|symbol, row| {
            previous.is_none_or(|chain| !chain.assets.equities.symbols.contains_key(symbol))
                || newly_selected(symbol, row)
        });
        if !probes.assets.equities.symbols.is_empty() {
            let rpc = bounded_rpc_client(chain.rpc_url.clone())
                .map_err(CandidateContractError::Deferred)?;
            let provider = ProviderBuilder::new().connect_client(rpc);
            confirm_configured_assets_respond(&provider, &probes)
                .await
                .map_err(CandidateContractError::probe)?;
        }
        // Issuance vault mode and the orchestrator entry matter only where
        // the chain wraps and redeems the equity; a trading-only enable on a
        // secondary is hedged and never minted there.
        for (symbol, _) in role.rebalanced_equities(&chain.assets) {
            if !newly_rebalanced(role, previous.map(|chain| &chain.assets), symbol) {
                continue;
            }
            let mode = issuance
                .vault_mode(symbol)
                .await
                .map_err(|error| CandidateContractError::Deferred(error.into()))?;
            if mode == VaultModeTag::Orchestrator
                && candidate
                    .orchestrator
                    .as_ref()
                    .is_none_or(|config| config.addresses.get(chain.chain).is_none())
            {
                return Err(CandidateContractError::Refused(
                    OrchestratorEntryMissing {
                        chain: chain.chain,
                        symbol: symbol.clone(),
                    }
                    .into(),
                ));
            }
        }
    }
    Ok(())
}

fn newly_selected_listing(
    role: st0x_config::ChainRole,
    previous: Option<&st0x_config::ChainAssets>,
    candidate: &st0x_config::ChainAssets,
    symbol: &Symbol,
    row: &st0x_config::ChainEquityAsset,
) -> bool {
    let old = previous.and_then(|assets| assets.equities.symbols.get(symbol));
    let starts_trading = row.trading == st0x_config::OperationMode::Enabled
        && old.is_none_or(|old| old.trading != st0x_config::OperationMode::Enabled);
    let selected = role
        .rebalanced_equities(candidate)
        .iter()
        .any(|(selected, _)| *selected == symbol);
    starts_trading || (selected && newly_rebalanced(role, previous, symbol))
}

/// Whether `symbol`, rebalanced on the candidate, was not rebalanced on the
/// running chain in this role.
fn newly_rebalanced(
    role: st0x_config::ChainRole,
    previous: Option<&st0x_config::ChainAssets>,
    symbol: &Symbol,
) -> bool {
    !previous.is_some_and(|assets| {
        role.rebalanced_equities(assets)
            .iter()
            .any(|(selected, _)| *selected == symbol)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secondary_trading_enable_needs_preflight_even_without_rebalancing() {
        use st0x_config::{
            ChainAssets, ChainEquityAsset, ChainRole, OperationMode, RebalancingMode,
        };
        let symbol = Symbol::new("AAPL").unwrap();
        let mut old = ChainAssets::default();
        old.equities.symbols.insert(
            symbol.clone(),
            ChainEquityAsset {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_ids: vec![],
                trading: OperationMode::Disabled,
                rebalancing: RebalancingMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
                target_share: None,
            },
        );
        let mut candidate = old.clone();
        candidate.equities.symbols.get_mut(&symbol).unwrap().trading = OperationMode::Enabled;
        let row = &candidate.equities.symbols[&symbol];
        assert!(newly_selected_listing(
            ChainRole::Secondary,
            Some(&old),
            &candidate,
            &symbol,
            row
        ));
        assert!(
            ChainRole::Secondary
                .rebalanced_equities(&candidate)
                .is_empty(),
            "a trading-only secondary enable needs no issuance or orchestrator check"
        );
        assert!(!newly_selected_listing(
            ChainRole::Secondary,
            Some(&candidate),
            &candidate,
            &symbol,
            row
        ));
        candidate.equities.symbols.get_mut(&symbol).unwrap().trading = OperationMode::Disabled;
        candidate
            .equities
            .symbols
            .get_mut(&symbol)
            .unwrap()
            .rebalancing = RebalancingMode::Enabled;
        assert!(newly_selected_listing(
            ChainRole::Secondary,
            Some(&old),
            &candidate,
            &symbol,
            &candidate.equities.symbols[&symbol]
        ));
        assert!(newly_rebalanced(ChainRole::Secondary, Some(&old), &symbol));
        assert!(!newly_rebalanced(
            ChainRole::Secondary,
            Some(&candidate),
            &symbol
        ));
    }

    #[test]
    fn contract_mismatch_is_refused_through_context() {
        let mismatch = ContractMismatch::Decimals {
            chain: Chain::Base,
            symbol: Symbol::new("AAPL").unwrap(),
            token: Address::ZERO,
            decimals: 6,
            field: "tokenized_equity",
        };
        let error = anyhow::Error::new(mismatch).context("candidate boot probe");
        assert!(matches!(
            CandidateContractError::probe(error),
            CandidateContractError::Refused(_)
        ));
    }

    #[test]
    fn transport_error_is_deferred() {
        let error = anyhow::anyhow!("RPC temporarily unavailable");
        assert!(matches!(
            CandidateContractError::probe(error),
            CandidateContractError::Deferred(_)
        ));
    }
}
