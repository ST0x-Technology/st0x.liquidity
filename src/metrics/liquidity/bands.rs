//! The `EquityBands` family: where each chain's vault sits against its own
//! rebalancing band, as the allocation planner sees it.
//!
//! The trigger plans one symbol at a time, so the publisher keeps the latest
//! bands of every symbol and replaces the whole family with them each time
//! one symbol changes. A symbol the planner could not size has no series.
//! The wrapper ratios the trigger reads go to the shared store, where the
//! inventory publisher values each chain's wrapped shares with them.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
use std::time::SystemTime;

use st0x_evm::Chain;
use st0x_execution::{FractionalShares, Symbol};

use super::{LiqFamilies, LiqFamily, LiqMetric, LiqSample, float_value, push_sample, strip_prefix};
use crate::rebalancing::trigger::allocation::{BandVerdict, SymbolBands};

/// Publishes the `EquityBands` family. One per process; the rebalancing
/// trigger owns it.
pub(crate) struct EquityBandPublisher {
    families: &'static LiqFamilies,
    bands: Mutex<BTreeMap<Symbol, SymbolBands>>,
}

impl EquityBandPublisher {
    pub(crate) fn new(families: &'static LiqFamilies) -> Self {
        Self {
            families,
            bands: Mutex::new(BTreeMap::new()),
        }
    }

    /// Records the wrapper ratio last read for `symbol` on `chain`.
    pub(crate) fn record_wrapper_ratio(
        &self,
        symbol: &Symbol,
        chain: Chain,
        per_wrapped: FractionalShares,
    ) {
        self.families
            .record_wrapper_ratio(symbol, chain, per_wrapped);
    }

    /// Records `symbol`'s latest bands, or forgets them when the planner
    /// could not size the symbol, and publishes every symbol's. The trigger
    /// plans symbols on parallel workers, so the lock is held through the
    /// publish and publishes never interleave. The bands are read before the
    /// lock, so a check whose read was slower can still publish older bands
    /// over newer ones; the symbol's next check corrects them.
    pub(crate) fn record(&self, symbol: &Symbol, bands: Option<SymbolBands>) {
        let mut held = self.bands.lock().unwrap_or_else(PoisonError::into_inner);
        match bands {
            Some(bands) => held.insert(symbol.clone(), bands),
            None => held.remove(symbol),
        };

        self.families.replace(
            LiqFamily::EquityBands,
            band_samples(&held),
            SystemTime::now(),
        );
        drop(held);
    }
}

/// `liq_equity_underlying_total` for every symbol held, and the share,
/// verdict, target and deviation of each of its chain bands.
fn band_samples(held: &BTreeMap<Symbol, SymbolBands>) -> Vec<LiqSample> {
    let mut samples = Vec::new();
    for (symbol, bands) in held {
        push_sample(
            &mut samples,
            LiqMetric::EquityUnderlyingTotal,
            vec![("symbol", strip_prefix(symbol.as_str()).to_string())],
            float_value(bands.total.inner()),
        );
        for band in &bands.chains {
            let labels = vec![
                ("chain", band.chain.as_str().to_string()),
                ("symbol", strip_prefix(symbol.as_str()).to_string()),
            ];
            let verdict = match band.verdict {
                BandVerdict::Below => -1.0,
                BandVerdict::Within => 0.0,
                BandVerdict::Above => 1.0,
            };
            push_sample(
                &mut samples,
                LiqMetric::EquityChainShare,
                labels.clone(),
                float_value(band.share),
            );
            push_sample(
                &mut samples,
                LiqMetric::EquityChainTarget,
                labels.clone(),
                float_value(band.target.inner()),
            );
            push_sample(
                &mut samples,
                LiqMetric::EquityChainDeviation,
                labels.clone(),
                float_value(band.band.inner()),
            );
            push_sample(
                &mut samples,
                LiqMetric::EquityChainVerdict,
                labels,
                Ok(verdict),
            );
        }
    }

    samples
}

#[cfg(test)]
mod tests {
    use rain_math_float::Float;

    use st0x_config::{DeviationBand, TargetShare};
    use st0x_evm::Chain;
    use st0x_execution::FractionalShares;

    use super::*;
    use crate::metrics::liquidity::inventory::tests::{leaked_families, rendered_store};
    use crate::metrics::liquidity::tests::series;
    use crate::rebalancing::trigger::allocation::ChainBand;

    fn band(chain: Chain, share: &str, verdict: BandVerdict) -> ChainBand {
        ChainBand {
            chain,
            share: Float::parse(share.to_string()).unwrap(),
            target: TargetShare::new(Float::parse("0.5".to_string()).unwrap()).unwrap(),
            band: DeviationBand::new(Float::parse("0.05".to_string()).unwrap()).unwrap(),
            verdict,
        }
    }

    fn symbol_bands(total: &str, chains: Vec<ChainBand>) -> SymbolBands {
        SymbolBands {
            total: FractionalShares::new(Float::parse(total.to_string()).unwrap()),
            chains,
        }
    }

    fn rendered(families: &LiqFamilies) -> BTreeMap<String, f64> {
        rendered_store(families)
            .into_iter()
            .filter(|((name, _), _)| name.starts_with("liq_equity_chain_"))
            .map(|((name, labels), value)| (format!("{name}{labels:?}"), value))
            .collect()
    }

    /// A wrapper ratio the trigger reads goes to the shared store, whatever
    /// the planner makes of the symbol.
    #[test]
    fn wrapper_ratios_go_to_the_shared_store() {
        let families = leaked_families();
        let publisher = EquityBandPublisher::new(families);
        let aapl = Symbol::new("tAAPL").unwrap();
        let ratio = |value: &str| FractionalShares::new(Float::parse(value.to_string()).unwrap());

        publisher.record_wrapper_ratio(&aapl, Chain::Robinhood, ratio("1.5"));
        publisher.record(&aapl, None);

        let ratios: Vec<(String, Chain, String)> = families
            .wrapper_ratios()
            .into_iter()
            .map(|((symbol, chain), per_wrapped)| {
                (symbol, chain, per_wrapped.inner().format().unwrap())
            })
            .collect();
        assert_eq!(
            ratios,
            vec![("AAPL".to_string(), Chain::Robinhood, "1.5".to_string())]
        );
    }

    /// Each symbol's bands stay published while another symbol is planned,
    /// and a symbol the planner could not size leaves the family.
    #[test]
    fn the_family_holds_every_symbols_latest_bands() {
        let families = leaked_families();
        let publisher = EquityBandPublisher::new(families);
        let aapl = Symbol::new("tAAPL").unwrap();
        let tsla = Symbol::new("tTSLA").unwrap();

        publisher.record(
            &aapl,
            Some(symbol_bands(
                "120",
                vec![
                    band(Chain::Base, "0.6", BandVerdict::Within),
                    band(Chain::Robinhood, "0.05", BandVerdict::Below),
                ],
            )),
        );
        publisher.record(
            &tsla,
            Some(symbol_bands(
                "10",
                vec![band(Chain::Base, "0.9", BandVerdict::Above)],
            )),
        );

        let store = rendered_store(families);
        let value = |name: &str, chain: &str, symbol: &str| {
            store
                .get(&series(name, &[("chain", chain), ("symbol", symbol)]))
                .copied()
        };
        assert_eq!(value("liq_equity_chain_share", "base", "AAPL"), Some(0.6));
        assert_eq!(value("liq_equity_chain_verdict", "base", "AAPL"), Some(0.0));
        assert_eq!(
            value("liq_equity_chain_verdict", "robinhood", "AAPL"),
            Some(-1.0)
        );
        assert_eq!(value("liq_equity_chain_verdict", "base", "TSLA"), Some(1.0));
        assert_eq!(
            value("liq_equity_chain_target", "robinhood", "AAPL"),
            Some(0.5)
        );
        assert_eq!(
            value("liq_equity_chain_deviation", "robinhood", "AAPL"),
            Some(0.05)
        );
        assert_eq!(
            store
                .get(&series(
                    "liq_equity_underlying_total",
                    &[("symbol", "AAPL")]
                ))
                .copied(),
            Some(120.0)
        );

        publisher.record(&aapl, None);

        let remaining = rendered(families);
        assert_eq!(remaining.len(), 4, "{remaining:?}");
        assert_eq!(
            rendered_store(families)
                .get(&series(
                    "liq_equity_underlying_total",
                    &[("symbol", "AAPL")]
                ))
                .copied(),
            None
        );
        assert_eq!(
            rendered_store(families)
                .get(&series(
                    "liq_equity_chain_share",
                    &[("chain", "base"), ("symbol", "TSLA")]
                ))
                .copied(),
            Some(0.9)
        );
    }
}
