//! The `EquityBands` family: where each chain's vault sits against its own
//! rebalancing band, as the allocation planner sees it.
//!
//! The trigger plans one symbol at a time, so the publisher keeps the latest
//! bands of every symbol and replaces the whole family with them each time
//! one symbol changes. A symbol the planner could not size has no series.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
use std::time::SystemTime;

use st0x_execution::Symbol;

use super::{LiqFamilies, LiqFamily, LiqMetric, LiqSample, float_value, push_sample, strip_prefix};
use crate::rebalancing::trigger::allocation::{BandVerdict, ChainBand};

/// Publishes the `EquityBands` family. One per process; the rebalancing
/// trigger owns it.
pub(crate) struct EquityBandPublisher {
    families: &'static LiqFamilies,
    bands: Mutex<BTreeMap<Symbol, Vec<ChainBand>>>,
}

impl EquityBandPublisher {
    pub(crate) fn new(families: &'static LiqFamilies) -> Self {
        Self {
            families,
            bands: Mutex::new(BTreeMap::new()),
        }
    }

    /// Records `symbol`'s latest bands, or forgets them when the planner
    /// could not size the symbol, and publishes every symbol's. The trigger
    /// plans symbols on parallel workers, so the lock is held through the
    /// publish and publishes never interleave. The bands are read before the
    /// lock, so a check whose read was slower can still publish older bands
    /// over newer ones; the symbol's next check corrects them.
    pub(crate) fn record(&self, symbol: &Symbol, bands: Option<Vec<ChainBand>>) {
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

/// `liq_equity_chain_share` and `liq_equity_chain_verdict` for every chain
/// band held.
fn band_samples(held: &BTreeMap<Symbol, Vec<ChainBand>>) -> Vec<LiqSample> {
    let mut samples = Vec::new();
    for (symbol, bands) in held {
        for band in bands {
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

    use st0x_evm::Chain;

    use super::*;
    use crate::metrics::liquidity::inventory::tests::{leaked_families, rendered_store};
    use crate::metrics::liquidity::tests::series;

    fn band(chain: Chain, share: &str, verdict: BandVerdict) -> ChainBand {
        ChainBand {
            chain,
            share: Float::parse(share.to_string()).unwrap(),
            verdict,
        }
    }

    fn rendered(families: &LiqFamilies) -> BTreeMap<String, f64> {
        rendered_store(families)
            .into_iter()
            .filter(|((name, _), _)| name.starts_with("liq_equity_chain_"))
            .map(|((name, labels), value)| (format!("{name}{labels:?}"), value))
            .collect()
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
            Some(vec![
                band(Chain::Base, "0.6", BandVerdict::Within),
                band(Chain::Robinhood, "0.05", BandVerdict::Below),
            ]),
        );
        publisher.record(
            &tsla,
            Some(vec![band(Chain::Base, "0.9", BandVerdict::Above)]),
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

        publisher.record(&aapl, None);

        let remaining = rendered(families);
        assert_eq!(remaining.len(), 2, "{remaining:?}");
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
