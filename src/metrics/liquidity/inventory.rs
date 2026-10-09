//! The `Inventory` family: equity and cash balances on the primary chain
//! vault, at the broker and in the Base wallet, and each hedged chain's vault
//! on its own.
//!
//! The inventory write guard publishes this family each time it releases the
//! write lock (see `BroadcastingWriteGuard`), so the series follow every
//! balance change. Each publish carries the generation it read the view at,
//! and the store ignores an older one, so a publisher that pauses after its
//! read cannot put older balances back.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use rain_math_float::{Float, FloatError};

use st0x_config::Ctx;
use st0x_finance::Usdc;

use super::settings::cash_reserved;
use super::{
    LiqFamilies, LiqFamily, LiqMetric, LiqSample, LiqValueError, float_value, push_sample,
    strip_prefix,
};
use crate::inventory::InventoryView;
use crate::inventory::view::{OnchainByChain, SymbolBalances, UsdcBalances};

/// What the inventory builder reads, copied from the view while its lock is
/// held so the samples can be built after the lock is released.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InventoryInput {
    symbols: Vec<SymbolBalances>,
    usdc: UsdcBalances,
    by_chain: OnchainByChain,
}

impl InventoryInput {
    pub(crate) fn from_view(view: &InventoryView) -> Self {
        Self {
            symbols: view.symbol_balances(),
            usdc: view.usdc_balances(),
            by_chain: view.onchain_by_chain(),
        }
    }
}

/// Publishes the `Inventory` family from the inventory view. One per process;
/// the inventory owns it.
pub(crate) struct InventoryPublisher {
    families: &'static LiqFamilies,
    cash_reserved: Option<Float>,
    /// Set once boot has restored the view. Before that the view holds no
    /// balances, or only the ones a recovery step seeded, and publishing it
    /// would read as zero balances.
    started: AtomicBool,
    /// Runs as each publish starts, so a test can look at the inventory lock.
    #[cfg(test)]
    before_publish: Option<Box<dyn Fn() + Send + Sync>>,
}

/// A view read, numbered in the order the reads happened.
#[derive(Debug)]
pub(crate) struct InventoryRead {
    generation: u64,
    input: InventoryInput,
}

impl InventoryPublisher {
    pub(crate) fn new(ctx: &Ctx, families: &'static LiqFamilies) -> Self {
        Self {
            families,
            cash_reserved: cash_reserved(ctx),
            started: AtomicBool::new(false),
            #[cfg(test)]
            before_publish: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn before_publish(self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            before_publish: Some(Box::new(hook)),
            ..self
        }
    }

    /// Starts publishing, with a read of the restored view. Call it while a
    /// lock on the view is held, like [`Self::read_changed`].
    pub(crate) fn start(&self, view: &InventoryView) -> InventoryRead {
        self.started.store(true, Ordering::SeqCst);
        self.read_changed(view)
    }

    pub(crate) fn started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Reads a view that was just changed. Call it while the write lock is
    /// held, so generations follow the order of the writes.
    pub(crate) fn read_changed(&self, view: &InventoryView) -> InventoryRead {
        InventoryRead {
            generation: self.families.next_generation(),
            input: InventoryInput::from_view(view),
        }
    }

    /// Builds the samples and stores them, unless a newer read was stored
    /// first. Call it after the lock is released.
    pub(crate) fn publish(&self, read: InventoryRead) {
        #[cfg(test)]
        if let Some(hook) = &self.before_publish {
            hook();
        }

        let InventoryRead { generation, input } = read;
        self.families.replace_at_generation(
            LiqFamily::Inventory,
            generation,
            inventory_samples(&input, self.cash_reserved),
            SystemTime::now(),
        );
    }
}

/// Every `liq_equity_*` and `liq_usdc_*` sample. The ported series keep the
/// exporter's formulas: their ratios are 0 when the denominator is 0, and the
/// primary-chain series exclude other chains and wallet tokens.
pub(crate) fn inventory_samples(
    input: &InventoryInput,
    cash_reserved: Option<Float>,
) -> Vec<LiqSample> {
    let mut samples = Vec::new();

    for balances in &input.symbols {
        symbol_samples(&mut samples, balances);
    }

    usdc_samples(&mut samples, &input.usdc, cash_reserved);
    let broker_gross = input.usdc.offchain_gross.map(Usdc::inner);
    chain_samples(&mut samples, &input.by_chain, broker_gross);

    samples
}

/// The per-chain series. Each vault's ratio divides by the gross broker
/// cash, so it is absent until that cash is read, and absent while the vault
/// and the broker are both 0: these series are the bot's own, with no
/// exporter value to keep.
fn chain_samples(
    samples: &mut Vec<LiqSample>,
    by_chain: &OnchainByChain,
    broker_gross: Option<Float>,
) {
    for balance in &by_chain.equities {
        push_sample(
            samples,
            LiqMetric::EquityChainAvailable,
            vec![
                ("chain", balance.chain.as_str().to_string()),
                ("symbol", strip_prefix(balance.symbol.as_str()).to_string()),
            ],
            float_value(balance.available.inner()),
        );
    }

    for balance in &by_chain.usdc {
        let labels = vec![("chain", balance.chain.as_str().to_string())];
        let available = balance.available.inner();
        let values = [
            (LiqMetric::UsdcChainAvailable, Ok(available)),
            (LiqMetric::UsdcChainInflight, Ok(balance.inflight.inner())),
        ];
        for (metric, value) in values {
            push_sample(samples, metric, labels.clone(), exact(value));
        }

        let ratio = broker_gross.and_then(|broker_gross| {
            (available + broker_gross)
                .and_then(|whole| defined_share_of(available, whole))
                .transpose()
        });
        if let Some(ratio) = ratio {
            push_sample(samples, LiqMetric::UsdcChainRatio, labels, exact(ratio));
        }
    }
}

fn symbol_samples(samples: &mut Vec<LiqSample>, balances: &SymbolBalances) {
    let labels = vec![("symbol", strip_prefix(balances.symbol.as_str()).to_string())];
    let onchain = balances.onchain_available.inner();
    let offchain = balances.offchain_available.inner();
    let onchain_inflight = balances.onchain_inflight.inner();
    let offchain_inflight = balances.offchain_inflight.inner();

    let values = [
        (LiqMetric::EquityOnchainAvailable, Ok(onchain)),
        (LiqMetric::EquityOffchainAvailable, Ok(offchain)),
        (
            LiqMetric::EquityInflightTotal,
            onchain_inflight + offchain_inflight,
        ),
        (
            LiqMetric::EquityTotal,
            sum([onchain, offchain, onchain_inflight, offchain_inflight]),
        ),
        (
            LiqMetric::EquityUnwrapped,
            Ok(balances.base_wallet_unwrapped.inner()),
        ),
        (
            LiqMetric::EquityWrapped,
            Ok(balances.base_wallet_wrapped.inner()),
        ),
        (
            LiqMetric::EquityRatio,
            (onchain + offchain).and_then(|denominator| share_of(onchain, denominator)),
        ),
    ];

    for (metric, value) in values {
        push_sample(samples, metric, labels.clone(), exact(value));
    }
}

/// Pushes the unlabelled cash samples.
fn usdc_samples(samples: &mut Vec<LiqSample>, usdc: &UsdcBalances, cash_reserved: Option<Float>) {
    let onchain = usdc.onchain_available.inner();
    let onchain_inflight = usdc.onchain_inflight.inner();
    let offchain = usdc.offchain_available.inner();
    let offchain_inflight = usdc.offchain_inflight.inner();
    let gross = usdc.offchain_gross.map(Usdc::inner);

    // The gross balance once read, the reserve-adjusted one until then.
    let alpaca_total = usdc.offchain_gross.map_or(offchain, Usdc::inner);

    let present = [
        (LiqMetric::UsdcOnchainAvailable, Ok(onchain)),
        (LiqMetric::UsdcOnchainInflight, Ok(onchain_inflight)),
        (LiqMetric::UsdcOffchainAvailable, Ok(offchain)),
        (LiqMetric::UsdcOffchainInflight, Ok(offchain_inflight)),
        (LiqMetric::UsdcAlpacaTotal, Ok(alpaca_total)),
        (
            LiqMetric::UsdcInflightTotal,
            onchain_inflight + offchain_inflight,
        ),
        (
            LiqMetric::UsdcTotal,
            sum([onchain, alpaca_total, onchain_inflight, offchain_inflight]),
        ),
        (
            LiqMetric::UsdcRatio,
            (onchain + alpaca_total).and_then(|denominator| share_of(onchain, denominator)),
        ),
    ];
    for (metric, value) in present {
        push_sample(samples, metric, vec![], exact(value));
    }

    let optional = [
        (LiqMetric::UsdcOffchainGross, gross.map(Ok)),
        (
            LiqMetric::UsdcAlpacaUsdc,
            usdc.alpaca_usdc.map(|usdc| Ok(usdc.inner())),
        ),
        (
            LiqMetric::UsdcInflightEthereumWallet,
            usdc.ethereum_wallet.map(|usdc| Ok(usdc.inner())),
        ),
        (
            LiqMetric::UsdcInflightBaseWallet,
            usdc.base_wallet.map(|usdc| Ok(usdc.inner())),
        ),
        (
            LiqMetric::UsdcRebalanceable,
            usdc.withdrawable_cash.map(|withdrawable| {
                rebalanceable(withdrawable.inner(), cash_reserved, gross, offchain)
            }),
        ),
    ];
    for (metric, value) in optional {
        if let Some(value) = value {
            push_sample(samples, metric, vec![], exact(value));
        }
    }
}

/// Withdrawable cash above the reserve, never below 0. The reserve is the
/// configured one, else the gap between gross and available broker cash once
/// the gross is read, else nothing.
fn rebalanceable(
    withdrawable: Float,
    cash_reserved: Option<Float>,
    gross: Option<Float>,
    offchain: Float,
) -> Result<Float, FloatError> {
    let reserve = match (cash_reserved, gross) {
        (Some(reserved), _) => reserved,
        (None, Some(gross)) => (gross - offchain)?,
        (None, None) => Float::zero()?,
    };

    (withdrawable - reserve)?.max(Float::zero()?)
}

fn sum<const N: usize>(values: [Float; N]) -> Result<Float, FloatError> {
    values
        .into_iter()
        .try_fold(Float::zero()?, |total, value| total + value)
}

/// `part / whole`, published as 0 when `whole` is 0, as the exporter did.
fn share_of(part: Float, whole: Float) -> Result<Float, FloatError> {
    if whole.is_zero()? {
        return Float::zero();
    }

    part / whole
}

/// `part / whole`, or `None` when `whole` is 0 and the share is undefined.
fn defined_share_of(part: Float, whole: Float) -> Result<Option<Float>, FloatError> {
    if whole.is_zero()? {
        return Ok(None);
    }

    (part / whole).map(Some)
}

fn exact(value: Result<Float, FloatError>) -> Result<f64, LiqValueError> {
    float_value(value?)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use alloy::primitives::address;
    use serde_json::{Value, json};

    use st0x_config::create_test_ctx_with_order_owner;
    use st0x_evm::Chain;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::view::{ChainEquityBalance, ChainUsdcBalance};
    use crate::metrics::liquidity::tests::{SeriesKey, parse_exposition, series};

    fn shares(value: Float) -> FractionalShares {
        FractionalShares::new(value)
    }

    fn symbol(name: &str, onchain: Float, offchain: Float) -> SymbolBalances {
        SymbolBalances {
            symbol: Symbol::new(name).unwrap(),
            onchain_available: shares(onchain),
            onchain_inflight: shares(float!(0)),
            offchain_available: shares(offchain),
            offchain_inflight: shares(float!(0)),
            base_wallet_unwrapped: shares(float!(0)),
            base_wallet_wrapped: shares(float!(0)),
        }
    }

    fn usdc(onchain: Float, offchain: Float) -> UsdcBalances {
        UsdcBalances {
            onchain_available: Usdc::new(onchain),
            onchain_inflight: Usdc::new(float!(0)),
            offchain_available: Usdc::new(offchain),
            offchain_inflight: Usdc::new(float!(0)),
            offchain_gross: None,
            withdrawable_cash: None,
            alpaca_usdc: None,
            ethereum_wallet: None,
            base_wallet: None,
        }
    }

    fn input(symbols: Vec<SymbolBalances>, usdc: UsdcBalances) -> InventoryInput {
        InventoryInput {
            symbols,
            usdc,
            by_chain: OnchainByChain::default(),
        }
    }

    pub(crate) fn render_family(
        family: LiqFamily,
        samples: Vec<LiqSample>,
    ) -> BTreeMap<SeriesKey, f64> {
        let families = LiqFamilies::default();
        families.replace(family, samples, SystemTime::UNIX_EPOCH);
        let mut body = String::new();
        families.render_into(&mut body);

        parse_exposition(&body)
            .into_iter()
            .filter(|((name, _), _)| name != "liq_collector_last_success_ts_seconds")
            .collect()
    }

    fn render(input: &InventoryInput, cash_reserved: Option<Float>) -> BTreeMap<SeriesKey, f64> {
        render_family(
            LiqFamily::Inventory,
            inventory_samples(input, cash_reserved),
        )
    }

    fn usdc_series(rendered: &BTreeMap<SeriesKey, f64>, name: &str) -> Option<f64> {
        rendered.get(&series(name, &[])).copied()
    }

    /// The goldens feed the exporter the dashboard DTO, so the same fixture
    /// reaches the builder through this adapter. The DTO has no per-chain
    /// rows; those have their own tests.
    pub(crate) fn input_from_dto(inventory: &st0x_dto::Inventory) -> InventoryInput {
        let usdc = &inventory.usdc;

        InventoryInput {
            symbols: inventory
                .per_symbol
                .iter()
                .map(|row| SymbolBalances {
                    symbol: row.symbol.clone(),
                    onchain_available: row.onchain_available,
                    onchain_inflight: row.onchain_inflight,
                    offchain_available: row.offchain_available,
                    offchain_inflight: row.offchain_inflight,
                    base_wallet_unwrapped: row.inflight_equity.base_wallet_unwrapped,
                    base_wallet_wrapped: row.inflight_equity.base_wallet_wrapped,
                })
                .collect(),
            usdc: UsdcBalances {
                onchain_available: usdc.onchain_available,
                onchain_inflight: usdc.onchain_inflight,
                offchain_available: usdc.offchain_available,
                offchain_inflight: usdc.offchain_inflight,
                offchain_gross: usdc.offchain_gross,
                withdrawable_cash: usdc.withdrawable_cash,
                alpaca_usdc: usdc.alpaca_usdc,
                ethereum_wallet: usdc.inflight_cash.ethereum_wallet,
                base_wallet: usdc.inflight_cash.base_wallet,
            },
            by_chain: OnchainByChain::default(),
        }
    }

    /// The golden fixtures: the exporter reads the WebSocket seed, which
    /// carries these four keys.
    pub(crate) struct StateFixture {
        pub(crate) settings: st0x_dto::Settings,
        pub(crate) inventory: st0x_dto::Inventory,
        pub(crate) positions: Vec<st0x_dto::Position>,
        pub(crate) equity_prices: Vec<st0x_dto::EquityPrice>,
    }

    impl StateFixture {
        pub(crate) fn assert_matches(&self, fixture_json: &str) {
            let fixture: Value = serde_json::from_str(fixture_json).unwrap();
            assert_eq!(
                json!({
                    "settings": self.settings,
                    "inventory": self.inventory,
                    "positions": self.positions,
                    "equityPrices": self.equity_prices,
                }),
                fixture
            );
        }
    }

    fn dto_settings(cash_reserved: Option<Float>) -> st0x_dto::Settings {
        st0x_dto::Settings {
            equity_target: Some(0.5),
            equity_deviation: 0.2,
            usdc_target: Some(0.4),
            usdc_deviation: Some(0.1),
            cash_reserved: cash_reserved.map(st0x_finance::Usd::new),
            execution_threshold: "$2.5".to_string(),
            assets: Vec::new(),
            wallet: None,
            log_level: "Info".to_string(),
            server_port: 8080,
            orderbook: "0x0000000000000000000000000000000000000002".to_string(),
            deployment_block: 1,
            broker: "alpaca".to_string(),
            order_polling_interval: 5,
            inventory_poll_interval: 15,
        }
    }

    fn dto_symbol(
        name: &str,
        [
            onchain,
            onchain_inflight,
            offchain,
            offchain_inflight,
            unwrapped,
            wrapped,
        ]: [Float; 6],
    ) -> st0x_dto::SymbolInventory {
        st0x_dto::SymbolInventory {
            symbol: Symbol::new(name).unwrap(),
            onchain_available: shares(onchain),
            onchain_inflight: shares(onchain_inflight),
            offchain_available: shares(offchain),
            offchain_inflight: shares(offchain_inflight),
            inflight_equity: st0x_dto::InFlightEquity {
                base_wallet_unwrapped: shares(unwrapped),
                base_wallet_wrapped: shares(wrapped),
            },
        }
    }

    fn price(name: &str, price_usd: Float, expires_at: &str) -> st0x_dto::EquityPrice {
        st0x_dto::EquityPrice {
            symbol: Symbol::new(name).unwrap(),
            status: st0x_dto::EquityPriceStatus::Available {
                price_usd,
                observed_at: "2026-10-08T00:00:00Z".parse().unwrap(),
                expires_at: expires_at.parse().unwrap(),
            },
        }
    }

    fn position(name: &str, net: Float) -> st0x_dto::Position {
        st0x_dto::Position {
            symbol: Symbol::new(name).unwrap(),
            net,
        }
    }

    /// Prefixed symbols, a symbol with nothing available, every optional
    /// cash reading present, and the reserve taken from the gross balance.
    pub(crate) fn state_fixture() -> StateFixture {
        StateFixture {
            settings: dto_settings(None),
            inventory: st0x_dto::Inventory {
                per_symbol: vec![
                    dto_symbol(
                        "RKLB",
                        [
                            float!(30),
                            float!(0),
                            float!(10),
                            float!(0),
                            float!(0),
                            float!(0),
                        ],
                    ),
                    dto_symbol(
                        "tAAPL",
                        [
                            float!(12.5),
                            float!(2),
                            float!(37.5),
                            float!(0.5),
                            float!(3),
                            float!(1.25),
                        ],
                    ),
                    dto_symbol(
                        "wtTSLA",
                        [
                            float!(0),
                            float!(4),
                            float!(0),
                            float!(1),
                            float!(0),
                            float!(0),
                        ],
                    ),
                ],
                usdc: st0x_dto::UsdcInventory {
                    symbol: "USDC".to_string(),
                    onchain_available: Usdc::new(float!(2500.5)),
                    onchain_inflight: Usdc::new(float!(100)),
                    offchain_available: Usdc::new(float!(1000.25)),
                    offchain_inflight: Usdc::new(float!(50)),
                    offchain_gross: Some(Usdc::new(float!(1500.5))),
                    withdrawable_cash: Some(Usdc::new(float!(800))),
                    alpaca_usdc: Some(Usdc::new(float!(12.5))),
                    inflight_cash: st0x_dto::InFlightCash {
                        ethereum_wallet: Some(Usdc::new(float!(3))),
                        base_wallet: None,
                    },
                },
            },
            positions: vec![
                position("AAPL", float!(10)),
                position("wtTSLA", float!(-3)),
                position("RKLB", float!(4)),
            ],
            equity_prices: vec![
                price("tAAPL", float!(2.5), "2099-01-01T00:00:00Z"),
                price("tSPYM", float!(7), "2099-01-01T00:00:00Z"),
                price("wtTSLA", float!(9), "2000-01-01T00:00:00Z"),
            ],
        }
    }

    /// A configured reserve above withdrawable cash, no gross reading, and
    /// no cash at all, so every ratio has a zero denominator.
    pub(crate) fn state_reserved_fixture() -> StateFixture {
        StateFixture {
            settings: dto_settings(Some(float!(900))),
            inventory: st0x_dto::Inventory {
                per_symbol: vec![dto_symbol(
                    "tAAPL",
                    [
                        float!(0),
                        float!(0),
                        float!(0),
                        float!(0),
                        float!(0),
                        float!(0),
                    ],
                )],
                usdc: st0x_dto::UsdcInventory {
                    symbol: "USDC".to_string(),
                    onchain_available: Usdc::new(float!(0)),
                    onchain_inflight: Usdc::new(float!(0)),
                    offchain_available: Usdc::new(float!(0)),
                    offchain_inflight: Usdc::new(float!(0)),
                    offchain_gross: None,
                    withdrawable_cash: Some(Usdc::new(float!(800))),
                    alpaca_usdc: None,
                    inflight_cash: st0x_dto::InFlightCash::empty(),
                },
            },
            positions: Vec::new(),
            equity_prices: Vec::new(),
        }
    }

    fn names_of(family: LiqFamily) -> Vec<&'static str> {
        LiqMetric::ALL
            .into_iter()
            .filter(|metric| metric.family() == Some(family))
            .map(LiqMetric::name)
            .collect()
    }

    pub(crate) fn golden_for(family: LiqFamily, golden: &str) -> BTreeMap<SeriesKey, f64> {
        let names = names_of(family);
        parse_exposition(golden)
            .into_iter()
            .filter(|((name, _), _)| names.contains(&name.as_str()))
            .collect()
    }

    fn assert_inventory_matches_golden(fixture: &StateFixture, json: &str, golden: &str) {
        fixture.assert_matches(json);

        let cash_reserved = fixture.settings.cash_reserved.map(st0x_finance::Usd::inner);
        let actual = render(&input_from_dto(&fixture.inventory), cash_reserved);

        assert_eq!(actual, golden_for(LiqFamily::Inventory, golden));
    }

    #[test]
    fn inventory_matches_the_exporter_golden() {
        assert_inventory_matches_golden(
            &state_fixture(),
            include_str!("testdata/state.json"),
            include_str!("testdata/state.prom"),
        );
    }

    #[test]
    fn inventory_with_a_reserve_and_no_cash_matches_the_exporter_golden() {
        assert_inventory_matches_golden(
            &state_reserved_fixture(),
            include_str!("testdata/state-reserved.json"),
            include_str!("testdata/state-reserved.prom"),
        );
    }

    #[test]
    fn ratios_are_zero_when_nothing_is_available() {
        let mut empty = symbol("TSLA", float!(0), float!(0));
        empty.onchain_inflight = shares(float!(4));

        let rendered = render(&input(vec![empty], usdc(float!(0), float!(0))), None);

        assert_eq!(
            rendered.get(&series("liq_equity_ratio", &[("symbol", "TSLA")])),
            Some(&0.0)
        );
        assert_eq!(
            rendered.get(&series("liq_equity_total", &[("symbol", "TSLA")])),
            Some(&4.0)
        );
        assert_eq!(usdc_series(&rendered, "liq_usdc_ratio"), Some(0.0));
    }

    #[test]
    fn a_missing_gross_reading_totals_the_available_broker_cash() {
        let mut cash = usdc(float!(300), float!(100));
        cash.onchain_inflight = Usdc::new(float!(20));
        cash.offchain_inflight = Usdc::new(float!(5));

        let rendered = render(&input(Vec::new(), cash), None);

        assert_eq!(
            rendered,
            BTreeMap::from([
                (series("liq_usdc_onchain_available", &[]), 300.0),
                (series("liq_usdc_onchain_inflight", &[]), 20.0),
                (series("liq_usdc_offchain_available", &[]), 100.0),
                (series("liq_usdc_offchain_inflight", &[]), 5.0),
                (series("liq_usdc_alpaca_total", &[]), 100.0),
                (series("liq_usdc_inflight_total", &[]), 25.0),
                (series("liq_usdc_total", &[]), 425.0),
                (series("liq_usdc_ratio", &[]), 0.75),
            ])
        );
    }

    #[test]
    fn the_gross_reading_replaces_available_broker_cash_in_totals_and_ratio() {
        let mut cash = usdc(float!(300), float!(100));
        cash.offchain_gross = Some(Usdc::new(float!(700)));

        let rendered = render(&input(Vec::new(), cash), None);

        assert_eq!(
            usdc_series(&rendered, "liq_usdc_offchain_gross"),
            Some(700.0)
        );
        assert_eq!(usdc_series(&rendered, "liq_usdc_alpaca_total"), Some(700.0));
        assert_eq!(usdc_series(&rendered, "liq_usdc_total"), Some(1000.0));
        assert_eq!(usdc_series(&rendered, "liq_usdc_ratio"), Some(0.3));
    }

    #[test]
    fn rebalanceable_cash_follows_the_reserve_rules() {
        let cases = [
            ("no withdrawable reading", None, None, None, None),
            (
                "no reserve at all",
                Some(float!(800)),
                None,
                None,
                Some(800.0),
            ),
            (
                "reserve from the gross gap",
                Some(float!(800)),
                None,
                Some(float!(1500)),
                Some(300.0),
            ),
            (
                "configured reserve beats the gross gap",
                Some(float!(800)),
                Some(float!(100)),
                Some(float!(1500)),
                Some(700.0),
            ),
            (
                "reserve above withdrawable",
                Some(float!(800)),
                Some(float!(900)),
                None,
                Some(0.0),
            ),
        ];

        for (case, withdrawable, cash_reserved, gross, expected) in cases {
            let mut cash = usdc(float!(0), float!(1000));
            cash.withdrawable_cash = withdrawable.map(Usdc::new);
            cash.offchain_gross = gross.map(Usdc::new);

            let rendered = render(&input(Vec::new(), cash), cash_reserved);

            assert_eq!(
                usdc_series(&rendered, "liq_usdc_rebalanceable"),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn optional_cash_readings_are_absent_until_read() {
        let mut cash = usdc(float!(1), float!(1));
        cash.alpaca_usdc = Some(Usdc::new(float!(12.5)));
        cash.base_wallet = Some(Usdc::new(float!(0)));

        let rendered = render(&input(Vec::new(), cash), None);

        assert_eq!(usdc_series(&rendered, "liq_usdc_alpaca_usdc"), Some(12.5));
        assert_eq!(
            usdc_series(&rendered, "liq_usdc_inflight_base_wallet"),
            Some(0.0)
        );
        assert_eq!(
            usdc_series(&rendered, "liq_usdc_inflight_ethereum_wallet"),
            None
        );
        assert_eq!(usdc_series(&rendered, "liq_usdc_offchain_gross"), None);
        assert_eq!(usdc_series(&rendered, "liq_usdc_rebalanceable"), None);
    }

    #[test]
    fn per_chain_series_list_each_vault_with_its_own_ratio() {
        let mut cash = usdc(float!(2000), float!(1000));
        cash.offchain_gross = Some(Usdc::new(float!(1500)));
        let input = InventoryInput {
            symbols: Vec::new(),
            usdc: cash,
            by_chain: OnchainByChain {
                equities: vec![
                    ChainEquityBalance {
                        symbol: Symbol::new("tAAPL").unwrap(),
                        chain: Chain::Base,
                        available: shares(float!(50)),
                    },
                    ChainEquityBalance {
                        symbol: Symbol::new("tAAPL").unwrap(),
                        chain: Chain::Robinhood,
                        available: shares(float!(9)),
                    },
                ],
                usdc: vec![
                    ChainUsdcBalance {
                        chain: Chain::Base,
                        available: Usdc::new(float!(2000)),
                        inflight: Usdc::new(float!(250)),
                    },
                    ChainUsdcBalance {
                        chain: Chain::Robinhood,
                        available: Usdc::new(float!(500)),
                        inflight: Usdc::new(float!(0)),
                    },
                ],
            },
        };

        let per_chain: BTreeMap<SeriesKey, f64> = render(&input, None)
            .into_iter()
            .filter(|((name, _), _)| name.contains("_chain_"))
            .collect();

        assert_eq!(
            per_chain,
            BTreeMap::from([
                (
                    series(
                        "liq_equity_chain_available",
                        &[("chain", "base"), ("symbol", "AAPL")]
                    ),
                    50.0
                ),
                (
                    series(
                        "liq_equity_chain_available",
                        &[("chain", "robinhood"), ("symbol", "AAPL")]
                    ),
                    9.0
                ),
                (
                    series("liq_usdc_chain_available", &[("chain", "base")]),
                    2000.0
                ),
                (
                    series("liq_usdc_chain_inflight", &[("chain", "base")]),
                    250.0
                ),
                (
                    series("liq_usdc_chain_ratio", &[("chain", "base")]),
                    4.0 / 7.0
                ),
                (
                    series("liq_usdc_chain_available", &[("chain", "robinhood")]),
                    500.0
                ),
                (
                    series("liq_usdc_chain_inflight", &[("chain", "robinhood")]),
                    0.0
                ),
                (
                    series("liq_usdc_chain_ratio", &[("chain", "robinhood")]),
                    0.25
                ),
            ])
        );
    }

    fn one_chain_vault(available: Float, cash: UsdcBalances) -> InventoryInput {
        InventoryInput {
            symbols: Vec::new(),
            usdc: cash,
            by_chain: OnchainByChain {
                equities: Vec::new(),
                usdc: vec![ChainUsdcBalance {
                    chain: Chain::HyperEvm,
                    available: Usdc::new(available),
                    inflight: Usdc::new(float!(0)),
                }],
            },
        }
    }

    fn hyperevm_ratio(input: &InventoryInput) -> Option<f64> {
        render(input, None)
            .get(&series("liq_usdc_chain_ratio", &[("chain", "hyperevm")]))
            .copied()
    }

    /// The share is undefined, so the bot-only ratio is absent rather than
    /// the exporter's 0; the vault's own series stay.
    #[test]
    fn a_chain_vault_and_broker_both_empty_leave_the_ratio_absent() {
        let mut cash = usdc(float!(0), float!(0));
        cash.offchain_gross = Some(Usdc::new(float!(0)));
        let input = one_chain_vault(float!(0), cash);

        assert_eq!(hyperevm_ratio(&input), None);
        assert_eq!(
            render(&input, None)
                .get(&series(
                    "liq_usdc_chain_available",
                    &[("chain", "hyperevm")]
                ))
                .copied(),
            Some(0.0)
        );
    }

    /// Before the gross broker cash is read, the available broker cash can
    /// be 0 while the broker holds cash, which would publish a vault holding
    /// part of the cash as 1. The ratio waits for the gross reading.
    #[test]
    fn an_unread_gross_broker_cash_leaves_the_ratio_absent() {
        let unread = one_chain_vault(float!(400), usdc(float!(0), float!(0)));
        assert_eq!(hyperevm_ratio(&unread), None);

        let mut cash = usdc(float!(0), float!(0));
        cash.offchain_gross = Some(Usdc::new(float!(600)));
        let read = one_chain_vault(float!(400), cash);
        assert_eq!(hyperevm_ratio(&read), Some(0.4));
    }

    /// The view read is the same data the dashboard DTO shows, so the
    /// builder sees what the exporter saw.
    #[test]
    fn view_input_matches_the_dashboard_inventory() {
        let view = InventoryView::default()
            .with_equity(
                Symbol::new("tAAPL").unwrap(),
                shares(float!(12.5)),
                shares(float!(37.5)),
            )
            .with_usdc(Usdc::new(float!(2500.5)), Usdc::new(float!(1000.25)))
            .with_offchain_gross_usd_cents(150_050)
            .with_withdrawable_cash_cents(80_000);

        let from_view = InventoryInput::from_view(&view);
        let from_dashboard = input_from_dto(&view.to_dto());

        assert_eq!(from_view.symbols, from_dashboard.symbols);
        assert_eq!(from_view.usdc, from_dashboard.usdc);
        assert_eq!(
            from_view.by_chain.usdc,
            vec![ChainUsdcBalance {
                chain: Chain::Base,
                available: Usdc::new(float!(2500.5)),
                inflight: Usdc::new(float!(0)),
            }]
        );
    }

    pub(crate) fn leaked_families() -> &'static LiqFamilies {
        Box::leak(Box::default())
    }

    pub(crate) fn publisher(families: &'static LiqFamilies) -> InventoryPublisher {
        InventoryPublisher::new(
            &create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )),
            families,
        )
    }

    pub(crate) fn rendered_store(families: &LiqFamilies) -> BTreeMap<SeriesKey, f64> {
        let mut body = String::new();
        families.render_into(&mut body);
        parse_exposition(&body)
    }

    fn view_with(symbol: &str, onchain: Float) -> InventoryView {
        InventoryView::default().with_equity(
            Symbol::new(symbol).unwrap(),
            shares(onchain),
            shares(float!(0)),
        )
    }

    #[test]
    fn a_read_published_after_a_newer_one_is_ignored() {
        let families = leaked_families();
        let publisher = publisher(families);

        let older = publisher.read_changed(&view_with("AAPL", float!(1)));
        let newer = publisher.read_changed(&view_with("AAPL", float!(2)));
        publisher.publish(newer);
        publisher.publish(older);

        assert_eq!(
            rendered_store(families).get(&series(
                "liq_equity_onchain_available",
                &[("symbol", "AAPL")]
            )),
            Some(&2.0)
        );
    }

    /// A second publisher on the same store, as a new bot session in the
    /// same process would create, is not stuck behind the first one's
    /// generations.
    #[test]
    fn a_later_publisher_on_the_same_store_is_not_ignored() {
        let families = leaked_families();
        let first = publisher(families);
        for onchain in [float!(1), float!(2), float!(3)] {
            first.publish(first.read_changed(&view_with("AAPL", onchain)));
        }

        let second = publisher(families);
        second.publish(second.read_changed(&view_with("AAPL", float!(9))));

        assert_eq!(
            rendered_store(families).get(&series(
                "liq_equity_onchain_available",
                &[("symbol", "AAPL")]
            )),
            Some(&9.0)
        );
    }

    #[test]
    fn a_symbol_that_leaves_the_view_leaves_the_series() {
        let families = leaked_families();
        let publisher = publisher(families);

        publisher.publish(publisher.read_changed(&view_with("AAPL", float!(1))));
        publisher.publish(publisher.read_changed(&view_with("TSLA", float!(2))));

        let symbols: Vec<String> = rendered_store(families)
            .into_keys()
            .filter(|(name, _)| name == "liq_equity_onchain_available")
            .map(|(_, labels)| labels[0].1.clone())
            .collect();
        assert_eq!(symbols, ["TSLA"]);
    }
}
