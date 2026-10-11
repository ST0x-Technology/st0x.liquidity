//! The `Pnl(window)` families: every `liq_pnl_*` series of one PnL window,
//! built from the same `PnlResponse` `GET /pnl` returns for that window.
//!
//! Each window is its own family, replaced as one unit when its report
//! succeeds. A window whose report fails keeps its last samples (see
//! `pnl_refresh`), and its `liq_collector_last_success_ts_seconds` stops
//! advancing.
//!
//! The values follow the exporter sidecar, with one exception: a day
//! component that does not parse. The exporter counted it as 0; here the day
//! series it adds up are left out, and so is every running total after it,
//! with an error log. A summary decimal that does not parse leaves its series
//! absent, as in the exporter.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use tracing::{error, warn};

use st0x_float_macro::float;

use super::{
    LiqMetric, LiqSample, LiqValueError, count_value, float_value, push_sample,
    signed_integer_value, strip_prefix, timestamp_value,
};
use crate::dashboard::pnl::{
    PnlResponse, PnlSummary, PnlSymbolSummary, PnlWindow, PnlWindowSymbol,
};

/// Day buckets published per window: the last ones, after the running
/// totals are computed over every bucket.
pub(crate) const PNL_CHART_DAYS: usize = 90;

/// One PnL window, as the `window` label and the collector name carry it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum PnlWindowKey {
    OneDay,
    OneWeek,
    OneMonth,
    YearToDate,
    OneYear,
    All,
}

impl PnlWindowKey {
    /// The order a refresh cycle runs the windows in: the board's default
    /// window first. Each cycle starts one entry later and wraps around, so
    /// the windows a slow cycle skips change from cycle to cycle.
    pub(crate) const REFRESH_ORDER: [Self; 6] = [
        Self::OneWeek,
        Self::OneDay,
        Self::All,
        Self::OneMonth,
        Self::YearToDate,
        Self::OneYear,
    ];

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::OneDay => "1d",
            Self::OneWeek => "1w",
            Self::OneMonth => "1m",
            Self::YearToDate => "ytd",
            Self::OneYear => "1y",
            Self::All => "all",
        }
    }

    pub(crate) const fn collector(self) -> &'static str {
        match self {
            Self::OneDay => "pnl_1d",
            Self::OneWeek => "pnl_1w",
            Self::OneMonth => "pnl_1m",
            Self::YearToDate => "pnl_ytd",
            Self::OneYear => "pnl_1y",
            Self::All => "pnl_all",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PnlSamplesError {
    #[error("day bucket arithmetic failed")]
    DayArithmetic(#[from] FloatError),
}

/// Every `liq_pnl_*` sample of one window. Fails only when the day-bucket
/// sums cannot be computed; the caller then keeps the window's last samples.
pub(crate) fn pnl_samples(
    report: &PnlResponse,
    window: PnlWindowKey,
) -> Result<Vec<LiqSample>, PnlSamplesError> {
    let mut samples = Vec::new();

    summary_samples(&mut samples, &report.summary, window);
    cost_samples(&mut samples, report, window);
    capital_samples(&mut samples, report, window);
    for row in rows_with_distinct_symbols(&report.symbols, window) {
        symbol_samples(&mut samples, row, window);
    }
    sample_stats_samples(&mut samples, report, window);
    day_samples(&mut samples, &report.windows, window)?;

    Ok(samples)
}

/// The per-symbol rows whose `symbol` label no other row shares. Two rows
/// that strip to the same label (`tAAPL` and `wtAAPL`) would publish the same
/// series twice, and the store keeps only one of them, so both are left out
/// with an error log. The exporter wrote both lines.
fn rows_with_distinct_symbols(
    rows: &[PnlSymbolSummary],
    window: PnlWindowKey,
) -> impl Iterator<Item = &PnlSymbolSummary> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for row in rows {
        *counts.entry(strip_prefix(row.symbol.as_str())).or_insert(0) += 1;
    }
    for (symbol, count) in &counts {
        if *count > 1 {
            error!(
                ?window,
                symbol,
                rows = count,
                "Left out the liq_pnl_symbol series of a symbol label that several PnL rows share"
            );
        }
    }

    rows.iter()
        .filter(move |row| counts.get(strip_prefix(row.symbol.as_str())) == Some(&1))
}

/// The eleven summary streams, in the exporter's `PNL_STREAMS` order.
#[derive(Clone, Copy)]
enum PnlStream {
    CounterTrade,
    OnchainNetting,
    DirectionalInventoryBaseline,
    DirectionalImbalanceExcess,
    DirectionalExposure,
    Total,
    GrossRealized,
    TrackedCosts,
    TrackedRevenue,
    NetRealized,
    Realized,
}

impl PnlStream {
    const ALL: [Self; 11] = [
        Self::CounterTrade,
        Self::OnchainNetting,
        Self::DirectionalInventoryBaseline,
        Self::DirectionalImbalanceExcess,
        Self::DirectionalExposure,
        Self::Total,
        Self::GrossRealized,
        Self::TrackedCosts,
        Self::TrackedRevenue,
        Self::NetRealized,
        Self::Realized,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::CounterTrade => "counter_trade",
            Self::OnchainNetting => "onchain_netting",
            Self::DirectionalInventoryBaseline => "directional_inventory_baseline",
            Self::DirectionalImbalanceExcess => "directional_imbalance_excess",
            Self::DirectionalExposure => "directional_exposure",
            Self::Total => "total",
            Self::GrossRealized => "gross_realized",
            Self::TrackedCosts => "tracked_costs",
            Self::TrackedRevenue => "tracked_revenue",
            Self::NetRealized => "net_realized",
            Self::Realized => "realized",
        }
    }

    fn of_summary(self, summary: &PnlSummary) -> &str {
        match self {
            Self::CounterTrade => &summary.counter_trade_pnl_usd,
            Self::OnchainNetting => &summary.onchain_netting_pnl_usd,
            Self::DirectionalInventoryBaseline => &summary.directional_inventory_baseline_pnl_usd,
            Self::DirectionalImbalanceExcess => &summary.directional_imbalance_excess_pnl_usd,
            Self::DirectionalExposure => &summary.directional_exposure_pnl_usd,
            Self::Total => &summary.total_pnl_usd,
            Self::GrossRealized => &summary.gross_realized_pnl_usd,
            Self::TrackedCosts => &summary.tracked_costs_usd,
            Self::TrackedRevenue => &summary.tracked_revenue_usd,
            Self::NetRealized => &summary.net_realized_pnl_usd,
            Self::Realized => &summary.realized_pnl_usd,
        }
    }

    fn of_symbol(self, row: &PnlSymbolSummary) -> &str {
        match self {
            Self::CounterTrade => &row.counter_trade_pnl_usd,
            Self::OnchainNetting => &row.onchain_netting_pnl_usd,
            Self::DirectionalInventoryBaseline => &row.directional_inventory_baseline_pnl_usd,
            Self::DirectionalImbalanceExcess => &row.directional_imbalance_excess_pnl_usd,
            Self::DirectionalExposure => &row.directional_exposure_pnl_usd,
            Self::Total => &row.total_pnl_usd,
            Self::GrossRealized => &row.gross_realized_pnl_usd,
            Self::TrackedCosts => &row.tracked_costs_usd,
            Self::TrackedRevenue => &row.tracked_revenue_usd,
            Self::NetRealized => &row.net_realized_pnl_usd,
            Self::Realized => &row.realized_pnl_usd,
        }
    }
}

/// The four component streams the day charts stack. The two aggregate
/// streams would count each component twice.
#[derive(Clone, Copy)]
enum ChartStream {
    CounterTrade,
    OnchainNetting,
    DirectionalInventoryBaseline,
    DirectionalImbalanceExcess,
}

impl ChartStream {
    const ALL: [Self; 4] = [
        Self::CounterTrade,
        Self::OnchainNetting,
        Self::DirectionalInventoryBaseline,
        Self::DirectionalImbalanceExcess,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::CounterTrade => "counter_trade",
            Self::OnchainNetting => "onchain_netting",
            Self::DirectionalInventoryBaseline => "directional_inventory_baseline",
            Self::DirectionalImbalanceExcess => "directional_imbalance_excess",
        }
    }

    fn of_day_row(self, row: &PnlWindowSymbol) -> &str {
        match self {
            Self::CounterTrade => &row.counter_trade_pnl_usd,
            Self::OnchainNetting => &row.onchain_netting_pnl_usd,
            Self::DirectionalInventoryBaseline => &row.directional_inventory_baseline_pnl_usd,
            Self::DirectionalImbalanceExcess => &row.directional_imbalance_excess_pnl_usd,
        }
    }
}

fn window_labels(window: PnlWindowKey) -> Vec<(&'static str, String)> {
    vec![("window", window.label().to_string())]
}

fn labelled(window: PnlWindowKey, labels: &[(&'static str, &str)]) -> Vec<(&'static str, String)> {
    let mut all = window_labels(window);
    all.extend(
        labels
            .iter()
            .map(|(key, value)| (*key, (*value).to_string())),
    );
    all
}

fn decimal(value: &str) -> Result<Float, LiqValueError> {
    Float::parse(value.to_string()).map_err(|source| LiqValueError::Decimal {
        value: value.to_string(),
        source,
    })
}

fn decimal_value(value: &str) -> Result<f64, LiqValueError> {
    decimal(value).and_then(float_value)
}

fn rfc3339_value(value: &str) -> Result<f64, LiqValueError> {
    let at = DateTime::parse_from_rfc3339(value).map_err(|source| LiqValueError::Timestamp {
        value: value.to_string(),
        source,
    })?;

    timestamp_value(at.with_timezone(&Utc))
}

fn summary_samples(samples: &mut Vec<LiqSample>, summary: &PnlSummary, window: PnlWindowKey) {
    let usd_streams = PnlStream::ALL
        .map(|stream| (stream.label(), stream.of_summary(summary)))
        .into_iter()
        .chain([
            ("inventory_drift", summary.inventory_drift_usd.as_str()),
            ("onchain_notional", summary.onchain_notional_usd.as_str()),
            ("offchain_notional", summary.offchain_notional_usd.as_str()),
        ]);
    for (stream, value) in usd_streams {
        push_sample(
            samples,
            LiqMetric::PnlSummaryUsd,
            labelled(window, &[("stream", stream)]),
            decimal_value(value),
        );
    }

    for (kind, value) in [
        ("matched", &summary.matched_shares),
        ("inventory_drift", &summary.inventory_drift_shares),
        ("open_long", &summary.open_long_shares),
        ("open_short", &summary.open_short_shares),
        ("unmatched_offchain", &summary.unmatched_offchain_shares),
    ] {
        push_sample(
            samples,
            LiqMetric::PnlSummaryShares,
            labelled(window, &[("kind", kind)]),
            decimal_value(value),
        );
    }

    for (kind, count) in [
        ("onchain_fill", summary.onchain_fill_count),
        ("offchain_fill", summary.offchain_fill_count),
        ("matched_lot", summary.matched_lot_count),
        ("open_lot", summary.open_lot_count),
        (
            "unmatched_offchain_fill",
            summary.unmatched_offchain_fill_count,
        ),
    ] {
        push_sample(
            samples,
            LiqMetric::PnlSummaryCount,
            labelled(window, &[("kind", kind)]),
            count_value(count),
        );
    }
}

fn cost_samples(samples: &mut Vec<LiqSample>, report: &PnlResponse, window: PnlWindowKey) {
    let costs = &report.costs;

    for (category, value) in [
        ("broker_fees", &costs.broker_fees_usd),
        ("tokenization_fees", &costs.tokenization_fees_usd),
        ("cctp_fees", &costs.cctp_fees_usd),
        ("bot_gas", &costs.bot_gas_usd),
        ("margin_interest", &costs.margin_interest_usd),
        ("wallet_transfer_fees", &costs.wallet_transfer_fees_usd),
        ("regulatory_fees", &costs.regulatory_fees_usd),
        ("unclassified", &costs.unclassified_costs_usd),
    ] {
        push_sample(
            samples,
            LiqMetric::PnlCostUsd,
            labelled(window, &[("category", category)]),
            decimal_value(value),
        );
    }

    push_sample(
        samples,
        LiqMetric::PnlRevenueUsd,
        labelled(window, &[("category", "dividend_income")]),
        decimal_value(&costs.dividend_revenue_usd),
    );
    push_sample(
        samples,
        LiqMetric::PnlCostEntries,
        window_labels(window),
        count_value(costs.cost_entry_count),
    );
    push_sample(
        samples,
        LiqMetric::PnlCostMissingObservations,
        window_labels(window),
        count_value(costs.missing_cost_observation_count),
    );

    for entry in &costs.coverage {
        push_sample(
            samples,
            LiqMetric::PnlCostCoverage,
            labelled(
                window,
                &[("source", entry.source), ("status", entry.status)],
            ),
            Ok(1.0),
        );
    }
}

fn capital_samples(samples: &mut Vec<LiqSample>, report: &PnlResponse, window: PnlWindowKey) {
    let capital = &report.capital;

    if let Some(average) = &capital.average_deployed_capital_usd {
        push_sample(
            samples,
            LiqMetric::PnlCapitalAvgDeployedUsd,
            window_labels(window),
            decimal_value(average),
        );
    }
    if let Some(annualized) = &capital.annualized_return_pct {
        push_sample(
            samples,
            LiqMetric::PnlCapitalAnnualizedReturnPct,
            window_labels(window),
            decimal_value(annualized),
        );
    }
    if let Some(coverage_days) = capital.coverage_days {
        push_sample(
            samples,
            LiqMetric::PnlCapitalCoverageDays,
            window_labels(window),
            signed_integer_value(coverage_days),
        );
    }
    push_sample(
        samples,
        LiqMetric::PnlCapitalSampleDays,
        window_labels(window),
        count_value(capital.sample_days),
    );
}

fn symbol_samples(samples: &mut Vec<LiqSample>, row: &PnlSymbolSummary, window: PnlWindowKey) {
    let symbol = strip_prefix(row.symbol.as_str());

    let usd_columns = PnlStream::ALL
        .map(|stream| (stream.label(), stream.of_symbol(row)))
        .into_iter()
        .chain([("inventory_drift", row.inventory_drift_usd.as_str())]);
    for (col, value) in usd_columns {
        push_sample(
            samples,
            LiqMetric::PnlSymbolUsd,
            labelled(window, &[("symbol", symbol), ("col", col)]),
            decimal_value(value),
        );
    }

    for (kind, value) in [
        ("matched", &row.matched_shares),
        ("inventory_drift", &row.inventory_drift_shares),
        ("open_long", &row.open_long_shares),
        ("open_short", &row.open_short_shares),
    ] {
        push_sample(
            samples,
            LiqMetric::PnlSymbolShares,
            labelled(window, &[("symbol", symbol), ("kind", kind)]),
            decimal_value(value),
        );
    }

    push_sample(
        samples,
        LiqMetric::PnlSymbolLots,
        labelled(window, &[("symbol", symbol)]),
        count_value(row.matched_lot_count),
    );

    // A matched lot has two legs, and the volume counts each leg.
    push_sample(
        samples,
        LiqMetric::PnlSymbolVolumeShares,
        labelled(window, &[("symbol", symbol)]),
        decimal(&row.matched_shares)
            .and_then(|matched| Ok((matched * float!(2))?))
            .and_then(float_value),
    );
}

fn sample_stats_samples(samples: &mut Vec<LiqSample>, report: &PnlResponse, window: PnlWindowKey) {
    let stats = &report.sample_stats;

    push_sample(
        samples,
        LiqMetric::PnlSampleTotalFills,
        window_labels(window),
        count_value(stats.total_fill_count),
    );
    push_sample(
        samples,
        LiqMetric::PnlSampleSymbols,
        window_labels(window),
        count_value(stats.symbol_count),
    );
    if let Some(first_at) = &stats.first_at {
        push_sample(
            samples,
            LiqMetric::PnlSampleFirstTsSeconds,
            window_labels(window),
            rfc3339_value(first_at),
        );
    }
    if let Some(last_at) = &stats.last_at {
        push_sample(
            samples,
            LiqMetric::PnlSampleLastTsSeconds,
            window_labels(window),
            rfc3339_value(last_at),
        );
    }
    push_sample(
        samples,
        LiqMetric::PnlWarnings,
        window_labels(window),
        count_value(report.warnings.len()),
    );
}

/// One day bucket's sums and the running totals through it. A value is
/// `None` once a component it adds up did not parse: that sum is unknown, and
/// so is every running total after it.
struct DayRow<'a> {
    day: &'a str,
    symbols: BTreeMap<String, Option<Float>>,
    streams: [Option<Float>; 4],
    cumulative_symbols: BTreeMap<String, Option<Float>>,
    cumulative_streams: [Option<Float>; 4],
}

/// Adds `value` to a sum that is still known. An unknown side stays unknown.
fn add_known(sum: Option<Float>, value: Option<Float>) -> Result<Option<Float>, FloatError> {
    match (sum, value) {
        (Some(sum), Some(value)) => (sum + value).map(Some),
        _ => Ok(None),
    }
}

/// A day component, or `None` with an error log when it does not parse. The
/// exporter counted such a component as 0 (`dec(...) or 0`); a 0 would
/// publish a wrong day and wrong running totals as if they were right, so the
/// series it feeds are left out instead.
fn day_component(value: &str, window: PnlWindowKey, day: &str, symbol: &str) -> Option<Float> {
    decimal(value)
        .inspect_err(|error| {
            error!(
                ?window,
                day,
                symbol,
                %error,
                "Left out the liq_pnl_day series of an unparseable PnL day component"
            );
        })
        .ok()
}

/// The day series. The running totals run over every bucket, sorted by day,
/// and only then keep the last [`PNL_CHART_DAYS`], so the last bucket's
/// running total still equals the window's total.
fn day_samples(
    samples: &mut Vec<LiqSample>,
    buckets: &[PnlWindow],
    window: PnlWindowKey,
) -> Result<(), PnlSamplesError> {
    let mut sorted: Vec<&PnlWindow> = buckets.iter().collect();
    sorted.sort_by(|left, right| left.window_id.cmp(&right.window_id));

    let mut cumulative_symbols: BTreeMap<String, Option<Float>> = BTreeMap::new();
    let mut cumulative_streams = [Some(float!(0)); 4];
    let mut rows = Vec::with_capacity(sorted.len());

    for bucket in sorted {
        if bucket.window_id.is_empty() {
            warn!(?window, "Skipped a PnL day bucket without a day");
            continue;
        }
        let day = bucket.window_id.as_str();

        let mut symbols: BTreeMap<String, Option<Float>> = BTreeMap::new();
        let mut streams = [Some(float!(0)); 4];
        for row in &bucket.symbols {
            let symbol = strip_prefix(row.symbol.as_str()).to_string();
            let total = day_component(&row.total_pnl_usd, window, day, &symbol);
            for (sum, stream) in streams.iter_mut().zip(ChartStream::ALL) {
                let value = day_component(stream.of_day_row(row), window, day, &symbol);
                *sum = add_known(*sum, value)?;
            }
            let day_total = symbols.entry(symbol).or_insert(Some(float!(0)));
            *day_total = add_known(*day_total, total)?;
        }

        for (symbol, value) in &symbols {
            let running = cumulative_symbols
                .entry(symbol.clone())
                .or_insert(Some(float!(0)));
            *running = add_known(*running, *value)?;
        }
        for (running, value) in cumulative_streams.iter_mut().zip(streams) {
            *running = add_known(*running, value)?;
        }

        rows.push(DayRow {
            day,
            symbols,
            streams,
            cumulative_symbols: cumulative_symbols.clone(),
            cumulative_streams,
        });
    }

    let first_kept = rows.len().saturating_sub(PNL_CHART_DAYS);
    for row in &rows[first_kept..] {
        push_day_row(samples, row, window);
    }

    Ok(())
}

fn push_day_row(samples: &mut Vec<LiqSample>, row: &DayRow<'_>, window: PnlWindowKey) {
    let known = |values: &BTreeMap<String, Option<Float>>| {
        values
            .iter()
            .filter_map(|(symbol, value)| value.map(|value| (symbol.clone(), value)))
            .collect::<Vec<_>>()
    };
    for (symbol, value) in known(&row.symbols) {
        push_sample(
            samples,
            LiqMetric::PnlDayUsd,
            labelled(window, &[("day", row.day), ("symbol", &symbol)]),
            float_value(value),
        );
    }
    for (symbol, value) in known(&row.cumulative_symbols) {
        push_sample(
            samples,
            LiqMetric::PnlDayCumUsd,
            labelled(window, &[("day", row.day), ("symbol", &symbol)]),
            float_value(value),
        );
    }
    for (stream, value) in ChartStream::ALL.into_iter().zip(row.streams) {
        let Some(value) = value else { continue };
        push_sample(
            samples,
            LiqMetric::PnlDayStreamUsd,
            labelled(window, &[("day", row.day), ("stream", stream.label())]),
            float_value(value),
        );
    }
    for (stream, value) in ChartStream::ALL.into_iter().zip(row.cumulative_streams) {
        let Some(value) = value else { continue };
        push_sample(
            samples,
            LiqMetric::PnlDayCumStreamUsd,
            labelled(window, &[("day", row.day), ("stream", stream.label())]),
            float_value(value),
        );
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use serde_json::Value;

    use st0x_finance::Symbol;

    use super::*;
    use crate::dashboard::pnl::{
        PnlAvailableRange, PnlCapitalSummary, PnlCostCoverage, PnlCostSummary, PnlSampleStats,
    };
    use crate::metrics::liquidity::inventory::tests::render_family;
    use crate::metrics::liquidity::tests::{SeriesKey, parse_exposition, series};
    use crate::metrics::liquidity::{LiqFamilies, LiqFamily};

    fn summary(values: &[&str; 20], counts: [usize; 5]) -> PnlSummary {
        let [
            counter_trade,
            onchain_netting,
            baseline,
            excess,
            exposure,
            total,
            gross,
            costs,
            revenue,
            net,
            realized,
            matched,
            onchain_notional,
            offchain_notional,
            drift_shares,
            drift_usd,
            open_long,
            open_short,
            unmatched_shares,
            unmatched_notional,
        ] = values.map(str::to_string);
        let [
            onchain_fills,
            offchain_fills,
            matched_lots,
            open_lots,
            unmatched_fills,
        ] = counts;

        PnlSummary {
            counter_trade_pnl_usd: counter_trade,
            onchain_netting_pnl_usd: onchain_netting,
            directional_inventory_baseline_pnl_usd: baseline,
            directional_imbalance_excess_pnl_usd: excess,
            directional_exposure_pnl_usd: exposure,
            total_pnl_usd: total,
            gross_realized_pnl_usd: gross,
            tracked_costs_usd: costs,
            tracked_revenue_usd: revenue,
            net_realized_pnl_usd: net,
            realized_pnl_usd: realized,
            matched_shares: matched,
            onchain_notional_usd: onchain_notional,
            offchain_notional_usd: offchain_notional,
            inventory_drift_shares: drift_shares,
            inventory_drift_usd: drift_usd,
            open_long_shares: open_long,
            open_short_shares: open_short,
            unmatched_offchain_shares: unmatched_shares,
            unmatched_offchain_notional_usd: unmatched_notional,
            onchain_fill_count: onchain_fills,
            offchain_fill_count: offchain_fills,
            matched_lot_count: matched_lots,
            open_lot_count: open_lots,
            unmatched_offchain_fill_count: unmatched_fills,
        }
    }

    fn symbol_row(symbol: &str, values: &[&str; 17], matched_lots: usize) -> PnlSymbolSummary {
        let [
            counter_trade,
            onchain_netting,
            baseline,
            excess,
            exposure,
            total,
            gross,
            costs,
            revenue,
            net,
            realized,
            matched,
            drift_shares,
            drift_usd,
            open_long,
            open_short,
            unmatched_shares,
        ] = values.map(str::to_string);

        PnlSymbolSummary {
            symbol: Symbol::new(symbol).unwrap(),
            counter_trade_pnl_usd: counter_trade,
            onchain_netting_pnl_usd: onchain_netting,
            directional_inventory_baseline_pnl_usd: baseline,
            directional_imbalance_excess_pnl_usd: excess,
            directional_exposure_pnl_usd: exposure,
            total_pnl_usd: total,
            gross_realized_pnl_usd: gross,
            tracked_costs_usd: costs,
            tracked_revenue_usd: revenue,
            net_realized_pnl_usd: net,
            realized_pnl_usd: realized,
            matched_shares: matched,
            inventory_drift_shares: drift_shares,
            inventory_drift_usd: drift_usd,
            open_long_shares: open_long,
            open_short_shares: open_short,
            unmatched_offchain_shares: unmatched_shares,
            matched_lot_count: matched_lots,
            onchain_fill_count: matched_lots,
            offchain_fill_count: matched_lots,
            unmatched_offchain_fill_count: 0,
        }
    }

    /// One symbol row of a day bucket: the four chart streams, then the
    /// directional exposure and the total.
    pub(crate) fn day_symbol(symbol: &str, values: [&str; 6]) -> PnlWindowSymbol {
        let [
            counter_trade,
            onchain_netting,
            baseline,
            excess,
            exposure,
            total,
        ] = values.map(str::to_string);

        PnlWindowSymbol {
            symbol: Symbol::new(symbol).unwrap(),
            counter_trade_pnl_usd: counter_trade,
            onchain_netting_pnl_usd: onchain_netting,
            directional_inventory_baseline_pnl_usd: baseline,
            directional_imbalance_excess_pnl_usd: excess,
            directional_exposure_pnl_usd: exposure,
            total_pnl_usd: total,
        }
    }

    pub(crate) fn day_bucket(day: &str, symbols: Vec<PnlWindowSymbol>) -> PnlWindow {
        PnlWindow {
            window_id: day.to_string(),
            start_at: format!("{day}T05:00:00.000Z"),
            end_at: format!("{day}T04:59:59.999Z"),
            label: day.to_string(),
            is_weekend: false,
            market_session: "regular".to_string(),
            counter_trading_session: "regular".to_string(),
            granularity: "day",
            symbols,
        }
    }

    fn costs(coverage: Vec<PnlCostCoverage>) -> PnlCostSummary {
        PnlCostSummary {
            total_tracked_costs_usd: "14.25".to_string(),
            total_tracked_revenue_usd: "1.5".to_string(),
            counter_trade_costs_usd: "9".to_string(),
            onchain_netting_costs_usd: "0".to_string(),
            directional_exposure_costs_usd: "0".to_string(),
            generic_costs_usd: "5.25".to_string(),
            dividend_revenue_usd: "1.5".to_string(),
            offchain_execution_fees_usd: "2".to_string(),
            tokenization_fees_usd: "3.5".to_string(),
            cctp_fees_usd: "0.75".to_string(),
            conversion_slippage_usd: "0".to_string(),
            oracle_write_cost_usd: "0".to_string(),
            broker_fees_usd: "2".to_string(),
            regulatory_fees_usd: "0.01".to_string(),
            margin_interest_usd: "4".to_string(),
            bot_gas_usd: "1.99".to_string(),
            wallet_transfer_fees_usd: "0".to_string(),
            unclassified_costs_usd: "2".to_string(),
            cost_entry_count: 7,
            missing_cost_observation_count: 2,
            coverage,
        }
    }

    fn coverage(source: &'static str, status: &'static str) -> PnlCostCoverage {
        PnlCostCoverage {
            source,
            accounting_bucket: "generic",
            effect: "cost",
            status,
            amount_usd: "0".to_string(),
            note: "fixture",
        }
    }

    pub(crate) fn report(
        available_range: (Option<&str>, Option<&str>),
        windows: Vec<PnlWindow>,
    ) -> PnlResponse {
        PnlResponse {
            attribution_method: "backend_position_fill_replay_fifo",
            as_of_rowid: 42,
            warnings: vec!["first".to_string(), "second".to_string()],
            available_range: PnlAvailableRange {
                first_at: available_range.0.map(|day| format!("{day}T14:30:00Z")),
                last_at: available_range.1.map(|day| format!("{day}T15:00:00Z")),
                first_date: available_range.0.map(str::to_string),
                last_date: available_range.1.map(str::to_string),
            },
            sample_stats: PnlSampleStats {
                first_at: Some("2026-02-20T14:30:00.25Z".to_string()),
                last_at: Some("2026-03-03T15:00:00+00:00".to_string()),
                symbol_count: 2,
                onchain_fill_count: 5,
                offchain_fill_count: 4,
                total_fill_count: 9,
                symbols: Vec::new(),
            },
            summary: summary(
                &[
                    "12.5", "-3.25", "4", "-1.75", "2.25", "11.5", "20", "14.25", "n/a", "7.25",
                    "11.5", "300", "6000", "5950.5", "-2", "-30.5", "10", "0", "1.5", "22.5",
                ],
                [5, 4, 3, 2, 1],
            ),
            costs: costs(vec![
                coverage("tokenization_fees", "included"),
                coverage("wallet_transfer_fees", "not_ingested"),
            ]),
            capital: PnlCapitalSummary {
                average_deployed_capital_usd: Some("12500.5".to_string()),
                annualized_return_pct: None,
                coverage_days: Some(3),
                sample_days: 3,
                first_snapshot_day: Some("2026-03-01".to_string()),
                last_snapshot_day: Some("2026-03-03".to_string()),
                excluded_days: Vec::new(),
            },
            symbols: vec![
                symbol_row(
                    "tAAPL",
                    &[
                        "10", "-1", "3", "-1.5", "1.5", "10.5", "15", "6", "1", "10", "10.5",
                        "200", "-1", "-15.25", "8", "0", "1.5",
                    ],
                    2,
                ),
                symbol_row(
                    "RKLB",
                    &[
                        "2.5", "-2.25", "1", "-0.25", "0.75", "1", "5", "8.25", "0.5", "-2.75",
                        "1", "", "-1", "-15.25", "2", "0", "0",
                    ],
                    1,
                ),
            ],
            symbol_universe: vec![Symbol::new("RKLB").unwrap(), Symbol::new("tAAPL").unwrap()],
            entries: Vec::new(),
            cost_entries: Vec::new(),
            total: 3,
            has_more: true,
            windows,
        }
    }

    /// Three days, out of order: two symbols that both strip to `AAPL` on
    /// the first day, a day without symbol rows, and an unparseable
    /// component on the last day.
    pub(crate) fn pnl_fixture() -> PnlResponse {
        report(
            (Some("2026-02-20"), Some("2026-03-03")),
            vec![
                day_bucket(
                    "2026-03-03",
                    vec![day_symbol(
                        "tAAPL",
                        ["1.5", "bad", "0.5", "-0.25", "0.25", "1.75"],
                    )],
                ),
                day_bucket(
                    "2026-03-01",
                    vec![
                        day_symbol("tAAPL", ["2", "-1", "0.5", "0", "0.5", "1.5"]),
                        day_symbol("wtAAPL", ["1", "0", "0", "0", "0", "1"]),
                        day_symbol("RKLB", ["0.25", "0.5", "-1", "0.75", "-0.25", "0.5"]),
                    ],
                ),
                day_bucket("2026-03-02", Vec::new()),
            ],
        )
    }

    /// 95 consecutive day buckets, so the last 90 are published. `RKLB`
    /// has fills only on the first day, which the cut drops, so it appears
    /// in the running totals only.
    pub(crate) fn pnl_days_fixture() -> PnlResponse {
        let first = chrono::NaiveDate::from_ymd_opt(2025, 12, 1).unwrap();
        let buckets = (0..95_u64)
            .map(|offset| {
                let day = first
                    .checked_add_days(chrono::Days::new(offset))
                    .unwrap()
                    .to_string();
                let total = format!("{offset}.5");
                let mut symbols = vec![day_symbol(
                    "tTSLA",
                    [&total, "0.25", "-0.5", "0.125", "-0.375", &total],
                )];
                if offset == 0 {
                    symbols.push(day_symbol("RKLB", ["3", "0", "0", "0", "0", "3"]));
                }
                day_bucket(&day, symbols)
            })
            .collect();

        report((Some("2025-12-01"), Some("2026-03-05")), buckets)
    }

    fn assert_matches_fixture(report: &PnlResponse, fixture_json: &str) {
        let fixture: Value = serde_json::from_str(fixture_json).unwrap();
        assert_eq!(serde_json::to_value(report).unwrap(), fixture);
    }

    fn pnl_names() -> Vec<&'static str> {
        LiqMetric::ALL
            .into_iter()
            .filter(|metric| metric.per_pnl_window())
            .map(LiqMetric::name)
            .collect()
    }

    fn golden(golden: &str) -> BTreeMap<SeriesKey, f64> {
        let names = pnl_names();
        parse_exposition(golden)
            .into_iter()
            .filter(|((name, _), _)| names.contains(&name.as_str()))
            .collect()
    }

    fn render_windows(report: &PnlResponse, windows: &[PnlWindowKey]) -> BTreeMap<SeriesKey, f64> {
        let families = LiqFamilies::default();
        for window in windows {
            families.replace(
                LiqFamily::Pnl(*window),
                pnl_samples(report, *window).unwrap(),
                std::time::SystemTime::UNIX_EPOCH,
            );
        }
        let mut body = String::new();
        families.render_into(&mut body);

        parse_exposition(&body)
            .into_iter()
            .filter(|((name, _), _)| name != "liq_collector_last_success_ts_seconds")
            .collect()
    }

    /// The exporter queried every window and got the same report back, so
    /// each window's series equal the builder's for that window, except the
    /// two the fixture's unparseable component feeds: the exporter counted it
    /// as 0, the bot leaves them out.
    #[test]
    fn every_window_matches_the_exporter_golden() {
        let report = pnl_fixture();
        assert_matches_fixture(&report, include_str!("testdata/pnl.json"));

        let mut expected = golden(include_str!("testdata/pnl.prom"));
        for window in PnlWindowKey::REFRESH_ORDER {
            let unparsed = |name: &str| {
                series(
                    name,
                    &[
                        ("window", window.label()),
                        ("day", "2026-03-03"),
                        ("stream", "onchain_netting"),
                    ],
                )
            };
            assert_eq!(
                expected.remove(&unparsed("liq_pnl_day_stream_usd")),
                Some(0.0)
            );
            assert_eq!(
                expected.remove(&unparsed("liq_pnl_day_cum_stream_usd")),
                Some(-0.5)
            );
        }

        assert_eq!(
            render_windows(&report, &PnlWindowKey::REFRESH_ORDER),
            expected
        );
    }

    #[test]
    fn the_last_ninety_day_buckets_match_the_exporter_golden() {
        let report = pnl_days_fixture();
        assert_matches_fixture(&report, include_str!("testdata/pnl-days.json"));

        assert_eq!(
            render_windows(&report, &[PnlWindowKey::All]),
            golden(include_str!("testdata/pnl-days.prom"))
        );
    }

    fn rendered(report: &PnlResponse) -> BTreeMap<SeriesKey, f64> {
        render_family(
            LiqFamily::Pnl(PnlWindowKey::OneWeek),
            pnl_samples(report, PnlWindowKey::OneWeek).unwrap(),
        )
    }

    fn day_series(name: &str, day: &str, key: &str, value: &str) -> SeriesKey {
        series(name, &[("window", "1w"), ("day", day), (key, value)])
    }

    #[test]
    fn unparseable_summary_decimals_leave_their_series_absent() {
        let rendered = rendered(&pnl_fixture());

        assert_eq!(
            rendered.get(&series(
                "liq_pnl_summary_usd",
                &[("window", "1w"), ("stream", "tracked_revenue")]
            )),
            None
        );
        assert_eq!(
            rendered.get(&series(
                "liq_pnl_summary_usd",
                &[("window", "1w"), ("stream", "total")]
            )),
            Some(&11.5)
        );
        assert_eq!(
            rendered.get(&series(
                "liq_pnl_symbol_volume_shares",
                &[("window", "1w"), ("symbol", "RKLB")]
            )),
            None
        );
        assert_eq!(
            rendered.get(&series(
                "liq_pnl_symbol_volume_shares",
                &[("window", "1w"), ("symbol", "AAPL")]
            )),
            Some(&400.0)
        );
    }

    #[test]
    fn missing_capital_figures_are_absent_and_the_sample_days_stay() {
        let mut report = pnl_fixture();
        report.capital = PnlCapitalSummary::default();

        let rendered = rendered(&report);
        let capital: Vec<_> = rendered
            .iter()
            .filter(|((name, _), _)| name.starts_with("liq_pnl_capital_"))
            .collect();

        assert_eq!(
            capital,
            [(
                &series("liq_pnl_capital_sample_days", &[("window", "1w")]),
                &0.0
            )]
        );
    }

    #[test]
    fn sample_timestamps_keep_whole_microseconds() {
        let rendered = rendered(&pnl_fixture());

        assert_eq!(
            rendered.get(&series(
                "liq_pnl_sample_first_ts_seconds",
                &[("window", "1w")]
            )),
            Some(&1_771_597_800.25)
        );
        assert_eq!(
            rendered.get(&series(
                "liq_pnl_sample_last_ts_seconds",
                &[("window", "1w")]
            )),
            Some(&1_772_550_000.0)
        );
    }

    #[test]
    fn day_buckets_sum_stripped_symbols_and_leave_out_what_a_bad_component_feeds() {
        let rendered = rendered(&pnl_fixture());

        let day = |name: &str, day: &str, key: &str, value: &str| {
            rendered.get(&day_series(name, day, key, value)).copied()
        };
        assert_eq!(
            day("liq_pnl_day_usd", "2026-03-01", "symbol", "AAPL"),
            Some(2.5)
        );
        assert_eq!(day("liq_pnl_day_usd", "2026-03-02", "symbol", "AAPL"), None);
        assert_eq!(
            day("liq_pnl_day_cum_usd", "2026-03-02", "symbol", "RKLB"),
            Some(0.5)
        );
        assert_eq!(
            day("liq_pnl_day_cum_usd", "2026-03-03", "symbol", "AAPL"),
            Some(4.25)
        );
        assert_eq!(
            day(
                "liq_pnl_day_stream_usd",
                "2026-03-02",
                "stream",
                "counter_trade"
            ),
            Some(0.0)
        );
        assert_eq!(
            day(
                "liq_pnl_day_stream_usd",
                "2026-03-03",
                "stream",
                "onchain_netting"
            ),
            None
        );
        assert_eq!(
            day(
                "liq_pnl_day_cum_stream_usd",
                "2026-03-03",
                "stream",
                "onchain_netting"
            ),
            None
        );
        // The bad component feeds only its own stream: the symbol's total and
        // the other streams of that day stay.
        assert_eq!(
            day(
                "liq_pnl_day_stream_usd",
                "2026-03-03",
                "stream",
                "counter_trade"
            ),
            Some(1.5)
        );
        assert_eq!(
            day(
                "liq_pnl_day_cum_stream_usd",
                "2026-03-02",
                "stream",
                "onchain_netting"
            ),
            Some(-0.5)
        );
    }

    /// `tAAPL` and `wtAAPL` both publish as `AAPL`, so neither row's series
    /// is published rather than one silently replacing the other.
    #[test]
    fn symbol_rows_that_share_a_label_leave_their_series_out() {
        let mut report = pnl_fixture();
        let template = report.symbols[0].clone();
        report.symbols = vec![
            PnlSymbolSummary {
                symbol: Symbol::new("tAAPL").unwrap(),
                ..template.clone()
            },
            PnlSymbolSummary {
                symbol: Symbol::new("wtAAPL").unwrap(),
                ..template.clone()
            },
            PnlSymbolSummary {
                symbol: Symbol::new("RKLB").unwrap(),
                ..template
            },
        ];

        let symbols: std::collections::BTreeSet<String> = rendered(&report)
            .into_keys()
            .filter(|(name, _)| name.starts_with("liq_pnl_symbol_"))
            .filter_map(|(_, labels)| {
                labels
                    .into_iter()
                    .find(|(key, _)| key == "symbol")
                    .map(|(_, symbol)| symbol)
            })
            .collect();

        assert_eq!(
            symbols,
            std::collections::BTreeSet::from(["RKLB".to_string()])
        );
    }

    /// A running total with an unknown day in it is unknown: the symbol's
    /// later running totals are left out, while its later day values stay.
    #[test]
    fn a_bad_day_total_leaves_out_the_later_running_totals_of_that_symbol() {
        let report = report(
            (Some("2026-03-01"), Some("2026-03-03")),
            vec![
                day_bucket(
                    "2026-03-01",
                    vec![day_symbol("tAAPL", ["1", "0", "0", "0", "0", "oops"])],
                ),
                day_bucket(
                    "2026-03-02",
                    vec![
                        day_symbol("tAAPL", ["2", "0", "0", "0", "0", "2"]),
                        day_symbol("RKLB", ["1", "0", "0", "0", "0", "1"]),
                    ],
                ),
            ],
        );
        let rendered = rendered(&report);
        let day = |name: &str, day: &str, symbol: &str| {
            rendered
                .get(&day_series(name, day, "symbol", symbol))
                .copied()
        };

        assert_eq!(day("liq_pnl_day_usd", "2026-03-01", "AAPL"), None);
        assert_eq!(day("liq_pnl_day_cum_usd", "2026-03-01", "AAPL"), None);
        assert_eq!(day("liq_pnl_day_usd", "2026-03-02", "AAPL"), Some(2.0));
        assert_eq!(day("liq_pnl_day_cum_usd", "2026-03-02", "AAPL"), None);
        assert_eq!(day("liq_pnl_day_cum_usd", "2026-03-02", "RKLB"), Some(1.0));
        assert_eq!(
            rendered
                .get(&day_series(
                    "liq_pnl_day_cum_stream_usd",
                    "2026-03-02",
                    "stream",
                    "counter_trade"
                ))
                .copied(),
            Some(4.0)
        );
    }

    #[test]
    fn the_running_totals_include_the_buckets_the_cut_drops() {
        let rendered = render_family(
            LiqFamily::Pnl(PnlWindowKey::All),
            pnl_samples(&pnl_days_fixture(), PnlWindowKey::All).unwrap(),
        );
        let days: std::collections::BTreeSet<&str> = rendered
            .keys()
            .filter(|(name, _)| name == "liq_pnl_day_stream_usd")
            .filter_map(|(_, labels)| {
                labels
                    .iter()
                    .find(|(key, _)| key == "day")
                    .map(|(_, day)| day.as_str())
            })
            .collect();

        assert_eq!(days.len(), PNL_CHART_DAYS);
        assert_eq!(days.first(), Some(&"2025-12-06"));
        assert_eq!(days.last(), Some(&"2026-03-05"));
        // 0.5 + 1.5 + ... + 94.5 over all 95 buckets.
        assert_eq!(
            rendered.get(&series(
                "liq_pnl_day_cum_usd",
                &[("window", "all"), ("day", "2026-03-05"), ("symbol", "TSLA")]
            )),
            Some(&4512.5)
        );
        assert_eq!(
            rendered.get(&series(
                "liq_pnl_day_cum_usd",
                &[("window", "all"), ("day", "2025-12-06"), ("symbol", "RKLB")]
            )),
            Some(&3.0)
        );
    }

    #[test]
    fn a_window_family_drops_samples_labelled_with_another_window() {
        let families = LiqFamilies::default();
        let mut samples = pnl_samples(&pnl_fixture(), PnlWindowKey::OneDay).unwrap();
        samples.extend(pnl_samples(&pnl_fixture(), PnlWindowKey::OneWeek).unwrap());
        families.replace(
            LiqFamily::Pnl(PnlWindowKey::OneDay),
            samples,
            std::time::SystemTime::UNIX_EPOCH,
        );

        let mut body = String::new();
        families.render_into(&mut body);
        assert!(body.contains("window=\"1d\""), "{body}");
        assert!(!body.contains("window=\"1w\""), "{body}");
    }
}
