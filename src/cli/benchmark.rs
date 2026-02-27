//! Alpaca tokenization benchmarking command.
//!
//! Runs N round trips (buy -> mint -> redeem -> sell) for a given
//! symbol and produces a TSV report with timing data and a CLI
//! summary with min/max/avg/median statistics.

use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use csv::WriterBuilder;
use serde::Serialize;
use std::fmt::Display;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tracing::info;

use st0x_evm::Wallet;
use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Direction, Executor,
    FractionalShares, MarketOrder, OrderState, Positive, Symbol, TimeInForce,
};

use crate::config::{BrokerCtx, Ctx};
use crate::threshold::Usd;
use crate::tokenization::{AlpacaTokenizationService, TokenizationRequestStatus, Tokenizer};
use crate::tokenized_equity_mint::IssuerRequestId;

/// Phase of a tokenization round trip being measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoundTripPhase {
    Mint,
    Redeem,
}

impl Display for RoundTripPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mint => write!(f, "mint"),
            Self::Redeem => write!(f, "redeem"),
        }
    }
}

/// Timed observation of one phase (mint or redeem) of a round trip.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Measurement {
    pub(crate) trip: usize,
    #[serde(serialize_with = "serialize_display")]
    pub(crate) phase: RoundTripPhase,
    #[serde(serialize_with = "serialize_utc")]
    pub(crate) started_at: DateTime<Utc>,
    #[serde(serialize_with = "serialize_utc")]
    pub(crate) completed_at: DateTime<Utc>,
    #[serde(rename = "duration_secs", serialize_with = "serialize_duration")]
    pub(crate) duration: Duration,
    pub(crate) fees: Option<Usd>,
    #[serde(serialize_with = "serialize_display")]
    pub(crate) status: TokenizationRequestStatus,
}

fn serialize_display<T: Display, S: serde::Serializer>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

fn serialize_utc<S: serde::Serializer>(
    dt: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

fn serialize_duration<S: serde::Serializer>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("{:.1}", duration.as_secs_f64()))
}

/// Collected measurements from a series of round trips.
pub(crate) struct BenchmarkSummary {
    pub(crate) symbol: Symbol,
    pub(crate) round_trips: usize,
    pub(crate) measurements: Vec<Measurement>,
}

fn tsv_writer<W: Write>(writer: W) -> csv::Writer<W> {
    WriterBuilder::new().delimiter(b'\t').from_writer(writer)
}

/// Duration statistics (min, max, avg, median).
struct DurationStats {
    min: Duration,
    max: Duration,
    avg: Duration,
    median: Duration,
}

impl DurationStats {
    fn compute(durations: &mut [Duration]) -> Option<Self> {
        if durations.is_empty() {
            return None;
        }

        durations.sort();

        let min = *durations.first()?;
        let max = *durations.last()?;

        let total: Duration = durations.iter().sum();
        let count = u32::try_from(durations.len()).unwrap_or(u32::MAX);
        let avg = total / count;

        let median = if durations.len() % 2 == 0 {
            let midpoint = durations.len() / 2;
            (durations[midpoint - 1] + durations[midpoint]) / 2
        } else {
            durations[durations.len() / 2]
        };

        Some(Self {
            min,
            max,
            avg,
            median,
        })
    }
}

impl BenchmarkSummary {
    /// Write the full TSV report (header + all measurements).
    #[cfg(test)]
    fn write_tsv<W: Write>(&self, writer: W) -> csv::Result<()> {
        let mut tsv = tsv_writer(writer);

        self.measurements
            .iter()
            .try_for_each(|measurement| tsv.serialize(measurement))?;

        Ok(tsv.flush()?)
    }

    fn durations_for_phase(&self, phase: RoundTripPhase) -> Vec<Duration> {
        self.measurements
            .iter()
            .filter(|measurement| measurement.phase == phase)
            .map(|measurement| measurement.duration)
            .collect()
    }

    fn full_trip_durations(&self) -> Vec<Duration> {
        (1..=self.round_trips)
            .filter_map(|trip| {
                let mint = self
                    .measurements
                    .iter()
                    .find(|measurement| {
                        measurement.trip == trip && measurement.phase == RoundTripPhase::Mint
                    })?
                    .duration;

                let redeem = self
                    .measurements
                    .iter()
                    .find(|measurement| {
                        measurement.trip == trip && measurement.phase == RoundTripPhase::Redeem
                    })?
                    .duration;

                Some(mint + redeem)
            })
            .collect()
    }

    /// Write the CLI summary table to the given writer.
    pub(crate) fn write_summary<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writeln!(
            writer,
            "Benchmark complete: {} round trips for {}",
            self.round_trips, self.symbol,
        )?;
        writeln!(writer)?;

        writeln!(
            writer,
            "{:<11}| {:<8}| {:<8}| {:<8}| Median",
            "Phase", "Min", "Max", "Avg",
        )?;
        writeln!(writer, "-----------+---------+---------+---------+--------",)?;

        [
            ("Mint", self.durations_for_phase(RoundTripPhase::Mint)),
            ("Redeem", self.durations_for_phase(RoundTripPhase::Redeem)),
            ("Full trip", self.full_trip_durations()),
        ]
        .into_iter()
        .filter_map(|(label, mut durations)| {
            DurationStats::compute(&mut durations).map(|stats| (label, stats))
        })
        .try_for_each(|(label, stats)| write_stats_row(writer, label, &stats))
    }
}

fn format_duration(duration: &Duration) -> String {
    format!("{:.1}s", duration.as_secs_f64())
}

fn write_stats_row<W: Write>(
    writer: &mut W,
    label: &str,
    stats: &DurationStats,
) -> std::io::Result<()> {
    writeln!(
        writer,
        "{:<11}| {:<8}| {:<8}| {:<8}| {}",
        label,
        format_duration(&stats.min),
        format_duration(&stats.max),
        format_duration(&stats.avg),
        format_duration(&stats.median),
    )
}

/// Default order polling interval.
const ORDER_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum order polling attempts before giving up.
const ORDER_POLL_MAX_ATTEMPTS: usize = 150;

/// Poll an executor order until it reaches a terminal state.
async fn poll_order_until_filled(
    executor: &AlpacaBrokerApi,
    order_id: &<AlpacaBrokerApi as Executor>::OrderId,
) -> anyhow::Result<()> {
    for _attempt in 0..ORDER_POLL_MAX_ATTEMPTS {
        let state = executor.get_order_status(order_id).await?;

        match state {
            OrderState::Filled { .. } => return Ok(()),
            OrderState::Failed { error_reason, .. } => {
                anyhow::bail!(
                    "Order failed: {}",
                    error_reason.unwrap_or_else(|| "unknown".to_string()),
                );
            }
            OrderState::Pending | OrderState::Submitted { .. } => {
                tokio::time::sleep(ORDER_POLL_INTERVAL).await;
            }
        }
    }

    anyhow::bail!("Order polling timed out after {ORDER_POLL_MAX_ATTEMPTS} attempts")
}

/// Measure a single mint operation.
async fn measure_mint(
    tokenization_service: &AlpacaTokenizationService<impl Wallet>,
    symbol: &Symbol,
    quantity: FractionalShares,
    wallet: Address,
    trip: usize,
) -> anyhow::Result<Measurement> {
    let started_at = Utc::now();
    let start_instant = tokio::time::Instant::now();

    let issuer_request_id = IssuerRequestId::new(uuid::Uuid::new_v4().to_string());

    let mint_request = tokenization_service
        .request_mint(symbol.clone(), quantity, wallet, issuer_request_id)
        .await?;

    let completed = tokenization_service
        .poll_mint_until_complete(&mint_request.id)
        .await?;

    let duration = start_instant.elapsed();

    Ok(Measurement {
        trip,
        phase: RoundTripPhase::Mint,
        started_at,
        completed_at: Utc::now(),
        duration,
        fees: completed.fees,
        status: completed.status,
    })
}

/// Measure a single redemption operation.
async fn measure_redeem(
    tokenization_service: &AlpacaTokenizationService<impl Wallet>,
    token: Address,
    quantity: FractionalShares,
    trip: usize,
) -> anyhow::Result<Measurement> {
    let started_at = Utc::now();
    let start_instant = tokio::time::Instant::now();

    let amount = quantity.to_u256_18_decimals()?;

    let tx_hash = Tokenizer::send_for_redemption(tokenization_service, token, amount).await?;
    info!(tx_hash = %tx_hash, "Redemption transfer sent");

    let redemption_request = Tokenizer::poll_for_redemption(tokenization_service, &tx_hash).await?;

    let completed =
        Tokenizer::poll_redemption_until_complete(tokenization_service, &redemption_request.id)
            .await?;

    let duration = start_instant.elapsed();

    Ok(Measurement {
        trip,
        phase: RoundTripPhase::Redeem,
        started_at,
        completed_at: Utc::now(),
        duration,
        fees: completed.fees,
        status: completed.status,
    })
}

/// Place a market order and poll until filled.
async fn place_and_fill<W: Write>(
    executor: &AlpacaBrokerApi,
    symbol: &Symbol,
    quantity: Positive<FractionalShares>,
    direction: Direction,
    stdout: &mut W,
) -> anyhow::Result<()> {
    use Direction::{Buy, Sell};

    let verb = match direction {
        Buy => "Buying",
        Sell => "Selling",
    };

    writeln!(stdout, "   {verb} {quantity} {symbol}...")?;

    let order = MarketOrder {
        symbol: symbol.clone(),
        shares: quantity,
        direction,
    };

    let placement = executor.place_market_order(order).await?;
    info!(order_id = %placement.order_id, %direction, "order placed");
    poll_order_until_filled(executor, &placement.order_id).await?;

    let past = match direction {
        Buy => "Buy",
        Sell => "Sell",
    };

    writeln!(stdout, "   {past} filled")?;
    Ok(())
}

/// Run the Alpaca tokenization benchmark.
pub(super) async fn alpaca_benchmark_command<W: Write>(
    stdout: &mut W,
    symbol: Symbol,
    round_trips: usize,
    quantity: Positive<FractionalShares>,
    output_path: Option<PathBuf>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-benchmark requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;

    let token_addresses = ctx.equities.get(&symbol).ok_or_else(|| {
        anyhow::anyhow!("No token addresses configured for {symbol} in equities config")
    })?;

    let token = token_addresses.unwrapped;
    let wallet = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "Starting Alpaca tokenization benchmark")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Round trips: {round_trips}")?;
    writeln!(stdout, "   Quantity per trip: {quantity}")?;
    writeln!(stdout, "   Token: {token}")?;
    writeln!(stdout, "   Wallet: {wallet}")?;
    writeln!(stdout)?;

    let broker_mode = if alpaca_auth.is_sandbox() {
        AlpacaBrokerApiMode::Sandbox
    } else {
        AlpacaBrokerApiMode::Production
    };

    let broker_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_auth.api_key.clone(),
        api_secret: alpaca_auth.api_secret.clone(),
        account_id: rebalancing_ctx.alpaca_broker_auth.account_id,
        mode: Some(broker_mode),
        asset_cache_ttl: std::time::Duration::from_secs(3600),
        time_in_force: TimeInForce::default(),
    };

    let executor = AlpacaBrokerApi::try_from_ctx(broker_auth).await?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        rebalancing_ctx.base_wallet().clone(),
        rebalancing_ctx.redemption_wallet,
    );

    let timestamp = Utc::now().format("%Y%m%dT%H%M%S");
    let report_path =
        output_path.unwrap_or_else(|| PathBuf::from(format!("benchmark-{symbol}-{timestamp}.tsv")));

    let report_file = File::create(&report_path)?;
    let mut tsv = tsv_writer(report_file);

    let mut measurements = Vec::new();

    for trip in 1..=round_trips {
        writeln!(stdout, "--- Round trip {trip}/{round_trips} ---")?;

        place_and_fill(&executor, &symbol, quantity, Direction::Buy, stdout).await?;

        writeln!(stdout, "   Minting...")?;
        let mint = measure_mint(
            &tokenization_service,
            &symbol,
            quantity.inner(),
            wallet,
            trip,
        )
        .await?;

        writeln!(
            stdout,
            "   Mint {}: {:.1}s",
            mint.status,
            mint.duration.as_secs_f64(),
        )?;
        tsv.serialize(&mint)?;
        tsv.flush()?;
        measurements.push(mint);

        writeln!(stdout, "   Redeeming...")?;
        let redeem = measure_redeem(&tokenization_service, token, quantity.inner(), trip).await?;

        writeln!(
            stdout,
            "   Redeem {}: {:.1}s",
            redeem.status,
            redeem.duration.as_secs_f64(),
        )?;
        tsv.serialize(&redeem)?;
        tsv.flush()?;
        measurements.push(redeem);

        place_and_fill(&executor, &symbol, quantity, Direction::Sell, stdout).await?;

        writeln!(stdout)?;
    }

    let summary = BenchmarkSummary {
        symbol: symbol.clone(),
        round_trips,
        measurements,
    };

    writeln!(stdout)?;
    summary.write_summary(stdout)?;
    writeln!(stdout)?;
    writeln!(stdout, "Report saved to {}", report_path.display())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    use super::*;

    fn make_measurement(
        trip: usize,
        phase: RoundTripPhase,
        duration_secs: f64,
        fees: Option<Usd>,
    ) -> Measurement {
        let started_at = Utc.with_ymd_and_hms(2026, 2, 27, 18, 30, 0).unwrap();
        let duration = Duration::from_secs_f64(duration_secs);
        let completed_at = started_at + chrono::Duration::from_std(duration).unwrap();

        Measurement {
            trip,
            phase,
            started_at,
            completed_at,
            duration,
            fees,
            status: TokenizationRequestStatus::Completed,
        }
    }

    #[test]
    fn tsv_record_with_fees() {
        let measurement = make_measurement(1, RoundTripPhase::Mint, 75.2, Some(Usd(dec!(1.50))));
        let mut output = Vec::new();
        let mut writer = tsv_writer(&mut output);
        writer.serialize(&measurement).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let tsv = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = tsv.lines().collect();
        assert_eq!(lines.len(), 2, "header + 1 data row");
        assert_eq!(
            lines[0],
            "trip\tphase\tstarted_at\tcompleted_at\tduration_secs\tfees\tstatus",
        );
        assert_eq!(
            lines[1],
            "1\tmint\t2026-02-27T18:30:00Z\t2026-02-27T18:31:15Z\t75.2\t1.50\tcompleted",
        );
    }

    #[test]
    fn tsv_record_without_fees() {
        let measurement = make_measurement(1, RoundTripPhase::Redeem, 89.1, None);
        let mut output = Vec::new();
        let mut writer = tsv_writer(&mut output);
        writer.serialize(&measurement).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let tsv = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = tsv.lines().collect();
        assert_eq!(
            lines[1],
            "1\tredeem\t2026-02-27T18:30:00Z\t2026-02-27T18:31:29Z\t89.1\t\tcompleted",
        );
    }

    #[test]
    fn tsv_full_report_includes_header_and_measurements() {
        let measurements = vec![
            make_measurement(1, RoundTripPhase::Mint, 75.2, None),
            make_measurement(1, RoundTripPhase::Redeem, 89.1, None),
            make_measurement(2, RoundTripPhase::Mint, 80.0, None),
            make_measurement(2, RoundTripPhase::Redeem, 91.0, None),
        ];

        let summary = BenchmarkSummary {
            symbol: Symbol::new("AAPL").unwrap(),
            round_trips: 2,
            measurements,
        };

        let mut output = Vec::new();
        summary.write_tsv(&mut output).unwrap();
        let tsv = String::from_utf8(output).unwrap();

        let lines: Vec<&str> = tsv.lines().collect();
        assert_eq!(lines.len(), 5, "header + 4 data rows");
        assert_eq!(
            lines[0],
            "trip\tphase\tstarted_at\tcompleted_at\tduration_secs\tfees\tstatus",
        );

        assert!(lines[1].starts_with("1\tmint\t"));
        assert!(lines[2].starts_with("1\tredeem\t"));
        assert!(lines[3].starts_with("2\tmint\t"));
        assert!(lines[4].starts_with("2\tredeem\t"));
    }

    #[test]
    fn summary_stats_correct_for_known_values() {
        let measurements = vec![
            make_measurement(1, RoundTripPhase::Mint, 72.1, None),
            make_measurement(1, RoundTripPhase::Redeem, 88.0, None),
            make_measurement(2, RoundTripPhase::Mint, 85.3, None),
            make_measurement(2, RoundTripPhase::Redeem, 95.2, None),
            make_measurement(3, RoundTripPhase::Mint, 77.1, None),
            make_measurement(3, RoundTripPhase::Redeem, 91.1, None),
        ];

        let summary = BenchmarkSummary {
            symbol: Symbol::new("AAPL").unwrap(),
            round_trips: 3,
            measurements,
        };

        let mut output = Vec::new();
        summary.write_summary(&mut output).unwrap();
        let text = String::from_utf8(output).unwrap();

        assert!(
            text.contains("Benchmark complete: 3 round trips for AAPL"),
            "Missing header line in: {text}",
        );
        assert!(text.contains("Mint"), "Missing Mint row in: {text}");
        assert!(text.contains("Redeem"), "Missing Redeem row in: {text}");
        assert!(
            text.contains("Full trip"),
            "Missing Full trip row in: {text}",
        );
    }

    #[test]
    fn duration_stats_min_max_avg_median_odd_count() {
        let mut durations = vec![
            Duration::from_secs_f64(72.1),
            Duration::from_secs_f64(85.3),
            Duration::from_secs_f64(77.1),
        ];

        let stats = DurationStats::compute(&mut durations).unwrap();

        assert!(
            (stats.min.as_secs_f64() - 72.1).abs() < 0.01,
            "min: {}",
            stats.min.as_secs_f64(),
        );
        assert!(
            (stats.max.as_secs_f64() - 85.3).abs() < 0.01,
            "max: {}",
            stats.max.as_secs_f64(),
        );

        let expected_avg = (72.1 + 85.3 + 77.1) / 3.0;
        assert!(
            (stats.avg.as_secs_f64() - expected_avg).abs() < 0.1,
            "avg: {} expected: {expected_avg}",
            stats.avg.as_secs_f64(),
        );

        assert!(
            (stats.median.as_secs_f64() - 77.1).abs() < 0.01,
            "median: {}",
            stats.median.as_secs_f64(),
        );
    }

    #[test]
    fn duration_stats_min_max_avg_median_even_count() {
        let mut durations = vec![
            Duration::from_secs_f64(72.0),
            Duration::from_secs_f64(85.0),
            Duration::from_secs_f64(77.0),
            Duration::from_secs_f64(80.0),
        ];

        let stats = DurationStats::compute(&mut durations).unwrap();

        assert!(
            (stats.min.as_secs_f64() - 72.0).abs() < 0.01,
            "min: {}",
            stats.min.as_secs_f64(),
        );
        assert!(
            (stats.max.as_secs_f64() - 85.0).abs() < 0.01,
            "max: {}",
            stats.max.as_secs_f64(),
        );

        let expected_median = f64::midpoint(77.0, 80.0);
        assert!(
            (stats.median.as_secs_f64() - expected_median).abs() < 0.01,
            "median: {} expected: {expected_median}",
            stats.median.as_secs_f64(),
        );
    }

    #[test]
    fn duration_stats_empty_returns_none() {
        assert!(DurationStats::compute(&mut []).is_none());
    }

    #[test]
    fn duration_stats_single_element() {
        let mut durations = vec![Duration::from_secs_f64(42.5)];
        let stats = DurationStats::compute(&mut durations).unwrap();

        assert!(
            (stats.min.as_secs_f64() - 42.5).abs() < 0.01,
            "single element min",
        );
        assert!(
            (stats.max.as_secs_f64() - 42.5).abs() < 0.01,
            "single element max",
        );
        assert!(
            (stats.avg.as_secs_f64() - 42.5).abs() < 0.01,
            "single element avg",
        );
        assert!(
            (stats.median.as_secs_f64() - 42.5).abs() < 0.01,
            "single element median",
        );
    }

    #[test]
    fn round_trip_phase_display() {
        assert_eq!(RoundTripPhase::Mint.to_string(), "mint");
        assert_eq!(RoundTripPhase::Redeem.to_string(), "redeem");
    }

    #[test]
    fn streaming_tsv_writes_incrementally() {
        let mut output = Vec::new();
        let mut writer = tsv_writer(&mut output);

        let measurement = make_measurement(1, RoundTripPhase::Mint, 75.0, None);
        writer.serialize(&measurement).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("trip\t"));
        assert!(lines[1].starts_with("1\tmint\t"));
    }
}
