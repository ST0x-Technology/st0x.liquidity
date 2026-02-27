//! Benchmark reporting: TSV output and CLI summary statistics.

use csv::WriterBuilder;
use std::io::Write;
use std::time::Duration;

use st0x_execution::Symbol;

use super::measurement::{Measurement, RoundTripPhase};

/// Collected measurements from a series of round trips.
pub(crate) struct BenchmarkSummary {
    pub(crate) symbol: Symbol,
    pub(crate) round_trips: usize,
    pub(crate) measurements: Vec<Measurement>,
}

pub(super) fn tsv_writer<W: Write>(writer: W) -> csv::Writer<W> {
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

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    use super::*;
    use crate::threshold::Usd;
    use crate::tokenization::TokenizationRequestStatus;

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
