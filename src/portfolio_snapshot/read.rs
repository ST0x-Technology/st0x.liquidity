//! Capital and return-on-capital computation for `/pnl`, reading only the
//! reactor-maintained `portfolio_snapshot` table (see SPEC.md "Portfolio
//! Capital and Return Tracking"). Never folds the `events` table: capital is
//! not watermarked to any `as_of_rowid` and always reflects the live snapshot
//! table.
//!
//! Inclusion is decided per ET day, never per row: a day is included in the
//! capital sample only when every nonzero balance that counts toward capital
//! has a known, fresh USD mark. Cash and positive equity count at every
//! location, including the current Alpaca position at the Hedging venue. A
//! negative equity position does not count because the collateral or margin
//! supporting shorts is not modeled. Per-row exclusion of a row that does
//! count would understate capital and mechanically inflate the reported
//! return -- the dangerous direction for a financial metric to be wrong in --
//! so a single bad counted row excludes the whole day instead.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use rain_math_float::Float;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};
use thiserror::Error;

use st0x_execution::{EmptySymbolError, HasZero, Symbol};
use st0x_float_macro::float;
use st0x_float_serde::parse_float_string_or_hex;

use super::write::et_day as et_day_of;
use crate::inventory::{PortfolioAsset, PortfolioLocation};

/// Balances marked more than this many days before their `et_day` exclude
/// the whole day from the capital sample. Only equity marks are
/// staleness-checked -- USDC's mark is a fixed par assumption captured fresh
/// at capture time.
const MARK_STALENESS_THRESHOLD_DAYS: i64 = 7;

/// Minimum included snapshot days, spanning at least this many calendar
/// days, before an annualized return is computed. A single day's return
/// extrapolated by 365x is exactly the misleading-financial-number failure
/// mode the project's fail-fast rules exist to prevent.
const MIN_SAMPLE_DAYS_FOR_ANNUALIZATION: usize = 2;
const MIN_COVERAGE_DAYS_FOR_ANNUALIZATION: i64 = 2;

const DAYS_PER_YEAR: Float = float!(365);
const PERCENT: Float = float!(100);

/// Errors loading and interpreting persisted `portfolio_snapshot` rows.
#[derive(Debug, Error)]
pub(crate) enum ReadError {
    #[error("failed to load portfolio snapshot rows: {0}")]
    Database(#[from] sqlx::Error),
    #[error("failed to parse a persisted portfolio snapshot balance: {0}")]
    Float(#[from] rain_math_float::FloatError),
    #[error("failed to parse persisted portfolio snapshot et_day '{value}': {source}")]
    InvalidEtDay {
        value: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("failed to parse persisted portfolio snapshot asset '{value}': {source}")]
    InvalidAsset {
        value: String,
        #[source]
        source: EmptySymbolError,
    },
    #[error("failed to parse persisted portfolio snapshot location: '{value}'")]
    InvalidLocation { value: String },
    #[error("failed to parse persisted portfolio snapshot mark_captured_at '{value}': {source}")]
    InvalidMarkTimestamp {
        value: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("sample day count {sample_days} does not fit in i64 for the coverage-gap check")]
    SampleDaysOverflow { sample_days: usize },
}

/// Why a day was excluded from the capital sample.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DayExclusionReason {
    MissingMark(PortfolioAsset),
    StaleMark(PortfolioAsset, DateTime<Utc>),
}

/// A day's deployed capital, or the reason it could not be computed. Mutually
/// exclusive by construction -- a day is never both a computed total and an
/// exclusion reason at once, so the two are no longer independent `Option`s
/// that could (invalidly) both be `Some` or both be `None`.
#[derive(Debug, Clone)]
pub(crate) enum DayCapital {
    Included(Float),
    Excluded(DayExclusionReason),
}

impl PartialEq for DayCapital {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Included(left), Self::Included(right)) => left.eq(*right).unwrap_or(false),
            (Self::Excluded(left), Self::Excluded(right)) => left == right,
            (Self::Included(_), Self::Excluded(_)) | (Self::Excluded(_), Self::Included(_)) => {
                false
            }
        }
    }
}

/// One ET day's deployed capital, or the reason it could not be computed.
#[derive(Debug, Clone)]
pub(crate) struct PortfolioDay {
    pub(crate) et_day: NaiveDate,
    pub(crate) capital: DayCapital,
}

/// An inclusive, independently-optional ET-day range: each side is `None`
/// when that bound is unset (open), never a sentinel date. A named struct
/// rather than a bare `(Option<NaiveDate>, Option<NaiveDate>)` tuple so
/// `from` and `to` cannot be transposed at a call site -- the same swap risk
/// `ResolvedRowid` (`src/dashboard/pnl/source.rs`) exists to prevent for a
/// pair of same-typed rowids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct EtDayRange {
    pub(crate) from: Option<NaiveDate>,
    pub(crate) to: Option<NaiveDate>,
}

/// Capital and return-on-capital figures for `/pnl`, computed over a set of
/// [`PortfolioDay`]s with the guards described below.
#[derive(Debug, Clone)]
pub(crate) struct CapitalSummary {
    pub(crate) average_deployed_capital_usd: Option<Float>,
    pub(crate) annualized_return_pct: Option<Float>,
    pub(crate) coverage_days: Option<i64>,
    pub(crate) sample_days: usize,
    pub(crate) first_snapshot_day: Option<NaiveDate>,
    pub(crate) last_snapshot_day: Option<NaiveDate>,
    pub(crate) warnings: Vec<String>,
}

/// A single `(location, asset)` balance row loaded from `portfolio_snapshot`
/// for one ET day, before day-level inclusion is decided.
struct RawBalanceRow {
    asset: PortfolioAsset,
    available: Float,
    inflight: Float,
    usd_mark: Option<Float>,
    mark_captured_at: Option<DateTime<Utc>>,
}

/// Loads every `portfolio_snapshot` row in `et_day_range` (inclusive both
/// ends; each side `None` leaves that end of the range open), grouped and
/// evaluated per ET day.
pub(crate) async fn load_portfolio_days(
    pool: &SqlitePool,
    et_day_range: EtDayRange,
) -> Result<Vec<PortfolioDay>, ReadError> {
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT et_day, location, asset, available_balance, inflight_balance, usd_mark, \
         mark_captured_at \
         FROM portfolio_snapshot",
    );
    let EtDayRange { from, to } = et_day_range;
    let has_condition = from.is_some();
    if let Some(from) = from {
        query.push(" WHERE et_day >= ");
        query.push_bind(from.to_string());
    }
    if let Some(to) = to {
        query.push(if has_condition {
            " AND et_day <= "
        } else {
            " WHERE et_day <= "
        });
        query.push_bind(to.to_string());
    }
    query.push(" ORDER BY et_day ASC, asset ASC");

    let rows = query
        .build_query_as::<(
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
        )>()
        .fetch_all(pool)
        .await?;

    let mut by_day: BTreeMap<NaiveDate, Vec<RawBalanceRow>> = BTreeMap::new();
    for (et_day, location, asset, available, inflight, usd_mark, mark_captured_at) in rows {
        let et_day = parse_et_day(&et_day)?;
        parse_portfolio_location(&location)?;
        let asset = parse_portfolio_asset(&asset)?;
        let available = parse_float_string_or_hex(&available)?;
        let inflight = parse_float_string_or_hex(&inflight)?;
        let usd_mark = usd_mark
            .as_deref()
            .map(parse_float_string_or_hex)
            .transpose()?;
        let mark_captured_at = mark_captured_at
            .as_deref()
            .map(parse_mark_captured_at)
            .transpose()?;

        by_day.entry(et_day).or_default().push(RawBalanceRow {
            asset,
            available,
            inflight,
            usd_mark,
            mark_captured_at,
        });
    }

    by_day
        .into_iter()
        .map(|(et_day, rows)| evaluate_day(et_day, rows))
        .collect()
}

/// Computes [`CapitalSummary`] from already-loaded [`PortfolioDay`]s and the
/// range's numeric net realized PnL accumulator, not a re-parsed response
/// string.
///
/// `query_et_day_range` has the same `(from, to)` bounds that scope
/// `net_realized_pnl_usd`: the PnL numerator is computed over the caller's
/// whole query range, not over snapshot coverage, so annualizing by
/// `365 / coverage_days` is only valid when snapshot coverage fully spans that
/// range. See [`coverage_spans_query_range`].
pub(crate) fn capital_summary(
    days: &[PortfolioDay],
    net_realized_pnl_usd: Float,
    query_et_day_range: EtDayRange,
) -> Result<CapitalSummary, ReadError> {
    let mut warnings = exclusion_warnings(days);

    let included: Vec<(NaiveDate, Float)> = days
        .iter()
        .filter_map(|day| match day.capital {
            DayCapital::Included(total) => Some((day.et_day, total)),
            DayCapital::Excluded(_) => None,
        })
        .collect();
    let sample_days = included.len();

    if sample_days == 0 {
        return Ok(CapitalSummary {
            average_deployed_capital_usd: None,
            annualized_return_pct: None,
            coverage_days: None,
            sample_days: 0,
            first_snapshot_day: None,
            last_snapshot_day: None,
            warnings,
        });
    }

    let first_snapshot_day = included[0].0;
    let last_snapshot_day = included[included.len() - 1].0;
    let coverage_days = (last_snapshot_day - first_snapshot_day).num_days() + 1;
    // Converts sample_days (bounded by however many portfolio_snapshot rows
    // were actually loaded, realistically tiny) to i64 rather than
    // converting coverage_days (a calendar-day span with no such bound) to
    // usize: the smaller-magnitude direction is the one that can fail fast
    // without ever plausibly doing so, instead of silently defaulting.
    let sample_days_i64 = i64::try_from(sample_days)
        .map_err(|_conversion_error| ReadError::SampleDaysOverflow { sample_days })?;
    if sample_days_i64 < coverage_days {
        warnings.push(format!(
            "Portfolio snapshot coverage has gaps: {sample_days} captured day(s) out of \
             {coverage_days} calendar day(s) in range"
        ));
    }

    let mut total = Float::zero()?;
    for (_, day_total) in &included {
        total = (total + *day_total)?;
    }
    let sample_days_float = Float::parse(sample_days.to_string())?;
    let average = (total / sample_days_float)?;

    if average.is_zero()? {
        warnings.push(format!(
            "Average deployed capital over {sample_days} sampled day(s) is zero; return on \
             capital cannot be computed"
        ));
        return Ok(CapitalSummary {
            average_deployed_capital_usd: None,
            annualized_return_pct: None,
            coverage_days: Some(coverage_days),
            sample_days,
            first_snapshot_day: Some(first_snapshot_day),
            last_snapshot_day: Some(last_snapshot_day),
            warnings,
        });
    }

    let meets_sample_and_coverage_minimums = sample_days >= MIN_SAMPLE_DAYS_FOR_ANNUALIZATION
        && coverage_days >= MIN_COVERAGE_DAYS_FOR_ANNUALIZATION;

    let annualized_return_pct = if !meets_sample_and_coverage_minimums {
        warnings.push(format!(
            "Annualized return on capital requires at least \
             {MIN_SAMPLE_DAYS_FOR_ANNUALIZATION} included snapshot days spanning at least \
             {MIN_COVERAGE_DAYS_FOR_ANNUALIZATION} calendar days; only {sample_days} day(s) \
             covering {coverage_days} calendar day(s) are available"
        ));
        None
    } else if !coverage_spans_query_range(first_snapshot_day, last_snapshot_day, query_et_day_range)
    {
        warnings.push(
            "Snapshot coverage does not span the full query range; annualized return omitted \
             to avoid conflating a partial-coverage denominator with full-range realized PnL"
                .to_owned(),
        );
        None
    } else {
        let coverage_days_float = Float::parse(coverage_days.to_string())?;
        let annual_factor = (DAYS_PER_YEAR / coverage_days_float)?;
        let period_return = (net_realized_pnl_usd / average)?;
        Some(((period_return * annual_factor)? * PERCENT)?)
    };

    Ok(CapitalSummary {
        average_deployed_capital_usd: Some(average),
        annualized_return_pct,
        coverage_days: Some(coverage_days),
        sample_days,
        first_snapshot_day: Some(first_snapshot_day),
        last_snapshot_day: Some(last_snapshot_day),
        warnings,
    })
}

/// Whether `[first_snapshot_day, last_snapshot_day]` fully covers
/// `query_et_day_range`: the realized-PnL numerator passed into
/// [`capital_summary`] is scoped to the caller's query range, not to
/// snapshot coverage, so annualizing by `365 / coverage_days` is only valid
/// when the two line up. An unbounded query side (`None`, "load every
/// captured day") can never be proven spanned by a finite snapshot range --
/// `portfolio_snapshot` does not backfill history -- so it conservatively
/// counts as not spanning rather than assuming the coverage happens to reach
/// back to the true start of history.
fn coverage_spans_query_range(
    first_snapshot_day: NaiveDate,
    last_snapshot_day: NaiveDate,
    query_et_day_range: EtDayRange,
) -> bool {
    let EtDayRange {
        from: query_from,
        to: query_to,
    } = query_et_day_range;
    query_from.is_some_and(|from| first_snapshot_day <= from)
        && query_to.is_some_and(|to| last_snapshot_day >= to)
}

fn exclusion_warnings(days: &[PortfolioDay]) -> Vec<String> {
    days.iter()
        .filter_map(|day| match &day.capital {
            DayCapital::Included(_) => None,
            DayCapital::Excluded(reason) => Some(describe_exclusion(day.et_day, reason)),
        })
        .collect()
}

fn describe_exclusion(et_day: NaiveDate, reason: &DayExclusionReason) -> String {
    match reason {
        DayExclusionReason::MissingMark(asset) => format!(
            "Portfolio capital sample excludes {et_day}: no USD mark observed yet for {asset}"
        ),
        DayExclusionReason::StaleMark(asset, mark_captured_at) => format!(
            "Portfolio capital sample excludes {et_day}: USD mark for {asset} is stale \
             (last observed {mark_captured_at})"
        ),
    }
}

fn evaluate_day(et_day: NaiveDate, rows: Vec<RawBalanceRow>) -> Result<PortfolioDay, ReadError> {
    let mut total = Float::zero()?;

    for row in rows {
        let balance = (row.available + row.inflight)?;
        if balance.is_zero()? {
            continue;
        }

        // Deployed capital = cash everywhere + positive equity holdings
        // everywhere (SPEC.md "Portfolio Capital and Return Tracking"). The
        // Hedging venue contains the broker's one current equity position;
        // counter-trades consume or replenish that inventory rather than
        // creating a separate hedge leg. A negative broker position does not
        // contribute under this long-inventory definition because the margin
        // supporting shorts is not modeled. Skip it before mark validation:
        // a row that cannot contribute must not gate the whole day.
        if matches!(&row.asset, PortfolioAsset::Equity(_)) && balance.is_negative()? {
            continue;
        }

        let Some(mark) = row.usd_mark else {
            return Ok(PortfolioDay {
                et_day,
                capital: DayCapital::Excluded(DayExclusionReason::MissingMark(row.asset)),
            });
        };

        // write.rs always sets usd_mark and mark_captured_at together, so a
        // mark without a timestamp should never occur in practice. Treat it
        // the same as a missing mark rather than fabricating a timestamp for
        // StaleMark.
        let Some(mark_captured_at) = row.mark_captured_at else {
            return Ok(PortfolioDay {
                et_day,
                capital: DayCapital::Excluded(DayExclusionReason::MissingMark(row.asset)),
            });
        };

        if is_stale_mark(&row.asset, mark_captured_at, et_day) {
            return Ok(PortfolioDay {
                et_day,
                capital: DayCapital::Excluded(DayExclusionReason::StaleMark(
                    row.asset,
                    mark_captured_at,
                )),
            });
        }

        total = (total + (balance * mark)?)?;
    }

    Ok(PortfolioDay {
        et_day,
        capital: DayCapital::Included(total),
    })
}

fn is_stale_mark(
    asset: &PortfolioAsset,
    mark_captured_at: DateTime<Utc>,
    et_day: NaiveDate,
) -> bool {
    if !matches!(asset, PortfolioAsset::Equity(_)) {
        return false;
    }

    // Both sides must be the same calendar basis: `et_day` is already an ET
    // calendar day (see this module's callers), so `mark_captured_at` -- a
    // stored UTC timestamp -- is converted through the same ET-day helper
    // the rest of this module relies on, rather than `.date_naive()` (the
    // UTC calendar date). ET trails UTC by 4-5 hours, so a mark timestamped
    // in the ~20:00-23:59 ET window has a UTC date one day ahead of its true
    // ET day; comparing that raw UTC date against `et_day` would understate
    // the mark's age by a day and let a genuinely stale mark pass the gate.
    let mark_et_day = et_day_of(mark_captured_at);
    let age_days = (et_day - mark_et_day).num_days();
    age_days > MARK_STALENESS_THRESHOLD_DAYS
}

fn parse_et_day(value: &str) -> Result<NaiveDate, ReadError> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|source| ReadError::InvalidEtDay {
        value: value.to_owned(),
        source,
    })
}

/// Inverse of `PortfolioLocation`'s `Display` impl (`src/inventory/view.rs`),
/// which is what `PortfolioSnapshotProjection` persists into the `location`
/// column (`row.row.location.to_string()`).
fn parse_portfolio_location(value: &str) -> Result<PortfolioLocation, ReadError> {
    match value {
        "market_making" => Ok(PortfolioLocation::MarketMaking),
        "hedging" => Ok(PortfolioLocation::Hedging),
        "ethereum_wallet" => Ok(PortfolioLocation::EthereumWallet),
        "base_wallet_unwrapped" => Ok(PortfolioLocation::BaseWalletUnwrapped),
        "base_wallet_wrapped" => Ok(PortfolioLocation::BaseWalletWrapped),
        _ => Err(ReadError::InvalidLocation {
            value: value.to_owned(),
        }),
    }
}

fn parse_portfolio_asset(value: &str) -> Result<PortfolioAsset, ReadError> {
    if value == "USDC" {
        return Ok(PortfolioAsset::Usdc);
    }

    Symbol::new(value)
        .map(PortfolioAsset::Equity)
        .map_err(|source| ReadError::InvalidAsset {
            value: value.to_owned(),
            source,
        })
}

fn parse_mark_captured_at(value: &str) -> Result<DateTime<Utc>, ReadError> {
    DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.with_timezone(&Utc))
        .map_err(|source| ReadError::InvalidMarkTimestamp {
            value: value.to_owned(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use chrono::{Days, TimeZone};

    use crate::test_utils::setup_test_db;

    use super::*;

    fn usd(value: i64) -> Float {
        float!(&value.to_string())
    }

    async fn insert_row(
        pool: &SqlitePool,
        et_day: &str,
        location: &str,
        asset: &str,
        available: &str,
        usd_mark: Option<&str>,
        mark_captured_at: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO portfolio_snapshot \
             (et_day, captured_at, location, asset, available_balance, inflight_balance, \
              usd_mark, mark_captured_at) \
             VALUES (?, ?, ?, ?, ?, '0', ?, ?)",
        )
        .bind(et_day)
        .bind(format!("{et_day}T04:05:00+00:00"))
        .bind(location)
        .bind(asset)
        .bind(available)
        .bind(usd_mark)
        .bind(mark_captured_at)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_portfolio_days_sums_all_rows_for_a_fully_marked_day() {
        let pool = setup_test_db().await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "USDC",
            "1000",
            Some("1"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "AAPL",
            "2",
            Some("50"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].et_day,
            NaiveDate::from_ymd_opt(2026, 7, 18).unwrap()
        );
        match &days[0].capital {
            DayCapital::Included(total_usd) => assert!(total_usd.eq(usd(1100)).unwrap()),
            DayCapital::Excluded(reason) => panic!("expected day to be included, got {reason:?}"),
        }
    }

    #[tokio::test]
    async fn load_portfolio_days_excludes_whole_day_on_missing_mark() {
        let pool = setup_test_db().await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "USDC",
            "1000",
            Some("1"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "AAPL",
            "2",
            None,
            None,
        )
        .await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].capital,
            DayCapital::Excluded(DayExclusionReason::MissingMark(PortfolioAsset::Equity(
                Symbol::new("AAPL").unwrap()
            )))
        );
    }

    #[tokio::test]
    async fn load_portfolio_days_excludes_whole_day_on_stale_mark() {
        let pool = setup_test_db().await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "AAPL",
            "2",
            Some("50"),
            Some("2026-07-01T04:05:00+00:00"),
        )
        .await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 1);
        let expected_captured_at = Utc.with_ymd_and_hms(2026, 7, 1, 4, 5, 0).unwrap();
        assert_eq!(
            days[0].capital,
            DayCapital::Excluded(DayExclusionReason::StaleMark(
                PortfolioAsset::Equity(Symbol::new("AAPL").unwrap()),
                expected_captured_at
            ))
        );
    }

    #[tokio::test]
    async fn load_portfolio_days_zero_balance_row_with_no_mark_does_not_exclude_day() {
        let pool = setup_test_db().await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "USDC",
            "1000",
            Some("1"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;
        insert_row(&pool, "2026-07-18", "hedging", "AAPL", "0", None, None).await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 1);
        match &days[0].capital {
            DayCapital::Included(total_usd) => assert!(total_usd.eq(usd(1000)).unwrap()),
            DayCapital::Excluded(reason) => panic!("expected day to be included, got {reason:?}"),
        }
    }

    #[tokio::test]
    async fn load_portfolio_days_respects_et_day_range_bounds() {
        let pool = setup_test_db().await;
        insert_row(
            &pool,
            "2026-07-17",
            "market_making",
            "USDC",
            "1000",
            Some("1"),
            Some("2026-07-17T04:05:00+00:00"),
        )
        .await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "USDC",
            "2000",
            Some("1"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;
        insert_row(
            &pool,
            "2026-07-19",
            "market_making",
            "USDC",
            "3000",
            Some("1"),
            Some("2026-07-19T04:05:00+00:00"),
        )
        .await;

        let range = EtDayRange {
            from: Some(NaiveDate::from_ymd_opt(2026, 7, 18).unwrap()),
            to: Some(NaiveDate::from_ymd_opt(2026, 7, 18).unwrap()),
        };
        let days = load_portfolio_days(&pool, range).await.unwrap();

        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].et_day,
            NaiveDate::from_ymd_opt(2026, 7, 18).unwrap()
        );
    }

    /// An open-ended range (only `from` set) must include every day at or
    /// after it, with no upper bound.
    #[tokio::test]
    async fn load_portfolio_days_respects_open_ended_lower_bound() {
        let pool = setup_test_db().await;
        insert_row(
            &pool,
            "2026-07-17",
            "market_making",
            "USDC",
            "1000",
            Some("1"),
            Some("2026-07-17T04:05:00+00:00"),
        )
        .await;
        insert_row(
            &pool,
            "2026-07-19",
            "market_making",
            "USDC",
            "3000",
            Some("1"),
            Some("2026-07-19T04:05:00+00:00"),
        )
        .await;

        let range = EtDayRange {
            from: Some(NaiveDate::from_ymd_opt(2026, 7, 18).unwrap()),
            to: None,
        };
        let days = load_portfolio_days(&pool, range).await.unwrap();

        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].et_day,
            NaiveDate::from_ymd_opt(2026, 7, 19).unwrap()
        );
    }

    /// Positive equity inventory counts at both venues: the Hedging row is
    /// the broker's current holding, not a distinct short leg paired with the
    /// MarketMaking balance.
    #[tokio::test]
    async fn deployed_capital_includes_positive_equity_at_both_venues() {
        let pool = setup_test_db().await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "USDC",
            "5000",
            Some("1"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;
        insert_row(
            &pool,
            "2026-07-18",
            "hedging",
            "USDC",
            "2000",
            Some("1"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;
        insert_row(
            &pool,
            "2026-07-18",
            "market_making",
            "AAPL",
            "10",
            Some("150"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;
        // The broker's current positive AAPL inventory.
        insert_row(
            &pool,
            "2026-07-18",
            "hedging",
            "AAPL",
            "10",
            Some("150"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 1);
        match &days[0].capital {
            DayCapital::Included(total_usd) => assert!(
                total_usd.eq(usd(10000)).unwrap(),
                "expected cash (5000 + 2000) + equity at both venues \
                 (10 * 150 + 10 * 150 = 3000) = 10000, got {total_usd:?}"
            ),
            DayCapital::Excluded(reason) => panic!("expected day to be included, got {reason:?}"),
        }
    }

    /// A positive Hedging-venue equity row is real broker inventory and
    /// contributes to deployed capital even when no MarketMaking row exists.
    #[tokio::test]
    async fn positive_hedging_equity_row_alone_contributes_to_capital() {
        let pool = setup_test_db().await;
        insert_row(
            &pool,
            "2026-07-18",
            "hedging",
            "AAPL",
            "10",
            Some("150"),
            Some("2026-07-18T04:05:00+00:00"),
        )
        .await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 1);
        match &days[0].capital {
            DayCapital::Included(total_usd) => assert!(
                total_usd.eq(usd(1500)).unwrap(),
                "the broker's 10 positive AAPL shares at 150 must contribute 1500"
            ),
            DayCapital::Excluded(reason) => panic!("expected day to be included, got {reason:?}"),
        }
    }

    /// Reallocating the same equity inventory between venues must not change
    /// deployed capital. A counter-trade moves the holdings in opposite
    /// directions while preserving their combined quantity.
    #[tokio::test]
    async fn equity_capital_is_invariant_to_venue_allocation() {
        let pool = setup_test_db().await;
        for (et_day, market_making, hedging) in
            [("2026-07-18", "60", "40"), ("2026-07-19", "50", "50")]
        {
            for (location, available) in [("market_making", market_making), ("hedging", hedging)] {
                insert_row(
                    &pool,
                    et_day,
                    location,
                    "AAPL",
                    available,
                    Some("100"),
                    Some(&format!("{et_day}T04:05:00+00:00")),
                )
                .await;
            }
        }

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 2);
        for day in days {
            match day.capital {
                DayCapital::Included(total_usd) => assert!(
                    total_usd.eq(usd(10000)).unwrap(),
                    "100 total AAPL shares at 100 must contribute 10000 regardless of venue"
                ),
                DayCapital::Excluded(reason) => {
                    panic!("expected day to be included, got {reason:?}")
                }
            }
        }
    }

    /// A negative broker position is not positive equity inventory. Its
    /// margin requirement is not modeled, so it contributes nothing and does
    /// not require a mark.
    #[tokio::test]
    async fn negative_hedging_equity_does_not_contribute_or_gate_the_day() {
        let pool = setup_test_db().await;
        insert_row(&pool, "2026-07-18", "hedging", "AAPL", "-10", None, None).await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 1);
        match &days[0].capital {
            DayCapital::Included(total_usd) => assert!(total_usd.eq(usd(0)).unwrap()),
            DayCapital::Excluded(reason) => panic!("expected day to be included, got {reason:?}"),
        }
    }

    /// USDC (cash) contributes to capital at EVERY location, including both
    /// venues and every wallet-transit point -- only equity's Hedging leg is
    /// special-cased.
    #[tokio::test]
    async fn usdc_contributes_to_capital_at_every_location() {
        let pool = setup_test_db().await;
        for (location, available) in [
            ("market_making", "1000"),
            ("hedging", "2000"),
            ("ethereum_wallet", "300"),
            ("base_wallet_unwrapped", "40"),
        ] {
            insert_row(
                &pool,
                "2026-07-18",
                location,
                "USDC",
                available,
                Some("1"),
                Some("2026-07-18T04:05:00+00:00"),
            )
            .await;
        }

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();

        assert_eq!(days.len(), 1);
        match &days[0].capital {
            DayCapital::Included(total_usd) => {
                assert!(total_usd.eq(usd(3340)).unwrap(), "got {total_usd:?}");
            }
            DayCapital::Excluded(reason) => panic!("expected day to be included, got {reason:?}"),
        }
    }

    fn included_day(day: NaiveDate, total: i64) -> PortfolioDay {
        PortfolioDay {
            et_day: day,
            capital: DayCapital::Included(usd(total)),
        }
    }

    fn day(offset: u64) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 7, 14)
            .unwrap()
            .checked_add_days(Days::new(offset))
            .unwrap()
    }

    #[test]
    fn capital_summary_hand_computed_two_day_average_and_annualized_return() {
        let days = vec![included_day(day(0), 1000), included_day(day(1), 2000)];
        let query_range = EtDayRange {
            from: Some(day(0)),
            to: Some(day(1)),
        };

        let summary = capital_summary(&days, usd(30), query_range).unwrap();

        assert_eq!(summary.sample_days, 2);
        assert_eq!(summary.coverage_days, Some(2));
        assert_eq!(summary.first_snapshot_day, Some(day(0)));
        assert_eq!(summary.last_snapshot_day, Some(day(1)));
        assert!(
            summary
                .average_deployed_capital_usd
                .unwrap()
                .eq(usd(1500))
                .unwrap()
        );
        assert!(summary.annualized_return_pct.unwrap().eq(usd(365)).unwrap());
        assert!(summary.warnings.is_empty());
    }

    #[test]
    fn capital_summary_hand_computed_five_day_average_and_annualized_return() {
        let totals = [800, 900, 1000, 1100, 1200];
        let days: Vec<_> = totals
            .iter()
            .enumerate()
            .map(|(offset, total)| included_day(day(offset as u64), *total))
            .collect();
        let query_range = EtDayRange {
            from: Some(day(0)),
            to: Some(day(4)),
        };

        let summary = capital_summary(&days, usd(50), query_range).unwrap();

        assert_eq!(summary.sample_days, 5);
        assert_eq!(summary.coverage_days, Some(5));
        assert!(
            summary
                .average_deployed_capital_usd
                .unwrap()
                .eq(usd(1000))
                .unwrap()
        );
        assert!(summary.annualized_return_pct.unwrap().eq(usd(365)).unwrap());
    }

    /// `capital_summary` itself emits no warning for the zero-snapshot-rows
    /// case; `CAPITAL_UNAVAILABLE_NOTE` is added one layer up, by
    /// `apply_capital_summary` (`src/dashboard/pnl/source.rs`), which this
    /// test does not exercise.
    #[test]
    fn capital_summary_zero_included_days_returns_none_without_a_warning() {
        let summary = capital_summary(&[], usd(0), EtDayRange::default()).unwrap();

        assert!(summary.average_deployed_capital_usd.is_none());
        assert!(summary.annualized_return_pct.is_none());
        assert_eq!(summary.coverage_days, None);
        assert_eq!(summary.sample_days, 0);
        assert_eq!(summary.first_snapshot_day, None);
        assert_eq!(summary.last_snapshot_day, None);
        assert_eq!(summary.warnings, Vec::<String>::new());
    }

    #[test]
    fn capital_summary_single_included_day_reports_average_but_no_annualized_figure() {
        let days = vec![included_day(day(0), 1000)];

        let summary = capital_summary(&days, usd(10), EtDayRange::default()).unwrap();

        assert_eq!(summary.sample_days, 1);
        assert_eq!(summary.coverage_days, Some(1));
        assert!(
            summary
                .average_deployed_capital_usd
                .unwrap()
                .eq(usd(1000))
                .unwrap()
        );
        assert!(summary.annualized_return_pct.is_none());
        assert_eq!(summary.warnings.len(), 1);
        assert!(summary.warnings[0].contains("Annualized return on capital requires"));
    }

    #[test]
    fn capital_summary_average_capital_zero_returns_none_without_dividing() {
        let days = vec![included_day(day(0), 0), included_day(day(1), 0)];

        let summary = capital_summary(&days, usd(10), EtDayRange::default()).unwrap();

        assert!(summary.average_deployed_capital_usd.is_none());
        assert!(summary.annualized_return_pct.is_none());
        assert_eq!(summary.coverage_days, Some(2));
        assert_eq!(summary.sample_days, 2);
        assert_eq!(summary.warnings.len(), 1);
        assert!(summary.warnings[0].contains("is zero"));
    }

    #[test]
    fn capital_summary_missing_mark_and_stale_mark_produce_distinct_warnings() {
        let stale_at = Utc.with_ymd_and_hms(2026, 7, 1, 4, 5, 0).unwrap();
        let days = vec![
            PortfolioDay {
                et_day: day(0),
                capital: DayCapital::Excluded(DayExclusionReason::MissingMark(
                    PortfolioAsset::Equity(Symbol::new("AAPL").unwrap()),
                )),
            },
            PortfolioDay {
                et_day: day(1),
                capital: DayCapital::Excluded(DayExclusionReason::StaleMark(
                    PortfolioAsset::Equity(Symbol::new("TSLA").unwrap()),
                    stale_at,
                )),
            },
        ];

        let summary = capital_summary(&days, usd(0), EtDayRange::default()).unwrap();

        assert_eq!(summary.sample_days, 0);
        assert_eq!(summary.warnings.len(), 2);
        assert!(summary.warnings[0].contains("no USD mark observed yet for AAPL"));
        assert!(summary.warnings[1].contains("USD mark for TSLA is stale"));
    }

    #[test]
    fn capital_summary_gap_in_coverage_adds_warning_but_still_annualizes() {
        let days = vec![included_day(day(0), 1000), included_day(day(4), 2000)];
        let query_range = EtDayRange {
            from: Some(day(0)),
            to: Some(day(4)),
        };

        let summary = capital_summary(&days, usd(30), query_range).unwrap();

        assert_eq!(summary.sample_days, 2);
        assert_eq!(summary.coverage_days, Some(5));
        assert!(
            summary
                .average_deployed_capital_usd
                .unwrap()
                .eq(usd(1500))
                .unwrap()
        );
        assert!(summary.annualized_return_pct.unwrap().eq(usd(146)).unwrap());
        assert!(
            summary
                .warnings
                .iter()
                .any(|warning| warning.contains("coverage has gaps"))
        );
    }

    /// The realized-PnL numerator is scoped to the caller's whole query
    /// range, not to snapshot coverage: when the query range extends beyond
    /// what was actually captured, annualizing by `365 / coverage_days`
    /// would conflate a wider-period PnL figure with a narrower capital
    /// sample. Average capital is still reported honestly.
    #[test]
    fn capital_summary_omits_annualized_when_query_range_exceeds_snapshot_coverage() {
        let days = vec![included_day(day(0), 1000), included_day(day(1), 2000)];
        // Query spans a month before the earliest captured day: realized PnL
        // over that whole month must not be annualized against the 2-day
        // coverage denominator.
        let query_range = EtDayRange {
            from: Some(day(0).checked_sub_days(Days::new(30)).unwrap()),
            to: Some(day(1)),
        };

        let summary = capital_summary(&days, usd(30), query_range).unwrap();

        assert!(
            summary
                .average_deployed_capital_usd
                .unwrap()
                .eq(usd(1500))
                .unwrap(),
            "average deployed capital must still be reported"
        );
        assert!(summary.annualized_return_pct.is_none());
        assert!(
            summary
                .warnings
                .iter()
                .any(|warning| warning.contains("does not span the full query range"))
        );
    }

    /// The mirror case: when snapshot coverage fully spans the query range,
    /// annualization proceeds as usual.
    #[test]
    fn capital_summary_annualizes_when_coverage_fully_spans_query_range() {
        let days = vec![included_day(day(0), 1000), included_day(day(1), 2000)];
        let query_range = EtDayRange {
            from: Some(day(0)),
            to: Some(day(1)),
        };

        let summary = capital_summary(&days, usd(30), query_range).unwrap();

        assert!(summary.annualized_return_pct.unwrap().eq(usd(365)).unwrap());
    }

    /// Boundary of `is_stale_mark`: exactly `MARK_STALENESS_THRESHOLD_DAYS`
    /// old must stay fresh (included); one day older must flip to stale
    /// (excluded).
    #[tokio::test]
    async fn load_portfolio_days_mark_staleness_boundary() {
        let pool = setup_test_db().await;
        let et_day_value = "2026-07-18";
        let fresh_mark = "2026-07-11T04:05:00+00:00"; // exactly 7 days old
        let stale_mark = "2026-07-10T04:05:00+00:00"; // 8 days old

        insert_row(
            &pool,
            et_day_value,
            "market_making",
            "AAPL",
            "2",
            Some("50"),
            Some(fresh_mark),
        )
        .await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();
        assert_eq!(days.len(), 1);
        match &days[0].capital {
            DayCapital::Included(_) => {}
            DayCapital::Excluded(reason) => {
                panic!("expected exactly-7-day-old mark to stay fresh, got {reason:?}")
            }
        }

        sqlx::query("DELETE FROM portfolio_snapshot WHERE et_day = ?")
            .bind(et_day_value)
            .execute(&pool)
            .await
            .unwrap();
        insert_row(
            &pool,
            et_day_value,
            "market_making",
            "AAPL",
            "2",
            Some("50"),
            Some(stale_mark),
        )
        .await;

        let days = load_portfolio_days(&pool, EtDayRange::default())
            .await
            .unwrap();
        assert_eq!(days.len(), 1);
        match &days[0].capital {
            DayCapital::Excluded(DayExclusionReason::StaleMark(..)) => {}
            other => panic!("expected an 8-day-old mark to be excluded as stale, got {other:?}"),
        }
    }

    /// A mark timestamped in the evening ET window has a UTC calendar date
    /// one day ahead of its true ET calendar day. Comparing that raw UTC
    /// date against `et_day` (as `.date_naive()` did before this fix) would
    /// shrink the computed age by a day, letting a mark that is genuinely 8
    /// ET-calendar-days old pass as only 7 days old (still fresh). This test
    /// would fail under that UTC/ET mismatch.
    #[test]
    fn is_stale_mark_uses_et_calendar_day_not_utc_calendar_day() {
        let et_day = NaiveDate::from_ymd_opt(2026, 7, 18).unwrap();
        let asset = PortfolioAsset::Equity(Symbol::new("AAPL").unwrap());

        // 2026-07-10T23:00:00 EDT (UTC-4) = 2026-07-11T03:00:00Z: the UTC
        // calendar date (07-11) is 7 days before et_day, but the true ET
        // calendar day (07-10) is 8 days before -- past the threshold.
        let mark_captured_at = Utc.with_ymd_and_hms(2026, 7, 11, 3, 0, 0).unwrap();

        assert!(
            is_stale_mark(&asset, mark_captured_at, et_day),
            "a mark whose true ET day is 8 days before et_day must be stale, even though its \
             UTC calendar date is only 7 days before et_day"
        );
    }
}
