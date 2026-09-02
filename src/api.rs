//! HTTP API endpoints for health checks, log retrieval, and order status.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use alloy::primitives::U256;
use axum::Json;
use axum::Router;
use axum::extract::{ConnectInfo, Path, Query, Request, State};
use axum::http::StatusCode;
use axum::http::header::{CACHE_CONTROL, HeaderName};
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use st0x_config::{BrokerCtx, OpsApiConfig};
use st0x_dto::{
    EquityTimings, HedgeLatencies, InfraReport, RebalanceTimings, ReliabilityReport, TradingVenue,
};
use st0x_execution::Symbol;
use st0x_execution::alpaca_broker_api::AccountActivitiesQuery;
use st0x_finance::FractionalShares;
use st0x_tokenization::IssuerRequestId;

use crate::AppState;
use crate::dashboard::pnl::{
    PnlError, PnlQuery, PnlResponse, acquire_pnl_report_permit, build_pnl_report_with_permit,
    validate_pnl_snapshot_rowid,
};
use crate::dashboard::transfer_loader::{InvalidTransferKind, TransferKind};
use crate::dashboard::{TradePage, TradeProtocol, TradeQuery, query_trades};
use crate::equity_redemption::{EquityRedemptionEvent, RedemptionAggregateId};
use crate::iap_auth::{IapVerifier, require_iap};
use crate::performance::equity_timing::load_equity_timings;
use crate::performance::infra::{load_dependency_stats, load_monitor_telemetry};
use crate::performance::rebalance::load_rebalance_timings;
use crate::performance::reliability::{
    aggregate_log_entries, load_failure_events, load_job_queue_health,
};
use crate::performance::{ReportRange, hedge_latency_report, load_hedge_performance};
use crate::rebalancing::RebalancingService;
use crate::rebalancing::equity::{CrossVenueEquityTransfer, RecheckError, RecheckOutcome};
use crate::tokenized_equity_mint::TokenizedEquityMintEvent;

/// Comma-separated filter for transfer kinds in query parameters.
///
/// Parses `"equity_mint,usdc_bridge"` into `vec![EquityMint, UsdcBridge]`.
fn parse_transfer_kind_filter(value: &str) -> Result<Vec<TransferKind>, InvalidTransferKind> {
    value
        .split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(TransferKind::from_str)
        .collect()
}

static STARTED_AT: LazyLock<DateTime<Utc>> = LazyLock::new(Utc::now);
const DEFAULT_RAINDEX_ORDERS_PAGE_SIZE: u32 = 50;
const MAX_RAINDEX_ORDERS_PAGE_SIZE: u32 = 100;

/// Upper bound on ERROR/WARN log entries aggregated per reliability report.
const MAX_RELIABILITY_LOG_ENTRIES: usize = 50_000;

const GIT_COMMIT: &str = match option_env!("ST0X_GIT_COMMIT") {
    Some(val) => val,
    None => "dev",
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: String,
    timestamp: DateTime<Utc>,
    git_commit: String,
    uptime_seconds: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogResponse {
    entries: Vec<serde_json::Value>,
    total: usize,
    has_more: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingOrderResponse {
    view_id: String,
    status: String,
    symbol: String,
    direction: String,
    shares: String,
    executor: String,
    placed_at: String,
    submitted_at: Option<String>,
    shares_filled: Option<String>,
    avg_price: Option<String>,
}

/// Where the stranded equity physically sits for a failed transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StuckLocation {
    /// At the issuer (Alpaca): a mint was accepted but failed before tokens
    /// were received on-chain.
    Issuer,
    /// In the bot wallet as unwrapped tokenized equity: a mint received tokens
    /// but failed while wrapping.
    BotWalletUnwrapped,
    /// In the bot wallet as wrapped vault shares: a mint wrapped tokens but
    /// failed depositing into Raindex.
    BotWalletWrapped,
    /// In the redemption wallet: a redemption sent tokens but failed during
    /// detection or was rejected.
    RedemptionWallet,
}

/// Why a transfer is stranded, mirroring the terminal failure event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StuckReason {
    MintAcceptanceFailed,
    WrappingFailed,
    RaindexDepositFailed,
    DetectionFailed,
    RedemptionRejected,
}

#[derive(Debug, Clone, Serialize)]
struct StuckTransferInfo {
    #[serde(rename = "stuckAmount")]
    amount: String,
    #[serde(rename = "stuckLocation")]
    location: StuckLocation,
    #[serde(rename = "stuckReason")]
    reason: StuckReason,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TradeResponse {
    entries: Vec<serde_json::Value>,
    total: usize,
    has_more: bool,
}

async fn health(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let uptime = Utc::now() - *STARTED_AT;

    // Gated on the startup barrier: every essential run loop has
    // acknowledged before this reports healthy, which includes the conductor
    // and therefore `startup_smoke_checks` (chain ids, cutoff probe, broker
    // round-trip, asset read canary) plus the account verification done at
    // executor construction. A deploy probe polling this endpoint therefore
    // cannot see a 200 from a bot that failed its startup checks.
    let (status_code, status) = if state.health.is_ready() {
        (StatusCode::OK, "healthy")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "starting")
    };

    (
        status_code,
        Json(HealthResponse {
            status: status.to_string(),
            timestamp: Utc::now(),
            git_commit: GIT_COMMIT.to_string(),
            uptime_seconds: uptime.num_seconds(),
        }),
    )
}

/// Deadline for bringing the PnL ledger current on the `/pnl` request path.
/// Catch-up work is proportional to the un-ingested backlog, not to the
/// request, and concurrent requests serialize on the ledger's internal mutex
/// before any admission control -- so past this deadline the request sheds
/// with 503 instead of queueing behind ingestion. The boot-path catch-up
/// stays unbounded: first-deploy backfill may legitimately exceed any
/// request deadline. Cancellation is safe because each ingest batch commits
/// its rows and checkpoint atomically; an elapsed deadline only rolls back
/// the in-flight batch.
const PNL_CATCH_UP_TIMEOUT: Duration = Duration::from_secs(10);

async fn pnl(
    State(state): State<AppState>,
    Query(query): Query<PnlQuery>,
) -> Result<Json<PnlResponse>, (StatusCode, String)> {
    let after = query
        .activity_after()
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let until = query
        .activity_until()
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    query
        .symbol_filter(&mut Vec::new())
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let head = tokio::time::timeout(PNL_CATCH_UP_TIMEOUT, state.pnl_ledger.catch_up())
        .await
        .map_err(|_elapsed| {
            warn!("PnL ledger catch-up exceeded its request deadline");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "PnL ledger catch-up timed out".to_string(),
            )
        })?
        .map_err(|error| pnl_error_response(PnlError::Ledger(error)))?;
    validate_pnl_snapshot_rowid(head, &query).map_err(pnl_error_response)?;
    let permit =
        acquire_pnl_report_permit(&state.pnl_report_admission).map_err(pnl_error_response)?;

    let activities = if let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &state.ctx.broker {
        alpaca_auth
            .fetch_account_activities(&AccountActivitiesQuery::pnl(after, until))
            .await
            .map_err(|error| {
                error!(%error, "Failed to fetch Alpaca account activities for PnL");
                (
                    StatusCode::BAD_GATEWAY,
                    "Failed to fetch Alpaca account activities".to_string(),
                )
            })?
    } else {
        Vec::new()
    };

    build_pnl_report_with_permit(&state.pool, &query, activities, Utc::now(), permit, head)
        .await
        .map(Json)
        .map_err(pnl_error_response)
}

fn pnl_error_response(error: PnlError) -> (StatusCode, String) {
    match error {
        PnlError::InvalidDate { .. }
        | PnlError::InvalidSnapshotRowid { .. }
        | PnlError::InvalidSymbolFilter { .. } => (StatusCode::BAD_REQUEST, error.to_string()),
        PnlError::InvalidLedgerRow { .. }
        | PnlError::MalformedPayload { .. }
        | PnlError::InvalidFinancialField { .. }
        | PnlError::InvalidInternalDecimal { .. } => {
            error!(%error, "Failed to build PnL report from ledger data");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build PnL report".to_string(),
            )
        }
        PnlError::Ledger(error) => {
            error!(%error, "PnL ledger ingestion failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build PnL report".to_string(),
            )
        }
        PnlError::Database(error) => {
            error!(%error, "Failed to build PnL report");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build PnL report".to_string(),
            )
        }
        PnlError::PortfolioSnapshot(error) => {
            error!(%error, "Failed to load portfolio snapshot data for PnL report");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build PnL report".to_string(),
            )
        }
        PnlError::Arithmetic(error) => {
            error!(%error, "PnL arithmetic failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build PnL report".to_string(),
            )
        }
        PnlError::ReplayAdmission(error) => {
            warn!(%error, "PnL report capacity exhausted");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "PnL report capacity exhausted".to_string(),
            )
        }
        PnlError::ReplayWorker(error) => {
            error!(%error, "PnL replay worker failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build PnL report".to_string(),
            )
        }
    }
}

#[derive(Deserialize, Default)]
struct LogsQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    search: Option<String>,
    level: Option<String>,
    target: Option<String>,
    since: Option<String>,
    until: Option<String>,
}

/// Returns log entries with pagination, search, level, and time range
/// filters. Entries are returned newest-first.
///
/// - `limit`: page size (default 100, max 5000)
/// - `offset`: skip N matching entries from the newest (default 0)
/// - `search`: case-insensitive substring filter across the raw JSON line
/// - `level`: comma-separated log levels to include (e.g. `ERROR,WARN`)
/// - `target`: comma-separated domain targets to include (e.g. `hedge,rebalance`)
/// - `since`: ISO 8601 UTC lower bound (inclusive)
/// - `until`: ISO 8601 UTC upper bound (inclusive)
async fn logs(State(state): State<AppState>, Query(query): Query<LogsQuery>) -> Json<LogResponse> {
    let limit = query.limit.unwrap_or(100).min(5000);
    let offset = query.offset.unwrap_or(0);

    let Some(ref log_dir) = state.ctx.log_dir else {
        return Json(LogResponse {
            entries: Vec::new(),
            total: 0,
            has_more: false,
        });
    };

    let filter = LogFilter {
        search_lower: query
            .search
            .as_deref()
            .filter(|val| !val.is_empty())
            .map(str::to_lowercase),
        levels: query
            .level
            .as_deref()
            .filter(|val| !val.is_empty())
            .map(|val| {
                val.split(',')
                    .map(|part| part.trim().to_uppercase())
                    .collect()
            }),
        targets: query
            .target
            .as_deref()
            .filter(|val| !val.is_empty())
            .map(|val| {
                val.split(',')
                    .map(|part| part.trim().to_lowercase())
                    .collect()
            }),
        since: query.since.as_deref().and_then(|val| {
            DateTime::parse_from_rfc3339(val)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        }),
        until: query.until.as_deref().and_then(|val| {
            DateTime::parse_from_rfc3339(val)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        }),
    };

    let (entries, total, has_more) = read_matching_entries(log_dir, &filter, offset, limit);

    Json(LogResponse {
        entries,
        total,
        has_more,
    })
}

struct LogFilter {
    search_lower: Option<String>,
    levels: Option<Vec<String>>,
    targets: Option<Vec<String>>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
}

/// Reads log entries from `log_dir` in newest-first order, applying filters.
///
/// Optimizations over a naive read-all approach:
/// - **Date-based file skipping**: log files are named with dates
///   (e.g. `st0x-hedge.log.2026-04-27`); files outside the `since`/`until`
///   window are skipped entirely.
/// - **Pre-JSON string filtering**: level and search filters are applied on
///   the raw line before the expensive `serde_json::from_str` parse.
/// - **Streaming line reads**: files are read line-by-line via `BufReader`
///   instead of loading the entire file into memory.
/// - **Early termination**: once enough entries have been collected past the
///   requested page, remaining files are skipped.
fn read_matching_entries(
    log_dir: &str,
    filter: &LogFilter,
    offset: usize,
    limit: usize,
) -> (Vec<serde_json::Value>, usize, bool) {
    use std::io::BufRead;

    let Ok(dir) = std::fs::read_dir(log_dir) else {
        return (Vec::new(), 0, false);
    };

    let mut log_files: Vec<_> = dir
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("st0x-hedge.log"))
        })
        .collect();

    // Sort by name then reverse so newest files are read first.
    log_files.sort_by_key(std::fs::DirEntry::file_name);
    log_files.reverse();

    // Pre-compute level filter strings for fast raw-line matching.
    // JSON format: `"level":"INFO"` — we match this substring directly.
    let level_needles: Option<Vec<String>> = filter.levels.as_ref().map(|levels| {
        levels
            .iter()
            .map(|lvl| format!("\"level\":\"{lvl}\""))
            .collect()
    });

    let target_needles: Option<Vec<String>> = filter.targets.as_ref().map(|targets| {
        targets
            .iter()
            .map(|tgt| format!("\"target\":\"{tgt}\""))
            .collect()
    });

    let page_end = offset + limit;
    let mut total: usize = 0;
    let mut page_entries: Vec<serde_json::Value> = Vec::new();

    for file_entry in &log_files {
        // Date-based file skipping: the date suffix (e.g. "2026-04-27")
        // lets us skip entire files outside the time window.
        if let Some(since) = filter.since
            && let Some(file_date) = extract_log_file_date(file_entry)
        {
            let file_end_of_day = file_date
                .and_hms_opt(23, 59, 59)
                .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));

            if let Some(eod) = file_end_of_day
                && eod < since
            {
                continue;
            }
        }

        if let Some(until) = filter.until
            && let Some(file_date) = extract_log_file_date(file_entry)
        {
            let file_start_of_day = file_date
                .and_hms_opt(0, 0, 0)
                .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));

            if let Some(sod) = file_start_of_day
                && sod > until
            {
                continue;
            }
        }

        let Ok(file) = std::fs::File::open(file_entry.path()) else {
            continue;
        };
        let reader = std::io::BufReader::new(file);

        // Read lines into a vec for this file so we can reverse (newest last
        // in file -> newest first for display). We only collect lines that
        // pass the cheap string-level filters.
        let mut file_matches: Vec<serde_json::Value> = Vec::new();

        for line_result in reader.lines() {
            let Ok(line) = line_result else {
                continue;
            };

            // Fast string-level filtering BEFORE JSON parsing.
            if let Some(needles) = &level_needles
                && !needles.iter().any(|needle| line.contains(needle))
            {
                continue;
            }

            if let Some(needles) = &target_needles
                && !needles.iter().any(|needle| line.contains(needle))
            {
                continue;
            }

            if let Some(query) = &filter.search_lower
                && !line.to_lowercase().contains(query.as_str())
            {
                continue;
            }

            // Expensive: parse JSON only for lines that passed string filters.
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };

            // Time-range filter requires parsed timestamp.
            if (filter.since.is_some() || filter.until.is_some())
                && let Some(ts_str) = value["timestamp"].as_str()
                && let Ok(ts) = DateTime::parse_from_rfc3339(ts_str)
            {
                let ts_utc = ts.with_timezone(&Utc);

                if let Some(since) = filter.since
                    && ts_utc < since
                {
                    continue;
                }

                if let Some(until) = filter.until
                    && ts_utc > until
                {
                    continue;
                }
            }

            file_matches.push(value);
        }

        file_matches.reverse();

        for entry in file_matches {
            if total >= offset && page_entries.len() < limit {
                page_entries.push(entry);
            }

            total += 1;
        }

        // Early termination: if we've filled the page and have at least one
        // extra entry (to know has_more), skip remaining files.
        if page_entries.len() >= limit && total > page_end {
            break;
        }
    }

    let has_more = total > page_end;

    (page_entries, total, has_more)
}

/// Extracts the date from a log filename like `st0x-hedge.log.2026-04-27`.
fn extract_log_file_date(entry: &std::fs::DirEntry) -> Option<chrono::NaiveDate> {
    let name = entry.file_name();
    let name_str = name.to_str()?;
    let date_suffix = name_str.strip_prefix("st0x-hedge.log.")?;
    chrono::NaiveDate::parse_from_str(date_suffix, "%Y-%m-%d").ok()
}

/// Returns non-terminal offchain orders (Pending, Submitted, PartiallyFilled, Cancelling).
async fn pending_orders(State(state): State<AppState>) -> Json<Vec<PendingOrderResponse>> {
    let rows: Vec<(String, String, String)> = match sqlx::query_as(
        "SELECT view_id, status, payload FROM offchain_order_view \
         WHERE status IN ('Pending', 'Submitted', 'PartiallyFilled', 'Cancelling') \
         ORDER BY rowid DESC LIMIT 100",
    )
    .fetch_all(&state.pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(target: "dashboard", %error, "Failed to load pending orders");
            return Json(Vec::new());
        }
    };

    let orders = rows
        .into_iter()
        .filter_map(|(view_id, status, payload_str)| {
            parse_pending_order(view_id, status, &payload_str)
        })
        .collect();

    Json(orders)
}

fn parse_pending_order(
    view_id: String,
    status: String,
    payload_str: &str,
) -> Option<PendingOrderResponse> {
    let payload: serde_json::Value = serde_json::from_str(payload_str).ok()?;
    let inner = payload.get("Live")?.get(&status)?;

    Some(PendingOrderResponse {
        view_id,
        symbol: inner["symbol"].as_str()?.to_string(),
        direction: inner["direction"].as_str()?.to_string(),
        shares: inner["shares"].as_str().unwrap_or("0").to_string(),
        executor: inner["executor"].as_str().unwrap_or("unknown").to_string(),
        placed_at: inner["placed_at"].as_str().unwrap_or("").to_string(),
        submitted_at: inner["submitted_at"].as_str().map(String::from),
        shares_filled: inner["shares_filled"].as_str().map(String::from),
        avg_price: inner["avg_price"].as_str().map(String::from),
        status,
    })
}

#[derive(Deserialize, Default)]
struct TradesQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    symbol: Option<String>,
    venue: Option<String>,
    since: Option<String>,
    until: Option<String>,
    #[serde(default)]
    trade_protocol: TradeProtocol,
}

/// Paginated trade history from both onchain and offchain fills.
///
/// Returns newest-first. Supports filtering by symbol, venue, and time range.
async fn trades(
    State(state): State<AppState>,
    Query(query): Query<TradesQuery>,
) -> Result<Json<TradeResponse>, StatusCode> {
    let limit = query.limit.unwrap_or(100).min(500);
    let offset = query.offset.unwrap_or(0);

    let since_dt = parse_trade_filter_time(query.since.as_deref(), "since")?;
    let until_dt = parse_trade_filter_time(query.until.as_deref(), "until")?;
    let venues = parse_trade_venues(query.venue.as_deref())?;
    let symbols = parse_trade_symbols(query.symbol.as_deref())?;

    let page = query_trades(
        &state.pool,
        &TradeQuery {
            symbols,
            venues,
            since: since_dt,
            until: until_dt,
            trade_protocol: query.trade_protocol,
            limit,
            offset,
        },
    )
    .await
    .inspect_err(|error| warn!(target: "dashboard", ?error, "Failed to load trade history"))
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let TradePage {
        trades,
        total,
        has_more,
    } = page;
    let entries = trades
        .iter()
        .map(|trade| query.trade_protocol.serialize_trade(trade))
        .collect::<Result<Vec<_>, _>>()
        .inspect_err(
            |error| warn!(target: "dashboard", ?error, "Failed to serialize trade history"),
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(TradeResponse {
        entries,
        total,
        has_more,
    }))
}

fn parse_trade_filter_time(
    value: Option<&str>,
    parameter: &'static str,
) -> Result<Option<DateTime<Utc>>, StatusCode> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| {
            DateTime::parse_from_rfc3339(value)
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .inspect_err(|error| {
                    warn!(target: "dashboard", %error, %parameter, %value, "Invalid trade-history timestamp filter");
                })
                .map_err(|_| StatusCode::BAD_REQUEST)
        })
        .transpose()
}

fn parse_trade_venues(value: Option<&str>) -> Result<Option<Vec<TradingVenue>>, StatusCode> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .map(TradingVenue::from_str)
                .collect::<Result<Vec<_>, _>>()
                .inspect_err(|error| {
                    warn!(target: "dashboard", %error, %value, "Invalid trade-history venue filter");
                })
                .map_err(|_| StatusCode::BAD_REQUEST)
        })
        .transpose()
}

fn parse_trade_symbols(value: Option<&str>) -> Result<Option<Vec<Symbol>>, StatusCode> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .map(Symbol::from_str)
                .collect::<Result<Vec<_>, _>>()
                .inspect_err(|error| {
                    warn!(target: "dashboard", %error, %value, "Invalid trade-history symbol filter");
                })
                .map_err(|_| StatusCode::BAD_REQUEST)
        })
        .transpose()
}

#[derive(Deserialize, Default)]
struct TransfersQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    kind: Option<String>,
    since: Option<String>,
    until: Option<String>,
}

/// Paginated transfer history using event-sourced aggregate replay.
///
/// Replays transfer aggregates to produce proper DTO statuses, then
/// applies time-range filtering and pagination.
async fn transfers_endpoint(
    State(state): State<AppState>,
    Query(query): Query<TransfersQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let limit = query.limit.unwrap_or(100).min(500);
    let offset = query.offset.unwrap_or(0);

    let since_dt = query.since.as_deref().and_then(|val| {
        DateTime::parse_from_rfc3339(val)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });
    let until_dt = query.until.as_deref().and_then(|val| {
        DateTime::parse_from_rfc3339(val)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    let kind_filter = match query.kind.as_deref().filter(|val| !val.is_empty()) {
        Some(value) => Some(parse_transfer_kind_filter(value).map_err(|error| {
            tracing::warn!(target: "dashboard", %error, "Invalid transfer kind filter");
            StatusCode::BAD_REQUEST
        })?),
        None => None,
    };

    let loaded = crate::dashboard::transfer_loader::load_all_transfer_operations(
        &state.pool,
        kind_filter.as_deref(),
    )
    .await;

    let mut operations = loaded.operations;

    // Filter by time range
    if since_dt.is_some() || until_dt.is_some() {
        operations.retain(|op| {
            let started = op.started_at();

            if let Some(ref since) = since_dt
                && started < *since
            {
                return false;
            }

            if let Some(ref until) = until_dt
                && started > *until
            {
                return false;
            }

            true
        });
    }

    // Sort newest first
    operations.sort_by_key(|op| std::cmp::Reverse(op.started_at()));

    let filtered_total = operations.len();
    let start = offset.min(filtered_total);
    let end = filtered_total.min(offset + limit);
    let has_more = end < filtered_total;

    let entries: Vec<serde_json::Value> = operations[start..end]
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<_, _>>()
        .map_err(|error| {
            tracing::error!(
                target: "dashboard",
                %error,
                "Failed to serialize transfer operation"
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut response = serde_json::json!({
        "entries": entries,
        "total": filtered_total,
        "hasMore": has_more,
    });

    if !loaded.warnings.is_empty() {
        response["warnings"] =
            serde_json::to_value(&loaded.warnings).unwrap_or_else(|_| serde_json::json!([]));
    }

    Ok(Json(response))
}

/// Returns the full event history for a single transfer aggregate.
///
/// The frontend uses this to populate the detail modal with tx hashes,
/// IDs, failure reasons, and other debugging context.
async fn transfer_events(
    State(state): State<AppState>,
    Path((kind_str, aggregate_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let kind = TransferKind::from_str(&kind_str).map_err(|_| StatusCode::NOT_FOUND)?;
    let aggregate_type = kind.aggregate_type();

    let rows: Vec<(String, String, i64)> = match sqlx::query_as(
        "SELECT event_type, payload, sequence \
         FROM events \
         WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
         ORDER BY sequence ASC",
    )
    .bind(aggregate_type)
    .bind(&aggregate_id)
    .fetch_all(&state.pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(
                target: "dashboard",
                %error,
                %kind,
                %aggregate_id,
                "Failed to load transfer event history"
            );
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let stuck = stuck_transfer_info(kind, &rows);

    let events: Vec<serde_json::Value> = rows
        .iter()
        .map(|(event_type, payload, sequence)| {
            let step = event_step(event_type);
            let inner = event_payload_inner(step, payload, *sequence);

            serde_json::json!({
                "step": step,
                "sequence": sequence,
                "payload": inner,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "events": events,
        "stuck": stuck,
    })))
}

fn event_step(event_type: &str) -> &str {
    event_type.split("::").last().unwrap_or("Unknown")
}

fn event_payload_inner(step: &str, payload: &str, sequence: i64) -> serde_json::Value {
    match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(parsed) => parsed
            .as_object()
            .and_then(|obj| obj.get(step).cloned())
            .unwrap_or(parsed),

        Err(error) => {
            warn!(
                target: "dashboard",
                %error,
                step,
                sequence,
                "Failed to parse event payload for display"
            );

            serde_json::json!({
                "parseError": error.to_string(),
                "sequence": sequence,
            })
        }
    }
}

fn stuck_transfer_info(
    kind: TransferKind,
    rows: &[(String, String, i64)],
) -> Option<StuckTransferInfo> {
    match kind {
        TransferKind::EquityMint => stuck_mint_info(rows),
        TransferKind::EquityRedemption => stuck_redemption_info(rows),
        TransferKind::UsdcBridge => None,
    }
}

/// Parses each persisted event payload into the typed mint event so the
/// terminal-failure classification is checked exhaustively by the compiler:
/// renaming or adding a variant forces this match to be updated, unlike a
/// match on raw event-name strings.
fn stuck_mint_info(rows: &[(String, String, i64)]) -> Option<StuckTransferInfo> {
    use TokenizedEquityMintEvent::*;

    let mut quantity = None;
    let mut accepted = false;
    let mut terminal = None;

    for (event_type, payload, sequence) in rows {
        let Some(event) = parse_event::<TokenizedEquityMintEvent>(event_type, payload, *sequence)
        else {
            continue;
        };

        match event {
            MintRequested {
                quantity: requested,
                ..
            } => quantity = Some(FractionalShares::new(requested).to_string()),

            MintAccepted { .. } => accepted = true,

            MintAcceptanceFailed { .. } if accepted => {
                terminal = Some((StuckLocation::Issuer, StuckReason::MintAcceptanceFailed));
            }

            WrappingFailed { .. } => {
                terminal = Some((
                    StuckLocation::BotWalletUnwrapped,
                    StuckReason::WrappingFailed,
                ));
            }

            RaindexDepositFailed { .. } => {
                terminal = Some((
                    StuckLocation::BotWalletWrapped,
                    StuckReason::RaindexDepositFailed,
                ));
            }

            // Terminal "not stuck" states with no further events to scan: a
            // rejected mint never left the issuer, a deposited mint completed
            // successfully, and an operator-reconciled mint was resolved
            // out-of-band (drives to the `Reconciled` terminal).
            MintRejected { .. } | DepositedIntoRaindex { .. } | OperatorReconciled { .. } => {
                return None;
            }
            // ProviderCompletionRecovered un-failed the mint and put it back on
            // the success path, so any stuck state observed before it no longer
            // applies; the mint is re-derived from subsequent events (it may fail
            // again), hence a soft reset rather than an early return.
            ProviderCompletionRecovered { .. } => terminal = None,
            MintAcceptanceFailed { .. }
            | TokensReceived { .. }
            | WrapSubmitted { .. }
            | TokensWrapped { .. }
            | VaultDepositSubmitted { .. }
            | MintAuthorizationSigned { .. }
            | MintAuthorizationDelivered { .. } => {}
        }
    }

    let (location, reason) = terminal?;
    let amount = quantity?;

    Some(StuckTransferInfo {
        amount,
        location,
        reason,
    })
}

fn stuck_redemption_info(rows: &[(String, String, i64)]) -> Option<StuckTransferInfo> {
    use EquityRedemptionEvent::*;

    let mut requested_quantity: Option<String> = None;
    let mut withdrawn_amount: Option<U256> = None;
    let mut unwrapped_quantity: Option<String> = None;
    let mut sent = false;
    let mut terminal = None;

    for (event_type, payload, sequence) in rows {
        let Some(event) = parse_event::<EquityRedemptionEvent>(event_type, payload, *sequence)
        else {
            continue;
        };

        match event {
            VaultWithdrawPending { quantity, .. } | VaultWithdrawSubmitted { quantity, .. } => {
                requested_quantity = requested_quantity
                    .or_else(|| Some(FractionalShares::new(quantity).to_string()));
            }

            WithdrawnFromRaindex {
                quantity,
                wrapped_amount,
                actual_wrapped_amount,
                ..
            } => {
                requested_quantity = requested_quantity
                    .or_else(|| Some(FractionalShares::new(quantity).to_string()));
                withdrawn_amount = Some(actual_wrapped_amount.unwrap_or(wrapped_amount));
            }

            TokensUnwrapped {
                quantity,
                unwrapped_amount,
                ..
            } => {
                // Prefer the recorded share quantity; when absent (older events),
                // fall back to the actual unwrapped underlying amount -- the tokens
                // physically stranded in the redemption wallet -- rather than the
                // wrapped withdrawn amount, which can differ on a non-1:1 ratio.
                unwrapped_quantity = quantity
                    .map(|qty| FractionalShares::new(qty).to_string())
                    .or_else(|| shares_from_u256_18_decimal(unwrapped_amount));
            }

            TokensSent { .. } => sent = true,

            DetectionFailed { .. } if sent => {
                terminal = Some((
                    StuckLocation::RedemptionWallet,
                    StuckReason::DetectionFailed,
                ));
            }

            RedemptionRejected { .. } if sent => {
                terminal = Some((
                    StuckLocation::RedemptionWallet,
                    StuckReason::RedemptionRejected,
                ));
            }

            // A terminal success (including recovery, which evolves straight to
            // Completed) or an operator reconciliation means nothing is stranded.
            TransferFailed { .. }
            | Completed { .. }
            | ProviderCompletionRecovered { .. }
            | OperatorReconciled { .. } => {
                return None;
            }

            DetectionFailed { .. }
            | RedemptionRejected { .. }
            | UnwrapPending { .. }
            | UnwrapSubmitted { .. }
            | SendPending { .. }
            | Detected { .. } => {}
        }
    }

    let (location, reason) = terminal?;
    let amount = unwrapped_quantity
        .or_else(|| withdrawn_amount.and_then(shares_from_u256_18_decimal))
        .or(requested_quantity)?;

    Some(StuckTransferInfo {
        amount,
        location,
        reason,
    })
}

/// Deserializes a persisted event row into its typed event, logging and
/// skipping on failure. Persisted events are always well-formed (written by
/// the framework), so a failure here signals a schema mismatch worth surfacing
/// rather than silently dropping stuck detection.
fn parse_event<Event: serde::de::DeserializeOwned>(
    event_type: &str,
    payload: &str,
    sequence: i64,
) -> Option<Event> {
    serde_json::from_str(payload)
        .inspect_err(|error| {
            tracing::warn!(
                target: "dashboard",
                %error,
                %event_type,
                sequence,
                "Failed to parse event for stuck detection"
            );
        })
        .ok()
}

fn shares_from_u256_18_decimal(amount: U256) -> Option<String> {
    FractionalShares::from_u256_18_decimals(amount)
        .ok()
        .map(|shares| shares.to_string())
}

/// Returns the full event history for a single trade aggregate.
///
/// For onchain trades (direct Raindex or adapter-routed), returns Filled +
/// optional Enriched events.
/// For offchain trades (Alpaca), returns the full order lifecycle
/// (Placed -> Submitted -> PartiallyFilled -> Filled/Failed).
async fn trade_events(
    State(state): State<AppState>,
    Path((venue_str, aggregate_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let venue = TradingVenue::from_str(&venue_str).map_err(|_| StatusCode::NOT_FOUND)?;
    let aggregate_type = if venue.is_onchain() {
        "OnChainTrade"
    } else {
        "OffchainOrder"
    };

    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT event_type, payload, sequence \
         FROM events \
         WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
         ORDER BY sequence ASC",
    )
    .bind(aggregate_type)
    .bind(&aggregate_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|error| {
        warn!(
            target: "dashboard",
            %error,
            venue = %venue,
            %aggregate_id,
            "Failed to load trade event history"
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let events: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(event_type, payload, sequence)| {
            let step = event_step(&event_type);
            let inner = event_payload_inner(step, &payload, sequence);

            serde_json::json!({
                "step": step,
                "sequence": sequence,
                "payload": inner,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "events": events })))
}

fn unavailable_json(reason: &str) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "unavailable": true,
        "reason": reason,
    }))
}

#[derive(Deserialize, Default)]
struct RaindexOrdersQuery {
    page: Option<u32>,
    page_size: Option<u32>,
}

/// Proxies the bot's active Raindex orders from the st0x REST API.
/// When `[rest_api]` is not configured, returns an unavailable indicator
/// so the dashboard can show a friendly message instead of an error.
#[allow(clippy::cognitive_complexity)]
async fn raindex_orders(
    State(state): State<AppState>,
    Query(query): Query<RaindexOrdersQuery>,
) -> Json<serde_json::Value> {
    let RaindexOrdersQuery { page, page_size } = query;
    let Some(rest_api) = &state.ctx.rest_api else {
        return unavailable_json("REST API not configured (simulate mode)");
    };

    let owner = state.ctx.vault_owner();
    let url = format!(
        "{}/v1/orders/owner/{:#x}",
        rest_api.url.trim_end_matches('/'),
        owner
    );

    let page = page.unwrap_or(1).max(1);
    let page_size = page_size
        .unwrap_or(DEFAULT_RAINDEX_ORDERS_PAGE_SIZE)
        .clamp(1, MAX_RAINDEX_ORDERS_PAGE_SIZE);

    let mut request = rest_api
        .http_client
        .get(&url)
        .query(&[("page", page), ("pageSize", page_size)]);

    if let (Some(key_id), Some(key_secret)) = (&rest_api.key_id, &rest_api.key_secret) {
        request = request.basic_auth(key_id, Some(key_secret));
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(target: "dashboard", %error, %url, "Failed to reach st0x REST API");
            return unavailable_json("REST API unreachable");
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        tracing::warn!(target: "dashboard", %status, %url, "st0x REST API returned error");
        return unavailable_json("REST API returned an error");
    }

    match response.text().await {
        Ok(body) => match serde_json::from_str(&body) {
            Ok(value) => Json(value),
            Err(error) => {
                tracing::warn!(target: "dashboard", %error, "st0x REST API returned non-JSON body");
                unavailable_json("REST API returned non-JSON")
            }
        },
        Err(error) => {
            tracing::warn!(target: "dashboard", %error, "Failed to read st0x REST API response body");
            unavailable_json("Failed to read REST API response")
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// Shared handle for resuming interrupted tokenization transfers and
/// re-checking failed ones at runtime, set by the conductor after startup
/// completes.
pub(crate) struct RecoveryHandle {
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
    /// Needed by `recheck` recovery to rebuild in-memory tracking before the
    /// recovery event is dispatched, so the reactor applies its inventory
    /// effect on the live bot.
    pub(crate) rebalancing_service: Arc<RebalancingService>,
}

/// Prevents concurrent `/transfers/resume` requests from racing through
/// duplicate mint/redemption recovery flows.
pub(crate) struct ResumeLock(pub(crate) Mutex<()>);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InterruptedTransfersResponse {
    interrupted_mints: Vec<String>,
    interrupted_redemptions: Vec<String>,
}

async fn interrupted_transfers(
    State(state): State<AppState>,
) -> Result<
    (
        [(HeaderName, &'static str); 1],
        Json<InterruptedTransfersResponse>,
    ),
    (StatusCode, Json<ErrorResponse>),
> {
    let mints = crate::tokenized_equity_mint::interrupted_mint_ids(&state.pool)
        .await
        .map_err(|error| {
            error!(?error, "Failed to query interrupted mints");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to query interrupted mints".to_string(),
                }),
            )
        })?;

    let redemptions = crate::equity_redemption::interrupted_redemption_ids(&state.pool)
        .await
        .map_err(|error| {
            error!(?error, "Failed to query interrupted redemptions");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to query interrupted redemptions".to_string(),
                }),
            )
        })?;

    Ok((
        [(CACHE_CONTROL, "no-store")],
        Json(InterruptedTransfersResponse {
            interrupted_mints: mints.into_iter().map(|id| id.to_string()).collect(),
            interrupted_redemptions: redemptions.into_iter().map(|id| id.to_string()).collect(),
        }),
    ))
}

/// Wire contract for `/transfers/resume`, shared with the CLI wrapper so the
/// two cannot drift.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResumeResponse {
    pub(crate) mints_attempted: usize,
    pub(crate) mints_failed: usize,
    pub(crate) redemptions_attempted: usize,
    pub(crate) redemptions_failed: usize,
}

async fn resume_transfers(
    State(state): State<AppState>,
) -> Result<Json<ResumeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let _guard = state.resume_lock.0.try_lock().map_err(|_| {
        (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "A resume operation is already in progress".to_string(),
            }),
        )
    })?;

    let handle = state.recovery.get().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Recovery not ready yet (conductor still starting)".to_string(),
            }),
        )
    })?;

    let mints = crate::tokenized_equity_mint::interrupted_mint_ids(&state.pool)
        .await
        .map_err(|error| {
            error!(?error, "Failed to query interrupted mints");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to query interrupted mints".to_string(),
                }),
            )
        })?;

    let redemptions = crate::equity_redemption::interrupted_redemption_ids(&state.pool)
        .await
        .map_err(|error| {
            error!(?error, "Failed to query interrupted redemptions");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to query interrupted redemptions".to_string(),
                }),
            )
        })?;

    let mints_attempted = mints.len();
    let redemptions_attempted = redemptions.len();
    let mut mints_failed = 0usize;
    let mut redemptions_failed = 0usize;

    for mint_id in &mints {
        if let Err(error) = handle.transfer.resume_mint(mint_id).await {
            error!(%mint_id, ?error, "Failed to resume mint");
            mints_failed += 1;
        }
    }

    for redemption_id in &redemptions {
        if let Err(error) = handle.transfer.resume_redemption(redemption_id).await {
            error!(%redemption_id, ?error, "Failed to resume redemption");
            redemptions_failed += 1;
        }
    }

    info!(
        mints_attempted,
        mints_failed,
        redemptions_attempted,
        redemptions_failed,
        "Transfer recovery completed via API"
    );

    Ok(Json(ResumeResponse {
        mints_attempted,
        mints_failed,
        redemptions_attempted,
        redemptions_failed,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RecheckResponse {
    outcome: RecheckOutcome,
}

/// Re-checks a single failed (or active) transfer against the tokenization
/// provider, recovering it in-process so the live inventory view is corrected.
///
/// Runs inside the bot so the recovery event dispatches through the
/// reactor-wired store; shares `resume_lock` with `/transfers/resume` so the
/// two cannot race onchain wraps.
///
/// Auth: reachable on two mounts with two different gates. The bare mount is
/// loopback-only (`require_loopback`) -- the in-container `st0x-cli` path --
/// and the `/liquidity-write` mount is IAP-verified for operators coming
/// through the load balancer. No unauthenticated network route to the
/// mutation endpoints remains.
async fn recheck_transfer(
    State(state): State<AppState>,
    Path((kind_str, id)): Path<(String, String)>,
) -> Result<Json<RecheckResponse>, (StatusCode, Json<ErrorResponse>)> {
    let kind = TransferKind::from_str(&kind_str).map_err(|error| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Unknown transfer kind: {error}"),
            }),
        )
    })?;

    let _guard = state.resume_lock.0.try_lock().map_err(|_| {
        (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "A resume or recheck operation is already in progress".to_string(),
            }),
        )
    })?;

    let handle = state.recovery.get().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Recovery not ready yet (conductor still starting)".to_string(),
            }),
        )
    })?;

    let outcome = match kind {
        TransferKind::EquityMint => {
            let mint_id: IssuerRequestId = id.parse().map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("Invalid mint ID: {error}"),
                    }),
                )
            })?;

            handle
                .transfer
                .recover_mint(&mint_id, &state.pool, &handle.rebalancing_service)
                .await
        }

        TransferKind::EquityRedemption => {
            let redemption_id = id.parse::<RedemptionAggregateId>().map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("Invalid redemption ID: {error}"),
                    }),
                )
            })?;

            handle
                .transfer
                .recover_redemption(&redemption_id, &handle.rebalancing_service)
                .await
        }

        TransferKind::UsdcBridge => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "recheck is not supported for USDC bridges".to_string(),
                }),
            ));
        }
    }
    .map_err(|error| {
        error!(?error, %id, "Failed to recheck transfer");
        let (status, message) = recheck_error_response(&error);
        (status, Json(ErrorResponse { error: message }))
    })?;

    info!(%id, ?kind, ?outcome, "Transfer recheck completed via API");

    Ok(Json(RecheckResponse { outcome }))
}

/// Maps a [`RecheckError`] to an HTTP status and operator-facing message.
///
/// Distinguishes not-recoverable conditions (the persisted aggregate state
/// does not permit provider-completion recovery -- retrying will not help) and
/// transient upstream failures (the provider was unreachable -- retry later)
/// from genuinely internal failures, so the operator running `transfer recheck`
/// during an incident learns whether to retry without reading bot logs. Bodies
/// for the not-recoverable variants carry the typed error's message, which only
/// references aggregate/request ids; internal failures stay generic so they do
/// not leak internals (the full error is logged at the call site).
fn recheck_error_response(error: &RecheckError) -> (StatusCode, String) {
    use RecheckError::{
        Database, MalformedTokenizationRequestId, MalformedWallet, Mint, MissingTxHash,
        NoAcceptedRequest, Rebalancing, Redemption, Tokenizer,
    };

    match error {
        NoAcceptedRequest(_)
        | MissingTxHash(_)
        | MalformedWallet { .. }
        | MalformedTokenizationRequestId { .. } => {
            (StatusCode::UNPROCESSABLE_ENTITY, error.to_string())
        }
        Tokenizer(_) => (
            StatusCode::BAD_GATEWAY,
            "Tokenization provider unavailable; retry later".to_string(),
        ),
        Mint(_) | Redemption(_) | Rebalancing(_) | Database(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to recheck transfer".to_string(),
        ),
    }
}

/// Date-range query window shared by the `/performance/*` endpoints.
/// Defaults to the last 7 days ending now.
#[derive(Debug, Deserialize)]
struct PerformanceRangeQuery {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

/// Widest span accepted by `/performance/*` range queries. Generous for any
/// real dashboard preset (the widest, "ALL", spans a bit over a year today)
/// while rejecting a pathological `from`/`to` that would otherwise make
/// `hedge_latency_report`'s dense bucket generation iterate unboundedly.
const MAX_PERFORMANCE_RANGE_DAYS: i64 = 3650;

fn parse_report_range(query: &PerformanceRangeQuery) -> Result<ReportRange, StatusCode> {
    let to = query.to.unwrap_or_else(Utc::now);
    let from = query.from.unwrap_or(to - chrono::Duration::days(7));
    if from >= to || to - from > chrono::Duration::days(MAX_PERFORMANCE_RANGE_DAYS) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(ReportRange { from, to })
}

async fn performance_latencies(
    State(state): State<AppState>,
    Query(query): Query<PerformanceRangeQuery>,
) -> Result<Json<HedgeLatencies>, StatusCode> {
    let report_range = parse_report_range(&query)?;
    let performances = load_hedge_performance(&state.pool, &report_range)
        .await
        .inspect_err(|error| error!(%error, "Failed to load hedge performance"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(hedge_latency_report(&performances, &report_range)))
}

async fn performance_rebalances(
    State(state): State<AppState>,
    Query(query): Query<PerformanceRangeQuery>,
) -> Result<Json<RebalanceTimings>, StatusCode> {
    let range = parse_report_range(&query)?;

    let timings = load_rebalance_timings(&state.pool, &range)
        .await
        .inspect_err(|error| error!(%error, "Failed to load rebalance timings"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(timings))
}

async fn performance_equity_rebalances(
    State(state): State<AppState>,
    Query(query): Query<PerformanceRangeQuery>,
) -> Result<Json<EquityTimings>, StatusCode> {
    let range = parse_report_range(&query)?;

    let timings = load_equity_timings(&state.pool, &range)
        .await
        .inspect_err(|error| error!(%error, "Failed to load equity timings"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(timings))
}

async fn performance_reliability(
    State(state): State<AppState>,
    Query(query): Query<PerformanceRangeQuery>,
) -> Result<Json<ReliabilityReport>, StatusCode> {
    let range = parse_report_range(&query)?;

    let (entries, log_entries_truncated) = if let Some(log_dir) = state.ctx.log_dir.as_deref() {
        let log_dir = log_dir.to_owned();
        let filter = LogFilter {
            search_lower: None,
            levels: Some(vec!["ERROR".to_string(), "WARN".to_string()]),
            targets: None,
            since: Some(range.from),
            until: Some(range.to),
        };
        // Log scanning is synchronous file I/O; keep it off the async
        // workers. The cap bounds memory during error storms, slightly
        // undercounting the noisiest windows rather than exhausting the
        // dashboard process.
        let (entries, total, _) = tokio::task::spawn_blocking(move || {
            read_matching_entries(&log_dir, &filter, 0, MAX_RELIABILITY_LOG_ENTRIES)
        })
        .await
        .inspect_err(|error| error!(%error, "Log aggregation task failed"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let truncated = total > MAX_RELIABILITY_LOG_ENTRIES;
        if truncated {
            warn!(
                total,
                cap = MAX_RELIABILITY_LOG_ENTRIES,
                "Reliability log aggregation truncated by entry cap"
            );
        }
        (entries, truncated)
    } else {
        (Vec::new(), false)
    };
    let (log_buckets, log_targets) = aggregate_log_entries(&entries, &range);

    let failure_events = load_failure_events(&state.pool, &range)
        .await
        .inspect_err(|error| error!(%error, "Failed to load failure events"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let job_queues = load_job_queue_health(&state.pool)
        .await
        .inspect_err(|error| error!(%error, "Failed to load job queue health"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ReliabilityReport {
        log_buckets,
        log_targets,
        failure_events,
        job_queues,
        log_entries_truncated,
    }))
}

async fn performance_infra(
    State(state): State<AppState>,
    Query(query): Query<PerformanceRangeQuery>,
) -> Result<Json<InfraReport>, StatusCode> {
    let range = parse_report_range(&query)?;
    // The two loaders hit independent tables, so run them concurrently rather
    // than paying both round-trips in series.
    let (monitor, dependencies) = tokio::try_join!(
        async {
            load_monitor_telemetry(
                &state.pool,
                &range,
                state.ctx.chains.sole_trading().orderbook,
            )
            .await
            .inspect_err(|error| error!(%error, "Failed to load monitor telemetry"))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        },
        async {
            load_dependency_stats(&state.pool, &range)
                .await
                .inspect_err(|error| error!(%error, "Failed to load dependency stats"))
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        },
    )?;

    Ok(Json(InfraReport {
        monitor,
        dependencies,
    }))
}

/// The role-gated ops API: the same handlers the dashboard routes use, mounted
/// under a prefix the load balancer routes to a role-specific IAP backend.
///
/// The prefix is the role. The URL map sends `/liquidity-read/*` and
/// `/liquidity-write/*` to different backend services, each with its own IAP
/// policy bound to a Workspace group, so who may call which prefix is group
/// membership. The middleware here re-checks the assertion and pins the
/// audience per prefix, which is what stops a read-tier token being replayed
/// against the write path and what refuses a caller who reached the VM from
/// inside the VPC without passing IAP at all.
///
/// Returns nothing when no audiences are configured: a deployment with no load
/// balancer in front of it must not expose these paths, and unmounting them is
/// a stronger guarantee than serving a 401.
fn ops_api_routes(ops_api: Option<&OpsApiConfig>) -> Router<AppState> {
    let Some(ops_api) = ops_api else {
        return Router::new();
    };

    // One HTTP client shared by both verifiers (reqwest::Client clones are
    // Arc clones): both fetch the same Google JWKS document. The timeouts
    // are load-bearing, not hygiene -- the verifier's refresh slot is held
    // for the duration of a fetch, so an unbounded request during a Google
    // outage would pin it. 10s matches the kms_jwt convention.
    // Static timeouts on the default TLS backend -- build cannot fail here.
    #[allow(clippy::expect_used)]
    let jwks_http = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("static client config cannot fail");
    let read_verifier = Arc::new(IapVerifier::new(
        &ops_api.read_audience,
        "read",
        jwks_http.clone(),
    ));
    let write_verifier = Arc::new(IapVerifier::new(
        &ops_api.write_audience,
        "write",
        jwks_http,
    ));

    let read = Router::new()
        .route("/liquidity-read/health", get(health))
        .route("/liquidity-read/pnl", get(pnl))
        .route("/liquidity-read/logs", get(logs))
        .route("/liquidity-read/orders/pending", get(pending_orders))
        .route("/liquidity-read/orders/raindex", get(raindex_orders))
        .route("/liquidity-read/trades", get(trades))
        .route(
            "/liquidity-read/trades/{venue}/{aggregate_id}/events",
            get(trade_events),
        )
        .route("/liquidity-read/transfers", get(transfers_endpoint))
        .route(
            "/liquidity-read/transfers/{kind}/{aggregate_id}/events",
            get(transfer_events),
        )
        .route(
            "/liquidity-read/transfers/interrupted",
            get(interrupted_transfers),
        )
        .route(
            "/liquidity-read/performance/latencies",
            get(performance_latencies),
        )
        .route(
            "/liquidity-read/performance/rebalances",
            get(performance_rebalances),
        )
        .route(
            "/liquidity-read/performance/equity-rebalances",
            get(performance_equity_rebalances),
        )
        .route(
            "/liquidity-read/performance/reliability",
            get(performance_reliability),
        )
        .route("/liquidity-read/performance/infra", get(performance_infra))
        .layer(axum::middleware::from_fn(move |request, next| {
            let verifier = Arc::clone(&read_verifier);
            async move { require_iap(verifier, request, next).await }
        }));

    // `recheck` re-drives a transfer and completes it if the provider has
    // settled it, and `resume` re-drives EVERY interrupted transfer: both
    // move real state and belong to the narrower group. Resume must be here
    // because the bare mounts below are loopback-only -- without this mount,
    // bulk resume would have no network route at all and an incident with
    // SSH unavailable could not recover interrupted transfers.
    let write = Router::new()
        .route(
            "/liquidity-write/transfers/recheck/{kind}/{id}",
            post(recheck_transfer),
        )
        .route("/liquidity-write/transfers/resume", post(resume_transfers))
        .layer(axum::middleware::from_fn(move |request, next| {
            let verifier = Arc::clone(&write_verifier);
            async move { require_iap(verifier, request, next).await }
        }));

    read.merge(write)
}

/// Refuses any request whose TCP peer is not loopback.
///
/// The `/transfers/*` mutation endpoints exist on their bare paths for exactly
/// one caller: `st0x-cli` running inside the bot's own container (`docker
/// exec`), whose resume/recheck verbs delegate to the running server so
/// recovery dispatches through the in-process reactor. That caller connects to
/// 127.0.0.1 inside the container's network namespace. Anything arriving over
/// the published port -- the VPC, an IAP tunnel, the load balancer -- reaches
/// the container through its bridge interface and carries a non-loopback peer,
/// so it is refused here and must use the IAP-verified `/liquidity-write`
/// mount instead. A request with no recorded peer address is refused too:
/// fail closed rather than guess.
async fn require_loopback(
    request: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let peer_is_loopback = request
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .is_some_and(|info| info.0.ip().is_loopback());

    if !peer_is_loopback {
        warn!(
            target: "api",
            path = %request.uri().path(),
            "Refusing non-loopback caller on an operator-only mutation path"
        );
        // Same JSON error shape as the handlers this guard fronts, so ops
        // tooling parses one contract for the whole route family.
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "operator-only path: use the in-container CLI or the \
                        /liquidity-write mount"
                    .to_string(),
            }),
        ));
    }

    Ok(next.run(request).await)
}

pub(crate) fn routes(ops_api: Option<&OpsApiConfig>) -> Router<AppState> {
    // The operator socket: mutation endpoints for the in-container CLI only.
    // The IAP-gated `/liquidity-write` mount is the network route to the same
    // handlers.
    let loopback_only = Router::new()
        .route("/transfers/resume", post(resume_transfers))
        .route("/transfers/recheck/{kind}/{id}", post(recheck_transfer))
        .route_layer(axum::middleware::from_fn(require_loopback));

    Router::new()
        .merge(ops_api_routes(ops_api))
        .merge(loopback_only)
        .route("/health", get(health))
        .route("/performance/latencies", get(performance_latencies))
        .route("/performance/rebalances", get(performance_rebalances))
        .route(
            "/performance/equity-rebalances",
            get(performance_equity_rebalances),
        )
        .route("/performance/reliability", get(performance_reliability))
        .route("/performance/infra", get(performance_infra))
        .route("/pnl", get(pnl))
        .route("/logs", get(logs))
        .route("/orders/pending", get(pending_orders))
        .route("/trades", get(trades))
        .route("/trades/{venue}/{aggregate_id}/events", get(trade_events))
        .route("/transfers", get(transfers_endpoint))
        .route(
            "/transfers/{kind}/{aggregate_id}/events",
            get(transfer_events),
        )
        .route("/orders/raindex", get(raindex_orders))
        .route("/transfers/interrupted", get(interrupted_transfers))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use alloy::primitives::{Address, TxHash};
    use axum::body::{Body, to_bytes};
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use chrono::{NaiveDate, TimeZone};
    use chrono_tz::America::New_York;
    use httpmock::Method::GET;
    use sqlx::SqlitePool;
    use tokio::sync::broadcast;
    use tower::ServiceExt;
    use uuid::uuid;

    use st0x_config::{
        BrokerCtx, Ctx, ExecutionThreshold, RestApiCtx, create_test_ctx_with_order_owner,
    };
    use st0x_dto::TradeOutcome;
    use st0x_event_sorcery::{ReactorHarness, StoreBuilder};
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
        DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS, Direction, ExecutorOrderId, Positive,
        SupportedExecutor, Symbol, TimeInForce,
    };
    use st0x_finance::Usd;
    use st0x_float_macro::float;
    use st0x_tokenization::{
        MintVerificationError, TokenizerError, issuer_request_id, tokenization_request_id,
    };

    use super::*;
    use crate::dashboard;
    use crate::inventory::{
        self, BroadcastingInventory, PortfolioAsset, PortfolioBalanceRow, PortfolioLocation,
    };
    use crate::offchain::order::{OffchainOrder, OffchainOrderEvent, OffchainOrderId};
    use crate::onchain_trade::{
        InventoryVenue, OnChainTrade, OnChainTradeCommand, OnChainTradeId, OnChainTradeSource,
    };
    use crate::performance::equity_timing::EquityTimingProjection;
    use crate::performance::reliability::LifecycleFailureProjection;
    use crate::portfolio_snapshot::{
        PortfolioBalanceRowWithMark, PortfolioSnapshot, PortfolioSnapshotCommand,
        PortfolioSnapshotId, PortfolioSnapshotProjection, et_day,
    };
    use crate::position::{Position, PositionCommand, TradeId};
    use crate::tokenized_equity_mint::TokenizedEquityMint;

    async fn empty_app_state(ctx: Ctx) -> AppState {
        let (sender, _) = broadcast::channel(16);
        // Use the shared-cache test pool so the apalis `Jobs` table (set up by
        // `setup_test_db`) is visible to the reliability job-queue-health query.
        let pool = crate::test_utils::setup_test_db().await;

        AppState {
            settings: dashboard::settings_from_ctx(&ctx),
            ctx: ctx.clone(),
            pnl_ledger: Arc::new(crate::dashboard::pnl::PnlLedger::new(pool.clone())),
            pool,
            event_sender: sender.clone(),
            inventory: Arc::new(BroadcastingInventory::new(
                inventory::InventoryView::default(),
                sender,
            )),
            equity_prices: crate::dashboard::equity_price::EquityPriceStore::new(
                &ctx.chains.sole_trading().assets,
            ),
            recovery: Arc::new(tokio::sync::OnceCell::new()),
            resume_lock: Arc::new(ResumeLock(Mutex::new(()))),
            pnl_report_admission: crate::dashboard::pnl::pnl_report_admission(),
            metrics_handle: crate::metrics::setup().expect("metrics setup"),
            health: crate::startup::HealthGate::default(),
        }
    }

    #[tokio::test]
    async fn pnl_internal_failures_return_a_stable_generic_response() {
        let left = rain_math_float::Float::parse("1e2147483646".to_owned()).unwrap();
        let right = rain_math_float::Float::parse("1e2".to_owned()).unwrap();
        let arithmetic = PnlError::from((left * right).unwrap_err());

        let worker = tokio::spawn(std::future::pending::<()>());
        worker.abort();
        let worker = PnlError::ReplayWorker(worker.await.unwrap_err());

        for error in [arithmetic, worker] {
            let (status, body) = pnl_error_response(error);

            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(body, "Failed to build PnL report");
        }
    }

    #[tokio::test]
    async fn pnl_route_returns_a_stable_generic_response_for_report_failures() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        sqlx::query(
            "INSERT INTO portfolio_snapshot ( \
               et_day, captured_at, location, asset, available_balance, inflight_balance, \
               usd_mark, mark_captured_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("2026-05-15")
        .bind("2026-05-15T04:05:00+00:00")
        .bind("market_making")
        .bind("USDC")
        .bind("not-a-float")
        .bind("0")
        .bind("1")
        .bind("2026-05-15T04:05:00+00:00")
        .execute(&state.pool)
        .await
        .unwrap();

        let response = build_app(state)
            .oneshot(Request::builder().uri("/pnl").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body_to_string(response).await, "Failed to build PnL report");
    }

    #[tokio::test]
    async fn pnl_route_rejects_requests_when_report_capacity_is_exhausted() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        let _permits = (0..crate::dashboard::pnl::MAX_CONCURRENT_PNL_REPORTS)
            .map(|_| acquire_pnl_report_permit(&state.pnl_report_admission).unwrap())
            .collect::<Vec<_>>();

        let response = build_app(state)
            .oneshot(Request::builder().uri("/pnl").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body_to_string(response).await,
            "PnL report capacity exhausted"
        );
    }

    #[test]
    fn stuck_mint_info_reports_accepted_provider_failure() {
        let mint_id = issuer_request_id("mint-1");
        let mint_accepted_payload = format!(
            r#"{{"MintAccepted":{{"issuer_request_id":"{mint_id}","tokenization_request_id":"tok-1","accepted_at":"2026-01-01T00:00:01Z"}}}}"#
        );

        let rows = vec![
            event_row(
                "TokenizedEquityMintEvent::MintRequested",
                r#"{"MintRequested":{"symbol":"AAPL","quantity":"12.5","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "TokenizedEquityMintEvent::MintAccepted",
                &mint_accepted_payload,
                1,
            ),
            event_row(
                "TokenizedEquityMintEvent::MintAcceptanceFailed",
                r#"{"MintAcceptanceFailed":{"reason":"timeout","failed_at":"2026-01-01T00:01:00Z"}}"#,
                2,
            ),
        ];

        let stuck = stuck_transfer_info(TransferKind::EquityMint, &rows).expect("stuck amount");

        assert_eq!(stuck.amount, "12.5");
        assert_eq!(stuck.location, StuckLocation::Issuer);
        assert_eq!(stuck.reason, StuckReason::MintAcceptanceFailed);
    }

    #[test]
    fn parse_pending_order_handles_cancelling_status() {
        // The cancel-and-replace flow introduced the non-terminal `Cancelling`
        // state; the /orders/pending endpoint must surface it like any other
        // live order rather than dropping it on the floor.
        let payload = r#"{"Live":{"Cancelling":{"symbol":"AAPL","direction":"Sell","shares":"1.5","executor":"DryRun","placed_at":"2026-01-01T00:00:00Z","submitted_at":"2026-01-01T00:00:01Z","shares_filled":"0.5","avg_price":"195.25"}}}"#;

        let parsed = parse_pending_order("order-1".to_string(), "Cancelling".to_string(), payload)
            .expect("Cancelling order should parse");

        assert_eq!(parsed.view_id, "order-1");
        assert_eq!(parsed.status, "Cancelling");
        assert_eq!(parsed.symbol, "AAPL");
        assert_eq!(parsed.direction, "Sell");
        assert_eq!(parsed.shares, "1.5");
        assert_eq!(parsed.executor, "DryRun");
        assert_eq!(parsed.placed_at, "2026-01-01T00:00:00Z");
        assert_eq!(parsed.submitted_at.as_deref(), Some("2026-01-01T00:00:01Z"));
        assert_eq!(parsed.shares_filled.as_deref(), Some("0.5"));
        assert_eq!(parsed.avg_price.as_deref(), Some("195.25"));
    }

    #[tokio::test]
    async fn offchain_trade_history_includes_failed_orders() {
        let state = empty_app_state(create_test_ctx_with_order_owner(Address::ZERO)).await;
        let pool = state.pool.clone();
        let payload = r#"{"Live":{"Failed":{"symbol":"SPCX","shares":"1","direction":"Buy","executor":"AlpacaBrokerApi","retained_fill":{"Priced":{"shares_filled":"0.25","avg_price":"25","partially_filled_at":"2026-01-01T00:00:00Z"}},"executor_order_id":"broker-order","error":"asset is not tradable","placed_at":"2026-01-01T00:00:00Z","failed_at":"2026-01-01T00:00:01Z"}}}"#;
        let view_id = "00000000-0000-0000-0000-000000000141";
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind(view_id)
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap();
        let filled_view_id = "00000000-0000-0000-0000-000000000140";
        let filled_payload = r#"{"Live":{"Filled":{"symbol":"AAPL","shares":"2","direction":"Sell","executor":"AlpacaBrokerApi","executor_order_id":"broker-fill","price":"100","placed_at":"2026-01-01T00:00:00Z","submitted_at":"2026-01-01T00:00:00Z","filled_at":"2026-01-01T00:00:00.123456789Z"}}}"#;
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind(filled_view_id)
            .bind(filled_payload)
            .execute(&pool)
            .await
            .unwrap();
        let cancelled_view_id = "00000000-0000-0000-0000-000000000139";
        let cancelled_payload = r#"{"Live":{"Cancelled":{"symbol":"MSFT","shares":"1","requested_shares":"1.5","filled_shares":"0","direction":"Sell","executor":"AlpacaBrokerApi","executor_order_id":"broker-cancel","reason":"MarketOpenReplacement","placed_at":"2026-01-01T00:00:01Z","cancelled_at":"2026-01-01T00:00:02Z"}}}"#;
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind(cancelled_view_id)
            .bind(cancelled_payload)
            .execute(&pool)
            .await
            .unwrap();

        let entries = query_trades(
            &pool,
            &TradeQuery {
                venues: None,
                limit: usize::MAX,
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
        )
        .await
        .unwrap()
        .trades;

        assert_eq!(entries.len(), 3);
        let failed = entries
            .iter()
            .find(|trade| trade.id == view_id)
            .expect("failed trade should be loaded");
        assert_eq!(failed.symbol, Symbol::new("SPCX").unwrap());
        match &failed.outcome {
            TradeOutcome::Failed {
                error,
                accepted_shares,
                filled_shares,
                remaining_shares,
                excess_shares,
            } => {
                assert_eq!(error, "asset is not tradable");
                assert_eq!(accepted_shares, &None);
                assert!(
                    filled_shares
                        .unwrap()
                        .inner()
                        .inner()
                        .eq(st0x_float_macro::float!(0.25))
                        .unwrap()
                );
                assert_eq!(remaining_shares, &None);
                assert_eq!(excess_shares, &None);
            }
            TradeOutcome::Filled | TradeOutcome::Cancelled { .. } => {
                panic!("failed projection must remain failed")
            }
        }

        let legacy_response = build_app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/trades")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(legacy_response.status(), StatusCode::OK);
        let legacy_body: serde_json::Value =
            serde_json::from_str(&body_to_string(legacy_response).await).unwrap();
        assert_eq!(legacy_body["total"], 1);
        assert_eq!(legacy_body["entries"][0]["id"], filled_view_id);
        assert_eq!(
            legacy_body["entries"][0]["filledAt"],
            "2026-01-01T00:00:00.123456789Z"
        );
        assert_eq!(legacy_body["entries"][0]["outcome"]["status"], "filled");
        assert!(
            legacy_body["entries"]
                .as_array()
                .unwrap()
                .iter()
                .all(|trade| trade["id"] != view_id),
            "legacy clients must not receive failed trades"
        );

        let legacy_page = build_app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/trades?limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let legacy_page: serde_json::Value =
            serde_json::from_str(&body_to_string(legacy_page).await).unwrap();
        assert_eq!(legacy_page["total"], 1);
        assert_eq!(legacy_page["entries"][0]["id"], filled_view_id);
        assert_eq!(legacy_page["hasMore"], false);

        let v1_response = build_app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/trades?trade_protocol=terminal_outcomes_v1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(v1_response.status(), StatusCode::OK);
        let v1_body: serde_json::Value =
            serde_json::from_str(&body_to_string(v1_response).await).unwrap();
        assert_eq!(v1_body["total"], 2);
        assert!(
            v1_body["entries"]
                .as_array()
                .unwrap()
                .iter()
                .all(|trade| trade["id"] != cancelled_view_id),
            "v1 clients must not receive cancelled trades"
        );
        let v1_outcome = &v1_body["entries"][0]["outcome"];
        assert!(v1_outcome.get("acceptedShares").is_none());
        assert_eq!(v1_outcome["filledShares"], "0.25");
        assert_eq!(v1_outcome["remainingShares"], "0.75");
        assert_eq!(v1_outcome["excessShares"], "0");

        let v1_page = build_app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/trades?trade_protocol=terminal_outcomes_v1&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v1_page: serde_json::Value =
            serde_json::from_str(&body_to_string(v1_page).await).unwrap();
        assert_eq!(v1_page["total"], 2);
        assert_eq!(v1_page["entries"][0]["id"], view_id);
        assert_eq!(v1_page["hasMore"], true);

        let response = build_app(state)
            .oneshot(
                Request::builder()
                    .uri("/trades?trade_protocol=terminal_outcomes_v2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_str(&body_to_string(response).await).unwrap();
        assert_eq!(body["total"], 3);
        assert_eq!(body["entries"][0]["occurredAt"], "2026-01-01T00:00:02Z");
        let failed = body["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|trade| trade["id"] == view_id)
            .expect("v2 clients should receive failed trades");
        assert_eq!(failed["outcome"]["status"], "failed");
        assert_eq!(failed["outcome"]["error"], "asset is not tradable");
        assert_eq!(failed["outcome"]["acceptedShares"], serde_json::Value::Null);
        assert_eq!(failed["outcome"]["filledShares"], "0.25");
        assert_eq!(
            failed["outcome"]["remainingShares"],
            serde_json::Value::Null
        );
        assert_eq!(failed["outcome"]["excessShares"], serde_json::Value::Null);
        let cancelled = body["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|trade| trade["id"] == cancelled_view_id)
            .expect("v2 clients should receive cancelled trades");
        assert_eq!(cancelled["shares"], "1.5");
        assert_eq!(cancelled["outcome"]["status"], "cancelled");
        assert_eq!(cancelled["outcome"]["acceptedShares"], "1");
        assert_eq!(cancelled["outcome"]["filledShares"], "0");
        assert_eq!(cancelled["outcome"]["remainingShares"], "1");
        assert_eq!(cancelled["outcome"]["excessShares"], "0");
    }

    #[tokio::test]
    async fn offchain_trade_history_skips_malformed_rows() {
        let state = empty_app_state(create_test_ctx_with_order_owner(Address::ZERO)).await;
        let pool = state.pool.clone();
        let malformed_payload = r#"{"Live":{"Failed":{"symbol":"SPCX"}}}"#;
        let malformed_view_id = "00000000-0000-0000-0000-000000000142";
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind(malformed_view_id)
            .bind(malformed_payload)
            .execute(&pool)
            .await
            .unwrap();
        let valid_payload = r#"{"Live":{"Failed":{"symbol":"SPCX","shares":"1","direction":"Buy","executor":"AlpacaBrokerApi","retained_fill":{"Priced":{"shares_filled":"0.25","avg_price":"25","partially_filled_at":"2026-01-01T00:00:00Z"}},"executor_order_id":"broker-order","error":"asset is not tradable","placed_at":"2026-01-01T00:00:00Z","failed_at":"2026-01-01T00:00:01Z"}}}"#;
        let valid_view_id = "00000000-0000-0000-0000-000000000143";
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind(valid_view_id)
            .bind(valid_payload)
            .execute(&pool)
            .await
            .unwrap();

        let entries = query_trades(
            &pool,
            &TradeQuery {
                venues: None,
                limit: usize::MAX,
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
        )
        .await
        .unwrap()
        .trades;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, valid_view_id);

        let response = build_app(state)
            .oneshot(
                Request::builder()
                    .uri("/trades?trade_protocol=terminal_outcomes_v1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_str(&body_to_string(response).await).unwrap();
        assert_eq!(body["total"], 1);
        assert_eq!(body["entries"][0]["id"], valid_view_id);
    }

    #[tokio::test]
    async fn trade_row_loading_replays_repaired_bebop_source() {
        let state = empty_app_state(create_test_ctx_with_order_owner(Address::ZERO)).await;
        let pool = state.pool.clone();
        let now = Utc::now();
        let id = OnChainTradeId {
            tx_hash: TxHash::ZERO,
            log_index: 194,
        };
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store
            .send(
                &id,
                OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Legacy,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: float!(10),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_number: 12345,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OnChainTradeCommand::AttributeSource {
                    source: OnChainTradeSource::Inventory {
                        operator: Address::repeat_byte(0x8b),
                        venue: InventoryVenue::Bebop,
                    },
                },
            )
            .await
            .unwrap();

        let bebop = query_trades(
            &pool,
            &TradeQuery {
                venues: Some(vec![TradingVenue::Bebop]),
                limit: usize::MAX,
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
        )
        .await
        .unwrap()
        .trades;
        let alpaca = query_trades(
            &pool,
            &TradeQuery {
                venues: Some(vec![TradingVenue::Alpaca]),
                limit: usize::MAX,
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
        )
        .await
        .unwrap()
        .trades;

        assert_eq!(bebop.len(), 1);
        assert_eq!(bebop[0].venue, TradingVenue::Bebop);
        assert!(alpaca.is_empty());

        let response = build_app(state)
            .oneshot(
                Request::builder()
                    .uri("/trades?trade_protocol=terminal_outcomes_v2&venue=raindex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_str(&body_to_string(response).await).unwrap();
        assert_eq!(body["total"], 1);
        assert_eq!(body["entries"][0]["id"], id.to_string());
        assert_eq!(body["entries"][0]["venue"], "raindex");
    }

    /// Every filter dimension, asserted against the real SQL rather than an
    /// in-memory predicate: the matching case uses inclusive `since`/`until`
    /// set to the trade's exact outcome timestamp, which is the boundary the
    /// generated `occurred_at` column has to reproduce to the nanosecond.
    #[tokio::test]
    async fn failed_offchain_trade_history_respects_all_filters() {
        let state = empty_app_state(create_test_ctx_with_order_owner(Address::ZERO)).await;
        let pool = state.pool.clone();
        let payload = r#"{"Live":{"Failed":{"symbol":"SPCX","shares":"1","direction":"Buy","executor":"AlpacaBrokerApi","retained_fill":null,"executor_order_id":null,"error":"asset is not tradable","placed_at":"2026-01-01T00:00:00Z","failed_at":"2026-01-01T00:00:01Z"}}}"#;
        let view_id = "00000000-0000-0000-0000-000000000143";
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind(view_id)
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap();

        let matching = query_trades(
            &pool,
            &TradeQuery {
                symbols: Some(vec![Symbol::new("SPCX").unwrap()]),
                venues: Some(vec![TradingVenue::Alpaca]),
                since: Some("2026-01-01T00:00:01Z".parse().unwrap()),
                until: Some("2026-01-01T00:00:01Z".parse().unwrap()),
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
        )
        .await
        .unwrap();
        assert_eq!(matching.total, 1);
        assert_eq!(matching.trades[0].id, view_id);

        for excluding in [
            TradeQuery {
                symbols: Some(vec![Symbol::new("AAPL").unwrap()]),
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
            TradeQuery {
                venues: Some(vec![TradingVenue::DryRun]),
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
            TradeQuery {
                since: Some("2026-01-01T00:00:02Z".parse().unwrap()),
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
            TradeQuery {
                until: Some("2026-01-01T00:00:00Z".parse().unwrap()),
                ..TradeQuery::all(TradeProtocol::TerminalOutcomesV3)
            },
        ] {
            let page = query_trades(&pool, &excluding).await.unwrap();
            assert_eq!(page.total, 0);
            assert!(page.trades.is_empty());
        }
    }

    #[tokio::test]
    async fn trade_history_rejects_invalid_filters() {
        let state = empty_app_state(create_test_ctx_with_order_owner(Address::ZERO)).await;
        let app = build_app(state);

        for uri in [
            "/trades?venue=unknown",
            "/trades?symbol=%20",
            "/trades?since=not-a-timestamp",
            "/trades?until=not-a-timestamp",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "URI: {uri}");
        }
    }

    #[tokio::test]
    async fn trade_history_saturates_an_overflowing_page_end() {
        let state = empty_app_state(create_test_ctx_with_order_owner(Address::ZERO)).await;
        let response = build_app(state)
            .oneshot(
                Request::builder()
                    .uri(format!("/trades?offset={}", usize::MAX))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_str(&body_to_string(response).await).unwrap();
        assert_eq!(body["entries"], serde_json::json!([]));
        assert_eq!(body["total"], 0);
        assert_eq!(body["hasMore"], false);
    }

    #[tokio::test]
    async fn offchain_order_view_status_column_maps_cancelling_and_cancelled() {
        // The generated `status` column must map every lifecycle state; a
        // missing CASE arm yields NULL and hides the order from the
        // status-filtered pending-orders query.
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let cancelling_payload = r#"{"Live":{"Cancelling":{"symbol":"AAPL","direction":"Sell","shares":"1.5","executor":"DryRun","placed_at":"2026-01-01T00:00:00Z","submitted_at":"2026-01-01T00:00:01Z"}}}"#;
        let cancelled_payload = r#"{"Live":{"Cancelled":{"symbol":"AAPL","direction":"Sell","shares":"1.5","executor":"DryRun","placed_at":"2026-01-01T00:00:00Z","cancelled_at":"2026-01-01T00:00:02Z"}}}"#;
        for (view_id, payload) in [
            ("order-cancelling", cancelling_payload),
            ("order-cancelled", cancelled_payload),
        ] {
            sqlx::query(
                "INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)",
            )
            .bind(view_id)
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap();
        }

        let statuses: Vec<(String, String)> =
            sqlx::query_as("SELECT view_id, status FROM offchain_order_view ORDER BY view_id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            statuses,
            vec![
                ("order-cancelled".to_string(), "Cancelled".to_string()),
                ("order-cancelling".to_string(), "Cancelling".to_string()),
            ]
        );

        // The pending-orders filter must surface Cancelling (non-terminal)
        // and exclude Cancelled (terminal).
        let pending: Vec<(String,)> = sqlx::query_as(
            "SELECT view_id FROM offchain_order_view \
             WHERE status IN ('Pending', 'Submitted', 'PartiallyFilled', 'Cancelling')",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(pending, vec![("order-cancelling".to_string(),)]);
    }

    #[test]
    fn stuck_redemption_info_prefers_actual_unwrapped_quantity() {
        let rows = vec![
            event_row(
                "EquityRedemptionEvent::VaultWithdrawPending",
                r#"{"VaultWithdrawPending":{"symbol":"AAPL","quantity":"10","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"10000000000000000000","pending_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "EquityRedemptionEvent::TokensUnwrapped",
                r#"{"TokensUnwrapped":{"quantity":"9.75","underlying_token":"0x0000000000000000000000000000000000000002","unwrap_tx_hash":"0x1111111111111111111111111111111111111111111111111111111111111111","unwrapped_amount":"9750000000000000000","unwrapped_at":"2026-01-01T00:00:01Z"}}"#,
                1,
            ),
            event_row(
                "EquityRedemptionEvent::TokensSent",
                r#"{"TokensSent":{"redemption_wallet":"0x0000000000000000000000000000000000000003","redemption_tx":"0x2222222222222222222222222222222222222222222222222222222222222222","sent_at":"2026-01-01T00:00:02Z"}}"#,
                2,
            ),
            event_row(
                "EquityRedemptionEvent::DetectionFailed",
                r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:01:00Z"}}"#,
                3,
            ),
        ];

        let stuck =
            stuck_transfer_info(TransferKind::EquityRedemption, &rows).expect("stuck amount");

        assert_eq!(stuck.amount, "9.75");
        assert_eq!(stuck.location, StuckLocation::RedemptionWallet);
        assert_eq!(stuck.reason, StuckReason::DetectionFailed);
    }

    #[test]
    fn stuck_mint_info_reports_wrapping_failure_in_bot_wallet() {
        let rows = vec![
            event_row(
                "TokenizedEquityMintEvent::MintRequested",
                r#"{"MintRequested":{"symbol":"AAPL","quantity":"7","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "TokenizedEquityMintEvent::WrappingFailed",
                r#"{"WrappingFailed":{"symbol":"AAPL","quantity":"7","reason":"revert","failed_at":"2026-01-01T00:01:00Z"}}"#,
                1,
            ),
        ];

        let stuck = stuck_transfer_info(TransferKind::EquityMint, &rows).expect("stuck amount");

        assert_eq!(stuck.amount, "7");
        assert_eq!(stuck.location, StuckLocation::BotWalletUnwrapped);
        assert_eq!(stuck.reason, StuckReason::WrappingFailed);
    }

    #[test]
    fn stuck_mint_info_reports_deposit_failure_as_wrapped_in_bot_wallet() {
        let rows = vec![
            event_row(
                "TokenizedEquityMintEvent::MintRequested",
                r#"{"MintRequested":{"symbol":"AAPL","quantity":"3.5","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "TokenizedEquityMintEvent::RaindexDepositFailed",
                r#"{"RaindexDepositFailed":{"reason":"revert","failed_at":"2026-01-01T00:01:00Z"}}"#,
                1,
            ),
        ];

        let stuck = stuck_transfer_info(TransferKind::EquityMint, &rows).expect("stuck amount");

        assert_eq!(stuck.amount, "3.5");
        assert_eq!(stuck.location, StuckLocation::BotWalletWrapped);
        assert_eq!(stuck.reason, StuckReason::RaindexDepositFailed);
    }

    #[test]
    fn stuck_mint_info_returns_none_for_completed_mint() {
        let rows = vec![
            event_row(
                "TokenizedEquityMintEvent::MintRequested",
                r#"{"MintRequested":{"symbol":"AAPL","quantity":"7","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "TokenizedEquityMintEvent::DepositedIntoRaindex",
                r#"{"DepositedIntoRaindex":{"vault_deposit_tx_hash":"0x1111111111111111111111111111111111111111111111111111111111111111","deposited_at":"2026-01-01T00:02:00Z"}}"#,
                1,
            ),
        ];

        assert!(stuck_transfer_info(TransferKind::EquityMint, &rows).is_none());
    }

    #[test]
    fn stuck_redemption_info_cleared_after_provider_completion_recovery() {
        let rows = vec![
            event_row(
                "EquityRedemptionEvent::VaultWithdrawPending",
                r#"{"VaultWithdrawPending":{"symbol":"AAPL","quantity":"10","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"10000000000000000000","pending_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "EquityRedemptionEvent::TokensSent",
                r#"{"TokensSent":{"redemption_wallet":"0x0000000000000000000000000000000000000003","redemption_tx":"0x2222222222222222222222222222222222222222222222222222222222222222","sent_at":"2026-01-01T00:00:02Z"}}"#,
                1,
            ),
            event_row(
                "EquityRedemptionEvent::RedemptionRejected",
                r#"{"RedemptionRejected":{"reason":"rejected","rejected_at":"2026-01-01T00:01:00Z"}}"#,
                2,
            ),
            event_row(
                "EquityRedemptionEvent::ProviderCompletionRecovered",
                r#"{"ProviderCompletionRecovered":{"tokenization_request_id":"tok-1","recovered_at":"2026-01-01T00:02:00Z"}}"#,
                3,
            ),
        ];

        assert!(
            stuck_transfer_info(TransferKind::EquityRedemption, &rows).is_none(),
            "a recovered redemption must not be reported as stranded"
        );
    }

    #[test]
    fn stuck_mint_info_cleared_after_operator_reconciled() {
        let rows = vec![
            event_row(
                "TokenizedEquityMintEvent::MintRequested",
                r#"{"MintRequested":{"symbol":"AAPL","quantity":"12.5","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "TokenizedEquityMintEvent::MintAccepted",
                r#"{"MintAccepted":{"issuer_request_id":"mint-1","tokenization_request_id":"tok-1","accepted_at":"2026-01-01T00:00:01Z"}}"#,
                1,
            ),
            event_row(
                "TokenizedEquityMintEvent::MintAcceptanceFailed",
                r#"{"MintAcceptanceFailed":{"reason":"timeout","failed_at":"2026-01-01T00:01:00Z"}}"#,
                2,
            ),
            event_row(
                "TokenizedEquityMintEvent::OperatorReconciled",
                r#"{"OperatorReconciled":{"reason":"wrapped manually via wrap-equity","reconciled_at":"2026-01-01T00:02:00Z"}}"#,
                3,
            ),
        ];

        assert!(
            stuck_transfer_info(TransferKind::EquityMint, &rows).is_none(),
            "an operator-reconciled mint must not be reported as stranded"
        );
    }

    #[test]
    fn stuck_redemption_info_cleared_after_operator_reconciled() {
        let rows = vec![
            event_row(
                "EquityRedemptionEvent::VaultWithdrawPending",
                r#"{"VaultWithdrawPending":{"symbol":"AAPL","quantity":"10","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"10000000000000000000","pending_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "EquityRedemptionEvent::TokensSent",
                r#"{"TokensSent":{"redemption_wallet":"0x0000000000000000000000000000000000000003","redemption_tx":"0x2222222222222222222222222222222222222222222222222222222222222222","sent_at":"2026-01-01T00:00:02Z"}}"#,
                1,
            ),
            event_row(
                "EquityRedemptionEvent::RedemptionRejected",
                r#"{"RedemptionRejected":{"reason":"rejected","rejected_at":"2026-01-01T00:01:00Z"}}"#,
                2,
            ),
            event_row(
                "EquityRedemptionEvent::OperatorReconciled",
                r#"{"OperatorReconciled":{"reason":"deposited manually via vault-deposit","reconciled_at":"2026-01-01T00:02:00Z"}}"#,
                3,
            ),
        ];

        assert!(
            stuck_transfer_info(TransferKind::EquityRedemption, &rows).is_none(),
            "an operator-reconciled redemption must not be reported as stranded"
        );
    }

    #[test]
    fn stuck_transfer_info_serializes_to_dashboard_wire_format() {
        let info = StuckTransferInfo {
            amount: "12.5".to_string(),
            location: StuckLocation::BotWalletWrapped,
            reason: StuckReason::RaindexDepositFailed,
        };

        let json = serde_json::to_value(&info).expect("serialize stuck info");

        assert_eq!(json["stuckAmount"], serde_json::json!("12.5"));
        assert_eq!(
            json["stuckLocation"],
            serde_json::json!("bot_wallet_wrapped")
        );
        assert_eq!(
            json["stuckReason"],
            serde_json::json!("raindex_deposit_failed")
        );
    }

    #[test]
    fn stuck_mint_info_ignores_acceptance_failure_without_acceptance() {
        // MintAcceptanceFailed without a preceding MintAccepted (the `if
        // accepted` guard) means equity never left the issuer, so nothing is
        // stranded.
        let rows = vec![
            event_row(
                "TokenizedEquityMintEvent::MintRequested",
                r#"{"MintRequested":{"symbol":"AAPL","quantity":"7","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "TokenizedEquityMintEvent::MintAcceptanceFailed",
                r#"{"MintAcceptanceFailed":{"reason":"never accepted","failed_at":"2026-01-01T00:01:00Z"}}"#,
                1,
            ),
        ];

        assert!(stuck_transfer_info(TransferKind::EquityMint, &rows).is_none());
    }

    #[test]
    fn stuck_redemption_info_falls_back_to_withdrawn_amount_when_not_unwrapped() {
        // No TokensUnwrapped event, so the amount comes from the withdrawn
        // wrapped amount (18-decimal U256 -> shares), not the requested
        // quantity.
        let rows = vec![
            event_row(
                "EquityRedemptionEvent::VaultWithdrawPending",
                r#"{"VaultWithdrawPending":{"symbol":"AAPL","quantity":"5","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"5000000000000000000","pending_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "EquityRedemptionEvent::WithdrawnFromRaindex",
                r#"{"WithdrawnFromRaindex":{"symbol":"AAPL","quantity":"5","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"9000000000000000000","raindex_withdraw_tx":"0x1111111111111111111111111111111111111111111111111111111111111111","withdrawn_at":"2026-01-01T00:00:01Z"}}"#,
                1,
            ),
            event_row(
                "EquityRedemptionEvent::TokensSent",
                r#"{"TokensSent":{"redemption_wallet":"0x0000000000000000000000000000000000000003","redemption_tx":"0x2222222222222222222222222222222222222222222222222222222222222222","sent_at":"2026-01-01T00:00:02Z"}}"#,
                2,
            ),
            event_row(
                "EquityRedemptionEvent::DetectionFailed",
                r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:01:00Z"}}"#,
                3,
            ),
        ];

        let stuck =
            stuck_transfer_info(TransferKind::EquityRedemption, &rows).expect("stuck amount");

        assert_eq!(stuck.amount, "9");
        assert_eq!(stuck.location, StuckLocation::RedemptionWallet);
    }

    #[test]
    fn stuck_redemption_info_uses_unwrapped_amount_when_quantity_absent() {
        // TokensUnwrapped with no recorded `quantity` but a known
        // `unwrapped_amount`: the stranded amount must be the actual unwrapped
        // underlying tokens (9.5), not the larger wrapped withdrawn amount (10).
        let rows = vec![
            event_row(
                "EquityRedemptionEvent::WithdrawnFromRaindex",
                r#"{"WithdrawnFromRaindex":{"symbol":"AAPL","quantity":"10","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"10000000000000000000","raindex_withdraw_tx":"0x1111111111111111111111111111111111111111111111111111111111111111","withdrawn_at":"2026-01-01T00:00:01Z"}}"#,
                0,
            ),
            event_row(
                "EquityRedemptionEvent::TokensUnwrapped",
                r#"{"TokensUnwrapped":{"quantity":null,"underlying_token":"0x0000000000000000000000000000000000000002","unwrap_tx_hash":"0x3333333333333333333333333333333333333333333333333333333333333333","unwrapped_amount":"9500000000000000000","unwrapped_at":"2026-01-01T00:00:02Z"}}"#,
                1,
            ),
            event_row(
                "EquityRedemptionEvent::TokensSent",
                r#"{"TokensSent":{"redemption_wallet":"0x0000000000000000000000000000000000000003","redemption_tx":"0x2222222222222222222222222222222222222222222222222222222222222222","sent_at":"2026-01-01T00:00:03Z"}}"#,
                2,
            ),
            event_row(
                "EquityRedemptionEvent::DetectionFailed",
                r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:01:00Z"}}"#,
                3,
            ),
        ];

        let stuck =
            stuck_transfer_info(TransferKind::EquityRedemption, &rows).expect("stuck amount");

        assert_eq!(stuck.amount, "9.5");
        assert_eq!(stuck.location, StuckLocation::RedemptionWallet);
        assert_eq!(stuck.reason, StuckReason::DetectionFailed);
    }

    #[test]
    fn stuck_redemption_info_falls_back_to_requested_quantity() {
        // Neither unwrapped nor withdrawn amounts are present, so the requested
        // quantity is the last-resort amount.
        let rows = vec![
            event_row(
                "EquityRedemptionEvent::VaultWithdrawPending",
                r#"{"VaultWithdrawPending":{"symbol":"AAPL","quantity":"5","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"5000000000000000000","pending_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "EquityRedemptionEvent::TokensSent",
                r#"{"TokensSent":{"redemption_wallet":"0x0000000000000000000000000000000000000003","redemption_tx":"0x2222222222222222222222222222222222222222222222222222222222222222","sent_at":"2026-01-01T00:00:02Z"}}"#,
                1,
            ),
            event_row(
                "EquityRedemptionEvent::RedemptionRejected",
                r#"{"RedemptionRejected":{"reason":"rejected","rejected_at":"2026-01-01T00:01:00Z"}}"#,
                2,
            ),
        ];

        let stuck =
            stuck_transfer_info(TransferKind::EquityRedemption, &rows).expect("stuck amount");

        assert_eq!(stuck.amount, "5");
        assert_eq!(stuck.reason, StuckReason::RedemptionRejected);
    }

    #[test]
    fn stuck_redemption_info_ignores_detection_failure_before_tokens_sent() {
        // DetectionFailed before TokensSent (the `sent` guard) means tokens
        // never left, so nothing is stranded in the redemption wallet.
        let rows = vec![
            event_row(
                "EquityRedemptionEvent::VaultWithdrawPending",
                r#"{"VaultWithdrawPending":{"symbol":"AAPL","quantity":"5","token":"0x0000000000000000000000000000000000000001","wrapped_amount":"5000000000000000000","pending_at":"2026-01-01T00:00:00Z"}}"#,
                0,
            ),
            event_row(
                "EquityRedemptionEvent::DetectionFailed",
                r#"{"DetectionFailed":{"failure":"Timeout","failed_at":"2026-01-01T00:01:00Z"}}"#,
                1,
            ),
        ];

        assert!(stuck_transfer_info(TransferKind::EquityRedemption, &rows).is_none());
    }

    fn event_row(event_type: &str, payload: &str, sequence: i64) -> (String, String, i64) {
        (event_type.to_string(), payload.to_string(), sequence)
    }

    fn build_app(state: AppState) -> Router {
        routes(None).with_state(state)
    }

    async fn body_to_string(response: axum::response::Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn get_log_response(app: &Router, uri: &str) -> LogResponse {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_to_string(response).await;
        serde_json::from_str(&body).expect("valid LogResponse JSON")
    }

    #[tokio::test]
    async fn pnl_rejects_invalid_only_symbol_filter_before_loading_report() {
        let mock_server = httpmock::MockServer::start();
        let account_activity_mock = mock_server.mock(|when, then| {
            when.method(GET).path("/v1/accounts/activities");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({ "message": "should not be called" }));
        });
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.broker = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            auth: st0x_execution::AlpacaBrokerAuth::Basic {
                api_key: "test_key_id".to_owned(),
                api_secret: "test_secret_key".to_owned(),
            },
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Mock(mock_server.base_url())),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        });
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/pnl?symbol=RKLB%27%29%3B%20DROP%20TABLE%20events%3B%20--")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        account_activity_mock.assert_calls(0);
    }

    fn portfolio_snapshot_usdc_row(
        amount: i64,
        captured_at: chrono::DateTime<Utc>,
    ) -> PortfolioBalanceRowWithMark {
        PortfolioBalanceRowWithMark {
            row: PortfolioBalanceRow {
                location: PortfolioLocation::MarketMaking,
                asset: PortfolioAsset::Usdc,
                available: float!(&amount.to_string()),
                inflight: float!(0),
            },
            usd_mark: Some(float!(1)),
            mark_captured_at: Some(captured_at),
        }
    }

    /// Seeds a real onchain-sell/offchain-buy hedge pair for `RKLB` through
    /// the `Position` aggregate's own commands (`Store::send`, never a raw
    /// `events` INSERT -- direct writes to that table are forbidden, see
    /// docs/cqrs.md). Onchain sell at 10, offchain buy at 8: a 2-per-share
    /// realized spread, matching this test's `grossRealizedPnlUsd` assertion.
    async fn seed_position_pnl_fill(pool: &SqlitePool, symbol: &Symbol) {
        let (position, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let threshold = ExecutionThreshold::whole_share();
        let block_timestamp = Utc.with_ymd_and_hms(2026, 3, 8, 14, 0, 0).unwrap();
        let broker_timestamp = Utc.with_ymd_and_hms(2026, 3, 8, 14, 1, 0).unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let one_share = Positive::new(FractionalShares::new(float!(1))).unwrap();

        position
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFillAt {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        tx_hash: TxHash::with_last_byte(1),
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Sell,
                    price_usdc: float!(10),
                    block_timestamp,
                    block_number: None,
                    seen_at: block_timestamp,
                },
            )
            .await
            .unwrap();

        position
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrderAt {
                    offchain_order_id,
                    shares: one_share,
                    direction: Direction::Buy,
                    executor: SupportedExecutor::DryRun,
                    threshold,
                    placed_at: block_timestamp,
                },
            )
            .await
            .unwrap();

        position
            .send(
                symbol,
                PositionCommand::CompleteOffChainOrder {
                    offchain_order_id,
                    shares_filled: one_share,
                    direction: Direction::Buy,
                    executor_order_id: ExecutorOrderId::new("test-fill"),
                    price: Usd::new(float!(8)),
                    broker_timestamp,
                },
            )
            .await
            .unwrap();
    }

    /// End-to-end `/pnl` coverage for capital/return-on-capital figures: three
    /// portfolio_snapshot days are captured through the real
    /// aggregate (`Store::send`, never raw SQL), each `captured_at` built
    /// from the actual "just after ET midnight" local time (mirroring
    /// `write.rs`'s `CAPTURE_BUFFER` scheme) across the actual 2026 US DST
    /// spring-forward Sunday (March 8) -- so the UTC offset genuinely shifts
    /// between the March 8 and March 9 captures -- and each row's aggregate
    /// id is derived through the same DST-aware [`et_day`] the production
    /// job uses, not assumed from the loop variable. The response is
    /// asserted at the JSON boundary so camelCase field naming is verified
    /// too.
    #[tokio::test]
    async fn pnl_endpoint_reports_capital_from_persisted_portfolio_snapshots() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;

        let portfolio_snapshot_store = StoreBuilder::<PortfolioSnapshot>::new(state.pool.clone())
            .with(Arc::new(PortfolioSnapshotProjection::new(
                state.pool.clone(),
            )))
            .build(())
            .await
            .unwrap();

        for (year, month, day, amount) in
            [(2026, 3, 7, 1000), (2026, 3, 8, 2000), (2026, 3, 9, 3000)]
        {
            let captured_at = New_York
                .with_ymd_and_hms(year, month, day, 0, 5, 0)
                .single()
                .unwrap()
                .with_timezone(&Utc);
            let target_et_day = et_day(captured_at);
            assert_eq!(
                target_et_day,
                NaiveDate::from_ymd_opt(year, month, day).unwrap(),
                "captured_at should round-trip to the same calendar day it was built from, \
                 proving et_day resolves the correct EST/EDT offset either side of the \
                 spring-forward transition"
            );
            portfolio_snapshot_store
                .send(
                    &PortfolioSnapshotId(target_et_day),
                    PortfolioSnapshotCommand::Capture {
                        captured_at,
                        rows: vec![portfolio_snapshot_usdc_row(amount, captured_at)],
                    },
                )
                .await
                .unwrap();
        }

        seed_position_pnl_fill(&state.pool, &Symbol::new("RKLB").unwrap()).await;

        let app = build_app(state);

        let inclusive_range = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/pnl?fromDate=2026-03-07&toDate=2026-03-09")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(inclusive_range.status(), StatusCode::OK);
        let body = body_to_string(inclusive_range).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["capital"]["sampleDays"], serde_json::json!(3));
        assert_eq!(report["capital"]["coverageDays"], serde_json::json!(3));
        assert_eq!(
            report["capital"]["averageDeployedCapitalUsd"],
            serde_json::json!("2000")
        );
        assert_eq!(
            report["capital"]["firstSnapshotDay"],
            serde_json::json!("2026-03-07")
        );
        assert_eq!(
            report["capital"]["lastSnapshotDay"],
            serde_json::json!("2026-03-09")
        );
        assert_eq!(
            report["summary"]["gross_realized_pnl_usd"],
            serde_json::Value::Null
        );
        assert_eq!(
            report["summary"]["grossRealizedPnlUsd"],
            serde_json::json!("2")
        );

        let no_bounds = app
            .clone()
            .oneshot(Request::builder().uri("/pnl").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(no_bounds.status(), StatusCode::OK);
        let body = body_to_string(no_bounds).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["capital"]["sampleDays"], serde_json::json!(3));
        assert_eq!(
            report["capital"]["averageDeployedCapitalUsd"],
            serde_json::json!("2000")
        );

        let dst_day_only = app
            .oneshot(
                Request::builder()
                    .uri("/pnl?fromDate=2026-03-08&toDate=2026-03-08")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(dst_day_only.status(), StatusCode::OK);
        let body = body_to_string(dst_day_only).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["capital"]["sampleDays"], serde_json::json!(1));
        assert_eq!(report["capital"]["coverageDays"], serde_json::json!(1));
        assert_eq!(
            report["capital"]["averageDeployedCapitalUsd"],
            serde_json::json!("2000")
        );
        assert_eq!(
            report["capital"]["annualizedReturnPct"],
            serde_json::Value::Null
        );
        assert_eq!(
            report["capital"]["firstSnapshotDay"],
            serde_json::json!("2026-03-08")
        );
        assert_eq!(
            report["capital"]["lastSnapshotDay"],
            serde_json::json!("2026-03-08")
        );
    }

    fn write_test_logs(dir: &std::path::Path, filename: &str, content: &str) {
        std::fs::write(dir.join(filename), content).unwrap();
    }

    const THREE_ENTRY_LOG: &str = r#"{"timestamp":"2026-04-20T10:00:00Z","level":"INFO","target":"st0x_hedge","message":"Bot started"}
{"timestamp":"2026-04-20T10:00:01Z","level":"DEBUG","target":"st0x_hedge","message":"Polling"}
{"timestamp":"2026-04-20T10:00:02Z","level":"WARN","target":"st0x_hedge","message":"Slow response"}"#;

    #[tokio::test]
    async fn performance_latencies_returns_empty_report_on_fresh_database() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/latencies")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["summary"]["fillCount"], serde_json::json!(0));
        assert_eq!(report["totalCycles"], serde_json::json!(0));
        assert_eq!(report["cycles"], serde_json::json!([]));
        assert_eq!(report["openExposures"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn performance_latencies_reports_seeded_fills() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        let now = chrono::Utc::now();
        // The hedge-latency read model recomputes open exposure from the
        // reactor-maintained tables: a fill with no covering cycle is uncovered,
        // so a single hedge_fill row is both one detection sample (zero latency)
        // and one open-exposure entry.
        let timestamp = now.to_rfc3339();
        sqlx::query(
            "INSERT INTO hedge_fill (symbol, tx_hash, log_index, block_timestamp, seen_at) \
             VALUES ('AAPL', '0x01', 0, $1, $1)",
        )
        .bind(&timestamp)
        .execute(&state.pool)
        .await
        .unwrap();
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/latencies")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["summary"]["fillCount"], serde_json::json!(1));
        assert_eq!(
            report["openExposures"][0]["symbol"],
            serde_json::json!("AAPL")
        );
        assert_eq!(
            report["summary"]["stages"]["detection"]["p50Ms"],
            serde_json::json!(0)
        );
    }

    #[tokio::test]
    async fn performance_rebalances_returns_empty_report_on_fresh_database() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/rebalances")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["totalOperations"], serde_json::json!(0));
        assert_eq!(report["operations"], serde_json::json!([]));
        assert_eq!(report["stageSummary"], serde_json::json!([]));
        assert_eq!(report["attestationTrend"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn performance_reliability_returns_empty_report_on_fresh_database() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/reliability")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["logBuckets"], serde_json::json!([]));
        assert_eq!(report["logTargets"], serde_json::json!([]));
        assert_eq!(report["failureEvents"], serde_json::json!([]));
        assert_eq!(report["jobQueues"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn performance_reliability_rejects_inverted_range() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/reliability\
                         ?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn performance_reliability_rejects_equal_range() {
        // `from == to` is an empty interval and must be rejected with 400,
        // matching the latencies and rebalances endpoints.
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/reliability\
                         ?from=2026-01-01T00:00:00Z&to=2026-01-01T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn performance_infra_returns_empty_report_on_fresh_database() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/infra")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        // The full literal: a missing field or a camelCase typo must fail
        // loudly, not match a `null` produced by indexing into nothing.
        assert_eq!(
            report,
            serde_json::json!({
                "monitor": {
                    "currentLagBlocks": null,
                    "currentLagSampledAt": null,
                    "blockLag": [],
                    "poll": {
                        "cycles": 0,
                        "errors": 0,
                        "skippedTicks": 0,
                        "duration": null,
                    },
                },
                "dependencies": [],
            })
        );
    }

    #[tokio::test]
    async fn performance_infra_rejects_inverted_range() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/infra\
                         ?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn performance_infra_reports_seeded_telemetry() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let orderbook = ctx.chains.sole_trading().orderbook;
        let state = empty_app_state(ctx).await;
        let now = chrono::Utc::now();

        crate::telemetry::record_block_lag(
            &state.pool,
            &crate::telemetry::BlockLagSample {
                sampled_at: now,
                orderbook,
                chain_tip: 120,
                cutoff_block: Some(117),
                last_processed_block: Some(100),
            },
        )
        .await
        .unwrap();
        crate::telemetry::record_poll_cycle(
            &state.pool,
            crate::telemetry::Monitor::OrderFill,
            orderbook,
            now,
            std::time::Duration::from_millis(40),
            1,
            Ok::<(), &std::convert::Infallible>(()),
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO dependency_call_samples \
             (recorded_at, dependency, operation, duration_ms, outcome, error) \
             VALUES ($1, 'rpc', 'eth_getLogs', 120, 'error', 'timeout')",
        )
        .bind(crate::telemetry::sqlite_timestamp(now))
        .execute(&state.pool)
        .await
        .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/infra")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        // Lag = cutoff_block (117) - checkpoint (100).
        assert_eq!(report["monitor"]["currentLagBlocks"], serde_json::json!(17));
        assert_eq!(
            report["monitor"]["blockLag"][0]["maxLagBlocks"],
            serde_json::json!(17)
        );
        // The bucket start anchors the dashboard's x-axis: it must parse as
        // a timestamp inside the report's default 7-day window.
        let bucket_start = report["monitor"]["blockLag"][0]["start"]
            .as_str()
            .expect("bucket start must be a timestamp string");
        let bucket_start = chrono::DateTime::parse_from_rfc3339(bucket_start)
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert!(
            bucket_start <= now && now - bucket_start <= chrono::Duration::days(7),
            "bucket start {bucket_start} must fall inside the default window"
        );
        assert_eq!(report["monitor"]["poll"]["cycles"], serde_json::json!(1));
        assert_eq!(
            report["monitor"]["poll"]["skippedTicks"],
            serde_json::json!(1)
        );
        assert_eq!(
            report["monitor"]["poll"]["duration"]["p50Ms"],
            serde_json::json!(40)
        );
        assert_eq!(
            report["dependencies"][0]["dependency"],
            serde_json::json!("rpc")
        );
        assert_eq!(
            report["dependencies"][0]["operation"],
            serde_json::json!("eth_getLogs")
        );
        assert_eq!(report["dependencies"][0]["calls"], serde_json::json!(1));
        assert_eq!(report["dependencies"][0]["errors"], serde_json::json!(1));
        assert_eq!(
            report["dependencies"][0]["latency"]["p50Ms"],
            serde_json::json!(120)
        );
        assert_eq!(
            report["dependencies"][0]["buckets"][0]["p50Ms"],
            serde_json::json!(120)
        );
    }

    #[tokio::test]
    async fn performance_reliability_reports_seeded_failures_and_jobs() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        let now = chrono::Utc::now();

        // The failure read model reads the reactor-maintained table. Drive the
        // real reactor path (record()) via the harness rather than a raw
        // INSERT, so a schema change to record() cannot leave this end-to-end
        // test green while the live reactor breaks.
        ReactorHarness::new(LifecycleFailureProjection::new(state.pool.clone()))
            .receive::<OffchainOrder>(
                OffchainOrderId::new(),
                OffchainOrderEvent::Failed {
                    error: "rejected".to_string(),
                    filled_shares: None,
                    failed_at: now,
                },
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO Jobs (job, id, job_type, status, attempts, run_at) \
             VALUES ('{}', 'job-1', 'queue::A', 'Pending', 0, 1750000000)",
        )
        .execute(&state.pool)
        .await
        .unwrap();
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/reliability")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(
            report["failureEvents"][0]["eventType"],
            serde_json::json!("OffchainOrderEvent::Failed")
        );
        assert_eq!(report["failureEvents"][0]["count"], serde_json::json!(1));
        assert_eq!(
            report["jobQueues"][0]["jobType"],
            serde_json::json!("queue::A")
        );
        assert_eq!(report["jobQueues"][0]["pending"], serde_json::json!(1));
    }

    #[tokio::test]
    async fn performance_reliability_aggregates_log_dir_entries() {
        let temp_dir = tempfile::tempdir().unwrap();
        // Three log entries: one ERROR and one WARN within the default 7-day
        // window, one INFO that must be filtered out.
        let log_content = concat!(
            "{\"timestamp\":\"2026-06-15T10:00:00Z\",\"level\":\"ERROR\",",
            "\"target\":\"hedge\",\"message\":\"crash\"}\n",
            "{\"timestamp\":\"2026-06-15T10:00:01Z\",\"level\":\"WARN\",",
            "\"target\":\"rebalance\",\"message\":\"retry\"}\n",
            "{\"timestamp\":\"2026-06-15T10:00:02Z\",\"level\":\"INFO\",",
            "\"target\":\"hedge\",\"message\":\"ok\"}",
        );
        write_test_logs(temp_dir.path(), "st0x-hedge.log.2026-06-15", log_content);

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/reliability\
                         ?from=2026-06-15T00:00:00Z&to=2026-06-16T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

        // Only ERROR and WARN entries appear; INFO is excluded.
        let buckets = &report["logBuckets"];
        let targets = &report["logTargets"];
        assert_eq!(
            buckets.as_array().unwrap().len(),
            1,
            "both entries fall in the same hour -> exactly one bucket"
        );
        assert_eq!(buckets[0]["errors"], serde_json::json!(1));
        assert_eq!(buckets[0]["warnings"], serde_json::json!(1));
        // Two distinct (target, level) pairs: hedge/ERROR and rebalance/WARN.
        assert_eq!(targets.as_array().unwrap().len(), 2);
        // Well under the entry cap, so the partial-data flag must be false.
        assert_eq!(report["logEntriesTruncated"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn performance_latencies_rejects_inverted_range() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/latencies\
                         ?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// `from == to` is an empty interval and must be rejected with 400.
    #[tokio::test]
    async fn performance_latencies_rejects_equal_from_and_to() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/latencies\
                         ?from=2026-01-01T00:00:00Z&to=2026-01-01T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// A pathological span (e.g. `to` far in the future) must be rejected
    /// rather than accepted and handed to `hedge_latency_report`, whose
    /// dense bucket generation would otherwise iterate once per bucket
    /// across the entire span.
    #[tokio::test]
    async fn performance_latencies_rejects_range_wider_than_max_span() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/latencies\
                         ?from=2000-01-01T00:00:00Z&to=9000-01-01T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// Seeds a fill with a non-zero detection latency (seen_at > block_timestamp)
    /// and verifies the value survives reactor write -> SQL load -> report assembly
    /// -> JSON serialization, ending up in `summary.stages.detection.p50Ms`.
    #[tokio::test]
    async fn performance_latencies_reports_nonzero_detection_latency() {
        use crate::performance::HedgeLatencyProjection;
        use st0x_event_sorcery::ReactorHarness;
        use st0x_execution::{Direction, FractionalShares};
        use st0x_float_macro::float;

        use crate::position::{Position, PositionEvent, TradeId};

        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;

        let harness = ReactorHarness::new(HedgeLatencyProjection::new(state.pool.clone()));

        // Fill with block_timestamp at T=0 and seen_at at T+3s: detection
        // latency must be exactly 3000 ms.
        let block_ts = chrono::Utc::now() - chrono::Duration::minutes(5);
        let seen_ts = block_ts + chrono::Duration::seconds(3);

        harness
            .receive::<Position>(
                st0x_execution::Symbol::new("AAPL").unwrap(),
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: alloy::primitives::TxHash::random(),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: block_ts,
                    block_number: None,
                    seen_at: seen_ts,
                },
            )
            .await
            .unwrap();

        let app = build_app(state);

        // Use a wide range spanning both timestamps.
        let from = (block_ts - chrono::Duration::hours(1))
            .to_rfc3339()
            .replace("+00:00", "Z");
        let to = (seen_ts + chrono::Duration::hours(1))
            .to_rfc3339()
            .replace("+00:00", "Z");
        let uri = format!("/performance/latencies?from={from}&to={to}");

        let response = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

        assert_eq!(
            report["summary"]["stages"]["detection"]["p50Ms"],
            serde_json::json!(3000)
        );
    }

    #[tokio::test]
    async fn performance_rebalances_rejects_inverted_range() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/rebalances\
                         ?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn performance_rebalances_reports_seeded_operations() {
        use st0x_event_sorcery::ReactorHarness;
        use st0x_float_macro::float;

        use crate::performance::rebalance::RebalanceTimingProjection;
        use crate::usdc_rebalance::{
            RebalanceDirection, UsdcRebalance, UsdcRebalanceEvent, UsdcRebalanceId,
        };

        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;

        // Drive the real reactor (not a hand-crafted JSON blob) so this test
        // stays at the public API surface and is decoupled from the private
        // StoredOperation serde schema. One BaseToAlpaca WithdrawalSubmitting
        // event leaves an open, in-progress Withdrawal stage.
        let harness = ReactorHarness::new(RebalanceTimingProjection::new(state.pool.clone()));
        let operation_id = UsdcRebalanceId(uuid::Uuid::new_v4());
        harness
            .receive::<UsdcRebalance>(
                operation_id,
                UsdcRebalanceEvent::WithdrawalSubmitting {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: st0x_finance::Usdc::new(float!(500)),
                    from_block: 1,
                    submitting_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/rebalances")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["totalOperations"], serde_json::json!(1));
        assert_eq!(
            report["operations"][0]["status"],
            serde_json::json!("in_progress")
        );
        assert_eq!(
            report["operations"][0]["direction"],
            serde_json::json!("base_to_alpaca")
        );
    }

    #[tokio::test]
    async fn performance_equity_rebalances_returns_empty_report_on_fresh_database() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/equity-rebalances")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["totalOperations"], serde_json::json!(0));
        assert_eq!(report["skippedOperations"], serde_json::json!(0));
        assert_eq!(report["operations"], serde_json::json!([]));
        assert_eq!(report["stageSummary"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn performance_equity_rebalances_rejects_inverted_range() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/performance/equity-rebalances\
                         ?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn performance_equity_rebalances_reports_seeded_operations() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;

        // Drive the real reactor (not a hand-crafted JSON blob) so this test
        // stays at the public API surface, mirroring
        // `performance_rebalances_reports_seeded_operations` above. One
        // `MintRequested` event leaves an open, in-progress mint operation.
        let harness = ReactorHarness::new(EquityTimingProjection::new(state.pool.clone()));
        let operation_id = issuer_request_id("equity-rebalances-seed");
        harness
            .receive::<TokenizedEquityMint>(
                operation_id,
                TokenizedEquityMintEvent::MintRequested {
                    symbol: st0x_execution::Symbol::new("AAPL").unwrap(),
                    quantity: float!(5),
                    wallet: Address::repeat_byte(0x11),
                    requested_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/performance/equity-rebalances")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let report: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
        assert_eq!(report["totalOperations"], serde_json::json!(1));
        assert_eq!(report["operations"][0]["kind"], serde_json::json!("mint"));
        assert_eq!(
            report["operations"][0]["status"],
            serde_json::json!("in_progress")
        );
    }

    #[tokio::test]
    async fn health_reports_starting_with_503_until_startup_completes() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = body_to_string(response).await;
        let health_response: HealthResponse =
            serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(health_response.status, "starting");
    }

    #[tokio::test]
    async fn health_reports_healthy_with_200_once_startup_completes() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let state = empty_app_state(ctx).await;
        state.health.set_ready();
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let health_response: HealthResponse =
            serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(health_response.status, "healthy");
        assert!(health_response.timestamp <= chrono::Utc::now());
        assert!(!health_response.git_commit.is_empty());
        assert!(health_response.uptime_seconds >= 0);
    }

    #[tokio::test]
    async fn logs_returns_empty_when_no_log_dir() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let result = get_log_response(&app, "/logs").await;
        assert!(result.entries.is_empty());
        assert_eq!(result.total, 0);
        assert!(!result.has_more);
    }

    #[tokio::test]
    async fn logs_returns_entries_newest_first() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_test_logs(
            temp_dir.path(),
            "st0x-hedge.log.2026-04-20",
            THREE_ENTRY_LOG,
        );

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let app = build_app(empty_app_state(ctx).await);

        let result = get_log_response(&app, "/logs").await;
        assert_eq!(result.entries.len(), 3);
        assert_eq!(result.total, 3);
        assert!(!result.has_more);
        // Newest first
        assert_eq!(result.entries[0]["message"], "Slow response");
        assert_eq!(result.entries[2]["message"], "Bot started");
    }

    #[tokio::test]
    async fn logs_respects_limit_parameter() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_test_logs(
            temp_dir.path(),
            "st0x-hedge.log.2026-04-20",
            THREE_ENTRY_LOG,
        );

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let app = build_app(empty_app_state(ctx).await);

        let result = get_log_response(&app, "/logs?limit=2").await;
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.total, 3);
        assert!(result.has_more);
        // Newest 2 entries
        assert_eq!(result.entries[0]["message"], "Slow response");
        assert_eq!(result.entries[1]["message"], "Polling");
    }

    #[tokio::test]
    async fn logs_paginates_with_offset() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_test_logs(
            temp_dir.path(),
            "st0x-hedge.log.2026-04-20",
            THREE_ENTRY_LOG,
        );

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let app = build_app(empty_app_state(ctx).await);

        // Page 1: newest 2
        let page1 = get_log_response(&app, "/logs?limit=2&offset=0").await;
        assert_eq!(page1.entries.len(), 2);
        assert!(page1.has_more);
        assert_eq!(page1.entries[0]["message"], "Slow response");

        // Page 2: older entries
        let page2 = get_log_response(&app, "/logs?limit=2&offset=2").await;
        assert_eq!(page2.entries.len(), 1);
        assert!(!page2.has_more);
        assert_eq!(page2.entries[0]["message"], "Bot started");
    }

    #[tokio::test]
    async fn logs_filters_by_search_term() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_test_logs(
            temp_dir.path(),
            "st0x-hedge.log.2026-04-20",
            THREE_ENTRY_LOG,
        );

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let app = build_app(empty_app_state(ctx).await);

        let result = get_log_response(&app, "/logs?search=slow").await;
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.total, 1);
        assert!(!result.has_more);
        assert_eq!(result.entries[0]["message"], "Slow response");
    }

    #[tokio::test]
    async fn logs_filters_by_multiple_levels() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_test_logs(
            temp_dir.path(),
            "st0x-hedge.log.2026-04-20",
            THREE_ENTRY_LOG,
        );

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let app = build_app(empty_app_state(ctx).await);

        let result = get_log_response(&app, "/logs?level=INFO,WARN").await;
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.total, 2);
        // Newest first: WARN before INFO
        assert_eq!(result.entries[0]["message"], "Slow response");
        assert_eq!(result.entries[1]["message"], "Bot started");
    }

    #[tokio::test]
    async fn logs_filters_by_time_range() {
        let temp_dir = tempfile::tempdir().unwrap();
        write_test_logs(
            temp_dir.path(),
            "st0x-hedge.log.2026-04-20",
            THREE_ENTRY_LOG,
        );

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let app = build_app(empty_app_state(ctx).await);

        // Only entries between 10:00:00 and 10:00:01 inclusive
        let result = get_log_response(
            &app,
            "/logs?since=2026-04-20T10:00:00Z&until=2026-04-20T10:00:01Z",
        )
        .await;
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.total, 2);
        assert_eq!(result.entries[0]["message"], "Polling");
        assert_eq!(result.entries[1]["message"], "Bot started");
    }

    #[tokio::test]
    async fn logs_combines_search_and_pagination() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log_content = (0..5)
            .map(|idx| {
                format!(
                    r#"{{"timestamp":"2026-04-20T10:00:0{idx}Z","level":"INFO","message":"trade {idx}"}}"#,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        write_test_logs(temp_dir.path(), "st0x-hedge.log.2026-04-20", &log_content);

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let app = build_app(empty_app_state(ctx).await);

        // Newest first: trade 4, trade 3, trade 2, trade 1, trade 0
        let page1 = get_log_response(&app, "/logs?search=trade&limit=2").await;
        assert_eq!(page1.entries.len(), 2);
        assert_eq!(page1.total, 5);
        assert!(page1.has_more);
        assert_eq!(page1.entries[0]["message"], "trade 4");
        assert_eq!(page1.entries[1]["message"], "trade 3");

        let page2 = get_log_response(&app, "/logs?search=trade&limit=2&offset=2").await;
        assert_eq!(page2.entries.len(), 2);
        assert!(page2.has_more);
        assert_eq!(page2.entries[0]["message"], "trade 2");
        assert_eq!(page2.entries[1]["message"], "trade 1");
    }

    #[tokio::test]
    async fn raindex_orders_returns_unavailable_when_rest_api_not_configured() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        assert!(ctx.rest_api.is_none());

        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/orders/raindex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(parsed["unavailable"], true);
        assert_eq!(parsed["reason"], "REST API not configured (simulate mode)");
    }

    #[tokio::test]
    async fn raindex_orders_returns_unavailable_when_upstream_unreachable() {
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.rest_api = Some(RestApiCtx::unauthenticated(
            "http://127.0.0.1:1".to_string(),
        ));

        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/orders/raindex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("response must be valid JSON even on error");

        assert_eq!(parsed["unavailable"], true);
        assert!(parsed["reason"].as_str().unwrap().contains("unreachable"),);
    }

    #[tokio::test]
    async fn raindex_orders_proxies_successful_upstream_response() {
        let mock_server = httpmock::MockServer::start();
        let upstream_body = serde_json::json!({
            "orders": [{
                "orderHash": "0xabcd",
                "owner": "0x0000000000000000000000000000000000000000",
                "inputToken": {"address": "0x1111", "symbol": "USDC", "decimals": 6},
                "outputToken": {"address": "0x2222", "symbol": "wtTSLA", "decimals": 18},
                "outputVaultBalance": "1000",
                "ioRatio": "0.5",
                "createdAt": 1_718_452_800,
                "orderbookId": "0x3333"
            }],
            "pagination": {
                "page": 1,
                "pageSize": 20,
                "totalOrders": 1,
                "totalPages": 1,
                "hasMore": false
            }
        });

        let owner = Address::ZERO;
        let expected_path = format!("/v1/orders/owner/{owner:#x}");

        mock_server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(&expected_path)
                .query_param("page", "1")
                .query_param("pageSize", "50");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(upstream_body.clone());
        });

        let mut ctx = create_test_ctx_with_order_owner(owner);
        ctx.rest_api = Some(RestApiCtx::unauthenticated(mock_server.base_url()));

        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/orders/raindex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(parsed["orders"][0]["orderHash"], "0xabcd");
        assert_eq!(parsed["pagination"]["totalOrders"], 1);
    }

    #[tokio::test]
    async fn raindex_orders_forwards_clamped_pagination_to_upstream() {
        let mock_server = httpmock::MockServer::start();
        let upstream_body = serde_json::json!({
            "orders": [],
            "pagination": {
                "page": 3,
                "pageSize": 100,
                "totalOrders": 0,
                "totalPages": 0,
                "hasMore": false
            }
        });

        let owner = Address::ZERO;
        let expected_path = format!("/v1/orders/owner/{owner:#x}");

        mock_server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(&expected_path)
                .query_param("page", "3")
                .query_param("pageSize", "100");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(upstream_body.clone());
        });

        let mut ctx = create_test_ctx_with_order_owner(owner);
        ctx.rest_api = Some(RestApiCtx::unauthenticated(mock_server.base_url()));

        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/orders/raindex?page=3&page_size=500")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(parsed["pagination"]["page"], 3);
        assert_eq!(parsed["pagination"]["pageSize"], 100);
    }

    #[tokio::test]
    async fn raindex_orders_clamps_zero_pagination_inputs_to_one() {
        let mock_server = httpmock::MockServer::start();
        let upstream_body = serde_json::json!({
            "orders": [],
            "pagination": {
                "page": 1,
                "pageSize": 1,
                "totalOrders": 0,
                "totalPages": 0,
                "hasMore": false
            }
        });

        let owner = Address::ZERO;
        let expected_path = format!("/v1/orders/owner/{owner:#x}");

        mock_server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(&expected_path)
                .query_param("page", "1")
                .query_param("pageSize", "1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(upstream_body.clone());
        });

        let mut ctx = create_test_ctx_with_order_owner(owner);
        ctx.rest_api = Some(RestApiCtx::unauthenticated(mock_server.base_url()));

        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/orders/raindex?page=0&page_size=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(parsed["pagination"]["page"], 1);
        assert_eq!(parsed["pagination"]["pageSize"], 1);
    }

    #[tokio::test]
    async fn raindex_orders_returns_unavailable_on_upstream_500() {
        let mock_server = httpmock::MockServer::start();
        let owner = Address::ZERO;

        mock_server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!("/v1/orders/owner/{owner:#x}"));
            then.status(500).body("Internal Server Error");
        });

        let mut ctx = create_test_ctx_with_order_owner(owner);
        ctx.rest_api = Some(RestApiCtx::unauthenticated(mock_server.base_url()));

        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/orders/raindex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_string(response).await;
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("response must be valid JSON on upstream error");

        assert_eq!(parsed["unavailable"], true);
        assert!(parsed["reason"].as_str().unwrap().contains("error"));
    }

    #[tokio::test]
    async fn interrupted_transfers_returns_empty_on_fresh_db() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/transfers/interrupted")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .unwrap(),
            "no-store"
        );

        let body = body_to_string(response).await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(parsed["interruptedMints"], serde_json::json!([]));
        assert_eq!(parsed["interruptedRedemptions"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn resume_transfers_returns_503_before_conductor_ready() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/transfers/resume")
                    // The operator socket: tests stand in for the
                    // in-container CLI, which connects from loopback.
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = body_to_string(response).await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(
            parsed["error"],
            "Recovery not ready yet (conductor still starting)"
        );
    }

    #[tokio::test]
    async fn recheck_transfer_returns_503_before_conductor_ready() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/transfers/recheck/equity_mint/some-id")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = body_to_string(response).await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(
            parsed["error"],
            "Recovery not ready yet (conductor still starting)"
        );
    }

    /// The bare mutation mounts exist for the in-container CLI alone. A
    /// caller that arrived over the published port carries the bridge
    /// interface's peer address, and one with no recorded peer at all is
    /// indistinguishable from that -- both must be refused before the handler
    /// runs.
    #[tokio::test]
    async fn refuses_mutation_paths_to_non_loopback_callers() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        for (peer, label) in [
            (
                Some(ConnectInfo(SocketAddr::from(([172, 18, 0, 1], 9)))),
                "bridge peer",
            ),
            (None, "no recorded peer"),
        ] {
            let mut request = Request::builder().method("POST").uri("/transfers/resume");
            if let Some(info) = peer {
                request = request.extension(info);
            }

            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{label}");
        }
    }

    /// Pins the production serve wiring: `serve_with_peer_info` must record
    /// the TCP peer, or the loopback gate fails closed and the in-container
    /// CLI's recovery verbs 403 in production while every hand-injected
    /// ConnectInfo test stays green. This is the one test that goes through a
    /// real socket instead of injecting the extension.
    #[tokio::test]
    async fn real_listener_supplies_peer_info_to_the_loopback_gate() {
        let app = axum::Router::new()
            .route("/op", axum::routing::post(|| async { "ok" }))
            .layer(axum::middleware::from_fn(require_loopback));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("ephemeral port binds");
        let addr = listener.local_addr().expect("bound socket has an address");
        tokio::spawn(crate::serve_with_peer_info(listener, app));

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/op"))
            .send()
            .await
            .expect("server reachable");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }

    /// `routes(None)` must leave the role prefixes unmounted entirely: a
    /// deployment with no load balancer serves 404, never a 401 that suggests
    /// the path exists and wants credentials.
    #[tokio::test]
    async fn role_prefixes_are_unmounted_without_ops_api_config() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let app = build_app(empty_app_state(ctx).await);

        for (method, uri) in [
            ("GET", "/liquidity-read/transfers/interrupted"),
            ("GET", "/liquidity-read/pnl"),
            ("GET", "/liquidity-read/health"),
            ("POST", "/liquidity-write/transfers/recheck/equity_mint/x"),
            ("POST", "/liquidity-write/transfers/resume"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
        }
    }

    /// With audiences configured the role prefixes ARE mounted, and a request
    /// without an IAP assertion is refused by the middleware (401), not lost
    /// to routing (404).
    #[tokio::test]
    async fn role_prefixes_demand_iap_when_configured() {
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);
        let ops_api = st0x_config::OpsApiConfig {
            read_audience: "/projects/1/global/backendServices/11".to_string(),
            write_audience: "/projects/1/global/backendServices/22".to_string(),
        };
        let app = routes(Some(&ops_api)).with_state(empty_app_state(ctx).await);

        for (method, uri) in [
            ("GET", "/liquidity-read/transfers/interrupted"),
            ("GET", "/liquidity-read/pnl"),
            ("GET", "/liquidity-read/health"),
            ("POST", "/liquidity-write/transfers/recheck/equity_mint/x"),
            ("POST", "/liquidity-write/transfers/resume"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {uri}"
            );
        }
    }

    #[test]
    fn recheck_error_response_distinguishes_recoverability() {
        // Not-recoverable: the persisted aggregate state forbids recovery, so
        // retrying will not help -> 422 carrying the typed reason.
        let mint_id = issuer_request_id("mint-1");
        let (status, message) =
            recheck_error_response(&RecheckError::NoAcceptedRequest(mint_id.clone()));
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            message,
            format!("mint {mint_id} has no accepted provider request to re-check")
        );

        let (status, _) = recheck_error_response(&RecheckError::MissingTxHash(
            tokenization_request_id("tok-1"),
        ));
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        let parse_error = "not-an-address".parse::<Address>().unwrap_err();
        let (status, _) = recheck_error_response(&RecheckError::MalformedWallet {
            id: mint_id,
            source: parse_error,
        });
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        // Transient upstream provider failure -> 502 so the operator knows to
        // retry, with a generic message that does not leak provider internals.
        let (status, message) = recheck_error_response(&RecheckError::Tokenizer(
            TokenizerError::MintVerification(MintVerificationError::ReceiptNotFound {
                tx_hash: alloy::primitives::TxHash::random(),
            }),
        ));
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(message, "Tokenization provider unavailable; retry later");

        // Genuinely internal failure -> 500 with a generic body.
        let (status, message) =
            recheck_error_response(&RecheckError::Database(sqlx::Error::RowNotFound));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(message, "Failed to recheck transfer");
    }
}
