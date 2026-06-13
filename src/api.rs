//! HTTP API endpoints for health checks, log retrieval, and order status.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;

use alloy::primitives::U256;
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use st0x_dto::TradingVenue;
use st0x_execution::FractionalShares;

use crate::AppState;
use crate::dashboard::transfer_loader::{InvalidTransferKind, TransferKind};
use crate::equity_redemption::{EquityRedemptionEvent, RedemptionAggregateId};
use crate::rebalancing::RebalancingService;
use crate::rebalancing::equity::{CrossVenueEquityTransfer, RecheckError, RecheckOutcome};
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizedEquityMintEvent};

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

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct TradeEntry {
    id: String,
    filled_at: String,
    venue: String,
    direction: String,
    symbol: String,
    shares: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradeResponse {
    entries: Vec<TradeEntry>,
    total: usize,
    has_more: bool,
}

async fn health() -> Json<HealthResponse> {
    let uptime = Utc::now() - *STARTED_AT;

    Json(HealthResponse {
        status: "healthy".to_string(),
        timestamp: Utc::now(),
        git_commit: GIT_COMMIT.to_string(),
        uptime_seconds: uptime.num_seconds(),
    })
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

/// Returns non-terminal offchain orders (Pending, Submitted, PartiallyFilled).
async fn pending_orders(State(state): State<AppState>) -> Json<Vec<PendingOrderResponse>> {
    let rows: Vec<(String, String, String)> = match sqlx::query_as(
        "SELECT view_id, status, payload FROM offchain_order_view \
         WHERE status IN ('Pending', 'Submitted', 'PartiallyFilled') \
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
}

/// Paginated trade history from both onchain and offchain fills.
///
/// Returns newest-first. Supports filtering by symbol, venue, and time range.
async fn trades(
    State(state): State<AppState>,
    Query(query): Query<TradesQuery>,
) -> Json<TradeResponse> {
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

    let venues = query
        .venue
        .as_deref()
        .filter(|val| !val.is_empty())
        .map(|val| {
            val.split(',')
                .map(|part| part.trim().to_string())
                .collect::<Vec<_>>()
        });

    let symbols = query
        .symbol
        .as_deref()
        .filter(|val| !val.is_empty())
        .map(|val| {
            val.split(',')
                .map(|part| part.trim().to_string())
                .collect::<Vec<_>>()
        });

    let trade_filter = TradeFilter {
        symbols,
        venues,
        since: since_dt,
        until: until_dt,
    };

    let mut all_trades = load_trade_rows(&state.pool, &trade_filter).await;
    all_trades.sort_by(|lhs, rhs| rhs.filled_at.cmp(&lhs.filled_at));

    let total = all_trades.len();
    let end = total.min(offset + limit);
    let entries = all_trades[offset.min(total)..end].to_vec();
    let has_more = end < total;

    Json(TradeResponse {
        entries,
        total,
        has_more,
    })
}

struct TradeFilter {
    symbols: Option<Vec<String>>,
    venues: Option<Vec<String>>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
}

impl TradeFilter {
    fn matches_symbol(&self, symbol: &str) -> bool {
        self.symbols
            .as_ref()
            .is_none_or(|syms| syms.iter().any(|sym| sym == symbol))
    }

    fn matches_time(&self, timestamp: DateTime<Utc>) -> bool {
        if let Some(since) = self.since
            && timestamp < since
        {
            return false;
        }
        if let Some(until) = self.until
            && timestamp > until
        {
            return false;
        }
        true
    }

    fn matches_venue(&self, venue: &str) -> bool {
        self.venues
            .as_ref()
            .is_none_or(|vals| vals.iter().any(|val| val == venue))
    }
}

async fn load_trade_rows(pool: &SqlitePool, filter: &TradeFilter) -> Vec<TradeEntry> {
    let include_onchain = filter.matches_venue("raindex");
    let include_offchain = filter.matches_venue("alpaca")
        || filter.matches_venue("dry_run")
        || filter.venues.is_none();

    let mut trades = Vec::new();

    if include_onchain {
        trades.extend(load_onchain_trade_rows(pool, filter).await);
    }

    if include_offchain {
        trades.extend(load_offchain_trade_rows(pool, filter).await);
    }

    trades
}

async fn load_onchain_trade_rows(pool: &SqlitePool, filter: &TradeFilter) -> Vec<TradeEntry> {
    let rows: Vec<(String, String)> = match sqlx::query_as(
        "SELECT aggregate_id, payload FROM events \
         WHERE event_type = 'OnChainTradeEvent::Filled' \
         ORDER BY rowid DESC",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(target: "dashboard", %error, "Failed to load onchain trades");
            return Vec::new();
        }
    };

    rows.into_iter()
        .filter_map(|(aggregate_id, payload_str)| {
            let payload: serde_json::Value = serde_json::from_str(&payload_str).ok()?;
            let filled = payload.get("Filled")?;

            let trade_symbol = filled["symbol"].as_str()?;
            let filled_at_str = filled["block_timestamp"].as_str()?;
            let filled_at = DateTime::parse_from_rfc3339(filled_at_str)
                .ok()?
                .with_timezone(&Utc);

            if !filter.matches_symbol(trade_symbol) || !filter.matches_time(filled_at) {
                return None;
            }

            let direction = filled["direction"].as_str()?;
            let amount = filled["amount"].as_str()?;

            Some(TradeEntry {
                id: aggregate_id,
                filled_at: filled_at.to_rfc3339(),
                venue: "raindex".to_string(),
                direction: direction.to_string(),
                symbol: trade_symbol.to_string(),
                shares: amount.to_string(),
            })
        })
        .collect()
}

async fn load_offchain_trade_rows(pool: &SqlitePool, filter: &TradeFilter) -> Vec<TradeEntry> {
    let rows: Vec<(String, String)> = match sqlx::query_as(
        "SELECT view_id, payload FROM offchain_order_view \
         WHERE status = 'Filled'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(target: "dashboard", %error, "Failed to load offchain trades");
            return Vec::new();
        }
    };

    rows.into_iter()
        .filter_map(|(view_id, payload_str)| {
            let payload: serde_json::Value = serde_json::from_str(&payload_str).ok()?;
            let filled = payload.get("Live")?.get("Filled")?;

            let trade_symbol = filled["symbol"].as_str()?;
            let filled_at_str = filled["filled_at"].as_str()?;
            let filled_at = DateTime::parse_from_rfc3339(filled_at_str)
                .ok()?
                .with_timezone(&Utc);

            if !filter.matches_symbol(trade_symbol) || !filter.matches_time(filled_at) {
                return None;
            }

            let executor = filled["executor"].as_str().unwrap_or("AlpacaBrokerApi");
            let venue = match executor {
                "DryRun" => "dry_run",
                _ => "alpaca",
            };

            if !filter.matches_venue(venue) {
                return None;
            }

            let direction = filled["direction"].as_str()?;
            let shares = filled["shares"].as_str()?;

            Some(TradeEntry {
                id: view_id,
                filled_at: filled_at.to_rfc3339(),
                venue: venue.to_string(),
                direction: direction.to_string(),
                symbol: trade_symbol.to_string(),
                shares: shares.to_string(),
            })
        })
        .collect()
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
            // Neither strands equity: a rejected mint never left the issuer,
            // and a deposited mint completed successfully.
            MintRejected { .. } | DepositedIntoRaindex { .. } => return None,
            // Recovery un-failed the mint and put it back on the success path,
            // so any stuck state observed before it no longer applies; the
            // mint is re-derived from subsequent events (it may fail again).
            ProviderCompletionRecovered { .. } => terminal = None,
            MintAcceptanceFailed { .. }
            | TokensReceived { .. }
            | WrapSubmitted { .. }
            | TokensWrapped { .. }
            | VaultDepositSubmitted { .. } => {}
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
            // Completed) means nothing is stranded.
            TransferFailed { .. } | Completed { .. } | ProviderCompletionRecovered { .. } => {
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
/// For onchain trades (Raindex), returns Filled + optional Enriched events.
/// For offchain trades (Alpaca), returns the full order lifecycle
/// (Placed -> Submitted -> PartiallyFilled -> Filled/Failed).
async fn trade_events(
    State(state): State<AppState>,
    Path((venue_str, aggregate_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let venue = TradingVenue::from_str(&venue_str).map_err(|_| StatusCode::NOT_FOUND)?;
    let aggregate_type = match venue {
        TradingVenue::Raindex => "OnChainTrade",
        TradingVenue::Alpaca | TradingVenue::DryRun => "OffchainOrder",
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

    let owner = state.ctx.order_owner();
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
) -> Result<Json<InterruptedTransfersResponse>, (StatusCode, Json<ErrorResponse>)> {
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

    Ok(Json(InterruptedTransfersResponse {
        interrupted_mints: mints.into_iter().map(|id| id.to_string()).collect(),
        interrupted_redemptions: redemptions.into_iter().map(|id| id.to_string()).collect(),
    }))
}

/// Wire contract for `/transfers/resume`, shared with the CLI wrapper so the
/// two cannot drift.
#[derive(Serialize, serde::Deserialize)]
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
/// TODO(auth): like `/transfers/resume`, this mutation endpoint is currently
/// unauthenticated. The `/transfers/*` mutation endpoints need an auth guard
/// before the listener is exposed beyond the operator network -- tracked as a
/// follow-up. Until then, deployment must keep the bind address firewalled.
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
            handle
                .transfer
                .recover_mint(
                    &IssuerRequestId::new(&id),
                    &state.pool,
                    &handle.rebalancing_service,
                )
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
    match error {
        RecheckError::NoAcceptedRequest(_)
        | RecheckError::MissingTxHash(_)
        | RecheckError::MalformedWallet { .. } => {
            (StatusCode::UNPROCESSABLE_ENTITY, error.to_string())
        }
        RecheckError::Tokenizer(_) => (
            StatusCode::BAD_GATEWAY,
            "Tokenization provider unavailable; retry later".to_string(),
        ),
        RecheckError::Mint(_)
        | RecheckError::Redemption(_)
        | RecheckError::Rebalancing(_)
        | RecheckError::Database(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to recheck transfer".to_string(),
        ),
    }
}

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
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
        .route("/transfers/resume", post(resume_transfers))
        .route("/transfers/recheck/{kind}/{id}", post(recheck_transfer))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::Address;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use sqlx::SqlitePool;
    use tokio::sync::broadcast;
    use tower::ServiceExt;

    use st0x_config::{Ctx, RestApiCtx, create_test_ctx_with_order_owner};

    use super::*;
    use crate::dashboard;
    use crate::inventory::{self, BroadcastingInventory};

    async fn empty_app_state(ctx: Ctx) -> AppState {
        let (sender, _) = broadcast::channel(16);
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        AppState {
            settings: dashboard::settings_from_ctx(&ctx),
            ctx,
            pool,
            event_sender: sender.clone(),
            inventory: Arc::new(BroadcastingInventory::new(
                inventory::InventoryView::default(),
                sender,
            )),
            recovery: Arc::new(tokio::sync::OnceCell::new()),
            resume_lock: Arc::new(ResumeLock(Mutex::new(()))),
        }
    }

    #[test]
    fn stuck_mint_info_reports_accepted_provider_failure() {
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
        ];

        let stuck = stuck_transfer_info(TransferKind::EquityMint, &rows).expect("stuck amount");

        assert_eq!(stuck.amount, "12.5");
        assert_eq!(stuck.location, StuckLocation::Issuer);
        assert_eq!(stuck.reason, StuckReason::MintAcceptanceFailed);
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
        routes().with_state(state)
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

    fn write_test_logs(dir: &std::path::Path, filename: &str, content: &str) {
        std::fs::write(dir.join(filename), content).unwrap();
    }

    const THREE_ENTRY_LOG: &str = r#"{"timestamp":"2026-04-20T10:00:00Z","level":"INFO","target":"st0x_hedge","message":"Bot started"}
{"timestamp":"2026-04-20T10:00:01Z","level":"DEBUG","target":"st0x_hedge","message":"Polling"}
{"timestamp":"2026-04-20T10:00:02Z","level":"WARN","target":"st0x_hedge","message":"Slow response"}"#;

    #[tokio::test]
    async fn test_health_endpoint() {
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

    #[test]
    fn recheck_error_response_distinguishes_recoverability() {
        use crate::tokenization::{MintVerificationError, TokenizerError};
        use crate::tokenized_equity_mint::TokenizationRequestId;

        // Not-recoverable: the persisted aggregate state forbids recovery, so
        // retrying will not help -> 422 carrying the typed reason.
        let (status, message) = recheck_error_response(&RecheckError::NoAcceptedRequest(
            IssuerRequestId::new("mint-1"),
        ));
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            message,
            "mint mint-1 has no accepted provider request to re-check"
        );

        let (status, _) = recheck_error_response(&RecheckError::MissingTxHash(
            TokenizationRequestId("tok-1".to_string()),
        ));
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        let parse_error = "not-an-address".parse::<Address>().unwrap_err();
        let (status, _) = recheck_error_response(&RecheckError::MalformedWallet {
            id: IssuerRequestId::new("mint-1"),
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
