//! HTTP API endpoints for health checks, log retrieval, and order status.

use std::str::FromStr;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use rocket::form::{self, FromFormField, ValueField};
use rocket::http::Status;
use rocket::request::FromParam;
use rocket::response::content::RawJson;
use rocket::serde::json::Json;
use rocket::serde::{Deserialize, Serialize};
use rocket::{Route, State, get, routes};
use sqlx::SqlitePool;

use crate::config::Ctx;
use crate::dashboard::transfer_loader::TransferKind;

impl<'a> FromParam<'a> for TransferKind {
    type Error = String;

    fn from_param(param: &'a str) -> Result<Self, Self::Error> {
        param.parse()
    }
}

/// Comma-separated filter for transfer kinds in query parameters.
///
/// Parses `"equity_mint,usdc_bridge"` into `vec![EquityMint, UsdcBridge]`.
struct TransferKindFilter(Vec<TransferKind>);

impl<'v> FromFormField<'v> for TransferKindFilter {
    fn from_value(field: ValueField<'v>) -> form::Result<'v, Self> {
        let kinds: Result<Vec<TransferKind>, _> = field
            .value
            .split(',')
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .map(TransferKind::from_str)
            .collect();

        kinds
            .map(TransferKindFilter)
            .map_err(|error| form::Error::validation(error).into())
    }
}

static STARTED_AT: LazyLock<DateTime<Utc>> = LazyLock::new(Utc::now);

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

#[get("/health")]
fn health() -> Json<HealthResponse> {
    let uptime = Utc::now() - *STARTED_AT;

    Json(HealthResponse {
        status: "healthy".to_string(),
        timestamp: Utc::now(),
        git_commit: GIT_COMMIT.to_string(),
        uptime_seconds: uptime.num_seconds(),
    })
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
#[get("/logs?<limit>&<offset>&<search>&<level>&<target>&<since>&<until>")]
fn logs(
    ctx: &State<Ctx>,
    limit: Option<usize>,
    offset: Option<usize>,
    search: Option<&str>,
    level: Option<&str>,
    target: Option<&str>,
    since: Option<&str>,
    until: Option<&str>,
) -> Json<LogResponse> {
    let limit = limit.unwrap_or(100).min(5000);
    let offset = offset.unwrap_or(0);

    let Some(ref log_dir) = ctx.log_dir else {
        return Json(LogResponse {
            entries: Vec::new(),
            total: 0,
            has_more: false,
        });
    };

    let filter = LogFilter {
        search_lower: search
            .filter(|query| !query.is_empty())
            .map(str::to_lowercase),
        levels: level.filter(|lvl| !lvl.is_empty()).map(|lvl| {
            lvl.split(',')
                .map(|part| part.trim().to_uppercase())
                .collect()
        }),
        targets: target.filter(|tgt| !tgt.is_empty()).map(|tgt| {
            tgt.split(',')
                .map(|part| part.trim().to_lowercase())
                .collect()
        }),
        since: since.and_then(|val| {
            DateTime::parse_from_rfc3339(val)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        }),
        until: until.and_then(|val| {
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
#[get("/orders/pending")]
async fn pending_orders(pool: &State<SqlitePool>) -> Json<Vec<PendingOrderResponse>> {
    let rows: Vec<(String, String, String)> = match sqlx::query_as(
        "SELECT view_id, status, payload FROM offchain_order_view \
         WHERE status IN ('Pending', 'Submitted', 'PartiallyFilled') \
         ORDER BY rowid DESC LIMIT 100",
    )
    .fetch_all(pool.inner())
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

/// Paginated trade history from both onchain and offchain fills.
///
/// Returns newest-first. Supports filtering by symbol, venue, and time range.
#[get("/trades?<limit>&<offset>&<symbol>&<venue>&<since>&<until>")]
async fn trades(
    pool: &State<SqlitePool>,
    limit: Option<usize>,
    offset: Option<usize>,
    symbol: Option<&str>,
    venue: Option<&str>,
    since: Option<&str>,
    until: Option<&str>,
) -> Json<TradeResponse> {
    let limit = limit.unwrap_or(100).min(500);
    let offset = offset.unwrap_or(0);

    let since_dt = since.and_then(|val| {
        DateTime::parse_from_rfc3339(val)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });
    let until_dt = until.and_then(|val| {
        DateTime::parse_from_rfc3339(val)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    let venues = venue.filter(|val| !val.is_empty()).map(|val| {
        val.split(',')
            .map(|part| part.trim().to_string())
            .collect::<Vec<_>>()
    });

    let symbols = symbol.filter(|val| !val.is_empty()).map(|val| {
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

    let mut all_trades = load_trade_rows(pool.inner(), &trade_filter).await;
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

            let direction = filled["direction"].as_str().unwrap_or("Buy");
            let amount = filled["amount"].as_str().unwrap_or("0");

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

            let direction = filled["direction"].as_str().unwrap_or("Buy");
            let shares = filled["shares"].as_str().unwrap_or("0");

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

/// Paginated transfer history using event-sourced aggregate replay.
///
/// Replays transfer aggregates to produce proper DTO statuses, then
/// applies time-range filtering and pagination.
#[get("/transfers?<limit>&<offset>&<kind>&<since>&<until>")]
async fn transfers_endpoint(
    pool: &State<SqlitePool>,
    limit: Option<usize>,
    offset: Option<usize>,
    kind: Option<TransferKindFilter>,
    since: Option<&str>,
    until: Option<&str>,
) -> Result<Json<serde_json::Value>, Status> {
    let limit = limit.unwrap_or(100).min(500);
    let offset = offset.unwrap_or(0);

    let since_dt = since.and_then(|val| {
        DateTime::parse_from_rfc3339(val)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });
    let until_dt = until.and_then(|val| {
        DateTime::parse_from_rfc3339(val)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    let kind_filter = kind.map(|filter| filter.0);

    let loaded = crate::dashboard::transfer_loader::load_all_transfer_operations(
        pool.inner(),
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
            Status::InternalServerError
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
#[get("/transfers/<kind>/<aggregate_id>/events")]
async fn transfer_events(
    pool: &State<SqlitePool>,
    kind: TransferKind,
    aggregate_id: &str,
) -> Option<Json<serde_json::Value>> {
    let aggregate_type = kind.aggregate_type();

    let rows: Vec<(String, String, i64)> = match sqlx::query_as(
        "SELECT event_type, payload, sequence \
         FROM events \
         WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
         ORDER BY sequence ASC",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .fetch_all(pool.inner())
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
            return None;
        }
    };

    let events: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(event_type, payload, sequence)| {
            let step = event_type.split("::").last().unwrap_or("Unknown");
            let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap_or_default();

            // Unwrap the variant key: {"MintRequested": {fields...}} -> {fields...}
            let inner = parsed
                .as_object()
                .and_then(|obj| obj.get(step).cloned())
                .unwrap_or(parsed);

            serde_json::json!({
                "step": step,
                "sequence": sequence,
                "payload": inner,
            })
        })
        .collect();

    Some(Json(serde_json::json!({ "events": events })))
}

/// Returns the full event history for a single trade aggregate.
///
/// For onchain trades (Raindex), returns Filled + optional Enriched events.
/// For offchain trades (Alpaca), returns the full order lifecycle
/// (Placed -> Submitted -> PartiallyFilled -> Filled/Failed).
#[get("/trades/<venue>/<aggregate_id>/events")]
async fn trade_events(
    pool: &State<SqlitePool>,
    venue: &str,
    aggregate_id: &str,
) -> Option<Json<serde_json::Value>> {
    let aggregate_type = match venue {
        "raindex" => "OnChainTrade",
        "alpaca" | "dry_run" => "OffchainOrder",
        _ => return None,
    };

    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT event_type, payload, sequence \
         FROM events \
         WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
         ORDER BY sequence ASC",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .fetch_all(pool.inner())
    .await
    .ok()?;

    let events: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(event_type, payload, sequence)| {
            let step = event_type.split("::").last().unwrap_or("Unknown");
            let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap_or_default();

            let inner = parsed
                .as_object()
                .and_then(|obj| obj.get(step).cloned())
                .unwrap_or(parsed);

            serde_json::json!({
                "step": step,
                "sequence": sequence,
                "payload": inner,
            })
        })
        .collect();

    Some(Json(serde_json::json!({ "events": events })))
}

fn unavailable_json(reason: &str) -> RawJson<String> {
    RawJson(
        serde_json::json!({
            "unavailable": true,
            "reason": reason,
        })
        .to_string(),
    )
}

/// Proxies the bot's active Raindex orders from the st0x REST API.
/// When `[rest_api]` is not configured, returns an unavailable indicator
/// so the dashboard can show a friendly message instead of an error.
#[get("/orders/raindex")]
#[allow(clippy::cognitive_complexity)]
async fn raindex_orders(ctx: &State<Ctx>) -> RawJson<String> {
    let Some(rest_api) = &ctx.rest_api else {
        return unavailable_json("REST API not configured (simulate mode)");
    };

    let owner = ctx.order_owner();
    let url = format!(
        "{}/v1/orders/owner/{:#x}",
        rest_api.url.trim_end_matches('/'),
        owner
    );

    let mut request = rest_api.http_client.get(&url);

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
        Ok(body) => RawJson(body),
        Err(error) => {
            tracing::warn!(target: "dashboard", %error, "Failed to read st0x REST API response body");
            unavailable_json("Failed to read REST API response")
        }
    }
}

pub(crate) fn routes() -> Vec<Route> {
    routes![
        health,
        logs,
        pending_orders,
        trades,
        trade_events,
        transfers_endpoint,
        transfer_events,
        raindex_orders
    ]
}

#[cfg(test)]
mod tests {
    use rocket::local::asynchronous::Client;

    use super::*;
    use crate::config::tests::create_test_ctx_with_order_owner;

    #[test]
    fn test_num_of_routes() {
        let routes_list = routes();
        assert_eq!(routes_list.len(), 8);
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        let rocket = rocket::build()
            .mount("/", routes![health, logs])
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/health").dispatch().await;
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let health_response: HealthResponse =
            serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(health_response.status, "healthy");
        assert!(health_response.timestamp <= chrono::Utc::now());
        assert!(!health_response.git_commit.is_empty());
        assert!(health_response.uptime_seconds >= 0);
    }

    fn make_test_rocket(ctx: Ctx) -> rocket::Rocket<rocket::Build> {
        rocket::build()
            .mount("/", routes![health, logs])
            .manage(ctx)
    }

    async fn get_log_response(client: &Client, uri: &str) -> LogResponse {
        let response = client.get(uri).dispatch().await;
        assert_eq!(response.status(), Status::Ok);
        let body = response.into_string().await.expect("response body");
        serde_json::from_str(&body).expect("valid LogResponse JSON")
    }

    fn write_test_logs(dir: &std::path::Path, filename: &str, content: &str) {
        std::fs::write(dir.join(filename), content).unwrap();
    }

    const THREE_ENTRY_LOG: &str = r#"{"timestamp":"2026-04-20T10:00:00Z","level":"INFO","target":"st0x_hedge","message":"Bot started"}
{"timestamp":"2026-04-20T10:00:01Z","level":"DEBUG","target":"st0x_hedge","message":"Polling"}
{"timestamp":"2026-04-20T10:00:02Z","level":"WARN","target":"st0x_hedge","message":"Slow response"}"#;

    #[tokio::test]
    async fn logs_returns_empty_when_no_log_dir() {
        let ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        let client = Client::tracked(make_test_rocket(ctx))
            .await
            .expect("valid rocket instance");

        let result = get_log_response(&client, "/logs").await;
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

        let mut ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let client = Client::tracked(make_test_rocket(ctx))
            .await
            .expect("valid rocket instance");

        let result = get_log_response(&client, "/logs").await;
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

        let mut ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let client = Client::tracked(make_test_rocket(ctx))
            .await
            .expect("valid rocket instance");

        let result = get_log_response(&client, "/logs?limit=2").await;
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

        let mut ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let client = Client::tracked(make_test_rocket(ctx))
            .await
            .expect("valid rocket instance");

        // Page 1: newest 2
        let page1 = get_log_response(&client, "/logs?limit=2&offset=0").await;
        assert_eq!(page1.entries.len(), 2);
        assert!(page1.has_more);
        assert_eq!(page1.entries[0]["message"], "Slow response");

        // Page 2: older entries
        let page2 = get_log_response(&client, "/logs?limit=2&offset=2").await;
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

        let mut ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let client = Client::tracked(make_test_rocket(ctx))
            .await
            .expect("valid rocket instance");

        let result = get_log_response(&client, "/logs?search=slow").await;
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

        let mut ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let client = Client::tracked(make_test_rocket(ctx))
            .await
            .expect("valid rocket instance");

        let result = get_log_response(&client, "/logs?level=INFO,WARN").await;
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

        let mut ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let client = Client::tracked(make_test_rocket(ctx))
            .await
            .expect("valid rocket instance");

        // Only entries between 10:00:00 and 10:00:01 inclusive
        let result = get_log_response(
            &client,
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

        let mut ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.log_dir = Some(temp_dir.path().to_str().unwrap().to_string());

        let client = Client::tracked(make_test_rocket(ctx))
            .await
            .expect("valid rocket instance");

        // Newest first: trade 4, trade 3, trade 2, trade 1, trade 0
        let page1 = get_log_response(&client, "/logs?search=trade&limit=2").await;
        assert_eq!(page1.entries.len(), 2);
        assert_eq!(page1.total, 5);
        assert!(page1.has_more);
        assert_eq!(page1.entries[0]["message"], "trade 4");
        assert_eq!(page1.entries[1]["message"], "trade 3");

        let page2 = get_log_response(&client, "/logs?search=trade&limit=2&offset=2").await;
        assert_eq!(page2.entries.len(), 2);
        assert!(page2.has_more);
        assert_eq!(page2.entries[0]["message"], "trade 2");
        assert_eq!(page2.entries[1]["message"], "trade 1");
    }

    #[tokio::test]
    async fn raindex_orders_returns_unavailable_when_rest_api_not_configured() {
        let ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        assert!(ctx.rest_api.is_none());

        let rocket = rocket::build()
            .mount("/", routes![raindex_orders])
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/orders/raindex").dispatch().await;
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(parsed["unavailable"], true);
        assert_eq!(parsed["reason"], "REST API not configured (simulate mode)");
    }

    #[tokio::test]
    async fn raindex_orders_returns_unavailable_when_upstream_unreachable() {
        let mut ctx = create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO);
        ctx.rest_api = Some(crate::config::RestApiCtx::unauthenticated(
            "http://127.0.0.1:1".to_string(),
        ));

        let rocket = rocket::build()
            .mount("/", routes![raindex_orders])
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/orders/raindex").dispatch().await;
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
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

        let owner = alloy::primitives::Address::ZERO;
        let expected_path = format!("/v1/orders/owner/{owner:#x}");

        mock_server.mock(|when, then| {
            when.method(httpmock::Method::GET).path(&expected_path);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(upstream_body.clone());
        });

        let mut ctx = create_test_ctx_with_order_owner(owner);
        ctx.rest_api = Some(crate::config::RestApiCtx::unauthenticated(
            mock_server.base_url(),
        ));

        let rocket = rocket::build()
            .mount("/", routes![raindex_orders])
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/orders/raindex").dispatch().await;
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON response");

        assert_eq!(parsed["orders"][0]["orderHash"], "0xabcd");
        assert_eq!(parsed["pagination"]["totalOrders"], 1);
    }

    #[tokio::test]
    async fn raindex_orders_returns_unavailable_on_upstream_500() {
        let mock_server = httpmock::MockServer::start();
        let owner = alloy::primitives::Address::ZERO;

        mock_server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!("/v1/orders/owner/{owner:#x}"));
            then.status(500).body("Internal Server Error");
        });

        let mut ctx = create_test_ctx_with_order_owner(owner);
        ctx.rest_api = Some(crate::config::RestApiCtx::unauthenticated(
            mock_server.base_url(),
        ));

        let rocket = rocket::build()
            .mount("/", routes![raindex_orders])
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/orders/raindex").dispatch().await;
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("response must be valid JSON on upstream error");

        assert_eq!(parsed["unavailable"], true);
        assert!(parsed["reason"].as_str().unwrap().contains("error"));
    }
}
