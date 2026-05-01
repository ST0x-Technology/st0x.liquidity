//! HTTP API endpoints for health checks, log retrieval, and order status.

use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use rocket::response::content::RawJson;
use rocket::serde::json::Json;
use rocket::serde::{Deserialize, Serialize};
use rocket::{Route, State, get, routes};
use sqlx::SqlitePool;

use crate::config::Ctx;

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
/// - `since`: ISO 8601 UTC lower bound (inclusive)
/// - `until`: ISO 8601 UTC upper bound (inclusive)
#[get("/logs?<limit>&<offset>&<search>&<level>&<since>&<until>")]
fn logs(
    ctx: &State<Ctx>,
    limit: Option<usize>,
    offset: Option<usize>,
    search: Option<&str>,
    level: Option<&str>,
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
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
}

/// Reads log entries from `log_dir` in newest-first order, applying
/// filters. Iterates files in reverse (newest first) and only retains
/// entries within the requested page window to avoid loading the entire
/// log history into memory at once.
fn read_matching_entries(
    log_dir: &str,
    filter: &LogFilter,
    offset: usize,
    limit: usize,
) -> (Vec<serde_json::Value>, usize, bool) {
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

    // Sort by name then reverse so newest files are read first
    log_files.sort_by_key(std::fs::DirEntry::file_name);
    log_files.reverse();

    let page_start = offset;
    let page_end = offset + limit;
    let mut total: usize = 0;
    let mut page_entries: Vec<serde_json::Value> = Vec::new();

    for file_entry in log_files {
        let Ok(content) = std::fs::read_to_string(file_entry.path()) else {
            continue;
        };

        // Collect matching entries from this file, then reverse so
        // newest lines (at the end of the file) come first.
        let mut file_matches: Vec<serde_json::Value> = Vec::new();

        for line in content.lines() {
            if let Some(query) = &filter.search_lower
                && !line.to_lowercase().contains(query.as_str())
            {
                continue;
            }

            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };

            if let Some(levels) = &filter.levels {
                let entry_level = value["level"].as_str().unwrap_or("").to_uppercase();
                if !levels.contains(&entry_level) {
                    continue;
                }
            }

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
            if total >= page_start && page_entries.len() < limit {
                page_entries.push(entry);
            }

            total += 1;
        }
    }

    let has_more = total > page_end;

    (page_entries, total, has_more)
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
            tracing::warn!(%error, "Failed to load pending orders");
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
            tracing::warn!(%error, "Failed to load onchain trades");
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
            tracing::warn!(%error, "Failed to load offchain trades");
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

/// Paginated transfer history via direct SQL on the events table.
///
/// Extracts timestamps from event payloads and status from the latest
/// event type. No event replay needed — purely SQL-based for performance.
#[get("/transfers?<limit>&<offset>&<kind>&<since>&<until>")]
async fn transfers_endpoint(
    pool: &State<SqlitePool>,
    limit: Option<usize>,
    offset: Option<usize>,
    kind: Option<&str>,
    since: Option<&str>,
    until: Option<&str>,
) -> Json<serde_json::Value> {
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

    let kinds: Option<Vec<String>> = kind
        .filter(|val| !val.is_empty())
        .map(|val| val.split(',').map(|part| part.trim().to_string()).collect());

    // Each aggregate type stores its "started" timestamp in the first
    // event payload under a type-specific JSON path.
    let type_configs: Vec<(&str, &str, &str)> = vec![
        (
            "TokenizedEquityMint",
            "equity_mint",
            "$.MintRequested.requested_at",
        ),
        (
            "EquityRedemption",
            "equity_redemption",
            "$.WithdrawnFromRaindex.withdrawn_at",
        ),
        (
            "UsdcRebalance",
            "usdc_bridge",
            "$.ConversionInitiated.initiated_at",
        ),
    ];

    let mut all_entries: Vec<serde_json::Value> = Vec::new();

    for (aggregate_type, kind_label, started_path) in &type_configs {
        if let Some(ref kind_filter) = kinds
            && !kind_filter.iter().any(|val| val == *kind_label)
        {
            continue;
        }

        let rows = load_transfer_summary(pool.inner(), aggregate_type, started_path).await;

        for row in rows {
            let started_at = row.started_at.as_deref().unwrap_or("");

            if let Some(ref since) = since_dt
                && let Ok(ts) = DateTime::parse_from_rfc3339(started_at)
                && ts.with_timezone(&Utc) < *since
            {
                continue;
            }
            if let Some(ref until) = until_dt
                && let Ok(ts) = DateTime::parse_from_rfc3339(started_at)
                && ts.with_timezone(&Utc) > *until
            {
                continue;
            }

            let status = row.latest_event.split("::").last().unwrap_or("Unknown");

            all_entries.push(serde_json::json!({
                "id": row.aggregate_id,
                "kind": kind_label,
                "status": { "status": status },
                "startedAt": started_at,
                "updatedAt": row.updated_at.as_deref().unwrap_or(started_at),
            }));
        }
    }

    // Sort newest first by startedAt
    all_entries.sort_by(|lhs, rhs| {
        let lhs_ts = lhs["startedAt"].as_str().unwrap_or("");
        let rhs_ts = rhs["startedAt"].as_str().unwrap_or("");
        rhs_ts.cmp(lhs_ts)
    });

    let filtered_total = all_entries.len();
    let end = filtered_total.min(offset + limit);
    let entries = &all_entries[offset.min(filtered_total)..end];
    let has_more = end < filtered_total;

    Json(serde_json::json!({
        "entries": entries,
        "total": filtered_total,
        "hasMore": has_more,
    }))
}

struct TransferSummaryRow {
    aggregate_id: String,
    latest_event: String,
    started_at: Option<String>,
    updated_at: Option<String>,
}

async fn load_transfer_summary(
    pool: &SqlitePool,
    aggregate_type: &str,
    started_path: &str,
) -> Vec<TransferSummaryRow> {
    // Get the started_at from the first event's payload, the latest
    // event type, and the last event's timestamp for updated_at.
    let rows: Vec<(String, String, Option<String>, Option<String>)> =
        match sqlx::query_as(
            "SELECT \
                e.aggregate_id, \
                (SELECT e2.event_type FROM events e2 \
                 WHERE e2.aggregate_type = ?1 \
                   AND e2.aggregate_id = e.aggregate_id \
                 ORDER BY e2.sequence DESC LIMIT 1) AS latest_event, \
                (SELECT COALESCE(\
                    json_extract(e3.payload, ?2), \
                    json_extract(e3.payload, '$.' || SUBSTR(e3.event_type, INSTR(e3.event_type, '::') + 2) || '.initiated_at'), \
                    json_extract(e3.payload, '$.' || SUBSTR(e3.event_type, INSTR(e3.event_type, '::') + 2) || '.requested_at'), \
                    json_extract(e3.payload, '$.' || SUBSTR(e3.event_type, INSTR(e3.event_type, '::') + 2) || '.withdrawn_at') \
                 ) FROM events e3 \
                 WHERE e3.aggregate_type = ?1 \
                   AND e3.aggregate_id = e.aggregate_id \
                 ORDER BY e3.sequence ASC LIMIT 1) AS started_at, \
                (SELECT COALESCE(\
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.completed_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.failed_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.deposited_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.confirmed_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.initiated_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.withdrawn_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.requested_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.wrapped_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.unwrapped_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.sent_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.bridged_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.deposit_confirmed_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.deposit_initiated_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.converted_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.burned_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.attested_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.minted_at'), \
                    json_extract(e4.payload, '$.' || SUBSTR(e4.event_type, INSTR(e4.event_type, '::') + 2) || '.accepted_at') \
                 ) FROM events e4 \
                 WHERE e4.aggregate_type = ?1 \
                   AND e4.aggregate_id = e.aggregate_id \
                 ORDER BY e4.sequence DESC LIMIT 1) AS updated_at \
             FROM events e \
             WHERE e.aggregate_type = ?1 \
             GROUP BY e.aggregate_id \
             ORDER BY MAX(e.rowid) DESC",
        )
        .bind(aggregate_type)
        .bind(started_path)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, %aggregate_type, "Failed to load transfer summary");
            return Vec::new();
        }
    };

    rows.into_iter()
        .map(
            |(aggregate_id, latest_event, started_at, updated_at)| TransferSummaryRow {
                aggregate_id,
                latest_event,
                started_at,
                updated_at,
            },
        )
        .collect()
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
            tracing::warn!(%error, %url, "Failed to reach st0x REST API");
            return unavailable_json("REST API unreachable");
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        tracing::warn!(%status, %url, "st0x REST API returned error");
        return unavailable_json("REST API returned an error");
    }

    match response.text().await {
        Ok(body) => RawJson(body),
        Err(error) => {
            tracing::warn!(%error, "Failed to read st0x REST API response body");
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
        transfers_endpoint,
        raindex_orders
    ]
}

#[cfg(test)]
mod tests {
    use rocket::http::Status;
    use rocket::local::asynchronous::Client;

    use super::*;
    use crate::config::tests::create_test_ctx_with_order_owner;

    #[test]
    fn test_num_of_routes() {
        let routes_list = routes();
        assert_eq!(routes_list.len(), 6);
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
