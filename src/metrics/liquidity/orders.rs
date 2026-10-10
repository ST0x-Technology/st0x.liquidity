//! The `PendingOrders` and `Raindex` families, refreshed every 60 seconds by
//! `liq-orders-refresh`.
//!
//! The pending-order series count what `GET /orders/pending` returns, through
//! the same loader: the newest 100 non-terminal rows, without the ones whose
//! payload does not parse. A malformed row still takes one of the 100.
//! `liq_pending_orders_uncapped_total` counts every non-terminal row instead.
//! A read that fails keeps the last series: the endpoint answers no orders
//! then, but a 0 here would read as a fresh count. The Raindex series read the
//! total from the same fetch `GET /orders/raindex` proxies, page 1 of 50
//! orders.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use serde_json::Value;
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::{Instant, MissedTickBehavior, timeout};
use tracing::{error, info, warn};

use st0x_config::Ctx;

use super::refresh::record_collector_duration;
use super::{
    LIQ_FAMILIES, LiqFamilies, LiqFamily, LiqMetric, LiqSample, count_value, push_sample,
    signed_integer_value,
};
use crate::dashboard::order_loader::{
    PendingOrderResponse, RaindexOrders, count_pending_orders, fetch_raindex_orders,
    try_load_pending_orders,
};

/// The exporter's cadence and the scrape interval.
const ORDERS_REFRESH_PERIOD: Duration = Duration::from_secs(60);

/// The exporter's Raindex request: the first page of 50 orders, within 35 s.
const RAINDEX_PAGE: u32 = 1;
const RAINDEX_PAGE_SIZE: u32 = 50;
const RAINDEX_TIMEOUT: Duration = Duration::from_secs(35);

/// Longest the pending-order queries may take before the cycle keeps the
/// last pending-order series.
const PENDING_ORDERS_TIMEOUT: Duration = Duration::from_secs(30);

/// `liq_pending_orders_total`, `liq_pending_orders{status}` for each status
/// with an order, and the uncapped count when it was read.
pub(crate) fn pending_order_samples(
    orders: &[PendingOrderResponse],
    uncapped: Option<i64>,
) -> Vec<LiqSample> {
    let mut samples = Vec::new();

    push_sample(
        &mut samples,
        LiqMetric::PendingOrdersTotal,
        Vec::new(),
        count_value(orders.len()),
    );

    let mut by_status: BTreeMap<&str, usize> = BTreeMap::new();
    for order in orders {
        *by_status.entry(order.status.as_str()).or_insert(0) += 1;
    }
    for (status, count) in by_status {
        push_sample(
            &mut samples,
            LiqMetric::PendingOrders,
            vec![("status", status.to_string())],
            count_value(count),
        );
    }

    if let Some(uncapped) = uncapped {
        push_sample(
            &mut samples,
            LiqMetric::PendingOrdersUncappedTotal,
            Vec::new(),
            signed_integer_value(uncapped),
        );
    }

    samples
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RaindexSamplesError {
    #[error("the Raindex orders body is not a JSON object")]
    NotAnObject,
    #[error("the Raindex orders pagination is not a JSON object")]
    PaginationNotAnObject,
    #[error("the Raindex totalOrders {0} is not a finite number")]
    TotalOrdersNotANumber(Value),
}

/// `liq_raindex_orders_unavailable{reason}` and, while available,
/// `liq_raindex_orders_total`. A missing, null or empty `pagination`, or a
/// missing `totalOrders`, reads as 0 orders, as the exporter read it. A
/// `totalOrders` that is a number, or a string that parses as a finite number
/// (the exporter's `float()`), is the total. A body whose shape the exporter
/// could not read, or any other `totalOrders`, is an error, and the caller
/// keeps the last family: the exporter failed its whole body on such a value,
/// and a 0 would read as a fresh count.
pub(crate) fn raindex_samples(
    orders: &RaindexOrders,
) -> Result<Vec<LiqSample>, RaindexSamplesError> {
    let mut samples = Vec::new();

    let body = match orders {
        RaindexOrders::Unavailable { reason } => {
            push_sample(
                &mut samples,
                LiqMetric::RaindexOrdersUnavailable,
                vec![("reason", (*reason).to_string())],
                Ok(1.0),
            );
            return Ok(samples);
        }
        RaindexOrders::Available(body) => body,
    };

    let Value::Object(body) = body else {
        return Err(RaindexSamplesError::NotAnObject);
    };

    let total = match body.get("pagination") {
        None | Some(Value::Null) => Some(0.0),
        Some(Value::Object(pagination)) => total_orders(pagination.get("totalOrders"))?,
        Some(other) if json_falsy(other) => Some(0.0),
        Some(_) => return Err(RaindexSamplesError::PaginationNotAnObject),
    };
    if let Some(total) = total {
        push_sample(
            &mut samples,
            LiqMetric::RaindexOrdersTotal,
            Vec::new(),
            Ok(total),
        );
    }

    push_sample(
        &mut samples,
        LiqMetric::RaindexOrdersUnavailable,
        vec![("reason", String::new())],
        Ok(0.0),
    );

    Ok(samples)
}

/// `None` when the exporter published no total: a null `totalOrders`. A
/// string is trimmed and parsed, as Python's `float()` reads it; one that
/// does not parse, or parses to infinity or NaN, is an error.
fn total_orders(total: Option<&Value>) -> Result<Option<f64>, RaindexSamplesError> {
    let Some(total) = total else {
        return Ok(Some(0.0));
    };

    let parsed = match total {
        Value::Null => return Ok(None),
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    };

    parsed
        .filter(|value| value.is_finite())
        .map(Some)
        .ok_or_else(|| RaindexSamplesError::TotalOrdersNotANumber(total.clone()))
}

/// Python's falsiness for a JSON value the exporter replaced with `{}`.
fn json_falsy(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(flag) => !flag,
        Value::Number(number) => number.as_f64() == Some(0.0),
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(fields) => fields.is_empty(),
    }
}

/// The capped orders and the uncapped count, read in one transaction so
/// both see the same rows. A transaction that does not open, or a capped
/// query that fails, is an error; a failed uncapped count leaves only the
/// count out.
async fn read_pending_orders(
    pool: &SqlitePool,
) -> Result<(Vec<PendingOrderResponse>, Option<i64>), sqlx::Error> {
    let mut transaction = pool.begin().await?;

    let orders = try_load_pending_orders(&mut *transaction).await?;
    let uncapped = count_pending_orders(&mut *transaction)
        .await
        .inspect_err(|error| warn!(%error, "Failed to count the pending orders"))
        .ok();

    if let Err(error) = transaction.rollback().await {
        warn!(%error, "Failed to end the read of the pending orders");
    }

    Ok((orders, uncapped))
}

/// Every 60 seconds: refreshes the pending-order and Raindex families.
#[derive(Clone)]
pub(crate) struct LiqOrdersRefresh {
    ctx: Ctx,
    pool: SqlitePool,
    families: &'static LiqFamilies,
}

impl LiqOrdersRefresh {
    pub(crate) fn new(ctx: Ctx, pool: SqlitePool) -> Self {
        Self::with_families(ctx, pool, &LIQ_FAMILIES)
    }

    fn with_families(ctx: Ctx, pool: SqlitePool, families: &'static LiqFamilies) -> Self {
        Self {
            ctx,
            pool,
            families,
        }
    }

    async fn refresh_once(&self) {
        self.refresh_pending_orders().await;
        self.refresh_raindex().await;
    }

    /// Publishes the pending-order family and records the run, failed or
    /// not, in `metrics_refresh_duration_seconds`.
    async fn refresh_pending_orders(&self) {
        let started = Instant::now();
        self.publish_pending_orders().await;
        record_collector_duration(LiqFamily::PendingOrders.collector(), started.elapsed());
    }

    /// Publishes the Raindex family and records the run, failed or not, in
    /// `metrics_refresh_duration_seconds`.
    async fn refresh_raindex(&self) {
        let started = Instant::now();
        self.publish_raindex().await;
        record_collector_duration(LiqFamily::Raindex.collector(), started.elapsed());
    }

    /// A read that fails or takes too long keeps the last series, so their
    /// collector time stops advancing; a failed uncapped count leaves only
    /// that series absent.
    async fn publish_pending_orders(&self) {
        let read = read_pending_orders(&self.pool);
        let (orders, uncapped) = match timeout(PENDING_ORDERS_TIMEOUT, read).await {
            Ok(Ok(read)) => read,
            Ok(Err(error)) => {
                error!(%error, "Kept the last liq_ pending orders: the read failed");
                return;
            }
            Err(_) => {
                warn!(
                    timeout = ?PENDING_ORDERS_TIMEOUT,
                    "Kept the last liq_ pending orders: the queries took too long"
                );
                return;
            }
        };

        self.families.replace(
            LiqFamily::PendingOrders,
            pending_order_samples(&orders, uncapped),
            SystemTime::now(),
        );
    }

    /// A fetch that takes too long, or a body that cannot be read, keeps the
    /// last series, so their collector time stops advancing.
    async fn publish_raindex(&self) {
        let fetch = fetch_raindex_orders(&self.ctx, Some(RAINDEX_PAGE), Some(RAINDEX_PAGE_SIZE));
        let Ok(orders) = timeout(RAINDEX_TIMEOUT, fetch).await else {
            warn!(
                timeout = ?RAINDEX_TIMEOUT,
                "Kept the last liq_ Raindex orders: the fetch took too long"
            );
            return;
        };

        match raindex_samples(&orders) {
            Ok(samples) => self
                .families
                .replace(LiqFamily::Raindex, samples, SystemTime::now()),
            Err(error) => warn!(%error, "Kept the last liq_ Raindex orders"),
        }
    }
}

impl SupervisedTask for LiqOrdersRefresh {
    async fn run(&mut self) -> TaskResult {
        info!("liq_ orders refresh started");

        let mut interval = tokio::time::interval(ORDERS_REFRESH_PERIOD);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            self.refresh_once().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use st0x_config::{RestApiCtx, create_test_ctx_with_order_owner};

    use super::*;
    use crate::metrics::liquidity::inventory::tests::{
        leaked_families, render_family, rendered_store,
    };
    use crate::metrics::liquidity::tests::{SeriesKey, parse_exposition, series};
    use crate::test_utils::setup_test_db;

    fn order(
        view_id: &str,
        status: &str,
        symbol: &str,
        direction: &str,
        shares: &str,
    ) -> PendingOrderResponse {
        PendingOrderResponse {
            view_id: view_id.to_string(),
            status: status.to_string(),
            symbol: symbol.to_string(),
            direction: direction.to_string(),
            shares: shares.to_string(),
            executor: "AlpacaBrokerApi".to_string(),
            placed_at: String::new(),
            submitted_at: None,
            shares_filled: None,
            avg_price: None,
        }
    }

    fn fixture_orders() -> Vec<PendingOrderResponse> {
        let mut submitted = order(
            "00000000-0000-0000-0000-000000000003",
            "Submitted",
            "tAAPL",
            "Buy",
            "2",
        );
        submitted.placed_at = "2026-03-03T14:30:00Z".to_string();
        submitted.submitted_at = Some("2026-03-03T14:30:01Z".to_string());
        let mut sell = order(
            "00000000-0000-0000-0000-000000000002",
            "Pending",
            "RKLB",
            "Sell",
            "1.5",
        );
        sell.placed_at = "2026-03-03T14:29:00Z".to_string();
        let mut buy = order(
            "00000000-0000-0000-0000-000000000001",
            "Pending",
            "RKLB",
            "Buy",
            "3",
        );
        buy.placed_at = "2026-03-03T14:28:00Z".to_string();

        vec![submitted, sell, buy]
    }

    fn golden_for(families: &[LiqFamily], golden: &str) -> BTreeMap<SeriesKey, f64> {
        let names: Vec<&str> = LiqMetric::ALL
            .into_iter()
            .filter(|metric| {
                metric
                    .family()
                    .is_some_and(|family| families.contains(&family))
            })
            .map(LiqMetric::name)
            .collect();
        parse_exposition(golden)
            .into_iter()
            .filter(|((name, _), _)| names.contains(&name.as_str()))
            .collect()
    }

    fn render_orders(
        orders: &[PendingOrderResponse],
        raindex: &RaindexOrders,
    ) -> BTreeMap<SeriesKey, f64> {
        let mut rendered = render_family(
            LiqFamily::PendingOrders,
            pending_order_samples(orders, None),
        );
        rendered.extend(render_family(
            LiqFamily::Raindex,
            raindex_samples(raindex).unwrap(),
        ));
        rendered
    }

    #[test]
    fn orders_match_the_exporter_golden() {
        let fixture: Value = serde_json::from_str(include_str!("testdata/orders.json")).unwrap();
        let orders = fixture_orders();
        assert_eq!(serde_json::to_value(&orders).unwrap(), fixture["pending"]);

        assert_eq!(
            render_orders(
                &orders,
                &RaindexOrders::Available(fixture["raindex"].clone())
            ),
            golden_for(
                &[LiqFamily::PendingOrders, LiqFamily::Raindex],
                include_str!("testdata/orders.prom")
            )
        );
    }

    #[test]
    fn unavailable_raindex_orders_and_no_pending_orders_match_the_exporter_golden() {
        let fixture: Value =
            serde_json::from_str(include_str!("testdata/orders-unavailable.json")).unwrap();
        let reason = "REST API unreachable";
        assert_eq!(fixture["pending"], json!([]));
        assert_eq!(
            fixture["raindex"],
            json!({"unavailable": true, "reason": reason})
        );

        assert_eq!(
            render_orders(&[], &RaindexOrders::Unavailable { reason }),
            golden_for(
                &[LiqFamily::PendingOrders, LiqFamily::Raindex],
                include_str!("testdata/orders-unavailable.prom")
            )
        );
    }

    fn raindex_total(body: Value) -> Option<f64> {
        render_family(
            LiqFamily::Raindex,
            raindex_samples(&RaindexOrders::Available(body)).unwrap(),
        )
        .get(&series("liq_raindex_orders_total", &[]))
        .copied()
    }

    #[test]
    fn a_missing_pagination_or_total_reads_as_zero_and_a_null_total_is_absent() {
        assert_eq!(raindex_total(json!({"orders": []})), Some(0.0));
        assert_eq!(raindex_total(json!({"pagination": null})), Some(0.0));
        assert_eq!(raindex_total(json!({"pagination": []})), Some(0.0));
        assert_eq!(raindex_total(json!({"pagination": {}})), Some(0.0));
        assert_eq!(
            raindex_total(json!({"pagination": {"totalOrders": null}})),
            None
        );
        assert_eq!(
            raindex_total(json!({"pagination": {"totalOrders": 7}})),
            Some(7.0)
        );
    }

    /// The exporter's `float()` reads a numeric string, surrounding spaces
    /// included, so the bot does too.
    #[test]
    fn a_numeric_string_total_reads_as_its_number() {
        assert_eq!(
            raindex_total(json!({"pagination": {"totalOrders": "12"}})),
            Some(12.0)
        );
        assert_eq!(
            raindex_total(json!({"pagination": {"totalOrders": " 4 "}})),
            Some(4.0)
        );
        assert_eq!(
            raindex_total(json!({"pagination": {"totalOrders": "2.5e1"}})),
            Some(25.0)
        );
    }

    /// The exporter failed its whole body on a total `float()` could not
    /// read. The bot keeps the last family instead, so the collector time
    /// stops advancing, and never publishes such a total as 0.
    #[test]
    fn a_malformed_total_is_an_error_and_never_zero() {
        for total in [
            json!("x"),
            json!(""),
            json!("NaN"),
            json!("inf"),
            json!(true),
            json!([3]),
            json!({"count": 3}),
        ] {
            assert!(
                matches!(
                    raindex_samples(&RaindexOrders::Available(
                        json!({"pagination": {"totalOrders": total.clone()}})
                    )),
                    Err(RaindexSamplesError::TotalOrdersNotANumber(value)) if value == total
                ),
                "{total}"
            );
        }
    }

    #[test]
    fn a_body_the_exporter_could_not_read_is_an_error() {
        assert!(matches!(
            raindex_samples(&RaindexOrders::Available(json!([1]))),
            Err(RaindexSamplesError::NotAnObject)
        ));
        assert!(matches!(
            raindex_samples(&RaindexOrders::Available(json!({"pagination": [1]}))),
            Err(RaindexSamplesError::PaginationNotAnObject)
        ));
    }

    async fn insert_order(pool: &SqlitePool, view_id: &str, payload: &str) {
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind(view_id)
            .bind(payload)
            .execute(pool)
            .await
            .unwrap();
    }

    fn pending_payload(symbol: &str) -> String {
        json!({"Live": {"Pending": {
            "symbol": symbol,
            "direction": "Buy",
            "shares": "1",
            "executor": "AlpacaBrokerApi",
            "placed_at": "2026-03-03T14:30:00Z"
        }}})
        .to_string()
    }

    fn refresh(pool: SqlitePool, families: &'static LiqFamilies) -> LiqOrdersRefresh {
        LiqOrdersRefresh::with_families(
            create_test_ctx_with_order_owner(alloy::primitives::Address::ZERO),
            pool,
            families,
        )
    }

    fn pending_series(families: &'static LiqFamilies) -> BTreeMap<SeriesKey, f64> {
        rendered_store(families)
            .into_iter()
            .filter(|((name, _), _)| name.starts_with("liq_pending_orders"))
            .collect()
    }

    #[tokio::test]
    async fn no_pending_orders_publish_a_zero_total_and_no_status_series() {
        let families = leaked_families();
        refresh(setup_test_db().await, families)
            .refresh_pending_orders()
            .await;

        assert_eq!(
            pending_series(families),
            BTreeMap::from([
                (series("liq_pending_orders_total", &[]), 0.0),
                (series("liq_pending_orders_uncapped_total", &[]), 0.0),
            ])
        );
    }

    /// The capped series count what the endpoint returns: the newest 100
    /// rows, without the one whose payload does not parse. The uncapped count takes every live row,
    /// malformed ones included, and no terminal row.
    #[tokio::test]
    async fn the_capped_series_follow_the_endpoint_and_the_uncapped_count_every_live_row() {
        let pool = setup_test_db().await;
        for index in 0..150 {
            insert_order(
                &pool,
                &format!("pending-{index:03}"),
                &pending_payload("RKLB"),
            )
            .await;
        }
        insert_order(
            &pool,
            "malformed",
            r#"{"Live":{"Pending":{"direction":"Buy"}}}"#,
        )
        .await;
        insert_order(
            &pool,
            "filled",
            r#"{"Live":{"Filled":{"symbol":"AAPL","shares":"2","direction":"Sell","executor":"AlpacaBrokerApi","executor_order_id":"broker-fill","price":"100","placed_at":"2026-01-01T00:00:00Z","submitted_at":"2026-01-01T00:00:00Z","filled_at":"2026-01-01T00:00:00Z"}}}"#,
        )
        .await;
        let families = leaked_families();

        refresh(pool, families).refresh_pending_orders().await;

        assert_eq!(
            pending_series(families),
            BTreeMap::from([
                (series("liq_pending_orders_total", &[]), 99.0),
                (series("liq_pending_orders", &[("status", "Pending")]), 99.0),
                (series("liq_pending_orders_uncapped_total", &[]), 151.0),
            ])
        );
    }

    /// The endpoint answers no orders when its query fails, but the series
    /// keep their last values and their collector time, so a staleness
    /// alert sees the failure instead of a fresh 0.
    #[tokio::test]
    async fn a_failed_read_keeps_the_last_series_and_their_collector_time() {
        let pool = setup_test_db().await;
        insert_order(&pool, "pending", &pending_payload("RKLB")).await;
        let families = leaked_families();
        let task = refresh(pool.clone(), families);
        task.refresh_pending_orders().await;
        let before = rendered_store(families);
        assert_eq!(
            before.get(&series("liq_pending_orders_total", &[])),
            Some(&1.0)
        );

        pool.close().await;
        task.refresh_pending_orders().await;

        assert_eq!(rendered_store(families), before);
    }

    #[tokio::test]
    async fn a_read_that_fails_before_any_success_publishes_nothing() {
        let pool = setup_test_db().await;
        pool.close().await;
        let families = leaked_families();

        refresh(pool, families).refresh_pending_orders().await;

        assert_eq!(pending_series(families), BTreeMap::new());
    }

    /// Both collectors record each run in the refresh histogram, like the
    /// performance collectors, a failed one included.
    #[tokio::test]
    async fn each_orders_collector_run_records_its_duration() {
        let recorder = crate::metrics::local_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let pool = setup_test_db().await;
        let mut task = refresh(pool.clone(), leaked_families());
        task.ctx.rest_api = Some(RestApiCtx::unauthenticated(
            "http://127.0.0.1:1".to_string(),
        ));

        task.refresh_once().await;
        pool.close().await;
        task.refresh_pending_orders().await;

        let durations = parse_exposition(&handle.render());
        let runs = |collector: &str| {
            durations
                .get(&series(
                    "metrics_refresh_duration_seconds_count",
                    &[("collector", collector)],
                ))
                .copied()
        };
        assert_eq!(runs("pending_orders"), Some(2.0));
        assert_eq!(runs("raindex"), Some(1.0));
    }

    #[tokio::test]
    async fn the_raindex_family_reads_the_proxy_fetch() {
        let families = leaked_families();
        let mut task = refresh(setup_test_db().await, families);
        task.ctx.rest_api = Some(RestApiCtx::unauthenticated(
            "http://127.0.0.1:1".to_string(),
        ));

        task.refresh_raindex().await;

        assert_eq!(
            rendered_store(families)
                .into_iter()
                .filter(|((name, _), _)| name.starts_with("liq_raindex"))
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([(
                series(
                    "liq_raindex_orders_unavailable",
                    &[("reason", "REST API unreachable")]
                ),
                1.0
            )])
        );
    }
}
