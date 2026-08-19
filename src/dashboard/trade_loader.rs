//! Trade history served from the materialized order and trade views.
//!
//! `/trades`, the dashboard's initial state, and delivery reconciliation page
//! terminal trades straight out of `onchain_trade_view` and
//! `offchain_order_view` -- the projections the event-sourcing framework keeps
//! current -- so a request costs a bounded index range per venue side for the
//! page, plus an index-only count, instead of replaying every aggregate.
//!
//! Rows carry the serialized aggregate, not a serialized [`Trade`], and
//! `try_into_trade` converts only the page being returned. The generated
//! columns the queries filter and sort on are therefore pure read keys: the
//! DTO still has exactly one source of truth, and a key cannot drift from the
//! payload it is derived from.

use std::num::TryFromIntError;
use std::str::FromStr;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use sqlx::{Row, SqlitePool};
use tracing::warn;

use st0x_dto::{Trade, TradingVenue};
use st0x_execution::Symbol;
use st0x_finance::{FractionalShares, NotPositive};

use super::TradeProtocol;
use crate::offchain::order::{OffchainOrder, OffchainOrderId, TradeConversionError};
use crate::onchain_trade::{OnChainTrade, OnChainTradeId, ParseOnChainTradeIdError};

/// Trades returned to a dashboard client that asks for no explicit page.
const MAX_TRADES: usize = 100;

/// Which materialized view a trade comes from. Onchain and offchain trades
/// live in separate projections, so every query is a union of the two sides
/// that the venue filter did not rule out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Onchain,
    Offchain,
}

impl Side {
    const fn table(self) -> &'static str {
        match self {
            Self::Onchain => "onchain_trade_view",
            Self::Offchain => "offchain_order_view",
        }
    }

    /// Column holding the comparator's grouping key. Onchain trades from one
    /// transaction share a `tx_hash` and tie-break on `log_index`; every other
    /// trade's group is its own id.
    const fn sort_group(self) -> &'static str {
        match self {
            Self::Onchain => "tx_hash",
            Self::Offchain => "view_id",
        }
    }

    const fn log_index(self) -> &'static str {
        match self {
            Self::Onchain => "log_index",
            Self::Offchain => "NULL",
        }
    }

    const fn discriminant(self) -> i64 {
        match self {
            Self::Onchain => 0,
            Self::Offchain => 1,
        }
    }

    const fn from_discriminant(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::Onchain),
            1 => Some(Self::Offchain),
            _ => None,
        }
    }

    const fn holds(self, venue: TradingVenue) -> bool {
        venue.is_onchain() == matches!(self, Self::Onchain)
    }
}

/// A trade-history request: the filters, and the page to return.
pub(crate) struct TradeQuery {
    pub(crate) symbols: Option<Vec<Symbol>>,
    pub(crate) venues: Option<Vec<TradingVenue>>,
    pub(crate) since: Option<DateTime<Utc>>,
    pub(crate) until: Option<DateTime<Utc>>,
    pub(crate) trade_protocol: TradeProtocol,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
}

impl TradeQuery {
    /// Every trade the protocol can carry, unfiltered and unpaged -- what
    /// delivery reconciliation needs to decide which trades were never
    /// handed to the dashboard.
    pub(crate) fn all(trade_protocol: TradeProtocol) -> Self {
        Self {
            symbols: None,
            venues: None,
            since: None,
            until: None,
            trade_protocol,
            limit: usize::MAX,
            offset: 0,
        }
    }

    /// The newest page a dashboard client gets on connect.
    pub(crate) fn newest(trade_protocol: TradeProtocol) -> Self {
        Self {
            limit: MAX_TRADES,
            ..Self::all(trade_protocol)
        }
    }
}

/// A page of trade history, with the counts a client needs to page through it.
#[derive(Debug)]
pub(crate) struct TradePage {
    pub(crate) trades: Vec<Trade>,
    /// Rows matching the filters.
    ///
    /// A row holding an aggregate with no representable trade is counted here
    /// but never returned, so this is an upper bound on what a client can
    /// render rather than an exact count. Making it exact would mean deciding
    /// representability in SQL, over a serialized `Float` -- the duplication
    /// the venue cross-check in [`convert_row`] exists to police.
    pub(crate) total: usize,
    /// Whether a further page exists.
    ///
    /// Derived from the rows the query consumed, not the trades it yielded, so
    /// a skipped row cannot make a final page look like it has a successor.
    pub(crate) has_more: bool,
}

/// Loads one page of trade history, newest first.
///
/// Ordering reproduces `st0x_dto::sort_trades_newest_first`: newest
/// `occurred_at` first, then ascending sort group, then descending numeric
/// `log_index` for trades sharing a transaction.
pub(crate) async fn query_trades(
    pool: &SqlitePool,
    query: &TradeQuery,
) -> Result<TradePage, TradeHistoryError> {
    let branches: Vec<(Side, Filter)> = [Side::Onchain, Side::Offchain]
        .into_iter()
        .filter_map(|side| side_filter(query, side).map(|filter| (side, filter)))
        .collect();

    if branches.is_empty() {
        return Ok(TradePage {
            trades: Vec::new(),
            total: 0,
            has_more: false,
        });
    }

    let total = count_matching(pool, &branches).await?;
    let (trades, rows_read) = fetch_page(pool, query, &branches).await?;

    Ok(TradePage {
        trades,
        total,
        has_more: query.offset.saturating_add(rows_read) < total,
    })
}

/// Builds one side's filter, or `None` when the venue filter excludes it
/// entirely -- in which case its view is never touched.
fn side_filter(query: &TradeQuery, side: Side) -> Option<Filter> {
    let mut filter = Filter::default();

    if let Some(requested) = &query.venues {
        let venues = expand_venues(requested, query.trade_protocol);
        let side_venues: Vec<TradingVenue> = venues
            .into_iter()
            .filter(|venue| side.holds(*venue))
            .collect();

        if side_venues.is_empty() {
            return None;
        }

        filter.push_in("venue", side_venues.iter().map(ToString::to_string));
    }

    if let Some(symbols) = &query.symbols {
        filter.push_in("symbol", symbols.iter().map(ToString::to_string));
    }

    // Onchain trades are fills, which every protocol carries, so only the
    // offchain side narrows by terminal outcome -- and only when the protocol
    // drops one, since `occurred_at IS NOT NULL` already selects exactly the
    // terminal rows.
    //
    // The leading `+` strips the term's index affinity without changing its
    // meaning. Without it SQLite drives the scan from
    // `idx_offchain_order_view_status` and sorts every match in a temp
    // b-tree; with it, the ordering index drives the scan and LIMIT stops it
    // early -- which is the whole point of paging in SQL.
    if side == Side::Offchain
        && let Some(statuses) = query.trade_protocol.narrowed_terminal_statuses()
    {
        filter.push_in(
            "+status",
            statuses.iter().map(|status| (*status).to_owned()),
        );
    }

    if let Some(since) = query.since {
        filter.push_comparison("occurred_at", ">=", sortable_timestamp(since));
    }

    if let Some(until) = query.until {
        filter.push_comparison("occurred_at", "<=", sortable_timestamp(until));
    }

    Some(filter)
}

/// Expands a requested venue list to the venues actually stored.
///
/// Protocols older than v3 cannot express adapter attribution, so they
/// collapse every onchain venue onto `raindex`. A request naming any venue in
/// a collapsed group therefore has to match every venue in that group, which
/// is what [`TradingVenue::legacy_compatible`] defines. This reproduces the
/// pairwise comparison the in-memory filter used, as an IN list.
fn expand_venues(requested: &[TradingVenue], trade_protocol: TradeProtocol) -> Vec<TradingVenue> {
    match trade_protocol {
        TradeProtocol::TerminalOutcomesV3 => requested.to_vec(),
        TradeProtocol::LegacyFills
        | TradeProtocol::TerminalOutcomesV1
        | TradeProtocol::TerminalOutcomesV2 => TradingVenue::ALL
            .into_iter()
            .filter(|stored| {
                requested
                    .iter()
                    .any(|venue| venue.legacy_compatible() == stored.legacy_compatible())
            })
            .collect(),
    }
}

/// Formats a timestamp the way the views' `occurred_at` generated columns do:
/// fixed-width nanoseconds and no zone suffix, so a lexicographic comparison
/// in SQL is a chronological one.
fn sortable_timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Nanos, true)
        .trim_end_matches('Z')
        .to_owned()
}

/// A side's WHERE clause and the values its placeholders bind, kept together
/// so they cannot fall out of order.
#[derive(Default)]
struct Filter {
    clauses: Vec<String>,
    binds: Vec<String>,
}

impl Filter {
    fn push_in(&mut self, column: &str, values: impl IntoIterator<Item = String>) {
        let values: Vec<String> = values.into_iter().collect();
        let placeholders = vec!["?"; values.len()].join(", ");
        self.clauses.push(format!("{column} IN ({placeholders})"));
        self.binds.extend(values);
    }

    fn push_comparison(&mut self, column: &str, operator: &str, value: String) {
        self.clauses.push(format!("{column} {operator} ?"));
        self.binds.push(value);
    }

    /// Terminal trades are exactly the rows with an outcome timestamp, so
    /// every query starts from that and appends the caller's filters.
    fn where_sql(&self) -> String {
        std::iter::once(String::from("occurred_at IS NOT NULL"))
            .chain(self.clauses.iter().cloned())
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

/// `AssertSqlSafe`: the statement is assembled from `&'static str` fragments
/// -- table and column names chosen by [`Side`] and this module's own literals
/// -- and every caller-supplied value is a bound `?` placeholder. No user input
/// reaches the SQL text.
async fn count_matching(
    pool: &SqlitePool,
    branches: &[(Side, Filter)],
) -> Result<usize, TradeHistoryError> {
    let mut total: i64 = 0;

    for (side, filter) in branches {
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            side.table(),
            filter.where_sql()
        );
        let mut counted = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql));
        for bind in &filter.binds {
            counted = counted.bind(bind);
        }
        total += counted.fetch_one(pool).await?;
    }

    usize::try_from(total).map_err(|source| TradeHistoryError::CountOutOfRange { total, source })
}

/// Fetches the requested page.
///
/// Each side is ordered and truncated to `offset + limit` on its own index
/// before the union, so the merge sorts at most two bounded lists rather than
/// the whole history. Taking the top N of each side and then the top N of the
/// union yields the same rows as sorting everything.
///
/// `AssertSqlSafe`: see [`count_matching`] -- the text is all static fragments
/// and every value is bound.
async fn fetch_page(
    pool: &SqlitePool,
    query: &TradeQuery,
    branches: &[(Side, Filter)],
) -> Result<(Vec<Trade>, usize), TradeHistoryError> {
    let bound = clamp_to_i64(query.offset.saturating_add(query.limit));
    let sql = format!(
        "SELECT view_id, payload, venue, side FROM ({}) \
         ORDER BY occurred_at DESC, sort_group ASC, log_index DESC \
         LIMIT ? OFFSET ?",
        branches
            .iter()
            .map(|(side, filter)| branch_sql(*side, filter))
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    );

    let mut page = sqlx::query(sqlx::AssertSqlSafe(sql));
    for (_, filter) in branches {
        for bind in &filter.binds {
            page = page.bind(bind);
        }
        page = page.bind(bound);
    }
    page = page.bind(clamp_to_i64(query.limit));
    page = page.bind(clamp_to_i64(query.offset));

    let rows = page.fetch_all(pool).await?;
    let rows_read = rows.len();
    let trades = rows
        .into_iter()
        .map(|row| {
            let view_id: String = row.try_get("view_id")?;
            let payload: String = row.try_get("payload")?;
            let venue: Option<String> = row.try_get("venue")?;
            let side: i64 = row.try_get("side")?;

            convert_row(&view_id, &payload, venue.as_deref(), side)
        })
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>, _>>()?;

    Ok((trades, rows_read))
}

fn branch_sql(side: Side, filter: &Filter) -> String {
    format!(
        "SELECT * FROM (\
           SELECT view_id, payload, occurred_at, venue, \
                  {group} AS sort_group, {log_index} AS log_index, {side} AS side \
             FROM {table} WHERE {predicates} \
            ORDER BY occurred_at DESC, sort_group ASC, log_index DESC \
            LIMIT ?)",
        group = side.sort_group(),
        log_index = side.log_index(),
        side = side.discriminant(),
        table = side.table(),
        predicates = filter.where_sql(),
    )
}

/// `usize` page parameters are unbounded on the wire; SQLite takes an `i64`.
/// Saturating is exact rather than lenient here: no result set can reach
/// `i64::MAX` rows, so a clamped offset pages past the end either way.
fn clamp_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Converts one view row, or skips it with a warning when the stored payload
/// cannot be read -- a corrupt or superseded row must not take the whole page
/// down with it.
fn convert_row(
    view_id: &str,
    payload: &str,
    venue: Option<&str>,
    side: i64,
) -> Result<Option<Trade>, TradeHistoryError> {
    let Some(side) = Side::from_discriminant(side) else {
        warn!(target: "dashboard", view_id, side, "Skipping trade history row with an unknown side");
        return Ok(None);
    };

    let trade = match parse_row(view_id, payload, side) {
        Ok(trade) => trade,
        Err(error) => {
            warn!(
                target: "dashboard",
                view_id,
                %error,
                "Skipping unreadable trade history row"
            );
            return Ok(None);
        }
    };

    // The views derive `venue` in SQL, duplicating the mapping `try_into_trade`
    // applies in Rust. Comparing them on every row returned makes that
    // duplication self-policing: an unmapped source variant leaves the column
    // NULL and would otherwise be silently invisible to venue filters while
    // still appearing in unfiltered history.
    //
    // This is deliberately fatal rather than a skip, and the blast radius is
    // wider than history rendering: `query_trades` also backs delivery
    // reconciliation, so drift here fails that at startup as well as failing
    // the endpoint. That asymmetry against the unreadable-payload skip above
    // is the point -- a payload nobody can read is data, a venue nobody
    // mapped is a bug in this repo.
    if venue != Some(trade.venue.to_string().as_str()) {
        return Err(TradeHistoryError::VenueMismatch {
            view_id: view_id.to_owned(),
            indexed: venue.map(ToOwned::to_owned),
            replayed: trade.venue,
        });
    }

    Ok(Some(trade))
}

fn parse_row(view_id: &str, payload: &str, side: Side) -> Result<Trade, TradeRowError> {
    match side {
        Side::Onchain => {
            let OnChainTradeProjectionPayload::Live(trade) = serde_json::from_str(payload)?;
            Ok(trade.try_into_trade(&OnChainTradeId::from_str(view_id)?)?)
        }
        Side::Offchain => {
            let OffchainOrderProjectionPayload::Live(order) = serde_json::from_str(payload)?;
            Ok(order.try_into_trade(&OffchainOrderId::from_str(view_id)?)?)
        }
    }
}

#[derive(Deserialize)]
enum OnChainTradeProjectionPayload {
    Live(OnChainTrade),
}

#[derive(Deserialize)]
enum OffchainOrderProjectionPayload {
    Live(OffchainOrder),
}

/// Why a single view row could not be turned into a [`Trade`]. Every variant
/// is per-row and recoverable, so these are logged and skipped rather than
/// failing the request.
#[derive(Debug, thiserror::Error)]
enum TradeRowError {
    #[error("invalid projection payload: {0}")]
    Payload(#[from] serde_json::Error),
    #[error("invalid onchain trade id: {0}")]
    OnchainId(#[from] ParseOnChainTradeIdError),
    #[error("invalid offchain order id: {0}")]
    OffchainId(#[from] uuid::Error),
    #[error("onchain trade cannot be represented in history: {0}")]
    OnchainConversion(#[from] NotPositive<FractionalShares>),
    #[error("offchain order cannot be represented in history: {0}")]
    OffchainConversion(#[from] TradeConversionError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TradeHistoryError {
    #[error("failed to query trade history: {0}")]
    Database(#[from] sqlx::Error),
    #[error("trade history matched {total} rows, which does not fit a usize")]
    CountOutOfRange {
        total: i64,
        #[source]
        source: TryFromIntError,
    },
    #[error(
        "trade history row {view_id} is indexed under venue {indexed:?} but replays as \
         {replayed}; the view's venue mapping has drifted from OnChainTradeSource"
    )]
    VenueMismatch {
        view_id: String,
        indexed: Option<String>,
        replayed: TradingVenue,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use proptest::prelude::*;
    use st0x_dto::{TradeOutcome, sort_trades_newest_first};
    use st0x_event_sorcery::{Store, StoreBuilder};
    use st0x_execution::{
        ClientOrderId, Direction, ExecutorOrderId, FractionalShares, MarketSession, Positive,
        SupportedExecutor,
    };
    use st0x_finance::Usd;
    use st0x_float_macro::float;

    use super::*;
    use crate::offchain::order::{
        CancellationReason, CounterTradeOrderKind, OffchainOrderCommand, noop_order_placer,
    };
    use crate::onchain_trade::{
        InventoryVenue, OnChainTradeCommand, OnChainTradeSource, ParseOnChainTradeIdError,
    };
    use crate::test_utils::setup_test_db;

    const V2: TradeProtocol = TradeProtocol::TerminalOutcomesV2;
    const V3: TradeProtocol = TradeProtocol::TerminalOutcomesV3;

    fn tx_hash(byte: u8) -> alloy::primitives::TxHash {
        alloy::primitives::TxHash::repeat_byte(byte)
    }

    async fn onchain_store(pool: &SqlitePool) -> Arc<Store<OnChainTrade>> {
        let (store, _view) = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store
    }

    async fn offchain_store(pool: &SqlitePool) -> Arc<Store<OffchainOrder>> {
        let (store, _view) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();
        store
    }

    async fn witness(
        store: &Store<OnChainTrade>,
        id: &OnChainTradeId,
        source: OnChainTradeSource,
        symbol: &str,
        at: DateTime<Utc>,
    ) {
        store
            .send(
                id,
                OnChainTradeCommand::WitnessAt {
                    source,
                    symbol: Symbol::new(symbol).unwrap(),
                    amount: float!(10),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_number: 1,
                    block_timestamp: at,
                    filled_at: at,
                },
            )
            .await
            .unwrap();
    }

    async fn place(store: &Store<OffchainOrder>, id: &OffchainOrderId, symbol: &str) {
        place_with(store, id, symbol, SupportedExecutor::AlpacaBrokerApi).await;
    }

    async fn place_with(
        store: &Store<OffchainOrder>,
        id: &OffchainOrderId,
        symbol: &str,
        executor: SupportedExecutor,
    ) {
        store
            .send(
                id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new(symbol).unwrap(),
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Sell,
                    executor,
                    client_order_id: ClientOrderId::from_uuid(id.as_uuid()),
                    kind: CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
    }

    async fn accept(store: &Store<OffchainOrder>, id: &OffchainOrderId) {
        store
            .send(
                id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("broker"),
                    placed_shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    submitted_at: Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
    }

    async fn fill(store: &Store<OffchainOrder>, id: &OffchainOrderId, at: DateTime<Utc>) {
        fill_with(store, id, at, SupportedExecutor::AlpacaBrokerApi).await;
    }

    /// Places, accepts and fills in one step -- the whole lifecycle, since a
    /// trade only reaches history once it is terminal.
    async fn fill_with(
        store: &Store<OffchainOrder>,
        id: &OffchainOrderId,
        at: DateTime<Utc>,
        executor: SupportedExecutor,
    ) {
        place_with(store, id, "TSLA", executor).await;
        accept(store, id).await;
        store
            .send(
                id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(200)),
                    filled_at: at,
                },
            )
            .await
            .unwrap();
    }

    async fn page(pool: &SqlitePool, query: &TradeQuery) -> TradePage {
        query_trades(pool, query).await.unwrap()
    }

    async fn ids(pool: &SqlitePool, query: &TradeQuery) -> Vec<String> {
        page(pool, query)
            .await
            .trades
            .into_iter()
            .map(|trade| trade.id)
            .collect()
    }

    #[tokio::test]
    async fn empty_database_returns_no_trades() {
        let pool = setup_test_db().await;
        let result = page(&pool, &TradeQuery::newest(V2)).await;
        assert_eq!(result.total, 0);
        assert!(result.trades.is_empty());
    }

    #[tokio::test]
    async fn onchain_fills_are_served_from_the_view() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let id = OnChainTradeId {
            tx_hash: tx_hash(0),
            log_index: 0,
        };
        witness(&store, &id, OnChainTradeSource::Raindex, "AAPL", Utc::now()).await;

        let trades = page(&pool, &TradeQuery::newest(V2)).await.trades;

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].id, id.to_string());
        assert_eq!(trades[0].symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(trades[0].venue, TradingVenue::Raindex);
        assert_eq!(trades[0].direction, Direction::Buy);
        assert_eq!(trades[0].outcome, TradeOutcome::Filled);
    }

    /// Attribution rewrites the same view row rather than adding one, and the
    /// venue the query reports follows it.
    #[tokio::test]
    async fn source_attribution_updates_the_indexed_venue_in_place() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let id = OnChainTradeId {
            tx_hash: tx_hash(0),
            log_index: 194,
        };
        witness(&store, &id, OnChainTradeSource::Legacy, "AAPL", Utc::now()).await;
        assert_eq!(
            page(&pool, &TradeQuery::newest(V3)).await.trades[0].venue,
            TradingVenue::Raindex
        );

        store
            .send(
                &id,
                OnChainTradeCommand::AttributeSource {
                    source: OnChainTradeSource::Inventory {
                        operator: alloy::primitives::Address::repeat_byte(0x8b),
                        venue: InventoryVenue::Bebop,
                    },
                },
            )
            .await
            .unwrap();

        let trades = page(&pool, &TradeQuery::newest(V3)).await.trades;
        assert_eq!(trades.len(), 1, "attribution must not duplicate the row");
        assert_eq!(trades[0].venue, TradingVenue::Bebop);
    }

    /// The `venue` generated column is a SQL restatement of
    /// `OnChainTradeSource::trading_venue`. The exhaustive match makes a new
    /// variant a compile error here, and the assertion catches a mapping that
    /// drifts.
    #[tokio::test]
    async fn indexed_venue_matches_trading_venue_for_every_source() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let operator = alloy::primitives::Address::repeat_byte(0x8b);
        let sources = [
            OnChainTradeSource::Legacy,
            OnChainTradeSource::Raindex,
            OnChainTradeSource::Inventory {
                operator,
                venue: InventoryVenue::Bebop,
            },
            OnChainTradeSource::Inventory {
                operator,
                venue: InventoryVenue::UniswapV4,
            },
            OnChainTradeSource::UnrecognizedInventory { operator },
        ];

        for (log_index, source) in sources.into_iter().enumerate() {
            match source {
                OnChainTradeSource::Legacy
                | OnChainTradeSource::Raindex
                | OnChainTradeSource::Inventory { .. }
                | OnChainTradeSource::UnrecognizedInventory { .. } => {}
            }

            let id = OnChainTradeId {
                tx_hash: tx_hash(0),
                log_index: u64::try_from(log_index).unwrap(),
            };
            witness(&store, &id, source, "AAPL", Utc::now()).await;

            let indexed: String =
                sqlx::query_scalar("SELECT venue FROM onchain_trade_view WHERE view_id = ?")
                    .bind(id.to_string())
                    .fetch_one(&pool)
                    .await
                    .unwrap();

            assert_eq!(
                indexed,
                source.trading_venue().to_string(),
                "SQL venue mapping drifted for {source:?}"
            );
        }
    }

    #[tokio::test]
    async fn terminal_offchain_outcomes_are_served_with_their_provenance() {
        let pool = setup_test_db().await;
        let store = offchain_store(&pool).await;

        let filled_id = OffchainOrderId::new();
        fill(&store, &filled_id, Utc::now()).await;

        let failed_id = OffchainOrderId::new();
        place(&store, &failed_id, "NVDA").await;
        store
            .send(
                &failed_id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "asset is not tradable".to_string(),
                },
            )
            .await
            .unwrap();

        let cancelled_id = OffchainOrderId::new();
        place(&store, &cancelled_id, "MSFT").await;
        accept(&store, &cancelled_id).await;
        store
            .send(
                &cancelled_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(0.25)),
                    avg_price: Usd::new(float!(100)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &cancelled_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &cancelled_id,
                OffchainOrderCommand::ConfirmCancellation {
                    filled_shares: FractionalShares::new(float!(0.25)),
                    cancelled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let trades = page(&pool, &TradeQuery::newest(V3)).await.trades;
        let find = |id: &OffchainOrderId| {
            trades
                .iter()
                .find(|trade| trade.id == id.to_string())
                .unwrap_or_else(|| panic!("{id} should be in history"))
        };

        assert_eq!(trades.len(), 3);
        assert_eq!(find(&filled_id).outcome, TradeOutcome::Filled);
        assert_eq!(find(&filled_id).venue, TradingVenue::Alpaca);
        assert!(matches!(
            &find(&failed_id).outcome,
            TradeOutcome::Failed { error, .. } if error == "asset is not tradable"
        ));
        assert!(matches!(
            find(&cancelled_id).outcome,
            TradeOutcome::Cancelled {
                filled_shares: Some(filled),
                remaining_shares: Some(remaining),
                ..
            } if filled.inner().inner().eq(float!(0.25)).unwrap()
                && remaining.inner().inner().eq(float!(0.75)).unwrap()
        ));
    }

    #[tokio::test]
    async fn nonterminal_offchain_orders_are_absent() {
        let pool = setup_test_db().await;
        let store = offchain_store(&pool).await;

        let pending = OffchainOrderId::new();
        place(&store, &pending, "NVDA").await;

        let submitted = OffchainOrderId::new();
        place(&store, &submitted, "NVDA").await;
        accept(&store, &submitted).await;

        let partially_filled = OffchainOrderId::new();
        place(&store, &partially_filled, "NVDA").await;
        accept(&store, &partially_filled).await;
        store
            .send(
                &partially_filled,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(0.5)),
                    avg_price: Usd::new(float!(100)),
                    partially_filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let cancelling = OffchainOrderId::new();
        place(&store, &cancelling, "NVDA").await;
        accept(&store, &cancelling).await;
        store
            .send(
                &cancelling,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        let result = page(&pool, &TradeQuery::newest(V3)).await;
        assert_eq!(result.total, 0);
        assert!(result.trades.is_empty());
    }

    /// The comparator's exact branches: descending time, then ascending sort
    /// group, then DESCENDING NUMERIC log index within one transaction --
    /// which is why `:20` must precede `:11` and `:2`.
    #[tokio::test]
    async fn ordering_reproduces_the_dto_comparator() {
        let pool = setup_test_db().await;
        let onchain = onchain_store(&pool).await;
        let offchain = offchain_store(&pool).await;
        let tied = DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
        let older = DateTime::from_timestamp(1_700_000_000, 123_456_788).unwrap();

        for log_index in [2_u64, 11, 20] {
            witness(
                &onchain,
                &OnChainTradeId {
                    tx_hash: tx_hash(0xaa),
                    log_index,
                },
                OnChainTradeSource::Raindex,
                "AAPL",
                tied,
            )
            .await;
        }
        witness(
            &onchain,
            &OnChainTradeId {
                tx_hash: tx_hash(0xab),
                log_index: 0,
            },
            OnChainTradeSource::Raindex,
            "AAPL",
            tied,
        )
        .await;
        witness(
            &onchain,
            &OnChainTradeId {
                tx_hash: tx_hash(0xac),
                log_index: 0,
            },
            OnChainTradeSource::Raindex,
            "AAPL",
            older,
        )
        .await;
        fill(&offchain, &OffchainOrderId::new(), tied).await;

        let from_sql = page(&pool, &TradeQuery::newest(V3)).await.trades;
        let mut expected = from_sql.clone();
        sort_trades_newest_first(&mut expected);

        assert_eq!(
            from_sql.iter().map(|t| &t.id).collect::<Vec<_>>(),
            expected.iter().map(|t| &t.id).collect::<Vec<_>>(),
            "SQL ordering must equal sort_trades_newest_first"
        );
        let same_tx: Vec<&String> = from_sql
            .iter()
            .map(|trade| &trade.id)
            .filter(|id| id.starts_with(&format!("0x{}", "aa".repeat(32))))
            .collect();
        assert_eq!(
            same_tx,
            [
                format!("0x{}:20", "aa".repeat(32)),
                format!("0x{}:11", "aa".repeat(32)),
                format!("0x{}:2", "aa".repeat(32)),
            ]
            .iter()
            .collect::<Vec<_>>(),
            "same-transaction fills tie-break on numeric log index, descending"
        );
    }

    #[tokio::test]
    async fn venue_filter_collapses_adapter_attribution_for_legacy_protocols_only() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let id = OnChainTradeId {
            tx_hash: tx_hash(0),
            log_index: 0,
        };
        witness(
            &store,
            &id,
            OnChainTradeSource::Inventory {
                operator: alloy::primitives::Address::repeat_byte(0x8b),
                venue: InventoryVenue::Bebop,
            },
            "AAPL",
            Utc::now(),
        )
        .await;

        let raindex = |protocol| TradeQuery {
            venues: Some(vec![TradingVenue::Raindex]),
            ..TradeQuery::newest(protocol)
        };

        assert_eq!(
            ids(&pool, &raindex(V2)).await,
            [id.to_string()],
            "a legacy protocol collapses bebop onto raindex"
        );
        assert!(
            ids(&pool, &raindex(V3)).await.is_empty(),
            "v3 distinguishes bebop from raindex"
        );
        assert_eq!(
            ids(
                &pool,
                &TradeQuery {
                    venues: Some(vec![TradingVenue::Bebop]),
                    ..TradeQuery::newest(V3)
                }
            )
            .await,
            [id.to_string()]
        );
        assert!(
            ids(
                &pool,
                &TradeQuery {
                    venues: Some(vec![TradingVenue::Alpaca]),
                    ..TradeQuery::newest(V3)
                }
            )
            .await
            .is_empty(),
            "an offchain-only venue filter must exclude the onchain view"
        );
    }

    /// The protocol narrows outcomes in SQL, before the page limit -- so a
    /// client on an older protocol still gets a full page of the outcomes it
    /// understands rather than a page padded with ones it does not.
    #[tokio::test]
    async fn protocol_narrows_outcomes_before_the_page_limit() {
        let pool = setup_test_db().await;
        let store = offchain_store(&pool).await;
        let now = Utc::now();

        for index in 0..MAX_TRADES {
            let id = OffchainOrderId::new();
            place(&store, &id, "NVDA").await;
            store
                .send(
                    &id,
                    OffchainOrderCommand::MarkPlacementFailed {
                        error: format!("failure {index}"),
                    },
                )
                .await
                .unwrap();
        }
        let filled_id = OffchainOrderId::new();
        fill(&store, &filled_id, now - chrono::Duration::days(1)).await;

        let legacy = page(&pool, &TradeQuery::newest(TradeProtocol::LegacyFills)).await;

        assert_eq!(legacy.total, 1);
        assert_eq!(legacy.trades.len(), 1);
        assert_eq!(legacy.trades[0].id, filled_id.to_string());
    }

    #[tokio::test]
    async fn symbol_and_time_filters_apply_in_sql() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let early = DateTime::from_timestamp(1_700_000_000, 100).unwrap();
        let late = DateTime::from_timestamp(1_700_000_000, 200).unwrap();
        let early_id = OnChainTradeId {
            tx_hash: tx_hash(1),
            log_index: 0,
        };
        let late_id = OnChainTradeId {
            tx_hash: tx_hash(2),
            log_index: 0,
        };
        witness(
            &store,
            &early_id,
            OnChainTradeSource::Raindex,
            "AAPL",
            early,
        )
        .await;
        witness(&store, &late_id, OnChainTradeSource::Raindex, "TSLA", late).await;

        assert_eq!(
            ids(
                &pool,
                &TradeQuery {
                    symbols: Some(vec![Symbol::new("AAPL").unwrap()]),
                    ..TradeQuery::newest(V3)
                }
            )
            .await,
            [early_id.to_string()]
        );

        // Both bounds are inclusive, and discriminate at nanosecond distance.
        assert_eq!(
            ids(
                &pool,
                &TradeQuery {
                    since: Some(late),
                    ..TradeQuery::newest(V3)
                }
            )
            .await,
            [late_id.to_string()]
        );
        assert_eq!(
            ids(
                &pool,
                &TradeQuery {
                    until: Some(early),
                    ..TradeQuery::newest(V3)
                }
            )
            .await,
            [early_id.to_string()]
        );
        assert_eq!(
            page(
                &pool,
                &TradeQuery {
                    since: Some(early),
                    until: Some(late),
                    ..TradeQuery::newest(V3)
                }
            )
            .await
            .total,
            2,
            "an inclusive range spanning both must keep both"
        );
    }

    #[tokio::test]
    async fn pagination_walks_the_whole_history_without_gaps_or_repeats() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let now = Utc::now();
        for log_index in 0..7_u64 {
            witness(
                &store,
                &OnChainTradeId {
                    tx_hash: tx_hash(0),
                    log_index,
                },
                OnChainTradeSource::Raindex,
                "AAPL",
                now - chrono::Duration::seconds(log_index.cast_signed()),
            )
            .await;
        }

        let all = ids(&pool, &TradeQuery::newest(V3)).await;
        assert_eq!(all.len(), 7);

        let mut paged = Vec::new();
        for offset in (0..7).step_by(3) {
            let result = page(
                &pool,
                &TradeQuery {
                    limit: 3,
                    offset,
                    ..TradeQuery::newest(V3)
                },
            )
            .await;
            assert_eq!(result.total, 7, "total counts matches, not the page");
            paged.extend(result.trades.into_iter().map(|trade| trade.id));
        }
        assert_eq!(paged, all);
    }

    /// `/trades` accepts any `usize` offset, so the clamp to SQLite's `i64`
    /// must page past the end rather than fail the request.
    #[tokio::test]
    async fn offset_beyond_the_end_returns_an_empty_page() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        witness(
            &store,
            &OnChainTradeId {
                tx_hash: tx_hash(0),
                log_index: 0,
            },
            OnChainTradeSource::Raindex,
            "AAPL",
            Utc::now(),
        )
        .await;

        for offset in [2, usize::MAX] {
            let result = page(
                &pool,
                &TradeQuery {
                    offset,
                    ..TradeQuery::newest(V3)
                },
            )
            .await;
            assert_eq!(result.total, 1);
            assert!(result.trades.is_empty(), "offset {offset} must page past");
        }
    }

    /// A row whose payload cannot be read must not take the page down with it,
    /// matching how trade history has always treated unreadable rows.
    #[tokio::test]
    async fn unreadable_rows_are_skipped_with_the_rest_of_the_page_intact() {
        let pool = setup_test_db().await;
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind("00000000-0000-0000-0000-000000000142")
            .bind(r#"{"Live":{"Failed":{"symbol":"SPCX","failed_at":"2026-01-01T00:00:01Z"}}}"#)
            .execute(&pool)
            .await
            .unwrap();
        let valid_id = "00000000-0000-0000-0000-000000000143";
        sqlx::query("INSERT INTO offchain_order_view (view_id, version, payload) VALUES (?, 1, ?)")
            .bind(valid_id)
            .bind(
                r#"{"Live":{"Failed":{"symbol":"SPCX","shares":"1","direction":"Buy","executor":"AlpacaBrokerApi","retained_fill":null,"executor_order_id":null,"error":"asset is not tradable","placed_at":"2026-01-01T00:00:00Z","failed_at":"2026-01-01T00:00:02Z"}}}"#,
            )
            .execute(&pool)
            .await
            .unwrap();

        let page = page(&pool, &TradeQuery::newest(V3)).await;

        assert_eq!(page.trades.len(), 1);
        assert_eq!(page.trades[0].id, valid_id);
        // `total` counts matching rows, so it still sees the row the page
        // dropped -- but `has_more` comes from the rows the query consumed, so
        // the client is not sent back for a page that cannot exist.
        assert_eq!(page.total, 2);
        assert!(!page.has_more);
    }

    /// The page a dashboard client gets on connect is capped, however much
    /// history exists behind it.
    #[tokio::test]
    async fn the_default_page_is_capped_at_max_trades() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let at = Utc::now();

        for log_index in 0..=u64::try_from(MAX_TRADES).unwrap() {
            let id = OnChainTradeId {
                tx_hash: tx_hash(0),
                log_index,
            };
            witness(&store, &id, OnChainTradeSource::Raindex, "AAPL", at).await;
        }

        let page = page(&pool, &TradeQuery::newest(V3)).await;

        assert_eq!(page.trades.len(), MAX_TRADES);
        assert_eq!(page.total, MAX_TRADES + 1);
        assert!(page.has_more);
    }

    /// An unmapped source variant would leave `venue` NULL: invisible to venue
    /// filters yet present in unfiltered history. That silent split must be a
    /// hard error instead.
    /// The drift this guards against cannot be staged from SQL: a source the
    /// view's CASE does not map is a source `OnChainTradeSource` cannot
    /// deserialize either, so a bogus payload is skipped as unreadable long
    /// before venues are compared. Only a new Rust variant added without the
    /// matching CASE arm produces the mismatch, which is why the row is
    /// converted directly -- a real stored payload against the NULL venue that
    /// an unmapped variant would index under.
    #[tokio::test]
    async fn a_venue_the_view_could_not_map_is_an_error_not_a_silent_row() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let id = OnChainTradeId {
            tx_hash: tx_hash(0),
            log_index: 0,
        };
        witness(&store, &id, OnChainTradeSource::Raindex, "AAPL", Utc::now()).await;

        let (view_id, payload): (String, String) =
            sqlx::query_as("SELECT view_id, payload FROM onchain_trade_view")
                .fetch_one(&pool)
                .await
                .unwrap();

        let error =
            convert_row(&view_id, &payload, None, Side::Onchain.discriminant()).unwrap_err();

        assert!(
            matches!(
                error,
                TradeHistoryError::VenueMismatch {
                    indexed: None,
                    replayed: TradingVenue::Raindex,
                    ..
                }
            ),
            "expected a venue mismatch, got {error:?}"
        );
    }

    /// The neighbouring case: a payload neither side can read is a skipped row,
    /// not a mismatch, because the venue comparison is never reached.
    #[tokio::test]
    async fn a_source_neither_side_understands_is_skipped_not_mismatched() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let id = OnChainTradeId {
            tx_hash: tx_hash(0),
            log_index: 0,
        };
        witness(&store, &id, OnChainTradeSource::Raindex, "AAPL", Utc::now()).await;
        sqlx::query(
            "UPDATE onchain_trade_view SET payload = json_set(payload, '$.Live.source', 'Martian')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let page = page(&pool, &TradeQuery::newest(V3)).await;

        assert!(page.trades.is_empty());
    }

    /// Paging in SQL is only a win if each branch walks its ordering index and
    /// stops at the limit. The offchain side is the fragile one: any
    /// index-eligible `status` term makes SQLite prefer
    /// `idx_offchain_order_view_status` and sort every match in a temp b-tree,
    /// which is why the narrowed protocols write `+status`.
    ///
    /// The statements come from the production builders rather than literals,
    /// so losing the `+` in `side_filter` fails here instead of quietly
    /// regressing the endpoint.
    #[tokio::test]
    async fn every_branch_walks_its_ordering_index_instead_of_sorting() {
        let pool = setup_test_db().await;
        // `EXPLAIN QUERY PLAN` answers four columns -- id, parent, notused,
        // detail -- and only the last is the human-readable step. The
        // placeholders stay unbound: the planner never sees their values, so
        // the plan is the one the bound statement gets.
        let plan_for = async |sql: String| {
            sqlx::query(sqlx::AssertSqlSafe(format!("EXPLAIN QUERY PLAN {sql}")))
                .fetch_all(&pool)
                .await
                .unwrap()
                .iter()
                .map(|row| row.get::<String, _>("detail"))
                .collect::<Vec<_>>()
                .join(" | ")
        };

        let branches = [
            ("onchain", Side::Onchain, V3, "idx_onchain_trade_view_order"),
            (
                "offchain, protocol carrying every outcome",
                Side::Offchain,
                V3,
                "idx_offchain_order_view_occurred_at",
            ),
            (
                "offchain, protocol narrowing outcomes",
                Side::Offchain,
                TradeProtocol::LegacyFills,
                "idx_offchain_order_view_occurred_at",
            ),
        ];

        for (branch, side, protocol, index) in branches {
            let query = TradeQuery::newest(protocol);
            let filter = side_filter(&query, side).expect("an unfiltered query keeps both sides");
            let plan = plan_for(branch_sql(side, &filter)).await;

            assert!(
                plan.contains(index),
                "{branch} branch must use {index}: {plan}"
            );
            // The offchain branch does carry a `USE TEMP B-TREE FOR LAST TERM
            // OF ORDER BY` -- its `log_index` is the constant NULL, so the
            // final term sorts blocks of one row. What must never appear is a
            // sort of the whole result, which is what losing the index means.
            assert!(
                !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
                "{branch} branch must not re-sort its rows: {plan}"
            );
        }
    }

    /// The offchain `venue` column restates the `SupportedExecutor` match in
    /// `OffchainOrder::try_into_trade`, and an unmapped executor is fatal at
    /// read time -- so it gets the same compile-forcing guard as the onchain
    /// side.
    #[tokio::test]
    async fn indexed_venue_matches_trading_venue_for_every_executor() {
        let pool = setup_test_db().await;
        let store = offchain_store(&pool).await;

        for executor in [
            SupportedExecutor::AlpacaBrokerApi,
            SupportedExecutor::DryRun,
        ] {
            match executor {
                SupportedExecutor::AlpacaBrokerApi | SupportedExecutor::DryRun => {}
            }

            let id = OffchainOrderId::new();
            fill_with(&store, &id, Utc::now(), executor).await;

            let indexed: String =
                sqlx::query_scalar("SELECT venue FROM offchain_order_view WHERE view_id = ?")
                    .bind(id.to_string())
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let replayed = page(&pool, &TradeQuery::all(V3))
                .await
                .trades
                .into_iter()
                .find(|trade| trade.id == id.to_string())
                .expect("the filled order must reach trade history");

            assert_eq!(indexed, replayed.venue.to_string(), "executor {executor:?}");
        }
    }

    #[test]
    fn expanding_a_venue_filter_matches_the_protocols_venue_equivalence() {
        for requested in TradingVenue::ALL {
            for protocol in [
                TradeProtocol::LegacyFills,
                TradeProtocol::TerminalOutcomesV1,
                TradeProtocol::TerminalOutcomesV2,
                TradeProtocol::TerminalOutcomesV3,
            ] {
                let expanded = expand_venues(&[requested], protocol);
                for stored in TradingVenue::ALL {
                    let matched = match protocol {
                        TradeProtocol::TerminalOutcomesV3 => requested == stored,
                        _ => requested.legacy_compatible() == stored.legacy_compatible(),
                    };
                    assert_eq!(
                        expanded.contains(&stored),
                        matched,
                        "{requested} under {protocol:?} vs stored {stored}"
                    );
                }
            }
        }
    }

    proptest! {
        /// The generated `occurred_at` column has to order timestamps exactly
        /// as `DateTime<Utc>` does. chrono's serde writes 0, 3, 6 or 9
        /// fractional digits, so the raw payload string is NOT sortable --
        /// '.5Z' sorts above '.500000Z', and 'Z' above '.5Z'.
        #[test]
        fn sortable_timestamps_order_identically_to_datetimes(
            seconds in 1_600_000_000_i64..1_900_000_000,
            nanos in 0_u32..1_000_000_000,
            other_seconds in 1_600_000_000_i64..1_900_000_000,
            other_nanos in 0_u32..1_000_000_000,
        ) {
            let left = DateTime::from_timestamp(seconds, nanos).unwrap();
            let right = DateTime::from_timestamp(other_seconds, other_nanos).unwrap();

            prop_assert_eq!(
                sortable_timestamp(left).cmp(&sortable_timestamp(right)),
                left.cmp(&right)
            );
            prop_assert_eq!(sortable_timestamp(left).len(), 29);
        }
    }

    #[test]
    fn onchain_and_offchain_ids_never_interleave_within_a_transaction_group() {
        // The comparator falls back to whole-id order across venues while the
        // query compares only the hash prefix. They agree because an onchain
        // id's second byte is 'x', which is above every hex digit a UUID can
        // start with, so no UUID can sort between two ids sharing a hash.
        let hash = format!("0x{}", "ab".repeat(32));
        for uuid in [
            "00000000-0000-0000-0000-000000000000",
            "0fffffff-ffff-ffff-ffff-ffffffffffff",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ] {
            let mut bounds = [format!("{hash}:2"), format!("{hash}:11")];
            bounds.sort();
            let [low, high] = &bounds;
            let between = uuid > low.as_str() && uuid < high.as_str();
            assert!(!between, "{uuid} must not sort between ids sharing a hash");
        }
    }

    /// `parse_row` deserializes the payload before it parses the id, so the id
    /// branch is only reachable with a payload that reads -- and the payload
    /// comes from the projection rather than a literal, so it cannot drift
    /// from what the aggregate actually serializes.
    #[tokio::test]
    async fn a_malformed_onchain_view_id_is_a_row_error_not_a_panic() {
        let pool = setup_test_db().await;
        let store = onchain_store(&pool).await;
        let id = OnChainTradeId {
            tx_hash: tx_hash(0),
            log_index: 0,
        };
        witness(&store, &id, OnChainTradeSource::Raindex, "AAPL", Utc::now()).await;
        let payload: String =
            sqlx::query_scalar("SELECT payload FROM onchain_trade_view WHERE view_id = ?")
                .bind(id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();

        let error = parse_row("not-a-trade-id", &payload, Side::Onchain).unwrap_err();

        assert!(
            matches!(
                error,
                TradeRowError::OnchainId(ParseOnChainTradeIdError::MissingDelimiter { .. })
            ),
            "expected a view id error, got {error:?}"
        );
    }
}
