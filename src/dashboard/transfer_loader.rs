//! Loads cross-venue transfer state for dashboard display.

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::Deserialize;
use sqlx::{Row, SqlitePool};
use thiserror::Error;
use tracing::warn;

use std::fmt::{self, Display};
use std::num::TryFromIntError;
use std::str::FromStr;

use st0x_dto::{TransferOperation, TransferWarning};
use st0x_finance::Id;
use st0x_tokenization::IssuerRequestId;

use crate::equity_redemption::{EquityRedemption, RedemptionAggregateId};
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::{UsdcRebalance, UsdcRebalanceId};

/// The three categories of cross-venue transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferKind {
    EquityMint,
    EquityRedemption,
    UsdcBridge,
}

impl TransferKind {
    /// The cqrs-es aggregate type stored in the `events` table.
    pub(crate) fn aggregate_type(self) -> &'static str {
        match self {
            Self::EquityMint => "TokenizedEquityMint",
            Self::EquityRedemption => "EquityRedemption",
            Self::UsdcBridge => "UsdcRebalance",
        }
    }

    fn table(self) -> &'static str {
        match self {
            Self::EquityMint => "tokenized_equity_mint_view",
            Self::EquityRedemption => "equity_redemption_view",
            Self::UsdcBridge => "usdc_rebalance_view",
        }
    }

    const fn discriminant(self) -> i64 {
        match self {
            Self::EquityMint => 0,
            Self::EquityRedemption => 1,
            Self::UsdcBridge => 2,
        }
    }

    const fn from_discriminant(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::EquityMint),
            1 => Some(Self::EquityRedemption),
            2 => Some(Self::UsdcBridge),
            _ => None,
        }
    }

    fn warning(self, view_id: &str) -> TransferWarning {
        match self {
            Self::EquityMint => TransferWarning::MintReplayFailed {
                id: Id::new(view_id.to_owned()),
            },
            Self::EquityRedemption => TransferWarning::RedemptionReplayFailed {
                id: Id::new(view_id.to_owned()),
            },
            Self::UsdcBridge => TransferWarning::BridgeReplayFailed {
                id: Id::new(view_id.to_owned()),
            },
        }
    }

    const fn category_unavailable_warning(self) -> TransferWarning {
        match self {
            Self::EquityMint => TransferWarning::MintCategoryUnavailable,
            Self::EquityRedemption => TransferWarning::RedemptionCategoryUnavailable,
            Self::UsdcBridge => TransferWarning::BridgeCategoryUnavailable,
        }
    }
}

impl Display for TransferKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EquityMint => formatter.write_str("equity_mint"),
            Self::EquityRedemption => formatter.write_str("equity_redemption"),
            Self::UsdcBridge => formatter.write_str("usdc_bridge"),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum InvalidTransferKind {
    #[error("unknown transfer kind: {0}")]
    Unknown(String),
}

impl FromStr for TransferKind {
    type Err = InvalidTransferKind;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "equity_mint" => Ok(Self::EquityMint),
            "equity_redemption" => Ok(Self::EquityRedemption),
            "usdc_bridge" => Ok(Self::UsdcBridge),
            other => Err(InvalidTransferKind::Unknown(other.to_owned())),
        }
    }
}

const ALL_TRANSFER_KINDS: [TransferKind; 3] = [
    TransferKind::EquityMint,
    TransferKind::EquityRedemption,
    TransferKind::UsdcBridge,
];

#[derive(Debug, Default)]
pub(crate) struct TransferHistoryQuery {
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    pub(crate) kinds: Option<Vec<TransferKind>>,
    pub(crate) since: Option<DateTime<Utc>>,
    pub(crate) until: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub(crate) struct TransferHistoryPage {
    pub(crate) operations: Vec<TransferOperation>,
    pub(crate) warnings: Vec<TransferWarning>,
    pub(crate) total: usize,
    pub(crate) has_more: bool,
}

#[derive(Debug, Error)]
pub(crate) enum TransferHistoryError {
    #[error("failed to query transfer history: {0}")]
    Database(#[from] sqlx::Error),
    #[error("transfer history matched {total} rows, which does not fit a usize")]
    CountOutOfRange {
        total: i64,
        #[source]
        source: TryFromIntError,
    },
    #[error("transfer history query produced unknown kind discriminant {value}")]
    UnknownKind { value: i64 },
}

#[derive(Default)]
struct HistoryFilter {
    clauses: Vec<&'static str>,
    binds: Vec<String>,
}

impl HistoryFilter {
    fn from_query(query: &TransferHistoryQuery) -> Self {
        let mut filter = Self::default();

        if let Some(since) = query.since {
            filter.clauses.push("started_at >= ?");
            filter.binds.push(sortable_timestamp(since));
        }

        if let Some(until) = query.until {
            filter.clauses.push("started_at <= ?");
            filter.binds.push(sortable_timestamp(until));
        }

        filter
    }

    fn where_sql(&self) -> String {
        std::iter::once("started_at IS NOT NULL")
            .chain(self.clauses.iter().copied())
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

/// Query one bounded page of transfer history from aggregate projections.
pub(crate) async fn query_transfer_history(
    pool: &SqlitePool,
    query: &TransferHistoryQuery,
) -> Result<TransferHistoryPage, TransferHistoryError> {
    let kinds: Vec<TransferKind> = ALL_TRANSFER_KINDS
        .into_iter()
        .filter(|kind| {
            query
                .kinds
                .as_ref()
                .is_none_or(|requested| requested.contains(kind))
        })
        .collect();
    if kinds.is_empty() {
        return Ok(TransferHistoryPage {
            operations: Vec::new(),
            warnings: Vec::new(),
            total: 0,
            has_more: false,
        });
    }

    let filter = HistoryFilter::from_query(query);
    let total = count_transfer_rows(pool, &kinds, &filter).await?;
    let (operations, warnings, rows_read) =
        fetch_transfer_page(pool, query, &kinds, &filter).await?;

    Ok(TransferHistoryPage {
        operations,
        warnings,
        total,
        has_more: query.offset.saturating_add(rows_read) < total,
    })
}

async fn count_transfer_rows(
    pool: &SqlitePool,
    kinds: &[TransferKind],
    filter: &HistoryFilter,
) -> Result<usize, TransferHistoryError> {
    let mut total = 0_i64;

    for kind in kinds {
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            kind.table(),
            filter.where_sql()
        );
        let mut query = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql));
        for bind in &filter.binds {
            query = query.bind(bind);
        }
        total += query.fetch_one(pool).await?;
    }

    usize::try_from(total).map_err(|source| TransferHistoryError::CountOutOfRange { total, source })
}

async fn fetch_transfer_page(
    pool: &SqlitePool,
    query: &TransferHistoryQuery,
    kinds: &[TransferKind],
    filter: &HistoryFilter,
) -> Result<(Vec<TransferOperation>, Vec<TransferWarning>, usize), TransferHistoryError> {
    let bound = clamp_to_i64(query.offset.saturating_add(query.limit));
    let sql = format!(
        "SELECT view_id, payload, kind FROM ({}) \
         ORDER BY started_at DESC, kind ASC, view_id ASC LIMIT ? OFFSET ?",
        kinds
            .iter()
            .map(|kind| transfer_branch_sql(*kind, filter))
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    );

    let mut page = sqlx::query(sqlx::AssertSqlSafe(sql));
    for _kind in kinds {
        for bind in &filter.binds {
            page = page.bind(bind);
        }
        page = page.bind(bound);
    }
    page = page.bind(clamp_to_i64(query.limit));
    page = page.bind(clamp_to_i64(query.offset));

    let rows = page.fetch_all(pool).await?;
    let rows_read = rows.len();
    let mut operations = Vec::with_capacity(rows_read);
    let mut warnings = Vec::new();

    for row in rows {
        let view_id: String = row.try_get("view_id")?;
        let payload: String = row.try_get("payload")?;
        let kind_value: i64 = row.try_get("kind")?;
        let kind = TransferKind::from_discriminant(kind_value)
            .ok_or(TransferHistoryError::UnknownKind { value: kind_value })?;

        match convert_projection_row(kind, &view_id, &payload) {
            Ok(operation) => operations.push(operation),
            Err(error) => {
                warn!(
                    target: "dashboard",
                    %view_id,
                    %kind,
                    %error,
                    "Skipping unreadable transfer history row"
                );
                warnings.push(kind.warning(&view_id));
            }
        }
    }

    Ok((operations, warnings, rows_read))
}

fn transfer_branch_sql(kind: TransferKind, filter: &HistoryFilter) -> String {
    format!(
        "SELECT * FROM (SELECT view_id, payload, started_at, {kind} AS kind \
         FROM {table} WHERE {predicates} \
         ORDER BY started_at DESC, view_id ASC LIMIT ?)",
        kind = kind.discriminant(),
        table = kind.table(),
        predicates = filter.where_sql(),
    )
}

fn sortable_timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Nanos, true)
        .trim_end_matches('Z')
        .to_owned()
}

fn clamp_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn convert_projection_row(
    kind: TransferKind,
    view_id: &str,
    payload: &str,
) -> Result<TransferOperation, TransferRowError> {
    match kind {
        TransferKind::EquityMint => {
            let ProjectionPayload::Live(entity): ProjectionPayload<TokenizedEquityMint> =
                serde_json::from_str(payload)?;
            let id = IssuerRequestId::from_str(view_id)?;
            Ok(entity.to_dto(&id))
        }
        TransferKind::EquityRedemption => {
            let ProjectionPayload::Live(entity): ProjectionPayload<EquityRedemption> =
                serde_json::from_str(payload)?;
            let id = RedemptionAggregateId::from_str(view_id)?;
            Ok(entity.to_dto(&id))
        }
        TransferKind::UsdcBridge => {
            let ProjectionPayload::Live(entity): ProjectionPayload<UsdcRebalance> =
                serde_json::from_str(payload)?;
            let id = UsdcRebalanceId::from_str(view_id)?;
            Ok(entity.to_dto(&id))
        }
    }
}

#[derive(Deserialize)]
enum ProjectionPayload<Entity> {
    Live(Entity),
}

#[derive(Debug, Error)]
enum TransferRowError {
    #[error("invalid projection payload: {0}")]
    Payload(#[from] serde_json::Error),
    #[error("invalid transfer aggregate id: {0}")]
    Id(#[from] uuid::Error),
}

/// Loaded transfers split into active (in-progress) and recent (terminal).
pub(crate) struct LoadedTransfers {
    pub(crate) active: Vec<TransferOperation>,
    pub(crate) recent: Vec<TransferOperation>,
    pub(crate) warnings: Vec<TransferWarning>,
}

/// Load transfer projections for the dashboard WebSocket seed.
///
/// Active: non-terminal transfers (in progress).
/// Recent: terminal transfers (completed/failed) within the last 24 hours.
pub(crate) async fn load_transfers(pool: &SqlitePool) -> LoadedTransfers {
    let cutoff = Utc::now() - Duration::hours(24);

    let (mint, redemption, usdc) = tokio::join!(
        load_category(pool, cutoff, TransferKind::EquityMint),
        load_category(pool, cutoff, TransferKind::EquityRedemption),
        load_category(pool, cutoff, TransferKind::UsdcBridge),
    );
    let categories: [CategoryResult; 3] = (mint, redemption, usdc).into();

    let merged = categories
        .into_iter()
        .fold(CategoryResult::empty(), |mut acc, result| {
            acc.active.extend(result.active);
            acc.recent.extend(result.recent);
            acc.warnings.extend(result.warnings);
            acc
        });

    let mut active = merged.active;
    let mut recent = merged.recent;

    active.sort_by_key(|transfer| std::cmp::Reverse(transfer.updated_at()));
    recent.sort_by_key(|transfer| std::cmp::Reverse(transfer.updated_at()));

    LoadedTransfers {
        active,
        recent,
        warnings: merged.warnings,
    }
}

/// Result of loading a single transfer category.
struct CategoryResult {
    active: Vec<TransferOperation>,
    recent: Vec<TransferOperation>,
    warnings: Vec<TransferWarning>,
}

impl CategoryResult {
    fn empty() -> Self {
        Self {
            active: Vec::new(),
            recent: Vec::new(),
            warnings: Vec::new(),
        }
    }
}

async fn load_category(
    pool: &SqlitePool,
    cutoff: DateTime<Utc>,
    kind: TransferKind,
) -> CategoryResult {
    let sql = format!(
        "SELECT view_id, payload FROM {table} WHERE terminal_at IS NULL \
         UNION ALL \
         SELECT view_id, payload FROM {table} WHERE terminal_at >= ?",
        table = kind.table(),
    );
    let rows = match sqlx::query_as::<_, (String, String)>(sqlx::AssertSqlSafe(sql))
        .bind(sortable_timestamp(cutoff))
        .fetch_all(pool)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            warn!(target: "dashboard", %kind, %error, "Failed to load transfer projections");
            return CategoryResult {
                warnings: vec![kind.category_unavailable_warning()],
                ..CategoryResult::empty()
            };
        }
    };

    let mut transfers = Vec::with_capacity(rows.len());
    let mut warnings = Vec::new();
    for (view_id, payload) in rows {
        match convert_projection_row(kind, &view_id, &payload) {
            Ok(transfer) => transfers.push(transfer),
            Err(error) => {
                warn!(
                    target: "dashboard",
                    %view_id,
                    %kind,
                    %error,
                    "Skipping unreadable transfer seed row"
                );
                warnings.push(kind.warning(&view_id));
            }
        }
    }

    let (active, recent): (Vec<_>, Vec<_>) = transfers
        .into_iter()
        .filter(|transfer| !transfer.is_terminal() || transfer.updated_at() >= cutoff)
        .partition(|transfer| !transfer.is_terminal());

    CategoryResult {
        active,
        recent,
        warnings,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash};
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use st0x_dto::{
        EquityMintOperation, EquityMintStatus, EquityMintTag, EquityRedemptionOperation,
        EquityRedemptionStatus, EquityRedemptionTag, TransferOperation, TransferWarning,
        UsdcBridgeDirection, UsdcBridgeOperation, UsdcBridgeStatus, UsdcBridgeTag,
    };
    use st0x_event_sorcery::{EventSourced, StoreBuilder};
    use st0x_execution::{ClientOrderId, FractionalShares, Symbol};
    use st0x_finance::{Id, Usdc};
    use st0x_float_macro::float;
    use st0x_tokenization::{IssuerRequestId, issuer_request_id};

    use super::*;
    use crate::equity_redemption::{
        EquityRedemptionEvent, RedemptionAggregateId, redemption_aggregate_id,
    };
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::tokenized_equity_mint::TokenizedEquityMintEvent;
    use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceEvent};

    fn mint_transfer(status: EquityMintStatus) -> TransferOperation {
        TransferOperation::EquityMint(EquityMintOperation {
            id: Id::<EquityMintTag>::new("mint-1".to_string()),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: FractionalShares::new(float!(10)),
            status,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }

    fn usdc_transfer(status: UsdcBridgeStatus) -> TransferOperation {
        TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::<UsdcBridgeTag>::new("usdc-1".to_string()),
            direction: UsdcBridgeDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            status,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }

    #[test]
    fn classify_active_transfer() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let transfer = mint_transfer(EquityMintStatus::Minting);

        assert!(!transfer.is_terminal());
        assert!(transfer.updated_at() >= cutoff);
    }

    #[test]
    fn classify_recent_completed_transfer() {
        let transfer = mint_transfer(EquityMintStatus::Completed {
            completed_at: Utc::now(),
        });

        assert!(transfer.is_terminal());
    }

    #[test]
    fn classify_old_completed_transfer_discarded() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);

        let mut transfer = usdc_transfer(UsdcBridgeStatus::Completed {
            completed_at: Utc::now() - chrono::Duration::hours(48),
        });

        if let TransferOperation::UsdcBridge(ref mut op) = transfer {
            op.updated_at = Utc::now() - chrono::Duration::hours(48);
        }

        assert!(transfer.is_terminal());
        assert!(transfer.updated_at() < cutoff);
    }

    #[tokio::test]
    async fn load_transfers_empty_database() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let loaded = load_transfers(&pool).await;

        assert!(loaded.active.is_empty());
        assert!(loaded.recent.is_empty());
        assert!(loaded.warnings.is_empty());
    }

    #[tokio::test]
    async fn load_transfers_reads_materialized_projection_without_events() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let id = issuer_request_id("projected-mint");
        let mint = TokenizedEquityMint::originate(&TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(1),
            wallet: Address::ZERO,
            requested_at: Utc::now(),
        })
        .unwrap();

        sqlx::query(
            "INSERT INTO tokenized_equity_mint_view (view_id, version, payload) \
             VALUES (?1, 1, ?2)",
        )
        .bind(id.to_string())
        .bind(serde_json::json!({ "Live": mint }).to_string())
        .execute(&pool)
        .await
        .unwrap();

        let loaded = load_transfers(&pool).await;

        assert_eq!(loaded.active.len(), 1);
        assert!(loaded.recent.is_empty());
        assert!(loaded.warnings.is_empty());
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        payload: serde_json::Value,
    ) {
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
             event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, ?3, ?4, '1.0', ?5, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(serde_json::to_string(&payload).unwrap())
        .execute(pool)
        .await
        .unwrap();
    }

    struct SeededTransferIds {
        active_mint: IssuerRequestId,
        failed_mint: IssuerRequestId,
        active_redemption: RedemptionAggregateId,
        usdc: Uuid,
    }

    /// Seeds the database with transfer events spanning all three aggregate
    /// types and covering active, recent-terminal, and old-terminal cases.
    async fn seed_transfer_events(pool: &SqlitePool) -> SeededTransferIds {
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);
        let two_days_ago = now - Duration::hours(48);

        let active_mint_id = issuer_request_id("active-mint-1");
        let failed_mint_id = issuer_request_id("failed-mint-1");

        // 1. Active mint (non-terminal: only MintRequested)
        insert_event(
            pool,
            "TokenizedEquityMint",
            &active_mint_id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintRequested",
            serde_json::to_value(TokenizedEquityMintEvent::MintRequested {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(10),
                wallet: Address::ZERO,
                requested_at: now,
            })
            .unwrap(),
        )
        .await;

        // 2. Recent failed mint (terminal, within 24h)
        insert_event(
            pool,
            "TokenizedEquityMint",
            &failed_mint_id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintRequested",
            serde_json::to_value(TokenizedEquityMintEvent::MintRequested {
                symbol: Symbol::new("TSLA").unwrap(),
                quantity: float!(5),
                wallet: Address::ZERO,
                requested_at: one_hour_ago,
            })
            .unwrap(),
        )
        .await;

        insert_event(
            pool,
            "TokenizedEquityMint",
            &failed_mint_id.to_string(),
            2,
            "TokenizedEquityMintEvent::MintRejected",
            serde_json::to_value(TokenizedEquityMintEvent::MintRejected {
                reason: "rejected".to_string(),
                rejected_at: one_hour_ago,
            })
            .unwrap(),
        )
        .await;

        let old_redemption_id = redemption_aggregate_id("old-redemption-1");
        let active_redemption_id = redemption_aggregate_id("active-redemption-1");

        // 3. Old failed redemption (terminal, >24h ago -- should NOT appear)
        insert_event(
            pool,
            "EquityRedemption",
            &old_redemption_id.to_string(),
            1,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            serde_json::to_value(EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: Symbol::new("MSFT").unwrap(),
                quantity: float!(20),
                token: Address::ZERO,
                wrapped_amount: alloy::primitives::U256::from(20),
                actual_wrapped_amount: None,
                raindex_withdraw_tx: TxHash::ZERO,
                raindex_withdraw_block: None,
                withdrawn_at: two_days_ago,
            })
            .unwrap(),
        )
        .await;

        insert_event(
            pool,
            "EquityRedemption",
            &old_redemption_id.to_string(),
            2,
            "EquityRedemptionEvent::TransferFailed",
            serde_json::to_value(EquityRedemptionEvent::TransferFailed {
                tx_hash: None,
                reason: None,
                failed_at: two_days_ago,
            })
            .unwrap(),
        )
        .await;

        // 4. Active redemption (non-terminal: only WithdrawnFromRaindex)
        insert_event(
            pool,
            "EquityRedemption",
            &active_redemption_id.to_string(),
            1,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            serde_json::to_value(EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: Symbol::new("NVDA").unwrap(),
                quantity: float!(15),
                token: Address::ZERO,
                wrapped_amount: alloy::primitives::U256::from(15),
                actual_wrapped_amount: None,
                raindex_withdraw_tx: TxHash::ZERO,
                raindex_withdraw_block: None,
                withdrawn_at: now,
            })
            .unwrap(),
        )
        .await;

        // 5. Recent failed USDC rebalance (terminal, within 24h)
        let usdc_id = Uuid::new_v4();

        insert_event(
            pool,
            "UsdcRebalance",
            &usdc_id.to_string(),
            1,
            "UsdcRebalanceEvent::ConversionInitiated",
            serde_json::to_value(UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(500)),
                order_id: ClientOrderId::from_uuid(usdc_id),
                initiated_at: one_hour_ago,
            })
            .unwrap(),
        )
        .await;

        insert_event(
            pool,
            "UsdcRebalance",
            &usdc_id.to_string(),
            2,
            "UsdcRebalanceEvent::ConversionFailed",
            serde_json::to_value(UsdcRebalanceEvent::ConversionFailed {
                reason: "insufficient funds".to_string(),
                failed_at: one_hour_ago,
            })
            .unwrap(),
        )
        .await;

        SeededTransferIds {
            active_mint: active_mint_id,
            failed_mint: failed_mint_id,
            active_redemption: active_redemption_id,
            usdc: usdc_id,
        }
    }

    async fn backfill_transfer_projections(pool: &SqlitePool) {
        let _ = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .build(EquityTransferServices::panicking())
            .await
            .unwrap();
        let _ = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .build(EquityTransferServices::panicking())
            .await
            .unwrap();
        let _ = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn load_transfers_non_empty_database() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let seeded = seed_transfer_events(&pool).await;
        backfill_transfer_projections(&pool).await;
        let loaded = load_transfers(&pool).await;

        // Active should contain the in-progress mint and in-progress redemption
        assert_eq!(
            loaded.active.len(),
            2,
            "expected 2 active transfers, got: {:?}",
            loaded.active
        );

        let has_active_mint = loaded.active.iter().any(|transfer| {
            matches!(
                transfer,
                TransferOperation::EquityMint(EquityMintOperation {
                    status: EquityMintStatus::Minting,
                    ..
                })
            )
        });
        assert!(has_active_mint, "expected active Minting transfer");

        let active_mint = loaded
            .active
            .iter()
            .find(|transfer| matches!(transfer, TransferOperation::EquityMint(_)))
            .unwrap();
        if let TransferOperation::EquityMint(op) = active_mint {
            assert_eq!(
                op.id,
                Id::<EquityMintTag>::new(seeded.active_mint.to_string())
            );
        }

        let has_active_redemption = loaded.active.iter().any(|transfer| {
            matches!(
                transfer,
                TransferOperation::EquityRedemption(EquityRedemptionOperation {
                    status: EquityRedemptionStatus::Withdrawing,
                    ..
                })
            )
        });
        assert!(
            has_active_redemption,
            "expected active Withdrawing redemption"
        );

        let active_redemption = loaded
            .active
            .iter()
            .find(|transfer| matches!(transfer, TransferOperation::EquityRedemption(_)))
            .unwrap();
        if let TransferOperation::EquityRedemption(op) = active_redemption {
            assert_eq!(
                op.id,
                Id::<EquityRedemptionTag>::new(seeded.active_redemption.to_string())
            );
        }

        // Recent should contain the recently failed mint and the USDC rebalance,
        // but NOT the old redemption
        assert_eq!(
            loaded.recent.len(),
            2,
            "expected 2 recent transfers, got: {:?}",
            loaded.recent
        );

        let has_failed_mint = loaded.recent.iter().any(|transfer| {
            matches!(
                transfer,
                TransferOperation::EquityMint(EquityMintOperation {
                    status: EquityMintStatus::Failed { .. },
                    ..
                })
            )
        });
        assert!(has_failed_mint, "expected a recently failed mint in recent");

        let has_failed_usdc = loaded.recent.iter().any(|transfer| {
            matches!(
                transfer,
                TransferOperation::UsdcBridge(UsdcBridgeOperation {
                    status: UsdcBridgeStatus::Failed { .. },
                    ..
                })
            )
        });
        assert!(
            has_failed_usdc,
            "expected a recently failed USDC bridge in recent"
        );

        // Verify the old redemption is excluded
        let has_old_redemption = loaded
            .active
            .iter()
            .chain(loaded.recent.iter())
            .any(|transfer| {
                matches!(
                    transfer,
                    TransferOperation::EquityRedemption(st0x_dto::EquityRedemptionOperation {
                        status: EquityRedemptionStatus::Failed { .. },
                        ..
                    })
                )
            });
        assert!(
            !has_old_redemption,
            "old failed redemption should not appear in active or recent"
        );

        // Verify IDs appear correctly in the returned DTOs
        if let TransferOperation::UsdcBridge(usdc_op) = loaded
            .recent
            .iter()
            .find(|transfer| matches!(transfer, TransferOperation::UsdcBridge(_)))
            .unwrap()
        {
            assert_eq!(
                usdc_op.id,
                Id::<UsdcBridgeTag>::new(seeded.usdc.to_string()),
                "USDC bridge ID should match the aggregate_id"
            );
        }

        if let TransferOperation::EquityMint(mint_op) = loaded
            .recent
            .iter()
            .find(|transfer| matches!(transfer, TransferOperation::EquityMint(_)))
            .unwrap()
        {
            assert_eq!(
                mint_op.id,
                Id::<EquityMintTag>::new(seeded.failed_mint.to_string()),
                "failed mint ID should match the aggregate_id"
            );
        }

        // No warnings when all loads succeed
        assert!(
            loaded.warnings.is_empty(),
            "expected no warnings, got: {:?}",
            loaded.warnings
        );
    }

    #[test]
    fn transfer_kind_round_trips_through_display_and_parse() {
        for kind in [
            TransferKind::EquityMint,
            TransferKind::EquityRedemption,
            TransferKind::UsdcBridge,
        ] {
            let text = kind.to_string();
            let parsed: TransferKind = text.parse().unwrap();
            assert_eq!(parsed, kind);
        }
    }

    #[test]
    fn transfer_kind_rejects_unknown_value() {
        assert!("invalid".parse::<TransferKind>().is_err());
    }

    #[test]
    fn transfer_kind_maps_to_aggregate_types() {
        assert_eq!(
            TransferKind::EquityMint.aggregate_type(),
            "TokenizedEquityMint"
        );
        assert_eq!(
            TransferKind::EquityRedemption.aggregate_type(),
            "EquityRedemption"
        );
        assert_eq!(TransferKind::UsdcBridge.aggregate_type(), "UsdcRebalance");
    }

    #[tokio::test]
    async fn transfer_history_returns_warnings_for_malformed_projection() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let bad_mint_id = issuer_request_id("bad-mint-1");
        let payload = serde_json::json!({
            "Live": {
                "MintRequested": {
                    "requested_at": Utc::now(),
                    "malformed": true
                }
            }
        });
        sqlx::query(
            "INSERT INTO tokenized_equity_mint_view (view_id, version, payload) \
             VALUES (?1, 1, ?2)",
        )
        .bind(bad_mint_id.to_string())
        .bind(payload.to_string())
        .execute(&pool)
        .await
        .unwrap();

        let result = query_transfer_history(
            &pool,
            &TransferHistoryQuery {
                limit: 100,
                ..TransferHistoryQuery::default()
            },
        )
        .await
        .unwrap();

        assert!(result.operations.is_empty());
        assert_eq!(result.total, 1);
        assert_eq!(result.warnings.len(), 1, "expected one warning");
        match result.warnings.as_slice() {
            [TransferWarning::MintReplayFailed { id }] => {
                assert_eq!(id, &Id::<EquityMintTag>::new(bad_mint_id.to_string()));
            }
            other => panic!("expected MintReplayFailed, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn transfer_history_filters_by_kind_before_paging() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let now = Utc::now();
        let mint_id = issuer_request_id("mint-filter");
        let mint = TokenizedEquityMint::originate(&TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: float!(1),
            wallet: Address::ZERO,
            requested_at: now,
        })
        .unwrap();
        sqlx::query(
            "INSERT INTO tokenized_equity_mint_view (view_id, version, payload) \
             VALUES (?1, 1, ?2)",
        )
        .bind(mint_id.to_string())
        .bind(serde_json::json!({ "Live": mint }).to_string())
        .execute(&pool)
        .await
        .unwrap();

        let redemption_id = redemption_aggregate_id("redemption-filter");
        let redemption = EquityRedemption::VaultWithdrawPending {
            symbol: Symbol::new("MSFT").unwrap(),
            quantity: float!(2),
            token: Address::ZERO,
            wrapped_amount: alloy::primitives::U256::from(2),
            pending_at: now,
        };
        sqlx::query(
            "INSERT INTO equity_redemption_view (view_id, version, payload) \
             VALUES (?1, 1, ?2)",
        )
        .bind(redemption_id.to_string())
        .bind(serde_json::json!({ "Live": redemption }).to_string())
        .execute(&pool)
        .await
        .unwrap();

        let usdc_id = Uuid::new_v4();
        let usdc = UsdcRebalance::Converting {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(100)),
            order_id: ClientOrderId::from_uuid(usdc_id),
            initiated_at: now,
        };
        sqlx::query(
            "INSERT INTO usdc_rebalance_view (view_id, version, payload) \
             VALUES (?1, 1, ?2)",
        )
        .bind(usdc_id.to_string())
        .bind(serde_json::json!({ "Live": usdc }).to_string())
        .execute(&pool)
        .await
        .unwrap();

        let mint_only = query_transfer_history(
            &pool,
            &TransferHistoryQuery {
                limit: 100,
                kinds: Some(vec![TransferKind::EquityMint]),
                ..TransferHistoryQuery::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(mint_only.total, 1);
        assert!(
            mint_only
                .operations
                .iter()
                .all(|op| { matches!(op, TransferOperation::EquityMint(_)) }),
            "expected only mint operations, got: {:?}",
            mint_only.operations
        );

        assert!(
            !mint_only.operations.is_empty(),
            "expected at least one mint operation"
        );

        let deduplicated = query_transfer_history(
            &pool,
            &TransferHistoryQuery {
                limit: 100,
                kinds: Some(vec![TransferKind::EquityMint, TransferKind::EquityMint]),
                ..TransferHistoryQuery::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(deduplicated.total, 1);

        let all = query_transfer_history(
            &pool,
            &TransferHistoryQuery {
                limit: 100,
                ..TransferHistoryQuery::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(all.total, 3);
        assert_eq!(all.operations.len(), 3);
    }

    #[tokio::test]
    async fn transfer_history_pages_projection_rows_before_decoding() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let times = [
            Utc::now() - Duration::hours(3),
            Utc::now() - Duration::hours(2),
            Utc::now() - Duration::hours(1),
        ];

        for (index, requested_at) in times.iter().copied().enumerate() {
            let label = format!("mint-{index}");
            let id = issuer_request_id(&label);
            let entity = TokenizedEquityMint::originate(&TokenizedEquityMintEvent::MintRequested {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(1),
                wallet: Address::ZERO,
                requested_at,
            })
            .unwrap();
            let payload = serde_json::json!({ "Live": entity });

            sqlx::query(
                "INSERT INTO tokenized_equity_mint_view (view_id, version, payload) \
                 VALUES (?1, 1, ?2)",
            )
            .bind(id.to_string())
            .bind(payload.to_string())
            .execute(&pool)
            .await
            .unwrap();
        }

        sqlx::query(
            "INSERT INTO tokenized_equity_mint_view (view_id, version, payload) \
             VALUES ('unreadable-old-row', 1, '{\"Live\":{\"invalid\":true}}')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let page = query_transfer_history(
            &pool,
            &TransferHistoryQuery {
                limit: 2,
                ..TransferHistoryQuery::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(page.total, 3);
        assert!(page.has_more);
        assert_eq!(page.operations.len(), 2);
        assert!(page.warnings.is_empty());
        assert!(page.operations[0].started_at() > page.operations[1].started_at());

        let bounded = query_transfer_history(
            &pool,
            &TransferHistoryQuery {
                limit: 100,
                since: Some(times[1]),
                until: Some(times[2]),
                ..TransferHistoryQuery::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(bounded.total, 2);
        assert_eq!(bounded.operations.len(), 2);
    }

    #[tokio::test]
    async fn transfer_projection_backfills_existing_aggregates() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let id = issuer_request_id("backfilled-mint");
        insert_event(
            &pool,
            "TokenizedEquityMint",
            &id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintRequested",
            serde_json::to_value(TokenizedEquityMintEvent::MintRequested {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(1),
                wallet: Address::ZERO,
                requested_at: Utc::now(),
            })
            .unwrap(),
        )
        .await;

        let (_store, _projection) = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .build(EquityTransferServices::panicking())
            .await
            .unwrap();

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tokenized_equity_mint_view WHERE view_id = ?1",
        )
        .bind(id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1);

        let page = query_transfer_history(
            &pool,
            &TransferHistoryQuery {
                limit: 100,
                ..TransferHistoryQuery::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.operations.len(), 1);
    }

    #[tokio::test]
    async fn transfer_history_tables_use_their_ordering_indexes() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        for (table, index) in [
            (
                "tokenized_equity_mint_view",
                "idx_tokenized_equity_mint_view_started_at",
            ),
            (
                "equity_redemption_view",
                "idx_equity_redemption_view_started_at",
            ),
            ("usdc_rebalance_view", "idx_usdc_rebalance_view_started_at"),
        ] {
            let sql = format!(
                "EXPLAIN QUERY PLAN SELECT view_id FROM {table} \
                 WHERE started_at IS NOT NULL ORDER BY started_at DESC, view_id ASC LIMIT 100"
            );
            let rows: Vec<(i64, i64, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
                .fetch_all(&pool)
                .await
                .unwrap();
            assert!(
                rows.iter().any(|(_, _, _, detail)| detail.contains(index)),
                "expected {index} in query plan: {rows:?}"
            );
        }

        for (table, index) in [
            (
                "tokenized_equity_mint_view",
                "idx_tokenized_equity_mint_view_terminal_at",
            ),
            (
                "equity_redemption_view",
                "idx_equity_redemption_view_terminal_at",
            ),
            ("usdc_rebalance_view", "idx_usdc_rebalance_view_terminal_at"),
        ] {
            let sql = format!(
                "EXPLAIN QUERY PLAN SELECT view_id FROM {table} \
                 WHERE terminal_at >= ?"
            );
            let rows: Vec<(i64, i64, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
                .bind(sortable_timestamp(Utc::now() - Duration::hours(24)))
                .fetch_all(&pool)
                .await
                .unwrap();
            assert!(
                rows.iter().any(|(_, _, _, detail)| detail.contains(index)),
                "expected {index} in query plan: {rows:?}"
            );
        }
    }

    #[tokio::test]
    async fn usdc_terminal_timestamp_respects_directional_completion() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let now = Utc::now();
        let cases = [
            ("ConversionComplete", "BaseToAlpaca", "converted_at", true),
            ("ConversionComplete", "AlpacaToBase", "converted_at", false),
            (
                "DepositConfirmed",
                "AlpacaToBase",
                "deposit_confirmed_at",
                true,
            ),
            (
                "DepositConfirmed",
                "BaseToAlpaca",
                "deposit_confirmed_at",
                false,
            ),
        ];

        for (state, direction, timestamp_field, expected_terminal) in cases {
            let id = Uuid::new_v4();
            let payload = serde_json::json!({
                "Live": {
                    (state): {
                        "direction": direction,
                        (timestamp_field): now,
                    }
                }
            });
            sqlx::query(
                "INSERT INTO usdc_rebalance_view (view_id, version, payload) \
                 VALUES (?1, 1, ?2)",
            )
            .bind(id.to_string())
            .bind(payload.to_string())
            .execute(&pool)
            .await
            .unwrap();

            let terminal_at: Option<String> = sqlx::query_scalar(
                "SELECT terminal_at FROM usdc_rebalance_view WHERE view_id = ?1",
            )
            .bind(id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(terminal_at.is_some(), expected_terminal);
        }
    }

    #[tokio::test]
    async fn load_transfers_produces_warning_for_malformed_aggregate() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let bad_mint_id = issuer_request_id("bad-mint-1");

        sqlx::query(
            "INSERT INTO tokenized_equity_mint_view (view_id, version, payload) \
             VALUES (?1, 1, ?2)",
        )
        .bind(bad_mint_id.to_string())
        .bind(
            serde_json::json!({
                "Live": {
                    "MintRequested": {
                        "requested_at": Utc::now(),
                        "malformed": true
                    }
                }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        let loaded = load_transfers(&pool).await;

        assert!(loaded.active.is_empty());
        assert!(loaded.recent.is_empty());
        assert_eq!(loaded.warnings.len(), 1, "expected one replay warning");
        match loaded.warnings.as_slice() {
            [TransferWarning::MintReplayFailed { id }] => {
                assert_eq!(id, &Id::<EquityMintTag>::new(bad_mint_id.to_string()));
            }
            other => panic!("expected MintReplayFailed, got: {other:?}"),
        }
    }
}
