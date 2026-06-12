//! Loads transfer aggregates from the event store for dashboard display.

use chrono::{Duration, Utc};
use sqlx::SqlitePool;
use thiserror::Error;
use tracing::warn;

use std::fmt::{self, Debug, Display};
use std::str::FromStr;

use st0x_dto::{TransferOperation, TransferWarning};
use st0x_event_sorcery::{EventSourced, load_all_ids, load_entity};
use st0x_finance::Id;

use crate::equity_redemption::EquityRedemption;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

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

/// Result of [`load_all_transfer_operations`] including any replay warnings.
pub(crate) struct AllTransferOperations {
    pub(crate) operations: Vec<TransferOperation>,
    pub(crate) warnings: Vec<TransferWarning>,
}

/// Loaded transfers split into active (in-progress) and recent (terminal).
pub(crate) struct LoadedTransfers {
    pub(crate) active: Vec<TransferOperation>,
    pub(crate) recent: Vec<TransferOperation>,
    pub(crate) warnings: Vec<TransferWarning>,
}

/// Load all transfer aggregates, classified into active and recent.
///
/// Active: non-terminal transfers (in progress).
/// Recent: terminal transfers (completed/failed) within the last 24 hours.
pub(crate) async fn load_transfers(pool: &SqlitePool) -> LoadedTransfers {
    let cutoff = Utc::now() - Duration::hours(24);

    let categories = [
        load_category::<TokenizedEquityMint, _, _>(
            pool,
            &cutoff,
            TransferWarning::MintCategoryUnavailable,
            |id| TransferWarning::MintReplayFailed {
                id: Id::new(id.to_string()),
            },
            TokenizedEquityMint::to_dto,
        )
        .await,
        load_category::<EquityRedemption, _, _>(
            pool,
            &cutoff,
            TransferWarning::RedemptionCategoryUnavailable,
            |id| TransferWarning::RedemptionReplayFailed {
                id: Id::new(id.to_string()),
            },
            EquityRedemption::to_dto,
        )
        .await,
        load_category::<UsdcRebalance, _, _>(
            pool,
            &cutoff,
            TransferWarning::BridgeCategoryUnavailable,
            |id| TransferWarning::BridgeReplayFailed {
                id: Id::new(id.to_string()),
            },
            UsdcRebalance::to_dto,
        )
        .await,
    ];

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

/// Load all transfer DTOs for the REST endpoint's paginated listing.
///
/// Unlike [`load_transfers`] (which partitions into active/recent with a
/// 24h cutoff for the WebSocket), this returns every transfer without
/// filtering.  The caller handles time-range filtering and pagination.
pub(crate) async fn load_all_transfer_operations(
    pool: &SqlitePool,
    kind_filter: Option<&[TransferKind]>,
) -> AllTransferOperations {
    let include = |kind: TransferKind| kind_filter.is_none_or(|allowed| allowed.contains(&kind));

    let mut operations = Vec::new();
    let mut warnings = Vec::new();

    if include(TransferKind::EquityMint) {
        let (ops, warns) = replay_all::<TokenizedEquityMint>(
            pool,
            |id| TransferWarning::MintReplayFailed {
                id: Id::new(id.to_string()),
            },
            TokenizedEquityMint::to_dto,
        )
        .await;

        operations.extend(ops);
        warnings.extend(warns);
    }

    if include(TransferKind::EquityRedemption) {
        let (ops, warns) = replay_all::<EquityRedemption>(
            pool,
            |id| TransferWarning::RedemptionReplayFailed {
                id: Id::new(id.to_string()),
            },
            EquityRedemption::to_dto,
        )
        .await;

        operations.extend(ops);
        warnings.extend(warns);
    }

    if include(TransferKind::UsdcBridge) {
        let (ops, warns) = replay_all::<UsdcRebalance>(
            pool,
            |id| TransferWarning::BridgeReplayFailed {
                id: Id::new(id.to_string()),
            },
            UsdcRebalance::to_dto,
        )
        .await;

        operations.extend(ops);
        warnings.extend(warns);
    }

    AllTransferOperations {
        operations,
        warnings,
    }
}

/// Replay every aggregate of a given type and convert to DTOs.
///
/// Returns both the successfully replayed operations and any warnings for
/// aggregates that failed to load, so the caller can surface them.
async fn replay_all<Entity>(
    pool: &SqlitePool,
    make_replay_warning: impl Fn(&Entity::Id) -> TransferWarning + Send + Sync,
    convert: impl Fn(&Entity, &Entity::Id) -> TransferOperation + Send + Sync,
) -> (Vec<TransferOperation>, Vec<TransferWarning>)
where
    Entity: EventSourced,
    Entity::Id: Debug,
    <Entity::Id as FromStr>::Err: Debug,
{
    let ids = match load_all_ids::<Entity>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(?error, "Failed to load aggregate IDs for REST listing");
            return (Vec::new(), Vec::new());
        }
    };

    let mut operations = Vec::with_capacity(ids.len());
    let mut warnings = Vec::new();

    for id in &ids {
        match replay_aggregate::<Entity, _>(pool, id, &make_replay_warning).await {
            Ok(entity) => operations.push(convert(&entity, id)),
            Err(warning) => {
                warn!(?warning, "Skipping transfer in REST listing");
                warnings.push(warning);
            }
        }
    }

    (operations, warnings)
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

/// Replay an aggregate from the event store, returning the entity on success
/// or a dashboard warning on failure.
async fn replay_aggregate<Entity, MakeWarning>(
    pool: &SqlitePool,
    id: &Entity::Id,
    make_warning: &MakeWarning,
) -> Result<Entity, TransferWarning>
where
    Entity: EventSourced,
    Entity::Id: Debug,
    <Entity::Id as FromStr>::Err: Debug,
    MakeWarning: Fn(&Entity::Id) -> TransferWarning + Send + Sync,
{
    match load_entity::<Entity>(pool, id).await {
        Ok(Some(entity)) => Ok(entity),

        Ok(None) => {
            warn!(?id, "Aggregate has events but replayed to empty state");
            Err(make_warning(id))
        }

        Err(error) => {
            warn!(?error, ?id, "Failed to load aggregate");
            Err(make_warning(id))
        }
    }
}

async fn load_category<Entity, MakeWarning, Convert>(
    pool: &SqlitePool,
    cutoff: &chrono::DateTime<Utc>,
    category_unavailable: TransferWarning,
    make_replay_warning: MakeWarning,
    convert: Convert,
) -> CategoryResult
where
    Entity: EventSourced,
    Entity::Id: Debug,
    <Entity::Id as FromStr>::Err: Debug,
    MakeWarning: Fn(&Entity::Id) -> TransferWarning + Send + Sync,
    Convert: Fn(&Entity, &Entity::Id) -> TransferOperation + Send + Sync,
{
    let ids = match load_all_ids::<Entity>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(?error, "Failed to load aggregate IDs");
            return CategoryResult {
                warnings: vec![category_unavailable],
                ..CategoryResult::empty()
            };
        }
    };

    let mut replayed = Vec::with_capacity(ids.len());

    for id in &ids {
        replayed.push((
            id,
            replay_aggregate::<Entity, _>(pool, id, &make_replay_warning).await,
        ));
    }

    let warnings: Vec<TransferWarning> = replayed
        .iter()
        .filter_map(|(_, result)| result.as_ref().err().cloned())
        .collect();

    let transfers: Vec<TransferOperation> = replayed
        .iter()
        .filter_map(|(id, result)| result.as_ref().ok().map(|entity| convert(entity, id)))
        .collect();

    let (active, recent): (Vec<_>, Vec<_>) = transfers
        .into_iter()
        .filter(|transfer| !transfer.is_terminal() || transfer.updated_at() >= *cutoff)
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
    use st0x_execution::{ClientOrderId, FractionalShares, Symbol};
    use st0x_finance::{Id, Usdc};
    use st0x_float_macro::float;

    use super::*;
    use crate::equity_redemption::{
        EquityRedemptionEvent, RedemptionAggregateId, redemption_aggregate_id,
    };
    use crate::tokenized_equity_mint::{
        IssuerRequestId, TokenizedEquityMintEvent, issuer_request_id,
    };
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

    #[tokio::test]
    async fn load_transfers_non_empty_database() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let seeded = seed_transfer_events(&pool).await;
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
    async fn load_all_transfer_operations_returns_warnings_for_malformed_aggregate() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let bad_mint_id = issuer_request_id("bad-mint-1");

        insert_event(
            &pool,
            "TokenizedEquityMint",
            &bad_mint_id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintRequested",
            serde_json::json!({"malformed": true}),
        )
        .await;

        let result = load_all_transfer_operations(&pool, None).await;

        assert!(result.operations.is_empty());
        assert_eq!(result.warnings.len(), 1, "expected one warning");
        match result.warnings.as_slice() {
            [TransferWarning::MintReplayFailed { id }] => {
                assert_eq!(id, &Id::<EquityMintTag>::new(bad_mint_id.to_string()));
            }
            other => panic!("expected MintReplayFailed, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn load_all_transfer_operations_filters_by_kind() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        seed_transfer_events(&pool).await;

        let mint_only =
            load_all_transfer_operations(&pool, Some(&[TransferKind::EquityMint])).await;

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
    }

    #[tokio::test]
    async fn load_transfers_produces_warning_for_malformed_aggregate() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let bad_mint_id = issuer_request_id("bad-mint-1");

        // Insert an event with a payload that can't be deserialized as a
        // TokenizedEquityMintEvent — this triggers a MintReplayFailed warning.
        insert_event(
            &pool,
            "TokenizedEquityMint",
            &bad_mint_id.to_string(),
            1,
            "TokenizedEquityMintEvent::MintRequested",
            serde_json::json!({"malformed": true}),
        )
        .await;

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
