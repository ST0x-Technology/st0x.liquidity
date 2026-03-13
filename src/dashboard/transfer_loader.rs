//! Loads transfer aggregates from the event store for dashboard display.

use chrono::{Duration, Utc};
use sqlx::SqlitePool;
use tracing::warn;

use st0x_dto::TransferOperation;
use st0x_event_sorcery::{load_all_ids, load_entity};

use crate::equity_redemption::EquityRedemption;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

/// Loaded transfers split into active (in-progress) and recent (terminal).
pub(crate) struct LoadedTransfers {
    pub(crate) active: Vec<TransferOperation>,
    pub(crate) recent: Vec<TransferOperation>,
}

/// Load all transfer aggregates, classified into active and recent.
///
/// Active: non-terminal transfers (in progress).
/// Recent: terminal transfers (completed/failed) within the last 24 hours.
pub(crate) async fn load_transfers(pool: &SqlitePool) -> LoadedTransfers {
    let cutoff = Utc::now() - Duration::hours(24);

    let mut active = Vec::new();
    let mut recent = Vec::new();

    load_mint_transfers(pool, &cutoff, &mut active, &mut recent).await;
    load_redemption_transfers(pool, &cutoff, &mut active, &mut recent).await;
    load_usdc_transfers(pool, &cutoff, &mut active, &mut recent).await;

    active.sort_by_key(|transfer| std::cmp::Reverse(transfer.updated_at()));
    recent.sort_by_key(|transfer| std::cmp::Reverse(transfer.updated_at()));

    LoadedTransfers { active, recent }
}

async fn load_mint_transfers(
    pool: &SqlitePool,
    cutoff: &chrono::DateTime<Utc>,
    active: &mut Vec<TransferOperation>,
    recent: &mut Vec<TransferOperation>,
) {
    let ids = match load_all_ids::<TokenizedEquityMint>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(?error, "Failed to load mint aggregate IDs");
            return;
        }
    };

    for id in &ids {
        let entity = match load_entity::<TokenizedEquityMint>(pool, id).await {
            Ok(Some(entity)) => entity,
            Ok(None) => continue,
            Err(error) => {
                warn!(?error, ?id, "Failed to load mint aggregate");
                continue;
            }
        };

        classify(entity.to_dto(id), cutoff, active, recent);
    }
}

async fn load_redemption_transfers(
    pool: &SqlitePool,
    cutoff: &chrono::DateTime<Utc>,
    active: &mut Vec<TransferOperation>,
    recent: &mut Vec<TransferOperation>,
) {
    let ids = match load_all_ids::<EquityRedemption>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(?error, "Failed to load redemption aggregate IDs");
            return;
        }
    };

    for id in &ids {
        let entity = match load_entity::<EquityRedemption>(pool, id).await {
            Ok(Some(entity)) => entity,
            Ok(None) => continue,
            Err(error) => {
                warn!(?error, ?id, "Failed to load redemption aggregate");
                continue;
            }
        };

        classify(entity.to_dto(id), cutoff, active, recent);
    }
}

async fn load_usdc_transfers(
    pool: &SqlitePool,
    cutoff: &chrono::DateTime<Utc>,
    active: &mut Vec<TransferOperation>,
    recent: &mut Vec<TransferOperation>,
) {
    let ids = match load_all_ids::<UsdcRebalance>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            warn!(?error, "Failed to load USDC rebalance aggregate IDs");
            return;
        }
    };

    for id in &ids {
        let entity = match load_entity::<UsdcRebalance>(pool, id).await {
            Ok(Some(entity)) => entity,
            Ok(None) => continue,
            Err(error) => {
                warn!(?error, ?id, "Failed to load USDC rebalance aggregate");
                continue;
            }
        };

        classify(entity.to_dto(id), cutoff, active, recent);
    }
}

fn classify(
    transfer: TransferOperation,
    cutoff: &chrono::DateTime<Utc>,
    active: &mut Vec<TransferOperation>,
    recent: &mut Vec<TransferOperation>,
) {
    if !transfer.is_terminal() {
        active.push(transfer);
    } else if &transfer.updated_at() >= cutoff {
        recent.push(transfer);
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash};
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use st0x_dto::{
        EquityMintOperation, EquityMintStatus, EquityMintTag, EquityRedemptionStatus,
        TransferOperation, UsdcBridgeDirection, UsdcBridgeOperation, UsdcBridgeStatus,
        UsdcBridgeTag,
    };
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_finance::{Id, Usdc};
    use st0x_float_macro::float;

    use super::*;
    use crate::equity_redemption::EquityRedemptionEvent;
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
        let mut active = Vec::new();
        let mut recent = Vec::new();

        classify(
            mint_transfer(EquityMintStatus::Minting),
            &cutoff,
            &mut active,
            &mut recent,
        );

        assert_eq!(active.len(), 1);
        assert!(recent.is_empty());
    }

    #[test]
    fn classify_recent_completed_transfer() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let mut active = Vec::new();
        let mut recent = Vec::new();

        classify(
            mint_transfer(EquityMintStatus::Completed {
                completed_at: Utc::now(),
            }),
            &cutoff,
            &mut active,
            &mut recent,
        );

        assert!(active.is_empty());
        assert_eq!(recent.len(), 1);
    }

    #[test]
    fn classify_old_completed_transfer_discarded() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let mut active = Vec::new();
        let mut recent = Vec::new();

        let mut transfer = usdc_transfer(UsdcBridgeStatus::Completed {
            completed_at: Utc::now() - chrono::Duration::hours(48),
        });

        if let TransferOperation::UsdcBridge(ref mut op) = transfer {
            op.updated_at = Utc::now() - chrono::Duration::hours(48);
        }

        classify(transfer, &cutoff, &mut active, &mut recent);

        assert!(active.is_empty());
        assert!(recent.is_empty());
    }

    #[tokio::test]
    async fn load_transfers_empty_database() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let loaded = load_transfers(&pool).await;

        assert!(loaded.active.is_empty());
        assert!(loaded.recent.is_empty());
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

    /// Seeds the database with transfer events spanning all three aggregate
    /// types and covering active, recent-terminal, and old-terminal cases.
    /// Returns the UUID used for the USDC rebalance aggregate.
    async fn seed_transfer_events(pool: &SqlitePool) -> Uuid {
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);
        let two_days_ago = now - Duration::hours(48);

        // 1. Active mint (non-terminal: only MintRequested)
        insert_event(
            pool,
            "TokenizedEquityMint",
            "active-mint-1",
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
            "failed-mint-1",
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
            "failed-mint-1",
            2,
            "TokenizedEquityMintEvent::MintRejected",
            serde_json::to_value(TokenizedEquityMintEvent::MintRejected {
                reason: "rejected".to_string(),
                rejected_at: one_hour_ago,
            })
            .unwrap(),
        )
        .await;

        // 3. Old failed redemption (terminal, >24h ago -- should NOT appear)
        insert_event(
            pool,
            "EquityRedemption",
            "old-redemption-1",
            1,
            "EquityRedemptionEvent::WithdrawnFromRaindex",
            serde_json::to_value(EquityRedemptionEvent::WithdrawnFromRaindex {
                symbol: Symbol::new("MSFT").unwrap(),
                quantity: float!(20),
                token: Address::ZERO,
                wrapped_amount: alloy::primitives::U256::from(20),
                raindex_withdraw_tx: TxHash::ZERO,
                withdrawn_at: two_days_ago,
            })
            .unwrap(),
        )
        .await;

        insert_event(
            pool,
            "EquityRedemption",
            "old-redemption-1",
            2,
            "EquityRedemptionEvent::TransferFailed",
            serde_json::to_value(EquityRedemptionEvent::TransferFailed {
                tx_hash: None,
                failed_at: two_days_ago,
            })
            .unwrap(),
        )
        .await;

        // 4. Recent failed USDC rebalance (terminal, within 24h)
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
                order_id: usdc_id,
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

        usdc_id
    }

    #[tokio::test]
    async fn load_transfers_non_empty_database() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let usdc_id = seed_transfer_events(&pool).await;
        let loaded = load_transfers(&pool).await;

        // Active should contain only the in-progress mint
        assert_eq!(
            loaded.active.len(),
            1,
            "expected 1 active transfer, got: {:?}",
            loaded.active
        );
        let active_mint = &loaded.active[0];
        assert!(
            matches!(
                active_mint,
                TransferOperation::EquityMint(EquityMintOperation {
                    status: EquityMintStatus::Minting,
                    ..
                })
            ),
            "expected active Minting transfer, got: {active_mint:?}"
        );
        if let TransferOperation::EquityMint(op) = active_mint {
            assert_eq!(op.id, Id::<EquityMintTag>::new("active-mint-1".to_string()));
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
                Id::<UsdcBridgeTag>::new(usdc_id.to_string()),
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
                Id::<EquityMintTag>::new("failed-mint-1".to_string()),
                "failed mint ID should match the aggregate_id"
            );
        }
    }
}
