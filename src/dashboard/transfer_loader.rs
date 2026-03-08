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
    use alloy::primitives::TxHash;
    use chrono::Utc;
    use rust_decimal::Decimal;

    use st0x_dto::{
        EquityMintOperation, EquityMintStatus, EquityMintTag, TransferOperation,
        UsdcBridgeDirection, UsdcBridgeOperation, UsdcBridgeStatus, UsdcBridgeTag,
    };
    use st0x_finance::{Id, Usdc};

    use super::*;

    fn mint_transfer(status: EquityMintStatus) -> TransferOperation {
        TransferOperation::EquityMint(EquityMintOperation {
            id: Id::<EquityMintTag>::new("mint-1".to_string()),
            symbol: st0x_execution::Symbol::new("AAPL").unwrap(),
            quantity: st0x_execution::FractionalShares::new(Decimal::from(10)),
            status,
            completed_stages: vec![],
            started_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }

    fn usdc_transfer(status: UsdcBridgeStatus) -> TransferOperation {
        TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::<UsdcBridgeTag>::new("usdc-1".to_string()),
            direction: UsdcBridgeDirection::AlpacaToBase,
            amount: Usdc::new(Decimal::from(1000)),
            status,
            completed_stages: vec![],
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
                token: TxHash::random(),
                wrap: TxHash::random(),
                vault_deposit: TxHash::random(),
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
            burn: TxHash::random(),
            mint: TxHash::random(),
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
}
