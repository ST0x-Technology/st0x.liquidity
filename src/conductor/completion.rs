//! Service requirements for durable equity work, independent of admission switches.

use std::collections::BTreeSet;

use anyhow::Context;
use sqlx::SqlitePool;
use st0x_evm::Chain;
use st0x_execution::Symbol;

use super::load_transfer_jobs;
use crate::rebalancing::equity::{TransferEquityToHedging, TransferEquityToMarketMaking};
use crate::unwrapped_equity_recovery::UnwrappedEquityRecoveryJob;
use crate::wrapped_equity_recovery::WrappedEquityRecoveryJob;

pub(super) type UnfinishedListings = BTreeSet<(Chain, Symbol)>;

pub(super) async fn unfinished_listings(pool: &SqlitePool) -> anyhow::Result<UnfinishedListings> {
    let mut listings = UnfinishedListings::new();
    for id in crate::tokenized_equity_mint::interrupted_mint_ids(pool).await? {
        listings.insert(first_listing(pool, "TokenizedEquityMint", &id.to_string()).await?);
    }
    let redemptions = crate::equity_redemption::interrupted_redemption_ids(pool)
        .await?
        .into_iter()
        .chain(crate::equity_redemption::failed_sent_redemption_ids(pool).await?);
    for id in redemptions {
        listings.insert(first_listing(pool, "EquityRedemption", &id.to_string()).await?);
    }
    for kind in ["WrappedEquityRecovery", "UnwrappedEquityRecovery"] {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT aggregate_id FROM events e WHERE aggregate_type = ? \
             AND sequence = (SELECT MAX(sequence) FROM events WHERE aggregate_type = e.aggregate_type AND aggregate_id = e.aggregate_id) \
             AND event_type NOT LIKE '%::OrphanDeposited' \
             AND event_type NOT LIKE '%::RecoveryFailed' \
             AND event_type NOT LIKE '%::DispatchedToMint' \
             AND event_type NOT LIKE '%::DispatchedToRedemption'",
        ).bind(kind).fetch_all(pool).await?;
        for id in ids {
            listings.insert(first_listing(pool, kind, &id).await?);
        }
    }
    for row in load_transfer_jobs::<TransferEquityToMarketMaking>(pool).await? {
        if !row.is_terminal() {
            listings.insert((row.task.chain, row.task.symbol));
        }
    }
    for row in load_transfer_jobs::<TransferEquityToHedging>(pool).await? {
        if !row.is_terminal() {
            listings.insert((row.task.chain, row.task.symbol));
        }
    }
    for row in load_transfer_jobs::<WrappedEquityRecoveryJob>(pool).await? {
        if !row.is_terminal() {
            listings.insert((Chain::Base, row.task.symbol));
        }
    }
    for row in load_transfer_jobs::<UnwrappedEquityRecoveryJob>(pool).await? {
        if !row.is_terminal() {
            listings.insert((Chain::Base, row.task.symbol));
        }
    }
    Ok(listings)
}

async fn first_listing(pool: &SqlitePool, kind: &str, id: &str) -> anyhow::Result<(Chain, Symbol)> {
    let payload: String = sqlx::query_scalar(
        "SELECT payload FROM events WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY sequence LIMIT 1",
    ).bind(kind).bind(id).fetch_one(pool).await?;
    listing_from_payload(&payload).with_context(|| format!("unfinished {kind} {id} has no listing"))
}

fn listing_from_payload(payload: &str) -> anyhow::Result<(Chain, Symbol)> {
    let payload: serde_json::Value = serde_json::from_str(payload)?;
    let variants = payload
        .as_object()
        .context("event must be an externally tagged object")?;
    anyhow::ensure!(
        variants.len() == 1,
        "event must contain exactly one variant"
    );
    let fields = variants
        .values()
        .next()
        .context("event has no variant")?
        .as_object()
        .context("event variant must contain an object")?;
    let symbol =
        serde_json::from_value(fields.get("symbol").context("event has no symbol")?.clone())?;
    let chain = fields
        .get("chain")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or(Chain::Base);
    Ok((chain, symbol))
}

#[cfg(test)]
mod tests {
    use super::{listing_from_payload, unfinished_listings};
    use crate::wrapped_equity_recovery::{WrappedEquityRecovery, WrappedEquityRecoveryJob};
    use apalis::prelude::Status;
    use st0x_evm::Chain;
    use st0x_execution::Symbol;

    #[test]
    fn legacy_recovery_runs_on_base() {
        let (chain, symbol) = listing_from_payload(r#"{"Detected":{"symbol":"AAPL"}}"#).unwrap();
        assert_eq!(chain, Chain::Base);
        assert_eq!(symbol, Symbol::new("AAPL").unwrap());
    }

    #[test]
    fn recorded_transfer_chain_is_preserved() {
        let payload =
            serde_json::json!({"MintRequested": {"symbol": "AAPL", "chain": Chain::Ethereum}});
        assert_eq!(
            listing_from_payload(&payload.to_string()).unwrap().0,
            Chain::Ethereum
        );
    }

    #[test]
    fn all_origin_events_resolve_their_listing() {
        for variant in [
            "MintRequested",
            "MintAccepted",
            "VaultWithdrawPending",
            "VaultWithdrawSubmitting",
            "VaultWithdrawSubmitted",
            "WithdrawnFromRaindex",
            "Detected",
        ] {
            let legacy = serde_json::json!({variant: {"symbol": "AAPL"}});
            assert_eq!(
                listing_from_payload(&legacy.to_string()).unwrap().0,
                Chain::Base
            );
            let explicit =
                serde_json::json!({variant: {"symbol": "AAPL", "chain": Chain::Ethereum}});
            assert_eq!(
                listing_from_payload(&explicit.to_string()).unwrap().0,
                Chain::Ethereum
            );
        }
    }

    #[test]
    fn invalid_explicit_chain_and_ambiguous_variants_are_refused() {
        assert!(
            listing_from_payload(r#"{"MintRequested":{"symbol":"AAPL","chain":"unknown"}}"#)
                .is_err()
        );
        assert!(
            listing_from_payload(
                r#"{"MintRequested":{"symbol":"AAPL"},"Detected":{"symbol":"TSLA"}}"#
            )
            .is_err()
        );
    }

    #[test]
    fn missing_symbol_is_an_error() {
        assert!(listing_from_payload(r#"{"Detected":{}}"#).is_err());
    }
    #[tokio::test]
    async fn recovery_jobs_build_completion_services_only_while_runnable() {
        let pool = crate::test_utils::setup_test_db().await;
        for (index, status, attempts) in [
            (0, Status::Pending, 0),
            (1, Status::Queued, 0),
            (2, Status::Running, 0),
            (3, Status::Failed, 1),
            (4, Status::Failed, 25),
            (5, Status::Killed, 0),
            (6, Status::Done, 0),
        ] {
            let symbol = format!("S{index}");
            let payload =
                serde_json::json!({"symbol": symbol, "recovery_id": uuid::Uuid::new_v4()});
            sqlx::query("INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority) VALUES (?, ?, ?, ?, ?, 25, 0, 0)")
                .bind(payload.to_string().into_bytes()).bind(uuid::Uuid::new_v4().to_string())
                .bind(std::any::type_name::<WrappedEquityRecoveryJob>())
                .bind(status.to_string()).bind(attempts).execute(&pool).await.unwrap();
        }
        let listings = unfinished_listings(&pool).await.unwrap();
        assert_eq!(listings.len(), 4);
        for index in 0..4 {
            assert!(listings.contains(&(Chain::Base, Symbol::new(format!("S{index}")).unwrap())));
        }
    }
    #[tokio::test]
    async fn recovery_aggregates_require_services_until_terminal() {
        use crate::wrapped_equity_recovery::aggregate::WrappedEquityRecoveryEvent;
        let pool = crate::test_utils::setup_test_db().await;
        let id = uuid::Uuid::new_v4().to_string();
        crate::test_utils::try_persist_event::<WrappedEquityRecovery>(
            &pool,
            &id,
            1,
            &WrappedEquityRecoveryEvent::Detected {
                symbol: Symbol::new("AAPL").unwrap(),
                shares: st0x_execution::FractionalShares::ZERO,
                detected_at: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
        assert_eq!(unfinished_listings(&pool).await.unwrap().len(), 1);
        crate::test_utils::try_persist_event::<WrappedEquityRecovery>(
            &pool,
            &id,
            2,
            &WrappedEquityRecoveryEvent::RecoveryFailed {
                reason: "terminal".into(),
                failed_at: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
        assert!(unfinished_listings(&pool).await.unwrap().is_empty());
    }
}
