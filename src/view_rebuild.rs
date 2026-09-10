//! Operator rebuild of a materialized view or read model by replaying its
//! event streams from scratch: the escape hatch for a view corrupted by a lost
//! update. One definition shared by the `stox view rebuild` CLI and the ops API
//! route, so the set of rebuildable views and their single-id support cannot
//! drift between the two.
//!
//! A rebuild is not atomic with the live projection: the row is deleted, then
//! the events are replayed. A live write that loads the view before the delete
//! and saves after it hits the projection's optimistic lock, which is the same
//! lost-update class this tool repairs; the next `catch_up` (startup or another
//! rebuild) heals it. The rebuild never leaves a wrong-and-stuck row.

use serde::Deserialize;
use sqlx::SqlitePool;
use st0x_event_sorcery::{Projection, ProjectionError};
use thiserror::Error;

use crate::offchain::order::{OffchainOrder, OffchainOrderId};
use crate::performance::equity_timing::EquityTimingProjection;
use crate::performance::rebalance::RebalanceTimingProjection;
use crate::performance::reliability::LifecycleFailureProjection;
use crate::portfolio_snapshot::PortfolioSnapshotProjection;
use crate::position::Position;
use crate::vault_registry::{VaultRegistry, VaultRegistryId};
use st0x_execution::Symbol;

/// A view or read model an operator may rebuild. Kebab-cased for both the
/// CLI value and the wire (`position`, `offchain-order`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RebuildableView {
    /// Position aggregate (position_view)
    Position,
    /// Offchain order aggregate (offchain_order_view)
    OffchainOrder,
    /// Vault registry aggregate (vault_registry_view)
    VaultRegistry,
    /// Rebalance stage-timing read model (rebalance_stage_timing). Replays every
    /// `UsdcRebalance` event stream through the reactor fold. Whole model only.
    RebalanceTiming,
    /// Equity mint/redemption stage-timing read model (equity_stage_timing).
    /// Replays every `TokenizedEquityMint`/`EquityRedemption` event stream
    /// through the reactor fold. Whole model only.
    EquityTiming,
    /// Lifecycle-failure read model (lifecycle_failure_event). Replays every
    /// failure across all four subscribed streams through the reactor fold.
    /// Whole model only.
    LifecycleFailure,
    /// Daily portfolio snapshot read model. Replays captures and every audited
    /// historical-mark correction. Whole model only.
    PortfolioSnapshot,
}

impl RebuildableView {
    /// The kebab-case name used on the CLI and the wire.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Position => "position",
            Self::OffchainOrder => "offchain-order",
            Self::VaultRegistry => "vault-registry",
            Self::RebalanceTiming => "rebalance-timing",
            Self::EquityTiming => "equity-timing",
            Self::LifecycleFailure => "lifecycle-failure",
            Self::PortfolioSnapshot => "portfolio-snapshot",
        }
    }

    /// Whether one aggregate's view can be rebuilt on its own. The per-aggregate
    /// views can; the reactor-fold read models replay the whole model.
    pub const fn supports_single_id(self) -> bool {
        match self {
            Self::Position | Self::OffchainOrder | Self::VaultRegistry => true,
            Self::RebalanceTiming
            | Self::EquityTiming
            | Self::LifecycleFailure
            | Self::PortfolioSnapshot => false,
        }
    }
}

impl std::fmt::Display for RebuildableView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Which rows to rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuildScope {
    /// One aggregate's view, by its id as the operator typed it.
    Id(String),
    /// Every row of the view or read model.
    All,
}

/// What a rebuild did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewRebuilt {
    pub view: RebuildableView,
    pub scope: RebuildScope,
    /// Events replayed, reported by the read models; the per-aggregate views
    /// do not count.
    pub replayed: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ViewRebuildError {
    /// A single-id rebuild was requested for a read model that only replays
    /// the whole model.
    #[error("{view} rebuild replays the whole read model and cannot rebuild a single id")]
    WholeModelOnly { view: RebuildableView },
    #[error("invalid {view} id {id:?}: {message}")]
    InvalidId {
        view: RebuildableView,
        id: String,
        message: String,
    },
    #[error(transparent)]
    Position(ProjectionError<Position>),
    #[error(transparent)]
    OffchainOrder(ProjectionError<OffchainOrder>),
    #[error(transparent)]
    VaultRegistry(ProjectionError<VaultRegistry>),
    #[error(transparent)]
    RebalanceTiming(crate::performance::rebalance::ProjectionError),
    #[error(transparent)]
    EquityTiming(crate::performance::equity_timing::ProjectionError),
    #[error(transparent)]
    LifecycleFailure(crate::performance::reliability::FailureProjectionError),
    #[error(transparent)]
    PortfolioSnapshot(crate::portfolio_snapshot::projection::ProjectionError),
}

impl ViewRebuildError {
    /// Whether the failure is the caller's (bad scope or id) rather than the
    /// store's.
    pub const fn is_caller_error(&self) -> bool {
        matches!(self, Self::WholeModelOnly { .. } | Self::InvalidId { .. })
    }
}

/// Rebuilds `view` for `scope` by deleting the affected rows and replaying the
/// event log. Operates directly on the store; no other service is needed.
pub async fn rebuild_view(
    pool: &SqlitePool,
    view: RebuildableView,
    scope: RebuildScope,
) -> Result<ViewRebuilt, ViewRebuildError> {
    let invalid_id = |id: &str, message: String| ViewRebuildError::InvalidId {
        view,
        id: id.to_owned(),
        message,
    };

    let replayed = match (view, &scope) {
        (RebuildableView::Position, RebuildScope::Id(id)) => {
            let symbol: Symbol = id
                .parse()
                .map_err(|error| invalid_id(id, format!("{error}")))?;
            Projection::<Position>::sqlite(pool.clone())
                .rebuild(&symbol)
                .await
                .map_err(ViewRebuildError::Position)?;
            None
        }
        (RebuildableView::Position, RebuildScope::All) => {
            Projection::<Position>::sqlite(pool.clone())
                .rebuild_all()
                .await
                .map_err(ViewRebuildError::Position)?;
            None
        }
        (RebuildableView::OffchainOrder, RebuildScope::Id(id)) => {
            let order_id: OffchainOrderId = id
                .parse()
                .map_err(|error| invalid_id(id, format!("{error}")))?;
            Projection::<OffchainOrder>::sqlite(pool.clone())
                .rebuild(&order_id)
                .await
                .map_err(ViewRebuildError::OffchainOrder)?;
            None
        }
        (RebuildableView::OffchainOrder, RebuildScope::All) => {
            Projection::<OffchainOrder>::sqlite(pool.clone())
                .rebuild_all()
                .await
                .map_err(ViewRebuildError::OffchainOrder)?;
            None
        }
        (RebuildableView::VaultRegistry, RebuildScope::Id(id)) => {
            let registry_id: VaultRegistryId = id
                .parse()
                .map_err(|error| invalid_id(id, format!("{error}")))?;
            Projection::<VaultRegistry>::sqlite(pool.clone())
                .rebuild(&registry_id)
                .await
                .map_err(ViewRebuildError::VaultRegistry)?;
            None
        }
        (RebuildableView::VaultRegistry, RebuildScope::All) => {
            Projection::<VaultRegistry>::sqlite(pool.clone())
                .rebuild_all()
                .await
                .map_err(ViewRebuildError::VaultRegistry)?;
            None
        }
        (
            RebuildableView::RebalanceTiming
            | RebuildableView::EquityTiming
            | RebuildableView::LifecycleFailure
            | RebuildableView::PortfolioSnapshot,
            RebuildScope::Id(_),
        ) => return Err(ViewRebuildError::WholeModelOnly { view }),
        (RebuildableView::RebalanceTiming, RebuildScope::All) => Some(
            RebalanceTimingProjection::new(pool.clone())
                .rebuild_all()
                .await
                .map_err(ViewRebuildError::RebalanceTiming)?,
        ),
        (RebuildableView::EquityTiming, RebuildScope::All) => Some(
            EquityTimingProjection::new(pool.clone())
                .rebuild_all()
                .await
                .map_err(ViewRebuildError::EquityTiming)?,
        ),
        (RebuildableView::LifecycleFailure, RebuildScope::All) => Some(
            LifecycleFailureProjection::new(pool.clone())
                .rebuild_all()
                .await
                .map_err(ViewRebuildError::LifecycleFailure)?,
        ),
        (RebuildableView::PortfolioSnapshot, RebuildScope::All) => Some(
            PortfolioSnapshotProjection::new(pool.clone())
                .rebuild_all()
                .await
                .map_err(ViewRebuildError::PortfolioSnapshot)?,
        ),
    };

    Ok(ViewRebuilt {
        view,
        scope,
        replayed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn read_models_refuse_a_single_id() {
        let pool = setup_test_db().await;
        for view in [
            RebuildableView::RebalanceTiming,
            RebuildableView::EquityTiming,
            RebuildableView::LifecycleFailure,
            RebuildableView::PortfolioSnapshot,
        ] {
            let error = rebuild_view(&pool, view, RebuildScope::Id("x".to_owned()))
                .await
                .unwrap_err();
            assert!(
                matches!(error, ViewRebuildError::WholeModelOnly { view: refused } if refused == view),
                "{view}: {error}"
            );
            assert!(!view.supports_single_id());
        }
    }

    #[tokio::test]
    async fn a_malformed_id_is_a_caller_error_before_any_store_access() {
        let pool = setup_test_db().await;
        let error = rebuild_view(
            &pool,
            RebuildableView::VaultRegistry,
            RebuildScope::Id("no-delimiter".to_owned()),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, ViewRebuildError::InvalidId { .. }),
            "{error}"
        );
        assert!(error.is_caller_error());
    }

    #[tokio::test]
    async fn an_empty_store_rebuilds_every_view_to_nothing() {
        let pool = setup_test_db().await;
        for view in [
            RebuildableView::Position,
            RebuildableView::OffchainOrder,
            RebuildableView::VaultRegistry,
            RebuildableView::RebalanceTiming,
            RebuildableView::EquityTiming,
            RebuildableView::LifecycleFailure,
            RebuildableView::PortfolioSnapshot,
        ] {
            let rebuilt = rebuild_view(&pool, view, RebuildScope::All)
                .await
                .unwrap_or_else(|error| panic!("{view}: {error}"));
            assert_eq!(rebuilt.view, view);
            assert_eq!(rebuilt.scope, RebuildScope::All);
            assert_eq!(
                rebuilt.replayed,
                (!view.supports_single_id()).then_some(0),
                "{view}: read models report a replay count, per-aggregate views do not"
            );
        }
    }
}
