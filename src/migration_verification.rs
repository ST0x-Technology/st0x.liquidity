//! Verifies that migrations apply cleanly to a real (prod/staging) database
//! and that every currently persisted event still replays under the
//! CURRENT aggregate code.
//!
//! Catches cases where an event or aggregate shape change breaks legacy
//! data that no migration has repaired yet. Never mutates the database it
//! is pointed at: everything runs against a disposable `VACUUM INTO` copy
//! in a scratch temp directory. Safe to point at a live database (once the
//! writer is stopped) or a downloaded snapshot -- see
//! `src/bin/verify-migrations.rs` for the CLI entry point used both as a
//! pre-deploy gate and for manual testing while developing a migration.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::str::FromStr;

use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use thiserror::Error;

use st0x_config::DeploymentSymbolPolicy;
use st0x_event_sorcery::{EventSourced, load_all_ids, load_entity};
use st0x_execution::Symbol;

use crate::bot_gas::BotGasReceiptCost;
use crate::equity_redemption::EquityRedemption;
use crate::inventory::snapshot::InventorySnapshot;
use crate::offchain::order::OffchainOrder;
use crate::onchain_trade::OnChainTrade;
use crate::portfolio_snapshot::PortfolioSnapshot;
use crate::position::Position;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::unwrapped_equity_recovery::aggregate::UnwrappedEquityRecovery;
use crate::usdc_rebalance::UsdcRebalance;
use crate::vault_registry::VaultRegistry;
use crate::wrapped_equity_recovery::aggregate::WrappedEquityRecovery;

/// One aggregate instance that failed to replay under current code.
#[derive(Debug)]
pub struct ReplayFailure {
    pub aggregate_id: String,
    pub error: String,
}

/// Replay results for every persisted instance of one aggregate type.
#[derive(Debug)]
pub struct AggregateReplayReport {
    pub aggregate_type: &'static str,
    pub total: usize,
    pub failures: Vec<ReplayFailure>,
}

impl AggregateReplayReport {
    fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }
}

/// Full result of verifying migrations and event replay against a database
/// copy.
#[derive(Debug)]
pub struct VerificationReport {
    pub replay_reports: Vec<AggregateReplayReport>,
    symbol_compatibility: SymbolCompatibilityReport,
}

impl VerificationReport {
    pub fn has_failures(&self) -> bool {
        self.replay_reports
            .iter()
            .any(AggregateReplayReport::has_failures)
            || self.symbol_compatibility.has_failures()
    }
}

impl fmt::Display for VerificationReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "Migrations applied cleanly.")?;
        writeln!(formatter, "Aggregate replay check:")?;
        for report in &self.replay_reports {
            writeln!(
                formatter,
                "  {}: {} aggregate(s), {} failure(s)",
                report.aggregate_type,
                report.total,
                report.failures.len()
            )?;
            for failure in &report.failures {
                writeln!(
                    formatter,
                    "    - aggregate_id={}: {}",
                    failure.aggregate_id, failure.error
                )?;
            }
        }
        write!(formatter, "{}", self.symbol_compatibility)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SymbolReferenceSource {
    Position,
    UnacknowledgedOnChainTrade,
    OpenOffchainOrder,
    VaultRegistry,
    InventorySnapshot,
    OpenMint,
    OpenRedemption,
    WrappedEquityRecovery,
    UnwrappedEquityRecovery,
    PortfolioSnapshot,
}

impl SymbolReferenceSource {
    fn allows_retirement(self) -> bool {
        matches!(
            self,
            Self::VaultRegistry | Self::InventorySnapshot | Self::PortfolioSnapshot
        )
    }
}

impl fmt::Display for SymbolReferenceSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Position => "position",
            Self::UnacknowledgedOnChainTrade => "unacknowledged onchain trade",
            Self::OpenOffchainOrder => "open offchain order",
            Self::VaultRegistry => "vault registry",
            Self::InventorySnapshot => "inventory snapshot",
            Self::OpenMint => "open mint",
            Self::OpenRedemption => "open redemption",
            Self::WrappedEquityRecovery => "wrapped-equity recovery",
            Self::UnwrappedEquityRecovery => "unwrapped-equity recovery",
            Self::PortfolioSnapshot => "portfolio snapshot",
        };
        formatter.write_str(label)
    }
}

type SymbolReferences = BTreeMap<Symbol, BTreeSet<SymbolReferenceSource>>;

#[derive(Debug)]
struct SymbolCompatibilityReport {
    missing: SymbolReferences,
    blocked_retired: SymbolReferences,
    allowed_retired: SymbolReferences,
    stale_retired: BTreeSet<Symbol>,
}

impl SymbolCompatibilityReport {
    fn new(policy: &DeploymentSymbolPolicy, references: &SymbolReferences) -> Self {
        let missing = references
            .iter()
            .filter(|(symbol, _)| !policy.configured().contains(*symbol))
            .filter(|(symbol, _)| !policy.retired().contains(*symbol))
            .map(|(symbol, sources)| (symbol.clone(), sources.clone()))
            .collect();
        let blocked_retired = references
            .iter()
            .filter(|(symbol, _)| policy.retired().contains(*symbol))
            .filter(|(_, sources)| sources.iter().any(|source| !source.allows_retirement()))
            .map(|(symbol, sources)| (symbol.clone(), sources.clone()))
            .collect();
        let allowed_retired = references
            .iter()
            .filter(|(symbol, _)| policy.retired().contains(*symbol))
            .filter(|(_, sources)| sources.iter().all(|source| source.allows_retirement()))
            .map(|(symbol, sources)| (symbol.clone(), sources.clone()))
            .collect();
        let stale_retired = policy
            .retired()
            .iter()
            .filter(|symbol| !references.contains_key(*symbol))
            .cloned()
            .collect();

        Self {
            missing,
            blocked_retired,
            allowed_retired,
            stale_retired,
        }
    }

    fn has_failures(&self) -> bool {
        !self.missing.is_empty()
            || !self.blocked_retired.is_empty()
            || !self.stale_retired.is_empty()
    }
}

impl fmt::Display for SymbolCompatibilityReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "Permanent-state/config symbol check:")?;

        if self.missing.is_empty()
            && self.blocked_retired.is_empty()
            && self.allowed_retired.is_empty()
            && self.stale_retired.is_empty()
        {
            writeln!(formatter, "  Compatible.")?;
            return Ok(());
        }

        for (symbol, sources) in &self.missing {
            writeln!(
                formatter,
                "  - {symbol}: absent from [assets.equities] but referenced by {}",
                DisplaySources(sources)
            )?;
        }
        for (symbol, sources) in &self.blocked_retired {
            writeln!(
                formatter,
                "  - {symbol}: listed in retired_symbols but still required by active durable \
                 state; referenced by {}",
                DisplaySources(sources)
            )?;
        }
        for (symbol, sources) in &self.allowed_retired {
            writeln!(
                formatter,
                "  - {symbol}: intentionally retired; referenced by {}",
                DisplaySources(sources)
            )?;
        }
        for symbol in &self.stale_retired {
            writeln!(
                formatter,
                "  - {symbol}: listed in retired_symbols but has no durable reference"
            )?;
        }

        Ok(())
    }
}

struct DisplaySources<'sources>(&'sources BTreeSet<SymbolReferenceSource>);

impl fmt::Display for DisplaySources<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, source) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{source}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("failed to open source database at {path}")]
    OpenSource {
        path: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to snapshot source database into scratch copy")]
    Vacuum(#[source] sqlx::Error),
    #[error("failed to open scratch database copy")]
    OpenScratch(#[source] sqlx::Error),
    #[error("migrations failed to apply")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("failed to clear stale snapshots before replay check")]
    ClearSnapshots(#[source] sqlx::Error),
    #[error("failed to create scratch directory")]
    ScratchDir(#[from] std::io::Error),
}

/// Verifies migrations and event replay against a copy of `source_db_path`.
///
/// `source_db_path` is opened read-only and copied via `VACUUM INTO` into a
/// scratch temp file before anything runs against it -- the source is never
/// modified. Safe to point at a live database (after its writer is stopped)
/// or a downloaded snapshot.
pub async fn verify_migrations(
    source_db_path: &Path,
    symbol_policy: &DeploymentSymbolPolicy,
) -> Result<VerificationReport, VerificationError> {
    let source_options = SqliteConnectOptions::new()
        .filename(source_db_path)
        .read_only(true);
    let source_pool = SqlitePool::connect_with(source_options)
        .await
        .map_err(|source| VerificationError::OpenSource {
            path: source_db_path.display().to_string(),
            source,
        })?;

    let scratch_dir = tempfile::tempdir()?;
    let scratch_path = scratch_dir.path().join("verify-migrations-scratch.db");

    sqlx::query("VACUUM INTO ?1")
        .bind(scratch_path.display().to_string())
        .execute(&source_pool)
        .await
        .map_err(VerificationError::Vacuum)?;
    source_pool.close().await;

    let scratch_options = SqliteConnectOptions::new().filename(&scratch_path);
    let scratch_pool = SqlitePool::connect_with(scratch_options)
        .await
        .map_err(VerificationError::OpenScratch)?;

    sqlx::migrate!()
        .set_ignore_missing(true)
        .run(&scratch_pool)
        .await?;

    clear_snapshots(&scratch_pool).await?;

    let (replay_reports, symbol_references) =
        run_replay_checks_with_references(&scratch_pool).await;
    let symbol_compatibility = SymbolCompatibilityReport::new(symbol_policy, &symbol_references);

    scratch_pool.close().await;

    Ok(VerificationReport {
        replay_reports,
        symbol_compatibility,
    })
}

/// Historical snapshots reflect the aggregate shape at the time they were
/// taken. If no events have appended since, `load_entity` returns the
/// cached snapshot directly and never touches the underlying events --
/// masking exactly the "old event no longer deserializes under current
/// code" bug this check exists to catch. Clearing snapshots forces retained
/// streams to replay from their full raw event history, the same as what
/// happens in production when a `SCHEMA_VERSION` bump clears stale
/// snapshots on deploy. `InventorySnapshot` is excluded because compaction
/// may leave its snapshot as the only durable current state.
async fn clear_snapshots(pool: &SqlitePool) -> Result<(), VerificationError> {
    sqlx::query("DELETE FROM snapshots WHERE aggregate_type <> ?1")
        .bind(InventorySnapshot::AGGREGATE_TYPE)
        .execute(pool)
        .await
        .map_err(VerificationError::ClearSnapshots)?;

    Ok(())
}

#[cfg(test)]
async fn run_replay_checks(pool: &SqlitePool) -> Vec<AggregateReplayReport> {
    run_replay_checks_with_references(pool).await.0
}

async fn run_replay_checks_with_references(
    pool: &SqlitePool,
) -> (Vec<AggregateReplayReport>, SymbolReferences) {
    let mut reports = Vec::new();
    let mut references = SymbolReferences::new();

    macro_rules! check {
        ($entity:ty) => {{
            reports.push(check_replay::<$entity>(pool, &mut references).await);
        }};
    }

    check!(Position);
    check!(OnChainTrade);
    check!(OffchainOrder);
    check!(VaultRegistry);
    check!(InventorySnapshot);
    check!(PortfolioSnapshot);
    check!(UsdcRebalance);
    check!(TokenizedEquityMint);
    check!(EquityRedemption);
    check!(WrappedEquityRecovery);
    check!(UnwrappedEquityRecovery);
    check!(BotGasReceiptCost);

    (reports, references)
}

async fn check_replay<Entity>(
    pool: &SqlitePool,
    references: &mut SymbolReferences,
) -> AggregateReplayReport
where
    Entity: DurableSymbolReferences + EventSourced,
    <Entity::Id as FromStr>::Err: fmt::Debug,
{
    let ids = match load_all_ids::<Entity>(pool).await {
        Ok(ids) => ids,
        Err(error) => {
            return AggregateReplayReport {
                aggregate_type: Entity::AGGREGATE_TYPE,
                total: 0,
                failures: vec![ReplayFailure {
                    aggregate_id: "*".to_string(),
                    error: format!("failed to enumerate aggregate ids: {error}"),
                }],
            };
        }
    };

    let mut failures = Vec::new();
    for id in &ids {
        match load_entity::<Entity>(pool, id).await {
            Ok(Some(entity)) => entity.add_durable_symbol_references(references),
            Ok(None) => failures.push(ReplayFailure {
                aggregate_id: id.to_string(),
                error: "replayed to empty state".to_string(),
            }),
            Err(error) => failures.push(ReplayFailure {
                aggregate_id: id.to_string(),
                error: error.to_string(),
            }),
        }
    }

    AggregateReplayReport {
        aggregate_type: Entity::AGGREGATE_TYPE,
        total: ids.len(),
        failures,
    }
}

fn add_reference(
    references: &mut SymbolReferences,
    symbol: &Symbol,
    source: SymbolReferenceSource,
) {
    references.entry(symbol.clone()).or_default().insert(source);
}

trait DurableSymbolReferences {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences);
}

impl DurableSymbolReferences for Position {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        add_reference(references, &self.symbol, SymbolReferenceSource::Position);
    }
}

impl DurableSymbolReferences for OnChainTrade {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        if self.acknowledged_at.is_none() {
            add_reference(
                references,
                &self.symbol,
                SymbolReferenceSource::UnacknowledgedOnChainTrade,
            );
        }
    }
}

impl DurableSymbolReferences for OffchainOrder {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        match self {
            Self::Pending { .. }
            | Self::Submitted { .. }
            | Self::PartiallyFilled { .. }
            | Self::Cancelling { .. } => add_reference(
                references,
                self.symbol(),
                SymbolReferenceSource::OpenOffchainOrder,
            ),
            Self::Filled { .. } | Self::Failed { .. } | Self::Cancelled { .. } => {}
        }
    }
}

impl DurableSymbolReferences for VaultRegistry {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        for vault in self
            .equity_vaults
            .values()
            .flat_map(|vaults| vaults.values())
        {
            add_reference(
                references,
                &vault.symbol,
                SymbolReferenceSource::VaultRegistry,
            );
        }
    }
}

impl DurableSymbolReferences for InventorySnapshot {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        let symbols = self
            .onchain_equity
            .values()
            .flat_map(BTreeMap::keys)
            .chain(self.offchain_equity.keys())
            .chain(self.base_wallet_unwrapped_equity.keys())
            .chain(self.base_wallet_wrapped_equity.keys())
            .chain(self.inflight_mints.keys())
            .chain(self.inflight_redemptions.keys());
        for symbol in symbols {
            add_reference(references, symbol, SymbolReferenceSource::InventorySnapshot);
        }
    }
}

impl DurableSymbolReferences for PortfolioSnapshot {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        for symbol in self.captured_equity_symbols() {
            add_reference(references, symbol, SymbolReferenceSource::PortfolioSnapshot);
        }
    }
}

impl DurableSymbolReferences for UsdcRebalance {
    fn add_durable_symbol_references(&self, _references: &mut SymbolReferences) {}
}

impl DurableSymbolReferences for TokenizedEquityMint {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        if self.is_terminal() {
            return;
        }
        add_reference(references, self.symbol(), SymbolReferenceSource::OpenMint);
    }
}

impl DurableSymbolReferences for EquityRedemption {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        if self.is_terminal() {
            return;
        }
        add_reference(
            references,
            self.symbol(),
            SymbolReferenceSource::OpenRedemption,
        );
    }
}

impl DurableSymbolReferences for WrappedEquityRecovery {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        if self.is_terminal() {
            return;
        }
        add_reference(
            references,
            self.symbol(),
            SymbolReferenceSource::WrappedEquityRecovery,
        );
    }
}

impl DurableSymbolReferences for UnwrappedEquityRecovery {
    fn add_durable_symbol_references(&self, references: &mut SymbolReferences) {
        if self.is_terminal() {
            return;
        }
        add_reference(
            references,
            self.symbol(),
            SymbolReferenceSource::UnwrappedEquityRecovery,
        );
    }
}

impl DurableSymbolReferences for BotGasReceiptCost {
    fn add_durable_symbol_references(&self, _references: &mut SymbolReferences) {}
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use alloy::primitives::{Address, TxHash};
    use chrono::Utc;
    use serde_json::json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use st0x_config::{DeploymentSymbolPolicy, ExecutionThreshold};
    use st0x_dto::Direction;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::Chain;
    use st0x_execution::{
        ClientOrderId, FractionalShares, MarketSession, Positive, SupportedExecutor, Symbol,
    };
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::snapshot::{InventorySnapshotCommand, InventorySnapshotId};
    use crate::inventory::{PortfolioAsset, PortfolioBalanceRow, PortfolioLocation};
    use crate::onchain_trade::OnChainTradeSource;
    use crate::portfolio_snapshot::{
        PortfolioBalanceRowWithMark, PortfolioSnapshotCommand, PortfolioSnapshotId,
    };
    use crate::position::{PositionCommand, TradeId};
    use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceEvent};
    use crate::vault_registry::{VaultRegistryCommand, VaultRegistryId};

    const A_USDC_REBALANCE_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    const REPAIR_LEGACY_USDC_CONVERSION_CONFIRMED_EVENTS: &str = include_str!(
        "../migrations/20260701223808_repair_legacy_usdc_conversion_confirmed_events.sql"
    );

    async fn migrated_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        pool
    }

    async fn source_database() -> (TempDir, PathBuf, SqlitePool) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.db");
        let pool = SqlitePool::connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        (directory, path, pool)
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        event_version: &str,
        payload: serde_json::Value,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?, ?, ?, ?, ?, ?, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(event_version)
        .bind(payload.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_snapshot(pool: &SqlitePool, aggregate_type: &str, aggregate_id: &str) {
        sqlx::query(
            "INSERT INTO snapshots \
             (aggregate_type, aggregate_id, last_sequence, payload, timestamp) \
             VALUES (?, ?, 1, '{}', '2026-01-01T00:00:00Z')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_inventory_snapshot_only(pool: &SqlitePool, symbol: &str) {
        let store = StoreBuilder::<InventorySnapshot>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = InventorySnapshotId {
            orderbook: Address::repeat_byte(0x11),
            owner: Address::repeat_byte(0x22),
        };

        store
            .send(
                &id,
                InventorySnapshotCommand::OnchainEquity {
                    chain: Chain::Base,
                    balances: BTreeMap::from([(
                        Symbol::new(symbol).unwrap(),
                        FractionalShares::new(float!(1)),
                    )]),
                    fetched_at: Utc::now(),
                    block_number: Some(1),
                },
            )
            .await
            .unwrap();
    }

    fn one_share_threshold() -> ExecutionThreshold {
        ExecutionThreshold::shares(Positive::new(FractionalShares::new(float!(1))).unwrap())
    }

    async fn insert_position_initialized(pool: &SqlitePool, symbol: &str) {
        let (store, _) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let symbol = Symbol::new(symbol).unwrap();

        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: one_share_threshold(),
                    trade_id: TradeId {
                        chain: Chain::Base,
                        tx_hash: TxHash::repeat_byte(0x33),
                        log_index: 1,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(1),
                    block_timestamp: Utc::now(),
                    block_number: Some(1),
                },
            )
            .await
            .unwrap();
    }

    async fn insert_vault_registry_seed(pool: &SqlitePool, symbol: &str) {
        let (store, _) = StoreBuilder::<VaultRegistry>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = VaultRegistryId {
            orderbook: Address::repeat_byte(0x11),
            owner: Address::repeat_byte(0x22),
        };
        store
            .send(
                &id,
                VaultRegistryCommand::SeedEquityVaultFromConfig {
                    token: Address::repeat_byte(0x33),
                    vault_id: alloy::primitives::B256::repeat_byte(0x44),
                    symbol: Symbol::new(symbol).unwrap(),
                },
            )
            .await
            .unwrap();
    }

    async fn insert_portfolio_snapshot_captured(pool: &SqlitePool, symbol: &str) {
        let store = StoreBuilder::<PortfolioSnapshot>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let captured_at = Utc::now();
        store
            .send(
                &PortfolioSnapshotId(chrono::NaiveDate::from_ymd_opt(2026, 8, 20).unwrap()),
                PortfolioSnapshotCommand::Capture {
                    captured_at,
                    rows: vec![PortfolioBalanceRowWithMark {
                        row: PortfolioBalanceRow {
                            location: PortfolioLocation::MarketMaking(Chain::Base),
                            asset: PortfolioAsset::Equity(Symbol::new(symbol).unwrap()),
                            available: float!(0),
                            inflight: float!(0),
                        },
                        usd_mark: None,
                        mark_captured_at: None,
                    }],
                },
            )
            .await
            .unwrap();
    }

    fn symbol_policy(configured: &[&str], retired: &[&str]) -> DeploymentSymbolPolicy {
        DeploymentSymbolPolicy::new(
            configured
                .iter()
                .map(|symbol| Symbol::new(*symbol).unwrap()),
            retired.iter().map(|symbol| Symbol::new(*symbol).unwrap()),
        )
        .unwrap()
    }

    fn find_report<'reports>(
        reports: &'reports [AggregateReplayReport],
        aggregate_type: &str,
    ) -> &'reports AggregateReplayReport {
        reports
            .iter()
            .find(|report| report.aggregate_type == aggregate_type)
            .unwrap()
    }

    fn references_for(entity: &impl DurableSymbolReferences) -> SymbolReferences {
        let mut references = SymbolReferences::new();
        entity.add_durable_symbol_references(&mut references);
        references
    }

    fn contains_source(
        references: &SymbolReferences,
        symbol: &Symbol,
        source: SymbolReferenceSource,
    ) -> bool {
        references
            .get(symbol)
            .is_some_and(|sources| sources.contains(&source))
    }

    #[test]
    fn unfinished_state_classifiers_exclude_terminal_state() {
        let symbol = Symbol::new("QSEP").unwrap();
        let now = Utc::now();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();

        let unacknowledged_trade = OnChainTrade {
            source: OnChainTradeSource::Raindex,
            symbol: symbol.clone(),
            amount: float!(1),
            direction: Direction::Buy,
            price_usdc: float!(1),
            block_number: Some(1),
            block_timestamp: now,
            filled_at: now,
            enrichment: None,
            acknowledged_at: None,
        };
        assert!(contains_source(
            &references_for(&unacknowledged_trade),
            &symbol,
            SymbolReferenceSource::UnacknowledgedOnChainTrade,
        ));
        assert!(
            references_for(&OnChainTrade {
                acknowledged_at: Some(now),
                ..unacknowledged_trade
            })
            .is_empty()
        );

        let open_order = OffchainOrder::Pending {
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::AlpacaBrokerApi,
            placed_at: now,
            market_session: MarketSession::Regular,
            close_flatten: false,
        };
        assert!(contains_source(
            &references_for(&open_order),
            &symbol,
            SymbolReferenceSource::OpenOffchainOrder,
        ));
        assert!(
            references_for(&OffchainOrder::Failed {
                symbol: symbol.clone(),
                shares,
                requested_shares: None,
                direction: Direction::Buy,
                executor: SupportedExecutor::AlpacaBrokerApi,
                retained_fill: None,
                filled_shares: None,
                executor_order_id: None,
                error: "rejected".to_string(),
                placed_at: now,
                failed_at: now,
            })
            .is_empty()
        );

        let open_mint = TokenizedEquityMint::MintRequested {
            symbol: symbol.clone(),
            quantity: float!(1),
            wallet: Address::ZERO,
            requested_at: now,
        };
        assert!(contains_source(
            &references_for(&open_mint),
            &symbol,
            SymbolReferenceSource::OpenMint,
        ));
        assert!(
            references_for(&TokenizedEquityMint::Failed {
                symbol: symbol.clone(),
                quantity: float!(1),
                reason: "failed".to_string(),
                requested_at: now,
                failed_at: now,
            })
            .is_empty()
        );

        let open_redemption = EquityRedemption::VaultWithdrawPending {
            symbol: symbol.clone(),
            quantity: float!(1),
            token: Address::ZERO,
            wrapped_amount: alloy::primitives::U256::ZERO,
            pending_at: now,
        };
        assert!(contains_source(
            &references_for(&open_redemption),
            &symbol,
            SymbolReferenceSource::OpenRedemption,
        ));
        assert!(
            references_for(&EquityRedemption::Failed {
                symbol: symbol.clone(),
                quantity: float!(1),
                raindex_withdraw_tx: None,
                redemption_tx: None,
                tokenization_request_id: None,
                reason: None,
                started_at: now,
                failed_at: now,
            })
            .is_empty()
        );

        let wrapped_recovery = WrappedEquityRecovery::Detected {
            symbol: symbol.clone(),
            shares: FractionalShares::new(float!(1)),
            detected_at: now,
        };
        assert!(contains_source(
            &references_for(&wrapped_recovery),
            &symbol,
            SymbolReferenceSource::WrappedEquityRecovery,
        ));
        assert!(
            references_for(&WrappedEquityRecovery::Failed {
                symbol: symbol.clone(),
                shares: FractionalShares::new(float!(1)),
                reason: "failed".to_string(),
                failed_at: now,
            })
            .is_empty()
        );

        let unwrapped_recovery = UnwrappedEquityRecovery::Detected {
            symbol: symbol.clone(),
            shares: FractionalShares::new(float!(1)),
            detected_at: now,
        };
        assert!(contains_source(
            &references_for(&unwrapped_recovery),
            &symbol,
            SymbolReferenceSource::UnwrappedEquityRecovery,
        ));
        assert!(
            references_for(&UnwrappedEquityRecovery::Failed {
                symbol,
                shares: FractionalShares::new(float!(1)),
                reason: "failed".to_string(),
                failed_at: now,
            })
            .is_empty()
        );
    }

    #[tokio::test]
    async fn replays_a_well_formed_position_cleanly() {
        let pool = migrated_pool().await;
        insert_position_initialized(&pool, "AAPL").await;

        let reports = run_replay_checks(&pool).await;

        let position_report = find_report(&reports, "Position");
        assert_eq!(position_report.total, 1);
        assert!(position_report.failures.is_empty());
    }

    #[tokio::test]
    async fn reports_a_failure_for_an_unparseable_event() {
        let pool = migrated_pool().await;
        insert_event(
            &pool,
            "UsdcRebalance",
            A_USDC_REBALANCE_ID,
            1,
            "UsdcRebalanceEvent::SomeVariantThatNoLongerExists",
            "1.0",
            json!({"garbage": "payload"}),
        )
        .await;

        let reports = run_replay_checks(&pool).await;

        let usdc_report = find_report(&reports, "UsdcRebalance");
        assert_eq!(usdc_report.total, 1);
        assert_eq!(usdc_report.failures.len(), 1);
        assert_eq!(usdc_report.failures[0].aggregate_id, A_USDC_REBALANCE_ID);
    }

    /// The prod failure this migration repairs is `UsdcRebalance` *hydration*,
    /// not a SQLite field-shape mismatch: a legacy `ConversionConfirmed`
    /// carrying the pre-split `filled_amount` field no longer deserializes into
    /// the current `ConversionAmounts` (source + received) shape, so the whole
    /// aggregate fails to replay. This seeds that exact legacy stream, proves it
    /// breaks replay before the repair, then proves the repair migration makes
    /// it hydrate cleanly under current code -- exercising the replay path the
    /// deploy gate depends on, which the pure JSON-shape assertions in
    /// `tests/migrations.rs` never reach.
    #[tokio::test]
    async fn repair_migration_makes_legacy_conversion_confirmed_replay() {
        let pool = migrated_pool().await;

        // The originating event in its current shape so the stream is valid up
        // to the conversion confirmation.
        insert_event(
            &pool,
            "UsdcRebalance",
            A_USDC_REBALANCE_ID,
            1,
            "UsdcRebalanceEvent::ConversionInitiated",
            "2.0",
            serde_json::to_value(&UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(1082.711862)),
                order_id: ClientOrderId::from_uuid(Uuid::from_u128(1)),
                initiated_at: Utc::now(),
            })
            .unwrap(),
        )
        .await;

        // The confirmation in its *legacy* shape: a single `filled_amount`
        // string instead of the current source/received split. This is exactly
        // what prod persisted before the model change and cannot deserialize
        // under the current event schema.
        insert_event(
            &pool,
            "UsdcRebalance",
            A_USDC_REBALANCE_ID,
            2,
            "UsdcRebalanceEvent::ConversionConfirmed",
            "1.0",
            json!({
                "ConversionConfirmed": {
                    "direction": "BaseToAlpaca",
                    "filled_amount": "1082.711862",
                    "converted_at": "2026-07-01T19:58:41.907Z"
                }
            }),
        )
        .await;

        let before_reports = run_replay_checks(&pool).await;
        let before = find_report(&before_reports, "UsdcRebalance");
        assert_eq!(before.total, 1);
        assert_eq!(before.failures.len(), 1);
        assert_eq!(before.failures[0].aggregate_id, A_USDC_REBALANCE_ID);

        sqlx::raw_sql(REPAIR_LEGACY_USDC_CONVERSION_CONFIRMED_EVENTS)
            .execute(&pool)
            .await
            .unwrap();

        let after_reports = run_replay_checks(&pool).await;
        let after = find_report(&after_reports, "UsdcRebalance");
        assert_eq!(after.total, 1);
        assert!(after.failures.is_empty());
    }

    #[tokio::test]
    async fn empty_database_replays_cleanly_for_every_aggregate_type() {
        let pool = migrated_pool().await;

        let reports = run_replay_checks(&pool).await;

        assert_eq!(reports.len(), 12);
        for report in &reports {
            assert_eq!(report.total, 0, "{}", report.aggregate_type);
            assert!(report.failures.is_empty(), "{}", report.aggregate_type);
        }
    }

    /// Guards the exact bug class this check exists to catch: a snapshot
    /// taken under an old aggregate/event shape would otherwise let
    /// `load_entity` skip straight to the cached (and now stale-shaped)
    /// state without ever touching the underlying events, silently masking
    /// legacy data that no longer replays under current code.
    #[tokio::test]
    async fn clear_snapshots_removes_all_rows() {
        let pool = migrated_pool().await;
        insert_snapshot(&pool, "Position", "AAPL").await;
        insert_snapshot(&pool, "UsdcRebalance", A_USDC_REBALANCE_ID).await;

        clear_snapshots(&pool).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn clear_snapshots_preserves_compacted_inventory_state() {
        let pool = migrated_pool().await;
        insert_inventory_snapshot_only(&pool, "QSEP").await;

        clear_snapshots(&pool).await.unwrap();

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM snapshots WHERE aggregate_type = 'InventorySnapshot'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn verify_migrations_never_mutates_the_source_and_covers_every_aggregate_type() {
        let (_source_dir, source_path, setup_pool) = source_database().await;
        insert_position_initialized(&setup_pool, "AAPL").await;
        setup_pool.close().await;

        let bytes_before = std::fs::read(&source_path).unwrap();

        let report = verify_migrations(&source_path, &symbol_policy(&["AAPL"], &[]))
            .await
            .unwrap();

        let bytes_after = std::fs::read(&source_path).unwrap();
        assert_eq!(bytes_before, bytes_after, "source database was mutated");

        assert!(!report.has_failures());
        assert_eq!(report.replay_reports.len(), 12);
        assert_eq!(find_report(&report.replay_reports, "Position").total, 1);
    }

    #[tokio::test]
    async fn verify_migrations_fails_clearly_on_a_missing_source() {
        let error = verify_migrations(
            Path::new("/nonexistent/path/does-not-exist.db"),
            &symbol_policy(&[], &[]),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, VerificationError::OpenSource { .. }));
    }

    #[tokio::test]
    async fn qsep_registry_reference_blocks_config_removal_and_names_the_source() {
        let (_source_dir, source_path, pool) = source_database().await;
        insert_vault_registry_seed(&pool, "QSEP").await;
        pool.close().await;

        let report = verify_migrations(&source_path, &symbol_policy(&["AAPL"], &[]))
            .await
            .unwrap();

        assert!(report.has_failures());
        let rendered = report.to_string();
        assert!(rendered.contains("QSEP"), "{rendered}");
        assert!(rendered.contains("vault registry"), "{rendered}");
    }

    #[tokio::test]
    async fn explicit_qsep_retirement_allows_the_known_reference() {
        let (_source_dir, source_path, pool) = source_database().await;
        insert_vault_registry_seed(&pool, "QSEP").await;
        pool.close().await;

        let report = verify_migrations(&source_path, &symbol_policy(&["AAPL"], &["QSEP"]))
            .await
            .unwrap();

        assert!(!report.has_failures(), "{report}");
    }

    #[tokio::test]
    async fn retired_symbol_with_open_position_reference_blocks_config_removal() {
        let (_source_dir, source_path, pool) = source_database().await;
        insert_position_initialized(&pool, "QSEP").await;
        insert_vault_registry_seed(&pool, "QSEP").await;
        pool.close().await;

        let report = verify_migrations(&source_path, &symbol_policy(&["AAPL"], &["QSEP"]))
            .await
            .unwrap();

        assert!(report.has_failures());
        let rendered = report.to_string();
        assert!(rendered.contains("QSEP"), "{rendered}");
        assert!(rendered.contains("position"), "{rendered}");
        assert!(rendered.contains("vault registry"), "{rendered}");
    }

    #[tokio::test]
    async fn snapshot_only_inventory_reference_blocks_symbol_removal() {
        let (_source_dir, source_path, pool) = source_database().await;
        insert_inventory_snapshot_only(&pool, "QSEP").await;
        pool.close().await;

        let report = verify_migrations(&source_path, &symbol_policy(&["AAPL"], &[]))
            .await
            .unwrap();

        assert!(report.has_failures());
        let rendered = report.to_string();
        assert!(rendered.contains("QSEP"), "{rendered}");
        assert!(rendered.contains("inventory snapshot"), "{rendered}");
    }

    #[tokio::test]
    async fn retained_portfolio_snapshot_event_row_blocks_symbol_removal() {
        let (_source_dir, source_path, pool) = source_database().await;
        insert_portfolio_snapshot_captured(&pool, "QSEP").await;
        pool.close().await;

        let report = verify_migrations(&source_path, &symbol_policy(&["AAPL"], &[]))
            .await
            .unwrap();

        assert!(report.has_failures());
        let rendered = report.to_string();
        assert!(rendered.contains("QSEP"), "{rendered}");
        assert!(rendered.contains("portfolio snapshot"), "{rendered}");
    }

    #[tokio::test]
    async fn stale_retirement_without_durable_reference_is_rejected() {
        let (_source_dir, source_path, pool) = source_database().await;
        pool.close().await;

        let report = verify_migrations(&source_path, &symbol_policy(&[], &["QSEP"]))
            .await
            .unwrap();

        assert!(report.has_failures());
        assert!(report.to_string().contains("no durable reference"));
    }
}
