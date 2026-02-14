//! Apalis job for processing onchain DEX events into hedging trades.
//!
//! [`ProcessOnchainEventJob`] replaces the custom `event_queue` polling
//! loop for live event processing. Each job carries one blockchain
//! event (ClearV3 or TakeOrderV3) and its run method performs trade
//! conversion, vault discovery, position accumulation, and offchain
//! order placement.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use apalis::prelude::Data;
use cqrs_es::AggregateError;
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;
use tracing::{debug, error, info, warn};

use st0x_execution::{EmptySymbolError, Executor, MarketOrder, Symbol};

use crate::conductor::job::{Job, Label};
use crate::config::Ctx;
use crate::dual_write::DualWriteContext;
use crate::offchain::execution::{OffchainExecution, find_execution_by_id};
use crate::offchain_order::BrokerOrderId;
use crate::onchain::accumulator::{self, CleanedUpExecution, TradeProcessingResult};
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::{TradeEvent, extract_owned_vaults, extract_vaults_from_clear};
use crate::onchain::{OnChainError, OnchainTrade, USDC_BASE};
use crate::symbol::cache::SymbolCache;
use crate::symbol::lock::get_symbol_lock;
use crate::vault_registry::{
    VaultRegistry, VaultRegistryAggregate, VaultRegistryCommand, VaultRegistryError,
};

/// Runtime dependencies for onchain event processing.
#[derive(Clone)]
pub(crate) struct ProcessOnchainEventCtx<P, E> {
    pub(crate) pool: SqlitePool,
    pub(crate) ctx: Ctx,
    pub(crate) executor: E,
    pub(crate) provider: P,
    pub(crate) dual_write_context: DualWriteContext,
    pub(crate) cache: SymbolCache,
    pub(crate) feed_id_cache: Arc<FeedIdCache>,
    pub(crate) vault_registry_cqrs: Arc<SqliteCqrs<VaultRegistryAggregate>>,
}

/// Apalis job carrying a single onchain event for processing.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) struct ProcessOnchainEventJob<P, E> {
    pub(crate) event: TradeEvent,
    pub(crate) log: Log,
    #[serde(skip)]
    _phantom: PhantomData<(P, E)>,
}

impl<P, E> ProcessOnchainEventJob<P, E> {
    pub(crate) fn new(event: TradeEvent, log: Log) -> Self {
        Self {
            event,
            log,
            _phantom: PhantomData,
        }
    }
}

impl<P, E> fmt::Debug for ProcessOnchainEventJob<P, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessOnchainEventJob")
            .field(
                "event_type",
                &match &self.event {
                    TradeEvent::ClearV3(_) => "ClearV3",
                    TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
                },
            )
            .field("tx_hash", &self.log.transaction_hash)
            .field("log_index", &self.log.log_index)
            .finish()
    }
}

impl<P, E> Clone for ProcessOnchainEventJob<P, E> {
    fn clone(&self) -> Self {
        Self {
            event: self.event.clone(),
            log: self.log.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Errors from processing a single onchain event job.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProcessOnchainEventError<ExecutorError> {
    #[error("Onchain trade processing error: {0}")]
    OnChain(#[from] OnChainError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Executor error: {0}")]
    Executor(ExecutorError),

    #[error(transparent)]
    EmptySymbol(#[from] EmptySymbolError),

    #[error("Config error: {0}")]
    Config(#[from] crate::config::CtxError),

    #[error("Vault registry command failed: {0}")]
    VaultRegistry(#[from] AggregateError<VaultRegistryError>),

    #[error("Execution with ID {0} not found")]
    ExecutionNotFound(i64),
}

impl<P, E> Job for ProcessOnchainEventJob<P, E>
where
    P: Provider + Clone + Send + Sync + Unpin + 'static,
    E: Executor + Clone + Send + Sync + Unpin + 'static,
{
    type Ctx = ProcessOnchainEventCtx<P, E>;
    type Error = ProcessOnchainEventError<E::Error>;

    fn label(&self) -> Label {
        Label::new("process-onchain-event")
    }

    async fn run(self, ctx: Data<Self::Ctx>) -> Result<(), Self::Error> {
        let label = self.label();
        debug!(%label, "Processing onchain event");
        ctx.process_event(self.event, self.log).await
    }
}

impl<P, E> ProcessOnchainEventCtx<P, E>
where
    P: Provider + Clone,
    E: Executor + Clone,
{
    async fn process_event(
        &self,
        event: TradeEvent,
        log: Log,
    ) -> Result<(), ProcessOnchainEventError<E::Error>> {
        let tx_hash = log.transaction_hash;
        let log_index = log.log_index;
        let block_number = log.block_number;

        let order_owner = self.ctx.order_owner()?;

        let onchain_trade = self.convert_to_trade(&event, &log).await?;

        let Some(trade) = onchain_trade else {
            info!(
                "Event filtered out (no matching owner): \
                 event_type={}, tx_hash={tx_hash:?}, \
                 log_index={log_index:?}",
                event_type_name(&event),
            );
            return Ok(());
        };

        self.discover_vaults(&event, &trade, order_owner).await?;

        let symbol_lock = get_symbol_lock(trade.symbol.base()).await;
        let _guard = symbol_lock.lock().await;

        info!(
            "Processing trade: symbol={}, amount={}, \
             direction={:?}, tx_hash={tx_hash:?}, \
             log_index={log_index:?}",
            trade.symbol, trade.amount, trade.direction,
        );

        let execution = self.process_trade(trade.clone(), block_number).await?;

        self.submit_execution(execution, tx_hash).await
    }

    async fn submit_execution(
        &self,
        execution: Option<OffchainExecution>,
        tx_hash: Option<alloy::primitives::B256>,
    ) -> Result<(), ProcessOnchainEventError<E::Error>> {
        let Some(exec) = execution else {
            return Ok(());
        };

        let Some(exec_id) = exec.id else {
            error!(
                "Execution returned without ID, \
                 tx_hash={tx_hash:?}"
            );
            return Ok(());
        };

        self.execute_offchain(exec_id).await
    }

    async fn convert_to_trade(
        &self,
        event: &TradeEvent,
        log: &Log,
    ) -> Result<Option<OnchainTrade>, ProcessOnchainEventError<E::Error>> {
        let order_owner = self.ctx.order_owner()?;

        let onchain_trade = match event {
            TradeEvent::ClearV3(clear_event) => {
                OnchainTrade::try_from_clear_v3(
                    &self.ctx.evm,
                    &self.cache,
                    &self.provider,
                    *clear_event.clone(),
                    log.clone(),
                    &self.feed_id_cache,
                    order_owner,
                )
                .await?
            }
            TradeEvent::TakeOrderV3(take_event) => {
                OnchainTrade::try_from_take_order_if_target_owner(
                    &self.cache,
                    &self.provider,
                    *take_event.clone(),
                    log.clone(),
                    order_owner,
                    &self.feed_id_cache,
                )
                .await?
            }
        };

        Ok(onchain_trade)
    }

    async fn discover_vaults(
        &self,
        event: &TradeEvent,
        trade: &OnchainTrade,
        order_owner: Address,
    ) -> Result<(), ProcessOnchainEventError<E::Error>> {
        let base_symbol = trade.symbol.base();
        let expected_equity_token = trade.equity_token;

        let owned_vaults = match event {
            TradeEvent::ClearV3(clear_event) => extract_vaults_from_clear(clear_event),
            TradeEvent::TakeOrderV3(take_event) => extract_owned_vaults(
                &take_event.config.order,
                take_event.config.inputIOIndex,
                take_event.config.outputIOIndex,
            ),
        };

        let our_vaults = owned_vaults
            .into_iter()
            .filter(|vault| vault.owner == order_owner);

        let aggregate_id = VaultRegistry::aggregate_id(self.ctx.evm.orderbook, order_owner);

        for owned_vault in our_vaults {
            let vault = owned_vault.vault;

            let command = if vault.token == USDC_BASE {
                VaultRegistryCommand::DiscoverUsdcVault {
                    vault_id: vault.vault_id,
                    discovered_in: trade.tx_hash,
                }
            } else if vault.token == expected_equity_token {
                VaultRegistryCommand::DiscoverEquityVault {
                    token: vault.token,
                    vault_id: vault.vault_id,
                    discovered_in: trade.tx_hash,
                    symbol: base_symbol.clone(),
                }
            } else {
                warn!(
                    vault_id = %vault.vault_id,
                    token = %vault.token,
                    usdc = %USDC_BASE,
                    expected_equity_token = %expected_equity_token,
                    "Vault token does not match USDC or \
                     expected equity token, skipping"
                );
                continue;
            };

            self.vault_registry_cqrs
                .execute(&aggregate_id, command)
                .await?;
        }

        Ok(())
    }

    async fn process_trade(
        &self,
        trade: OnchainTrade,
        block_number: Option<u64>,
    ) -> Result<Option<OffchainExecution>, ProcessOnchainEventError<E::Error>> {
        crate::dual_write::initialize_position(
            &self.dual_write_context,
            trade.symbol.base(),
            self.dual_write_context.execution_threshold(),
        )
        .await
        .unwrap_or_else(|e| {
            debug!(
                "Position initialization skipped \
                 (likely already exists): {e}, symbol={}",
                trade.symbol.base()
            );
        });

        crate::dual_write::acknowledge_onchain_fill(&self.dual_write_context, &trade)
            .await
            .unwrap_or_else(|e| {
                error!(
                    "Failed to execute \
                 Position::AcknowledgeOnChainFill: {e}, \
                 symbol={}",
                    trade.symbol
                );
            });

        let executor_type = self.executor.to_supported_executor();

        let mut sql_tx = self.pool.begin().await?;

        let TradeProcessingResult {
            execution,
            cleaned_up_executions,
        } = accumulator::process_onchain_trade(
            &mut sql_tx,
            &self.dual_write_context,
            trade.clone(),
            executor_type,
        )
        .await
        .inspect_err(|e| {
            error!(
                "Failed to process trade through accumulator: \
                 {e}, tx_hash={:?}, log_index={}",
                trade.tx_hash, trade.log_index
            );
        })?;

        sql_tx.commit().await?;

        self.record_trade_results(
            &trade,
            block_number,
            execution.as_ref(),
            cleaned_up_executions,
        )
        .await;

        Ok(execution)
    }

    async fn record_trade_results(
        &self,
        trade: &OnchainTrade,
        block_number: Option<u64>,
        execution: Option<&OffchainExecution>,
        cleaned_up_executions: Vec<CleanedUpExecution>,
    ) {
        if let Some(bn) = block_number {
            crate::dual_write::witness_trade(&self.dual_write_context, trade, bn)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "Failed to execute OnChainTrade::Witness: \
                     {e}, tx_hash={:?}",
                        trade.tx_hash
                    );
                });
        }

        if let Some(exec) = execution {
            let base_symbol = trade.symbol.base();

            crate::dual_write::place_offchain_order(&self.dual_write_context, exec, base_symbol)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "Failed to execute \
                     Position::PlaceOffChainOrder: {e}, \
                     execution_id={:?}",
                        exec.id
                    );
                });

            crate::dual_write::place_order(&self.dual_write_context, exec)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "Failed to execute OffchainOrder::Place: \
                     {e}, execution_id={:?}",
                        exec.id
                    );
                });
        }

        for cleaned_up in cleaned_up_executions {
            self.cleanup_stale_execution(cleaned_up).await;
        }
    }

    async fn cleanup_stale_execution(&self, cleaned_up: CleanedUpExecution) {
        let execution_id = cleaned_up.execution_id;

        if let Err(e) = crate::dual_write::mark_failed(
            &self.dual_write_context,
            execution_id,
            cleaned_up.error_reason.clone(),
        )
        .await
        {
            error!(
                "Failed to execute OffchainOrder::MarkFailed \
                 for stale execution {execution_id}: {e}"
            );
        }

        if let Err(e) = crate::dual_write::fail_offchain_order(
            &self.dual_write_context,
            execution_id,
            &cleaned_up.symbol,
            cleaned_up.error_reason,
        )
        .await
        {
            error!(
                "Failed to execute \
                 Position::FailOffChainOrder for stale \
                 execution {execution_id}: {e}"
            );
        }
    }

    async fn execute_offchain(
        &self,
        execution_id: i64,
    ) -> Result<(), ProcessOnchainEventError<E::Error>> {
        let execution = find_execution_by_id(&self.pool, execution_id)
            .await?
            .ok_or(ProcessOnchainEventError::ExecutionNotFound(execution_id))?;

        info!("Executing offchain order: {execution:?}");

        let market_order = MarketOrder {
            symbol: to_executor_ticker(&execution.symbol)?,
            shares: execution.shares,
            direction: execution.direction,
        };

        let placement = self
            .executor
            .place_market_order(market_order)
            .await
            .map_err(ProcessOnchainEventError::Executor)?;

        info!("Order placed with ID: {}", placement.order_id);

        self.confirm_broker_submission(execution_id, &placement.order_id)
            .await;

        Ok(())
    }

    async fn confirm_broker_submission(
        &self,
        execution_id: i64,
        order_id: &(impl ToString + std::fmt::Debug + Sync),
    ) {
        let broker_order_id = BrokerOrderId::new(order_id);

        if let Err(e) = crate::dual_write::confirm_submission(
            &self.dual_write_context,
            execution_id,
            broker_order_id.clone(),
        )
        .await
        {
            error!(
                "Failed to execute \
                 OffchainOrder::ConfirmSubmission: {e}, \
                 execution_id={execution_id}, \
                 order_id={broker_order_id:?}"
            );
        }
    }
}

fn event_type_name(event: &TradeEvent) -> &'static str {
    match event {
        TradeEvent::ClearV3(_) => "ClearV3",
        TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
    }
}

/// Maps database symbols to current executor-recognized tickers.
/// Handles corporate actions like SPLG -> SPYM rename (Oct 31, 2025).
/// Remove once proper tSPYM tokens are issued onchain.
fn to_executor_ticker(symbol: &Symbol) -> Result<Symbol, EmptySymbolError> {
    match symbol.to_string().as_str() {
        "SPLG" => Symbol::new("SPYM"),
        _ => Ok(symbol.clone()),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, LogData};

    use st0x_execution::MockExecutor;

    use super::*;
    use crate::bindings::IOrderBookV5::{ClearConfigV2, OrderV4};
    use crate::conductor::job::Job;

    #[tokio::test]
    async fn process_onchain_event_job_has_correct_label() {
        let event = TradeEvent::ClearV3(Box::new(crate::bindings::IOrderBookV5::ClearV3 {
            sender: Address::ZERO,
            alice: OrderV4::default(),
            bob: OrderV4::default(),
            clearConfig: ClearConfigV2::default(),
        }));

        let log = Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data: LogData::default(),
            },
            block_hash: None,
            block_number: Some(100),
            block_timestamp: None,
            transaction_hash: Some(B256::ZERO),
            transaction_index: None,
            log_index: Some(0),
            removed: false,
        };

        type TestProvider = alloy::providers::RootProvider;
        type TestJob = ProcessOnchainEventJob<TestProvider, MockExecutor>;

        let job: TestJob = ProcessOnchainEventJob::new(event, log);
        assert_eq!(job.label().to_string(), "process-onchain-event");
    }
}
