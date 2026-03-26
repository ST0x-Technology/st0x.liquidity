//! Maps onchain events to `TrackedOrderCommand`s and dispatches them
//! through the CQRS store.
//!
//! Each event type is processed differently:
//! - `AddOrderV3`: filter, classify scenario, dispatch `Discover`
//! - `RemoveOrderV3`: dispatch `MarkRemoved`
//! - `TakeOrderV3`: compute order hash, dispatch `RecordFill`

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::rpc::types::Log;
use chrono::Utc;
use std::sync::Arc;
use tracing::{debug, info, trace};

use st0x_event_sorcery::{SendError, Store};
use st0x_shared::bindings::IOrderBookV6;

use crate::classification::classify_order_metadata;
use crate::tracked_order::{
    OrderFilter, OrderHash, OrderType, Scenario, SupportedDirection, TrackedOrder,
    TrackedOrderCommand,
};

/// Processes decoded onchain events into TrackedOrder commands.
///
/// Holds references to the CQRS store and order filter.
/// Stateless beyond those references — all state lives in
/// the event store.
pub(crate) struct EventProcessor {
    store: Arc<Store<TrackedOrder>>,
    filter: OrderFilter,
    /// The bot's own wallet address, used to identify our fills
    /// in TakeOrderV3 events.
    bot_address: Address,
}

/// Errors from event processing.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EventProcessorError {
    #[error("failed to send command to TrackedOrder aggregate: {0}")]
    Send(#[from] SendError<TrackedOrder>),
}

/// Result of attempting late classification when MetaV1_2 arrives.
pub(crate) enum ClassificationOutcome {
    /// Order existed with Unknown type, now classified.
    Classified,

    /// Order existed but was already classified — no-op.
    AlreadyClassified,

    /// Aggregate doesn't exist yet (AddOrderV3 hasn't arrived).
    OrderNotFound,
}

impl EventProcessor {
    pub(crate) fn new(
        store: Arc<Store<TrackedOrder>>,
        filter: OrderFilter,
        bot_address: Address,
    ) -> Self {
        Self {
            store,
            filter,
            bot_address,
        }
    }

    /// Processes an `AddOrderV3` event.
    ///
    /// Filters out excluded owners and unsupported token pairs,
    /// classifies the order from its metadata, then dispatches
    /// a `Discover` command.
    ///
    /// `meta` is the raw bytes from the corresponding `MetaV1_2`
    /// event (same transaction). If `None`, the order is discovered
    /// with `OrderType::Unknown`.
    pub(crate) async fn process_add_order(
        &self,
        event: &IOrderBookV6::AddOrderV3,
        log: &Log,
        meta: Option<&Bytes>,
    ) -> Result<(), EventProcessorError> {
        let owner = event.order.owner;

        if self.filter.is_excluded_owner(owner) {
            trace!(
                ?owner,
                order_hash = %event.orderHash,
                "Skipping order from excluded owner"
            );
            return Ok(());
        }

        let valid_input_tokens: Vec<Address> =
            event.order.validInputs.iter().map(|io| io.token).collect();

        let valid_output_tokens: Vec<Address> =
            event.order.validOutputs.iter().map(|io| io.token).collect();

        let Some(SupportedDirection {
            symbol,
            scenario,
            output_token,
            input_token,
        }) = self
            .filter
            .match_supported_pair(&valid_input_tokens, &valid_output_tokens)
        else {
            trace!(
                order_hash = %event.orderHash,
                "Skipping order with unsupported token pair"
            );
            return Ok(());
        };

        let order_hash = OrderHash::new(event.orderHash);
        let block = log.block_number.unwrap_or(0);

        let order_type = meta.map(classify_order_metadata).unwrap_or_default();

        info!(
            %order_hash,
            %symbol,
            ?scenario,
            ?order_type,
            block,
            "Discovered new order"
        );

        self.store
            .send(
                &order_hash,
                TrackedOrderCommand::Discover {
                    owner,
                    symbol,
                    scenario,
                    output_token,
                    input_token,
                    order_type,
                    max_output: max_output_from_order(&event.order, scenario, output_token),
                    block,
                    discovered_at: Utc::now(),
                },
            )
            .await?;

        Ok(())
    }

    /// Processes a `RemoveOrderV3` event.
    ///
    /// Dispatches `MarkRemoved` for the order hash. If the order
    /// was never tracked (filtered out on AddOrderV3), the command
    /// is a no-op in the aggregate.
    pub(crate) async fn process_remove_order(
        &self,
        event: &IOrderBookV6::RemoveOrderV3,
    ) -> Result<(), EventProcessorError> {
        let order_hash = OrderHash::new(event.orderHash);

        debug!(
            %order_hash,
            "Processing order removal"
        );

        self.store
            .send(
                &order_hash,
                TrackedOrderCommand::MarkRemoved {
                    removed_at: Utc::now(),
                },
            )
            .await?;

        Ok(())
    }

    /// Processes a `TakeOrderV3` event.
    ///
    /// Computes the order hash from the embedded OrderV4, then
    /// dispatches `RecordFill`. The `is_ours` flag is set if the
    /// event sender matches the bot's own address.
    pub(crate) async fn process_take_order(
        &self,
        event: &IOrderBookV6::TakeOrderV3,
    ) -> Result<(), EventProcessorError> {
        let order_hash = OrderHash::from_order(&event.config.order);
        let is_ours = event.sender == self.bot_address;

        // TODO: The `output` field in TakeOrderV3 is a Rain Float
        // (bytes32), not a plain integer. `from_be_bytes` gives raw
        // bits which are only valid while `max_output` is the
        // `U256::MAX` placeholder. Once the Order Classification step
        // provides a real max_output, both this conversion and the
        // aggregate's fill arithmetic must use `rain_math_float::Float`
        // to decode the mantissa+exponent correctly.
        let amount_out = U256::from_be_bytes(event.output.0);

        debug!(
            %order_hash,
            is_ours,
            amount_out = %amount_out,
            "Processing order fill"
        );

        self.store
            .send(
                &order_hash,
                TrackedOrderCommand::RecordFill {
                    amount_out,
                    is_ours,
                    filled_at: Utc::now(),
                },
            )
            .await?;

        Ok(())
    }

    /// Classifies an already-discovered order when MetaV1_2 arrives
    /// after AddOrderV3 in the live event loop.
    ///
    /// Pre-checks aggregate existence to distinguish "order not found"
    /// (MetaV1_2 arrived before AddOrderV3) from "already classified"
    /// (order exists but is already non-Unknown). The caller uses this
    /// to decide whether to cache metadata for later use.
    pub(crate) async fn classify_order(
        &self,
        order_hash_bytes: B256,
        meta: &Bytes,
    ) -> Result<ClassificationOutcome, EventProcessorError> {
        let order_hash = OrderHash::new(order_hash_bytes);

        // Check if the aggregate exists before attempting classification.
        // This distinguishes "MetaV1_2 arrived first" from "order already classified".
        let order = self.store.load(&order_hash).await?;

        let Some(order) = order else {
            debug!(
                %order_hash,
                "Late MetaV1_2: order not yet discovered, deferring"
            );
            return Ok(ClassificationOutcome::OrderNotFound);
        };

        if !matches!(order.order_type(), OrderType::Unknown) {
            debug!(
                %order_hash,
                "Late MetaV1_2: order already classified"
            );
            return Ok(ClassificationOutcome::AlreadyClassified);
        }

        let order_type = classify_order_metadata(meta);

        debug!(
            %order_hash,
            ?order_type,
            "Late MetaV1_2: classifying existing order"
        );

        self.store
            .send(&order_hash, TrackedOrderCommand::Classify { order_type })
            .await?;

        Ok(ClassificationOutcome::Classified)
    }
}

/// Extracts the max output for the tracked order.
///
/// For now, we set max_output to U256::MAX as a placeholder.
/// The actual max_output is determined by the Rainlang expression
/// at evaluation time, not from the AddOrderV3 event data alone.
/// The order classification step (future PR) will populate this
/// by evaluating the bytecode.
fn max_output_from_order(
    _order: &IOrderBookV6::OrderV4,
    _scenario: Scenario,
    _output_token: Address,
) -> U256 {
    U256::MAX
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, U256};
    use alloy::rpc::types::Log as RpcLog;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::Symbol;
    use st0x_shared::bindings::IOrderBookV6;

    use super::*;
    use crate::tracked_order::TrackedOrder;

    const USDC: Address = Address::repeat_byte(0x01);
    const WT_AAPL: Address = Address::repeat_byte(0xAA);
    const EXCLUDED: Address = Address::repeat_byte(0xFF);
    const BOT: Address = Address::repeat_byte(0xB0);
    const OTHER_TAKER: Address = Address::repeat_byte(0x77);

    fn symbol(ticker: &str) -> Symbol {
        Symbol::new(ticker).unwrap()
    }

    fn test_filter() -> OrderFilter {
        OrderFilter::new(EXCLUDED, USDC, vec![(symbol("AAPL"), WT_AAPL)])
    }

    fn test_order(owner: Address) -> IOrderBookV6::OrderV4 {
        IOrderBookV6::OrderV4 {
            owner,
            evaluable: IOrderBookV6::EvaluableV4 {
                interpreter: Address::repeat_byte(0x02),
                store: Address::repeat_byte(0x03),
                bytecode: Bytes::from_static(&[0x01]),
            },
            validInputs: vec![IOrderBookV6::IOV2 {
                token: USDC,
                vaultId: B256::ZERO,
            }],
            validOutputs: vec![IOrderBookV6::IOV2 {
                token: WT_AAPL,
                vaultId: B256::ZERO,
            }],
            nonce: B256::repeat_byte(0x01),
        }
    }

    fn mock_log(block_number: u64) -> Log {
        RpcLog {
            block_number: Some(block_number),
            ..RpcLog::default()
        }
    }

    async fn test_processor() -> (EventProcessor, Arc<Store<TrackedOrder>>) {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let (store, _projection) = StoreBuilder::<TrackedOrder>::new(pool)
            .build(())
            .await
            .unwrap();

        let processor = EventProcessor::new(store.clone(), test_filter(), BOT);
        (processor, store)
    }

    // ── AddOrderV3 ─────────────────────────────────────────────

    #[tokio::test]
    async fn add_order_creates_tracked_order() {
        let (processor, store) = test_processor().await;
        let owner_addr = Address::repeat_byte(0x05);
        let order = test_order(owner_addr);
        let order_hash_bytes = OrderHash::from_order(&order);

        let event = IOrderBookV6::AddOrderV3 {
            sender: owner_addr,
            orderHash: order_hash_bytes.into_inner(),
            order,
        };

        processor
            .process_add_order(&event, &mock_log(100), None)
            .await
            .unwrap();

        let tracked = store.load(&order_hash_bytes).await.unwrap();
        let Some(TrackedOrder::Active {
            owner,
            symbol: tracked_symbol,
            scenario,
            output_token,
            input_token,
            max_output,
            remaining_output,
            discovered_block,
            ..
        }) = tracked
        else {
            panic!("Expected Active tracked order, got: {tracked:?}");
        };

        assert_eq!(owner, owner_addr);
        assert_eq!(tracked_symbol.to_string(), "AAPL");
        assert_eq!(scenario, Scenario::A);
        assert_eq!(output_token, WT_AAPL);
        assert_eq!(input_token, USDC);
        assert_eq!(max_output, U256::MAX);
        assert_eq!(remaining_output, U256::MAX);
        assert_eq!(discovered_block, 100);
    }

    #[tokio::test]
    async fn add_order_from_excluded_owner_is_skipped() {
        let (processor, store) = test_processor().await;
        let order = test_order(EXCLUDED);
        let order_hash_bytes = OrderHash::from_order(&order);

        let event = IOrderBookV6::AddOrderV3 {
            sender: EXCLUDED,
            orderHash: order_hash_bytes.into_inner(),
            order,
        };

        processor
            .process_add_order(&event, &mock_log(100), None)
            .await
            .unwrap();

        let tracked = store.load(&order_hash_bytes).await.unwrap();
        assert!(
            tracked.is_none(),
            "Excluded owner's order should not be tracked"
        );
    }

    #[tokio::test]
    async fn add_order_with_unsupported_tokens_is_skipped() {
        let (processor, store) = test_processor().await;
        let unknown_token = Address::repeat_byte(0x99);

        let mut order = test_order(Address::repeat_byte(0x05));
        // Replace output token with unknown
        order.validOutputs = vec![IOrderBookV6::IOV2 {
            token: unknown_token,
            vaultId: B256::ZERO,
        }];
        let order_hash_bytes = OrderHash::from_order(&order);

        let event = IOrderBookV6::AddOrderV3 {
            sender: Address::repeat_byte(0x05),
            orderHash: order_hash_bytes.into_inner(),
            order,
        };

        processor
            .process_add_order(&event, &mock_log(100), None)
            .await
            .unwrap();

        let tracked = store.load(&order_hash_bytes).await.unwrap();
        assert!(
            tracked.is_none(),
            "Unsupported token pair should not be tracked"
        );
    }

    // ── RemoveOrderV3 ──────────────────────────────────────────

    #[tokio::test]
    async fn remove_order_transitions_to_removed() {
        let (processor, store) = test_processor().await;
        let owner_addr = Address::repeat_byte(0x05);
        let order = test_order(owner_addr);
        let order_hash_bytes = OrderHash::from_order(&order);

        // First, add the order
        let add_event = IOrderBookV6::AddOrderV3 {
            sender: owner_addr,
            orderHash: order_hash_bytes.into_inner(),
            order: order.clone(),
        };
        processor
            .process_add_order(&add_event, &mock_log(100), None)
            .await
            .unwrap();

        // Then remove it
        let remove_event = IOrderBookV6::RemoveOrderV3 {
            sender: owner_addr,
            orderHash: order_hash_bytes.into_inner(),
            order,
        };
        processor.process_remove_order(&remove_event).await.unwrap();

        let tracked = store.load(&order_hash_bytes).await.unwrap();
        let Some(TrackedOrder::Removed {
            owner,
            symbol: tracked_symbol,
            ..
        }) = tracked
        else {
            panic!("Expected Removed tracked order, got: {tracked:?}");
        };

        assert_eq!(owner, owner_addr);
        assert_eq!(tracked_symbol.to_string(), "AAPL");
    }

    #[tokio::test]
    async fn remove_untracked_order_is_noop() {
        let (processor, _store) = test_processor().await;
        let order = test_order(Address::repeat_byte(0x05));
        let order_hash_bytes = OrderHash::from_order(&order);

        // Remove without prior add -- should not error
        let remove_event = IOrderBookV6::RemoveOrderV3 {
            sender: Address::repeat_byte(0x05),
            orderHash: order_hash_bytes.into_inner(),
            order,
        };

        processor.process_remove_order(&remove_event).await.unwrap();
    }

    // ── TakeOrderV3 ────────────────────────────────────────────

    #[tokio::test]
    async fn take_order_by_others_records_fill() {
        let (processor, store) = test_processor().await;
        let owner_addr = Address::repeat_byte(0x05);
        let order = test_order(owner_addr);
        let order_hash_bytes = OrderHash::from_order(&order);

        // Add the order first
        let add_event = IOrderBookV6::AddOrderV3 {
            sender: owner_addr,
            orderHash: order_hash_bytes.into_inner(),
            order: order.clone(),
        };
        processor
            .process_add_order(&add_event, &mock_log(100), None)
            .await
            .unwrap();

        let fill_amount = U256::from(100u64);

        // Another taker fills part of it
        let take_event = IOrderBookV6::TakeOrderV3 {
            sender: OTHER_TAKER,
            config: IOrderBookV6::TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::ZERO,
                outputIOIndex: U256::ZERO,
                signedContext: vec![],
            },
            input: B256::ZERO,
            output: B256::from(fill_amount),
        };
        processor.process_take_order(&take_event).await.unwrap();

        let tracked = store.load(&order_hash_bytes).await.unwrap();
        let Some(TrackedOrder::Active {
            remaining_output,
            owner,
            ..
        }) = tracked
        else {
            panic!("Expected Active after partial fill, got: {tracked:?}");
        };

        assert_eq!(owner, owner_addr);
        assert_eq!(remaining_output, U256::MAX - fill_amount);
    }

    #[tokio::test]
    async fn take_order_by_bot_sets_is_ours() {
        let (processor, store) = test_processor().await;
        let owner_addr = Address::repeat_byte(0x05);
        let order = test_order(owner_addr);
        let order_hash_bytes = OrderHash::from_order(&order);

        // Add the order
        let add_event = IOrderBookV6::AddOrderV3 {
            sender: owner_addr,
            orderHash: order_hash_bytes.into_inner(),
            order: order.clone(),
        };
        processor
            .process_add_order(&add_event, &mock_log(100), None)
            .await
            .unwrap();

        let fill_amount = U256::from(50u64);

        // Bot fills the order
        let take_event = IOrderBookV6::TakeOrderV3 {
            sender: BOT,
            config: IOrderBookV6::TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::ZERO,
                outputIOIndex: U256::ZERO,
                signedContext: vec![],
            },
            input: B256::ZERO,
            output: B256::from(fill_amount),
        };
        processor.process_take_order(&take_event).await.unwrap();

        let tracked = store.load(&order_hash_bytes).await.unwrap();
        let Some(TrackedOrder::Active {
            remaining_output,
            owner,
            ..
        }) = tracked
        else {
            panic!("Expected Active after bot's partial fill, got: {tracked:?}");
        };

        assert_eq!(owner, owner_addr);
        assert_eq!(remaining_output, U256::MAX - fill_amount);
    }

    #[tokio::test]
    async fn take_order_for_untracked_order_is_noop() {
        let (processor, store) = test_processor().await;
        let order = test_order(Address::repeat_byte(0x05));
        let order_hash_bytes = OrderHash::from_order(&order);

        // Fill without prior add -- should not error
        let take_event = IOrderBookV6::TakeOrderV3 {
            sender: OTHER_TAKER,
            config: IOrderBookV6::TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::ZERO,
                outputIOIndex: U256::ZERO,
                signedContext: vec![],
            },
            input: B256::ZERO,
            output: B256::from(U256::from(100u64)),
        };

        processor.process_take_order(&take_event).await.unwrap();

        let tracked = store.load(&order_hash_bytes).await.unwrap();
        assert!(tracked.is_none(), "Untracked order should remain absent");
    }
}
