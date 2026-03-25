//! TrackedOrder CQRS aggregate for tracking discovered Raindex orders.
//!
//! Lifecycle: Active -> Exhausted (when remaining output reaches zero)
//!            Active -> Removed   (when owner removes the order)
//!
//! The aggregate ID is the order hash (keccak256 of ABI-encoded OrderV4).
//! Partial fills from `TakeOrderV3` events reduce `remaining_output`
//! while staying in `Active` state.

use alloy::primitives::{Address, B256, U256, keccak256};
use alloy::sol_types::SolValue;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use st0x_event_sorcery::{DomainEvent, EventSourced, Table};
use st0x_execution::Symbol;
use st0x_shared::bindings::IOrderBookV6;

mod filter;

pub(crate) use filter::{OrderFilter, SupportedDirection};

/// Strongly-typed aggregate ID: keccak256 of ABI-encoded OrderV4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct OrderHash(B256);

impl OrderHash {
    pub(crate) fn new(hash: B256) -> Self {
        Self(hash)
    }

    /// Returns the inner B256 value.
    #[cfg(test)]
    pub(crate) fn into_inner(self) -> B256 {
        self.0
    }

    /// Computes the order hash from an OrderV4 struct.
    ///
    /// The order hash is the keccak256 of the ABI-encoded OrderV4,
    /// matching the onchain computation in the Raindex orderbook.
    /// Used for TakeOrderV3 events which embed the order but don't
    /// include the precomputed hash.
    pub(crate) fn from_order(order: &IOrderBookV6::OrderV4) -> Self {
        Self(keccak256(order.abi_encode()))
    }
}

impl fmt::Display for OrderHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for OrderHash {
    type Err = <B256 as FromStr>::Err;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        source.parse().map(Self)
    }
}

/// Classification of an order's pricing mechanism.
/// Populated later by the Order Classification step; for now
/// all discovered orders start as `Unknown`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum OrderType {
    /// Not yet classified (classification is a separate step).
    Unknown,
}

/// Which side of the order involves USDC, determining the
/// bot's execution scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Scenario {
    /// User sells wtToken for USDC. Bot provides USDC, receives wtToken.
    A,
    /// User buys wtToken with USDC. Bot provides wtToken, receives USDC.
    B,
}

/// The TrackedOrder entity, modeled as an enum of lifecycle states.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TrackedOrder {
    /// Order discovered and available for taking.
    Active {
        owner: Address,
        /// Base equity symbol (e.g. "AAPL"), not the tokenized form.
        symbol: Symbol,
        scenario: Scenario,
        /// The token the order owner provides (their output).
        output_token: Address,
        /// The token the order owner wants (their input).
        input_token: Address,
        order_type: OrderType,
        /// Maximum output declared when the order was added.
        max_output: U256,
        /// Remaining output after partial fills by us or others.
        remaining_output: U256,
        discovered_block: u64,
        discovered_at: DateTime<Utc>,
    },

    /// Order fully consumed (remaining output reached zero).
    Exhausted {
        owner: Address,
        symbol: Symbol,
        scenario: Scenario,
        exhausted_at: DateTime<Utc>,
    },

    /// Order removed by its owner.
    Removed {
        owner: Address,
        symbol: Symbol,
        removed_at: DateTime<Utc>,
    },
}

/// Commands that drive TrackedOrder state transitions.
#[derive(Debug, Clone)]
pub(crate) enum TrackedOrderCommand {
    /// Initialize from an AddOrderV3 event.
    Discover {
        owner: Address,
        symbol: Symbol,
        scenario: Scenario,
        output_token: Address,
        input_token: Address,
        max_output: U256,
        block: u64,
        discovered_at: DateTime<Utc>,
    },

    /// Record a fill from a TakeOrderV3 event (ours or another taker's).
    RecordFill {
        amount_out: U256,
        is_ours: bool,
        filled_at: DateTime<Utc>,
    },

    /// Mark order as removed (from RemoveOrderV3 event).
    MarkRemoved { removed_at: DateTime<Utc> },
}

/// Events emitted by TrackedOrder command handling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TrackedOrderEvent {
    Discovered {
        owner: Address,
        symbol: Symbol,
        scenario: Scenario,
        output_token: Address,
        input_token: Address,
        order_type: OrderType,
        max_output: U256,
        discovered_block: u64,
        discovered_at: DateTime<Utc>,
    },

    Filled {
        amount_out: U256,
        is_ours: bool,
        remaining_output: U256,
        filled_at: DateTime<Utc>,
    },

    Exhausted {
        exhausted_at: DateTime<Utc>,
    },

    Removed {
        removed_at: DateTime<Utc>,
    },
}

impl DomainEvent for TrackedOrderEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Discovered { .. } => "TrackedOrderEvent::Discovered",
            Self::Filled { .. } => "TrackedOrderEvent::Filled",
            Self::Exhausted { .. } => "TrackedOrderEvent::Exhausted",
            Self::Removed { .. } => "TrackedOrderEvent::Removed",
        }
        .to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Errors from TrackedOrder command handling.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum TrackedOrderError {
    #[error(
        "Cannot record fill: amount_out {amount_out} exceeds \
         remaining_output {remaining_output}"
    )]
    FillExceedsRemaining {
        amount_out: U256,
        remaining_output: U256,
    },

    #[error("Order is already in terminal state")]
    AlreadyTerminal,
}

#[async_trait]
impl EventSourced for TrackedOrder {
    type Id = OrderHash;
    type Event = TrackedOrderEvent;
    type Command = TrackedOrderCommand;
    type Error = TrackedOrderError;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "TrackedOrder";
    const PROJECTION: Table = Table("tracked_order_view");
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            TrackedOrderEvent::Discovered {
                owner,
                symbol,
                scenario,
                output_token,
                input_token,
                order_type,
                max_output,
                discovered_block,
                discovered_at,
            } => Some(Self::Active {
                owner: *owner,
                symbol: symbol.clone(),
                scenario: *scenario,
                output_token: *output_token,
                input_token: *input_token,
                order_type: order_type.clone(),
                max_output: *max_output,
                remaining_output: *max_output,
                discovered_block: *discovered_block,
                discovered_at: *discovered_at,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        match (entity, event) {
            (
                Self::Active {
                    owner,
                    symbol,
                    scenario,
                    ..
                },
                TrackedOrderEvent::Filled {
                    remaining_output,
                    filled_at,
                    ..
                },
            ) => {
                if remaining_output.is_zero() {
                    Ok(Some(Self::Exhausted {
                        owner: *owner,
                        symbol: symbol.clone(),
                        scenario: *scenario,
                        exhausted_at: *filled_at,
                    }))
                } else {
                    let Self::Active {
                        owner,
                        symbol,
                        scenario,
                        output_token,
                        input_token,
                        order_type,
                        max_output,
                        discovered_block,
                        discovered_at,
                        ..
                    } = entity
                    else {
                        // unreachable: outer match already matched Active
                        return Ok(None);
                    };

                    Ok(Some(Self::Active {
                        owner: *owner,
                        symbol: symbol.clone(),
                        scenario: *scenario,
                        output_token: *output_token,
                        input_token: *input_token,
                        order_type: order_type.clone(),
                        max_output: *max_output,
                        remaining_output: *remaining_output,
                        discovered_block: *discovered_block,
                        discovered_at: *discovered_at,
                    }))
                }
            }

            (Self::Active { owner, symbol, .. }, TrackedOrderEvent::Removed { removed_at }) => {
                Ok(Some(Self::Removed {
                    owner: *owner,
                    symbol: symbol.clone(),
                    removed_at: *removed_at,
                }))
            }

            (Self::Active { .. }, TrackedOrderEvent::Exhausted { exhausted_at }) => {
                let Self::Active {
                    owner,
                    symbol,
                    scenario,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Ok(Some(Self::Exhausted {
                    owner: *owner,
                    symbol: symbol.clone(),
                    scenario: *scenario,
                    exhausted_at: *exhausted_at,
                }))
            }

            // Exhausted on already-Exhausted is idempotent.
            // This happens because RecordFill emitting remaining=0
            // triggers evolve -> Exhausted, then the subsequent
            // Exhausted event arrives on the already-Exhausted entity.
            (Self::Exhausted { .. }, TrackedOrderEvent::Exhausted { .. }) => {
                Ok(Some(entity.clone()))
            }

            // Events on terminal states or unexpected combinations: no-op
            _ => Ok(None),
        }
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            TrackedOrderCommand::Discover {
                owner,
                symbol,
                scenario,
                output_token,
                input_token,
                max_output,
                block,
                discovered_at,
            } => Ok(vec![TrackedOrderEvent::Discovered {
                owner,
                symbol,
                scenario,
                output_token,
                input_token,
                order_type: OrderType::Unknown,
                max_output,
                discovered_block: block,
                discovered_at,
            }]),

            // Cannot record fill or remove for non-existent order
            TrackedOrderCommand::RecordFill { .. } | TrackedOrderCommand::MarkRemoved { .. } => {
                Ok(vec![])
            }
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self, command) {
            // Duplicate discovery is idempotent
            (Self::Active { .. }, TrackedOrderCommand::Discover { .. }) => Ok(vec![]),

            (
                Self::Active {
                    remaining_output, ..
                },
                TrackedOrderCommand::RecordFill {
                    amount_out,
                    is_ours,
                    filled_at,
                },
            ) => {
                if amount_out > *remaining_output {
                    return Err(TrackedOrderError::FillExceedsRemaining {
                        amount_out,
                        remaining_output: *remaining_output,
                    });
                }

                let new_remaining = *remaining_output - amount_out;
                let mut events = vec![TrackedOrderEvent::Filled {
                    amount_out,
                    is_ours,
                    remaining_output: new_remaining,
                    filled_at,
                }];

                if new_remaining.is_zero() {
                    events.push(TrackedOrderEvent::Exhausted {
                        exhausted_at: filled_at,
                    });
                }

                Ok(events)
            }

            (Self::Active { .. }, TrackedOrderCommand::MarkRemoved { removed_at }) => {
                Ok(vec![TrackedOrderEvent::Removed { removed_at }])
            }

            // Terminal states reject all commands
            (Self::Exhausted { .. } | Self::Removed { .. }, _) => {
                Err(TrackedOrderError::AlreadyTerminal)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, U256, fixed_bytes};
    use chrono::Utc;

    use st0x_event_sorcery::{AggregateError, LifecycleError, TestStore, replay};
    use st0x_execution::Symbol;
    use st0x_shared::bindings::IOrderBookV6;

    use super::*;

    fn symbol(ticker: &str) -> Symbol {
        Symbol::new(ticker).unwrap()
    }

    fn discover_command(
        owner: Address,
        symbol: Symbol,
        scenario: Scenario,
        max_output: U256,
    ) -> TrackedOrderCommand {
        TrackedOrderCommand::Discover {
            owner,
            symbol,
            scenario,
            output_token: Address::repeat_byte(0xAA),
            input_token: Address::repeat_byte(0xBB),
            max_output,
            block: 100,
            discovered_at: Utc::now(),
        }
    }

    fn order_hash() -> OrderHash {
        OrderHash::new(B256::repeat_byte(0x11))
    }

    // ── Discovery ──────────────────────────────────────────────

    #[tokio::test]
    async fn discover_creates_active_order() {
        let store = TestStore::<TrackedOrder>::new(());
        let owner = Address::repeat_byte(0x01);
        let max_output = U256::from(1000u64);

        store
            .send(
                &order_hash(),
                discover_command(owner, symbol("AAPL"), Scenario::A, max_output),
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        assert!(
            matches!(
                order,
                Some(TrackedOrder::Active {
                    ref symbol,
                    remaining_output,
                    discovered_block: 100,
                    ..
                }) if symbol.to_string() == "AAPL"
                   && remaining_output == max_output
            ),
            "Expected Active order with AAPL and remaining_output={max_output}, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn discover_stores_scenario_and_token_addresses() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                TrackedOrderCommand::Discover {
                    owner: Address::repeat_byte(0x01),
                    symbol: symbol("TSLA"),
                    scenario: Scenario::B,
                    output_token: Address::repeat_byte(0xCC),
                    input_token: Address::repeat_byte(0xDD),
                    max_output: U256::from(500u64),
                    block: 42,
                    discovered_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        let Some(TrackedOrder::Active {
            scenario,
            output_token,
            input_token,
            ..
        }) = order
        else {
            panic!("Expected Active order, got: {order:?}");
        };

        assert_eq!(scenario, Scenario::B);
        assert_eq!(output_token, Address::repeat_byte(0xCC));
        assert_eq!(input_token, Address::repeat_byte(0xDD));
    }

    #[tokio::test]
    async fn duplicate_discover_is_idempotent() {
        let store = TestStore::<TrackedOrder>::new(());
        let owner = Address::repeat_byte(0x01);

        store
            .send(
                &order_hash(),
                discover_command(owner, symbol("AAPL"), Scenario::A, U256::from(1000u64)),
            )
            .await
            .unwrap();

        // Second discover on same aggregate: no-op
        store
            .send(
                &order_hash(),
                discover_command(owner, symbol("AAPL"), Scenario::A, U256::from(1000u64)),
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        assert!(matches!(order, Some(TrackedOrder::Active { .. })));
    }

    // ── Fills ──────────────────────────────────────────────────

    #[tokio::test]
    async fn partial_fill_reduces_remaining_output() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(1000u64),
                ),
            )
            .await
            .unwrap();

        store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(300u64),
                    is_ours: false,
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        let Some(TrackedOrder::Active {
            remaining_output, ..
        }) = order
        else {
            panic!("Expected Active order after partial fill, got: {order:?}");
        };
        assert_eq!(
            remaining_output,
            U256::from(700u64),
            "Expected 1000 - 300 = 700 remaining"
        );
    }

    #[tokio::test]
    async fn multiple_partial_fills_accumulate() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(1000u64),
                ),
            )
            .await
            .unwrap();

        // First fill: 300 (by someone else)
        store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(300u64),
                    is_ours: false,
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        // Second fill: 200 (by us)
        store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(200u64),
                    is_ours: true,
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        let Some(TrackedOrder::Active {
            remaining_output, ..
        }) = order
        else {
            panic!("Expected Active after two partial fills, got: {order:?}");
        };
        assert_eq!(
            remaining_output,
            U256::from(500u64),
            "Expected 1000 - 300 - 200 = 500 remaining"
        );
    }

    #[tokio::test]
    async fn fill_that_exhausts_transitions_to_exhausted() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(500u64),
                ),
            )
            .await
            .unwrap();

        store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(500u64),
                    is_ours: true,
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        assert!(
            matches!(
                order,
                Some(TrackedOrder::Exhausted { ref symbol, scenario: Scenario::A, .. })
                    if symbol.to_string() == "AAPL"
            ),
            "Expected Exhausted with AAPL Scenario::A, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn partial_fills_then_exhaust() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("GME"),
                    Scenario::B,
                    U256::from(100u64),
                ),
            )
            .await
            .unwrap();

        // Three fills: 40 + 35 + 25 = 100 (exactly exhausts)
        for amount in [40u64, 35, 25] {
            store
                .send(
                    &order_hash(),
                    TrackedOrderCommand::RecordFill {
                        amount_out: U256::from(amount),
                        is_ours: amount == 25, // last fill is ours
                        filled_at: Utc::now(),
                    },
                )
                .await
                .unwrap();
        }

        let order = store.load(&order_hash()).await.unwrap();
        assert!(
            matches!(order, Some(TrackedOrder::Exhausted { .. })),
            "Expected Exhausted after 40+35+25=100 fills on 100 max, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn fill_exceeding_remaining_is_rejected() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(100u64),
                ),
            )
            .await
            .unwrap();

        let result = store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(200u64),
                    is_ours: false,
                    filled_at: Utc::now(),
                },
            )
            .await;

        let error = result.unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TrackedOrderError::FillExceedsRemaining { .. }
                ))
            ),
            "Expected FillExceedsRemaining, got: {error:?}"
        );
    }

    // ── Removal ────────────────────────────────────────────────

    #[tokio::test]
    async fn mark_removed_transitions_to_removed() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(1000u64),
                ),
            )
            .await
            .unwrap();

        store
            .send(
                &order_hash(),
                TrackedOrderCommand::MarkRemoved {
                    removed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        assert!(
            matches!(
                order,
                Some(TrackedOrder::Removed { ref symbol, .. })
                    if symbol.to_string() == "AAPL"
            ),
            "Expected Removed with AAPL, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn remove_partially_filled_order() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(1000u64),
                ),
            )
            .await
            .unwrap();

        // Partial fill first
        store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(300u64),
                    is_ours: false,
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        // Then remove
        store
            .send(
                &order_hash(),
                TrackedOrderCommand::MarkRemoved {
                    removed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        assert!(
            matches!(order, Some(TrackedOrder::Removed { .. })),
            "Expected Removed after partial fill + remove, got: {order:?}"
        );
    }

    // ── Terminal state rejection ───────────────────────────────

    #[tokio::test]
    async fn fill_on_exhausted_is_rejected() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(100u64),
                ),
            )
            .await
            .unwrap();

        // Exhaust
        store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(100u64),
                    is_ours: true,
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        // Try another fill on exhausted order
        let result = store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(10u64),
                    is_ours: false,
                    filled_at: Utc::now(),
                },
            )
            .await;

        let error = result.unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TrackedOrderError::AlreadyTerminal
                ))
            ),
            "Expected AlreadyTerminal, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn fill_on_removed_is_rejected() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(1000u64),
                ),
            )
            .await
            .unwrap();

        store
            .send(
                &order_hash(),
                TrackedOrderCommand::MarkRemoved {
                    removed_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let result = store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(10u64),
                    is_ours: false,
                    filled_at: Utc::now(),
                },
            )
            .await;

        let error = result.unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TrackedOrderError::AlreadyTerminal
                ))
            ),
            "Expected AlreadyTerminal, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn remove_on_exhausted_is_rejected() {
        let store = TestStore::<TrackedOrder>::new(());

        store
            .send(
                &order_hash(),
                discover_command(
                    Address::repeat_byte(0x01),
                    symbol("AAPL"),
                    Scenario::A,
                    U256::from(100u64),
                ),
            )
            .await
            .unwrap();

        store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(100u64),
                    is_ours: true,
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let result = store
            .send(
                &order_hash(),
                TrackedOrderCommand::MarkRemoved {
                    removed_at: Utc::now(),
                },
            )
            .await;

        let error = result.unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TrackedOrderError::AlreadyTerminal
                ))
            ),
            "Expected AlreadyTerminal, got: {error:?}"
        );
    }

    // ── Event replay ───────────────────────────────────────────

    #[test]
    fn replay_discover_then_fill_then_exhaust() {
        let events = vec![
            TrackedOrderEvent::Discovered {
                owner: Address::repeat_byte(0x01),
                symbol: symbol("AAPL"),
                scenario: Scenario::A,
                output_token: Address::repeat_byte(0xAA),
                input_token: Address::repeat_byte(0xBB),
                order_type: OrderType::Unknown,
                max_output: U256::from(100u64),
                discovered_block: 50,
                discovered_at: Utc::now(),
            },
            TrackedOrderEvent::Filled {
                amount_out: U256::from(60u64),
                is_ours: false,
                remaining_output: U256::from(40u64),
                filled_at: Utc::now(),
            },
            TrackedOrderEvent::Filled {
                amount_out: U256::from(40u64),
                is_ours: true,
                remaining_output: U256::ZERO,
                filled_at: Utc::now(),
            },
            TrackedOrderEvent::Exhausted {
                exhausted_at: Utc::now(),
            },
        ];

        let entity = replay::<TrackedOrder>(events).unwrap();
        assert!(
            matches!(entity, Some(TrackedOrder::Exhausted { ref symbol, .. }) if symbol.to_string() == "AAPL"),
            "Expected Exhausted AAPL after replay, got: {entity:?}"
        );
    }

    #[test]
    fn replay_discover_then_remove() {
        let events = vec![
            TrackedOrderEvent::Discovered {
                owner: Address::repeat_byte(0x01),
                symbol: symbol("TSLA"),
                scenario: Scenario::B,
                output_token: Address::repeat_byte(0xAA),
                input_token: Address::repeat_byte(0xBB),
                order_type: OrderType::Unknown,
                max_output: U256::from(500u64),
                discovered_block: 10,
                discovered_at: Utc::now(),
            },
            TrackedOrderEvent::Removed {
                removed_at: Utc::now(),
            },
        ];

        let entity = replay::<TrackedOrder>(events).unwrap();
        assert!(
            matches!(entity, Some(TrackedOrder::Removed { ref symbol, .. }) if symbol.to_string() == "TSLA"),
            "Expected Removed TSLA after replay, got: {entity:?}"
        );
    }

    // ── OrderHash ID ───────────────────────────────────────────

    #[test]
    fn order_hash_display_roundtrips() {
        let hash = OrderHash::new(B256::repeat_byte(0xAB));
        let displayed = hash.to_string();
        let parsed: OrderHash = displayed.parse().unwrap();
        assert_eq!(hash, parsed);
    }

    // ── RecordFill on non-existent order ───────────────────────

    #[tokio::test]
    async fn fill_on_nonexistent_order_is_noop() {
        let store = TestStore::<TrackedOrder>::new(());

        // RecordFill as initialize command: should produce no events
        store
            .send(
                &order_hash(),
                TrackedOrderCommand::RecordFill {
                    amount_out: U256::from(100u64),
                    is_ours: false,
                    filled_at: Utc::now(),
                },
            )
            .await
            .unwrap();

        let order = store.load(&order_hash()).await.unwrap();
        assert!(
            order.is_none(),
            "Expected no order after fill on nonexistent aggregate"
        );
    }

    // ── Order hash computation ──────────────────────────────────

    #[test]
    fn from_order_produces_deterministic_hash() {
        let order = IOrderBookV6::OrderV4 {
            owner: Address::repeat_byte(0x01),
            evaluable: IOrderBookV6::EvaluableV4 {
                interpreter: Address::repeat_byte(0x02),
                store: Address::repeat_byte(0x03),
                bytecode: Bytes::from_static(&[0x01, 0x02, 0x03]),
            },
            validInputs: vec![IOrderBookV6::IOV2 {
                token: Address::repeat_byte(0xAA),
                vaultId: B256::repeat_byte(0x01),
            }],
            validOutputs: vec![IOrderBookV6::IOV2 {
                token: Address::repeat_byte(0xBB),
                vaultId: B256::repeat_byte(0x02),
            }],
            nonce: B256::repeat_byte(0xFF),
        };

        let hash_a = OrderHash::from_order(&order);
        let hash_b = OrderHash::from_order(&order);
        assert_eq!(hash_a, hash_b, "Same order must produce same hash");
    }

    #[test]
    fn different_orders_produce_different_hashes() {
        let order_a = IOrderBookV6::OrderV4 {
            owner: Address::repeat_byte(0x01),
            evaluable: IOrderBookV6::EvaluableV4 {
                interpreter: Address::repeat_byte(0x02),
                store: Address::repeat_byte(0x03),
                bytecode: Bytes::from_static(&[0x01]),
            },
            validInputs: vec![],
            validOutputs: vec![],
            nonce: B256::repeat_byte(0x01),
        };

        let order_b = IOrderBookV6::OrderV4 {
            owner: Address::repeat_byte(0x01),
            evaluable: IOrderBookV6::EvaluableV4 {
                interpreter: Address::repeat_byte(0x02),
                store: Address::repeat_byte(0x03),
                bytecode: Bytes::from_static(&[0x01]),
            },
            validInputs: vec![],
            validOutputs: vec![],
            nonce: B256::repeat_byte(0x02), // different nonce
        };

        assert_ne!(
            OrderHash::from_order(&order_a),
            OrderHash::from_order(&order_b),
            "Different orders must produce different hashes"
        );
    }

    #[test]
    fn from_order_matches_new_for_known_hash() {
        // If we know the expected hash, from_order should match new()
        let order = IOrderBookV6::OrderV4 {
            owner: Address::repeat_byte(0x01),
            evaluable: IOrderBookV6::EvaluableV4 {
                interpreter: Address::repeat_byte(0x02),
                store: Address::repeat_byte(0x03),
                bytecode: Bytes::default(),
            },
            validInputs: vec![],
            validOutputs: vec![],
            nonce: B256::ZERO,
        };

        let computed = OrderHash::from_order(&order);
        let expected = OrderHash::new(fixed_bytes!(
            "274f40538a842ed59a86ca5e89490671b36dbc25cad49dc9791bb654f159edd6"
        ));
        assert_eq!(computed, expected);
    }
}
