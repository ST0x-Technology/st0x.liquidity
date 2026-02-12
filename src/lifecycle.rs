//! Lifecycle adapter for event-sourced entities.
//!
//! # The Problem
//!
//! Event-sourced entities (aggregates, views) naturally model as state
//! machines: genesis events create them, subsequent events transition
//! between valid states. The type `T` represents this clean business
//! model - a perfect world where every event is valid.
//!
//! Reality is messier:
//! - `Aggregate::apply` and `View::update` are **infallible** (no
//!   `Result` return)
//! - Financial applications **cannot panic** on arithmetic overflow
//! - Events might arrive before genesis (replay ordering, bugs)
//! - Transitions might fail (overflow, invalid state combinations)
//!
//! # The Solution
//!
//! `Lifecycle<T, E>` wraps your clean domain model `T` and handles
//! infrastructure concerns:
//!
//! - **`T`** - Your business model. Clean state machine with valid
//!   states only.
//! - **`Lifecycle<T, E>`** - Adapter that adds lifecycle tracking and
//!   error capture.
//!
//! This separation keeps `T` focused on domain logic while
//! `Lifecycle` handles:
//! - Tracking whether the entity exists yet (`Uninitialized`)
//! - Capturing failures without panicking (`Failed`)
//! - Preserving the last valid state for debugging/recovery
//!
//! # Usage
//!
//! ```ignore
//! fn apply(&mut self, event: Self::Event) {
//!     *self = self
//!         .clone()
//!         .transition(&event, MyEntity::apply_transition)
//!         .or_initialize(&event, MyEntity::from_event);
//! }
//! ```
//!
//! - `transition()` applies events to an existing entity
//! - `or_initialize()` handles genesis events if the entity doesn't exist yet
//! - Failures transition to `Failed` instead of panicking

use async_trait::async_trait;
use cqrs_es::persist::GenericQuery;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, Query, View};
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteViewRepository;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use tracing::error;

/// Associates a domain entity with its event type.
///
/// Required by [`Lifecycle`] to carry typed event information in
/// error states instead of opaque strings.
pub(crate) trait EventSourced {
    type Event: DomainEvent + Eq;
}

/// A query that materializes a `Lifecycle` aggregate as its own
/// view in SQLite.
pub(crate) type SqliteQuery<State, CustomError = Never> = GenericQuery<
    SqliteViewRepository<Lifecycle<State, CustomError>, Lifecycle<State, CustomError>>,
    Lifecycle<State, CustomError>,
    Lifecycle<State, CustomError>,
>;

/// A lifecycle wrapper for event-sourced entities.
///
/// Wraps entity data `State` and tracks whether the entity is
/// uninitialized, live, or failed due to an error during event
/// application.
///
/// # Type Parameters
///
/// - `State`: The entity data type (e.g., `Position`,
///   `OnChainTrade`). Must implement [`EventSourced`] to associate
///   with its event type.
/// - `CustomError`: The domain-specific error type for fallible
///   transitions (e.g., `ArithmeticError`).
///   Use [`Never`] for entities with no fallible operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum Lifecycle<State: EventSourced, CustomError = Never> {
    /// No events have been applied yet. This is the default state.
    Uninitialized,

    /// Normal operational state containing valid entity data.
    Live(State),

    /// Error state entered when event application fails.
    ///
    /// The entity becomes unusable, preventing further damage from
    /// cascading errors. The `last_valid_state` preserves the state
    /// before failure for debugging and potential recovery.
    Failed {
        error: LifecycleError<State, CustomError>,
        last_valid_state: Option<Box<State>>,
    },
}

impl<State: EventSourced, CustomError> Default for Lifecycle<State, CustomError> {
    fn default() -> Self {
        Self::Uninitialized
    }
}

/// An uninhabited type for entities with no fallible operations.
///
/// Similar to `std::convert::Infallible` but implements
/// `Serialize`/`Deserialize` for compatibility with `cqrs_es`
/// bounds.
///
/// Since this enum has no variants, values of type `Never`
/// cannot be constructed, making
/// `LifecycleError::Custom(Never)` unreachable at runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("never")]
pub(crate) enum Never {}

/// Errors that can occur during lifecycle transitions.
///
/// Type-safe: carries the actual aggregate state and event that
/// caused the error rather than opaque debug strings.
///
/// # Type Parameters
///
/// - `State`: The entity type ([`EventSourced`] implementor)
/// - `CustomError`: The domain-specific error
///   (e.g., `ArithmeticError`)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LifecycleError<State: EventSourced, CustomError = Never> {
    /// A transition event was applied to an uninitialized entity.
    #[error("operation on uninitialized state")]
    Uninitialized,
    /// An initialization event was applied to an already-live
    /// entity.
    #[error("initialization on already-live state")]
    AlreadyInitialized,
    /// An event was applied that doesn't match the current state.
    #[error("event '{event:?}' not applicable to state '{state:?}'")]
    Mismatch {
        state: Box<Lifecycle<State, CustomError>>,
        event: State::Event,
    },
    /// A domain-specific error (e.g., arithmetic overflow).
    #[error(transparent)]
    Custom(#[from] CustomError),
}

impl<State: EventSourced + Debug, CustomError: Display + Debug> Lifecycle<State, CustomError> {
    pub(crate) fn live(&self) -> Result<&State, LifecycleError<State, CustomError>>
    where
        State: Clone,
        CustomError: Clone,
    {
        match self {
            Self::Live(inner) => Ok(inner),
            Self::Uninitialized => Err(LifecycleError::Uninitialized),
            Self::Failed { error, .. } => Err(error.clone()),
        }
    }

    /// Apply a transition to a live entity.
    ///
    /// - If Live: applies the transition
    /// - If Uninitialized: returns Failed with
    ///   last_valid_state = None
    /// - If Failed: returns self unchanged
    pub(crate) fn transition<Apply>(self, event: &State::Event, apply: Apply) -> Self
    where
        Apply: FnOnce(&State::Event, &State) -> Result<State, LifecycleError<State, CustomError>>,
    {
        match self {
            Self::Live(current) => match apply(event, &current) {
                Ok(new_state) => Self::Live(new_state),
                Err(err) => {
                    error!("Lifecycle failed during transition: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_state: Some(Box::new(current)),
                    }
                }
            },
            Self::Uninitialized => Self::Failed {
                error: LifecycleError::Uninitialized,
                last_valid_state: None,
            },
            failed @ Self::Failed { .. } => failed,
        }
    }

    /// Initialize from an uninitialized state.
    ///
    /// - If Uninitialized: applies the initialization
    /// - If Failed with last_valid_state = None: was never live,
    ///   try to init
    /// - If Live or Failed with last_valid_state: returns Failed
    ///   (already initialized)
    pub(crate) fn initialize<Init>(self, event: &State::Event, init: Init) -> Self
    where
        Init: FnOnce(&State::Event) -> Result<State, LifecycleError<State, CustomError>>,
    {
        match self {
            Self::Uninitialized
            | Self::Failed {
                last_valid_state: None,
                ..
            } => match init(event) {
                Ok(new_state) => Self::Live(new_state),
                Err(err) => {
                    error!(
                        "Lifecycle failed during initialization: \
                         {err}"
                    );
                    Self::Failed {
                        error: err,
                        last_valid_state: None,
                    }
                }
            },
            Self::Live(current) => {
                let err = LifecycleError::AlreadyInitialized;
                error!("Lifecycle failed during initialization: {err}");
                Self::Failed {
                    error: err,
                    last_valid_state: Some(Box::new(current)),
                }
            }
            failed @ Self::Failed { .. } => failed,
        }
    }

    /// Try to initialize if transition failed on uninitialized
    /// state.
    ///
    /// - If Live: returns self (transition succeeded)
    /// - If Failed with last_valid_state = Some: returns self
    ///   (real error)
    /// - If Failed with last_valid_state = None: was
    ///   uninitialized, try to init
    /// - If Uninitialized: try to init
    pub(crate) fn or_initialize<Init>(self, event: &State::Event, init: Init) -> Self
    where
        Init: FnOnce(&State::Event) -> Result<State, LifecycleError<State, CustomError>>,
    {
        match &self {
            Self::Uninitialized
            | Self::Failed {
                last_valid_state: None,
                ..
            } => self.initialize(event, init),

            Self::Live(_)
            | Self::Failed {
                last_valid_state: Some(_),
                ..
            } => self,
        }
    }
}

/// Blanket View impl: any `Lifecycle<State, CustomError>` that
/// is an `Aggregate` can serve as its own materialized view by
/// replaying events through `apply`.
impl<State, CustomError> View<Self> for Lifecycle<State, CustomError>
where
    Self: Aggregate,
    State: EventSourced + Debug,
    CustomError: Debug,
{
    fn update(&mut self, event: &EventEnvelope<Self>) {
        self.apply(event.payload.clone());
    }
}

/// Blanket impl allowing `Arc<Q>` to be used as a `Query`
/// when `Q: Query`.
///
/// This enables sharing a single query instance across multiple
/// CQRS frameworks (e.g., mint, redemption, USDC) without
/// needing adapter wrappers.
#[async_trait]
impl<QueryImpl, State, CustomError> Query<Lifecycle<State, CustomError>> for Arc<QueryImpl>
where
    QueryImpl: Query<Lifecycle<State, CustomError>> + Send + Sync,
    State: EventSourced,
    Lifecycle<State, CustomError>: Aggregate,
{
    async fn dispatch(
        &self,
        aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<State, CustomError>>],
    ) {
        QueryImpl::dispatch(self, aggregate_id, events).await;
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use cqrs_es::DomainEvent;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use st0x_execution::{ArithmeticError, FractionalShares, Symbol};

    use super::*;
    use crate::position::{Position, PositionEvent};
    use crate::threshold::ExecutionThreshold;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
    struct TestState {
        value: i32,
    }

    impl EventSourced for TestState {
        type Event = TestEvent;
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("test error: {0}")]
    struct TestError(String);

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum TestEvent {
        Initialize { value: i32 },
        Increment { amount: i32 },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Initialize { .. } => "TestEvent::Initialize".to_string(),
                Self::Increment { .. } => "TestEvent::Increment".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
    #[error("test command error")]
    struct TestCommandError;

    #[async_trait]
    impl Aggregate for Lifecycle<TestState, TestError> {
        type Command = ();
        type Event = TestEvent;
        type Error = TestCommandError;
        type Services = ();

        fn aggregate_type() -> String {
            "TestState".to_string()
        }

        fn apply(&mut self, event: Self::Event) {
            *self = self
                .clone()
                .transition(&event, |event, current| match event {
                    TestEvent::Initialize { .. } => Err(LifecycleError::AlreadyInitialized),
                    TestEvent::Increment { amount } => Ok(TestState {
                        value: current.value + amount,
                    }),
                })
                .or_initialize(&event, |event| match event {
                    TestEvent::Initialize { value } => Ok(TestState { value: *value }),
                    _ => Err(LifecycleError::Mismatch {
                        state: Box::new(Lifecycle::Uninitialized),
                        event: event.clone(),
                    }),
                });
        }

        async fn handle(
            &self,
            _command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(vec![])
        }
    }

    #[test]
    fn transition_on_active_succeeds() {
        let state: Lifecycle<TestState, TestError> = Lifecycle::Live(TestState { value: 42 });

        let event = TestEvent::Increment { amount: 10 };
        let state = state.transition(&event, |event, current| match event {
            TestEvent::Increment { amount } => Ok(TestState {
                value: current.value + amount,
            }),
            _ => Err(LifecycleError::Mismatch {
                state: Box::new(Lifecycle::Live(current.clone())),
                event: event.clone(),
            }),
        });

        let Lifecycle::Live(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 52);
    }

    #[test]
    fn transition_on_uninitialized_corrupts_with_none() {
        let state: Lifecycle<TestState, TestError> = Lifecycle::Uninitialized;

        let state = state.transition(&TestEvent::Increment { amount: 10 }, |_, _| {
            Ok(TestState { value: 100 })
        });

        let Lifecycle::Failed {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };
        assert!(matches!(error, LifecycleError::Uninitialized));
        assert!(last_valid_state.is_none());
    }

    #[test]
    fn or_initialize_after_transition_on_uninitialized() {
        let state: Lifecycle<TestState, TestError> = Lifecycle::Uninitialized;
        let event = TestEvent::Initialize { value: 42 };

        let state = state
            .transition(&event, |_, _| Ok(TestState { value: 999 }))
            .or_initialize(&event, |event| match event {
                TestEvent::Initialize { value } => Ok(TestState { value: *value }),
                _ => Err(LifecycleError::Mismatch {
                    state: Box::new(Lifecycle::Uninitialized),
                    event: event.clone(),
                }),
            });

        let Lifecycle::Live(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 42);
    }

    #[test]
    fn or_initialize_skipped_after_successful_transition() {
        let state: Lifecycle<TestState, TestError> = Lifecycle::Live(TestState { value: 10 });
        let event = TestEvent::Increment { amount: 5 };

        let state = state
            .transition(&event, |event, current| match event {
                TestEvent::Increment { amount } => Ok(TestState {
                    value: current.value + amount,
                }),
                _ => Err(LifecycleError::Mismatch {
                    state: Box::new(Lifecycle::Live(current.clone())),
                    event: event.clone(),
                }),
            })
            .or_initialize(&event, |_| Ok(TestState { value: 999 }));

        let Lifecycle::Live(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 15);
    }

    #[test]
    fn or_initialize_skipped_after_real_transition_error() {
        let state: Lifecycle<TestState, TestError> = Lifecycle::Live(TestState { value: 42 });
        let event = TestEvent::Increment { amount: 10 };

        let state = state
            .transition(&event, |_, _| {
                Err(LifecycleError::Custom(TestError("real error".into())))
            })
            .or_initialize(&event, |_| Ok(TestState { value: 999 }));

        let Lifecycle::Failed {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };
        assert!(matches!(error, LifecycleError::Custom(TestError(msg)) if msg == "real error"));
        assert!(last_valid_state.is_some());
    }

    #[test]
    fn or_initialize_with_non_init_event_corrupts() {
        let state: Lifecycle<TestState, TestError> = Lifecycle::Uninitialized;
        let event = TestEvent::Increment { amount: 10 };

        let state = state
            .transition(&event, |_, _| Ok(TestState { value: 999 }))
            .or_initialize(&event, |event| match event {
                TestEvent::Initialize { value } => Ok(TestState { value: *value }),
                _ => Err(LifecycleError::Mismatch {
                    state: Box::new(Lifecycle::Uninitialized),
                    event: event.clone(),
                }),
            });

        let Lifecycle::Failed {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };
        assert!(matches!(error, LifecycleError::Mismatch { .. }));
        assert!(last_valid_state.is_none());
    }

    #[test]
    fn multiple_transitions_accumulate() {
        let mut state: Lifecycle<TestState, TestError> = Lifecycle::Live(TestState { value: 0 });

        for idx in 1..=3 {
            let event = TestEvent::Increment { amount: idx };
            state = state
                .transition(&event, |event, current| match event {
                    TestEvent::Increment { amount } => Ok(TestState {
                        value: current.value + amount,
                    }),
                    _ => Err(LifecycleError::Mismatch {
                        state: Box::new(Lifecycle::Live(current.clone())),
                        event: event.clone(),
                    }),
                })
                .or_initialize(&event, |event| {
                    Err(LifecycleError::Mismatch {
                        state: Box::new(Lifecycle::Uninitialized),
                        event: event.clone(),
                    })
                });
        }

        let Lifecycle::Live(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 6);
    }

    #[test]
    fn init_then_transitions() {
        let mut state: Lifecycle<TestState, TestError> = Lifecycle::Uninitialized;

        let init_event = TestEvent::Initialize { value: 10 };
        state = state
            .transition(&init_event, |event, current| {
                Err(LifecycleError::Mismatch {
                    state: Box::new(Lifecycle::Live(current.clone())),
                    event: event.clone(),
                })
            })
            .or_initialize(&init_event, |event| match event {
                TestEvent::Initialize { value } => Ok(TestState { value: *value }),
                _ => Err(LifecycleError::Mismatch {
                    state: Box::new(Lifecycle::Uninitialized),
                    event: event.clone(),
                }),
            });

        let Lifecycle::Live(inner) = &state else {
            panic!("Expected Active state after init");
        };
        assert_eq!(inner.value, 10);

        let transition_event = TestEvent::Increment { amount: 5 };
        state = state
            .transition(&transition_event, |event, current| match event {
                TestEvent::Increment { amount } => Ok(TestState {
                    value: current.value + amount,
                }),
                _ => Err(LifecycleError::Mismatch {
                    state: Box::new(Lifecycle::Live(current.clone())),
                    event: event.clone(),
                }),
            })
            .or_initialize(&transition_event, |event| {
                Err(LifecycleError::Mismatch {
                    state: Box::new(Lifecycle::Uninitialized),
                    event: event.clone(),
                })
            });

        let Lifecycle::Live(inner) = state else {
            panic!("Expected Active state after transition");
        };
        assert_eq!(inner.value, 15);
    }

    struct MockQuery {
        dispatch_called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Query<Lifecycle<TestState, TestError>> for MockQuery {
        async fn dispatch(
            &self,
            _aggregate_id: &str,
            _events: &[EventEnvelope<Lifecycle<TestState, TestError>>],
        ) {
            self.dispatch_called.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn arc_query_delegates_to_inner() {
        let dispatch_called = Arc::new(AtomicBool::new(false));

        let mock_query = Arc::new(MockQuery {
            dispatch_called: Arc::clone(&dispatch_called),
        });

        Query::<Lifecycle<TestState, TestError>>::dispatch(&mock_query, "test-id", &[]).await;

        assert!(
            dispatch_called.load(Ordering::SeqCst),
            "Expected dispatch to be called on inner query"
        );
    }

    type PositionLifecycle = Lifecycle<Position, ArithmeticError<FractionalShares>>;

    #[test]
    fn view_update_transitions_uninitialized_to_live() {
        let mut lifecycle = PositionLifecycle::default();
        assert!(matches!(lifecycle, Lifecycle::Uninitialized));

        let symbol = Symbol::new("AAPL").unwrap();
        let initialized_at = Utc::now();

        let event = PositionEvent::Initialized {
            symbol: symbol.clone(),
            threshold: ExecutionThreshold::whole_share(),
            initialized_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        lifecycle.update(&envelope);

        let Lifecycle::Live(position) = lifecycle else {
            panic!("Expected Live state after update, got: {lifecycle:?}");
        };

        assert_eq!(position.symbol, symbol);
        assert_eq!(position.net, FractionalShares::ZERO);
        assert_eq!(position.accumulated_long, FractionalShares::ZERO);
        assert_eq!(position.accumulated_short, FractionalShares::ZERO);
        assert_eq!(position.pending_offchain_order_id, None);
        assert_eq!(position.last_updated, Some(initialized_at));
    }

    #[test]
    fn view_update_clones_payload_leaving_envelope_usable() {
        let mut lifecycle = PositionLifecycle::default();

        let symbol = Symbol::new("TSLA").unwrap();
        let initialized_at = Utc::now();

        let event = PositionEvent::Initialized {
            symbol: symbol.clone(),
            threshold: ExecutionThreshold::whole_share(),
            initialized_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: symbol.to_string(),
            sequence: 1,
            payload: event.clone(),
            metadata: HashMap::new(),
        };

        lifecycle.update(&envelope);

        assert_eq!(envelope.aggregate_id, symbol.to_string());
        assert_eq!(envelope.sequence, 1);
        assert_eq!(envelope.payload, event);
        assert!(envelope.metadata.is_empty());

        assert!(matches!(lifecycle, Lifecycle::Live(_)));
    }
}
