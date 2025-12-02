use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use tracing::error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum State<T, E> {
    Uninitialized,
    Active(T),
    Corrupted {
        error: StateError<E>,
        last_valid_state: Option<Box<T>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StateError<E> {
    #[error("event applied to uninitialized state")]
    EventOnUninitialized,
    #[error("event '{event}' not applicable to state '{state}'")]
    EventNotApplicable { state: String, event: String },
    #[error(transparent)]
    Custom(E),
}

impl<T, E: Display> State<T, E> {
    pub(crate) fn initialize<Ev, F>(&mut self, event: Ev, f: F)
    where
        Ev: DomainEvent,
        F: FnOnce(Ev) -> Result<T, StateError<E>>,
    {
        let old = std::mem::replace(self, Self::Uninitialized);

        match old {
            Self::Uninitialized => match f(event) {
                Ok(state) => *self = Self::Active(state),
                Err(err) => {
                    error!("State corrupted during initialization: {err}");
                    *self = Self::Corrupted {
                        error: err,
                        last_valid_state: None,
                    };
                }
            },
            Self::Active(prev) => {
                let state_name = std::any::type_name::<T>();
                let event_name = event.event_type();
                error!(
                    "State corrupted: event '{event_name}' not applicable to state '{state_name}'"
                );
                *self = Self::Corrupted {
                    error: StateError::EventNotApplicable {
                        state: state_name.into(),
                        event: event_name,
                    },
                    last_valid_state: Some(Box::new(prev)),
                };
            }
            corrupted @ Self::Corrupted { .. } => {
                error!("initialize called on corrupted state, preserving original error");
                *self = corrupted;
            }
        }
    }

    pub(crate) fn transition<Ev, F>(&mut self, event: Ev, f: F)
    where
        Ev: DomainEvent,
        F: FnOnce(Ev, &T) -> Result<T, StateError<E>>,
    {
        let old = std::mem::replace(self, Self::Uninitialized);

        match old {
            Self::Active(current) => match f(event, &current) {
                Ok(new_state) => *self = Self::Active(new_state),
                Err(err) => {
                    error!("State corrupted: {err}");
                    *self = Self::Corrupted {
                        error: err,
                        last_valid_state: Some(Box::new(current)),
                    };
                }
            },
            Self::Uninitialized => {
                let event_name = event.event_type();
                error!("State corrupted: event '{event_name}' applied to uninitialized state");
                *self = Self::Corrupted {
                    error: StateError::EventOnUninitialized,
                    last_valid_state: None,
                };
            }
            corrupted @ Self::Corrupted { .. } => {
                error!("transition called on corrupted state, preserving original error");
                *self = corrupted;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestState {
        value: i32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("test error: {0}")]
    struct TestError(String);

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum TestEvent {
        Initialize { value: i32 },
        Increment { amount: i32 },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Initialize { .. } => "TestEvent::Initialize".into(),
                Self::Increment { .. } => "TestEvent::Increment".into(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".into()
        }
    }

    #[test]
    fn initialize_transitions_uninitialized_to_active() {
        let mut state: State<TestState, TestError> = State::Uninitialized;

        state.initialize(TestEvent::Initialize { value: 42 }, |event| {
            let TestEvent::Initialize { value } = event else {
                return Err(StateError::Custom(TestError("wrong event".into())));
            };
            Ok(TestState { value })
        });

        let State::Active(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 42);
    }

    #[test]
    fn initialize_transitions_to_corrupted_on_error() {
        let mut state: State<TestState, TestError> = State::Uninitialized;

        state.initialize(TestEvent::Initialize { value: 42 }, |_| {
            Err(StateError::Custom(TestError("init failed".into())))
        });

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };

        assert!(matches!(error, StateError::Custom(TestError(msg)) if msg == "init failed"));
        assert!(last_valid_state.is_none());
    }

    #[test]
    fn initialize_on_active_corrupts_with_event_info() {
        let mut state: State<TestState, TestError> = State::Active(TestState { value: 42 });

        state.initialize(TestEvent::Initialize { value: 100 }, |_| {
            Ok(TestState { value: 100 })
        });

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };

        let StateError::EventNotApplicable { state, event } = error else {
            panic!("Expected EventNotApplicable");
        };
        assert!(state.contains("TestState"));
        assert_eq!(event, "TestEvent::Initialize");
        assert_eq!(last_valid_state.unwrap().value, 42);
    }

    #[test]
    fn initialize_on_corrupted_preserves_original_error() {
        let mut state: State<TestState, TestError> = State::Corrupted {
            error: StateError::Custom(TestError("original".into())),
            last_valid_state: None,
        };

        state.initialize(TestEvent::Initialize { value: 100 }, |_| {
            Ok(TestState { value: 100 })
        });

        let State::Corrupted { error, .. } = state else {
            panic!("Expected Corrupted state");
        };

        assert!(matches!(error, StateError::Custom(TestError(msg)) if msg == "original"));
    }

    #[test]
    fn transition_transforms_active_state() {
        let mut state: State<TestState, TestError> = State::Active(TestState { value: 42 });

        state.transition(TestEvent::Increment { amount: 10 }, |event, current| {
            let TestEvent::Increment { amount } = event else {
                return Err(StateError::Custom(TestError("wrong event".into())));
            };
            Ok(TestState {
                value: current.value + amount,
            })
        });

        let State::Active(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 52);
    }

    #[test]
    fn transition_to_corrupted_on_error() {
        let mut state: State<TestState, TestError> = State::Active(TestState { value: 42 });

        state.transition(TestEvent::Increment { amount: 10 }, |_, _| {
            Err(StateError::Custom(TestError("transition failed".into())))
        });

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };

        assert!(matches!(error, StateError::Custom(TestError(msg)) if msg == "transition failed"));
        assert_eq!(last_valid_state.unwrap().value, 42);
    }

    #[test]
    fn transition_on_uninitialized_corrupts() {
        let mut state: State<TestState, TestError> = State::Uninitialized;

        state.transition(TestEvent::Increment { amount: 10 }, |_, _| {
            Ok(TestState { value: 100 })
        });

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };

        assert!(matches!(error, StateError::EventOnUninitialized));
        assert!(last_valid_state.is_none());
    }

    #[test]
    fn transition_on_corrupted_preserves_original_error() {
        let mut state: State<TestState, TestError> = State::Corrupted {
            error: StateError::Custom(TestError("original".into())),
            last_valid_state: Some(Box::new(TestState { value: 42 })),
        };

        state.transition(TestEvent::Increment { amount: 10 }, |_, _| {
            Ok(TestState { value: 100 })
        });

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };

        assert!(matches!(error, StateError::Custom(TestError(msg)) if msg == "original"));
        assert_eq!(last_valid_state.unwrap().value, 42);
    }

    #[test]
    fn multiple_transitions_accumulate() {
        let mut state: State<TestState, TestError> = State::Active(TestState { value: 0 });

        state.transition(TestEvent::Increment { amount: 1 }, |event, current| {
            let TestEvent::Increment { amount } = event else {
                return Err(StateError::Custom(TestError("wrong".into())));
            };
            Ok(TestState {
                value: current.value + amount,
            })
        });
        state.transition(TestEvent::Increment { amount: 2 }, |event, current| {
            let TestEvent::Increment { amount } = event else {
                return Err(StateError::Custom(TestError("wrong".into())));
            };
            Ok(TestState {
                value: current.value + amount,
            })
        });
        state.transition(TestEvent::Increment { amount: 3 }, |event, current| {
            let TestEvent::Increment { amount } = event else {
                return Err(StateError::Custom(TestError("wrong".into())));
            };
            Ok(TestState {
                value: current.value + amount,
            })
        });

        let State::Active(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 6);
    }

    #[test]
    fn transition_after_corruption_preserves_first_error() {
        let mut state: State<TestState, TestError> = State::Active(TestState { value: 42 });

        state.transition(TestEvent::Increment { amount: 10 }, |_, _| {
            Err(StateError::Custom(TestError("first failure".into())))
        });
        state.transition(TestEvent::Increment { amount: 100 }, |event, current| {
            let TestEvent::Increment { amount } = event else {
                return Err(StateError::Custom(TestError("wrong".into())));
            };
            Ok(TestState {
                value: current.value + amount,
            })
        });

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };

        assert!(matches!(error, StateError::Custom(TestError(msg)) if msg == "first failure"));
        assert_eq!(last_valid_state.unwrap().value, 42);
    }
}
