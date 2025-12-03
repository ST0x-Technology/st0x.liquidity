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
    #[error("operation on uninitialized state")]
    Uninitialized,
    #[error("event '{event}' not applicable to state '{state}'")]
    Mismatch { state: String, event: String },
    #[error(transparent)]
    Custom(#[from] E),
}

impl<T, E: Display> State<T, E> {
    pub(crate) fn active(&self) -> Result<&T, StateError<E>>
    where
        E: Clone,
    {
        match self {
            Self::Active(inner) => Ok(inner),
            Self::Uninitialized => Err(StateError::Uninitialized),
            Self::Corrupted { error, .. } => Err(error.clone()),
        }
    }

    /// Apply a transition to an active state.
    ///
    /// - If Active: applies the transition
    /// - If Uninitialized: returns Corrupted with last_valid_state = None
    /// - If Corrupted: returns self unchanged
    pub(crate) fn transition<Ev, F>(self, event: &Ev, f: F) -> Self
    where
        F: FnOnce(&Ev, &T) -> Result<T, StateError<E>>,
    {
        match self {
            Self::Active(current) => match f(event, &current) {
                Ok(new_state) => Self::Active(new_state),
                Err(err) => {
                    error!("State corrupted during transition: {err}");
                    Self::Corrupted {
                        error: err,
                        last_valid_state: Some(Box::new(current)),
                    }
                }
            },
            Self::Uninitialized => Self::Corrupted {
                error: StateError::Uninitialized,
                last_valid_state: None,
            },
            corrupted @ Self::Corrupted { .. } => corrupted,
        }
    }

    /// Initialize from an uninitialized state.
    pub(crate) fn initialize<Ev, F>(self, event: &Ev, f: F) -> Self
    where
        F: FnOnce(&Ev) -> Result<T, StateError<E>>,
    {
        match self {
            Self::Uninitialized
            | Self::Corrupted {
                last_valid_state: None,
                ..
            } => match f(event) {
                Ok(new_state) => Self::Active(new_state),
                Err(err) => {
                    error!("State corrupted during initialization: {err}");
                    Self::Corrupted {
                        error: err,
                        last_valid_state: None,
                    }
                }
            },
            already_initialized => already_initialized,
        }
    }

    /// Try to initialize if transition failed on uninitialized state.
    ///
    /// - If Active: returns self (transition succeeded)
    /// - If Corrupted with last_valid_state = Some: returns self (real error)
    /// - If Corrupted with last_valid_state = None: was uninitialized, try to init
    /// - If Uninitialized: try to init
    pub(crate) fn or_initialize<Ev, F>(self, event: &Ev, f: F) -> Self
    where
        F: FnOnce(&Ev) -> Result<T, StateError<E>>,
    {
        match &self {
            Self::Active(_)
            | Self::Corrupted {
                last_valid_state: Some(_),
                ..
            } => self,
            Self::Corrupted {
                last_valid_state: None,
                ..
            }
            | Self::Uninitialized => self.initialize(event, f),
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
        Migrate { value: i32 },
        Increment { amount: i32 },
    }

    #[test]
    fn transition_on_active_succeeds() {
        let state: State<TestState, TestError> = State::Active(TestState { value: 42 });

        let state = state.transition(&TestEvent::Increment { amount: 10 }, |ev, cur| match ev {
            TestEvent::Increment { amount } => Ok(TestState {
                value: cur.value + amount,
            }),
            _ => Err(StateError::Mismatch {
                state: format!("{cur:?}"),
                event: format!("{ev:?}"),
            }),
        });

        let State::Active(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 52);
    }

    #[test]
    fn transition_on_uninitialized_corrupts_with_none() {
        let state: State<TestState, TestError> = State::Uninitialized;

        let state = state.transition(&TestEvent::Increment { amount: 10 }, |_, _| {
            Ok(TestState { value: 100 })
        });

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };
        assert!(matches!(error, StateError::Uninitialized));
        assert!(last_valid_state.is_none());
    }

    #[test]
    fn or_initialize_after_transition_on_uninitialized() {
        let state: State<TestState, TestError> = State::Uninitialized;
        let event = TestEvent::Initialize { value: 42 };

        let state = state
            .transition(&event, |_, _| Ok(TestState { value: 999 }))
            .or_initialize(&event, |ev| match ev {
                TestEvent::Initialize { value } => Ok(TestState { value: *value }),
                _ => Err(StateError::Mismatch {
                    state: "Uninitialized".into(),
                    event: format!("{ev:?}"),
                }),
            });

        let State::Active(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 42);
    }

    #[test]
    fn or_initialize_skipped_after_successful_transition() {
        let state: State<TestState, TestError> = State::Active(TestState { value: 10 });
        let event = TestEvent::Increment { amount: 5 };

        let state = state
            .transition(&event, |ev, cur| match ev {
                TestEvent::Increment { amount } => Ok(TestState {
                    value: cur.value + amount,
                }),
                _ => Err(StateError::Mismatch {
                    state: format!("{cur:?}"),
                    event: format!("{ev:?}"),
                }),
            })
            .or_initialize(&event, |_| Ok(TestState { value: 999 }));

        let State::Active(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 15);
    }

    #[test]
    fn or_initialize_skipped_after_real_transition_error() {
        let state: State<TestState, TestError> = State::Active(TestState { value: 42 });
        let event = TestEvent::Increment { amount: 10 };

        let state = state
            .transition(&event, |_, _| {
                Err(StateError::Custom(TestError("real error".into())))
            })
            .or_initialize(&event, |_| Ok(TestState { value: 999 }));

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };
        assert!(matches!(error, StateError::Custom(TestError(msg)) if msg == "real error"));
        assert!(last_valid_state.is_some());
    }

    #[test]
    fn or_initialize_with_non_init_event_corrupts() {
        let state: State<TestState, TestError> = State::Uninitialized;
        let event = TestEvent::Increment { amount: 10 };

        let state = state
            .transition(&event, |_, _| Ok(TestState { value: 999 }))
            .or_initialize(&event, |ev| match ev {
                TestEvent::Initialize { value } => Ok(TestState { value: *value }),
                _ => Err(StateError::Mismatch {
                    state: "Uninitialized".into(),
                    event: format!("{ev:?}"),
                }),
            });

        let State::Corrupted {
            error,
            last_valid_state,
        } = state
        else {
            panic!("Expected Corrupted state");
        };
        assert!(matches!(error, StateError::Mismatch { .. }));
        assert!(last_valid_state.is_none());
    }

    #[test]
    fn multiple_transitions_accumulate() {
        let mut state: State<TestState, TestError> = State::Active(TestState { value: 0 });

        for i in 1..=3 {
            let event = TestEvent::Increment { amount: i };
            state = state
                .transition(&event, |ev, cur| match ev {
                    TestEvent::Increment { amount } => Ok(TestState {
                        value: cur.value + amount,
                    }),
                    _ => Err(StateError::Mismatch {
                        state: format!("{cur:?}"),
                        event: format!("{ev:?}"),
                    }),
                })
                .or_initialize(&event, |ev| {
                    Err(StateError::Mismatch {
                        state: "Uninitialized".into(),
                        event: format!("{ev:?}"),
                    })
                });
        }

        let State::Active(inner) = state else {
            panic!("Expected Active state");
        };
        assert_eq!(inner.value, 6);
    }

    #[test]
    fn init_then_transitions() {
        let mut state: State<TestState, TestError> = State::Uninitialized;

        let init_event = TestEvent::Initialize { value: 10 };
        state = state
            .transition(&init_event, |ev, cur| {
                Err(StateError::Mismatch {
                    state: format!("{cur:?}"),
                    event: format!("{ev:?}"),
                })
            })
            .or_initialize(&init_event, |ev| match ev {
                TestEvent::Initialize { value } | TestEvent::Migrate { value } => {
                    Ok(TestState { value: *value })
                }
                TestEvent::Increment { .. } => Err(StateError::Mismatch {
                    state: "Uninitialized".into(),
                    event: format!("{ev:?}"),
                }),
            });

        let State::Active(inner) = &state else {
            panic!("Expected Active state after init");
        };
        assert_eq!(inner.value, 10);

        let transition_event = TestEvent::Increment { amount: 5 };
        state = state
            .transition(&transition_event, |ev, cur| match ev {
                TestEvent::Increment { amount } => Ok(TestState {
                    value: cur.value + amount,
                }),
                _ => Err(StateError::Mismatch {
                    state: format!("{cur:?}"),
                    event: format!("{ev:?}"),
                }),
            })
            .or_initialize(&transition_event, |ev| {
                Err(StateError::Mismatch {
                    state: "Uninitialized".into(),
                    event: format!("{ev:?}"),
                })
            });

        let State::Active(inner) = state else {
            panic!("Expected Active state after transition");
        };
        assert_eq!(inner.value, 15);
    }
}
