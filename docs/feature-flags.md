# Feature Flags

## Flag inventory

| Flag                 | Purpose                             | Enabled by                  |
| -------------------- | ----------------------------------- | --------------------------- |
| `test-support`       | Test infra visible to e2e tests     | explicit feature enablement |
| `mock`               | Mock executor / EVM implementations | dev-deps only               |
| `wallet-turnkey`     | Turnkey wallet support              | `all-wallets`               |
| `wallet-private-key` | Local signer wallet support         | `all-wallets`               |
| `all-wallets`        | All wallet backends                 | `default`                   |

## `test-support` vs `cfg(test)`

- **`cfg(test)`** is true only for the crate currently being compiled for its
  own unit tests (for example `#[cfg(test)] mod tests`). It is not set when that
  crate is compiled as a dependency of an integration test or another target. It
  is set by the compiler, not by Cargo feature selection.
- **`feature = "test-support"`** is a normal Cargo feature. Declaring a crate in
  `[dev-dependencies]` does not automatically enable that feature. If code in
  `tests/` or another build target needs feature-gated APIs, the feature must be
  enabled explicitly via `--features`, a dependency feature list, or normal
  feature unification during the test build.

### When to use which

| Scenario                                  | Gate                                |
| ----------------------------------------- | ----------------------------------- |
| Unit test helper (same file, `mod tests`) | `#[cfg(test)]`                      |
| Type/method used by e2e tests in `tests/` | `#[cfg(feature = "test-support")]`  |
| Type that must exist in all builds but is | Always compiled; methods gated with |
| only functional in tests                  | `#[cfg(feature = "test-support")]`  |

### Common pitfall: dead code warnings

A `pub` method gated on `cfg(any(test, feature = "test-support"))` will trigger
`dead_code` warnings during unit-test builds if no unit test calls it, because
`cfg(test)` makes it visible to that crate's test build. If the API exists only
for integration tests or external test harnesses, prefer
`#[cfg(feature = "test-support")]` so it is compiled only when the feature is
actually enabled. If unit tests also need it,
`cfg(any(test, feature =
"test-support"))` is correct, but the method still
needs real callers in unit-test builds.

### Pattern: zero-size prod / functional test type

When a type needs to exist in all builds (to avoid `#[cfg]` on every function
parameter and call site) but only does real work in tests:

```rust
#[derive(Clone)]
pub struct MyTestHook(
    #[cfg(feature = "test-support")] Arc<AtomicBool>,
    #[cfg(not(feature = "test-support"))] (),
);

impl MyTestHook {
    pub fn new() -> Self { /* ... */ }

    #[cfg(feature = "test-support")]
    pub fn arm(&self) { /* ... */ }

    fn check(&self) -> bool {
        #[cfg(feature = "test-support")]
        { self.0.swap(false, Ordering::SeqCst) }
        #[cfg(not(feature = "test-support"))]
        false
    }
}
```

The type compiles everywhere (zero-size in prod). Methods that only integration
tests or feature-enabled test builds call are gated on
`feature =
"test-support"`. The `check` method always exists but returns `false`
in prod (the compiler eliminates the branch). If unit tests in the same crate
also need test-only helpers, gate those helpers with
`cfg(any(test, feature =
"test-support"))` instead of assuming `cfg(test)` will
propagate through dependency builds.
