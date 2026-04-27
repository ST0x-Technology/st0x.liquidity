# Feature Flags

## Flag inventory

| Flag                 | Purpose                             | Enabled by    |
| -------------------- | ----------------------------------- | ------------- |
| `test-support`       | Test infra visible to e2e tests     | dev-deps only |
| `mock`               | Mock executor / EVM implementations | dev-deps only |
| `wallet-turnkey`     | Turnkey wallet support              | `all-wallets` |
| `wallet-private-key` | Local signer wallet support         | `all-wallets` |
| `all-wallets`        | All wallet backends                 | `default`     |

## `test-support` vs `cfg(test)`

- **`cfg(test)`** is true when compiling unit tests (`#[cfg(test)] mod tests`)
  AND when compiling the lib for integration tests. It's set by the compiler,
  not controllable per-crate.
- **`feature = "test-support"`** is a cargo feature enabled only by
  `[dev-dependencies]`. It gates types/methods that integration tests (`tests/`)
  need but unit tests and production builds don't.

### When to use which

| Scenario                                  | Gate                                |
| ----------------------------------------- | ----------------------------------- |
| Unit test helper (same file, `mod tests`) | `#[cfg(test)]`                      |
| Type/method used by e2e tests in `tests/` | `#[cfg(feature = "test-support")]`  |
| Type that must exist in all builds but is | Always compiled; methods gated with |
| only functional in tests                  | `#[cfg(feature = "test-support")]`  |

### Common pitfall: dead code warnings

A `pub` method gated on `cfg(any(test, feature = "test-support"))` will trigger
`dead_code` warnings during `cargo test` if no unit test calls it — because
`cfg(test)` makes it visible to the compiler but nothing in the lib crate
references it. Fix: gate on `feature = "test-support"` only (not `test`). The
method won't exist in unit test builds but will exist in e2e builds where
dev-deps enable `test-support`.

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

The type compiles everywhere (zero-size in prod). Methods that only tests call
are gated on `feature = "test-support"`. The `check` method always exists but
returns `false` in prod (the compiler eliminates the branch).
