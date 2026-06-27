# Feature Flags

## Flag inventory

| Flag                 | Purpose                             | Enabled by                   |
| -------------------- | ----------------------------------- | ---------------------------- |
| `test-support`       | Test infra visible to e2e tests     | via dev-deps (Cargo feature) |
| `mock`               | Mock executor / EVM implementations | via dev-deps (Cargo feature) |
| `wallet-turnkey`     | Turnkey wallet support              | `all-wallets`                |
| `wallet-private-key` | Local signer wallet support         | `all-wallets`                |
| `all-wallets`        | All wallet backends                 | `default`                    |

## `test-support` vs `cfg(test)`

- **`cfg(test)`** is set by the compiler only for the crate currently being
  compiled as a test target — either the lib's own unit-test compilation
  (`#[cfg(test)] mod tests`) or an integration test crate (`tests/foo.rs`)
  itself. Library dependencies compiled as part of an integration test build do
  NOT get `cfg(test)` set; from their perspective it's a regular build.
- **`feature = "test-support"`** is a cargo feature, activated through Cargo's
  normal feature selection. We list it under `[dev-dependencies]` (via a
  self-reference: `st0x-hedge = { features = ["test-support"] }`), so its
  activation is scoped to dev builds — tests, examples, benches — and not
  production. It gates types/methods that integration tests (`tests/`) need to
  see across the crate boundary.

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
references it.

**Preferred fix:** use the new infra in unit tests — either by adding it to
existing tests or writing new ones. This is the best outcome when it improves
correctness guarantees and/or code quality.

**Fallback fix:** gate on `feature = "test-support"` only (not `test`). The
method won't exist in unit test builds but will exist in e2e builds where
dev-deps enable `test-support`.

### Dead code across a stacked PR: forward vs backward

When work is split into a Graphite stack, `deny(dead_code)` forces a choice
about temporarily-unused code. Distinguish code that is _not yet born_ from code
that _has died_:

- **Forward-dead** — introduced ahead of its consumer, which lands in the next
  upstack PR. Acceptable to keep behind `#[allow(dead_code)]` **only** with a
  comment naming the exact upstack issue + branch (and the PR once it exists)
  that consumes it. That upstack PR must be the _immediate next_ one in the
  stack, so the allow is never stranded. This is how a stack stays small: a base
  PR can introduce a type/trait/method whose first caller arrives one PR later.
- **Backward-dead** — superseded, i.e. a new path replaced it. MUST be removed
  in the same PR that introduces the replacement. Never annotate-and-keep
  superseded code; an `#[allow(dead_code)]` on a corpse is forbidden. When the
  replacement lands, the replaced code dies with it.

The test is simple: is the code waiting to be _born_ (a consumer is coming) or
has it _died_ (its consumers left)? Forward-dead may wait one PR behind an
attributed allow; backward-dead goes now.

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
