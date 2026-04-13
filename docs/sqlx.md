# SQLx

## Offline mode (`SQLX_OFFLINE=true`)

The nix dev shell sets `SQLX_OFFLINE=true` (in `flake.nix`), which tells sqlx
compile-time macros (`query!`, `query_scalar!`, etc.) to use cached metadata
from the `.sqlx/` directory instead of connecting to the database. This means:

- Regular `cargo check`/`cargo build`/`cargo nextest run` never need a running
  database -- they read from `.sqlx/` cache files checked into version control.
- If you add or change a `query!` macro invocation, you must regenerate the
  cache before the change will compile under `SQLX_OFFLINE=true`.

## Regenerating the query cache

```bash
cargo sqlx prepare --workspace -- --all-targets
```

Then check the updated `.sqlx/` files into version control.

### Pitfall: `#[cfg(test)]` queries don't work with offline mode

`cargo sqlx prepare` does NOT collect queries from `#[cfg(test)]` code, even
with `-- --all-targets`. This is a known limitation -- the `--all-targets` flag
is supposed to compile test targets during preparation, but in practice the
test-only queries are silently skipped.

When you then run `cargo nextest run` (which enables `cfg(test)`), the compiler
sees the query macro, finds no cached metadata, and fails with:

```
`SQLX_OFFLINE=true` but there is no cached data for this query
```

**The fix: use runtime query functions in test code.** Instead of the
compile-time macro `sqlx::query_scalar!("...")`, use the runtime function
`sqlx::query_scalar("...")`. The non-macro version doesn't need offline cache
entries. Since test code runs against a real in-memory database anyway,
compile-time query verification adds no value.

```rust
// BROKEN in offline mode -- macro needs cache entry that prepare won't generate
#[cfg(test)]
let count = sqlx::query_scalar!("SELECT COUNT(*) FROM my_table")
    .fetch_one(pool).await?;

// WORKS -- runtime query, no cache needed
#[cfg(test)]
let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM my_table")
    .fetch_one(pool).await?;
```

Note the type annotation on the `let` binding -- the runtime function doesn't
infer return types like the macro does.

## New worktrees

New git worktrees don't have a `dev.db` file. You'll see sqlx compile errors
like "unable to open database file" if you try to run without
`SQLX_OFFLINE=true` or without a database. Fix:

```bash
sqlx db reset -y
```

This creates and migrates the local database. Not needed for compilation under
`SQLX_OFFLINE=true`, but required for running the binary or running
`cargo sqlx prepare` (which connects to the DB to verify queries).

## Pitfall documentation policy

When you encounter a non-obvious sqlx issue (or any tooling footgun), document
it here. Future developers and agents will hit the same problems -- a few lines
of documentation saves hours of debugging.
