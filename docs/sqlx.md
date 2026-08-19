# SQLx

## Offline mode (`SQLX_OFFLINE=true`)

The nix dev shell sets `SQLX_OFFLINE=true` (in `flake.nix`), which tells sqlx
compile-time macros (`query!`, `query_scalar!`, etc.) to use cached metadata
from the `.sqlx/` directory instead of connecting to the database. This means:

- Regular `cargo check`/`cargo nextest run` typically don't need a running
  database -- they read from `.sqlx/` cache files checked into version control.
  Exception: test code using `query!` macros (see below).
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

## Generated columns as projection read keys

A materialized view stores the serialized aggregate in a `payload JSON` column.
Adding `STORED GENERATED` columns over `json_extract(payload, ...)` gives the
view sort and filter keys without a second source of truth for the wire shape.
Three things bite:

- **SQLite cannot `ALTER TABLE ADD` a STORED generated column.** Adding one to
  an existing view table means DROP + CREATE. That is safe for a projection --
  views are rebuilt from the event log at startup -- but never for a table
  holding anything the event log cannot regenerate.
- **chrono timestamps are not sortable as stored.** Its serde impl emits
  `SecondsFormat::AutoSi`, padding to 0, 3, 6 or 9 fractional digits depending
  on the value. Lexicographically `...20.5Z` sorts _before_ `...20Z` ('.' <
  'Z'), and `...20.500Z` sorts _after_ `...20.500000Z` though they are the same
  instant. A generated column that orders chronologically has to normalize to a
  constant width first; the Rust side must format bind values the same way.
- **An extra indexed predicate can defeat the ordering index.** SQLite happily
  drives a scan from a narrow equality index (`status IN (...)`) and then sorts
  every match in a temp b-tree, which throws away the early `LIMIT` termination
  that paging in SQL exists for. Writing the term `+status` strips its index
  affinity without changing its meaning, leaving the ordering index to drive the
  scan. Assert the plan with `EXPLAIN QUERY PLAN` in a test:
  `SCAN ... USING INDEX` with no `USE TEMP B-TREE FOR ORDER BY`.

## Pitfall documentation policy

When you encounter a non-obvious sqlx issue (or any tooling footgun), document
it here. Future developers and agents will hit the same problems -- a few lines
of documentation saves hours of debugging.
