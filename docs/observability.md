# Observability

Best practices for logging, tracing, and monitoring in this codebase.

## Tracing targets

Use the `target:` field in `tracing` macros to categorize log output by
subsystem. This enables per-subsystem filtering via `RUST_LOG` (e.g.,
`RUST_LOG=hedge=debug,wallet=trace`).

```rust
// Good — target scopes the log to a subsystem
info!(target: "hedge", %symbol, %shares, "Hedging trade");
trace!(target: "wallet", asset_count, "Listed wallet assets");

// Avoid — no target means the log uses the module path, which is
// an implementation detail and harder to filter on
info!(%symbol, %shares, "Hedging trade");
```

### Existing targets

| Target         | Subsystem                      |
| -------------- | ------------------------------ |
| `hedge`        | Hedging / position management  |
| `orderbook`    | Onchain orderbook interactions |
| `rebalancing`  | Portfolio rebalancing          |
| `startup`      | Application initialization     |
| `tokenization` | Tokenized equity minting       |
| `wallet`       | Alpaca wallet / onchain wallet |

When adding a new subsystem, pick a short, descriptive target name and add it to
this table.

## Sensitive data

Never log raw API response bodies, private keys, or full account balances. Log
non-sensitive metadata (counts, IDs, status codes) instead:

```rust
// Bad — leaks full wallet holdings
trace!(body = %text, "Wallet assets response body");

// Good — logs only the count
let assets = serde_json::from_str::<Vec<WalletAsset>>(&text)?;
trace!(target: "wallet", asset_count = assets.len(), "Listed wallet assets");
```
