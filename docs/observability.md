# Observability

Best practices for logging, tracing, and monitoring in this codebase.

## Sink levels

`log_level` is the minimum level for stdout and OpenTelemetry exports.
`RUST_LOG` may refine the stdout filter for an operator session. When local
daily files are enabled, `log_dir` and `file_log_level` must be configured
together; neither field has an implicit counterpart. The local file layer uses
only `file_log_level`, so `RUST_LOG` cannot increase disk usage.

Production uses `log_level = "trace"` so Docker's `gcplogs` driver exports full
diagnostics, while `file_log_level = "info"` limits the rotating files stored
beside SQLite. Rotation retains seven daily files, which caps file count but not
bytes. The lower file level reduces within-day growth and disk-exhaustion risk;
it does not enforce a per-file byte limit or filesystem quota.

The active GCP configs are config-as-data in `T0Trade/t0.devops`, not the baked
`config/*-gcp` copies in this repository. A logging-schema release must promote
the matching image and runtime config together. Staging's automatic image roll
must be paused before merging an incompatible schema change; production already
pins its image and config in one gated promotion.

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

| Target              | Subsystem                                                |
| ------------------- | -------------------------------------------------------- |
| `hedge`             | Hedging / position management                            |
| `operational_alert` | Operator alerts (ERROR events the log pipeline pages on) |
| `orderbook`         | Onchain orderbook interactions                           |
| `rebalancing`       | Portfolio rebalancing                                    |
| `startup`           | Application initialization                               |
| `tokenization`      | Tokenized equity minting                                 |
| `wallet`            | Alpaca wallet / onchain wallet                           |

When adding a new subsystem, pick a short, descriptive target name and add it to
this table AND to `DOMAIN_TARGETS` in `crates/config/src/telemetry.rs`, so the
default `EnvFilter` captures it.

When overriding filtering with `RUST_LOG`, always keep a bare level segment
(e.g. `RUST_LOG=warn,hedge=trace`): the bare level is what admits targets you
did not list, so the ERROR-severity `operational_alert` events keep flowing to
the pipeline that pages operators even while you focus on one subsystem.

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

## Registry reloads

`registry_applied_generation` identifies the persisted copy the process runs.
`registry_invalid` is one while latest content is refused or unusable, and
clears when it is applied, equivalent to running, or confirmed as running. It is
restored by observing latest content after a restart.

`registry_reloads_total{result}` counts `applied`, `unchanged`, `refused`,
`deferred`, `start_failed` and `fallback` outcomes.
`registry_last_reload_timestamp_seconds{result}` records the last outcome time,
restored from the manifest at boot. A refused generation is not rejudged;
deferred approval/RPC checks retry with backoff. Refusal, startup failure and
fallback emit error logs with generation, SHA-256 and reason.

`registry_carried_forward_symbols` counts removed rows retained as disabled.
`conductor_completion_only_symbols` counts symbols with services retained solely
to finish durable work. `registry_reload_held_seconds` exposes deployment hold
age; the watcher ignores holds older than fifteen minutes.
`registry_fetch_errors_total` counts transient token-file access failures.

Production remains pinned during the state-seeding rollout. Its watcher does not
apply updates until the separate unpin release and matching t0.devops gate
changes land. A refused token file does not alert: whoever publishes it checks
`registry_invalid` and the "registry candidate refused" log. Fallback, which
also covers startup failure, alerts through
`liquidity-token-file-fallback-production` in
`observability/alerting/liquidity.rules.yml`.
