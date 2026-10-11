# Observability

Best practices for logging, tracing, and monitoring in this codebase.

## Sink levels

`log_level` is the minimum level for ordinary stdout and OpenTelemetry log
records, and `file_log_level` is the minimum for ordinary local-file records.
`operations_audit` is the deliberate exception: console, OpenTelemetry log, and
configured local-file sinks all enforce an INFO floor for that target.
`RUST_LOG` may refine the stdout filter for an operator session, but the console
filter separately admits `operations_audit` through INFO before combining it
with the operator filter. Field-specific `RUST_LOG` directives therefore cannot
hide audit successes. When local daily files are enabled, `log_dir` and
`file_log_level` must be configured together; neither field has an implicit
counterpart. The local file layer uses only `file_log_level`, so `RUST_LOG`
cannot increase disk usage.

Production uses `log_level = "trace"` so Docker's `gcplogs` driver exports full
diagnostics, while `file_log_level = "info"` limits the rotating files stored
beside SQLite. Rotation retains seven daily files, which caps file count but not
bytes. The lower file level reduces within-day growth and disk-exhaustion risk;
it does not enforce a per-file byte limit or filesystem quota.

The active GCP runtime config sources are `config/prod/st0x-hedge.toml` and
`config/staging/st0x-hedge.toml` in this repository.
`.github/workflows/config-drift.yml` validates both against the current schema,
while `.github/workflows/build-oci.yml` validates and promotes the staging
config with the image. Production promotion runs only through a manual
`workflow_dispatch` of `.github/workflows/production-release.yml`: a version
selects that tag's images and config, while a blank version selects the master
config and preserves the live image tags.

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

### Configured domain targets

| Target              | Subsystem                                                |
| ------------------- | -------------------------------------------------------- |
| `api`               | HTTP API request handling                                |
| `backfill`          | Historical data recovery                                 |
| `bridge`            | Cross-chain bridge processing                            |
| `broker`            | Offchain broker integration                              |
| `cqrs`              | Aggregate commands and events                            |
| `dashboard`         | Dashboard data and updates                               |
| `equity`            | Equity position and transfer processing                  |
| `evm`               | EVM RPC and contract interactions                        |
| `gas`               | Wallet gas monitoring                                    |
| `hedge`             | Hedging and position management                          |
| `iap`               | IAP authentication and key verification                  |
| `inventory`         | Inventory accounting                                     |
| `market_data`       | Market data ingestion                                    |
| `operational_alert` | Operator alerts (ERROR events the log pipeline pages on) |
| `orderbook`         | Onchain orderbook interactions                           |
| `rebalance`         | Portfolio rebalancing                                    |
| `reliability`       | Reliability and health signals                           |
| `shutdown`          | Process shutdown                                         |
| `startup`           | Application initialization                               |
| `tokenization`      | Tokenized equity minting                                 |
| `wallet`            | Alpaca and onchain wallet operations                     |
| `operations_audit`  | Versioned mutation audit events (mandatory INFO floor)   |

This table mirrors `DOMAIN_TARGETS` in `crates/config/src/telemetry.rs`, plus
the deliberate `operations_audit` exception. Add a new configured domain target
to both places so the default `EnvFilter` captures it. The audit target instead
has dedicated console and default sink filters that enforce its INFO floor.

When overriding filtering with `RUST_LOG`, always keep a bare level segment
(e.g. `RUST_LOG=warn,hedge=trace`): the bare level is what admits targets you
did not list, so the ERROR-severity `operational_alert` events keep flowing to
the pipeline that pages operators even while you focus on one subsystem.

## Operations audit delivery

`operations_audit` carries the stable `st0x.operations.audit.v1` fields. The
production and staging runtime configs select JSON stdout, so Docker's `gcplogs`
driver forwards each record to Cloud Logging as one serialized JSON line.
Parsing that line into queryable Cloud Logging fields depends on the configured
ingestion pipeline. The schema is compatible with VictoriaLogs, but VictoriaLogs
receives events only when `[telemetry]` configures its OTLP exporter; the
deployed configs currently rely on stdout and do not invent an OTLP endpoint.

The recorder can synchronously detect that the tracing target is disabled. That
immediate refusal emits a dimensioned ERROR on `operational_alert` so it remains
visible without relying on the disabled audit target. Emitting a tracing event
does not acknowledge per-event delivery by stdout, Cloud Logging, or the
asynchronous OTLP exporter. Exporter failures are telemetry-pipeline health and
must be monitored as such; they are not event-correlated recorder failures.

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
