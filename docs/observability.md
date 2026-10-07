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

The active GCP configs are `config/staging/st0x-hedge.toml` and
`config/prod/st0x-hedge.toml` in this repository. Each release validates the
file inside the image that will run it and publishes it as a
`liquidity-runtime-config` version together with the image digests. A production
config-only release (empty `version`) rolls no image: it re-uses the digests
that are live. A logging-schema release must therefore ship the matching image
and runtime config together: a merge to master rolls staging with both from the
same commit (`build-oci.yml`), and production pins both in one gated
`production-release.yml` run.

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

## Metrics

`/metrics` is the `metrics` recorder's output followed by the `liq_*` contract
(see SPEC.md, "Prometheus metrics (`liq_*` contract)"). The contract lives in
`src/metrics/liquidity.rs`; its builders live in the submodules beside it.

Every `liq_*` block has a `# HELP` line and a `# TYPE <name> gauge` line (ADR
0026). The exporter sidecar writes no `# TYPE` lines, so its names are untyped.
Check a body with `promtool check metrics --lint=none --extended` (parse and
cardinality, no lint; `--lint=none` alone is refused): the default lint warns on
gauges whose names end in `_total` and on the `_ms` names, which keep the
exporter's names on purpose.

**Never use a `liq_` name in `metrics::counter!`, `gauge!`, `histogram!` or
their `describe_*` forms.** The recorder never forgets a label set, so a
departed symbol would keep its last value forever, and the name would get a
second writer. A test scans `src/` for this.

### Catalog

| Name                                    | Labels                                                                                                                     | Family     | Published                                                                                          |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ---------- | -------------------------------------------------------------------------------------------------- |
| `liq_bot_info`                          | `git_commit`                                                                                                               | `health`   | `1`; first 12 characters of the build commit (`dev` in local builds)                               |
| `liq_bot_start_timestamp_seconds`       |                                                                                                                            | `health`   | Unix time the process started                                                                      |
| `liq_settings_info`                     | `broker`, `log_level`, `orderbook`, `server_port`, `trading_mode`, `turnkey_organization`, `wallet_address`, `wallet_kind` | `settings` | `1`; a missing wallet or organization gives `""`, port 0 gives `""`, `trading_mode` is always `""` |
| `liq_settings_equity_target`            |                                                                                                                            | `settings` | primary chain default target share; absent with only per-symbol targets                            |
| `liq_settings_equity_deviation`         |                                                                                                                            | `settings` | always                                                                                             |
| `liq_settings_usdc_target`              |                                                                                                                            | `settings` | the one active corridor's target (the primary chain's with several); absent without one            |
| `liq_settings_usdc_deviation`           |                                                                                                                            | `settings` | as above                                                                                           |
| `liq_settings_cash_reserved`            |                                                                                                                            | `settings` | absent when not configured                                                                         |
| `liq_settings_execution_threshold_usd`  |                                                                                                                            | `settings` | dollar threshold; absent for a share-count threshold                                               |
| `liq_settings_order_polling_seconds`    |                                                                                                                            | `settings` | always                                                                                             |
| `liq_settings_inventory_poll_seconds`   |                                                                                                                            | `settings` | always                                                                                             |
| `liq_settings_deployment_block`         |                                                                                                                            | `settings` | always                                                                                             |
| `liq_asset_counter_trading`             | `symbol`                                                                                                                   | `settings` | `1` or `0`, every symbol the primary chain lists                                                   |
| `liq_asset_extended_hours`              | `symbol`                                                                                                                   | `settings` | `1` or `0`; absent while counter trading is disabled                                               |
| `liq_asset_rebalancing`                 | `symbol`                                                                                                                   | `settings` | `1` when the symbol starts new rebalancing operations                                              |
| `liq_collector_last_success_ts_seconds` | `collector`                                                                                                                | store      | Unix time each family was last published                                                           |

`symbol` labels drop a leading `wt` or `t` only before an uppercase letter
(`tAAPL` and `wtAAPL` give `AAPL`; `tsla` stays).

### Adding a family

1. Spec the names in SPEC.md first. A new meaning gets a new name; an existing
   name never changes meaning.
2. Add the `LiqMetric` variants (name, help, sorted label keys, owning family)
   and, for a new source, a `LiqFamily` variant with its `collector` string.
   Extend the catalog test and this table.
3. Write the builder in its own submodule. It takes a metrics-owned input type,
   not a dashboard DTO, computes in exact types, and converts once with
   `float_value` or `integer_value`. A value that does not convert is skipped
   and logged.
4. Publish with `LIQ_FAMILIES.replace(family, samples, now)`. Each call replaces
   the whole family.
5. Add a golden case: a fixture JSON under `src/metrics/liquidity/testdata/`, a
   case in `scripts/liq-parity/golden.py`, and the expected `.prom` written by
   running `python3 -I scripts/liq-parity/golden.py <exporter.py>` against a
   local checkout of the exporter. The goldens have no `# TYPE` lines; the test
   compares samples only. Extend `PORTED` and, where needed, `KNOWN_DIFFS` in
   `scripts/liq-parity/compare.py`.

### Board pins

Until consumers move to the bot, every `liq_` selector on the liquidity boards
reads `job="t0-liquidity-exporter"`. `observability/gen-t0-liquidity.py` adds
the matcher and fails if any `liq_` selector is left unpinned.
`python3 observability/gen-t0-liquidity.py --check` (run in CI) fails when the
committed board JSON differs from the generator output.

### Checking parity on a live environment

Run this after each release that ports names, while the exporter sidecar still
runs next to the bot. Run it from a local checkout of this repository: the VMs
have no checkout, and both endpoints listen only on the VM itself, so each
snapshot is read over the IAP SSH access described in `docs/cli-ops.md`.

1. Check out the commit the VM runs. `compare.py`'s list of ported names must
   match the deployed release: a newer checkout reports names the release does
   not publish yet as missing. The commit is the `git_commit` label of
   `liq_bot_info` in the bot's `/metrics`.
2. Set `VM` and `PROJECT` to the staging or production values from
   `docs/cli-ops.md`, then run the whole block from the repository root. It
   takes three snapshot pairs about two minutes apart into a new directory, and
   compares them only after all three succeed. Each pair is one SSH command that
   reads both bodies back to back:

   ```bash
   (
     set -euo pipefail
     dir=$(mktemp -d)
     echo "snapshots in $dir"
     for i in 1 2 3; do
       gcloud compute ssh "$VM" --project "$PROJECT" --zone europe-west3-b \
         --tunnel-through-iap --command \
         'curl -fsS --max-time 10 localhost:8001/metrics &&
          echo "# ---- exporter ----" &&
          curl -fsS --max-time 10 localhost:9101/metrics' > "$dir/pair-$i.prom"
       sed '/^# ---- exporter ----$/,$d' "$dir/pair-$i.prom" > "$dir/bot-$i.prom"
       sed '1,/^# ---- exporter ----$/d' "$dir/pair-$i.prom" > "$dir/exporter-$i.prom"
       if [ "$i" -lt 3 ]; then sleep 120; fi
     done
     status=0
     python3 -I scripts/liq-parity/compare.py --drop-list --known-diffs \
       "$dir/bot-1.prom" "$dir/exporter-1.prom" \
       "$dir/bot-2.prom" "$dir/exporter-2.prom" \
       "$dir/bot-3.prom" "$dir/exporter-3.prom" || status=$?
     echo "compare.py exit status: $status"
   )
   ```

   `curl -f` fails on an HTTP error and `--max-time` fails a request that
   stalls. The `&&` chain makes the SSH command fail with it, so a dead or hung
   endpoint stops the block before the comparison. A fresh directory per run
   means a failed run can never compare the files of an earlier one.
3. If the block ends without a `compare.py exit status:` line, the capture
   failed (expired `gcloud` credentials, wrong VM or project, missing IAP
   access, or a dead endpoint); fix that and run it again. Otherwise read that
   status. `0` means no series disagreed in all three pairs. `1` lists the
   series that disagree in every pair: each one is a bug in the port or a
   difference missing from `KNOWN_DIFFS`. `compare.py` also compares the
   `# TYPE` of each name both sides publish. The exporter writes no `# TYPE`
   lines, so the bot's `gauge` against the exporter's untyped is expected for
   every `liq_*` name, and `--known-diffs` accepts it. Any other `type differs:`
   line (for example a bot `counter`) is a bug in the port. `2` means a snapshot
   is unusable, and `compare.py` prints the file and the reason. For
   `no ported liq_* series`, the wrong endpoint was read or a target was
   degraded; check the endpoint and run it again. For any other reason
   (`not UTF-8 text`, `not a Prometheus text body`, `repeated series`,
   `repeated label name`, or `repeated TYPE`) in a `bot-N.prom` file, report it
   as a bug in the port, because a rerun gives the same result.

The block only reads, so it is safe to rerun.
