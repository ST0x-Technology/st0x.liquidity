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

## Console format

`log_format` selects the stdout shape. `text` is the human-readable line. `json`
writes one flattened JSON object per line:

```json
{"timestamp":"...","level":"ERROR","target":"operational_alert","message":"Low gas: ...","alert":true,"kind":"Low gas","span":{...},"spans":[...]}
```

The message and every event field are top-level keys, so once a log shipper
parses the line the text is at `message` (where the `operational_alerts`
extractors read it) and each field is under its own name. Do not give an event a
field named `message`, `timestamp`, `level`, `target`, `span` or `spans`: it
would collide with those keys. A test in `crates/config/src/telemetry.rs`
(`no_tracing_event_records_a_reserved_console_key`) scans every `trace!`,
`debug!`, `info!`, `warn!`, `error!` and `event!` call under `src/` and
`crates/`, in its `(`, `[` and `{` forms, for these names. It does not see
fields that another macro passes through, such as OpenTelemetry's `otel_warn!`.
This rule covers the dependencies too: `st0x-alpaca` v0.2.1 logs
`Tokenization
request failed` with a `message` field that holds the Alpaca error
body. ST0x-Technology/st0x.alpaca#11 renames it to `error_body`.

Where `[telemetry]` is set (e2e, local), OpenTelemetry's own internal logs
(targets starting with `opentelemetry`, for example
`BatchLogProcessor.LogsDropped`) record `message` as a field next to an empty
format string, so the console writes them as their own flat line with one
`message`, the real text, and no span context, and the rolling file writes them
with one `fields.message`. An export error records no `message`, so its line's
`message` is its name and its error. Only the SDK objects that
`TelemetryCtx::setup` builds write these logs, and the server calls it only when
the config has a `[telemetry]` section. `config/staging/st0x-hedge.toml` and
`config/prod/st0x-hedge.toml` have none, so no `opentelemetry*` line reaches the
staging or production console or rolling files, and this handling does not
change them. The duplicate-`message` handling is a workaround for opentelemetry
before 0.32: 0.32 (open-telemetry/opentelemetry-rust#3317) stops adding the
empty format string, so after that bump only the `<name>: <error>` fallback for
export errors is still needed.

The rolling file layer is not flattened. It keeps the message under `fields`:
the dashboard's log panel (`dashboard/src/lib/components/log-panel.svelte`) and
the t0.devops liquidity exporter (`ship_botlogs`, which feeds the
`liquidity-botlogs` log) read `fields` and `fields.message` from `/logs`, which
serves these files.

Switch an environment to `json` only where the log shipper parses JSON, staging
before production, and only on a build whose `st0x-alpaca` has that rename
(st0x.alpaca#11 released and bumped here). Before it, each failed tokenization
request writes two `message` keys on one line. Unflattened JSON (any build
before the flattening) puts the text at `jsonPayload.fields.message`, where no
extractor looks. Rollback is `log_format = "text"`.

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
| `liq_event`         | One line per committed trade or transfer event           |
| `liq_trade`         | One line per trade at a terminal status                  |
| `liq_transfer`      | One line per transfer status change                      |
| `operational_alert` | Operator alerts (ERROR events the log pipeline pages on) |
| `orderbook`         | Onchain orderbook interactions                           |
| `rebalancing`       | Portfolio rebalancing                                    |
| `startup`           | Application initialization                               |
| `tokenization`      | Tokenized equity minting                                 |
| `wallet`            | Alpaca wallet / onchain wallet                           |

### Trade, transfer and event lines

The dashboard `Broadcaster` reactor writes the `liq_trade` and `liq_event` lines
at INFO from `Reactor::react_committed`, and writes a `liq_transfer` line there
when an event changes a transfer's status (`src/dashboard/event_lines.rs`).
`TransferLineSweep` also writes `liq_transfer` lines from the transfer
projections, as described below. The store calls `react_committed` once per
commit and never when it replays events, so a replay does not write old lines
again. Never write these lines from `evolve()`: it also runs on every replay.

Every line has an `event_id`, `<aggregate type>:<aggregate id>:<sequence>`, that
names the committed event behind it. The sequence comes from the event store, so
the id stays the same across restarts. One commit can write two lines with the
same `event_id` (a `liq_event` and a `liq_trade` or `liq_transfer`), so a
consumer drops a duplicate line by target and `event_id` together. A row is not
a line, though: a trade can get a second `liq_trade` line (a venue correction)
and a transfer can repeat its status from the sweep below, each under another
`event_id`. So a table shows one row per trade `id`, or per transfer `kind` and
`id`, from its latest line. Each line always has the same fields; a value that
is not known is an empty string.

The reactor writes each line at most once. A crash between the commit and the
reactor, or a failed reload of the entity in the reactor (logged at WARN on
target `dashboard` with the `event_id`), loses it. A command sent through a
store without the `Broadcaster` writes no lines. That covers `send_command` and
every operator path that builds its own store: the CLI's manual transfers,
transfer failures and reconciles, `clear-pending-burn`, and the hedge release
that fails an offchain order. Their events still reach the `events` table and
the event endpoints, but not these logs. `TransferLineSweep` heals only the
`liq_transfer` lines; the `liq_trade` and `liq_event` lines of these paths stay
missing.

A sweep heals the `liq_transfer` lines (`TransferLineSweep`). A minute after
start, so the price subscription has sent its marks, and then every 60 seconds,
it reads every transfer in progress and every transfer that ended in the last 24
hours. It writes the line of a status this process has not written, under the
`event_id` of the last event the transfer's projection applied. A read older
than the last event the reactor saw writes nothing, so a row never goes back to
an earlier status, and a reactor event older than the last one the sweep read
writes nothing either. So a lost line, or a status an operator path changed,
appears within a minute, and a consumer drops the line as a duplicate when that
last event's own line was already written. The sweep reads the projections, so
it sees an operator change only when that change runs through a store with the
projection. Every production command on a transfer does, the equity reconciles
of the API and the CLI included; a bare `send_command` would not be seen. A
swept equity line values the transfer at the mark when the sweep runs, not when
the event committed. When an equity transfer's symbol has no live mark, the
sweep skips the transfer only while this process has written no line for it and
the transfer started before this process did (the first pass after a restart,
when an earlier process can have written its line), and tries again on the next
pass. Any other line is written at once with an empty `usd`, as the reactor
does: a status that differs from the one written, and the first line of a
transfer that started after this process (a mint or redemption an operator ran
from the offline CLI, say). A mark can stay away longer than the 24 hours the
sweep keeps an ended transfer (no quote over a weekend, or a symbol with no
price), so waiting would lose the line. A transfer from before the restart with
no line whose mark stays away until it leaves that window gets no boot line. The
status memory is in-process, so after a restart the first pass writes the status
of every transfer it reads once, and seeds the memory so later events do not
repeat an unchanged status. When a transfer's last event did not change its
status (a `RecordPendingBurn` after `BridgingSubmitting`, for example), that
boot line has a new `event_id` and repeats the status the row already shows. A
projection row the sweep cannot read is logged at WARN once per version, then at
DEBUG.

The sweep takes a projection row's `version` as the sequence of the last event
the projection applied. Each update the projection applies raises `version` by
one, so the two agree only while the projection loses no update. A lost update
leaves every later version of that transfer below its true sequence, and a
healed line then carries the `event_id` of an earlier event, which a consumer
that already holds that `event_id` drops as a duplicate.

The sweep does not cover `liq_trade` or `liq_event`: a trade line a crash lost
stays lost, and so does the `liq_trade` line of a hedge release.

| Target         | Fields after `event_id`                                                                                           |
| -------------- | ----------------------------------------------------------------------------------------------------------------- |
| `liq_trade`    | `id`, `occurred_at`, `venue`, `direction`, `symbol`, `shares`, `status`, `error`, `filled_shares`, `price`, `usd` |
| `liq_transfer` | `kind`, `id`, `symbol`, `direction`, `amount`, `status`, `started_at`, `usd`                                      |
| `liq_event`    | `parent`, `venue`, `kind`, `id`, `sequence`, `step`, `payload`                                                    |

- `liq_trade`: `occurred_at` is when the trade filled or ended, which can be
  well before the line when the bot catches up on onchain fills. `usd` is
  `shares * price`. `price` and `usd` are empty for a failed or cancelled trade.
  `shares` is the requested quantity for a failed or cancelled counter-trade,
  and `filled_shares` is what it filled before it ended (empty for a fill and
  when the broker did not report it). An onchain trade's `shares` and `price`
  are in wrapped shares, as Raindex settles them; a counter-trade's are in
  underlying shares. A broker fill price that is not positive is logged once at
  ERROR by the offchain order when it records the fill (with symbol, broker
  order id and price), and the trade is written without `price` and `usd`. A
  venue correction of an onchain trade (`SourceAttributed`) writes the trade
  again with its new venue.
- `liq_transfer`: written only when the status differs from the last one this
  process wrote for the transfer, terminal statuses included, by the reactor or
  the sweep. `usd` values a USDC bridge at its amount and an equity transfer at
  its symbol's mark: the reactor's line uses the mark when it writes, and a
  swept line uses the mark when the sweep runs. Either is empty with no live
  mark, except that the sweep writes no first line without one for a transfer
  that started before this process.
- `liq_event`: `parent` is `trade` (with `venue`) or `transfer` (with `kind`). A
  trade's `venue` is its venue when the event commits, so an onchain trade's
  events from before a `SourceAttributed` correction keep the old venue. Join a
  trade's events on `parent` and `id` only, and a transfer's on `parent`, `kind`
  and `id`. `step` is the event's variant name and `payload` is the variant's
  fields as a JSON string, with every `signature` and `raw` value replaced by
  `"redacted"`, in the same shape `/liquidity-read/trades/{venue}/{id}/events`
  and `/liquidity-read/transfers/{kind}/{id}/events` return. `Position` events
  get no line: they belong to no trade or transfer.

### Operational alert kinds

Every `operational_alert` line carries a `kind` field, an `AlertKind` from
`src/alerts/mod.rs`. Send alerts through `Notifier::notify(kind, message)`. A
direct `error!(target: "operational_alert", ...)` line must also log
`kind = AlertKind::Variant.as_str()` over a plain string literal message. A test
scans every direct line: it fails on a line without a kind, and it checks the
kind against the message literal with the same rules `CapturingNotifier` uses
(below).

- An existing kind's string is exactly the label the `operational_alerts`
  extractor (t0.devops `observability-consumer`) gives the line once that
  extractor carries the full phrase list of the unclassified rule in
  `observability/alerting/liquidity.rules.yml`; a test pins the extracted list
  against that rule. Compare the field with the extractor's label on staging
  only after the deployed extractor has the full list.
- A new alert gets a new kind in the `not_extracted` group, whose string is a
  fixed phrase of its message. Teach the extractor that phrase and add a rule;
  then move the kind into the `extracted` group in the extractor's order.
- Every kind's string must appear in its message. `CapturingNotifier` checks
  every alert a test drives: an extracted kind must be what the extractor reads
  from the message, and the message of a not-extracted kind must hold no
  extracted phrase.
- A job's terminal failure page (a dead letter or a supervised worker's terminal
  failure) takes its kind from its text: the most specific extracted phrase in
  the rendered alert, else `JobFailedAfterRetries`. The job's
  `TERMINAL_FAILURE_MSG` decides the kind when the embedded error names nothing
  more specific, so an override must carry the phrase of its own extracted kind.
  `build_supervised_worker!` and `build_best_effort_worker!` fail the build on a
  message with no extracted phrase, and a test checks every
  `TERMINAL_FAILURE_MSG` in the source.

The phrase extractor is transitional. Once the `operational_alerts` pager reads
`jsonPayload.kind`, a new alert needs only a new `AlertKind` and a rule in
`observability/alerting/liquidity.rules.yml`, and the extracted/not_extracted
split goes. `AlertKind::most_specific_in` stays until the wrapper alerts (job
dead letters, worker terminal failures) take their kind from the error type
instead of its rendered text.

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

### Native performance metrics

These typed recorder metrics count at the source, so a later board can use
`increase()` and `histogram_quantile()` instead of the 24-hour `liq_*`
snapshots. Duration histograms render with fixed buckets (`_bucket{le}` lines),
not as summaries; `metrics_refresh_duration_seconds` adds buckets from 90 to 300
seconds, because a P&L refresh can take longer than a minute. A supervised
upkeep task drains the histogram buffers every 5 seconds, so they stay bounded
when nothing scrapes `/metrics`. Each fill watcher registers its
`order_fill_poll_cycles_total` outcomes and
`order_fill_poll_skipped_ticks_total` at 0 when it starts, and the telemetry
channel does the same for `telemetry_samples_dropped_total`, so `increase()`
sees the first error, pause or drop.

`order_fill_poll_cycles_total{outcome}` is `ok`, `error`, or `paused`. A
`paused` cycle succeeds but ingests nothing, because the cutoff block is unknown
or behind the checkpoint while a checkpoint exists. An unknown lag keeps
`order_fill_block_lag_blocks` at its last value, so
`time() - order_fill_block_lag_sampled_timestamp_seconds` shows how old that
value is. A cutoff block behind the checkpoint is a known lag of 0 with a fresh
sample time, so only `outcome="paused"` shows that stall.

| Name                                             | Type      | Labels                               | Recorded                                                 |
| ------------------------------------------------ | --------- | ------------------------------------ | -------------------------------------------------------- |
| `dependency_calls_total`                         | counter   | `dependency`, `operation`, `outcome` | every RPC and broker call, before the telemetry channel  |
| `dependency_call_duration_seconds`               | histogram | `dependency`, `operation`            | same                                                     |
| `telemetry_samples_dropped_total`                | counter   |                                      | a dependency sample the full or closed channel dropped   |
| `order_fill_poll_cycles_total`                   | counter   | `chain`, `outcome`                   | each order-fill poll cycle                               |
| `order_fill_poll_skipped_ticks_total`            | counter   | `chain`                              | poll ticks dropped because the previous cycle overran    |
| `order_fill_poll_duration_seconds`               | histogram | `chain`                              | each order-fill poll cycle                               |
| `order_fill_block_lag_blocks`                    | gauge     | `chain`                              | each poll that knows the cutoff block and the checkpoint |
| `order_fill_block_lag_sampled_timestamp_seconds` | gauge     | `chain`                              | same, set to the poll's sample time                      |
| `metrics_refresh_duration_seconds`               | histogram | `collector`                          | each prices/performance/orders/PnL run (PnL: see below)  |
| `log_events_total`                               | counter   | `level`, `target`                    | each error and warning the file log writes               |

### Catalog

| Name                                    | Labels                                                                                                                     | Family           | Published                                                                                                                      |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `liq_bot_info`                          | `git_commit`                                                                                                               | `health`         | `1`; first 12 characters of the build commit (`dev` in local builds)                                                           |
| `liq_bot_start_timestamp_seconds`       |                                                                                                                            | `health`         | Unix time the process started                                                                                                  |
| `liq_settings_info`                     | `broker`, `log_level`, `orderbook`, `server_port`, `trading_mode`, `turnkey_organization`, `wallet_address`, `wallet_kind` | `settings`       | `1`; a missing wallet or organization gives `""`, port 0 gives `""`, `trading_mode` is always `""`                             |
| `liq_settings_equity_target`            |                                                                                                                            | `settings`       | primary chain default target share; absent with only per-symbol targets                                                        |
| `liq_settings_equity_deviation`         |                                                                                                                            | `settings`       | always                                                                                                                         |
| `liq_settings_usdc_target`              |                                                                                                                            | `settings`       | the target of the one corridor the USDC trigger can act on (the primary chain's with several); absent without one              |
| `liq_settings_usdc_deviation`           |                                                                                                                            | `settings`       | as above                                                                                                                       |
| `liq_settings_cash_reserved`            |                                                                                                                            | `settings`       | absent when not configured                                                                                                     |
| `liq_settings_execution_threshold_usd`  |                                                                                                                            | `settings`       | dollar threshold; absent for a share-count threshold                                                                           |
| `liq_settings_order_polling_seconds`    |                                                                                                                            | `settings`       | always                                                                                                                         |
| `liq_settings_inventory_poll_seconds`   |                                                                                                                            | `settings`       | always                                                                                                                         |
| `liq_settings_deployment_block`         |                                                                                                                            | `settings`       | always                                                                                                                         |
| `liq_asset_counter_trading`             | `symbol`                                                                                                                   | `settings`       | `1` or `0`, every symbol the primary chain lists                                                                               |
| `liq_asset_extended_hours`              | `symbol`                                                                                                                   | `settings`       | `1` or `0`; absent while counter trading is disabled                                                                           |
| `liq_asset_rebalancing`                 | `symbol`                                                                                                                   | `settings`       | `1` when the symbol starts new rebalancing operations                                                                          |
| `liq_usdc_corridor_target`              | `chain`                                                                                                                    | `settings`       | every configured USDC corridor, whatever the USDC mode                                                                         |
| `liq_usdc_corridor_deviation`           | `chain`                                                                                                                    | `settings`       | as above                                                                                                                       |
| `liq_usdc_corridor_active`              | `chain`                                                                                                                    | `settings`       | 1 while the USDC trigger can act on that corridor (USDC mode and that chain's cash rebalancing enabled), else 0                |
| `liq_equity_onchain_available`          | `symbol`                                                                                                                   | `inventory`      | primary chain vault                                                                                                            |
| `liq_equity_offchain_available`         | `symbol`                                                                                                                   | `inventory`      | broker                                                                                                                         |
| `liq_equity_inflight_total`             | `symbol`                                                                                                                   | `inventory`      | primary chain vault plus broker in flight                                                                                      |
| `liq_equity_total`                      | `symbol`                                                                                                                   | `inventory`      | onchain plus offchain plus in flight; excludes wallet tokens and other chains                                                  |
| `liq_equity_unwrapped`                  | `symbol`                                                                                                                   | `inventory`      | Base wallet unwrapped tokens                                                                                                   |
| `liq_equity_wrapped`                    | `symbol`                                                                                                                   | `inventory`      | Base wallet wrapped tokens                                                                                                     |
| `liq_equity_ratio`                      | `symbol`                                                                                                                   | `inventory`      | onchain over onchain plus offchain; `0` when both are 0                                                                        |
| `liq_equity_chain_available`            | `chain`, `symbol`                                                                                                          | `inventory`      | each hedged chain vault a snapshot read, in its wrapped shares: never sum across chains or with broker                         |
| `liq_equity_chain_inflight`             | `chain`, `symbol`                                                                                                          | `inventory`      | equity in flight from each hedged chain vault a snapshot read: wrapped shares, then underlying once a provider poll lists it   |
| `liq_equity_chain_share`                | `chain`, `symbol`                                                                                                          | `equity_bands`   | the planner's share of the symbol on each chain, in underlying shares, when the trigger plans it and while its transfer runs   |
| `liq_equity_chain_verdict`              | `chain`, `symbol`                                                                                                          | `equity_bands`   | -1 below the chain's own band, 0 within, 1 above; no series for a paused chain or one without a target                         |
| `liq_usdc_onchain_available`            |                                                                                                                            | `inventory`      | primary chain vault                                                                                                            |
| `liq_usdc_onchain_inflight`             |                                                                                                                            | `inventory`      | primary chain vault in flight                                                                                                  |
| `liq_usdc_offchain_available`           |                                                                                                                            | `inventory`      | broker cash after the reserve                                                                                                  |
| `liq_usdc_offchain_gross`               |                                                                                                                            | `inventory`      | broker cash before the reserve; absent until read                                                                              |
| `liq_usdc_offchain_inflight`            |                                                                                                                            | `inventory`      | broker cash in flight                                                                                                          |
| `liq_usdc_alpaca_usdc`                  |                                                                                                                            | `inventory`      | USDC token in the broker account; absent until read                                                                            |
| `liq_usdc_alpaca_total`                 |                                                                                                                            | `inventory`      | gross broker cash, or available broker cash until the gross is read                                                            |
| `liq_usdc_inflight_total`               |                                                                                                                            | `inventory`      | vault plus broker in flight                                                                                                    |
| `liq_usdc_inflight_ethereum_wallet`     |                                                                                                                            | `inventory`      | absent until read                                                                                                              |
| `liq_usdc_inflight_base_wallet`         |                                                                                                                            | `inventory`      | absent until read                                                                                                              |
| `liq_usdc_total`                        |                                                                                                                            | `inventory`      | vault plus broker total plus in flight                                                                                         |
| `liq_usdc_ratio`                        |                                                                                                                            | `inventory`      | vault over vault plus broker total; `0` when both are 0                                                                        |
| `liq_usdc_rebalanceable`                |                                                                                                                            | `inventory`      | withdrawable minus the reserve, never below 0; absent until withdrawable is read                                               |
| `liq_usdc_chain_available`              | `chain`                                                                                                                    | `inventory`      | each hedged chain vault a snapshot has read                                                                                    |
| `liq_usdc_chain_inflight`               | `chain`                                                                                                                    | `inventory`      | as above                                                                                                                       |
| `liq_usdc_chain_ratio`                  | `chain`                                                                                                                    | `inventory`      | vault available over itself plus gross broker cash; absent until the gross is read or both are 0                               |
| `liq_position_last_price_usd`           | `symbol`                                                                                                                   | `prices`         | live price of each symbol with a position; refreshed every 60 s                                                                |
| `liq_equity_exposure_usd`               | `symbol`                                                                                                                   | `prices`         | net position times that price                                                                                                  |
| `liq_hedge_latency_ms`                  | `quantile`, `stage`                                                                                                        | `latencies`      | 24 h nearest-rank `p50`, `p90`, `p95`, `p99`, `max`; absent for a stage without samples                                        |
| `liq_hedge_latency_ms_samples`          | `stage`                                                                                                                    | `latencies`      | samples behind each stage                                                                                                      |
| `liq_open_exposure_fill_count`          | `symbol`                                                                                                                   | `latencies`      | fills after the symbol's latest hedge placement                                                                                |
| `liq_open_exposure_oldest_ts_seconds`   | `symbol`                                                                                                                   | `latencies`      | block time of the oldest unhedged fill                                                                                         |
| `liq_reliability_log_count_24h`         | `level`                                                                                                                    | `reliability`    | errors and warnings in 24 h; both rows once seeded after a start, 0 without file logging                                       |
| `liq_log_target_count_24h`              | `level`, `target`                                                                                                          | `reliability`    | per target with events in 24 h; one-minute buckets, no entry cap                                                               |
| `liq_failure_event_count_24h`           | `event_type`                                                                                                               | `reliability`    | lifecycle failure events in 24 h                                                                                               |
| `liq_job_queue`                         | `job_type`, `state`                                                                                                        | `reliability`    | queue counts now, not windowed                                                                                                 |
| `liq_block_lag_blocks`                  | `chain`                                                                                                                    | `infra`          | latest sampled lag of each hedged chain; absent until known                                                                    |
| `liq_block_lag_sampled_ts_seconds`      | `chain`                                                                                                                    | `infra`          | time of that sample; present without the lag while ingestion is paused                                                         |
| `liq_poll_cycles_24h`                   | `chain`                                                                                                                    | `infra`          | order-fill poll cycles in 24 h                                                                                                 |
| `liq_poll_errors_24h`                   | `chain`                                                                                                                    | `infra`          | failed cycles in 24 h                                                                                                          |
| `liq_poll_skipped_ticks_24h`            | `chain`                                                                                                                    | `infra`          | dropped poll ticks in 24 h                                                                                                     |
| `liq_poll_duration_ms`                  | `chain`, `quantile`                                                                                                        | `infra`          | 24 h poll duration; absent with no cycles                                                                                      |
| `liq_dependency_calls_24h`              | `dependency`, `operation`                                                                                                  | `infra`          | external calls in 24 h                                                                                                         |
| `liq_dependency_errors_24h`             | `dependency`, `operation`                                                                                                  | `infra`          | failed calls in 24 h                                                                                                           |
| `liq_dependency_latency_ms`             | `dependency`, `operation`, `quantile`                                                                                      | `infra`          | 24 h call latency                                                                                                              |
| `liq_rebalance_stage_ms`                | `kind`, `quantile`, `stage`                                                                                                | `rebalances`     | 30 d completed stage duration; refreshed every 5 min                                                                           |
| `liq_attestation_last_ms`               | `kind`                                                                                                                     | `rebalances`     | latest CCTP attestation in 30 d; absent with none                                                                              |
| `liq_pnl_summary_usd`                   | `stream`, `window`                                                                                                         | `pnl_<window>`   | the 11 summary streams plus `inventory_drift`, `onchain_notional`, `offchain_notional`; absent when the decimal does not parse |
| `liq_pnl_summary_shares`                | `kind`, `window`                                                                                                           | `pnl_<window>`   | `matched`, `inventory_drift`, `open_long`, `open_short`, `unmatched_offchain`                                                  |
| `liq_pnl_summary_count`                 | `kind`, `window`                                                                                                           | `pnl_<window>`   | `onchain_fill`, `offchain_fill`, `matched_lot`, `open_lot`, `unmatched_offchain_fill`                                          |
| `liq_pnl_cost_usd`                      | `category`, `window`                                                                                                       | `pnl_<window>`   | tracked costs by category                                                                                                      |
| `liq_pnl_revenue_usd`                   | `category`, `window`                                                                                                       | `pnl_<window>`   | `dividend_income`                                                                                                              |
| `liq_pnl_cost_entries`                  | `window`                                                                                                                   | `pnl_<window>`   | cost entries                                                                                                                   |
| `liq_pnl_cost_missing_observations`     | `window`                                                                                                                   | `pnl_<window>`   | missing cost observations                                                                                                      |
| `liq_pnl_cost_coverage`                 | `source`, `status`, `window`                                                                                               | `pnl_<window>`   | `1` per cost source                                                                                                            |
| `liq_pnl_capital_avg_deployed_usd`      | `window`                                                                                                                   | `pnl_<window>`   | absent when not computed                                                                                                       |
| `liq_pnl_capital_annualized_return_pct` | `window`                                                                                                                   | `pnl_<window>`   | absent when not computed                                                                                                       |
| `liq_pnl_capital_coverage_days`         | `window`                                                                                                                   | `pnl_<window>`   | absent when not computed                                                                                                       |
| `liq_pnl_capital_sample_days`           | `window`                                                                                                                   | `pnl_<window>`   | always                                                                                                                         |
| `liq_pnl_symbol_usd`                    | `col`, `symbol`, `window`                                                                                                  | `pnl_<window>`   | the 11 streams plus `inventory_drift`, per symbol                                                                              |
| `liq_pnl_symbol_shares`                 | `kind`, `symbol`, `window`                                                                                                 | `pnl_<window>`   | `matched`, `inventory_drift`, `open_long`, `open_short`                                                                        |
| `liq_pnl_symbol_lots`                   | `symbol`, `window`                                                                                                         | `pnl_<window>`   | matched lots                                                                                                                   |
| `liq_pnl_symbol_volume_shares`          | `symbol`, `window`                                                                                                         | `pnl_<window>`   | matched shares times 2 (one per leg); absent when they do not parse                                                            |
| `liq_pnl_sample_total_fills`            | `window`                                                                                                                   | `pnl_<window>`   | fills                                                                                                                          |
| `liq_pnl_sample_symbols`                | `window`                                                                                                                   | `pnl_<window>`   | symbols with fills                                                                                                             |
| `liq_pnl_sample_first_ts_seconds`       | `window`                                                                                                                   | `pnl_<window>`   | first fill time; absent without fills                                                                                          |
| `liq_pnl_sample_last_ts_seconds`        | `window`                                                                                                                   | `pnl_<window>`   | last fill time; absent without fills                                                                                           |
| `liq_pnl_warnings`                      | `window`                                                                                                                   | `pnl_<window>`   | warnings in the report                                                                                                         |
| `liq_pnl_day_usd`                       | `day`, `symbol`, `window`                                                                                                  | `pnl_<window>`   | each day bucket's total per symbol; last 90 buckets; absent when its total does not parse                                      |
| `liq_pnl_day_cum_usd`                   | `day`, `symbol`, `window`                                                                                                  | `pnl_<window>`   | running total over every bucket, every symbol seen so far                                                                      |
| `liq_pnl_day_stream_usd`                | `day`, `stream`, `window`                                                                                                  | `pnl_<window>`   | each day bucket's 4 chart streams; absent, with every later running total, when a component does not parse                     |
| `liq_pnl_day_cum_stream_usd`            | `day`, `stream`, `window`                                                                                                  | `pnl_<window>`   | running total per chart stream                                                                                                 |
| `liq_pending_orders`                    | `status`                                                                                                                   | `pending_orders` | count per status among the newest 100 live broker order rows, without unparseable ones; absent at 0; every 60 s                |
| `liq_pending_orders_total`              |                                                                                                                            | `pending_orders` | the newest 100 live broker order rows, without unparseable ones (they still take a slot); kept when the read fails             |
| `liq_pending_orders_uncapped_total`     |                                                                                                                            | `pending_orders` | every live broker order row, uncapped and unparsed; absent when the count fails                                                |
| `liq_raindex_orders_total`              |                                                                                                                            | `raindex`        | `pagination.totalOrders` of the st0x REST API (missing reads as 0, a numeric string as its number); absent while unavailable   |
| `liq_raindex_orders_unavailable`        | `reason`                                                                                                                   | `raindex`        | `1` with the reason while unavailable, else `0` with `reason=""`                                                               |
| `liq_collector_last_success_ts_seconds` | `collector`                                                                                                                | store            | Unix time each family was last published                                                                                       |

`symbol` labels drop a leading `wt` or `t` only before an uppercase letter
(`tAAPL` and `wtAAPL` give `AAPL`; `tsla` stays).

The `liq_pnl_*` rows are published per `window` (`1d`, `1w`, `1m`, `ytd`, `1y`,
`all`) every 5 minutes. A window whose report fails keeps its last samples, and
its `pnl_<window>` collector time stops advancing. The six windows share one
replay; a report past the cycle's 120-second wait keeps running and publishes
its windows when it ends. The windows publish together after the shared report,
so one window's slow Alpaca activities fetch delays all six.

A performance collector's `metrics_refresh_duration_seconds` sample is the time
of that refresh, failed or not. A PnL window's sample runs from the start of the
shared report to that window's publish, failed or not, so it includes the shared
replay and every earlier window's Alpaca fetch and capital step. Later windows
in the refresh order (`1w`, `1d`, `all`, `1m`, `ytd`, `1y`) always show a longer
duration, so read a long `pnl_1y` sample as a slow cycle, not as one slow
window.

### Adding a family

1. Spec the names in SPEC.md first. A new meaning gets a new name; an existing
   name never changes meaning.
2. Add the `LiqMetric` variants (name, help, sorted label keys, owning family)
   and, for a new source, a `LiqFamily` variant with its `collector` string.
   Extend the catalog test and this table.
3. Write the builder in its own submodule. It takes a metrics-owned input type,
   not a dashboard DTO, computes in exact types, and converts once with
   `float_value` or `integer_value`. A value that does not convert is skipped
   and logged. The exception is a family that copies an endpoint: it takes that
   endpoint's DTO from the same loader, so the two cannot drift. The performance
   families take the DTO of their `/performance/*` endpoint, the pending orders
   take `PendingOrderResponse` and the P&L windows take the `/pnl` response.
   Their label values are the DTO's serde names, so a serde rename changes a
   published label value: a contract change, as in step 1.
4. Publish with `LIQ_FAMILIES.replace(family, samples, now)`. Each call replaces
   the whole family.
5. Add a golden case: a fixture JSON under `src/metrics/liquidity/testdata/`, a
   case in `scripts/liq-parity/golden.py`, and the expected `.prom` written by
   running `python3 -I scripts/liq-parity/golden.py <exporter.py>` against a
   local checkout of the exporter. The goldens have no `# TYPE` lines; the test
   compares samples only. Extend `PORTED` and, where needed, `KNOWN_DIFFS` in
   `scripts/liq-parity/compare.py`.

### Board source

Both the exporter sidecar and the bot serve `liq_*`, so a selector without a job
would add the two together. The liquidity boards have a `Source` variable,
`$source`, and every `liq_` selector on them reads `job=~"$source"`:

| Source               | Job matcher                          |
| -------------------- | ------------------------------------ |
| `exporter` (default) | `t0-liquidity-exporter`              |
| `bot`                | `t0-liquidity\|t0-liquidity-staging` |

The bot's job is `t0-liquidity` in production and `t0-liquidity-staging` in
staging. `$env` already picks the project, so one regex covers both. PromQL
anchors a regex matcher, so `bot` never matches the exporter job.

The bot never emits `liq_up`, so the header's Bot pill reads the exporter's
`liq_up` on the exporter source and the bot scrape's own `up` on the bot source:
`max(liq_up{job=~"$source"}) or max(up{job=~"$source",job!="t0-liquidity-exporter"})`.
A stopped bot shows red on either source. A stopped exporter shows no data, not
red, because the exporter's own `up` says nothing about the bot.

Pick `bot` to compare the two side by side. Stage 4 of the migration flips the
default to `bot` in `SOURCE_VAR`, a one-line change; a later stage removes the
variable with the exporter.

The log panels follow `Source` too. A Cloud Logging query cannot pick its log by
a variable, so each one queries both logs, and its first transformation keeps
the frame of the source picked (`filterByRefId` on `${source:text}-<name>`):

| Panel                | `exporter`                          | `bot` (log `liquidity-bot`)                    |
| -------------------- | ----------------------------------- | ---------------------------------------------- |
| Trades               | `liquidity-trades`                  | `jsonPayload.target="liq_trade"`               |
| Rebalances           | `liquidity-transfers`               | `jsonPayload.target="liq_transfer"`            |
| Detail dialog events | none (status history)               | `jsonPayload.target="liq_event"`               |
| Logs                 | `liquidity-botlogs`                 | the bot's own lines, same level/target filters |
| Orders               | `liquidity-orders` on either source | the bot writes no order lines                  |

The bot's lines use the exporter's field names, so the columns are the same. The
bot source adds a USD column to Trades and Rebalances (the lines' `usd`) and the
detail dialog's event timeline, and the dialog drops a line whose `event_id` it
already has. Every row carries its project (`resource.labels.project_id`), so
the dialog never shows another environment's row. The generator fails on a log
panel without both queries and the transformation.

On the bot source, Trades' "Last updated" is when the bot logged the terminal
status. For an onchain fill the bot caught up on, that is later than the fill
(the exporter stamps the fill time); the dialog's "Occurred At" is the fill
time.

The hidden source's queries still run. On the exporter source, each refresh also
scans the bot's log for Trades, Rebalances and the dialog's events (about 10
seconds each over 30 days while the log is empty), and the Logs tab for its log
lines: one more Cloud Logging list call per panel against the project's read
quota. The plugin shows a throttled call as missing rows, not as an error. The
bot source holds rows only once the bot logs JSON (`log_format = "json"`) and
its stdout reaches Cloud Logging as `liquidity-bot`; until then it shows empty
log panels. Stage 4 flips them with the metrics, in the same `SOURCE_VAR`
change, after staging shows rows on the bot source, and a later stage removes
the exporter's queries.

`observability/gen-t0-liquidity.py` adds the matcher and fails if any `liq_`
selector is left without exactly `job=~"$source"`.
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
