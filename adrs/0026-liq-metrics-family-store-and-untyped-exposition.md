# ADR 0026: Publish `liq_*` from a family store, typed as gauges

- **Status:** Proposed
- **Date:** 2026-10-08
- **Linear:** RAI-2998

## Context

The `liq_*` series are a published contract. The capital probe, the shareholder
TVL report and the liquidity board read them. An exporter sidecar first
published them by polling the bot's API, and it replaced each group of series as
one unit on every poll, so a symbol, chain or day that left the source left
`/metrics` too. It wrote no `# TYPE` lines, so Managed Prometheus stores every
`liq_*` name as untyped. Managed Prometheus ingests each untyped sample twice,
as a gauge and as a counter, so the `liq_*` names cost about twice the samples
they need.

The bot must now publish the same names itself. The `metrics` recorder that
serves the bot's other series cannot do that faithfully:

- It never forgets a label set. A departed symbol keeps its last value forever,
  and P&L day buckets grow without limit.
- `PrometheusBuilder::idle_timeout` expires by metric kind, so it would also
  expire rarely set gauges such as `registry_*` and `hedge_floor_shares`, and it
  only runs during a render.
- Setting stale label sets to 0 publishes false values: a removed symbol would
  show 0 shares.
- A second recorder has the same no-removal behaviour, and its registry is
  private.

The bot's series can stay untyped, like the exporter's, or be typed. Every
`liq_*` name is a gauge: snapshots, rolling `_24h` windows that go down,
`*_total` names that mean a count now, and precomputed quantile values that are
not real summaries. If the bot publishes them untyped, typing them later is a
second migration with a second history break.

## Decision

1. **A family store beside the recorder.** `src/metrics/liquidity.rs` holds a
   process-wide store of families. Each refresh replaces all samples of one
   family. `/metrics` renders the recorder output, then the store.
   - Every `liq_*` name is a `LiqMetric` variant that belongs to exactly one
     family. A sample for a name its family does not own is dropped and logged.
     No `liq_` name goes through the `metrics` macros; a test scans the source
     for that.
   - Samples are stored as `Arc<[LiqSample]>`. A render clones the `Arc`s under
     a `std::sync::Mutex` and formats outside it, so a large render never blocks
     a refresh.
   - The store publishes `liq_collector_last_success_ts_seconds{collector}`, the
     time each family was last replaced.
   - If the recorder output already holds a `liq_` name, the store skips its own
     block for it and logs an error, so the body stays valid.
2. **Typed gauges.** `liq_*` blocks have one `# HELP` line and one
   `# TYPE <name> gauge` line per name. Samples are grouped one block per name,
   sorted by name and label set. Label values are escaped by our own escaper
   (backslash, double quote, newline), because the recorder's sanitizer treats
   an input backslash as an escape prefix.

## Consequences

- Departed symbols, chains and days leave `/metrics` on the next refresh, as
  they did with the exporter.
- Each name has one gauge descriptor, and each sample is ingested once, not
  twice. Tools treat the names as gauges, so they do not suggest `rate()` on
  them.
- There is one migration, not two: the names do not need a later change from
  untyped to gauge.
- The exporter's untyped history and the bot's gauge history are separate
  descriptors. A query by name with no `job` selector does not join them, so a
  chart like that has a gap at the cutover. We accept this cost. A chart that
  selects by `job` switches job at the cutover, typed or not.
- While the exporter runs beside the bot, one name exists as untyped (exporter
  job) and as a gauge (bot job). Nothing counts it twice: the liquidity board
  selects by `job=~"$source"`, and the capital probe and the TVL report are
  pinned to the exporter job (t0.devops#787).
- With `--known-diffs`, `scripts/liq-parity/compare.py` accepts the bot's
  `gauge` against the exporter's untyped for every `liq_*` name, and reports any
  other type difference. The Rust tests check that the bot types every name as a
  gauge. The exporter goldens have no `# TYPE` lines, so the Rust golden tests
  compare samples only.
- The default `promtool check metrics` lint warns on gauges whose names end in
  `_total` (for example `liq_pending_orders_total`,
  `liq_pending_orders_uncapped_total`, `liq_raindex_orders_total` and the
  inventory totals such as `liq_equity_total`) and on the `_ms` names. These are
  known exceptions: the names keep the exporter's meaning, and a rename is a new
  name. Check the body with `promtool check metrics --lint=none --extended`,
  which parses it and prints the cardinality without the lint.
- The contract has two code paths for metrics. New native metrics that are not
  part of the `liq_*` contract still use the `metrics` macros and are typed.
- The store's value rules (finite values only, exact arithmetic converted once)
  live in one module, and every builder goes through them.
