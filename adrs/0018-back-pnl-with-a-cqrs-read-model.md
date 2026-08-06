# ADR 0018: Back /pnl with a CQRS read model instead of raw SQL over the events table

- **Status:** Accepted
- **Date:** 2026-08-05
- **Linear:** RAI-1506
- **Related:** ADR 0016 (event row ID as the shared immutable ingestion cursor)

## Context

`/pnl` is the last consumer that queries the `events` table directly.
`src/dashboard/pnl/source.rs` runs hand-written SQL with literal
`aggregate_type` / `event_type` strings and re-parses raw JSON payloads per
request. An event rename or payload change compiles clean and breaks `/pnl`
silently at runtime; three such latent defects surfaced in one debugging session
(missing broker-mock activities endpoint masking a null-`fees` report failure
masking a decimal-overflow failure), each invisible behind the previous because
the report fails wholesale on the first error it meets.

Constraints discovered in the code, which shape any replacement:

1. `asOfRowid` is a global `events.rowid` watermark, and real row IDs leak into
   the response (`opening_rowid`/`closing_rowid`, ordering tie-breakers). Any
   derived store must carry the genuine rowid per row.
2. The FIFO replay folds fills in execution-timestamp order, while events arrive
   in persistence (rowid) order. Late-arriving events (backfills, slow broker
   reconciliation) insert into the middle of the fold and re-pair every later
   match. Matched lots therefore cannot be materialized incrementally without
   changing reported semantics.
3. Reactor delivery is at-most-once: `cqrs-es` dispatches queries in a plain
   awaited loop whose `dispatch` returns `()`, so reactor errors are logged and
   swallowed. A read model for financial reporting cannot depend on per-event
   delivery.
4. Reactors receive `(id, event)` only -- no rowid, no sequence -- so rowid
   stamping cannot come from the reactor callback.
5. Production has a full event history; the read model must backfill it.
6. Mint fee attribution spans events (`MintRequested` carries the symbol, the
   terminal event carries the fee), so ingestion must process events in rowid
   order with a small support table.
7. Duplicate business events produce audit warnings during replay; the store
   must keep every occurrence as its own row.

Since the task was specified, `#1121` moved report arithmetic from exact `Num`
rationals to Rain `Float` (~4us/op, revm-executed) and added replay admission
control (`PnlReportAdmission`, 2 permits, `spawn_blocking`, excess requests
503). This contains the replay's cost but does not reduce it; per-request cost
still grows with history. Separately, unreported mint fees are now a counted
"missing cost observation" (`missing_cost_observation_count`) rather than a
report failure, an outcome the read model must be able to represent.

## Decision

Serve `/pnl` from a typed, append-only **PnL ledger** maintained by a
checkpointed ingester over the event stream. The FIFO replay itself stays at
query time, unchanged, reading ledger rows instead of raw events.

```
Store::send() commits events (source of truth: events table)
      |
      +--------------------------------------------+
      |                                            |
      v                                            v
events table                          PnlLedgerReactor
(never touched by pnl SQL)            deps!: [Position, TokenizedEquityMint,
      ^                                       UsdcRebalance, BotGasReceiptCost]
      |                                            |
      | typed stream API                           |  nudge only
      | (st0x-event-sorcery):                      |  (payload ignored)
      |   events_since::<E>(pool, after)           v
      |     -> Vec<Sequenced<E>>          PnlLedger::catch_up()  [the ingester]
      |   head_rowid(pool) -> i64           - serialized by async Mutex
      +------------------------------.      - loads typed events with
                                     |        rowid > checkpoint, per entity
                                     |      - exhaustive match on event enums
                                     '----> - insert ledger rows (duplicate
                                              rowids no-op via ON CONFLICT)
                                            - checkpoint advance, same tx
                                                   |
                                                   v
                                    pnl ledger tables (typed, append-only)
                                    pnl_onchain_fill      pnl_offchain_fill
                                    pnl_offchain_placement pnl_manual_adjustment
                                    pnl_cost_entry        pnl_bot_gas_cost
                                    pnl_mint_symbol       pnl_ledger_checkpoint
                                                   |
GET /pnl                                           |
      |                                            v
      +--> ensure_fresh() ---------> load rows WHERE event_rowid <= asOfRowid
           (catch_up to head,               |
            outside the replay              v
            admission permit)      FIFO replay (replay.rs, algorithm unchanged,
                                   under PnlReportAdmission + spawn_blocking)
                                            |
                                            v
                                   PnlResponse (shape unchanged)
```

### Ledger tables (one sqlx migration)

One row per relevant event occurrence, PK `event_rowid` (the genuine
`events.rowid`), decimals as canonical decimal TEXT (parsed with `Float::parse`
at read time), timestamps RFC3339 TEXT:

- `pnl_onchain_fill(event_rowid, symbol, tx_hash, log_index, shares,
  direction, price_usd, executed_at)`
- `pnl_offchain_fill(event_rowid, symbol, offchain_order_id, shares,
  direction, price_usd, executed_at)`
- `pnl_offchain_placement(event_rowid, symbol, offchain_order_id,
  placed_at)`
- `pnl_manual_adjustment(event_rowid, symbol, target_net, price_usd NULL,
  adjusted_at)`
- `pnl_cost_entry(event_rowid, source, aggregate_id, symbol NULL,
  amount_usd NULL, occurred_at)`
  -- `amount_usd` NULL persists the "mint fees not reported" observation so the
  read side can count it into `missing_cost_observation_count`; omitting that
  row would silently regress the merged fix.
- `pnl_bot_gas_cost(event_rowid, chain, tx_hash, usd_cost,
  operation_category, symbol NULL, occurred_at)`
- Support: `pnl_mint_symbol(aggregate_id PK, symbol)`,
  `pnl_ledger_checkpoint(id=1, last_rowid, ledger_version)`.

Duplicate business events remain distinct rows (PK is the rowid alone); dedup
and its audit warnings stay in the replay.

### Ingestion: checkpoint + catch-up, reactor as doorbell only

`st0x-event-sorcery` gains a typed stream API (the only place allowed to SQL the
`events` table):
`events_since::<Entity>(pool, after_rowid) ->
Vec<Sequenced<Entity>>` (rowid,
id, sequence, typed event, filtered on `Entity::AGGREGATE_TYPE`) and
`head_rowid(pool)`.

`PnlLedger::catch_up()` (mutex-serialized): read `head_rowid`; if the checkpoint
is at head, return; otherwise ingest `checkpoint..head` for each source entity
via an exhaustive `match` on the typed event enum (a new or renamed variant is a
compile error), insert the rows with `ON CONFLICT(event_rowid) DO NOTHING` (only
a duplicate rowid is a no-op; a `NOT NULL`/`CHECK` violation aborts the batch so
the checkpoint can never advance past a row the ledger failed to ingest --
`INSERT OR IGNORE` would silently swallow those too), and advance the checkpoint
in the same transaction -- rows and progress marker commit atomically, so every
committed state satisfies "every event at or below `last_rowid` that maps to a
ledger row has that row committed". Event variants that carry no replay input --
no share movement, no fee, no gas cost (the empty arms of the per-entity
`match`es) -- map to no row and are absent by design. Large gaps (first-deploy
backfill, rebuilds) are ingested in bounded batches, each batch's rows and
checkpoint bump in their own transaction: peak memory stays flat and a
mid-backfill crash resumes from the last batch instead of starting over.

`PnlLedgerReactor`
(`deps!: [Position, TokenizedEquityMint, UsdcRebalance,
BotGasReceiptCost]`)
ignores the delivered payload entirely and calls `catch_up()` -- a direct
in-process call, not a queued job: correctness never depends on a nudge arriving
(the checkpoint self-heals on the next trigger), so durable queueing would add
latency and moving parts for redundant reliability. `/pnl` calls
`ensure_fresh()` (a catch-up) before resolving its watermark, giving
read-your-writes even if every nudge was swallowed; it runs outside the replay
admission permit (async I/O, usually a no-op). The checkpoint starting at zero
makes first-deploy backfill the same code path as steady-state ingestion. A
`LEDGER_VERSION` mismatch at startup truncates the ledger and re-ingests (the
rebuild path).

### What does not change

The FIFO replay, bucketing, dedup, warnings, diagnostics, windows, pagination,
response shape, and `asOfRowid` semantics are preserved byte-for-byte: ledger
rows carry genuine rowids, `event_rowid <=
asOfRowid` selects exactly what
`rowid <= asOfRowid` selects today, and the replay is the same code over the
same values. Non-event inputs (`position_view`, `portfolio_snapshot`, live
Alpaca account activities) are untouched. Business validations that fail the
report today keep failing it at read time; ingest stores what the event says.

### Input inventory

Position: `OnChainOrderFilled`, `OffChainOrderFilled`, `ManualPositionAdjusted`,
`OffChainOrderPlaced` (audit warnings only); the six remaining variants are
explicitly skipped (no share movement). TokenizedEquityMint: `MintRequested`
(symbol attribution), `TokensReceived`, `ProviderCompletionRecovered`.
UsdcRebalance: `Bridged`, `BridgingCompletionRecovered`. BotGasReceiptCost:
`Recorded`.

## Consequences

Positive:

- No SQL in the PnL module names an aggregate type or event type; event
  consumption is typed and exhaustively matched, so schema drift becomes a
  compile error instead of a silent runtime report failure.
- Ingestion is lossless and self-healing (checkpoint over the durable log)
  despite at-most-once reactor delivery, and backfill is free.
- Per-request input cost drops from scanning and JSON-parsing the whole event
  log to indexed range scans over compact typed rows.
- The ledger's append-only, rowid-stamped inputs make matched-entry memoization
  keyed by the `asOfRowid` watermark trivially correct later (immutable inputs
  per key; a new event is a new key). This is the designated follow-up for
  replay cost -- explicitly whole-result caching per watermark, not
  resume-from-baseline, which is unsound because the execution-timestamp fold
  does not commute with rowid-order arrival.

Negative / accepted:

- FIFO matching stays at query time, so per-request arithmetic cost (now
  Float-priced after #1121) is unchanged until the memoization follow-up;
  admission control continues to bound its blast radius.
- Every commit on the four source aggregates pays a small inline ingest in
  `Store::send()`; if tracing shows it matters, the mitigation is debouncing
  inside `PnlLedger` (coalescing notify), not changing the reactor contract.
- New moving part (the checkpointed ingester) and eight new tables (six ledger,
  two support) to operate and rebuild.

Rollout: (1) event-sorcery `events_since`/`head_rowid` upstream; (2) migration +
ingester with tests; (3) reactor + conductor wiring + startup catch-up; (4) cut
`/pnl` over and delete the raw-SQL/JSON path, adapting `tests.rs` (existing
tests seed raw events and keep exercising the full path through
`ensure_fresh()`).

## Alternatives considered

- **Materialize the report (or matched lots) in a view table:** unsound under
  constraint 2 (late events re-pair matches; incremental maintenance cannot
  unfold state) and cannot serve arbitrary historical `asOfRowid` values without
  either per-watermark storage or a second replay path that must agree forever.
- **Payload-driven reactor writes (hedge-latency style):** acceptable for
  gap-tolerant analytics, not for money: at-most-once delivery loses rows
  permanently, and the payload lacks the rowid the watermark semantics require.
- **Typed per-request replay with no tables:** kills the string literals and raw
  SQL but re-decodes full history per request and is not a maintained read
  model; kept only as a fallback shape if the ledger had to be descoped.

## Open questions

- Whether `events_since`/`head_rowid` land upstream in `st0x-event-sorcery`
  first (the right home; the API must not foreclose upcaster support) or start
  as a literal-free generic helper in `st0x-hedge` outside `src/dashboard/pnl/`.
- `load_position_view`'s raw SQL over the `position_view` view table is out of
  scope here (not the `events` table) but is a candidate follow-up via
  `Projection` access.
