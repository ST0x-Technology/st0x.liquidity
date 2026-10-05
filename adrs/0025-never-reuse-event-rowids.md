# ADR 0025: Never reuse event rowids

- **Status:** Proposed
- **Date:** 2026-10-05
- **Linear:** RAI-2895
- **Amends:** ADR 0016, ADR 0018 (Back /pnl with a CQRS read model)

## Context

ADR 0016 and ADR 0018 use `events.rowid` as the shared immutable ingestion
cursor: the PnL ledger records `last_rowid` and reads only rows above it, and
`asOfRowid` bounds historical reports. Both assume a rowid, once handed out,
never names another row.

The `events` table breaks that. It has a composite primary key and no
`AUTOINCREMENT`, so SQLite gives a new row `MAX(rowid) + 1`. `InventorySnapshot`
opts into compaction (`CompactionPolicy::CompactAfterSnapshot`), and the cleanup
loop deletes its covered events every tick. Those are often the newest rows in
the table. Once they are deleted, the next events take their numbers.

`PnlLedger::ingest_batch` moves the checkpoint to `head_rowid`, the highest
rowid over every aggregate, when every source stream is drained. So the ledger
can checkpoint on an `InventorySnapshot` row, compaction deletes it, and the
next source events land at or below the checkpoint, where `events_since` never
reads them:

- If a fill and its `OnChainFillApplied` both land there, the fill is silently
  missing from P&L.
- If only the fill does, the basis update finds no row, ingestion fails with
  `MissingOnchainFillBasisTarget`, and the ledger stops advancing.

In production the ledger skipped a GRND fill and two FGI placements on
2026-10-01 without an error. On 2026-10-02 at 21:04:26Z it skipped a wtQQQM fill
whose basis arrived one row later, 6.5 minutes after a compaction deleted 17
rows. The checkpoint was 682757, the rowid of the unread fill. The startup catch
up in `setup_pnl_ledger` returns that error, so every restart from 2026-10-03
02:57Z failed until the ledger was rebuilt by hand on 2026-10-05.

## Decision

### Event rowids come from `AUTOINCREMENT`

A migration rebuilds `events` with `id INTEGER PRIMARY KEY AUTOINCREMENT`. `id`
aliases `rowid`, the copy keeps every existing rowid, and
`(aggregate_type, aggregate_id, sequence)` stays as a `UNIQUE` constraint, so
duplicate sequence rejection (event-sorcery's optimistic locking matches any
unique violation) and the aggregate load index are unchanged. Both secondary
indexes are recreated.

With `AUTOINCREMENT`, SQLite tracks the highest id ever used in
`sqlite_sequence` and never hands out a lower one, whatever is deleted. The
cursor in ADR 0016 and ADR 0018 then holds for every reader without each reader
knowing which aggregates compact.

### Seed the sequence above the ledger checkpoint

Rowids handed out before the migration and since deleted are not in the table,
and SQLite kept no record of them, so the copy alone can leave the sequence
below the ledger checkpoint. The ledger itself no longer trusts the checkpoint
after the version reset below, but the checkpoint is the highest head any catch
up reached, at or above every head `/pnl` reported, so `asOfRowid` watermarks
already issued name numbers at or below it. The migration sets the `events`
entry in `sqlite_sequence` to the larger of `MAX(id)` and
`pnl_ledger_checkpoint.last_rowid`, so no such watermark comes to cover a new
event. Numbers deleted above both before the migration may be handed out once
more; every number handed out after the migration is never reused.

### Rebuild the ledger once, under any release

The ledger may already have skipped events below its checkpoint. The migration
sets `pnl_ledger_checkpoint.ledger_version` to 0, a version no release uses, so
the next catch up truncates the ledger and reads every event again. Doing it in
the migration rather than by bumping `LEDGER_VERSION` makes the previous release
rebuild too if it is rolled back to before the new one's first catch up commits.

### A ledger failure does not stop the bot

`setup_pnl_ledger` logs a failed startup catch up, pages the operator through
the `Notifier` like the other startup steps that start without their work, and
continues. The ledger is a read model; it must never keep the bot from hedging.
The failure is counted in `pnl_ledger_catch_up_failures_total`, the reactor
retries on every source event, and `/pnl` answers 500 and logs the ingestion
error until ingestion recovers.

`MissingOnchainFillBasisTarget` stays fail closed. With unique rowids, a missing
fill row means the event log is corrupt, and the ledger must not report P&L over
it.

## Tradeoffs & Consequences

- The migration copies the whole `events` table at startup, about 570,000 rows
  in production on 2026-10-05. The copy is written before the old table is
  dropped, so the WAL grows by about the size of the table during the migration,
  and the dropped table's pages, about 1.15 GB in production, stay in the file
  on the freelist. Production's file is in incremental auto vacuum mode: the
  loader warns at every startup when it is not, and has not warned since
  2026-09-01. The cleanup loop then reclaims up to 100 of those pages per hourly
  tick, and only on ticks where compaction deleted events, so the database file
  can stay about twice its size for months or longer. To reclaim them at once,
  run `PRAGMA incremental_vacuum` against the file with the bot stopped in the
  deploy window: it moves the live `events` pages in one write transaction and
  grows the WAL by about their size. A file in another mode reclaims nothing
  either way, and needs `PRAGMA auto_vacuum = INCREMENTAL; VACUUM;` with the bot
  stopped.
- `events` shows an `id` column. `rowid` still resolves to the same value, so
  existing SQL keeps working.
- The first startup after the upgrade spends about a minute on the ledger
  rebuild (56 s at head 682956 on 2026-10-05).
- A wedged ledger no longer surfaces as a failed deploy. It shows as an
  operational alert, 500s from `/pnl`, the error log,
  `pnl_ledger_catch_up_failures_total` and `pnl_ledger_checkpoint_lag_events`.
- The next caller of `catch_up` retries a failed startup catch up: the reactor
  inside the command dispatch of each source event, and every `/pnl` request.
  After a transient failure during a rebuild, that caller carries the rest of
  it: a dispatch for up to about a minute, where a crash loop used to spend the
  same minute at the next boot, or a `/pnl` request for up to
  `PNL_CATCH_UP_TIMEOUT` before it answers 503.
- The previous release runs on the rebuilt table: it applies migrations with
  `set_ignore_missing(true)`, names every inserted column and reads `rowid`.

## Alternatives considered

### Cap the ledger checkpoint at its own source rows

Rejected. Moving the checkpoint only to the highest rowid among `Position`,
`TokenizedEquityMint`, `UsdcRebalance` and `BotGasReceiptCost` keeps new rows
above it, because those aggregates are never compacted. It fixes only the
ledger, leaves `asOfRowid` able to name a reused row, and breaks again the day
one of those aggregates compacts.

### Keep the newest row in `compact_events`

Rejected. Never deleting the highest rowid stops SQLite from reusing numbers,
but only as long as every delete follows the rule.
`20260429143927_delete_alpaca_wallet_usdc_events.sql` already deletes events
outside compaction, and the change needs an event-sorcery release.

### Read the ledger by sequence per aggregate instead of a global rowid

Rejected. ADR 0018 orders ingestion by rowid across aggregates (mint fee
attribution spans events) and exposes rowids through `asOfRowid`. Replacing the
cursor changes the public API and the ingestion design to fix a storage
property.
