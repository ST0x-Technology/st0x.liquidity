# ADR 0016: Store bot-gas costs in the event stream

- **Status:** Proposed
- **Date:** 2026-07-23
- **Amends:** ADR 0015
- **Linear:** RAI-1406 (parent RAI-1402 -- Extended-hours PnL leaks)

## Context

The PnL API supports historical reports through `asOfRowid`. That value is a row
ID from the shared CQRS `events` table: a report at row 100 must use only facts
known by event 100.

ADR 0015 requires bot-paid gas to be persisted as immutable receipt-cost facts.
The first implementation put those facts in a separate `bot_gas_cost` table.
That table has its own row IDs, so an event row ID cannot bound it. Loading all
gas rows makes an old report change when gas is recorded later; comparing the
two tables' unrelated row IDs would also be incorrect.

## Decision

Persist each bot-paid receipt cost as a CQRS event in the shared `events` table.
The aggregate is identified by chain and transaction hash and accepts one
recording:

- recording the same fact again is idempotent and emits no new event;
- recording different facts for the same receipt fails with a typed conflict
  error;
- invalid or non-positive financial values fail before event persistence.

PnL reads bot-gas cost events with the same `rowid <= asOfRowid` boundary used
for its other event-backed facts. The event row ID is therefore the shared
immutable ingestion cursor; no comparison between independent table row IDs is
needed.

The initial implementation reads the event payloads directly. If query volume
later requires a projection, each projected row must retain its source event row
ID and PnL must continue filtering on that value.

## Consequences

### Positive

- Historical PnL reports remain stable when later gas costs are recorded.
- Bot-gas costs reuse the API's existing snapshot contract instead of adding a
  second cursor.
- Aggregate state makes identical retries idempotent and conflicting retries
  explicit.
- Receipt costs are append-only facts rather than mutable rows.

### Negative / costs

- Bot-gas recording needs a CQRS aggregate and one shared framework instance in
  the server wiring.
- PnL must deserialize the bot-gas event payload.
- Direct SQL insertion into a standalone bot-gas ledger is no longer the source
  of truth.

### Neutral

- ADR 0015's required receipt, payer, valuation, category, and symbol fields do
  not change.
- The public `asOfRowid` API does not change.

## Alternatives considered

### Keep a separate table and compare its row ID with `asOfRowid`

Rejected. SQLite row IDs from different tables are unrelated and do not express
which fact was known first.

### Return a composite event-and-gas snapshot token

Rejected for now. It would change the public PnL API and require clients to
store a new token when the existing event stream already provides a shared
ordering.

### Attach the latest event row ID to each gas row

Rejected. A gas row recorded after event 100 could still receive watermark 100,
making it incorrectly visible in a historical report at event 100. Advancing the
watermark safely would require an event, which is the chosen design.

### Exclude bot gas from historical reports

Rejected. A report would have different cost coverage depending on whether it
requested the current or a historical snapshot.

## Follow-ups

- Model and test the bot-gas receipt-cost aggregate.
- Route recording through the aggregate's single server-side CQRS framework.
- Load bot-gas events through the existing PnL event snapshot boundary.
- Remove the standalone mutable bot-gas ledger table from the proposed
  implementation.
