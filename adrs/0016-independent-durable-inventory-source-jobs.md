# ADR 0016: Run inventory observations as independent durable source jobs

- Status: Accepted
- Date: 2026-07-16

## Context

[RAI-386](https://linear.app/makeitrain/issue/RAI-386/convert-inventory-polling-to-pollinventory-apalis-job)
is marked as an Apalis migration, but inventory polling still runs through the
long-lived `InventoryMonitor`. Each timer tick calls one monolithic
`InventoryPollingService::poll_and_record`, which polls inflight tokenization,
onchain vaults, wallets, and the broker in sequence.

Those reads do not share a failure domain:

- the tokenization provider supplies pending mint and redemption requests;
- Raindex supplies equity-vault and USDC-vault balances through separate RPC
  calls;
- Ethereum and Base wallets supply separate token balances; and
- the broker supplies positions, cash fields, and Alpaca USDC in one
  `get_inventory` response.

The current sequencing couples otherwise independent observations. An inflight
poll failure aborts the entire tick. Within the onchain group, an equity RPC
failure prevents the USDC poll. Within the wallet group, an Ethereum failure
prevents every Base observation. A process crash also loses the timer's next due
time because the timer is not durable.

Simply running these reads concurrently is unsafe. Balance events actively
enqueue equity and USDC rebalancing checks. As documented by
[RAI-703](https://linear.app/makeitrain/issue/RAI-703/suppress-rebalancing-triggers-on-degraded-inventory-poll-ticks-stale-counterpart-venue),
a fresh observation from one venue can therefore drive a money-moving decision
against a stale counterpart. The current inflight-first ordering is another
implicit safety gate: balance events must not initiate duplicate tokenization
while the provider's pending-request state is unknown. Independent jobs remove
the notion of one ordered poll tick, so neither invariant may continue to rely
on call order.

The existing Apalis infrastructure already provides typed queues, delayed
enqueueing, supervised workers, retry budgets, and SQLite persistence. The
missing decision is the durable work-unit boundary and the freshness contract
between observations and rebalancing.

## Decision

### Use one job per independently repeatable external observation

Introduce these stateless Apalis job types, each with its own queue and worker:

- `PollInflightEquity`
- `PollOnchainEquity`
- `PollOnchainUsdc`
- `PollEthereumWalletUsdc`
- `PollBaseWalletUsdc`
- `PollBaseWalletUnwrappedEquity`
- `PollBaseWalletWrappedEquity`
- `PollOffchainInventory`

`PollOffchainInventory` remains one job because the broker exposes positions,
cash fields, and Alpaca USDC as one `get_inventory` observation. Splitting that
response into independently fetched jobs would repeat the same external call and
could manufacture internally inconsistent broker snapshots. This is the boundary
where the values belong together.

Each concrete job type has a distinct worker and queue. An enum payload handled
by one worker is not sufficient because it would retain shared worker
backpressure and serial execution across sources.

`InventoryPollingService` remains the integration adapter, but exposes one
domain capability per job instead of a public monolithic `poll_and_record`.
Remote reads execute independently. Writes to the shared `InventorySnapshot`
aggregate may use a narrow critical section so concurrent jobs cannot race the
aggregate's optimistic version; that persistence detail must not serialize the
external calls or their schedules.

### Make each job own exactly one durable schedule

Each successful job execution enqueues exactly one copy of itself with
`JobQueue::push_with_delay` using the configured inventory poll interval. A
source read or snapshot-persistence failure is logged and the next cadence is
still scheduled, preserving the current transient-failure behavior without
multiplying retry loops. Failure to persist the follow-up job is returned to
Apalis so the durable schedule itself is retried.

At startup, bootstrap each configured source independently: remove its
non-terminal rows left by an earlier process and enqueue one immediate job. This
preserves the current immediate first poll and guarantees that repeated restarts
cannot create multiple self-rescheduling loops. Wallet jobs are not bootstrapped
when the corresponding wallet or token set is unconfigured.

### Persist successful observations separately from changed balances

A source job records a typed successful-observation marker only after every
snapshot command derived from that external observation has been persisted. The
marker is recorded even when all balances are unchanged, because value
deduplication must not erase evidence that a source was successfully observed. A
partial write or failed read records no success marker.

Observation markers carry the source and the time at which the external read
completed. They are folded into durable inventory state and hydrated on restart.
Rebalancing eligibility therefore does not depend on in-memory job history or on
whether a balance happened to change.

### Make rebalancing fail closed on required-source freshness

Rebalancing check jobs, rather than polling order, enforce freshness. At check
execution time, every required observation must be present and no older than the
configured inventory poll interval:

- equity checks require fresh `PollInflightEquity`, `PollOnchainEquity`, and
  `PollOffchainInventory` observations;
- USDC checks require fresh `PollOnchainUsdc` and `PollOffchainInventory`
  observations; and
- wallet recovery dispatch remains source-local because wallet balances do not
  participate in the cross-venue equity or USDC ratio.

If any required source is missing or stale, the check completes without
dispatching a transfer. A later successful observation enqueues a fresh check,
so recovery is automatic and level-triggered rather than dependent on a
particular event ordering.

RAI-386 supplies the independent durable polling substrate. RAI-703 is stacked
on it and changes the rebalancing trigger contract to consume the persisted
freshness markers. The stack is not operationally complete until both changes
are present; independent scheduling must not be deployed without the freshness
gate.

## Consequences

### Positive

- An outage in one RPC, wallet, provider, or broker no longer prevents unrelated
  inventory observations.
- Every source has a durable next run, immediate cold-start bootstrap, and an
  independently observable queue and worker.
- Rebalancing safety becomes an explicit, testable domain invariant instead of
  an accidental consequence of function order.
- Unchanged successful polls restore freshness and re-enable rebalancing after
  an outage without requiring an unrelated balance change.
- The job boundaries match external consistency boundaries: independently
  repeatable reads are separate, while one broker response stays together.

### Negative / costs

- The conductor must wire and supervise eight polling workers and bootstrap
  eight queues instead of one monitor.
- The shared snapshot aggregate still serializes its short persistence sections,
  so the design removes external-I/O coupling rather than pretending SQLite
  writes are parallel.
- Successful-observation markers add event volume even when balances do not
  change. Existing inventory-event compaction must include and preserve the
  newest marker for every source.
- Independent jobs create more possible interleavings, requiring tests for
  duplicate bootstrap, delayed scheduling, partial persistence, stale-source
  suppression, and recovery when freshness returns.
- During a prolonged source outage, affected money-moving rebalancing fails
  closed. This may leave an imbalance uncorrected until the source recovers.

### Neutral

- Snapshot values and the dashboard's merged inventory model remain unchanged.
- The existing poll interval remains both the cadence and the maximum accepted
  observation age; this ADR does not introduce a second timing knob.
- Wallet observations remain visible and may drive their existing recovery jobs,
  but do not become counterpart requirements for balance-ratio decisions they do
  not feed.

## Alternatives considered

### One durable `PollInventory` job preserving the current sequence

**Pros:** Smallest migration from `InventoryMonitor`; one queue and one worker;
preserves inflight-first ordering.

**Cons:** A failure still suppresses unrelated reads, retries repeat successful
external calls, and RAI-703 remains tied to a fragile tick-wide control flow.

**Rejected:** It makes the timer durable but does not achieve the requested
failure isolation.

### One job type with an `InventorySource` payload

**Pros:** Less worker wiring and less repeated job implementation code.

**Cons:** All sources share one worker queue and therefore one concurrency and
backpressure domain. A slow source delays unrelated pending jobs even though
their payloads differ.

**Rejected:** The serialization boundary would remain an implementation accident
rather than an external-system boundary.

### One job that runs all source reads concurrently

**Pros:** Reduces wall-clock duration while retaining a single bootstrap and
schedule.

**Cons:** Individual reads are not durably scheduled or retried; a crash loses
the whole in-memory join; the next run and failure accounting remain coupled.

**Rejected:** Concurrency inside one process is not independent durable work.

### Require all sources to join one poll-generation barrier

**Pros:** Rebalancing sees a coherent epoch and never mixes generations.

**Cons:** One unhealthy source blocks every downstream decision and discards the
intentional partial-freshness behavior. It also requires durable barrier cleanup
for crashed generations.

**Rejected:** A freshness eligibility check provides the safety property without
coupling unrelated observations or withholding useful snapshots.

### Treat balance changes as proof of freshness

**Pros:** Requires no new observation event and adds no event volume.

**Cons:** Unchanged balances produce no snapshot event today, so a healthy
source would eventually appear stale forever and could not re-enable rebalancing
after an outage.

**Rejected:** Freshness describes a successful observation, not a value change.

## Follow-ups

- RAI-386: introduce the source capabilities, Apalis jobs, queues, workers,
  bootstrap behavior, and durable observation markers; remove `InventoryMonitor`
  and the erased monolithic `Poller` API.
- RAI-703: make equity and USDC check scheduling level-triggered by successful
  observations and reject checks whose required source markers are missing or
  stale.
- Extend inventory-event compaction and hydration tests to prove the newest
  marker for every source survives restart and compaction.
- Add queue-level tests proving each source self-reschedules once, source
  failure does not stop another worker, and repeated bootstrap does not multiply
  polling loops.
