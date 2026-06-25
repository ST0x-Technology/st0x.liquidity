# ADR 0013: Extract the equity family's handler side effects into the existing durable-job orchestrators

- Status: Proposed
- Date: 2026-06-25

## Context

Upgrading liquidity to event-sorcery 0.2.0-rc2 (the jobs model) requires every
`EventSourced` command handler to be **pure** — no I/O inside `initialize` /
`transition`. In 0.2.0-rc2 the trait drops `type Services` for
`type Jobs: JobList` and the handlers become synchronous, taking
`&mut JobQueue`. A handler that `await`s a service mid-command cannot exist.
RAI-924 (the mechanical bump) is therefore blocked until no liquidity handler
performs I/O.

OffchainOrder (RAI-926) was the first such extraction and proved the shape. The
pattern is self-contained: a command handler emits a **request event** instead
of doing I/O; a durable job/orchestrator picks up that event and performs the
external call; and an **outcome command** records the call's result back on the
aggregate through a pure handler. OffchainOrder had **no pre-existing
orchestration layer** — the broker call was lifted into a new durable path and
re-driven by purpose-built jobs (`poll_status`, `reconcile_fill`,
`handle_rejection`).

The equity family is different. Four aggregates still do inline I/O in their
handlers via shared/aggregate `Services` bundles:

- **`TokenizedEquityMint`** — `tokenizer.request_mint` (initialize),
  `tokenizer.poll_mint_until_complete` (`Poll`). Its wrap/deposit I/O was
  **already** lifted into the orchestrator by the earlier mint-path extraction:
  the handler receives pure `SubmitWrap` / `WrapTokens` / `SubmitVaultDeposit` /
  `DepositToVault` commands.
- **`EquityRedemption`** — all of withdraw / unwrap / send-for-redemption still
  inline across `SubmitWithdraw` / `ConfirmWithdraw` / `SubmitUnwrap` /
  `ConfirmUnwrap` / `SendTokens`.
- **`UnwrappedEquityRecovery`** / **`WrappedEquityRecovery`** — wrap / deposit /
  `resume_mint` / `resume_redemption` inline through `*_or_fail` helpers that
  convert terminal service failures into `RecoveryFailed` **events** (and
  surface retryable failures as aggregate errors for the job to retry).

Crucially, the equity family **already has** a durable-job orchestration layer:
`CrossVenueEquityTransfer::{resume_mint, resume_redemption}` runs inside the
existing `TransferEquityToMarketMaking` / `TransferEquityToHedging` apalis jobs,
and each recovery aggregate has its own reactor-enqueued job:
`UnwrappedEquityRecovery` is driven by `UnwrappedEquityRecoveryJob`, and
`WrappedEquityRecovery` by `WrappedEquityRecoveryJob`. For the mint path the
orchestrator already does the wrap/deposit I/O and sends the aggregate **pure**
commands — exactly the target shape, applied to only part of the workflow. The
remaining extraction therefore **extends each recovery job's existing `perform`
loop** — mirroring how the two transfer jobs already share `resume_mint` /
`resume_redemption` — rather than adding a new shared recovery driver: the two
recovery jobs keep their distinct orphan paths and share only the lifted onchain
step wrappers (Decision 3).

Two things are duplicated today and will diverge if each aggregate is extracted
independently:

1. The onchain step wrappers (lookup derivative -> submit wrap -> confirm wrap;
   resolve vault -> submit deposit -> confirm tx) exist both as private helpers
   on `CrossVenueEquityTransfer` (`wrap_received_mint`, `deposit_wrapped_mint`)
   and as near-identical `*_or_fail` helpers on the two recovery aggregates.
2. The "I/O outcome -> pure outcome command" wiring.

This ADR decides how the remaining extraction is shaped so the four
per-aggregate PRs (RAI-928/929/930/931) converge instead of producing four
divergent copies, and what RAI-927 puts in place first.

## Decision

1. **Complete the pattern the mint path already started: move all remaining
   inline handler I/O up into the existing job-driven orchestrators, and reduce
   every equity-family handler to pure event recording.** After this, all four
   aggregates have `type Services = ()` and `initialize` / `transition` touch no
   I/O, unblocking the RAI-924 bump.

2. **Do not introduce new per-concern apalis jobs for the equity family.** The
   `TransferEquityToMarketMaking` / `TransferEquityToHedging` jobs and the
   recovery job already provide the durable, at-least-once execution context.
   The extraction extends their orchestration (`resume_mint`,
   `resume_redemption`, and each recovery job's `perform` loop) to perform the
   I/O the handlers do today and to send the aggregate pure outcome commands.
   This diverges deliberately from RAI-926, which had no orchestrator to extend;
   adding a second, parallel orchestration layer here would be strictly worse.

3. **Lift the shared onchain step wrappers into one home (RAI-927).** A single
   set of outcome-agnostic functions — submit wrap, confirm wrap, submit vault
   deposit (resolve token + vault), with their shared error type — replaces both
   the `CrossVenueEquityTransfer` private helpers and the recovery `*_or_fail`
   SDK bodies. Callers map the `Result` into their own events/commands.

4. **Preserve the recovery failure-to-event semantic across the move.** A
   terminal I/O failure in a recovery job's `perform` loop is reported back to
   the aggregate as a pure outcome command (e.g.
   `RecordRecoveryFailure { reason }`); the aggregate's handler still decides to
   emit `RecoveryFailed`. Retryable failures stay job errors so apalis retries
   them — the same terminal-vs-retryable split the `*_or_fail` helpers encode
   today, relocated from the handler to the job.

5. **Outcome commands are idempotent against aggregate state.** Jobs are
   at-least-once, so re-running an orchestrator step that already advanced the
   aggregate must no-op (the existing state-guarded command arms already do
   this; new outcome commands follow the same rule).

6. **Orchestrator I/O follows the mint path's sequencing contract.** Each step a
   job drives loads current aggregate state, performs **one** external mutation,
   persists the intermediate outcome event, and only **then** performs the next
   confirmation/polling I/O. Decision 5 makes the aggregate idempotent against a
   replayed _command_; this rule additionally requires every external _call_
   lifted into a job to be idempotent on its own or gated by a pre-call
   aggregate-state check — the same request-mint / OffchainOrder pattern that
   checks whether the call already landed before re-issuing it. This load ->
   mutate -> record -> proceed ordering is what keeps a crashed-and-retried job
   safe: the retry re-reads the persisted outcome and skips the work already
   done instead of duplicating the external mutation.

## Consequences

### Positive

- Unblocks the RAI-924 bump: every liquidity handler becomes pure, so the
  Services -> Jobs flip is mechanical.
- One tested home for the onchain step wrappers; the recovery duplication is
  deleted rather than re-templated four times.
- No second orchestration layer — the durable execution semantics (retry,
  fail-stop, recovery handoff) stay exactly where they are today.
- Crash-safety improves where it was still open: a mint that crashes between
  `request_mint` and the event commit no longer **loses** the request, because
  the request is an event and the external call lives in the durable job. This
  buys aggregate-state idempotency — the request survives the crash and is
  re-driven — not end-to-end replay-safety on its own: not duplicating the
  external call on retry is the job's responsibility (Decision 6), through an
  idempotent call or a pre-call aggregate-state check.

### Negative / costs

- Mint and redemption gain intermediate aggregate states/events (a pure
  "requested" state distinct from "accepted", outcome events for poll results).
  More events on the audit trail and more command arms, though each arm is
  trivial.
- `resume_redemption` grows: redemption's I/O currently lives entirely in
  handlers, so more logic moves into the orchestrator than for mint.
- The extraction is large (four RAI-926-sized PRs) and touches financial
  crash-recovery paths; each PR must preserve exact event payloads and recovery
  invariants.

### Neutral

- Event payloads already persisted stay frozen; new request/outcome events are
  additive.
- The existing job wiring (workers, circuits, reactors, failure injection) is
  unchanged; only what the orchestrators call changes.

## Alternatives considered

- **New per-concern jobs (RAI-926 style) for the equity family.** Would mirror
  OffchainOrder literally but create a second orchestration layer alongside
  `CrossVenueEquityTransfer`, splitting the workflow across two drivers and
  duplicating recovery/retry policy. Rejected: the orchestrator already exists
  and already sends pure commands for half the workflow.
- **Keep `Services` and only bump where handlers happen to be pure.** 0.2.0-rc2
  has no `Services` associated type; a hybrid is not expressible. The whole
  family must be pure before the bump.
- **Per-aggregate copies of the step wrappers (skip RAI-927).** Four divergent
  copies of wrap/deposit logic across mint + two recovery aggregates; drift is
  guaranteed in financial code. Rejected.
- **Move recovery failures to job errors instead of events.** Would lose the
  first-class `RecoveryFailed` audit entries the recovery design depends on.
  Rejected.

## Follow-ups

- **RAI-927** — land the shared onchain step wrappers + error type + unit tests;
  rewire `CrossVenueEquityTransfer`'s mint orchestrator onto them as the first
  consumer. Unblocks the per-aggregate work.
- **RAI-928** — `TokenizedEquityMint`: pure `RequestMint` (emit intent) + Poll;
  `request_mint` / `poll_mint_until_complete` move into the orchestrator with
  outcome commands.
- **RAI-929** — `EquityRedemption`: withdraw / unwrap / send move into
  `resume_redemption`; handlers record outcomes.
- **RAI-930 / RAI-931** — recovery aggregates: helper I/O moves into each
  recovery job's `perform` loop onto the shared wrappers, deleting the duplicate
  `*_or_fail` SDK bodies; `RecoveryFailed` preserved via an outcome command.
- **RAI-924** — flip `type Services` -> `type Jobs`, handlers sync, pushes
  through the buffered `JobQueue`; pin event-sorcery 0.2.0-rc2.
- **RAI-925** — excise the local `conductor::job` machinery the library now
  owns.
