# adrs/

Architecture Decision Records (ADRs) for `st0x.liquidity`. Each ADR captures a
single significant architectural decision: the context that forced a choice, the
options considered, the option taken, and the consequences accepted.

## What an ADR is

An ADR is an **immutable historical record** of a decision at the moment it was
made. It is not a living document. Once accepted, the body of an ADR is not
edited to reflect later developments — instead, a new ADR either supersedes the
old one or amends it, and the old one's status is updated to point at the
successor.

Treat ADRs the way you treat git history: append-only, dated, signed by context.

## When to write one

Write an ADR when a decision is:

- **Architectural** — affects how components fit together, what responsibilities
  a layer has, or what the public contract of a subsystem is.
- **Non-obvious** — a future contributor (human or agent) would reasonably
  wonder "why did we do it this way?" without it.
- **Costly to reverse** — replacing a framework, switching a serialization
  format, restructuring a workspace, picking a domain modeling pattern.

If a decision is local, easily reversible, or fully captured by the code itself,
it does not need an ADR. Naming conventions, domain terminology, and how-to
guides belong in [`/docs`](../docs/) instead.

The repository root [`AGENTS.md`](../AGENTS.md) gives the standing rule: when an
architectural decision is not already answered by existing docs, write an ADR
here, summarize it briefly for review, and stop for approval before proceeding
with that direction.

## Approval workflow

ADRs follow the same review path as code: a contributor opens a PR with the new
ADR file, the rest of the team reviews on GitHub, and the ADR moves to
**Accepted** when the PR is approved and merged. There is no separate approval
channel — the merge IS the approval, and the merge commit IS the dated record.

While a PR is open, the ADR's `Status:` header stays **Proposed**. The author
flips it to **Accepted** in the same PR once reviewers approve, before merging.

## File layout

```
adrs/
  README.md                     # this file
  NNNN-short-kebab-name.md      # one ADR per decision
```

- `NNNN` is a zero-padded four-digit index, allocated sequentially. The next ADR
  is the highest existing index plus one. Indices are never reused, even if an
  ADR is superseded.
- The kebab name should be terse and decision-shaped (e.g.
  `0002-axum-and-tower.md`, not `0002-web-framework.md`).

## ADR template

```markdown
# ADR NNNN: <decision in one line>

- Status: Proposed | Accepted | Superseded by ADR-XXXX | Deprecated
- Date: YYYY-MM-DD

## Context

What forced the decision? What constraints, requirements, or pain points are in
play? What did we know at the time?

## Decision

What we decided, stated affirmatively. If the decision has multiple parts,
enumerate them.

## Consequences

### Positive

What we gain.

### Negative / costs

What we accept as the price. Be honest — an ADR with no costs section is
suspicious.

### Neutral

Side effects that are neither wins nor losses.

## Alternatives considered

The options we rejected, with one or two sentences each on why.

## Follow-ups

Concrete migration work, documentation tasks, or audits that fall out of the
decision.
```

## Statuses

- **Proposed** — drafted, awaiting review. Do not act on the decision yet.
- **Accepted** — approved by the team. The decision is in force.
- **Superseded by ADR-XXXX** — overridden by a later ADR. The body stays as
  written; only the status header changes, and the superseding ADR links back.
- **Deprecated** — the decision no longer applies but no replacement was
  recorded.

## Index

| #                                                                       | Title                                                                                | Status   |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | -------- |
| [0001](0001-cash-bp-for-equity-hedges.md)                               | Use Alpaca `cash` (not `non_marginable_buying_power`) for equity hedge preflight     | Accepted |
| [0002](0002-axum-and-tower.md)                                          | Adopt Axum and lean on Tower for both transport and business logic                   | Accepted |
| [0003](0003-durable-usdc-rebalance-guard-on-restart.md)                 | Reconstruct the USDC rebalancing guard from persisted state on startup               | Accepted |
| [0004](0004-typed-identifiers.md)                                       | Model identifiers by what determines them; derive the string form                    | Accepted |
| [0005](0005-max-token-approvals-on-startup.md)                          | Grant one-time MAX token approvals to trusted spenders on startup                    | Accepted |
| [0006](0006-base-to-alpaca-explicit-alpaca-deposit-send.md)             | BaseToAlpaca deposit sends USDC to Alpaca explicitly, idempotently                   | Accepted |
| [0008](0008-counter-trading-during-dividend-freeze.md)                  | Counter-trading (broker hedging) during a dividend freeze                            | Proposed |
| [0009](0009-record-overfill-as-acceptance.md)                           | Record a broker over-fill as an acceptance, not a failure                            | Accepted |
| [0010](0010-bounded-pending-ack-set-for-cross-process-exactly-once.md)  | Bounded pending-acknowledgement set for cross-process exactly-once                   | Accepted |
| [0014](0014-runtime-stuck-pending-recovery-and-serialized-placement.md) | Recover stuck Pending placements at runtime, and serialize broker-placement attempts | Accepted |
| [0015](0015-extract-st0x-finance-without-changing-its-contract.md)      | Extract `st0x-finance` without changing its contract                                 | Proposed |
