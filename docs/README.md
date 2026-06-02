# docs/

Living reference documentation for `st0x.liquidity`. Read when relevant to your
current task; update when you discover something a future contributor (human or
agent) would benefit from knowing.

## What belongs here

- **How-to guides** — concrete procedures (e.g.
  [how-to-add-new-asset.md](how-to-add-new-asset.md), [cli-ops.md](cli-ops.md)).
- **Domain references** — naming, terminology, conceptual models (e.g.
  [domain.md](domain.md), [float.md](float.md)).
- **Subsystem deep-dives** — non-obvious behavior of internal components (e.g.
  [conductor.md](conductor.md), [cqrs.md](cqrs.md)).
- **Framework / library notes** — gotchas, idioms, integration pitfalls for the
  external dependencies we use heavily (e.g. [alloy.md](alloy.md),
  [sqlx.md](sqlx.md), [ttdd.md](ttdd.md), [feature-flags.md](feature-flags.md),
  [observability.md](observability.md)).

## File organization

- **Naming**: kebab-case (e.g. `how-to-add-new-asset.md`, `feature-flags.md`).
- **Granularity**: new file when the topic is a standalone how-to, domain
  reference, subsystem deep-dive, or framework note that another doc would not
  reasonably absorb. Otherwise extend an existing doc with a new section.

## What does NOT belong here

- **Architectural decisions.** Those go in [`/adrs`](../adrs/) at the repo root.
  ADRs are immutable historical records of _why_ we chose one option over
  others; `docs/` entries are mutable references describing _what_ the system
  currently does and _how_ to work with it.
- **The product specification.** That lives in [SPEC.md](../SPEC.md).
- **The roadmap.** That lives in [ROADMAP.md](../ROADMAP.md).

## Lifecycle

These documents are expected to drift unless maintained. When research or
trial-and-error reveals a non-obvious pattern, pitfall, or framework behavior,
document it here (in the relevant existing file or a new one) so the next person
does not have to rediscover it. Prioritize: framework-specific idioms,
integration gotchas, and "X doesn't work because Y" findings.

If a doc here describes behavior that has changed, update it in the same PR that
changes the behavior. If you delete a feature, delete or revise the doc.

## Index

| File                                               | Purpose                                                                    |
| -------------------------------------------------- | -------------------------------------------------------------------------- |
| [alloy.md](alloy.md)                               | Alloy types, `FixedBytes` aliases, mocks, encoding                         |
| [cli-ops.md](cli-ops.md)                           | CLI operational procedures                                                 |
| [conductor.md](conductor.md)                       | Conductor orchestration layer (task supervisor, apalis workers, lifecycle) |
| [cqrs.md](cqrs.md)                                 | Event sourcing with `st0x-event-sorcery`                                   |
| [domain.md](domain.md)                             | Domain terminology and naming conventions (source of truth for names)      |
| [feature-flags.md](feature-flags.md)               | Cargo feature flags, `test-support` vs `cfg(test)`                         |
| [float.md](float.md)                               | `Float` type, precision-safe financial arithmetic                          |
| [how-to-add-new-asset.md](how-to-add-new-asset.md) | Procedure for onboarding a new tokenized equity                            |
| [observability.md](observability.md)               | Tracing, metrics, logging conventions                                      |
| [sqlx.md](sqlx.md)                                 | sqlx usage notes and pitfalls                                              |
| [ttdd.md](ttdd.md)                                 | Type-driven TDD workflow                                                   |
