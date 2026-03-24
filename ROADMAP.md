# Roadmap

This document tracks the current goal, upcoming milestones, and backlog. The
current focus section has full task breakdown with dependency graphs. Next
milestones are goals we know are coming but haven't fully planned. Backlog is
grouped by system aspect.

---

## Current priority: go live with counter trading again

> NOTE: mostly operational, so no issues/milestone

- [x] Give @alastairong access to our CLI to handover ops
- [x] Make sure the CD pipeline is working
  - PR:
    [#503 Unify CI/CD pipeline, split Nix builds, modernize deployment infra](https://github.com/ST0x-Technology/st0x.liquidity/pull/503)
- [ ] [#512 Add travel rule info to Alpaca crypto withdrawal requests](https://github.com/ST0x-Technology/st0x.liquidity/issues/512)
- [ ] Deploy RKLB counter trading only with raw private key
- [ ] Deploy RKLB-only counter trading with turnkey
- [ ] Prod turnkey counter trading all assets except RKLB

## Live counter trading with auto-rebalancing in prod

Manual rebalancing cannot keep up with liquidity demand. And auto-rebalancing
that works when everything goes well but requires an additional manual
intervention to complete a stuck cross-venue transfer is still an improvement.
All the core auto-rebalancing logic is ready so let's start getting some
benefits out of it.

[Milestone: Live counter trading with auto-rebalancing in prod](https://github.com/ST0x-Technology/st0x.liquidity/milestone/1)

The `prod` branch tracks automatic deployments. PR
[#499 give alastair access to the remote cli](https://github.com/ST0x-Technology/st0x.liquidity/pull/499)
is the remaining deploy-related PR in scope.

> Legend: 🔵 in progress | 🟡 ready (deps done or have stable branches to stack
> on) | 🔴 blocked

```mermaid
graph LR
    classDef wip fill:#1a2e5c,stroke:#3b82f6,color:#93c5fd
    classDef ready fill:#5c4a1a,stroke:#f59e0b,color:#fcd34d
    classDef blocked fill:#5c1a1a,stroke:#ef4444,color:#fca5a5

    id296["#296 Apalis infra<br/>PR #483"]:::wip
    id421["#421 Order lifecycle jobs"]:::ready
    id434["#434 Turnkey in production"]:::ready
    id181["#181 Trade history"]:::ready
    id407["#407 Cash reserve<br/>PR #484"]:::wip
    id491["#491 Trading toggle<br/>PR #492"]:::wip
    id449["#449 Transfer ID tracking"]:::ready
    ops(["Ops: validate rebalancing<br/>#443 #444 #445 #446"]):::blocked

    id296 --> id421
    id407 --> id491 --> ops
    id449 --> ops
    id421 & id434 & id181 --> ops

    click id296 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/296"
    click id421 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/421"
    click id434 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/434"
    click id181 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/181"
    click id407 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/407"
    click id491 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/491"
    click id449 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/449"
```

- [ ] [#296 Set up apalis + task-supervisor infrastructure and convert event processing pipeline](https://github.com/ST0x-Technology/st0x.liquidity/issues/296)
  - PR:
    [#483 redesign the orchestration layer](https://github.com/ST0x-Technology/st0x.liquidity/pull/483)
- [ ] [#421 Convert order lifecycle to apalis jobs](https://github.com/ST0x-Technology/st0x.liquidity/issues/421)
- [ ] [#434 Use Turnkey for all onchain operations](https://github.com/ST0x-Technology/st0x.liquidity/issues/434)
- [ ] [#181 Dashboard: Trade History Panel](https://github.com/ST0x-Technology/st0x.liquidity/issues/181)
- [ ] [#407 Untouchable cash reserve for rebalancing](https://github.com/ST0x-Technology/st0x.liquidity/issues/407)
  - PR:
    [#484 add an untouchable brokerage cash reserve](https://github.com/ST0x-Technology/st0x.liquidity/pull/484)
- [ ] [#491 Runtime trading enable/disable toggle](https://github.com/ST0x-Technology/st0x.liquidity/issues/491)
  - PR:
    [#492 feat/trading-switch](https://github.com/ST0x-Technology/st0x.liquidity/pull/492)
- [ ] [#449 Track active transfer aggregate IDs in inventory view](https://github.com/ST0x-Technology/st0x.liquidity/issues/449)

Ops (no code changes — validate rebalancing in production):

- [ ] [#443 See end-to-end USDC auto rebalancing in both directions](https://github.com/ST0x-Technology/st0x.liquidity/issues/443)
- [ ] [#444 See repeated USDC auto rebalancing](https://github.com/ST0x-Technology/st0x.liquidity/issues/444)
- [ ] [#445 See repeated single-equity auto rebalancing with counter trading](https://github.com/ST0x-Technology/st0x.liquidity/issues/445)
- [ ] [#446 Get a day of stable multi-equity auto rebalancing with counter trading](https://github.com/ST0x-Technology/st0x.liquidity/issues/446)

### Completed

- [x] [#447 Safely go live with production counter trading and auto-rebalancing](https://github.com/ST0x-Technology/st0x.liquidity/issues/447)
- [x] [#377 Dashboard backend: serve inventory history and transfer status via WebSocket](https://github.com/ST0x-Technology/st0x.liquidity/issues/377)
  - PR:
    [#482 implement the inventory panel](https://github.com/ST0x-Technology/st0x.liquidity/pull/482)
- [x] [#453 Live bidirectional equity auto-rebalancing](https://github.com/ST0x-Technology/st0x.liquidity/issues/453)
- [x] [#354 Replace Fireblocks with Turnkey for onchain transaction signing](https://github.com/ST0x-Technology/st0x.liquidity/issues/354)
  - PR:
    [#390 add turnkey support to st0x-evm](https://github.com/ST0x-Technology/st0x.liquidity/pull/390)
- [x] [#380 Configure wallet provider selection (Turnkey vs Fireblocks) in main crate](https://github.com/ST0x-Technology/st0x.liquidity/issues/380)
  - PR:
    [#394 Wallet selection on hedge bot](https://github.com/ST0x-Technology/st0x.liquidity/pull/394)
- [x] [#376 Review and update DTO types for inventory snapshots and transfer status](https://github.com/ST0x-Technology/st0x.liquidity/issues/376)
  - PR:
    [#382 update DTO types for inventory snapshots and transfer status](https://github.com/ST0x-Technology/st0x.liquidity/pull/382)
- [x] [#378 Dashboard frontend: inventory and transfer status panels](https://github.com/ST0x-Technology/st0x.liquidity/issues/378)
  - PR:
    [#392 add frontend components for displaying inventory information](https://github.com/ST0x-Technology/st0x.liquidity/pull/392)
- [x] [#435 System observability sufficient for production monitoring](https://github.com/ST0x-Technology/st0x.liquidity/issues/435)

---

## Order Taker Bot

A separate bot that monitors Raindex orderbooks and takes profitable orders
rather than providing liquidity. Shares infrastructure with the hedge bot via
a common workspace crate.

- PR: [#357 St0x direct taker spec](https://github.com/ST0x-Technology/st0x.liquidity/pull/357)

```mermaid
graph LR
    classDef wip fill:#1a2e5c,stroke:#3b82f6,color:#93c5fd

    scaffold["Scaffold<br/>PR #529"]:::wip
    collector["Order Collector<br/>PR #530"]:::wip
    classify["Order Classification<br/>PR #532"]:::wip
    profit["Profitability Strategy<br/>PR #535"]:::wip

    scaffold --> collector --> classify --> profit
```

- [ ] Project scaffold and shared crate extraction
  - PR:
    [#529 Taker Bot: project scaffolding and shared crate extraction](https://github.com/ST0x-Technology/st0x.liquidity/pull/529)
- [ ] Order collector
  - PR:
    [#530 Taker Bot: order collector](https://github.com/ST0x-Technology/st0x.liquidity/pull/530)
- [ ] Order classification
  - PR:
    [#532 Taker Bot: Order Classification](https://github.com/ST0x-Technology/st0x.liquidity/pull/532)
- [ ] Profitability strategy
  - PR:
    [#535 Taker Bot: profitability strategy](https://github.com/ST0x-Technology/st0x.liquidity/pull/535)

---

## Robust auto-recovery

Cover all the edge cases and implement recovery mechanisms for recovering stuck
funds or reconciling bookkeeping without needing manual interventions.

[Milestone: Robust auto-recovery](https://github.com/ST0x-Technology/st0x.liquidity/milestone/2)

> Legend: 🔵 in progress | 🔴 blocked

```mermaid
graph LR
    classDef wip fill:#1a2e5c,stroke:#3b82f6,color:#93c5fd
    classDef blocked fill:#5c1a1a,stroke:#ef4444,color:#fca5a5

    id433["#433 In-flight balances"]:::blocked
    id427["#427 Tokenization tracking<br/>PR #481"]:::wip
    id423["#423 Inventory polling jobs"]:::blocked
    id425["#425 USDC dispatch"]:::blocked
    id452["#452 Equity dispatch"]:::blocked
    id436["#436 USDC on Alpaca"]:::blocked
    id437["#437 USDC on Ethereum"]:::blocked
    id438["#438 USDC on Base"]:::blocked
    id441["#441 All in-flight USDC"]:::blocked
    id439["#439 Wrapped equity"]:::blocked
    id440["#440 Unwrapped equity"]:::blocked
    id442(["#442 All in-flight capital"]):::blocked

    id433 & id425 --> id436 & id437 & id438 --> id441
    id427 & id452 --> id439 & id440
    id441 & id439 & id440 --> id442

    click id433 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/433"
    click id427 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/427"
    click id423 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/423"
    click id425 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/425"
    click id452 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/452"
    click id436 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/436"
    click id437 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/437"
    click id438 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/438"
    click id441 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/441"
    click id439 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/439"
    click id440 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/440"
    click id442 href "https://github.com/ST0x-Technology/st0x.liquidity/issues/442"
```

- [ ] [#433 Extend inventory snapshot to include in-flight balances](https://github.com/ST0x-Technology/st0x.liquidity/issues/433)
- [ ] [#427 Track tokenization requests and set in-flight balance accordingly](https://github.com/ST0x-Technology/st0x.liquidity/issues/427)
  - PR:
    [#481 Track in-flight tokenization requests and set inflight balance accordingly](https://github.com/ST0x-Technology/st0x.liquidity/pull/481)
- [ ] [#423 Convert inventory polling and rebalancing to apalis jobs](https://github.com/ST0x-Technology/st0x.liquidity/issues/423)
- [ ] [#425 Unify USDC recovery dispatch with standard transfer lifecycle](https://github.com/ST0x-Technology/st0x.liquidity/issues/425)
- [ ] [#452 Unify equity recovery dispatch with standard transfer lifecycle](https://github.com/ST0x-Technology/st0x.liquidity/issues/452)
- [ ] [#436 Recover from detected USDC on Alpaca](https://github.com/ST0x-Technology/st0x.liquidity/issues/436)
- [ ] [#437 Recover from detected USDC on Ethereum](https://github.com/ST0x-Technology/st0x.liquidity/issues/437)
- [ ] [#438 Recover from detected USDC on Base](https://github.com/ST0x-Technology/st0x.liquidity/issues/438)
- [ ] [#441 Recover from all detected in-flight USDC](https://github.com/ST0x-Technology/st0x.liquidity/issues/441)
- [ ] [#439 Recover from detected wrapped equity tokens on Base](https://github.com/ST0x-Technology/st0x.liquidity/issues/439)
- [ ] [#440 Recover from detected unwrapped equity tokens on Base](https://github.com/ST0x-Technology/st0x.liquidity/issues/440)
- [ ] [#442 Recover from any detected in-flight capital](https://github.com/ST0x-Technology/st0x.liquidity/issues/442)

### Completed

- [x] [#428 Check USDC balance in Alpaca crypto wallet](https://github.com/ST0x-Technology/st0x.liquidity/issues/428)
  - PR:
    [#485 Check USDC balance in Alpaca crypto wallet](https://github.com/ST0x-Technology/st0x.liquidity/pull/485)
- [x] [#429 Ethereum USDC balance polling](https://github.com/ST0x-Technology/st0x.liquidity/issues/429)
  - PR:
    [#459 Poll Ethereum wallet USDC balance in inventory service](https://github.com/ST0x-Technology/st0x.liquidity/pull/459)
- [x] [#430 Base USDC balance polling (outside Raindex)](https://github.com/ST0x-Technology/st0x.liquidity/issues/430)
  - PR:
    [#460 Poll Base wallet USDC balance in inventory service](https://github.com/ST0x-Technology/st0x.liquidity/pull/460)
- [x] [#431 Base unwrapped equity token balance polling](https://github.com/ST0x-Technology/st0x.liquidity/issues/431)
  - PR:
    [#461 Add Base wallet equity token balance polling](https://github.com/ST0x-Technology/st0x.liquidity/pull/461)
- [x] [#432 Base wrapped equity token balance polling](https://github.com/ST0x-Technology/st0x.liquidity/issues/432)
  - PR:
    [#476 Add Base wrapped equity token balance polling](https://github.com/ST0x-Technology/st0x.liquidity/pull/476)

---

## Next milestones

### Harden production

Once auto-rebalancing is live, priority shifts to making the deployment robust
and observable. The system runs on a remote server handling real capital --
securing access, encrypting traffic, and having visibility into what's happening
are prerequisites for operating with confidence. The quant researcher also needs
execution data and monitoring tools to analyze and optimize trading parameters.

#### Infrastructure security

- [ ] [#513 Single source of truth for Nix secret declarations](https://github.com/ST0x-Technology/st0x.liquidity/issues/513)
- [ ] [#286 No TLS configured for dashboard and WebSocket traffic](https://github.com/ST0x-Technology/st0x.liquidity/issues/286)
- [ ] [#293 Granular SSH access control: limit root access and per-user deploy keys](https://github.com/ST0x-Technology/st0x.liquidity/issues/293)
- [ ] [#294 Document secret management setup and opsec](https://github.com/ST0x-Technology/st0x.liquidity/issues/294)

#### Release process

- [ ] [#456 Decide for a prod release trigger](https://github.com/ST0x-Technology/st0x.liquidity/issues/456)

#### Reliability

- [ ] [#263 Check offchain inventory before placing counter trades](https://github.com/ST0x-Technology/st0x.liquidity/issues/263)
- [ ] [#277 No automatic retry after offchain order failure](https://github.com/ST0x-Technology/st0x.liquidity/issues/277)
- [ ] [#240 Conversion slippage not tracked, causing inventory drift](https://github.com/ST0x-Technology/st0x.liquidity/issues/240)
- [ ] [#469 Verify all onchain operations await required confirmations](https://github.com/ST0x-Technology/st0x.liquidity/issues/469)
- [ ] [#16 Handle reorgs](https://github.com/ST0x-Technology/st0x.liquidity/issues/16)
- [ ] [#404 Stuck transfer blocks rebalancing with no timeout or recovery](https://github.com/ST0x-Technology/st0x.liquidity/issues/404)

#### Remaining job conversions

De-scoped from go-live because they self-heal on the next cycle. Converting to
apalis improves reliability and gives better observability into job health.

- [ ] [#422 Convert position checker to apalis job](https://github.com/ST0x-Technology/st0x.liquidity/issues/422)
- [ ] [#424 Convert startup operations and executor maintenance to apalis jobs and supervised tasks](https://github.com/ST0x-Technology/st0x.liquidity/issues/424)

#### Quant research data

Ensure the event store captures all data needed for quantitative research --
execution timing, market conditions, and operational metrics.

- [ ] [#303 Audit external integrations: record start/end timestamps for all calls in event store](https://github.com/ST0x-Technology/st0x.liquidity/issues/303)
- [ ] [#338 Benchmark Alpaca tokenization pipeline latency for external partners](https://github.com/ST0x-Technology/st0x.liquidity/issues/338)

#### Dashboard

- [ ] [#399 Dashboard: surface transfer error details and transaction hashes](https://github.com/ST0x-Technology/st0x.liquidity/issues/399)

### Request for liquidity flow

(Future -- no issues yet)

### Improve capital efficiency

(Future -- no issues yet)

---

## Backlog

Work that doesn't fit into the current epics. These are organized by area but
not yet prioritized as epic goals.

- [x] [#516 `alpaca-withdraw` offers no way to specify destination](https://github.com/ST0x-Technology/st0x.liquidity/issues/516)
  - PR:
    [#517 CLI Withdraw improvement](https://github.com/ST0x-Technology/st0x.liquidity/pull/517)
- [x] [#510 UsdcRebalance aggregate loses original operation start time across phase boundaries](https://github.com/ST0x-Technology/st0x.liquidity/issues/510)
  - PR:
    [#519 Preserve original USDC rebalance start time across phases](https://github.com/ST0x-Technology/st0x.liquidity/pull/519)
- [x] [#525 Alpaca rejects orders with more than 9 decimal places](https://github.com/ST0x-Technology/st0x.liquidity/issues/525)
  - PR:
    [#521 fix: alpaca 9 decimal points precision fix](https://github.com/ST0x-Technology/st0x.liquidity/pull/521)
- [ ] [#528 Alpaca tokenization polling fails to deserialize list requests response](https://github.com/ST0x-Technology/st0x.liquidity/issues/528)
  - PR:
    [#536 fix: harden Alpaca tokenization polling against malformed request history](https://github.com/ST0x-Technology/st0x.liquidity/pull/536)
- [ ] [#531 Add missing commands to the CLI](https://github.com/ST0x-Technology/st0x.liquidity/issues/531)
  - PR:
    [#540 feat: add direct CLI wrap-equity command](https://github.com/ST0x-Technology/st0x.liquidity/pull/540)
  - PR:
    [#538 feat: add direct CLI unwrap-equity command](https://github.com/ST0x-Technology/st0x.liquidity/pull/538)
  - PR:
    [#537 feat: add generic Raindex vault withdraw CLI](https://github.com/ST0x-Technology/st0x.liquidity/pull/537)
- [ ] [#488 Inconsistent rebalancing semantics: whitelist config vs blacklist trigger](https://github.com/ST0x-Technology/st0x.liquidity/issues/488)
- [ ] [#500 Resume interrupted tokenization aggregates after restart](https://github.com/ST0x-Technology/st0x.liquidity/issues/500)

### Fireblocks Contract Calls

### Event-sorcery framework

- [ ] [#489 Aggregate evolve transitions silently drop earlier-state context](https://github.com/ST0x-Technology/st0x.liquidity/issues/489)
- [ ] [#465 Add test utilities to event-sorcery (regression testing, schema version management)](https://github.com/ST0x-Technology/st0x.liquidity/issues/465)
- [ ] [#467 Plan event-sorcery library extraction and backend agnosticism](https://github.com/ST0x-Technology/st0x.liquidity/issues/467)
  - Move corresponding issues to new repo when extracted
  - Make library agnostic to the `cqrs-es` backend
- [ ] [#450 Persisted multi-entity Reactor in event-sorcery](https://github.com/ST0x-Technology/st0x.liquidity/issues/450)
- [ ] [#451 Event replay for in-memory Reactors on startup](https://github.com/ST0x-Technology/st0x.liquidity/issues/451)

### Trading improvements

- [x] [#514 CLI buy/sell commands reject fractional quantities](https://github.com/ST0x-Technology/st0x.liquidity/issues/514)
  - PR:
    [#518 CLI buy/sell commands accept fractional quantities](https://github.com/ST0x-Technology/st0x.liquidity/pull/518)
- [ ] [#413 Add 24/5 limit order support during market close](https://github.com/ST0x-Technology/st0x.liquidity/issues/413)
  - PR:
    [#533 feat: add manual Alpaca Broker API limit orders for extended hours](https://github.com/ST0x-Technology/st0x.liquidity/pull/533)
- [ ] [#322 CLI should not depend on database](https://github.com/ST0x-Technology/st0x.liquidity/issues/322)

### Admin dashboard

#### Design & specification

- [ ] [#470 Explore CLI/dashboard design for improved usability and consistency](https://github.com/ST0x-Technology/st0x.liquidity/issues/470)

#### Core functionality

- [ ] [#400 Dashboard: add Raindex vault links to inventory equity rows](https://github.com/ST0x-Technology/st0x.liquidity/issues/400)
- [ ] [#401 Dashboard: distinguish enabled vs disabled assets with toggle](https://github.com/ST0x-Technology/st0x.liquidity/issues/401)
- [ ] [#405 Dashboard: per-stage transfer balance visibility](https://github.com/ST0x-Technology/st0x.liquidity/issues/405)
- [ ] [#406 Inventory updates require page refresh after rebalancing operations](https://github.com/ST0x-Technology/st0x.liquidity/issues/406)
  - PR:
    [#410 add poll_notify to trigger immediate inventory re-poll](https://github.com/ST0x-Technology/st0x.liquidity/pull/410)
- [x] [#233 Historical data display on dashboard](https://github.com/ST0x-Technology/st0x.liquidity/issues/233)

#### Advanced features

- [ ] [#472 Integrate Effect library into dashboard for improved UX](https://github.com/ST0x-Technology/st0x.liquidity/issues/472)
- [ ] [#178 Dashboard: Performance Metrics Panel](https://github.com/ST0x-Technology/st0x.liquidity/issues/178)
- [ ] [#180 Dashboard: Spreads Panel](https://github.com/ST0x-Technology/st0x.liquidity/issues/180)
- [ ] [#183 Dashboard: Circuit Breaker](https://github.com/ST0x-Technology/st0x.liquidity/issues/183)
- [ ] [#184 Dashboard: Schwab OAuth Integration](https://github.com/ST0x-Technology/st0x.liquidity/issues/184)
- [ ] [#185 Dashboard: Grafana Embedding](https://github.com/ST0x-Technology/st0x.liquidity/issues/185)
- [ ] [#186 Dashboard: HyperDX Health Status](https://github.com/ST0x-Technology/st0x.liquidity/issues/186)

### Architecture & code quality

Improve type system, naming consistency, mock configurations, and module
extraction.

#### Web framework & comms

- [ ] [#408 Migrate from Rocket to Axum](https://github.com/ST0x-Technology/st0x.liquidity/issues/408)
  - PR:
    [#507 Replace Rocket with Axum web framework](https://github.com/ST0x-Technology/st0x.liquidity/pull/507)
  - Unlocks apalis-board for job/worker/queue observability; better
    composability with tower-based deps
- [ ] Unify websocket comms
  - PR:
    [#501 unify the websocket comms](https://github.com/ST0x-Technology/st0x.liquidity/pull/501)

#### Type system & domain modeling

- [ ] [#473 Implement type-level newtype composition (Tagged qualifiers)](https://github.com/ST0x-Technology/st0x.liquidity/issues/473)
- [ ] [#468 Create abstraction for converting between domain types and DTOs](https://github.com/ST0x-Technology/st0x.liquidity/issues/468)

#### Naming & refactoring

- [ ] [#509 WalletPollingCtx: narrow wallet fields from dyn Wallet to dyn Evm + Address](https://github.com/ST0x-Technology/st0x.liquidity/issues/509)
- [ ] [#464 Rename `*_cash` fields to `*_usdc` in InventorySnapshot and related types](https://github.com/ST0x-Technology/st0x.liquidity/issues/464)
- [ ] [#466 Fix boolean blindness in `wrapper::mock` and other mock modules](https://github.com/ST0x-Technology/st0x.liquidity/issues/466)

#### Build & dependencies

- [ ] [#471 Remove submodule dependencies and provide ABIs via nix derivations](https://github.com/ST0x-Technology/st0x.liquidity/issues/471)
- [ ] [#479 Verify CI clippy runs without redundant -D flags](https://github.com/ST0x-Technology/st0x.liquidity/issues/479)
- [ ] [#505 Upstream float-serde and float-macro into rain-math-float repo](https://github.com/ST0x-Technology/st0x.liquidity/issues/505)

#### Monolith extraction

Split monolith into focused crates for faster builds, stricter abstraction
boundaries, and reduced coupling.

- [ ] [#54 Create BrokerAuthProvider trait to abstract authentication across codebase](https://github.com/ST0x-Technology/st0x.liquidity/issues/54)
- [ ] [#269 Extract st0x-tokenization crate with Tokenizer trait](https://github.com/ST0x-Technology/st0x.liquidity/issues/269)
- [ ] [#270 Extract st0x-vault crate with Vault trait](https://github.com/ST0x-Technology/st0x.liquidity/issues/270)
- [ ] [#271 Extract st0x-rebalance crate](https://github.com/ST0x-Technology/st0x.liquidity/issues/271)
- [ ] [#272 Convert st0x-hedge to library crate and create st0x-server binary](https://github.com/ST0x-Technology/st0x.liquidity/issues/272)

### Black-box e2e testing

- [ ] [#455 Leaky tests](https://github.com/ST0x-Technology/st0x.liquidity/issues/455)
  - PR:
    [#487 fix flaky tests](https://github.com/ST0x-Technology/st0x.liquidity/pull/487)
- [ ] E2e: fire all trades concurrently and assert eventual consistency
  - PR:
    [#527 e2e: fire all trades concurrently and assert eventual consistency](https://github.com/ST0x-Technology/st0x.liquidity/pull/527)
- [ ] [#416 E2e tests expose crate internals that should not be public](https://github.com/ST0x-Technology/st0x.liquidity/issues/416)
- [ ] [#417 No chaos/fault injection testing for failure recovery](https://github.com/ST0x-Technology/st0x.liquidity/issues/417)
- [ ] [#418 No invariant monitoring under randomized workloads](https://github.com/ST0x-Technology/st0x.liquidity/issues/418)
- [ ] [#419 No adversarial environment simulation in e2e tests](https://github.com/ST0x-Technology/st0x.liquidity/issues/419)

---

## Completed

### Completed: Testing, tooling, and bug fixes (Mar 2026)

- [x] [#312 Decimal precision artifacts from Rain Float and U256 conversions cause production failures](https://github.com/ST0x-Technology/st0x.liquidity/issues/312)
  - PR:
    [#347 Use rain Float instead of Decimal](https://github.com/ST0x-Technology/st0x.liquidity/pull/347)
- [x] [#402 Dashboard: add block explorer links for bot address and transfers](https://github.com/ST0x-Technology/st0x.liquidity/issues/402)
- [x] Full e2e test suite
  - PR:
    [#346 full e2e test suite](https://github.com/ST0x-Technology/st0x.liquidity/pull/346)
  - PR:
    [#420 refactor e2e tests](https://github.com/ST0x-Technology/st0x.liquidity/pull/420)
  - PR:
    [#497 fix: e2e tests using the dashboard](https://github.com/ST0x-Technology/st0x.liquidity/pull/497)
- [x] Fix conductor race condition causing duplicate broker orders
  - PR:
    [#351 Fix conductor race condition](https://github.com/ST0x-Technology/st0x.liquidity/pull/351)
- [x] Configurable polling, CCTP overrides, SQLite auto-create
  - PR:
    [#342 Add configurations to bot ctx](https://github.com/ST0x-Technology/st0x.liquidity/pull/342)
  - PR:
    [#343 Configurable polling, CCTP overrides, SQLite auto-create](https://github.com/ST0x-Technology/st0x.liquidity/pull/343)
  - PR:
    [#344 httmock upgrade to 0.8](https://github.com/ST0x-Technology/st0x.liquidity/pull/344)
- [x] [#379 Dashboard integration: verify nix build, deployment, and end-to-end data flow](https://github.com/ST0x-Technology/st0x.liquidity/issues/379)

### Completed: Pre-production hardening (Mar 2026)

- [x] [#353 Per-asset operations config: independent trading/rebalancing toggles, vault IDs, operational limits](https://github.com/ST0x-Technology/st0x.liquidity/issues/353)
  - PR:
    [#355 per-asset operations config: independent trading/rebalancing toggles](https://github.com/ST0x-Technology/st0x.liquidity/pull/355)
- [x] [#332 No way to enable/disable individual asset markets](https://github.com/ST0x-Technology/st0x.liquidity/issues/332)
  - PR:
    [#335 add per-asset market enable/disable toggle](https://github.com/ST0x-Technology/st0x.liquidity/pull/335)
- [x] [#331 Add CLI command to journal equities between Alpaca accounts](https://github.com/ST0x-Technology/st0x.liquidity/issues/331)
  - PR:
    [#334 add CLI command to journal equities between Alpaca accounts](https://github.com/ST0x-Technology/st0x.liquidity/pull/334)
- [x] [#333 Alpaca transfer shows Processing then disappears from pending list](https://github.com/ST0x-Technology/st0x.liquidity/issues/333)
- [x] [#329 Trade validation rejects wrapped tokenized equity symbols (wt prefix)](https://github.com/ST0x-Technology/st0x.liquidity/issues/329)
  - PR:
    [#330 fix trade validation for wrapped tokenized equity symbols](https://github.com/ST0x-Technology/st0x.liquidity/pull/330)

### Completed: CI/CD Improvements

- [x] [#256 Implement CI/CD improvements from infra deployment spec](https://github.com/ST0x-Technology/st0x.liquidity/issues/256)
  - PR:
    [#259 Terraform infra provisioning and Nix-powered secret management and service deployments](https://github.com/ST0x-Technology/st0x.liquidity/pull/259)

### Completed: Production Fixes (Jan 2026)

- [x] [#251 Manual operations on trading venues not detected by inventory tracking](https://github.com/ST0x-Technology/st0x.liquidity/issues/251)
  - PR:
    [#253 Fix inventory tracking](https://github.com/ST0x-Technology/st0x.liquidity/pull/253)
- [x] [#234 Spec out infrastructure and deployment improvements](https://github.com/ST0x-Technology/st0x.liquidity/issues/234)
  - PR:
    [#237 Propose better deployment and infra setup](https://github.com/ST0x-Technology/st0x.liquidity/pull/237)
- PR:
  [#266 Fix cqrs inventory management](https://github.com/ST0x-Technology/st0x.liquidity/pull/266)

### Completed: Alpaca Production Go-Live

- [x] [#223 Auto-rebalancing flow missing USDC/USD conversion step](https://github.com/ST0x-Technology/st0x.liquidity/issues/223)
  - PR:
    [#227 usdc-usd conversion during auto-rebalancing](https://github.com/ST0x-Technology/st0x.liquidity/pull/227)
- [x] [#228 Set up Alpaca instance for real trading in production](https://github.com/ST0x-Technology/st0x.liquidity/issues/228)
  - PR:
    [#230 set up alpaca deployment configurations](https://github.com/ST0x-Technology/st0x.liquidity/pull/230)
- [x] [#229 Set up dashboard deployment](https://github.com/ST0x-Technology/st0x.liquidity/issues/229)
  - PR:
    [#230 set up alpaca deployment configurations](https://github.com/ST0x-Technology/st0x.liquidity/pull/230)
- [x] [#244 Implement dollar-value-based counter trade trigger to use alpaca fractional share trading feature](https://github.com/ST0x-Technology/st0x.liquidity/issues/244)
  - PR:
    [#236 implement dollar-threshold counter trade trigger condition](https://github.com/ST0x-Technology/st0x.liquidity/pull/236)
- [x] [#226 Refactor Alpaca services with Broker API client support](https://github.com/ST0x-Technology/st0x.liquidity/issues/226)
  - PR:
    [#225 refactor Alpaca services with Broker API client support](https://github.com/ST0x-Technology/st0x.liquidity/pull/225)

### Completed: Admin Dashboard Foundation

- [x] [#188 spec out the dashboard](https://github.com/ST0x-Technology/st0x.liquidity/issues/188)
  - PR:
    [#203 Admin Dashboard specification and roadmap](https://github.com/ST0x-Technology/st0x.liquidity/pull/203)
- [x] [#207 Initialize WS backend for the dashboard](https://github.com/ST0x-Technology/st0x.liquidity/issues/207)
  - PR:
    [#204 Add dashboard backend API](https://github.com/ST0x-Technology/st0x.liquidity/pull/204)
- [x] [#208 Initialize Svelte frontend with shadcn](https://github.com/ST0x-Technology/st0x.liquidity/issues/208)
  - PR:
    [#221 init dashboard frontend with SvelteKit and shadcn-svelte](https://github.com/ST0x-Technology/st0x.liquidity/pull/221)
- [x] [#209 Add dashboard skeleton and live event tracking](https://github.com/ST0x-Technology/st0x.liquidity/issues/209)
  - PR:
    [#206 Add dashboard skeleton and live event tracking](https://github.com/ST0x-Technology/st0x.liquidity/pull/206)

### Completed: CQRS/ES Migration with Automated Rebalancing

Migrated to event-sourced architecture through 3 phases. Automated rebalancing
implemented as first major feature on new architecture (Alpaca-only, Schwab
remains manual).

#### Phase 1: Dual-Write Foundation (Shadow Mode)

- [x] [#124 Add CQRS/ES migration specification to SPEC.md](https://github.com/ST0x-Technology/st0x.liquidity/issues/124)
  - PR:
    [#145 Phased CQRS/ES migration specification](https://github.com/ST0x-Technology/st0x.liquidity/pull/145)
- [x] [#125 Implement event sourcing infrastructure with SchwabAuth aggregate](https://github.com/ST0x-Technology/st0x.liquidity/issues/125)
  - PR:
    [#146 Event sourcing infrastructure and SchwabAuth aggregate](https://github.com/ST0x-Technology/st0x.liquidity/pull/146)
- [x] [#126 Implement OnChainTrade aggregate](https://github.com/ST0x-Technology/st0x.liquidity/issues/126)
  - PR:
    [#160 onchain trade aggregate](https://github.com/ST0x-Technology/st0x.liquidity/pull/160)
- [x] [#127 Implement Position aggregate](https://github.com/ST0x-Technology/st0x.liquidity/issues/127)
  - PR:
    [#148 Position aggregate](https://github.com/ST0x-Technology/st0x.liquidity/pull/148)
- [x] [#128 Implement OffchainOrder aggregate](https://github.com/ST0x-Technology/st0x.liquidity/issues/128)
  - PR:
    [#149 OffchainOrder aggregate](https://github.com/ST0x-Technology/st0x.liquidity/pull/149)
- [x] [#129 Implement data migration binary for existing CRUD data](https://github.com/ST0x-Technology/st0x.liquidity/issues/129)
  - PR:
    [#150 Add existing data to the event store](https://github.com/ST0x-Technology/st0x.liquidity/pull/150)
- [x] [#130 Implement dual-write in Conductor](https://github.com/ST0x-Technology/st0x.liquidity/issues/130)
  - PR:
    [#158 implement dual-write for CQRS/ES aggregates](https://github.com/ST0x-Technology/st0x.liquidity/pull/158)
- [x] [#169 Lifecycle wrapper for event-sourced aggregates](https://github.com/ST0x-Technology/st0x.liquidity/issues/169)
  - PR:
    [#168 Lifecycle wrapper for event-sourced aggregates](https://github.com/ST0x-Technology/st0x.liquidity/pull/168)

#### Phase 2: Automated Rebalancing (Alpaca-Only)

- [x] [#132 Implement Alpaca crypto wallet service](https://github.com/ST0x-Technology/st0x.liquidity/issues/132)
  - PR:
    [#151 Implement Alpaca crypto wallet service](https://github.com/ST0x-Technology/st0x.liquidity/pull/151)
- [x] [#133 Implement Circle CCTP bridge service](https://github.com/ST0x-Technology/st0x.liquidity/issues/133)
  - PR:
    [#156 Add Circle CCTP bridge service for cross-chain USDC transfers](https://github.com/ST0x-Technology/st0x.liquidity/pull/156)
- [x] [#134 Implement Rain OrderBook vault service](https://github.com/ST0x-Technology/st0x.liquidity/issues/134)
  - PR:
    [#157 Add Rain OrderBook V5 vault service for deposits and withdrawals](https://github.com/ST0x-Technology/st0x.liquidity/pull/157)
- [x] [#135 Implement TokenizedEquityMint and EquityRedemption aggregates](https://github.com/ST0x-Technology/st0x.liquidity/issues/135)
  - PR:
    [#170 Tokenized equity mint and redemption aggregates](https://github.com/ST0x-Technology/st0x.liquidity/pull/170)
- [x] [#136 Implement UsdcRebalance aggregate](https://github.com/ST0x-Technology/st0x.liquidity/issues/136)
  - PR:
    [#159 UsdcRebalance aggregate](https://github.com/ST0x-Technology/st0x.liquidity/pull/159)
- [x] [#171 Implement Alpaca tokenization API client](https://github.com/ST0x-Technology/st0x.liquidity/issues/171)
  - PR:
    [#172 Implement Alpaca tokenization API client](https://github.com/ST0x-Technology/st0x.liquidity/pull/172)
- [x] [#137 Implement rebalancing orchestration](https://github.com/ST0x-Technology/st0x.liquidity/issues/137)
  - PR:
    [#173 Implement rebalancing orchestration managers](https://github.com/ST0x-Technology/st0x.liquidity/pull/173)
- [x] [#138 Implement InventoryView and imbalance detection](https://github.com/ST0x-Technology/st0x.liquidity/issues/138)
  - PR:
    [#174 Implement InventoryView and imbalance detection](https://github.com/ST0x-Technology/st0x.liquidity/pull/174)
- [x] [#139 Wire up rebalancing triggers from inventory events](https://github.com/ST0x-Technology/st0x.liquidity/issues/139)
  - PR:
    [#175 Wire up rebalancing triggers](https://github.com/ST0x-Technology/st0x.liquidity/pull/175)
- [x] [#120 Migrate to V5 orderbook](https://github.com/ST0x-Technology/st0x.liquidity/issues/120)
  - PR:
    [#154 Upgrade to V5 orderbook](https://github.com/ST0x-Technology/st0x.liquidity/pull/154)
- [x] [#224 Manual rebalancing CLI commands](https://github.com/ST0x-Technology/st0x.liquidity/issues/224)
  - PR:
    [#190 manual rebalancing CLI](https://github.com/ST0x-Technology/st0x.liquidity/pull/190)

### Completed: Multi-Crate Architecture Phase 1

- [x] [#199 Proposal: multi-crate architecture for faster builds and stricter boundaries](https://github.com/ST0x-Technology/st0x.liquidity/issues/199)
  - PR:
    [#200 Add multi-crate architecture section to SPEC.md](https://github.com/ST0x-Technology/st0x.liquidity/pull/200)
- [x] [#194 Multi-crate architecture: prerequisite refactors](https://github.com/ST0x-Technology/st0x.liquidity/issues/194)
  - PR:
    [#222 decouple VaultService from Evm, remove signer type parameter](https://github.com/ST0x-Technology/st0x.liquidity/pull/222)

### Completed: Bug Fixes

- [x] [#261 Fix inventory tracking post-upgrade bugs](https://github.com/ST0x-Technology/st0x.liquidity/issues/261)
  - PR:
    [#262 don't rely on reverse cache lookup of token address](https://github.com/ST0x-Technology/st0x.liquidity/pull/262)
- [x] [#250 Successful fractional Alpaca orders show as errors in logs](https://github.com/ST0x-Technology/st0x.liquidity/issues/250)
  - PR:
    [#252 fix successful orders getting marked as failed](https://github.com/ST0x-Technology/st0x.liquidity/pull/252)
- [x] [#245 Counter trades use whole shares instead of fractional shares](https://github.com/ST0x-Technology/st0x.liquidity/issues/245)
  - PR:
    [#246 Fix fractional counter trades](https://github.com/ST0x-Technology/st0x.liquidity/pull/246)
- [x] [#243 Intermittent RPC failures with load-balanced providers](https://github.com/ST0x-Technology/st0x.liquidity/issues/243)
  - PR:
    [#242 improve onchain ops reliability](https://github.com/ST0x-Technology/st0x.liquidity/pull/242)
- [x] [#238 CCTP fee API returns float for minimum_fee, deserialization fails](https://github.com/ST0x-Technology/st0x.liquidity/issues/238)
  - PR:
    [#239 fix fee/slippage handling during rebalancing](https://github.com/ST0x-Technology/st0x.liquidity/pull/239)
- [x] [#231 Alpaca Broker API market hours check fails with response decoding error](https://github.com/ST0x-Technology/st0x.liquidity/issues/231)
  - PR:
    [#232 Fix market hours decoding](https://github.com/ST0x-Technology/st0x.liquidity/pull/232)
- [x] [#220 Dual-write consistency bugs in offchain order handling](https://github.com/ST0x-Technology/st0x.liquidity/issues/220)
  - PR:
    [#219 Fix dual-write consistency bugs in offchain order handling](https://github.com/ST0x-Technology/st0x.liquidity/pull/219)
- [x] [#218 contract errors lack human-readable decoding](https://github.com/ST0x-Technology/st0x.liquidity/issues/218)
  - PR:
    [#217 add error decoding, refactor cctp module, fix nonce handling](https://github.com/ST0x-Technology/st0x.liquidity/pull/217)
- [x] [#216 RPC nodes return incomplete data from get_logs](https://github.com/ST0x-Technology/st0x.liquidity/issues/216)
  - PR:
    [#215 Add fallback to tx receipt when get_logs returns incomplete data](https://github.com/ST0x-Technology/st0x.liquidity/pull/215)
- [x] [#214 Fix denormalized net position column](https://github.com/ST0x-Technology/st0x.liquidity/issues/214)
  - PR:
    [#213 Fix net position tracking](https://github.com/ST0x-Technology/st0x.liquidity/pull/213)
- [x] [#211 Schwab orders stuck in PENDING status](https://github.com/ST0x-Technology/st0x.liquidity/issues/211)
  - PR:
    [#210 Fix legacy table not updating from PENDING to SUBMITTED](https://github.com/ST0x-Technology/st0x.liquidity/pull/210)
- [x] [#167 tSPLG hedging as SPYM](https://github.com/ST0x-Technology/st0x.liquidity/issues/167)
  - PR:
    [#166 splg symbol](https://github.com/ST0x-Technology/st0x.liquidity/pull/166)
- [x] [#164 Fix take order inputs and outputs](https://github.com/ST0x-Technology/st0x.liquidity/issues/164)
  - PR:
    [#163 fix take order IO handling](https://github.com/ST0x-Technology/st0x.liquidity/pull/163)
- [x] [#162 Fix rain-math-float deployment bug](https://github.com/ST0x-Technology/st0x.liquidity/issues/162)
  - PR:
    [#161 fix deployment](https://github.com/ST0x-Technology/st0x.liquidity/pull/161)
- [x] [#119 Bot stuck in infinite retry loop due to stale PENDING execution](https://github.com/ST0x-Technology/st0x.liquidity/issues/119)
  - PR:
    [#118 Fix: Clean up stale PENDING executions to prevent infinite retry loops](https://github.com/ST0x-Technology/st0x.liquidity/pull/118)

### Completed: Multi-Broker Support

- [x] [#60 Create a complete abstraction for Broker](https://github.com/ST0x-Technology/st0x.liquidity/issues/60)
  - PR:
    [#72 broker trait](https://github.com/ST0x-Technology/st0x.liquidity/pull/72)
- [x] [#76 Integrate Alpaca](https://github.com/ST0x-Technology/st0x.liquidity/issues/76)
  - PR:
    [#83 Feat/alpaca](https://github.com/ST0x-Technology/st0x.liquidity/pull/83)
- [x] [#192 Add Alpaca Broker API executor](https://github.com/ST0x-Technology/st0x.liquidity/issues/192)
  - PR:
    [#191 Trading via Alpaca Broker API](https://github.com/ST0x-Technology/st0x.liquidity/pull/191)

### Completed: Performance Monitoring & Profitability Analysis

- [x] [#91 Track onchain Pyth prices](https://github.com/ST0x-Technology/st0x.liquidity/issues/91)
  - PR:
    [#84 Pyth prices](https://github.com/ST0x-Technology/st0x.liquidity/pull/84)
- [x] [#48 Calculate and save P&L for each Schwab trade](https://github.com/ST0x-Technology/st0x.liquidity/issues/48)
  - PR:
    [#90 Reporter service with P&L calculations](https://github.com/ST0x-Technology/st0x.liquidity/pull/90)
- [x] [#75 Track onchain gas fees](https://github.com/ST0x-Technology/st0x.liquidity/issues/75)
  - PR:
    [#82 gas fee tracking](https://github.com/ST0x-Technology/st0x.liquidity/pull/82)
- [x] [#79 track block timestamps](https://github.com/ST0x-Technology/st0x.liquidity/issues/79)
  - PR:
    [#80 Add block timestamp field](https://github.com/ST0x-Technology/st0x.liquidity/pull/80)
- [x] [#81 Fix block timestamp collection](https://github.com/ST0x-Technology/st0x.liquidity/issues/81)
  - PR:
    [#100 Fix block timestamp collection](https://github.com/ST0x-Technology/st0x.liquidity/pull/100)
- [x] [#9 Implement performance reporting process](https://github.com/ST0x-Technology/st0x.liquidity/issues/9)
  - PR:
    [#90 Reporter service with P&L calculations](https://github.com/ST0x-Technology/st0x.liquidity/pull/90)
- [x] [#6 Integrate HyperDX](https://github.com/ST0x-Technology/st0x.liquidity/issues/6)
  - PR:
    [#101 hyperdx](https://github.com/ST0x-Technology/st0x.liquidity/pull/101)
- [x] [#88 store schwab db tokens in encrypted format](https://github.com/ST0x-Technology/st0x.liquidity/issues/88)
  - PR:
    [#98 Fix/tokencryption](https://github.com/ST0x-Technology/st0x.liquidity/pull/98)

### Completed: Deployment & Infrastructure

- [x] [#11 Set up automated deployment](https://github.com/ST0x-Technology/st0x.liquidity/issues/11)
  - PR: [#39 CD](https://github.com/ST0x-Technology/st0x.liquidity/pull/39)
- [x] [#106 fix two instance deployment configuration](https://github.com/ST0x-Technology/st0x.liquidity/issues/106)
  - PR:
    [#102 fix deployment & implement a rollback script](https://github.com/ST0x-Technology/st0x.liquidity/pull/102)
- [x] [#108 fix deployment permissions](https://github.com/ST0x-Technology/st0x.liquidity/issues/108)
  - PR:
    [#109 simplify permissions](https://github.com/ST0x-Technology/st0x.liquidity/pull/109)
- [x] [#110 fix logging and db env vars in deployment](https://github.com/ST0x-Technology/st0x.liquidity/issues/110)
  - PR:
    [#111 fix logging and db env vars](https://github.com/ST0x-Technology/st0x.liquidity/pull/111)

### Completed: Code Quality & Documentation

- [x] [#50 Centralize suffix handling logic](https://github.com/ST0x-Technology/st0x.liquidity/issues/50)
- [x] [#68 Refactor visibility levels to improve dead code identification](https://github.com/ST0x-Technology/st0x.liquidity/issues/68)
- [x] [#67 Fix market hours check for weekends/holidays](https://github.com/ST0x-Technology/st0x.liquidity/issues/67)
- [x] [#65 Address the 100% CPU utilization issue](https://github.com/ST0x-Technology/st0x.liquidity/issues/65)
- [x] [#58 Add dry-run mode to the bot](https://github.com/ST0x-Technology/st0x.liquidity/issues/58)
  - PR:
    [#55 Dry run mode](https://github.com/ST0x-Technology/st0x.liquidity/pull/55)
- [x] [#97 Fix README and generalize AI docs](https://github.com/ST0x-Technology/st0x.liquidity/issues/97)
  - PR:
    [#93 AGENTS.md and update docs](https://github.com/ST0x-Technology/st0x.liquidity/pull/93)
- [x] [#104 update ai and human docs post broker crate](https://github.com/ST0x-Technology/st0x.liquidity/issues/104)
  - PR:
    [#103 broker crate docs and AGENTS.md downsize to fix Claude Code warning](https://github.com/ST0x-Technology/st0x.liquidity/pull/103)

### Completed: MVP Core Functionality

The core arbitrage bot functionality is now live and operational:

- [x] [#3 Set up trade event stream (Clear and TakeOrder events)](https://github.com/ST0x-Technology/st0x.liquidity/issues/3)
  - PR:
    [#1 Trade event stream](https://github.com/ST0x-Technology/st0x.liquidity/pull/1)
- [x] [#12 Set up CI checks](https://github.com/ST0x-Technology/st0x.liquidity/issues/12)
  - PR:
    [#13 Set up the project](https://github.com/ST0x-Technology/st0x.liquidity/pull/13)
- [x] [#14 Set up the crate with bindings and env](https://github.com/ST0x-Technology/st0x.liquidity/issues/14)
  - PR:
    [#13 Set up the project](https://github.com/ST0x-Technology/st0x.liquidity/pull/13)
- [x] [#4 Set up SQLite and store incoming trades](https://github.com/ST0x-Technology/st0x.liquidity/issues/4)
- [x] [#19 Take the opposite side of the trade on Schwab](https://github.com/ST0x-Technology/st0x.liquidity/issues/19)
  - PR:
    [#20 Take the opposite side of the trade](https://github.com/ST0x-Technology/st0x.liquidity/pull/20)
- [x] [#22 Create a CLI command for manually placing a Schwab trade](https://github.com/ST0x-Technology/st0x.liquidity/issues/22)
  - PR:
    [#25 Create a CLI command for manually placing a Schwab trade](https://github.com/ST0x-Technology/st0x.liquidity/pull/25)
- [x] [#23 Create a CLI command for manually taking the opposite end of an onchain order](https://github.com/ST0x-Technology/st0x.liquidity/issues/23)
  - PR:
    [#28 Create a CLI command for taking the opposite side of a trade](https://github.com/ST0x-Technology/st0x.liquidity/pull/28)
- [x] [#24 Stop the bot during market close and restart on market open](https://github.com/ST0x-Technology/st0x.liquidity/issues/24)
  - PR:
    [#53 Only run the bot during market open hours](https://github.com/ST0x-Technology/st0x.liquidity/pull/53)

### [SPEC.md](https://github.com/ST0x-Technology/st0x.liquidity/blob/master/SPEC.md)

See the specification for detailed system behavior documentation.
