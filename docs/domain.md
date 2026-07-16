# Domain Model

This document is the source of truth for terminology and naming conventions in
the st0x liquidity codebase. Code names must be consistent with this document.

## Naming Conventions

### Config / Secrets / Ctx / Env

Types follow a strict naming convention based on their role:

| Suffix     | Meaning                  | Source                               | Example                       |
| ---------- | ------------------------ | ------------------------------------ | ----------------------------- |
| `*Env`     | CLI args / env vars      | clap-powered struct                  | `Env`, `CliEnv`               |
| `*Config`  | Non-secret settings      | Plaintext TOML (`config/*.toml`)     | `BrokerConfig`, `EvmConfig`   |
| `*Secrets` | Secret credentials       | Encrypted TOML (`secret/*.toml.age`) | `BrokerSecrets`, `EvmSecrets` |
| `*Ctx`     | Combined runtime context | Assembled from runtime state         | `BrokerCtx`, `EvmCtx`         |

**Rules:**

- A `*Env` type is a clap-powered struct that captures CLI flags and environment
  variables. Its only purpose is to specify paths to config and secrets files.
  It should not contain application logic or configuration values itself.
- A type named `*Config` must never contain secret data (API keys, private keys,
  RPC URLs with credentials).
- A type named `*Secrets` must never contain non-secret data.
- A `*Ctx` type represents everything a subsystem needs to operate. It combines
  config and secrets, and may include additional runtime state (e.g., a database
  pool). It is the type passed to constructors and initialization functions.
- The Config/Secrets distinction is a concern of the crate that loads TOML
  files. Library crates (e.g., `st0x-execution`) may use `*Ctx` as their public
  construction interface without splitting Config from Secrets - that split
  belongs in the caller.

### Abstraction Coupling

Module-level documentation describes the module's **domain purpose**, not the
framework powering it. A trade accounting module is about accounting for trades
-- the fact that apalis happens to be the job framework is an implementation
detail that belongs in the implementation, not the module framing.

Struct-level documentation may mention the framework when relevant to
construction or wiring (e.g., "injected via `Data`"), but the module `//!`
docstring must remain implementation-agnostic.

### Job Queues

A `SqliteStorage<Job, ...>` type alias is a **job queue**, not "storage".
Variables and parameters of this type must be named `job_queue`, never
`storage`. The primary purpose is queueing jobs for processing; the fact that
SQLite backs it is an implementation detail.

### Operator Recovery Verbs

CLI commands that recover stuck or failed operations follow a fixed vocabulary
so each command's intent is unambiguous. A command is grouped under the object
it acts on (`transfer`, `position`, `view`, `cctp`) and uses exactly one of six
verbs -- one verb per intent:

- `resume` -- drive an interrupted, non-terminal operation forward; idempotent,
  never forces a transition.
- `recheck` -- re-query the external provider and apply the result (REST).
- `fail` -- force a stuck non-terminal operation to its `Failed` terminal;
  `--reason` required.
- `reconcile` -- release an already-terminal-failed operation handled
  out-of-band: clears the stranded guard and zeroes source-venue inflight
  without crediting available (the funds already left the source venue, whether
  stuck in transit or failed after arrival); `--reason` required.
- `set` -- overwrite aggregate-derived state to an operator-asserted value;
  `--reason` required.
- `rebuild` -- recompute a projection from the event log.

Two commands sit deliberately outside the verb set, named for their exact
effect: `cctp complete-mint` (a raw on-chain primitive touching no aggregate)
and `position release-hedge` (releases a Position's stuck pending-offchain-order
pointer so hedging can retry; `--reason` required).

`recover` is forbidden as a CLI verb -- it spans three unrelated intents and
carries no information. All state-mutating verbs emit events through aggregate
commands (CQRS); none write the `events` table directly. See the "Operator
Recovery Surface" section of [SPEC.md](../SPEC.md) for the full treatment.

### Refactoring Completeness

When renaming a type, **all** related names must change: variable names,
function names, parameters, test helpers. Zero mentions of the old name may
remain. A type rename without updating the surrounding vocabulary is incomplete
and confusing.

## Domain Glossary

### Trading Venue

A location where trades are executed. The system operates across two types of
venue:

- **Onchain venue (Raindex)**: A decentralized exchange on Base where tokenized
  equities and USDC are traded via limit orders placed and priced externally.
  The bot monitors fills on those orders and manages vault balances
  (deposits/withdrawals) for rebalancing; it does not add, remove, or reprice
  the orders themselves.
- **Offchain venue (brokerage)**: A traditional equity market accessed through
  the Alpaca Broker API. The bot executes offsetting trades here to hedge
  onchain exposure.

### Executor

An abstraction over an offchain trading venue. The `Executor` trait provides a
uniform interface for placing market orders, polling order status, and querying
inventory regardless of which brokerage is used. Implementations:
`AlpacaBrokerApi`, `MockExecutor`.

### Conductor

The orchestration layer that coordinates all concurrent subsystems within a bot
session. It initializes CQRS frameworks, subscribes to blockchain events, spawns
background tasks (order polling, position checking, queue processing,
rebalancing), and respects market hours by pausing trading outside open hours.

### Symbol

A type-safe stock ticker represented as a newtype (`Symbol(String)`). Tokenized
equities appear onchain in two forms, modeled by `TokenizedSymbol<Form>`:

- `tAAPL` - 1:1 minted tokenized shares (the unwrapped underlying of the `wt`
  vault shares; no dedicated Rust type)
- `wtCOIN` - ERC-4626 vault shares wrapping tokenized equity
  (`WrappedTokenizedShares`)
- `AAPL` - base equity stock (offchain, at the brokerage)
- `USDC` - stablecoin used as the quote currency onchain

Both forms resolve to the same base `Symbol` for offchain hedging (e.g. `tAAPL`
and `wtAAPL` both resolve to `AAPL`). `WrappedTokenizedShares` is the only
`TokenizationForm` implemented in code: Raindex events parse
`TokenizedSymbol<WrappedTokenizedShares>` (`wt` prefix), while Alpaca
tokenization operates on the base `Symbol` directly.

### Hedge

The core mechanism for capturing arbitrage while minimizing directional risk.
When an onchain trade fills (e.g., buying tokenized AAPL), the bot executes an
offsetting trade in the opposite direction at the offchain venue (selling AAPL
shares). This keeps the bot's net position close to zero while capturing the
spread between onchain and offchain prices.

Direction logic: positive net position -> sell offchain; negative net position
-> buy offchain.

### Post-Close Gap

The calendar-derived interval between the current extended-session close and the
next trading session. `PostCloseGap` distinguishes an ordinary overnight (next
session is the following calendar day), a multi-day closure (weekend or exchange
holiday), and unknown next-session metadata. The distinction drives close
flattening without hardcoding weekdays or holiday dates.

### Position

A CQRS aggregate tracking a single symbol's accumulated exposure across both
venues. It records accumulated long and short volumes, the current net exposure,
and the last known price. A position determines when it is ready for execution
based on its configured execution threshold.

### Execution Threshold

The minimum net exposure required before the system triggers an offsetting
offchain trade. Two modes:

- **Shares threshold**: Execute when net position reaches N shares (used by
  DryRun).
- **Dollar value threshold**: Execute when net position value reaches $N (used
  by Alpaca, which requires a $1 minimum for fractional trading).

### Rebalancing

Automated cross-venue inventory management that maintains target allocation
ratios between onchain and offchain holdings. Two dimensions:

- **Equity rebalancing**: If too many tokenized shares accumulate onchain,
  redeem them back to broker shares. If too many shares sit offchain, mint new
  tokens.
- **USDC rebalancing**: If too much USDC sits onchain, bridge it to the broker.
  If too much sits offchain, bridge it back to the onchain orderbook vault.

Rebalancing is only available with Alpaca Broker API (requires account-level
access for mint/redeem operations).

### Bridge

Infrastructure for moving assets between chains. Used by USDC rebalancing to
transfer stablecoins between Base (where the onchain venue operates) and
Ethereum (where broker on/off-ramps are available), via Circle CCTP.

### Tokenization

The process of creating or destroying onchain representations of offchain
equities:

- **Mint**: Convert broker-held shares into onchain tokenized equity tokens
  (e.g., 100 AAPL shares at Alpaca -> 100 tAAPL tokens on Base).
- **Redemption**: The reverse - burn onchain tokens to recover broker-held
  shares (e.g., 100 tAAPL tokens -> 100 AAPL shares at Alpaca).

Both operations provide an immutable audit trail, tracked as CQRS event-sourced
aggregates (`TokenizedEquityMint`, `EquityRedemption`).
