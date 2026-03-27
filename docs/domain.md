# Domain Model

This document is the source of truth for terminology and naming conventions in
the st0x liquidity codebase. Code names must be consistent with this document.

## Naming Conventions

### Config / Secrets / Ctx / Env

Types follow a strict naming convention based on their role:

| Suffix     | Meaning                  | Source                               | Example                       |
| ---------- | ------------------------ | ------------------------------------ | ----------------------------- |
| `*Env`     | CLI args / env vars      | clap-powered struct                  | `ServerEnv`, `ReporterEnv`    |
| `*Config`  | Non-secret settings      | Plaintext TOML (`config/*.toml`)     | `SchwabConfig`, `EvmConfig`   |
| `*Secrets` | Secret credentials       | Encrypted TOML (`secret/*.toml.age`) | `SchwabSecrets`, `EvmSecrets` |
| `*Ctx`     | Combined runtime context | Assembled from runtime state         | `SchwabCtx`, `EvmCtx`         |

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
  equities and USDC are traded via limit orders. The bot places and maintains
  orders on Raindex.
- **Offchain venue (brokerage)**: A traditional equity market accessed through a
  broker API (Alpaca Markets or Charles Schwab). The bot executes offsetting
  trades here to hedge onchain exposure.

### Executor

An abstraction over an offchain trading venue. The `Executor` trait provides a
uniform interface for placing market orders, polling order status, and querying
inventory regardless of which brokerage is used. Implementations: `Schwab`,
`AlpacaTradingApi`, `AlpacaBrokerApi`, `MockExecutor`.

### Conductor

The orchestration layer that coordinates all concurrent subsystems within a bot
session. It initializes CQRS frameworks, subscribes to blockchain events, spawns
background tasks (order polling, position checking, queue processing,
rebalancing), and respects market hours by pausing trading outside open hours.

### Symbol

A type-safe stock ticker represented as a newtype (`Symbol(String)`). Tokenized
equities appear onchain in two forms, modeled by `TokenizedSymbol<Form>`:

- `tAAPL` - 1:1 minted tokenized shares (`OneToOneTokenizedShares`)
- `wtCOIN` - ERC-4626 vault shares wrapping tokenized equity
  (`WrappedTokenizedShares`)
- `AAPL` - base equity stock (offchain, at the brokerage)
- `USDC` - stablecoin used as the quote currency onchain

Both forms resolve to the same base `Symbol` for offchain hedging (e.g. `tAAPL`
and `wtAAPL` both resolve to `AAPL`). Each context uses the specific form it
knows: Raindex events use `TokenizedSymbol<WrappedTokenizedShares>`, Alpaca
tokenization uses `TokenizedSymbol<OneToOneTokenizedShares>`.

### Dislocation

Net directional exposure caused by counterparty fills on Raindex. When someone
takes liquidity from our onchain orders, we gain or lose tokenized equity
exposure that we didn't intend to hold — this displacement from zero net
exposure is a dislocation. The system resolves dislocations by placing
offsetting trades at the offchain venue.

Dislocation is distinct from imbalance: dislocation is about unwanted
directional exposure (per asset, caused by trading), while imbalance is about
venue allocation ratios (caused by inventory drift over time).

Direction logic: positive dislocation (long exposure) -> sell offchain; negative
dislocation (short exposure) -> buy offchain.

### Imbalance

The deviation of the current venue allocation ratio from its target. Each asset
has a target onchain ratio (e.g., 50% on Raindex, 50% on Alpaca) with an
allowed deviation band. When the actual ratio drifts outside the band, the
system triggers a cross-venue transfer (mint, redemption, or USDC bridge) to
restore the target allocation.

Configured via `ImbalanceThreshold { target, deviation }` per asset category
(equity and USDC independently).

### Position

A CQRS aggregate tracking a single symbol's accumulated exposure across both
venues. It records accumulated long and short volumes, the current net
dislocation, and the last known price. A position determines when it is ready
for execution based on its configured execution threshold.

### Execution Threshold

The minimum dislocation required before the system triggers an offsetting
offchain trade. Two modes:

- **Shares threshold**: Execute when dislocation reaches N shares (used by
  Schwab, which does not support fractional shares, and DryRun).
- **Dollar value threshold**: Execute when dislocation value reaches $N (used
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

Both operations are tracked as CQRS event-sourced aggregates
(`TokenizedEquityMint`, `EquityRedemption`) providing an immutable audit trail.

### Reporter

A standalone service (separate binary) that computes P&L metrics from trade
history. It uses FIFO inventory accounting to calculate realized P&L, cumulative
P&L, and net position for each symbol. Runs on a polling interval against the
same SQLite database as the server, but requires no secrets (only a plaintext
config with the database path).
