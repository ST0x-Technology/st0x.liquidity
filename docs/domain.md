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

### Hedge

The core mechanism for capturing arbitrage while minimizing directional risk.
When an onchain trade fills (e.g., buying tokenized AAPL), the bot executes an
offsetting trade in the opposite direction at the offchain venue (selling AAPL
shares). This keeps the bot's net position close to zero while capturing the
spread between onchain and offchain prices.

Direction logic: positive net position -> sell offchain; negative net position
-> buy offchain.

### Position

A CQRS aggregate tracking a single symbol's accumulated exposure across both
venues. It records accumulated long and short volumes, the current net exposure,
and the last known price. A position determines when it is ready for execution
based on its configured execution threshold.

### Execution Threshold

The minimum net exposure required before the system triggers an offsetting
offchain trade. Two modes:

- **Shares threshold**: Execute when net position reaches N shares (used by
  Schwab, which does not support fractional shares, and DryRun).
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

Both operations are tracked as CQRS event-sourced aggregates
(`TokenizedEquityMint`, `EquityRedemption`) providing an immutable audit trail.

### Reporter

A standalone service (separate binary) that computes P&L metrics from trade
history. It uses FIFO inventory accounting to calculate realized P&L, cumulative
P&L, and net position for each symbol. Runs on a polling interval against the
same SQLite database as the server, but requires no secrets (only a plaintext
config with the database path).
