# st0x.liquidity

## Overview

Tokenized equity market making system that provides onchain liquidity and
captures arbitrage profits.

- **Onchain Liquidity**: Raindex orders continuously offer to buy/sell tokenized
  equities at spreads around oracle prices
- **Automatic Hedging**: When liquidity is taken onchain, the Rust bot executes
  offsetting trades on traditional brokerages to hedge the change in exposure
- **Profit Capture**: Earns the spread on every trade while hedging directional
  exposure

The system enables efficient price discovery for onchain tokenized equity
markets by providing continuous two-sided liquidity.

## Features

- **Supported Executors**: Execute hedges through Alpaca Broker API (managed
  accounts, auto-rebalancing) or dry-run mode for testing
- **Real-Time Hedging**: WebSocket-based monitoring for near instant execution
  when onchain liquidity is taken
- **Fractional Share Support**: Executes fractional shares on Alpaca; dry-run
  mirrors the same execution model for testing
- **Alpaca Hedge Preflight**: Checks available offchain shares for sells and
  cash buying power for buys (includes unsettled T+1 equity-sale proceeds,
  excludes margin) before submitting Alpaca hedge orders
- **Serialized Counter-Trade Submission**: Within one bot process, queued and
  periodic hedge submissions share a lock and reserve budget against active
  offchain orders before placing new Alpaca counter-trades
- **Complete Audit Trail**: Database tracking linking every onchain trade to
  offchain hedge executions
- **Exposure Hedging**: Automatically executes offsetting trades to reduce
  directional exposure from onchain fills
- **Operator Vault Controls**: CLI supports generic ERC20 deposits to and
  withdrawals from Raindex vaults, with a USDC-specific withdrawal shortcut
- **Orchestrator-Mode Mint Authorization**: For assets issuance serves through
  an `ST0xOrchestrator` vault, signs an EIP-712 MintAuthV1 recipient
  authorization (nonce persisted before delivery, byte-identical retries) and
  delivers it to issuance before the mint can complete; vault-direct assets are
  untouched (see SPEC.md "Mint Recipient Authorization")

## Getting Started

### Prerequisites

- **Nix with flakes enabled** - For reproducible development environment
- **[direnv](https://direnv.net/)** (recommended) - The repo includes an
- `.envrc` that automatically loads the Nix dev shell when you `cd` into the
  project. Without direnv, run `nix develop` manually in each terminal

### Development Setup

The Rust build has two compile-time dependencies that must be set up before
`cargo check` will succeed:

1. **Solidity ABI artifacts** - The Rust `sol!` macros reference JSON ABI files
   that are produced by per-feature Nix derivations under `nix/` and exposed via
   `ST0X_*_ABI` environment variables when you enter the dev shell -- no
   submodule checkout or manual `forge build` step required
2. **SQLite database** - The `sqlx::query!` macros validate SQL against a live
   database at compile time

```bash
git clone https://github.com/ST0x-Technology/st0x.liquidity.git
cd st0x.liquidity
direnv allow      # or `nix develop` if not using direnv
sqlx db create    # create SQLite database for sqlx macros
sqlx migrate run  # apply migrations
cargo check       # verify setup
```

Solidity ABIs are produced as per-feature Nix derivations under `nix/`
(`forge-std.nix`, `rain-math-float.nix`, `rain-orderbook.nix`, and
`raindex-governance.nix` -- the shared `RaindexInventory` ABI) and exposed to
`cargo` through environment variables set by the dev shell -- no submodule
checkout, no manual `forge build` required.

To reset the database: `sqlx db reset -y`

**AI agents**: For Rust/TypeScript work, run agents inside the dev shell so they
have access to all tooling (e.g., `nix develop -c claude`). For editing Nix
code, a regular shell is fine.

### Configuration

The application uses TOML configuration files split into plaintext config and
encrypted secrets. See `example.config.toml` and `example.secrets.toml` for all
available options. Operational intervals such as
`apalis_finished_job_cleanup_interval_secs` must be explicitly configured and
nonzero, as must `inventory_divergence_threshold` (the number of consecutive
offchain polls that must diverge from the inventory view before the poller
escalates a forced snapshot reconciliation).

When equities are configured, `[pricing].ws_url` and the encrypted
`[pricing].api_key` are also required. Remote endpoints must use `wss://`;
plaintext `ws://` is limited to `st0x-pricing` and loopback. The server
subscribes to the pricing service's Raindex `wt<symbol>` stream for
dashboard-only USD values; an outage leaves those values unavailable without
stopping hedging or falling back to a trade fill.

The `[chains.<name>.trading]` table requires an explicit `inventory_mode`
(`"legacy"` or `"managed"`) and a `vault_owner` address (the on-chain owner the
vaults are keyed by; no fallback). `"managed"` additionally requires an
`inventory` address (the shared `RaindexInventory` the bot operates via
`OPERATOR_ROLE`) and is forbidden from being set under `"legacy"`. Its required
`inventory_adapters` is an array of `{ venue, operator }` records attributing
public operator addresses to venues such as Bebop and Uniswap v4; unknown
operators remain visibly unattributed. This deployment metadata belongs in
plaintext config, not secrets or environment variables. Trade protocol v3
preserves configured and unknown onchain venues; older protocol versions
collapse adapter and Unknown Onchain venues to Raindex for compatibility. See
the `[chains.base]` block in `example.config.toml` for the full field
documentation.

The chain registry enforces four rules at startup, each failing closed rather
than skipping the chain:

- Every `[chains.<name>]` table needs a matching `[chains.<name>]` secrets entry
  supplying its `rpc_url`, and vice versa. Acting on a chain with no endpoint,
  and holding an endpoint for a chain with no addresses, both leave fund routing
  undefined.
- Exactly one chain may carry a `[trading]` table. The shape admits more, but
  the runtime drives a single fill watcher, so a second trading chain would be
  fully described and never read -- unhedged exposure presenting as a working
  config.
- At least one chain must be configured, and at least one of those must trade.
- Every `[chains.<name>]` entry must declare `required_confirmations` directly
  on the chain table (not inside `[trading]`). There is no default: the depth
  encodes that chain's reorg behaviour, so omitting it fails config parsing.

Current broker support is limited to `alpaca-broker-api` and `dry-run`.

#### Validating a config

`validate-config` runs the startup validation without starting the server or
reaching any external service:

```bash
# Config half only -- no secrets, no network, no clock.
cargo run --bin validate-config -- --config config/prod/st0x-hedge.toml

# Everything the deploy gate checks, including the config/secrets cross-checks.
cargo run --bin validate-config -- --config path/to/config.toml --secrets path/to/secrets.toml
```

Without `--secrets` it judges the config file alone: schema (unknown keys are
rejected), the port, chain, asset and `[rebalancing]` cross-field rules, and
every value the config carries on its own. What it cannot see is what the
secrets file supplies -- broker credentials, per-chain `rpc_url`s, wallet keys,
pricing and issuance API keys -- which stays the deploy gate's job. Both modes
run the same checks the bot runs at boot, so a rule broken here breaks startup.

Because the secrets-free mode needs nothing but the file, CI validates every
config the repository ships (`config/**/*.toml`, `example.config.toml`,
`e2e/config.toml`) on every pull request, via the
`every_repo_config_passes_config_only_validation` test in `st0x-config`. A
config edit is therefore caught in the pull request that makes it, rather than
by a deployed service refusing to boot.

```bash
cargo run --bin server -- --config path/to/config.toml --secrets path/to/secrets.toml
```

Manual wrap of tokenized equity into wrapped vault shares (requires rebalancing
mode and a configured liquidity wallet for the selected network):

```bash
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml wrap-equity --symbol AAPL --quantity 10.5
```

Manual unwrap of wrapped equity shares (requires rebalancing mode and a
configured liquidity wallet for the selected network):

```bash
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml unwrap-equity --symbol AAPL --quantity 10.5
```

Both commands default to Base and resolve addresses from [assets.equities]. On a
non Base network, pass the target network and the st0x.registry token list for
it:

```bash
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml wrap-equity --symbol RKLB --quantity 0.1 --network ethereum --registry path/to/st0x.registry/token-lists/ethereum.json
```

Manual cancellation of an open Alpaca order by the id printed at placement:

```bash
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml cancel 61e7b016-9c91-4a97-b912-615c9d365c9d
```

A cancel for an id the broker does not know reports it as unknown, and a cancel
for an order that already filled or was cancelled reports it as no longer
cancelable — neither is an error.

Manual repair of local position tracking after an operator trade or rebalance:

```bash
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml position set --symbol SPYM --zero --reason "manual rebalance completed"
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml position set --symbol SPYM --long 100 --price 200 --reason "manual buy not observed by bot"
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml position set --symbol SPYM --short 12.5 --price 200 --reason "manual sell not observed by bot"
```

`--price` (USDC per share) is required for a nonzero target when the symbol uses
a dollar-value execution threshold and no price is already known; without it the
repaired exposure could never be valued and would never hedge.

`position set` is rejected while the symbol still has a pending offchain hedge
order; resolve it first with `position release-hedge`, then retry.

After verifying a missing daily snapshot mark against a historical-price source,
set the preceding regular-session close through the audited CQRS repair command:

```bash
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml portfolio-snapshot set --day 2026-07-20 --symbol AAPL --usd-mark 211.18 --observed-at 2026-07-17T20:00:00Z --source "Nasdaq historical close" --reason "repair missing snapshot mark"
```

The repair updates every captured location for that symbol and day without
changing the live position price. If the read model needs recovery, stop the bot
first (the rebuild replays events read at its start; concurrent captures would
force it to be re-run), then replay all captured balances and corrections with:

```bash
cargo run -p st0x-cli -- --config path/to/config.toml --secrets path/to/secrets.toml view rebuild --aggregate portfolio-snapshot --all
```

### Brokerage Setup

**Alpaca Broker API** (managed accounts, supports auto-rebalancing):

For managed/omnibus accounts. Requires Broker API access from Alpaca. This is
the only integration that supports automatic portfolio rebalancing (USDC/equity
threshold-based).

Add credentials to your TOML config file under the `[broker]` section (see
`example.config.toml` and `example.secrets.toml`). Alpaca configs must also set
`broker.counter_trade_slippage_bps`, which controls the buy-side preflight
buffer and the protection bound on extended-hours limit orders;
`broker.extended_hours_reprice_timeout_secs`, the ordinary extended-hours
reprice cadence; `broker.close_flatten_reprice_timeout_secs`, the faster cadence
inside close-flatten; `broker.extended_hours_close_flatten_window_secs`, the
length of the final window before weekends, exchange holidays, or an unknown
next session; and `broker.close_flatten_cross_max_bps`, the maximum cross at the
session close. The maximum cross must be at least `counter_trade_slippage_bps`,
since the ramp starts there, and no more than `9,999` bps, the global
counter-trade slippage ceiling. All five are required and have no implicit
defaults. Ordinary extended-hours orders retain their 300-second timeout, while
close-flatten orders use the dedicated 60-second timeout and cross progressively
wider until the session closes.

Extended-hours limit orders use an ordered reference chain: an optional current
bid/ask quote source, the broker's **position mark**, then an emergency
`delayed_sip` quote. The current deployment has no primary quote provider, so
its effective behaviour remains mark first, delayed SIP second. The executor
capability is already present for a future source such as Alpaca SIP, and the
mark remains the fallback if that source is missing or fails. No market-data
feed config is exposed until a real provider is selected. See
[ADR 0019](adrs/0019-mark-priced-close-flatten-with-widening-cross.md).

Inside the close-flatten window the cross ramps linearly from
`counter_trade_slippage_bps` at the window's start to
`close_flatten_cross_max_bps` at the close, so each reprice crosses further than
the last and the bot converges on a fill before the gap. Outside it,
extended-hours orders keep the flat `counter_trade_slippage_bps` band. Only once
every reference source has failed does the attempt dead-letter (counted by
`hedge_dead_lettered_total{symbol,reason}`). Transient failures in queued
`PlaceHedge` attempts, such as timeouts and 5xx, receive three durable redrives
after 1s, 2s, and 4s; exhausting that budget increments the same dead-letter
metric. Scan-time transient and rate-limited preflight failures instead wait for
`CheckPositions` to re-enqueue the hedge on its next scan.

## Deployment

Both environments run on GCE VMs under docker compose, managed by
[t0.devops](https://github.com/T0Trade/t0.devops)
(`terraform/staging-liquidity`, `terraform/production-liquidity`). This repo
builds and ships the OCI images (`.github/workflows/build-oci.yml`: flake
`packages`, pushed and attested to the central Artifact Registry); a master
merge rolls staging automatically, production is a digest promotion in t0.devops
behind a PAM-gated apply. There is exactly one runtime config per environment
and it lives in t0.devops (config-as-data, mounted from Secret Manager); this
repo carries only test fixtures (`example.config.toml`, `e2e/config.toml`) and
the dividend-ops CLI config (`config/s01-issuer.toml`, see docs/cli-ops.md).

The old DigitalOcean/NixOS droplet world (deploy-rs, os.nix, infra/ Terraform,
nixos-anywhere bootstrap) is retired and deleted; the one agenix survivor is
`secret/s01-issuer.toml.age`, edited with `nix run .#secret` against
`secret/secrets.nix`.

### CI/CD

- **CI** (`.github/workflows/ci.yaml`): Builds all packages, runs tests and
  clippy inside nix derivations, and builds the dashboard. Runs for pull request
  activity and pushes to `master`.
- **CD** (`.github/workflows/build-oci.yml`): builds, signs, and pushes the
  images, then rolls staging. Runs on push to master.

To reproduce CI checks locally, use the same dev shell CI uses:

```bash
nix develop .#ci-backend -c cargo check --workspace
nix develop .#ci-backend -c cargo nextest run --workspace --all-features
nix develop .#ci-backend -c cargo clippy --workspace --all-targets --all-features
```

## Local Simulation

`nix run .#simulate` launches the full-system chaos eventual-consistency e2e
test (`full_system_concurrent`) with [mprocs](https://github.com/pvolok/mprocs)
running the dashboard and bot side-by-side. Trades fire in randomized order with
delayed broker fills; between rounds the test injects chaos (bot restarts, NAV
bumps, asset add/remove, broker latency) and then asserts hedging, mint, and
USDC rebalancing still converge. Open `http://localhost:5173` to watch the
dashboard while it runs. Set `SIMULATE_EXIT_AFTER_CHAOS=1` to exit once
assertions pass instead of idling for dashboard inspection.

`nix run .#simulate-market` runs the infinite market simulation instead —
continuous user trades at ~10-second intervals. Use this when you want to
observe long-running liquidity cycling rather than a single bounded chaos
scenario.

`nix run .#simulate-14d` starts the same stack as `simulate-market`, but
preloads 14 days of seeded hedge-latency, mint, redemption, and USDC-rebalance
history so Performance tab trends and the Transfers panel are populated
immediately.

`nix run .#simulate-trade-outcomes` starts the same stack as `simulate-market`,
but rotates every counter-trade through the three outcomes the trade history
renders: filled, rejected by the broker, and cancelled by the broker after a
partial fill. Use it to inspect the Status column, the venue/asset/time-range
filters, and the accepted/filled/unfilled breakdown in the detail panel without
waiting for a real failure.

`nix run .#simulate-failures` starts the same stack as `simulate-market`, then
creates failed mint and redemption rebalances whose mock Alpaca provider later
completes and prints the `transfer recheck` commands that recover them.

What `simulate-market` does:

1. Starts a local Anvil blockchain with deployed Raindex orderbook contracts
2. Deploys mock services: Alpaca broker, tokenization API, CCTP attestation
3. Creates Raindex liquidity orders — one buy and one sell per symbol (AAPL,
   TSLA) — all sharing a single USDC vault, with per-symbol equity vaults
4. Starts the bot (hedging, equity rebalancing, USDC bridging all enabled)
5. Starts the dashboard dev server
6. Continuously takes orders at 10-second intervals, simulating users buying and
   selling tokenized equities

The `simulate-14d` variant also preloads 14 days of history -- hedge-latency
cycles, equity mints, equity redemptions, and USDC rebalances (alternating
Alpaca<->Base direction) -- before live trades begin, so the Performance tab's
percentile charts and rebalance-stage breakdown, and the dashboard's Transfers
panel, all show a trend immediately instead of waiting for historical data to
accumulate. The dashboard's default `1W` view renders the most recent week of
that seed at daily granularity; switch to `2W` to see the full 14-day history,
still at daily granularity (within ~12h of the bot starting -- the seed is a
fixed point in time, so a much longer-running session ages its oldest day out of
the `2W` window).

The bot counter-trades each fill on the mock broker, mints/redeems to rebalance
equity supply between venues, and bridges USDC via mock CCTP to keep cash
balanced. If the system works correctly, the vaults never permanently drain —
the bot cycles liquidity back through hedging and rebalancing.

Press `Ctrl-C` to stop.

## Project Structure

### Cargo Workspace

Workspace crates:

- **`st0x-hedge`** (root) - Main arbitrage bot: event loop, CQRS/ES aggregates,
  conductor, and dashboard backend
- **`st0x-cli`** (`crates/cli/`) - Operator command-line application
- **`st0x-config`** (`crates/config/`) - TOML/secrets loading and runtime
  context assembly; restricted to the `st0x-hedge` and `st0x-cli` application
  crates
- **`st0x-dto`** (`crates/dto/`) - Dashboard DTOs and TypeScript binding
  generation
- **`st0x-execution`** (`crates/execution/`) - Standalone `Executor` trait
  abstraction with Alpaca Broker API and mock implementations
- **`st0x-tokenization`** (`crates/tokenization/`) - Standalone `Tokenizer`
  trait abstraction with Alpaca tokenization API and mock implementations
- **`st0x-bridge`** (`crates/bridge/`) - Cross-chain bridge abstractions and
  CCTP implementation
- **`st0x-raindex`** (`crates/raindex/`) - `Raindex` trait and shared domain
  types for Rain OrderBook vault operations
- **`st0x-registry`** (`crates/registry/`) - Shared reference-data registry:
  `SymbolCache` (token address -> symbol) and per-symbol `get_symbol_lock`
- **`st0x-wrapper`** (`crates/wrapper/`) - `Wrapper` trait and ERC-4626
  wrap/unwrap domain types
- **`st0x-evm`** (`crates/evm/`) - EVM wallet, provider, and test-chain support
- **`st0x-finance`** (`crates/finance/`) - Shared financial primitives:
  `Symbol`, `FractionalShares`, `Usdc`, `Usd`, and related domain types
- **`st0x-float-serde`** (`crates/float-serde/`) - Shared Rain Float formatting
  and serde helpers for workspace wire formats
- **`st0x-float-macro`** (`crates/float-macro/`) - Proc-macro for compile-time
  `Float` literals (`float!(1.5)`)

`st0x-event-sorcery` is an external git dependency (lives in the separate
[event-sorcery](https://github.com/ST0x-Technology/event-sorcery) repo) and is
not a workspace crate.

### Infrastructure

```
flake.nix                  # Nix flake: packages, devShells, OCI images
rust.nix                   # Rust package derivation
nix/oci-images.nix         # Bot/dashboard/datasette OCI images for the GCE VMs
keys.nix                   # age/SSH recipient roster for the s01 secret
config/
└── s01-issuer.toml        # dividend-ops CLI config (see docs/cli-ops.md)
secret/
├── secrets.nix            # ragenix rules (s01 secret only)
└── s01-issuer.toml.age    # encrypted dividend-ops CLI secrets
dashboard/                 # SvelteKit operations dashboard
.github/workflows/
├── ci.yaml                # Build, test, clippy, dashboard
├── build-oci.yml          # Build, sign, push the OCI images
└── release-tag.yml        # Promote a tagged image
```

## Development

### Building and Testing

```bash
cargo check                  # fast compilation check
cargo nextest run --workspace # run all tests
cargo clippy --workspace --all-targets --all-features -- -D clippy::all
cargo fmt                    # format Rust code
nix fmt                      # format Nix code (when editing .nix files)
```

### Flake Commands

All commands are run via `nix run .#<name>`. Commands that access infrastructure
or secrets decrypt state using your SSH key (`~/.ssh/id_ed25519` by default).
Pass `-i <path>` to use a different key.

**Development:**

| Command     | Usage                 | Notes                                           |
| ----------- | --------------------- | ----------------------------------------------- |
| `genBunNix` | `nix run .#genBunNix` | Regenerates `dashboard/bun.nix` from `bun.lock` |

**Building (Nix):**

| Command          | Usage                        | Notes               |
| ---------------- | ---------------------------- | ------------------- |
| `st0x-liquidity` | `nix build .#st0x-liquidity` | Build + tests       |
| `st0x-clippy`    | `nix build .#st0x-clippy`    | Clippy linting      |
| `st0x-dashboard` | `nix build .#st0x-dashboard` | SvelteKit dashboard |

**Deployment** happens in t0.devops (see the Deployment section above); this
repo has no deploy commands. The one operational wrapper left is
`nix run .#secret <file.age>` for the dividend-ops secret.

### Dashboard Dependencies

After changing `dashboard/bun.lock`, regenerate and format the Nix lockfile:

```bash
nix run .#genBunNix
nix fmt -- dashboard/bun.nix
```

CI will fail if `bun.nix` is out of sync with `bun.lock`.

## Documentation

- **[SPEC.md](SPEC.md)** - Complete technical specification and architecture
- **[docs/domain.md](docs/domain.md)** - Domain model, terminology, and naming
  conventions
- **[AGENTS.md](AGENTS.md)** - Development guidelines for AI-assisted coding
- **[example.config.toml](example.config.toml)** - Configuration reference
- **[example.secrets.toml](example.secrets.toml)** - Secrets reference

## How It Works

**Market Making Flow:**

1. **Provide Liquidity**: Raindex orders offer continuous two-sided liquidity
   for tokenized equities at spreads around oracle prices
2. **Detect Fills**: WebSocket monitors orderbook events when traders take
   liquidity onchain
3. **Parse Trade**: Extract details (symbol, amount, direction, price) from
   blockchain events
4. **Accumulate**: Batch positions until the configured execution threshold is
   reached (typically dollar-based for Alpaca Broker API, whole-share for
   `dry-run`)
5. **Hedge**: Execute offsetting market order on traditional brokerage to reduce
   exposure
6. **Track**: Maintain complete audit trail linking onchain fills to offchain
   hedges

**Profit Model**: The system earns the spread on each trade (difference between
onchain order price and offchain hedge execution price) while hedging
directional exposure.

**Note**: Alpaca Broker API supports fractional share execution and the bot can
hedge using dollar-value thresholds. `dry-run` remains available for local
testing with whole-share thresholds when that is operationally useful.
